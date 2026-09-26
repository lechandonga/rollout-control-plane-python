"""发布控制面核心服务：计划创建、推进、门禁、回滚、恢复。

并发模型：
- 每个计划一把锁（条纹锁），同一计划的所有变更串行化，因此并发请求
  的结果与某种串行执行一致；
- 后台放量任务受 max_concurrent_tasks 信号量限制，超限按 overflow_policy
  reject 或 queue；任务句柄在结束时必定释放，不泄漏后台资源。
持久化：每次状态变更立即经 Storage 原子落盘，中断后不丢计划、不留半更新。
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .config import ControlPlaneConfig
from .errors import (
    ConcurrencyLimitError,
    GateBlockedError,
    GroupConflictError,
    IllegalTransitionError,
    PlanNotFoundError,
    ValidationError,
)
from .models import (
    TRANSITIONS,
    GatePolicy,
    GateVerdict,
    HealthMetrics,
    InstanceGroupState,
    PlanStatus,
    ReleaseBatch,
    RolloutPlan,
    StablePoint,
)
from .storage import Storage

log = logging.getLogger("rollout.service")


class RolloutService:
    """所有公开操作在并发下与串行执行等价。"""

    _LOCK_STRIPES = 16

    def __init__(self, storage: Storage, config: ControlPlaneConfig | None = None):
        self.storage = storage
        self.config = config or ControlPlaneConfig()
        self._plan_locks = [threading.Lock() for _ in range(self._LOCK_STRIPES)]
        self._batch_locks = [threading.Lock() for _ in range(self._LOCK_STRIPES)]
        self._task_sem = threading.BoundedSemaphore(self.config.max_concurrent_tasks)
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_tasks,
            thread_name_prefix="rollout",
        )
        self._tasks: dict[str, threading.Event] = {}  # plan_id/batch_id -> cancel event
        self._tasks_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _lock_for(self, plan_id: str) -> threading.Lock:
        return self._plan_locks[hash(plan_id) % self._LOCK_STRIPES]

    def _batch_lock_for(self, batch_id: str) -> threading.Lock:
        return self._batch_locks[hash(batch_id) % self._LOCK_STRIPES]

    def _must_get(self, plan_id: str) -> RolloutPlan:
        plan = self.storage.get_plan(plan_id)
        if plan is None:
            raise PlanNotFoundError(f"plan {plan_id} not found")
        return plan

    def _must_get_batch(self, batch_id: str) -> ReleaseBatch:
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            raise PlanNotFoundError(f"batch {batch_id} not found")
        return batch

    def _assert_groups_free(self, groups: list[str]) -> None:
        """同一实例组上不允许两条相互冲突的进行中发布（单组计划或批次）。"""
        wanted = set(groups)
        for p in self.storage.list_plans():
            if p.group in wanted and not p.status.terminal:
                owner = (f"batch {p.batch_id}" if p.batch_id
                         else f"plan {p.plan_id}")
                raise GroupConflictError(p.group, owner)

    def _assert_not_batch_member(self, plan: RolloutPlan, action: str) -> None:
        """批次成员计划只能经批次接口操作，保证整批语义不被单计划绕过。"""
        if plan.batch_id is not None:
            raise IllegalTransitionError(
                plan.plan_id, plan.status.value, action,
                f"plan is managed by batch {plan.batch_id}; use batch APIs",
            )

    def _transition(self, plan: RolloutPlan, action: str) -> None:
        """状态机迁移校验：非法迁移抛出带区分原因的异常。"""
        key = (plan.status, action)
        if key not in TRANSITIONS:
            detail = ""
            if plan.status.terminal:
                detail = "plan is in a terminal state"
            raise IllegalTransitionError(plan.plan_id, plan.status.value, action, detail)
        old = plan.status
        plan.status = TRANSITIONS[key]
        plan.updated_at = time.time()
        log.info("plan %s: %s --%s--> %s", plan.plan_id, old.value, action, plan.status.value)

    def _evaluate_gate(self, plan: RolloutPlan) -> tuple[GateVerdict, str]:
        """健康门禁判定，返回 (判定, 依据) 供日志与观测使用。"""
        m = self.storage.get_health(plan.group)
        if not m.reported:
            return GateVerdict.WAITING, "no health metrics reported yet"
        if m.error_rate > plan.gate.max_error_rate:
            return GateVerdict.BLOCKED, (
                f"error_rate {m.error_rate:.3f} > max {plan.gate.max_error_rate:.3f}"
            )
        if m.unhealthy_ratio > plan.gate.max_unhealthy_ratio:
            return GateVerdict.BLOCKED, (
                f"unhealthy_ratio {m.unhealthy_ratio:.3f} > max "
                f"{plan.gate.max_unhealthy_ratio:.3f}"
            )
        return GateVerdict.PASS, (
            f"error_rate {m.error_rate:.3f} <= {plan.gate.max_error_rate:.3f}, "
            f"unhealthy_ratio {m.unhealthy_ratio:.3f} <= {plan.gate.max_unhealthy_ratio:.3f}"
        )

    def _apply_weight(self, plan: RolloutPlan, weight: int, version: str,
                      flush: bool = True) -> None:
        """把对外可见的组状态切到指定权重/版本并落盘（与计划同事务）。"""
        group = self.storage.get_group(plan.group) or InstanceGroupState(
            group=plan.group, version=version, weight=0
        )
        group.version = version
        group.weight = weight
        self.storage.save_group(group, flush=flush)

    # ------------------------------------------------------------------
    # 计划生命周期
    # ------------------------------------------------------------------
    def _validate_steps(self, steps: list[int]) -> None:
        if not steps:
            raise ValidationError("steps must be non-empty")
        prev = 0
        for w in steps:
            if not (0 < w <= 100):
                raise ValidationError(f"step weight {w} out of range (0,100]")
            if w <= prev:
                raise ValidationError("steps must be strictly increasing")
            if w - prev > self.config.max_step_weight_delta:
                raise ValidationError(
                    f"step delta {w - prev} exceeds max_step_weight_delta "
                    f"{self.config.max_step_weight_delta}"
                )
            prev = w
        if steps[-1] != 100:
            raise ValidationError("last step must be 100 (full rollout)")

    def _validate_gate(self, gate: GatePolicy) -> None:
        if not (0 <= gate.max_error_rate <= 1 and 0 <= gate.max_unhealthy_ratio <= 1):
            raise ValidationError("gate thresholds must be within [0,1]")

    def create_plan(self, version: str, group: str, steps: list[int],
                    gate: GatePolicy | None = None) -> RolloutPlan:
        if not version or not group:
            raise ValidationError("version and group must be non-empty")
        self._validate_steps(steps)
        gate = gate or GatePolicy()
        self._validate_gate(gate)
        self._assert_groups_free([group])
        plan = RolloutPlan.new(version, group, steps, gate)
        self.storage.save_plan(plan)
        log.info("plan %s created: version=%s group=%s steps=%s",
                 plan.plan_id, version, group, steps)
        return plan

    def start(self, plan_id: str) -> RolloutPlan:
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
            self._assert_not_batch_member(plan, "start")
            self._transition(plan, "start")
            self.storage.save_plan(plan)
            return plan

    def advance(self, plan_id: str) -> RolloutPlan:
        """推进一步。门禁未通过时不产生任何状态变更。"""
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
            self._assert_not_batch_member(plan, "advance")
            if plan.status != PlanStatus.RUNNING:
                self._transition(plan, "advance")  # 必然抛出，给出区分原因
            verdict, basis = self._evaluate_gate(plan)
            log.info("plan %s gate verdict=%s basis=%s", plan_id, verdict.value, basis)
            if verdict is GateVerdict.WAITING:
                raise GateBlockedError(f"plan {plan_id}: gate WAITING: {basis}")
            if verdict is GateVerdict.BLOCKED:
                raise GateBlockedError(f"plan {plan_id}: gate BLOCKED: {basis}")
            # 门禁通过：推进到下一步，权重单调递增，无跳跃（步长已校验）
            plan.step_index += 1
            weight = plan.steps[plan.step_index]
            self._apply_weight(plan, weight, plan.version)
            plan.stable = StablePoint(step_index=plan.step_index, weight=weight)
            if weight >= 100:
                self._transition(plan, "succeed")
                log.info("plan %s completed at weight=100", plan_id)
            else:
                self._transition(plan, "advance")
            self.storage.save_plan(plan)
            return plan

    def report_health(self, group: str, metrics: HealthMetrics) -> None:
        metrics.reported = True
        self.storage.save_health(group, metrics)
        log.info("group %s health reported: error_rate=%.3f unhealthy=%.3f",
                 group, metrics.error_rate, metrics.unhealthy_ratio)

    # ------------------------------------------------------------------
    # 回滚与中止
    # ------------------------------------------------------------------
    def rollback(self, plan_id: str, reason: str = "manual") -> RolloutPlan:
        """回滚到最近稳定点。幂等：已回滚完成的计划重复触发无副作用。"""
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
            self._assert_not_batch_member(plan, "rollback")
            if plan.status == PlanStatus.ROLLED_BACK:
                log.info("plan %s already ROLLED_BACK, no-op (reentrant)", plan_id)
                return plan
            if plan.status == PlanStatus.ROLLING_BACK:
                # 并发重复触发：直接完成本次回滚（等价于串行中的第二次调用）
                log.info("plan %s rollback already in progress, completing it", plan_id)
            elif plan.status == PlanStatus.RUNNING:
                self._transition(plan, "rollback")
            else:
                self._transition(plan, "rollback")  # 必然抛出，给出区分原因
            plan.failure_reason = reason
            self.storage.save_plan(plan)  # 先落 ROLLING_BACK，中断后可恢复
            self._finish_rollback(plan)
            return plan

    def _finish_rollback(self, plan: RolloutPlan, flush: bool = True) -> None:
        """恢复稳定点并进入 ROLLED_BACK。调用方须持有计划锁。"""
        stable_weight = plan.stable.weight
        stable_version = plan.version if plan.stable.step_index >= 0 else "previous"
        self._apply_weight(plan, stable_weight, stable_version, flush=flush)
        plan.step_index = plan.stable.step_index
        self._transition(plan, "finish_rollback")
        self.storage.save_plan(plan, flush=flush)
        log.warning("plan %s rolled back to stable weight=%d reason=%s",
                    plan.plan_id, stable_weight, plan.failure_reason)

    def abort(self, plan_id: str) -> RolloutPlan:
        """人工中止：PENDING 直接终止；RUNNING 走回滚路径回到稳定点。"""
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
            self._assert_not_batch_member(plan, "abort")
            if plan.status == PlanStatus.PENDING:
                self._transition(plan, "abort")
                self.storage.save_plan(plan)
                log.info("plan %s aborted before start", plan_id)
                return plan
        # RUNNING：复用回滚逻辑（内部重新取锁，串行化）
        return self.rollback(plan_id, reason="aborted")

    def fail(self, plan_id: str, reason: str = "health_failure") -> RolloutPlan:
        """失败注入：触发自动回滚。"""
        return self.rollback(plan_id, reason=reason)

    # ------------------------------------------------------------------
    # 后台自动放量（受并发上限与超时约束）
    # ------------------------------------------------------------------
    def run_async(self, plan_id: str, step_interval: float = 0.05) -> None:
        """后台自动放量：门禁通过则逐步推进；阻塞超过超时则自动回滚。"""
        plan = self._must_get(plan_id)
        self._assert_not_batch_member(plan, "run")
        acquired = self._task_sem.acquire(blocking=False)
        if not acquired:
            if self.config.overflow_policy == "queue":
                log.info("plan %s queued (task limit %d reached)",
                         plan_id, self.config.max_concurrent_tasks)
                self._task_sem.acquire()
            else:
                raise ConcurrencyLimitError(
                    f"task limit {self.config.max_concurrent_tasks} reached"
                )
        cancel = threading.Event()
        with self._tasks_lock:
            self._tasks[plan_id] = cancel
        try:
            self._executor.submit(self._run_loop, plan_id, step_interval, cancel)
        except Exception:
            self._task_sem.release()
            with self._tasks_lock:
                self._tasks.pop(plan_id, None)
            raise

    def _run_loop(self, plan_id: str, step_interval: float, cancel: threading.Event) -> None:
        try:
            while not cancel.is_set():
                plan = self.storage.get_plan(plan_id)
                if plan is None or plan.status != PlanStatus.RUNNING:
                    return
                try:
                    self.advance(plan_id)
                except GateBlockedError:
                    # 门禁阻塞：等待重试，超过单步超时则自动回滚
                    waited = 0.0
                    while waited < self.config.step_timeout_seconds:
                        time.sleep(step_interval)
                        waited += step_interval
                        try:
                            self.advance(plan_id)
                            break
                        except GateBlockedError:
                            continue
                    else:
                        log.warning("plan %s gate blocked beyond timeout, auto-rollback",
                                    plan_id)
                        self.rollback(plan_id, reason="gate_timeout")
                        return
                time.sleep(step_interval)
        finally:
            self._task_sem.release()
            with self._tasks_lock:
                self._tasks.pop(plan_id, None)

    def shutdown(self) -> None:
        """释放后台资源，不泄漏线程。"""
        with self._tasks_lock:
            for ev in self._tasks.values():
                ev.set()
        self._executor.shutdown(wait=True)

    # ------------------------------------------------------------------
    # 观测与恢复
    # ------------------------------------------------------------------
    def get_plan(self, plan_id: str) -> RolloutPlan:
        return self._must_get(plan_id)

    def observe(self, plan_id: str) -> dict:
        """可观测性：当前阶段、权重与门禁判定依据。"""
        plan = self._must_get(plan_id)
        verdict, basis = self._evaluate_gate(plan)
        group = self.storage.get_group(plan.group)
        return {
            "plan_id": plan.plan_id,
            "status": plan.status.value,
            "version": plan.version,
            "group": plan.group,
            "step_index": plan.step_index,
            "current_weight": plan.current_weight,
            "stable_weight": plan.stable.weight,
            "gate_verdict": verdict.value,
            "gate_basis": basis,
            "failure_reason": plan.failure_reason,
            "group_state": None if group is None else {
                "version": group.version, "weight": group.weight,
            },
        }

    def recover(self) -> None:
        """进程重启/任务中断后的恢复。

        - 批次 ROLLING_BACK：整批回滚被中断，把所有成员一起续做到
          ROLLED_BACK，不留「只回滚了一半」的对外状态；
        - ROLLING_BACK：回滚被中断，重新执行到 ROLLED_BACK；
        - RUNNING：放量被中断，对外组状态可能停在半更新，修复回最近稳定点；
        - 其余状态不变。所有修复均落盘。
        """
        self.storage.load()
        for batch in self.storage.list_batches():
            with self._batch_lock_for(batch.batch_id):
                if batch.status == PlanStatus.ROLLING_BACK:
                    log.warning(
                        "recover: batch %s interrupted mid-rollback, finishing",
                        batch.batch_id)
                    members = self._batch_members(batch)
                    for plan in members:
                        if plan.status == PlanStatus.RUNNING:
                            # 崩溃发生在阶段一落盘前：补齐整批标记
                            self._transition(plan, "rollback")
                            plan.failure_reason = batch.failure_reason
                            self.storage.save_plan(plan, flush=False)
                    self.storage.save_batch(batch, members)
                    self._finish_batch_rollback(batch, members)
        for plan in self.storage.list_plans():
            with self._lock_for(plan.plan_id):
                if plan.status == PlanStatus.ROLLING_BACK:
                    log.warning("recover: plan %s interrupted mid-rollback, finishing",
                                plan.plan_id)
                    self._finish_rollback(plan)
                elif plan.status == PlanStatus.RUNNING:
                    group = self.storage.get_group(plan.group)
                    if group and group.weight != plan.stable.weight:
                        log.warning(
                            "recover: plan %s group weight %d != stable %d, repairing",
                            plan.plan_id, group.weight, plan.stable.weight)
                        self._apply_weight(plan, plan.stable.weight, plan.version)
                        plan.step_index = plan.stable.step_index
                        self.storage.save_plan(plan)

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """等待所有后台任务自然结束（不取消）。超时返回 False。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._tasks_lock:
                if not self._tasks:
                    return True
            time.sleep(0.01)
        return False

    # ------------------------------------------------------------------
    # 多组协同发布批次
    # ------------------------------------------------------------------
    def _batch_transition(self, batch: ReleaseBatch, action: str) -> None:
        """批次状态机迁移校验（复用与计划相同的迁移表）。"""
        key = (batch.status, action)
        if key not in TRANSITIONS:
            detail = ""
            if batch.status.terminal:
                detail = "batch is in a terminal state"
            raise IllegalTransitionError(batch.batch_id, batch.status.value,
                                         action, detail)
        old = batch.status
        batch.status = TRANSITIONS[key]
        batch.updated_at = time.time()
        log.info("batch %s: %s --%s--> %s",
                 batch.batch_id, old.value, action, batch.status.value)

    def _batch_members(self, batch: ReleaseBatch) -> list[RolloutPlan]:
        plans = []
        for pid in batch.plan_ids:
            plan = self.storage.get_plan(pid)
            if plan is None:
                raise PlanNotFoundError(
                    f"batch {batch.batch_id}: member plan {pid} missing"
                )
            plans.append(plan)
        return plans

    def create_batch(self, members: list[dict]) -> ReleaseBatch:
        """创建覆盖多个实例组的发布批次。

        members: [{"group": ..., "version": ..., "steps": [...], "gate": ...}, ...]
        每个组有独立的版本与放量节奏；批次与成员计划在同一次原子落盘写入。
        """
        if not members:
            raise ValidationError("batch must contain at least one member group")
        groups = [m.get("group", "") for m in members]
        if any(not g for g in groups):
            raise ValidationError("every member must have a non-empty group")
        if len(set(groups)) != len(groups):
            raise ValidationError("duplicate group in batch members")
        for m in members:
            if not m.get("version"):
                raise ValidationError("every member must have a non-empty version")
            self._validate_steps(list(m.get("steps") or []))
            self._validate_gate(m.get("gate") or GatePolicy())
        # 与任何进行中发布（单组计划或其他批次）冲突 -> 可区分原因
        self._assert_groups_free(groups)
        batch = ReleaseBatch.new(plan_ids=[])  # 先拿 batch_id
        plans = [
            RolloutPlan.new(m["version"], m["group"], list(m["steps"]),
                            m.get("gate") or GatePolicy(), batch_id=batch.batch_id)
            for m in members
        ]
        batch.plan_ids = [p.plan_id for p in plans]
        self.storage.save_batch(batch, plans)
        log.info("batch %s created: groups=%s", batch.batch_id, groups)
        return batch

    def start_batch(self, batch_id: str) -> ReleaseBatch:
        with self._batch_lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            self._batch_transition(batch, "start")
            for plan in self._batch_members(batch):
                self._transition(plan, "start")
            self.storage.save_batch(batch, self._batch_members(batch))
            return batch

    def advance_batch(self, batch_id: str) -> ReleaseBatch:
        """整批推进一步。

        先对全体未完成成员做门禁判定：任一成员未通过则整批不动（其余组
        不会各自扩量到无法回收的位置）；全部通过才把所有成员一起推进
        一步，并在同一次原子落盘中持久化。
        """
        with self._batch_lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status != PlanStatus.RUNNING:
                self._batch_transition(batch, "advance")  # 必然抛出，给出区分原因
            members = self._batch_members(batch)
            active = [p for p in members if p.status == PlanStatus.RUNNING]
            # 阶段一：全部门禁判定，任何变更前完成
            verdicts: dict[str, tuple[GateVerdict, str]] = {}
            for plan in active:
                verdict, basis = self._evaluate_gate(plan)
                verdicts[plan.plan_id] = (verdict, basis)
                log.info("batch %s member %s (%s) gate verdict=%s basis=%s",
                         batch_id, plan.plan_id, plan.group, verdict.value, basis)
            blocked = [(p, verdicts[p.plan_id]) for p in active
                       if verdicts[p.plan_id][0] is not GateVerdict.PASS]
            if blocked:
                plan, (verdict, basis) = blocked[0]
                raise GateBlockedError(
                    f"batch {batch_id}: member group {plan.group} gate "
                    f"{verdict.value}: {basis} (batch not advanced)"
                )
            # 阶段二：整批一起推进一步，单次原子落盘
            for plan in active:
                plan.step_index += 1
                weight = plan.steps[plan.step_index]
                self._apply_weight(plan, weight, plan.version, flush=False)
                plan.stable = StablePoint(step_index=plan.step_index, weight=weight)
                if weight >= 100:
                    self._transition(plan, "succeed")
                else:
                    self._transition(plan, "advance")
                plan.updated_at = time.time()
                self.storage.save_plan(plan, flush=False)
            if all(p.status == PlanStatus.SUCCEEDED for p in members):
                self._batch_transition(batch, "succeed")
                log.info("batch %s completed: all members at weight=100", batch_id)
            else:
                self._batch_transition(batch, "advance")
            self.storage.save_batch(batch)  # 唯一一次落盘，覆盖上述全部变更
            return batch

    def rollback_batch(self, batch_id: str, reason: str = "manual") -> ReleaseBatch:
        """整批回滚：所有成员一起回到各自最近稳定点。

        - 幂等：已 ROLLED_BACK 的批次重复触发为 no-op；并发触发由批次锁
          串行化，与串行执行等价；
        - 两阶段落盘：先把批次与全部成员置为 ROLLING_BACK（中断可恢复），
          再统一恢复稳定点并置 ROLLED_BACK，单次原子落盘，对外（磁盘与
          持锁的观测接口）不会看到「部分组已回滚、部分未回滚」；
        - 已 SUCCEEDED 的成员本就停在自己的稳定点（100），保持原状。
        """
        with self._batch_lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status == PlanStatus.ROLLED_BACK:
                log.info("batch %s already ROLLED_BACK, no-op (reentrant)", batch_id)
                return batch
            if batch.status == PlanStatus.ROLLING_BACK:
                log.info("batch %s rollback already in progress, completing it",
                         batch_id)
            elif batch.status == PlanStatus.RUNNING:
                self._batch_transition(batch, "rollback")
            else:
                self._batch_transition(batch, "rollback")  # 必然抛出，给出区分原因
            batch.failure_reason = reason
            members = self._batch_members(batch)
            # 阶段一：整批标记 ROLLING_BACK 并落盘，中断后 recover 可续做
            for plan in members:
                if plan.status == PlanStatus.RUNNING:
                    self._transition(plan, "rollback")
                    plan.failure_reason = reason
                    self.storage.save_plan(plan, flush=False)
            self.storage.save_batch(batch, members)
            # 阶段二：统一恢复稳定点，单次原子落盘
            self._finish_batch_rollback(batch, members)
            return batch

    def _finish_batch_rollback(self, batch: ReleaseBatch,
                               members: list[RolloutPlan]) -> None:
        """恢复所有成员稳定点并结束批次回滚。调用方须持有批次锁。"""
        for plan in members:
            if plan.status == PlanStatus.ROLLING_BACK:
                self._finish_rollback(plan, flush=False)
        if batch.status == PlanStatus.ROLLING_BACK:
            self._batch_transition(batch, "finish_rollback")
        self.storage.save_batch(batch, members)  # 唯一一次落盘
        log.warning("batch %s rolled back, reason=%s",
                    batch.batch_id, batch.failure_reason)

    def abort_batch(self, batch_id: str) -> ReleaseBatch:
        """人工中止：PENDING 整批终止；RUNNING 走整批回滚路径。"""
        with self._batch_lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status == PlanStatus.PENDING:
                self._batch_transition(batch, "abort")
                members = self._batch_members(batch)
                for plan in members:
                    self._transition(plan, "abort")
                    self.storage.save_plan(plan, flush=False)
                self.storage.save_batch(batch, members)
                log.info("batch %s aborted before start", batch_id)
                return batch
        return self.rollback_batch(batch_id, reason="aborted")

    def run_batch_async(self, batch_id: str, step_interval: float = 0.05) -> None:
        raise NotImplementedError

    def observe_batch(self, batch_id: str) -> dict:
        """批次观测：每个成员的步进/权重/判定依据 + 整批统一结论。

        持批次锁读取：回滚/推进中途的部分状态对外不可见，观察者只能
        看到操作前或操作完成后的完整快照。
        """
        with self._batch_lock_for(batch_id):
            return self._observe_batch_locked(batch_id)

    def _observe_batch_locked(self, batch_id: str) -> dict:
        batch = self._must_get_batch(batch_id)
        members = []
        non_pass: list[str] = []
        saw_blocked = saw_waiting = False
        for plan in self._batch_members(batch):
            verdict, basis = self._evaluate_gate(plan)
            if verdict is GateVerdict.BLOCKED:
                saw_blocked = True
                non_pass.append(f"{plan.group}: BLOCKED ({basis})")
            elif verdict is GateVerdict.WAITING:
                saw_waiting = True
                non_pass.append(f"{plan.group}: WAITING ({basis})")
            group = self.storage.get_group(plan.group)
            members.append({
                "plan_id": plan.plan_id,
                "group": plan.group,
                "version": plan.version,
                "status": plan.status.value,
                "step_index": plan.step_index,
                "current_weight": plan.current_weight,
                "stable_weight": plan.stable.weight,
                "gate_verdict": verdict.value,
                "gate_basis": basis,
                "group_state": None if group is None else {
                    "version": group.version, "weight": group.weight,
                },
            })
        if saw_blocked:
            overall = GateVerdict.BLOCKED.value
        elif saw_waiting:
            overall = GateVerdict.WAITING.value
        else:
            overall = GateVerdict.PASS.value
        return {
            "batch_id": batch.batch_id,
            "status": batch.status.value,
            "failure_reason": batch.failure_reason,
            "overall_verdict": overall,
            "overall_basis": ("; ".join(non_pass) if non_pass
                              else "all member gates pass"),
            "members": members,
        }
