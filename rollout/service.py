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
    BatchNotFoundError,
    ConcurrencyLimitError,
    GateBlockedError,
    GroupConflictError,
    IllegalTransitionError,
    PlanNotFoundError,
    ValidationError,
)
from .models import (
    TRANSITIONS,
    BatchPlan,
    GatePolicy,
    GateVerdict,
    GroupTarget,
    HealthMetrics,
    InstanceGroupState,
    PlanStatus,
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
        self._task_sem = threading.BoundedSemaphore(self.config.max_concurrent_tasks)
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_tasks,
            thread_name_prefix="rollout",
        )
        self._tasks: dict[str, threading.Event] = {}  # plan_id -> cancel event
        self._tasks_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _lock_for(self, plan_id: str) -> threading.Lock:
        return self._plan_locks[hash(plan_id) % self._LOCK_STRIPES]

    def _must_get(self, plan_id: str) -> RolloutPlan:
        plan = self.storage.get_plan(plan_id)
        if plan is None:
            raise PlanNotFoundError(f"plan {plan_id} not found")
        return plan

    def _must_get_batch(self, batch_id: str) -> BatchPlan:
        batch = self.storage.get_batch(batch_id)
        if batch is None:
            raise BatchNotFoundError(f"batch {batch_id} not found")
        return batch

    def _transition(self, plan, action: str) -> None:
        """状态机迁移校验（单组计划与批次共用）：非法迁移抛出带区分原因的异常。"""
        entity_id = getattr(plan, "plan_id", None) or plan.batch_id
        key = (plan.status, action)
        if key not in TRANSITIONS:
            detail = ""
            if plan.status.terminal:
                detail = "plan is in a terminal state"
            raise IllegalTransitionError(entity_id, plan.status.value, action, detail)
        old = plan.status
        plan.status = TRANSITIONS[key]
        plan.updated_at = time.time()
        log.info("plan %s: %s --%s--> %s", entity_id, old.value, action, plan.status.value)

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

    def _check_group_conflicts(self, groups: list[str], exclude_id: str) -> None:
        """同一实例组上不允许同时存在两条进行中的发布（单组计划或批次）。

        冲突时抛出 GroupConflictError（reason=group_conflict），与参数非法、
        状态机错误可区分。exclude_id 用于排除自身（start 时复查）。
        """
        wanted = set(groups)
        for plan in self.storage.list_plans():
            if plan.plan_id == exclude_id or plan.status.terminal:
                continue
            if plan.group in wanted:
                raise GroupConflictError(plan.group, f"plan {plan.plan_id}")
        for batch in self.storage.list_batches():
            if batch.batch_id == exclude_id or batch.status.terminal:
                continue
            for g in batch.groups():
                if g in wanted:
                    raise GroupConflictError(g, f"batch {batch.batch_id}")

    def _evaluate_group_gate(self, group: str, gate: GatePolicy) -> tuple[GateVerdict, str]:
        """健康门禁判定，返回 (判定, 依据) 供日志与观测使用。"""
        m = self.storage.get_health(group)
        if not m.reported:
            return GateVerdict.WAITING, "no health metrics reported yet"
        if m.error_rate > gate.max_error_rate:
            return GateVerdict.BLOCKED, (
                f"error_rate {m.error_rate:.3f} > max {gate.max_error_rate:.3f}"
            )
        if m.unhealthy_ratio > gate.max_unhealthy_ratio:
            return GateVerdict.BLOCKED, (
                f"unhealthy_ratio {m.unhealthy_ratio:.3f} > max "
                f"{gate.max_unhealthy_ratio:.3f}"
            )
        return GateVerdict.PASS, (
            f"error_rate {m.error_rate:.3f} <= {gate.max_error_rate:.3f}, "
            f"unhealthy_ratio {m.unhealthy_ratio:.3f} <= {gate.max_unhealthy_ratio:.3f}"
        )

    def _evaluate_gate(self, plan: RolloutPlan) -> tuple[GateVerdict, str]:
        return self._evaluate_group_gate(plan.group, plan.gate)

    def _apply_weight(self, plan: RolloutPlan, weight: int, version: str) -> None:
        """把对外可见的组状态切到指定权重/版本并落盘（与计划同事务）。"""
        group = self.storage.get_group(plan.group) or InstanceGroupState(
            group=plan.group, version=version, weight=0
        )
        group.version = version
        group.weight = weight
        self.storage.save_group(group)

    # ------------------------------------------------------------------
    # 计划生命周期
    # ------------------------------------------------------------------
    def create_plan(self, version: str, group: str, steps: list[int],
                    gate: GatePolicy | None = None) -> RolloutPlan:
        if not version or not group:
            raise ValidationError("version and group must be non-empty")
        self._validate_steps(steps)
        gate = gate or GatePolicy()
        self._validate_gate(gate)
        self._check_group_conflicts([group], exclude_id="")
        plan = RolloutPlan.new(version, group, steps, gate)
        self.storage.save_plan(plan)
        log.info("plan %s created: version=%s group=%s steps=%s",
                 plan.plan_id, version, group, steps)
        return plan

    def start(self, plan_id: str) -> RolloutPlan:
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
            if plan.status == PlanStatus.PENDING:
                # 启动前复查：创建后可能有其它发布占用了该组
                self._check_group_conflicts([plan.group], exclude_id=plan_id)
            self._transition(plan, "start")
            self.storage.save_plan(plan)
            return plan

    def advance(self, plan_id: str) -> RolloutPlan:
        """推进一步。门禁未通过时不产生任何状态变更。"""
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
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

    def _finish_rollback(self, plan: RolloutPlan) -> None:
        """恢复稳定点并进入 ROLLED_BACK。调用方须持有计划锁。"""
        stable_weight = plan.stable.weight
        stable_version = plan.version if plan.stable.step_index >= 0 else "previous"
        self._apply_weight(plan, stable_weight, stable_version)
        plan.step_index = plan.stable.step_index
        self._transition(plan, "finish_rollback")
        self.storage.save_plan(plan)
        log.warning("plan %s rolled back to stable weight=%d reason=%s",
                    plan.plan_id, stable_weight, plan.failure_reason)

    def abort(self, plan_id: str) -> RolloutPlan:
        """人工中止：PENDING 直接终止；RUNNING 走回滚路径回到稳定点。"""
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
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
    # 多组协同发布（批次）
    # ------------------------------------------------------------------
    def create_batch_plan(self, targets: list[GroupTarget]) -> BatchPlan:
        """创建一次覆盖多个实例组的发布批次。

        每个组有各自的版本与放量节奏；批次内组名必须互不相同，且不得与
        任何进行中的单组计划或其它批次冲突。
        """
        if not targets:
            raise ValidationError("batch targets must be non-empty")
        seen: set[str] = set()
        for t in targets:
            if not t.group or not t.version:
                raise ValidationError("target group and version must be non-empty")
            if t.group in seen:
                raise ValidationError(f"duplicate group in batch: {t.group}")
            seen.add(t.group)
            self._validate_steps(t.steps)
            self._validate_gate(t.gate)
        self._check_group_conflicts(list(seen), exclude_id="")
        batch = BatchPlan.new(targets)
        self.storage.save_batch(batch)
        log.info("batch %s created: groups=%s", batch.batch_id, sorted(seen))
        return batch

    def start_batch(self, batch_id: str) -> BatchPlan:
        with self._lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status == PlanStatus.PENDING:
                self._check_group_conflicts(batch.groups(), exclude_id=batch_id)
            self._transition(batch, "start")
            self.storage.save_batch(batch)
            return batch

    def advance_batch(self, batch_id: str) -> BatchPlan:
        """整批推进一步：先对每组做门禁判定，全部通过才原子推进。

        任一组被挡下（WAITING/BLOCKED）时，整批不产生任何状态变更——
        其余组不得继续各自扩量到无法回收的位置。
        """
        with self._lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status != PlanStatus.RUNNING:
                self._transition(batch, "advance")  # 必然抛出，给出区分原因
            verdicts: list[tuple[GroupTarget, GateVerdict, str]] = []
            for t in batch.targets:
                verdict, basis = self._evaluate_group_gate(t.group, t.gate)
                t.last_verdict, t.last_basis = verdict.value, basis
                verdicts.append((t, verdict, basis))
                log.info("batch %s group %s gate verdict=%s basis=%s",
                         batch_id, t.group, verdict.value, basis)
            blocked = [(t, v, b) for t, v, b in verdicts if v is not GateVerdict.PASS]
            if blocked:
                t, v, b = blocked[0]
                raise GateBlockedError(
                    f"batch {batch_id}: group {t.group} gate {v.value}: {b}"
                )
            # 全部门禁通过：各组分别推进一步，整批原子生效（单次落盘）
            groups = []
            for t in batch.targets:
                if t.done:
                    continue
                t.step_index += 1
                weight = t.steps[t.step_index]
                t.stable = StablePoint(step_index=t.step_index, weight=weight)
                groups.append(self._group_state_for(t.group, weight, t.version))
            if all(t.done for t in batch.targets):
                self._transition(batch, "succeed")
                log.info("batch %s completed: all groups at weight=100", batch_id)
            else:
                self._transition(batch, "advance")
            self.storage.save_batch_with_groups(batch, groups)
            return batch

    def rollback_batch(self, batch_id: str, reason: str = "manual") -> BatchPlan:
        """整批回滚：所有组一起恢复各自的最近稳定点。

        幂等且可重入：已 ROLLED_BACK 重复触发为 no-op；并发重复触发与
        串行执行等价。组状态与批次状态在同一把锁内一次落盘，对外不会
        出现「部分组已回滚、部分未回滚」的中间状态。
        """
        with self._lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status == PlanStatus.ROLLED_BACK:
                log.info("batch %s already ROLLED_BACK, no-op (reentrant)", batch_id)
                return batch
            if batch.status == PlanStatus.ROLLING_BACK:
                log.info("batch %s rollback already in progress, completing it", batch_id)
            elif batch.status == PlanStatus.RUNNING:
                self._transition(batch, "rollback")
            else:
                self._transition(batch, "rollback")  # 必然抛出，给出区分原因
            batch.failure_reason = reason
            self.storage.save_batch(batch)  # 先落 ROLLING_BACK，中断后可恢复
            self._finish_batch_rollback(batch)
            return batch

    def _finish_batch_rollback(self, batch: BatchPlan) -> None:
        """恢复各组稳定点并进入 ROLLED_BACK。调用方须持有批次锁。"""
        groups = []
        for t in batch.targets:
            stable_version = t.version if t.stable.step_index >= 0 else "previous"
            groups.append(self._group_state_for(t.group, t.stable.weight, stable_version))
            t.step_index = t.stable.step_index
        self._transition(batch, "finish_rollback")
        self.storage.save_batch_with_groups(batch, groups)
        log.warning("batch %s rolled back: groups=%s reason=%s",
                    batch.batch_id, batch.groups(), batch.failure_reason)

    def abort_batch(self, batch_id: str) -> BatchPlan:
        """人工中止：PENDING 直接终止；RUNNING 走整批回滚路径。"""
        with self._lock_for(batch_id):
            batch = self._must_get_batch(batch_id)
            if batch.status == PlanStatus.PENDING:
                self._transition(batch, "abort")
                self.storage.save_batch(batch)
                log.info("batch %s aborted before start", batch_id)
                return batch
        return self.rollback_batch(batch_id, reason="aborted")

    def _group_state_for(self, group: str, weight: int, version: str) -> InstanceGroupState:
        state = self.storage.get_group(group) or InstanceGroupState(
            group=group, version=version, weight=0
        )
        state.version = version
        state.weight = weight
        return state

    def get_batch(self, batch_id: str) -> BatchPlan:
        return self._must_get_batch(batch_id)

    def observe_batch(self, batch_id: str) -> dict:
        """批次可观测性：每组当前步、权重、判定结论与依据 + 整批统一结论。"""
        batch = self._must_get_batch(batch_id)
        per_group = []
        for t in batch.targets:
            verdict, basis = self._evaluate_group_gate(t.group, t.gate)
            group = self.storage.get_group(t.group)
            per_group.append({
                "group": t.group,
                "version": t.version,
                "step_index": t.step_index,
                "current_weight": t.current_weight,
                "stable_weight": t.stable.weight,
                "gate_verdict": verdict.value,
                "gate_basis": basis,
                "last_verdict": t.last_verdict,
                "last_basis": t.last_basis,
                "group_state": None if group is None else {
                    "version": group.version, "weight": group.weight,
                },
            })
        return {
            "batch_id": batch.batch_id,
            "status": batch.status.value,
            "conclusion": self._batch_conclusion(batch),
            "failure_reason": batch.failure_reason,
            "groups": per_group,
        }

    @staticmethod
    def _batch_conclusion(batch: BatchPlan) -> str:
        """整批统一结论（可解释快照）。"""
        if batch.status == PlanStatus.SUCCEEDED:
            return "all groups rolled out to 100%"
        if batch.status == PlanStatus.ROLLED_BACK:
            return f"all groups restored to stable points ({batch.failure_reason})"
        if batch.status == PlanStatus.ABORTED:
            return "aborted before start"
        done = sum(1 for t in batch.targets if t.done)
        return f"{batch.status.value.lower()}: {done}/{len(batch.targets)} groups done"

    def run_batch_async(self, batch_id: str, step_interval: float = 0.05) -> None:
        """后台自动整批放量：门禁全过则逐步推进；阻塞超时则整批自动回滚。"""
        acquired = self._task_sem.acquire(blocking=False)
        if not acquired:
            if self.config.overflow_policy == "queue":
                log.info("batch %s queued (task limit %d reached)",
                         batch_id, self.config.max_concurrent_tasks)
                self._task_sem.acquire()
            else:
                raise ConcurrencyLimitError(
                    f"task limit {self.config.max_concurrent_tasks} reached"
                )
        cancel = threading.Event()
        with self._tasks_lock:
            self._tasks[batch_id] = cancel
        try:
            self._executor.submit(self._run_batch_loop, batch_id, step_interval, cancel)
        except Exception:
            self._task_sem.release()
            with self._tasks_lock:
                self._tasks.pop(batch_id, None)
            raise

    def _run_batch_loop(self, batch_id: str, step_interval: float,
                        cancel: threading.Event) -> None:
        try:
            while not cancel.is_set():
                batch = self.storage.get_batch(batch_id)
                if batch is None or batch.status != PlanStatus.RUNNING:
                    return
                try:
                    self.advance_batch(batch_id)
                except GateBlockedError:
                    waited = 0.0
                    while waited < self.config.step_timeout_seconds:
                        time.sleep(step_interval)
                        waited += step_interval
                        try:
                            self.advance_batch(batch_id)
                            break
                        except GateBlockedError:
                            continue
                    else:
                        log.warning(
                            "batch %s gate blocked beyond timeout, auto-rollback",
                            batch_id)
                        self.rollback_batch(batch_id, reason="gate_timeout")
                        return
                time.sleep(step_interval)
        finally:
            self._task_sem.release()
            with self._tasks_lock:
                self._tasks.pop(batch_id, None)

    # ------------------------------------------------------------------
    # 后台自动放量（受并发上限与超时约束）
    # ------------------------------------------------------------------
    def run_async(self, plan_id: str, step_interval: float = 0.05) -> None:
        """后台自动放量：门禁通过则逐步推进；阻塞超过超时则自动回滚。"""
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

        - ROLLING_BACK：回滚被中断，重新执行到 ROLLED_BACK；
        - RUNNING：放量被中断，对外组状态可能停在半更新，修复回最近稳定点；
        - 其余状态不变。所有修复均落盘。
        """
        self.storage.load()
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
        for batch in self.storage.list_batches():
            with self._lock_for(batch.batch_id):
                if batch.status == PlanStatus.ROLLING_BACK:
                    log.warning("recover: batch %s interrupted mid-rollback, finishing",
                                batch.batch_id)
                    self._finish_batch_rollback(batch)
                elif batch.status == PlanStatus.RUNNING:
                    dirty = [
                        t for t in batch.targets
                        if (g := self.storage.get_group(t.group))
                        and g.weight != t.stable.weight
                    ]
                    if dirty:
                        log.warning(
                            "recover: batch %s groups %s diverged from stable, repairing",
                            batch.batch_id, [t.group for t in dirty])
                        groups = []
                        for t in batch.targets:
                            groups.append(self._group_state_for(
                                t.group, t.stable.weight,
                                t.version if t.stable.step_index >= 0 else "previous"))
                            t.step_index = t.stable.step_index
                        self.storage.save_batch_with_groups(batch, groups)

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """等待所有后台任务自然结束（不取消）。超时返回 False。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._tasks_lock:
                if not self._tasks:
                    return True
            time.sleep(0.01)
        return False
