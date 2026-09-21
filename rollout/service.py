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
        gate = gate or GatePolicy()
        if not (0 <= gate.max_error_rate <= 1 and 0 <= gate.max_unhealthy_ratio <= 1):
            raise ValidationError("gate thresholds must be within [0,1]")
        plan = RolloutPlan.new(version, group, steps, gate)
        self.storage.save_plan(plan)
        log.info("plan %s created: version=%s group=%s steps=%s",
                 plan.plan_id, version, group, steps)
        return plan

    def start(self, plan_id: str) -> RolloutPlan:
        with self._lock_for(plan_id):
            plan = self._must_get(plan_id)
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
