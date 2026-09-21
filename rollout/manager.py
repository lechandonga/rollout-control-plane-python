"""Core control plane: plans, promotion, rollback, recovery."""
from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

from . import statemachine
from .config import Config
from .errors import (
    InvalidTransition,
    PlanAlreadyExists,
    PlanNotFound,
    RollbackStablePointMissing,
)
from .health import HealthGate
from .journal import EventKind, Journal
from .models import (
    GateStatus,
    Instance,
    InstanceGroup,
    InstanceStatus,
    Plan,
    PlanState,
    RolloutPolicy,
    StablePoint,
)
from .scheduler import Scheduler
from .store import Store


class RolloutManager:
    def __init__(self, store: Store, config: Config | None = None,
                 gate: HealthGate | None = None,
                 journal: Journal | None = None) -> None:
        self.config = config or Config()
        self._store = store
        self._gate = gate or HealthGate()
        self._journal = journal or Journal()
        self._scheduler = Scheduler(self.config, self._journal)
        self._plans: dict[str, Plan] = {}
        # One re-entrant lock per plan -> operations on the same plan
        # serialize; operations on different plans stay concurrent.
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        # Fresh single-use executors per gate evaluation: shut down after each
        # use so no background thread/resource leaks even on timeout.
        self._load(self._store.load_all())

    # ------------------------------------------------------------------ #
    # locking / loading helpers
    # ------------------------------------------------------------------ #
    def _lock_for(self, plan_id: str) -> threading.RLock:
        with self._locks_guard:
            lk = self._locks.get(plan_id)
            if lk is None:
                lk = threading.RLock()
                self._locks[plan_id] = lk
            return lk

    def _load(self, plans: dict[str, Plan]) -> None:
        self._plans = plans
        for p in plans.values():
            self._locks.setdefault(p.plan_id, threading.RLock())

    def _get(self, plan_id: str) -> Plan:
        p = self._plans.get(plan_id)
        if p is None:
            raise PlanNotFound(f"plan {plan_id} not found")
        return p

    def _persist(self, plan: Plan) -> None:
        plan.touch()
        self._store.save(plan)

    # ------------------------------------------------------------------ #
    # recovery
    # ------------------------------------------------------------------ #
    def recover(self) -> list[dict]:
        """Reconcile interrupted plans after a process restart.

        PROMOTING/ROLLING_BACK are transient states whose effects are only
        visible after a successful persist. Any plan found in them after a
        crash is moved back to the last durable state with an explicit reason.
        """
        report: list[dict] = []
        for pid in list(self._plans):
            with self._lock_for(pid):
                p = self._plans[pid]
                if p.state is PlanState.PROMOTING:
                    p.state = PlanState.PAUSED
                    p.reason = ("recovered: promotion step was interrupted; "
                                "resumed from last durable weight")
                    self._persist(p)
                    self._journal.emit(pid, EventKind.STATE_CHANGE,
                                       "recovered promoting -> paused",
                                       {"weight": p.weight})
                    report.append({"plan_id": pid, "recovered_to": "paused"})
                elif p.state is PlanState.ROLLING_BACK:
                    # Rollback applies the latest stable point as one atomic
                    # persisted write; if it never completed, the durable
                    # state is already the stable point. Finish the rollback.
                    sp = p.latest_stable_point()
                    if sp is not None:
                        self._apply_stable_point(p, sp)
                        statemachine.transition(p, "rollback_done")
                        p.reason = "recovered: interrupted rollback completed"
                        self._persist(p)
                        self._journal.emit(
                            pid, EventKind.ROLLBACK,
                            "recovered rollback completed at stable point",
                            {"weight": sp.weight, "version": sp.version})
                        report.append({"plan_id": pid,
                                       "recovered_to": "rolled_back"})
        return report

    # ------------------------------------------------------------------ #
    # plan lifecycle
    # ------------------------------------------------------------------ #
    def create_plan(self, plan_id: str, app: str, version: str,
                    target_group: str, step_weight: int = 10,
                    from_version: str = "",
                    gate_required: bool = True) -> Plan:
        self.config.validate_step(step_weight)
        with self._lock_for(plan_id):
            if plan_id in self._plans:
                raise PlanAlreadyExists(f"plan {plan_id} already exists")
            if not version or not target_group or not app:
                raise ValueError("app, version and target_group are required")
            baseline_version = from_version or f"{version}~baseline"
            policy = RolloutPolicy(step_weight=step_weight,
                                   gate_required=gate_required)
            p = Plan(plan_id=plan_id, app=app, version=version,
                     target_group=target_group, policy=policy,
                     from_version=baseline_version)
            stable = InstanceGroup(
                name="stable", version=baseline_version,
                instances=[Instance(f"{plan_id}-stable-0", "stable",
                                    baseline_version, InstanceStatus.READY)])
            canary = InstanceGroup(
                name=target_group, version=version,
                instances=[Instance(f"{plan_id}-canary-0", target_group,
                                    version, InstanceStatus.PROVISIONING)])
            p.groups = {"stable": stable, target_group: canary}
            # The pre-rollout externally visible state is stable point #0.
            p.stable_points.append(StablePoint(
                ts=time.time(), version=baseline_version, weight=0,
                groups=copy.deepcopy(p.groups)))
            self._persist(p)
            self._plans[plan_id] = p
            self._journal.emit(plan_id, EventKind.INFO, "plan created", {
                "version": version, "from_version": baseline_version,
                "target_group": target_group, "step_weight": step_weight})
            return p

    def get_plan(self, plan_id: str) -> Plan:
        return self._get(plan_id)

    def list_plans(self) -> list[Plan]:
        return list(self._plans.values())

    # ------------------------------------------------------------------ #
    # promotion
    # ------------------------------------------------------------------ #
    def _canary_group(self, p: Plan) -> InstanceGroup:
        return p.groups[p.target_group]

    def _apply_stable_point(self, p: Plan, sp: StablePoint) -> None:
        p.groups = copy.deepcopy(sp.groups)
        if p.target_group not in p.groups:
            p.groups[p.target_group] = InstanceGroup(
                p.target_group, sp.version, [])
        p.weight = sp.weight

    def _record_stable_point(self, p: Plan) -> None:
        last = p.latest_stable_point()
        if last is not None and last.weight == p.weight:
            # Same externally-visible state already captured; don't stack
            # duplicate points on a successful gate retry.
            return
        p.stable_points.append(StablePoint(
            ts=time.time(), version=p.version, weight=p.weight,
            groups=copy.deepcopy(p.groups)))

    def _evaluate_with_timeout(self, p: Plan):
        executor = ThreadPoolExecutor(max_workers=1,
                                      thread_name_prefix=f"gate-{p.plan_id}")
        fut = executor.submit(self._gate.evaluate, p)
        try:
            return fut.result(timeout=self.config.step_timeout_s)
        except FutTimeout:
            fut.cancel()
            raise
        finally:
            # wait=True blocks for the (possibly still-running) evaluation,
            # then disposes its thread deterministically.
            executor.shutdown(wait=True, cancel_futures=True)

    def promote(self, plan_id: str) -> Plan:
        """Advance one canary step if the health gate passes."""
        plan_lock = self._lock_for(plan_id)
        # Cheap pre-check without holding a scheduler slot.
        with plan_lock:
            p = self._get(plan_id)
            if p.state in (PlanState.COMPLETED, PlanState.ROLLED_BACK,
                           PlanState.ABORTED, PlanState.ROLLING_BACK,
                           PlanState.FAILED):
                raise InvalidTransition(
                    plan_id, p.state.value, "promote",
                    f"cannot promote from terminal/in-flight state "
                    f"{p.state.value}")
        # Acquire the concurrency slot WITHOUT the plan lock, so queue wait
        # never blocks other operations on the same plan.
        self._scheduler.acquire(plan_id)
        try:
            with plan_lock:
                p = self._get(plan_id)
                # Re-check under lock: state may have changed while queued.
                if p.state in (PlanState.COMPLETED, PlanState.ROLLED_BACK,
                               PlanState.ABORTED, PlanState.ROLLING_BACK,
                               PlanState.FAILED):
                    raise InvalidTransition(
                        plan_id, p.state.value, "promote",
                        f"cannot promote from terminal/in-flight state "
                        f"{p.state.value}")
                was_paused = p.state is PlanState.PAUSED
                statemachine.transition(p, "start_promotion")
                if was_paused:
                    # Re-evaluate the held, previously-blocked weight: a
                    # blocked weight must pass its gate before any increase.
                    new_weight = p.weight
                else:
                    # 1) weight moves by exactly one configured step, never
                    #    past 100 and never skipping an intermediate value.
                    step = p.policy.step_weight
                    self.config.validate_step(step)
                    new_weight = min(100, p.weight + step)
                    p.weight = new_weight
                    canary = self._canary_group(p)
                    for inst in canary.instances:
                        if inst.status in (InstanceStatus.PROVISIONING,
                                           InstanceStatus.INACTIVE):
                            inst.status = InstanceStatus.READY
                    self._journal.emit(
                        plan_id, EventKind.STATE_CHANGE,
                        f"weight advanced {new_weight - step} -> {new_weight}",
                        {"weight": new_weight})

                # 2) evaluate health at the new weight with a step timeout.
                try:
                    snap = self._evaluate_with_timeout(p)
                except FutTimeout:
                    statemachine.transition(p, "gate_block")
                    p.reason = (f"health evaluation timed out after "
                                f"{self.config.step_timeout_s}s at weight "
                                f"{new_weight}; holding weight")
                    self._persist(p)
                    self._journal.emit(
                        plan_id, EventKind.GATE_BLOCKED,
                        "gate evaluation timed out; held for retry",
                        {"weight": new_weight})
                    return p
                p.history.append(snap)

                # 3a) catastrophic -> automatic rollback
                if snap.critical:
                    p.reason = ("critical health failure at weight "
                                f"{new_weight}: {snap.failed_checks}")
                    self._journal.emit(
                        plan_id, EventKind.FAILED,
                        "critical health failure; auto rollback",
                        self._gate.evidence(snap))
                    self._persist(p)
                    return self._do_rollback(p, automatic=True)

                # 3b) gate not passed -> hold weight, no new stable point
                if snap.status is GateStatus.BLOCKED:
                    statemachine.transition(p, "gate_block")
                    p.reason = (f"health gate blocked at weight {new_weight}: "
                                f"{snap.failed_checks}")
                    self._persist(p)
                    self._journal.emit(
                        plan_id, EventKind.GATE_BLOCKED,
                        "health gate blocked; weight held",
                        self._gate.evidence(snap))
                    return p

                # 3c) passed: the new externally visible state is a stable
                #     point before completion is announced.
                self._record_stable_point(p)
                if new_weight >= 100:
                    statemachine.transition(p, "complete")
                    for inst in p.groups["stable"].instances:
                        inst.status = InstanceStatus.INACTIVE
                    p.reason = "weight 100 healthy; rollout completed"
                    self._persist(p)
                    self._journal.emit(
                        plan_id, EventKind.COMPLETED,
                        "rollout completed", {"weight": 100})
                else:
                    p.reason = (f"gate passed at weight {new_weight}; "
                                f"awaiting next promotion")
                    self._persist(p)
                    self._journal.emit(
                        plan_id, EventKind.INFO,
                        f"step stable at weight {new_weight}",
                        {"weight": new_weight})
                return p
        finally:
            self._scheduler.release()

    # ------------------------------------------------------------------ #
    # abort / rollback
    # ------------------------------------------------------------------ #
    def abort(self, plan_id: str, reason: str = "manual abort") -> Plan:
        with self._lock_for(plan_id):
            p = self._get(plan_id)
            statemachine.transition(p, "abort")
            p.reason = reason
            self._persist(p)
            self._journal.emit(plan_id, EventKind.STATE_CHANGE,
                               "plan aborted", {"reason": reason,
                                                "weight": p.weight})
            return p

    def _do_rollback(self, p: Plan, automatic: bool) -> Plan:
        # Caller must hold the plan lock.
        sp = p.latest_stable_point()
        if sp is None:
            raise RollbackStablePointMissing(
                f"plan {p.plan_id}: no stable point to roll back to")
        statemachine.transition(p, "start_rollback")
        self._apply_stable_point(p, sp)
        statemachine.transition(p, "rollback_done")
        p.reason = ("automatic rollback: " if automatic else "manual rollback: ")
        p.reason += f"restored version {sp.version} at weight {sp.weight}"
        self._persist(p)
        self._journal.emit(
            p.plan_id, EventKind.ROLLBACK,
            "rolled back to latest stable point",
            {"version": sp.version, "weight": sp.weight,
             "automatic": automatic, "stable_point_ts": sp.ts})
        return p

    def rollback(self, plan_id: str) -> Plan:
        """Roll back to the latest stable point; idempotent."""
        with self._lock_for(plan_id):
            p = self._get(plan_id)
            # Idempotent re-entry: if already sitting at the latest stable
            # point in ROLLED_BACK, report success with no new side effects.
            if p.state is PlanState.ROLLED_BACK:
                sp = p.latest_stable_point()
                if sp is not None and p.weight == sp.weight:
                    self._journal.emit(
                        plan_id, EventKind.ROLLBACK,
                        "rollback re-entered while already at stable point; "
                        "no-op",
                        {"weight": sp.weight})
                    return p
            return self._do_rollback(p, automatic=False)

    # ------------------------------------------------------------------ #
    # observability
    # ------------------------------------------------------------------ #
    def status(self, plan_id: str) -> dict:
        with self._lock_for(plan_id):
            p = self._get(plan_id)
            sp = p.latest_stable_point()
            return {
                "plan_id": p.plan_id,
                "app": p.app,
                "state": p.state.value,
                "version": p.version,
                "from_version": p.from_version,
                "serving_version": (p.version if p.weight > 0
                                    else (sp.version if sp else p.from_version)),
                "weight": p.weight,
                "reason": p.reason,
                "target_group": p.target_group,
                "groups": {k: g.to_dict() for k, g in p.groups.items()},
                "latest_stable_point": (
                    {"version": sp.version, "weight": sp.weight, "ts": sp.ts}
                    if sp else None),
                "stable_point_count": len(p.stable_points),
                "last_evidence": (
                    self._gate.evidence(p.history[-1]) if p.history else None),
                "updated_at": p.updated_at,
                "version_counter": p.version_counter,
            }

    def events(self, plan_id: str | None = None) -> list[dict]:
        return [e.to_dict() for e in self._journal.events(plan_id)]

    def shutdown(self, timeout: float | None = None) -> None:
        self._scheduler.shutdown(timeout)
