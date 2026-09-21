"""核心服务测试：并发放量、回滚重入、中断恢复、非法迁移。

每个用例打印输入与判定依据，便于审计。
"""
import logging
import threading

import pytest

from rollout.config import ControlPlaneConfig
from rollout.errors import (
    ConcurrencyLimitError,
    GateBlockedError,
    IllegalTransitionError,
    PlanNotFoundError,
    ValidationError,
)
from rollout.models import GatePolicy, HealthMetrics, PlanStatus
from rollout.service import RolloutService
from rollout.storage import Storage

log = logging.getLogger("test.service")


def make_service(tmp_path, **cfg):
    config = ControlPlaneConfig(storage_path=str(tmp_path / "state.json"), **cfg)
    storage = Storage(config.storage_path)
    return RolloutService(storage, config)


def healthy(svc, group, error_rate=0.0, unhealthy=0.0):
    svc.report_health(group, HealthMetrics(error_rate=error_rate,
                                           unhealthy_ratio=unhealthy))


# ---------------------------------------------------------------- 非法迁移

def test_illegal_transitions_have_distinct_reasons(tmp_path):
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 100])
    log.info("input: advance on PENDING plan %s", plan.plan_id)
    with pytest.raises(IllegalTransitionError) as e1:
        svc.advance(plan.plan_id)
    log.info("verdict: reason=%s msg=%s", e1.value.reason, e1.value)
    assert "PENDING" in str(e1.value)

    with pytest.raises(IllegalTransitionError) as e2:
        svc.rollback(plan.plan_id)
    log.info("verdict: rollback on PENDING -> %s", e2.value)

    svc.start(plan.plan_id)
    healthy(svc, "g1")
    svc.advance(plan.plan_id)
    svc.advance(plan.plan_id)
    assert svc.get_plan(plan.plan_id).status == PlanStatus.SUCCEEDED
    with pytest.raises(IllegalTransitionError) as e3:
        svc.advance(plan.plan_id)
    assert "terminal" in str(e3.value)
    log.info("verdict: advance on SUCCEEDED -> %s", e3.value)

    with pytest.raises(PlanNotFoundError):
        svc.get_plan("nope")


def test_validation_rejects_bad_steps(tmp_path):
    svc = make_service(tmp_path, max_step_weight_delta=30)
    for steps in ([], [0, 100], [50, 40, 100], [50, 100], [10, 50]):
        with pytest.raises(ValidationError) as e:
            svc.create_plan("v2", "g1", steps)
        log.info("input: steps=%s verdict: %s", steps, e.value)


# ---------------------------------------------------------------- 门禁

def test_gate_blocks_and_waits(tmp_path):
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 100])
    svc.start(plan.plan_id)
    # 未上报 -> WAITING
    with pytest.raises(GateBlockedError) as e1:
        svc.advance(plan.plan_id)
    assert "WAITING" in str(e1.value)
    log.info("verdict: no metrics -> %s", e1.value)
    # 指标超标 -> BLOCKED，状态不变
    healthy(svc, "g1", error_rate=0.5)
    with pytest.raises(GateBlockedError) as e2:
        svc.advance(plan.plan_id)
    assert "BLOCKED" in str(e2.value)
    plan = svc.get_plan(plan.plan_id)
    assert plan.step_index == -1 and plan.current_weight == 0
    log.info("verdict: blocked, step_index=%d weight=%d (no partial state)",
             plan.step_index, plan.current_weight)
    # 恢复健康 -> 放行
    healthy(svc, "g1")
    svc.advance(plan.plan_id)
    assert svc.get_plan(plan.plan_id).current_weight == 10


# ---------------------------------------------------------------- 并发放量

def test_concurrent_advance_is_serializable(tmp_path):
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 20, 30, 40, 100])
    svc.start(plan.plan_id)
    healthy(svc, "g1")
    errors, successes = [], []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        try:
            svc.advance(plan.plan_id)
            successes.append(1)
        except GateBlockedError:
            pass
        except IllegalTransitionError:
            pass  # 串行等价：到达终态后的推进被拒绝
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    plan = svc.get_plan(plan.plan_id)
    log.info("input: 8 concurrent advance; verdict: successes=%d step_index=%d",
             len(successes), plan.step_index)
    # 与串行执行一致：成功数 == 推进步数，权重恰好是已完成的步
    assert len(successes) == plan.step_index + 1
    assert plan.current_weight == plan.steps[plan.step_index]
    group = svc.storage.get_group("g1")
    assert group.weight == plan.current_weight


# ---------------------------------------------------------------- 回滚重入

def test_rollback_reentrant_no_side_effects(tmp_path):
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 50, 100])
    svc.start(plan.plan_id)
    healthy(svc, "g1")
    svc.advance(plan.plan_id)
    svc.advance(plan.plan_id)  # stable = 50
    rb = svc.rollback(plan.plan_id, reason="test")
    assert rb.status == PlanStatus.ROLLED_BACK
    group = svc.storage.get_group("g1")
    log.info("verdict: after rollback weight=%d (stable=%d)", group.weight,
             rb.stable.weight)
    assert group.weight == 50
    # 重复触发：无副作用
    again = svc.rollback(plan.plan_id, reason="duplicate")
    assert again.status == PlanStatus.ROLLED_BACK
    assert svc.storage.get_group("g1").weight == 50
    # 并发重复触发
    barrier = threading.Barrier(6)
    results = []

    def worker():
        barrier.wait()
        results.append(svc.rollback(plan.plan_id).status)

    ts = [threading.Thread(target=worker) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(s == PlanStatus.ROLLED_BACK for s in results)
    assert svc.storage.get_group("g1").weight == 50
    log.info("verdict: 6 concurrent reentrant rollbacks, weight still 50")


def test_abort_pending_and_running(tmp_path):
    svc = make_service(tmp_path)
    p1 = svc.create_plan("v2", "g1", [10, 100])
    svc.abort(p1.plan_id)
    assert svc.get_plan(p1.plan_id).status == PlanStatus.ABORTED

    p2 = svc.create_plan("v3", "g2", [10, 100])
    svc.start(p2.plan_id)
    healthy(svc, "g2")
    svc.advance(p2.plan_id)
    svc.abort(p2.plan_id)
    p2 = svc.get_plan(p2.plan_id)
    assert p2.status == PlanStatus.ROLLED_BACK
    assert p2.failure_reason == "aborted"
    assert svc.storage.get_group("g2").weight == p2.stable.weight
    log.info("verdict: abort RUNNING -> ROLLED_BACK at stable=%d", p2.stable.weight)


# ---------------------------------------------------------------- 中断恢复

def test_recovery_after_crash_mid_rollout(tmp_path):
    cfg = dict(storage_path=str(tmp_path / "state.json"))
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 50, 100])
    svc.start(plan.plan_id)
    healthy(svc, "g1")
    svc.advance(plan.plan_id)  # stable=10
    # 模拟崩溃：组状态被部分更新但计划未落下一步
    from rollout.models import InstanceGroupState
    svc.storage.save_group(InstanceGroupState(group="g1", version="v2", weight=50))
    # 重启恢复
    svc2 = RolloutService(Storage(cfg["storage_path"]), ControlPlaneConfig(**cfg))
    svc2.recover()
    group = svc2.storage.get_group("g1")
    restored = svc2.get_plan(plan.plan_id)
    log.info("verdict: recovered weight=%d step_index=%d (expect stable=10)",
             group.weight, restored.step_index)
    assert group.weight == 10
    assert restored.step_index == 0
    assert restored.status == PlanStatus.RUNNING  # 计划未丢失，可继续


def test_recovery_finishes_interrupted_rollback(tmp_path):
    svc = make_service(tmp_path)
    plan = svc.create_plan("v2", "g1", [10, 100])
    svc.start(plan.plan_id)
    healthy(svc, "g1")
    svc.advance(plan.plan_id)
    # 模拟回滚进行到一半崩溃：状态停在 ROLLING_BACK
    svc._transition(plan, "rollback")
    plan.failure_reason = "crash"
    svc.storage.save_plan(plan)
    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    restored = svc2.get_plan(plan.plan_id)
    assert restored.status == PlanStatus.ROLLED_BACK
    assert svc2.storage.get_group("g1").weight == restored.stable.weight
    log.info("verdict: interrupted rollback finished, status=%s", restored.status)


# ---------------------------------------------------------------- 并发上限

def test_concurrency_limit_reject(tmp_path):
    svc = make_service(tmp_path, max_concurrent_tasks=1, step_timeout_seconds=5.0)
    try:
        plans = []
        for i in range(2):
            p = svc.create_plan("v2", f"g{i}", [10, 100])
            svc.start(p.plan_id)
            plans.append(p)
        # 第一个任务占住唯一槽位（门禁 WAITING 会一直重试）
        svc.run_async(plans[0].plan_id, step_interval=0.02)
        import time
        time.sleep(0.1)
        with pytest.raises(ConcurrencyLimitError) as e:
            svc.run_async(plans[1].plan_id)
        log.info("verdict: overflow rejected reason=%s", e.value.reason)
    finally:
        svc.shutdown()
    # 关闭后无残留后台线程
    assert not [t for t in threading.enumerate() if t.name.startswith("rollout")]


def test_run_async_completes_and_auto_rollback_on_gate_timeout(tmp_path):
    svc = make_service(tmp_path, step_timeout_seconds=0.15)
    try:
        p = svc.create_plan("v2", "g1", [10, 100])
        svc.start(p.plan_id)
        healthy(svc, "g1")
        svc.run_async(p.plan_id, step_interval=0.02)
        assert svc.wait_idle()
        assert svc.get_plan(p.plan_id).status == PlanStatus.SUCCEEDED
        log.info("verdict: run_async reached SUCCEEDED")

        p2 = svc.create_plan("v3", "g2", [10, 100])
        svc.start(p2.plan_id)
        healthy(svc, "g2", error_rate=0.9)  # 门禁永不通过
        svc2 = svc
        svc2.run_async(p2.plan_id, step_interval=0.02)
        assert svc2.wait_idle()
        p2 = svc.get_plan(p2.plan_id)
        assert p2.status == PlanStatus.ROLLED_BACK
        assert p2.failure_reason == "gate_timeout"
        log.info("verdict: gate timeout auto-rollback, reason=%s", p2.failure_reason)
    finally:
        svc.shutdown()
