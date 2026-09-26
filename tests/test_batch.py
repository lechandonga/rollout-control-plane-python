"""多组协同发布测试：整批推进、整批回滚、组冲突、中断恢复与旧数据兼容。

每个用例打印输入与判定依据，便于审计。
"""
import json
import logging
import threading

import pytest

from rollout.config import ControlPlaneConfig
from rollout.errors import (
    GateBlockedError,
    GroupConflictError,
    IllegalTransitionError,
    ValidationError,
)
from rollout.models import HealthMetrics, PlanStatus
from rollout.service import RolloutService
from rollout.storage import Storage

log = logging.getLogger("test.batch")


def make_service(tmp_path, **cfg):
    config = ControlPlaneConfig(storage_path=str(tmp_path / "state.json"), **cfg)
    storage = Storage(config.storage_path)
    return RolloutService(storage, config)


def healthy(svc, group, error_rate=0.0, unhealthy=0.0):
    svc.report_health(group, HealthMetrics(error_rate=error_rate,
                                           unhealthy_ratio=unhealthy))


def make_batch(svc, groups=("g1", "g2"), steps=None, version="v2"):
    steps = steps or [10, 100]
    return svc.create_batch([
        {"group": g, "version": f"{version}-{g}", "steps": list(steps)}
        for g in groups
    ])


# ---------------------------------------------------------------- 协同推进

def test_batch_advance_lockstep_and_observe(tmp_path):
    svc = make_service(tmp_path)
    batch = svc.create_batch([
        {"group": "g1", "version": "v2", "steps": [10, 100]},
        {"group": "g2", "version": "v3", "steps": [20, 40, 100]},
    ])
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    obs = svc.observe_batch(batch.batch_id)
    by_group = {m["group"]: m for m in obs["members"]}
    log.info("verdict: after 1st advance members=%s overall=%s",
             {g: m["current_weight"] for g, m in by_group.items()},
             obs["overall_verdict"])
    # 各组按各自节奏推进，权重与组状态一致
    assert by_group["g1"]["current_weight"] == 10
    assert by_group["g1"]["version"] == "v2"
    assert by_group["g2"]["current_weight"] == 20
    assert by_group["g2"]["version"] == "v3"
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 20
    assert obs["overall_verdict"] == "PASS"
    assert obs["status"] == "RUNNING"
    # 每组的判定结论与依据可查
    assert by_group["g1"]["gate_verdict"] == "PASS"
    assert "error_rate" in by_group["g1"]["gate_basis"]

    svc.advance_batch(batch.batch_id)  # g1 -> 100 SUCCEEDED, g2 -> 40
    svc.advance_batch(batch.batch_id)  # g2 -> 100
    obs = svc.observe_batch(batch.batch_id)
    log.info("verdict: batch final status=%s", obs["status"])
    assert obs["status"] == "SUCCEEDED"
    assert all(m["current_weight"] == 100 for m in obs["members"])


def test_batch_gate_block_stops_all_groups(tmp_path):
    """任一组被门禁挡下时，其余组不得继续各自扩量。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # 两组都到 10
    # g2 指标恶化
    healthy(svc, "g2", error_rate=0.9)
    with pytest.raises(GateBlockedError) as e:
        svc.advance_batch(batch.batch_id)
    log.info("verdict: g2 blocked -> %s", e.value)
    assert "g2" in str(e.value)
    obs = svc.observe_batch(batch.batch_id)
    by_group = {m["group"]: m for m in obs["members"]}
    # 整批不动：g1 没有独自扩量到无法回收的位置
    assert by_group["g1"]["current_weight"] == 10
    assert by_group["g2"]["current_weight"] == 10
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 10
    assert obs["overall_verdict"] == "BLOCKED"
    assert "g2" in obs["overall_basis"]
    log.info("verdict: overall=%s basis=%s", obs["overall_verdict"],
             obs["overall_basis"])
    # g2 恢复后整批可继续
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    assert svc.storage.get_group("g1").weight == 100


def test_concurrent_batch_advance_is_serializable(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, steps=[10, 20, 30, 40, 100])
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    errors, successes = [], []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        try:
            svc.advance_batch(batch.batch_id)
            successes.append(1)
        except (GateBlockedError, IllegalTransitionError):
            pass
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    obs = svc.observe_batch(batch.batch_id)
    log.info("input: 8 concurrent advance_batch; verdict: successes=%d "
             "member steps=%s", len(successes),
             [m["step_index"] for m in obs["members"]])
    # 与串行一致：成功数 == 整批推进步数，且各成员齐步走
    assert len(successes) == 5
    assert obs["status"] == "SUCCEEDED"
    assert all(m["current_weight"] == 100 for m in obs["members"])


# ---------------------------------------------------------------- 整批回滚

def test_batch_rollback_restores_all_groups(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # 两组 stable=10
    rb = svc.rollback_batch(batch.batch_id, reason="alarm")
    assert rb.status == PlanStatus.ROLLED_BACK
    obs = svc.observe_batch(batch.batch_id)
    log.info("verdict: batch rollback -> %s", [m["status"] for m in obs["members"]])
    # 所有成员一起回到各自最近稳定点
    assert all(m["status"] == "ROLLED_BACK" for m in obs["members"])
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 10
    assert obs["failure_reason"] == "alarm"


def test_batch_rollback_reentrant_and_concurrent(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    svc.rollback_batch(batch.batch_id, reason="first")
    # 重复触发：无副作用
    again = svc.rollback_batch(batch.batch_id, reason="duplicate")
    assert again.status == PlanStatus.ROLLED_BACK
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 10
    # 并发重复触发
    barrier = threading.Barrier(6)
    results, errors = [], []

    def worker():
        barrier.wait()
        try:
            results.append(svc.rollback_batch(batch.batch_id).status)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=worker) for _ in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    assert all(s == PlanStatus.ROLLED_BACK for s in results)
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 10
    log.info("verdict: 6 concurrent batch rollbacks, weights still 10/10")


def test_batch_abort_pending_and_running(tmp_path):
    svc = make_service(tmp_path)
    b1 = make_batch(svc)
    svc.abort_batch(b1.batch_id)
    obs = svc.observe_batch(b1.batch_id)
    assert obs["status"] == "ABORTED"
    assert all(m["status"] == "ABORTED" for m in obs["members"])

    b2 = make_batch(svc)
    svc.start_batch(b2.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(b2.batch_id)
    svc.abort_batch(b2.batch_id)
    obs = svc.observe_batch(b2.batch_id)
    assert obs["status"] == "ROLLED_BACK"
    assert obs["failure_reason"] == "aborted"
    log.info("verdict: abort RUNNING batch -> %s", obs["status"])


def test_member_plan_direct_ops_rejected(tmp_path):
    """批次成员计划不允许绕过批次单独操作。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    member = svc.storage.get_plan(svc.storage.get_batch(batch.batch_id).plan_ids[0])
    for action in (svc.start, svc.advance, svc.rollback, svc.abort):
        with pytest.raises(IllegalTransitionError) as e:
            action(member.plan_id)
        assert "batch" in str(e.value)
        log.info("verdict: direct %s on member -> %s", action.__name__, e.value)
    with pytest.raises(IllegalTransitionError):
        svc.run_async(member.plan_id)


# ---------------------------------------------------------------- 组冲突

def test_group_conflict_distinct_reason(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    # 单组计划撞上进行中批次
    with pytest.raises(GroupConflictError) as e1:
        svc.create_plan("v9", "g1", [10, 100])
    assert e1.value.reason == "group_conflict"
    assert "batch" in str(e1.value)
    log.info("verdict: plan on busy group -> %s", e1.value)
    # 另一个批次撞上同一组
    with pytest.raises(GroupConflictError) as e2:
        svc.create_batch([{"group": "g2", "version": "v9", "steps": [10, 100]}])
    assert e2.value.reason == "group_conflict"
    # 单组计划之间同样冲突
    p = svc.create_plan("v9", "g3", [10, 100])
    with pytest.raises(GroupConflictError):
        svc.create_plan("v10", "g3", [10, 100])
    # 批次终态后组被释放
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    svc.rollback_batch(batch.batch_id)
    svc.create_plan("v9", "g1", [10, 100])
    log.info("verdict: group released after batch terminal, conflict reason ok")


def test_batch_validation_errors(tmp_path):
    svc = make_service(tmp_path)
    with pytest.raises(ValidationError):
        svc.create_batch([])
    with pytest.raises(ValidationError):  # 批次内组重复
        svc.create_batch([
            {"group": "g1", "version": "v2", "steps": [10, 100]},
            {"group": "g1", "version": "v3", "steps": [10, 100]},
        ])
    with pytest.raises(ValidationError):  # 成员步序列非法
        svc.create_batch([{"group": "g1", "version": "v2", "steps": [50, 40]}])
    log.info("verdict: empty/duplicate/bad-steps batches rejected")


# ---------------------------------------------------------------- 中断恢复

def test_recovery_finishes_interrupted_batch_rollback(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # stable=10/10
    # 模拟回滚进行到一半崩溃：批次与成员停在 ROLLING_BACK
    svc._batch_transition(svc.storage.get_batch(batch.batch_id), "rollback")
    members = [svc.storage.get_plan(pid)
               for pid in svc.storage.get_batch(batch.batch_id).plan_ids]
    for p in members:
        svc._transition(p, "rollback")
    svc.storage.save_batch(svc.storage.get_batch(batch.batch_id), members)
    # 重启恢复
    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    obs = svc2.observe_batch(batch.batch_id)
    log.info("verdict: recovered batch status=%s members=%s", obs["status"],
             [m["status"] for m in obs["members"]])
    assert obs["status"] == "ROLLED_BACK"
    assert all(m["status"] == "ROLLED_BACK" for m in obs["members"])
    # 不留「只回滚了一半」的对外状态
    assert svc2.storage.get_group("g1").weight == 10
    assert svc2.storage.get_group("g2").weight == 10


def test_recovery_repairs_half_advanced_batch(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # stable=10/10
    # 模拟崩溃：g2 组状态被部分更新但批次未落下一步
    from rollout.models import InstanceGroupState
    svc.storage.save_group(InstanceGroupState(group="g2", version="v3-g2",
                                              weight=100))
    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    assert svc2.storage.get_group("g2").weight == 10
    obs = svc2.observe_batch(batch.batch_id)
    assert obs["status"] == "RUNNING"  # 批次未丢失，可继续
    log.info("verdict: half-advanced group repaired to stable=10")


def test_run_batch_async_and_gate_timeout(tmp_path):
    svc = make_service(tmp_path, step_timeout_seconds=0.15)
    try:
        b1 = make_batch(svc, groups=("g1", "g2"))
        svc.start_batch(b1.batch_id)
        healthy(svc, "g1")
        healthy(svc, "g2")
        svc.run_batch_async(b1.batch_id, step_interval=0.02)
        assert svc.wait_idle()
        assert svc.observe_batch(b1.batch_id)["status"] == "SUCCEEDED"
        log.info("verdict: run_batch_async reached SUCCEEDED")

        b2 = make_batch(svc, groups=("g3", "g4"))
        svc.start_batch(b2.batch_id)
        healthy(svc, "g3")
        healthy(svc, "g4", error_rate=0.9)  # g4 门禁永不通过
        svc.run_batch_async(b2.batch_id, step_interval=0.02)
        assert svc.wait_idle()
        obs = svc.observe_batch(b2.batch_id)
        assert obs["status"] == "ROLLED_BACK"
        assert obs["failure_reason"] == "gate_timeout"
        # 整批一起回滚：g3 也没有被留在扩量位置
        assert svc.storage.get_group("g3").weight == 0
        assert svc.storage.get_group("g4").weight == 0
        log.info("verdict: gate timeout -> whole batch rolled back")
    finally:
        svc.shutdown()


# ---------------------------------------------------------------- 旧数据兼容

def test_legacy_v2_storage_without_batches_loads(tmp_path):
    """升级前的 v2 存储文件（无 batches 字段、计划无 batch_id）正常加载。"""
    path = tmp_path / "state.json"
    path.write_text(json.dumps({
        "format_version": 2,
        "plans": [{
            "plan_id": "legacy1", "version": "v2", "group": "g1",
            "steps": [10, 100], "status": "PENDING", "step_index": -1,
            "stable": {"step_index": -1, "weight": 0},
            "gate": {"max_error_rate": 0.05, "max_unhealthy_ratio": 0.2},
            "failure_reason": None, "created_at": 1.0, "updated_at": 1.0,
            "format_version": 2,
        }],
        "groups": [{"group": "g1", "version": "v1", "weight": 0,
                    "instances_healthy": True}],
        "health": [],
    }))
    svc = RolloutService(Storage(str(path)),
                         ControlPlaneConfig(storage_path=str(path)))
    svc.recover()
    plan = svc.get_plan("legacy1")
    assert plan.batch_id is None  # 缺失字段的确定缺省
    assert svc.storage.list_batches() == []
    # 旧单组计划按原语义继续工作
    svc.start("legacy1")
    healthy(svc, "g1")
    svc.advance("legacy1")
    svc.advance("legacy1")
    assert svc.get_plan("legacy1").status == PlanStatus.SUCCEEDED
    assert svc.storage.get_group("g1").weight == 100
    log.info("verdict: legacy v2 plan completed under single-group semantics")


def test_legacy_v1_plan_still_rolls_back(tmp_path):
    """v1 计划（无 gate/stable/batch_id）缺省加载后回滚语义不变。"""
    path = tmp_path / "state.json"
    path.write_text(json.dumps({
        "format_version": 1,
        "plans": [{
            "plan_id": "old1", "version": "v2", "group": "g1",
            "steps": [10, 100], "status": "PENDING",
        }],
    }))
    svc = RolloutService(Storage(str(path)),
                         ControlPlaneConfig(storage_path=str(path)))
    svc.recover()
    svc.start("old1")
    healthy(svc, "g1")
    svc.advance("old1")
    svc.rollback("old1", reason="compat")
    plan = svc.get_plan("old1")
    assert plan.status == PlanStatus.ROLLED_BACK
    assert svc.storage.get_group("g1").weight == plan.stable.weight
    log.info("verdict: v1 plan rollback ok, stable=%d", plan.stable.weight)


def test_incompatible_batch_record_rejected(tmp_path):
    """无法兼容的批次记录明确拒绝，而不是按默认值继续。"""
    from rollout.errors import StorageCorruptError
    path = tmp_path / "state.json"
    path.write_text(json.dumps({
        "format_version": 3,
        "plans": [],
        "batches": [{"batch_id": "b1", "plan_ids": [], "status": "PENDING",
                     "format_version": 99}],
    }))
    with pytest.raises(StorageCorruptError) as e:
        Storage(str(path)).load()
    log.info("verdict: bad batch format -> %s", e.value)
