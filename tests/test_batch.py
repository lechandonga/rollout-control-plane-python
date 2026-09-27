"""多组协同发布（批次）测试：协同推进、整批回滚、冲突、恢复与旧数据兼容。

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
from rollout.models import (
    GatePolicy,
    GroupTarget,
    HealthMetrics,
    InstanceGroupState,
    PlanStatus,
)
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


def make_batch(svc, spec):
    """spec: {group: (version, steps)}"""
    targets = [GroupTarget(group=g, version=v, steps=s) for g, (v, s) in spec.items()]
    return svc.create_batch_plan(targets)


# ---------------------------------------------------------------- 协同推进与观测

def test_batch_advance_per_group_progress_and_overall_conclusion(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 50, 100]), "g2": ("v1.1", [20, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")

    svc.advance_batch(batch.batch_id)
    obs = svc.observe_batch(batch.batch_id)
    by_group = {g["group"]: g for g in obs["groups"]}
    log.info("verdict: after 1st advance obs=%s", obs["conclusion"])
    assert obs["status"] == "RUNNING"
    # 每组各自的步、权重、判定结论与依据
    assert by_group["g1"]["current_weight"] == 10
    assert by_group["g2"]["current_weight"] == 20
    for g in obs["groups"]:
        assert g["gate_verdict"] == "PASS"
        assert g["gate_basis"]
        assert g["group_state"]["weight"] == g["current_weight"]

    svc.advance_batch(batch.batch_id)  # g1->50, g2->100(done)
    svc.advance_batch(batch.batch_id)  # g1->100
    obs = svc.observe_batch(batch.batch_id)
    log.info("verdict: final status=%s conclusion=%s", obs["status"], obs["conclusion"])
    assert obs["status"] == "SUCCEEDED"
    assert "100%" in obs["conclusion"]
    assert svc.storage.get_group("g1").weight == 100
    assert svc.storage.get_group("g2").weight == 100


def test_batch_gate_block_stops_whole_batch(tmp_path):
    """任一组被门禁挡下时，其余组不得继续扩量（整批无状态变更）。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # 两组都到 10

    healthy(svc, "g2", error_rate=0.5)  # g2 超标
    with pytest.raises(GateBlockedError) as e:
        svc.advance_batch(batch.batch_id)
    log.info("verdict: g2 blocked -> %s", e.value)
    assert "g2" in str(e.value) and "BLOCKED" in str(e.value)
    # 整批无状态变更：g1 不得独自扩量
    obs = svc.observe_batch(batch.batch_id)
    by_group = {g["group"]: g for g in obs["groups"]}
    assert by_group["g1"]["current_weight"] == 10
    assert svc.storage.get_group("g1").weight == 10
    assert svc.storage.get_group("g2").weight == 10
    # 被挡组的判定依据可查询
    assert by_group["g2"]["last_verdict"] == "BLOCKED"
    assert "error_rate" in by_group["g2"]["last_basis"]
    # g2 恢复健康后整批可继续
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    assert svc.storage.get_group("g1").weight == 100


# ---------------------------------------------------------------- 整批回滚

def test_batch_rollback_restores_all_groups_and_is_reentrant(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 50, 100]), "g2": ("v2", [30, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # g1=10, g2=30
    svc.advance_batch(batch.batch_id)  # g1=50, g2=100

    rb = svc.rollback_batch(batch.batch_id, reason="regression")
    assert rb.status == PlanStatus.ROLLED_BACK
    # 所有组一起回到各自的最近稳定点（g1=50, g2=100 即其稳定点）
    assert svc.storage.get_group("g1").weight == 50
    assert svc.storage.get_group("g2").weight == 100
    obs = svc.observe_batch(batch.batch_id)
    log.info("verdict: batch rollback conclusion=%s", obs["conclusion"])
    assert "regression" in obs["failure_reason"] or rb.failure_reason == "regression"

    # 重复触发：无副作用
    again = svc.rollback_batch(batch.batch_id, reason="duplicate")
    assert again.status == PlanStatus.ROLLED_BACK
    assert svc.storage.get_group("g1").weight == 50

    # 并发重复触发：与串行等价，无额外副作用
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
    assert svc.storage.get_group("g1").weight == 50
    assert svc.storage.get_group("g2").weight == 100
    log.info("verdict: 6 concurrent batch rollbacks, groups unchanged")


def test_batch_rollback_atomic_snapshot_via_storage(tmp_path):
    """回滚后磁盘上的快照必须整批一致（不存在半回滚的持久化状态）。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    svc.rollback_batch(batch.batch_id, reason="atomic-check")
    # 直接从磁盘重新加载，验证持久化快照一致
    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    restored = svc2.get_batch(batch.batch_id)
    assert restored.status == PlanStatus.ROLLED_BACK
    assert svc2.storage.get_group("g1").weight == 10
    assert svc2.storage.get_group("g2").weight == 10
    log.info("verdict: reloaded snapshot consistent, both groups at stable=10")


def test_batch_illegal_transitions_have_distinct_reasons(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100])})
    with pytest.raises(IllegalTransitionError) as e1:
        svc.advance_batch(batch.batch_id)
    assert "PENDING" in str(e1.value)
    with pytest.raises(IllegalTransitionError) as e2:
        svc.rollback_batch(batch.batch_id)
    assert "PENDING" in str(e2.value)
    svc.abort_batch(batch.batch_id)
    assert svc.get_batch(batch.batch_id).status == PlanStatus.ABORTED
    with pytest.raises(IllegalTransitionError) as e3:
        svc.start_batch(batch.batch_id)
    assert "terminal" in str(e3.value)
    log.info("verdict: illegal batch transitions -> %s / %s / %s", e1.value, e2.value, e3.value)


def test_batch_validation_rejects_bad_input(tmp_path):
    svc = make_service(tmp_path)
    with pytest.raises(ValidationError):
        svc.create_batch_plan([])
    with pytest.raises(ValidationError) as e:
        make_batch(svc, {"g1": ("v2", [10, 100]), "g1b": ("v2", [10, 100])}) \
            if False else svc.create_batch_plan([
                GroupTarget(group="g1", version="v2", steps=[10, 100]),
                GroupTarget(group="g1", version="v3", steps=[10, 100]),
            ])
    assert "duplicate" in str(e.value)
    with pytest.raises(ValidationError):
        svc.create_batch_plan([GroupTarget(group="g1", version="v2", steps=[50, 40])])
    log.info("verdict: invalid batch input rejected with validation_error")


# ---------------------------------------------------------------- 组冲突

def test_group_conflict_between_plan_and_batch(tmp_path):
    svc = make_service(tmp_path)
    # 进行中的单组计划占用 g1 -> 批次创建冲突
    plan = svc.create_plan("v2", "g1", [10, 100])
    svc.start(plan.plan_id)
    with pytest.raises(GroupConflictError) as e1:
        make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [10, 100])})
    assert e1.value.reason == "group_conflict"
    log.info("verdict: batch vs running plan -> %s", e1.value)

    # 进行中的批次占用 g3 -> 单组计划创建冲突
    batch = make_batch(svc, {"g3": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    with pytest.raises(GroupConflictError) as e2:
        svc.create_plan("v9", "g3", [10, 100])
    assert e2.value.reason == "group_conflict"

    # 批次 vs 批次
    with pytest.raises(GroupConflictError) as e3:
        make_batch(svc, {"g3": ("v3", [10, 100])})
    assert e3.value.reason == "group_conflict"

    # 创建时不冲突、启动时冲突：另一个批次先启动占了 g5
    b1 = svc.create_batch_plan([GroupTarget(group="g5", version="v1", steps=[10, 100])])
    # 先让 b1 之外的计划结束以释放？直接制造启动期冲突：
    b2_holder = svc.create_batch_plan([GroupTarget(group="g6", version="v1", steps=[10, 100])])
    svc.start_batch(b2_holder.batch_id)
    # g6 被占用期间，针对 g6 的新批次创建即冲突
    with pytest.raises(GroupConflictError):
        svc.create_batch_plan([GroupTarget(group="g6", version="v2", steps=[10, 100])])
    svc.start_batch(b1.batch_id)  # g5 无冲突，可正常启动
    assert svc.get_batch(b1.batch_id).status == PlanStatus.RUNNING
    log.info("verdict: conflicts distinguishable, non-conflicting batch starts fine")


def test_conflict_released_after_termination(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    svc.advance_batch(batch.batch_id)  # g1 -> 10
    svc.advance_batch(batch.batch_id)  # g1 -> 100 -> SUCCEEDED
    # 批次终态后，同组可创建新发布
    plan = svc.create_plan("v3", "g1", [10, 100])
    svc.start(plan.plan_id)
    assert svc.get_plan(plan.plan_id).status == PlanStatus.RUNNING
    log.info("verdict: group reusable after batch reached terminal state")


# ---------------------------------------------------------------- 并发批次

def test_concurrent_batches_on_disjoint_groups_serializable(tmp_path):
    svc = make_service(tmp_path)
    b1 = make_batch(svc, {"g1": ("v2", [10, 100])})
    b2 = make_batch(svc, {"g2": ("v2", [20, 100])})
    svc.start_batch(b1.batch_id)
    svc.start_batch(b2.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    errors = []
    barrier = threading.Barrier(4)

    def worker(batch_id):
        barrier.wait()
        try:
            svc.advance_batch(batch_id)
            svc.advance_batch(batch_id)
        except IllegalTransitionError:
            pass  # 串行等价：到达终态后的推进被拒绝
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=worker, args=(bid,))
          for bid in (b1.batch_id, b2.batch_id) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    # 与串行一致：两个批次都到达 SUCCEEDED，权重 100
    assert svc.get_batch(b1.batch_id).status == PlanStatus.SUCCEEDED
    assert svc.get_batch(b2.batch_id).status == PlanStatus.SUCCEEDED
    assert svc.storage.get_group("g1").weight == 100
    assert svc.storage.get_group("g2").weight == 100
    log.info("verdict: concurrent disjoint batches both SUCCEEDED")


# ---------------------------------------------------------------- 中断恢复

def test_recovery_finishes_interrupted_batch_rollback(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # 两组 stable=10
    # 模拟回滚进行到一半崩溃：状态停在 ROLLING_BACK，组权重仍是 10
    batch = svc.get_batch(batch.batch_id)  # 重新取已发布副本再模拟崩溃
    svc._transition(batch, "rollback")
    batch.failure_reason = "crash"
    svc.storage.save_batch(batch)

    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    restored = svc2.get_batch(batch.batch_id)
    assert restored.status == PlanStatus.ROLLED_BACK
    # 整批一致：两组都回到各自稳定点，不存在半回滚
    assert svc2.storage.get_group("g1").weight == 10
    assert svc2.storage.get_group("g2").weight == 10
    log.info("verdict: interrupted batch rollback finished consistently")


def test_recovery_repairs_diverged_batch_group(tmp_path):
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 50, 100]), "g2": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # stable: g1=10, g2=10
    # 模拟崩溃：g1 组状态被部分更新到 50，但批次未落下一步
    svc.storage.save_group(InstanceGroupState(group="g1", version="v2", weight=50))

    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    restored = svc2.get_batch(batch.batch_id)
    assert restored.status == PlanStatus.RUNNING  # 计划未丢失
    assert svc2.storage.get_group("g1").weight == 10  # 修复回稳定点
    assert svc2.storage.get_group("g2").weight == 10
    log.info("verdict: diverged group repaired to stable=10, batch still RUNNING")


# ---------------------------------------------------------------- 旧数据兼容

def test_legacy_v2_storage_without_batches_loads(tmp_path):
    """升级前的 v2 存储文件（无 batches 字段）必须能加载，单组语义不变。"""
    path = str(tmp_path / "state.json")
    legacy = {
        "format_version": 2,
        "plans": [{
            "plan_id": "legacy1", "version": "v2", "group": "g1",
            "steps": [10, 100], "status": "PENDING", "step_index": -1,
            "format_version": 2,
        }],
        "groups": [{"group": "g1", "version": "v1", "weight": 0}],
        "health": [],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(legacy, f)
    svc = RolloutService(Storage(path), ControlPlaneConfig(storage_path=path))
    svc.recover()  # 触发加载
    # 旧单组计划按原语义继续工作
    plan = svc.get_plan("legacy1")
    assert plan.status == PlanStatus.PENDING
    svc.start("legacy1")
    healthy(svc, "g1")
    svc.advance("legacy1")
    assert svc.storage.get_group("g1").weight == 10
    # 批次列表确定缺省为空
    assert svc.storage.list_batches() == []
    log.info("verdict: legacy v2 storage loaded, single-group plan works, batches empty")


def test_legacy_v1_plan_defaults_still_apply(tmp_path):
    """v1 计划缺失 gate/stable 字段 -> 确定缺省；加载后可正常推进与回滚。"""
    path = str(tmp_path / "state.json")
    legacy = {
        "format_version": 1,
        "plans": [{
            "plan_id": "old1", "version": "v2", "group": "g1",
            "steps": [10, 100], "status": "PENDING",
        }],
        "groups": [],
        "health": [],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(legacy, f)
    svc = RolloutService(Storage(path), ControlPlaneConfig(storage_path=path))
    svc.recover()
    plan = svc.get_plan("old1")
    assert plan.gate.max_error_rate == 0.05  # 确定缺省
    assert plan.stable.weight == 0
    svc.start("old1")
    healthy(svc, "g1")
    svc.advance("old1")
    svc.rollback("old1", reason="compat-check")
    assert svc.get_plan("old1").status == PlanStatus.ROLLED_BACK
    assert svc.storage.get_group("g1").weight == 10  # 回到稳定点
    log.info("verdict: v1 plan defaults applied, rollout+rollback work")


def test_incompatible_batch_format_rejected(tmp_path):
    """无法兼容的批次格式版本必须明确拒绝，而不是按默认值继续。"""
    from rollout.errors import StorageCorruptError
    path = str(tmp_path / "state.json")
    bad = {
        "format_version": 3,
        "plans": [],
        "batches": [{"batch_id": "b1", "targets": [], "status": "PENDING",
                     "format_version": 99}],
        "groups": [],
        "health": [],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(bad, f)
    with pytest.raises(StorageCorruptError) as e:
        Storage(path).load()
    assert e.value.reason == "storage_corrupt"
    log.info("verdict: incompatible batch format rejected -> %s", e.value)


# ---------------------------------------------------------------- 后台整批放量

def test_run_batch_async_auto_rollback_on_gate_timeout(tmp_path):
    svc = make_service(tmp_path, step_timeout_seconds=0.15)
    try:
        batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [10, 100])})
        svc.start_batch(batch.batch_id)
        healthy(svc, "g1")
        healthy(svc, "g2", error_rate=0.9)  # g2 门禁永不通过
        svc.run_batch_async(batch.batch_id, step_interval=0.02)
        assert svc.wait_idle()
        restored = svc.get_batch(batch.batch_id)
        assert restored.status == PlanStatus.ROLLED_BACK
        assert restored.failure_reason == "gate_timeout"
        # 整批一起回滚：两组都在各自稳定点（0）
        assert svc.storage.get_group("g1").weight == 0
        assert svc.storage.get_group("g2").weight == 0
        log.info("verdict: batch gate timeout -> whole batch rolled back")
    finally:
        svc.shutdown()


def test_run_batch_async_completes(tmp_path):
    svc = make_service(tmp_path)
    try:
        batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [50, 100])})
        svc.start_batch(batch.batch_id)
        healthy(svc, "g1")
        healthy(svc, "g2")
        svc.run_batch_async(batch.batch_id, step_interval=0.02)
        assert svc.wait_idle()
        assert svc.get_batch(batch.batch_id).status == PlanStatus.SUCCEEDED
        assert svc.storage.get_group("g1").weight == 100
        assert svc.storage.get_group("g2").weight == 100
        log.info("verdict: run_batch_async reached SUCCEEDED for all groups")
    finally:
        svc.shutdown()
