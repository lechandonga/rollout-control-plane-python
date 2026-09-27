"""整批发布对外快照一致性测试。

覆盖需求要求的四条路径：
1. 整批推进中并发查询：任何一次 observe_batch 都必须是推进前或推进后的
   完整快照，不允许「部分组新权重、部分组旧权重」的半截状态；
2. 整批回滚中并发查询：同理，不允许「部分组已回稳定点、部分组还在放量
   权重」；回滚请求结束后所有组必须都在各自最近稳定点；
3. 重复与并发触发回滚：幂等、无额外副作用，快照始终自洽；
4. 中断恢复后查询：恢复后的批次状态、各组权重与整批结论必须自洽。

一致性判定（对单次快照而言）：
- 每组的 group_state.weight 必须与该组 current_weight 相等（组尚未建立
  时 current_weight 必须为 0）；
- 整批结论必须与同一份快照里的各组进度对得上：SUCCEEDED 时所有组必须
  都已 100%；RUNNING 时结论里的 done 计数必须等于明细中已达 100% 的组数；
- ROLLED_BACK 时所有组必须都在各自的最近稳定点。
"""
import logging
import threading

from rollout.config import ControlPlaneConfig
from rollout.models import GroupTarget, HealthMetrics, InstanceGroupState, PlanStatus
from rollout.service import RolloutService
from rollout.storage import Storage

log = logging.getLogger("test.snapshot")


def make_service(tmp_path, **cfg):
    config = ControlPlaneConfig(storage_path=str(tmp_path / "state.json"), **cfg)
    storage = Storage(config.storage_path)
    return RolloutService(storage, config)


def healthy(svc, group, error_rate=0.0, unhealthy=0.0):
    svc.report_health(group, HealthMetrics(error_rate=error_rate,
                                           unhealthy_ratio=unhealthy))


def make_batch(svc, spec):
    targets = [GroupTarget(group=g, version=v, steps=s) for g, (v, s) in spec.items()]
    return svc.create_batch_plan(targets)


def assert_snapshot_consistent(obs):
    """校验单次快照内部自洽；不自洽时抛出带完整快照的断言错误。"""
    total = len(obs["groups"])
    done = 0
    for g in obs["groups"]:
        if g["group_state"] is None:
            # 组状态尚未建立：该组必须还没开始放量
            assert g["current_weight"] == 0, (
                f"group {g['group']} has no state but weight "
                f"{g['current_weight']}: {obs}"
            )
        else:
            # 核心不变量：对外组权重必须与批次内该组进度一致
            assert g["group_state"]["weight"] == g["current_weight"], (
                f"torn snapshot on group {g['group']}: group_state weight "
                f"{g['group_state']['weight']} != current_weight "
                f"{g['current_weight']}: {obs}"
            )
        if g["current_weight"] >= 100:
            done += 1
    status = obs["status"]
    if status == "SUCCEEDED":
        # 结论说整批推完，明细里就不允许还有组没到位
        assert done == total, f"conclusion/details mismatch: {obs}"
        assert "100%" in obs["conclusion"]
    elif status in ("PENDING", "RUNNING"):
        # 结论里的 done 计数必须与明细对得上
        assert f"{done}/{total}" in obs["conclusion"], (
            f"conclusion {obs['conclusion']!r} disagrees with details "
            f"({done}/{total} done): {obs}"
        )
    elif status == "ROLLED_BACK":
        # 结论说整批已回滚，明细里每组都必须停在各自稳定点
        for g in obs["groups"]:
            assert g["current_weight"] == g["stable_weight"], (
                f"group {g['group']} not at stable point after rollback: {obs}"
            )
            assert g["group_state"] is not None
            assert g["group_state"]["weight"] == g["stable_weight"], (
                f"group {g['group']} state not restored to stable point: {obs}"
            )


# ---------------------------------------------------------------- 推进中并发查询

def test_observe_batch_during_async_run_is_always_consistent(tmp_path):
    """后台自动整批放量在跑的同时被反复查询：每次都必须拿到完整快照。"""
    svc = make_service(tmp_path)
    try:
        spec = {
            "g1": ("v2", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
            "g2": ("v2", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
            "g3": ("v2", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
        }
        batch = make_batch(svc, spec)
        svc.start_batch(batch.batch_id)
        for g in spec:
            healthy(svc, g)

        stop = threading.Event()
        violations = []
        reads = [0]

        def reader():
            while not stop.is_set():
                obs = svc.observe_batch(batch.batch_id)
                reads[0] += 1
                try:
                    assert_snapshot_consistent(obs)
                except AssertionError as e:
                    violations.append(str(e))
                    return

        readers = [threading.Thread(target=reader) for _ in range(4)]
        [t.start() for t in readers]
        svc.run_batch_async(batch.batch_id, step_interval=0.01)
        assert svc.wait_idle()
        # 让 reader 再扫几轮终态快照后停下
        for _ in range(3):
            assert_snapshot_consistent(svc.observe_batch(batch.batch_id))
        stop.set()
        [t.join() for t in readers]

        assert not violations, f"torn snapshots observed: {violations[:3]}"
        assert reads[0] > 0
        final = svc.observe_batch(batch.batch_id)
        assert final["status"] == "SUCCEEDED"
        assert_snapshot_consistent(final)
        log.info("verdict: %d concurrent reads during async run, all consistent",
                 reads[0])
    finally:
        svc.shutdown()


# ---------------------------------------------------------------- 回滚中并发查询

def test_observe_batch_during_concurrent_rollback_is_consistent(tmp_path):
    """整批回滚进行中并发查询：快照要么是回滚前、要么是回滚后，不能各一半。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {
        "g1": ("v2", [10, 50, 100]),
        "g2": ("v2", [20, 40, 100]),
        "g3": ("v2", [30, 100]),
    })
    svc.start_batch(batch.batch_id)
    for g in ("g1", "g2", "g3"):
        healthy(svc, g)
    svc.advance_batch(batch.batch_id)
    svc.advance_batch(batch.batch_id)  # g1=50, g2=40, g3=100

    seen_rolled_back = threading.Event()
    violations = []

    def reader():
        while not seen_rolled_back.is_set():
            obs = svc.observe_batch(batch.batch_id)
            try:
                assert_snapshot_consistent(obs)
            except AssertionError as e:
                violations.append(str(e))
                seen_rolled_back.set()
                return
            if obs["status"] == "ROLLED_BACK":
                seen_rolled_back.set()

    def rollbacker(i):
        svc.rollback_batch(batch.batch_id, reason=f"concurrent-{i}")

    readers = [threading.Thread(target=reader) for _ in range(3)]
    [t.start() for t in readers]
    rollers = [threading.Thread(target=rollbacker, args=(i,)) for i in range(4)]
    [t.start() for t in rollers]
    [t.join() for t in rollers]
    # 回滚请求全部结束后：再查必须所有组都在各自稳定点
    obs = svc.observe_batch(batch.batch_id)
    assert obs["status"] == "ROLLED_BACK"
    assert_snapshot_consistent(obs)
    seen_rolled_back.set()
    [t.join() for t in readers]

    assert not violations, f"torn snapshots during rollback: {violations[:3]}"
    # 各组稳定点：g1=50, g2=40, g3=100
    assert svc.storage.get_group("g1").weight == 50
    assert svc.storage.get_group("g2").weight == 40
    assert svc.storage.get_group("g3").weight == 100
    log.info("verdict: concurrent rollback + reads consistent, groups at stable")


def test_repeated_and_concurrent_rollback_no_extra_effects(tmp_path):
    """重复触发、并发触发回滚：幂等，无副作用，快照前后一致。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 50, 100]),
                             "g2": ("v2", [30, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # stable: g1=10, g2=30
    svc.advance_batch(batch.batch_id)  # stable: g1=50, g2=100

    # 并发 + 重复触发
    barrier = threading.Barrier(8)
    errors = []

    def worker():
        barrier.wait()
        try:
            svc.rollback_batch(batch.batch_id, reason="dup")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    ts = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert not errors
    # 再重复触发一次：no-op
    svc.rollback_batch(batch.batch_id, reason="dup-again")

    # 无额外副作用：权重恰为各自稳定点，不多不少
    assert svc.storage.get_group("g1").weight == 50
    assert svc.storage.get_group("g2").weight == 100
    obs = svc.observe_batch(batch.batch_id)
    assert obs["status"] == "ROLLED_BACK"
    assert_snapshot_consistent(obs)
    by_group = {g["group"]: g for g in obs["groups"]}
    assert by_group["g1"]["stable_weight"] == 50
    assert by_group["g2"]["stable_weight"] == 100
    log.info("verdict: 8 concurrent + 1 repeated rollback, idempotent, consistent")


# ---------------------------------------------------------------- 中断恢复后查询

def test_observe_after_recovery_from_interrupted_rollback(tmp_path):
    """回滚进行到一半进程中断：恢复后查询必须是自洽的整批已回滚快照。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 100]), "g2": ("v2", [20, 100])})
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # stable: g1=10, g2=20
    # 模拟回滚中断：状态停在 ROLLING_BACK
    svc._transition(batch, "rollback")
    batch.failure_reason = "crash"
    svc.storage.save_batch(batch)

    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    obs = svc2.observe_batch(batch.batch_id)
    assert obs["status"] == "ROLLED_BACK"
    assert_snapshot_consistent(obs)
    by_group = {g["group"]: g for g in obs["groups"]}
    assert by_group["g1"]["group_state"]["weight"] == 10
    assert by_group["g2"]["group_state"]["weight"] == 20
    log.info("verdict: post-recovery snapshot consistent, conclusion=%s",
             obs["conclusion"])


def test_observe_after_recovery_from_partial_group_repair(tmp_path):
    """推进中断导致组权重与稳定点分叉：恢复后不允许只剩一半组被修复。"""
    svc = make_service(tmp_path)
    batch = make_batch(svc, {"g1": ("v2", [10, 50, 100]),
                             "g2": ("v2", [10, 100]),
                             "g3": ("v2", [10, 100])})
    svc.start_batch(batch.batch_id)
    for g in ("g1", "g2", "g3"):
        healthy(svc, g)
    svc.advance_batch(batch.batch_id)  # stable: 全部 10
    # 模拟崩溃：g1/g2 的组状态被部分推进，g3 未动
    svc.storage.save_group(InstanceGroupState(group="g1", version="v2", weight=50))
    svc.storage.save_group(InstanceGroupState(group="g2", version="v2", weight=100))

    svc2 = RolloutService(Storage(svc.config.storage_path), svc.config)
    svc2.recover()
    obs = svc2.observe_batch(batch.batch_id)
    assert obs["status"] == "RUNNING"  # 计划未丢
    assert_snapshot_consistent(obs)
    # 所有分叉组都被修回稳定点，不存在「只修了一半」
    for g in ("g1", "g2", "g3"):
        assert svc2.storage.get_group(g).weight == 10
    log.info("verdict: all diverged groups repaired, snapshot consistent")


# ---------------------------------------------------------------- 单组计划同样保证

def test_observe_plan_during_async_run_is_consistent(tmp_path):
    """单组发布的查询路径享有同一条快照一致性保证。"""
    svc = make_service(tmp_path)
    try:
        plan = svc.create_plan("v2", "g1", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
        svc.start(plan.plan_id)
        healthy(svc, "g1")

        stop = threading.Event()
        violations = []

        def reader():
            while not stop.is_set():
                obs = svc.observe(plan.plan_id)
                try:
                    if obs["group_state"] is not None:
                        assert obs["group_state"]["weight"] == obs["current_weight"], \
                            f"torn plan snapshot: {obs}"
                    if obs["status"] == "SUCCEEDED":
                        assert obs["current_weight"] == 100
                except AssertionError as e:
                    violations.append(str(e))
                    return

        readers = [threading.Thread(target=reader) for _ in range(3)]
        [t.start() for t in readers]
        svc.run_async(plan.plan_id, step_interval=0.01)
        assert svc.wait_idle()
        stop.set()
        [t.join() for t in readers]

        assert not violations, f"torn plan snapshots: {violations[:3]}"
        assert svc.observe(plan.plan_id)["status"] == "SUCCEEDED"
        log.info("verdict: single-plan observe consistent during async run")
    finally:
        svc.shutdown()
