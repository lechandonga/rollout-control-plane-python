"""多组协同发布：对外快照一致性测试。

覆盖需求中的四类边界：
  1. 整批推进中被反复查询 -> 每次都是推进前/推进后的完整快照；
  2. 整批回滚中被反复查询 -> 看不到半回滚，结束后所有组都在稳定点；
  3. 重复 / 并发触发回滚 -> 无额外副作用，对外快照始终自洽；
  4. 进程中断恢复后查询 -> 全部组一起恢复，无半恢复、无可疑中间态。

所有断言基于同一份 observe_batch 返回内的自洽性：整批 status/
conclusion 必须与同一份明细里的各组进度、组权重对得上。
"""
import json
import logging
import threading
import time

from rollout.config import ControlPlaneConfig
from rollout.models import GroupTarget, HealthMetrics, InstanceGroupState
from rollout.service import RolloutService
from rollout.storage import Storage

log = logging.getLogger("test.batch.consistency")


def make_service(tmp_path, **cfg):
    config = ControlPlaneConfig(storage_path=str(tmp_path / "state.json"), **cfg)
    return RolloutService(Storage(config.storage_path), config)


def healthy(svc, group, error_rate=0.0, unhealthy=0.0):
    svc.report_health(group, HealthMetrics(error_rate=error_rate,
                                           unhealthy_ratio=unhealthy))


def make_batch(svc, groups):
    """groups: [(group, version, steps), ...]"""
    targets = [GroupTarget(group=g, version=v, steps=s) for g, v, s in groups]
    return svc.create_batch_plan(targets)


def group_weights(obs):
    """一份快照内各组的对外权重：优先 group_state，未落任何发布时按 0。"""
    weights = {}
    for g in obs["groups"]:
        if g["group_state"] is None:
            weights[g["group"]] = 0
        else:
            weights[g["group"]] = g["group_state"]["weight"]
    return weights


def assert_snapshot_coherent(obs):
    """断言一份批次快照内部完全自洽。

    核心不变式（任何世代都必须成立）：
      - 组的对外权重 == 明细里的当前步权重（两者来自同一次发布）；
      - SUCCEEDED 当且仅当所有组权重 100，结论与之一致；
      - ROLLED_BACK 时所有组权重等于各自稳定点，结论与之一致；
      - RUNNING/ROLLING_BACK 时不得有组停在 100 以外的"跨代"权重
        （即不允许出现部分组新一代、部分组旧一代的混合），且结论中
        的 done 计数与明细一致。
    """
    status = obs["status"]
    groups = obs["groups"]
    assert len({g["group"] for g in groups}) == len(groups), "组名重复"
    weights = group_weights(obs)
    for g in groups:
        # 明细进度与对外组权重必须对得上（同一份快照、同一世代）
        assert weights[g["group"]] == g["current_weight"], (
            f"status={status} group={g['group']} 对外权重 "
            f"{weights[g['group']]} != 明细当前权重 {g['current_weight']} "
            f"（跨代半截快照）obs={obs}")
    all100 = all(w == 100 for w in weights.values())
    done_count = sum(1 for g in groups if g["current_weight"] == 100)
    if status == "SUCCEEDED":
        assert all100, f"SUCCEEDED 但仍有组未到 100: {weights}"
        assert "100%" in obs["conclusion"]
    else:
        assert not all100, f"{status} 但所有组都已 100，结论自相矛盾: {obs}"
        if status == "ROLLED_BACK":
            for g in groups:
                assert weights[g["group"]] == g["stable_weight"], (
                    f"ROLLED_BACK 但 {g['group']} 权重 "
                    f"{weights[g['group']]} != 稳定点 {g['stable_weight']}")
            assert "restored" in obs["conclusion"]
        elif status in ("RUNNING", "ROLLING_BACK"):
            # 结论里的 done/total 必须与明细一致
            expected = f"{status.lower()}: {done_count}/{len(groups)} groups done"
            assert obs["conclusion"] == expected, (
                f"结论 {obs['conclusion']!r} 与明细进度不一致，期望 {expected!r}")
    return weights


def assert_snapshot_rollback_window(obs):
    """回滚窗口内的快照自洽性（允许带回滚待修复的遗留分歧）。

    要求：
      - 各组对外权重必须整代一致（要么全旧、要么全新），不允许部分组
        已回到稳定点、部分组还停在放量后权重；
      - 明细 current_weight 彼此整代一致，且结论中的 done 计数与明细一致；
      - ROLLED_BACK 世代必须严格自洽（明细==组权重==稳定点）。
    """
    status = obs["status"]
    groups = obs["groups"]
    weights = group_weights(obs)
    detail = {g["group"]: g["current_weight"] for g in groups}
    done_count = sum(1 for g in groups if g["current_weight"] == 100)

    assert len(set(weights.values())) == 1, f"回滚窗口出现半回滚（组间跨代）: {weights}"
    assert len(set(detail.values())) == 1, f"回滚窗口明细跨代: {detail}"

    if status == "ROLLED_BACK":
        # 回滚结束后必须严格自洽，全部回到稳定点
        assert_snapshot_coherent(obs)
    elif status in ("RUNNING", "ROLLING_BACK"):
        expected = f"{status.lower()}: {done_count}/{len(groups)} groups done"
        assert obs["conclusion"] == expected, (
            f"结论 {obs['conclusion']!r} 与明细不一致，期望 {expected!r}")
    return weights


class ConcurrentReaders:
    """在写操作进行期间反复查询批次，收集每份快照并逐份校验自洽性。"""

    def __init__(self, svc, batch_id, n_readers=6, strict=True):
        self.svc = svc
        self.batch_id = batch_id
        self.n_readers = n_readers
        # strict=True：明细权重必须等于对外组权重（推进窗口的保证）；
        # strict=False：允许 ROLLING_BACK 世代带着待修复的遗留分歧，但仍
        # 要求组间整代一致、结论与明细计数自洽（回滚窗口的保证）。
        self.strict = strict
        self.stop = threading.Event()
        self.errors: list[Exception] = []
        self.snapshots: list[dict] = []
        self.weight_vectors: list[tuple] = []
        self._threads: list[threading.Thread] = []

    def _reader(self):
        while not self.stop.is_set():
            try:
                obs = self.svc.observe_batch(self.batch_id)
                weights = (assert_snapshot_coherent(obs) if self.strict
                           else assert_snapshot_rollback_window(obs))
                self.snapshots.append(obs)
                self.weight_vectors.append(tuple(sorted(weights.items())))
            except Exception as e:  # noqa: BLE001
                self.errors.append(e)

    def __enter__(self):
        self._threads = [
            threading.Thread(target=self._reader, name=f"reader-{i}")
            for i in range(self.n_readers)
        ]
        for t in self._threads:
            t.start()
        return self

    def __exit__(self, *exc):
        # 让读者再跑一小会儿，覆盖写操作完成前后的边界
        time.sleep(0.05)
        self.stop.set()
        for t in self._threads:
            t.join()
        return False

    def distinct_vectors(self):
        return sorted(set(self.weight_vectors))


# ---------------------------------------------------------------- 推进中并发查询

def test_concurrent_reads_during_batch_advance_see_only_coherent_snapshots(
        tmp_path, monkeypatch):
    """整批推进的同时反复查询：只能看到推进前或推进后的完整世代。"""
    svc = make_service(tmp_path)
    try:
        spec = [("g1", "v2", [10, 100]),
                ("g2", "v2", [10, 100]),
                ("g3", "v2", [10, 100])]
        batch = make_batch(svc, spec)
        svc.start_batch(batch.batch_id)
        for g, _, _ in spec:
            healthy(svc, g)
        svc.advance_batch(batch.batch_id)  # 先到 (10,10,10)

        # 注入每组构建之间的延迟，把"逐组变更"窗口稳定放大。
        # 修复前：get_group 返回活对象且被原地修改 -> 读者必抓到混合世代；
        # 修复后：只在发布点一次性切换 -> 窗口内也只看得到整代快照。
        orig_group_state_for = svc._group_state_for

        def slow_group_state_for(group, weight, version):
            state = orig_group_state_for(group, weight, version)
            time.sleep(0.02)
            return state

        monkeypatch.setattr(svc, "_group_state_for", slow_group_state_for)

        with ConcurrentReaders(svc, batch.batch_id) as readers:
            time.sleep(0.02)  # 确保读者已开始
            svc.advance_batch(batch.batch_id)  # (10,10,10) -> (100,100,100)

        assert not readers.errors, readers.errors[:3]
        vectors = readers.distinct_vectors()
        log.info("推进期间观测到的权重世代: %s", vectors)
        # 读者必须同时覆盖过推进前与推进后两个世代（证明窗口确实被观测到）
        assert (("g1", 10), ("g2", 10), ("g3", 10)) in vectors
        assert (("g1", 100), ("g2", 100), ("g3", 100)) in vectors
        # 但绝不允许出现任何混合世代（部分 10、部分 100）
        for vec in vectors:
            ws = [w for _, w in vec]
            assert len(set(ws)) == 1, f"半截快照: {vec}"

        obs = svc.observe_batch(batch.batch_id)
        assert obs["status"] == "SUCCEEDED"
        assert_snapshot_coherent(obs)
    finally:
        svc.shutdown()

# ---------------------------------------------------------------- 回滚中并发查询

def test_concurrent_reads_during_batch_rollback_see_only_coherent_snapshots(
        tmp_path, monkeypatch):
    """回滚进行中反复查询：看不到半回滚；结束后所有组都在稳定点。

    先构造一组"权重残留到更高世代"的现场（等价于中断/外部写入留下的
    半更新），再发起整批回滚；读者在回滚进入逐组处理的瞬间才启动，
    只覆盖回滚窗口本身（不评估回滚开始前的遗留现场）。
    """
    svc = make_service(tmp_path)
    try:
        spec = [("g1", "v2", [10, 100]),
                ("g2", "v2", [10, 100]),
                ("g3", "v2", [10, 100])]
        batch = make_batch(svc, spec)
        svc.start_batch(batch.batch_id)
        for g, _, _ in spec:
            healthy(svc, g)
        svc.advance_batch(batch.batch_id)  # (10,10,10)，稳定点=10
        # 遗留半更新现场：组权重已在 100，批次明细仍停在 10
        for g, _, _ in spec:
            svc.storage.save_group(InstanceGroupState(group=g, version="v2",
                                                      weight=100))

        # 回滚逐组处理的窗口：修复前活对象被逐组原地改 -> 读者抓到半回滚；
        # 修复后只在发布点一次切换 -> 读者只看到整代快照。
        orig_group_state_for = svc._group_state_for
        started = threading.Event()

        def slow_group_state_for(group, weight, version):
            started.set()  # 回滚已进入逐组处理：读者此刻才开始
            state = orig_group_state_for(group, weight, version)
            time.sleep(0.03)
            return state

        monkeypatch.setattr(svc, "_group_state_for", slow_group_state_for)

        rollback_errors: list[Exception] = []

        def do_rollback():
            try:
                svc.rollback_batch(batch.batch_id, reason="regression")
            except Exception as e:  # noqa: BLE001
                rollback_errors.append(e)

        writer = threading.Thread(target=do_rollback)
        writer.start()
        assert started.wait(timeout=2.0)  # 确保读者覆盖的是回滚窗口
        with ConcurrentReaders(svc, batch.batch_id, strict=False) as readers:
            writer.join()

        assert not rollback_errors, rollback_errors
        assert not readers.errors, readers.errors[:3]
        vectors = readers.distinct_vectors()
        log.info("回滚窗口内观测到的权重世代: %s", vectors)
        # 窗口内只能看到：整代残留 100，或整代回到稳定点 10；
        # 绝不允许 (100,100,10) 这种半回滚混合。
        for vec in vectors:
            ws = [w for _, w in vec]
            assert len(set(ws)) == 1, f"半回滚快照: {vec}"
        assert (("g1", 10), ("g2", 10), ("g3", 10)) in vectors

        # 请求结束后：所有组都已回到各自最近稳定点
        obs = svc.observe_batch(batch.batch_id)
        assert obs["status"] == "ROLLED_BACK"
        weights = assert_snapshot_coherent(obs)
        assert weights == {"g1": 10, "g2": 10, "g3": 10}
        for g in obs["groups"]:
            assert g["group_state"]["weight"] == g["stable_weight"] == 10
    finally:
        svc.shutdown()


def test_concurrent_reads_during_normal_rollback_strictly_coherent(
        tmp_path, monkeypatch):
    """无遗留分歧时回滚：即便在回滚窗口内，严格档（明细==组权重）也始终成立。"""
    svc = make_service(tmp_path)
    try:
        # 三步步长，回滚发起时整批停在中间世代 50（稳定点=50）
        spec2 = [("h1", "v2", [10, 50, 100]),
                 ("h2", "v2", [10, 50, 100]),
                 ("h3", "v2", [10, 50, 100])]
        batch2 = make_batch(svc, spec2)
        svc.start_batch(batch2.batch_id)
        for g, _, _ in spec2:
            healthy(svc, g)
        svc.advance_batch(batch2.batch_id)  # (10,10,10)
        svc.advance_batch(batch2.batch_id)  # (50,50,50)，稳定点=50

        orig_group_state_for = svc._group_state_for
        started = threading.Event()

        def slow_group_state_for(group, weight, version):
            started.set()
            state = orig_group_state_for(group, weight, version)
            time.sleep(0.03)
            return state

        monkeypatch.setattr(svc, "_group_state_for", slow_group_state_for)

        def do_rollback():
            svc.rollback_batch(batch2.batch_id, reason="alarm")

        writer = threading.Thread(target=do_rollback)
        writer.start()
        assert started.wait(timeout=2.0)
        with ConcurrentReaders(svc, batch2.batch_id, strict=True) as readers:
            writer.join()

        assert not readers.errors, readers.errors[:3]
        vectors = readers.distinct_vectors()
        log.info("常规回滚窗口内观测到的权重世代: %s", vectors)
        assert (("h1", 50), ("h2", 50), ("h3", 50)) in vectors
        obs = svc.observe_batch(batch2.batch_id)
        assert obs["status"] == "ROLLED_BACK"
        assert group_weights(obs) == {"h1": 50, "h2": 50, "h3": 50}
    finally:
        svc.shutdown()


# ---------------------------------------------------------------- 重复与并发回滚

def test_repeated_and_concurrent_batch_rollback_no_extra_side_effects(tmp_path):
    """重复、并发触发整批回滚：无额外副作用，每份对外快照都自洽。"""
    svc = make_service(tmp_path)
    spec = [("g1", "v2", [10, 50, 100]), ("g2", "v2", [30, 100])]
    batch = make_batch(svc, spec)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # g1=10, g2=30
    svc.advance_batch(batch.batch_id)  # g1=50, g2=100（稳定点）

    first = svc.rollback_batch(batch.batch_id, reason="regression")
    assert first.status.value == "ROLLED_BACK"
    stable = {"g1": 50, "g2": 100}

    # 重复触发：no-op，failure_reason 不被覆盖，权重不动
    again = svc.rollback_batch(batch.batch_id, reason="duplicate")
    assert again.status.value == "ROLLED_BACK"
    assert again.failure_reason == "regression"
    assert_snapshot_coherent(svc.observe_batch(batch.batch_id))

    # 并发重复触发：全部成功且等价于串行，无额外副作用
    barrier = threading.Barrier(8)
    results, errors = [], []

    def worker():
        barrier.wait()
        try:
            rb = svc.rollback_batch(batch.batch_id, reason="concurrent")
            results.append(rb.status.value)
            assert_snapshot_coherent(svc.observe_batch(batch.batch_id))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    assert results == ["ROLLED_BACK"] * 8
    assert svc.storage.get_group("g1").weight == stable["g1"]
    assert svc.storage.get_group("g2").weight == stable["g2"]
    obs = svc.observe_batch(batch.batch_id)
    assert group_weights(obs) == stable
    assert_snapshot_coherent(obs)
    log.info("重复+8 线程并发回滚后：%s, weights=%s", obs["status"], stable)


def test_concurrent_rollback_from_running_all_finish_at_stable(tmp_path):
    """从 RUNNING 同时发起多笔回滚：每个调用返回时整批都已完整回滚。"""
    svc = make_service(tmp_path)
    spec = [("g1", "v2", [10, 100]), ("g2", "v2", [20, 100])]
    batch = make_batch(svc, spec)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)  # 稳定点 g1=10, g2=20

    barrier = threading.Barrier(6)
    rets, errors = [], []

    def worker():
        barrier.wait()
        try:
            rb = svc.rollback_batch(batch.batch_id, reason="race")
            # 每个调用自己的返回也必须是自洽完整快照
            assert rb.status.value == "ROLLED_BACK"
            for t in rb.targets:
                assert t.step_index == t.stable.step_index
            rets.append(rb)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    assert len(rets) == 6
    obs = svc.observe_batch(batch.batch_id)
    assert group_weights(obs) == {"g1": 10, "g2": 20}
    assert_snapshot_coherent(obs)

# ---------------------------------------------------------------- 后台放量中并发查询

def test_concurrent_reads_during_background_batch_run(tmp_path, monkeypatch):
    """后台自动整批放量在跑的同时反复查询：每份快照都是完整世代。"""
    svc = make_service(tmp_path)
    try:
        spec = [("g1", "v2", [10, 40, 70, 100]),
                ("g2", "v2", [20, 60, 100]),
                ("g3", "v2", [30, 100])]
        batch = make_batch(svc, spec)
        svc.start_batch(batch.batch_id)
        for g, _, _ in spec:
            healthy(svc, g)

        # 放大每一步发布之间的逐组构建窗口，给并发读者充分机会
        orig_group_state_for = svc._group_state_for

        def slow_group_state_for(group, weight, version):
            state = orig_group_state_for(group, weight, version)
            time.sleep(0.01)
            return state

        monkeypatch.setattr(svc, "_group_state_for", slow_group_state_for)

        readers = ConcurrentReaders(svc, batch.batch_id, n_readers=4)
        readers.__enter__()
        try:
            svc.run_batch_async(batch.batch_id, step_interval=0.01)
            assert svc.wait_idle(timeout=10.0)
        finally:
            readers.__exit__(None, None, None)

        assert not readers.errors, readers.errors[:3]
        vectors = readers.distinct_vectors()
        log.info("后台放量期间观测到 %d 份快照、%d 个权重世代: %s",
                 len(readers.snapshots), len(vectors), vectors)
        # 每个世代内三组权重必须各自匹配其明细（assert_snapshot_coherent 已
        # 在读者线程逐份校验）；这里再确保没有任何"整代矛盾"的结论。
        assert len(readers.snapshots) > 10
        final = svc.observe_batch(batch.batch_id)
        assert final["status"] == "SUCCEEDED"
        assert group_weights(final) == {"g1": 100, "g2": 100, "g3": 100}
        assert_snapshot_coherent(final)
    finally:
        svc.shutdown()


def test_concurrent_reads_during_background_batch_auto_rollback(tmp_path):
    """后台放量门禁超时触发整批自动回滚期间，快照同样只可能整代一致。"""
    svc = make_service(tmp_path, step_timeout_seconds=0.2)
    try:
        spec = [("g1", "v2", [10, 100]), ("g2", "v2", [10, 100])]
        batch = make_batch(svc, spec)
        svc.start_batch(batch.batch_id)
        healthy(svc, "g1")
        healthy(svc, "g2", error_rate=0.9)  # g2 永不放行 -> 超时整批回滚
        readers = ConcurrentReaders(svc, batch.batch_id, n_readers=4,
                                    strict=False)
        readers.__enter__()
        try:
            svc.run_batch_async(batch.batch_id, step_interval=0.02)
            assert svc.wait_idle(timeout=10.0)
        finally:
            readers.__exit__(None, None, None)
        assert not readers.errors, readers.errors[:3]
        final = svc.observe_batch(batch.batch_id)
        assert final["status"] == "ROLLED_BACK"
        assert final["failure_reason"] == "gate_timeout"
        assert group_weights(final) == {"g1": 0, "g2": 0}
        assert_snapshot_coherent(final)
    finally:
        svc.shutdown()


# ---------------------------------------------------------------- 中断恢复后查询

def test_repair_after_crash_is_all_or_nothing_and_snapshots_coherent(tmp_path):
    """RUNNING 中断遗留多个半更新组：恢复必须整批一次性拉回，无半恢复。"""
    svc = make_service(tmp_path)
    spec = [("g1", "v2", [10, 50, 100]),
            ("g2", "v2", [10, 100]),
            ("g3", "v2", [10, 50, 100]),
            ("g4", "v2", [20, 100])]
    batch = make_batch(svc, spec)
    svc.start_batch(batch.batch_id)
    for g, _, _ in spec:
        healthy(svc, g)
    svc.advance_batch(batch.batch_id)  # 稳定点：g1/g2/g3=10, g4=20
    stable = {"g1": 10, "g2": 10, "g3": 10, "g4": 20}
    # 崩溃现场：多组被部分更新到更高权重，批次明细仍停在稳定点世代
    svc.storage.save_group(InstanceGroupState(group="g1", version="v2", weight=50))
    svc.storage.save_group(InstanceGroupState(group="g3", version="v2", weight=100))
    svc.storage.save_group(InstanceGroupState(group="g4", version="v2", weight=100))

    # 新进程恢复
    svc2 = make_service(tmp_path)
    svc2.recover()
    restored = svc2.get_batch(batch.batch_id)
    assert restored.status.value == "RUNNING"
    obs = svc2.observe_batch(batch.batch_id)
    weights = assert_snapshot_coherent(obs)
    assert weights == stable  # 所有组（不只是偏离组）一起回到一致世代
    log.info("恢复后快照: status=%s weights=%s", obs["status"], weights)

    # 磁盘上的持久化快照也必须整批一致（重新加载再验一次）
    svc3 = make_service(tmp_path)
    svc3.storage.load()
    reloaded = svc3.observe_batch(batch.batch_id)
    assert group_weights(reloaded) == stable
    assert_snapshot_coherent(reloaded)

    # 恢复幂等：再恢复一次不产生新变化，之后批次可继续推进
    svc3.recover()
    assert group_weights(svc3.observe_batch(batch.batch_id)) == stable
    svc3.advance_batch(batch.batch_id)
    after = svc3.observe_batch(batch.batch_id)
    assert_snapshot_coherent(after)
    assert after["groups"]  # 明细仍完整
    log.info("恢复后继续推进: %s", group_weights(after))


def test_recovery_finishes_interrupted_rollback_for_all_groups(tmp_path):
    """ROLLING_BACK 中断：恢复把所有组一起完成回滚，不留半回滚。"""
    svc = make_service(tmp_path)
    spec = [("g1", "v2", [10, 50, 100]),
            ("g2", "v2", [30, 100]),
            ("g3", "v2", [10, 100])]
    batch = make_batch(svc, spec)
    svc.start_batch(batch.batch_id)
    for g, _, _ in spec:
        healthy(svc, g)
    svc.advance_batch(batch.batch_id)  # g1=10,g2=30,g3=10
    svc.advance_batch(batch.batch_id)  # g1=50,g2=100(done),g3=100(done)
    stable = {"g1": 50, "g2": 100, "g3": 100}
    # 回滚进行到一半崩溃：状态停在 ROLLING_BACK；组权重停在上一世代
    crash_batch = svc.get_batch(batch.batch_id)
    svc._transition(crash_batch, "rollback")
    crash_batch.failure_reason = "crash"
    svc.storage.save_batch(crash_batch)

    svc2 = make_service(tmp_path)
    svc2.recover()
    obs = svc2.observe_batch(batch.batch_id)
    assert obs["status"] == "ROLLED_BACK"
    assert obs["failure_reason"] == "crash"
    weights = assert_snapshot_coherent(obs)
    assert weights == stable
    # 不能只剩一半组被恢复：目标数与组状态数齐整，且全部等于稳定点
    assert len(obs["groups"]) == 3
    for g in obs["groups"]:
        assert g["group_state"]["weight"] == g["stable_weight"]

    # 恢复后再查多次，结论与明细始终自洽
    for _ in range(5):
        assert_snapshot_coherent(svc2.observe_batch(batch.batch_id))
    log.info("中断回滚恢复完成: weights=%s", weights)


def test_recovered_snapshot_matches_disk_file_exactly(tmp_path):
    """恢复落盘后的 JSON 文件本身就必须是整批一致的完整快照。"""
    svc = make_service(tmp_path)
    spec = [("g1", "v2", [10, 100]), ("g2", "v2", [10, 100])]
    batch = make_batch(svc, spec)
    svc.start_batch(batch.batch_id)
    healthy(svc, "g1")
    healthy(svc, "g2")
    svc.advance_batch(batch.batch_id)
    svc.storage.save_group(InstanceGroupState(group="g1", version="v2", weight=100))

    svc2 = make_service(tmp_path)
    svc2.recover()
    with open(svc2.config.storage_path, encoding="utf-8") as f:
        raw = json.load(f)
    groups_on_disk = {g["group"]: g["weight"] for g in raw["groups"]}
    batch_on_disk = raw["batches"][0]
    detail = {t["group"]: t["steps"][t["step_index"]]
              for t in batch_on_disk["targets"]}
    assert groups_on_disk == detail == {"g1": 10, "g2": 10}
    assert batch_on_disk["status"] == "RUNNING"
    log.info("磁盘快照一致: %s / %s", groups_on_disk, detail)
