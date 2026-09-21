"""Step timeout, concurrency/queue limits and resource cleanup."""
import threading
import time

import pytest

from rollout.errors import LimitExceeded, QueueTimeout
from rollout.health import HealthGate
from rollout.models import PlanState
from conftest import log_case, log_verdict, make_manager


def test_gate_timeout_holds_weight_without_partial_state(tmp_path):
    def slow(a, v, w):
        time.sleep(1.0)
        return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 1.0}

    m = make_manager(str(tmp_path / "s.json"),
                     gate=HealthGate(provider=slow),
                     max_step_weight=25, step_timeout_s=0.2)
    m.create_plan("t1", "app", "v2", "canary", step_weight=25)
    t0 = time.time()
    p = m.promote("t1")
    elapsed = time.time() - t0
    st = m.status("t1")
    log_case("gate_timeout", timeout_s=0.2, elapsed=round(elapsed, 2))
    log_verdict(state=st["state"], weight=st["weight"], reason=st["reason"])
    # weight advance is durable, but plan is held paused pending a re-check
    assert p.state is PlanState.PAUSED
    assert st["weight"] == 25
    assert "timed out" in st["reason"]
    # scheduler slot was released even on the timeout path
    assert m._scheduler.active_count() == 0


def test_concurrency_reject_policy(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    def blocking(a, v, w):
        entered.set()
        release.wait(timeout=5)
        return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 1.0}

    m = make_manager(str(tmp_path / "s.json"),
                     gate=HealthGate(provider=blocking),
                     max_concurrent_rollouts=1, on_limit="reject")
    m.create_plan("t2", "app", "v2", "canary", step_weight=25)
    th = threading.Thread(target=m.promote, args=("t2",))
    th.start()
    entered.wait(timeout=2)
    m.create_plan("t3", "app", "v3", "canary", step_weight=25)
    log_case("concurrency_reject", active=m._scheduler.active_count())
    with pytest.raises(LimitExceeded) as ei:
        m.promote("t3")
    log_verdict(code=ei.value.code, kind=ei.value.kind, limit=ei.value.limit)
    assert ei.value.kind == "concurrent_rollouts"
    release.set(); th.join()


def test_queue_timeout(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    def blocking(a, v, w):
        entered.set()
        release.wait(timeout=5)
        return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 1.0}

    m = make_manager(str(tmp_path / "s.json"),
                     gate=HealthGate(provider=blocking),
                     max_concurrent_rollouts=1, on_limit="queue",
                     queue_timeout_s=0.2)
    m.create_plan("t4", "app", "v2", "canary", step_weight=25)
    m.create_plan("t4b", "app", "v3", "canary", step_weight=25)
    th = threading.Thread(target=m.promote, args=("t4",))
    th.start()
    entered.wait(timeout=2)
    log_case("queue_timeout", queue_timeout_s=0.2)
    with pytest.raises(QueueTimeout):
        m.promote("t4b")
    # no queued task remains and slot is intact
    assert m._scheduler.queued_count() == 0
    release.set(); th.join()
    assert m._scheduler.active_count() == 0


def test_queue_depth_limit(tmp_path):
    entered = threading.Event()
    release = threading.Event()

    def blocking(a, v, w):
        entered.set()
        release.wait(timeout=5)
        return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 1.0}

    m = make_manager(str(tmp_path / "s.json"),
                     gate=HealthGate(provider=blocking),
                     max_concurrent_rollouts=1, on_limit="queue",
                     queue_max=1, queue_timeout_s=5)
    m.create_plan("t5", "app", "v2", "canary", step_weight=25)
    m.create_plan("t5b", "app", "v3", "canary", step_weight=25)
    m.create_plan("t5c", "app", "v4", "canary", step_weight=25)
    th = threading.Thread(target=m.promote, args=("t5",))
    th.start()
    entered.wait(timeout=2)
    queued_th = threading.Thread(target=lambda: _swallow(m.promote, "t5b"))
    queued_th.start()
    time.sleep(0.1)  # let it enter the queue
    log_case("queue_depth", queued=m._scheduler.queued_count(), queue_max=1)
    with pytest.raises(LimitExceeded) as ei:
        m.promote("t5c")  # third contender: queue full
    log_verdict(kind=ei.value.kind, actual=ei.value.actual)
    assert ei.value.kind == "queue_depth"
    release.set(); th.join(); queued_th.join()
    assert m._scheduler.active_count() == 0
    assert m._scheduler.queued_count() == 0


def test_step_weight_bounds_enforced(tmp_path):
    m = make_manager(str(tmp_path / "s.json"), max_step_weight=25)
    with pytest.raises(LimitExceeded) as ei:
        m.create_plan("t6", "app", "v2", "canary", step_weight=50)
    log_case("step_too_large", requested=50, cap=25)
    log_verdict(kind=ei.value.kind, limit=ei.value.limit)
    assert ei.value.kind == "step_weight_max"


def test_shutdown_drains_without_leaked_threads(tmp_path):
    m = make_manager(str(tmp_path / "s.json"), max_concurrent_rollouts=2)
    m.create_plan("t7", "app", "v2", "canary", step_weight=25)
    m.promote("t7")
    m.shutdown(timeout=2)
    # after shutdown, acquiring is rejected rather than hanging
    with pytest.raises(LimitExceeded) as ei:
        m.promote("t7")
    assert ei.value.kind == "scheduler_closed"


def _swallow(fn, *args):
    try:
        fn(*args)
    except Exception:
        pass
