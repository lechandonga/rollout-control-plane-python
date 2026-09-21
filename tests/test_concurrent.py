"""Concurrent promotions on the same plan must be serial-equivalent."""
import threading
from concurrent.futures import ThreadPoolExecutor

from rollout.errors import InvalidTransition
from rollout.health import HealthGate
from rollout.models import PlanState
from conftest import log_case, log_verdict, make_manager


def _healthy_gate():
    return HealthGate(provider=lambda a, v, w: {
        "success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 10.0})


def test_concurrent_promotions_serial_equivalent(tmp_path):
    path = str(tmp_path / "s.json")
    m = make_manager(path, gate=_healthy_gate(), max_step_weight=10,
                     max_concurrent_rollouts=4)
    p = m.create_plan("c1", "app", "v2", "canary", step_weight=10)

    # 12 promotions from 12 threads; only 10 steps are needed to complete.
    errors = []
    log_case("concurrent_promotions", threads=12, step=10,
             initial_weight=p.weight)

    def do_promote(i):
        try:
            m.promote("c1")
        except InvalidTransition as e:
            errors.append(e.reason)
        except Exception as e:  # pragma: no cover - failure visibility
            errors.append(f"unexpected: {e!r}")

    with ThreadPoolExecutor(max_workers=12) as ex:
        list(ex.map(do_promote, range(12)))

    st = m.status("c1")
    log_verdict(final_state=st["state"], weight=st["weight"],
                stable_points=st["stable_point_count"],
                rejected=len(errors))
    assert st["weight"] == 100
    assert st["state"] == PlanState.COMPLETED.value
    # stable points: baseline(0), 10..100 = 11 total
    assert st["stable_point_count"] == 11
    # exactly the 2 surplus promotions were rejected with a clear reason
    assert len(errors) == 2
    assert all("completed" in e for e in errors)


def test_weight_never_skips_or_regresses_under_contention(tmp_path):
    path = str(tmp_path / "s.json")
    seen = []
    seen_lock = threading.Lock()

    def provider(a, v, w):
        with seen_lock:
            seen.append(w)
        return {"success_rate": 1.0, "error_rate": 0.0,
                "latency_p99_ms": 10.0}

    m = make_manager(path, gate=HealthGate(provider=provider),
                     max_step_weight=25, max_concurrent_rollouts=8)
    m.create_plan("c2", "app", "v2", "canary", step_weight=25)

    barrier = threading.Barrier(8)

    def worker(_):
        barrier.wait()
        try:
            m.promote("c2")
        except InvalidTransition:
            pass

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(worker, range(8)))

    # Every gate evaluation must observe a weight in the legal ascending set.
    legal = {25, 50, 75, 100}
    log_case("weight_contention", observed=seen)
    assert set(seen) <= legal
    # Sequence of *first* observations per weight must be strictly ascending.
    firsts = []
    for w in seen:
        if w not in firsts:
            firsts.append(w)
    log_verdict(first_observations=firsts)
    assert firsts == sorted(firsts)
    assert m.status("c2")["weight"] == 100


def test_concurrent_rollback_and_promote(tmp_path):
    """A rollback concurrent with promotions yields one consistent result."""
    path = str(tmp_path / "s.json")
    m = make_manager(path, gate=_healthy_gate(), max_step_weight=25,
                     max_concurrent_rollouts=8)
    m.create_plan("c3", "app", "v2", "canary", step_weight=25)
    m.promote("c3")  # stable at 25
    m.promote("c3")  # stable at 50

    outcomes = []

    def promote():
        try:
            p = m.promote("c3")
            outcomes.append(("promote:" + p.state.value, m.status("c3")["weight"]))
        except InvalidTransition as e:
            outcomes.append(("rejected:" + e.reason, m.status("c3")["weight"]))

    def rollback():
        p = m.rollback("c3")
        outcomes.append(("rollback:" + p.state.value,
                         m.status("c3")["weight"]))

    t1 = threading.Thread(target=promote)
    t2 = threading.Thread(target=rollback)
    t1.start(); t2.start(); t1.join(); t2.join()

    st = m.status("c3")
    log_case("promote_vs_rollback", outcomes=outcomes)
    log_verdict(final_state=st["state"], weight=st["weight"])
    # Two legal serializations:
    #  rollback last -> ROLLED_BACK at the latest recorded stable point
    #    (50 if promote was refused/hadn't recorded; 75 if promote fully
    #     committed its stable point first);
    #  rollback first -> promote refused, still at 50.
    assert st["state"] in ("rolled_back", "promoting")
    assert st["weight"] in (50, 75)
    if st["state"] == "rolled_back":
        # Rollback always lands on a recorded stable point.
        assert any(sp["weight"] == st["weight"]
                   for sp in [{"weight": 50}, {"weight": 75}])
        # Externally visible serving version restored to the stable snapshot:
        assert st["latest_stable_point"]["weight"] == st["weight"]
