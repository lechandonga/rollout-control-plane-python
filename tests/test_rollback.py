"""Auto rollback, idempotent re-entry and manual abort semantics."""
import pytest

from rollout.errors import InvalidTransition
from rollout.health import HealthGate
from rollout.models import GateStatus, PlanState
from conftest import log_case, log_verdict, make_manager


def _gate_seq(seq):
    it = iter(seq)
    return HealthGate(provider=lambda a, v, w: next(it))


_HEALTHY = {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 10.0}


def test_manual_rollback_restores_last_stable_point(tmp_path):
    m = make_manager(str(tmp_path / "s.json"), gate=_gate_seq([_HEALTHY, _HEALTHY]),
                     max_step_weight=25)
    m.create_plan("r1", "app", "v2", "canary", step_weight=25)
    m.promote("r1")  # stable@25
    m.promote("r1")  # stable@50
    before = m.status("r1")
    assert before["weight"] == 50 and before["serving_version"] == "v2"

    p = m.rollback("r1")
    after = m.status("r1")
    log_case("manual_rollback", weight_before=50)
    log_verdict(state=after["state"], weight=after["weight"],
                serving=after["serving_version"],
                canary_statuses=[i["status"]
                                 for i in after["groups"]["canary"]["instances"]])
    assert p.state is PlanState.ROLLED_BACK
    assert after["weight"] == 50  # latest stable point
    assert after["serving_version"] == "v2"
    assert after["latest_stable_point"]["weight"] == 50


def test_rollback_to_baseline_when_first_step_fails_critically(tmp_path):
    gate = _gate_seq([
        {"success_rate": 0.05, "error_rate": 0.95, "latency_p99_ms": 10}])
    m = make_manager(str(tmp_path / "s.json"), gate=gate, max_step_weight=25)
    m.create_plan("r2", "app", "v9", "canary", step_weight=25)
    p = m.promote("r2")  # critical at 25 -> auto rollback to baseline
    st = m.status("r2")
    log_case("critical_first_step", observed_weight=25)
    log_verdict(state=st["state"], weight=st["weight"],
                serving=st["serving_version"], reason=st["reason"])
    assert p.state is PlanState.ROLLED_BACK
    assert st["weight"] == 0
    assert st["serving_version"] == "v9~baseline"
    # baseline instances are ready again
    assert all(i["status"] == "ready"
               for i in st["groups"]["stable"]["instances"])


def test_rollback_reentry_has_no_extra_side_effects(tmp_path):
    gate = _gate_seq([
        {"success_rate": 0.1, "error_rate": 0.9, "latency_p99_ms": 10}])
    m = make_manager(str(tmp_path / "s.json"), gate=gate)
    m.create_plan("r3", "app", "v2", "canary", step_weight=10)
    m.promote("r3")  # auto rollback
    st1 = m.status("r3")
    events_before = len(m.events("r3"))
    log_case("rollback_reentry", state=st1["state"], weight=st1["weight"],
             counter=st1["version_counter"])

    p2 = m.rollback("r3")
    p3 = m.rollback("r3")
    st2 = m.status("r3")
    log_verdict(counter=st2["version_counter"], weight=st2["weight"],
                extra_events=len(m.events("r3")) - events_before)
    assert st2["state"] == PlanState.ROLLED_BACK.value
    assert st2["weight"] == 0
    assert p2 is not None and p3 is not None
    # no extra persisted mutation
    assert st2["version_counter"] == st1["version_counter"]
    # only the two no-op observability events were appended
    assert len(m.events("r3")) - events_before == 2


def test_non_critical_gate_failure_holds_without_rollback(tmp_path):
    gate = _gate_seq([
        _HEALTHY,
        {"success_rate": 0.9, "error_rate": 0.1, "latency_p99_ms": 50},
    ])
    m = make_manager(str(tmp_path / "s.json"), gate=gate, max_step_weight=25)
    m.create_plan("r4", "app", "v2", "canary", step_weight=25)
    m.promote("r4")  # 25 healthy
    m.promote("r4")  # 50 blocked (not critical)
    st = m.status("r4")
    log_case("non_critical_block", evidence=st["last_evidence"])
    log_verdict(state=st["state"], weight=st["weight"],
                stable_points=st["stable_point_count"])
    assert st["state"] == PlanState.PAUSED.value
    assert st["weight"] == 50
    # blocked weight did NOT become a stable point (still baseline+25 = 2)
    assert st["stable_point_count"] == 2
    assert st["last_evidence"]["failed_checks"]  # reason is interpretable


def test_manual_abort_is_terminal_and_explained(tmp_path):
    m = make_manager(str(tmp_path / "s.json"), max_step_weight=25)
    m.create_plan("r5", "app", "v2", "canary", step_weight=25)
    m.promote("r5")
    m.abort("r5", reason="stop the line")
    st = m.status("r5")
    assert st["state"] == PlanState.ABORTED.value
    assert st["weight"] == 25  # externally visible state frozen, not rewound
    with pytest.raises(InvalidTransition) as ei:
        m.promote("r5")
    log_case("abort_then_promote", reason=st["reason"])
    log_verdict(reject_reason=ei.value.reason)
    assert "aborted" in ei.value.reason
