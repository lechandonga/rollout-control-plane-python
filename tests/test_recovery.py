"""Process restart recovery and persisted-format evolution."""
import json

from rollout.errors import IncompatibleData
from rollout.models import PlanState
from rollout.store import Store
from conftest import log_case, log_verdict, make_manager


def test_restart_after_durable_step(tmp_path):
    path = str(tmp_path / "s.json")
    m1 = make_manager(path, max_step_weight=25)
    m1.create_plan("x1", "app", "v2", "canary", step_weight=25)
    m1.promote("x1")  # durable stable@25; transient state promoting

    # Fresh process: construct manager from the same file.
    m2 = make_manager(path, max_step_weight=25)
    assert m2.status("x1")["state"] == PlanState.PROMOTING.value
    report = m2.recover()
    st = m2.status("x1")
    log_case("restart_recovery", report=report)
    log_verdict(state=st["state"], weight=st["weight"])
    assert report == [{"plan_id": "x1", "recovered_to": "paused"}]
    assert st["state"] == PlanState.PAUSED.value
    assert st["weight"] == 25
    # must pass the held gate again before increasing -> no jump
    m2.promote("x1")
    assert m2.status("x1")["weight"] == 25
    m2.promote("x1")
    assert m2.status("x1")["weight"] == 50


def test_restart_preserves_plan_metadata(tmp_path):
    path = str(tmp_path / "s.json")
    m1 = make_manager(path, max_step_weight=25)
    m1.create_plan("x2", "app", "v7", "canary", step_weight=25,
                   from_version="v6")
    m1.promote("x2")
    m2 = make_manager(path, max_step_weight=25)
    p = m2.get_plan("x2")
    assert p.version == "v7" and p.from_version == "v6"
    assert p.policy.step_weight == 25 and p.target_group == "canary"
    assert len(p.stable_points) == 2


def test_no_half_update_when_write_is_atomic(tmp_path):
    path = str(tmp_path / "s.json")
    m = make_manager(path, max_step_weight=25)
    m.create_plan("x3", "app", "v2", "canary", step_weight=25)
    m.promote("x3")
    # The document on disk must parse and contain the full plan record.
    doc = json.load(open(path))
    assert doc["format"] == "rollout-store"
    rec = doc["plans"]["x3"]
    assert rec["weight"] == 25 and rec["groups"] and rec["stable_points"]
    # no temp files left behind
    leftovers = [f for f in tmp_path.iterdir() if f.name.startswith(".rollout-")]
    assert leftovers == []


def test_legacy_v1_missing_fields_get_stable_defaults(tmp_path):
    path = str(tmp_path / "s.json")
    v1 = {
        "format": "rollout-store",
        "schema_version": 1,
        "plans": {
            "old": {"plan_id": "old", "app": "a", "version": "v1",
                    "target_group": "g"},
        },
    }
    json.dump(v1, open(path, "w"))
    plans = Store(path).load_all()
    p = plans["old"]
    log_case("v1_migration", raw=v1["plans"]["old"])
    log_verdict(reason=p.reason, weight=p.weight,
                step=p.policy.step_weight,
                counter=p.version_counter, state=p.state.value)
    assert p.weight == 0 and p.reason == ""
    assert p.policy.step_weight == 10  # documented default
    assert p.version_counter == 0
    assert p.state is PlanState.PENDING


def test_future_schema_is_refused(tmp_path):
    path = str(tmp_path / "s.json")
    json.dump({"format": "rollout-store", "schema_version": 99,
               "plans": {}}, open(path, "w"))
    log_case("future_schema", schema_version=99)
    try:
        Store(path).load_all()
        raise AssertionError("must refuse")
    except IncompatibleData as e:
        log_verdict(refused=str(e))
        assert "99" in str(e)


def test_missing_required_field_is_refused(tmp_path):
    path = str(tmp_path / "s.json")
    json.dump({"format": "rollout-store", "schema_version": 2,
               "plans": {"bad": {"plan_id": "bad"}}}, open(path, "w"))
    try:
        Store(path).load_all()
        raise AssertionError("must refuse")
    except IncompatibleData as e:
        log_case("missing_required", error=str(e))
        assert "version" in str(e) and "target_group" in str(e)


def test_corrupt_file_is_refused_not_defaulted(tmp_path):
    path = str(tmp_path / "s.json")
    open(path, "w").write("{broken json")
    try:
        Store(path).load_all()
        raise AssertionError("must refuse")
    except IncompatibleData as e:
        log_verdict(refused=str(e))
