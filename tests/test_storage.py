"""持久化测试：原子写、格式演进缺省、不兼容明确拒绝。"""
import json
import logging

import pytest

from rollout.errors import StorageCorruptError
from rollout.models import GatePolicy, RolloutPlan
from rollout.storage import Storage

log = logging.getLogger("test.storage")


def test_roundtrip(tmp_path):
    path = str(tmp_path / "state.json")
    s = Storage(path)
    plan = RolloutPlan.new("v2", "g1", [10, 100], GatePolicy())
    s.save_plan(plan)
    log.info("input: plan=%s", plan)
    s2 = Storage(path)
    s2.load()
    got = s2.get_plan(plan.plan_id)
    assert got is not None and got.version == "v2" and got.steps == [10, 100]
    log.info("verdict: roundtrip ok, plan_id=%s", got.plan_id)


def test_v1_format_defaults(tmp_path):
    """v1 缺少 gate/stable/failure_reason，加载后获得确定缺省。"""
    path = tmp_path / "state.json"
    path.write_text(json.dumps({
        "format_version": 1,
        "plans": [{
            "plan_id": "p1", "version": "v2", "group": "g1",
            "steps": [50, 100], "status": "PENDING",
        }],
    }))
    s = Storage(str(path))
    s.load()
    p = s.get_plan("p1")
    assert p.gate.max_error_rate == 0.05
    assert p.stable.weight == 0
    assert p.failure_reason is None
    log.info("verdict: v1 defaults applied, gate=%s stable=%s", p.gate, p.stable)


def test_unsupported_version_rejected(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"format_version": 99, "plans": []}))
    s = Storage(str(path))
    with pytest.raises(StorageCorruptError) as ei:
        s.load()
    log.info("input: format_version=99; verdict: rejected reason=%s", ei.value.reason)


def test_corrupt_json_rejected(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json")
    with pytest.raises(StorageCorruptError):
        Storage(str(path)).load()
