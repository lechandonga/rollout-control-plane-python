"""HTTP 接口测试：端到端流程与错误码可区分。"""
import importlib
import logging
import os

import pytest

log = logging.getLogger("test.api")


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("ROLLOUT_STORAGE", str(tmp_path / "api_state.json"))
    import main
    importlib.reload(main)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        yield c


def test_full_rollout_flow(client):
    r = client.post("/plans", json={"version": "v2", "group": "g1",
                                    "steps": [10, 100]})
    assert r.status_code == 201
    plan_id = r.json()["plan_id"]
    log.info("input: create plan -> %s", r.json())

    assert client.post(f"/plans/{plan_id}/start").status_code == 200
    # 未上报健康 -> 409 gate_blocked
    r = client.post(f"/plans/{plan_id}/advance")
    assert r.status_code == 409 and r.json()["error"] == "gate_blocked"
    log.info("verdict: advance without metrics -> %s", r.json())

    client.post("/groups/g1/health", json={"error_rate": 0.0})
    assert client.post(f"/plans/{plan_id}/advance").status_code == 200
    r = client.post(f"/plans/{plan_id}/advance")
    assert r.json()["status"] == "SUCCEEDED"
    # 终态后推进 -> 409 illegal_transition
    r = client.post(f"/plans/{plan_id}/advance")
    assert r.status_code == 409 and r.json()["error"] == "illegal_transition"
    log.info("verdict: advance after SUCCEEDED -> %s", r.json())


def test_error_codes_distinct(client):
    assert client.get("/plans/nope").status_code == 404
    r = client.post("/plans", json={"version": "v2", "group": "g1",
                                    "steps": [50, 40]})
    assert r.status_code == 422 and r.json()["error"] == "validation_error"
    log.info("verdict: bad steps -> %s", r.json())


def test_rollback_via_api(client):
    r = client.post("/plans", json={"version": "v2", "group": "g1",
                                    "steps": [10, 100]})
    plan_id = r.json()["plan_id"]
    client.post(f"/plans/{plan_id}/start")
    client.post("/groups/g1/health", json={})
    client.post(f"/plans/{plan_id}/advance")
    r = client.post(f"/plans/{plan_id}/rollback", json={"reason": "alarm"})
    body = r.json()
    assert body["status"] == "ROLLED_BACK"
    assert body["group_state"]["weight"] == body["stable_weight"]
    assert body["failure_reason"] == "alarm"
    log.info("verdict: rollback -> %s", body)
