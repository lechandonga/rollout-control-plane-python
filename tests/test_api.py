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


def test_batch_flow_via_api(client):
    r = client.post("/batches", json={"members": [
        {"group": "g1", "version": "v2", "steps": [10, 100]},
        {"group": "g2", "version": "v3", "steps": [20, 100]},
    ]})
    assert r.status_code == 201
    batch_id = r.json()["batch_id"]
    log.info("input: create batch -> %s", r.json())

    assert client.post(f"/batches/{batch_id}/start").status_code == 200
    client.post("/groups/g1/health", json={})
    client.post("/groups/g2/health", json={"error_rate": 0.9})
    # g2 门禁挡下 -> 整批 409，且 g1 未独自扩量
    r = client.post(f"/batches/{batch_id}/advance")
    assert r.status_code == 409 and r.json()["error"] == "gate_blocked"
    body = client.get(f"/batches/{batch_id}").json()
    assert body["overall_verdict"] == "BLOCKED"
    assert all(m["current_weight"] == 0 for m in body["members"])
    log.info("verdict: blocked batch -> %s", body["overall_basis"])
    # 恢复后整批推进到完成
    client.post("/groups/g2/health", json={})
    assert client.post(f"/batches/{batch_id}/advance").status_code == 200
    r = client.post(f"/batches/{batch_id}/advance")
    assert r.json()["status"] == "SUCCEEDED"
    # 与进行中单组计划冲突 -> 409 group_conflict
    client.post("/plans", json={"version": "v9", "group": "g9",
                                "steps": [10, 100]})
    r = client.post("/batches", json={"members": [
        {"group": "g9", "version": "v10", "steps": [10, 100]}]})
    assert r.status_code == 409 and r.json()["error"] == "group_conflict"
    log.info("verdict: conflicting batch -> %s", r.json())


def test_batch_rollback_via_api(client):
    r = client.post("/batches", json={"members": [
        {"group": "g1", "version": "v2", "steps": [10, 100]},
        {"group": "g2", "version": "v3", "steps": [10, 100]},
    ]})
    batch_id = r.json()["batch_id"]
    client.post(f"/batches/{batch_id}/start")
    client.post("/groups/g1/health", json={})
    client.post("/groups/g2/health", json={})
    client.post(f"/batches/{batch_id}/advance")
    r = client.post(f"/batches/{batch_id}/rollback", json={"reason": "alarm"})
    body = r.json()
    assert body["status"] == "ROLLED_BACK"
    assert all(m["status"] == "ROLLED_BACK" for m in body["members"])
    assert all(m["group_state"]["weight"] == m["stable_weight"]
               for m in body["members"])
    # 重复回滚幂等
    r = client.post(f"/batches/{batch_id}/rollback")
    assert r.json()["status"] == "ROLLED_BACK"
    log.info("verdict: batch rollback idempotent -> %s", body["status"])
