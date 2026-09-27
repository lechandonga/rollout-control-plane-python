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
    r = client.post("/batches", json={"targets": [
        {"group": "g1", "version": "v2", "steps": [10, 100]},
        {"group": "g2", "version": "v1.1", "steps": [50, 100]},
    ]})
    assert r.status_code == 201
    batch_id = r.json()["batch_id"]
    log.info("input: create batch -> %s", r.json()["conclusion"])

    assert client.post(f"/batches/{batch_id}/start").status_code == 200
    # 未上报健康 -> 409 gate_blocked，整批不推进
    r = client.post(f"/batches/{batch_id}/advance")
    assert r.status_code == 409 and r.json()["error"] == "gate_blocked"

    client.post("/groups/g1/health", json={})
    client.post("/groups/g2/health", json={})
    r = client.post(f"/batches/{batch_id}/advance")
    body = r.json()
    by_group = {g["group"]: g for g in body["groups"]}
    assert by_group["g1"]["current_weight"] == 10
    assert by_group["g2"]["current_weight"] == 50
    assert all(g["gate_verdict"] == "PASS" for g in body["groups"])
    log.info("verdict: per-group progress -> %s", body["conclusion"])

    # 整批回滚：所有组一起回稳定点
    r = client.post(f"/batches/{batch_id}/rollback", json={"reason": "alarm"})
    body = r.json()
    assert body["status"] == "ROLLED_BACK"
    assert all(g["group_state"]["weight"] == g["stable_weight"]
               for g in body["groups"])
    log.info("verdict: batch rollback -> %s", body["conclusion"])


def test_batch_conflict_via_api(client):
    r = client.post("/plans", json={"version": "v2", "group": "g1",
                                    "steps": [10, 100]})
    plan_id = r.json()["plan_id"]
    client.post(f"/plans/{plan_id}/start")
    r = client.post("/batches", json={"targets": [
        {"group": "g1", "version": "v3", "steps": [10, 100]},
    ]})
    assert r.status_code == 409 and r.json()["error"] == "group_conflict"
    log.info("verdict: conflicting batch -> %s", r.json())
    assert client.get("/batches/nope").status_code == 404


def test_batch_concurrent_gets_see_only_coherent_snapshots(client):
    """HTTP 层：整批推进期间并发 GET，每份响应都必须是自洽完整快照。"""
    import threading
    import time

    import main

    r = client.post("/batches", json={"targets": [
        {"group": "g1", "version": "v2", "steps": [10, 100]},
        {"group": "g2", "version": "v2", "steps": [10, 100]},
        {"group": "g3", "version": "v2", "steps": [10, 100]},
    ]})
    batch_id = r.json()["batch_id"]
    assert client.post(f"/batches/{batch_id}/start").status_code == 200
    for g in ("g1", "g2", "g3"):
        client.post(f"/groups/{g}/health", json={})
    assert client.post(f"/batches/{batch_id}/advance").status_code == 200

    # 放大逐组发布窗口
    orig = main._service._group_state_for

    def slow(group, weight, version):
        state = orig(group, weight, version)
        time.sleep(0.02)
        return state

    main._service._group_state_for = slow
    errors, vectors = [], set()
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            body = client.get(f"/batches/{batch_id}").json()
            ws = {}
            for g in body["groups"]:
                w = 0 if g["group_state"] is None else g["group_state"]["weight"]
                ws[g["group"]] = w
                if w != g["current_weight"]:
                    errors.append(f"{body['status']}: {g['group']} "
                                  f"group_weight={w} != detail={g['current_weight']}")
            if body["status"] == "SUCCEEDED" and not all(v == 100 for v in ws.values()):
                errors.append(f"SUCCEEDED 但明细未齐: {ws}")
            vectors.add(tuple(sorted(ws.items())))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.02)
    resp = client.post(f"/batches/{batch_id}/advance")
    assert resp.status_code == 200
    time.sleep(0.05)
    stop.set()
    for t in threads:
        t.join()
    main._service._group_state_for = orig

    assert not errors, errors[:3]
    assert (("g1", 10), ("g2", 10), ("g3", 10)) in vectors
    assert (("g1", 100), ("g2", 100), ("g3", 100)) in vectors
    for vec in vectors:
        assert len({w for _, w in vec}) == 1, f"半截快照: {vec}"
    log.info("verdict: HTTP concurrent reads saw only whole generations: %s",
             sorted(vectors))
