"""HTTP surface: distinguishable status codes, reasons and observability."""
from fastapi.testclient import TestClient

from rollout.api import create_app
from rollout.health import HealthGate
from conftest import make_manager


def _client(path, gate=None, **cfg):
    m = make_manager(path, gate=gate, **cfg)
    return TestClient(create_app(m)), m


def test_full_flow_and_error_codes(tmp_path):
    c, m = _client(str(tmp_path / "s.json"), max_step_weight=25)
    with c:
        r = c.post("/plans", json={"plan_id": "h1", "app": "app",
                                   "version": "v2", "target_group": "canary",
                                   "step_weight": 25})
        assert r.status_code == 201
        assert c.post("/plans", json={"plan_id": "h1", "app": "app",
                                      "version": "v2",
                                      "target_group": "canary"}).status_code == 409

        for _ in range(4):
            r = c.post("/plans/h1/promote")
            assert r.status_code == 200, r.text
        assert r.json()["state"] == "completed"

        r = c.post("/plans/h1/promote")
        assert r.status_code == 409
        body = r.json()["detail"]
        assert body["code"] == "invalid_transition" and body["reason"]

        r = c.get("/plans/h1/events")
        kinds = {e["kind"] for e in r.json()["events"]}
        assert {"state_change", "completed"} <= kinds


def test_gate_blocked_is_visible_with_evidence(tmp_path):
    gate = HealthGate(provider=lambda a, v, w: {
        "success_rate": 0.4, "error_rate": 0.6, "latency_p99_ms": 10})
    c, m = _client(str(tmp_path / "s.json"), gate=gate, max_step_weight=25)
    with c:
        c.post("/plans", json={"plan_id": "h2", "app": "app",
                               "version": "v2", "target_group": "canary",
                               "step_weight": 25})
        r = c.post("/plans/h2/promote")
        st = r.json()
        assert st["state"] == "rolled_back" and st["weight"] == 0
        ev = c.get("/plans/h2/events").json()["events"]
        kinds = [e["kind"] for e in ev]
        assert "failed" in kinds and "rollback" in kinds
        failed = next(e for e in ev if e["kind"] == "failed")
        assert failed["evidence"]["critical"] is True


def test_not_found_and_healthz(tmp_path):
    c, _ = _client(str(tmp_path / "s.json"))
    with c:
        assert c.get("/plans/nope/status").status_code == 404
        assert c.get("/healthz").status_code == 200
