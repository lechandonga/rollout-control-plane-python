"""Health gate: decides whether a promotion step may proceed."""
from __future__ import annotations

import time
from typing import Callable

from .models import GateStatus, HealthSnapshot, Plan

MetricProvider = Callable[[str, str, int], dict]


def _default_metrics(app: str, version: str, weight: int) -> dict:
    """Local simulation: a fresh canary is assumed healthy."""
    return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 10.0}


class HealthGate:
    """Threshold-based gate; the metric source is injectable for tests."""

    def __init__(self, min_success_rate: float = 0.95,
                 max_error_rate: float = 0.05,
                 max_latency_p99_ms: float = 1000.0,
                 critical_success_rate: float = 0.5,
                 critical_error_rate: float = 0.5,
                 provider: MetricProvider | None = None) -> None:
        self.min_success_rate = min_success_rate
        self.max_error_rate = max_error_rate
        self.max_latency_p99_ms = max_latency_p99_ms
        self.critical_success_rate = critical_success_rate
        self.critical_error_rate = critical_error_rate
        self._provider = provider or _default_metrics

    def evaluate(self, plan: Plan) -> HealthSnapshot:
        metrics = self._provider(plan.app, plan.version, plan.weight)
        sr = float(metrics["success_rate"])
        er = float(metrics["error_rate"])
        p99 = float(metrics["latency_p99_ms"])
        checks = {
            "success_rate": sr >= self.min_success_rate,
            "error_rate": er <= self.max_error_rate,
            "latency_p99_ms": p99 <= self.max_latency_p99_ms,
        }
        status = GateStatus.PASSED if all(checks.values()) else GateStatus.BLOCKED
        critical = (sr < self.critical_success_rate
                    or er > self.critical_error_rate)
        snap = HealthSnapshot(
            ts=time.time(), weight=plan.weight, success_rate=sr,
            error_rate=er, latency_p99_ms=p99, status=status,
            critical=critical)
        snap.failed_checks = [k for k, ok in checks.items() if not ok]  # type: ignore[attr-defined]
        return snap

    def evidence(self, snap: HealthSnapshot) -> dict:
        return {
            "weight": snap.weight,
            "success_rate": snap.success_rate,
            "error_rate": snap.error_rate,
            "latency_p99_ms": snap.latency_p99_ms,
            "failed_checks": getattr(snap, "failed_checks", []),
            "critical": snap.critical,
            "thresholds": {
                "min_success_rate": self.min_success_rate,
                "max_error_rate": self.max_error_rate,
                "max_latency_p99_ms": self.max_latency_p99_ms,
            },
        }
