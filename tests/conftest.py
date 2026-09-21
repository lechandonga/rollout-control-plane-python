import logging
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from rollout.config import Config
from rollout.journal import Journal
from rollout.manager import RolloutManager
from rollout.store import Store


def healthy(a, v, w):
    return {"success_rate": 1.0, "error_rate": 0.0, "latency_p99_ms": 10.0}


@pytest.fixture
def tmp_store(tmp_path):
    return Store(str(tmp_path / "rollout.json"))


@pytest.fixture
def journal():
    return Journal()


@pytest.fixture
def manager(tmp_store, journal):
    return RolloutManager(
        tmp_store,
        config=Config(max_concurrent_rollouts=8, max_step_weight=25,
                      step_timeout_s=5, queue_timeout_s=5),
        journal=journal,
    )


def make_manager(path, gate=None, **cfg):
    base = dict(max_concurrent_rollouts=8, max_step_weight=25,
                step_timeout_s=5, queue_timeout_s=5)
    base.update(cfg)
    return RolloutManager(Store(path), config=Config(**base),
                          gate=gate, journal=Journal())


def log_case(title: str, **inputs):
    """Print test inputs + decision basis (shown with -s / captured in logs)."""
    logging.getLogger("rollout.test").info("CASE %s inputs=%s", title, inputs)
    print(f"\n[CASE] {title} inputs={inputs}")


def log_verdict(**verdict):
    logging.getLogger("rollout.test").info("VERDICT %s", verdict)
    print(f"[VERDICT] {verdict}")
