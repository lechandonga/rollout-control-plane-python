"""Local service entrypoint.

Run with:  uv run fastapi dev main.py   (or)  uv run python -m uvicorn main:app
State is persisted to ./data/rollout.json by default.
"""
from __future__ import annotations

import os

from rollout.api import create_app
from rollout.config import Config
from rollout.journal import Journal
from rollout.manager import RolloutManager
from rollout.store import Store

DATA_PATH = os.environ.get("ROLLOUT_DATA", "data/rollout.json")

journal = Journal()
manager = RolloutManager(
    Store(DATA_PATH),
    config=Config(
        max_concurrent_rollouts=int(os.environ.get("MAX_CONCURRENT", "4")),
        max_step_weight=int(os.environ.get("MAX_STEP_WEIGHT", "25")),
        step_timeout_s=float(os.environ.get("STEP_TIMEOUT", "30")),
        queue_timeout_s=float(os.environ.get("QUEUE_TIMEOUT", "60")),
    ),
    journal=journal,
)
app = create_app(manager)
