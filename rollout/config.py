"""Configurable upper bounds for concurrency, step size and timeouts."""
from __future__ import annotations

import dataclasses

from .errors import LimitExceeded


@dataclasses.dataclass(frozen=True)
class Config:
    max_concurrent_rollouts: int = 4
    max_step_weight: int = 25
    min_step_weight: int = 1
    step_timeout_s: float = 30.0
    queue_timeout_s: float = 60.0
    queue_max: int = 16
    # When the concurrency limit is hit: "queue" waits, "reject" raises.
    on_limit: str = "queue"

    def __post_init__(self) -> None:
        if self.max_concurrent_rollouts < 1:
            raise ValueError("max_concurrent_rollouts must be >= 1")
        if not (1 <= self.min_step_weight <= self.max_step_weight <= 100):
            raise ValueError("invalid step weight bounds")
        if self.on_limit not in ("queue", "reject"):
            raise ValueError("on_limit must be 'queue' or 'reject'")
        if self.step_timeout_s <= 0 or self.queue_timeout_s <= 0:
            raise ValueError("timeouts must be positive")
        if self.queue_max < 0:
            raise ValueError("queue_max must be >= 0")

    def validate_step(self, step_weight: int) -> None:
        """Reject a step that would jump beyond configured bounds."""
        if not isinstance(step_weight, int) or isinstance(step_weight, bool):
            raise LimitExceeded("step_weight_type", self.max_step_weight)
        if step_weight < self.min_step_weight:
            raise LimitExceeded("step_weight_min", self.min_step_weight, step_weight)
        if step_weight > self.max_step_weight:
            raise LimitExceeded("step_weight_max", self.max_step_weight, step_weight)
