"""Structured observability: waiting / gate-blocked / failed / completed."""
from __future__ import annotations

import dataclasses
import enum
import logging
import time
from collections import deque


class EventKind(str, enum.Enum):
    WAITING = "waiting"
    GATE_BLOCKED = "gate_blocked"
    FAILED = "failed"
    COMPLETED = "completed"
    STATE_CHANGE = "state_change"
    ROLLBACK = "rollback"
    INFO = "info"


@dataclasses.dataclass
class Event:
    ts: float
    plan_id: str
    kind: EventKind
    message: str
    evidence: dict = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "plan_id": self.plan_id,
            "kind": self.kind.value,
            "message": self.message,
            "evidence": self.evidence,
        }


class Journal:
    """In-memory ring of structured events, also mirrored to the logger."""

    def __init__(self, capacity: int = 4096) -> None:
        self._events: deque[Event] = deque(maxlen=capacity)
        self._logger = logging.getLogger("rollout")

    def emit(self, plan_id: str, kind: EventKind, message: str,
             evidence: dict | None = None) -> Event:
        ev = Event(time.time(), plan_id, kind, message, dict(evidence or {}))
        self._events.append(ev)
        # Distinguishable level per kind.
        level = {
            EventKind.FAILED: logging.ERROR,
            EventKind.GATE_BLOCKED: logging.WARNING,
            EventKind.COMPLETED: logging.INFO,
        }.get(kind, logging.INFO)
        self._logger.log(
            level,
            "[%s] plan=%s %s evidence=%s",
            kind.value, plan_id, message, ev.evidence,
        )
        return ev

    def events(self, plan_id: str | None = None) -> list[Event]:
        if plan_id is None:
            return list(self._events)
        return [e for e in self._events if e.plan_id == plan_id]
