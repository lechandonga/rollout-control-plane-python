"""Bounded concurrency, queueing, timeouts; no leaked background resources."""
from __future__ import annotations

import threading

from .config import Config
from .errors import LimitExceeded, QueueTimeout
from .journal import EventKind, Journal


class Scheduler:
    def __init__(self, config: Config | None = None,
                 journal: Journal | None = None) -> None:
        self.config = config or Config()
        self._journal = journal
        self._sem = threading.BoundedSemaphore(self.config.max_concurrent_rollouts)
        self._lock = threading.Lock()
        self._active = 0
        self._queued = 0
        self._closed = False

    def active_count(self) -> int:
        with self._lock:
            return self._active

    def queued_count(self) -> int:
        with self._lock:
            return self._queued

    def acquire(self, plan_id: str = "-") -> None:
        """Acquire a slot, queueing or rejecting per config policy."""
        with self._lock:
            if self._closed:
                raise LimitExceeded("scheduler_closed", 0)
        if self._sem.acquire(blocking=False):
            with self._lock:
                self._active += 1
            return

        # No free slot.
        if self.config.on_limit == "reject":
            raise LimitExceeded(
                "concurrent_rollouts", self.config.max_concurrent_rollouts,
                self.active_count())

        with self._lock:
            if self._queued >= self.config.queue_max:
                raise LimitExceeded("queue_depth", self.config.queue_max,
                                    self._queued)
            self._queued += 1
        try:
            if self._journal is not None:
                self._journal.emit(
                    plan_id, EventKind.WAITING, "waiting for rollout slot",
                    {"queued_ahead": self._queued,
                     "active": self.active_count(),
                     "queue_timeout_s": self.config.queue_timeout_s})
            if not self._sem.acquire(timeout=self.config.queue_timeout_s):
                raise QueueTimeout(
                    f"plan {plan_id}: queued longer than "
                    f"{self.config.queue_timeout_s}s")
            with self._lock:
                self._active += 1
        finally:
            with self._lock:
                self._queued -= 1

    def release(self) -> None:
        with self._lock:
            if self._active > 0:
                self._active -= 1
        self._sem.release()

    def shutdown(self, timeout: float | None = None) -> None:
        """Drain in-flight tasks; no task thread is left running afterwards."""
        with self._lock:
            self._closed = True
            slots = self.config.max_concurrent_rollouts
            active = self._active
        # Taking every permit waits until all active tasks have released.
        acquired = 0
        deadline_ev = threading.Event()
        for _ in range(slots):
            if self._sem.acquire(timeout=timeout):
                acquired += 1
            else:
                break
        for _ in range(acquired):
            self._sem.release()
        if acquired < slots:
            raise TimeoutError(
                f"scheduler shutdown: {active} task(s) still active")
        deadline_ev.set()
