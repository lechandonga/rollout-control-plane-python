"""Crash-safe local persistence with schema migration semantics."""
from __future__ import annotations

import json
import os
import tempfile
import threading

from .errors import IncompatibleData
from .models import SCHEMA_VERSION, Plan

_STORE_FORMAT = "rollout-store"
_REQUIRED_PLAN_FIELDS = ("plan_id", "app", "version", "target_group")


class Store:
    """JSON-file backed store.

    Atomic writes (tmp + fsync + rename); per-store lock; whole-document
    rewrite so a crash leaves either the old or the new document, never a
    torn half-update.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Reading / migration
    # ------------------------------------------------------------------ #
    def _read_doc(self) -> dict:
        if not os.path.exists(self.path):
            return {"format": _STORE_FORMAT, "schema_version": SCHEMA_VERSION,
                    "plans": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                doc = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            raise IncompatibleData(
                f"store at {self.path} is unreadable: {exc}") from exc
        return self._migrate(doc)

    def _migrate(self, doc: object) -> dict:
        if not isinstance(doc, dict):
            raise IncompatibleData("top-level store document must be an object")
        fmt = doc.get("format")
        # Very old dumps may have been a bare {plan_id: plan} mapping.
        if fmt is None and isinstance(doc.get("plans"), dict) is False:
            if all(isinstance(v, dict) for v in doc.values()) and doc:
                doc = {"format": _STORE_FORMAT, "schema_version": 1,
                       "plans": dict(doc)}
                fmt = _STORE_FORMAT
            else:
                raise IncompatibleData("missing 'format' and not a plan mapping")
        if fmt != _STORE_FORMAT:
            raise IncompatibleData(f"unknown store format: {fmt!r}")

        sv = doc.get("schema_version", 1)
        if not isinstance(sv, int) or isinstance(sv, bool):
            raise IncompatibleData(f"schema_version must be int, got {type(sv)}")
        if sv > SCHEMA_VERSION:
            raise IncompatibleData(
                f"store schema_version {sv} newer than supported "
                f"{SCHEMA_VERSION}; refusing to read")
        plans = doc.get("plans")
        if not isinstance(plans, dict):
            raise IncompatibleData("'plans' must be an object")
        # v1 -> v2: no structural rename; v2 added optional fields with stable
        # defaults (policy.step_weight=10, reason='', history=[], ...).
        # Plan.from_dict supplies those deterministically.
        return {"format": _STORE_FORMAT, "schema_version": SCHEMA_VERSION,
                "plans": plans}

    def load_all(self) -> dict[str, Plan]:
        with self._lock:
            doc = self._read_doc()
            result: dict[str, Plan] = {}
            for pid, raw in doc["plans"].items():
                if not isinstance(raw, dict):
                    raise IncompatibleData(f"plan {pid} must be an object")
                missing = [k for k in _REQUIRED_PLAN_FIELDS if k not in raw]
                if missing:
                    raise IncompatibleData(
                        f"plan {pid} missing required fields: {missing}")
                result[pid] = Plan.from_dict(raw)
            return result

    # ------------------------------------------------------------------ #
    # Atomic writing
    # ------------------------------------------------------------------ #
    def _write_doc(self, doc: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".rollout-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            # Best-effort directory fsync for durability of the rename.
            try:
                dfd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def save(self, plan: Plan) -> None:
        with self._lock:
            doc = self._read_doc()
            doc["plans"][plan.plan_id] = plan.to_dict()
            self._write_doc(doc)

    def delete(self, plan_id: str) -> None:
        with self._lock:
            doc = self._read_doc()
            doc["plans"].pop(plan_id, None)
            self._write_doc(doc)
