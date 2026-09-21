"""Distinguishable error types for the rollout control plane."""


class RolloutError(Exception):
    """Base class for all control-plane errors."""

    code = "rollout_error"


class PlanNotFound(RolloutError):
    code = "plan_not_found"


class InvalidTransition(RolloutError):
    """An operation is not allowed from the plan's current state."""

    code = "invalid_transition"

    def __init__(self, plan_id: str, state: str, action: str, reason: str):
        self.plan_id = plan_id
        self.state = state
        self.action = action
        self.reason = reason
        super().__init__(
            f"plan {plan_id}: action {action!r} invalid in state "
            f"{state!r}: {reason}"
        )


class HealthGateBlocked(RolloutError):
    """Promotion must not continue because health criteria are not met."""

    code = "health_gate_blocked"

    def __init__(self, plan_id: str, weight: int, evidence: dict):
        self.plan_id = plan_id
        self.weight = weight
        self.evidence = evidence
        super().__init__(
            f"plan {plan_id}: health gate blocked at weight {weight}: {evidence}"
        )


class LimitExceeded(RolloutError):
    """A configurable capacity/step/timeout limit was exceeded (reject)."""

    code = "limit_exceeded"

    def __init__(self, kind: str, limit: int, actual: int | None = None):
        self.kind = kind
        self.limit = limit
        self.actual = actual
        super().__init__(
            f"{kind} limit exceeded: limit={limit}"
            + (f" actual={actual}" if actual is not None else "")
        )


class QueueTimeout(RolloutError):
    """A queued task waited longer than the configured queue timeout."""

    code = "queue_timeout"


class IncompatibleData(RolloutError):
    """Persisted data cannot be understood by this version and is refused."""

    code = "incompatible_data"


class RollbackStablePointMissing(RolloutError):
    code = "rollback_stable_point_missing"


class PlanAlreadyExists(RolloutError):
    code = "plan_already_exists"
