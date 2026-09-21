"""Legal state transitions and their rejection reasons."""
from __future__ import annotations

from .errors import InvalidTransition
from .models import Plan, PlanState

# action -> {from_state: to_state}
TRANSITIONS: dict[str, dict[PlanState, PlanState]] = {
    "start_promotion": {
        PlanState.PENDING: PlanState.PROMOTING,
        PlanState.PAUSED: PlanState.PROMOTING,
        PlanState.PROMOTING: PlanState.PROMOTING,  # idempotent re-entry
    },
    "gate_block": {
        PlanState.PROMOTING: PlanState.PAUSED,
    },
    "complete": {
        PlanState.PROMOTING: PlanState.COMPLETED,
    },
    "abort": {
        PlanState.PENDING: PlanState.ABORTED,
        PlanState.PROMOTING: PlanState.ABORTED,
        PlanState.PAUSED: PlanState.ABORTED,
    },
    "start_rollback": {
        PlanState.PROMOTING: PlanState.ROLLING_BACK,
        PlanState.PAUSED: PlanState.ROLLING_BACK,
        PlanState.PENDING: PlanState.ROLLING_BACK,
        PlanState.FAILED: PlanState.ROLLING_BACK,  # retry after failed rollback
    },
    "rollback_done": {
        PlanState.ROLLING_BACK: PlanState.ROLLED_BACK,
    },
    "rollback_failed": {
        PlanState.ROLLING_BACK: PlanState.FAILED,
    },
}

# Human-readable, distinguishable reasons for rejecting an action.
_REJECT_REASON: dict[str, dict[PlanState, str]] = {
    "start_promotion": {
        PlanState.COMPLETED: "plan already completed at weight 100",
        PlanState.ROLLED_BACK: "plan was rolled back; create a new plan",
        PlanState.ABORTED: "plan was aborted and cannot be resumed",
        PlanState.FAILED: "plan failed; only rollback retry is allowed",
        PlanState.ROLLING_BACK: "rollback in progress; promotion refused",
    },
    "gate_block": {
        PlanState.PENDING: "promotion has not started",
        PlanState.PAUSED: "already paused at a gate",
        PlanState.COMPLETED: "plan already completed",
        PlanState.ROLLED_BACK: "plan was rolled back",
        PlanState.ABORTED: "plan was aborted",
        PlanState.FAILED: "plan failed",
        PlanState.ROLLING_BACK: "rollback in progress",
    },
    "complete": {
        PlanState.PENDING: "promotion has not started",
        PlanState.PAUSED: "blocked at a health gate",
        PlanState.COMPLETED: "already completed",
        PlanState.ROLLED_BACK: "plan was rolled back",
        PlanState.ABORTED: "plan was aborted",
        PlanState.FAILED: "plan failed",
        PlanState.ROLLING_BACK: "rollback in progress",
    },
    "abort": {
        PlanState.COMPLETED: "cannot abort a completed plan",
        PlanState.ROLLED_BACK: "already rolled back",
        PlanState.ABORTED: "already aborted",
        PlanState.ROLLING_BACK: "rollback in progress; abort refused",
        PlanState.FAILED: "plan already failed terminally",
    },
    "start_rollback": {
        PlanState.COMPLETED: "plan completed healthy; rollback refused",
        PlanState.ROLLED_BACK: "already rolled back",
        PlanState.ABORTED: "aborted plan has no in-flight change to undo",
    },
}


def transition(plan: Plan, action: str) -> PlanState:
    """Apply a legal transition or raise InvalidTransition with a reason."""
    table = TRANSITIONS.get(action)
    if table is None:
        raise InvalidTransition(plan.plan_id, plan.state.value, action,
                                f"unknown action {action!r}")
    new_state = table.get(plan.state)
    if new_state is None:
        reason = _REJECT_REASON.get(action, {}).get(
            plan.state, f"action {action!r} not allowed from {plan.state.value}")
        raise InvalidTransition(plan.plan_id, plan.state.value, action, reason)
    plan.state = new_state
    return new_state
