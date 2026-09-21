"""Illegal migrations are rejected with distinguishable reasons."""
import pytest

from rollout.errors import InvalidTransition
from rollout.models import Plan, PlanState, RolloutPolicy
from rollout.statemachine import transition


def _plan(state=PlanState.PENDING):
    return Plan("p", "app", "v2", "canary", RolloutPolicy(), state=state)


@pytest.mark.parametrize("state,action", [
    (PlanState.COMPLETED, "start_promotion"),
    (PlanState.ROLLED_BACK, "start_promotion"),
    (PlanState.ABORTED, "start_promotion"),
    (PlanState.ROLLING_BACK, "start_promotion"),
    (PlanState.FAILED, "start_promotion"),
    (PlanState.COMPLETED, "abort"),
    (PlanState.ROLLED_BACK, "abort"),
    (PlanState.ABORTED, "abort"),
    (PlanState.ROLLING_BACK, "abort"),
    (PlanState.COMPLETED, "start_rollback"),
    (PlanState.ROLLED_BACK, "start_rollback"),
    (PlanState.ABORTED, "start_rollback"),
])
def test_illegal_transitions_distinguishable(state, action):
    from conftest import log_case, log_verdict
    p = _plan(state)
    log_case("illegal_transition", state=state.value, action=action)  # noqa
    with pytest.raises(InvalidTransition) as ei:
        transition(p, action)
    log_verdict(code=ei.value.code, reason=ei.value.reason,
                from_state=state.value)
    assert ei.value.reason  # a specific, non-empty reason exists
    assert ei.value.state == state.value


def test_legal_path():
    p = _plan()
    assert transition(p, "start_promotion") is PlanState.PROMOTING
    assert transition(p, "gate_block") is PlanState.PAUSED
    assert transition(p, "start_promotion") is PlanState.PROMOTING
    assert transition(p, "start_rollback") is PlanState.ROLLING_BACK
    assert transition(p, "rollback_done") is PlanState.ROLLED_BACK


def test_pending_can_abort():
    p = _plan(PlanState.PENDING)
    assert transition(p, "abort") is PlanState.ABORTED
