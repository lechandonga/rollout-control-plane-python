"""骨架冒烟测试：模块可导入、状态机迁移表自洽。"""
from rollout.models import PlanStatus, TRANSITIONS, RolloutPlan, GatePolicy


def test_import_and_transition_table():
    assert (PlanStatus.PENDING, "start") in TRANSITIONS
    plan = RolloutPlan.new("v2", "g1", [10, 50, 100], GatePolicy())
    assert plan.status == PlanStatus.PENDING
    assert plan.current_weight == 0
