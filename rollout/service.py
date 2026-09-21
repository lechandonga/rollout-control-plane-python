"""发布控制面核心服务：计划创建、推进、门禁、回滚、恢复。"""
from __future__ import annotations

from .config import ControlPlaneConfig
from .models import RolloutPlan, GatePolicy, HealthMetrics
from .storage import Storage


class RolloutService:
    """所有公开操作在并发下与串行执行等价（骨架）。"""

    def __init__(self, storage: Storage, config: ControlPlaneConfig | None = None):
        self.storage = storage
        self.config = config or ControlPlaneConfig()

    def create_plan(self, version: str, group: str, steps: list[int],
                    gate: GatePolicy | None = None) -> RolloutPlan:
        raise NotImplementedError

    def start(self, plan_id: str) -> RolloutPlan:
        raise NotImplementedError

    def advance(self, plan_id: str) -> RolloutPlan:
        raise NotImplementedError

    def report_health(self, group: str, metrics: HealthMetrics) -> None:
        raise NotImplementedError

    def rollback(self, plan_id: str, reason: str = "manual") -> RolloutPlan:
        raise NotImplementedError

    def abort(self, plan_id: str) -> RolloutPlan:
        raise NotImplementedError

    def get_plan(self, plan_id: str) -> RolloutPlan:
        raise NotImplementedError

    def observe(self, plan_id: str) -> dict:
        """可观测性：当前阶段与判定依据。"""
        raise NotImplementedError

    def recover(self) -> None:
        """进程重启/任务中断后的恢复。"""
        raise NotImplementedError
