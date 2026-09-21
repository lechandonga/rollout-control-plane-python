"""本地持久化：JSON 文件 + 原子写，模拟版本/实例组/权重的持久存储。"""
from __future__ import annotations

import threading

from .models import RolloutPlan, InstanceGroupState, HealthMetrics


class Storage:
    """进程重启后仍可恢复的持久化存储（骨架）。"""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._plans: dict[str, RolloutPlan] = {}
        self._groups: dict[str, InstanceGroupState] = {}
        self._health: dict[str, HealthMetrics] = {}

    # ---- 计划 ----
    def save_plan(self, plan: RolloutPlan) -> None:
        raise NotImplementedError

    def get_plan(self, plan_id: str) -> RolloutPlan | None:
        raise NotImplementedError

    def list_plans(self) -> list[RolloutPlan]:
        raise NotImplementedError

    # ---- 实例组 ----
    def save_group(self, group: InstanceGroupState) -> None:
        raise NotImplementedError

    def get_group(self, name: str) -> InstanceGroupState | None:
        raise NotImplementedError

    # ---- 健康指标 ----
    def save_health(self, group: str, metrics: HealthMetrics) -> None:
        raise NotImplementedError

    def get_health(self, group: str) -> HealthMetrics:
        raise NotImplementedError

    # ---- 生命周期 ----
    def load(self) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError
