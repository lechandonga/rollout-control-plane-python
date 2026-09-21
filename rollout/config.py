"""可配置上限：并发任务数、放量步长与超时。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ControlPlaneConfig:
    max_concurrent_tasks: int = 4      # 同时进行的后台放量/回滚任务上限
    max_step_weight_delta: int = 100    # 相邻两步权重差上限（放量步长）
    step_timeout_seconds: float = 30.0 # 单步操作超时
    overflow_policy: str = "reject"    # 超限策略：reject | queue
    storage_path: str = "rollout_state.json"
