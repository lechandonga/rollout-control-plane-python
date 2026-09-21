"""可区分的错误类型：每种非法操作都有独立原因码。"""


class RolloutError(Exception):
    """控制面错误基类。reason 为机器可读的区分码。"""

    reason: str = "internal_error"


class IllegalTransitionError(RolloutError):
    """非法状态迁移。"""

    reason = "illegal_transition"

    def __init__(self, plan_id: str, status: str, action: str, detail: str = ""):
        self.plan_id = plan_id
        self.status = status
        self.action = action
        msg = f"plan {plan_id}: cannot {action} while {status}"
        if detail:
            msg += f" ({detail})"
        super().__init__(msg)


class GateBlockedError(RolloutError):
    """健康门禁未通过，禁止继续放量。"""

    reason = "gate_blocked"


class PlanNotFoundError(RolloutError):
    reason = "plan_not_found"


class ValidationError(RolloutError):
    """请求参数非法（权重序列、阈值等）。"""

    reason = "validation_error"


class ConcurrencyLimitError(RolloutError):
    """并发任务数超限，按策略拒绝。"""

    reason = "concurrency_limit"


class StorageCorruptError(RolloutError):
    """持久化数据无法兼容，明确拒绝而非静默继续。"""

    reason = "storage_corrupt"
