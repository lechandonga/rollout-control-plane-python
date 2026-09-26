"""领域模型：发布计划、实例组、流量权重与状态机定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any


class PlanStatus(str, enum.Enum):
    """发布计划状态机。

    合法迁移：
      PENDING      -> RUNNING            (start)
      PENDING      -> ABORTED            (abort)
      RUNNING      -> RUNNING            (advance 一个放量步)
      RUNNING      -> SUCCEEDED          (到达 100%)
      RUNNING      -> ROLLING_BACK       (失败自动回滚 / 人工中止)
      ROLLING_BACK -> ROLLED_BACK        (回滚完成)
    其余迁移一律非法，必须被拒绝并给出可区分原因。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    ABORTED = "ABORTED"

    @property
    def terminal(self) -> bool:
        return self in (PlanStatus.SUCCEEDED, PlanStatus.ROLLED_BACK, PlanStatus.ABORTED)


# 合法迁移表：(from_status, action) -> to_status
TRANSITIONS: dict[tuple[PlanStatus, str], PlanStatus] = {
    (PlanStatus.PENDING, "start"): PlanStatus.RUNNING,
    (PlanStatus.PENDING, "abort"): PlanStatus.ABORTED,
    (PlanStatus.RUNNING, "advance"): PlanStatus.RUNNING,
    (PlanStatus.RUNNING, "succeed"): PlanStatus.SUCCEEDED,
    (PlanStatus.RUNNING, "rollback"): PlanStatus.ROLLING_BACK,
    (PlanStatus.RUNNING, "abort"): PlanStatus.ROLLING_BACK,
    (PlanStatus.ROLLING_BACK, "finish_rollback"): PlanStatus.ROLLED_BACK,
}


class GateVerdict(str, enum.Enum):
    """健康门禁判定结果，用于可观测性与日志区分。"""

    PASS = "PASS"
    BLOCKED = "BLOCKED"      # 指标未达标，门禁阻塞
    WAITING = "WAITING"      # 指标尚未上报，等待中


@dataclass
class HealthMetrics:
    """实例组健康指标。error_rate / unhealthy_ratio 均为 0..1。"""

    error_rate: float = 0.0
    unhealthy_ratio: float = 0.0
    reported: bool = False  # 是否已有上报；未上报时门禁为 WAITING


@dataclass
class GatePolicy:
    """健康门禁阈值。"""

    max_error_rate: float = 0.05
    max_unhealthy_ratio: float = 0.2


@dataclass
class RolloutStep:
    """单步放量：新版本流量权重（0..100）。"""

    weight: int


@dataclass
class StablePoint:
    """最近一个稳定点快照：回滚必须恢复到此处。"""

    step_index: int
    weight: int


@dataclass
class RolloutPlan:
    """发布计划（持久化实体）。"""

    plan_id: str
    version: str            # 目标应用版本
    group: str              # 目标实例组
    steps: list[int]        # 放量权重序列，如 [10, 50, 100]
    status: PlanStatus = PlanStatus.PENDING
    step_index: int = -1    # 当前已完成的步下标；-1 表示尚未开始
    stable: StablePoint = field(default_factory=lambda: StablePoint(step_index=-1, weight=0))
    gate: GatePolicy = field(default_factory=GatePolicy)
    failure_reason: str | None = None
    # 非空表示该计划由某个多组发布批次托管，只能经批次接口操作
    batch_id: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    # 持久化格式版本，用于格式演进
    format_version: int = 3

    @staticmethod
    def new(version: str, group: str, steps: list[int], gate: GatePolicy,
            batch_id: str | None = None) -> "RolloutPlan":
        return RolloutPlan(
            plan_id=uuid.uuid4().hex[:12],
            version=version,
            group=group,
            steps=list(steps),
            gate=gate,
            batch_id=batch_id,
        )

    @property
    def current_weight(self) -> int:
        if self.step_index < 0:
            return 0
        return self.steps[self.step_index]


@dataclass
class ReleaseBatch:
    """多组协同发布批次（持久化实体）。

    批次本身不持有放量状态，放量进度由各成员计划（RolloutPlan，batch_id
    指向本批次）分别记录；批次状态机与计划状态机一致，复用 TRANSITIONS。
    批次的推进与回滚对成员是「整批原子」的：要么全部成员一起前进一步，
    要么全部回到各自最近稳定点。
    """

    batch_id: str
    plan_ids: list[str]                 # 成员计划，顺序即创建时的组顺序
    status: PlanStatus = PlanStatus.PENDING
    failure_reason: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    # 批次记录自身的格式版本（批次自 v1 起步）
    format_version: int = 1

    @staticmethod
    def new(plan_ids: list[str]) -> "ReleaseBatch":
        return ReleaseBatch(batch_id=uuid.uuid4().hex[:12], plan_ids=list(plan_ids))


@dataclass
class InstanceGroupState:
    """实例组对外可见状态：当前生效版本与流量权重。"""

    group: str
    version: str            # 当前承载流量的版本
    weight: int = 0         # 该版本的流量权重 0..100
    instances_healthy: bool = True


# ---- 序列化 / 反序列化（含格式演进） ----

SUPPORTED_FORMAT_VERSIONS = (1, 2, 3)


def plan_to_dict(plan: RolloutPlan) -> dict[str, Any]:
    d = asdict(plan)
    d["status"] = plan.status.value
    return d


def plan_from_dict(data: dict[str, Any]) -> RolloutPlan:
    """从持久化字典恢复计划。

    格式演进规则：
      - format_version 缺失视为 v1；
      - v1 缺少的字段（gate / failure_reason / stable）使用确定缺省；
      - v2 缺少的字段（batch_id）缺省为 None（独立单组计划）；
      - 高于当前支持的版本或版本非法 -> 抛出 ValueError，明确拒绝。
    """
    fv = data.get("format_version", 1)
    if fv not in SUPPORTED_FORMAT_VERSIONS:
        raise ValueError(f"unsupported format_version: {fv!r}")
    gate_data = data.get("gate") or {}
    stable_data = data.get("stable") or {}
    return RolloutPlan(
        plan_id=data["plan_id"],
        version=data["version"],
        group=data["group"],
        steps=[int(s) for s in data["steps"]],
        status=PlanStatus(data["status"]),
        step_index=int(data.get("step_index", -1)),
        stable=StablePoint(
            step_index=int(stable_data.get("step_index", -1)),
            weight=int(stable_data.get("weight", 0)),
        ),
        gate=GatePolicy(
            max_error_rate=float(gate_data.get("max_error_rate", 0.05)),
            max_unhealthy_ratio=float(gate_data.get("max_unhealthy_ratio", 0.2)),
        ),
        failure_reason=data.get("failure_reason"),
        batch_id=data.get("batch_id"),  # v2 及更早无此字段 -> None
        created_at=float(data.get("created_at", 0.0)),
        updated_at=float(data.get("updated_at", 0.0)),
        format_version=3,
    )


SUPPORTED_BATCH_FORMAT_VERSIONS = (1,)


def batch_to_dict(batch: ReleaseBatch) -> dict[str, Any]:
    d = asdict(batch)
    d["status"] = batch.status.value
    return d


def batch_from_dict(data: dict[str, Any]) -> ReleaseBatch:
    """从持久化字典恢复批次。版本不兼容 -> ValueError，明确拒绝。"""
    fv = data.get("format_version", 1)
    if fv not in SUPPORTED_BATCH_FORMAT_VERSIONS:
        raise ValueError(f"unsupported batch format_version: {fv!r}")
    return ReleaseBatch(
        batch_id=data["batch_id"],
        plan_ids=[str(p) for p in data["plan_ids"]],
        status=PlanStatus(data["status"]),
        failure_reason=data.get("failure_reason"),
        created_at=float(data.get("created_at", 0.0)),
        updated_at=float(data.get("updated_at", 0.0)),
        format_version=1,
    )
