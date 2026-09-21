"""Domain model: plans, instance groups, traffic weights, stable points."""
from __future__ import annotations

import dataclasses
import enum
import time


SCHEMA_VERSION = 2


class PlanState(str, enum.Enum):
    PENDING = "pending"
    PROMOTING = "promoting"        # traffic weight moving toward canary
    PAUSED = "paused"              # blocked at a health gate or manually held
    COMPLETED = "completed"        # 100% shifted and healthy
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"    # terminal: returned to a stable point
    ABORTED = "aborted"            # terminal: human abort, no rollback taken
    FAILED = "failed"              # terminal: rollback attempted but failed


class GateStatus(str, enum.Enum):
    UNKNOWN = "unknown"
    PASSED = "passed"
    BLOCKED = "blocked"


class InstanceStatus(str, enum.Enum):
    PROVISIONING = "provisioning"
    READY = "ready"
    DRAINING = "draining"
    INACTIVE = "inactive"


@dataclasses.dataclass
class Instance:
    instance_id: str
    group: str
    version: str
    status: InstanceStatus = InstanceStatus.PROVISIONING

    def to_dict(self) -> dict:
        return dataclasses.asdict(self) | {"status": self.status.value}

    @classmethod
    def from_dict(cls, d: dict) -> "Instance":
        return cls(
            instance_id=d["instance_id"],
            group=d["group"],
            version=d["version"],
            status=InstanceStatus(d.get("status", InstanceStatus.READY.value)),
        )


@dataclasses.dataclass
class InstanceGroup:
    name: str
    version: str
    instances: list[Instance] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "instances": [i.to_dict() for i in self.instances],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "InstanceGroup":
        return cls(
            name=d["name"],
            version=d["version"],
            instances=[Instance.from_dict(i) for i in d.get("instances", [])],
        )


@dataclasses.dataclass
class RolloutPolicy:
    """Stepwise canary policy. Weights are percentages, strictly ascending."""

    step_weight: int = 10                 # traffic added per promotion step
    gate_required: bool = True            # require health gate at each step
    max_weight: int = 100

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RolloutPolicy":
        return cls(
            step_weight=d.get("step_weight", 10),
            gate_required=d.get("gate_required", True),
            max_weight=d.get("max_weight", 100),
        )


@dataclasses.dataclass
class HealthSnapshot:
    """Evidence recorded at a promotion decision."""

    ts: float
    weight: int
    success_rate: float
    error_rate: float
    latency_p99_ms: float
    status: GateStatus
    critical: bool = False          # catastrophic -> automatic rollback

    def to_dict(self) -> dict:
        d = dataclasses.asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "HealthSnapshot":
        return cls(
            ts=d["ts"],
            weight=d["weight"],
            success_rate=d["success_rate"],
            error_rate=d["error_rate"],
            latency_p99_ms=d["latency_p99_ms"],
            status=GateStatus(d.get("status", GateStatus.UNKNOWN.value)),
            critical=d.get("critical", False),
        )


@dataclasses.dataclass
class StablePoint:
    """An externally-observable state we can safely roll back to."""

    ts: float
    version: str
    weight: int
    groups: dict[str, InstanceGroup]

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "version": self.version,
            "weight": self.weight,
            "groups": {k: v.to_dict() for k, v in self.groups.items()},
        }

    @classmethod
    def from_dict(cls, d: dict) -> "StablePoint":
        return cls(
            ts=d["ts"],
            version=d["version"],
            weight=d["weight"],
            groups={k: InstanceGroup.from_dict(v) for k, v in d["groups"].items()},
        )


@dataclasses.dataclass
class Plan:
    plan_id: str
    app: str
    version: str
    target_group: str
    policy: RolloutPolicy
    from_version: str = ""       # baseline version serving before rollout
    state: PlanState = PlanState.PENDING
    weight: int = 0                       # canary traffic percentage
    groups: dict[str, InstanceGroup] = dataclasses.field(default_factory=dict)
    stable_points: list[StablePoint] = dataclasses.field(default_factory=list)
    history: list[HealthSnapshot] = dataclasses.field(default_factory=list)
    reason: str = ""                      # last human-readable decision reason
    created_at: float = dataclasses.field(default_factory=time.time)
    updated_at: float = dataclasses.field(default_factory=time.time)
    version_counter: int = 0             # optimistic-concurrency counter

    def touch(self) -> None:
        self.updated_at = time.time()
        self.version_counter += 1

    def latest_stable_point(self) -> StablePoint | None:
        return self.stable_points[-1] if self.stable_points else None

    def to_dict(self) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "plan_id": self.plan_id,
            "app": self.app,
            "version": self.version,
            "target_group": self.target_group,
            "from_version": self.from_version,
            "policy": self.policy.to_dict(),
            "state": self.state.value,
            "weight": self.weight,
            "groups": {k: v.to_dict() for k, v in self.groups.items()},
            "stable_points": [s.to_dict() for s in self.stable_points],
            "history": [h.to_dict() for h in self.history],
            "reason": self.reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "version_counter": self.version_counter,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Plan":
        sv = d.get("schema_version", 1)
        if sv > SCHEMA_VERSION:
            from .errors import IncompatibleData

            raise IncompatibleData(
                f"plan {d.get('plan_id')}: schema_version {sv} newer than "
                f"supported {SCHEMA_VERSION}"
            )
        return cls(
            plan_id=d["plan_id"],
            app=d["app"],
            version=d["version"],
            target_group=d["target_group"],
            from_version=d.get("from_version", ""),
            policy=RolloutPolicy.from_dict(d.get("policy", {})),
            state=PlanState(d.get("state", PlanState.PENDING.value)),
            weight=d.get("weight", 0),
            groups={k: InstanceGroup.from_dict(v) for k, v in d.get("groups", {}).items()},
            stable_points=[StablePoint.from_dict(s) for s in d.get("stable_points", [])],
            history=[HealthSnapshot.from_dict(h) for h in d.get("history", [])],
            reason=d.get("reason", ""),
            created_at=d.get("created_at", time.time()),
            updated_at=d.get("updated_at", time.time()),
            version_counter=d.get("version_counter", 0),
        )
