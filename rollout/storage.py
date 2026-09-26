"""本地持久化：JSON 文件 + 原子写，模拟版本/实例组/权重的持久存储。

- 写路径：先写临时文件并 fsync，再 os.replace 原子替换，保证刷新/中断
  不会留下半更新状态（要么旧文件，要么新文件）。
- 读路径：格式版本不兼容时抛出 StorageCorruptError，明确拒绝而非静默
  按默认值继续；旧版本缺失字段由 models.plan_from_dict 提供确定缺省。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import asdict

from .errors import StorageCorruptError
from .models import (
    BatchPlan,
    HealthMetrics,
    InstanceGroupState,
    RolloutPlan,
    batch_from_dict,
    batch_to_dict,
    plan_from_dict,
    plan_to_dict,
)

log = logging.getLogger("rollout.storage")

_TOP_LEVEL_VERSION = 3  # 存储文件顶层格式版本（v3 起新增 batches，读取兼容 v1/v2）
_SUPPORTED_TOP_LEVEL_VERSIONS = (1, 2, _TOP_LEVEL_VERSION)


class Storage:
    """进程重启后仍可恢复的持久化存储。"""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._plans: dict[str, RolloutPlan] = {}
        self._batches: dict[str, BatchPlan] = {}
        self._groups: dict[str, InstanceGroupState] = {}
        self._health: dict[str, HealthMetrics] = {}

    # ---- 计划 ----
    def save_plan(self, plan: RolloutPlan) -> None:
        with self._lock:
            self._plans[plan.plan_id] = plan
            self.flush()

    def get_plan(self, plan_id: str) -> RolloutPlan | None:
        with self._lock:
            return self._plans.get(plan_id)

    def list_plans(self) -> list[RolloutPlan]:
        with self._lock:
            return list(self._plans.values())

    # ---- 批次（多组协同发布） ----
    def save_batch(self, batch: BatchPlan) -> None:
        with self._lock:
            self._batches[batch.batch_id] = batch
            self.flush()

    def get_batch(self, batch_id: str) -> BatchPlan | None:
        with self._lock:
            return self._batches.get(batch_id)

    def list_batches(self) -> list[BatchPlan]:
        with self._lock:
            return list(self._batches.values())

    def save_batch_with_groups(self, batch: BatchPlan,
                               groups: list[InstanceGroupState]) -> None:
        """批次状态与多个组状态在同一把锁内更新并一次原子落盘。

        对外（任何经 get_group/get_batch 的读取）只能看到更新前或更新后
        的快照，不会看到「部分组已变更」的中间状态。
        """
        with self._lock:
            self._batches[batch.batch_id] = batch
            for g in groups:
                self._groups[g.group] = g
            self.flush()

    # ---- 实例组 ----
    def save_group(self, group: InstanceGroupState) -> None:
        with self._lock:
            self._groups[group.group] = group
            self.flush()

    def get_group(self, name: str) -> InstanceGroupState | None:
        with self._lock:
            return self._groups.get(name)

    # ---- 健康指标 ----
    def save_health(self, group: str, metrics: HealthMetrics) -> None:
        with self._lock:
            self._health[group] = metrics
            self.flush()

    def get_health(self, group: str) -> HealthMetrics:
        with self._lock:
            return self._health.get(group, HealthMetrics())

    # ---- 生命周期 ----
    def load(self) -> None:
        """从磁盘加载。文件不存在视为空存储；格式不兼容明确拒绝。"""
        with self._lock:
            if not os.path.exists(self.path):
                log.info("storage file %s absent, starting empty", self.path)
                return
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                raise StorageCorruptError(f"cannot read storage {self.path}: {e}") from e
            if not isinstance(raw, dict):
                raise StorageCorruptError("storage root must be an object")
            version = raw.get("format_version", 1)
            if version not in _SUPPORTED_TOP_LEVEL_VERSIONS:
                raise StorageCorruptError(
                    f"unsupported storage format_version: {version!r}"
                )
            try:
                self._plans = {
                    p["plan_id"]: plan_from_dict(p)
                    for p in raw.get("plans", [])
                }
                # v1/v2 没有 batches 字段：确定缺省为空集合
                self._batches = {
                    b["batch_id"]: batch_from_dict(b)
                    for b in raw.get("batches", [])
                }
                self._groups = {
                    g["group"]: InstanceGroupState(
                        group=g["group"],
                        version=g["version"],
                        # v1 可能缺少 weight / instances_healthy，使用确定缺省
                        weight=int(g.get("weight", 0)),
                        instances_healthy=bool(g.get("instances_healthy", True)),
                    )
                    for g in raw.get("groups", [])
                }
                self._health = {
                    h["group"]: HealthMetrics(
                        error_rate=float(h.get("error_rate", 0.0)),
                        unhealthy_ratio=float(h.get("unhealthy_ratio", 0.0)),
                        reported=bool(h.get("reported", False)),
                    )
                    for h in raw.get("health", [])
                }
            except (KeyError, TypeError, ValueError) as e:
                raise StorageCorruptError(f"incompatible storage data: {e}") from e
            log.info(
                "loaded storage: %d plans, %d batches, %d groups",
                len(self._plans), len(self._batches), len(self._groups),
            )

    def flush(self) -> None:
        """原子落盘：tmp 文件 + fsync + os.replace。"""
        with self._lock:
            payload = {
                "format_version": _TOP_LEVEL_VERSION,
                "plans": [plan_to_dict(p) for p in self._plans.values()],
                "batches": [batch_to_dict(b) for b in self._batches.values()],
                "groups": [asdict(g) for g in self._groups.values()],
                "health": [
                    {"group": name, **asdict(m)} for name, m in self._health.items()
                ],
            }
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
