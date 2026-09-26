"""渐进式发布控制面 HTTP 服务入口。

错误映射：每种 RolloutError.reason 映射到可区分的 HTTP 状态码与响应体。
"""
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from rollout.config import ControlPlaneConfig
from rollout.errors import RolloutError
from rollout.models import GatePolicy, GroupTarget, HealthMetrics
from rollout.service import RolloutService
from rollout.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

_STATUS_BY_REASON = {
    "plan_not_found": 404,
    "batch_not_found": 404,
    "illegal_transition": 409,
    "gate_blocked": 409,
    "group_conflict": 409,
    "validation_error": 422,
    "concurrency_limit": 429,
    "storage_corrupt": 500,
    "internal_error": 500,
}

_config = ControlPlaneConfig(
    storage_path=os.environ.get("ROLLOUT_STORAGE", "rollout_state.json")
)
_storage = Storage(_config.storage_path)
_service = RolloutService(_storage, _config)
_service.recover()  # 启动即恢复中断状态

app = FastAPI(title="rollout-control-plane")


@app.exception_handler(RolloutError)
async def rollout_error_handler(_: Request, exc: RolloutError):
    return JSONResponse(
        status_code=_STATUS_BY_REASON.get(exc.reason, 500),
        content={"error": exc.reason, "detail": str(exc)},
    )


class CreatePlanRequest(BaseModel):
    version: str
    group: str
    steps: list[int] = Field(..., min_length=1)
    gate: GatePolicy | None = None


class HealthReport(BaseModel):
    error_rate: float = 0.0
    unhealthy_ratio: float = 0.0


class RollbackRequest(BaseModel):
    reason: str = "manual"


class BatchTargetRequest(BaseModel):
    group: str
    version: str
    steps: list[int] = Field(..., min_length=1)
    gate: GatePolicy | None = None


class CreateBatchRequest(BaseModel):
    targets: list[BatchTargetRequest] = Field(..., min_length=1)


@app.get("/")
def read_root():
    return {"service": "rollout-control-plane", "status": "ok"}


@app.post("/plans", status_code=201)
def create_plan(req: CreatePlanRequest):
    plan = _service.create_plan(req.version, req.group, req.steps, req.gate)
    return _service.observe(plan.plan_id)


@app.post("/plans/{plan_id}/start")
def start_plan(plan_id: str):
    _service.start(plan_id)
    return _service.observe(plan_id)


@app.post("/plans/{plan_id}/advance")
def advance_plan(plan_id: str):
    _service.advance(plan_id)
    return _service.observe(plan_id)


@app.post("/plans/{plan_id}/run")
def run_plan(plan_id: str):
    _service.run_async(plan_id)
    return _service.observe(plan_id)


@app.post("/plans/{plan_id}/rollback")
def rollback_plan(plan_id: str, req: RollbackRequest | None = None):
    _service.rollback(plan_id, reason=(req.reason if req else "manual"))
    return _service.observe(plan_id)


@app.post("/plans/{plan_id}/abort")
def abort_plan(plan_id: str):
    _service.abort(plan_id)
    return _service.observe(plan_id)


@app.get("/plans/{plan_id}")
def get_plan(plan_id: str):
    return _service.observe(plan_id)


@app.get("/plans")
def list_plans():
    return [_service.observe(p.plan_id) for p in _storage.list_plans()]


@app.post("/groups/{group}/health")
def report_health(group: str, req: HealthReport):
    _service.report_health(group, HealthMetrics(
        error_rate=req.error_rate, unhealthy_ratio=req.unhealthy_ratio))
    return {"group": group, "recorded": True}


# ---- 多组协同发布（批次） ----

@app.post("/batches", status_code=201)
def create_batch(req: CreateBatchRequest):
    targets = [
        GroupTarget(group=t.group, version=t.version, steps=t.steps,
                    gate=t.gate or GatePolicy())
        for t in req.targets
    ]
    batch = _service.create_batch_plan(targets)
    return _service.observe_batch(batch.batch_id)


@app.post("/batches/{batch_id}/start")
def start_batch(batch_id: str):
    _service.start_batch(batch_id)
    return _service.observe_batch(batch_id)


@app.post("/batches/{batch_id}/advance")
def advance_batch(batch_id: str):
    _service.advance_batch(batch_id)
    return _service.observe_batch(batch_id)


@app.post("/batches/{batch_id}/run")
def run_batch(batch_id: str):
    _service.run_batch_async(batch_id)
    return _service.observe_batch(batch_id)


@app.post("/batches/{batch_id}/rollback")
def rollback_batch(batch_id: str, req: RollbackRequest | None = None):
    _service.rollback_batch(batch_id, reason=(req.reason if req else "manual"))
    return _service.observe_batch(batch_id)


@app.post("/batches/{batch_id}/abort")
def abort_batch(batch_id: str):
    _service.abort_batch(batch_id)
    return _service.observe_batch(batch_id)


@app.get("/batches/{batch_id}")
def get_batch(batch_id: str):
    return _service.observe_batch(batch_id)


@app.get("/batches")
def list_batches():
    return [_service.observe_batch(b.batch_id) for b in _storage.list_batches()]


@app.on_event("shutdown")
def _shutdown():
    _service.shutdown()
