"""FastAPI HTTP surface for the control plane."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .errors import (
    HealthGateBlocked,
    InvalidTransition,
    LimitExceeded,
    PlanAlreadyExists,
    PlanNotFound,
    QueueTimeout,
    RollbackStablePointMissing,
    RolloutError,
)
from .manager import RolloutManager
from .models import Plan


class CreatePlanRequest(BaseModel):
    plan_id: str = Field(min_length=1)
    app: str = Field(min_length=1)
    version: str = Field(min_length=1)
    target_group: str = Field(min_length=1)
    from_version: str = ""
    step_weight: int = 10
    gate_required: bool = True


class ActionRequest(BaseModel):
    reason: str = ""


def _plan_view(p: Plan) -> dict:
    return p.to_dict()


def create_app(manager: RolloutManager) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Recover interrupted plans when the process starts.
        app.state.recovery = manager.recover()
        yield

    app = FastAPI(title="Progressive Rollout Control Plane", version="0.1.0",
                  lifespan=lifespan)

    def fail(exc: RolloutError, status_code: int) -> HTTPException:
        return HTTPException(status_code=status_code, detail={
            "code": exc.code,
            "message": str(exc),
            **({"reason": exc.reason}
               if isinstance(exc, InvalidTransition) else {}),
            **({"evidence": exc.evidence, "weight": exc.weight}
               if isinstance(exc, HealthGateBlocked) else {}),
            **({"kind": exc.kind, "limit": exc.limit, "actual": exc.actual}
               if isinstance(exc, LimitExceeded) else {}),
        })

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True,
                "active": manager._scheduler.active_count(),
                "queued": manager._scheduler.queued_count()}

    @app.post("/plans", status_code=201)
    def create_plan(req: CreatePlanRequest) -> dict:
        try:
            p = manager.create_plan(
                req.plan_id, req.app, req.version, req.target_group,
                step_weight=req.step_weight,
                from_version=req.from_version,
                gate_required=req.gate_required)
        except PlanAlreadyExists as e:
            raise fail(e, 409)
        except LimitExceeded as e:
            raise fail(e, 422)
        except ValueError as e:
            raise HTTPException(status_code=422, detail={"message": str(e)})
        return _plan_view(p)

    @app.get("/plans")
    def list_plans() -> dict:
        return {"plans": [_plan_view(p) for p in manager.list_plans()]}

    @app.get("/plans/{plan_id}")
    def get_plan(plan_id: str) -> dict:
        try:
            return _plan_view(manager.get_plan(plan_id))
        except PlanNotFound as e:
            raise fail(e, 404)

    @app.get("/plans/{plan_id}/status")
    def plan_status(plan_id: str) -> dict:
        try:
            return manager.status(plan_id)
        except PlanNotFound as e:
            raise fail(e, 404)

    @app.post("/plans/{plan_id}/promote")
    def promote(plan_id: str) -> dict:
        try:
            manager.promote(plan_id)
            return manager.status(plan_id)
        except PlanNotFound as e:
            raise fail(e, 404)
        except InvalidTransition as e:
            raise fail(e, 409)
        except LimitExceeded as e:
            raise fail(e, 429 if e.kind in ("concurrent_rollouts",
                                           "queue_depth",
                                           "scheduler_closed") else 422)
        except QueueTimeout as e:
            raise fail(e, 503)

    @app.post("/plans/{plan_id}/abort")
    def abort(plan_id: str, req: ActionRequest) -> dict:
        try:
            manager.abort(plan_id, reason=req.reason or "manual abort")
            return manager.status(plan_id)
        except PlanNotFound as e:
            raise fail(e, 404)
        except InvalidTransition as e:
            raise fail(e, 409)

    @app.post("/plans/{plan_id}/rollback")
    def rollback(plan_id: str) -> dict:
        try:
            manager.rollback(plan_id)
            return manager.status(plan_id)
        except PlanNotFound as e:
            raise fail(e, 404)
        except InvalidTransition as e:
            raise fail(e, 409)
        except RollbackStablePointMissing as e:
            raise fail(e, 409)

    @app.get("/plans/{plan_id}/events")
    def events(plan_id: str) -> dict:
        try:
            manager.get_plan(plan_id)
        except PlanNotFound as e:
            raise fail(e, 404)
        return {"plan_id": plan_id, "events": manager.events(plan_id)}

    @app.get("/events")
    def all_events() -> dict:
        return {"events": manager.events(None)}

    return app
