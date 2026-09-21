"""渐进式发布控制面 HTTP 服务入口。"""
from fastapi import FastAPI

from rollout.config import ControlPlaneConfig
from rollout.service import RolloutService
from rollout.storage import Storage

app = FastAPI(title="rollout-control-plane")

_config = ControlPlaneConfig()
_storage = Storage(_config.storage_path)
_service = RolloutService(_storage, _config)


@app.get("/")
def read_root():
    return {"service": "rollout-control-plane", "status": "ok"}
