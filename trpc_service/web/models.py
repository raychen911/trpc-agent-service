from typing import Literal

from pydantic import BaseModel, ConfigDict


class ServiceInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    version: str
    environment: str
    docs_url: str


class HealthResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: str
    version: str


class ReadinessResponse(HealthResponse):
    checks: dict[str, Literal["ok"]]
