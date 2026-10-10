"""Health and metrics endpoints."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

ReadinessCheck = Callable[[], Awaitable[bool]]


def create_health_router(readiness: ReadinessCheck | None = None) -> APIRouter:
    router = APIRouter(tags=["operations"])

    @router.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/health/ready")
    async def ready(response: Response) -> dict[str, str]:
        healthy = True if readiness is None else await readiness()
        if not healthy:
            response.status_code = 503
        return {"status": "ok" if healthy else "unavailable"}

    @router.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return router


__all__ = ["create_health_router"]
