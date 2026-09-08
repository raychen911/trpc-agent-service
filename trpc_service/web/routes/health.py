from fastapi import APIRouter, Request
from sqlalchemy import text

from trpc_service.web.models import HealthResponse, ReadinessResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", response_model=HealthResponse)
async def liveness(request: Request) -> HealthResponse:
    settings = request.app.state.settings
    return HealthResponse(service=settings.app_name, version=settings.app_version)


@router.get("/ready", response_model=ReadinessResponse)
async def readiness(request: Request) -> ReadinessResponse:
    settings = request.app.state.settings
    with request.app.state.database.session_factory() as session:
        session.execute(text("SELECT 1"))
    return ReadinessResponse(
        service=settings.app_name,
        version=settings.app_version,
        checks={"application": "ok", "database": "ok"},
    )
