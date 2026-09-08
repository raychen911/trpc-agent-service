from fastapi import APIRouter, Request

from trpc_service.web.models import ServiceInfo

router = APIRouter(tags=["service"])


@router.get("/", response_model=ServiceInfo)
async def service_info(request: Request) -> ServiceInfo:
    settings = request.app.state.settings
    return ServiceInfo(
        name=settings.app_name,
        version=settings.app_version,
        environment=settings.environment,
        docs_url=request.app.docs_url or "",
    )
