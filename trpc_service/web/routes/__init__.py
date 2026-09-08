from trpc_service.web.routes.admin import router as admin_router
from trpc_service.web.routes.gateway import router as gateway_router
from trpc_service.web.routes.health import router as health_router
from trpc_service.web.routes.root import router as root_router

__all__ = ["admin_router", "gateway_router", "health_router", "root_router"]
