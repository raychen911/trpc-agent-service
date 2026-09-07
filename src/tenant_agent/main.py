"""FastAPI application factory and role lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from tenant_agent.api import install_routes
from tenant_agent.container import ApplicationContainer
from tenant_agent.observability import setup_telemetry
from tenant_agent.security import configure_safe_logging
from tenant_agent.settings import ServiceRole, Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    container = ApplicationContainer.build(settings)
    setup_telemetry(settings, container.redactor)
    configure_safe_logging(settings.log_level, container.redactor)
    stop = asyncio.Event()
    tasks: list[asyncio.Task[None]] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        try:
            await container.initialize()
            role = settings.service_role
            if role in {ServiceRole.ALL, ServiceRole.WORKER}:
                tasks.append(asyncio.create_task(container.worker.run_forever(stop), name="agent-worker"))
            if role in {ServiceRole.ALL, ServiceRole.OUTBOX}:
                tasks.append(asyncio.create_task(container.outbox.run_forever(stop), name="outbox-worker"))
                tasks.append(
                    asyncio.create_task(
                        container.audit_maintenance.run_forever(stop),
                        name="audit-maintenance",
                    )
                )
                tasks.append(
                    asyncio.create_task(container.wecom_bot.run_forever(stop), name="wecom-bot-manager")
                )
            yield
        finally:
            stop.set()
            if tasks:
                await asyncio.wait(tasks, timeout=settings.shutdown_grace_seconds)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            await container.close()

    app = FastAPI(
        title="Multi-Tenant tRPC-Agent Platform",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url=None,
    )
    app.state.container = container

    @app.exception_handler(RequestValidationError)
    async def safe_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        del request
        details = [
            {
                "location": [str(part) for part in error.get("loc", ())],
                "type": str(error.get("type", "validation_error")),
                "message": container.redactor.text(str(error.get("msg", "invalid input"))),
            }
            for error in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": details})

    install_routes(app)
    FastAPIInstrumentor.instrument_app(app, excluded_urls="health/live,health/ready,metrics")
    return app
