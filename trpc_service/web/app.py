"""Operational HTTP surface for the phase-two Gateway."""

from __future__ import annotations

import asyncio

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from trpc_service.persistence import Database
from trpc_service.telemetry.runtime import registry, queue_snapshot, configure, shutdown as stop_telemetry
from trpc_service.operations.readiness import readiness


def create_app(database_url: str) -> FastAPI:
    database = Database(database_url)
    metrics = registry
    app = FastAPI(title="tRPC Agent multi-tenant gateway", version="0.2.0")
    app.state.database = database
    app.state.metrics = metrics

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, error):
        # Pydantic/FastAPI's default payload echoes invalid input, including a
        # mistakenly pasted secret. Validation failures must not expose that input.
        return JSONResponse({"detail": "request validation failed"}, status_code=422)

    from trpc_service.management import management_router
    app.include_router(management_router(database))

    @app.on_event("startup")
    async def startup() -> None:
        await asyncio.to_thread(database.check_schema)
        configure("gateway", database)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await asyncio.to_thread(stop_telemetry)
        database.close()

    @app.get("/health/live")
    async def live() -> dict:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(verbose: bool = Query(False)):
        healthy, checks = await asyncio.to_thread(readiness, database)
        if healthy:
            return {"status": "ready", "checks": checks}
        if not verbose:
            # Keep the stable probe contract for load balancers. Operators can
            # request the dependency breakdown with ?verbose=true.
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "unavailable", "checks": checks}, status_code=503)

    @app.get("/metrics")
    async def metric_snapshot() -> dict:
        return {"process": metrics.snapshot(), "queues": await asyncio.to_thread(queue_snapshot, database)}

    return app
