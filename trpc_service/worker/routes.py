"""Worker internal HTTP routes."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import aclosing

from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from trpc_service.transport.auth import InternalToken
from trpc_service.transport.models import INTERNAL_TOKEN_HEADER
from trpc_service.transport.models import WorkerApprovalTask
from trpc_service.transport.models import WorkerTask
from trpc_service.worker.service import WorkerService

_UNAUTHORIZED_BODY = {"detail": "Internal authentication required."}
_BAD_REQUEST_BODY = {"detail": "Invalid internal request body."}
_UNAVAILABLE_BODY = {"detail": "Service is temporarily unavailable."}


def _check_token(request: Request, token: InternalToken) -> JSONResponse | None:
    candidate = request.headers.get(INTERNAL_TOKEN_HEADER)
    if not token.matches(candidate):
        return JSONResponse(status_code=401, content=_UNAUTHORIZED_BODY)
    return None


def register_worker_routes(
    app: FastAPI,
    worker_service: WorkerService,
    internal_token: InternalToken,
    approval_service=None,
) -> None:
    if getattr(app.state, "_worker_routes_registered", False):
        return
    app.state._worker_routes_registered = True

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/internal/v1/chat")
    async def chat(request: Request) -> JSONResponse:
        auth_error = _check_token(request, internal_token)
        if auth_error is not None:
            return auth_error

        try:
            body = await request.json()
            task = WorkerTask.model_validate(body)
        except (ValidationError, json.JSONDecodeError, ValueError):
            return JSONResponse(status_code=422, content=_BAD_REQUEST_BODY)

        result = await worker_service.chat(task)
        return JSONResponse(content=result.model_dump(mode="json"))

    @app.post("/internal/v1/chat/stream", response_model=None)
    async def chat_stream(request: Request) -> JSONResponse | StreamingResponse:
        auth_error = _check_token(request, internal_token)
        if auth_error is not None:
            return auth_error

        try:
            body = await request.json()
            task = WorkerTask.model_validate(body)
        except (ValidationError, json.JSONDecodeError, ValueError):
            return JSONResponse(status_code=422, content=_BAD_REQUEST_BODY)

        async def _event_stream() -> AsyncIterator[str]:
            from trpc_service.transport.sse import encode_worker_event
            # aclosing propagates a client disconnect (StreamingResponse
            # closes this generator) down to the agent turn so its spans
            # end immediately instead of at GC time.
            async with aclosing(worker_service.stream(task)) as events:
                async for event in events:
                    yield encode_worker_event(event)

        return StreamingResponse(
            _event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no"
            },
        )

    @app.post("/internal/v1/approvals/decide")
    async def approvals_decide(request: Request) -> JSONResponse:
        auth_error = _check_token(request, internal_token)
        if auth_error is not None:
            return auth_error
        if approval_service is None:
            return JSONResponse(status_code=503, content=_UNAVAILABLE_BODY)

        try:
            body = await request.json()
            task = WorkerApprovalTask.model_validate(body)
        except (ValidationError, json.JSONDecodeError, ValueError):
            return JSONResponse(status_code=422, content=_BAD_REQUEST_BODY)

        result = await approval_service.decide(task)
        return JSONResponse(content=result.model_dump(mode="json"))


__all__ = ["register_worker_routes"]
