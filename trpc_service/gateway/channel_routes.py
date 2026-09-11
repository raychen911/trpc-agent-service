"""Gateway console HTTP routes for the Web Console Adapter."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, StrictStr

from trpc_service.channels.adapter import AdapterRegistry
from trpc_service.gateway.channel_service import (
    ChannelAccessDeniedError,
    ChannelIngressService,
    ChannelTenantFormatError,
    ChannelTenantNotFoundError,
    ChannelTenantUnavailableError,
)

CONSOLE_VALIDATION_ERROR_TEXT = "Invalid request format."


class ConsoleChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: StrictStr
    user_id: StrictStr
    conversation_id: StrictStr
    message_id: StrictStr
    message: StrictStr


def _public_sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _console_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    if request.url.path.startswith("/api/console/"):
        return JSONResponse(
            status_code=422,
            content={"detail": CONSOLE_VALIDATION_ERROR_TEXT},
        )
    return await request_validation_exception_handler(request, exc)


def register_console_routes(
    app: FastAPI,
    channel_service: ChannelIngressService,
    adapter_registry: AdapterRegistry,
) -> None:
    """Mount console HTTP routes. Idempotent per FastAPI instance."""
    if getattr(app.state, "_console_routes_registered", False):
        return
    app.state._console_routes_registered = True

    app.add_exception_handler(RequestValidationError, _console_validation_exception_handler)

    adapter = adapter_registry.get("web_console")

    @app.post("/api/console/messages")
    async def console_chat(req: ConsoleChatRequest) -> dict[str, Any]:
        payload = {
            "tenant_id": req.tenant_id,
            "user_id": req.user_id,
            "conversation_id": req.conversation_id,
            "message_id": req.message_id,
            "message": req.message,
        }
        try:
            inbound = adapter.decode(payload)
        except ValueError:
            raise HTTPException(status_code=422, detail=CONSOLE_VALIDATION_ERROR_TEXT) from None

        try:
            reply = await channel_service.chat(inbound)
        except ChannelTenantFormatError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        except ChannelAccessDeniedError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from None
        except ChannelTenantNotFoundError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from None
        except ChannelTenantUnavailableError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from None

        return adapter.encode_sync(reply.response)

    @app.post("/api/console/messages/stream")
    async def console_stream(req: ConsoleChatRequest) -> StreamingResponse:
        payload = {
            "tenant_id": req.tenant_id,
            "user_id": req.user_id,
            "conversation_id": req.conversation_id,
            "message_id": req.message_id,
            "message": req.message,
        }
        try:
            inbound = adapter.decode(payload)
        except ValueError:
            raise HTTPException(status_code=422, detail=CONSOLE_VALIDATION_ERROR_TEXT) from None

        async def _event_generator() -> AsyncIterator[str]:
            async for event in channel_service.stream(inbound):
                encoded = adapter.encode_event(event)
                yield _public_sse(encoded)
                if event.type in ("done", "error"):
                    return

        return StreamingResponse(
            _event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no"
            },
        )


__all__ = ["register_console_routes"]
