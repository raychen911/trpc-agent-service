"""Gateway public HTTP routes: health, UI, sync chat, SSE stream."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from contextlib import aclosing
from pathlib import Path
from typing import Any

from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse

from trpc_service.gateway.admission import AdmittedTenant
from trpc_service.gateway.admission import resolve_gateway_tenant
from trpc_service.gateway.client import WorkerClient
from trpc_service.gateway.client import WorkerClientError
from trpc_service.gateway.errors import (
    SAFE_ERROR_TEXT,
    map_worker_error,
)
from trpc_service.transport.models import (
    WorkerTask, )
from trpc_service.web.schemas import ChatRequest
from trpc_service.web.schemas import ChatResponse

_STATIC_DIR = Path(__file__).resolve().parent.parent / "web" / "static"


def _public_sse(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def register_gateway_routes(app: FastAPI, worker_client: WorkerClient) -> None:
    """Mount public Gateway routes. Idempotent per FastAPI instance."""
    if getattr(app.state, "_gateway_routes_registered", False):
        return
    app.state._gateway_routes_registered = True

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        html_path = _STATIC_DIR / "index.html"
        return FileResponse(html_path, media_type="text/html")

    @app.post("/api/chat", response_model=ChatResponse)
    async def chat(
            req: ChatRequest,
            admitted: AdmittedTenant = Depends(resolve_gateway_tenant),
    ) -> ChatResponse:
        task = WorkerTask(
            protocol_version=1,
            request_id=uuid.uuid4(),
            tenant_id=admitted.context.tenant_id,
            app_id=admitted.context.app_id,
            config_version=admitted.config_version,
            user_id=admitted.context.user_id,
            channel=admitted.context.channel,
            session_id=req.session_id,
            message_id=req.message_id,
            message=req.message,
        )
        try:
            result = await worker_client.chat(task)
        except WorkerClientError as exc:
            return ChatResponse(session_id=req.session_id, response=map_worker_error(exc.code))
        except Exception:  # noqa: BLE001
            return ChatResponse(session_id=req.session_id, response=SAFE_ERROR_TEXT)

        if result.error_code is not None:
            return ChatResponse(session_id=req.session_id, response=map_worker_error(result.error_code))
        return ChatResponse(session_id=req.session_id, response=result.response)

    @app.post("/api/chat/stream")
    async def chat_stream(
            req: ChatRequest,
            request: Request,
            admitted: AdmittedTenant = Depends(resolve_gateway_tenant),
    ) -> StreamingResponse:
        task = WorkerTask(
            protocol_version=1,
            request_id=uuid.uuid4(),
            tenant_id=admitted.context.tenant_id,
            app_id=admitted.context.app_id,
            config_version=admitted.config_version,
            user_id=admitted.context.user_id,
            channel=admitted.context.channel,
            session_id=req.session_id,
            message_id=req.message_id,
            message=req.message,
        )

        async def _public_event_stream() -> AsyncIterator[str]:
            try:
                # aclosing: disconnect detected mid-loop (the is_disconnected
                # return) or any early return closes the worker SSE client
                # stream now, ending its CLIENT span promptly.
                async with aclosing(worker_client.stream(task)) as events:
                    async for event in events:
                        if await request.is_disconnected():
                            return
                        if event.type == "error":
                            yield _public_sse({
                                "type": "error",
                                "data": map_worker_error(event.error_code),
                                "session_id": req.session_id,
                            })
                            return
                        if event.type == "delta":
                            yield _public_sse({
                                "type": "delta",
                                "data": event.data,
                                "session_id": req.session_id,
                            })
                        elif event.type == "tool":
                            yield _public_sse({
                                "type": "tool",
                                "data": {
                                    "kind":
                                    event.data.kind,
                                    "name":
                                    event.data.name,
                                    "args" if event.data.kind == "call" else "response":
                                    (event.data.args if event.data.kind == "call" else event.data.response),
                                },
                                "session_id": req.session_id,
                            })
                        elif event.type == "done":
                            yield _public_sse({
                                "type": "done",
                                "data": None,
                                "session_id": req.session_id,
                            })
                            return
            except WorkerClientError as exc:
                yield _public_sse({
                    "type": "error",
                    "data": map_worker_error(exc.code),
                    "session_id": req.session_id,
                })
            except Exception:  # noqa: BLE001
                yield _public_sse({
                    "type": "error",
                    "data": SAFE_ERROR_TEXT,
                    "session_id": req.session_id,
                })

        return StreamingResponse(
            _public_event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no"
            },
        )


__all__ = [
    "register_gateway_routes",
]
