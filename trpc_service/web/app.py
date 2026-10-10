"""Role-specific FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from trpc_service.version import __version__
from trpc_service.web.errors import install_error_handlers
from trpc_service.web.health import ReadinessCheck, create_health_router


class _BodyLimitMiddleware:
    """Reject oversized requests before Starlette buffers JSON bodies."""

    def __init__(self, app: ASGIApp, *, max_bytes: int = 4 * 1024 * 1024) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length is not None:
            try:
                too_large = int(content_length) > self.max_bytes
            except (TypeError, ValueError):
                too_large = True
            if too_large:
                response = JSONResponse({"error": "request_too_large"}, status_code=413)
                await response(scope, receive, send)
                return

        seen = 0

        async def limited_receive() -> Message:
            nonlocal seen
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"")
                seen += len(body)
                if seen > self.max_bytes:
                    raise _RequestBodyTooLarge
            return message

        try:
            await self.app(scope, limited_receive, send)
        except _RequestBodyTooLarge:
            response = JSONResponse({"error": "request_too_large"}, status_code=413)
            await response(scope, receive, send)


class _RequestBodyTooLarge(Exception):
    pass


def create_base_app(*, title: str, readiness: ReadinessCheck | None = None) -> FastAPI:
    app = FastAPI(
        title=title,
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(_BodyLimitMiddleware)

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Pydantic's default detail echoes invalid input.  Admin/channel
        # payloads may contain secret references or provider content, so use a
        # stable generic response instead.
        del request, exc
        return JSONResponse({"error": "invalid_request"}, status_code=422)

    app.include_router(create_health_router(readiness))
    install_error_handlers(app)
    return app


__all__ = ["create_base_app"]
