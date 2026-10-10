"""Non-leaking API error responses."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from trpc_service.runtime import BindingMismatch, UnknownBinding
from trpc_service.tenant.auth import AuthenticationError, AuthorizationError
from trpc_service.tenant.control import ControlVersionConflict, IdempotencyConflict


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AuthenticationError)
    async def authentication_error(request: Request, exc: AuthenticationError) -> JSONResponse:
        return JSONResponse({"error": "unauthenticated"}, status_code=401)

    @app.exception_handler(AuthorizationError)
    async def authorization_error(request: Request, exc: AuthorizationError) -> JSONResponse:
        return JSONResponse({"error": "forbidden"}, status_code=403)

    @app.exception_handler(UnknownBinding)
    async def unknown_binding(request: Request, exc: UnknownBinding) -> JSONResponse:
        return JSONResponse({"error": "not_found"}, status_code=404)

    @app.exception_handler(BindingMismatch)
    async def binding_mismatch(request: Request, exc: BindingMismatch) -> JSONResponse:
        return JSONResponse({"error": "invalid_callback"}, status_code=400)

    @app.exception_handler(IdempotencyConflict)
    async def idempotency_conflict(request: Request, exc: IdempotencyConflict) -> JSONResponse:
        return JSONResponse({"error": "idempotency_conflict"}, status_code=409)

    @app.exception_handler(ControlVersionConflict)
    async def version_conflict(request: Request, exc: ControlVersionConflict) -> JSONResponse:
        return JSONResponse({"error": "version_conflict"}, status_code=412)


__all__ = ["install_error_handlers"]
