# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""FastAPI router exposing tenant CRUD, rollback and audit inspection."""

from __future__ import annotations

import hmac
import logging
import os
import socket
from importlib.resources import files
from typing import Any
from typing import Optional

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from trpc_service.log import AuditLogger
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.metrics import PrometheusMetricsReader
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager

logger = logging.getLogger(__name__)


class MutationMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    by: str = "admin"
    reason: str = ""


class TenantMutation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant: Tenant
    metadata: MutationMetadata = Field(default_factory=MutationMetadata)


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    by: str = "admin"


def create_admin_router(
    *,
    manager: TenantConfigManager,
    audit_logger: Optional[AuditLogger] = None,
    api_key: Optional[str] = None,
    metrics: Optional[EnterpriseMetrics] = None,
    prometheus_url: Optional[str] = None,
    prometheus_reader: Optional[Any] = None,
) -> APIRouter:
    """Create the local/production Admin API router.

    When ``api_key`` is configured every endpoint requires the matching
    ``X-Admin-API-Key`` header. Local development may omit the key, while a
    deployed instance must always provide one through secret management.
    """
    router = APIRouter(prefix="/admin", tags=["admin"])
    metrics = metrics or get_enterprise_metrics()
    prometheus_url = prometheus_url or os.environ.get("PROMETHEUS_URL")
    prometheus_reader = prometheus_reader or (PrometheusMetricsReader(prometheus_url) if prometheus_url else None)

    @router.get("", include_in_schema=False)
    async def admin_root() -> RedirectResponse:
        return RedirectResponse(url="/admin/ui", status_code=307)

    @router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    async def admin_ui() -> HTMLResponse:
        html = files("trpc_service.web.admin").joinpath("_ui.html").read_text(encoding="utf-8")
        return HTMLResponse(
            html,
            headers={
                "Cache-Control":
                "no-store",
                "Content-Security-Policy":
                ("default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
                 "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; "
                 "frame-ancestors 'none'"),
                "Referrer-Policy":
                "no-referrer",
                "X-Content-Type-Options":
                "nosniff",
            },
        )

    def authorize(x_admin_api_key: Optional[str]) -> None:
        if api_key is not None and not hmac.compare_digest(x_admin_api_key or "", api_key):
            raise HTTPException(status_code=401, detail="invalid admin credential")

    @router.get("/health")
    async def health(x_admin_api_key: Optional[str] = Header(default=None)) -> dict[str, str]:
        authorize(x_admin_api_key)
        return {"status": "ok"}

    @router.get("/tenants")
    async def list_tenants(
            include_disabled: bool = False,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> list[dict[str, Any]]:
        authorize(x_admin_api_key)
        return [tenant.model_dump(mode="json") for tenant in manager.list(active_only=not include_disabled)]

    @router.get("/tenants/{tenant_id}")
    async def get_tenant(
            tenant_id: str,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        authorize(x_admin_api_key)
        tenant = manager.get(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail="tenant not found")
        return tenant.model_dump(mode="json")

    @router.post("/tenants", status_code=201)
    async def create_tenant(
            mutation: TenantMutation,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        authorize(x_admin_api_key)
        try:
            tenant = manager.register(
                mutation.tenant,
                by=mutation.metadata.by,
                reason=mutation.metadata.reason or "create via Admin API",
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return tenant.model_dump(mode="json")

    @router.put("/tenants/{tenant_id}")
    async def update_tenant(
            tenant_id: str,
            mutation: TenantMutation,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        authorize(x_admin_api_key)
        if mutation.tenant.tenant_id != tenant_id:
            raise HTTPException(status_code=400, detail="tenant id in path and body must match")
        try:
            tenant = manager.update(
                mutation.tenant,
                by=mutation.metadata.by,
                reason=mutation.metadata.reason or "update via Admin API",
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return tenant.model_dump(mode="json")

    @router.delete("/tenants/{tenant_id}", status_code=204, response_class=Response)
    async def delete_tenant(
            tenant_id: str,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> Response:
        authorize(x_admin_api_key)
        try:
            manager.delete(tenant_id, by="admin", reason="delete via Admin API")
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return Response(status_code=204)

    @router.get("/tenants/{tenant_id}/history")
    async def tenant_history(
            tenant_id: str,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> list[dict[str, Any]]:
        authorize(x_admin_api_key)
        return [version.model_dump(mode="json") for version in manager.history(tenant_id)]

    @router.post("/tenants/{tenant_id}/rollback")
    async def rollback_tenant(
            tenant_id: str,
            request: RollbackRequest,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        authorize(x_admin_api_key)
        try:
            tenant = manager.rollback(tenant_id, request.version, by=request.by)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return tenant.model_dump(mode="json")

    @router.get("/audit")
    async def query_audit(
            tenant_id: Optional[str] = None,
            tool_name: Optional[str] = None,
            decision: Optional[str] = None,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> list[dict[str, Any]]:
        authorize(x_admin_api_key)
        if audit_logger is None:
            return []
        entries = await audit_logger.query(
            tenant_id=tenant_id,
            tool_name=tool_name,
            decision=decision,
        )
        return [entry.model_dump(mode="json") for entry in entries]

    @router.get("/metrics")
    async def query_metrics(
            tenant_id: Optional[str] = None,
            x_admin_api_key: Optional[str] = Header(default=None),
    ) -> dict[str, Any]:
        authorize(x_admin_api_key)
        snapshot = metrics.snapshot(tenant_id)
        scope = "process"
        error = None
        if prometheus_reader is not None:
            try:
                snapshot = await prometheus_reader.snapshot(tenant_id)
                scope = "prometheus"
            except Exception as exc:  # noqa: BLE001 - keep local diagnostics available during monitoring outages
                error = type(exc).__name__
                logger.warning("failed to query Prometheus metrics; using the process-local snapshot", exc_info=True)
        return {
            "scope":
            scope,
            "service_name":
            "all" if scope == "prometheus" else os.environ.get("OTEL_SERVICE_NAME", "trpc-agent-gateway"),
            "instance_id":
            None if scope == "prometheus" else os.environ.get("OTEL_SERVICE_INSTANCE_ID") or os.environ.get("HOSTNAME")
            or socket.gethostname(),
            "source_error":
            error,
            **snapshot,
        }

    return router
