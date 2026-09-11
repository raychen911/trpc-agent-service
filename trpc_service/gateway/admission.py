"""Gateway tenant admission: resolve X-Tenant-ID into an AdmittedTenant + config version."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Header
from fastapi import HTTPException
from fastapi import Request

from trpc_service.config.tenant_repository import (
    TenantConfigRepository,
    TenantRepositoryDataError,
    TenantRepositoryUnavailableError,
)
from trpc_service.gateway.errors import ACCESS_DENIED_TEXT
from trpc_service.tenant.context import InvalidTenantIdError
from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.context import validate_tenant_id

WEB_USER_ID = "user_default"
WEB_CHANNEL = "web"
TENANT_UNAVAILABLE = "Tenant is not available."
TENANT_SERVICE_UNAVAILABLE = "Tenant service is temporarily unavailable."


@dataclass(frozen=True, slots=True)
class AdmittedTenant:
    """Result of Gateway tenant admission: context + config version for the task."""

    context: TenantContext
    config_version: int


async def resolve_gateway_tenant(
    request: Request,
    x_tenant_id: Annotated[str, Header(alias="X-Tenant-ID")],
) -> AdmittedTenant:
    """FastAPI dependency: validate header, query repository, return AdmittedTenant.

    Missing header -> 422 (FastAPI auto).
    Invalid format -> 422 with fixed message.
    Unknown/disabled -> 403 with fixed message.
    Repository unavailable -> 503 with fixed message.
    """
    try:
        validate_tenant_id(x_tenant_id)
    except InvalidTenantIdError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    repository: TenantConfigRepository = request.app.state.tenant_repository
    try:
        config = await repository.get(x_tenant_id)
    except (TenantRepositoryUnavailableError, TenantRepositoryDataError):
        raise HTTPException(status_code=503, detail=TENANT_SERVICE_UNAVAILABLE) from None

    if config is None or not config.enabled:
        raise HTTPException(status_code=403, detail=TENANT_UNAVAILABLE) from None

    # Stage 6A1 governance: the legacy web entry must use an admitted channel,
    # and whenever the tenant restricts users this anonymous compatibility
    # entry is denied outright — it cannot prove a real platform user identity.
    governance = config.governance
    if WEB_CHANNEL not in governance.allowed_channels or governance.allowed_user_ids:
        raise HTTPException(status_code=403, detail=ACCESS_DENIED_TEXT) from None

    context = TenantContext(
        tenant_id=x_tenant_id,
        app_id=config.app.app_id,
        user_id=WEB_USER_ID,
        channel=WEB_CHANNEL,
        session_id=None,
    )
    return AdmittedTenant(context=context, config_version=config.version)


__all__ = ["AdmittedTenant", "TENANT_UNAVAILABLE", "TENANT_SERVICE_UNAVAILABLE", "resolve_gateway_tenant"]
