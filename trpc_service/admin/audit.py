"""Helpers for append-only control-plane audit records."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.auth import ManagementActor
from trpc_service.admin.models import ManagementAuditLog


def append_management_audit(
    session: AsyncSession,
    actor: ManagementActor,
    *,
    action: str,
    resource_type: str,
    resource_id: str,
    tenant_id: UUID | None = None,
    reason: str | None = None,
    details_redacted: dict[str, Any] | None = None,
) -> None:
    """Append a redacted audit fact in the caller's existing transaction."""

    now = datetime.now(timezone.utc)
    all_roles = set(actor.roles)
    for roles in actor.tenant_roles.values():
        all_roles.update(roles)
    session.add(
        ManagementAuditLog(
            tenant_id=tenant_id,
            actor_subject=actor.subject,
            actor_roles=sorted(all_roles),
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            reason=reason,
            details_redacted=details_redacted or {},
            occurred_at=now,
            created_at=now,
        ))
