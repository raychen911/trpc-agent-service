"""Tenant-scoped audit evidence, bounded exports and audited access."""

from datetime import datetime, timezone
import json
import re
import uuid

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from sqlalchemy import select

from trpc_service.persistence.models import AuditLogRow, TenantRow, TenantConfigVersionRow
from trpc_service.tenant.models import AuditPolicy
from .admin_auth import current_actor, principal

_fields = ("audit_id", "tenant_id", "channel", "user_id", "session_id", "agent_name", "tool_name", "decision",
           "latency_ms", "error_type", "cost", "trace_id", "request_id", "config_version", "policy_version", "redacted",
           "created_at")
_metadata = {
    "execution_id", "operation_id", "binding_id", "profile_id", "migration_id", "action_id", "attempt_id", "actor_id",
    "agent_app_id", "currency", "price_id", "amount", "evidence_hash", "revision", "authz_epoch",
    "previous_active_version", "delivery_id", "target_version", "percent", "count", "offset", "archive_id",
    "sequence", "row_count", "sha256", "previous_archive_id", "retention_days", "source"
}


def evidence(row):
    value = {name: getattr(row, name) for name in _fields}
    value["actor"] = row.user_id  # Keep the existing API field.
    value["metadata"] = {
        key: item
        for key, item in (row.metadata_json or {}).items() if key in _metadata and (
            isinstance(item, (int, bool)) or isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", item))
    }
    return value


def authorize_audit(session, tenant_id):
    identity = principal.get()
    if identity and identity["role"] == "operator":
        return
    if not identity or tenant_id not in identity.get("tenant_ids", []):
        raise HTTPException(403, "audit scope denied")
    tenant = session.get(TenantRow, tenant_id)
    config = session.get(TenantConfigVersionRow, (tenant_id, tenant.active_config_version)) if tenant else None
    policy = AuditPolicy.model_validate(config.config_json.get("audit_policy", {})) if config else AuditPolicy()
    if identity["role"] not in policy.allowed_viewer_roles:
        raise HTTPException(403, "role is not allowed by tenant audit policy")


def audit_router(database):
    router = APIRouter()

    def read(tenant_id, limit, offset, decision, trace_id, session_id, tool_name, actor, since, until, export=False):
        from trpc_service.telemetry.runtime import current_trace_id
        from trpc_service.telemetry.logging import current_fields
        if any(value is not None and value.utcoffset() is None for value in (since, until)):
            raise HTTPException(422, "audit time filters require a timezone")
        if since and until and since > until:
            raise HTTPException(422, "invalid audit time range")
        with database.sessions.begin() as session:
            authorize_audit(session, tenant_id)
            query = select(AuditLogRow).where(AuditLogRow.tenant_id == tenant_id)
            for field, value in (("decision", decision), ("trace_id", trace_id), ("session_id", session_id),
                                 ("tool_name", tool_name), ("user_id", actor)):
                if value is not None:
                    query = query.where(getattr(AuditLogRow, field) == value)
            if since:
                query = query.where(AuditLogRow.created_at >= since)
            boundary = until or datetime.now(timezone.utc)
            query = query.where(AuditLogRow.created_at <= boundary)
            ordering = AuditLogRow.created_at if export else AuditLogRow.created_at.desc()
            rows = list(
                session.scalars(
                    query.order_by(ordering, AuditLogRow.audit_id).offset(offset).limit(limit + int(export))))
            result = [evidence(row) for row in rows[:limit]]
            session.add(
                AuditLogRow(audit_id=uuid.uuid4().hex,
                            tenant_id=tenant_id,
                            user_id=current_actor(),
                            decision="audit_exported" if export else "audit_read",
                            trace_id=current_trace_id(),
                            request_id=current_fields().get("request_id"),
                            redacted=True,
                            metadata_json={
                                "count": len(result),
                                "offset": offset
                            }))
        return result, boundary, len(rows) > limit

    @router.get("/tenants/{tenant_id}/audit")
    def audit(tenant_id: str,
              response: Response,
              limit: int = Query(100, ge=1, le=500),
              offset: int = Query(0, ge=0),
              decision: str | None = Query(None, max_length=64),
              trace_id: str | None = Query(None, max_length=64),
              session_id: str | None = Query(None, max_length=128),
              tool_name: str | None = Query(None, max_length=128),
              actor: str | None = Query(None, max_length=128),
              since: datetime | None = None,
              until: datetime | None = None):
        response.headers["Cache-Control"] = "no-store"
        return read(tenant_id, limit, offset, decision, trace_id, session_id, tool_name, actor, since, until)[0]

    @router.get("/tenants/{tenant_id}/audit/export")
    def export(tenant_id: str,
               limit: int = Query(500, ge=1, le=500),
               offset: int = Query(0, ge=0),
               decision: str | None = Query(None, max_length=64),
               trace_id: str | None = Query(None, max_length=64),
               session_id: str | None = Query(None, max_length=128),
               tool_name: str | None = Query(None, max_length=128),
               actor: str | None = Query(None, max_length=128),
               since: datetime | None = None,
               until: datetime | None = None):
        values, boundary, more = read(tenant_id,
                                      limit,
                                      offset,
                                      decision,
                                      trace_id,
                                      session_id,
                                      tool_name,
                                      actor,
                                      since,
                                      until,
                                      export=True)
        return Response("".join(json.dumps(jsonable_encoder(value), ensure_ascii=True) + "\n" for value in values),
                        media_type="application/x-ndjson",
                        headers={
                            "Content-Disposition": 'attachment; filename="audit.ndjson"',
                            "Cache-Control": "no-store",
                            "X-Audit-Until": boundary.isoformat(),
                            "X-Next-Offset": str(offset + len(values)) if more else ""
                        })

    return router
