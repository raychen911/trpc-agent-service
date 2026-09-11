"""Authenticated archive inspection, verified downloads and operator execution."""

import json
from types import SimpleNamespace
from typing import Literal
import uuid

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from pydantic import BaseModel, Field, ConfigDict

from trpc_service.persistence.models import AuditLogRow
from trpc_service.storage.audit_archive import ArchiveIntegrityError, archive_summary, receipts
from trpc_service.telemetry.runtime import current_trace_id
from .admin_auth import principal, current_actor
from .audit_management import authorize_audit, evidence
from .audit_retention import archive_service, audit_database, run_tenant


class ArchiveBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(default=200, ge=1, le=500)


def archive_router(database):
    router = APIRouter()

    def access(tenant):
        with database.sessions() as session:
            authorize_audit(session, tenant)

    def audit_access(tenant, decision, *, archive_id=None):
        with database.sessions.begin() as session:
            session.add(
                AuditLogRow(audit_id=uuid.uuid4().hex,
                            tenant_id=tenant,
                            user_id=current_actor(),
                            decision=decision,
                            trace_id=current_trace_id(),
                            redacted=True,
                            metadata_json={"archive_id": archive_id} if archive_id else {}))

    def invoke(function):
        try:
            return function()
        except HTTPException:
            raise
        except LookupError:
            raise HTTPException(404, "archive or published audit policy not found") from None
        except ArchiveIntegrityError:
            raise HTTPException(409, "archive integrity verification failed") from None
        except Exception:
            raise HTTPException(503, "archive storage or key configuration unavailable") from None

    @router.get("/tenants/{tenant_id}/audit/archives")
    def archives(tenant_id: str,
                 response: Response,
                 source: Literal["platform", "tenant_audit"] = "platform",
                 limit: int = Query(100, ge=1, le=500),
                 after_sequence: int = Query(0, ge=0)):
        access(tenant_id)
        response.headers["Cache-Control"] = "no-store"

        def read():
            with audit_database(database, tenant_id, source) as (target, _):
                with target.sessions() as session:
                    sequence = AuditLogRow.metadata_json["sequence"].as_integer()
                    values = [
                        archive_summary(row) for row in session.scalars(
                            receipts(session, tenant_id).where(
                                sequence > after_sequence).order_by(sequence).limit(limit))
                    ]
            audit_access(tenant_id, "audit_archives_listed")
            return values

        return invoke(read)

    @router.get("/tenants/{tenant_id}/audit/archives/verify")
    def verify(tenant_id: str,
               response: Response,
               source: Literal["platform", "tenant_audit"] = "platform",
               limit: int = Query(100, ge=1, le=500),
               after_sequence: int = Query(0, ge=0)):
        access(tenant_id)
        response.headers["Cache-Control"] = "no-store"

        def check():
            with archive_service(database, tenant_id, source) as (archive, _):
                result = archive.verify_chain(tenant_id, limit=limit, after_sequence=after_sequence)
            audit_access(tenant_id, "audit_archives_verified")
            return result

        return invoke(check)

    @router.get("/tenants/{tenant_id}/audit/archives/{archive_id}")
    def download(tenant_id: str, archive_id: str, source: Literal["platform", "tenant_audit"] = "platform"):
        access(tenant_id)

        def read():
            with archive_service(database, tenant_id, source) as (archive, _):
                rows = archive.read(tenant_id, archive_id)
            # Downloads use the same deny-by-construction presentation boundary
            # as hot audit exports; the encrypted archive preserves full evidence.
            body = "".join(
                json.dumps(jsonable_encoder(evidence(SimpleNamespace(**row))), ensure_ascii=True) + "\n"
                for row in rows)
            audit_access(tenant_id, "audit_archive_downloaded", archive_id=archive_id)
            return Response(body,
                            media_type="application/x-ndjson",
                            headers={
                                "Cache-Control": "no-store",
                                "Content-Disposition": 'attachment; filename="audit-archive.ndjson"'
                            })

        return invoke(read)

    @router.post("/tenants/{tenant_id}/audit/archives/run")
    def run(tenant_id: str, body: ArchiveBatch):
        identity = principal.get()
        if not identity or identity["role"] != "operator":
            raise HTTPException(403, "archive retirement requires a platform operator")
        return invoke(lambda: run_tenant(database, tenant_id, limit=body.limit))

    return router
