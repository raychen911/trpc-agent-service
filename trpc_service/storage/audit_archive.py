"""Encrypted, version-pinned audit archives with transactional SQL retirement.

The retained audit receipt is the archive index. Failed uploads/readback/commits
leave the original rows intact. An interrupted upload may leave an unindexed
object, never a receipt that falsely claims SQL retirement succeeded.
"""

from datetime import timezone, timedelta
from io import BytesIO
import hashlib
import json
import re
import uuid

from sqlalchemy import delete, select, text

from trpc_service.channels.persistence import ContextCipher, database_now
from trpc_service.persistence.models import AuditLogRow
from trpc_service.telemetry.runtime import operation, count

ARCHIVED = "audit_archive_committed"
MAX_BYTES = 8 * 1024 * 1024
_identifier = re.compile(r"[a-z][a-z0-9_-]{2,63}")


class ArchiveIntegrityError(ValueError):
    """Do not forward backend errors, object paths or ciphertext to callers."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def record(row):
    value = {column.name: getattr(row, column.name) for column in AuditLogRow.__table__.columns}
    created = value["created_at"]
    value["created_at"] = created.replace(tzinfo=timezone.utc).isoformat() if created.utcoffset() is None else \
        created.astimezone(timezone.utc).isoformat()
    return value


def receipts(session, tenant):
    return select(AuditLogRow).where(AuditLogRow.tenant_id == tenant, AuditLogRow.decision == ARCHIVED)


def archive_summary(row):
    fields = row.metadata_json
    names = ("sequence", "row_count", "cutoff", "sha256", "previous_archive_id", "config_version", "retention_days",
             "source")
    return {
        "archive_id": row.audit_id,
        "tenant_id": row.tenant_id,
        "created_at": row.created_at,
        **{
            key: fields[key]
            for key in names
        }
    }


class AuditArchive:

    def __init__(self, database, client, bucket, cipher: ContextCipher):
        self.database, self.client, self.bucket, self.cipher = database, client, bucket, cipher

    def check_storage(self):
        from minio.commonconfig import ENABLED
        if self.client.get_bucket_versioning(self.bucket).status != ENABLED:
            raise ValueError("audit archive bucket must have versioning enabled")

    def _read(self, tenant, archive_id, manifest):
        key = f"audit/{tenant}/{archive_id}.fernet"
        if (manifest.get("object_key") != key or manifest.get("object_version") in {None, "", "null"}
                or manifest.get("bucket") != self.bucket or manifest.get("format") != "trpc-audit-v1"):
            raise ArchiveIntegrityError("archive manifest is invalid")
        response = self.client.get_object(self.bucket, key, version_id=manifest["object_version"])
        try:
            payload = response.read(MAX_BYTES + 1)
        finally:
            response.close()
            response.release_conn()
        try:
            if len(payload) > MAX_BYTES or hashlib.sha256(payload).hexdigest() != manifest["sha256"]:
                raise ValueError("archive digest mismatch")
            value = self.cipher.open((tenant, "audit_archive", archive_id), payload.decode("ascii"))
            if value["manifest"] != {
                    key: item
                    for key, item in manifest.items() if key not in {"object_version", "sha256"}
            }:
                raise ValueError("archive manifest authentication failed")
            rows = value["records"]
            if (len(rows) != manifest["row_count"] or not rows
                    or any(row["tenant_id"] != tenant or row["decision"] == ARCHIVED for row in rows)
                    or len({row["audit_id"]
                            for row in rows}) != len(rows)):
                raise ValueError("archive row set is invalid")
            return rows
        except (ValueError, KeyError, TypeError, UnicodeError):
            raise ArchiveIntegrityError("archive integrity verification failed") from None

    def read(self, tenant, archive_id):
        with self.database.sessions() as session:
            row = session.scalar(receipts(session, tenant).where(AuditLogRow.audit_id == archive_id))
            if row is None:
                raise LookupError("archive not found")
            return self._read(tenant, archive_id, dict(row.metadata_json))

    def run_batch(self, tenant, policy, *, config_version, source="platform", limit=200):
        from trpc_service.governance.admin_auth import current_actor
        from trpc_service.telemetry.runtime import current_trace_id
        from trpc_service.telemetry.logging import current_fields
        if not _identifier.fullmatch(tenant) or not 1 <= limit <= 500 or source not in {"platform", "tenant_audit"}:
            raise ValueError("invalid archive scope or batch size")
        if not policy.archive_enabled:
            return {"status": "disabled", "row_count": 0}
        with operation("audit.archive", attributes={"tenant.id": tenant}):
            self.check_storage()
            with self.database.sessions.begin() as session:
                if self.database.engine.dialect.name == "postgresql":
                    # Serialize archive workers for this tenant without depending
                    # on a TenantRow in an independently deployed audit database.
                    lock = int.from_bytes(hashlib.sha256(("audit:" + tenant).encode()).digest()[:8], "big", signed=True)
                    if not session.scalar(text("SELECT pg_try_advisory_xact_lock(:identity)"), {"identity": lock}):
                        return {"status": "busy", "row_count": 0}
                now = database_now(session)
                cutoff = now - timedelta(days=policy.retention_days)
                rows = list(
                    session.scalars(
                        select(AuditLogRow).where(AuditLogRow.tenant_id == tenant, AuditLogRow.decision != ARCHIVED,
                                                  AuditLogRow.created_at < cutoff).order_by(
                                                      AuditLogRow.created_at,
                                                      AuditLogRow.audit_id).limit(limit).with_for_update()))
                if not rows:
                    return {"status": "idle", "row_count": 0}
                previous = session.scalar(
                    receipts(session,
                             tenant).order_by(AuditLogRow.metadata_json["sequence"].as_integer().desc()).limit(1))
                if previous:
                    # Fail closed if the preceding committed archive is missing
                    # or has been replaced. Do not extend a damaged chain.
                    self._read(tenant, previous.audit_id, dict(previous.metadata_json))
                archive_id = uuid.uuid4().hex
                manifest = {
                    "actor": current_actor("audit-retention-worker"),
                    "archived_at": now.isoformat(),
                    "format": "trpc-audit-v1",
                    "bucket": self.bucket,
                    "object_key": f"audit/{tenant}/{archive_id}.fernet",
                    "source": source,
                    "sequence": previous.metadata_json["sequence"] + 1 if previous else 1,
                    "previous_archive_id": previous.audit_id if previous else None,
                    "previous_sha256": previous.metadata_json["sha256"] if previous else None,
                    "row_count": len(rows),
                    "cutoff": cutoff.isoformat(),
                    "retention_days": policy.retention_days,
                    "config_version": config_version
                }
                records = [record(row) for row in rows]
                # Normalize before encryption so the exact same bytes/types are
                # compared after the external object store readback.
                content = json.loads(canonical({"manifest": manifest, "records": records}))
                payload = self.cipher.seal((tenant, "audit_archive", archive_id), content).encode("ascii")
                if len(payload) > MAX_BYTES:
                    raise ValueError("archive batch exceeds the encrypted size limit; reduce batch size")
                result = self.client.put_object(self.bucket,
                                                manifest["object_key"],
                                                BytesIO(payload),
                                                len(payload),
                                                content_type="application/octet-stream")
                manifest = {
                    **manifest, "object_version": result.version_id,
                    "sha256": hashlib.sha256(payload).hexdigest()
                }
                if self._read(tenant, archive_id, manifest) != records:
                    raise ArchiveIntegrityError("archive readback differs from locked SQL records")
                deleted = session.execute(
                    delete(AuditLogRow).where(AuditLogRow.tenant_id == tenant,
                                              AuditLogRow.audit_id.in_([row.audit_id for row in rows])))
                if deleted.rowcount != len(rows):
                    raise ArchiveIntegrityError("archive source changed before retirement")
                session.add(
                    AuditLogRow(audit_id=archive_id,
                                tenant_id=tenant,
                                user_id=manifest["actor"],
                                trace_id=current_trace_id(),
                                request_id=current_fields().get("request_id"),
                                decision=ARCHIVED,
                                config_version=config_version,
                                redacted=True,
                                metadata_json=manifest))
            count("audit.archived_rows", len(rows))
            return {
                "status": "archived",
                "archive_id": archive_id,
                "row_count": len(rows),
                "sha256": manifest["sha256"]
            }

    def verify_chain(self, tenant, *, limit=500, after_sequence=0):
        if not 1 <= limit <= 500 or after_sequence < 0:
            raise ValueError("invalid chain verification page")
        with self.database.sessions() as session:
            sequence = AuditLogRow.metadata_json["sequence"].as_integer()
            previous = session.scalar(receipts(session, tenant).where(sequence == after_sequence)) \
                if after_sequence else None
            if after_sequence and previous is None:
                raise ArchiveIntegrityError("chain checkpoint is missing")
            rows = list(
                session.scalars(
                    receipts(session, tenant).where(sequence > after_sequence).order_by(sequence).limit(limit + 1)))
            if previous:
                self._read(tenant, previous.audit_id, dict(previous.metadata_json))
            expected = after_sequence + 1
            for row in rows[:limit]:
                value = row.metadata_json
                if (value["sequence"] != expected
                        or value["previous_archive_id"] != (previous.audit_id if previous else None)
                        or value["previous_sha256"] != (previous.metadata_json["sha256"] if previous else None)):
                    raise ArchiveIntegrityError("archive chain has a missing or modified receipt")
                self._read(tenant, row.audit_id, dict(value))
                expected += 1
                previous = row
            return {
                "verified": len(rows[:limit]),
                "next_sequence": expected - 1 if len(rows) > limit else None,
                "complete": len(rows) <= limit
            }
