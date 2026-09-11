"""Tenant-scoped PostgreSQL metadata for immutable S3 artifact versions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

_TENANT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ArtifactRepositoryDataError(ValueError):
    """Artifact metadata violates the storage contract."""


class ArtifactRepositoryUnavailableError(RuntimeError):
    """Artifact metadata storage is unavailable."""


@dataclass(frozen=True, slots=True)
class ArtifactVersionRecord:
    """A single object version, deliberately without user-content fields."""

    tenant_id: str
    artifact_path: str
    version: int
    object_key: str
    digest: str
    size_bytes: int
    mime_type: str
    metadata: dict[str, Any]
    created_at: datetime
    available: bool


class ArtifactRepository:
    """Minimal protocol implemented by :class:`SqlArtifactRepository`."""

    async def reserve_version(
        self,
        *,
        tenant_id: str,
        artifact_path: str,
        object_key: str,
        digest: str,
        size_bytes: int,
        mime_type: str,
        metadata: dict[str, Any],
    ) -> ArtifactVersionRecord:
        raise NotImplementedError

    async def mark_available(self, record: ArtifactVersionRecord) -> None:
        raise NotImplementedError

    async def latest_available(self, *, tenant_id: str, artifact_path: str) -> ArtifactVersionRecord | None:
        raise NotImplementedError

    async def get_available(
        self,
        *,
        tenant_id: str,
        artifact_path: str,
        version: int,
    ) -> ArtifactVersionRecord | None:
        raise NotImplementedError

    async def list_available(self, *, tenant_id: str, artifact_path: str) -> tuple[ArtifactVersionRecord, ...]:
        raise NotImplementedError

    async def list_keys(
        self,
        *,
        tenant_id: str,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, ...]:
        raise NotImplementedError

    async def mark_deleted(self, *, tenant_id: str, artifact_path: str) -> tuple[ArtifactVersionRecord, ...]:
        raise NotImplementedError


class SqlArtifactRepository(ArtifactRepository):
    """Artifact metadata repository.

    Version allocation locks one tenant/path metadata row, making concurrent
    saves from different Workers produce distinct monotonically increasing
    versions without relying on an object-store listing.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._closed = False

    async def reserve_version(
        self,
        *,
        tenant_id: str,
        artifact_path: str,
        object_key: str,
        digest: str,
        size_bytes: int,
        mime_type: str,
        metadata: dict[str, Any],
    ) -> ArtifactVersionRecord:
        _validate_values(tenant_id, artifact_path, object_key, digest, size_bytes, mime_type, metadata)
        artifact_metadata, artifact_versions = _tables()
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    pg_insert(artifact_metadata).values(
                        tenant_id=tenant_id,
                        artifact_path=artifact_path,
                        state="active",
                    ).on_conflict_do_update(
                        index_elements=(artifact_metadata.c.tenant_id, artifact_metadata.c.artifact_path),
                        set_={
                            "state": "active",
                            "deleted_at": None
                        },
                    ))
                await conn.execute(
                    sa.select(artifact_metadata.c.tenant_id).where(
                        artifact_metadata.c.tenant_id == tenant_id,
                        artifact_metadata.c.artifact_path == artifact_path,
                    ).with_for_update())
                next_version = (await conn.execute(
                    sa.select(sa.func.coalesce(sa.func.max(artifact_versions.c.version) + 1, 0)).where(
                        artifact_versions.c.tenant_id == tenant_id,
                        artifact_versions.c.artifact_path == artifact_path,
                    ))).scalar_one()
                row = (await conn.execute(artifact_versions.insert().values(
                    tenant_id=tenant_id,
                    artifact_path=artifact_path,
                    version=next_version,
                    object_key=object_key,
                    content_digest=digest,
                    size_bytes=size_bytes,
                    mime_type=mime_type,
                    custom_metadata=metadata,
                    state="pending",
                ).returning(artifact_versions))).one()
        except IntegrityError:
            raise ArtifactRepositoryDataError("artifact metadata violates storage constraints") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None
        return _record(row._mapping)

    async def mark_available(self, record: ArtifactVersionRecord) -> None:
        _, artifact_versions = _tables()
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                result = await conn.execute(artifact_versions.update().where(
                    artifact_versions.c.tenant_id == record.tenant_id,
                    artifact_versions.c.artifact_path == record.artifact_path,
                    artifact_versions.c.version == record.version,
                    artifact_versions.c.state == "pending",
                ).values(state="available"))
                if result.rowcount != 1:
                    raise ArtifactRepositoryDataError("artifact version is not pending")
        except ArtifactRepositoryDataError:
            raise
        except IntegrityError:
            raise ArtifactRepositoryDataError("artifact metadata violates storage constraints") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None

    async def latest_available(self, *, tenant_id: str, artifact_path: str) -> ArtifactVersionRecord | None:
        values = await self.list_available(tenant_id=tenant_id, artifact_path=artifact_path)
        return values[-1] if values else None

    async def get_available(
        self,
        *,
        tenant_id: str,
        artifact_path: str,
        version: int,
    ) -> ArtifactVersionRecord | None:
        _validate_lookup(tenant_id, artifact_path)
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise ArtifactRepositoryDataError("artifact version must be a non-negative integer")
        artifact_metadata, artifact_versions = _tables()
        self._require_open()
        stmt = sa.select(artifact_versions).join(
            artifact_metadata,
            sa.and_(
                artifact_metadata.c.tenant_id == artifact_versions.c.tenant_id,
                artifact_metadata.c.artifact_path == artifact_versions.c.artifact_path,
            ),
        ).where(
            artifact_versions.c.tenant_id == tenant_id,
            artifact_versions.c.artifact_path == artifact_path,
            artifact_versions.c.version == version,
            artifact_versions.c.state == "available",
            artifact_metadata.c.state == "active",
        )
        return await self._one_or_none(stmt)

    async def list_available(self, *, tenant_id: str, artifact_path: str) -> tuple[ArtifactVersionRecord, ...]:
        _validate_lookup(tenant_id, artifact_path)
        artifact_metadata, artifact_versions = _tables()
        self._require_open()
        stmt = sa.select(artifact_versions).join(
            artifact_metadata,
            sa.and_(
                artifact_metadata.c.tenant_id == artifact_versions.c.tenant_id,
                artifact_metadata.c.artifact_path == artifact_versions.c.artifact_path,
            ),
        ).where(
            artifact_versions.c.tenant_id == tenant_id,
            artifact_versions.c.artifact_path == artifact_path,
            artifact_versions.c.state == "available",
            artifact_metadata.c.state == "active",
        ).order_by(artifact_versions.c.version.asc())
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(stmt)).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None
        return tuple(_record(row._mapping) for row in rows)

    async def list_keys(
        self,
        *,
        tenant_id: str,
        app_name: str,
        user_id: str,
        session_id: str | None,
    ) -> tuple[str, ...]:
        _validate_lookup(tenant_id, "x")
        if not isinstance(app_name, str) or not app_name or not isinstance(user_id, str) or not user_id:
            raise ArtifactRepositoryDataError("artifact namespace is invalid")
        if session_id is not None and (not isinstance(session_id, str) or not session_id):
            raise ArtifactRepositoryDataError("artifact namespace is invalid")
        prefix = f"{app_name}/{user_id}/{session_id or 'user'}/"
        artifact_metadata, _ = _tables()
        self._require_open()
        stmt = sa.select(artifact_metadata.c.artifact_path).where(
            artifact_metadata.c.tenant_id == tenant_id,
            artifact_metadata.c.state == "active",
            artifact_metadata.c.artifact_path.like(f"{_escape_like(prefix)}%", escape="\\"),
        ).order_by(artifact_metadata.c.artifact_path.asc())
        try:
            async with self._engine.connect() as conn:
                paths = (await conn.execute(stmt)).scalars().all()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None
        return tuple(path.removeprefix(prefix) for path in paths)

    async def mark_deleted(self, *, tenant_id: str, artifact_path: str) -> tuple[ArtifactVersionRecord, ...]:
        _validate_lookup(tenant_id, artifact_path)
        artifact_metadata, artifact_versions = _tables()
        self._require_open()
        try:
            async with self._engine.begin() as conn:
                active = (await conn.execute(
                    sa.select(artifact_metadata.c.tenant_id).where(
                        artifact_metadata.c.tenant_id == tenant_id,
                        artifact_metadata.c.artifact_path == artifact_path,
                        artifact_metadata.c.state == "active",
                    ).with_for_update())).first()
                if active is None:
                    return ()
                rows = (await conn.execute(
                    sa.select(artifact_versions).where(
                        artifact_versions.c.tenant_id == tenant_id,
                        artifact_versions.c.artifact_path == artifact_path,
                        artifact_versions.c.state == "available",
                    ).with_for_update())).fetchall()
                await conn.execute(artifact_versions.update().where(
                    artifact_versions.c.tenant_id == tenant_id,
                    artifact_versions.c.artifact_path == artifact_path,
                    artifact_versions.c.state == "available",
                ).values(state="deleted"))
                await conn.execute(artifact_metadata.update().where(
                    artifact_metadata.c.tenant_id == tenant_id,
                    artifact_metadata.c.artifact_path == artifact_path,
                    artifact_metadata.c.state == "active",
                ).values(state="deleted", deleted_at=sa.func.now()))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None
        return tuple(_record(row._mapping) for row in rows)

    async def _one_or_none(self, stmt) -> ArtifactVersionRecord | None:
        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(stmt)).first()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ArtifactRepositoryUnavailableError("artifact metadata database unavailable") from None
        return _record(row._mapping) if row is not None else None

    def _require_open(self) -> None:
        if self._closed:
            raise ArtifactRepositoryUnavailableError("artifact metadata repository is closed")

    async def close(self) -> None:
        self._closed = True


def _tables():
    """Import after Alembic/schema installation; keeps unit adapters lightweight."""
    from trpc_service.storage.schema import artifact_metadata, artifact_versions

    return artifact_metadata, artifact_versions


def _record(row) -> ArtifactVersionRecord:
    return ArtifactVersionRecord(
        tenant_id=row["tenant_id"],
        artifact_path=row["artifact_path"],
        version=row["version"],
        object_key=row["object_key"],
        digest=row["content_digest"],
        size_bytes=row["size_bytes"],
        mime_type=row["mime_type"],
        metadata=dict(row["custom_metadata"]),
        created_at=row["created_at"],
        available=row["state"] == "available",
    )


def _validate_values(
    tenant_id: str,
    artifact_path: str,
    object_key: str,
    digest: str,
    size_bytes: int,
    mime_type: str,
    metadata: dict[str, Any],
) -> None:
    _validate_lookup(tenant_id, artifact_path)
    if not isinstance(object_key, str) or not object_key.startswith(f"{tenant_id}/"):
        raise ArtifactRepositoryDataError("artifact object key is invalid")
    if _DIGEST_PATTERN.fullmatch(digest) is None:
        raise ArtifactRepositoryDataError("artifact content digest is invalid")
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
        raise ArtifactRepositoryDataError("artifact size is invalid")
    if not isinstance(mime_type, str) or not mime_type.strip() or len(mime_type) > 255:
        raise ArtifactRepositoryDataError("artifact MIME type is invalid")
    if not isinstance(metadata, dict):
        raise ArtifactRepositoryDataError("artifact metadata is invalid")


def _validate_lookup(tenant_id: str, artifact_path: str) -> None:
    if not isinstance(tenant_id, str) or _TENANT_ID_PATTERN.fullmatch(tenant_id) is None:
        raise ArtifactRepositoryDataError("invalid tenant ID")
    if not isinstance(artifact_path, str) or not artifact_path or len(artifact_path) > 1024:
        raise ArtifactRepositoryDataError("artifact path is invalid")


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


__all__ = [
    "ArtifactRepository",
    "ArtifactRepositoryDataError",
    "ArtifactRepositoryUnavailableError",
    "ArtifactVersionRecord",
    "SqlArtifactRepository",
]
