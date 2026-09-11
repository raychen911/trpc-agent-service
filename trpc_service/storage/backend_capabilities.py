"""Shared, tenant-scoped non-state backend capabilities.

R1C deliberately keeps this module small: Redis/SQL Session and Memory stay
owned by :mod:`trpc_service.storage.backend_resolver`; this adds the two missing
shared capabilities required by the platform, S3 Artifacts and SQL Knowledge.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from minio import Minio
from minio.error import S3Error
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.config.tenant import TenantBackendProfile
from trpc_service.storage.artifact_repository import SqlArtifactRepository
from trpc_service.storage.database import DatabaseSettings, check_database_readiness, create_database_engine
from trpc_service.storage.knowledge_repository import SqlTenantKnowledge
from trpc_service.storage.s3_artifact_service import S3ArtifactService

_BUCKET_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?$")


class BackendConfigurationError(ValueError):
    """A backend cannot be configured safely; its message never includes secrets."""


@dataclass(frozen=True, slots=True)
class S3Settings:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "S3Settings":
        import os

        values = os.environ if environ is None else environ
        raw_endpoint = values.get("TRPC_S3_ENDPOINT", "").strip()
        access_key = values.get("TRPC_S3_ACCESS_KEY", "").strip()
        secret_key = values.get("TRPC_S3_SECRET_KEY", "").strip()
        bucket = values.get("TRPC_S3_BUCKET", "trpc-artifacts").strip()
        if not raw_endpoint or not access_key or not secret_key or not _BUCKET_RE.fullmatch(bucket):
            raise BackendConfigurationError("S3 backend configuration is invalid")
        try:
            parsed = urlsplit(raw_endpoint if "://" in raw_endpoint else f"http://{raw_endpoint}")
            if not parsed.hostname or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
                raise ValueError
            endpoint = parsed.netloc
            secure = parsed.scheme == "https"
            if parsed.scheme not in {"http", "https"}:
                raise ValueError
        except ValueError:
            raise BackendConfigurationError("S3 backend configuration is invalid") from None
        return cls(endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket, secure=secure)


def build_minio_client(settings: S3Settings) -> Minio:
    """Build the official client. Calls are moved to worker threads by callers."""
    return Minio(
        settings.endpoint,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        secure=settings.secure,
    )


@dataclass(frozen=True, slots=True)
class TenantBackendServices:
    """Resources selected for one tenant runtime; only this bundle owns close."""

    session: object
    memory: object
    artifact: object
    knowledge: object
    audit: object | None
    _close_state: dict[str, object] = field(default_factory=lambda: {
        "lock": asyncio.Lock(),
        "closed": False
    },
                                            compare=False,
                                            repr=False)

    async def close(self) -> None:
        """Close tenant-bound clients once; SDK state remains Worker-owned."""
        lock = self._close_state["lock"]
        async with lock:
            if self._close_state["closed"]:
                return
            for resource in (self.artifact, self.knowledge):
                close = getattr(resource, "close", None)
                if close is not None:
                    result = close()
                    if hasattr(result, "__await__"):
                        await result
            self._close_state["closed"] = True


class TenantBackendCapabilitiesResolver:
    """Worker-owned resolver for state, Artifact and Knowledge capabilities."""

    def __init__(self, state_resolver: TenantStateBackendResolver, engine: AsyncEngine, settings: S3Settings) -> None:
        self._state_resolver = state_resolver
        self._engine = engine
        self._settings = settings
        self._client = build_minio_client(settings)
        self._artifact_repository = SqlArtifactRepository(engine)
        self._closed = False

    def resolve(self, tenant_id: str, profile: TenantBackendProfile) -> TenantBackendServices:
        state = self._state_resolver.resolve(profile)
        return TenantBackendServices(
            session=state.session_service,
            memory=state.memory_service,
            artifact=S3ArtifactService(
                tenant_id=tenant_id,
                bucket=self._settings.bucket,
                client=self._client,
                repository=self._artifact_repository,
            ),
            knowledge=SqlTenantKnowledge(self._engine, tenant_id),
            audit=None,
        )

    def resolve_state_backend(self, profile: TenantBackendProfile):
        """Expose the already-owned selected state backend to the runtime."""
        return self._state_resolver.resolve(profile)

    async def check_ready(self) -> None:
        self._state_resolver.check_ready()
        exists = await asyncio.to_thread(self._client.bucket_exists, self._settings.bucket)
        if not exists:
            raise BackendConfigurationError("S3 backend is unavailable")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._state_resolver.close()
        http = getattr(self._client, "_http", None)
        clear = getattr(http, "clear", None)
        if clear is not None:
            await asyncio.to_thread(clear)


async def initialize_data_backends(environ: Mapping[str, str] | None = None) -> None:
    """Check shared Redis/PostgreSQL and create the configured S3 bucket once."""
    settings = S3Settings.from_env(environ)
    engine = create_database_engine(DatabaseSettings.from_env(environ))
    state = TenantStateBackendResolver.from_env(environ)
    try:
        await check_database_readiness(engine)
        state.check_ready()
        client = build_minio_client(settings)
        exists = await asyncio.to_thread(client.bucket_exists, settings.bucket)
        if not exists:
            await asyncio.to_thread(client.make_bucket, settings.bucket)
        # Verify a bucket selected by a concurrent initializer is usable.
        if not await asyncio.to_thread(client.bucket_exists, settings.bucket):
            raise BackendConfigurationError("S3 backend is unavailable")
    except (S3Error, OSError, ValueError):
        raise BackendConfigurationError("data backend initialization failed") from None
    finally:
        await state.close()
        await engine.dispose()


__all__ = [
    "BackendConfigurationError",
    "S3Settings",
    "TenantBackendCapabilitiesResolver",
    "TenantBackendServices",
    "build_minio_client",
    "initialize_data_backends",
]
