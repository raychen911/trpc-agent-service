"""Build a tenant-specific data plane from immutable backend configuration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, cast

from tenant_agent.models import BackendKind, BackendRef, TenantConfig
from tenant_agent.security import CompositeSecretResolver
from tenant_agent.settings import Settings
from tenant_agent.storage.base import (
    AuditRepository,
    ConfigRepository,
    MemoryRepository,
    SessionRepository,
    SummaryRepository,
    TenantDataPlane,
)
from tenant_agent.storage.external import (
    ExternalMemoryRepository,
    FilesystemArtifactRepository,
    QdrantKnowledgeRepository,
    S3ArtifactRepository,
)
from tenant_agent.storage.memory import InMemoryPlane
from tenant_agent.storage.redis import RedisPlane
from tenant_agent.storage.sql import SqlPlane

RESOURCE_KINDS: dict[str, set[BackendKind]] = {
    "session": {BackendKind.INMEMORY, BackendKind.REDIS, BackendKind.SQL},
    "memory": {
        BackendKind.INMEMORY,
        BackendKind.REDIS,
        BackendKind.SQL,
        BackendKind.EXTERNAL_MEMORY,
    },
    "summary": {BackendKind.INMEMORY, BackendKind.REDIS, BackendKind.SQL},
    "artifact": {
        BackendKind.INMEMORY,
        BackendKind.SQL,
        BackendKind.FILESYSTEM,
        BackendKind.S3,
    },
    "knowledge": {
        BackendKind.INMEMORY,
        BackendKind.SQL,
        BackendKind.LOCAL_VECTOR,
        BackendKind.QDRANT,
    },
    "audit": {BackendKind.INMEMORY, BackendKind.SQL},
}


class StorageRouter:
    """Caches connection pools by non-secret configuration fingerprint."""

    def __init__(
        self,
        *,
        settings: Settings,
        control: ConfigRepository,
        control_plane: Any,
        secrets: CompositeSecretResolver,
    ) -> None:
        self.settings = settings
        self.control = control
        self.control_plane = control_plane
        self.secrets = secrets
        self.inmemory = InMemoryPlane()
        self._adapters: dict[str, Any] = {}
        self._initialized: dict[int, Any] = {}
        self._initialization_guard = asyncio.Lock()
        self._adapter_guard = asyncio.Lock()

    async def initialize(self) -> None:
        await self._ensure_initialized(self.control_plane)
        await self._ensure_initialized(self.inmemory)

    async def close(self) -> None:
        unique = {id(adapter): adapter for adapter in self._adapters.values()}
        unique[id(self.inmemory)] = self.inmemory
        unique[id(self.control_plane)] = self.control_plane
        for adapter in reversed(tuple(unique.values())):
            await adapter.close()
        self._adapters.clear()
        self._initialized.clear()

    async def _ensure_initialized(self, adapter: Any) -> Any:
        identity = id(adapter)
        if self._initialized.get(identity) is adapter:
            return adapter
        async with self._initialization_guard:
            if self._initialized.get(identity) is not adapter:
                await adapter.initialize()
                # Keep the object, not only its numeric id, alive until close so
                # CPython id reuse cannot make an unrelated adapter look ready.
                self._initialized[identity] = adapter
        return adapter

    @staticmethod
    def _fingerprint(reference: BackendRef, resolved: str | None = None) -> str:
        safe = {
            "kind": reference.kind.value,
            "namespace": (
                None
                if reference.kind in {BackendKind.SQL, BackendKind.EXTERNAL_MEMORY}
                else reference.namespace
            ),
            "options": reference.options,
            "resolved_hash": hashlib.sha256(resolved.encode()).hexdigest() if resolved else None,
        }
        return hashlib.sha256(json.dumps(safe, sort_keys=True, default=str).encode()).hexdigest()

    async def _adapter(self, resource: str, reference: BackendRef) -> Any:
        if reference.kind not in RESOURCE_KINDS[resource]:
            raise ValueError(f"{reference.kind.value} cannot back the {resource} resource")
        if reference.kind is BackendKind.INMEMORY:
            return self.inmemory
        if (
            reference.kind is BackendKind.SQL
            and reference.dsn_ref
            and reference.dsn_ref.uri == "env://TAP_CONTROL_DATABASE_URL"
        ):
            return self.control_plane
        resolved = await self.secrets.resolve(reference.dsn_ref) if reference.dsn_ref else None
        key = self._fingerprint(reference, resolved)
        cached = self._adapters.get(key)
        if cached is not None:
            return cached

        async with self._adapter_guard:
            cached = self._adapters.get(key)
            if cached is not None:
                return cached
            if len(self._adapters) >= self.settings.storage_adapter_cache_max_entries:
                raise RuntimeError(
                    "storage adapter cache limit reached; drain the node or increase the reviewed limit"
                )

            adapter: Any
            if reference.kind is BackendKind.SQL:
                adapter = SqlPlane(
                    resolved or "",
                    echo=bool(reference.options.get("echo", False)),
                    pool_size=int(reference.options.get("pool_size", 10)),
                    create_schema=self.settings.auto_create_schema,
                )
            elif reference.kind is BackendKind.REDIS:
                adapter = RedisPlane(
                    resolved or "",
                    namespace=f"tap:{reference.namespace}",
                    cluster=bool(reference.options.get("cluster", False)),
                )
            elif reference.kind is BackendKind.FILESYSTEM:
                root = Path(reference.options.get("root", "./artifacts")) / reference.namespace
                adapter = FilesystemArtifactRepository(root)
            elif reference.kind is BackendKind.S3:
                adapter = S3ArtifactRepository(resolved or "", namespace=reference.namespace)
            elif reference.kind is BackendKind.EXTERNAL_MEMORY:
                adapter = ExternalMemoryRepository(resolved or "")
            elif reference.kind is BackendKind.QDRANT:
                adapter = QdrantKnowledgeRepository(resolved or "", namespace=reference.namespace)
            elif reference.kind is BackendKind.LOCAL_VECTOR:
                raw_path = Path(reference.options.get("path", f"./{reference.namespace}-vectors.db"))
                path = await asyncio.to_thread(lambda: raw_path.resolve().as_posix())
                adapter = SqlPlane(
                    f"sqlite+aiosqlite:///{path}",
                    create_schema=self.settings.auto_create_schema,
                )
            else:
                raise ValueError(f"unsupported backend {reference.kind.value}")
            initialized = await self._ensure_initialized(adapter)
            self._adapters[key] = initialized
            return initialized

    async def for_tenant(self, tenant: TenantConfig) -> TenantDataPlane:
        config = tenant.data_backends
        self._validate_backend_policy(tenant)
        sessions = await self._adapter("session", config.session)
        memories = await self._adapter("memory", config.memory)
        summaries = await self._adapter("summary", config.summary)
        artifacts = await self._adapter("artifact", config.artifact)
        knowledge = await self._adapter("knowledge", config.knowledge)
        audit = await self._adapter("audit", config.audit)

        leases: Any = sessions
        if self.settings.redis_url:
            global_ref = BackendRef.model_construct(
                kind=BackendKind.REDIS,
                dsn_ref=None,
                namespace="coordination",
                options={},
            )
            resolved = self.settings.redis_url.get_secret_value()
            key = self._fingerprint(global_ref, resolved)
            if key not in self._adapters:
                async with self._adapter_guard:
                    if key not in self._adapters:
                        adapter = await self._ensure_initialized(
                            RedisPlane(
                                resolved,
                                namespace="tap:coordination",
                                cluster=self.settings.redis_cluster,
                            )
                        )
                        self._adapters[key] = adapter
            leases = self._adapters[key]

        return TenantDataPlane(
            sessions=sessions,
            memories=memories,
            summaries=summaries,
            artifacts=artifacts,
            knowledge=knowledge,
            audit=audit,
            receipts=self.control_plane,
            usage=self.control_plane,
            concurrency=self.control_plane,
            outbox=self.control_plane,
            leases=leases,
        )

    def _validate_backend_policy(self, tenant: TenantConfig) -> None:
        if self.settings.environment != "production":
            return
        unsafe_kinds = {
            BackendKind.INMEMORY,
            BackendKind.FILESYSTEM,
            BackendKind.LOCAL_VECTOR,
        }
        unsafe_resources = [
            resource
            for resource in RESOURCE_KINDS
            if getattr(tenant.data_backends, resource).kind in unsafe_kinds
        ]
        if unsafe_resources:
            raise ValueError(
                "production requires shared tenant backends for: " + ", ".join(sorted(unsafe_resources))
            )

    async def preflight_tenant(self, tenant: TenantConfig) -> None:
        """Resolve and health-check every configured resource before activation."""

        self._validate_backend_policy(tenant)
        for resource in RESOURCE_KINDS:
            adapter = await self._adapter(resource, getattr(tenant.data_backends, resource))
            if isinstance(adapter, SqlPlane):
                await adapter.healthcheck_resource(resource)
                if self.settings.environment == "production":
                    await adapter.assert_runtime_role_unprivileged()
                continue
            healthcheck = getattr(adapter, "healthcheck", None)
            if healthcheck is None:
                raise RuntimeError(f"{resource} backend does not expose a health check")
            if not await healthcheck():
                raise RuntimeError(f"{resource} backend is not ready")

    async def audit_for_tenant(self, tenant: TenantConfig) -> AuditRepository:
        """Resolve only the audit port for lifecycle maintenance."""

        return cast(
            AuditRepository,
            await self._adapter("audit", tenant.data_backends.audit),
        )

    async def session_for_tenant(self, tenant: TenantConfig) -> SessionRepository:
        return cast(
            SessionRepository,
            await self._adapter("session", tenant.data_backends.session),
        )

    async def summary_for_tenant(self, tenant: TenantConfig) -> SummaryRepository:
        return cast(
            SummaryRepository,
            await self._adapter("summary", tenant.data_backends.summary),
        )

    async def memory_for_tenant(self, tenant: TenantConfig) -> MemoryRepository:
        return cast(
            MemoryRepository,
            await self._adapter("memory", tenant.data_backends.memory),
        )
