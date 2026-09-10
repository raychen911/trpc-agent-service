"""Resolve tenant storage profiles into concrete SDK and platform adapters."""

from __future__ import annotations

from pathlib import Path

from redis.asyncio import Redis
from trpc_agent_sdk.sessions import BaseSessionService, RedisSessionService, SqlSessionService

from trpc_service.config.models import StorageBackend, TenantRecord
from trpc_service.config.secrets import SecretResolver
from trpc_service.storage.artifact import LocalArtifactStore
from trpc_service.storage.contracts import ArtifactStore, KnowledgeStore, MemoryStore
from trpc_service.storage.database import Database
from trpc_service.storage.redis_adapter import RedisMemoryStore
from trpc_service.storage.sqlite_adapter import (
    SQLiteKnowledgeStore,
    SQLiteMemoryStore,
    SQLiteSessionService,
)


class TenantStorageRouter:
    def __init__(
        self,
        database: Database,
        secrets: SecretResolver | None = None,
        artifact_root: Path = Path("./data/artifacts"),
    ) -> None:
        self._database = database
        self._secrets = secrets or SecretResolver()
        self._artifact_store = LocalArtifactStore(database, artifact_root)
        self._redis_clients: dict[str, Redis] = {}

    def session_service_for(self, tenant: TenantRecord) -> BaseSessionService:
        config = tenant.storage_config
        if config.session_backend == StorageBackend.REDIS:
            return RedisSessionService(
                db_url=self._redis_url(config.redis_url_ref),
                is_async=True,
            )
        # The SDK defaults to expiring ORM attributes on commit. Its async SQL
        # get_session path reads those attributes after committing the TTL
        # refresh, which otherwise triggers an implicit lazy load and raises
        # MissingGreenlet on the second conversation turn.
        if self._database.is_sqlite:
            return SQLiteSessionService(
                db_url=self._database.database_url,
                is_async=True,
                expire_on_commit=False,
            )
        return SqlSessionService(db_url=self._database.database_url, is_async=True)

    def memory_store_for(self, tenant: TenantRecord) -> MemoryStore:
        config = tenant.storage_config
        if config.memory_backend == StorageBackend.REDIS:
            reference = self._require_redis_ref(config.redis_url_ref)
            client = self._redis_clients.get(reference)
            if client is None:
                client = Redis.from_url(self._secrets.resolve(reference), decode_responses=True)
                self._redis_clients[reference] = client
            return RedisMemoryStore(client)
        return SQLiteMemoryStore(self._database)

    def knowledge_store_for(self, tenant: TenantRecord) -> KnowledgeStore:
        """Return the fact-store knowledge adapter with tenant filtering enforced by calls."""

        del tenant
        return SQLiteKnowledgeStore(self._database)

    def artifact_store_for(self, tenant: TenantRecord) -> ArtifactStore:
        """Return the local content store; record tenant IDs define isolated paths."""

        del tenant
        return self._artifact_store

    async def ping_for(self, tenant: TenantRecord) -> bool:
        config = tenant.storage_config
        if StorageBackend.REDIS not in {
            config.session_backend,
            config.memory_backend,
        }:
            return await self._database.ping()
        reference = self._require_redis_ref(config.redis_url_ref)
        client = self._redis_clients.get(reference)
        if client is None:
            client = Redis.from_url(self._secrets.resolve(reference), decode_responses=True)
            self._redis_clients[reference] = client
        return bool(await client.ping())

    async def close(self) -> None:
        clients = list(self._redis_clients.values())
        self._redis_clients.clear()
        for client in clients:
            await client.aclose()

    def _redis_url(self, reference: str | None) -> str:
        return self._secrets.resolve(self._require_redis_ref(reference))

    @staticmethod
    def _require_redis_ref(reference: str | None) -> str:
        if reference is None:
            raise ValueError("redis_url_ref is required")
        return reference


__all__ = ["TenantStorageRouter"]
