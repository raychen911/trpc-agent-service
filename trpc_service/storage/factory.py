# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Build tRPC-Agent SDK state services from tenant storage policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from typing import Protocol

from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.memory import InMemoryMemoryService
from trpc_agent_sdk.memory import MemoryServiceConfig
from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SessionServiceConfig
from trpc_agent_sdk.sessions import SqlSessionService

from trpc_service.config import BackendType
from trpc_service.config import StoragePolicy
from .session_wrapper import RequestTaggingSessionService
from .observability import TracingMemoryService
from .guard import RedisSessionExecutionGuard
from .fencing import FencedRedisStorage, FencedSqlStorage, PostgresExecutionGuard, database_identity


class UnsupportedBackendError(ValueError):
    """Raised for adapter kinds not implemented by the built-in factory."""


@dataclass(slots=True)
class StorageBundle:
    """SDK services owned by one cached tenant runtime."""

    session_service: BaseSessionService
    memory_service: BaseMemoryService
    write_guards: dict = field(default_factory=dict)


class AtomicRedisMemoryService(RedisMemoryService):

    async def store_session(self, session, agent_context=None):
        async with self._redis_storage.create_db_session() as conn:
            events = [e.model_dump_json() for e in session.events if e.content and e.content.parts]
            ttl = self._memory_service_config.ttl
            await self._redis_storage.replace_memory(conn, f"memory:{session.save_key}:{session.id}", events,
                                                     int(ttl.ttl_seconds) if ttl.need_ttl_expire() else 0)


class ExternalStorageProvider(Protocol):

    def create_session(self, policy: StoragePolicy, config: SessionServiceConfig) -> Any:
        ...

    def create_memory(self, policy: StoragePolicy, config: MemoryServiceConfig) -> Any:
        ...


class StorageProviderFactory:
    """Create SDK-compatible Session and Memory service implementations."""

    def __init__(self, providers: dict[str, ExternalStorageProvider] | None = None, metrics: Any = None) -> None:
        self._providers = providers or {}
        self._metrics = metrics

    def register(self, name: str, provider: ExternalStorageProvider) -> None:
        self._providers[name] = provider

    def create(self, policy: StoragePolicy) -> StorageBundle:
        session_config = SessionServiceConfig(
            store_historical_events=policy.session != BackendType.MEMORY,
            ttl=SessionServiceConfig.create_ttl_config(ttl_seconds=policy.session_ttl_seconds),
        )
        memory_config = MemoryServiceConfig(
            enabled=True,
            ttl=MemoryServiceConfig.create_ttl_config(ttl_seconds=policy.memory_ttl_seconds),
        )
        session = self._session(policy, session_config)
        memory = self._memory(policy, memory_config)
        guards = {}
        for service, kind in ((session, policy.session), (memory, policy.memory)):
            if kind == BackendType.REDIS:
                identity = database_identity(policy.redis_url)
                if identity not in guards:
                    guards[identity] = RedisSessionExecutionGuard(policy.redis_url, prefix="trpc-service:data-lease")
                service._redis_storage = FencedRedisStorage(service._redis_storage, identity)
            elif kind == BackendType.SQL and policy.sql_url.startswith("postgresql"):
                identity = database_identity(policy.sql_url)
                if identity not in guards:
                    guards[identity] = PostgresExecutionGuard(policy.sql_url)
                service._sql_storage = FencedSqlStorage(service._sql_storage, identity)
                # SDK background cleanup has no request lease. Retention is a
                # separate administrative job, never an unfenced background write.
                service._stop_cleanup_task()
            elif kind == BackendType.EXTERNAL:
                native = getattr(service, "write_guards", None)
                if native:
                    guards.update(native)
        backend = policy.session.value
        memory_backend = policy.memory.value
        return StorageBundle(RequestTaggingSessionService(session, backend, self._metrics),
                             TracingMemoryService(memory, memory_backend, self._metrics), guards)

    def create_migration(self,
                         policy: StoragePolicy,
                         mode,
                         dirty_recorder=None,
                         shadow_sample_rate: float = 0.1) -> StorageBundle:
        """Backward-compatible Redis-to-PostgreSQL migration bundle."""
        return self.create_migration_route(policy, BackendType.REDIS, BackendType.SQL, mode, dirty_recorder,
                                           shadow_sample_rate)

    def create_migration_route(self,
                               policy: StoragePolicy,
                               source_backend,
                               target_backend,
                               mode,
                               dirty_recorder=None,
                               shadow_sample_rate: float = 0.1) -> StorageBundle:
        """Create a direction-aware Redis/PostgreSQL migration bundle."""
        from trpc_service.migration.routing import MigrationAwareMemoryService
        from trpc_service.migration.routing import MigrationAwareSessionService
        from trpc_service.migration.control import StorageRouteMode

        source_backend = BackendType(source_backend)
        target_backend = BackendType(target_backend)
        if (source_backend, target_backend) not in {(BackendType.REDIS, BackendType.SQL),
                                                    (BackendType.SQL, BackendType.REDIS)}:
            raise UnsupportedBackendError(
                f"unsupported migration route: {source_backend.value} -> {target_backend.value}")
        if BackendType.SQL in {source_backend, target_backend} and not policy.sql_url.startswith("postgresql"):
            raise UnsupportedBackendError("online SQL migration requires PostgreSQL")
        source_policy = policy.model_copy(update={"session": source_backend, "memory": source_backend})
        target_policy = policy.model_copy(update={"session": target_backend, "memory": target_backend})
        if mode == StorageRouteMode.SOURCE_ONLY:
            return self.create(source_policy)
        if mode == StorageRouteMode.TARGET_ONLY:
            return self.create(target_policy)
        source = self.create(source_policy)
        target = self.create(target_policy)
        source_session = source.session_service._delegate
        target_session = target.session_service._delegate
        session = MigrationAwareSessionService(source_session,
                                               target_session,
                                               mode,
                                               dirty_recorder,
                                               shadow_sample_rate=shadow_sample_rate)
        memory = MigrationAwareMemoryService(source.memory_service, target.memory_service, mode, dirty_recorder)
        route_backend = f"{source_backend.value}_to_{target_backend.value}"
        return StorageBundle(RequestTaggingSessionService(session, route_backend, self._metrics),
                             TracingMemoryService(memory, route_backend, self._metrics), {
                                 **source.write_guards,
                                 **target.write_guards
                             })

    def _external(self, policy: StoragePolicy) -> ExternalStorageProvider:
        provider = self._providers.get(policy.external_provider)
        if provider is None:
            raise UnsupportedBackendError(f"external storage provider is not registered: {policy.external_provider!r}")
        return provider

    def _session(self, policy: StoragePolicy, config: SessionServiceConfig) -> BaseSessionService:
        if policy.session == BackendType.MEMORY:
            return InMemorySessionService(session_config=config)
        if policy.session == BackendType.REDIS:
            return RedisSessionService(policy.redis_url, is_async=True, session_config=config)
        if policy.session == BackendType.SQL:
            return SqlSessionService(policy.sql_url, is_async=False, session_config=config)
        if policy.session == BackendType.EXTERNAL:
            return self._external(policy).create_session(policy, config)
        raise UnsupportedBackendError(f"unsupported session backend: {policy.session}")

    def _memory(self, policy: StoragePolicy, config: MemoryServiceConfig) -> BaseMemoryService:
        if policy.memory == BackendType.MEMORY:
            return InMemoryMemoryService(memory_service_config=config)
        if policy.memory == BackendType.REDIS:
            return AtomicRedisMemoryService(policy.redis_url, is_async=True, memory_service_config=config)
        if policy.memory == BackendType.SQL:
            return SqlMemoryService(policy.sql_url, is_async=False, memory_service_config=config)
        if policy.memory == BackendType.EXTERNAL:
            return self._external(policy).create_memory(policy, config)
        raise UnsupportedBackendError(f"unsupported memory backend: {policy.memory}")
