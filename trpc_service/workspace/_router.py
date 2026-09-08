# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-aware storage backend registry.

The tenant configuration is the source of truth for backend selection. The
router caches clients by connection identity, avoiding a new connection pool
for every agent turn while still allowing different tenants to select
different backends.
"""

from __future__ import annotations

from typing import Callable
from typing import Optional

from trpc_agent_sdk.abc import MemoryServiceABC
from trpc_agent_sdk.abc import SessionServiceABC
from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService
from pydantic import SecretStr
from trpc_service.config._secrets import DEFAULT_SECRET_RESOLVER
from trpc_service.config._secrets import SecretResolver
from trpc_service.config._secrets import resolve_secret

from trpc_service.tenant import ObjectBackendConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import VectorBackendConfig
from ._data_backends import InMemoryVectorStore
from ._data_backends import LocalObjectStore
from ._data_backends import ObjectStoreABC
from ._data_backends import QdrantVectorStore
from ._data_backends import S3CompatibleObjectStore
from ._data_backends import TenantObjectStore
from ._data_backends import TenantVectorStore
from ._data_backends import VectorStoreABC

VectorStoreFactory = Callable[[VectorBackendConfig], VectorStoreABC]
ObjectStoreFactory = Callable[[ObjectBackendConfig], ObjectStoreABC]


def _secret_value(
    value: Optional[SecretStr],
    *,
    resolver: SecretResolver = DEFAULT_SECRET_RESOLVER,
    tenant_id: Optional[str] = None,
) -> str:
    return resolve_secret(value, resolver=resolver, tenant_id=tenant_id)


def _is_async_mysql_url(url: str) -> bool:
    return any(driver in url for driver in ("+aiomysql", "+asyncmy"))


def _mysql_runtime_url(url: str) -> str:
    """Use the stable synchronous driver expected by upstream SQL services.

    The upstream SQL session implementation can lazily reload ORM attributes
    outside SQLAlchemy's greenlet context when an async MySQL driver is used.
    Normalising only the enterprise runtime client avoids that failure while
    still accepting common async URLs in tenant configuration.
    """
    return url.replace("mysql+aiomysql://", "mysql+pymysql://").replace("mysql+asyncmy://", "mysql+pymysql://")


def _validate_mysql_url(url: str) -> str:
    if not url.lower().startswith(("mysql://", "mysql+aiomysql://", "mysql+asyncmy://", "mysql+pymysql://")):
        raise ValueError("mysql backend requires a mysql:// compatible URL")
    return url


def _redis_session_builder(url: str) -> SessionServiceABC:
    return RedisSessionService(db_url=url)


def _mysql_session_builder(url: str) -> SessionServiceABC:
    url = _mysql_runtime_url(_validate_mysql_url(url))
    return SqlSessionService(db_url=url, is_async=False)


def _redis_memory_builder(url: str) -> MemoryServiceABC:
    return RedisMemoryService(db_url=url, enabled=True)


def _mysql_memory_builder(url: str) -> MemoryServiceABC:
    url = _mysql_runtime_url(_validate_mysql_url(url))
    return SqlMemoryService(db_url=url, is_async=False, enabled=True)


def _memory_vector_builder(_config: VectorBackendConfig) -> VectorStoreABC:
    return InMemoryVectorStore()


def _qdrant_vector_builder(config: VectorBackendConfig) -> VectorStoreABC:
    return QdrantVectorStore(
        url=_secret_value(config.url),
        api_key=_secret_value(config.api_key),
        collection=config.collection,
        dimensions=config.dimensions,
    )


def _local_object_builder(config: ObjectBackendConfig) -> ObjectStoreABC:
    return LocalObjectStore(config.local_path)


def _s3_object_builder(config: ObjectBackendConfig) -> ObjectStoreABC:
    return S3CompatibleObjectStore(
        bucket=config.bucket,
        endpoint_url=config.endpoint_url,
        region=config.region,
        access_key=_secret_value(config.access_key),
        secret_key=_secret_value(config.secret_key),
    )


class TenantStorageRouter:
    """Resolve tenant Session, Memory, Vector and Object storage adapters."""

    def __init__(self,
                 vector_factories: Optional[dict[str, VectorStoreFactory]] = None,
                 object_factories: Optional[dict[str, ObjectStoreFactory]] = None,
                 *,
                 redis_url: Optional[str] = None,
                 mysql_url: Optional[str] = None,
                 secret_resolver: Optional[SecretResolver] = None) -> None:
        self._session_services: dict[tuple[str, str], SessionServiceABC] = {}
        self._memory_services: dict[tuple[str, str], MemoryServiceABC] = {}
        self._vector_stores: dict[tuple[str, ...], TenantVectorStore] = {}
        self._object_stores: dict[tuple[str, ...], TenantObjectStore] = {}
        self._default_redis_url = redis_url or ""
        self._default_mysql_url = mysql_url or ""
        self._secret_resolver = secret_resolver or DEFAULT_SECRET_RESOLVER
        self._vector_factories: dict[str, VectorStoreFactory] = {
            "memory": _memory_vector_builder,
            "qdrant": _qdrant_vector_builder,
        }
        self._object_factories: dict[str, ObjectStoreFactory] = {
            "local": _local_object_builder,
            "s3": _s3_object_builder,
            "minio": _s3_object_builder,
            "cos": _s3_object_builder,
        }
        self._vector_factories.update(vector_factories or {})
        self._object_factories.update(object_factories or {})

    def register_vector_factory(self, backend: str, factory: VectorStoreFactory) -> None:
        """Register a vector adapter such as Milvus or pgvector."""
        self._vector_factories[backend] = factory

    def register_object_factory(self, backend: str, factory: ObjectStoreFactory) -> None:
        """Register or override an object-storage adapter."""
        self._object_factories[backend] = factory

    def _redis_url(self, tenant: Optional[Tenant]) -> str:
        configured = tenant.storage_config.redis_url if tenant is not None else None
        tenant_id = tenant.tenant_id if tenant is not None else None
        return _secret_value(configured, resolver=self._secret_resolver, tenant_id=tenant_id) or self._default_redis_url

    def _mysql_url(self, tenant: Optional[Tenant]) -> str:
        configured = tenant.storage_config.mysql_url if tenant is not None else None
        tenant_id = tenant.tenant_id if tenant is not None else None
        return _secret_value(configured, resolver=self._secret_resolver, tenant_id=tenant_id) or self._default_mysql_url

    def session_service(self, tenant: Optional[Tenant]) -> SessionServiceABC:
        backend = tenant.storage_config.session_backend.lower() if tenant is not None else "redis"
        if backend == "redis":
            identity = self._redis_url(tenant)
            if not identity:
                raise ValueError("redis session backend requires storage_config.redis_url or TRPC_SERVICE_REDIS_URL")
            builder = _redis_session_builder
        elif backend == "mysql":
            identity = self._mysql_url(tenant)
            if not identity:
                raise ValueError("mysql session backend requires storage_config.mysql_url or TRPC_SERVICE_MYSQL_URL")
            builder = _mysql_session_builder
        else:
            raise ValueError(f"unsupported session backend: {backend}")

        key = (backend, identity)
        if key not in self._session_services:
            self._session_services[key] = builder(identity)
        return self._session_services[key]

    def memory_service(self, tenant: Tenant) -> MemoryServiceABC:
        backend = tenant.storage_config.memory_backend.lower()
        if backend == "redis":
            identity = self._redis_url(tenant)
            if not identity:
                raise ValueError("redis memory backend requires storage_config.redis_url or TRPC_SERVICE_REDIS_URL")
            builder = _redis_memory_builder
        elif backend == "mysql":
            identity = self._mysql_url(tenant)
            if not identity:
                raise ValueError("mysql memory backend requires storage_config.mysql_url or TRPC_SERVICE_MYSQL_URL")
            builder = _mysql_memory_builder
        else:
            raise ValueError(f"unsupported memory backend: {backend}")

        key = (backend, identity)
        if key not in self._memory_services:
            self._memory_services[key] = builder(identity)
        return self._memory_services[key]

    def vector_store(self, tenant: Tenant) -> TenantVectorStore:
        """Return a tenant-scoped knowledge vector store."""
        config = tenant.storage_config.vector
        factory = self._vector_factories.get(config.backend)
        if factory is None:
            raise ValueError(f"vector backend '{config.backend}' requires a registered factory")
        identity = (
            tenant.tenant_id,
            config.backend,
            _secret_value(config.url, resolver=self._secret_resolver, tenant_id=tenant.tenant_id),
            _secret_value(config.api_key, resolver=self._secret_resolver, tenant_id=tenant.tenant_id),
            config.collection,
            str(config.dimensions or ""),
            config.embedding_model or "",
        )
        if identity not in self._vector_stores:
            self._vector_stores[identity] = TenantVectorStore(
                factory(
                    config.model_copy(
                        update={
                            "url":
                            SecretStr(
                                _secret_value(
                                    config.url,
                                    resolver=self._secret_resolver,
                                    tenant_id=tenant.tenant_id,
                                )) if config.url else None,
                            "api_key":
                            SecretStr(
                                _secret_value(
                                    config.api_key,
                                    resolver=self._secret_resolver,
                                    tenant_id=tenant.tenant_id,
                                )) if config.api_key else None,
                        })),
                tenant.tenant_id,
                backend_name=config.backend,
            )
        return self._vector_stores[identity]

    def object_store(self, tenant: Tenant) -> TenantObjectStore:
        """Return a tenant-scoped artifact object store."""
        config = tenant.storage_config.object
        factory = self._object_factories.get(config.backend)
        if factory is None:
            raise ValueError(f"object backend '{config.backend}' requires a registered factory")
        identity = (
            tenant.tenant_id,
            config.backend,
            config.endpoint_url or "",
            config.bucket,
            config.region or "",
            _secret_value(config.access_key, resolver=self._secret_resolver, tenant_id=tenant.tenant_id),
            _secret_value(config.secret_key, resolver=self._secret_resolver, tenant_id=tenant.tenant_id),
            config.local_path,
        )
        if identity not in self._object_stores:
            self._object_stores[identity] = TenantObjectStore(
                factory(
                    config.model_copy(
                        update={
                            "access_key":
                            SecretStr(
                                _secret_value(
                                    config.access_key,
                                    resolver=self._secret_resolver,
                                    tenant_id=tenant.tenant_id,
                                )) if config.access_key else None,
                            "secret_key":
                            SecretStr(
                                _secret_value(
                                    config.secret_key,
                                    resolver=self._secret_resolver,
                                    tenant_id=tenant.tenant_id,
                                )) if config.secret_key else None,
                        })),
                tenant.tenant_id,
                backend_name=config.backend,
            )
        return self._object_stores[identity]

    async def close(self) -> None:
        services = [
            *self._session_services.values(),
            *self._memory_services.values(),
            *self._vector_stores.values(),
            *self._object_stores.values(),
        ]
        self._session_services.clear()
        self._memory_services.clear()
        self._vector_stores.clear()
        self._object_stores.clear()
        seen: set[int] = set()
        for service in services:
            if id(service) in seen:
                continue
            seen.add(id(service))
            await service.close()
