"""
Storage Factory to instantiate and cache storage adapters per tenant.
"""

from typing import Dict
from trpc_service.config.models import StorageBackendConfig, StorageType
from trpc_service.storage.base import BaseStorageAdapter
from trpc_service.storage.memory_adapter import InMemoryStorageAdapter
from trpc_service.storage.sql_adapter import SQLStorageAdapter
from trpc_service.storage.redis_adapter import RedisStorageAdapter


class StorageFactory:
    """Factory to provide appropriate StorageAdapter instance."""

    def __init__(self):
        self._instances: Dict[str, BaseStorageAdapter] = {}
        # Default fallback in-memory adapter
        self._default_memory = InMemoryStorageAdapter()

    def get_adapter(self, config: StorageBackendConfig) -> BaseStorageAdapter:
        if config.storage_type == StorageType.IN_MEMORY:
            return self._default_memory

        if config.storage_type == StorageType.SQL:
            key = f"sql:{config.sql_url}"
            if key not in self._instances:
                self._instances[key] = SQLStorageAdapter(db_url=config.sql_url or "sqlite:///./data/trpc_agent.db")
            return self._instances[key]

        if config.storage_type == StorageType.REDIS:
            key = f"redis:{config.redis_url}"
            if key not in self._instances:
                self._instances[key] = RedisStorageAdapter(redis_url=config.redis_url or "redis://localhost:6379/0")
            return self._instances[key]

        # Hybrid/Default fallback
        return self._default_memory


storage_factory = StorageFactory()
