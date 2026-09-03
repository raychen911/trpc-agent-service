"""
Storage package initialization.
"""

from trpc_service.storage.base import BaseStorageAdapter
from trpc_service.storage.memory_adapter import InMemoryStorageAdapter
from trpc_service.storage.sql_adapter import SQLStorageAdapter
from trpc_service.storage.redis_adapter import RedisStorageAdapter
from trpc_service.storage.factory import StorageFactory, storage_factory
from trpc_service.storage.migration import StorageMigrationManager, migration_manager, MigrationStats

__all__ = [
    "BaseStorageAdapter",
    "InMemoryStorageAdapter",
    "SQLStorageAdapter",
    "RedisStorageAdapter",
    "StorageFactory",
    "storage_factory",
    "StorageMigrationManager",
    "migration_manager",
    "MigrationStats",
]
