# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Storage adapters and distributed execution guard."""

from .factory import StorageBundle
from .factory import StorageProviderFactory
from .factory import UnsupportedBackendError
from .factory import ExternalStorageProvider
from .guard import InMemorySessionExecutionGuard
from .guard import RedisSessionExecutionGuard
from .guard import SessionExecutionGuard
from .guard import SessionLockLostError
from .guard import SessionLockTimeoutError
from .guard import SessionLease
from .session_wrapper import RequestTaggingSessionService

__all__ = [
    "StorageBundle",
    "StorageProviderFactory",
    "UnsupportedBackendError",
    "ExternalStorageProvider",
    "InMemorySessionExecutionGuard",
    "RedisSessionExecutionGuard",
    "SessionExecutionGuard",
    "SessionLockLostError",
    "SessionLockTimeoutError",
    "SessionLease",
    "RequestTaggingSessionService",
]
