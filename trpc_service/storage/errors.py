"""Stable storage error categories used by retries and API boundaries."""


class StorageError(RuntimeError):
    """Base class for platform storage failures."""


class TransientStorageError(StorageError):
    """An operation may succeed when retried within the configured budget."""


class ConflictError(StorageError):
    """An optimistic revision or idempotency constraint was violated."""


class UnavailableError(TransientStorageError):
    """The selected backend is temporarily unavailable."""


class PermanentStorageError(StorageError):
    """The operation cannot succeed without configuration or data repair."""


class TenantBoundaryError(PermanentStorageError):
    """A caller attempted to access data outside its trusted tenant scope."""
