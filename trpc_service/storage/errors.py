"""Storage errors shared by concrete backend implementations."""


class SessionVersionConflict(RuntimeError):
    """Raised when a Session compare-and-swap version is stale."""


class StoredObjectNotFound(LookupError):
    """Raised when a tenant-scoped Artifact does not exist."""


class ArtifactIntegrityError(ValueError):
    """Raised when uploaded bytes do not match declared Artifact metadata."""


class EmbeddingDimensionError(ValueError):
    """Raised when an embedding provider violates its declared dimensions."""


class StaleExecutionLease(RuntimeError):
    """Raised when an expired Worker attempts to commit with an old fence."""


class ExecutionAlreadyRunning(RuntimeError):
    """Raised when another Worker still owns a valid Inbox execution lease."""


class IdempotencyConflict(ValueError):
    """Raised when one external message ID is reused with different content."""
