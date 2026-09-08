class StorageError(Exception):
    pass


class VersionConflictError(StorageError):
    pass


class DuplicateMessageError(StorageError):
    pass


class LockNotAcquiredError(StorageError):
    pass


class StorageNotFoundError(StorageError):
    pass


class InvalidArtifactKeyError(StorageError):
    pass
