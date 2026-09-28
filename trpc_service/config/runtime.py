"""Validated settings shared by lease-based background workers."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LeasedWorkerConfig:
    """Keep polling, lease and retry policy consistent as one immutable value."""

    lease_seconds: int
    poll_interval_seconds: float
    retry_base_seconds: float
    retry_max_seconds: float
    retry_jitter_ratio: float
    max_attempts: int

    def __post_init__(self) -> None:
        if self.lease_seconds < 3:
            raise ValueError("Worker lease must be at least three seconds")
        if self.poll_interval_seconds <= 0 or self.retry_base_seconds < 0:
            raise ValueError("Worker polling and retry intervals are invalid")
        if self.retry_max_seconds <= 0:
            raise ValueError("Worker retry cap must be positive")
        if not 0 <= self.retry_jitter_ratio <= 0.5:
            raise ValueError("Worker retry jitter must be between zero and 0.5")
        if self.max_attempts < 1:
            raise ValueError("Worker maximum attempts must be positive")
