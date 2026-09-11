"""Bounded transient retries and a small circuit breaker."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass

from trpc_service.storage import TransientStorageError
from trpc_service.storage import UnavailableError


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    max_elapsed_seconds: float = 10.0
    initial_delay_seconds: float = 0.1
    max_delay_seconds: float = 2.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.max_elapsed_seconds <= 0:
            raise ValueError("retry attempts and elapsed budget must be positive")


async def retry_async(operation, policy: RetryPolicy, *, idempotent=True, sleep=asyncio.sleep):
    started = time.monotonic()
    for attempt in range(1, policy.max_attempts + 1):
        try:
            remaining = policy.max_elapsed_seconds - (time.monotonic() - started)
            if remaining <= 0:
                raise asyncio.TimeoutError("operation exceeded its total deadline")
            return await asyncio.wait_for(operation(), timeout=remaining)
        except TransientStorageError:
            if not idempotent or attempt == policy.max_attempts:
                raise
            base_delay = min(policy.initial_delay_seconds * (2**(attempt - 1)), policy.max_delay_seconds)
            jitter = base_delay * policy.jitter_ratio * random.uniform(-1, 1)
            delay = max(0, base_delay + jitter)
            if time.monotonic() - started + delay > policy.max_elapsed_seconds:
                raise
            await sleep(delay)
    raise AssertionError("retry loop exhausted unexpectedly")


class CircuitBreaker:
    def __init__(self, *, failure_threshold: int, recovery_seconds: float) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.failures = 0
        self.opened_at: float | None = None

    def before_call(self) -> None:
        if self.opened_at is None:
            return
        if time.monotonic() - self.opened_at >= self.recovery_seconds:
            self.failures = 0
            self.opened_at = None
            return
        raise UnavailableError("storage circuit breaker is open")

    def record_success(self) -> None:
        self.failures = 0
        self.opened_at = None

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.opened_at = time.monotonic()
