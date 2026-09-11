"""Finite retries and circuit breaking never retry unknown side effects."""

import pytest

from trpc_service.reliability import CircuitBreaker
from trpc_service.reliability import RetryPolicy
from trpc_service.reliability import retry_async
from trpc_service.storage import TransientStorageError
from trpc_service.storage import UnavailableError


@pytest.mark.asyncio
async def test_retry_is_bounded_and_succeeds_within_budget() -> None:
    calls = 0
    delays = []

    async def operation():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransientStorageError("temporary")
        return "ok"

    result = await retry_async(
        operation,
        RetryPolicy(max_attempts=3, max_elapsed_seconds=10, initial_delay_seconds=0.1, jitter_ratio=0),
        sleep=lambda delay: append_delay(delays, delay),
    )
    assert result == "ok"
    assert delays == [0.1, 0.2]


@pytest.mark.asyncio
async def test_non_idempotent_operation_is_never_automatically_retried() -> None:
    calls = 0

    async def operation():
        nonlocal calls
        calls += 1
        raise TransientStorageError("unknown outcome")

    with pytest.raises(TransientStorageError):
        await retry_async(operation, RetryPolicy(), idempotent=False)
    assert calls == 1


def test_circuit_breaker_opens_after_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=2, recovery_seconds=60)
    breaker.record_failure()
    breaker.record_failure()
    with pytest.raises(UnavailableError, match="circuit"):
        breaker.before_call()


async def append_delay(target, delay):
    target.append(delay)
