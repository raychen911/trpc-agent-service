from __future__ import annotations

import random

from trpc_service.channels.runtime import ConnectionRetryPolicy


def test_connection_backoff_is_bounded_jittered_resettable_and_cancellable() -> None:
    policy = ConnectionRetryPolicy(random_source=random.Random(7))
    delays = [policy.next_delay() for _ in range(8)]
    bases = [1, 2, 4, 8, 16, 30, 30, 30]
    assert all(base <= delay <= base * 1.2 for base, delay in zip(bases, delays))
    policy.mark_stable(60)
    assert 1 <= policy.next_delay() <= 1.2
    policy.cancel()
    assert policy.cancelled


def test_auth_failure_waits_for_configuration_change() -> None:
    policy = ConnectionRetryPolicy()
    policy.authentication_failed(config_version=3)
    assert not policy.may_authenticate(config_version=3)
    assert policy.may_authenticate(config_version=4)
