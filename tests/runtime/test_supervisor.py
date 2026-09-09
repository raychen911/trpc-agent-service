"""Fair scheduling, backoff, and cooperative-shutdown tests."""

from __future__ import annotations

import asyncio

import pytest

from trpc_service.runtime.supervisor import (
    ExponentialBackoff,
    FairTenantRing,
    PollingSupervisor,
)


class StaticCatalog:
    def __init__(self, tenant_ids: tuple[str, ...]) -> None:
        self.tenant_ids = tenant_ids
        self.calls = 0

    async def list_active_tenant_ids(self) -> tuple[str, ...]:
        self.calls += 1
        return self.tenant_ids


def test_fair_ring_preserves_cursor_and_deterministically_adds_tenants() -> None:
    ring = FairTenantRing()
    ring.update(("tenant-c", "tenant-a", "tenant-b", "tenant-a"))

    assert ring.snapshot() == ("tenant-a", "tenant-b", "tenant-c")
    assert (ring.take(), ring.take()) == ("tenant-a", "tenant-b")
    ring.update(("tenant-a", "tenant-c", "tenant-d"))

    assert ring.snapshot() == ("tenant-c", "tenant-a", "tenant-d")
    assert tuple(ring.take() for _ in range(6)) == (
        "tenant-c",
        "tenant-a",
        "tenant-d",
        "tenant-c",
        "tenant-a",
        "tenant-d",
    )


def test_exponential_backoff_is_bounded_and_resettable() -> None:
    backoff = ExponentialBackoff(0.25, 1.0)

    assert [backoff.take() for _ in range(5)] == [0.25, 0.5, 1.0, 1.0, 1.0]
    backoff.reset()
    assert backoff.take() == 0.25


@pytest.mark.asyncio
async def test_supervisor_round_robins_busy_tenants_without_starvation() -> None:
    catalog = StaticCatalog(("tenant-a", "tenant-b", "tenant-c"))
    stop = asyncio.Event()
    visits: list[str] = []

    async def poll(tenant_id: str) -> bool:
        visits.append(tenant_id)
        if len(visits) == 9:
            stop.set()
        return True

    supervisor = PollingSupervisor(
        role="worker",
        catalog=catalog,
        poll_tenant=poll,
        catalog_refresh_seconds=60,
        idle_backoff_initial_seconds=0.001,
        idle_backoff_max_seconds=0.002,
    )
    await asyncio.wait_for(supervisor.run(stop), timeout=1)

    assert visits == ["tenant-a", "tenant-b", "tenant-c"] * 3
    assert catalog.calls == 1


@pytest.mark.asyncio
async def test_shutdown_cancels_and_awaits_the_inflight_claim() -> None:
    catalog = StaticCatalog(("tenant-a",))
    stop = asyncio.Event()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def poll(tenant_id: str) -> bool:
        assert tenant_id == "tenant-a"
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        return False

    supervisor = PollingSupervisor(
        role="worker",
        catalog=catalog,
        poll_tenant=poll,
        catalog_refresh_seconds=60,
        idle_backoff_initial_seconds=0.001,
        idle_backoff_max_seconds=0.002,
    )
    running = asyncio.create_task(supervisor.run(stop))
    await asyncio.wait_for(entered.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(running, timeout=1)

    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_empty_catalog_uses_backoff_instead_of_busy_looping() -> None:
    catalog = StaticCatalog(())
    stop = asyncio.Event()
    supervisor = PollingSupervisor(
        role="dispatcher",
        catalog=catalog,
        poll_tenant=lambda _: asyncio.sleep(0, result=False),
        catalog_refresh_seconds=60,
        idle_backoff_initial_seconds=0.005,
        idle_backoff_max_seconds=0.01,
    )

    running = asyncio.create_task(supervisor.run(stop))
    await asyncio.sleep(0.035)
    stop.set()
    await asyncio.wait_for(running, timeout=1)

    assert catalog.calls == 1
