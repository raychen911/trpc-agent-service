"""Opt-in tests for PostgreSQL-specific locking and claim semantics.

Set TAP_TEST_POSTGRES_URL to a disposable PostgreSQL database. These tests never
fall back to SQLite, so a green integration run is meaningful evidence.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete

from tenant_agent.storage import schema
from tenant_agent.storage.base import OutboxItem
from tenant_agent.storage.sql import SqlPlane

POSTGRES_URL = os.getenv("TAP_TEST_POSTGRES_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not POSTGRES_URL, reason="TAP_TEST_POSTGRES_URL is not configured"),
]


@pytest.mark.asyncio
async def test_postgres_skip_locked_claim_and_renewable_session_lease() -> None:
    assert POSTGRES_URL is not None
    first = SqlPlane(POSTGRES_URL)
    second = SqlPlane(POSTGRES_URL)
    await first.initialize()
    await second.initialize()
    suffix = uuid.uuid4().hex
    tenant_id = f"pg-{suffix[:12]}"
    session_id = f"session-{suffix}"
    outbox_id = f"outbox-{suffix}"
    entered = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async with first.acquire_session(
            tenant_id=tenant_id,
            session_id=session_id,
            owner="holder",
            wait_timeout=2,
            lease_seconds=0.3,
        ):
            entered.set()
            await release.wait()

    async def waiter() -> None:
        await entered.wait()
        async with second.acquire_session(
            tenant_id=tenant_id,
            session_id=session_id,
            owner="waiter",
            wait_timeout=3,
            lease_seconds=0.3,
        ):
            return None

    holder_task = asyncio.create_task(holder())
    waiter_task = asyncio.create_task(waiter())
    await entered.wait()
    await asyncio.sleep(0.7)
    assert not waiter_task.done(), "the PostgreSQL lease expired instead of renewing"
    release.set()
    await asyncio.gather(holder_task, waiter_task)

    await first.enqueue_outbox(
        OutboxItem(
            outbox_id=outbox_id,
            tenant_id=tenant_id,
            kind="test",
            payload={},
            status="pending",
            attempts=0,
            available_at=datetime.now(UTC),
        )
    )
    claims = await asyncio.gather(
        first.claim_outbox("one", limit=1, now=datetime.now(UTC)),
        second.claim_outbox("two", limit=1, now=datetime.now(UTC)),
    )
    assert sum(len(batch) for batch in claims) == 1

    async with first.engine.begin() as connection:
        await connection.execute(delete(schema.outbox).where(schema.outbox.c.outbox_id == outbox_id))
        await connection.execute(
            delete(schema.session_leases).where(
                schema.session_leases.c.tenant_id == tenant_id,
                schema.session_leases.c.session_id == session_id,
            )
        )
    await first.close()
    await second.close()
