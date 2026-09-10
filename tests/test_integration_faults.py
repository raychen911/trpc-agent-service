"""Fault tests against real Redis/PostgreSQL containers.

These tests are intentionally isolated from the default unit suite.  Run with
``pytest -m integration`` on a host with Docker available.
"""

from __future__ import annotations

import os
import time
from threading import RLock

import pytest

pytestmark = pytest.mark.integration

docker = pytest.importorskip("testcontainers.core.container")
redis_container_mod = pytest.importorskip("testcontainers.redis")
postgres_container_mod = pytest.importorskip("testcontainers.postgres")
psycopg = pytest.importorskip("psycopg")

from trpc_service.database import upgrade_database
from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.postgres_session_mailbox import PostgresSessionMailboxStore


@pytest.fixture(scope="module")
def redis_url():
    try:
        with redis_container_mod.RedisContainer("redis:7-alpine") as container:
            host = container.get_container_host_ip()
            port = container.get_exposed_port(6379)
            yield f"redis://{host}:{port}/0"
    except Exception as exc:  # noqa: BLE001 - infrastructure startup is environment-dependent
        pytest.skip(f"Docker/Redis unavailable: {exc}")


@pytest.fixture(scope="module")
def postgres_connection():
    try:
        with postgres_container_mod.PostgresContainer("postgres:16-alpine") as container:
            os.environ["TRPC_POSTGRES_AUTO_CREATE"] = "1"
            connection_url = container.get_connection_url().replace(
                "postgresql+psycopg2://", "postgresql://", 1
            )
            upgrade_database(connection_url)
            conn = psycopg.connect(connection_url)
            try:
                yield conn
            finally:
                conn.close()
    except Exception as exc:  # noqa: BLE001 - infrastructure startup is environment-dependent
        pytest.skip(f"Docker/PostgreSQL unavailable: {exc}")


def test_real_redis_retry_and_dead_letter(redis_url):
    transport = RedisStreamsTransport(
        redis_url,
        stream_key="integration:session-ready:stream",
        group="integration-workers",
        consumer="fault-test",
        max_attempts=2,
    )
    calls = []
    transport.submit({"tenant_id": "t", "aggregate_id": "s"})

    def fail(_payload):
        calls.append(1)
        raise RuntimeError("provider down")

    transport.consume_once(fail, block_ms=100)
    transport.consume_once(fail, block_ms=100)
    assert len(calls) == 2
    assert transport.client.xlen(transport.dead_letter_key) == 1
    transport.close()


def test_real_postgres_mailbox_lease_expiry_and_fencing(postgres_connection):
    mailbox = PostgresSessionMailboxStore(postgres_connection, RLock())
    mailbox.accept("tenant-it", "session-it", "message-it")
    first = mailbox.claim_session("tenant-it", "session-it", "worker-a", 1)
    assert first.claimed and first.lease is not None
    time.sleep(1.1)
    second = mailbox.claim_session("tenant-it", "session-it", "worker-b", 5)
    assert second.claimed and second.lease is not None
    with pytest.raises(SessionLeaseLost):
        mailbox.commit(first.lease)
    mailbox.commit(second.lease)
