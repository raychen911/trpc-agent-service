"""Encrypted event object-store contract tests."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from trpc_service.runtime.event_store import (
    EventStoreError,
    LocalEventObjectStore,
    RedisEventObjectStore,
    SqlEventObjectStore,
)
from trpc_service.storage import Database
from trpc_service.storage.models import EventObject, Tenant


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.closed = False

    async def set(self, name: str, value: bytes, *, nx: bool = False) -> object:
        if nx and name in self.values:
            return None
        self.values[name] = value
        return True

    async def get(self, name: str) -> object:
        return self.values.get(name)

    async def aclose(self) -> None:
        self.closed = True


def envelope(value: str = "ciphertext") -> tuple[str, str, str]:
    ciphertext = f"v1.{value}"
    digest = hashlib.sha256(ciphertext.encode()).hexdigest()
    object_key = f"sdk-events/v1/{'a' * 64}/{digest}"
    return object_key, ciphertext, digest


@pytest.mark.asyncio
async def test_local_store_is_create_only_idempotent_and_bounded(tmp_path: Path) -> None:
    store = LocalEventObjectStore(tmp_path / "events", max_object_bytes=1_024)
    object_key, ciphertext, digest = envelope()

    await store.put_if_absent(
        object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
    )
    await store.put_if_absent(
        object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
    )

    assert await store.get(object_key, tenant_id="tenant-a") == ciphertext
    target = tmp_path / "events" / "v1" / ("a" * 64) / digest
    target.write_text("v1.tampered", encoding="ascii")
    with pytest.raises(EventStoreError, match="conflicts"):
        await store.put_if_absent(
            object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
        )


@pytest.mark.asyncio
async def test_local_store_rejects_path_digest_and_size_ambiguity(tmp_path: Path) -> None:
    store = LocalEventObjectStore(tmp_path / "events", max_object_bytes=16)
    object_key, ciphertext, digest = envelope("small")

    with pytest.raises(EventStoreError, match="key"):
        await store.put_if_absent(
            "../escape", ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
        )
    with pytest.raises(EventStoreError, match="key and digest"):
        await store.put_if_absent(
            f"sdk-events/v1/{'a' * 64}/{'b' * 64}",
            ciphertext,
            tenant_id="tenant-a",
            ciphertext_sha256=digest,
        )
    large_key, large_ciphertext, large_digest = envelope("x" * 32)
    with pytest.raises(EventStoreError, match="size"):
        await store.put_if_absent(
            large_key,
            large_ciphertext,
            tenant_id="tenant-a",
            ciphertext_sha256=large_digest,
        )
    with pytest.raises(EventStoreError, match="unavailable"):
        await store.get(object_key, tenant_id="tenant-a")


@pytest.mark.asyncio
async def test_redis_store_uses_set_nx_and_detects_conflicting_existing_value() -> None:
    client = FakeRedis()
    store = RedisEventObjectStore(client, max_object_bytes=1_024)
    object_key, ciphertext, digest = envelope()

    await store.put_if_absent(
        object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
    )
    await store.put_if_absent(
        object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
    )
    assert await store.get(object_key, tenant_id="tenant-a") == ciphertext

    redis_key = next(iter(client.values))
    client.values[redis_key] = b"v1.tampered"
    with pytest.raises(EventStoreError, match="conflicts"):
        await store.put_if_absent(
            object_key, ciphertext, tenant_id="tenant-a", ciphertext_sha256=digest
        )

    await store.aclose()
    assert client.closed is True


@pytest.mark.asyncio
async def test_sql_store_is_tenant_scoped_create_only_and_digest_checked(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{(tmp_path / 'events.db').as_posix()}")
    await database.create_schema()
    async with database.session_factory.begin() as session:
        session.add_all(
            [
                Tenant(tenant_id="tenant-a", display_name="A"),
                Tenant(tenant_id="tenant-b", display_name="B"),
            ]
        )
    store = SqlEventObjectStore(database, max_object_bytes=1_024)
    object_key, ciphertext, digest = envelope()
    try:
        await store.put_if_absent(
            object_key,
            ciphertext,
            tenant_id="tenant-a",
            ciphertext_sha256=digest,
        )
        await store.put_if_absent(
            object_key,
            ciphertext,
            tenant_id="tenant-a",
            ciphertext_sha256=digest,
        )
        assert await store.get(object_key, tenant_id="tenant-a") == ciphertext
        with pytest.raises(EventStoreError, match="unavailable"):
            await store.get(object_key, tenant_id="tenant-b")

        async with database.session_factory.begin() as session:
            row = await session.get(EventObject, ("tenant-a", object_key))
            assert row is not None
            row.ciphertext = "v1.tampered"
        with pytest.raises(EventStoreError, match=r"size|digest"):
            await store.get(object_key, tenant_id="tenant-a")
    finally:
        await database.dispose()
