import asyncio
from typing import Any

import pytest

from trpc_service.config.settings import Role, ServiceSettings
from trpc_service.storage.artifact_gc import ArtifactGarbageCollector


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Connection:
    def __init__(self, rows: list[dict[str, object]], *, update_status: str = "UPDATE 1") -> None:
        self.rows = rows
        self.update_status = update_status
        self.fetch_args: tuple[object, ...] = ()
        self.executions: list[tuple[object, ...]] = []
        self.referenced_keys: set[str] = set()

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        assert "status='staged'" in sql
        assert "clock_timestamp()" in sql
        assert "FOR UPDATE SKIP LOCKED" in sql
        self.fetch_args = args
        return self.rows

    async def execute(self, sql: str, *args: object) -> str:
        assert "SET status='deleted'" in sql
        self.executions.append(args)
        return self.update_status

    async def fetchval(self, sql: str, key: str) -> bool:
        assert "SELECT EXISTS" in sql
        return key in self.referenced_keys


class _Acquire:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _Connection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def acquire(self) -> _Acquire:
        return _Acquire(self.connection)


class _Objects:
    def __init__(self, *, fail_key: str | None = None) -> None:
        self.fail_key = fail_key
        self.discards: list[tuple[str, str | None]] = []

    async def discard(self, key: str, *, tenant_id: str | None = None) -> None:
        self.discards.append((key, tenant_id))
        if key == self.fail_key:
            raise OSError("provider unavailable")


class _ListingObjects(_Objects):
    def __init__(self, keys: tuple[str, ...]) -> None:
        super().__init__()
        self.keys = keys
        self.orphan_discards: list[str] = []

    async def list_staged(self, **_kwargs: object) -> tuple[tuple[str, ...], None]:
        return self.keys, None

    async def discard_unreferenced_staged(self, key: str) -> None:
        self.orphan_discards.append(key)


def _row(tenant: str, artifact: str, key: str) -> dict[str, object]:
    return {"tenant_id": tenant, "artifact_id": artifact, "object_key": key}


@pytest.mark.asyncio
async def test_gc_selects_only_expired_staged_rows_and_marks_successes_deleted() -> None:
    connection = _Connection(
        [
            _row("tenant-a", "artifact-a", "tenants/a/staging/one"),
            _row("tenant-b", "artifact-b", "tenants/b/staging/two"),
        ]
    )
    objects = _Objects()
    collector = ArtifactGarbageCollector(
        _Pool(connection), objects, ttl_seconds=3_600, batch_size=20, poll_seconds=1
    )

    result = await collector.run_once()

    assert result.status == "pass"
    assert (result.scanned, result.deleted, result.failed) == (2, 2, 0)
    assert connection.fetch_args == (3_600.0, 20)
    assert objects.discards == [
        ("tenants/a/staging/one", "tenant-a"),
        ("tenants/b/staging/two", "tenant-b"),
    ]
    assert len(connection.executions) == 2


@pytest.mark.asyncio
async def test_gc_keeps_failed_object_staged_for_idempotent_retry() -> None:
    failed_key = "tenants/a/staging/failed"
    connection = _Connection(
        [
            _row("tenant-a", "artifact-a", failed_key),
            _row("tenant-b", "artifact-b", "tenants/b/staging/ok"),
        ]
    )
    collector = ArtifactGarbageCollector(
        _Pool(connection),
        _Objects(fail_key=failed_key),
        ttl_seconds=60,
        batch_size=2,
        poll_seconds=1,
    )

    result = await collector.run_once()

    assert result.status == "fail"
    assert (result.scanned, result.deleted, result.failed) == (2, 1, 1)
    assert connection.executions == [("tenant-b", "artifact-b", "tenants/b/staging/ok")]


@pytest.mark.asyncio
async def test_gc_deletes_only_unreferenced_expired_bucket_objects() -> None:
    referenced = "tenants/" + "a" * 64 + "/staging/11111111-1111-1111-1111-111111111111"
    orphan = "tenants/" + "b" * 64 + "/staging/22222222-2222-2222-2222-222222222222"
    connection = _Connection([])
    connection.referenced_keys.add(referenced)
    objects = _ListingObjects((referenced, orphan))
    collector = ArtifactGarbageCollector(
        _Pool(connection), objects, ttl_seconds=60, batch_size=10, poll_seconds=1
    )

    result = await collector.run_once()

    assert result.orphan_deleted == 1
    assert result.deleted == 1
    assert objects.orphan_discards == [orphan]


@pytest.mark.asyncio
async def test_gc_loop_stops_cooperatively() -> None:
    stop = asyncio.Event()
    connection = _Connection([])
    collector = ArtifactGarbageCollector(
        _Pool(connection), _Objects(), ttl_seconds=60, batch_size=1, poll_seconds=60
    )
    task = asyncio.create_task(collector.run(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, timeout=1)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"ttl_seconds": 59}, "ttl_seconds"),
        ({"batch_size": 0}, "batch_size"),
        ({"poll_seconds": 0}, "poll_seconds"),
    ],
)
def test_gc_rejects_unsafe_limits(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ArtifactGarbageCollector(_Pool(_Connection([])), _Objects(), **kwargs)


def test_gc_settings_and_role_are_conservative() -> None:
    settings = ServiceSettings(_env_file=None)
    assert Role.ARTIFACT_GC.value == "artifact-gc"
    assert settings.artifact_gc_batch_size == 100
    assert settings.artifact_gc_poll_seconds == 60
    assert settings.artifact_staging_ttl_seconds == 86_400
