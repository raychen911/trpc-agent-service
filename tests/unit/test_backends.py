from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import pytest

from trpc_service.storage.artifacts import (
    InMemoryArtifactStore,
    S3ArtifactStore,
    _ArtifactStoreBase,
    build_media_idempotency_key,
    media_idempotency_key,
)
from trpc_service.storage.redis_projection import RedisProjectionStore


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.put_calls = 0
        self.copy_calls = 0
        self.fail_put = False
        self.fail_copy = False
        self.fail_delete = False
        self.head_errors: dict[str, BaseException] = {}
        self.get_response: dict[str, Any] | None = None
        self.list_response: dict[str, Any] = {"Contents": []}

    def put_object(self, *, Bucket, Key, Body, Metadata):
        self.put_calls += 1
        if self.fail_put:
            self.objects[Key] = (Body, Metadata)
            raise RuntimeError("upload failed")
        self.objects[Key] = (Body, Metadata)

    def head_object(self, *, Bucket, Key):
        if Key in self.head_errors:
            raise self.head_errors[Key]
        return {
            "Metadata": self.objects[Key][1],
            "ContentLength": len(self.objects[Key][0]),
        }

    def copy_object(self, *, Bucket, Key, CopySource, MetadataDirective):
        self.copy_calls += 1
        if self.fail_copy:
            raise RuntimeError("copy failed")
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket, Key):
        if self.fail_delete:
            raise RuntimeError("delete failed")
        self.objects.pop(Key, None)

    def get_object(self, *, Bucket, Key):
        if self.get_response is not None:
            return self.get_response
        return {"Body": _Body(self.objects[Key][0])}

    def list_objects_v2(self, **_kwargs: object) -> dict[str, Any]:
        return self.list_response


class _Body:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def read(self) -> bytes:
        return self._content


class _Channel(Enum):
    FEISHU = "feishu"


class _ErrorWithResponse(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _ExplodingRecordStore(InMemoryArtifactStore):
    @staticmethod
    def _record(*args: Any, **kwargs: Any):
        raise RuntimeError("record failed")


class _BrokenHeadStore(InMemoryArtifactStore):
    def __init__(self, *, head: dict[str, Any] | None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._head = head
        self._head_calls = 0

    async def _head_if_present(self, key: str) -> dict[str, Any] | None:
        self._head_calls += 1
        if self._head_calls == 1:
            return None
        return self._head


class LuaProjectionRedis:
    """Small Redis double; the real Lua script is covered by the Redis contract suite."""

    def __init__(self) -> None:
        self.values: dict[str, tuple[int, str]] = {}

    async def eval(self, _script, _key_count, key, sequence, payload, _ttl):
        current = self.values.get(key)
        if current is not None and current[0] > sequence:
            return 0
        self.values[key] = (sequence, payload)
        return 1

    async def hmget(self, key, *_fields):
        current = self.values.get(key)
        if current is None:
            return [None, None]
        return [str(current[0]).encode(), json.dumps(json.loads(current[1])).encode()]


@pytest.mark.asyncio
async def test_artifact_staging_checks_checksum_and_tenant_scope() -> None:
    client = FakeS3()
    store = S3ArtifactStore(client, bucket="bucket")
    content = b"artifact"
    checksum = hashlib.sha256(content).hexdigest()
    staged = await store.stage("tenant-a", "artifact", content, checksum=checksum)
    committed = await store.commit("tenant-a", "artifact", staged)
    assert "/artifacts/" in committed
    assert staged not in client.objects
    with pytest.raises(ValueError, match="belong"):
        await store.commit("tenant-b", "artifact", committed)
    with pytest.raises(ValueError, match="checksum"):
        await store.stage("tenant-a", "artifact", content, checksum="wrong")


@pytest.mark.asyncio
async def test_s3_staging_scan_is_bounded_by_age_and_key_shape() -> None:
    client = FakeS3()
    valid = "tenants/" + "a" * 64 + "/staging/11111111-1111-1111-1111-111111111111"
    now = datetime.now(UTC)
    client.list_response = {
        "Contents": [
            {"Key": valid, "LastModified": now - timedelta(hours=2)},
            {"Key": valid.replace("/staging/", "/artifacts/"), "LastModified": now},
            {"Key": "tenants/bad/staging/value", "LastModified": now - timedelta(days=2)},
        ],
        "NextContinuationToken": "next-page",
    }
    store = S3ArtifactStore(client, bucket="bucket")

    keys, token = await store.list_staged(older_than=now - timedelta(hours=1), limit=100)

    assert keys == (valid,)
    assert token == "next-page"
    await store.discard_unreferenced_staged(valid)
    with pytest.raises(ValueError, match="valid staged"):
        await store.discard_unreferenced_staged("tenants/bad/staging/value")


@pytest.mark.asyncio
@pytest.mark.parametrize("store_kind", ["memory", "s3"])
async def test_media_ingestion_is_idempotent_and_returns_safe_metadata(store_kind: str) -> None:
    client = FakeS3()
    store = (
        InMemoryArtifactStore(max_size_bytes=1024)
        if store_kind == "memory"
        else S3ArtifactStore(client, bucket="bucket", max_size_bytes=1024)
    )
    content = b"downloaded media"
    checksum = hashlib.sha256(content).hexdigest()

    first = await store.ingest_media(
        "tenant-a",
        "feishu",
        "message-1",
        "provider-media-1",
        content,
        checksum=checksum,
        filename="report.pdf",
        content_type="application/pdf",
    )
    second = await store.ingest_media(
        "tenant-a", "feishu", "message-1", "provider-media-1", content
    )

    assert first == second
    assert first.checksum == checksum
    assert first.size_bytes == len(content)
    assert first.object_key.startswith("tenants/")
    assert "/artifacts/" in first.object_key
    assert "content" not in first.model_dump()
    assert "encryption_key" not in first.model_dump()
    assert await store.read("tenant-a", first.object_key) == content
    if store_kind == "s3":
        assert client.put_calls == 1
        assert client.copy_calls == 1
        assert all("/staging/" not in key for key in client.objects)


@pytest.mark.asyncio
async def test_media_ingestion_rejects_checksum_size_and_cleans_failed_upload() -> None:
    client = FakeS3()
    store = S3ArtifactStore(client, bucket="bucket", max_size_bytes=4)
    content = b"media"
    with pytest.raises(ValueError, match="maximum size"):
        await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert client.objects == {}

    store = S3ArtifactStore(client, bucket="bucket", max_size_bytes=1024)
    with pytest.raises(ValueError, match="checksum"):
        await store.ingest_media(
            "tenant-a", "feishu", "message", "media", content, checksum="wrong"
        )
    assert client.objects == {}

    client.fail_put = True
    with pytest.raises(RuntimeError, match="upload"):
        await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert client.objects == {}

    client.fail_put = False
    client.fail_copy = True
    with pytest.raises(RuntimeError, match="copy"):
        await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert client.objects == {}


@pytest.mark.asyncio
async def test_media_ingestion_is_tenant_isolated() -> None:
    store = InMemoryArtifactStore()
    content = b"tenant scoped"
    first = await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    second = await store.ingest_media("tenant-b", "feishu", "message", "media", content)
    assert first.object_key != second.object_key
    with pytest.raises(ValueError, match="belong"):
        await store.read("tenant-b", first.object_key)
    with pytest.raises(ValueError, match="belong"):
        await store.commit("tenant-b", first.artifact_id, first.object_key)


def test_artifact_helper_aliases_and_validation() -> None:
    key_from_enum = media_idempotency_key("tenant-a", _Channel.FEISHU, "message", "media")
    key_from_string = media_idempotency_key("tenant-a", "feishu", "message", "media")
    assert key_from_enum == key_from_string
    assert build_media_idempotency_key("tenant-a", "feishu", "message", "media") == key_from_string
    assert len(key_from_string) == 64

    with pytest.raises(ValueError, match="positive"):
        InMemoryArtifactStore(max_size_bytes=0)
    with pytest.raises(ValueError, match="required"):
        InMemoryArtifactStore._required("", "tenant_id")
    with pytest.raises(ValueError, match="required"):
        InMemoryArtifactStore._required(None, "tenant_id")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        InMemoryArtifactStore._required("x" * 4097, "tenant_id")

    store = InMemoryArtifactStore(max_size_bytes=4)
    with pytest.raises(TypeError, match="bytes"):
        store._validate_content("not bytes", None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="maximum size"):
        store._validate_content(b"12345", None)
    with pytest.raises(ValueError, match="checksum"):
        store._validate_content(b"1234", object())  # type: ignore[arg-type]
    assert store._validate_content(b"1234", hashlib.sha256(b"1234").hexdigest().upper())[1]

    with pytest.raises(TypeError, match="filename"):
        store._optional(1, "filename")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        store._optional("x" * 1025, "filename")
    assert store._optional(None, "filename") is None


@pytest.mark.asyncio
async def test_artifact_compatibility_interfaces_and_metadata_validation() -> None:
    store = InMemoryArtifactStore(max_size_bytes=1024)
    content = b"compatibility"
    checksum = hashlib.sha256(content).hexdigest()
    record = await store.ingest("tenant-a", _Channel.FEISHU, "message", "media", content)
    assert record.channel == "feishu"
    assert (
        store.media_idempotency_key("tenant-a", "feishu", "message", "media")
        == record.idempotency_key
    )
    assert (
        store.idempotency_key_for_media("tenant-a", "feishu", "message", "media")
        == record.idempotency_key
    )

    staging = await store.stage(
        "tenant-a",
        "artifact",
        content,
        checksum=checksum,
        metadata={"display-name": "safe", "source": "test"},
    )
    assert store.objects[staging][1]["display-name"] == "safe"
    await store.discard(staging)

    for metadata in (
        {"body": "no"},
        {"x-token-value": "no"},
        {"sha256": "no"},
        {"artifact-id-hash": "no"},
        {1: "no"},
        {"x": 1},
    ):
        with pytest.raises((TypeError, ValueError)):
            await store.stage("tenant-a", "artifact", content, checksum=checksum, metadata=metadata)  # type: ignore[arg-type]

    missing_metadata = await store.stage("tenant-a", "artifact", content, checksum=checksum)
    store.objects[missing_metadata][1].pop("sha256")
    with pytest.raises(ValueError, match="valid checksum"):
        await store.commit("tenant-a", "artifact", missing_metadata)

    for bad_checksum in ("x" * 63, "z" * 64):
        bad = await store.stage("tenant-a", "artifact", content, checksum=checksum)
        store.objects[bad][1]["sha256"] = bad_checksum
        with pytest.raises(ValueError, match="valid checksum"):
            await store.commit("tenant-a", "artifact", bad)


@pytest.mark.asyncio
async def test_artifact_ingestion_reuses_existing_object_and_rejects_conflicts() -> None:
    client = FakeS3()
    first_store = S3ArtifactStore(client, bucket="bucket")
    content = b"existing object"
    first = await first_store.ingest_media("tenant-a", "feishu", "message", "media", content)

    second_store = S3ArtifactStore(client, bucket="bucket")
    second = await second_store.ingest_media(
        "tenant-a", "feishu", "message", "media", content, filename="same.pdf"
    )
    assert second.object_key == first.object_key
    assert client.put_calls == 1
    assert client.copy_calls == 1

    client.objects[first.object_key] = (content, {"sha256": "0" * 64})
    with pytest.raises(ValueError, match="idempotency key conflicts"):
        await S3ArtifactStore(client, bucket="bucket").ingest_media(
            "tenant-a", "feishu", "message", "media", content
        )

    memory = InMemoryArtifactStore()
    await memory.ingest_media("tenant-a", "feishu", "message", "media", content)
    with pytest.raises(ValueError, match="idempotency key conflicts"):
        await memory.ingest_media("tenant-a", "feishu", "message", "media", b"different")
    with pytest.raises(ValueError, match="idempotency key conflicts"):
        await memory.ingest_media("tenant-a", "feishu", "message", "media", b"different size")


@pytest.mark.asyncio
async def test_artifact_ingestion_cleans_staging_when_head_or_record_fails() -> None:
    content = b"media"
    checksum = hashlib.sha256(content).hexdigest()

    no_head = _BrokenHeadStore(head=None)
    with pytest.raises(ValueError, match="staged artifact checksum"):
        await no_head.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert no_head.objects == {}

    bad_head = _BrokenHeadStore(
        head={"Metadata": {"sha256": "0" * 64}, "ContentLength": len(content)}
    )
    with pytest.raises(ValueError, match="staged artifact checksum"):
        await bad_head.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert bad_head.objects == {}

    wrong_size = _BrokenHeadStore(
        head={"Metadata": {"sha256": checksum}, "ContentLength": len(content) + 1}
    )
    with pytest.raises(ValueError, match="staged artifact size"):
        await wrong_size.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert wrong_size.objects == {}

    exploding = _ExplodingRecordStore()
    with pytest.raises(RuntimeError, match="record failed"):
        await exploding.ingest_media("tenant-a", "feishu", "message", "media", content)
    assert exploding.objects == {}


@pytest.mark.asyncio
async def test_base_store_contract_methods_are_explicitly_abstract_at_runtime() -> None:
    base = _ArtifactStoreBase(max_size_bytes=1)
    with pytest.raises(NotImplementedError):
        await base.stage("tenant", "artifact", b"x", checksum=hashlib.sha256(b"x").hexdigest())
    with pytest.raises(NotImplementedError):
        await base.commit("tenant", "artifact", "staged")
    with pytest.raises(NotImplementedError):
        await base.discard("staged")
    with pytest.raises(NotImplementedError):
        await base.read("tenant", "object")
    with pytest.raises(NotImplementedError):
        await base._head_if_present("object")
    with pytest.raises(NotImplementedError):
        await base._delete_any("object")


@pytest.mark.asyncio
async def test_s3_staged_api_validates_checksums_discard_and_missing_objects() -> None:
    client = FakeS3()
    store = S3ArtifactStore(client, bucket="bucket")
    content = b"s3 staged"
    checksum = hashlib.sha256(content).hexdigest()
    staged = await store.stage("tenant-a", "artifact", content, checksum=checksum)

    with pytest.raises(ValueError, match="checksum"):
        await store.commit("tenant-a", "artifact", staged, expected_checksum="0" * 64)
    assert staged in client.objects
    with pytest.raises(ValueError, match="belong"):
        await store.discard(staged, tenant_id="tenant-b")
    await store.discard(staged, tenant_id="tenant-a")
    await store.discard(staged)

    for error in (
        FileNotFoundError(),
        KeyError("missing"),
        _ErrorWithResponse("404"),
        _ErrorWithResponse("NoSuchKey"),
        _ErrorWithResponse("NotFound"),
    ):
        client.head_errors["missing"] = error
        assert await store._head_if_present("missing") is None
    client.head_errors["unknown"] = _ErrorWithResponse("500")
    with pytest.raises(_ErrorWithResponse):
        await store._head_if_present("unknown")
    client.head_errors["plain"] = RuntimeError("plain head failure")
    with pytest.raises(RuntimeError, match="plain head failure"):
        await store._head_if_present("plain")


@pytest.mark.asyncio
async def test_s3_read_checks_scope_body_checksum_and_size() -> None:
    client = FakeS3()
    store = S3ArtifactStore(client, bucket="bucket", max_size_bytes=4)
    content = b"read"
    record = await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    with pytest.raises(ValueError, match="belong"):
        await store.read("tenant-b", record.object_key)

    client.get_response = {}
    with pytest.raises(ValueError, match="readable body"):
        await store.read("tenant-a", record.object_key)
    client.get_response = {"Body": object()}
    with pytest.raises(ValueError, match="readable body"):
        await store.read("tenant-a", record.object_key)
    client.get_response = {"Body": _Body(b"tampered")}
    with pytest.raises(ValueError, match="checksum"):
        await store.read("tenant-a", record.object_key)
    client.objects[record.object_key][1]["sha256"] = hashlib.sha256(b"12345").hexdigest()
    client.get_response = {"Body": _Body(b"12345")}
    with pytest.raises(ValueError, match="maximum size"):
        await store.read("tenant-a", record.object_key)


@pytest.mark.asyncio
async def test_s3_upload_and_cleanup_tolerate_delete_failure() -> None:
    client = FakeS3()
    client.fail_put = True
    client.fail_delete = True
    store = S3ArtifactStore(client, bucket="bucket")
    with pytest.raises(RuntimeError, match="upload"):
        await store.ingest_media("tenant-a", "feishu", "message", "media", b"media")
    # The provider may leave an orphan when its delete call fails; the exception is not masked.
    assert client.objects


@pytest.mark.asyncio
async def test_inmemory_staged_api_checks_missing_checksum_scope_and_reads() -> None:
    store = InMemoryArtifactStore()
    content = b"memory staged"
    checksum = hashlib.sha256(content).hexdigest()
    staged = await store.stage("tenant-a", "artifact", content, checksum=checksum)
    with pytest.raises(ValueError, match="checksum"):
        await store.commit("tenant-a", "artifact", staged, expected_checksum="0" * 64)
    assert staged in store.objects
    with pytest.raises(ValueError, match="belong"):
        await store.discard(staged, tenant_id="tenant-b")
    await store.discard(staged, tenant_id="tenant-a")
    with pytest.raises(LookupError, match="does not exist"):
        await store.commit("tenant-a", "artifact", staged)

    record = await store.ingest_media("tenant-a", "feishu", "message", "media", content)
    with pytest.raises(LookupError, match="does not exist"):
        await store.read("tenant-a", f"{store._artifact_prefix('tenant-a')}missing")
    with pytest.raises(ValueError, match="belong"):
        await store.read("tenant-b", record.object_key)
    store.objects[record.object_key] = (b"tampered", store.objects[record.object_key][1])
    with pytest.raises(ValueError, match="checksum"):
        await store.read("tenant-a", record.object_key)
    assert await store._head_if_present("not-present") is None


@pytest.mark.asyncio
async def test_redis_projection_is_tenant_scoped_and_monotonic() -> None:
    redis = LuaProjectionRedis()
    store = RedisProjectionStore(redis)
    await store.put_session("tenant-a", "same-session", sequence=2, value={"v": 2})
    await store.put_session("tenant-b", "same-session", sequence=1, value={"v": 1})
    assert await store.get_session("tenant-a", "same-session", minimum_sequence=2) == {"v": 2}
    assert await store.get_session("tenant-a", "same-session", minimum_sequence=3) is None
    with pytest.raises(ValueError, match="backwards"):
        await store.put_session("tenant-a", "same-session", sequence=1, value={"v": 1})
