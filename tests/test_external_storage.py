from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace

import httpx
import pytest

from tenant_agent.models import ArtifactRecord, KnowledgeRecord, MemoryRecord
from tenant_agent.storage.base import ConcurrentWriteError
from tenant_agent.storage.external import (
    ExternalMemoryRepository,
    FilesystemArtifactRepository,
    QdrantKnowledgeRepository,
    S3ArtifactRepository,
)


@pytest.mark.asyncio
async def test_filesystem_artifact_is_atomic_scoped_and_checksum_verified(tmp_path: Path) -> None:
    repository = FilesystemArtifactRepository(tmp_path)
    await repository.initialize()
    content = b"hello artifact"
    record = ArtifactRecord(
        tenant_id="alpha",
        session_id="session",
        artifact_id="artifact-1",
        filename="note.txt",
        content_type="text/plain",
        size_bytes=len(content),
        checksum_sha256=hashlib.sha256(content).hexdigest(),
        storage_uri="filesystem://artifact-1",
    )
    await repository.put_artifact(record, content)
    await repository.put_artifact(record, content)
    assert await asyncio.to_thread(lambda: list(tmp_path.rglob("*.bundle")))
    assert not await asyncio.to_thread(lambda: list(tmp_path.rglob("*.bin")))
    assert not await asyncio.to_thread(lambda: list(tmp_path.rglob("*.json")))
    assert await repository.get_artifact("alpha", "artifact-1") == (record, content)
    assert await repository.get_artifact("beta", "artifact-1") is None
    assert [item.artifact_id async for item in repository.iter_artifacts("alpha")] == ["artifact-1"]
    with pytest.raises(ValueError):
        await repository.put_artifact(
            record.model_copy(update={"artifact_id": "bad", "checksum_sha256": "0" * 64}),
            content,
        )
    conflicting_content = b"other artifact"
    with pytest.raises(ConcurrentWriteError, match="artifact version"):
        await repository.put_artifact(
            record.model_copy(
                update={
                    "size_bytes": len(conflicting_content),
                    "checksum_sha256": hashlib.sha256(conflicting_content).hexdigest(),
                }
            ),
            conflicting_content,
        )

    race = record.model_copy(update={"artifact_id": "filesystem-race"})
    other_content = b"filesystem conflict"
    other = race.model_copy(
        update={
            "size_bytes": len(other_content),
            "checksum_sha256": hashlib.sha256(other_content).hexdigest(),
        }
    )
    race_results = await asyncio.gather(
        repository.put_artifact(race, content),
        repository.put_artifact(other, other_content),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ConcurrentWriteError) for result in race_results) == 1
    stored_race = await repository.get_artifact("alpha", "filesystem-race")
    assert stored_race == (race, content) or stored_race == (other, other_content)

    legacy = record.model_copy(update={"artifact_id": "legacy-torn"})
    legacy_content, legacy_metadata = repository._paths("alpha", "legacy-torn")  # type: ignore[attr-defined]
    legacy_metadata.parent.mkdir(parents=True, exist_ok=True)
    legacy_metadata.write_text(legacy.model_dump_json(), encoding="utf-8")
    with pytest.raises(ValueError, match="incomplete"):
        await repository.get_artifact("alpha", "legacy-torn")
    with pytest.raises(ValueError, match="incomplete"):
        [item async for item in repository.iter_artifacts("alpha")]
    assert not legacy_content.exists()
    await repository.close()


@pytest.mark.asyncio
async def test_external_memory_contract_and_tenant_validation() -> None:
    stored: dict[str, dict[str, object]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            stored[request.url.path] = json.loads(request.content)
            return httpx.Response(204)
        if request.url.path.endswith("/memories/export"):
            return httpx.Response(200, json={"items": list(stored.values()), "next_cursor": None})
        return httpx.Response(200, json={"items": list(stored.values())})

    repository = ExternalMemoryRepository(
        json.dumps(
            {
                "base_url": "https://memory.example",
                "authorization": "Bearer hidden-value",
                "timeout_seconds": 1,
            }
        )
    )
    await repository.initialize()
    assert repository._client is not None
    await repository._client.aclose()
    repository._client = httpx.AsyncClient(
        base_url=repository.base_url,
        headers=repository.headers,
        transport=httpx.MockTransport(handler),
    )
    record = MemoryRecord(
        memory_id="m1",
        tenant_id="alpha",
        user_id="user",
        content="remember this",
    )
    await repository.put_memory(record)
    assert await repository.search_memory("alpha", "user", "remember") == (record,)
    assert [item async for item in repository.iter_memories("alpha")] == [record]
    await repository.close()


def test_optional_production_adapters_fail_closed_without_extras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name in {"aioboto3", "qdrant_client"}:
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    with pytest.raises(RuntimeError):
        S3ArtifactRepository(json.dumps({"bucket": "test"}))
    qdrant = QdrantKnowledgeRepository(json.dumps({"url": "http://qdrant"}))

    async def initialize() -> None:
        with pytest.raises(RuntimeError):
            await qdrant.initialize()

    asyncio.run(initialize())


@pytest.mark.asyncio
async def test_s3_adapter_with_compatible_async_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    objects: dict[str, bytes] = {}

    class Body:
        def __init__(self, value: bytes) -> None:
            self.value = value

        async def read(self) -> bytes:
            return self.value

    class NoSuchKey(Exception):
        pass

    class PreconditionFailed(Exception):
        def __init__(self) -> None:
            super().__init__("precondition failed")
            self.response = {"Error": {"Code": "PreconditionFailed"}}

    class ConditionalRequestConflict(Exception):
        def __init__(self) -> None:
            super().__init__("conditional conflict")
            self.response = {"Error": {"Code": "ConditionalRequestConflict"}}

    conflict_once: set[str] = set()
    force_conflict_once = False

    class Client:
        exceptions = SimpleNamespace(NoSuchKey=NoSuchKey, PreconditionFailed=PreconditionFailed)

        async def __aenter__(self) -> Client:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def put_object(self, *, Key: str, Body: bytes, **kwargs: object) -> None:
            if force_conflict_once and Key not in conflict_once:
                conflict_once.add(Key)
                raise ConditionalRequestConflict
            if kwargs.get("IfNoneMatch") == "*" and Key in objects:
                raise PreconditionFailed
            objects[Key] = bytes(Body)

        async def get_object(self, *, Key: str, **kwargs: object) -> dict[str, Body]:
            del kwargs
            if Key not in objects:
                raise NoSuchKey
            return {"Body": Body(objects[Key])}

        async def list_objects_v2(self, *, Prefix: str, **kwargs: object) -> dict[str, object]:
            del kwargs
            return {
                "Contents": [{"Key": key} for key in sorted(objects) if key.startswith(Prefix)],
                "IsTruncated": False,
            }

    class Session:
        def client(self, *args: object, **kwargs: object) -> Client:
            del args, kwargs
            return Client()

    fake_aioboto = ModuleType("aioboto3")
    fake_aioboto.Session = Session  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "aioboto3", fake_aioboto)
    repository = S3ArtifactRepository(
        json.dumps(
            {
                "bucket": "bucket",
                "prefix": "prefix",
                "endpoint_url": "https://s3.example",
                "aws_access_key_id": "hidden",
                "aws_secret_access_key": "hidden",
            }
        )
    )
    await repository.initialize()
    content = b"s3 content"
    record = ArtifactRecord(
        tenant_id="alpha",
        session_id="session",
        artifact_id="artifact",
        filename="file.txt",
        content_type="text/plain",
        size_bytes=len(content),
        checksum_sha256=hashlib.sha256(content).hexdigest(),
        storage_uri="s3://bucket/artifact",
    )
    await repository.put_artifact(record, content)
    await repository.put_artifact(record, content)
    assert any(key.endswith(".bundle") for key in objects)
    assert not any(key.endswith((".bin", ".json")) for key in objects)
    assert await repository.get_artifact("alpha", "artifact") == (record, content)
    assert await repository.get_artifact("alpha", "missing") is None
    assert [item.artifact_id async for item in repository.iter_artifacts("alpha")] == ["artifact"]

    race_record = record.model_copy(update={"artifact_id": "race"})
    other_content = b"conflicting s3 content"
    conflicting_record = race_record.model_copy(
        update={
            "size_bytes": len(other_content),
            "checksum_sha256": hashlib.sha256(other_content).hexdigest(),
        }
    )
    results = await asyncio.gather(
        repository.put_artifact(race_record, content),
        repository.put_artifact(conflicting_record, other_content),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ConcurrentWriteError) for result in results) == 1
    stored = await repository.get_artifact("alpha", "race")
    assert stored == (race_record, content) or stored == (conflicting_record, other_content)

    retry_record = record.model_copy(update={"artifact_id": "retry409"})
    force_conflict_once = True
    await repository.put_artifact(retry_record, content)
    assert await repository.get_artifact("alpha", "retry409") == (retry_record, content)

    legacy_record = record.model_copy(update={"artifact_id": "legacy"})
    legacy_base = repository._base_key("alpha", "legacy")  # type: ignore[attr-defined]
    objects[f"{legacy_base}.json"] = legacy_record.model_dump_json().encode()
    objects[f"{legacy_base}.bin"] = content
    assert await repository.get_artifact("alpha", "legacy") == (legacy_record, content)
    objects.pop(f"{legacy_base}.bin")
    with pytest.raises(ValueError, match="incomplete"):
        await repository.get_artifact("alpha", "legacy")
    with pytest.raises(ValueError, match="incomplete"):
        [item async for item in repository.iter_artifacts("alpha")]
    await repository.close()


@pytest.mark.asyncio
async def test_qdrant_adapter_collection_filter_search_and_scroll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @dataclass
    class VectorParams:
        size: int
        distance: str

    @dataclass
    class PointStruct:
        id: str
        vector: list[float]
        payload: dict[str, object]

    @dataclass
    class MatchValue:
        value: object

    @dataclass
    class FieldCondition:
        key: str
        match: MatchValue

    @dataclass
    class Filter:
        must: list[FieldCondition]

    class Distance:
        COSINE = "cosine"

    models = SimpleNamespace(
        VectorParams=VectorParams,
        PointStruct=PointStruct,
        MatchValue=MatchValue,
        FieldCondition=FieldCondition,
        Filter=Filter,
        Distance=Distance,
    )

    class Client:
        def __init__(self, **kwargs: object) -> None:
            del kwargs
            self.collections: set[str] = set()
            self.points: list[PointStruct] = []
            self.closed = False

        async def get_collections(self) -> object:
            return SimpleNamespace(collections=[SimpleNamespace(name=name) for name in self.collections])

        async def create_collection(self, *, collection_name: str, **kwargs: object) -> None:
            del kwargs
            self.collections.add(collection_name)

        async def upsert(self, *, points: list[PointStruct], **kwargs: object) -> None:
            del kwargs
            self.points.extend(points)

        async def query_points(self, **kwargs: object) -> object:
            del kwargs
            return SimpleNamespace(points=self.points)

        async def scroll(self, **kwargs: object) -> tuple[list[PointStruct], None]:
            del kwargs
            return self.points, None

        async def close(self) -> None:
            self.closed = True

    fake_qdrant = ModuleType("qdrant_client")
    fake_qdrant.AsyncQdrantClient = Client  # type: ignore[attr-defined]
    fake_qdrant.models = models  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "qdrant_client", fake_qdrant)
    repository = QdrantKnowledgeRepository(
        json.dumps({"url": "https://qdrant.example", "api_key": "hidden"}),
        namespace="knowledge",
    )
    await repository.initialize()
    record = KnowledgeRecord(
        tenant_id="alpha",
        document_id="doc",
        chunk_id="chunk",
        text="knowledge",
        embedding=(1.0, 0.0),
        metadata={"kind": "test"},
    )
    await repository.put_knowledge(record)
    found = await repository.search_knowledge("alpha", (1.0, 0.0), metadata_filter={"kind": "test"})
    assert found == (record,)
    assert [item async for item in repository.iter_knowledge("alpha")] == [record]
    await repository.close()
