"""Contract tests for tenant-bound S3 artifact storage."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from google.genai.types import Blob, FileData
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from trpc_service.storage.artifact_repository import ArtifactVersionRecord
from trpc_service.storage.s3_artifact_service import (
    ArtifactStorageUnavailableError,
    S3ArtifactService,
    UnsupportedArtifactError,
)


@pytest.fixture(autouse=True)
def _run_object_boundary_inline(monkeypatch):
    """Keep unit tests deterministic; real MinIO threading is integration-tested."""

    async def invoke(callable_, *args, **kwargs):
        return callable_(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", invoke)


class _Repository:

    def __init__(self) -> None:
        self.records: dict[tuple[str, str, int], ArtifactVersionRecord] = {}
        self.deleted: set[tuple[str, str]] = set()
        self.next_versions: dict[tuple[str, str], int] = {}

    async def reserve_version(self, *, tenant_id, artifact_path, object_key, digest, size_bytes, mime_type, metadata):
        key = (tenant_id, artifact_path)
        version = self.next_versions.get(key, 0)
        self.next_versions[key] = version + 1
        record = ArtifactVersionRecord(
            tenant_id=tenant_id,
            artifact_path=artifact_path,
            version=version,
            object_key=object_key,
            digest=digest,
            size_bytes=size_bytes,
            mime_type=mime_type,
            metadata=metadata,
            created_at=datetime.now(UTC),
            available=False,
        )
        self.records[(tenant_id, artifact_path, version)] = record
        return record

    async def mark_available(self, record):
        self.records[(record.tenant_id, record.artifact_path, record.version)] = replace(record, available=True)

    async def latest_available(self, *, tenant_id, artifact_path):
        if (tenant_id, artifact_path) in self.deleted:
            return None
        matches = [
            value for (stored_tenant, stored_path, _), value in self.records.items()
            if stored_tenant == tenant_id and stored_path == artifact_path and value.available
        ]
        return max(matches, key=lambda item: item.version) if matches else None

    async def get_available(self, *, tenant_id, artifact_path, version):
        if (tenant_id, artifact_path) in self.deleted:
            return None
        record = self.records.get((tenant_id, artifact_path, version))
        return record if record and record.available else None

    async def list_available(self, *, tenant_id, artifact_path):
        if (tenant_id, artifact_path) in self.deleted:
            return ()
        return tuple(value for (stored_tenant, stored_path, _), value in sorted(self.records.items())
                     if stored_tenant == tenant_id and stored_path == artifact_path and value.available)

    async def list_keys(self, *, tenant_id, app_name, user_id, session_id):
        prefix = f"{app_name}/{user_id}/{session_id or 'user'}/"
        return tuple(
            sorted({
                path.removeprefix(prefix)
                for stored_tenant, path, _ in self.records if stored_tenant == tenant_id and path.startswith(prefix)
            }))

    async def mark_deleted(self, *, tenant_id, artifact_path):
        if (tenant_id, artifact_path) in self.deleted:
            return ()
        self.deleted.add((tenant_id, artifact_path))
        return tuple(value for (stored_tenant, stored_path, _), value in self.records.items()
                     if stored_tenant == tenant_id and stored_path == artifact_path and value.available)


class _Client:

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.removed: list[str] = []

    def put_object(self, bucket_name, object_name, data, length, content_type):
        self.objects[object_name] = data.read()

    def stat_object(self, bucket_name, object_name):
        return type("Stat", (), {"size": len(self.objects[object_name])})()

    def get_object(self, bucket_name, object_name):
        return type(
            "Response",
            (),
            {
                "read": lambda response: self.objects[object_name],
                "close": lambda response: None,
                "release_conn": lambda response: None,
            },
        )()

    def remove_object(self, bucket_name, object_name):
        self.removed.append(object_name)
        self.objects.pop(object_name, None)


@pytest.mark.asyncio
async def test_save_then_load_text_keeps_payload_and_hides_filename_from_object_key() -> None:
    client = _Client()
    repository = _Repository()
    service = S3ArtifactService(tenant_id="tenant_a", bucket="artifacts", client=client, repository=repository)
    artifact_id = ArtifactId(app_name="support", user_id="u-1", session_id="s-1", filename="private notes.txt")

    version = await service.save_artifact(
        artifact_id=artifact_id,
        artifact=Part(text="hello"),
        metadata={"kind": "note"},
    )

    assert version == 0
    record = await repository.latest_available(tenant_id="tenant_a", artifact_path="support/u-1/s-1/private notes.txt")
    assert record is not None
    assert record.digest == hashlib.sha256(b"hello").hexdigest()
    assert record.object_key.startswith("tenant_a/")
    assert "private" not in record.object_key
    assert (await service.load_artifact(artifact_id=artifact_id)).data == Part(text="hello")


@pytest.mark.asyncio
async def test_two_tenants_cannot_load_each_others_artifact() -> None:
    client = _Client()
    repository = _Repository()
    artifact_id = ArtifactId(app_name="support", user_id="u-1", session_id=None, filename="doc.txt")
    first = S3ArtifactService(tenant_id="tenant_a", bucket="artifacts", client=client, repository=repository)
    second = S3ArtifactService(tenant_id="tenant_b", bucket="artifacts", client=client, repository=repository)

    await first.save_artifact(
        artifact_id=artifact_id,
        artifact=Part(inline_data=Blob(data=b"one", mime_type="text/plain")),
    )

    assert await second.load_artifact(artifact_id=artifact_id) is None
    assert await second.list_versions(artifact_id=artifact_id) == []


@pytest.mark.asyncio
async def test_remote_file_url_is_rejected_before_object_write() -> None:
    client = _Client()
    service = S3ArtifactService(tenant_id="tenant_a", bucket="artifacts", client=client, repository=_Repository())
    artifact_id = ArtifactId(app_name="support", user_id="u-1", filename="doc.txt")

    with pytest.raises(UnsupportedArtifactError, match="remote artifact URLs are not supported"):
        await service.save_artifact(
            artifact_id=artifact_id,
            artifact=Part(file_data=FileData(file_uri="https://example.invalid/doc", mime_type="text/plain")),
        )

    assert client.objects == {}


@pytest.mark.asyncio
async def test_object_backend_error_is_mapped_without_backend_detail() -> None:

    class BrokenClient(_Client):

        def put_object(self, *args, **kwargs):
            raise RuntimeError("https://secret.invalid/access-key")

    service = S3ArtifactService(
        tenant_id="tenant_a",
        bucket="artifacts",
        client=BrokenClient(),
        repository=_Repository(),
    )
    artifact_id = ArtifactId(app_name="support", user_id="u-1", filename="doc.txt")

    with pytest.raises(ArtifactStorageUnavailableError) as raised:
        await service.save_artifact(artifact_id=artifact_id, artifact=Part(text="hello"))

    assert str(raised.value) == "artifact object storage unavailable"
    assert "secret" not in repr(raised.value)


@pytest.mark.asyncio
async def test_version_listing_and_delete_only_affect_the_bound_tenant() -> None:
    client = _Client()
    repository = _Repository()
    service = S3ArtifactService(tenant_id="tenant_a", bucket="artifacts", client=client, repository=repository)
    artifact_id = ArtifactId(app_name="support", user_id="u-1", filename="doc.txt")

    await service.save_artifact(artifact_id=artifact_id, artifact=Part(text="first"))
    await service.save_artifact(artifact_id=artifact_id, artifact=Part(text="second"))

    assert await service.list_versions(artifact_id=artifact_id) == [0, 1]
    assert (await service.get_artifact_version(artifact_id, 1)).version == 1
    assert len(await service.list_artifact_versions(artifact_id=artifact_id)) == 2

    await service.delete_artifact(artifact_id=artifact_id)
    await service.delete_artifact(artifact_id=artifact_id)

    assert await service.load_artifact(artifact_id=artifact_id) is None
    assert await service.list_versions(artifact_id=artifact_id) == []
    assert len(client.removed) == 2
