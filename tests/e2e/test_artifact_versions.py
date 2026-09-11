"""Real MinIO evidence for versioned source documents used by migration/reindexing."""

import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from trpc_service.storage import S3ArtifactService
from trpc_service.storage import TenantBoundaryError

from .conftest import required_env


def test_concurrent_real_artifact_writers(request, platform_database):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("concurrent object test requires real MinIO")
    from minio import Minio
    client = Minio(required_env("TRPC_MINIO_ENDPOINT"), access_key=required_env("TRPC_MINIO_ACCESS_KEY"),
                   secret_key=required_env("TRPC_MINIO_SECRET_KEY"), secure=False)
    bucket = f"e2e-{uuid.uuid4().hex}"
    client.make_bucket(bucket)
    artifact_id = ArtifactId(app_name="tenant_acme:support", user_id="usr", session_id="ses", filename="same.txt")

    def upload(index):
        service = S3ArtifactService(tenant_id="tenant_acme", bucket=bucket, client=client, database=platform_database)
        return asyncio.run(service.save_artifact(artifact_id=artifact_id, artifact=Part.from_text(text=str(index))))

    reader = S3ArtifactService(tenant_id="tenant_acme", bucket=bucket, client=client, database=platform_database)
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            versions = list(executor.map(upload, range(16)))
        assert sorted(versions) == list(range(16))
        values = {asyncio.run(reader.load_artifact(artifact_id=artifact_id, version=v)).data.text for v in versions}
        assert values == {str(i) for i in range(16)}
    finally:
        asyncio.run(reader.delete_artifact(artifact_id=artifact_id))
        client.remove_bucket(bucket)


@pytest.mark.asyncio
async def test_source_documents_survive_service_recreation_and_tenant_collision(request, platform_database):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("MinIO test requires --backend-mode=real; no mocked object storage")
    from minio import Minio

    client = Minio(required_env("TRPC_MINIO_ENDPOINT"), access_key=required_env("TRPC_MINIO_ACCESS_KEY"),
                   secret_key=required_env("TRPC_MINIO_SECRET_KEY"), secure=False)
    bucket = f"e2e-{uuid.uuid4().hex}"
    clients = {tenant: S3ArtifactService(tenant_id=tenant, bucket=bucket, client=client, database=platform_database)
               for tenant in ("tenant_a", "tenant_b")}
    ids = {
        tenant: ArtifactId(app_name=f"{tenant}:support", user_id="same-user",
                           session_id="same-session", filename="source.txt")
        for tenant in clients
    }
    try:
        for tenant, service in clients.items():
            for version in range(2):
                assert await service.save_artifact(
                    artifact_id=ids[tenant], artifact=Part.from_text(text=f"{tenant} 原文版本 {version} 🙂"),
                    metadata={"revision": version},
                ) == version
        for tenant in clients:
            reader = S3ArtifactService(tenant_id=tenant, bucket=bucket, client=client, database=platform_database)
            assert await reader.list_versions(artifact_id=ids[tenant]) == [0, 1]
            for version in range(2):
                entry = await reader.load_artifact(artifact_id=ids[tenant], version=version)
                assert entry.data.text == f"{tenant} 原文版本 {version} 🙂"
                assert entry.version.custom_metadata == {"revision": version}
        with pytest.raises(TenantBoundaryError):
            await clients["tenant_a"].load_artifact(artifact_id=ids["tenant_b"])
    finally:
        for tenant, service in clients.items():
            await service.delete_artifact(artifact_id=ids[tenant])
        client.remove_bucket(bucket)
