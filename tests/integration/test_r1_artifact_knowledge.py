"""R1C real PostgreSQL + MinIO evidence for tenant-scoped capabilities."""

from __future__ import annotations

import asyncio
import socket
import subprocess
import time
import uuid

import pytest
import sqlalchemy as sa
from minio import Minio
from sqlalchemy.ext.asyncio import create_async_engine
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from trpc_service.storage.artifact_repository import SqlArtifactRepository
from trpc_service.storage.knowledge_repository import SqlTenantKnowledge
from trpc_service.storage.s3_artifact_service import S3ArtifactService

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker, run_alembic

pytestmark = requires_docker


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _tenant() -> str:
    return f"t{uuid.uuid4().hex[:12]}"


def _wait_minio(client: Minio, bucket: str) -> None:
    for _ in range(60):
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            return
        except Exception:
            time.sleep(0.5)
    raise RuntimeError("test MinIO did not become ready")


@pytest.fixture(scope="module")
def r1c_services():
    if not docker_is_available():
        pytest.skip("Docker not available")
    pg = PostgreSQLContainer(name_prefix="trpc-r1c-pg")
    port = _port()
    container = f"trpc-r1c-minio-{uuid.uuid4().hex[:8]}"
    access, secret, bucket = "r1ctestaccess", "r1ctestsecret-0123456789", "r1c-artifacts"
    pg.start()
    try:
        assert run_alembic(pg.url, "upgrade", "head").returncode == 0
        subprocess.run([
            "docker", "run", "-d", "--name", container, "-p", f"{port}:9000", "-e", f"MINIO_ROOT_USER={access}", "-e",
            f"MINIO_ROOT_PASSWORD={secret}", "minio/minio:RELEASE.2025-04-22T22-12-26Z", "server", "/data"
        ],
                       check=True,
                       capture_output=True,
                       timeout=60)
        client = Minio(f"127.0.0.1:{port}", access_key=access, secret_key=secret, secure=False)
        _wait_minio(client, bucket)
        yield pg, client, bucket
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=20)
        pg.stop()


def _insert_tenant(pg: PostgreSQLContainer, tenant: str) -> None:
    governance = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],"tool_decisions":{},'
                  '"content_policy":{"enabled":false,"input_action":"block","output_action":"block"},"limits":null}')
    profile = '{"state_backend":"sql","artifact_backend":"s3","knowledge_backend":"sql","audit_backend":"sql"}'
    result = pg.run_sql(
        "INSERT INTO tenant_configs "
        "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance,"
        "backend_profile,audit_policy) VALUES "
        f"('{tenant}',true,1,'app_demo','test','default','[]'::jsonb,'{governance}'::jsonb,'{profile}'::jsonb,"
        "'{\"retention_days\":365,\"delivery_events\":\"all\"}'::jsonb)")
    assert result.success, result.output


def test_artifact_versions_are_shared_and_tenant_isolated(r1c_services) -> None:
    pg, client, bucket = r1c_services
    tenant_a, tenant_b = _tenant(), _tenant()
    _insert_tenant(pg, tenant_a)
    _insert_tenant(pg, tenant_b)

    async def scenario() -> None:
        engine = create_async_engine(pg.url)
        try:
            repo = SqlArtifactRepository(engine)
            artifact = ArtifactId(app_name="app_demo", user_id="user", session_id="session", filename="note.txt")
            first = S3ArtifactService(tenant_id=tenant_a, bucket=bucket, client=client, repository=repo)
            second = S3ArtifactService(tenant_id=tenant_a, bucket=bucket, client=client, repository=repo)
            assert await first.save_artifact(artifact_id=artifact, artifact=Part.from_text(text="one"),
                                             metadata={}) == 0
            assert await second.save_artifact(artifact_id=artifact, artifact=Part.from_text(text="two"),
                                              metadata={}) == 1
            loaded = await first.load_artifact(artifact_id=artifact, version=0)
            assert loaded.data.text == "one"
            other = S3ArtifactService(tenant_id=tenant_b, bucket=bucket, client=client, repository=repo)
            assert await other.list_artifact_keys(artifact_id=artifact) == []
        finally:
            await engine.dispose()

    asyncio.run(scenario())


def test_sql_knowledge_is_tenant_scoped(r1c_services) -> None:
    pg, _, _ = r1c_services
    tenant_a, tenant_b = _tenant(), _tenant()
    _insert_tenant(pg, tenant_a)
    _insert_tenant(pg, tenant_b)

    async def scenario() -> None:
        engine = create_async_engine(pg.url)
        try:
            await SqlTenantKnowledge(engine, tenant_a).upsert_document("doc-1", "shared database handbook",
                                                                       {"scope": "a"})
            await SqlTenantKnowledge(engine, tenant_b).upsert_document("doc-1", "different tenant text", {"scope": "b"})
            count = await engine.connect()
            try:
                assert (await count.execute(sa.text("SELECT count(*) FROM knowledge_documents WHERE tenant_id=:t"),
                                            {"t": tenant_a})).scalar_one() == 1
            finally:
                await count.close()
        finally:
            await engine.dispose()

    asyncio.run(scenario())
