"""Real PostgreSQL/MinIO audit archival and failure recovery; no backend emulation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from io import BytesIO
import json
import multiprocessing
import os
import socket
import uuid

from cryptography.fernet import Fernet
from fastapi import FastAPI
import httpx
from minio import Minio
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig, SUSPENDED
import pytest
from sqlalchemy import event, func, select
import uvicorn

from trpc_service.channels.persistence import ContextCipher
from trpc_service.persistence import Database
from trpc_service.persistence.models import AuditLogRow, utcnow
from trpc_service.storage.audit_archive import AuditArchive, ARCHIVED, ArchiveIntegrityError
from trpc_service.tenant.models import AuditPolicy

from .conftest import required_env


@pytest.fixture
def archive_backend(request, platform_database):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("audit archive acceptance requires real PostgreSQL and MinIO")
    client = Minio(required_env("TRPC_MINIO_ENDPOINT"),
                   access_key=required_env("TRPC_MINIO_ACCESS_KEY"),
                   secret_key=required_env("TRPC_MINIO_SECRET_KEY"),
                   secure=False)
    bucket = "audit-e2e-" + uuid.uuid4().hex
    client.make_bucket(bucket)
    client.set_bucket_versioning(bucket, VersioningConfig(ENABLED))
    key = Fernet.generate_key()
    archive = AuditArchive(platform_database, client, bucket, ContextCipher([key]))
    try:
        yield archive, key
    finally:
        # This bucket was created by this fixture, including every object version.
        for item in client.list_objects(bucket, recursive=True, include_version=True):
            client.remove_object(bucket, item.object_name, version_id=item.version_id)
        client.remove_bucket(bucket)


def seed(database, count=3, tenant="tenant_archive"):
    old = utcnow() - timedelta(days=200)
    with database.sessions.begin() as session:
        for index in range(count):
            session.add(
                AuditLogRow(audit_id=f"{tenant}-{index}",
                            tenant_id=tenant,
                            decision="tool_succeeded",
                            created_at=old,
                            tool_name="write_artifact",
                            cost=0.001,
                            redacted=True,
                            metadata_json={
                                "execution_id": f"exec_{index}",
                                "unexpected_body": "private-canary"
                            }))
        session.add(AuditLogRow(audit_id=tenant + "-recent", tenant_id=tenant, decision="audit_read", redacted=True))


def count_originals(database, tenant="tenant_archive"):
    with database.sessions() as session:
        return session.scalar(
            select(func.count()).select_from(AuditLogRow).where(AuditLogRow.tenant_id == tenant,
                                                                AuditLogRow.decision == "tool_succeeded"))


def manifest(archive, identity):
    with archive.database.sessions() as session:
        return dict(session.get(AuditLogRow, identity).metadata_json)


POLICY = AuditPolicy(archive_enabled=True, retention_days=180)


def test_retention_encryption_exact_version_and_key_rotation(archive_backend):
    archive, key = archive_backend
    seed(archive.database)
    seed(archive.database, tenant="tenant_other")
    assert archive.run_batch("tenant_archive", AuditPolicy(), config_version=1)["status"] == "disabled"
    result = archive.run_batch("tenant_archive", POLICY, config_version=1, limit=2)
    saved = manifest(archive, result["archive_id"])
    response = archive.client.get_object(archive.bucket, saved["object_key"], version_id=saved["object_version"])
    try:
        assert b"private-canary" not in response.read()
    finally:
        response.close()
        response.release_conn()
    # An overwrite creates another version. The exact archived version remains authoritative.
    archive.client.put_object(archive.bucket, saved["object_key"], BytesIO(b"replaced"), 8)
    assert len(archive.read("tenant_archive", result["archive_id"])) == 2
    new_key = Fernet.generate_key()
    reopened = AuditArchive(archive.database, archive.client, archive.bucket, ContextCipher([new_key, key]))
    second = reopened.run_batch("tenant_archive", POLICY, config_version=2)
    assert second["row_count"] == 1
    assert reopened.verify_chain("tenant_archive")["verified"] == 2
    assert count_originals(archive.database) == 0 and count_originals(archive.database, "tenant_other") == 3
    with archive.database.sessions() as session:
        assert session.get(AuditLogRow, "tenant_archive-recent")
    with pytest.raises(LookupError):
        reopened.read("tenant_other", result["archive_id"])
    wrong = AuditArchive(archive.database, archive.client, archive.bucket, ContextCipher([new_key]))
    with pytest.raises(ArchiveIntegrityError):
        wrong.read("tenant_archive", result["archive_id"])


def test_failed_actual_object_readback_preserves_sql(archive_backend):
    archive, _ = archive_backend
    seed(archive.database)
    original = archive.client.put_object

    def remove_uploaded_version(*args, **kwargs):
        result = original(*args, **kwargs)
        archive.client.remove_object(archive.bucket, result.object_name, version_id=result.version_id)
        return result

    archive.client.put_object = remove_uploaded_version
    try:
        with pytest.raises(Exception):
            archive.run_batch("tenant_archive", POLICY, config_version=1)
        assert count_originals(archive.database) == 3
        with archive.database.sessions() as session:
            assert not session.scalar(select(AuditLogRow.audit_id).where(AuditLogRow.decision == ARCHIVED))
    finally:
        archive.client.put_object = original
    assert archive.run_batch("tenant_archive", POLICY, config_version=1)["row_count"] == 3


def crash_after_readback(url, connect_args, bucket, key):
    database = Database(url, connect_args=connect_args)
    client = Minio(os.environ["TRPC_MINIO_ENDPOINT"],
                   access_key=os.environ["TRPC_MINIO_ACCESS_KEY"],
                   secret_key=os.environ["TRPC_MINIO_SECRET_KEY"],
                   secure=False)

    class CrashArchive(AuditArchive):

        def _read(self, *args):
            super()._read(*args)
            os._exit(73)

    CrashArchive(database, client, bucket, ContextCipher([key])).run_batch("tenant_archive", POLICY, config_version=1)


def test_process_death_after_upload_rolls_back_then_recovers(archive_backend):
    archive, key = archive_backend
    seed(archive.database)
    with archive.database.engine.connect() as connection:
        schema = connection.exec_driver_sql("SELECT current_schema()").scalar_one()
    child = multiprocessing.get_context("spawn").Process(target=crash_after_readback,
                                                         args=(archive.database.url, {
                                                             "options": "-csearch_path=" + schema
                                                         }, archive.bucket, key))
    child.start()
    try:
        child.join(timeout=45)
        assert child.exitcode == 73
        assert count_originals(archive.database) == 3
        assert archive.run_batch("tenant_archive", POLICY, config_version=1)["row_count"] == 3
        assert archive.verify_chain("tenant_archive")["verified"] == 1
        assert len(list(archive.client.list_objects(archive.bucket, recursive=True))) == 2
    finally:
        if child.is_alive():
            child.terminate()
            child.join(timeout=10)


def test_concurrent_workers_never_retire_a_row_twice(archive_backend):
    archive, _ = archive_backend
    seed(archive.database, count=31)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(lambda _: archive.run_batch("tenant_archive", POLICY, config_version=1, limit=10), range(4)))
    while count_originals(archive.database):
        results.append(archive.run_batch("tenant_archive", POLICY, config_version=1, limit=10))
    archived = [result for result in results if result["status"] == "archived"]
    ids = [row["audit_id"] for result in archived for row in archive.read("tenant_archive", result["archive_id"])]
    assert len(ids) == len(set(ids)) == 31
    page = archive.verify_chain("tenant_archive", limit=1)
    assert not page["complete"] and page["next_sequence"] == 1
    assert archive.verify_chain("tenant_archive", after_sequence=1)["complete"]


def test_receipt_tampering_and_missing_chain_are_detected(archive_backend):
    archive, _ = archive_backend
    seed(archive.database)
    batches = [archive.run_batch("tenant_archive", POLICY, config_version=1, limit=1) for _ in range(3)]
    identity = batches[1]["archive_id"]
    original = manifest(archive, identity)
    with archive.database.sessions.begin() as session:
        session.get(AuditLogRow, identity).metadata_json = {**original, "row_count": 123}
    with pytest.raises(ArchiveIntegrityError):
        archive.verify_chain("tenant_archive")
    with archive.database.sessions.begin() as session:
        session.delete(session.get(AuditLogRow, identity))
    with pytest.raises(ArchiveIntegrityError):
        archive.verify_chain("tenant_archive")


def test_nonversioned_bucket_never_retires_sql(archive_backend):
    archive, _ = archive_backend
    seed(archive.database)
    archive.client.set_bucket_versioning(archive.bucket, VersioningConfig(SUSPENDED))
    with pytest.raises(ValueError, match="versioning"):
        archive.run_batch("tenant_archive", POLICY, config_version=1)
    assert count_originals(archive.database) == 3


def test_sql_commit_failure_restores_deleted_rows(archive_backend):
    archive, _ = archive_backend
    seed(archive.database)

    def fail_commit(session):
        if any(isinstance(row, AuditLogRow) and row.decision == ARCHIVED for row in session.new):
            raise RuntimeError("injected SQL commit failure")

    event.listen(archive.database.sessions, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="injected SQL"):
            archive.run_batch("tenant_archive", POLICY, config_version=1)
        assert count_originals(archive.database) == 3
    finally:
        event.remove(archive.database.sessions, "before_commit", fail_commit)
    result = archive.run_batch("tenant_archive", POLICY, config_version=1)
    assert result["row_count"] == 3 and archive.verify_chain("tenant_archive")["verified"] == 1


@pytest.fixture
def configured_archive(archive_backend, monkeypatch):
    from trpc_service.management import ConfigurationStore, ConfigDraft
    from trpc_service.protected_demo import documents
    from trpc_service.tenant import TenantConfig
    archive, key = archive_backend
    monkeypatch.setenv("TRPC_ADMIN_TOKEN", "archive-operator-" * 3)
    monkeypatch.setenv("ARCHIVE_AUDITOR_TOKEN", "archive-auditor-" * 3)
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_ENDPOINT", required_env("TRPC_MINIO_ENDPOINT"))
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_ACCESS_KEY", required_env("TRPC_MINIO_ACCESS_KEY"))
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_SECRET_KEY", required_env("TRPC_MINIO_SECRET_KEY"))
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_SECURE", "false")
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_BUCKET", archive.bucket)
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_KEYS", key.decode())
    monkeypatch.setenv(
        "TRPC_ADMIN_PRINCIPALS",
        json.dumps([
            dict(actor="auditor",
                 role="tenant_auditor",
                 tenant_ids=["demo_wecom"],
                 token_ref="env://ARCHIVE_AUDITOR_TOKEN")
        ]))
    store = ConfigurationStore(archive.database)
    profiles, tenants = documents("audit_test_wecom", "audit_test_feishu")
    for profile in profiles:
        store.register_profile(profile, "test_operator")
    config = tenants[0].model_dump(mode="json")
    config["audit_policy"]["archive_enabled"] = True
    config = TenantConfig.model_validate(config)
    store.save_draft(
        ConfigDraft(config=config, profile_revisions={role: 1
                                                      for role in type(config.data_backends).model_fields}),
        "test_operator")
    store.publish(config.tenant_id, config.config_version, 0, "test_operator")
    seed(archive.database, tenant=config.tenant_id)
    return archive, key, config


@pytest.mark.asyncio
async def test_real_http_archive_download_and_auditor_permissions(configured_archive):
    from trpc_service.management import management_router
    archive, _, config = configured_archive
    app = FastAPI()
    app.include_router(management_router(archive.database))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="critical", lifespan="off"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(200):
            if server.started:
                break
            if serving.done():
                await serving
            await asyncio.sleep(0.05)
        assert server.started
        async with httpx.AsyncClient(base_url="http://127.0.0.1:" + str(listener.getsockname()[1]),
                                     timeout=30,
                                     trust_env=False) as client:
            root = "/admin/tenants/demo_wecom/audit/archives"
            client.headers["Authorization"] = "Bearer " + "archive-operator-" * 3
            response = await client.post(root + "/run", json={"limit": 2})
            assert response.status_code == 200, response.text
            identity = response.json()[0]["archive_id"]
            client.headers["Authorization"] = "Bearer " + "archive-auditor-" * 3
            assert (await client.post(root + "/run", json={})).status_code == 403
            assert (await client.get(root.replace("demo_wecom", "demo_feishu"))).status_code == 403
            assert (await client.get(root + "/verify")).json()["verified"] == 1
            response = await client.get(root + "/" + identity)
            assert response.status_code == 200 and len(response.text.splitlines()) == 2
            assert "private-canary" not in response.text
            assert response.headers["cache-control"] == "no-store"
            with archive.database.sessions() as session:
                assert session.scalar(
                    select(AuditLogRow.user_id).where(AuditLogRow.decision == "audit_archive_downloaded")) == "auditor"
    finally:
        server.should_exit = True
        await serving
        listener.close()


@pytest.mark.asyncio
async def test_scheduled_worker_archives_two_scans_and_drains(configured_archive, monkeypatch, tmp_path):
    from trpc_service.governance.audit_retention import worker, initialize_archive
    archive, _, config = configured_archive
    monkeypatch.setenv("TRPC_AUDIT_ARCHIVE_INTERVAL", "10")
    monkeypatch.setenv("TRPC_HEALTH_FILE", str(tmp_path / "archive-health.json"))
    assert initialize_archive()["versioned"]
    stop = asyncio.Event()
    task = asyncio.create_task(worker(archive.database, stop=stop))

    async def wait_for_retirement():
        for _ in range(200):
            if task.done():
                await task
                raise AssertionError("archive worker exited early")
            if await asyncio.to_thread(count_originals, archive.database, config.tenant_id) == 0:
                return
            await asyncio.sleep(0.1)
        raise AssertionError("scheduled archive did not complete")

    try:
        await wait_for_retirement()
        with archive.database.sessions.begin() as session:
            session.add(
                AuditLogRow(audit_id="second-scan",
                            tenant_id=config.tenant_id,
                            decision="tool_succeeded",
                            created_at=utcnow() - timedelta(days=200),
                            redacted=True))
        await wait_for_retirement()
        assert archive.verify_chain(config.tenant_id)["verified"] == 2
    finally:
        stop.set()
        await asyncio.wait_for(task, 30)
    health = json.loads((tmp_path / "archive-health.json").read_text())
    assert not health["ready"]
