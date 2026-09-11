"""Backup manifest integrity and restore preflight checks."""

import json

import pytest

from trpc_service.operations.backup import create_manifest, verify_manifest
from trpc_service.operations.snapshots import create_snapshot, restore_snapshot, validate_snapshot
from trpc_service.persistence import Database
from trpc_service.persistence.models import TenantRow


def test_manifest_round_trip_and_checksum(tmp_path, monkeypatch):
    monkeypatch.delenv("TRPC_REDIS_URL", raising=False)
    monkeypatch.delenv("TRPC_QDRANT_URL", raising=False)
    monkeypatch.delenv("TRPC_MINIO_ENDPOINT", raising=False)
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'backup.db'}")
    database.create_schema()
    target = tmp_path / "manifest.json"
    manifest = create_manifest(database, target)
    result = verify_manifest(database, target)
    assert result["verified"] is True
    assert manifest["sha256"]
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["tables"]["tenants"] += 1
    target.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        verify_manifest(database, target)
    database.close()


def test_logical_snapshot_restores_sql_data(tmp_path, monkeypatch):
    for name in ("TRPC_REDIS_URL", "TRPC_QDRANT_URL", "TRPC_MINIO_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    source = Database(f"sqlite+pysqlite:///{tmp_path / 'source.db'}")
    source.create_schema()
    with source.sessions.begin() as session:
        session.add(TenantRow(tenant_id="restore_tenant", name="Restore", status="active", active_config_version=1))
    snapshot = tmp_path / "snapshot"
    create_snapshot(source, snapshot)
    assert validate_snapshot(snapshot)["database"]["tenants"]
    target = Database(f"sqlite+pysqlite:///{tmp_path / 'target.db'}")
    target.create_schema()
    result = restore_snapshot(target, snapshot, destructive=True)
    assert result["restored"] is True
    with target.sessions() as session:
        assert session.get(TenantRow, "restore_tenant").name == "Restore"
    source.close()
    target.close()
