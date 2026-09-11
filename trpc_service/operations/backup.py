"""Cross-store backup evidence and restore preflight checks.

The actual bytes are produced by the managed PostgreSQL/Redis/Qdrant/MinIO
snapshot jobs. This manifest binds those snapshots to one schema and records
the dependency state needed to verify a restore before traffic is enabled.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select

from trpc_service.persistence.models import Base, ServiceSchemaRow
from trpc_service.persistence.schema import SCHEMA_VERSION


def _probe_stores() -> dict:
    stores: dict[str, object] = {}
    redis_url = os.environ.get("TRPC_REDIS_URL")
    if redis_url:
        import redis
        client = redis.Redis.from_url(redis_url, socket_connect_timeout=2, socket_timeout=2)
        try:
            client.ping()
            info = client.info("persistence")
            stores["redis"] = {"status": "ok", "dbsize": client.dbsize(),
                               "rdb_last_save_time": info.get("rdb_last_save_time")}
        finally:
            client.close()
    qdrant_url = os.environ.get("TRPC_QDRANT_URL")
    if qdrant_url:
        from qdrant_client import QdrantClient
        client = QdrantClient(url=qdrant_url, timeout=5)
        try:
            stores["qdrant"] = {"status": "ok", "collections": sorted(
                item.name for item in client.get_collections().collections)}
        finally:
            client.close()
    endpoint = os.environ.get("TRPC_MINIO_ENDPOINT")
    if endpoint:
        from minio import Minio
        client = Minio(endpoint,
                       access_key=os.environ.get("TRPC_MINIO_ACCESS_KEY", ""),
                       secret_key=os.environ.get("TRPC_MINIO_SECRET_KEY", ""),
                       secure=os.environ.get("TRPC_MINIO_SECURE", "false").lower() == "true")
        stores["minio"] = {"status": "ok", "buckets": sorted(item.name for item in client.list_buckets())}
    return stores


def create_manifest(database, output: str | Path) -> dict:
    """Capture a deterministic, non-secret backup manifest."""
    database.check_schema()
    with database.sessions() as session:
        schema = session.scalar(select(ServiceSchemaRow.version))
        tables = {
            table.name: session.scalar(select(func.count()).select_from(table))
            for table in Base.metadata.sorted_tables
        }
    manifest = {
        "format_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": schema,
        "expected_schema_version": SCHEMA_VERSION,
        "tables": tables,
        "stores": _probe_stores(),
    }
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    manifest["sha256"] = hashlib.sha256(payload).hexdigest()
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def verify_manifest(database, manifest_path: str | Path) -> dict:
    """Verify schema, table counts and configured store reachability after restore."""
    path = Path(manifest_path)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    supplied_hash = manifest.pop("sha256", None)
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if supplied_hash != hashlib.sha256(payload).hexdigest():
        raise ValueError("backup manifest checksum mismatch")
    database.check_schema()
    with database.sessions() as session:
        current_schema = session.scalar(select(ServiceSchemaRow.version))
        current_tables = {
            table.name: session.scalar(select(func.count()).select_from(table))
            for table in Base.metadata.sorted_tables
        }
    if current_schema != manifest.get("schema_version"):
        raise ValueError("restored schema version differs from backup manifest")
    if current_tables != manifest.get("tables"):
        raise ValueError("restored table counts differ from backup manifest")
    stores = _probe_stores()
    missing = sorted(set(manifest.get("stores", {})) - set(stores))
    if missing:
        raise ValueError("restored stores are unavailable: " + ", ".join(missing))
    return {"verified": True, "schema_version": current_schema, "stores": stores,
            "manifest": str(path)}
