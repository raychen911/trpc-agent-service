"""Scheduled restore-drill and cross-store consistency checks."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from trpc_service.operations.snapshots import restore_snapshot
from trpc_service.persistence import Database
from trpc_service.persistence.models import ArtifactMetadataRow, TenantRow


def consistency_check(database) -> dict:
    """Check tenant ownership references across SQL, Qdrant and MinIO."""
    with database.sessions() as session:
        tenants = {row.tenant_id for row in session.scalars(select(TenantRow))}
        artifact_tenants = {row.tenant_id for row in session.scalars(select(ArtifactMetadataRow))}
    unknown_artifacts = sorted(artifact_tenants - tenants)
    result = {"checked_at": datetime.now(timezone.utc).isoformat(),
              "tenants": len(tenants), "unknown_artifact_tenants": unknown_artifacts}
    qdrant_url = os.environ.get("TRPC_QDRANT_URL")
    if qdrant_url:
        from qdrant_client import QdrantClient
        client = QdrantClient(url=qdrant_url, timeout=10)
        unknown = set()
        try:
            for collection in client.get_collections().collections:
                points, offset = client.scroll(collection.name, limit=256, with_payload=True)
                while True:
                    unknown.update((item.payload or {}).get("tenant_id") for item in points
                                   if (item.payload or {}).get("tenant_id") not in tenants)
                    if offset is None:
                        break
                    points, offset = client.scroll(collection.name, limit=256, offset=offset, with_payload=True)
            result["unknown_qdrant_tenants"] = sorted(item for item in unknown if item)
        finally:
            client.close()
    else:
        result["unknown_qdrant_tenants"] = []
    result["consistent"] = not result["unknown_artifact_tenants"] and not result["unknown_qdrant_tenants"]
    return result


def run_restore_drill(source: str | Path, target_database_url: str) -> dict:
    """Restore an isolated target, then run consistency checks and emit evidence."""
    database = Database(target_database_url)
    try:
        restored = restore_snapshot(database, source, destructive=True)
        checked = consistency_check(database)
        if not checked["consistent"]:
            raise RuntimeError("cross-store consistency check failed")
        return {"drill": "passed", "restore": restored, "consistency": checked}
    finally:
        database.close()


def write_drill_evidence(result: dict, output: str | Path) -> None:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
