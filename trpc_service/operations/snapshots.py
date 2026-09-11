"""Portable logical snapshots for the four runtime stores.

Snapshots are written to an operator supplied directory. Secrets are never
copied; credentials are resolved only from the process environment at restore
time. Restores require an explicit flag and should target an isolated stack.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

from sqlalchemy import delete, select

from trpc_service.persistence.models import Base
from trpc_service.operations.backup import _probe_stores


def _encode(value):
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, time):
        return {"__type__": "time", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, bytes):
        return {"__type__": "bytes", "value": base64.b64encode(value).decode()}
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode(item) for item in value]
    return value


def _decode(value, column):
    if isinstance(value, dict) and "__type__" in value:
        kind = value["__type__"]
        if kind == "datetime":
            return datetime.fromisoformat(value["value"])
        if kind == "date":
            return date.fromisoformat(value["value"])
        if kind == "time":
            return time.fromisoformat(value["value"])
        if kind == "decimal":
            return Decimal(value["value"])
        if kind == "bytes":
            return base64.b64decode(value["value"])
    if isinstance(value, dict):
        return {key: _decode(item, column) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item, column) for item in value]
    return value


def _snapshot_database(database):
    database.check_schema()
    with database.sessions() as session:
        return {
            table.name: [{column.name: _encode(row._mapping[column]) for column in table.c}
                         for row in session.execute(select(table))]
            for table in Base.metadata.sorted_tables
        }


def _snapshot_redis():
    url = os.environ.get("TRPC_REDIS_URL")
    if not url:
        return None
    import redis
    client = redis.Redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
    try:
        entries = []
        for key in client.scan_iter():
            key_text = key.decode("utf-8", "surrogateescape") if isinstance(key, bytes) else key
            dump = client.dump(key)
            if dump is not None:
                entries.append({"key": key_text, "dump": base64.b64encode(dump).decode(),
                                "ttl": client.pttl(key)})
        return entries
    finally:
        client.close()


def _snapshot_qdrant():
    url = os.environ.get("TRPC_QDRANT_URL")
    if not url:
        return None
    from qdrant_client import QdrantClient
    client = QdrantClient(url=url, timeout=10)
    try:
        result = {}
        for collection in client.get_collections().collections:
            points, offset = [], None
            while True:
                page, offset = client.scroll(collection.name, limit=256, offset=offset, with_payload=True,
                                             with_vectors=True)
                points.extend([{"id": item.id, "payload": item.payload, "vector": item.vector} for item in page])
                if offset is None:
                    break
            result[collection.name] = points
        return result
    finally:
        client.close()


def _snapshot_minio(root: Path):
    endpoint = os.environ.get("TRPC_MINIO_ENDPOINT")
    if not endpoint:
        return None
    from minio import Minio
    client = Minio(endpoint, access_key=os.environ.get("TRPC_MINIO_ACCESS_KEY", ""),
                   secret_key=os.environ.get("TRPC_MINIO_SECRET_KEY", ""),
                   secure=os.environ.get("TRPC_MINIO_SECURE", "false").lower() == "true")
    objects = []
    object_root = root / "objects"
    for bucket in client.list_buckets():
        for item in client.list_objects(bucket.name, recursive=True):
            # Object keys may contain ':' and other characters invalid on
            # Windows; retain the original key in the manifest and use a
            # deterministic safe filename for the payload.
            safe_name = hashlib.sha256(item.object_name.encode("utf-8")).hexdigest()
            target = object_root / bucket.name / safe_name
            target.parent.mkdir(parents=True, exist_ok=True)
            response = client.get_object(bucket.name, item.object_name)
            try:
                target.write_bytes(response.read())
            finally:
                response.close()
                response.release_conn()
            objects.append({"bucket": bucket.name, "key": item.object_name,
                            "path": str(target.relative_to(root)), "size": target.stat().st_size,
                            "sha256": hashlib.sha256(target.read_bytes()).hexdigest()})
    return objects


def create_snapshot(database, output: str | Path) -> dict:
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    payload = {"database": _snapshot_database(database),
               "redis": _snapshot_redis(), "qdrant": _snapshot_qdrant()}
    payload["minio"] = _snapshot_minio(root)
    payload["stores"] = _probe_stores()
    body = {"format_version": 1, **payload}
    encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    manifest = {**body, "sha256": hashlib.sha256(encoded).hexdigest()}
    (root / "snapshot.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def restore_snapshot(database, root: str | Path, *, destructive: bool = False) -> dict:
    if not destructive:
        raise ValueError("restore requires destructive=True and an isolated target")
    root = Path(root)
    manifest = validate_snapshot(root)
    database.check_schema()
    with database.sessions.begin() as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(delete(table))
        for table in Base.metadata.sorted_tables:
            columns = {column.name: column for column in table.c}
            for item in manifest["database"].get(table.name, []):
                values = {key: _decode(value, columns[key]) for key, value in item.items()}
                session.execute(table.insert().values(values))
    redis_entries = manifest.get("redis")
    if redis_entries is not None:
        import redis
        client = redis.Redis.from_url(os.environ["TRPC_REDIS_URL"])
        try:
            client.flushdb()
            for item in redis_entries:
                client.restore(item["key"], max(0, item["ttl"]), base64.b64decode(item["dump"]), replace=True)
        finally:
            client.close()
    qdrant_data = manifest.get("qdrant")
    if qdrant_data is not None:
        from qdrant_client import QdrantClient, models
        client = QdrantClient(url=os.environ["TRPC_QDRANT_URL"], timeout=15)
        try:
            for collection, points in qdrant_data.items():
                if client.collection_exists(collection):
                    client.delete_collection(collection)
                dimension = len(points[0]["vector"]) if points else int(os.environ.get("TRPC_VECTOR_DIMENSIONS", "1"))
                client.create_collection(collection, vectors_config=models.VectorParams(
                    size=dimension, distance=models.Distance.COSINE))
                if points:
                    structures = [
                        models.PointStruct(id=item["id"], vector=item["vector"], payload=item.get("payload") or {})
                        for item in points
                    ]
                    client.upsert(collection, structures, wait=True)
        finally:
            client.close()
    minio_objects = manifest.get("minio")
    if minio_objects is not None:
        from minio import Minio
        client = Minio(os.environ["TRPC_MINIO_ENDPOINT"],
                       access_key=os.environ.get("TRPC_MINIO_ACCESS_KEY", ""),
                       secret_key=os.environ.get("TRPC_MINIO_SECRET_KEY", ""),
                       secure=os.environ.get("TRPC_MINIO_SECURE", "false").lower() == "true")
        try:
            from io import BytesIO
            for item in minio_objects:
                if not client.bucket_exists(item["bucket"]):
                    client.make_bucket(item["bucket"])
                source = root / item["path"]
                payload = source.read_bytes()
                if hashlib.sha256(payload).hexdigest() != item["sha256"]:
                    raise ValueError("snapshot object checksum mismatch")
                client.put_object(item["bucket"], item["key"], BytesIO(payload), len(payload))
        finally:
            client = None
    return {"restored": True, "database_tables": len(manifest["database"]),
            "redis_keys": len(redis_entries or []), "snapshot": str(root)}


def validate_snapshot(root: str | Path) -> dict:
    """Validate snapshot checksum and every copied MinIO object without mutating stores."""
    root = Path(root)
    manifest = json.loads((root / "snapshot.json").read_text(encoding="utf-8"))
    supplied = manifest.pop("sha256", None)
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if supplied != hashlib.sha256(encoded).hexdigest():
        raise ValueError("snapshot checksum mismatch")
    for item in manifest.get("minio") or []:
        payload = (root / item["path"]).read_bytes()
        if len(payload) != item["size"] or hashlib.sha256(payload).hexdigest() != item["sha256"]:
            raise ValueError("snapshot object checksum mismatch")
    return manifest
