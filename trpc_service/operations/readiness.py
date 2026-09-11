"""Dependency readiness probes used by orchestration and incident response."""

from __future__ import annotations

import os
from sqlalchemy import text
from trpc_service.persistence.models import IMAccountRow, utcnow


def _probe(name, fn, checks):
    try:
        fn()
    except Exception:  # probes must never leak credentials or URLs
        checks[name] = "failed"
        return False
    checks[name] = "ok"
    return True


def readiness(database) -> tuple[bool, dict[str, str]]:
    """Check the platform database and every explicitly configured dependency.

    Optional dependencies are checked only when their deployment URL is present;
    this keeps local SQLite development usable while making protected deployments
    fail closed when Redis, Qdrant or MinIO is unavailable.
    """
    checks: dict[str, str] = {}
    healthy = _probe("database", lambda: _database(database), checks)

    redis_url = os.environ.get("TRPC_REDIS_URL")
    if redis_url:
        healthy &= _probe("redis", lambda: _redis(redis_url), checks)
    qdrant_url = os.environ.get("TRPC_QDRANT_URL")
    if qdrant_url:
        healthy &= _probe("qdrant", lambda: _qdrant(qdrant_url), checks)
    minio_endpoint = os.environ.get("TRPC_MINIO_ENDPOINT")
    if minio_endpoint:
        healthy &= _probe("object", lambda: _object(minio_endpoint), checks)
    if os.environ.get("TRPC_RUNTIME_MODE") == "protected":
        checks["model_credentials"] = "configured" if os.environ.get("TRPC_MODEL_API_KEY") else "missing"
        healthy &= checks["model_credentials"] == "configured"
    if os.environ.get("TRPC_REQUIRE_IM_CHANNELS", "false").lower() == "true":
        healthy &= _probe("channels", lambda: _channels(database), checks)
    return healthy, checks


def _database(database):
    with database.sessions() as session:
        session.execute(text("SELECT 1"))


def _channels(database):
    with database.sessions() as session:
        accounts = list(session.query(IMAccountRow).all())
    if not accounts or any(row.owner_id is None or row.lease_expires_at is None
                           or row.lease_expires_at <= utcnow() for row in accounts):
        raise RuntimeError("no live IM account lease")


def _redis(url):
    import redis
    client = redis.Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1)
    try:
        client.ping()
    finally:
        client.close()


def _qdrant(url):
    from qdrant_client import QdrantClient
    client = QdrantClient(url=url, timeout=2)
    try:
        client.get_collections()
    finally:
        client.close()


def _object(endpoint):
    from minio import Minio
    client = Minio(endpoint,
                   access_key=os.environ.get("TRPC_MINIO_ACCESS_KEY", ""),
                   secret_key=os.environ.get("TRPC_MINIO_SECRET_KEY", ""),
                   secure=os.environ.get("TRPC_MINIO_SECURE", "false").lower() == "true")
    # A list operation validates credentials and network reachability without
    # mutating buckets or objects.
    next(iter(client.list_buckets()), None)
