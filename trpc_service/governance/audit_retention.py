"""Operator configuration and periodic execution for encrypted audit archival."""

import asyncio
from contextlib import contextmanager
import os
import signal

from minio import Minio
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig
import urllib3

from trpc_service.channels.persistence import ContextCipher
from trpc_service.persistence import Database
from trpc_service.persistence.models import TenantRow, TenantConfigVersionRow
from trpc_service.storage.audit_archive import AuditArchive
from trpc_service.storage.runtime_resources import resolve_env
from trpc_service.tenant import TenantConfig


def archive_client():
    endpoint = os.environ["TRPC_AUDIT_ARCHIVE_ENDPOINT"]
    secure = os.environ.get("TRPC_AUDIT_ARCHIVE_SECURE", "true")
    if secure not in {"true", "false"}:
        raise ValueError("archive TLS setting must be true or false")
    pool = urllib3.PoolManager(timeout=urllib3.Timeout(connect=5, read=15),
                               retries=urllib3.Retry(total=1, backoff_factor=0.2))
    return Minio(endpoint,
                 access_key=os.environ["TRPC_AUDIT_ARCHIVE_ACCESS_KEY"],
                 secret_key=os.environ["TRPC_AUDIT_ARCHIVE_SECRET_KEY"],
                 secure=secure == "true",
                 http_client=pool), pool


def initialize_archive():
    client, pool = archive_client()
    bucket = os.environ.get("TRPC_AUDIT_ARCHIVE_BUCKET", "trpc-audit-archives")
    try:
        if client.bucket_exists(bucket):
            if client.get_bucket_versioning(bucket).status != ENABLED:
                raise ValueError("existing archive bucket is not versioned; choose a dedicated new bucket")
        else:
            client.make_bucket(bucket)
            client.set_bucket_versioning(bucket, VersioningConfig(ENABLED))
        return {"status": "ready", "versioned": True}
    finally:
        pool.clear()


def tenant_config(database, tenant):
    with database.sessions() as session:
        row = session.get(TenantRow, tenant)
        version = session.get(TenantConfigVersionRow, (tenant, row.active_config_version)) if row else None
        if version is None or version.status != "published":
            raise LookupError("tenant has no published audit policy")
        return TenantConfig.model_validate(version.config_json)


@contextmanager
def audit_database(platform_database, tenant, source="platform"):
    from trpc_service.management import ConfigurationStore
    config = tenant_config(platform_database, tenant)
    if source not in {"platform", "tenant_audit"}:
        raise ValueError("unsupported audit source")
    target, owned = platform_database, False
    try:
        if source == "tenant_audit":
            profile = ConfigurationStore(platform_database).resource_profile(tenant, config.config_version, "audit")
            url = resolve_env(profile.secret_ref.uri)
            if url != platform_database.url:
                target, owned = Database(url), True
                target.check_schema()
        yield target, config
    finally:
        if owned:
            target.close()


def archive_cipher():
    keys = os.environ["TRPC_AUDIT_ARCHIVE_KEYS"].split(",")
    if not 1 <= len(keys) <= 10:
        raise ValueError("archive key ring must contain one to ten keys")
    return ContextCipher([key.encode() for key in keys])


@contextmanager
def archive_service(platform_database, tenant, source="platform"):
    with audit_database(platform_database, tenant, source) as (target, config):
        cipher = archive_cipher()
        client, pool = archive_client()
        try:
            yield AuditArchive(target, client, os.environ.get("TRPC_AUDIT_ARCHIVE_BUCKET", "trpc-audit-archives"),
                               cipher), config
        finally:
            pool.clear()


def run_tenant(database, tenant, *, limit=200):
    from trpc_service.management import ConfigurationStore
    config = tenant_config(database, tenant)
    if not config.audit_policy.archive_enabled:
        return [{"source": "platform", "status": "disabled", "row_count": 0}]
    profile = ConfigurationStore(database).resource_profile(tenant, config.config_version, "audit")
    sources = ["platform"]
    if resolve_env(profile.secret_ref.uri) != database.url:
        sources.append("tenant_audit")
    results = []
    for source in sources:
        with archive_service(database, tenant, source) as (archive, current):
            results.append({
                "source":
                source,
                **archive.run_batch(tenant,
                                    current.audit_policy,
                                    config_version=current.config_version,
                                    source=source,
                                    limit=limit)
            })
    return results


async def worker(database, *, stop=None):
    from sqlalchemy import select
    from trpc_service.telemetry.logging import emit
    from trpc_service.telemetry.runtime import configure, shutdown, operation, count
    from trpc_service.worker import wait_or_stop
    from trpc_service.worker_health import heartbeat
    interval = int(os.environ.get("TRPC_AUDIT_ARCHIVE_INTERVAL", "300"))
    batch = int(os.environ.get("TRPC_AUDIT_ARCHIVE_BATCH_SIZE", "200"))
    if not 10 <= interval <= 86400 or not 1 <= batch <= 500:
        raise ValueError("archive schedule or batch size is outside allowed bounds")
    cipher = archive_cipher()
    client, pool = archive_client()
    try:
        await asyncio.to_thread(
            AuditArchive(database, client, os.environ.get("TRPC_AUDIT_ARCHIVE_BUCKET", "trpc-audit-archives"),
                         cipher).check_storage)
    finally:
        pool.clear()
    own_signals = stop is None
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM) if own_signals else ():
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    configure("audit-retention", database)
    count("audit.retention_failures", 0)
    health = asyncio.create_task(heartbeat(database, stop))

    def tenants():
        with database.sessions() as session:
            return list(session.scalars(select(TenantRow.tenant_id).where(TenantRow.active_config_version > 0)))

    try:
        while not stop.is_set():
            try:
                with operation("audit.retention_scan"):
                    for tenant in await asyncio.to_thread(tenants):
                        if stop.is_set():
                            break
                        try:
                            await asyncio.to_thread(run_tenant, database, tenant, limit=batch)
                        except Exception as error:
                            count("audit.retention_failures")
                            emit("audit.retention_failed", level=40, tenant_id=tenant, error_type=type(error).__name__)
            except Exception as error:
                count("audit.retention_failures")
                emit("audit.retention_scan_failed", level=40, error_type=type(error).__name__)
            await wait_or_stop(stop, interval)
    finally:
        stop.set()
        await health
        shutdown()
