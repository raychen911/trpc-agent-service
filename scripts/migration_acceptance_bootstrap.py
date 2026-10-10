"""Create one isolated, fail-closed migration acceptance fixture.

The helper only creates new tenant/app/binding scopes.  It never truncates or
deletes data, and it refuses to continue when either Redis prefixes or any
guarded PostgreSQL target table already contains rows for a requested scope.
It is intentionally opt-in and prints identifiers/counts only; connection
strings and database errors are never included in its output.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import asyncpg
import redis.asyncio as redis_async

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trpc_service.storage.migration import PostgresMigrationGuard
from trpc_service.tenant.models import ModelPolicy, StorageSelection, TenantConfig

EXPECTED_RECORDS = 200
SESSIONS = 100
MEMORIES = 100
_CLIENT_TIMEOUT_SECONDS = 30.0
_CLOSE_TIMEOUT_SECONDS = 5.0
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_LOGGER = logging.getLogger(__name__)
_REQUIRED = (
    "TRPC_MIGRATION_SOURCE_REDIS_URL",
    "TRPC_MIGRATION_TARGET_DATABASE_DSN",
    "TRPC_MIGRATION_TENANT_ID",
    "TRPC_MIGRATION_ID",
    "TRPC_MIGRATION_EXPECTED_RECORDS",
    "TRPC_MIGRATION_APP_ID",
    "TRPC_MIGRATION_APP_REVISION",
    "TRPC_MIGRATION_CONFIG_VERSION",
    "TRPC_MIGRATION_BINDING_ID",
    "TRPC_MIGRATION_BINDING_REVISION",
)


@dataclass(frozen=True)
class BootstrapScope:
    tenant_id: str
    migration_id: str
    app_id: str
    binding_id: str | None


def _safe_id(name: str, value: str) -> str:
    if not _ID_RE.fullmatch(value):
        raise ValueError(f"{name} must be a bounded identifier")
    return value


def _positive(name: str, value: str) -> int:
    if not value.isdecimal() or int(value) < 1:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _runtime_role(target_dsn: str) -> str:
    role = unquote(urlsplit(target_dsn).username or "").strip()
    if not role:
        raise ValueError("target database DSN must include an explicit runtime role")
    if role.casefold() in {
        "trpc_migration",
        "trpc_worker",
        "postgres",
        "root",
        "trpc",
        "superuser",
        "owner",
        "trpc_owner",
    }:
        raise ValueError(
            "target database DSN must use a runtime role, not a schema/migration owner"
        )
    return role


def _endpoint(value: str) -> tuple[str, str, int]:
    parsed = urlsplit(value.replace("postgresql+asyncpg://", "postgresql://"))
    defaults = {"redis": 6379, "rediss": 6379, "postgres": 5432, "postgresql": 5432}
    try:
        hostname = parsed.hostname or ""
        port = parsed.port or defaults.get(parsed.scheme.lower())
    except ValueError as error:
        raise ValueError("source or target endpoint has an invalid host or port") from error
    if not parsed.scheme or not hostname or port is None:
        raise ValueError("source or target endpoint must include scheme, host, and port")
    return parsed.scheme.lower(), hostname.lower(), port


def _scope_from_env() -> tuple[BootstrapScope, BootstrapScope]:
    values = {name: os.environ[name] for name in _REQUIRED}
    base = BootstrapScope(
        tenant_id=_safe_id("TRPC_MIGRATION_TENANT_ID", values["TRPC_MIGRATION_TENANT_ID"]),
        migration_id=_safe_id("TRPC_MIGRATION_ID", values["TRPC_MIGRATION_ID"]),
        app_id=_safe_id("TRPC_MIGRATION_APP_ID", values["TRPC_MIGRATION_APP_ID"]),
        binding_id=_safe_id("TRPC_MIGRATION_BINDING_ID", values["TRPC_MIGRATION_BINDING_ID"]),
    )
    for name, value in (
        ("TRPC_MIGRATION_TENANT_ID", base.tenant_id),
        ("TRPC_MIGRATION_ID", base.migration_id),
        ("TRPC_MIGRATION_APP_ID", base.app_id),
        ("TRPC_MIGRATION_BINDING_ID", base.binding_id or ""),
    ):
        if not value.startswith("migration-acceptance-"):
            raise ValueError(f"{name} must use the migration-acceptance- prefix")
    _positive("TRPC_MIGRATION_APP_REVISION", values["TRPC_MIGRATION_APP_REVISION"])
    _positive("TRPC_MIGRATION_CONFIG_VERSION", values["TRPC_MIGRATION_CONFIG_VERSION"])
    _positive("TRPC_MIGRATION_BINDING_REVISION", values["TRPC_MIGRATION_BINDING_REVISION"])
    expected_records = _positive(
        "TRPC_MIGRATION_EXPECTED_RECORDS", values["TRPC_MIGRATION_EXPECTED_RECORDS"]
    )
    if expected_records != EXPECTED_RECORDS:
        raise ValueError(f"TRPC_MIGRATION_EXPECTED_RECORDS must be exactly {EXPECTED_RECORDS}")
    phase = BootstrapScope(
        tenant_id=_safe_id(
            "TRPC_MIGRATION_PHASE_TENANT_ID",
            os.getenv("TRPC_MIGRATION_PHASE_TENANT_ID", f"{base.tenant_id}-phase"),
        ),
        migration_id=_safe_id(
            "TRPC_MIGRATION_PHASE_ID",
            os.getenv("TRPC_MIGRATION_PHASE_ID", f"{base.migration_id}-phase"),
        ),
        app_id=_safe_id(
            "TRPC_MIGRATION_PHASE_APP_ID",
            os.getenv("TRPC_MIGRATION_PHASE_APP_ID", f"{base.app_id}-phase"),
        ),
        binding_id=None,
    )
    for name, value in (
        ("TRPC_MIGRATION_PHASE_TENANT_ID", phase.tenant_id),
        ("TRPC_MIGRATION_PHASE_ID", phase.migration_id),
        ("TRPC_MIGRATION_PHASE_APP_ID", phase.app_id),
    ):
        if not value.startswith("migration-acceptance-"):
            raise ValueError(f"{name} must use the migration-acceptance- prefix")
    identities = [
        base.tenant_id,
        base.migration_id,
        base.app_id,
        base.binding_id or "",
        phase.tenant_id,
        phase.migration_id,
        phase.app_id,
    ]
    if len(set(identities)) != len(identities):
        raise ValueError(
            "acceptance scopes must use unique tenant, app, binding, and migration identifiers"
        )
    return base, phase


def _projection_key(tenant_id: str, session_id: str) -> str:
    tenant = base64.urlsafe_b64encode(tenant_id.encode()).rstrip(b"=").decode()
    session = base64.urlsafe_b64encode(session_id.encode()).rstrip(b"=").decode()
    return f"trpc:projection:session:v2:{tenant}.{session}"


def _projection_tenant_prefix(tenant_id: str) -> str:
    encoded = base64.urlsafe_b64encode(tenant_id.encode()).rstrip(b"=").decode()
    return f"trpc:projection:session:v2:{encoded}."


async def _assert_redis_empty(client: Any, tenant_id: str) -> None:
    patterns = (
        f"trpc:projection:session:{tenant_id}:*",
        "trpc:projection:session:v2:*",
        f"trpc:memory:{tenant_id}:*",
    )
    for pattern in patterns:
        async for raw_key in client.scan_iter(match=pattern, count=1000):
            key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
            if pattern == "trpc:projection:session:v2:*" and not key.startswith(
                _projection_tenant_prefix(tenant_id)
            ):
                continue
            raise ValueError(f"Redis acceptance source prefix is not empty for {tenant_id}")


async def _assert_database_scope_empty(pool: asyncpg.Pool, scope: BootstrapScope) -> None:
    guard = PostgresMigrationGuard(pool)
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", scope.tenant_id)
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM tenants WHERE tenant_id=$1)", scope.tenant_id
        ):
            raise ValueError(f"acceptance tenant already exists: {scope.tenant_id}")
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM agent_apps WHERE tenant_id=$1)", scope.tenant_id
        ):
            raise ValueError(f"acceptance app scope already exists: {scope.tenant_id}")
        # Reuse this transaction's connection.  Calling the public guard
        # preflight here would acquire a second pooled connection while this
        # transaction is still open; through an ACK port-forward that leaves
        # an idle transaction checked out and can exhaust the small pool.
        preflight = await guard._target_empty_preflight(connection, scope.tenant_id)
        if not preflight.empty:
            raise ValueError(f"target guarded tables are not empty for {scope.tenant_id}")


def _config(scope: BootstrapScope, version: int, profile_id: str, backend: str) -> str:
    config = TenantConfig(
        tenant_id=scope.tenant_id,
        app_id=scope.app_id,
        version=version,
        model=ModelPolicy(provider="offline", model="deterministic-fake"),
        storage=StorageSelection(
            profile_id=profile_id,
            session_backend=backend,
            memory_backend=backend,
        ),
    )
    return config.model_dump_json()


async def _insert_database_scope(pool: asyncpg.Pool, scope: BootstrapScope, *, base: bool) -> None:
    source_profile = f"{scope.app_id}-redis"
    target_profile = f"{scope.app_id}-postgres"
    source_json = _config(scope, 1, source_profile, "redis")
    target_json = _config(scope, 2, target_profile, "postgresql")
    source_checksum = hashlib.sha256(source_json.encode()).hexdigest()
    target_checksum = hashlib.sha256(target_json.encode()).hexdigest()
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", scope.tenant_id)
        await connection.execute(
            "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,$2)",
            scope.tenant_id,
            "Migration acceptance isolated tenant",
        )
        await connection.execute(
            """
            INSERT INTO agent_apps (
                tenant_id,app_id,display_name,active_config_version,
                candidate_config_version,candidate_percent,control_version
            ) VALUES ($1,$2,$3,1,2,0,1)
            """,
            scope.tenant_id,
            scope.app_id,
            "Migration acceptance isolated app",
        )
        for profile_id, profile_json in (
            (source_profile, json.dumps({"backend": "redis"})),
            (target_profile, json.dumps({"backend": "postgresql"})),
        ):
            await connection.execute(
                """
                INSERT INTO storage_profiles (tenant_id,profile_id,profile_json)
                VALUES ($1,$2,$3::jsonb)
                """,
                scope.tenant_id,
                profile_id,
                profile_json,
            )
        for version, config_json, checksum in (
            (1, source_json, source_checksum),
            (2, target_json, target_checksum),
        ):
            await connection.execute(
                """
                INSERT INTO config_revisions (
                    tenant_id,app_id,version,config_json,checksum,created_by
                ) VALUES ($1,$2,$3,$4::jsonb,$5,'migration-acceptance-bootstrap')
                """,
                scope.tenant_id,
                scope.app_id,
                version,
                config_json,
                checksum,
            )
        if base and scope.binding_id is not None:
            await connection.execute(
                """
                INSERT INTO channel_bindings (
                    tenant_id,binding_id,app_id,channel,account_id,
                    secret_refs,capabilities,enabled,control_version
                ) VALUES ($1,$2,$3,'feishu',$4,'{}'::jsonb,'[\"text\"]'::jsonb,true,1)
                """,
                scope.tenant_id,
                scope.binding_id,
                scope.app_id,
                f"migration-acceptance-{scope.app_id}",
            )


async def _seed_redis(client: Any, scope: BootstrapScope) -> int:
    count = 0
    for index in range(SESSIONS):
        session_id = f"{scope.app_id}-session-{index:03d}"
        payload = {
            "app_id": scope.app_id,
            "principal_id": f"{scope.app_id}-principal-{index:03d}",
            "state": {"fixture": index},
            "version": 1,
            "next_sequence": 1,
            "events": [],
        }
        await client.hset(
            _projection_key(scope.tenant_id, session_id), "payload", json.dumps(payload)
        )
        count += 1
    for index in range(MEMORIES):
        memory_id = f"{scope.app_id}-memory-{index:03d}"
        payload = {
            "principal_id": f"{scope.app_id}-principal-{index:03d}",
            "session_id": f"{scope.app_id}-session-{index:03d}",
            "source_sequence": index + 1,
            "memory": {"fixture": index},
        }
        await client.set(f"trpc:memory:{scope.tenant_id}:{memory_id}", json.dumps(payload))
        count += 1
    return count


async def bootstrap() -> dict[str, Any]:
    if (
        os.getenv("TRPC_RUN_REAL_MIGRATION") != "1"
        or os.getenv("TRPC_MIGRATION_FULL_ACCEPTANCE") != "1"
    ):
        return {"status": "not_run", "reason": "both live migration opt-ins are required"}
    if os.getenv("TRPC_MIGRATION_BOOTSTRAP") != "1":
        return {"status": "not_run", "reason": "TRPC_MIGRATION_BOOTSTRAP=1 is required"}
    missing = [name for name in _REQUIRED if not os.getenv(name)]
    if missing:
        return {"status": "not_run", "reason": f"missing required environment: {missing[0]}"}
    base, phase = _scope_from_env()
    source_url = os.environ["TRPC_MIGRATION_SOURCE_REDIS_URL"]
    target_dsn = os.environ["TRPC_MIGRATION_TARGET_DATABASE_DSN"].replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    _runtime_role(target_dsn)
    source_endpoint = _endpoint(source_url)
    target_endpoint = _endpoint(target_dsn)
    if source_endpoint[1:] == target_endpoint[1:]:
        raise ValueError("source and target endpoints must be independent")
    redis = redis_async.from_url(
        source_url,
        decode_responses=False,
        socket_connect_timeout=_CLIENT_TIMEOUT_SECONDS,
        socket_timeout=_CLIENT_TIMEOUT_SECONDS,
    )
    pool = await asyncpg.create_pool(
        target_dsn,
        min_size=1,
        max_size=4,
        command_timeout=_CLIENT_TIMEOUT_SECONDS,
        timeout=_CLIENT_TIMEOUT_SECONDS,
        server_settings={
            "application_name": "trpc-migration-acceptance-bootstrap",
            "statement_timeout": str(int(_CLIENT_TIMEOUT_SECONDS * 1000)),
            "lock_timeout": "5000",
        },
    )
    try:
        for scope in (base, phase):
            await _assert_redis_empty(redis, scope.tenant_id)
            await _assert_database_scope_empty(pool, scope)
        await _insert_database_scope(pool, base, base=True)
        await _insert_database_scope(pool, phase, base=False)
        seeded = {scope.tenant_id: await _seed_redis(redis, scope) for scope in (base, phase)}
        if any(value != EXPECTED_RECORDS for value in seeded.values()):
            raise AssertionError("acceptance Redis seed count is not exactly 200")
        return {
            "status": "pass",
            "expected_records_per_scope": EXPECTED_RECORDS,
            "scopes": {
                "base": {
                    "tenant_id": base.tenant_id,
                    "migration_id": base.migration_id,
                    "app_id": base.app_id,
                    "binding_id": base.binding_id,
                    "seeded_records": seeded[base.tenant_id],
                },
                "phase": {
                    "tenant_id": phase.tenant_id,
                    "migration_id": phase.migration_id,
                    "app_id": phase.app_id,
                    "seeded_records": seeded[phase.tenant_id],
                },
            },
        }
    finally:
        try:
            await asyncio.wait_for(redis.aclose(), timeout=_CLOSE_TIMEOUT_SECONDS)
        except BaseException:
            connection_pool = getattr(redis, "connection_pool", None)
            disconnect = getattr(connection_pool, "disconnect", None)
            if callable(disconnect):
                try:
                    await asyncio.wait_for(
                        disconnect(inuse_connections=True), timeout=_CLOSE_TIMEOUT_SECONDS
                    )
                except BaseException as error:
                    _LOGGER.warning(
                        "migration bootstrap Redis pool termination failed: %s",
                        type(error).__name__,
                    )
        try:
            await asyncio.wait_for(pool.close(), timeout=_CLOSE_TIMEOUT_SECONDS)
        except BaseException:
            pool.terminate()


def main() -> int:
    try:
        result = asyncio.run(bootstrap())
    except ValueError as error:
        result = {"status": "fail", "reason": str(error)}
    except Exception as error:  # errors may contain connection details; never print them
        result = {"status": "fail", "error_type": type(error).__name__}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
