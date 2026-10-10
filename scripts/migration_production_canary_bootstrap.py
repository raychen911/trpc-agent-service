"""Provision one new, non-test production migration canary scope.

This command is intentionally separate from ``migration_acceptance_bootstrap``.
It creates a small source/target pair for a real ``migrate_data.py`` run, but
only after an explicit operator acknowledgement.  It never truncates, deletes,
or reuses a tenant; a collision is a hard failure.  Redis and PostgreSQL
credentials are read from the environment and are never included in output.

The command is not a migration gate.  It writes a provisioning report with
``production_gate=not_run``; only ``scripts/migrate_data.py
--production-confirm`` can produce production migration evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit

import asyncpg
import redis.asyncio as redis_async

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.migrate_data import (
    _atomic_write_json,
    _endpoint_fingerprint,
)
from trpc_service.storage.migration import (
    PostgresMigrationGuard,
    RedisMigrationSource,
)
from trpc_service.tenant.models import ModelPolicy, StorageSelection, TenantConfig

_CONFIRMATION = "I_UNDERSTAND_CREATE_NEW_PRODUCTION_CANARY"
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,95}$")
_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
_IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANARY_PREFIX = "production-canary-"
_BANNED_ID_MARKERS = frozenset(
    {
        "acceptance",
        "dummy",
        "example",
        "fixture",
        "mock",
        "simulated",
        "simulation",
        "test",
        "testing",
    }
)
_BANNED_ROLE_NAMES = frozenset(
    {
        "postgres",
        "root",
        "trpc",
        "trpc_migration",
        "trpc_worker",
        "superuser",
        "owner",
        "trpc_owner",
    }
)
_REQUIRED = (
    "TRPC_MIGRATION_SOURCE_REDIS_URL",
    "TRPC_MIGRATION_TARGET_DATABASE_DSN",
    "TRPC_MIGRATION_TENANT_ID",
    "TRPC_MIGRATION_ID",
    "TRPC_MIGRATION_APP_ID",
    "TRPC_MIGRATION_APP_REVISION",
    "TRPC_MIGRATION_CONFIG_VERSION",
    "TRPC_MIGRATION_BINDING_ID",
    "TRPC_MIGRATION_BINDING_REVISION",
)
_PROVISION_FLAG = "TRPC_MIGRATION_PROVISION"
_RUN_FLAG = "TRPC_RUN_REAL_MIGRATION"
_SESSIONS = 2
_MEMORIES = 2
_EXPECTED_RECORDS = _SESSIONS + _MEMORIES


@dataclass(frozen=True)
class CanaryScope:
    tenant_id: str
    migration_id: str
    app_id: str
    app_revision: int
    config_version: int
    binding_id: str
    binding_revision: int

    @property
    def target_config_version(self) -> int:
        return self.config_version + 1

    @property
    def source_profile_id(self) -> str:
        return f"{self.app_id}-redis"

    @property
    def target_profile_id(self) -> str:
        return f"{self.app_id}-postgres"


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _safe_id(name: str, value: str) -> str:
    if _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a bounded identifier")
    lowered = value.casefold()
    if any(marker in lowered for marker in _BANNED_ID_MARKERS):
        raise ValueError(f"{name} must not identify a test, fixture, or acceptance scope")
    if not lowered.startswith(_CANARY_PREFIX):
        raise ValueError(f"{name} must use the {_CANARY_PREFIX!r} prefix")
    return value


def _positive(name: str, value: str) -> int:
    if not value.isdecimal() or int(value) < 1:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def _scope_from_env() -> CanaryScope:
    values = {name: os.environ[name] for name in _REQUIRED}
    scope = CanaryScope(
        tenant_id=_safe_id("TRPC_MIGRATION_TENANT_ID", values["TRPC_MIGRATION_TENANT_ID"]),
        migration_id=_safe_id("TRPC_MIGRATION_ID", values["TRPC_MIGRATION_ID"]),
        app_id=_safe_id("TRPC_MIGRATION_APP_ID", values["TRPC_MIGRATION_APP_ID"]),
        app_revision=_positive(
            "TRPC_MIGRATION_APP_REVISION", values["TRPC_MIGRATION_APP_REVISION"]
        ),
        config_version=_positive(
            "TRPC_MIGRATION_CONFIG_VERSION", values["TRPC_MIGRATION_CONFIG_VERSION"]
        ),
        binding_id=_safe_id("TRPC_MIGRATION_BINDING_ID", values["TRPC_MIGRATION_BINDING_ID"]),
        binding_revision=_positive(
            "TRPC_MIGRATION_BINDING_REVISION", values["TRPC_MIGRATION_BINDING_REVISION"]
        ),
    )
    identities = (scope.tenant_id, scope.migration_id, scope.app_id, scope.binding_id)
    if len(set(identities)) != len(identities):
        raise ValueError("canary tenant, migration, app, and binding identifiers must be unique")
    if len(scope.target_profile_id) > 128:
        raise ValueError("canary app id is too long for generated storage profile identifiers")
    return scope


def _runtime_role(target_dsn: str) -> str:
    role = unquote(urlsplit(target_dsn).username or "").strip()
    if not role:
        raise ValueError("target database DSN must include an explicit runtime role")
    if role.casefold() in _BANNED_ROLE_NAMES:
        raise ValueError(
            "target database DSN must use a non-owner, non-worker, non-superuser runtime role"
        )
    return role


def _endpoint_host_port(value: str) -> tuple[str, int]:
    parsed = urlsplit(value.replace("postgresql+asyncpg://", "postgresql://"))
    defaults = {"redis": 6379, "rediss": 6379, "postgres": 5432, "postgresql": 5432}
    try:
        host = parsed.hostname
        port = parsed.port or defaults.get(parsed.scheme.casefold())
    except ValueError as error:
        raise ValueError("source or target endpoint has an invalid host or port") from error
    if not parsed.scheme or not host or port is None:
        raise ValueError("source or target endpoint must include scheme, host, and port")
    return host.casefold(), port


def _assert_independent_endpoints(source_url: str, target_dsn: str) -> None:
    if _endpoint_host_port(source_url) == _endpoint_host_port(target_dsn):
        raise ValueError("source and target endpoints must be independent")


def _release_binding() -> dict[str, str]:
    release_id = os.getenv("TRPC_RELEASE_ID", "").strip()
    release_nonce = os.getenv("TRPC_RELEASE_NONCE", "").strip()
    image_digest = os.getenv("TRPC_MIGRATION_IMAGE_DIGEST", "").strip()
    if _RELEASE_ID_RE.fullmatch(release_id) is None:
        raise ValueError("TRPC_RELEASE_ID is required for production-canary provisioning")
    if _RELEASE_NONCE_RE.fullmatch(release_nonce) is None:
        raise ValueError("TRPC_RELEASE_NONCE is required for production-canary provisioning")
    if _IMAGE_DIGEST_RE.fullmatch(image_digest) is None or image_digest in {
        "sha256:" + "0" * 64,
        "sha256:" + "f" * 64,
    }:
        raise ValueError("TRPC_MIGRATION_IMAGE_DIGEST must be a non-placeholder sha256 digest")
    return {
        "release_id": release_id,
        "release_nonce_sha256": _hash(release_nonce),
        "image_digest": image_digest,
    }


def _operator_confirmation() -> dict[str, str]:
    if os.getenv("TRPC_MIGRATION_PROVISION_CONFIRMATION") != _CONFIRMATION:
        raise ValueError("TRPC_MIGRATION_PROVISION_CONFIRMATION must equal " + repr(_CONFIRMATION))
    operator_id = os.getenv("TRPC_MIGRATION_OPERATOR_ID", "").strip()
    change_ticket = os.getenv("TRPC_MIGRATION_CHANGE_TICKET", "").strip()
    if not operator_id or not change_ticket:
        raise ValueError("TRPC_MIGRATION_OPERATOR_ID and TRPC_MIGRATION_CHANGE_TICKET are required")
    return {
        "status": "confirmed",
        "method": "explicit_environment_acknowledgement",
        "operator_id_sha256": _hash(operator_id),
        "change_ticket_sha256": _hash(change_ticket),
    }


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
            raise ValueError(f"Redis source scope is not empty for {tenant_id}")


def _role_contract_error(row: Mapping[str, Any], expected_role: str) -> str | None:
    session_user = str(row.get("session_user", ""))
    current_user = str(row.get("current_user", ""))
    role_name = str(row.get("rolname", ""))
    if session_user != expected_role or current_user != expected_role or role_name != expected_role:
        return "target database connection did not retain its explicit runtime role"
    if session_user != current_user:
        return "target database connection must not switch away from the explicit runtime role"
    if not row.get("rolcanlogin"):
        return "target database runtime role must be LOGIN"
    if row.get("rolsuper") or row.get("rolbypassrls"):
        return "target database runtime role must be NOSUPERUSER and NOBYPASSRLS"
    if row.get("rolcreaterole") or row.get("rolcreatedb") or row.get("rolreplication"):
        return "target database runtime role must not have administrative role attributes"
    if row.get("owns_public_objects"):
        return "target database runtime role must not own public tables or sequences"
    return None


async def _assert_runtime_role(connection: Any, expected_role: str) -> None:
    row = await connection.fetchrow(
        """
        SELECT session_user::text AS session_user,
               current_user::text AS current_user,
               r.rolname::text AS rolname,
               r.rolcanlogin,
               r.rolsuper,
               r.rolbypassrls,
               r.rolcreaterole,
               r.rolcreatedb,
               r.rolreplication,
               EXISTS (
                   SELECT 1
                     FROM pg_class AS c
                     JOIN pg_namespace AS n ON n.oid=c.relnamespace
                    WHERE n.nspname='public'
                      AND c.relkind IN ('r','p','m','S','f')
                      AND pg_get_userbyid(c.relowner)=r.rolname
               ) AS owns_public_objects
          FROM pg_roles AS r
         WHERE r.rolname=current_user
        """
    )
    if row is None:
        raise ValueError("target database runtime role attributes could not be verified")
    reason = _role_contract_error(row, expected_role)
    if reason is not None:
        raise ValueError(reason)


def _config(scope: CanaryScope, version: int, profile_id: str, backend: str) -> str:
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


async def _assert_database_scope_empty(
    pool: asyncpg.Pool, scope: CanaryScope, expected_role: str
) -> None:
    guard = PostgresMigrationGuard(pool)
    async with pool.acquire() as connection, connection.transaction():
        await _assert_runtime_role(connection, expected_role)
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", scope.tenant_id)
        await connection.fetchval(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", scope.tenant_id
        )
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM tenants WHERE tenant_id=$1)", scope.tenant_id
        ):
            raise ValueError(f"production-canary tenant already exists: {scope.tenant_id}")
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM agent_apps WHERE tenant_id=$1)", scope.tenant_id
        ):
            raise ValueError(f"production-canary app scope already exists: {scope.tenant_id}")
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM channel_bindings WHERE binding_id=$1)",
            scope.binding_id,
        ):
            raise ValueError(f"production-canary binding already exists: {scope.binding_id}")
        # This private method is deliberately used on the same connection so
        # the advisory lock, occupancy check, and inserts below share one
        # transaction.  Calling the public method here would release the lock
        # between preflight and provisioning.
        await guard._target_empty_preflight(connection, scope.tenant_id)


async def _insert_database_scope(
    pool: asyncpg.Pool, scope: CanaryScope, expected_role: str
) -> None:
    source_json = _config(scope, scope.config_version, scope.source_profile_id, "redis")
    target_json = _config(scope, scope.target_config_version, scope.target_profile_id, "postgresql")
    guard = PostgresMigrationGuard(pool)
    async with pool.acquire() as connection, connection.transaction():
        await _assert_runtime_role(connection, expected_role)
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", scope.tenant_id)
        await connection.fetchval(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", scope.tenant_id
        )
        # Recheck after Redis provisioning.  The first preflight prevents
        # avoidable writes; this second one closes the race before the first
        # database INSERT and still refuses a partially reused scope.
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM tenants WHERE tenant_id=$1)", scope.tenant_id
        ):
            raise ValueError(f"production-canary tenant already exists: {scope.tenant_id}")
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM channel_bindings WHERE binding_id=$1)",
            scope.binding_id,
        ):
            raise ValueError(f"production-canary binding already exists: {scope.binding_id}")
        await guard._target_empty_preflight(connection, scope.tenant_id)
        # Every INSERT is intentionally conflict-sensitive.  The helper never
        # turns a rerun into an update or cleanup operation.
        await connection.execute(
            "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,$2)",
            scope.tenant_id,
            "Production migration canary (offline model)",
        )
        await connection.execute(
            """
            INSERT INTO agent_apps (
                tenant_id,app_id,display_name,active_config_version,
                candidate_config_version,candidate_percent,control_version
            ) VALUES ($1,$2,$3,$4,$5,0,$6)
            """,
            scope.tenant_id,
            scope.app_id,
            "Production migration canary app",
            scope.config_version,
            scope.target_config_version,
            scope.app_revision,
        )
        for profile_id, profile_json in (
            (
                scope.source_profile_id,
                json.dumps({"backend": "redis", "scope": "production-canary"}),
            ),
            (
                scope.target_profile_id,
                json.dumps({"backend": "postgresql", "scope": "production-canary"}),
            ),
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
        for version, config_json in (
            (scope.config_version, source_json),
            (scope.target_config_version, target_json),
        ):
            await connection.execute(
                """
                INSERT INTO config_revisions (
                    tenant_id,app_id,version,config_json,checksum,created_by
                ) VALUES ($1,$2,$3,$4::jsonb,$5,$6)
                """,
                scope.tenant_id,
                scope.app_id,
                version,
                config_json,
                _hash(config_json),
                "migration-production-canary-bootstrap",
            )
        await connection.execute(
            """
            INSERT INTO tenant_policies (tenant_id,policy_version,policy_json)
            VALUES ($1,1,$2::jsonb)
            """,
            scope.tenant_id,
            json.dumps({"mode": "production-canary", "model_provider": "offline"}),
        )
        await connection.execute(
            """
            INSERT INTO channel_bindings (
                tenant_id,binding_id,app_id,channel,account_id,
                secret_refs,capabilities,enabled,control_version
            ) VALUES ($1,$2,$3,'feishu',$4,'{}'::jsonb,'["text"]'::jsonb,true,$5)
            """,
            scope.tenant_id,
            scope.binding_id,
            scope.app_id,
            f"{scope.binding_id}-account",
            scope.binding_revision,
        )


def _session_payload(scope: CanaryScope, index: int) -> dict[str, Any]:
    return {
        "app_id": scope.app_id,
        "principal_id": f"{scope.app_id}-principal-{index:02d}",
        "state": {"canary": index},
        "version": 1,
        "next_sequence": 2,
        "events": [
            {
                "sequence": 1,
                "event_id": f"{scope.app_id}-event-{index:02d}",
                "author": "migration-production-canary",
                "timestamp": float(index + 1),
                "event": {"kind": "canary"},
                "state_delta": {"canary": index},
            }
        ],
    }


async def _seed_redis(client: Any, scope: CanaryScope) -> int:
    seeded = 0
    for index in range(_SESSIONS):
        session_id = f"{scope.app_id}-session-{index:02d}"
        result = await client.hsetnx(
            _projection_key(scope.tenant_id, session_id),
            "payload",
            json.dumps(_session_payload(scope, index), separators=(",", ":")),
        )
        if not result:
            raise ValueError("Redis source scope changed during canary provisioning")
        seeded += 1
    for index in range(_MEMORIES):
        memory_id = f"{scope.app_id}-memory-{index:02d}"
        payload = {
            "principal_id": f"{scope.app_id}-principal-{index:02d}",
            "session_id": f"{scope.app_id}-session-{index:02d}",
            "source_sequence": index + 1,
            "memory": {"canary": index},
        }
        result = await client.set(
            f"trpc:memory:{scope.tenant_id}:{memory_id}",
            json.dumps(payload, separators=(",", ":")),
            nx=True,
        )
        if not result:
            raise ValueError("Redis source scope changed during canary provisioning")
        seeded += 1
    return seeded


def _base_report(
    *,
    status: str,
    scope: CanaryScope | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": 1,
        "scope": "production-canary-provisioning",
        "status": status,
        "gate": "pass" if status == "pass" else status,
        "production_gate": "not_run",
        "credentials_emitted": False,
    }
    if reason:
        report["rejection_reasons"] = [reason]
    if scope is not None:
        report["tenant_id"] = scope.tenant_id
        report["migration_id"] = scope.migration_id
        report["app_id"] = scope.app_id
        report["app_revision"] = scope.app_revision
        report["config_version"] = scope.config_version
        report["target_config_version"] = scope.target_config_version
        report["binding_id"] = scope.binding_id
        report["binding_revision"] = scope.binding_revision
    return report


async def provision() -> dict[str, Any]:
    if os.getenv(_RUN_FLAG) != "1" or os.getenv(_PROVISION_FLAG) != "1":
        return _base_report(
            status="not_run",
            reason=f"{_RUN_FLAG}=1 and {_PROVISION_FLAG}=1 are required",
        )
    missing = [name for name in _REQUIRED if not os.getenv(name)]
    if missing:
        return _base_report(status="not_run", reason=f"missing required environment: {missing[0]}")

    scope = _scope_from_env()
    source_url = os.environ["TRPC_MIGRATION_SOURCE_REDIS_URL"]
    target_dsn = os.environ["TRPC_MIGRATION_TARGET_DATABASE_DSN"].replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    expected_role = _runtime_role(target_dsn)
    _assert_independent_endpoints(source_url, target_dsn)
    operator = _operator_confirmation()
    release = _release_binding()
    source_endpoint_sha256 = _endpoint_fingerprint(source_url)
    target_endpoint_sha256 = _endpoint_fingerprint(target_dsn)

    redis = redis_async.from_url(source_url, decode_responses=False)
    pool = await asyncpg.create_pool(target_dsn, min_size=1, max_size=4)
    try:
        await _assert_redis_empty(redis, scope.tenant_id)
        await _assert_database_scope_empty(pool, scope, expected_role)
        seeded = await _seed_redis(redis, scope)
        source = RedisMigrationSource(cast(Any, redis), kinds=("session", "memory"))
        snapshot = await source.snapshot(scope.tenant_id)
        if seeded != _EXPECTED_RECORDS or snapshot.source_count != _EXPECTED_RECORDS:
            raise ValueError("production-canary Redis seed count is not exactly four")
        await _insert_database_scope(pool, scope, expected_role)
        return {
            **_base_report(status="pass", scope=scope),
            "source": {
                "backend": "redis",
                "endpoint_sha256": source_endpoint_sha256,
                "seeded_records": seeded,
                "snapshot_id": snapshot.source_snapshot_id,
                "source_count": snapshot.source_count,
                "source_checksum": snapshot.source_checksum,
            },
            "target": {
                "backend": "postgresql",
                "endpoint_sha256": target_endpoint_sha256,
                "runtime_role_sha256": _hash(expected_role),
                "target_preflight": "empty",
            },
            "release_binding": release,
            "operator_confirmation": operator,
            "provisioning": {
                "new_scope_only": True,
                "redis_seed": "pass",
                "database_scope": "pass",
                "secret_refs": "empty",
            },
        }
    finally:
        await redis.aclose()
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/migration-production-canary-bootstrap.json"),
    )
    args = parser.parse_args()
    scope: CanaryScope | None = None
    if all(os.getenv(name) for name in _REQUIRED):
        try:
            scope = _scope_from_env()
        except ValueError:
            # ``provision`` emits the safe validation reason; this best-effort
            # parse only keeps valid identities in a failure report so an
            # interrupted write can be reviewed without exposing a DSN.
            scope = None
    try:
        result = asyncio.run(provision())
    except ValueError as error:
        result = _base_report(status="fail", scope=scope, reason=str(error))
    except Exception as error:  # connection errors may contain DSNs; never print them
        result = _base_report(
            status="fail", scope=scope, reason=f"error_type={type(error).__name__}"
        )
    _atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
