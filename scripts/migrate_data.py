"""Run an explicitly opt-in live Redis-to-PostgreSQL tenant migration.

The default is a safe machine-readable ``not_run`` report.  This command never
uses fake stores and never treats missing credentials as a successful gate.
Set ``TRPC_RUN_REAL_MIGRATION=1`` and provide the source/target environment
variables before running it against an isolated tenant.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import json
import logging
import os
import re
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit

import asyncpg
import redis.asyncio as redis_async

from scripts.evidence_lineage import build_evidence, new_run_id, runtime_fingerprint
from trpc_service.storage.migration import (
    MAX_MIGRATION_BATCH_SIZE,
    MAX_MIGRATION_DB_POOL_SIZE,
    MAX_MIGRATION_EXPECTED_RECORDS,
    MigrationCoordinator,
    MigrationPhase,
    MigrationScopeManifest,
    MigrationSourceKind,
    PostgresMigrationCheckpointStore,
    PostgresMigrationGuard,
    PostgresMigrationTarget,
    RedisMigrationSource,
    canonical_migration_kinds,
)

_LOGGER = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parents[1]
_PRODUCER = "scripts.migrate_data"
_PRODUCTION_CONFIRMATION = "I_UNDERSTAND_REAL_MIGRATION"
_ALLOWED_PRODUCTION_FACTORIES = frozenset(
    {
        "production_migration_control.create",
        "trpc_service.storage.production_migration_control:create",
        "trpc_service.storage.migration_control:create",
    }
)
_PRODUCTION_FACTORY_ALIASES = {
    "production_migration_control.create": (
        "trpc_service.storage.production_migration_control:create"
    ),
    "trpc_service.storage.migration_control:create": (
        "trpc_service.storage.production_migration_control:create"
    ),
}
_PRODUCTION_PHASES = (
    MigrationPhase.PREPARE,
    MigrationPhase.BACKFILL,
    MigrationPhase.SHADOW_READ,
    MigrationPhase.DUAL_WRITE,
    MigrationPhase.CUTOVER,
    MigrationPhase.VERIFY,
    MigrationPhase.CLEANUP,
)

REQUIRED_ENV = (
    "TRPC_MIGRATION_SOURCE_REDIS_URL",
    "TRPC_MIGRATION_TARGET_DATABASE_DSN",
    "TRPC_MIGRATION_TENANT_ID",
    "TRPC_MIGRATION_ID",
    "TRPC_MIGRATION_APP_ID",
    "TRPC_MIGRATION_APP_REVISION",
    "TRPC_MIGRATION_CONFIG_VERSION",
    "TRPC_MIGRATION_BINDING_ID",
    "TRPC_MIGRATION_BINDING_REVISION",
    "TRPC_MIGRATION_OWNER_ID",
)

_PRODUCTION_RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_PRODUCTION_RELEASE_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")


def _endpoint_fingerprint(value: str) -> str:
    """Hash only the non-secret identity of a source or target endpoint."""

    parsed = urlsplit(value.replace("postgresql+asyncpg://", "postgresql://"))
    scheme = parsed.scheme.lower()
    default_port = {
        "redis": 6379,
        "rediss": 6379,
        "postgres": 5432,
        "postgresql": 5432,
    }.get(scheme)
    try:
        port = parsed.port or default_port
        hostname = parsed.hostname
    except ValueError as error:
        raise ValueError("migration endpoint must include a valid host and port") from error
    if not scheme or not hostname or port is None:
        raise ValueError("migration endpoint must include a scheme, host, and port")
    identity = "|".join(
        (
            scheme,
            hostname.lower(),
            str(port),
            parsed.path,
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _endpoint_host_port(value: str) -> tuple[str, int]:
    """Return the service identity used to reject same-endpoint migrations."""

    parsed = urlsplit(value.replace("postgresql+asyncpg://", "postgresql://"))
    scheme = parsed.scheme.lower()
    default_port = {
        "redis": 6379,
        "rediss": 6379,
        "postgres": 5432,
        "postgresql": 5432,
    }.get(scheme)
    try:
        port = parsed.port or default_port
        hostname = parsed.hostname
    except ValueError as error:
        raise ValueError("migration endpoint must include a valid host and port") from error
    if not hostname or port is None:
        raise ValueError("migration endpoint must include a scheme, host, and port")
    return hostname.lower(), port


def _require_runtime_target_role(target_dsn: str) -> str:
    """Keep live writes on the runtime role, separate from schema ownership."""

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


def _runtime_role_contract_error(row: Mapping[str, Any], expected_role: str) -> str | None:
    """Return a safe reason when a live target role is privileged or owns data."""

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


async def _assert_runtime_role_contract(pool: asyncpg.Pool, expected_role: str) -> None:
    """Verify the production target uses a login-only, non-owner role.

    This check is intentionally performed on the authenticated connection,
    rather than trusting only the username embedded in the DSN.  A custom
    superuser or table owner must not be able to replace the tenant-scoped
    runtime role at the production entry point.
    """

    async with pool.acquire() as connection, connection.transaction():
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
    reason = _runtime_role_contract_error(row, expected_role)
    if reason is not None:
        raise ValueError(reason)


def _independent_endpoints(source_url: str, target_dsn: str) -> bool:
    """Require source and target to be independently addressed services."""

    return _endpoint_host_port(source_url) != _endpoint_host_port(target_dsn)


def _operator_confirmation() -> dict[str, str]:
    """Require an explicit operator acknowledgement without storing secrets."""

    if os.getenv("TRPC_MIGRATION_PRODUCTION_CONFIRMATION") != _PRODUCTION_CONFIRMATION:
        raise ValueError(
            f"TRPC_MIGRATION_PRODUCTION_CONFIRMATION must equal {_PRODUCTION_CONFIRMATION!r}"
        )
    operator_id = os.getenv("TRPC_MIGRATION_OPERATOR_ID", "").strip()
    change_ticket = os.getenv("TRPC_MIGRATION_CHANGE_TICKET", "").strip()
    if not operator_id or not change_ticket:
        raise ValueError("TRPC_MIGRATION_OPERATOR_ID and TRPC_MIGRATION_CHANGE_TICKET are required")
    now = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    return {
        "status": "confirmed",
        "method": "cli_flag_and_environment_acknowledgement",
        "confirmed_at": now,
        "operator_id_sha256": hashlib.sha256(operator_id.encode("utf-8")).hexdigest(),
        "change_ticket_sha256": hashlib.sha256(change_ticket.encode("utf-8")).hexdigest(),
    }


def _image_digest() -> str:
    value = os.getenv("TRPC_MIGRATION_IMAGE_DIGEST", "").strip()
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None or value in {
        "sha256:" + "0" * 64,
        "sha256:" + "f" * 64,
    }:
        raise ValueError("TRPC_MIGRATION_IMAGE_DIGEST must be a non-placeholder sha256 digest")
    return value


def _require_release_binding() -> None:
    """Require production migration evidence to join one release bundle."""

    release_id = os.getenv("TRPC_RELEASE_ID", "").strip()
    release_nonce = os.getenv("TRPC_RELEASE_NONCE", "").strip()
    if (
        _PRODUCTION_RELEASE_ID_RE.fullmatch(release_id) is None
        or _PRODUCTION_RELEASE_NONCE_RE.fullmatch(release_nonce) is None
    ):
        raise ValueError(
            "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE are required and must be valid "
            "for production migration"
        )


def _utc_timestamp(value: datetime | None = None) -> str:
    timestamp = (value or datetime.now(UTC)).astimezone(UTC)
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _bounded_positive_int(value: Any, *, name: str, maximum: int) -> int:
    """Validate a finite, integral live-migration control value."""

    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    if isinstance(value, str):
        if not re.fullmatch(r"[0-9]+", value):
            raise ValueError(f"{name} must be a positive integer")
        number = int(value, 10)
    elif isinstance(value, int):
        number = value
    else:
        raise ValueError(f"{name} must be a positive integer")
    if number < 1 or number > maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return number


def _assert_safe_output_path(output: Path) -> None:
    if output.is_symlink() or any(
        parent.exists() and parent.is_symlink()
        for parent in (output.parent, *output.parent.parents)
    ):
        raise ValueError("migration report output must not use a symlink path")


def _atomic_write_json(output: Path, value: Mapping[str, Any]) -> None:
    """Write strict JSON through a same-directory temporary file and replace."""

    _assert_safe_output_path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    _assert_safe_output_path(output)
    rendered = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{output.name}.", suffix=".tmp", dir=str(output.parent)
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def _phase_record(
    *,
    tenant_id: str,
    migration_id: str,
    run_id: str,
    phase: MigrationPhase,
    result: Any,
    control_state: Mapping[str, Any],
    started_at: datetime,
    completed_at: datetime,
    source_snapshot_id: str,
    source_count: int,
    source_checksum: str,
) -> dict[str, Any]:
    """Build a self-contained phase record for the release validator.

    ``MigrationResult`` is intentionally a small state-machine return type.
    The production artifact adds the run/tenant binding and an observation
    interval here so a phase cannot be copied into another migration report.
    """

    case_deltas = dict(result.case_deltas)
    case_deltas.setdefault("target_checksum", case_deltas.get("checksum"))
    # Control-only phases do not rewrite data and therefore legitimately
    # return a zero/retained checkpoint count.  Keep that observed value for
    # auditability, while publishing the immutable migration snapshot totals
    # in the stable phase contract consumed by the release gate.
    if case_deltas.get("source_count") != source_count:
        case_deltas["observed_source_count"] = case_deltas.get("source_count")
        case_deltas["observed_target_count"] = case_deltas.get("target_count")
        case_deltas["observed_checksum"] = case_deltas.get("checksum")
        case_deltas["observed_target_checksum"] = case_deltas.get("target_checksum")
        case_deltas.update(
            {
                "source_count": source_count,
                "target_count": source_count,
                "checksum": source_checksum,
                "target_checksum": source_checksum,
            }
        )
    return {
        "tenant_id": tenant_id,
        "migration_id": migration_id,
        "run_id": run_id,
        "phase": phase.value,
        "source_snapshot_id": source_snapshot_id,
        "source_count": source_count,
        "source_checksum": source_checksum,
        "started_at": _utc_timestamp(started_at),
        "completed_at": _utc_timestamp(completed_at),
        "gate": result.gate,
        "case_deltas": case_deltas,
        "rejection_reasons": list(result.rejection_reasons),
        "control_state": dict(control_state),
    }


def _preflight_payload(value: Any | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not callable(getattr(value, "model_dump", None)):
        raise TypeError("migration target preflight must be a serializable model")
    payload = dict(value.model_dump(mode="json"))
    payload["empty"] = bool(getattr(value, "empty", not payload.get("non_empty_tables")))
    return payload


def _production_candidate(
    *,
    manifest: MigrationScopeManifest,
    phase_evidence: Mapping[str, Mapping[str, Any]],
    source_endpoint_sha256: str,
    target_endpoint_sha256: str,
    target_count: int,
    target_checksum: str,
    cleanup_state: Mapping[str, Any],
    rollback_state: Mapping[str, Any],
    operator_confirmation: Mapping[str, str],
    image_digest: str,
) -> dict[str, Any]:
    """Project live migration evidence into the release candidate contract."""

    phase_order = [phase.value for phase in (*_PRODUCTION_PHASES, MigrationPhase.ROLLBACK)]
    phases = {
        phase_name: {
            "status": "pass" if phase_evidence[phase_name].get("gate") == "pass" else "fail",
            "completed": phase_evidence[phase_name].get("gate") == "pass",
            "order": index,
        }
        for index, phase_name in enumerate(phase_order, start=1)
    }
    dual_write_state = phase_evidence[MigrationPhase.DUAL_WRITE.value].get("control_state")
    return {
        "mode": "real_redis_to_postgresql",
        "scope": "production",
        "tenant_id": manifest.tenant_id,
        "migration_id": manifest.migration_id,
        "is_simulation": False,
        "source": {
            "kind": MigrationSourceKind.REDIS.value,
            "is_real": True,
            "backend_id": "redis-source",
            "endpoint_identity": source_endpoint_sha256,
        },
        "target": {
            "kind": "postgresql",
            "is_real": True,
            "backend_id": "postgresql-target",
            "endpoint_identity": target_endpoint_sha256,
        },
        "phase_order": phase_order,
        "phases": phases,
        "verification": {
            "status": "pass",
            "source_count": manifest.source_count,
            "target_count": target_count,
            "source_checksum": manifest.source_checksum,
            "target_checksum": target_checksum,
            "differences": [],
        },
        "control": {
            "status": "pass",
            "tenant_id": manifest.tenant_id,
            "migration_id": manifest.migration_id,
            "tenant_scoped": all(
                phase.get("tenant_id") == manifest.tenant_id
                and phase.get("migration_id") == manifest.migration_id
                for phase in phase_evidence.values()
            ),
            "atomic_cutover": cleanup_state.get("atomic_cutover") is True,
            "dual_write_verified": (
                isinstance(dual_write_state, Mapping) and dual_write_state.get("dual_write") is True
            ),
            "cleanup_after_verify": cleanup_state.get("cleaned") is True,
            "rollback_verified": rollback_state.get("rollback_verified") is True,
        },
        "operator_attestation": {
            "status": "pass",
            "scope": "production",
            "operator_id": operator_confirmation["operator_id_sha256"],
            "attested_at": operator_confirmation["confirmed_at"],
            "source_target_reviewed": True,
            "checksums_reviewed": True,
            "control_reviewed": True,
        },
        "lineage": {"image_digest": image_digest},
    }


async def _load_production_control(
    spec: str, *, pool: asyncpg.Pool, tenant_id: str, migration_id: str
) -> Any:
    if spec not in _ALLOWED_PRODUCTION_FACTORIES:
        raise ValueError("TRPC_MIGRATION_CONTROL_FACTORY is not an approved production factory")
    import_spec = _PRODUCTION_FACTORY_ALIASES.get(spec, spec)
    module_name, separator, attribute = import_spec.partition(":")
    if not separator:
        module_name, separator, attribute = import_spec.rpartition(".")
    if not separator or not module_name or not attribute:
        raise ValueError("TRPC_MIGRATION_CONTROL_FACTORY must use module:callable syntax")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise TypeError("TRPC_MIGRATION_CONTROL_FACTORY is not callable")
    result = factory(pool=pool, tenant_id=tenant_id, migration_id=migration_id)
    if inspect.isawaitable(result):
        result = await result
    required = (
        "set_dual_write",
        "cutover",
        "cleanup",
        "rollback",
        "set_dual_write_fenced",
        "cutover_fenced",
        "cleanup_fenced",
        "rollback_fenced",
        "read_state",
    )
    if any(not callable(getattr(result, name, None)) for name in required):
        raise TypeError(
            "production migration control must expose unfenced and lease-fenced "
            "set_dual_write/cutover/cleanup/rollback hooks plus read_state"
        )
    return result


async def _control_state(control: Any, tenant_id: str, migration_id: str) -> dict[str, Any]:
    value = control.read_state(tenant_id, migration_id)
    if inspect.isawaitable(value):
        value = await value
    if not isinstance(value, Mapping):
        raise TypeError("production migration control read_state must return an object")
    # Keep control evidence JSON-safe and reject opaque objects instead of
    # serializing implementation details into a release artifact.
    state = dict(value)
    json.dumps(state, ensure_ascii=False, allow_nan=False)
    return state


def _require_control_state(
    state: Mapping[str, Any],
    *,
    dual_write: bool,
    active_profile: str,
    cleaned: bool,
    rolled_back: bool,
    mailbox_v2: str,
) -> dict[str, Any]:
    expected = {
        "dual_write": dual_write,
        "active_profile": active_profile,
        "cleaned": cleaned,
        "rolled_back": rolled_back,
        "mailbox_v2": mailbox_v2,
    }
    if any(key not in state for key in expected):
        raise ValueError("migration control state is missing required production fields")
    if any(state[key] != value for key, value in expected.items()):
        raise ValueError("migration control state does not match the expected phase")
    return {**state, "status": "pass"}


def _report(
    output: Path,
    *,
    gate: str,
    rejection_reasons: list[str],
    case_deltas: dict[str, Any] | None = None,
    production_gate: str | None = None,
    production_rejection_reasons: list[str] | None = None,
    migration_evidence: dict[str, Any] | None = None,
    production_candidate: dict[str, Any] | None = None,
    runtime: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    production_status = production_gate or ("fail" if gate == "fail" else "not_run")
    evidence = build_evidence(
        root=_ROOT,
        producer=_PRODUCER,
        run_id=run_id or new_run_id(_PRODUCER),
        generated_at=generated_at,
        runtime=runtime,
    )
    result = {
        "schema_version": 1,
        "baseline": "redis-source",
        "candidate": production_candidate or "postgresql-authoritative",
        "case_deltas": case_deltas or {},
        "gate": gate,
        "rejection_reasons": rejection_reasons,
        "production_gate": production_status,
        "production_rejection_reasons": (
            production_rejection_reasons
            if production_rejection_reasons is not None
            else rejection_reasons
            or [
                "live source/target phase passed, but deployment-owned dual-write, "
                "cutover, cleanup, and rollback were not executed"
            ]
        ),
        "run_id": evidence["run_id"],
        "evidence": evidence,
    }
    if migration_evidence is not None:
        migration = {
            **migration_evidence,
            "run_id": evidence["run_id"],
        }
        if migration.get("status") == "pass":
            lineage = dict(migration.get("lineage", {}))
            lineage.update(
                {
                    "status": "pass",
                    "checkout_current": True,
                    "producer": _PRODUCER,
                    "run_id": evidence["run_id"],
                    "source_fingerprint": evidence["source_fingerprint"],
                    "runtime_fingerprint": evidence["runtime_fingerprint"],
                }
            )
            migration["lineage"] = lineage
        result["migration_evidence"] = migration
    if production_candidate is not None:
        candidate = {
            **production_candidate,
            "run_id": evidence["run_id"],
        }
        lineage = dict(candidate.get("lineage", {}))
        lineage.update(
            {
                "status": "pass",
                "checkout_current": True,
                "producer": _PRODUCER,
                "run_id": evidence["run_id"],
                "source_fingerprint": evidence["source_fingerprint"],
                "runtime_fingerprint": evidence["runtime_fingerprint"],
            }
        )
        candidate["lineage"] = lineage
        result["candidate"] = candidate
    _atomic_write_json(output, result)
    return result


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    production_confirm = bool(getattr(args, "production_confirm", False))
    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if os.getenv("TRPC_RUN_REAL_MIGRATION") != "1":
        return _report(
            args.output,
            gate="not_run",
            rejection_reasons=[
                "TRPC_RUN_REAL_MIGRATION=1 was not supplied; live migration is opt-in"
            ],
        )
    if missing:
        return _report(
            args.output,
            gate="not_run",
            rejection_reasons=[
                f"missing required migration environment: {name}" for name in missing
            ],
        )

    source_url = os.environ["TRPC_MIGRATION_SOURCE_REDIS_URL"]
    target_dsn = os.environ["TRPC_MIGRATION_TARGET_DATABASE_DSN"].replace(
        "postgresql+asyncpg://", "postgresql://"
    )
    try:
        source_endpoint_sha256 = _endpoint_fingerprint(source_url)
        target_endpoint_sha256 = _endpoint_fingerprint(target_dsn)
        independent_endpoints = _independent_endpoints(source_url, target_dsn)
    except ValueError as error:
        return _report(args.output, gate="not_run", rejection_reasons=[str(error)])
    if not independent_endpoints:
        return _report(
            args.output,
            gate="not_run",
            rejection_reasons=[
                "source and target endpoints are identical; independent backends are required"
            ],
        )
    try:
        target_role = _require_runtime_target_role(target_dsn)
    except ValueError as error:
        return _report(args.output, gate="not_run", rejection_reasons=[str(error)])
    tenant_id = os.environ["TRPC_MIGRATION_TENANT_ID"]
    migration_id = os.environ["TRPC_MIGRATION_ID"]
    try:
        kinds = canonical_migration_kinds(
            item.strip()
            for item in os.getenv("TRPC_MIGRATION_KINDS", "session,memory").split(",")
            if item.strip()
        )
    except ValueError as error:
        return _report(
            args.output,
            gate="fail",
            rejection_reasons=[str(error)],
        )
    if production_confirm:
        if args.phase != MigrationPhase.PREPARE.value:
            return _report(
                args.output,
                gate="not_run",
                rejection_reasons=[
                    "--production-confirm runs the complete migration and must use "
                    "the default --phase prepare"
                ],
            )
        production_run_id = new_run_id(_PRODUCER)
        run_started_at = datetime.now(UTC)
        try:
            _require_release_binding()
            operator_confirmation = _operator_confirmation()
            image_digest = _image_digest()
        except ValueError as error:
            return _report(
                args.output,
                gate="not_run",
                rejection_reasons=[str(error)],
            )
        control_spec = os.getenv("TRPC_MIGRATION_CONTROL_FACTORY")
        if not control_spec:
            return _report(
                args.output,
                gate="not_run",
                rejection_reasons=[
                    "TRPC_MIGRATION_CONTROL_FACTORY is required for production migration"
                ],
            )
        if control_spec not in _ALLOWED_PRODUCTION_FACTORIES:
            return _report(
                args.output,
                gate="not_run",
                rejection_reasons=[
                    "TRPC_MIGRATION_CONTROL_FACTORY is not an approved production factory"
                ],
            )
    else:
        operator_confirmation = None
        control_spec = None
        image_digest = None
        production_run_id = None
        run_started_at = None

    def positive_env(name: str) -> int:
        return _bounded_positive_int(
            os.environ[name], name=name, maximum=MAX_MIGRATION_EXPECTED_RECORDS
        )

    try:
        batch_size = _bounded_positive_int(
            getattr(args, "batch_size", 500),
            name="--batch-size",
            maximum=MAX_MIGRATION_BATCH_SIZE,
        )
        db_pool_size = _bounded_positive_int(
            getattr(args, "db_pool_size", 4),
            name="--db-pool-size",
            maximum=MAX_MIGRATION_DB_POOL_SIZE,
        )
    except ValueError as error:
        return _report(args.output, gate="not_run", rejection_reasons=[str(error)])

    redis = redis_async.from_url(source_url, decode_responses=False)
    pool = await asyncpg.create_pool(target_dsn, min_size=1, max_size=db_pool_size)
    lease = None
    guard: PostgresMigrationGuard | None = None
    try:
        try:
            await _assert_runtime_role_contract(pool, target_role)
        except ValueError as error:
            return _report(args.output, gate="not_run", rejection_reasons=[str(error)])
        source = RedisMigrationSource(cast(Any, redis), kinds=kinds)
        source_snapshot = await source.snapshot(tenant_id)
        if source_snapshot.source_count < 1:
            raise ValueError("live migration source snapshot is empty")
        manifest = MigrationScopeManifest(
            tenant_id=tenant_id,
            migration_id=migration_id,
            source_kind=MigrationSourceKind.REDIS,
            kinds=kinds,
            source_snapshot_id=source_snapshot.source_snapshot_id,
            source_count=source_snapshot.source_count,
            source_checksum=source_snapshot.source_checksum,
            app_id=os.environ["TRPC_MIGRATION_APP_ID"],
            app_revision=positive_env("TRPC_MIGRATION_APP_REVISION"),
            config_version=positive_env("TRPC_MIGRATION_CONFIG_VERSION"),
            binding_id=os.environ["TRPC_MIGRATION_BINDING_ID"],
            binding_revision=positive_env("TRPC_MIGRATION_BINDING_REVISION"),
        )
        guard = PostgresMigrationGuard(pool)
        target_preflight: Any | None = None
        owner_id = os.environ["TRPC_MIGRATION_OWNER_ID"]
        if args.phase == MigrationPhase.PREPARE.value or production_confirm:
            lease, target_preflight = await guard.acquire_with_target_preflight(manifest, owner_id)
        else:
            lease = await guard.acquire(manifest, owner_id)
        checkpoints = PostgresMigrationCheckpointStore(pool)
        if production_confirm:
            control = await _load_production_control(
                cast(str, control_spec),
                pool=pool,
                tenant_id=tenant_id,
                migration_id=migration_id,
            )
            target = PostgresMigrationTarget(pool, control=control, manifest=manifest)
            coordinator = MigrationCoordinator(
                source,
                target,
                checkpoints,
                batch_size=batch_size,
                guard=guard,
                lease=lease,
                manifest=manifest,
            )
            phase_evidence: dict[str, dict[str, Any]] = {}
            expected_control = {
                MigrationPhase.PREPARE: {
                    "dual_write": False,
                    "active_profile": "source",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "ready",
                },
                MigrationPhase.BACKFILL: {
                    "dual_write": False,
                    "active_profile": "source",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "ready",
                },
                MigrationPhase.SHADOW_READ: {
                    "dual_write": False,
                    "active_profile": "source",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "ready",
                },
                MigrationPhase.DUAL_WRITE: {
                    "dual_write": True,
                    "active_profile": "source",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "dual-write",
                },
                MigrationPhase.CUTOVER: {
                    "dual_write": True,
                    "active_profile": "target",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "target",
                },
                MigrationPhase.VERIFY: {
                    "dual_write": True,
                    "active_profile": "target",
                    "cleaned": False,
                    "rolled_back": False,
                    "mailbox_v2": "target",
                },
                MigrationPhase.CLEANUP: {
                    "dual_write": False,
                    "active_profile": "target",
                    "cleaned": True,
                    "rolled_back": False,
                    "mailbox_v2": "target",
                },
                MigrationPhase.ROLLBACK: {
                    "dual_write": False,
                    "active_profile": "source",
                    "cleaned": False,
                    "rolled_back": True,
                    "mailbox_v2": "source",
                },
            }
            for phase in _PRODUCTION_PHASES:
                phase_started_at = datetime.now(UTC)
                phase_result = await coordinator.run(tenant_id, migration_id, phase)
                phase_completed_at = datetime.now(UTC)
                if phase_result.gate != "pass":
                    raise RuntimeError(f"production migration phase {phase.value} did not pass")
                expected = expected_control[phase]
                state = _require_control_state(
                    await _control_state(control, tenant_id, migration_id),
                    dual_write=bool(expected["dual_write"]),
                    active_profile=str(expected["active_profile"]),
                    cleaned=bool(expected["cleaned"]),
                    rolled_back=bool(expected["rolled_back"]),
                    mailbox_v2=str(expected["mailbox_v2"]),
                )
                phase_evidence[phase.value] = _phase_record(
                    tenant_id=tenant_id,
                    migration_id=migration_id,
                    run_id=cast(str, production_run_id),
                    phase=phase,
                    result=phase_result,
                    control_state=state,
                    started_at=phase_started_at,
                    completed_at=phase_completed_at,
                    source_snapshot_id=manifest.source_snapshot_id,
                    source_count=manifest.source_count,
                    source_checksum=manifest.source_checksum,
                )
            final_state = phase_evidence[MigrationPhase.CLEANUP.value]["control_state"]
            if final_state.get("rollback_verified") is not True:
                raise RuntimeError(
                    "production migration control must attest rollback_verified after cleanup"
                )
            if final_state.get("atomic_cutover") is not True:
                raise RuntimeError(
                    "production migration control must attest atomic_cutover after cleanup"
                )
            if final_state.get("cleaned") is not True:
                raise RuntimeError("production migration control must attest cleanup after verify")
            # Rollback is deliberately executed and observed after cleanup. A
            # capability flag alone is not sufficient evidence: this phase
            # must have its own completed checkpoint and control state.
            rollback_started_at = datetime.now(UTC)
            rollback_result = await coordinator.run(
                tenant_id, migration_id, MigrationPhase.ROLLBACK
            )
            rollback_completed_at = datetime.now(UTC)
            if rollback_result.gate != "pass":
                raise RuntimeError("production migration rollback phase did not pass")
            rollback_state = _require_control_state(
                await _control_state(control, tenant_id, migration_id),
                dual_write=False,
                active_profile="source",
                cleaned=False,
                rolled_back=True,
                mailbox_v2="source",
            )
            if rollback_state.get("rollback_verified") is not True:
                raise RuntimeError("production migration rollback was not observed")
            phase_evidence[MigrationPhase.ROLLBACK.value] = _phase_record(
                tenant_id=tenant_id,
                migration_id=migration_id,
                run_id=cast(str, production_run_id),
                phase=MigrationPhase.ROLLBACK,
                result=rollback_result,
                control_state=rollback_state,
                started_at=rollback_started_at,
                completed_at=rollback_completed_at,
                source_snapshot_id=manifest.source_snapshot_id,
                source_count=manifest.source_count,
                source_checksum=manifest.source_checksum,
            )
            verify_deltas = cast(
                Mapping[str, Any],
                phase_evidence[MigrationPhase.VERIFY.value]["case_deltas"],
            )
            target_count = int(verify_deltas["target_count"])
            target_checksum = str(verify_deltas["target_checksum"])
            run_finished_at = datetime.now(UTC)
            migration_evidence = {
                "status": "pass",
                "scope": "production",
                "is_simulation": False,
                "run_started_at": _utc_timestamp(cast(datetime, run_started_at)),
                "run_finished_at": _utc_timestamp(run_finished_at),
                "source": {
                    "kind": MigrationSourceKind.REDIS.value,
                    "is_real": True,
                    "endpoint_sha256": source_endpoint_sha256,
                    "snapshot_id": manifest.source_snapshot_id,
                    "source_count": manifest.source_count,
                    "source_checksum": manifest.source_checksum,
                },
                "target": {
                    "kind": "postgresql",
                    "is_real": True,
                    "endpoint_sha256": target_endpoint_sha256,
                    "target_count": target_count,
                    "target_checksum": target_checksum,
                },
                "target_empty_preflight": _preflight_payload(target_preflight),
                "manifest": manifest.model_dump(mode="json"),
                "phases": phase_evidence,
                "control": {
                    "factory": cast(str, control_spec),
                    "phase_count": len(phase_evidence),
                    "complete": set(phase_evidence)
                    == {phase.value for phase in (*_PRODUCTION_PHASES, MigrationPhase.ROLLBACK)},
                    "rollback_supported": rollback_state.get("rollback_verified") is True,
                    "rollback_observed": True,
                },
                "operator_confirmation": operator_confirmation,
                "lineage": {"image_digest": image_digest},
            }
            runtime = runtime_fingerprint(
                mode="real_migration",
                worker_identities=[os.environ["TRPC_MIGRATION_OWNER_ID"]],
                stream=source_url,
                group=migration_id,
                parameters={"phase_count": len(phase_evidence), "kinds": list(kinds)},
            )
            # The guard releases the persistent write barrier and lease in one
            # transaction.  Do not release the target barrier separately.
            await guard.release(lease)
            lease = None
            return _report(
                args.output,
                gate="pass",
                rejection_reasons=[],
                case_deltas={
                    "phase": "complete",
                    "phase_count": len(phase_evidence),
                    "source_count": manifest.source_count,
                    "source_checksum": manifest.source_checksum,
                    "target_count": target_count,
                    "target_checksum": target_checksum,
                    "target_empty_preflight": _preflight_payload(target_preflight),
                },
                production_gate="pass",
                production_rejection_reasons=[],
                migration_evidence=migration_evidence,
                production_candidate=_production_candidate(
                    manifest=manifest,
                    phase_evidence=phase_evidence,
                    source_endpoint_sha256=source_endpoint_sha256,
                    target_endpoint_sha256=target_endpoint_sha256,
                    target_count=target_count,
                    target_checksum=target_checksum,
                    cleanup_state=cast(Mapping[str, Any], final_state),
                    rollback_state=rollback_state,
                    operator_confirmation=cast(Mapping[str, str], operator_confirmation),
                    image_digest=cast(str, image_digest),
                ),
                runtime=runtime,
                run_id=cast(str, production_run_id),
                generated_at=run_finished_at,
            )

        target = PostgresMigrationTarget(pool, manifest=manifest)
        result = await MigrationCoordinator(
            source,
            target,
            checkpoints,
            batch_size=batch_size,
            guard=guard,
            lease=lease,
            manifest=manifest,
        ).run(tenant_id, migration_id, MigrationPhase(args.phase))
        if lease is not None and guard is not None:
            if MigrationPhase(args.phase) in {MigrationPhase.CLEANUP, MigrationPhase.ROLLBACK}:
                # The guard releases the persistent write barrier and lease in
                # one transaction.  Do not release the target barrier separately.
                await guard.release(lease)
            else:
                _LOGGER.warning(
                    "migration write barrier retained after non-terminal phase; "
                    "resume the migration before normal tenant writes are enabled"
                )
            lease = None
        return _report(
            args.output,
            gate=result.gate,
            rejection_reasons=list(result.rejection_reasons),
            case_deltas=result.case_deltas,
            production_gate="not_run",
            production_rejection_reasons=[
                f"live {args.phase} passed, but deployment-owned dual-write, cutover, "
                "cleanup, and rollback were not executed"
            ],
        )
    except Exception as error:
        return _report(
            args.output,
            gate="fail",
            rejection_reasons=[f"live migration raised {type(error).__name__}"],
        )
    finally:
        if lease is not None and guard is not None:
            # A failed or interrupted migration deliberately keeps the barrier
            # active.  Releasing it here would allow normal writers to enter a
            # partially migrated target; the next operator run can renew or
            # explicitly recover the fenced lease.
            _LOGGER.warning("migration write barrier retained after unsuccessful run")
        await redis.aclose()
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase", choices=[phase.value for phase in MigrationPhase], default="prepare"
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--db-pool-size", type=int, default=4)
    parser.add_argument(
        "--production-confirm",
        action="store_true",
        help=(
            "run all phases against explicit live source/target and emit production evidence; "
            "requires operator acknowledgement and a deployment-owned control factory"
        ),
    )
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/migration-live.json"))
    args = parser.parse_args()
    result = asyncio.run(_run(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
