#!/usr/bin/env python3
# ruff: noqa: E402
"""Create and remove an isolated synthetic tenant for the performance gate.

The fixture is deliberately a separate control-plane utility.  It never falls
back to the service or real-runtime DSN, and it does not contact Feishu.  A
live create or cleanup requires all of the following:

* ``--execute``;
* ``TRPC_RUN_REAL_MULTINODE=1``; and
* ``TRPC_PERF_FIXTURE_CONFIRM=I_UNDERSTAND_PERFORMANCE_FIXTURE``.

The default DSN policy only permits loopback PostgreSQL.  A remote DSN also
requires ``--allow-remote`` and the exact
``TRPC_PERF_FIXTURE_REMOTE_CONFIRM=I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE``
environment value.  Reports contain only synthetic resource identifiers and
the gate state; they never contain a DSN, secret reference, or message body.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg

_REPO_IMPORT_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_IMPORT_ROOT))

from trpc_service.config.secrets import SecretRef
from trpc_service.tenant.control import PostgresControlPlaneRepository
from trpc_service.tenant.models import Channel, ChannelBinding

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = ROOT / "runs" / "multitenant" / "performance-fixture-not-run.json"
OPT_IN_ENV = "TRPC_RUN_REAL_MULTINODE"
CONFIRM_ENV = "TRPC_PERF_FIXTURE_CONFIRM"
CONFIRM_VALUE = "I_UNDERSTAND_PERFORMANCE_FIXTURE"
REMOTE_CONFIRM_ENV = "TRPC_PERF_FIXTURE_REMOTE_CONFIRM"
REMOTE_CONFIRM_VALUE = "I_UNDERSTAND_REMOTE_PERFORMANCE_FIXTURE"
DATABASE_ENV = "TRPC_PERF_DATABASE_DSN"
PERFECT_TENANT_PREFIX = "perf-"
REPORT_SCHEMA_VERSION = 1
IDENTITY_KEYS = (
    "run_id",
    "tenant_id",
    "binding_id",
    "app_id",
    "account_id",
    "config_version",
    "channel",
)

# This is intentionally a literal, reviewed list.  Cleanup must never use a
# schema-wide operation or a caller-provided table name.
CLEANUP_TABLES: tuple[str, ...] = (
    "session_mailbox_items",
    "delivery_attempts",
    "outbound_messages",
    "turn_intents",
    "session_events",
    "session_summaries",
    "tool_executions",
    "session_turns",
    "inbound_messages",
    "channel_identities",
    "im_acceptance_evidence_events",
    "wecom_connection_state",
    "channel_bindings",
    "memories",
    "artifacts",
    "knowledge_embeddings",
    "knowledge_items",
    "outbox_events",
    "dead_letters",
    "confirmation_challenges",
    "tenant_budget_usage",
    "audit_logs",
    "migration_checkpoints",
    "migration_leases",
    "migration_write_barriers",
    "migration_scope_manifests",
    "fault_stage_controls",
    "admin_idempotency",
    "config_revisions",
    "storage_profiles",
    "tenant_policies",
    "session_mailboxes",
    "sessions",
    "agent_apps",
    "tenants",
)
_PRE_WRITE_BARRIER_CLEANUP_TABLES = tuple(
    table for table in CLEANUP_TABLES if table != "migration_write_barriers"
)
_LEGACY_CLEANUP_TABLES_V1 = tuple(
    table
    for table in _PRE_WRITE_BARRIER_CLEANUP_TABLES
    if table
    not in {
        "session_mailbox_items",
        "session_mailboxes",
        "wecom_connection_state",
        "im_acceptance_evidence_events",
    }
)

_SUFFIX = re.compile(r"^[0-9a-f]{32}$")
_POSTGRES_SCHEMES = {"postgres", "postgresql", "postgresql+asyncpg"}
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


class FixtureValidationError(ValueError):
    """Safe validation failure; callers must report only its type."""


class FixtureCreateError(RuntimeError):
    """Creation failed after the tenant ownership proof was established."""

    def __init__(self, partial_report: dict[str, Any] | None) -> None:
        super().__init__("performance fixture creation failed")
        self.partial_report = partial_report


class PoolFactory(Protocol):
    def __call__(self, dsn: str, **kwargs: Any) -> Awaitable[Any]: ...


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("create", "cleanup"):
        command = subparsers.add_parser(action)
        command.add_argument(
            "--execute",
            action="store_true",
            help="required together with the explicit performance opt-in environment values",
        )
        command.add_argument(
            "--output",
            type=Path,
            default=DEFAULT_REPORT,
            help="JSON report path (create output; cleanup result is written here too)",
        )
        command.add_argument(
            "--allow-remote",
            action="store_true",
            help="allow a non-loopback PostgreSQL host only with the exact remote confirmation",
        )
        if action == "cleanup":
            command.add_argument("--report", type=Path, required=True)
            command.add_argument("--tenant-id", required=True)
            command.add_argument("--run-id", required=True)
    return parser


def _connection_dsn(raw: str) -> str:
    return raw.replace("postgresql+asyncpg://", "postgresql://", 1)


def _dsn_host(raw: str) -> str:
    try:
        parsed = urlsplit(raw)
        if parsed.scheme not in _POSTGRES_SCHEMES or not parsed.hostname:
            raise FixtureValidationError("dedicated performance DSN must be a PostgreSQL URL")
        return parsed.hostname.lower().rstrip(".")
    except ValueError as exc:
        raise FixtureValidationError("dedicated performance DSN is invalid") from exc


def _opt_in_reasons(args: argparse.Namespace, env: Mapping[str, str]) -> list[str]:
    reasons: list[str] = []
    if not bool(getattr(args, "execute", False)):
        reasons.append("--execute is required")
    if env.get(OPT_IN_ENV) != "1":
        reasons.append(f"{OPT_IN_ENV}=1 is required")
    if env.get(CONFIRM_ENV) != CONFIRM_VALUE:
        reasons.append(f"{CONFIRM_ENV} exact confirmation is required")
    dsn = env.get(DATABASE_ENV)
    if not dsn:
        reasons.append(f"{DATABASE_ENV} is required; no fallback DSN is allowed")
        return reasons
    try:
        host = _dsn_host(dsn)
    except FixtureValidationError:
        reasons.append("dedicated performance DSN is not a valid PostgreSQL URL")
        return reasons
    if host not in _LOOPBACK_HOSTS:
        if not bool(getattr(args, "allow_remote", False)):
            reasons.append("--allow-remote is required for a non-loopback DSN")
        if env.get(REMOTE_CONFIRM_ENV) != REMOTE_CONFIRM_VALUE:
            reasons.append(f"{REMOTE_CONFIRM_ENV} exact confirmation is required")
    return reasons


def _manifest_checksum(values: Mapping[str, Any]) -> str:
    try:
        identity = {key: values[key] for key in IDENTITY_KEYS}
    except KeyError as exc:
        raise FixtureValidationError("fixture report identity is incomplete") from exc
    rendered = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _safe_report_path(path: Path) -> Path:
    candidate = path.expanduser()
    if candidate.is_symlink() or any(parent.is_symlink() for parent in candidate.parents):
        raise FixtureValidationError("report path must not be a symlink")
    if candidate.name in {"", ".", ".."}:
        raise FixtureValidationError("report path is invalid")
    return candidate.resolve()


def _write_report(path: Path, report: Mapping[str, Any]) -> None:
    target = _safe_report_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _base_report(*, gate: str, run_id: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "kind": "performance_fixture",
        "gate": gate,
        "production_gate": "not_run",
        "run_id": run_id,
    }


def build_not_run_report(reasons: list[str]) -> dict[str, Any]:
    report = _base_report(gate="not_run")
    report["rejection_reasons"] = list(reasons)
    report["production_rejection_reasons"] = [
        "fixture creation or cleanup was not explicitly authorized"
    ]
    return report


def _ids(suffix: str) -> dict[str, Any]:
    if not _SUFFIX.fullmatch(suffix):
        raise FixtureValidationError("fixture suffix is invalid")
    return {
        "run_id": f"perf-fixture-{suffix}",
        "tenant_id": f"{PERFECT_TENANT_PREFIX}{suffix}",
        "binding_id": f"perf-binding-{suffix}",
        "app_id": "perf-agent",
        "account_id": f"perf-account-{suffix}",
        "config_version": 1,
        "channel": Channel.FEISHU.value,
    }


def _request_hash(operation: str, run_id: str) -> str:
    return hashlib.sha256(f"performance-fixture:{operation}:{run_id}".encode()).hexdigest()


def _tenant_request_hash(ids: Mapping[str, Any]) -> str:
    """Bind the tenant idempotency record to the exact fixture manifest."""
    return f"{_request_hash('tenant', str(ids['run_id']))}:{_manifest_checksum(ids)}"


def _tenant_idempotency_key(run_id: str, manifest_checksum: str) -> str:
    """Use the checksum in the durable audit identity, not only in the report."""
    return f"{run_id}:tenant:{manifest_checksum}"


def _synthetic_binding(ids: Mapping[str, Any]) -> ChannelBinding:
    # These are references to intentionally absent variables.  No provider is
    # resolved by this fixture, and the binding is never sent to Feishu.
    refs = {
        name: SecretRef(uri=f"env://TRPC_PERF_FIXTURE_UNUSED_{name.upper()}")
        for name in ("app_secret", "verification_token", "encrypt_key")
    }
    return ChannelBinding(
        binding_id=str(ids["binding_id"]),
        tenant_id=str(ids["tenant_id"]),
        app_id=str(ids["app_id"]),
        channel=Channel.FEISHU,
        account_id=str(ids["account_id"]),
        secret_refs=refs,
        capabilities=frozenset({"text"}),
        enabled=True,
    )


def _synthetic_config_payload() -> dict[str, Any]:
    return {
        "model": {"provider": "offline", "model": "deterministic"},
        "storage": {"profile_id": "default"},
        "instructions": "Synthetic performance fixture; never contact a real IM provider.",
        "policy_version": 1,
    }


def _fixture_report(ids: Mapping[str, Any], report_path: Path) -> dict[str, Any]:
    report = _base_report(gate="pass", run_id=str(ids["run_id"]))
    report.update(
        {
            key: ids[key]
            for key in (
                "tenant_id",
                "binding_id",
                "app_id",
                "account_id",
                "config_version",
                "channel",
            )
        }
    )
    report["synthetic"] = True
    report["report_path"] = str(_safe_report_path(report_path))
    report["manifest_checksum"] = _manifest_checksum(report)
    report["cleanup_tables"] = list(CLEANUP_TABLES)
    report["production_rejection_reasons"] = [
        "synthetic fixture setup is not real IM or production acceptance evidence"
    ]
    return report


def _partial_fixture_report(
    ids: Mapping[str, Any], report_path: Path, error: BaseException
) -> dict[str, Any]:
    """Create a cleanup-capable report only after tenant creation succeeded."""
    report = _base_report(gate="partial", run_id=str(ids["run_id"]))
    report.update(
        {
            key: ids[key]
            for key in (
                "tenant_id",
                "binding_id",
                "app_id",
                "account_id",
                "config_version",
                "channel",
            )
        }
    )
    report.update(
        {
            "synthetic": True,
            "cleanup_ready": True,
            "report_path": str(_safe_report_path(report_path)),
            "cleanup_tables": list(CLEANUP_TABLES),
            "error_type": type(error).__name__,
            "production_rejection_reasons": [
                "fixture creation was partial; cleanup is limited to the owned synthetic tenant"
            ],
        }
    )
    report["manifest_checksum"] = _manifest_checksum(report)
    return report


async def _create_fixture(
    *,
    pool_factory: PoolFactory,
    dsn: str,
    report_path: Path,
    suffix: str | None = None,
    repository_factory: Callable[[Any], Any] = PostgresControlPlaneRepository,
) -> dict[str, Any]:
    suffix = suffix or uuid4().hex
    ids = _ids(suffix)
    pool = await pool_factory(_connection_dsn(dsn), min_size=1, max_size=2)
    tenant_created = False
    try:
        repository = repository_factory(pool)
        tenant = await repository.create_tenant(
            tenant_id=ids["tenant_id"],
            display_name="Synthetic performance fixture",
            actor="performance-fixture",
            idempotency_key=_tenant_idempotency_key(str(ids["run_id"]), _manifest_checksum(ids)),
            request_hash=_tenant_request_hash(ids),
        )
        tenant_created = True
        revision = await repository.create_config_revision(
            tenant_id=ids["tenant_id"],
            app_id=ids["app_id"],
            config=_synthetic_config_payload(),
            actor="performance-fixture",
            expected_version=int(tenant["control_version"]),
            idempotency_key=f"{ids['run_id']}:config",
            request_hash=_request_hash("config", str(ids["run_id"])),
        )
        if int(revision["version"]) != int(ids["config_version"]):
            raise FixtureValidationError("synthetic config revision is not version one")
        activated = await repository.activate_config(
            tenant_id=ids["tenant_id"],
            app_id=ids["app_id"],
            version=int(ids["config_version"]),
            percentage=100,
            actor="performance-fixture",
            expected_version=int(revision["tenant_control_version"]),
            idempotency_key=f"{ids['run_id']}:activate",
            request_hash=_request_hash("activate", str(ids["run_id"])),
        )
        binding = await repository.put_binding(
            tenant_id=ids["tenant_id"],
            binding_id=ids["binding_id"],
            binding=_synthetic_binding(ids),
            actor="performance-fixture",
            expected_version=int(activated["tenant_control_version"]),
            idempotency_key=f"{ids['run_id']}:binding",
            request_hash=_request_hash("binding", str(ids["run_id"])),
        )
        if not bool(binding.get("enabled", True)):
            raise FixtureValidationError("synthetic binding is disabled")
        return _fixture_report(ids, report_path)
    except Exception as error:
        # Do not issue a guessed delete.  Once create_tenant committed, emit a
        # signed-by-checksum partial report so an operator can perform the same
        # exact, tenant-owned cleanup even if a later control-plane operation
        # failed.  Before that point there is no tenant this fixture owns.
        partial_report = (
            _partial_fixture_report(ids, report_path, error) if tenant_created else None
        )
        raise FixtureCreateError(partial_report) from error
    finally:
        await pool.close()


def _validate_report(
    report_path: Path,
    report: Mapping[str, Any],
    *,
    tenant_id: str,
    run_id: str,
) -> dict[str, Any]:
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise FixtureValidationError("fixture report schema is unsupported")
    if report.get("kind") != "performance_fixture" or report.get("gate") not in {
        "pass",
        "partial",
    }:
        raise FixtureValidationError("fixture report is not a successful fixture report")
    if report.get("production_gate") != "not_run" or report.get("synthetic") is not True:
        raise FixtureValidationError("fixture report is not synthetic")
    if report.get("gate") == "partial" and report.get("cleanup_ready") is not True:
        raise FixtureValidationError("partial fixture report is not cleanup-ready")
    if report.get("tenant_id") != tenant_id or report.get("run_id") != run_id:
        raise FixtureValidationError("cleanup identity does not match the fixture report")
    suffix = tenant_id.removeprefix(PERFECT_TENANT_PREFIX)
    if not _SUFFIX.fullmatch(suffix):
        raise FixtureValidationError("cleanup tenant must use the perf- prefix")
    expected = _ids(suffix)
    for key in IDENTITY_KEYS:
        if report.get(key) != expected[key]:
            raise FixtureValidationError("fixture report identity is inconsistent")
    if report.get("manifest_checksum") != _manifest_checksum(report):
        raise FixtureValidationError("fixture report integrity check failed")
    recorded_cleanup_tables = tuple(report.get("cleanup_tables", ()))
    if recorded_cleanup_tables not in {
        CLEANUP_TABLES,
        _PRE_WRITE_BARRIER_CLEANUP_TABLES,
        _LEGACY_CLEANUP_TABLES_V1,
    }:
        raise FixtureValidationError("fixture report cleanup allowlist is inconsistent")
    recorded_path = report.get("report_path")
    requested_path = _safe_report_path(report_path)
    if (
        not isinstance(recorded_path, str)
        or _safe_report_path(Path(recorded_path)) != requested_path
    ):
        raise FixtureValidationError("fixture report path does not match the requested report")
    return dict(report)


async def _delete_fixture_rows(
    *, pool: Any, tenant_id: str, run_id: str, manifest_checksum: str
) -> dict[str, int]:
    if not tenant_id.startswith(PERFECT_TENANT_PREFIX):
        raise FixtureValidationError("cleanup tenant must use the perf- prefix")
    counts: dict[str, int] = {}
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        owned = await connection.fetchval(
            """
            SELECT 1
              FROM tenants AS tenant
              JOIN audit_logs AS audit ON audit.tenant_id=tenant.tenant_id
             WHERE tenant.tenant_id=$1
               AND tenant.display_name=$2
               AND audit.user_id=$3
               AND audit.decision=$4
               AND audit.idempotency_key=$5
               AND audit.trace_id=$6
            """,
            tenant_id,
            "Synthetic performance fixture",
            "performance-fixture",
            "tenant_created",
            _tenant_idempotency_key(run_id, manifest_checksum),
            f"admin:{_tenant_idempotency_key(run_id, manifest_checksum)}",
        )
        if owned != 1:
            raise FixtureValidationError("fixture ownership proof is missing")
        raw_counts = await connection.fetchval(
            "SELECT public.cleanup_performance_fixture($1, $2, $3)",
            tenant_id,
            run_id,
            manifest_checksum,
        )
        if isinstance(raw_counts, str):
            try:
                parsed_counts = json.loads(raw_counts)
            except json.JSONDecodeError as error:
                raise FixtureValidationError("fixture cleanup result is invalid") from error
        else:
            parsed_counts = raw_counts
        if not isinstance(parsed_counts, Mapping) or set(parsed_counts) != set(CLEANUP_TABLES):
            raise FixtureValidationError("fixture cleanup result is incomplete")
        for table in CLEANUP_TABLES:
            value = parsed_counts.get(table)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise FixtureValidationError("fixture cleanup result is invalid")
            counts[table] = value
    return counts


async def _cleanup_fixture(
    *,
    pool_factory: PoolFactory,
    dsn: str,
    report_path: Path,
    tenant_id: str,
    run_id: str,
) -> dict[str, Any]:
    try:
        raw = _safe_report_path(report_path).read_text(encoding="utf-8")
        if len(raw.encode("utf-8")) > 1_048_576:
            raise FixtureValidationError("fixture report is too large")
        parsed = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FixtureValidationError("fixture report cannot be read") from exc
    if not isinstance(parsed, Mapping):
        raise FixtureValidationError("fixture report must be a JSON object")
    report = _validate_report(report_path, parsed, tenant_id=tenant_id, run_id=run_id)
    pool = await pool_factory(_connection_dsn(dsn), min_size=1, max_size=2)
    try:
        counts = await _delete_fixture_rows(
            pool=pool,
            tenant_id=tenant_id,
            run_id=run_id,
            manifest_checksum=str(report["manifest_checksum"]),
        )
    finally:
        await pool.close()
    result = _base_report(gate="pass", run_id=run_id)
    result.update(
        {
            "tenant_id": tenant_id,
            "binding_id": report["binding_id"],
            "app_id": report["app_id"],
            "account_id": report["account_id"],
            "config_version": report["config_version"],
            "channel": report["channel"],
            "report_path": str(_safe_report_path(report_path)),
            "deleted_rows": counts,
            "cleanup_tables": list(CLEANUP_TABLES),
        }
    )
    result["manifest_checksum"] = _manifest_checksum(result)
    result["production_rejection_reasons"] = [
        "fixture cleanup is not production acceptance evidence"
    ]
    return result


def _error_report(*, action: str, error: BaseException) -> dict[str, Any]:
    report = _base_report(gate="fail")
    report.update(
        {
            "action": action,
            "error_type": type(error).__name__,
            "production_rejection_reasons": ["fixture operation failed"],
        }
    )
    return report


def _load_env() -> Mapping[str, str]:
    # Copying also makes tests unable to mutate the process environment through
    # an injected mapping.
    return dict(os.environ)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    env = _load_env()
    output = args.output
    reasons = _opt_in_reasons(args, env)
    if reasons:
        report = build_not_run_report(reasons)
        _write_report(output, report)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    dsn = env[DATABASE_ENV]
    try:
        if args.action == "create":
            suffix = uuid4().hex
            report = asyncio.run(
                _create_fixture(
                    pool_factory=asyncpg.create_pool,
                    dsn=dsn,
                    report_path=output,
                    suffix=suffix,
                )
            )
        else:
            report = asyncio.run(
                _cleanup_fixture(
                    pool_factory=asyncpg.create_pool,
                    dsn=dsn,
                    report_path=args.report,
                    tenant_id=args.tenant_id,
                    run_id=args.run_id,
                )
            )
        _write_report(output, report)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except Exception as error:
        report = (
            error.partial_report
            if isinstance(error, FixtureCreateError) and error.partial_report is not None
            else _error_report(action=args.action, error=error)
        )
        _write_report(output, report)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
