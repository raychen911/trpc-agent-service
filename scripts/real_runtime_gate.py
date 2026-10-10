#!/usr/bin/env python3
"""Run an explicitly opt-in, real multi-process Compose acceptance.

This is intentionally separate from the deterministic simulation gate.  It
only reports a real result after it has observed independent worker
containers, durable PostgreSQL acceptance, Redis Streams delivery, and the
requested fault/recovery evidence.  Without ``TRPC_RUN_REAL_MULTINODE=1`` and
``--execute`` it performs no Docker or network operation and writes
``gate=not_run``.

The command is safe with respect to existing Compose containers and volumes:
``--compose-up`` refuses a project that already has containers, and a project
created by this invocation is cleaned only with ``down --remove-orphans``;
``down --volumes`` is never called.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote, urlsplit
from uuid import UUID, uuid4

import asyncpg
import httpx
import redis.asyncio as redis_async
from redis.exceptions import RedisError

if __package__ in {None, ""}:
    # Keep the documented ``python scripts/real_runtime_gate.py`` invocation
    # equivalent to ``python -m scripts.real_runtime_gate``.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    build_evidence,
    canonical_sha256,
    current_release_binding,
    runtime_fingerprint,
    source_fingerprint,
    validate_release_binding,
)
from scripts.report_io import atomic_write_json
from trpc_service.channels.envelopes import InboundEnvelope, OutboundEnvelope, PayloadKind
from trpc_service.config.settings import (
    RUNTIME_DATABASE_ROLE,
    WORKER_DATABASE_ROLE,
    SchedulerVersion,
)
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import Acceptance, SessionLease, TurnCommit
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import Channel, ConversationKind

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COMPOSE_FILE = ROOT / "docker-compose.yml"
DEFAULT_ACCEPTANCE_OVERRIDE = ROOT / "deploy" / "acceptance-runtime.override.yml"
DEFAULT_TOXIPROXY_OVERRIDE = ROOT / "deploy" / "toxiproxy-runtime.override.yml"
DEFAULT_REDIS_STREAM = "trpc:session-ready:v2"
DEFAULT_REDIS_GROUP = "trpc-session-ready-v2"
TOXIPROXY_EXPECTED = {
    "postgres": {"listen": "0.0.0.0:15432", "upstream": "postgres:5432"},
    "redis": {"listen": "0.0.0.0:16379", "upstream": "redis:6379"},
    "minio": {"listen": "0.0.0.0:19000", "upstream": "minio:9000"},
}
OPT_IN = "TRPC_RUN_REAL_MULTINODE"
GLOBAL_WORKER_DATABASE_DSN_ENV = "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN"
GLOBAL_WORKER_DATABASE_ROLE_ENV = "TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE"
RUNTIME_DATABASE_ROLE_ENV = "TRPC_REAL_RUNTIME_DATABASE_ROLE"
REQUIRED_ENV = (
    "TRPC_REAL_DATABASE_DSN",
    "TRPC_REAL_REDIS_URL",
    "TRPC_REAL_TENANT_ID",
    "TRPC_REAL_BINDING_ID",
    "TRPC_REAL_SESSION_HMAC_KEY",
    GLOBAL_WORKER_DATABASE_DSN_ENV,
    GLOBAL_WORKER_DATABASE_ROLE_ENV,
)

# ``resolve_channel_binding`` is deliberately kept separate: gateway/runtime
# code uses it for one binding selected by an authenticated tenant request.
# The remaining service-owned SECURITY DEFINER entry points operate on
# cross-tenant state and are worker-only.  The dedicated worker retains the
# narrow routing function because worker/channel paths use it as well.
# Function names and signatures are safe schema metadata, not secrets.
RUNTIME_ROUTING_FUNCTION_SIGNATURES = ("public.resolve_channel_binding(text)",)
GLOBAL_WORKER_FUNCTION_SIGNATURES = (
    "public.list_channel_bindings(text)",
    "public.claim_outbox_events(text,text,integer,integer)",
    "public.sweep_expired_session_leases(integer)",
    "public.schedule_session_mailbox_retries(integer)",
    "public.reconcile_session_mailboxes(integer)",
    "public.reconcile_session_mailboxes_v2(integer,integer)",
)
ROLE_EVIDENCE_SCHEMA_VERSION = 1

# These names are intentionally stable: release reports and operators can
# distinguish an observed boundary from a final database state.  A final
# committed row alone is not enough evidence that a process was terminated at
# the requested point.
LOAD_STAGE_NAMES = (
    "acceptance.persisted",
    "turn.processing_observed",
    "worker.kill_requested",
    "worker.kill_completed",
    "worker.survivors_observed",
    "lease.takeover_observed",
    "stale_token_rejection_verified",
    "turn.commit_verified",
)
FAULT_STAGE_NAMES = (
    "toxiproxy.proxies_verified",
    "proxy.disable_requested",
    "proxy.disabled",
    "acceptance.persisted",
    "work_pending_while_disabled",
    "proxy.restore_requested",
    "proxy.restored",
    "post_restore.commit_verified",
    "duplicate_turn_verified",
    "duplicate_publish_verified",
    "dlq.dead_letter_verified",
)

PARTICIPATING_SERVICES = (
    "worker",
    "outbox-dispatcher",
    "channel-dispatcher",
    "post-turn-projector",
    "session-recovery",
)
COMPOSE_START_MODE_NONE = "none"
COMPOSE_START_MODE_GATE_OWNED = "gate-owned"
COMPOSE_START_MODE_WRAPPER_PRESTARTED = "wrapper-prestarted-owned"
EVIDENCE_PRODUCER = "scripts.real_runtime_gate"
SOURCE_FINGERPRINT_LABEL = "io.trpc.agent-service.source-fingerprint"
MIN_REAL_WORKERS = 4
MIN_PRODUCTION_MESSAGES = 200
MIN_PRODUCTION_DUPLICATES = 20
MIN_PRODUCTION_FAULT_MESSAGES = 8
MAX_REAL_WORKERS = 16
MAX_REAL_MESSAGES = 2_000
MAX_REAL_DUPLICATES = 2_000
MAX_REAL_FAULT_MESSAGES = 200
MAX_REAL_TIMEOUT_SECONDS = 900.0
HEALTH_POLL_INTERVAL_SECONDS = 0.5
MAX_HEALTH_WAIT_SECONDS = 120.0
WORKER_TERMINATION_POLL_INTERVAL_SECONDS = 0.25
ACTIVE_TURN_POLL_INTERVAL_SECONDS = 0.05
MAX_KILL_TARGET_AGE_SECONDS = 0.2
REAL_COMPOSE_PROJECT = "trpc-agent-service"
DEDICATED_RUNTIME_PROJECT_RE = re.compile(r"trpc-fault-[a-z0-9][a-z0-9-]{0,47}")
REAL_RUNTIME_REPORT_SCHEMA_VERSION = 1
_IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$", re.IGNORECASE)
_RUN_NONCE_RE = re.compile(r"^[0-9a-f]{32}$")


def _utc_timestamp(value: datetime | None = None) -> str:
    """Render an unambiguous UTC timestamp for report/marker binding."""

    timestamp = value or datetime.now(UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _valid_image_digest(value: Any) -> bool:
    return isinstance(value, str) and _IMAGE_DIGEST_RE.fullmatch(value) is not None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="required together with TRPC_RUN_REAL_MULTINODE=1",
    )
    parser.add_argument(
        "--phase",
        choices=("all", "load", "fault"),
        default="all",
        help="run all checks, only load/kill, or only dependency/DLQ faults",
    )
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=DEFAULT_COMPOSE_FILE,
        help="base Compose file (the data volumes are never removed)",
    )
    parser.add_argument(
        "--toxiproxy-override",
        type=Path,
        default=DEFAULT_TOXIPROXY_OVERRIDE,
        help="Compose override that routes app roles through Toxiproxy",
    )
    parser.add_argument(
        "--acceptance-override",
        type=Path,
        default=DEFAULT_ACCEPTANCE_OVERRIDE,
        help="opt-in Compose safety envelope for acceptance containers",
    )
    parser.add_argument(
        "--project", default=os.getenv("TRPC_REAL_COMPOSE_PROJECT", "trpc-agent-service")
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--messages", type=int, default=200)
    parser.add_argument("--duplicates", type=int, default=20)
    parser.add_argument("--fault-messages", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument(
        "--kill-worker",
        action="store_true",
        help="kill one worker while this run has an active turn",
    )
    parser.add_argument(
        "--allow-process-kill",
        action="store_true",
        help="second explicit acknowledgement for docker kill",
    )
    compose_mode = parser.add_mutually_exclusive_group()
    compose_mode.add_argument(
        "--compose-up",
        action="store_true",
        help="ensure the selected Compose services are up and scale workers",
    )
    compose_mode.add_argument(
        "--compose-prestarted",
        action="store_true",
        help=(
            "use an already-started dedicated wrapper-owned Compose project; "
            "requires an explicit caller flag and re-attests every container"
        ),
    )
    parser.add_argument(
        "--use-toxiproxy",
        action="store_true",
        help="include the Toxiproxy override and exercise Redis/PostgreSQL cuts",
    )
    parser.add_argument(
        "--republish-probe",
        action="store_true",
        help="actively XADD one authoritative inbound task for duplicate-delivery evidence",
    )
    parser.add_argument(
        "--redis-stream",
        default=os.getenv("TRPC_REAL_REDIS_STREAM", DEFAULT_REDIS_STREAM),
    )
    parser.add_argument(
        "--redis-group",
        default=os.getenv("TRPC_REAL_REDIS_GROUP", DEFAULT_REDIS_GROUP),
    )
    parser.add_argument(
        "--toxiproxy-api",
        default=os.getenv("TRPC_REAL_TOXIPROXY_API", "http://127.0.0.1:8474"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/real-runtime.json"),
    )
    parser.add_argument(
        "--require-production",
        action="store_true",
        help="return non-zero unless every requested real phase passes",
    )
    return parser


def _run_parameters(args: argparse.Namespace | None = None) -> dict[str, Any]:
    """Return only non-sensitive inputs that identify a real runtime case."""

    if args is None:
        return {"phase": "all"}
    compose_up = bool(getattr(args, "compose_up", False))
    compose_prestarted = bool(getattr(args, "compose_prestarted", False))
    compose_start_mode = (
        COMPOSE_START_MODE_GATE_OWNED
        if compose_up
        else COMPOSE_START_MODE_WRAPPER_PRESTARTED
        if compose_prestarted
        else COMPOSE_START_MODE_NONE
    )
    return {
        "phase": str(args.phase),
        "workers": int(args.workers),
        "messages": int(args.messages),
        "duplicates": int(args.duplicates),
        "fault_messages": int(args.fault_messages),
        "use_toxiproxy": bool(args.use_toxiproxy),
        "kill_worker": bool(args.kill_worker),
        "compose_up": compose_up or compose_prestarted,
        "compose_start_mode": compose_start_mode,
        "republish_probe": bool(args.republish_probe),
        "timeout_seconds": float(args.timeout_seconds),
        "redis_stream": str(args.redis_stream),
        "redis_group": str(args.redis_group),
    }


def _allowed_runtime_project(project: str) -> bool:
    """Limit destructive fault execution to the fixed or dedicated project."""

    return (
        project == REAL_COMPOSE_PROJECT
        or DEDICATED_RUNTIME_PROJECT_RE.fullmatch(project) is not None
    )


def _runtime_inputs_from_report(
    report: Mapping[str, Any],
    *,
    args: argparse.Namespace | None = None,
) -> dict[str, Any] | None:
    """Extract runtime identity only after the real evidence boundary passed.

    The runtime fingerprint binds every participating process/container, not
    just workers.  We deliberately keep this extraction separate from report
    serialization so a DSN, token, tenant, session, or message payload can
    never become part of the evidence envelope accidentally.
    """

    if report.get("gate") != "pass":
        return None
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return None
    preflight = candidate.get("preflight")
    if not isinstance(preflight, Mapping) or preflight.get("status") != "pass":
        return None
    # ``worker_containers`` is the native real-runtime preflight shape;
    # ``worker_processes`` is accepted for reports produced by a compatible
    # launcher that has already reduced the identity to process metadata.
    worker_containers = preflight.get("worker_containers")
    if worker_containers is None:
        worker_containers = preflight.get("worker_processes")
    if not isinstance(worker_containers, Sequence) or isinstance(
        worker_containers, (str, bytes, bytearray)
    ):
        return None
    if len(worker_containers) < MIN_REAL_WORKERS:
        return None
    image_attestation = preflight.get("image_attestation")
    if not isinstance(image_attestation, Mapping) or image_attestation.get("status") != "pass":
        return None
    expected_source = image_attestation.get("source_fingerprint")
    if not isinstance(expected_source, str) or not expected_source.strip():
        return None
    current_source = source_fingerprint(ROOT)
    if (
        current_source.get("status") != "available"
        or current_source.get("value") != expected_source
    ):
        return None
    worker_identities: list[dict[str, int | str]] = []
    image_ids: set[str] = set()
    for worker in worker_containers:
        if not isinstance(worker, Mapping):
            return None
        container_id = worker.get("container_id")
        pid = worker.get("pid")
        image_id = worker.get("image_id")
        source_label = worker.get("source_fingerprint")
        if not isinstance(container_id, str) or not container_id.strip():
            return None
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return None
        if not _valid_image_digest(image_id):
            return None
        assert isinstance(image_id, str)
        if not isinstance(source_label, str) or source_label != expected_source:
            return None
        image_ids.add(image_id)
        worker_identities.append(
            {
                "container_id": container_id,
                "pid": pid,
                "image_id": image_id,
                "source_fingerprint": source_label,
            }
        )
    if not worker_identities:
        return None
    if len({item["container_id"] for item in worker_identities}) != len(worker_identities):
        return None
    if len({item["pid"] for item in worker_identities}) != len(worker_identities):
        return None
    if len(image_ids) != 1 or image_attestation.get("image_id") not in image_ids:
        return None
    participating_services = preflight.get("participating_services")
    if not isinstance(participating_services, Mapping):
        return None
    if set(participating_services) != set(PARTICIPATING_SERVICES):
        return None
    participating_identities: list[dict[str, int | str]] = []
    participating_ids: set[str] = set()
    participating_pids: set[int] = set()
    participating_worker_ids: set[str] = set()
    participating_worker_identity_keys: set[tuple[str, int, str, str]] = set()
    for service in PARTICIPATING_SERVICES:
        containers = participating_services.get(service)
        if (
            not isinstance(containers, Sequence)
            or isinstance(containers, (str, bytes, bytearray))
            or not containers
        ):
            return None
        for container in containers:
            if not isinstance(container, Mapping):
                return None
            container_id = container.get("container_id")
            pid = container.get("pid")
            image_id = container.get("image_id")
            source_label = container.get("source_fingerprint")
            if (
                not isinstance(container_id, str)
                or not container_id.strip()
                or not isinstance(pid, int)
                or isinstance(pid, bool)
                or pid <= 0
                or not _valid_image_digest(image_id)
                or not isinstance(source_label, str)
                or source_label != expected_source
                or image_id not in image_ids
                or container.get("status") != "running"
                or container.get("health") != "healthy"
                or container.get("role") != service
            ):
                return None
            if container_id in participating_ids or pid in participating_pids:
                return None
            participating_ids.add(container_id)
            participating_pids.add(pid)
            if service == "worker":
                participating_worker_ids.add(container_id)
                participating_worker_identity_keys.add((container_id, pid, image_id, source_label))
            participating_identities.append(
                {
                    "role": service,
                    "container_id": container_id,
                    "pid": pid,
                    "image_id": image_id,
                    "source_fingerprint": source_label,
                }
            )
    if participating_worker_ids != {item["container_id"] for item in worker_identities}:
        return None
    if participating_worker_identity_keys != {
        (
            item["container_id"],
            item["pid"],
            item["image_id"],
            item["source_fingerprint"],
        )
        for item in worker_identities
    }:
        return None
    mode = candidate.get("mode")
    if not isinstance(mode, str) or not mode:
        return None
    # Always derive parameters from the parsed CLI namespace.  A report is an
    # output artifact, not a trusted source of runtime configuration; this
    # allowlist prevents a future candidate field from smuggling secrets into
    # the lineage hash.
    parameters = _run_parameters(args)
    stream = str(getattr(args, "redis_stream", "") or "") if args is not None else ""
    group = str(getattr(args, "redis_group", "") or "") if args is not None else ""
    if not stream or not group:
        return None
    return {
        "mode": mode,
        "worker_identities": worker_identities,
        "participating_identities": participating_identities,
        "stream": stream,
        "group": group,
        "parameters": dict(parameters),
    }


def _runtime_fingerprint_for_inputs(
    *,
    mode: str | None,
    worker_identities: Sequence[Any] | None,
    participating_identities: Sequence[Any] | None,
    stream: str | None,
    group: str | None,
    parameters: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build the real-runtime fingerprint while retaining only safe digests."""

    result = runtime_fingerprint(
        mode=mode,
        worker_identities=worker_identities,
        stream=stream,
        group=group,
        parameters=parameters,
    )
    if result.get("status") != "available" or participating_identities is None:
        return result
    participating = list(participating_identities)
    participating_hash = canonical_sha256(participating)
    material = {
        "mode": mode,
        "worker_identity_summary_sha256": result["worker_identity_summary_sha256"],
        "participating_identity_summary_sha256": participating_hash,
        "stream_group_sha256": result["stream_group_sha256"],
        "parameters_sha256": result["parameters_sha256"],
    }
    roles = {
        item.get("role")
        for item in participating
        if isinstance(item, Mapping) and isinstance(item.get("role"), str)
    }
    return {
        **result,
        "value": canonical_sha256(material),
        "participating_identity_summary_sha256": participating_hash,
        "participating_service_count": len(roles),
        "participating_container_count": len(participating),
    }


def _evidence_metadata(
    report: Mapping[str, Any],
    *,
    args: argparse.Namespace | None = None,
    run_id: str | None = None,
    runtime_inputs: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build current-candidate lineage at the final report write boundary."""

    inferred = runtime_inputs
    if inferred is None:
        inferred = _runtime_inputs_from_report(report, args=args)
    runtime: dict[str, Any] | None = None
    if inferred is not None:
        runtime = _runtime_fingerprint_for_inputs(
            mode=cast(str | None, inferred.get("mode")),
            worker_identities=cast(Sequence[Any] | None, inferred.get("worker_identities")),
            participating_identities=cast(
                Sequence[Any] | None, inferred.get("participating_identities")
            ),
            stream=cast(str | None, inferred.get("stream")),
            group=cast(str | None, inferred.get("group")),
            parameters=cast(Mapping[str, Any] | None, inferred.get("parameters")),
        )
    evidence = build_evidence(
        root=ROOT,
        producer=EVIDENCE_PRODUCER,
        run_id=run_id,
        generated_at=generated_at,
        runtime=runtime,
    )
    # ``build_evidence`` owns the generic allowlist.  These additional fields
    # are still safe digests/counts, so copy only the real-runtime contract
    # fields after the generic envelope has been built.
    if runtime is not None and runtime.get("status") == "available":
        safe_runtime = evidence.get("runtime_fingerprint")
        if isinstance(safe_runtime, dict):
            for key in (
                "participating_identity_summary_sha256",
                "participating_service_count",
                "participating_container_count",
            ):
                if key in runtime:
                    safe_runtime[key] = runtime[key]
    return evidence


def _assert_safe_output_path(output: Path) -> None:
    """Reject output files and existing parent components that are symlinks."""

    absolute = output.expanduser().absolute()
    if absolute.exists() and absolute.is_symlink():
        raise ValueError("report output symlink is not allowed")
    current = absolute.parent
    while True:
        if current.is_symlink():
            raise ValueError("report output parent symlink is not allowed")
        parent = current.parent
        if parent == current:
            break
        current = parent


def _bind_report_lineage(report: dict[str, Any], *, run_id: str, run_nonce: str) -> None:
    """Bind nested stage markers to the same opaque run identity."""

    candidate = report.get("candidate")
    if not isinstance(candidate, dict):
        return
    candidate.update(
        {
            "run_id": run_id,
            "run_nonce": run_nonce,
            "generated_at": report.get("generated_at"),
            "started_at": report.get("started_at"),
            "ended_at": report.get("ended_at"),
        }
    )

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            markers = value.get("stage_markers")
            if isinstance(markers, list):
                for marker in markers:
                    if isinstance(marker, dict):
                        marker.setdefault("run_id", run_id)
                        marker.setdefault("run_nonce", run_nonce)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(candidate)


def _marker_times_within_run_window(report: Mapping[str, Any]) -> bool:
    """Ensure every observed marker belongs to this report's execution window."""

    started_raw = report.get("started_at")
    ended_raw = report.get("ended_at")
    if not isinstance(started_raw, str) or not isinstance(ended_raw, str):
        return False
    try:
        started = datetime.fromisoformat(started_raw.replace("Z", "+00:00"))
        ended = datetime.fromisoformat(ended_raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if started.tzinfo is None or ended.tzinfo is None or ended < started:
        return False
    markers_seen = False

    def walk(value: Any) -> bool:
        nonlocal markers_seen
        if isinstance(value, dict):
            markers = value.get("stage_markers")
            if isinstance(markers, list):
                for marker in markers:
                    if not isinstance(marker, Mapping):
                        return False
                    markers_seen = True
                    observed_raw = marker.get("observed_at")
                    if not isinstance(observed_raw, str):
                        return False
                    try:
                        observed = datetime.fromisoformat(observed_raw.replace("Z", "+00:00"))
                    except ValueError:
                        return False
                    if observed.tzinfo is None or observed < started or observed > ended:
                        return False
            return all(walk(child) for child in value.values())
        if isinstance(value, list):
            return all(walk(child) for child in value)
        return True

    walked = walk(report.get("candidate"))
    return markers_seen and walked


def _write_report(
    output: Path,
    report: dict[str, Any],
    *,
    args: argparse.Namespace | None = None,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize a report with fresh source/runtime lineage and no secrets."""

    _assert_safe_output_path(output)
    report["schema_version"] = REAL_RUNTIME_REPORT_SCHEMA_VERSION
    started_at = report.get("started_at")
    if not isinstance(started_at, str) or not started_at.strip():
        started_at = _utc_timestamp()
    report["started_at"] = started_at
    generated_at = datetime.now(UTC)

    # A caller such as fault_injection_gate supplies a unique run id through
    # the environment.  The evidence helper validates it and creates a safe
    # opaque id when the value is absent or malformed.
    if report.get("gate") == "pass" and _runtime_inputs_from_report(report, args=args) is None:
        report["production_gate"] = "not_run"
        reason = "worker image/source attestation is unavailable or stale"
        rejection_reasons = report.setdefault("production_rejection_reasons", [])
        if isinstance(rejection_reasons, list) and reason not in rejection_reasons:
            rejection_reasons.append(reason)
    raw_run_id = report.get("run_id")
    if not isinstance(raw_run_id, str) or not raw_run_id.strip():
        raw_run_id = os.getenv("TRPC_REAL_RUN_ID")
    evidence = _evidence_metadata(
        report,
        args=args,
        run_id=raw_run_id if isinstance(raw_run_id, str) else None,
        runtime_inputs=runtime_inputs,
        generated_at=generated_at,
    )
    report["run_id"] = evidence["run_id"]
    run_nonce = report.get("run_nonce")
    if not isinstance(run_nonce, str) or _RUN_NONCE_RE.fullmatch(run_nonce) is None:
        run_nonce = uuid4().hex
    report["run_nonce"] = run_nonce
    report["generated_at"] = evidence["generated_at"]
    report["ended_at"] = _utc_timestamp()
    report["evidence"] = evidence
    evidence["run_nonce"] = run_nonce
    evidence["report_schema_version"] = REAL_RUNTIME_REPORT_SCHEMA_VERSION
    report["source_fingerprint"] = evidence.get("source_fingerprint")
    if bool(getattr(args, "execute", False)) and os.getenv(OPT_IN) == "1":
        try:
            expected_binding = current_release_binding(required=True)
        except ValueError as error:
            binding_reasons = [str(error)]
        else:
            binding_reasons = validate_release_binding(
                evidence,
                expected=expected_binding,
            )
        if binding_reasons:
            if report.get("gate") == "pass":
                report["gate"] = "not_run"
            report["production_gate"] = "not_run"
            report.setdefault("rejection_reasons", []).extend(binding_reasons)
            report.setdefault("production_rejection_reasons", []).extend(binding_reasons)
    _bind_report_lineage(report, run_id=str(report["run_id"]), run_nonce=run_nonce)
    if not _marker_times_within_run_window(report):
        reason = "runtime stage marker timing is missing or outside the run window"
        report["production_gate"] = "not_run"
        reasons = report.setdefault("production_rejection_reasons", [])
        if isinstance(reasons, list) and reason not in reasons:
            reasons.append(reason)
    rendered = atomic_write_json(output, report)
    print(rendered, end="")
    return report


def _stage_marker(name: str, status: str, **details: Any) -> dict[str, Any]:
    """Create a redaction-safe, machine-readable runtime stage marker."""

    marker: dict[str, Any] = {
        "name": name,
        "status": status,
        "observed_at": datetime.now(UTC).isoformat(),
    }
    marker.update({key: value for key, value in details.items() if value is not None})
    return marker


def _planned_stage_markers(names: Sequence[str], *, reason: str) -> list[dict[str, Any]]:
    return [_stage_marker(name, "not_run", reason=reason) for name in names]


def _not_run_report(reasons: Sequence[str], *, mode: str = "not_run") -> dict[str, Any]:
    reason_list = [str(reason) for reason in reasons]
    return {
        "schema_version": REAL_RUNTIME_REPORT_SCHEMA_VERSION,
        "run_nonce": uuid4().hex,
        "started_at": _utc_timestamp(),
        "baseline": {
            "independent_worker_processes": True,
            "postgresql_authoritative_acceptance": True,
            "redis_stream_delivery": True,
            "same_session_duplicate_and_ordering": True,
            "worker_kill_fencing_takeover": True,
            "toxiproxy_postgresql_redis_recovery": True,
            "outbound_dlq": True,
        },
        "candidate": {
            "mode": mode,
            "status": "not_run",
            "stage_markers": _planned_stage_markers(
                (*LOAD_STAGE_NAMES, *FAULT_STAGE_NAMES),
                reason="real runtime execution did not start",
            ),
        },
        "case_deltas": {},
        "gate": "not_run",
        "production_gate": "not_run",
        "rejection_reasons": reason_list,
        "production_rejection_reasons": reason_list,
    }


def _status(results: Sequence[dict[str, Any]]) -> str:
    values = [str(item.get("status", "not_run")) for item in results]
    if any(value == "fail" for value in values):
        return "fail"
    if any(value != "pass" for value in values):
        return "not_run"
    return "pass"


def _faults_skipped_after_load_failure(load: Mapping[str, Any]) -> dict[str, Any]:
    """Return an explicit, non-passing fault result after load failed.

    The fault phase contains several independent waits (dependency recovery,
    DLQ creation, and duplicate-publish observation). Running those waits
    after the load phase has already failed only delays the actionable result
    and can obscure the original failure. Keep the skipped phase in the
    report so the artifact remains complete and machine-readable, but never
    represent it as observed evidence.
    """

    reason = "skipped_due_to_load_failure"
    return {
        "status": "not_run",
        "reason": reason,
        "load_status": str(load.get("status", "not_run")),
        "load_reason": str(load.get("reason", "load phase did not pass")),
        "stage_markers": _planned_stage_markers(FAULT_STAGE_NAMES, reason=reason),
    }


def _production_scope_reasons(args: argparse.Namespace) -> list[str]:
    """Return explicit reasons a full run is below the production sample size."""

    reasons: list[str] = []
    if args.messages < MIN_PRODUCTION_MESSAGES:
        reasons.append(
            f"production acceptance requires messages >= {MIN_PRODUCTION_MESSAGES}; "
            f"requested {args.messages}"
        )
    if args.duplicates < MIN_PRODUCTION_DUPLICATES:
        reasons.append(
            f"production acceptance requires duplicates >= {MIN_PRODUCTION_DUPLICATES}; "
            f"requested {args.duplicates}"
        )
    if args.fault_messages < MIN_PRODUCTION_FAULT_MESSAGES:
        reasons.append(
            "production acceptance requires fault_messages >= "
            f"{MIN_PRODUCTION_FAULT_MESSAGES}; requested {args.fault_messages}"
        )
    if not getattr(args, "republish_probe", False):
        reasons.append(
            "production acceptance requires --republish-probe for real Redis "
            "duplicate-publish evidence"
        )
    return reasons


def _safe_failure(error: BaseException) -> dict[str, Any]:
    """Return a report-safe error without command output or connection URLs."""

    return {"status": "fail", "error_type": type(error).__name__}


def _command_result(command: Sequence[str], *, timeout: float = 30.0) -> dict[str, Any]:
    executable = shutil.which(command[0])
    if executable is None:
        return {"status": "not_run", "reason": f"{command[0]} is not installed"}
    try:
        completed = subprocess.run(  # noqa: S603 - command is assembled from fixed local selectors
            [executable, *command[1:]],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"status": "fail", "error_type": type(error).__name__}
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "exit_code": completed.returncode,
    }


def _command_output(command: Sequence[str], *, timeout: float = 30.0) -> dict[str, Any]:
    """Run one fixed local command and retain stdout only for local parsing."""

    executable = shutil.which(command[0])
    if executable is None:
        return {"status": "not_run", "reason": f"{command[0]} is not installed"}
    try:
        completed = subprocess.run(  # noqa: S603 - command is assembled from fixed local selectors
            [executable, *command[1:]],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"status": "fail", "error_type": type(error).__name__}
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
    }


def _compose_command(args: argparse.Namespace, *parts: str) -> list[str]:
    command = ["docker", "compose", "-f", str(args.compose_file)]
    if args.use_toxiproxy:
        command.extend(("-f", str(args.toxiproxy_override)))
    acceptance_override = getattr(args, "acceptance_override", DEFAULT_ACCEPTANCE_OVERRIDE)
    command.extend(("-f", str(acceptance_override)))
    command.extend(("-p", args.project))
    command.extend(parts)
    return command


def _compose_project_container_ids(args: argparse.Namespace) -> tuple[str, ...]:
    """Return all containers in this exact Compose project, including stopped ones."""

    result = _command_output(_compose_command(args, "ps", "-aq"), timeout=30)
    if result.get("status") != "pass":
        raise RuntimeError("Compose project inventory failed")
    output = result.get("stdout", "")
    if not isinstance(output, str):
        raise RuntimeError("Compose project inventory returned invalid output")
    return tuple(item.strip() for item in output.splitlines() if item.strip())


def _cleanup_owned_compose(args: argparse.Namespace) -> dict[str, Any] | None:
    """Stop only a Compose project that this invocation created, never its volumes."""

    if not getattr(args, "_compose_started_by_gate", False):
        return None
    # Clear ownership before the command so an exception or a second cleanup
    # path cannot issue a second destructive command.
    args._compose_started_by_gate = False
    result = _command_result(
        _compose_command(args, "down", "--remove-orphans"),
        timeout=180,
    )
    args._compose_cleanup_result = result
    return result


def _attach_compose_cleanup_evidence(
    report: dict[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    """Fail closed when a gate-owned Compose project was not confirmed removed."""

    cleanup = getattr(args, "_compose_cleanup_result", None)
    if not isinstance(cleanup, Mapping):
        return report
    candidate = report.setdefault("candidate", {})
    if isinstance(candidate, dict):
        candidate["compose_cleanup"] = dict(cleanup)
    if cleanup.get("status") == "pass":
        return report
    reason = "gate-owned Compose project cleanup did not complete"
    report["gate"] = "fail"
    if report.get("production_gate") == "pass":
        report["production_gate"] = "fail"
    for field in ("rejection_reasons", "production_rejection_reasons"):
        reasons = report.setdefault(field, [])
        if isinstance(reasons, list) and reason not in reasons:
            reasons.append(reason)
    return report


def _worker_ids(args: argparse.Namespace) -> tuple[str, ...]:
    return _service_ids(args, "worker")


def _service_ids(args: argparse.Namespace, service: str) -> tuple[str, ...]:
    """Return only currently running containers for one exact Compose service."""

    command = _compose_command(args, "ps", "-q", "--status", "running", service)
    result = _command_result(command)
    if result.get("status") != "pass":
        raise RuntimeError(f"docker compose {service} discovery failed")
    executable = shutil.which("docker")
    assert executable is not None
    completed = subprocess.run(  # noqa: S603 - fixed Docker executable and audited selectors
        [executable, *command[1:]],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return tuple(item.strip() for item in completed.stdout.splitlines() if item.strip())


def _parse_endpoint(raw: str, *, kind: str) -> dict[str, Any]:
    allowed_schemes = {
        "postgres": {"postgresql", "postgresql+asyncpg"},
        "redis": {"redis", "rediss"},
    }[kind]
    default_port = 5432 if kind == "postgres" else 6379
    parsed = urlsplit(raw)
    if parsed.scheme not in allowed_schemes:
        raise ValueError(f"{kind} connection URL has an unsupported scheme")
    try:
        host = parsed.hostname
        port = parsed.port or default_port
    except ValueError as error:
        raise ValueError(f"{kind} connection URL has an invalid port") from error
    if not host or not (1 <= port <= 65535):
        raise ValueError(f"{kind} connection URL has no valid host/port")
    # Return only routing metadata.  Userinfo, query strings and passwords are
    # deliberately excluded from all evidence reports.
    return {"scheme": parsed.scheme, "host": host, "port": port}


def _parse_database_endpoint(raw: str) -> dict[str, Any]:
    """Parse a PostgreSQL endpoint and retain only its authenticated role."""

    endpoint = _parse_endpoint(raw, kind="postgres")
    parsed = urlsplit(raw)
    role = unquote(parsed.username or "").strip()
    if not role:
        raise ValueError("postgres connection URL has no explicit database role")
    endpoint["role"] = role
    return endpoint


def _parse_connection_environment(environment_lines: Sequence[str]) -> dict[str, Any]:
    """Parse service routes while retaining no passwords or raw DSNs."""

    environment: dict[str, str] = {}
    for line in environment_lines:
        key, separator, value = line.partition("=")
        if separator and key in {
            "TRPC_SERVICE_DATABASE_DSN",
            "TRPC_SERVICE_WORKER_DATABASE_DSN",
            "TRPC_SERVICE_REDIS_URL",
            "TRPC_SERVICE_ROLE",
        }:
            environment[key] = value
    missing = [
        key
        for key in ("TRPC_SERVICE_DATABASE_DSN", "TRPC_SERVICE_REDIS_URL")
        if not environment.get(key)
    ]
    if missing:
        return {"valid": False, "reason": f"missing exact connection env: {','.join(missing)}"}
    if environment.get("TRPC_SERVICE_ROLE") in PARTICIPATING_SERVICES and not environment.get(
        "TRPC_SERVICE_WORKER_DATABASE_DSN"
    ):
        return {
            "valid": False,
            "reason": "missing exact connection env: TRPC_SERVICE_WORKER_DATABASE_DSN",
        }
    try:
        database = _parse_endpoint(environment["TRPC_SERVICE_DATABASE_DSN"], kind="postgres")
        redis = _parse_endpoint(environment["TRPC_SERVICE_REDIS_URL"], kind="redis")
        worker_database = (
            _parse_database_endpoint(environment["TRPC_SERVICE_WORKER_DATABASE_DSN"])
            if environment.get("TRPC_SERVICE_WORKER_DATABASE_DSN")
            else None
        )
    except ValueError as error:
        return {"valid": False, "reason": str(error)}
    result: dict[str, Any] = {
        "valid": True,
        "role": environment.get("TRPC_SERVICE_ROLE"),
        "database": database,
        "redis": redis,
    }
    if worker_database is not None:
        result["worker_database"] = worker_database
    return result


def _connection_routes_match(
    container: dict[str, Any], *, use_toxiproxy: bool, expected_role: str | None = None
) -> bool:
    expected = (
        {"host": "toxiproxy", "database_port": 15432, "redis_port": 16379}
        if use_toxiproxy
        else {"host": "postgres", "database_port": 5432, "redis_port": 6379}
    )
    routes = container.get("connection_env")
    if not isinstance(routes, dict) or routes.get("valid") is not True:
        return False
    database = routes.get("database")
    redis = routes.get("redis")
    worker_database = routes.get("worker_database")
    worker_route_valid = expected_role not in PARTICIPATING_SERVICES or (
        isinstance(worker_database, dict)
        and worker_database.get("role") == WORKER_DATABASE_ROLE
        and worker_database.get("host") == expected["host"]
        and worker_database.get("port") == expected["database_port"]
    )
    return (
        isinstance(database, dict)
        and isinstance(redis, dict)
        and database.get("host") == expected["host"]
        and database.get("port") == expected["database_port"]
        and redis.get("host") == expected["host"]
        and redis.get("port") == expected["redis_port"]
        and worker_route_valid
        and (expected_role is None or routes.get("role") == expected_role)
    )


def _inspect_container(
    args: argparse.Namespace, container_id: str, *, allow_stopped: bool = False
) -> dict[str, Any]:
    executable = shutil.which("docker")
    if executable is None:
        raise RuntimeError("docker is not installed")
    # JSON keeps image IDs and labels unambiguous even when a future image
    # reference contains whitespace.  We never return the raw inspect object;
    # environment values are reduced to parsed routing metadata below.
    inspect_format = "{{json .}}"
    completed = subprocess.run(  # noqa: S603 - fixed Docker executable and container ID selector
        [
            executable,
            "inspect",
            "--format",
            inspect_format,
            container_id,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode:
        raise RuntimeError("docker container inspection failed")
    try:
        inspected = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("docker container inspection returned invalid JSON") from error
    if not isinstance(inspected, Mapping):
        raise RuntimeError("docker container inspection returned no object")
    state = inspected.get("State")
    config = inspected.get("Config")
    if not isinstance(state, Mapping) or not isinstance(config, Mapping):
        raise RuntimeError("docker container inspection returned no process identity")
    container_value = inspected.get("Id")
    pid = state.get("Pid")
    status = state.get("Status")
    health_value = state.get("Health")
    health = health_value.get("Status") if isinstance(health_value, Mapping) else "none"
    image_id = inspected.get("Image")
    labels = config.get("Labels")
    source_label = labels.get(SOURCE_FINGERPRINT_LABEL) if isinstance(labels, Mapping) else None
    environment = config.get("Env")
    if not isinstance(container_value, str) or not container_value.strip():
        raise RuntimeError("docker container inspection returned no container ID")
    if not isinstance(pid, int) or isinstance(pid, bool):
        raise RuntimeError("docker container inspection returned no process identity")
    if not isinstance(status, str) or not status:
        raise RuntimeError("docker container inspection returned no container status")
    if pid <= 0 and not (allow_stopped and status.lower() in {"created", "exited", "dead"}):
        raise RuntimeError("docker container inspection returned no process identity")
    if not isinstance(environment, Sequence) or isinstance(environment, (str, bytes, bytearray)):
        environment = ()
    connection_env = _parse_connection_environment(
        [item for item in environment if isinstance(item, str)]
    )
    return {
        "container_id": container_value,
        "pid": pid,
        "status": status,
        "health": health if isinstance(health, str) and health else "unknown",
        "role": connection_env.get("role"),
        # Docker's container ``Image`` field is the immutable content ID
        # (sha256 in normal Docker engines), which is sufficient to prove
        # that every worker runs the same candidate image.
        "image_id": image_id if isinstance(image_id, str) and image_id.strip() else None,
        "source_fingerprint": (
            source_label if isinstance(source_label, str) and source_label.strip() else None
        ),
        # Do not include environment values in reports; this contains only
        # parsed, redaction-safe routing metadata.
        "connection_env": connection_env,
    }


def _worker_image_attestation(workers: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Prove that all target workers run the current source candidate.

    This is deliberately a read-only check over the already-inspected worker
    containers.  A process/PID list alone cannot establish provenance, and a
    missing or mixed image must never be promoted to production evidence.
    """

    if len(workers) < MIN_REAL_WORKERS:
        return {
            "status": "not_run",
            "reason": f"at least {MIN_REAL_WORKERS} worker containers are required",
        }
    current = source_fingerprint(ROOT)
    if current.get("status") != "available" or not isinstance(current.get("value"), str):
        return {
            "status": "not_run",
            "reason": "current source fingerprint is unavailable",
        }
    expected_source = current["value"]
    image_ids: set[str] = set()
    container_ids: set[str] = set()
    missing: list[str] = []
    stale: list[str] = []
    for worker in workers:
        container_id = worker.get("container_id")
        if isinstance(container_id, str) and container_id.strip():
            container_ids.add(container_id)
        image_id = worker.get("image_id")
        source_label = worker.get("source_fingerprint")
        if not _valid_image_digest(image_id):
            if isinstance(container_id, str) and container_id.strip():
                missing.append(container_id)
            continue
        assert isinstance(image_id, str)
        image_ids.add(image_id)
        if source_label != expected_source:
            if isinstance(container_id, str) and container_id.strip():
                stale.append(container_id)
    if len(container_ids) != len(workers):
        return {
            "status": "not_run",
            "reason": "worker container identities are not unique",
            "worker_count": len(workers),
        }
    if missing:
        return {
            "status": "not_run",
            "reason": "worker image ID or source label is missing",
            "missing_worker_count": len(missing),
            "worker_count": len(workers),
        }
    if stale:
        return {
            "status": "not_run",
            "reason": "worker image source label is missing or stale",
            "stale_worker_count": len(stale),
            "worker_count": len(workers),
        }
    if len(image_ids) != 1:
        return {
            "status": "not_run",
            "reason": "workers use mixed candidate image IDs",
            "image_count": len(image_ids),
            "worker_count": len(workers),
        }
    image_id = next(iter(image_ids))
    return {
        "status": "pass",
        "worker_count": len(workers),
        "image_id": image_id,
        "image_digest_verified": True,
        "algorithm": "sha256",
        "source_fingerprint": expected_source,
    }


def _wait_for_healthy_containers(
    args: argparse.Namespace,
    *,
    service: str,
    minimum: int,
    discover: Any,
) -> tuple[dict[str, Any], ...]:
    """Poll startup state until a complete healthy service inventory is observed.

    Compose reports a container as running before its healthcheck reaches
    ``healthy``.  A single inspect at that boundary made a valid startup look
    permanently ``not_run``.  Discovery and inspection are both retried so a
    container that is still starting (or briefly restarting) is never
    promoted from a partial inventory.
    """

    deadline = time.monotonic() + min(float(args.timeout_seconds), MAX_HEALTH_WAIT_SECONDS)
    latest: tuple[dict[str, Any], ...] = ()
    while True:
        try:
            container_ids = tuple(discover(args))
            latest = tuple(_inspect_container(args, item) for item in container_ids)
        except (OSError, RuntimeError, ValueError):
            # Docker can return an empty/temporarily stale inventory while a
            # service is being recreated.  Keep polling; the final caller
            # still returns not_run if readiness never becomes observable.
            latest = ()
        if len(latest) >= minimum and all(
            item.get("status") == "running" and item.get("health") == "healthy" for item in latest
        ):
            return latest
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return latest
        time.sleep(min(HEALTH_POLL_INTERVAL_SECONDS, remaining))


def _preflight(args: argparse.Namespace) -> dict[str, Any]:
    acceptance_override = Path(getattr(args, "acceptance_override", DEFAULT_ACCEPTANCE_OVERRIDE))
    args._compose_started_by_gate = False
    args._compose_cleanup_result = None
    try:
        _assert_safe_output_path(args.compose_file)
        _assert_safe_output_path(args.toxiproxy_override)
        _assert_safe_output_path(acceptance_override)
        compose_file = args.compose_file.resolve(strict=True)
        toxiproxy_override = args.toxiproxy_override.resolve(strict=True)
        acceptance_override = acceptance_override.resolve(strict=True)
    except (OSError, ValueError):
        return {"status": "not_run", "reason": "runtime Compose inputs are unavailable"}
    if compose_file != DEFAULT_COMPOSE_FILE.resolve():
        return {"status": "not_run", "reason": "runtime gate requires the repository Compose file"}
    if acceptance_override != DEFAULT_ACCEPTANCE_OVERRIDE.resolve():
        return {
            "status": "not_run",
            "reason": "runtime gate requires the repository acceptance safety override",
        }
    if args.use_toxiproxy and toxiproxy_override != DEFAULT_TOXIPROXY_OVERRIDE.resolve():
        return {
            "status": "not_run",
            "reason": "runtime gate requires the repository Toxiproxy override",
        }
    if not _allowed_runtime_project(args.project):
        return {
            "status": "not_run",
            "reason": (
                "runtime gate requires the fixed Compose project or a dedicated "
                "trpc-fault-* project"
            ),
        }
    if not args.compose_file.is_file():
        return {"status": "not_run", "reason": f"Compose file not found: {args.compose_file}"}
    if args.use_toxiproxy and not args.toxiproxy_override.is_file():
        return {
            "status": "not_run",
            "reason": f"Toxiproxy Compose override not found: {args.toxiproxy_override}",
        }
    if not acceptance_override.is_file():
        return {
            "status": "not_run",
            "reason": f"acceptance safety Compose override not found: {acceptance_override}",
        }
    if args.workers < MIN_REAL_WORKERS:
        return {
            "status": "not_run",
            "reason": f"at least {MIN_REAL_WORKERS} worker containers are required",
        }
    if args.workers > MAX_REAL_WORKERS:
        return {"status": "fail", "reason": "worker count exceeds the runtime safety cap"}
    if (
        args.messages < 1
        or args.messages > MAX_REAL_MESSAGES
        or args.duplicates < 0
        or args.duplicates > min(args.messages, MAX_REAL_DUPLICATES)
        or args.fault_messages < 1
        or args.fault_messages > MAX_REAL_FAULT_MESSAGES
    ):
        return {"status": "fail", "reason": "invalid message/duplicate counts"}
    if (
        not math.isfinite(args.timeout_seconds)
        or args.timeout_seconds < 1
        or args.timeout_seconds > MAX_REAL_TIMEOUT_SECONDS
    ):
        return {"status": "fail", "reason": "timeout exceeds the runtime safety bounds"}
    if args.redis_stream != DEFAULT_REDIS_STREAM or args.redis_group != DEFAULT_REDIS_GROUP:
        return {"status": "not_run", "reason": "production runtime requires SessionReady v2"}
    if getattr(args, "compose_prestarted", False):
        # A wrapper may hand off a project it created after checking that the
        # exact project was empty.  The gate still requires an explicit mode,
        # a dedicated project name, and a non-empty inventory; all containers
        # are re-attested below before any runtime work starts.
        if DEDICATED_RUNTIME_PROJECT_RE.fullmatch(args.project) is None:
            return {
                "status": "not_run",
                "reason": "--compose-prestarted requires a dedicated trpc-fault-* project",
            }
        try:
            existing = _compose_project_container_ids(args)
        except (OSError, RuntimeError, ValueError) as error:
            return _safe_failure(error) | {
                "reason": "prestarted Compose project ownership inventory failed"
            }
        if not existing:
            return {
                "status": "not_run",
                "reason": (
                    "--compose-prestarted requires a non-empty wrapper-owned Compose project; "
                    "no existing containers were found"
                ),
            }
    if args.compose_up:
        try:
            existing = _compose_project_container_ids(args)
        except (OSError, RuntimeError, ValueError) as error:
            return _safe_failure(error) | {"reason": "Compose project ownership check failed"}
        if existing:
            return {
                "status": "not_run",
                "reason": (
                    "refusing --compose-up because the Compose project already has containers; "
                    "caller-owned containers were not modified"
                ),
                "existing_container_count": len(existing),
            }
        # Mark ownership before `up`: a partial startup still needs the same
        # exact-project, volume-preserving cleanup path.
        args._compose_started_by_gate = True
        result = _command_result(
            _compose_command(
                args,
                "up",
                "-d",
                "--no-build",
                "--scale",
                f"worker={args.workers}",
                "postgres",
                "redis",
                "worker",
                *(("toxiproxy",) if args.use_toxiproxy else ()),
                "outbox-dispatcher",
                "channel-dispatcher",
                "post-turn-projector",
                "session-recovery",
            ),
            timeout=180,
        )
        if result.get("status") != "pass":
            return {"status": "not_run", "reason": "Compose stack could not be started"}
    try:
        workers = _wait_for_healthy_containers(
            args,
            service="worker",
            minimum=args.workers,
            discover=_worker_ids,
        )
    except (OSError, RuntimeError, ValueError) as error:
        return _safe_failure(error) | {"reason": "worker container inspection failed"}
    if len(workers) < args.workers:
        return {
            "status": "not_run",
            "reason": f"need {args.workers} running workers, found {len(workers)}",
            "worker_containers": workers,
        }
    worker_not_ready = [
        item
        for item in workers
        if item.get("status") != "running" or item.get("health") != "healthy"
    ]
    if worker_not_ready:
        return {
            "status": "not_run",
            "reason": (
                "every worker must be running and healthy; stopped containers are not counted"
            ),
            "worker_containers": workers,
        }
    if (
        len({item["container_id"] for item in workers}) < MIN_REAL_WORKERS
        or len({item["pid"] for item in workers}) < MIN_REAL_WORKERS
    ):
        return {
            "status": "not_run",
            "reason": "workers are not independent container processes",
            "worker_containers": workers,
        }
    image_attestation = _worker_image_attestation(workers)
    if image_attestation.get("status") != "pass":
        return {
            "status": "not_run",
            "reason": str(image_attestation.get("reason", "worker image attestation failed")),
            "worker_containers": workers,
            "image_attestation": image_attestation,
        }
    if not all(
        _connection_routes_match(item, use_toxiproxy=args.use_toxiproxy, expected_role="worker")
        for item in workers
    ):
        return {
            "status": "not_run",
            "reason": (
                "worker database/Redis connection environment does not match the selected route"
            ),
            "worker_containers": workers,
        }
    if any(item.get("role") != "worker" for item in workers):
        return {
            "status": "not_run",
            "reason": "worker role attestation is missing or mismatched",
            "worker_containers": workers,
            "image_attestation": image_attestation,
        }
    participating: dict[str, tuple[dict[str, Any], ...]] = {"worker": workers}
    try:
        for service in PARTICIPATING_SERVICES[1:]:
            inspected = _wait_for_healthy_containers(
                args,
                service=service,
                minimum=1,
                discover=lambda current_args, selected=service: _service_ids(
                    current_args, selected
                ),
            )
            if not inspected:
                return {
                    "status": "not_run",
                    "reason": f"no running {service} container was found",
                    "worker_containers": workers,
                    "participating_services": participating,
                }
            not_ready = [
                item
                for item in inspected
                if item.get("status") != "running" or item.get("health") != "healthy"
            ]
            if not_ready:
                return {
                    "status": "not_run",
                    "reason": f"{service} must be running and healthy",
                    "worker_containers": workers,
                    "participating_services": participating | {service: inspected},
                }
            if not all(
                _connection_routes_match(
                    item,
                    use_toxiproxy=args.use_toxiproxy,
                    expected_role=service,
                )
                for item in inspected
            ):
                return {
                    "status": "not_run",
                    "reason": f"{service} connection environment does not match the selected route",
                    "worker_containers": workers,
                    "participating_services": participating | {service: inspected},
                }
            if any(
                not _valid_image_digest(item.get("image_id"))
                or item.get("image_id") != image_attestation.get("image_id")
                or item.get("source_fingerprint") != image_attestation.get("source_fingerprint")
                or item.get("role") != service
                or not isinstance(item.get("pid"), int)
                or isinstance(item.get("pid"), bool)
                or item.get("pid", 0) <= 0
                for item in inspected
            ):
                return {
                    "status": "not_run",
                    "reason": f"{service} image, source, PID, or role attestation is incomplete",
                    "worker_containers": workers,
                    "participating_services": participating | {service: inspected},
                }
            participating[service] = inspected
    except (OSError, RuntimeError, ValueError) as error:
        return _safe_failure(error) | {
            "reason": "participating service container inspection failed",
            "worker_containers": workers,
            "participating_services": participating,
        }
    all_containers = tuple(item for items in participating.values() for item in items)
    all_container_ids = [str(item.get("container_id")) for item in all_containers]
    all_pids = [int(item["pid"]) for item in all_containers]
    if len(all_container_ids) != len(set(all_container_ids)):
        return {
            "status": "not_run",
            "reason": "participating service container identities are duplicated",
            "worker_containers": workers,
            "image_attestation": image_attestation,
            "participating_services": participating,
        }
    if len(all_pids) != len(set(all_pids)):
        return {
            "status": "not_run",
            "reason": "participating service process identities are duplicated",
            "worker_containers": workers,
            "image_attestation": image_attestation,
            "participating_services": participating,
        }
    return {
        "status": "pass",
        "worker_containers": workers,
        "image_attestation": image_attestation,
        "participating_services": participating,
    }


def _connection_dsn(raw: str) -> str:
    return raw.replace("postgresql+asyncpg://", "postgresql://")


async def _role_snapshot(
    connection: asyncpg.Connection,
    *,
    expected_functions: Sequence[str],
) -> dict[str, Any]:
    """Read authenticated PostgreSQL role facts without exposing credentials."""

    row = await connection.fetchrow(
        """
        SELECT current_user::text AS current_user,
               session_user::text AS session_user,
               r.rolname::text AS role_name,
               r.rolsuper AS role_superuser,
               r.rolbypassrls AS role_bypassrls
          FROM pg_roles AS r
         WHERE r.rolname = current_user
        """
    )
    if row is None:
        raise RuntimeError("authenticated PostgreSQL role metadata is unavailable")
    function_rows = await connection.fetch(
        """
        SELECT requested.signature,
               to_regprocedure(requested.signature) IS NOT NULL AS function_exists,
               CASE
                   WHEN to_regprocedure(requested.signature) IS NULL THEN false
                   ELSE has_function_privilege(
                       current_user,
                       to_regprocedure(requested.signature),
                       'EXECUTE'
                   )
               END AS execute_granted
          FROM unnest($1::text[]) AS requested(signature)
         ORDER BY requested.signature
        """,
        list(expected_functions),
    )
    functions = {
        str(item["signature"]): {
            "exists": bool(item["function_exists"]),
            "execute": bool(item["execute_granted"]),
        }
        for item in function_rows
    }
    return {
        "current_user": str(row["current_user"]),
        "session_user": str(row["session_user"]),
        "role_name": str(row["role_name"]),
        "role_superuser": bool(row["role_superuser"]),
        "role_bypassrls": bool(row["role_bypassrls"]),
        "functions": functions,
    }


async def _probe_global_function(
    connection: asyncpg.Connection,
    *,
    expected_access: str,
) -> dict[str, Any]:
    """Probe a read-only global function in a transaction.

    ``list_channel_bindings`` is deliberately called with a sentinel channel
    that cannot be a real binding.  The transaction makes this probe safe if
    a future implementation changes the function from read-only to mutating.
    Only the SQLSTATE class is retained on errors.
    """

    try:
        async with connection.transaction():
            await connection.fetch(
                "SELECT * FROM public.list_channel_bindings($1)",
                "__runtime_role_gate_missing_channel__",
            )
    except asyncpg.PostgresError as error:
        denied = getattr(error, "sqlstate", None) == "42501"
        return {
            "function": "public.list_channel_bindings(text)",
            "expected_access": expected_access,
            "observed_access": "denied" if denied else "error",
            "denied": denied,
            "error_type": type(error).__name__,
        }
    return {
        "function": "public.list_channel_bindings(text)",
        "expected_access": expected_access,
        "observed_access": "allowed",
        "denied": False,
    }


def _role_evidence_check(
    value: object,
) -> tuple[str, str | None]:
    """Validate the JSON-only database role contract at a release boundary.

    This function deliberately recomputes checks from observed snapshots; a
    producer-supplied ``checks`` map is diagnostic only and cannot promote an
    otherwise incomplete report.
    """

    if not isinstance(value, Mapping):
        return "not_run", "candidate.database_role_evidence is missing"
    if value.get("schema_version") != ROLE_EVIDENCE_SCHEMA_VERSION:
        return "not_run", "candidate.database_role_evidence schema is unsupported"
    if value.get("status") == "fail":
        return "fail", "candidate.database_role_evidence reported fail"
    if value.get("status") != "pass":
        return "not_run", "candidate.database_role_evidence did not pass"
    worker_expected = list(GLOBAL_WORKER_FUNCTION_SIGNATURES)
    runtime_allowed = list(RUNTIME_ROUTING_FUNCTION_SIGNATURES)
    all_expected = [*runtime_allowed, *worker_expected]
    if value.get("required_functions") != worker_expected:
        return "not_run", "candidate.database_role_evidence function allowlist is invalid"
    if value.get("runtime_allowed_functions") != runtime_allowed:
        return "not_run", "candidate.database_role_evidence runtime allowlist is invalid"
    runtime = value.get("runtime")
    worker = value.get("global_worker")
    if not isinstance(runtime, Mapping) or not isinstance(worker, Mapping):
        return "not_run", "candidate.database_role_evidence runtime/global-worker snapshots missing"
    expected_runtime = runtime.get("expected_role")
    expected_worker = worker.get("expected_role")
    if not isinstance(expected_runtime, str) or not expected_runtime:
        return "not_run", "candidate.database_role_evidence expected runtime role is missing"
    if not isinstance(expected_worker, str) or not expected_worker:
        return "not_run", "candidate.database_role_evidence expected worker role is missing"
    if expected_runtime != RUNTIME_DATABASE_ROLE:
        return "fail", f"runtime database role must be {RUNTIME_DATABASE_ROLE}"
    if expected_worker != WORKER_DATABASE_ROLE:
        return "fail", f"global worker database role must be {WORKER_DATABASE_ROLE}"
    runtime_snapshot = runtime.get("role_snapshot")
    worker_snapshot = worker.get("role_snapshot")
    if not isinstance(runtime_snapshot, Mapping) or not isinstance(worker_snapshot, Mapping):
        return "not_run", "candidate.database_role_evidence role snapshots missing"

    def _snapshot_fields(snapshot: Mapping[str, Any]) -> bool:
        return (
            isinstance(snapshot.get("current_user"), str)
            and bool(snapshot.get("current_user"))
            and isinstance(snapshot.get("session_user"), str)
            and bool(snapshot.get("session_user"))
            and isinstance(snapshot.get("role_name"), str)
            and bool(snapshot.get("role_name"))
            and type(snapshot.get("role_superuser")) is bool
            and type(snapshot.get("role_bypassrls")) is bool
        )

    if not _snapshot_fields(runtime_snapshot) or not _snapshot_fields(worker_snapshot):
        return "not_run", "candidate.database_role_evidence role identity fields missing"
    runtime_functions = runtime_snapshot.get("functions")
    worker_functions = worker_snapshot.get("functions")
    if not isinstance(runtime_functions, Mapping) or not isinstance(worker_functions, Mapping):
        return "not_run", "candidate.database_role_evidence function privileges missing"

    def _function_contract(functions: Mapping[str, Any]) -> bool:
        if set(functions) != set(all_expected):
            return False
        return all(
            isinstance(item, Mapping) and item.get("exists") is True for item in functions.values()
        )

    def _function_access(
        functions: Mapping[str, Any], signatures: Sequence[str], *, execute: bool
    ) -> bool:
        return all(
            isinstance(functions.get(signature), Mapping)
            and functions[signature].get("execute") is execute
            for signature in signatures
        )

    runtime_probe = runtime.get("global_function_probe")
    worker_probe = worker.get("global_function_probe")
    if not isinstance(runtime_probe, Mapping) or not isinstance(worker_probe, Mapping):
        return "not_run", "candidate.database_role_evidence function probes missing"
    if (
        runtime_probe.get("function") != "public.list_channel_bindings(text)"
        or runtime_probe.get("expected_access") != "denied"
        or runtime_probe.get("observed_access") != "denied"
        or runtime_probe.get("denied") is not True
        or worker_probe.get("function") != "public.list_channel_bindings(text)"
        or worker_probe.get("expected_access") != "allowed"
        or worker_probe.get("observed_access") != "allowed"
        or worker_probe.get("denied") is not False
    ):
        return "fail", "database global-function execution probe did not match the role contract"
    runtime_user = runtime_snapshot["current_user"]
    worker_user = worker_snapshot["current_user"]
    if (
        runtime_snapshot["current_user"] != RUNTIME_DATABASE_ROLE
        or runtime_snapshot["current_user"] != runtime_snapshot["session_user"]
        or runtime_snapshot["current_user"] != runtime_snapshot["role_name"]
        or runtime_snapshot["role_superuser"] is not False
        or runtime_snapshot["role_bypassrls"] is not False
        or not _function_contract(runtime_functions)
        or not _function_access(runtime_functions, runtime_allowed, execute=True)
        or not _function_access(runtime_functions, worker_expected, execute=False)
        or runtime_user == worker_user
        or worker_snapshot["current_user"] != WORKER_DATABASE_ROLE
        or worker_snapshot["current_user"] != worker_snapshot["session_user"]
        or worker_snapshot["current_user"] != worker_snapshot["role_name"]
        or worker_snapshot["current_user"] != expected_worker
        or worker_snapshot["role_superuser"] is not False
        or worker_snapshot["role_bypassrls"] is not True
        or not _function_contract(worker_functions)
        or not _function_access(worker_functions, runtime_allowed, execute=True)
        or not _function_access(worker_functions, worker_expected, execute=True)
    ):
        return "fail", "database role identities or exact function privileges violate the contract"
    return "pass", None


async def _database_role_evidence(
    pool: asyncpg.Pool,
) -> dict[str, Any]:
    """Collect ordinary-runtime and dedicated global-worker role evidence."""

    worker_dsn = os.getenv(GLOBAL_WORKER_DATABASE_DSN_ENV, "").strip()
    worker_role = os.getenv(GLOBAL_WORKER_DATABASE_ROLE_ENV, "").strip()
    missing = [
        name
        for name, value in (
            (GLOBAL_WORKER_DATABASE_DSN_ENV, worker_dsn),
            (GLOBAL_WORKER_DATABASE_ROLE_ENV, worker_role),
        )
        if not value
    ]
    if missing:
        return {
            "schema_version": ROLE_EVIDENCE_SCHEMA_VERSION,
            "status": "not_run",
            "required_functions": list(GLOBAL_WORKER_FUNCTION_SIGNATURES),
            "runtime_allowed_functions": list(RUNTIME_ROUTING_FUNCTION_SIGNATURES),
            "reason": "dedicated global-worker database credentials are not configured",
            "missing_variable_count": len(missing),
        }

    runtime_role = os.getenv(RUNTIME_DATABASE_ROLE_ENV, "").strip()
    worker_connection: asyncpg.Connection | None = None
    result: dict[str, Any] = {
        "schema_version": ROLE_EVIDENCE_SCHEMA_VERSION,
        "status": "not_run",
        "required_functions": list(GLOBAL_WORKER_FUNCTION_SIGNATURES),
        "runtime_allowed_functions": list(RUNTIME_ROUTING_FUNCTION_SIGNATURES),
        "runtime": {},
        "global_worker": {"expected_role": worker_role},
    }
    try:
        async with pool.acquire() as runtime_connection:
            runtime_snapshot = await _role_snapshot(
                runtime_connection,
                expected_functions=(
                    *RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                    *GLOBAL_WORKER_FUNCTION_SIGNATURES,
                ),
            )
            runtime_probe = await _probe_global_function(
                runtime_connection,
                expected_access="denied",
            )
        worker_connection = await asyncpg.connect(_connection_dsn(worker_dsn))
        worker_snapshot = await _role_snapshot(
            worker_connection,
            expected_functions=(
                *RUNTIME_ROUTING_FUNCTION_SIGNATURES,
                *GLOBAL_WORKER_FUNCTION_SIGNATURES,
            ),
        )
        worker_probe = await _probe_global_function(
            worker_connection,
            expected_access="allowed",
        )
        result["runtime"] = {
            "expected_role": runtime_role or None,
            "role_snapshot": runtime_snapshot,
            "global_function_probe": runtime_probe,
        }
        result["global_worker"] = {
            "expected_role": worker_role,
            "role_snapshot": worker_snapshot,
            "global_function_probe": worker_probe,
        }
        # The JSON validator is intentionally a final-state validator and
        # requires ``status=pass``.  Do not validate the collector's initial
        # ``not_run`` sentinel; all snapshots/probes are complete here, so
        # switch to the candidate terminal state before recomputing the
        # contract.  The validator then writes the authoritative outcome.
        result["status"] = "pass"
        status, reason = _role_evidence_check(result)
        result["status"] = status
        if reason is not None:
            result["reason"] = reason
    except (asyncpg.PostgresError, OSError, RuntimeError, ValueError) as error:
        # Do not serialize DSNs, SQL text, or database error messages.
        result["status"] = "not_run"
        result["reason"] = "database role evidence could not be collected"
        result["error_type"] = type(error).__name__
    finally:
        if worker_connection is not None:
            await worker_connection.close()
    return result


async def _open_runtime(
    args: argparse.Namespace,
) -> tuple[asyncpg.Pool, PostgresRuntimeRepository, TenantRuntime]:
    dsn = _connection_dsn(os.environ["TRPC_REAL_DATABASE_DSN"])
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=max(4, args.workers + 1))
    repository = PostgresRuntimeRepository(pool)
    key = os.environ["TRPC_REAL_SESSION_HMAC_KEY"].encode()
    if len(key) < 32:
        await pool.close()
        raise ValueError("TRPC_REAL_SESSION_HMAC_KEY must contain at least 32 bytes")
    return (
        pool,
        repository,
        TenantRuntime(
            repository,
            routing_key=key,
            scheduler_version=SchedulerVersion.V2,
        ),
    )


async def _open_global_worker_observer_pool(args: argparse.Namespace) -> asyncpg.Pool:
    """Open the attested direct global-worker connection for fault observation."""

    dsn = _connection_dsn(os.environ[GLOBAL_WORKER_DATABASE_DSN_ENV])
    return await asyncpg.create_pool(dsn, min_size=1, max_size=max(2, args.workers))


def _envelope(
    *,
    binding_account_id: str,
    message_id: str,
    user_id: str,
    text: str,
) -> InboundEnvelope:
    return InboundEnvelope(
        channel=Channel.FEISHU,
        account_id=binding_account_id,
        external_message_id=message_id,
        external_user_id=user_id,
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text=text,
        occurred_at=datetime.now(UTC),
    )


async def _accept_batch(
    runtime: TenantRuntime,
    repository: PostgresRuntimeRepository,
    *,
    prefix: str,
    count: int,
    duplicates: int,
    user_id: str,
) -> dict[str, Any]:
    binding_id = os.environ["TRPC_REAL_BINDING_ID"]
    route = await repository.resolve_binding(binding_id)
    if route is None:
        raise LookupError("TRPC_REAL_BINDING_ID is not an active database binding")
    account_id = route.binding.account_id
    # Deliberately make message IDs non-monotonic to exercise ordering by the
    # database acceptance order rather than by provider payload ordering.
    order = list(range(count))
    if len(order) >= 2:
        order[0], order[1] = order[1], order[0]
    accepted: list[Acceptance] = []
    for index in order:
        accepted.append(
            await runtime.accept(
                binding_id,
                _envelope(
                    binding_account_id=account_id,
                    message_id=f"{prefix}-{index}",
                    user_id=user_id,
                    text=f"real runtime acceptance {index}",
                ),
            )
        )
    for index in range(min(duplicates, count)):
        accepted.append(
            await runtime.accept(
                binding_id,
                _envelope(
                    binding_account_id=account_id,
                    message_id=f"{prefix}-{index}",
                    user_id=user_id,
                    text=f"duplicate real runtime acceptance {index}",
                ),
            )
        )
    unique = {item.inbound_id for item in accepted}
    session_ids = {item.context.session_id for item in accepted}
    if len(session_ids) != 1:
        raise AssertionError("same-user messages resolved to more than one session")
    return {
        "accepted_calls": len(accepted),
        "unique_inbound_ids": sorted(unique),
        "duplicate_calls": len(accepted) - len(unique),
        "session_id": next(iter(session_ids)),
        "tenant_id": os.environ["TRPC_REAL_TENANT_ID"],
        "message_order": order,
    }


async def _scoped_fetch(
    pool: asyncpg.Pool, tenant_id: str, query: str, *args: Any
) -> list[asyncpg.Record]:
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        return list(await connection.fetch(query, *args))


async def _scoped_fetchval(pool: asyncpg.Pool, tenant_id: str, query: str, *args: Any) -> Any:
    rows = await _scoped_fetch(pool, tenant_id, query, *args)
    if not rows:
        return None
    return rows[0][0]


async def _session_state(
    pool: asyncpg.Pool, *, tenant_id: str, session_id: str, inbound_ids: Sequence[str]
) -> dict[str, Any]:
    uuids = [UUID(item) for item in inbound_ids]
    rows = await _scoped_fetch(
        pool,
        tenant_id,
        """
        SELECT status, count(*)::bigint AS count
          FROM inbound_messages
         WHERE tenant_id=$1 AND inbound_id = ANY($2::uuid[])
         GROUP BY status
        """,
        tenant_id,
        uuids,
    )
    statuses = {str(row["status"]): int(row["count"]) for row in rows}
    session = await _scoped_fetch(
        pool,
        tenant_id,
        (
            "SELECT next_sequence, version, lease_owner, lease_epoch FROM sessions "
            "WHERE tenant_id=$1 AND session_id=$2"
        ),
        tenant_id,
        session_id,
    )
    events = await _scoped_fetch(
        pool,
        tenant_id,
        (
            "SELECT sequence FROM session_events "
            "WHERE tenant_id=$1 AND session_id=$2 ORDER BY sequence"
        ),
        tenant_id,
        session_id,
    )
    turns = await _scoped_fetchval(
        pool,
        tenant_id,
        """
        SELECT count(*)::bigint FROM session_turns
         WHERE tenant_id=$1 AND session_id=$2
           AND inbound_id = ANY($3::uuid[])
        """,
        tenant_id,
        session_id,
        uuids,
    )
    mailbox_rows = await _scoped_fetch(
        pool,
        tenant_id,
        """
        SELECT status, accepted_sequence, resolved_sequence,
               processing_sequence, queue_generation, lease_epoch,
               lease_owner, attempt, retry_count
          FROM session_mailboxes
         WHERE tenant_id=$1 AND session_id=$2
        """,
        tenant_id,
        session_id,
    )
    mailbox_items = await _scoped_fetch(
        pool,
        tenant_id,
        """
        SELECT count(*)::bigint AS item_count,
               count(*) FILTER (WHERE resolved_at IS NOT NULL)::bigint AS resolved_count,
               count(*) FILTER (WHERE resolved_at IS NULL)::bigint AS unresolved_count
          FROM session_mailbox_items
         WHERE tenant_id=$1 AND session_id=$2
        """,
        tenant_id,
        session_id,
    )
    mailbox_ready_outbox = await _scoped_fetchval(
        pool,
        tenant_id,
        """
        SELECT count(*)::bigint
          FROM outbox_events
         WHERE tenant_id=$1 AND aggregate_type='session'
           AND aggregate_id=$2 AND event_type='session.ready.v2'
           AND published_at IS NOT NULL
        """,
        tenant_id,
        session_id,
    )
    sequence_values = [int(row["sequence"]) for row in events]
    expected = list(range(1, len(sequence_values) + 1))
    mailbox = mailbox_rows[0] if mailbox_rows else None
    item_state = mailbox_items[0] if mailbox_items else None
    mailbox_item_count = int(item_state["item_count"]) if item_state else 0
    mailbox_resolved_count = int(item_state["resolved_count"]) if item_state else 0
    mailbox_unresolved_count = int(item_state["unresolved_count"]) if item_state else 0
    mailbox_complete = bool(
        mailbox
        and str(mailbox["status"]) == "IDLE"
        and int(mailbox["accepted_sequence"]) == int(mailbox["resolved_sequence"])
        and mailbox["processing_sequence"] is None
        and mailbox_item_count == int(mailbox["accepted_sequence"])
        and mailbox_resolved_count == mailbox_item_count
        and mailbox_unresolved_count == 0
        and int(mailbox_ready_outbox or 0) > 0
    )
    mailbox_v2 = {
        "status": "pass" if mailbox_complete else "not_run",
        "schema_version": 2,
        "mailbox_row_present": mailbox is not None,
        "status_value": str(mailbox["status"]) if mailbox else None,
        "accepted_sequence": int(mailbox["accepted_sequence"]) if mailbox else None,
        "resolved_sequence": int(mailbox["resolved_sequence"]) if mailbox else None,
        "processing_sequence": (
            int(mailbox["processing_sequence"])
            if mailbox and mailbox["processing_sequence"] is not None
            else None
        ),
        "queue_generation": int(mailbox["queue_generation"]) if mailbox else None,
        "lease_epoch": int(mailbox["lease_epoch"]) if mailbox else None,
        "item_count": mailbox_item_count,
        "resolved_item_count": mailbox_resolved_count,
        "unresolved_item_count": mailbox_unresolved_count,
        "published_ready_outbox": int(mailbox_ready_outbox or 0),
        "completion_verified": mailbox_complete,
    }
    return {
        "inbound_statuses": statuses,
        "next_sequence": int(session[0]["next_sequence"]) if session else None,
        "version": int(session[0]["version"]) if session else None,
        "lease_epoch": int(session[0]["lease_epoch"]) if session else None,
        "lease_owner_present": bool(session and session[0]["lease_owner"]),
        "event_count": len(sequence_values),
        "turn_count": int(turns or 0),
        "event_sequences_contiguous": sequence_values == expected,
        "scheduler_version": SchedulerVersion.V2.value,
        # Keep the legacy key as a scheduler-neutral alias for report readers.
        "published_inbound_outbox": int(mailbox_ready_outbox or 0),
        "published_scheduler_outbox": int(mailbox_ready_outbox or 0),
        "mailbox_v2": mailbox_v2,
        "mailbox_v2_completion": mailbox_v2,
    }


async def _wait_for_batch(
    pool: asyncpg.Pool,
    batch: dict[str, Any],
    *,
    wait_seconds: float,
    require_outbox: bool = True,
) -> dict[str, Any]:
    deadline = time.monotonic() + wait_seconds
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            latest = await _session_state(
                pool,
                tenant_id=str(batch["tenant_id"]),
                session_id=str(batch["session_id"]),
                inbound_ids=batch["unique_inbound_ids"],
            )
            statuses = latest["inbound_statuses"]
            committed = int(statuses.get("committed", 0))
            expected = len(batch["unique_inbound_ids"])
            turn_count = int(latest.get("turn_count", 0))
            mailbox_completion = latest.get("mailbox_v2_completion")
            if turn_count > expected:
                return {
                    "status": "fail",
                    "reason": "batch committed more turns than unique inbound ids",
                    "state": latest,
                }
            if (
                committed == expected
                and turn_count == expected
                and not latest["lease_owner_present"]
                and latest["event_sequences_contiguous"]
                and isinstance(mailbox_completion, Mapping)
                and mailbox_completion.get("status") == "pass"
                and (not require_outbox or latest["published_scheduler_outbox"] >= 1)
            ):
                return {"status": "pass", "state": latest}
        except (asyncpg.PostgresError, OSError):
            pass
        await asyncio.sleep(0.5)
    return {
        "status": "fail",
        "reason": "batch did not reach committed/contiguous state before timeout",
        "state": latest,
    }


def _redis_text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _stream_id_at_least(observed: str, target: str) -> bool:
    try:
        observed_parts = tuple(int(item) for item in observed.split("-", 1))
        target_parts = tuple(int(item) for item in target.split("-", 1))
    except (TypeError, ValueError):
        return False
    return len(observed_parts) == 2 and len(target_parts) == 2 and observed_parts >= target_parts


async def _redis_group_state(redis: Any, *, stream: str, group: str) -> dict[str, str] | None:
    groups = await redis.xinfo_groups(stream)
    for item in groups:
        if not isinstance(item, dict):
            continue
        normalized = {_redis_text(key): value for key, value in item.items()}
        if _redis_text(normalized.get("name", "")) == group:
            return {
                "name": group,
                "last_delivered_id": _redis_text(normalized.get("last-delivered-id", "0-0")),
            }
    return None


async def _redis_pending_contains(redis: Any, *, stream: str, group: str, stream_id: str) -> bool:
    rows = await redis.xpending_range(stream, group, stream_id, stream_id, 10)
    for row in rows or ():
        if isinstance(row, dict):
            observed = row.get("message_id", row.get(b"message_id", ""))
        elif isinstance(row, (tuple, list)) and row:
            observed = row[0]
        else:
            continue
        if _redis_text(observed) == stream_id:
            return True
    return False


async def _active_duplicate_publish_probe(
    pool: asyncpg.Pool,
    batch: dict[str, Any],
    *,
    stream: str,
    group: str,
    wait_seconds: float,
) -> dict[str, Any]:
    """Publish one exact authoritative outbox task and prove it was consumed once.

    This intentionally bypasses the normal dedupe Lua script: the purpose is
    to exercise duplicate delivery, not to prove that the dispatcher dedupes
    its own retry.  The report contains only routing/identity metadata.
    """

    inbound_ids = batch.get("unique_inbound_ids", ())
    tenant_id = str(batch.get("tenant_id", ""))
    if not tenant_id or not inbound_ids:
        return {"status": "not_run", "reason": "duplicate probe has no accepted inbound"}
    inbound_id = str(inbound_ids[0])
    session_id = str(batch.get("session_id", ""))
    if not session_id:
        return {"status": "not_run", "reason": "duplicate probe has no session identity"}
    try:
        rows = await _scoped_fetch(
            pool,
            tenant_id,
            """
            SELECT outbox_id, tenant_id, event_type, aggregate_id,
                   payload_json, trace_headers
             FROM outbox_events
             WHERE tenant_id=$1 AND aggregate_type='session' AND aggregate_id=$2
               AND event_type='session.ready.v2'
               AND published_at IS NOT NULL
             ORDER BY created_at ASC
             LIMIT 1
            """,
            tenant_id,
            session_id,
        )
    except asyncpg.PostgresError as error:
        return {
            "status": "not_run",
            "reason": f"authoritative outbox read unavailable: {type(error).__name__}",
        }
    if not rows:
        return {"status": "not_run", "reason": "authoritative inbound outbox row was not found"}
    row = rows[0]
    outbox_id = str(row["outbox_id"])
    if str(row["tenant_id"]) != tenant_id or str(row["aggregate_id"]) != session_id:
        return {
            "status": "fail",
            "reason": "authoritative outbox row does not match the accepted inbound",
        }
    redis: Any | None = None
    try:
        redis = redis_async.from_url(
            os.environ["TRPC_REAL_REDIS_URL"],
            decode_responses=False,
        )
        before_group = await _redis_group_state(redis, stream=stream, group=group)
        if before_group is None:
            return {
                "status": "not_run",
                "reason": "configured Redis consumer group was not found",
                "stream": stream,
                "group": group,
                "outbox_id": outbox_id,
                "inbound_id": inbound_id,
            }
        payload = row["payload_json"]
        trace_headers = row["trace_headers"] or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(trace_headers, str):
            trace_headers = json.loads(trace_headers)
        if not isinstance(payload, dict) or not isinstance(trace_headers, dict):
            return {"status": "fail", "reason": "authoritative outbox payload fields are invalid"}
        generation = payload.get("generation")
        priority = payload.get("priority")
        trace_id = payload.get("trace_id")
        created_at = payload.get("created_at")
        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 1
            or not isinstance(priority, int)
            or isinstance(priority, bool)
            or priority < 0
            or not isinstance(trace_id, str)
            or not trace_id
            or not isinstance(created_at, str)
            or not created_at
        ):
            return {"status": "fail", "reason": "SessionReady outbox payload is invalid"}
        duplicate_stream_id = await redis.xadd(
            stream,
            {
                "event_id": outbox_id,
                "tenant_id": str(row["tenant_id"]),
                "session_id": session_id,
                "generation": str(generation),
                "priority": str(priority),
                "trace_id": trace_id,
                "created_at": created_at,
            },
        )
        duplicate_stream_id = _redis_text(duplicate_stream_id)
        if not duplicate_stream_id or duplicate_stream_id == "None":
            return {"status": "fail", "reason": "Redis XADD did not return a stream id"}
        deadline = time.monotonic() + wait_seconds
        latest_group: dict[str, str] | None = before_group
        latest_state: dict[str, Any] = {}
        while time.monotonic() < deadline:
            latest_group = await _redis_group_state(redis, stream=stream, group=group)
            pending = await _redis_pending_contains(
                redis,
                stream=stream,
                group=group,
                stream_id=duplicate_stream_id,
            )
            latest_state = await _session_state(
                pool,
                tenant_id=tenant_id,
                session_id=str(batch["session_id"]),
                inbound_ids=(inbound_id,),
            )
            turn_count = int(latest_state.get("turn_count", 0))
            if turn_count > 1:
                return {
                    "status": "fail",
                    "reason": "active duplicate publish created more than one session turn",
                    "stream": stream,
                    "group": group,
                    "outbox_id": outbox_id,
                    "inbound_id": inbound_id,
                    "session_id": session_id,
                    "duplicate_stream_id": duplicate_stream_id,
                    "turn_count": turn_count,
                    "group_last_delivered_id": (
                        latest_group.get("last_delivered_id") if latest_group else None
                    ),
                    "pending_duplicate": pending,
                }
            group_consumed = bool(
                latest_group
                and _stream_id_at_least(latest_group["last_delivered_id"], duplicate_stream_id)
            )
            if group_consumed and not pending and turn_count == 1:
                return {
                    "status": "pass",
                    "stream": stream,
                    "group": group,
                    "outbox_id": outbox_id,
                    "inbound_id": inbound_id,
                    "session_id": session_id,
                    "duplicate_stream_id": duplicate_stream_id,
                    "group_last_delivered_id": (
                        latest_group.get("last_delivered_id") if latest_group else None
                    ),
                    "pending_duplicate": False,
                    "turn_count": turn_count,
                    "turn_count_exactly_one": True,
                }
            await asyncio.sleep(0.25)
        return {
            "status": "not_run",
            "reason": "consumer group did not consume and acknowledge the duplicate stream id",
            "stream": stream,
            "group": group,
            "outbox_id": outbox_id,
            "inbound_id": inbound_id,
            "duplicate_stream_id": duplicate_stream_id,
            "group_last_delivered_id": (
                latest_group.get("last_delivered_id") if latest_group else None
            ),
            "pending_duplicate": await _redis_pending_contains(
                redis,
                stream=stream,
                group=group,
                stream_id=duplicate_stream_id,
            ),
            "turn_count": int(latest_state.get("turn_count", 0)),
            "turn_count_exactly_one": int(latest_state.get("turn_count", 0)) == 1,
        }
    except (asyncpg.PostgresError, RedisError, OSError, TypeError, ValueError, KeyError) as error:
        return {
            "status": "not_run",
            "reason": f"duplicate Redis publish probe unavailable: {type(error).__name__}",
            "stream": stream,
            "group": group,
            "outbox_id": outbox_id,
            "inbound_id": inbound_id,
        }
    finally:
        if redis is not None:
            try:
                await redis.aclose()
            except (RedisError, OSError):
                pass


async def _active_turn_evidence(
    pool: asyncpg.Pool,
    tenant_id: str,
    session_id: str,
    inbound_ids: Sequence[str],
    *,
    turn_id: str | None = None,
    inbound_id: str | None = None,
    max_started_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    """Return processing evidence scoped to this acceptance batch only."""

    if not inbound_ids:
        return None
    if max_started_age_seconds is not None and max_started_age_seconds <= 0:
        raise ValueError("max_started_age_seconds must be positive")
    rows = await _scoped_fetch(
        pool,
        tenant_id,
        """
        SELECT session.lease_owner, session.lease_epoch,
               turn.turn_id, turn.inbound_id, turn.attempt, turn.fencing_token,
               turn.started_at,
               GREATEST(
                   0,
                   EXTRACT(EPOCH FROM (statement_timestamp() - turn.started_at))
               ) AS processing_age_seconds
          FROM session_turns turn
          JOIN sessions session
            ON session.tenant_id=turn.tenant_id AND session.session_id=turn.session_id
         WHERE turn.tenant_id=$1 AND turn.session_id=$2
           AND turn.inbound_id = ANY($3::uuid[])
           AND turn.status='processing'
           AND session.lease_owner IS NOT NULL
           AND turn.fencing_token=session.lease_epoch
           AND ($4::uuid IS NULL OR turn.turn_id=$4::uuid)
           AND ($5::uuid IS NULL OR turn.inbound_id=$5::uuid)
           AND (
               $6::double precision IS NULL
               OR turn.started_at >= statement_timestamp() - ($6 * interval '1 second')
           )
         ORDER BY turn.started_at DESC
         LIMIT 1
        """,
        tenant_id,
        session_id,
        [UUID(item) for item in inbound_ids],
        UUID(turn_id) if turn_id else None,
        UUID(inbound_id) if inbound_id else None,
        max_started_age_seconds,
    )
    if not rows:
        return None
    evidence = dict(rows[0])
    for name in ("turn_id", "inbound_id"):
        if evidence.get(name) is not None:
            evidence[name] = str(evidence[name])
    if evidence.get("started_at") is not None:
        evidence["started_at"] = _utc_timestamp(evidence["started_at"])
    if evidence.get("processing_age_seconds") is not None:
        evidence["processing_age_seconds"] = float(evidence["processing_age_seconds"])
    return evidence


async def _turn_state_evidence(
    pool: asyncpg.Pool,
    *,
    tenant_id: str,
    session_id: str,
    turn_id: str,
    inbound_id: str,
) -> dict[str, Any] | None:
    """Return the exact targeted turn state after worker termination."""

    rows = await _scoped_fetch(
        pool,
        tenant_id,
        """
        SELECT turn.turn_id, turn.inbound_id, turn.status, turn.attempt,
               turn.started_at, turn.committed_at,
               session.lease_owner, session.lease_epoch
          FROM session_turns turn
          JOIN sessions session
            ON session.tenant_id=turn.tenant_id AND session.session_id=turn.session_id
         WHERE turn.tenant_id=$1 AND turn.session_id=$2
           AND turn.turn_id=$3::uuid AND turn.inbound_id=$4::uuid
         LIMIT 1
        """,
        tenant_id,
        session_id,
        UUID(turn_id),
        UUID(inbound_id),
    )
    if not rows:
        return None
    evidence = dict(rows[0])
    for name in ("turn_id", "inbound_id"):
        if evidence.get(name) is not None:
            evidence[name] = str(evidence[name])
    for name in ("started_at", "committed_at"):
        if evidence.get(name) is not None:
            evidence[name] = _utc_timestamp(evidence[name])
    return evidence


async def _active_turn_owner(
    pool: asyncpg.Pool,
    tenant_id: str,
    session_id: str | None = None,
    inbound_ids: Sequence[str] = (),
) -> str | None:
    """Return a lease owner only for a supplied session and batch.

    The optional arguments preserve the helper's old call shape while making
    an unscoped query impossible: without both filters it returns ``None``.
    """

    if session_id is None or not inbound_ids:
        return None
    evidence = await _active_turn_evidence(pool, tenant_id, session_id, inbound_ids)
    owner = evidence.get("lease_owner") if evidence else None
    return str(owner) if owner else None


async def _wait_for_takeover(
    pool: asyncpg.Pool,
    *,
    tenant_id: str,
    session_id: str,
    inbound_ids: Sequence[str],
    killed_owner: str,
    previous_epoch: int,
    target_turn_id: str,
    target_inbound_id: str,
    wait_seconds: float,
) -> dict[str, Any]:
    """Observe a live replacement lease before the session is committed.

    A later ``attempt > 1`` by itself is insufficient: the report must show a
    newer epoch and a different live owner while the turn is processing.
    """

    deadline = time.monotonic() + wait_seconds
    latest: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        latest = await _active_turn_evidence(
            pool,
            tenant_id,
            session_id,
            inbound_ids,
            turn_id=target_turn_id,
            inbound_id=target_inbound_id,
            max_started_age_seconds=MAX_KILL_TARGET_AGE_SECONDS,
        )
        if latest:
            epoch = int(latest.get("lease_epoch") or 0)
            owner = str(latest.get("lease_owner") or "")
            attempt = int(latest.get("attempt") or 0)
            if epoch > previous_epoch and owner and owner != killed_owner and attempt > 1:
                return {
                    "status": "pass",
                    "lease_epoch_before": previous_epoch,
                    "lease_epoch_after": epoch,
                    "lease_epoch_monotonic": True,
                    "takeover_owner": owner,
                    "killed_owner": killed_owner,
                    "takeover_owner_differs": True,
                    "attempt": attempt,
                    "turn_id": str(latest.get("turn_id")),
                }
        await asyncio.sleep(ACTIVE_TURN_POLL_INTERVAL_SECONDS)
    return {
        "status": "not_run",
        "reason": "replacement lease was not observed while processing",
        "lease_epoch_before": previous_epoch,
        "latest_observation": {key: str(value) for key, value in (latest or {}).items()},
    }


async def _probe_stale_fencing_rejection(
    pool: asyncpg.Pool,
    repository: PostgresRuntimeRepository,
    *,
    batch: dict[str, Any],
    old_evidence: dict[str, Any] | None,
    takeover_observation: dict[str, Any],
) -> dict[str, Any]:
    """Attempt a commit with the killed worker's exact lease and verify fencing.

    The probe is only meaningful while the replacement is still processing the
    same turn.  A caught ``FencingConflict`` without that post-check is not
    accepted as evidence because the replacement could have lost its lease in
    the meantime.
    """

    if takeover_observation.get("status") != "pass" or old_evidence is None:
        return {"status": "not_run", "reason": "replacement processing lease was not observed"}
    tenant_id = str(batch.get("tenant_id", ""))
    session_id = str(batch.get("session_id", ""))
    inbound_ids = tuple(str(item) for item in batch.get("unique_inbound_ids", ()))
    old_owner = str(old_evidence.get("lease_owner") or "")
    old_epoch = int(old_evidence.get("lease_epoch") or 0)
    old_turn_id = str(old_evidence.get("turn_id") or "")
    inbound_id = str(old_evidence.get("inbound_id") or (inbound_ids[0] if inbound_ids else ""))
    new_owner = str(takeover_observation.get("takeover_owner") or "")
    if not all((tenant_id, session_id, inbound_id, old_owner, old_turn_id, old_epoch, new_owner)):
        return {"status": "not_run", "reason": "old/new lease evidence is incomplete"}
    try:
        current = await _active_turn_evidence(
            pool,
            tenant_id,
            session_id,
            inbound_ids,
            turn_id=old_turn_id,
            inbound_id=inbound_id,
        )
        if not current or str(current.get("lease_owner")) != new_owner:
            return {
                "status": "not_run",
                "reason": "replacement owner was no longer current before stale probe",
            }
        current_epoch = int(current.get("lease_epoch") or 0)
        if current_epoch <= old_epoch or str(current.get("turn_id")) != old_turn_id:
            return {
                "status": "not_run",
                "reason": "replacement epoch/turn did not supersede the killed lease",
            }
        acceptance = await repository.get_acceptance(tenant_id, inbound_id)
        snapshot = await repository.get_session_snapshot(tenant_id, session_id)
        if acceptance is None or snapshot is None:
            return {"status": "not_run", "reason": "acceptance or session snapshot was unavailable"}
        old_lease = SessionLease(
            tenant_id=tenant_id,
            session_id=session_id,
            turn_id=old_turn_id,
            inbound_id=inbound_id,
            worker_id=old_owner,
            fencing_token=old_epoch,
            expires_at=datetime.now(UTC) + timedelta(seconds=30),
            attempt=int(old_evidence.get("attempt") or 1),
            snapshot=snapshot,
        )
        await repository.commit(
            TurnCommit(
                context=acceptance.context,
                lease=old_lease,
                state=dict(snapshot.state),
                events=(),
            )
        )
    except FencingConflict:
        try:
            current_after = await _active_turn_evidence(
                pool,
                tenant_id,
                session_id,
                inbound_ids,
                turn_id=old_turn_id,
                inbound_id=inbound_id,
            )
        except (asyncpg.PostgresError, OSError, RuntimeError, ValueError, TypeError) as error:
            return {
                "status": "not_run",
                "reason": f"post-conflict lease verification unavailable: {type(error).__name__}",
                "old_owner": old_owner,
                "new_owner": new_owner,
                "old_lease_epoch": old_epoch,
            }
        owner_still_current = bool(
            current_after
            and str(current_after.get("lease_owner")) == new_owner
            and int(current_after.get("lease_epoch") or 0) > old_epoch
            and str(current_after.get("turn_id")) == old_turn_id
        )
        if not owner_still_current:
            return {
                "status": "not_run",
                "reason": (
                    "FencingConflict was caught after replacement lease was no longer current"
                ),
                "old_owner": old_owner,
                "new_owner": new_owner,
                "old_lease_epoch": old_epoch,
            }
        return {
            "status": "pass",
            "old_owner": old_owner,
            "new_owner": new_owner,
            "old_lease_epoch": old_epoch,
            "new_lease_epoch": int(current_after["lease_epoch"]) if current_after else None,
            "turn_id": old_turn_id,
            "owner_still_current": True,
            "fencing_conflict_caught": True,
            "old_token_rejected": True,
            "old_fencing_token": old_epoch,
        }
    except (asyncpg.PostgresError, OSError, RuntimeError, ValueError, TypeError) as error:
        return {
            "status": "not_run",
            "reason": f"stale-token probe unavailable: {type(error).__name__}",
            "old_owner": old_owner,
            "new_owner": new_owner,
            "old_lease_epoch": old_epoch,
        }
    return {
        "status": "fail",
        "reason": "stale worker commit unexpectedly succeeded",
        "old_owner": old_owner,
        "new_owner": new_owner,
        "old_lease_epoch": old_epoch,
        "old_token_rejected": False,
    }


def _worker_container_for_owner(
    workers: Sequence[dict[str, Any]], lease_owner: str
) -> dict[str, Any] | None:
    hostname = lease_owner.removeprefix("worker-")
    return next(
        (worker for worker in workers if str(worker.get("container_id", "")).startswith(hostname)),
        None,
    )


async def _wait_for_worker_termination(
    args: argparse.Namespace,
    container_id: str,
    *,
    wait_seconds: float,
) -> dict[str, Any]:
    """Observe a post-kill Docker state before accepting process termination.

    ``docker kill`` returning zero only acknowledges the signal delivery
    request.  The worker evidence boundary requires a subsequent inspect that
    observes the same container in Docker's exited/dead state.
    """

    deadline = time.monotonic() + max(0.1, wait_seconds)
    last_error: str | None = None
    while time.monotonic() < deadline:
        try:
            inspected = _inspect_container(args, container_id, allow_stopped=True)
        except (OSError, RuntimeError, ValueError) as error:
            last_error = type(error).__name__
        else:
            status = str(inspected.get("status", "")).lower()
            if status in {"exited", "dead"}:
                return {
                    "status": "pass",
                    "termination_verified": True,
                    "termination_status": status,
                    "termination_pid": inspected.get("pid"),
                    "termination_observed_at": _utc_timestamp(),
                }
            last_error = f"container remained {status or 'unknown'}"
        await asyncio.sleep(
            min(WORKER_TERMINATION_POLL_INTERVAL_SECONDS, max(0.0, deadline - time.monotonic()))
        )
    return {
        "status": "not_run",
        "reason": "worker termination was not observed in Docker",
        "termination_verified": False,
        **({"last_inspect_error": last_error} if last_error else {}),
    }


async def _kill_worker(args: argparse.Namespace, container_id: str) -> dict[str, Any]:
    if not args.allow_process_kill:
        return {"status": "not_run", "reason": "--allow-process-kill was not supplied"}
    result = _command_result(("docker", "kill", "--signal", "SIGKILL", container_id), timeout=30)
    if result.get("status") != "pass":
        return {"status": "fail", "reason": "docker kill failed", **result}
    termination = await _wait_for_worker_termination(
        args,
        container_id,
        wait_seconds=min(30.0, max(1.0, float(args.timeout_seconds) / 4)),
    )
    return {
        **termination,
        "killed_container_id": container_id,
    }


async def _load_phase(
    args: argparse.Namespace,
    pool: asyncpg.Pool,
    repository: PostgresRuntimeRepository,
    runtime: TenantRuntime,
    workers: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    started = time.perf_counter()
    stage_markers: list[dict[str, Any]] = []
    batch = await _accept_batch(
        runtime,
        repository,
        prefix=f"real-load-{uuid4().hex}",
        count=args.messages,
        duplicates=args.duplicates,
        user_id=f"real-runtime-user-{uuid4().hex}",
    )
    stage_markers.append(
        _stage_marker(
            "acceptance.persisted",
            "pass",
            unique_inbound_count=len(batch["unique_inbound_ids"]),
        )
    )
    kill: dict[str, Any] = {"status": "not_run", "reason": "worker kill not requested"}
    active_evidence: dict[str, Any] | None = None
    takeover_observation: dict[str, Any] = {
        "status": "not_run",
        "reason": "worker kill was not executed",
    }
    stale_token_rejection: dict[str, Any] = {
        "status": "not_run",
        "reason": "worker kill/takeover was not executed",
    }
    if args.kill_worker:
        # A kill is only considered evidence when a real turn is in flight.
        active_deadline = time.monotonic() + min(30.0, args.timeout_seconds / 4)
        active_owner: str | None = None
        while time.monotonic() < active_deadline:
            active_evidence = await _active_turn_evidence(
                pool,
                str(batch["tenant_id"]),
                str(batch["session_id"]),
                batch["unique_inbound_ids"],
                max_started_age_seconds=MAX_KILL_TARGET_AGE_SECONDS,
            )
            active_owner = (
                str(active_evidence["lease_owner"])
                if active_evidence and active_evidence.get("lease_owner")
                else None
            )
            if active_owner:
                break
            await asyncio.sleep(ACTIVE_TURN_POLL_INTERVAL_SECONDS)
        stage_markers.append(
            _stage_marker(
                "turn.processing_observed",
                "pass" if active_owner else "not_run",
                worker_id=active_owner,
                lease_epoch=(active_evidence or {}).get("lease_epoch"),
                turn_id=(str(active_evidence["turn_id"]) if active_evidence else None),
                reason=(
                    None if active_owner else "no processing turn observed before kill deadline"
                ),
            )
        )
        target = _worker_container_for_owner(workers, active_owner) if active_owner else None
        if target is not None:
            assert active_evidence is not None
            kill_requested_at = _utc_timestamp()
            stage_markers.append(
                _stage_marker(
                    "worker.kill_requested",
                    "pass",
                    worker_id=active_owner,
                )
            )
            kill = await _kill_worker(args, str(target["container_id"]))
            kill["kill_requested_at"] = kill_requested_at
            kill["killed_container_pid"] = target.get("pid")
            kill["killed_container_image_id"] = target.get("image_id")
            kill["killed_container_source_fingerprint"] = target.get("source_fingerprint")
            kill["active_worker_id"] = active_owner
            kill["active_turns_observed_before_kill"] = 1
            kill["old_owner"] = active_owner
            kill["old_lease_epoch"] = int(active_evidence.get("lease_epoch") or 0)
            kill["old_fencing_token"] = int(active_evidence.get("fencing_token") or 0)
            kill["kill_completed_at"] = str(kill.get("termination_observed_at") or _utc_timestamp())
            stage_markers.append(
                _stage_marker(
                    "worker.kill_completed",
                    str(kill.get("status", "not_run")),
                    reason=(
                        None
                        if kill.get("status") == "pass"
                        else str(kill.get("reason", "worker kill did not complete"))
                    ),
                )
            )
            if kill.get("status") == "pass" and active_evidence is not None:
                assert active_owner is not None
                target_turn_id = str(active_evidence.get("turn_id") or "")
                target_inbound_id = str(active_evidence.get("inbound_id") or "")
                target_state = await _turn_state_evidence(
                    pool,
                    tenant_id=str(batch["tenant_id"]),
                    session_id=str(batch["session_id"]),
                    turn_id=target_turn_id,
                    inbound_id=target_inbound_id,
                )
                kill["target_turn_state_after_kill"] = target_state
                target_status = str(target_state.get("status") or "") if target_state else ""
                if target_state is None:
                    takeover_observation = {
                        "status": "not_run",
                        "reason": "target turn evidence was missing after worker termination",
                        "target_turn_id": target_turn_id,
                        "target_inbound_id": target_inbound_id,
                        "target_status_after_kill": "missing",
                    }
                elif target_status == "committed":
                    takeover_observation = {
                        "status": "not_run",
                        "reason": (
                            "kill window missed: targeted turn committed before worker "
                            "termination was observed"
                        ),
                        "target_turn_id": target_turn_id,
                        "target_inbound_id": target_inbound_id,
                        "target_status_after_kill": target_status,
                    }
                elif target_status != "processing":
                    takeover_observation = {
                        "status": "not_run",
                        "reason": (
                            "targeted turn entered an unexpected state after worker termination: "
                            f"{target_status or 'unknown'}"
                        ),
                        "target_turn_id": target_turn_id,
                        "target_inbound_id": target_inbound_id,
                        "target_status_after_kill": target_status or "unknown",
                    }
                else:
                    kill["takeover_wait_started_at"] = _utc_timestamp()
                    takeover_observation = await _wait_for_takeover(
                        pool,
                        tenant_id=str(batch["tenant_id"]),
                        session_id=str(batch["session_id"]),
                        inbound_ids=batch["unique_inbound_ids"],
                        killed_owner=active_owner,
                        previous_epoch=int(active_evidence.get("lease_epoch") or 0),
                        target_turn_id=target_turn_id,
                        target_inbound_id=target_inbound_id,
                        wait_seconds=min(120.0, max(30.0, args.timeout_seconds / 2)),
                    )
                    if takeover_observation.get("status") == "pass":
                        stale_token_rejection = await _probe_stale_fencing_rejection(
                            pool,
                            repository,
                            batch=batch,
                            old_evidence=active_evidence,
                            takeover_observation=takeover_observation,
                        )
                    kill["takeover_wait_completed_at"] = _utc_timestamp()
                kill["takeover_decision_completed_at"] = _utc_timestamp()
            kill["old_token_rejected"] = stale_token_rejection.get("status") == "pass"
            kill["new_owner"] = takeover_observation.get("takeover_owner")
            kill["new_lease_epoch"] = takeover_observation.get("lease_epoch_after")
        elif active_owner:
            kill = {
                "status": "not_run",
                "reason": "active lease owner did not map to an inspected worker container",
                "active_worker_id": active_owner,
            }
            stage_markers.extend(
                (
                    _stage_marker(
                        "worker.kill_requested",
                        "not_run",
                        reason="active lease owner did not map to an inspected worker container",
                    ),
                    _stage_marker(
                        "worker.kill_completed",
                        "not_run",
                        reason="worker kill was not attempted",
                    ),
                )
            )
        else:
            kill = {
                "status": "not_run",
                "reason": "no active turn observed; kill was not claimed as fencing evidence",
            }
            stage_markers.extend(
                (
                    _stage_marker(
                        "worker.kill_requested",
                        "not_run",
                        reason="no active turn observed",
                    ),
                    _stage_marker(
                        "worker.kill_completed",
                        "not_run",
                        reason="worker kill was not attempted",
                    ),
                )
            )
    else:
        stage_markers.extend(
            _planned_stage_markers(
                (
                    "turn.processing_observed",
                    "worker.kill_requested",
                    "worker.kill_completed",
                ),
                reason="worker kill was not requested",
            )
        )
    completion = await _wait_for_batch(pool, batch, wait_seconds=args.timeout_seconds)
    after_workers: tuple[dict[str, Any], ...] = ()
    try:
        after_workers = tuple(_inspect_container(args, item) for item in _worker_ids(args))
    except (OSError, RuntimeError, ValueError):
        after_workers = ()
    killed_container_id = str(kill.get("killed_container_id", ""))
    survivor_containers = tuple(
        item for item in after_workers if item.get("container_id") != killed_container_id
    )
    healthy_survivors = tuple(
        item
        for item in survivor_containers
        if item.get("status") == "running" and item.get("health") == "healthy"
    )
    fencing: dict[str, Any] = {
        "status": "not_run",
        "reason": "worker kill was not executed",
        "old_token_rejection": stale_token_rejection,
        "old_token_rejected": False,
    }
    if kill.get("status") == "pass":
        attempts = await _scoped_fetchval(
            pool,
            str(batch["tenant_id"]),
            """
            SELECT count(*)::bigint FROM session_turns
             WHERE tenant_id=$1 AND inbound_id = ANY($2::uuid[]) AND attempt > 1
            """,
            str(batch["tenant_id"]),
            [UUID(item) for item in batch["unique_inbound_ids"]],
        )
        surviving_healthy = {item["container_id"] for item in healthy_survivors}
        takeover_observed = takeover_observation.get("status") == "pass"
        takeover_owner_differs = bool(takeover_observation.get("takeover_owner_differs"))
        epoch_monotonic = bool(takeover_observation.get("lease_epoch_monotonic"))
        takeover_owner = str(takeover_observation.get("takeover_owner") or "")
        takeover_survivor = (
            _worker_container_for_owner(healthy_survivors, takeover_owner)
            if takeover_owner
            else None
        )
        all_survivors_healthy = bool(survivor_containers) and len(healthy_survivors) == len(
            survivor_containers
        )
        fencing = {
            "status": "not_run",
            "attempts_after_takeover": int(attempts or 0),
            "surviving_worker_containers": len(surviving_healthy),
            "surviving_healthy_worker_containers": sorted(surviving_healthy),
            "killed_container_excluded": killed_container_id not in surviving_healthy,
            "takeover_observed": takeover_observed,
            "takeover_owner": takeover_observation.get("takeover_owner"),
            "killed_owner": active_evidence.get("lease_owner") if active_evidence else None,
            "takeover_owner_differs": takeover_owner_differs,
            "lease_epoch_before": takeover_observation.get("lease_epoch_before"),
            "lease_epoch_after": takeover_observation.get("lease_epoch_after"),
            "lease_epoch_monotonic": epoch_monotonic,
            "takeover_owner_mapped_to_healthy_survivor": takeover_survivor is not None,
            "all_survivors_running_healthy": all_survivors_healthy,
            "old_token_rejection": stale_token_rejection,
            "old_token_rejected": stale_token_rejection.get("status") == "pass",
            "old_fencing_token": (
                stale_token_rejection.get("old_fencing_token") or kill.get("old_fencing_token")
            ),
            "new_owner": takeover_owner,
            "new_lease_epoch": takeover_observation.get("lease_epoch_after"),
            "reason": (
                "takeover was observed, but stale-token rejection remains unproven"
                if takeover_observed
                else str(takeover_observation.get("reason", "takeover was not observed"))
            ),
        }
        fencing_requirements = (
            takeover_observed
            and takeover_owner_differs
            and epoch_monotonic
            and all_survivors_healthy
            and takeover_survivor is not None
            and killed_container_id not in surviving_healthy
            and stale_token_rejection.get("status") == "pass"
            and int(attempts or 0) > 0
            and completion.get("status") == "pass"
        )
        fencing_failure = (
            stale_token_rejection.get("status") == "fail" or completion.get("status") == "fail"
        )
        fencing["status"] = (
            "pass" if fencing_requirements else "fail" if fencing_failure else "not_run"
        )
        fencing["final_commit_contiguous"] = completion.get("status") == "pass"
        if fencing_requirements:
            fencing["reason"] = (
                "kill, takeover, healthy survivor, stale-token rejection, and final commit verified"
            )
        elif fencing_failure:
            fencing["reason"] = str(
                stale_token_rejection.get(
                    "reason", completion.get("reason", "fencing evidence failed")
                )
            )
        stage_markers.append(
            _stage_marker(
                "lease.takeover_observed",
                "pass" if takeover_observed else "not_run",
                attempts_after_takeover=int(attempts or 0),
                surviving_healthy_worker_count=len(surviving_healthy),
                lease_epoch_monotonic=epoch_monotonic,
                takeover_owner_differs=takeover_owner_differs,
                reason=(None if takeover_observed else str(fencing.get("reason"))),
            )
        )
        stage_markers.append(
            _stage_marker(
                "stale_token_rejection_verified",
                str(stale_token_rejection.get("status", "not_run")),
                reason=(
                    None
                    if stale_token_rejection.get("status") == "pass"
                    else str(
                        stale_token_rejection.get(
                            "reason", "stale-token rejection was not verified"
                        )
                    )
                ),
                fencing_conflict_caught=stale_token_rejection.get("fencing_conflict_caught"),
                owner_still_current=stale_token_rejection.get("owner_still_current"),
            )
        )
        survivor_ready = (
            takeover_observed and all_survivors_healthy and takeover_survivor is not None
        )
        stage_markers.append(
            _stage_marker(
                "worker.survivors_observed",
                "pass" if survivor_ready else "not_run",
                worker_count=len(survivor_containers),
                healthy_worker_count=len(healthy_survivors),
                all_survivors_running_healthy=all_survivors_healthy,
                takeover_owner_mapped_to_healthy_survivor=takeover_survivor is not None,
                reason=(
                    None
                    if survivor_ready
                    else "all surviving workers were not healthy or takeover owner was not mapped"
                ),
            )
        )
    else:
        stage_markers.append(
            _stage_marker(
                "lease.takeover_observed",
                "not_run",
                reason="worker kill was not executed",
            )
        )
        stage_markers.append(
            _stage_marker(
                "stale_token_rejection_verified",
                "not_run",
                reason=str(stale_token_rejection.get("reason", "worker kill was not executed")),
            )
        )
        stage_markers.append(
            _stage_marker(
                "worker.survivors_observed",
                "not_run",
                worker_count=len(survivor_containers),
                healthy_worker_count=len(healthy_survivors),
                reason="worker kill was not executed",
            )
        )
    stage_markers.append(
        _stage_marker(
            "turn.commit_verified",
            str(completion.get("status", "not_run")),
            reason=(
                None
                if completion.get("status") == "pass"
                else str(completion.get("reason", "turn completion was not verified"))
            ),
        )
    )
    passed = completion.get("status") == "pass" and (
        not args.kill_worker or (kill.get("status") == "pass" and fencing.get("status") == "pass")
    )
    return {
        "status": "pass" if passed else "fail" if completion.get("status") == "fail" else "not_run",
        "duration_seconds": time.perf_counter() - started,
        "batch": batch,
        "completion": completion,
        "worker_kill": kill,
        "fencing_takeover": fencing,
        "worker_containers_after": after_workers,
        "stage_markers": stage_markers,
    }


def _proxy_field_matches(key: str, expected: Any, observed: Any) -> bool:
    if key != "listen":
        return bool(observed == expected)
    if not isinstance(expected, str) or not isinstance(observed, str):
        return False
    if observed == expected:
        return True
    expected_host, expected_separator, expected_port = expected.rpartition(":")
    observed_host, observed_separator, observed_port = observed.rpartition(":")
    wildcard_hosts = {"0.0.0.0", "[::]"}  # noqa: S104 - intentional wildcard equivalence
    return (
        expected_separator == observed_separator == ":"
        and expected_port == observed_port
        and expected_host in wildcard_hosts
        and observed_host in wildcard_hosts
    )


def _safe_http_endpoint(raw: str) -> str | None:
    """Return a provider endpoint without userinfo or query credentials."""

    try:
        parsed = urlsplit(raw)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname not in {"toxiproxy", "localhost", "127.0.0.1", "::1"}
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            return None
        port = f":{parsed.port}" if parsed.port is not None else ""
        host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
        return f"{parsed.scheme}://{host}{port}"
    except ValueError:
        return None


async def _set_proxy(api: str, name: str, enabled: bool) -> dict[str, Any]:
    try:
        safe_api = _safe_http_endpoint(api)
        if safe_api is None:
            return {"status": "not_run", "reason": "Toxiproxy API endpoint is invalid"}
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(f"{safe_api}/proxies/{name}", json={"enabled": enabled})
            if response.status_code >= 300:
                return {
                    "status": "fail",
                    "reason": f"Toxiproxy {name} update returned {response.status_code}",
                }
            readback = await client.get(f"{safe_api}/proxies/{name}")
            if readback.status_code >= 300:
                return {
                    "status": "fail",
                    "reason": f"Toxiproxy {name} readback returned {readback.status_code}",
                }
            payload = readback.json()
            expected = TOXIPROXY_EXPECTED.get(name, {})
            if not isinstance(payload, dict):
                return {"status": "fail", "reason": f"Toxiproxy {name} readback was invalid"}
            mismatch = {
                key: {"expected": value, "observed": payload.get(key)}
                for key, value in {"name": name, "enabled": enabled, **expected}.items()
                if not _proxy_field_matches(key, value, payload.get(key))
            }
            if mismatch:
                return {
                    "status": "fail",
                    "reason": f"Toxiproxy {name} readback mismatch",
                    "readback": {
                        key: payload.get(key) for key in ("name", "enabled", "listen", "upstream")
                    },
                    "mismatch": mismatch,
                }
            return {
                "status": "pass",
                "api_endpoint": safe_api,
                "name": name,
                "enabled": bool(payload["enabled"]),
                "listen": str(payload["listen"]),
                "upstream": str(payload["upstream"]),
            }
    except (httpx.HTTPError, OSError) as error:
        return {"status": "not_run", "reason": f"Toxiproxy API unavailable: {type(error).__name__}"}
    except (TypeError, ValueError, KeyError):
        return {"status": "fail", "reason": f"Toxiproxy {name} readback was invalid"}


async def _proxy_ready(api: str) -> dict[str, Any]:
    try:
        safe_api = _safe_http_endpoint(api)
        if safe_api is None:
            return {"status": "not_run", "reason": "Toxiproxy API endpoint is invalid"}
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{safe_api}/proxies")
            response.raise_for_status()
            payload = response.json()
            proxies = payload if isinstance(payload, dict) else {}
            names = set(proxies)
            missing = {"postgres", "redis"} - names
            invalid: dict[str, Any] = {}
            for name in sorted({"postgres", "redis"} & names):
                observed = proxies.get(name)
                expected = TOXIPROXY_EXPECTED[name]
                if not isinstance(observed, dict):
                    invalid[name] = "proxy payload is invalid"
                    continue
                mismatch = {
                    key: {"expected": value, "observed": observed.get(key)}
                    for key, value in {"name": name, **expected}.items()
                    if not _proxy_field_matches(key, value, observed.get(key))
                }
                if observed.get("enabled") is not True:
                    mismatch["enabled"] = {"expected": True, "observed": observed.get("enabled")}
                if mismatch:
                    invalid[name] = mismatch
            return {
                "status": "fail" if invalid else "pass" if not missing else "not_run",
                "api_endpoint": safe_api,
                "proxies": sorted(names),
                "reason": (
                    f"missing proxies: {sorted(missing)}"
                    if missing
                    else "invalid proxy configuration"
                    if invalid
                    else ""
                ),
                "proxy_details": {
                    name: {key: proxies[name].get(key) for key in ("enabled", "listen", "upstream")}
                    for name in sorted({"postgres", "redis"} & names)
                    if isinstance(proxies.get(name), dict)
                },
                "proxy_endpoints": {
                    name: {
                        "api_endpoint": safe_api,
                        "listen": proxies[name].get("listen"),
                        "upstream": proxies[name].get("upstream"),
                        "enabled": proxies[name].get("enabled"),
                    }
                    for name in sorted({"postgres", "redis"} & names)
                    if isinstance(proxies.get(name), dict)
                },
                "invalid": invalid,
            }
    except (httpx.HTTPError, OSError, ValueError) as error:
        return {"status": "not_run", "reason": f"Toxiproxy API unavailable: {type(error).__name__}"}


async def _dependency_fault(
    args: argparse.Namespace,
    pool: asyncpg.Pool,
    repository: PostgresRuntimeRepository,
    runtime: TenantRuntime,
    *,
    proxy_name: str,
    prefix: str,
    observation_pool: asyncpg.Pool,
    disable_before_accept: bool = False,
) -> dict[str, Any]:
    batch: dict[str, Any] = {}
    stage_markers: list[dict[str, Any]] = []
    disabled: dict[str, Any] = {"status": "not_run", "reason": "proxy was not disabled"}
    enabled: dict[str, Any] = {"status": "not_run", "reason": "proxy was not disabled"}
    state_while_down: dict[str, Any] = {}
    pending_while_down = 0
    try:
        stage_markers.append(_stage_marker("proxy.disable_requested", "pass", component=proxy_name))
        if disable_before_accept:
            disabled = await _set_proxy(args.toxiproxy_api, proxy_name, False)
            stage_markers.append(
                _stage_marker(
                    "proxy.disabled",
                    str(disabled.get("status", "not_run")),
                    component=proxy_name,
                    reason=(
                        None
                        if disabled.get("status") == "pass"
                        else str(disabled.get("reason", "proxy was not disabled"))
                    ),
                )
            )
            if disabled.get("status") != "pass":
                return {
                    "status": "not_run",
                    "disable": disabled,
                    "stage_markers": stage_markers,
                }
        batch = await _accept_batch(
            runtime,
            repository,
            prefix=f"{prefix}-{uuid4().hex}",
            count=args.fault_messages,
            duplicates=0,
            user_id=f"{prefix}-user-{uuid4().hex}",
        )
        stage_markers.append(
            _stage_marker(
                "acceptance.persisted",
                "pass",
                component=proxy_name,
                unique_inbound_count=len(batch["unique_inbound_ids"]),
            )
        )
        if not disable_before_accept:
            disabled = await _set_proxy(args.toxiproxy_api, proxy_name, False)
            stage_markers.append(
                _stage_marker(
                    "proxy.disabled",
                    str(disabled.get("status", "not_run")),
                    component=proxy_name,
                    reason=(
                        None
                        if disabled.get("status") == "pass"
                        else str(disabled.get("reason", "proxy was not disabled"))
                    ),
                )
            )
            if disabled.get("status") != "pass":
                return {
                    "status": "not_run",
                    "batch": batch,
                    "disable": disabled,
                    "stage_markers": stage_markers,
                }
        await asyncio.sleep(2.0)
        state_while_down = await _session_state(
            observation_pool,
            tenant_id=str(batch["tenant_id"]),
            session_id=str(batch["session_id"]),
            inbound_ids=batch["unique_inbound_ids"],
        )
        pending_while_down = sum(
            count
            for status, count in state_while_down["inbound_statuses"].items()
            if status != "committed"
        )
        stage_markers.append(
            _stage_marker(
                "work_pending_while_disabled",
                "pass" if pending_while_down > 0 else "fail",
                component=proxy_name,
                uncommitted_count=pending_while_down,
            )
        )
    finally:
        if disabled.get("status") == "pass":
            stage_markers.append(
                _stage_marker("proxy.restore_requested", "pass", component=proxy_name)
            )
            enabled = await _set_proxy(args.toxiproxy_api, proxy_name, True)
            stage_markers.append(
                _stage_marker(
                    "proxy.restored",
                    str(enabled.get("status", "not_run")),
                    component=proxy_name,
                    reason=(
                        None
                        if enabled.get("status") == "pass"
                        else str(enabled.get("reason", "proxy was not restored"))
                    ),
                )
            )
    if enabled.get("status") != "pass":
        return {
            "status": "fail",
            "batch": batch,
            "disable": disabled,
            "enable": enabled,
            "state_while_down": state_while_down,
            "stage_markers": stage_markers,
        }
    if proxy_name == "postgres":
        # Toxiproxy closes active PostgreSQL sockets. Do not reuse a runtime
        # connection that predates recovery for subsequent write probes.
        await pool.expire_connections()
    completion = await _wait_for_batch(
        observation_pool,
        batch,
        wait_seconds=args.timeout_seconds,
    )
    stage_markers.append(
        _stage_marker(
            "post_restore.commit_verified",
            str(completion.get("status", "not_run")),
            component=proxy_name,
            reason=(
                None
                if completion.get("status") == "pass"
                else str(completion.get("reason", "post-restore completion was not verified"))
            ),
        )
    )
    expected_turn_count = len(batch["unique_inbound_ids"])
    observed_turn_count = int(completion.get("state", {}).get("turn_count", 0))
    duplicate_turns_verified = observed_turn_count == expected_turn_count
    stage_markers.append(
        _stage_marker(
            "duplicate_turn_verified",
            "pass" if duplicate_turns_verified else "not_run",
            component=proxy_name,
            expected_turn_count=expected_turn_count,
            observed_turn_count=observed_turn_count,
            reason=(
                None
                if duplicate_turns_verified
                else "final state did not independently prove one turn per accepted inbound"
            ),
        )
    )
    completion_status = str(completion.get("status", "not_run"))
    return {
        "status": "pass"
        if completion_status == "pass" and pending_while_down > 0 and duplicate_turns_verified
        else "not_run"
        if completion_status == "pass" and pending_while_down > 0
        else "fail",
        "batch": batch,
        "disable": disabled,
        "enable": enabled,
        "state_while_down": state_while_down,
        "uncommitted_while_proxy_down": pending_while_down,
        "completion_after_restore": completion,
        "expected_turn_count": expected_turn_count,
        "observed_turn_count": observed_turn_count,
        "duplicate_turns_verified": duplicate_turns_verified,
        "stage_markers": stage_markers,
    }


async def _seed_dlq(pool: asyncpg.Pool, tenant_id: str) -> dict[str, Any]:
    outbound_id = str(uuid4())
    outbox_id = str(uuid4())
    missing_binding = f"real-dlq-missing-{uuid4().hex}"
    payload = OutboundEnvelope(
        outbound_id=outbound_id,
        tenant_id=tenant_id,
        binding_id=missing_binding,
        channel=Channel.FEISHU,
        target_id="real-runtime-dlq-target",
        session_id=f"real-dlq-session-{uuid4().hex}",
        payload_kind=PayloadKind.TEXT,
        text="dlq acceptance probe",
    ).model_dump(mode="json")
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        await connection.execute(
            """
            INSERT INTO outbox_events (
                tenant_id,outbox_id,aggregate_type,aggregate_id,event_type,payload_json,
                trace_headers,attempts,available_at
            ) VALUES ($1,$2::uuid,'outbound',$3,'outbound.feishu.ready',$4::jsonb,
                      '{}'::jsonb,4,now())
            """,
            tenant_id,
            outbox_id,
            outbound_id,
            json.dumps(payload, separators=(",", ":")),
        )
    return {
        "outbox_id": outbox_id,
        "outbound_id": outbound_id,
        "missing_binding": missing_binding,
        "attempts_before": 4,
        "retry_limit": 5,
        "terminal_path": "exhausted_retry_terminal_path",
    }


async def _wait_for_dlq(
    pool: asyncpg.Pool,
    tenant_id: str,
    outbound_id: str,
    outbox_id: str,
    attempts_before: int,
    wait_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + wait_seconds
    latest: list[asyncpg.Record] = []
    while time.monotonic() < deadline:
        latest = await _scoped_fetch(
            pool,
            tenant_id,
            """
            SELECT status, reason, source_id FROM dead_letters
             WHERE tenant_id=$1 AND source_id=$2 ORDER BY created_at DESC LIMIT 1
            """,
            tenant_id,
            outbound_id,
        )
        outbox = await _scoped_fetch(
            pool,
            tenant_id,
            """
            SELECT attempts, last_error_type FROM outbox_events
             WHERE tenant_id=$1 AND outbox_id=$2::uuid
            """,
            tenant_id,
            outbox_id,
        )
        if latest:
            attempts_after = int(outbox[0]["attempts"]) if outbox else attempts_before
            increased = attempts_after > attempts_before
            terminal_open = str(latest[0].get("status", "")) == "open"
            return {
                "status": "pass" if increased and terminal_open else "not_run",
                "dead_letter": dict(latest[0]),
                "attempts_before": attempts_before,
                "attempts_after": attempts_after,
                "retry_attempts_increased": increased,
                "terminal_status_open": terminal_open,
                "terminal_path": "exhausted_retry_terminal_path",
                "reason": (
                    ""
                    if increased and terminal_open
                    else "dead-letter terminal status or attempts increment was not verified"
                ),
            }
        await asyncio.sleep(0.5)
    return {"status": "fail", "reason": "channel dispatcher did not create the DLQ record"}


async def _fault_phase(
    args: argparse.Namespace,
    pool: asyncpg.Pool,
    repository: PostgresRuntimeRepository,
    runtime: TenantRuntime,
) -> dict[str, Any]:
    if not args.use_toxiproxy:
        return {
            "status": "not_run",
            "reason": "dependency faults require --use-toxiproxy and its Compose override",
            "stage_markers": _planned_stage_markers(
                FAULT_STAGE_NAMES,
                reason="Toxiproxy was not requested",
            ),
        }
    proxy = await _proxy_ready(args.toxiproxy_api)
    if proxy.get("status") != "pass":
        return {
            "status": "not_run",
            "reason": proxy.get("reason", "Toxiproxy unavailable"),
            "toxiproxy": proxy,
            "stage_markers": [
                _stage_marker(
                    "toxiproxy.proxies_verified",
                    str(proxy.get("status", "not_run")),
                    reason=str(proxy.get("reason", "Toxiproxy unavailable")),
                ),
                *_planned_stage_markers(
                    FAULT_STAGE_NAMES[1:],
                    reason="Toxiproxy proxy inventory was not verified",
                ),
            ],
        }
    stage_markers: list[dict[str, Any]] = [
        _stage_marker("toxiproxy.proxies_verified", "pass", proxies=proxy.get("proxies", []))
    ]
    observation_pool = await _open_global_worker_observer_pool(args)
    try:
        redis_fault = await _dependency_fault(
            args,
            pool,
            repository,
            runtime,
            proxy_name="redis",
            prefix="real-redis-fault",
            observation_pool=observation_pool,
        )
        postgres_fault = await _dependency_fault(
            args,
            pool,
            repository,
            runtime,
            proxy_name="postgres",
            prefix="real-postgres-fault",
            observation_pool=observation_pool,
        )
    finally:
        await observation_pool.close()
    dlq_seed = await _seed_dlq(pool, str(os.environ["TRPC_REAL_TENANT_ID"]))
    dlq = await _wait_for_dlq(
        pool,
        str(os.environ["TRPC_REAL_TENANT_ID"]),
        dlq_seed["outbound_id"],
        dlq_seed["outbox_id"],
        int(dlq_seed["attempts_before"]),
        args.timeout_seconds,
    )
    if (
        getattr(args, "republish_probe", False)
        and redis_fault.get("status") == "pass"
        and redis_fault.get("batch")
    ):
        duplicate_publish_probe = await _active_duplicate_publish_probe(
            pool,
            redis_fault["batch"],
            stream=str(getattr(args, "redis_stream", DEFAULT_REDIS_STREAM)),
            group=str(getattr(args, "redis_group", DEFAULT_REDIS_GROUP)),
            wait_seconds=args.timeout_seconds,
        )
    else:
        duplicate_publish_probe = {
            "status": "not_run",
            "reason": (
                "active duplicate Redis publish probe was not requested for this fault phase"
            ),
        }
    components = (redis_fault, postgres_fault, dlq)
    for component in components[:2]:
        stage_markers.extend(component.get("stage_markers", []))
    stage_markers.append(
        _stage_marker(
            "dlq.dead_letter_verified",
            str(dlq.get("status", "not_run")),
            component="dlq",
            reason=(None if dlq.get("status") == "pass" else "DLQ evidence was not verified"),
        )
    )
    stage_markers.append(
        _stage_marker(
            "duplicate_publish_verified",
            str(duplicate_publish_probe.get("status", "not_run")),
            reason=(
                None
                if duplicate_publish_probe.get("status") == "pass"
                else str(
                    duplicate_publish_probe.get(
                        "reason", "duplicate Redis publish evidence was not verified"
                    )
                )
            ),
            duplicate_stream_id=duplicate_publish_probe.get("duplicate_stream_id"),
            turn_count=duplicate_publish_probe.get("turn_count"),
        )
    )
    component_results = (
        (*components, duplicate_publish_probe)
        if getattr(args, "republish_probe", False)
        else components
    )
    return {
        "status": _status(component_results),
        "toxiproxy": proxy,
        "redis": redis_fault,
        "postgres": postgres_fault,
        "dlq_seed": dlq_seed,
        "dlq": dlq,
        "republish_duplicate_publish_probe": duplicate_publish_probe,
        "stage_markers": stage_markers,
    }


async def _run_real(args: argparse.Namespace) -> dict[str, Any]:
    run_started_at = _utc_timestamp()
    try:
        preflight = _preflight(args)
    except BaseException:
        # `_preflight` may have created a partial stack before an unexpected
        # local error.  Preserve the same ownership rule as the normal finally
        # path even when preflight itself does not return a report.
        _cleanup_owned_compose(args)
        raise
    if preflight.get("status") != "pass":
        _cleanup_owned_compose(args)
        return {
            "schema_version": REAL_RUNTIME_REPORT_SCHEMA_VERSION,
            "started_at": run_started_at,
            "ended_at": _utc_timestamp(),
            "baseline": {"real_runtime_checks": True},
            "candidate": {
                "preflight": preflight,
                "mode": "real_compose_postgresql_redis",
                "parameters": _run_parameters(args),
                "stage_markers": _planned_stage_markers(
                    (*LOAD_STAGE_NAMES, *FAULT_STAGE_NAMES),
                    reason="real runtime preflight did not pass",
                ),
            },
            "case_deltas": {},
            "gate": preflight.get("status", "not_run"),
            "production_gate": "not_run",
            "rejection_reasons": [str(preflight.get("reason", "real preflight did not pass"))],
            "production_rejection_reasons": [
                str(preflight.get("reason", "real preflight did not pass"))
            ],
        }
    pool: asyncpg.Pool | None = None
    try:
        pool, repository, runtime = await _open_runtime(args)
        results: list[dict[str, Any]] = []
        role_evidence = await _database_role_evidence(pool)
        candidate: dict[str, Any] = {
            "mode": "real_compose_postgresql_redis",
            "preflight": preflight,
            "parameters": _run_parameters(args),
            "database_role_evidence": role_evidence,
            "started_at": run_started_at,
            "stage_markers": [],
        }
        if role_evidence.get("status") != "pass":
            role_status = str(role_evidence.get("status", "not_run"))
            role_reason = str(
                role_evidence.get(
                    "reason",
                    "dedicated global-worker PostgreSQL role evidence did not pass",
                )
            )
            return {
                "schema_version": REAL_RUNTIME_REPORT_SCHEMA_VERSION,
                "started_at": run_started_at,
                "ended_at": _utc_timestamp(),
                "baseline": {"real_runtime_checks": True},
                "candidate": candidate,
                "case_deltas": {},
                "gate": role_status,
                "production_gate": role_status if args.phase == "all" else "not_run",
                "rejection_reasons": [role_reason],
                "production_rejection_reasons": [role_reason],
            }
        if args.phase in {"all", "load"}:
            load = await _load_phase(
                args,
                pool,
                repository,
                runtime,
                preflight["worker_containers"],
            )
            if args.phase == "all" and not args.kill_worker:
                load = {
                    **load,
                    "status": "not_run",
                    "reason": "full real runtime acceptance requires --kill-worker",
                }
            candidate["load"] = load
            candidate["stage_markers"].extend(load.get("stage_markers", []))
            results.append(load)
        if args.phase in {"all", "fault"}:
            if args.phase == "all" and results and results[0].get("status") != "pass":
                # A failed load already establishes a failed full run. Do
                # not spend several additional timeout windows waiting for
                # fault/DLQ evidence that cannot upgrade the production gate.
                faults = _faults_skipped_after_load_failure(results[0])
            else:
                faults = await _fault_phase(args, pool, repository, runtime)
            candidate["faults"] = faults
            candidate["stage_markers"].extend(faults.get("stage_markers", []))
            results.append(faults)
        gate = _status(results)
        production_gate = gate if args.phase == "all" else "not_run"
        reasons: list[str] = []
        production_scope_reasons = _production_scope_reasons(args) if args.phase == "all" else []
        if production_scope_reasons and gate == "pass":
            production_gate = "not_run"
        if production_gate == "not_run" and args.phase != "all":
            reasons.append(
                "load-only/fault-only execution is scoped evidence, not full production acceptance"
            )
        if gate != "pass":
            reasons.append("one or more real runtime checks did not pass")
        return {
            "schema_version": REAL_RUNTIME_REPORT_SCHEMA_VERSION,
            "started_at": run_started_at,
            "ended_at": _utc_timestamp(),
            "baseline": {
                "min_worker_containers": args.workers,
                "load_messages": args.messages,
                "duplicate_messages": args.duplicates,
                "redis_and_postgres_faults": args.phase in {"all", "fault"},
                "dead_letter_observation": args.phase in {"all", "fault"},
            },
            "candidate": candidate,
            "case_deltas": {
                "requested_phase": args.phase,
                "worker_count": len(preflight["worker_containers"]),
            },
            "gate": gate,
            "production_gate": production_gate,
            "rejection_reasons": reasons,
            "production_rejection_reasons": [
                *reasons,
                *production_scope_reasons,
                (
                    "this report covers Compose multi-process/runtime faults only; real IM, "
                    "migration and Kubernetes gates remain separate"
                ),
            ],
        }
    except (
        asyncpg.PostgresError,
        OSError,
        RuntimeError,
        LookupError,
        AssertionError,
        ValueError,
    ) as error:
        failure = _safe_failure(error)
        return {
            "schema_version": REAL_RUNTIME_REPORT_SCHEMA_VERSION,
            "started_at": run_started_at,
            "ended_at": _utc_timestamp(),
            "baseline": {"real_runtime_checks": True},
            "candidate": {
                "mode": "real_compose_postgresql_redis",
                "failure": failure,
                "stage_markers": _planned_stage_markers(
                    (*LOAD_STAGE_NAMES, *FAULT_STAGE_NAMES),
                    reason="real runtime execution raised before stage evidence was complete",
                ),
            },
            "case_deltas": {},
            "gate": "fail",
            "production_gate": "fail",
            "rejection_reasons": ["real runtime execution raised an error"],
            "production_rejection_reasons": ["real runtime execution raised an error"],
        }
    finally:
        if pool is not None:
            await pool.close()
        _cleanup_owned_compose(args)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.execute or os.getenv(OPT_IN) != "1":
        report = _not_run_report(
            [f"real multi-process acceptance requires --execute and {OPT_IN}=1"]
        )
        _write_report(args.output, report, args=args)
        return 1 if args.require_production else 0
    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if missing:
        report = _not_run_report(
            [f"missing required real runtime environment: {name}" for name in missing]
        )
        _write_report(args.output, report, args=args)
        return 1 if args.require_production else 0
    try:
        current_release_binding(required=True)
    except ValueError as error:
        report = _not_run_report([str(error)])
        _write_report(args.output, report, args=args)
        return 1 if args.require_production else 0
    if args.phase in {"all", "load"} and args.kill_worker and not args.allow_process_kill:
        report = _not_run_report(["--kill-worker requires --allow-process-kill"])
        _write_report(args.output, report, args=args)
        return 1 if args.require_production else 0
    report = _attach_compose_cleanup_evidence(asyncio.run(_run_real(args)), args)
    _write_report(args.output, report, args=args)
    return (
        0
        if report.get("gate") == "pass"
        and (not args.require_production or report.get("production_gate") == "pass")
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
