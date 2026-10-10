#!/usr/bin/env python3
"""Run or plan repeatable dependency and process fault-acceptance scenarios.

The default mode is deliberately side-effect free: it writes a machine-readable
``not_run`` report and never contacts Docker, Redis, PostgreSQL, or Toxiproxy.
Real execution requires both ``--execute`` and ``TRPC_RUN_REAL_MULTINODE=1``.

This wrapper keeps the evidence boundary explicit.  Redis recovery, worker
fencing, republish, and the durable DLQ path can delegate to
``real_runtime_gate.py``.  The ambiguous provider boundary uses the dedicated
``ambiguous_provider_acceptance.py`` child and a real response-dropping HTTP
endpoint.  The enqueue/tool/commit kill boundaries use one dedicated
``fault_stage_acceptance.py`` child invocation.  The child itself
validates all dedicated ``TRPC_FAULT_*`` credentials and the exact worker
identity before it can terminate a process.  A timing-based kill is never
accepted as evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

try:  # Keep direct script-path and ``python -m`` invocations equivalent.
    from scripts.evidence_lineage import (
        build_evidence,
        canonical_sha256,
        current_release_binding,
        runtime_fingerprint,
        source_fingerprint,
        validate_current_candidate_evidence,
    )
    from scripts.report_io import atomic_write_json
except ModuleNotFoundError:  # pragma: no cover - exercised by direct CLI startup
    from evidence_lineage import (  # type: ignore[import-not-found, no-redef]
        build_evidence,
        canonical_sha256,
        current_release_binding,
        runtime_fingerprint,
        source_fingerprint,
        validate_current_candidate_evidence,
    )
    from report_io import atomic_write_json  # type: ignore[import-not-found, no-redef]

ROOT = Path(__file__).resolve().parents[1]
REAL_OPT_IN = "TRPC_RUN_REAL_MULTINODE"
REAL_REQUIRED_ENV = (
    "TRPC_REAL_DATABASE_DSN",
    "TRPC_REAL_REDIS_URL",
    "TRPC_REAL_TENANT_ID",
    "TRPC_REAL_BINDING_ID",
    "TRPC_REAL_SESSION_HMAC_KEY",
    # Production child runs must prove that cross-tenant SQL is executed by
    # the dedicated global-worker role, not by the ordinary runtime role.
    "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN",
    "TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE",
)
REAL_WORKER_IDENTITIES_ENV = "TRPC_REAL_WORKER_IDENTITIES"
DEFAULT_OVERRIDE = ROOT / "deploy" / "toxiproxy-runtime.override.yml"
FAULT_STAGE_SCENARIOS = {
    "worker_enqueue": "enqueue",
    "worker_tool": "tool",
    "worker_commit": "commit_txn_open",
}
FAULT_STAGE_REQUIRED_STAGES = tuple(FAULT_STAGE_SCENARIOS.values())
WORKER_SERVICE = "worker"
PRODUCER = "scripts.fault_injection_gate"
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-fA-F]{64}$")
MIN_REAL_WORKERS = 4
FAULT_STAGE_REQUIRED_MARKERS: dict[str, tuple[str, ...]] = {
    "enqueue": (
        "preflight.workers_verified",
        "acceptance.persisted",
        "control.armed",
        "marker.entered",
        "worker.terminated",
        "worker.survivors_observed",
        "v2.claim_before_observed",
        "turn.single_contiguous_verified",
        "turn.commit_verified",
        "outbound.intent_verified",
    ),
    "tool": (
        "preflight.workers_verified",
        "acceptance.persisted",
        "turn.processing_observed",
        "control.armed",
        "marker.entered",
        "worker.terminated",
        "worker.survivors_observed",
        "stale_token_rejection_verified",
        "turn.single_contiguous_verified",
        "turn.commit_verified",
        "tool.idempotent_execution_verified",
        "outbound.intent_verified",
        "v2.ack_before_execute",
    ),
    "commit_txn_open": (
        "preflight.workers_verified",
        "acceptance.persisted",
        "turn.processing_observed",
        "control.armed",
        "marker.entered",
        "worker.terminated",
        "worker.survivors_observed",
        "stale_token_rejection_verified",
        "turn.single_contiguous_verified",
        "turn.commit_verified",
        "outbound.intent_verified",
        "v2.ack_before_execute",
    ),
}

# Child reports are evidence artifacts, not transient command output.  The
# parent first asks the child to write into a private staging directory and
# only retains a strictly parsed copy below this fixed, run-scoped root.  The
# release gate derives the same root from its ``--directory`` argument.
FAULT_EVIDENCE_DIRECTORY = "fault-evidence"
FAULT_STAGING_DIRECTORY = ".fault-staging"

SCENARIOS: dict[str, dict[str, Any]] = {
    "redis_interrupt": {
        "label": "Redis interruption and recovery",
        "real_supported": True,
        "evidence": (
            "outbox remains durable while Redis is disabled; recovery republishes and "
            "the session commits"
        ),
        "assertions": (
            "proxy redis disabled",
            "at least one inbound remains uncommitted while disabled",
            "proxy redis restored",
            "all accepted inbound rows commit with contiguous events",
        ),
    },
    "worker_enqueue": {
        "label": "Worker termination at enqueue",
        "real_supported": False,
        "evidence": "worker dies before consuming a Redis delivery; pending delivery is reclaimed",
        "assertions": (
            "the delivery is not acknowledged before the kill",
            "a surviving worker claims the delivery",
            "the inbound is committed exactly once",
        ),
        "blocked_reason": (
            "the dedicated fault-stage acceptance child requires an explicit trpc-fault-* "
            "project, worker container, and TRPC_FAULT_* environment"
        ),
    },
    "worker_tool": {
        "label": "Worker termination during tool execution",
        "real_supported": False,
        "evidence": (
            "worker dies while a tool call is in flight; execution ledger prevents unsafe replay"
        ),
        "assertions": (
            "idempotent tool may retry under the same execution key",
            "non-idempotent unknown result is AMBIGUOUS",
            "no duplicate external side effect is claimed",
        ),
        "blocked_reason": (
            "the dedicated fault-stage acceptance child requires an explicit trpc-fault-* "
            "project, worker container, and TRPC_FAULT_* environment"
        ),
    },
    "worker_commit": {
        "label": "Worker termination during session commit",
        "real_supported": False,
        "evidence": "worker dies during the fenced PostgreSQL commit and a replacement retries",
        "assertions": (
            "the old fencing token cannot commit after takeover",
            "the replacement commit has one contiguous event sequence",
            "the outbound intent is not duplicated",
        ),
        "blocked_reason": (
            "the dedicated fault-stage acceptance child requires an explicit trpc-fault-* "
            "project, worker container, and TRPC_FAULT_* environment"
        ),
    },
    "fencing": {
        "label": "Worker kill, lease takeover, and fencing",
        "real_supported": True,
        "evidence": (
            "one active worker container is killed and another observes a retry/epoch takeover"
        ),
        "assertions": (
            "kill occurs only after a processing turn is observed",
            "a surviving worker observes an attempt greater than one",
            "the final event sequence is contiguous",
        ),
    },
    "republish": {
        "label": "Outbox republish after Redis recovery",
        "real_supported": True,
        "evidence": (
            "PostgreSQL outbox remains the source of truth and is republished after Redis "
            "recovery; "
            "an explicit duplicate Redis publish probe is still required"
        ),
        "assertions": (
            "unpublished outbox exists while Redis is disabled",
            "Redis is restored",
            "the same accepted inbound eventually commits",
            "duplicate delivery does not create a second turn",
            "the identical Redis task is actively published twice and correlated to one turn",
        ),
    },
    "dlq": {
        "label": "Durable dead-letter path",
        "real_supported": True,
        "evidence": (
            "an outbound record preloaded at the retry limit reaches the exhausted-retry "
            "terminal path in dead_letters"
        ),
        "assertions": (
            "the retry-limit terminal path is observed without loss",
            "the outbox attempts counter increases before dead_letters",
            "the report contains no provider secret or message body",
        ),
    },
    "ambiguous": {
        "label": "Ambiguous outbound result and manual replay",
        "real_supported": True,
        "evidence": (
            "a transport-unknown delivery is marked ambiguous and requires explicit confirmation"
        ),
        "assertions": (
            "ambiguous is not automatically retried",
            "replay without confirmation is rejected",
            "confirmed replay creates a new outbox attempt",
        ),
    },
}

# The wrapper keeps stage evidence explicit even for scenarios that are not yet
# wired to a real provider.  ``simulated`` markers in an offline report never
# contribute to ``production_gate``.
SCENARIO_STAGE_MARKERS: dict[str, tuple[str, ...]] = {
    "redis_interrupt": (
        "proxy.disable_requested",
        "proxy.disabled",
        "work_pending_while_disabled",
        "proxy.restore_requested",
        "proxy.restored",
        "post_restore.commit_verified",
        "duplicate_turn_verified",
    ),
    "worker_enqueue": FAULT_STAGE_REQUIRED_MARKERS["enqueue"],
    "worker_tool": FAULT_STAGE_REQUIRED_MARKERS["tool"],
    "worker_commit": FAULT_STAGE_REQUIRED_MARKERS["commit_txn_open"],
    "fencing": (
        "turn.processing_observed",
        "worker.kill_requested",
        "worker.kill_completed",
        "lease.takeover_observed",
        "worker.survivors_observed",
        "stale_token_rejection_verified",
        "turn.commit_verified",
    ),
    "republish": (
        "proxy.disable_requested",
        "proxy.disabled",
        "work_pending_while_disabled",
        "proxy.restore_requested",
        "proxy.restored",
        "post_restore.commit_verified",
        "duplicate_turn_verified",
        "duplicate_publish_verified",
    ),
    "dlq": ("dlq.dead_letter_verified",),
    "ambiguous": (
        "delivery.ambiguous_observed",
        "delivery.replay_confirmation_required",
        "delivery.replay_verified",
    ),
}

# This is deliberately part of the report, rather than only documentation.
# The release aggregator must validate these semantics independently of the
# generic evidence envelope before it can ever accept a fault-injection pass.
FAULT_REPORT_SCHEMA_VERSION = 1
FAULT_REPORT_MODE = "real_compose_fault_injection"


def _production_contract() -> dict[str, Any]:
    """Return the immutable production-pass contract for this scoped gate."""

    return {
        "schema_version": FAULT_REPORT_SCHEMA_VERSION,
        "mode": FAULT_REPORT_MODE,
        "required_scenarios": list(SCENARIOS),
        "required_scenario_status": "pass",
        "required_stage_markers": {
            name: list(markers) for name, markers in SCENARIO_STAGE_MARKERS.items()
        },
        "required_top_level_fields": [
            "schema_version",
            "baseline",
            "candidate",
            "case_deltas",
            "gate",
            "production_gate",
            "run_id",
            "evidence",
            "production_rejection_reasons",
        ],
        "required_candidate_fields": ["mode", "requested_scenario", "scenarios", "lineage"],
        "required_lineage_fields": ["status", "image_digest"],
        "required_case_delta_fields": ["requested", "passed"],
        "marker_requirements": {
            "status": "pass",
            "observed_at": "non-empty string",
            "duplicate_names": "reject",
        },
        "production_rules": [
            "requested_scenario must be all",
            "candidate.scenarios must contain exactly all required_scenarios",
            "case_deltas.requested and passed must each contain exactly all required_scenarios",
            "gate and production_gate must both be pass",
            "candidate.lineage.status must be pass and image_digest must match sha256:<64 hex>",
            "evidence must be current_candidate with available source and runtime fingerprints",
            "dedicated process-kill workers must attest to the current source and candidate image",
            (
                "scoped runtime children must remain production_gate=not_run; "
                "only this parent aggregates them"
            ),
            "offline/deterministic/simulated markers never qualify",
        ],
    }


SUPPORTED_REAL = tuple(name for name, item in SCENARIOS.items() if item["real_supported"])


def _scenario_stage_markers(
    scenario: str,
    *,
    status: str,
    reason: str,
    evidence: str | None = None,
) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for name in SCENARIO_STAGE_MARKERS[scenario]:
        marker: dict[str, Any] = {
            "name": name,
            "status": status,
            "reason": reason,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        if evidence is not None:
            marker["evidence"] = evidence
        markers.append(marker)
    return markers


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("all", *SCENARIOS),
        default="all",
        help="scenario to execute or plan (default: all)",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="run deterministic fault contracts and record simulation evidence",
    )
    parser.add_argument("--execute", action="store_true", help="allow real Compose execution")
    parser.add_argument(
        "--allow-process-kill",
        action="store_true",
        help="second explicit acknowledgement for process termination in real fault scenarios",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--messages", type=int, default=200)
    parser.add_argument("--duplicates", type=int, default=20)
    parser.add_argument("--fault-messages", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=300.0)
    parser.add_argument(
        "--project", default=os.getenv("TRPC_REAL_COMPOSE_PROJECT", "trpc-agent-service")
    )
    parser.add_argument(
        "--fault-project",
        default=os.getenv("TRPC_FAULT_COMPOSE_PROJECT"),
        help="dedicated fault-stage Compose project; must start with trpc-fault-",
    )
    parser.add_argument(
        "--fault-worker-container",
        default=os.getenv("TRPC_FAULT_WORKER_CONTAINER"),
        help="explicit worker container id for the one fault-stage child run",
    )
    parser.add_argument(
        "--fault-termination",
        choices=("stop", "kill"),
        default="kill",
        help="termination mode for the explicitly acknowledged fault-stage child",
    )
    parser.add_argument("--compose-file", type=Path, default=ROOT / "docker-compose.yml")
    parser.add_argument("--toxiproxy-override", type=Path, default=DEFAULT_OVERRIDE)
    parser.add_argument(
        "--toxiproxy-api", default=os.getenv("TRPC_REAL_TOXIPROXY_API", "http://127.0.0.1:8474")
    )
    parser.add_argument(
        "--ambiguous-provider-url",
        default=os.getenv("TRPC_AMBIGUOUS_PROVIDER_URL", ""),
        help=(
            "optional independently deployed response-drop provider URL; when omitted the "
            "acceptance child starts a bounded loopback endpoint"
        ),
    )
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/fault-injection.json")
    )
    parser.add_argument("--require-production", action="store_true")
    return parser


def _status(values: list[str]) -> str:
    if any(value == "fail" for value in values):
        return "fail"
    if any(value != "pass" for value in values):
        return "not_run"
    return "pass"


def _utc_timestamp() -> str:
    """Return an RFC3339 UTC timestamp for parent-side process observation."""

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _assert_safe_report_path(path: Path, *, within: Path | None = None) -> Path:
    """Resolve a report path only after rejecting symlinks and path escapes.

    Child reports are artifacts, not arbitrary user-controlled output.  The
    parent therefore confines them to the directory containing the requested
    parent report and rejects a symlink at every existing path component.
    """

    absolute = path.expanduser().absolute()
    current = absolute
    while True:
        if current.is_symlink():
            raise RuntimeError("fault child report path contains a symlink")
        parent = current.parent
        if parent == current:
            break
        current = parent
    resolved = absolute.resolve(strict=False)
    if within is not None:
        allowed = _assert_safe_report_path(within).resolve(strict=False)
        try:
            resolved.relative_to(allowed)
        except ValueError as error:
            raise RuntimeError("fault child report path escapes the report directory") from error
    return resolved


def _run_scoped_evidence_scope(report_directory: Path, run_id: str) -> Path:
    """Return the trusted ``fault-evidence/<run_id>`` directory.

    ``report_directory`` is the directory containing the parent report (in a
    production run this is ``runs/multitenant``).  Keeping the root relative
    to that directory makes release validation deterministic while allowing
    unit tests to use an isolated temporary report directory.
    """

    root = _assert_safe_report_path(
        report_directory / FAULT_EVIDENCE_DIRECTORY,
        within=report_directory,
    )
    return _assert_safe_report_path(root / run_id, within=root)


def _staging_scope(report_directory: Path, run_id: str) -> Path:
    return _assert_safe_report_path(
        report_directory / FAULT_STAGING_DIRECTORY / run_id,
        within=report_directory,
    )


def _retained_child_report(
    source: Path,
    *,
    report_directory: Path,
    run_id: str,
    filename: str,
    child: dict[str, Any],
) -> tuple[Path, Path]:
    """Retain a parsed child report in the trusted run-scoped evidence root.

    The destination is deliberately not the path supplied to the child.  A
    child can therefore not replace a previously retained artifact by
    manipulating its output path, and the release gate can later re-read the
    exact retained artifact instead of trusting a parent-copied hash.
    """

    source = _assert_safe_report_path(source)
    if not source.is_file() or source.is_symlink():
        raise RuntimeError("fault child staging report is not a regular file")
    scope = _run_scoped_evidence_scope(report_directory, run_id)
    destination = _assert_safe_report_path(scope / filename, within=scope)
    if destination.exists():
        raise RuntimeError("trusted fault child report already exists; refusing replacement")
    atomic_write_json(destination, child)
    destination = _assert_safe_report_path(destination, within=scope)
    if not destination.is_file() or destination.is_symlink():
        raise RuntimeError("trusted fault child report was not retained as a regular file")
    return destination, scope


def _strict_json_loads(text: str) -> Any:
    """Parse JSON without extensions or duplicate object members."""

    def reject_constant(value: str) -> Any:
        raise ValueError(f"non-standard JSON constant: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key: {key}")
            result[key] = value
        return result

    return json.loads(
        text,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )


def _child_observation(
    *,
    child_output: Path,
    child: Any | None,
    report_directory: Path,
    started_at: str,
    ended_at: str,
    observed_exit_code: int | None,
) -> dict[str, Any]:
    """Return secret-free, parent-observed child lineage metadata.

    The raw nonce is never copied into the parent report.  Real runtime
    children expose an opaque ``run_nonce`` while the fault-stage child emits
    only ``run_nonce_sha256``; both are normalized to the latter here.
    """

    confined = _assert_safe_report_path(child_output, within=report_directory)
    if confined.exists():
        child_stat = confined.stat()
        if not stat.S_ISREG(child_stat.st_mode):
            raise RuntimeError("fault child report is not a regular file")
    observation: dict[str, Any] = {
        "child_report": str(confined),
        "child_report_path_scope": str(_assert_safe_report_path(report_directory)),
        "child_report_path_confined": True,
        "child_started_at": started_at,
        "child_ended_at": ended_at,
        "observed_exit_code": observed_exit_code,
        "child_identity_verified": False,
        "child_timestamps_verified": False,
    }
    if child is None:
        return observation
    try:
        observation["child_report_sha256"] = canonical_sha256(child)
    except (TypeError, ValueError) as error:
        raise RuntimeError("fault child report is not canonical strict JSON") from error
    if not isinstance(child, dict):
        raise RuntimeError("fault child report is not a JSON object")
    child_run_id = child.get("run_id")
    provenance = child.get("execution_provenance")
    if not isinstance(child_run_id, str) or not child_run_id.strip():
        child_run_id = provenance.get("run_id") if isinstance(provenance, dict) else None
    nonce_hash = child.get("run_nonce_sha256")
    if not isinstance(nonce_hash, str):
        nonce = child.get("run_nonce")
        if isinstance(nonce, str) and nonce.strip():
            nonce_hash = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
    if not isinstance(nonce_hash, str) and isinstance(provenance, dict):
        nonce_hash = provenance.get("nonce_sha256")
    if isinstance(child_run_id, str) and child_run_id.strip():
        observation["child_run_id"] = child_run_id
    if isinstance(nonce_hash, str) and re.fullmatch(r"[0-9a-fA-F]{64}", nonce_hash):
        observation["child_nonce_sha256"] = nonce_hash.lower()
    observation["child_identity_verified"] = (
        isinstance(child_run_id, str)
        and bool(child_run_id.strip())
        and isinstance(nonce_hash, str)
        and re.fullmatch(r"[0-9a-fA-F]{64}", nonce_hash) is not None
    )
    report_started = child.get("started_at")
    report_ended = child.get("ended_at")
    parsed_started: datetime | None = None
    parsed_ended: datetime | None = None
    if isinstance(report_started, str) and isinstance(report_ended, str):
        try:
            parsed_started = datetime.fromisoformat(report_started.replace("Z", "+00:00"))
            parsed_ended = datetime.fromisoformat(report_ended.replace("Z", "+00:00"))
        except ValueError:
            parsed_started = None
            parsed_ended = None
        if (
            parsed_started is not None
            and parsed_ended is not None
            and parsed_started.tzinfo is not None
            and parsed_ended.tzinfo is not None
            and parsed_ended >= parsed_started
        ):
            observation["child_report_started_at"] = report_started
            observation["child_report_ended_at"] = report_ended
            observation["child_timestamps_verified"] = True
    return observation


def _fault_production_contract_errors(
    *,
    args: argparse.Namespace,
    requested: tuple[str, ...],
    scenarios: dict[str, dict[str, Any]],
    gate: str,
    image_digest: str,
    candidate: dict[str, Any],
    case_deltas: dict[str, Any],
    evidence: Any,
) -> list[str]:
    """Validate the semantic pass contract independently of the envelope.

    ``release_gate.py`` intentionally owns cross-report aggregation.  This
    local validator makes the fault report self-describing and fail-closed so
    that a future aggregator can apply the same checks without trusting a
    caller-provided ``production_gate`` field.
    """

    errors: list[str] = []
    required = tuple(SCENARIOS)
    if args.scenario != "all" or tuple(requested) != required:
        errors.append("requested scenario inventory is not the complete production set")
    if set(scenarios) != set(required) or len(scenarios) != len(required):
        errors.append("candidate.scenarios must contain exactly every required scenario")
    if gate != "pass":
        errors.append("fault scenario gate is not pass")

    for name in required:
        item = scenarios.get(name)
        if not isinstance(item, dict):
            errors.append(f"{name}: scenario result is missing")
            continue
        if item.get("status") != "pass":
            errors.append(f"{name}: scenario status is not pass")
        raw_markers = item.get("stage_markers")
        if not isinstance(raw_markers, list):
            errors.append(f"{name}: stage_markers must be a list")
            continue
        marker_by_name: dict[str, dict[str, Any]] = {}
        duplicate_names: set[str] = set()
        for marker in raw_markers:
            if not isinstance(marker, dict) or not isinstance(marker.get("name"), str):
                errors.append(f"{name}: stage marker is malformed")
                continue
            marker_name = marker["name"]
            if marker_name in marker_by_name:
                duplicate_names.add(marker_name)
            marker_by_name[marker_name] = marker
            if not isinstance(marker.get("observed_at"), str) or not marker["observed_at"].strip():
                errors.append(f"{name}: marker {marker_name} has no observed_at")
        if duplicate_names:
            errors.append(f"{name}: duplicate stage markers: {','.join(sorted(duplicate_names))}")
        for marker_name in SCENARIO_STAGE_MARKERS[name]:
            marker = marker_by_name.get(marker_name)
            if marker is None:
                errors.append(f"{name}: missing required stage marker {marker_name}")
            elif marker.get("status") != "pass":
                errors.append(f"{name}: required stage marker {marker_name} is not pass")

        if name in FAULT_STAGE_SCENARIOS:
            for field, expected in (
                ("child_schema_version", 1),
                ("child_mode", "fault_stage_acceptance"),
                ("child_gate", "pass"),
            ):
                if item.get(field) != expected:
                    errors.append(f"{name}: {field} must be {expected!r}")
            if item.get("child_production_gate") != "pass":
                errors.append(f"{name}: child_production_gate must be 'pass'")
            preflight = item.get("worker_preflight")
            if (
                not isinstance(preflight, dict)
                or preflight.get("status") != "pass"
                or preflight.get("worker_count", 0) < MIN_REAL_WORKERS
                or preflight.get("healthy_worker_count") != preflight.get("worker_count")
                or preflight.get("independent_processes") is not True
                or preflight.get("positive_pid_count") != preflight.get("worker_count")
            ):
                errors.append(
                    f"{name}: child preflight must prove at least {MIN_REAL_WORKERS} "
                    "independent healthy workers"
                )
            image_attestation = (
                preflight.get("image_attestation") if isinstance(preflight, dict) else None
            )
            source_fingerprint = (
                evidence.get("source_fingerprint", {}).get("value")
                if isinstance(evidence, dict)
                and isinstance(evidence.get("source_fingerprint"), dict)
                else None
            )
            if (
                not isinstance(image_attestation, dict)
                or image_attestation.get("status") != "pass"
                or image_attestation.get("source_fingerprint") != source_fingerprint
                or image_attestation.get("source_fingerprint_matches") is not True
                or not isinstance(image_attestation.get("image_id"), str)
                or IMAGE_DIGEST_RE.fullmatch(image_attestation["image_id"]) is None
                or image_attestation.get("image_id") != image_digest
            ):
                errors.append(
                    f"{name}: child image/source attestation must match the candidate image"
                )
        else:
            for field, expected in (("child_gate", "pass"), ("child_phase_status", "pass")):
                if item.get(field) != expected:
                    errors.append(f"{name}: {field} must be {expected!r}")
            if item.get("child_production_gate") != "not_run":
                errors.append(f"{name}: child_production_gate must be 'not_run'")
            if not isinstance(item.get("run_id"), str) or item.get("run_id") != item.get(
                "child_run_id"
            ):
                errors.append(f"{name}: child run identity is missing or mismatched")
            if not isinstance(item.get("child_report"), str) or not item["child_report"].strip():
                errors.append(f"{name}: child report path is missing")
            selected = item.get("evidence")
            if not isinstance(selected, dict) or selected.get("status") != "pass":
                errors.append(f"{name}: selected child evidence is not pass")
            selected_evidence = selected if isinstance(selected, dict) else {}
            if name == "republish":
                probe = item.get("duplicate_publish_probe")
                if not isinstance(probe, dict) or probe.get("status") != "pass":
                    errors.append("republish: duplicate_publish_probe must be pass")
            if name == "ambiguous":
                ledger = selected_evidence.get("provider_ledger")
                if (
                    selected_evidence.get("manual_confirmation_required") is not True
                    or selected_evidence.get("automatic_replay_count") != 0
                    or selected_evidence.get("confirmed_replay_status") != "pass"
                    or not isinstance(ledger, dict)
                    or ledger.get("accepted_count") != 1
                    or ledger.get("side_effect_count") != 1
                    or ledger.get("duplicate_replay_count") != 1
                ):
                    errors.append(
                        "ambiguous: response-drop query and idempotent manual replay evidence "
                        "must pass"
                    )
            preflight = item.get("worker_preflight")
            if (
                not isinstance(preflight, dict)
                or preflight.get("status") != "pass"
                or preflight.get("worker_count", 0) < MIN_REAL_WORKERS
                or preflight.get("healthy_worker_count") != preflight.get("worker_count")
                or preflight.get("independent_processes") is not True
            ):
                errors.append(
                    f"{name}: child preflight must prove at least {MIN_REAL_WORKERS} "
                    "independent healthy workers"
                )

    for field in ("requested", "passed"):
        value = case_deltas.get(field)
        if (
            not isinstance(value, list)
            or not all(isinstance(item, str) for item in value)
            or len(value) != len(set(value))
            or set(value) != set(required)
        ):
            errors.append(f"case_deltas.{field} must contain exactly every required scenario")
    lineage = candidate.get("lineage")
    if not isinstance(lineage, dict) or lineage.get("status") != "pass":
        errors.append("candidate.lineage.status must be pass")
    if IMAGE_DIGEST_RE.fullmatch(image_digest) is None:
        errors.append("candidate lineage image_digest must match sha256:<64 hex>")
    if not isinstance(evidence, dict):
        errors.append("current-candidate evidence envelope is missing")
    else:
        if evidence.get("schema_version") != 1 or evidence.get("kind") != "current_candidate":
            errors.append("evidence must use the current_candidate schema")
        for field in ("source_fingerprint", "runtime_fingerprint"):
            fingerprint = evidence.get(field)
            if (
                not isinstance(fingerprint, dict)
                or fingerprint.get("algorithm") != "sha256"
                or fingerprint.get("status") != "available"
                or IMAGE_DIGEST_RE.fullmatch("sha256:" + str(fingerprint.get("value", ""))) is None
            ):
                errors.append(f"evidence.{field} must be an available sha256 fingerprint")
        runtime_fingerprint_value = evidence.get("runtime_fingerprint")
        if (
            not isinstance(runtime_fingerprint_value, dict)
            or not isinstance(runtime_fingerprint_value.get("worker_count"), int)
            or runtime_fingerprint_value.get("worker_count", 0) < MIN_REAL_WORKERS
        ):
            errors.append(
                f"evidence.runtime_fingerprint must record at least {MIN_REAL_WORKERS} workers"
            )
    return errors


def _required_scenarios(name: str) -> tuple[str, ...]:
    return tuple(SCENARIOS) if name == "all" else (name,)


def _missing_real_environment(*, require_runtime_credentials: bool = True) -> list[str]:
    missing = (
        [name for name in REAL_REQUIRED_ENV if not os.getenv(name)]
        if require_runtime_credentials
        else []
    )
    if os.getenv(REAL_OPT_IN) != "1":
        missing.append(f"{REAL_OPT_IN}=1")
    return missing


def _runtime_worker_identities(required: int) -> tuple[str, ...]:
    """Read inspected worker identities without ever putting them in a report."""

    raw = os.getenv(REAL_WORKER_IDENTITIES_ENV, "")
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    if required < 1 or len(values) < required or len(set(values)) != len(values):
        return ()
    if any(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value) is None for value in values):
        return ()
    return values


def _runbook(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        "python",
        "scripts/fault_injection_gate.py",
        "--execute",
        "--scenario",
        "<scenario>",
        "--require-production",
        "--workers",
        str(args.workers),
        "--output",
        str(args.output),
    ]
    fault_command = [
        "python",
        "scripts/fault_injection_gate.py",
        "--execute",
        "--scenario",
        "all",
        "--allow-process-kill",
        "--fault-project",
        "trpc-fault-<run-id>",
        "--fault-worker-container",
        "<explicit-worker-container-id>",
        "--output",
        str(args.output),
    ]
    return {
        "real_prerequisites": [
            "Docker Engine and Compose v2",
            "at least four independent worker containers",
            "runtime PostgreSQL account, Redis, and Toxiproxy",
            f"{REAL_OPT_IN}=1 plus the five runtime TRPC_REAL_* variables and the dedicated "
            "TRPC_REAL_GLOBAL_WORKER_DATABASE_DSN/TRPC_REAL_GLOBAL_WORKER_DATABASE_ROLE",
            f"{REAL_WORKER_IDENTITIES_ENV}=comma-separated inspected worker identities",
        ],
        "command_template": command,
        "fault_stage_command_template": fault_command,
        "fault_stage_prerequisites": [
            "the dedicated Compose project must start with trpc-fault-",
            "one explicit, inspected worker container id is required",
            f"{REAL_OPT_IN}=1",
            "TRPC_RUN_FAULT_STAGE_ACCEPTANCE=1 and TRPC_FAULT_STAGE_ALLOW_KILL=1",
            "TRPC_FAULT_DATABASE_DSN, TRPC_FAULT_REDIS_URL, TRPC_FAULT_BINDING_SEED, "
            "TRPC_FAULT_SESSION_HMAC_KEY, TRPC_FAULT_RUN_ID, "
            "TRPC_FAULT_RUN_TOKEN, and TRPC_FAULT_OFFLINE_AGENT_DELAY_SECONDS",
            (
                "the child runs only the requested enqueue/tool/commit case; "
                "--scenario all runs all three"
            ),
            (
                "before process termination, every dedicated worker is read-only attested as "
                "healthy, current-source, and on one candidate image digest"
            ),
        ],
        "toxiproxy_start": [
            "docker",
            "compose",
            "-f",
            "docker-compose.yml",
            "-f",
            "deploy/toxiproxy-runtime.override.yml",
            "-p",
            args.project,
            "up",
            "-d",
            "--no-build",
            "--scale",
            f"worker={args.workers}",
            "postgres",
            "redis",
            "toxiproxy",
            "worker",
            "outbox-dispatcher",
            "channel-dispatcher",
            "post-turn-projector",
            "session-recovery",
        ],
        "rollback": [
            'restore the proxy with POST /proxies/{name} {"enabled":true}',
            "restart surviving worker and dispatcher roles",
            "verify the health endpoints and queue depth",
            "never run docker compose down -v",
        ],
        "stage_contract": (
            "enqueue/tool/commit require explicit runtime stage markers; do not infer a pass "
            "from a final committed row alone"
        ),
    }


def _not_run_report(reasons: list[str], args: argparse.Namespace) -> dict[str, Any]:
    scenarios = {
        name: {
            "status": "not_run",
            "label": definition["label"],
            "evidence": definition["evidence"],
            "assertions": list(definition["assertions"]),
            "stage_markers": _scenario_stage_markers(
                name,
                status="not_run",
                reason="real fault execution did not start",
            ),
            **({"reason": definition["blocked_reason"]} if "blocked_reason" in definition else {}),
        }
        for name, definition in SCENARIOS.items()
    }
    return {
        "schema_version": FAULT_REPORT_SCHEMA_VERSION,
        "production_contract": _production_contract(),
        "baseline": {
            "scenario_names": list(SCENARIOS),
            "all_required_for_production": True,
            "production_gate_must_not_be_upgraded_by_mock": True,
        },
        "candidate": {
            "mode": "not_run",
            "requested_scenario": args.scenario,
            "scenarios": scenarios,
        },
        "case_deltas": {"requested": list(_required_scenarios(args.scenario))},
        "gate": "not_run",
        "production_gate": "not_run",
        "rejection_reasons": reasons,
        "production_rejection_reasons": reasons,
        "runbook": _runbook(args),
    }


def _pytest_offline(output: Path) -> dict[str, Any]:
    started = time.perf_counter()
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests/unit/test_fault_injection_scenarios.py"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    return {
        "status": "pass" if completed.returncode == 0 else "fail",
        "exit_code": completed.returncode,
        "duration_seconds": time.perf_counter() - started,
        "selector": "tests/unit/test_fault_injection_scenarios.py",
        "summary": _pytest_summary(f"{completed.stdout}\n{completed.stderr}"),
        "report_output": str(output),
    }


def _pytest_summary(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if "passed" in line or "failed" in line]
    return lines[-1] if lines else "pytest did not report a summary"


def _real_command(args: argparse.Namespace, scenario: str, child_output: Path) -> list[str]:
    if scenario == "ambiguous":
        command = [
            sys.executable,
            str(ROOT / "scripts" / "ambiguous_provider_acceptance.py"),
            "--execute",
            "--project",
            args.project,
            "--timeout-seconds",
            str(min(max(float(args.timeout_seconds), 0.1), 60.0)),
            "--output",
            str(child_output),
        ]
        provider_url = str(getattr(args, "ambiguous_provider_url", "") or "").strip()
        if provider_url:
            command.extend(("--provider-url", provider_url))
        return command
    command = [
        sys.executable,
        str(ROOT / "scripts" / "real_runtime_gate.py"),
        "--execute",
        "--compose-file",
        str(args.compose_file),
        "--toxiproxy-override",
        str(args.toxiproxy_override),
        "--project",
        args.project,
        "--toxiproxy-api",
        args.toxiproxy_api,
        "--workers",
        str(args.workers),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--output",
        str(child_output),
    ]
    if scenario in {"redis_interrupt", "republish", "dlq"}:
        command.extend(
            (
                "--phase",
                "fault",
                "--fault-messages",
                str(args.fault_messages),
                "--use-toxiproxy",
            )
        )
        if scenario == "republish":
            command.append("--republish-probe")
        return command
    if scenario == "fencing":
        command.extend(
            (
                "--phase",
                "load",
                "--messages",
                str(args.messages),
                "--duplicates",
                str(args.duplicates),
                "--kill-worker",
                "--allow-process-kill",
                # The normal runtime used by the complete fault acceptance is
                # the same Toxiproxy-routed Compose project as the dependency
                # fault phases.  Keep the worker route attestation and the
                # host-side runtime connection on that one release binding.
                "--use-toxiproxy",
            )
        )
        return command
    raise ValueError(f"no real command for unsupported scenario: {scenario}")


def _fault_stage_not_run_result(scenario: str, reason: str) -> dict[str, Any]:
    """Build one stage result without implying that a process was killed."""

    return {
        "status": "not_run",
        "label": SCENARIOS[scenario]["label"],
        "mode": "real_fault_stage_acceptance",
        "stage": FAULT_STAGE_SCENARIOS[scenario],
        "reason": reason,
        "assertions": list(SCENARIOS[scenario]["assertions"]),
        "stage_markers": _scenario_stage_markers(
            scenario,
            status="not_run",
            reason=reason,
        ),
    }


def _fault_stage_result(
    scenario: str,
    *,
    status: str,
    reason: str,
    child: dict[str, Any] | None = None,
    child_output: Path | None = None,
    child_mtime_ns: int | None = None,
    exit_code: int | None = None,
    run_id: str | None = None,
    markers: list[dict[str, Any]] | None = None,
    observation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "status": status if status in {"pass", "fail", "not_run"} else "fail",
        "label": SCENARIOS[scenario]["label"],
        "mode": "real_fault_stage_acceptance",
        "stage": FAULT_STAGE_SCENARIOS[scenario],
        "reason": reason,
        "assertions": list(SCENARIOS[scenario]["assertions"]),
        "stage_markers": markers or _scenario_stage_markers(scenario, status=status, reason=reason),
    }
    if run_id:
        result["run_id"] = run_id
    if child_output is not None:
        result["child_report"] = str(child_output)
    if child_mtime_ns is not None:
        result["child_report_mtime_ns"] = child_mtime_ns
    if exit_code is not None:
        result["exit_code"] = exit_code
    if child is not None:
        result["child_schema_version"] = child.get("schema_version")
        result["child_mode"] = child.get("mode")
        result["child_gate"] = child.get("gate")
        result["child_production_gate"] = child.get("production_gate")
        if isinstance(child.get("worker_preflight"), dict):
            result["worker_preflight"] = dict(child["worker_preflight"])
        provenance = child.get("execution_provenance")
        if isinstance(provenance, dict):
            project = provenance.get("project")
            worker_container = provenance.get("worker_container")
            result["child_provenance"] = {
                "schema_version": provenance.get("schema_version"),
                "run_id": provenance.get("run_id"),
                "scheduler_version": provenance.get("scheduler_version"),
                "redis_stream": provenance.get("redis_stream"),
                "redis_group": provenance.get("redis_group"),
                "nonce_sha256": provenance.get("nonce_sha256"),
                "pid": provenance.get("pid"),
                "project_sha256": (
                    hashlib.sha256(project.encode("utf-8")).hexdigest()
                    if isinstance(project, str)
                    else None
                ),
                "worker_container_sha256": (
                    hashlib.sha256(worker_container.encode("utf-8")).hexdigest()
                    if isinstance(worker_container, str)
                    else None
                ),
            }
    if observation is not None:
        result.update(observation)
    return result


def _fault_stage_command(
    args: argparse.Namespace,
    child_output: Path,
    *,
    scenario: str | None = None,
) -> list[str] | None:
    """Return the only command used for the three exact-marker cases."""

    project = str(getattr(args, "fault_project", "") or "").strip()
    worker_container = str(getattr(args, "fault_worker_container", "") or "").strip()
    if not _valid_fault_project(project) or not _valid_container_selector(worker_container):
        return None
    command = [
        sys.executable,
        str(ROOT / "scripts" / "fault_stage_acceptance.py"),
        "--execute",
        "--project",
        project,
        "--worker-container",
        worker_container,
        "--termination",
        str(getattr(args, "fault_termination", "kill")),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--scheduler-version",
        "v2",
        "--output",
        str(child_output),
    ]
    if scenario is not None:
        if scenario not in FAULT_STAGE_SCENARIOS:
            raise ValueError(f"unsupported fault-stage scenario: {scenario}")
        command[command.index("--output") : command.index("--output")] = [
            "--scenario",
            scenario,
        ]
    if bool(getattr(args, "allow_process_kill", False)):
        command.append("--allow-process-kill")
    return command


_FAULT_PROJECT_PATTERN = re.compile(r"trpc-fault-[a-z0-9][a-z0-9_-]{0,49}\Z")
_RUNTIME_PROJECT_PATTERN = re.compile(r"trpc-fault-runtime-[a-z0-9][a-z0-9_-]{0,49}\Z")
_CONTAINER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_RUNTIME_SOURCE_LABEL = "io.trpc.agent-service.source-fingerprint"
_RUNTIME_WORKER_INSPECT_FORMAT = (
    "{{json .Config.Labels}}\t{{.Image}}\t{{.State.Status}}\t"
    "{{if .State.Health}}{{.State.Health.Status}}{{end}}"
)
_RUNTIME_WORKER_RECOVERY_TIMEOUT_SECONDS = 90.0


def _valid_fault_project(project: str) -> bool:
    return _FAULT_PROJECT_PATTERN.fullmatch(project) is not None


def _valid_container_selector(container: str) -> bool:
    return _CONTAINER_PATTERN.fullmatch(container) is not None


def _runtime_worker_inspect(container_id: str) -> dict[str, str] | None:
    """Read only the attestation fields needed for safe worker restoration."""

    if not _valid_container_selector(container_id):
        return None
    try:
        inspected = subprocess.run(  # noqa: S603 - fixed Docker inspect command
            [  # noqa: S607 - fixed executable
                "docker",
                "inspect",
                "--format",
                _RUNTIME_WORKER_INSPECT_FORMAT,
                container_id,
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if inspected.returncode != 0:
        return None
    line = inspected.stdout.strip().splitlines()
    if not line:
        return None
    fields = line[0].split("\t", 3)
    if len(fields) != 4:
        return None
    try:
        labels = json.loads(fields[0])
    except json.JSONDecodeError:
        return None
    if not isinstance(labels, dict):
        return None
    return {
        "container_id": container_id,
        "project": str(labels.get("com.docker.compose.project", "")),
        "service": str(labels.get("com.docker.compose.service", "")),
        "source_fingerprint": str(labels.get(_RUNTIME_SOURCE_LABEL, "")),
        "image_id": fields[1].strip(),
        "status": fields[2].strip().lower(),
        "health": fields[3].strip().lower(),
    }


def _runtime_worker_ids(project: str) -> tuple[str, ...] | None:
    try:
        listed = subprocess.run(  # noqa: S603 - fixed Docker worker selector
            [  # noqa: S607 - fixed executable
                "docker",
                "ps",
                "-aq",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={project}",
                "--filter",
                f"label=com.docker.compose.service={WORKER_SERVICE}",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if listed.returncode != 0:
        return None
    return tuple(
        sorted(
            {
                line.strip()
                for line in listed.stdout.splitlines()
                if _valid_container_selector(line.strip())
            }
        )
    )


def _parent_worker_preflight(project: str, required: int) -> dict[str, Any]:
    """Attest the live candidate workers for the provider-only child.

    The response-drop endpoint does not run inside a worker container, so its
    child report cannot authoritatively describe the Compose inventory.  Keep
    that observation in the parent process and require the same source/image
    binding as the rest of the fault report.
    """

    if _RUNTIME_PROJECT_PATTERN.fullmatch(project) is None:
        return {"status": "not_run", "reason": "runtime Compose project is not bounded"}
    expected_source = source_fingerprint(ROOT).get("value")
    expected_image = os.getenv("TRPC_REAL_IMAGE_DIGEST", "").strip()
    if (
        not isinstance(expected_source, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_source) is None
    ):
        return {"status": "not_run", "reason": "current source fingerprint is unavailable"}
    if IMAGE_DIGEST_RE.fullmatch(expected_image) is None:
        return {"status": "not_run", "reason": "candidate image digest is unavailable"}
    worker_ids = _runtime_worker_ids(project)
    if worker_ids is None or len(worker_ids) < max(MIN_REAL_WORKERS, required):
        return {
            "status": "not_run",
            "reason": f"runtime requires at least {max(MIN_REAL_WORKERS, required)} workers",
        }
    inspected: list[dict[str, str]] = []
    for worker_id in worker_ids:
        item = _runtime_worker_inspect(worker_id)
        if (
            item is None
            or item["project"] != project
            or item["service"] != WORKER_SERVICE
            or item["status"] != "running"
            or item["health"] != "healthy"
            or item["source_fingerprint"] != expected_source
            or item["image_id"] != expected_image
        ):
            return {
                "status": "not_run",
                "reason": "runtime worker health or candidate binding mismatched",
            }
        inspected.append(item)
    return {
        "status": "pass",
        "worker_count": len(inspected),
        "healthy_worker_count": len(inspected),
        "independent_processes": len({item["container_id"] for item in inspected})
        == len(inspected),
        "image_id": expected_image,
        "source_fingerprint": expected_source,
    }


def _fault_stage_worker_image_attestation(
    project: str,
    explicit_container: str,
    expected_source: str,
) -> dict[str, Any]:
    """Attest the dedicated fault-stage workers before any kill is requested.

    The fault-stage child intentionally owns the process-level markers, but
    its older preflight contract only proved worker count/PIDs.  Keep this
    read-only parent-side check separate from the child so a stage report
    cannot be accepted when the dedicated Compose project is serving a stale
    or mixed image.
    """

    if not re.fullmatch(r"[0-9a-f]{64}", expected_source or ""):
        return {"status": "not_run", "reason": "fault-stage source fingerprint is invalid"}
    worker_ids = _runtime_worker_ids(project)
    if worker_ids is None or len(worker_ids) < MIN_REAL_WORKERS:
        return {
            "status": "not_run",
            "reason": f"fault-stage requires at least {MIN_REAL_WORKERS} worker containers",
        }
    if explicit_container not in worker_ids:
        return {
            "status": "not_run",
            "reason": "fault-stage target worker is not in the inspected worker inventory",
        }
    inspected: list[dict[str, str]] = []
    for container_id in worker_ids:
        item = _runtime_worker_inspect(container_id)
        if item is None:
            return {"status": "not_run", "reason": "fault-stage worker inspection failed"}
        if (
            item["project"] != project
            or item["service"] != WORKER_SERVICE
            or item["status"] != "running"
            or item["health"] != "healthy"
            or item["source_fingerprint"] != expected_source
            or IMAGE_DIGEST_RE.fullmatch(item["image_id"]) is None
        ):
            return {
                "status": "not_run",
                "reason": "fault-stage worker image/source attestation mismatched",
            }
        inspected.append(item)
    image_ids = {item["image_id"] for item in inspected}
    if len(image_ids) != 1:
        return {
            "status": "not_run",
            "reason": "fault-stage workers use mixed candidate images",
        }
    return {
        "status": "pass",
        "worker_count": len(inspected),
        "image_count": len(image_ids),
        "image_id": next(iter(image_ids)),
        "source_fingerprint": expected_source,
        "source_fingerprint_matches": True,
    }


def _runtime_container_ids(value: Any) -> tuple[str, ...]:
    """Extract validated container identities from a child-report sequence."""

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    identities: list[str] = []
    for item in value:
        candidate = item.get("container_id") if isinstance(item, dict) else item
        if (
            isinstance(candidate, str)
            and _valid_container_selector(candidate)
            and candidate not in identities
        ):
            identities.append(candidate)
    return tuple(identities)


def _runtime_fencing_worker_evidence(
    child: Any,
) -> tuple[tuple[str, ...], str | None, tuple[str, ...]]:
    """Return exact pre-kill worker identities from the real-runtime schema.

    The worker preflight contains all original workers.  The fencing result
    separately records healthy survivors, while the kill record owns the
    identity of the terminated container.  Keeping these sources distinct
    prevents recovery from guessing a container from the current Docker
    inventory after the kill.
    """

    candidate = child.get("candidate") if isinstance(child, dict) else None
    if not isinstance(candidate, dict):
        return (), None, ()
    preflight = candidate.get("preflight")
    preflight_workers = preflight.get("worker_containers") if isinstance(preflight, dict) else None
    if preflight_workers is None and isinstance(preflight, dict):
        preflight_workers = preflight.get("worker_processes")
    worker_ids = list(_runtime_container_ids(preflight_workers))

    load = candidate.get("load")
    load = load if isinstance(load, dict) else {}
    fencing = load.get("fencing_takeover")
    fencing = fencing if isinstance(fencing, dict) else {}
    survivors = _runtime_container_ids(fencing.get("surviving_healthy_worker_containers"))

    killed_id: str | None = None
    worker_kill = load.get("worker_kill")
    worker_kill = worker_kill if isinstance(worker_kill, dict) else {}
    for source in (worker_kill, fencing):
        value = source.get("killed_container_id")
        if isinstance(value, str) and _valid_container_selector(value):
            killed_id = value
            break

    evidence_ids = list(survivors)
    if killed_id is not None:
        evidence_ids.append(killed_id)
    for identity in evidence_ids:
        if identity not in worker_ids:
            worker_ids.append(identity)
    return tuple(worker_ids), killed_id, survivors


def _runtime_worker_recovery(
    args: argparse.Namespace,
    scenario_result: dict[str, Any],
) -> dict[str, Any]:
    """Restore retained runtime worker containers without Compose recreation.

    A fencing child deliberately leaves its killed container exited.  Recovery
    uses only the exact Compose project/service labels and the image/source
    attestation observed by that child, then starts the retained container so
    its original environment, mounts, and network configuration remain intact.
    It never invokes ``docker compose up`` and never removes a volume.
    """

    project = str(getattr(args, "project", "") or "").strip()
    if _RUNTIME_PROJECT_PATTERN.fullmatch(project) is None:
        return {
            "status": "not_run",
            "reason": "runtime worker recovery requires a bounded trpc-fault-runtime-* project",
        }
    required = max(MIN_REAL_WORKERS, int(getattr(args, "workers", MIN_REAL_WORKERS)))
    preflight = scenario_result.get("worker_preflight")
    if not isinstance(preflight, dict) or preflight.get("status") != "pass":
        return {
            "status": "not_run",
            "reason": "runtime worker preflight attestation is unavailable",
        }
    expected_source = preflight.get("source_fingerprint") if isinstance(preflight, dict) else None
    expected_image = preflight.get("image_id")
    if not isinstance(expected_source, str) or not expected_source.strip():
        return {"status": "not_run", "reason": "runtime worker source attestation is unavailable"}
    if not isinstance(expected_image, str) or IMAGE_DIGEST_RE.fullmatch(expected_image) is None:
        return {"status": "not_run", "reason": "runtime worker image attestation is invalid"}
    retained_ids = scenario_result.get("_runtime_worker_containers")
    candidate_ids = _runtime_container_ids(retained_ids)
    killed_id = scenario_result.get("_runtime_killed_worker_container")
    if not isinstance(killed_id, str) or not _valid_container_selector(killed_id):
        return {
            "status": "not_run",
            "reason": "killed runtime worker container identity was not retained",
        }
    survivor_ids = _runtime_container_ids(
        scenario_result.get("_runtime_surviving_healthy_worker_containers")
    )
    if len(survivor_ids) < required - 1:
        return {
            "status": "not_run",
            "reason": (
                f"child fencing evidence retained only {len(survivor_ids)} healthy survivors; "
                f"need at least {required - 1}"
            ),
        }
    if killed_id in survivor_ids:
        return {
            "status": "not_run",
            "reason": "killed runtime worker is also listed as a healthy survivor",
        }
    if killed_id not in candidate_ids:
        return {
            "status": "not_run",
            "reason": "killed runtime worker is not part of the retained worker set",
        }
    if any(identity not in candidate_ids for identity in survivor_ids):
        return {
            "status": "not_run",
            "reason": "healthy survivor identity is not part of the retained worker set",
        }
    if len(candidate_ids) < required:
        return {
            "status": "not_run",
            "reason": (
                f"only {len(candidate_ids)} retained workers were identified; need {required}"
            ),
        }

    inspected: list[dict[str, str]] = []
    for container_id in candidate_ids:
        item = _runtime_worker_inspect(container_id)
        if item is None:
            return {"status": "not_run", "reason": "runtime worker identity inspection failed"}
        if item["project"] != project or item["service"] != WORKER_SERVICE:
            return {"status": "not_run", "reason": "runtime worker label attestation mismatched"}
        if item["source_fingerprint"] != expected_source:
            return {"status": "not_run", "reason": "runtime worker source attestation mismatched"}
        if IMAGE_DIGEST_RE.fullmatch(item["image_id"]) is None:
            return {"status": "not_run", "reason": "runtime worker image identity is invalid"}
        inspected.append(item)

    if any(item["image_id"] != expected_image for item in inspected):
        return {
            "status": "not_run",
            "reason": "runtime worker image attestation mismatched",
        }
    matching = inspected

    matching_by_id = {item["container_id"]: item for item in matching}
    unhealthy_survivors = [
        item
        for item in matching
        if item["container_id"] != killed_id
        and not (item["status"] == "running" and item["health"] == "healthy")
    ]
    if unhealthy_survivors:
        return {
            "status": "not_run",
            "reason": "retained healthy survivor container state is no longer confirmed",
        }
    killed = matching_by_id.get(killed_id)
    if killed is None:
        return {"status": "not_run", "reason": "killed runtime worker image attestation mismatched"}
    started_count = 0
    if killed["status"] == "running" and killed["health"] == "healthy":
        pass
    elif killed["status"] in {"exited", "dead"}:
        try:
            started = subprocess.run(  # noqa: S603 - exact retained container only
                ["docker", "start", killed_id],  # noqa: S607 - fixed executable
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired):
            return {"status": "fail", "reason": "retained runtime worker could not be started"}
        if started.returncode != 0:
            return {"status": "fail", "reason": "retained runtime worker start was rejected"}
        started_count = 1
    else:
        return {
            "status": "not_run",
            "reason": "killed runtime worker state is not a safely restorable exited container",
        }

    deadline = time.monotonic() + min(
        max(float(getattr(args, "timeout_seconds", 30.0)), 30.0),
        _RUNTIME_WORKER_RECOVERY_TIMEOUT_SECONDS,
    )
    latest = inspected
    while time.monotonic() < deadline:
        refreshed = [_runtime_worker_inspect(item["container_id"]) for item in matching]
        if all(item is not None for item in refreshed):
            latest = [item for item in refreshed if item is not None]
            healthy_count = sum(
                item["status"] == "running" and item["health"] == "healthy" for item in latest
            )
            killed_latest = next(
                (item for item in latest if item["container_id"] == killed_id),
                None,
            )
            killed_healthy = bool(
                killed_latest
                and killed_latest["status"] == "running"
                and killed_latest["health"] == "healthy"
            )
            if healthy_count >= required and killed_healthy:
                return {
                    "status": "pass",
                    "required_worker_count": required,
                    "worker_count": len(latest),
                    "healthy_worker_count": healthy_count,
                    "started_count": started_count,
                    "killed_container_id": killed_id,
                    "surviving_healthy_worker_containers": list(survivor_ids),
                    "worker_container_ids": [item["container_id"] for item in latest],
                    "source_fingerprint": expected_source,
                    "image_id": expected_image,
                }
        time.sleep(0.2)
    healthy_count = sum(
        item["status"] == "running" and item["health"] == "healthy" for item in latest
    )
    return {
        "status": "fail",
        "reason": (
            f"runtime worker recovery ended with {healthy_count} healthy workers; need {required}"
        ),
        "required_worker_count": required,
        "worker_count": len(latest),
        "healthy_worker_count": healthy_count,
        "started_count": started_count,
    }


def _expected_child_provenance(
    *,
    nonce: str,
    run_id: str,
    project: str,
    worker_container: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "project": project,
        "worker_container": worker_container,
        "scheduler_version": "v2",
        "redis_stream": "trpc:session-ready:v2",
        "redis_group": "trpc-session-ready-v2",
        "nonce_sha256": hashlib.sha256(nonce.encode("utf-8")).hexdigest(),
    }


def _fault_stage_markers(case: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    raw_markers = case.get("markers")
    if not isinstance(raw_markers, list):
        return [], ["case markers are missing or not a list"]
    markers = [marker for marker in raw_markers if isinstance(marker, dict)]
    if len(markers) != len(raw_markers):
        return markers, ["case markers contain a non-object entry"]
    by_name = {
        str(marker.get("name")): marker
        for marker in markers
        if isinstance(marker.get("name"), str) and marker.get("name")
    }
    stage = case.get("stage")
    if stage not in FAULT_STAGE_REQUIRED_MARKERS:
        return markers, ["case stage is not one of the required fault stages"]
    required = FAULT_STAGE_REQUIRED_MARKERS[stage]
    missing = [name for name in required if name not in by_name]
    nonpassing = [
        name for name in required if name in by_name and by_name[name].get("status") != "pass"
    ]
    malformed_timestamps = [
        str(marker.get("name"))
        for marker in markers
        if not isinstance(marker.get("observed_at"), str)
        or not marker.get("observed_at", "").strip()
    ]
    errors: list[str] = []
    if missing:
        errors.append("missing required fault-stage markers: " + ",".join(missing))
    if nonpassing:
        errors.append("required fault-stage markers are not pass: " + ",".join(nonpassing))
    if malformed_timestamps:
        errors.append("fault-stage markers have no observed_at: " + ",".join(malformed_timestamps))
    return markers, errors


def _validate_fault_stage_child(
    child: Any,
    *,
    expected_run_id: str,
    expected_provenance: dict[str, Any] | None = None,
    expected_stages: tuple[str, ...] = FAULT_STAGE_REQUIRED_STAGES,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Validate the child contract before any case can be reported as pass."""

    errors: list[str] = []
    if not isinstance(child, dict):
        return {}, ["fault-stage child report is not a JSON object"]
    if child.get("schema_version") != 1:
        errors.append("fault-stage child schema_version must be 1")
    if child.get("mode") != "fault_stage_acceptance":
        errors.append("fault-stage child mode must be fault_stage_acceptance")
    if child.get("run_id") != expected_run_id:
        errors.append("fault-stage child run_id does not match")
    nonce_hash = child.get("run_nonce_sha256")
    if not isinstance(nonce_hash, str) or re.fullmatch(r"[0-9a-fA-F]{64}", nonce_hash) is None:
        errors.append("fault-stage child run_nonce_sha256 is missing or invalid")
    started_at = child.get("started_at")
    ended_at = child.get("ended_at")
    try:
        started_dt = (
            datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            if isinstance(started_at, str)
            else None
        )
        ended_dt = (
            datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
            if isinstance(ended_at, str)
            else None
        )
    except ValueError:
        started_dt = ended_dt = None
    if (
        started_dt is None
        or ended_dt is None
        or started_dt.tzinfo is None
        or ended_dt.tzinfo is None
        or ended_dt < started_dt
    ):
        errors.append("fault-stage child started_at/ended_at are missing or invalid")
    provenance = child.get("execution_provenance")
    if expected_provenance is not None:
        if not isinstance(provenance, dict):
            errors.append("fault-stage child execution provenance is missing")
        else:
            for field, expected in expected_provenance.items():
                if provenance.get(field) != expected:
                    errors.append(f"fault-stage child provenance {field} does not match")
            if not isinstance(provenance.get("pid"), int) or provenance.get("pid", 0) < 1:
                errors.append("fault-stage child provenance pid is invalid")
    cases = child.get("cases")
    if not isinstance(cases, list):
        return {}, [*errors, "fault-stage child cases must be a list"]
    by_stage: dict[str, dict[str, Any]] = {}
    for case in cases:
        if not isinstance(case, dict):
            errors.append("fault-stage child contains a non-object case")
            continue
        stage = case.get("stage")
        if not isinstance(stage, str):
            errors.append("fault-stage child case has no stage")
            continue
        if stage in by_stage:
            errors.append(f"fault-stage child contains duplicate case: {stage}")
        by_stage[stage] = case
        status = case.get("status")
        if status not in {"pass", "fail", "not_run"}:
            errors.append(f"fault-stage case {stage} has an invalid status")
        identity = case.get("case")
        if not isinstance(identity, dict):
            errors.append(f"fault-stage case {stage} has no case identity")
        elif identity.get("run_id") != expected_run_id:
            errors.append(f"fault-stage case {stage} run_id does not match")
        if status == "pass":
            required_identity = ("case_id", "tenant_id", "session_id", "inbound_id", "message_id")
            identity_incomplete = (
                any(
                    not isinstance(identity.get(field), str) or not identity[field].strip()
                    for field in required_identity
                )
                if isinstance(identity, dict)
                else True
            )
            if identity_incomplete:
                errors.append(f"fault-stage case {stage} identity is incomplete")
            if not isinstance(case.get("control_id"), str) or not case["control_id"].strip():
                errors.append(f"fault-stage case {stage} has no control id")
            killed_container_id = case.get("killed_container_id")
            if not isinstance(killed_container_id, str) or not killed_container_id.strip():
                errors.append(f"fault-stage case {stage} has no killed container identity")
            _markers, marker_errors = _fault_stage_markers(case)
            errors.extend(f"{stage}: {error}" for error in marker_errors)
    expected = set(expected_stages)
    unexpected = set(by_stage) - expected
    if unexpected:
        errors.append(
            "fault-stage child contains cases outside the requested selection: "
            + ",".join(sorted(unexpected))
        )
    if set(by_stage) != expected:
        errors.append(
            "fault-stage child must contain exactly the requested cases: "
            + ",".join(expected_stages)
        )
    child_gate = child.get("gate")
    if child_gate not in {"pass", "fail", "not_run"}:
        errors.append("fault-stage child gate is missing or invalid")
    if child.get("production_gate") not in {"pass", "fail", "not_run"}:
        errors.append("fault-stage child production_gate is missing or invalid")
    if child_gate == "pass":
        if any(by_stage.get(stage, {}).get("status") != "pass" for stage in expected_stages):
            errors.append("fault-stage child gate=pass requires all requested cases to pass")
        expected_production_gate = (
            "pass" if expected == set(FAULT_STAGE_REQUIRED_STAGES) else "not_run"
        )
        if child.get("production_gate") != expected_production_gate:
            errors.append(
                "fault-stage child gate=pass requires production_gate=" + expected_production_gate
            )
    return by_stage, errors


def _requested_fault_stage_scenarios(args: argparse.Namespace) -> tuple[str, ...]:
    requested = str(getattr(args, "scenario", "all") or "all")
    if requested == "all":
        return tuple(FAULT_STAGE_SCENARIOS)
    if requested in FAULT_STAGE_SCENARIOS:
        return (requested,)
    raise ValueError(f"unsupported fault-stage scenario: {requested}")


def _restore_fault_stage_workers(project: str, explicit_container: str) -> bool:
    """Restore and verify only worker containers in the fault project.

    This runs after the child is forcibly timed out.  The label filters and
    explicit project prefix keep recovery bounded to the dedicated fault
    Compose project; no service-wide teardown or volume operation is used.
    A successful ``docker start`` is not enough: the caller only receives
    ``True`` after at least four retained workers are running and healthy.
    """

    if not _valid_fault_project(project) or not _valid_container_selector(explicit_container):
        return False
    listed = _runtime_worker_ids(project)
    if listed is None:
        return False
    container_ids = set(listed)
    # The selected container is always included, even if Docker's list output
    # is temporarily stale after the child terminates it.
    container_ids.add(explicit_container)
    restored = False
    for container_id in sorted(container_ids):
        inspected = _runtime_worker_inspect(container_id)
        if inspected is None or (
            inspected["project"] != project or inspected["service"] != WORKER_SERVICE
        ):
            continue
        if inspected["status"] == "running" and inspected["health"] == "healthy":
            continue
        try:
            started = subprocess.run(  # noqa: S603 - fixed Docker recovery command
                ["docker", "start", container_id],  # noqa: S607 - fixed executable
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        restored = restored or started.returncode == 0
    if not restored and len(container_ids) < MIN_REAL_WORKERS:
        return False
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        healthy = 0
        for container_id in container_ids:
            inspected = _runtime_worker_inspect(container_id)
            if (
                inspected is not None
                and inspected["project"] == project
                and inspected["service"] == WORKER_SERVICE
                and inspected["status"] == "running"
                and inspected["health"] == "healthy"
            ):
                healthy += 1
        if healthy >= MIN_REAL_WORKERS:
            return True
        time.sleep(0.2)
    return False


def _run_fault_stage_acceptance(
    args: argparse.Namespace,
    *,
    expected_source_fingerprint: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Run one selected stage (or all three) in one bounded child process."""

    requested_scenarios = _requested_fault_stage_scenarios(args)
    expected_stages = tuple(FAULT_STAGE_SCENARIOS[name] for name in requested_scenarios)

    def _not_run_for_requested(reason: str) -> dict[str, dict[str, Any]]:
        return {
            scenario: _fault_stage_not_run_result(scenario, reason)
            for scenario in requested_scenarios
        }

    worker_image_attestation: dict[str, Any] | None = None

    def _finish(results: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Attach the independent image check to every retained child result."""

        if worker_image_attestation is None:
            return results
        for result in results.values():
            preflight = result.get("worker_preflight")
            if isinstance(preflight, dict):
                preflight["image_attestation"] = dict(worker_image_attestation)
                preflight["image_id"] = worker_image_attestation.get("image_id")
                preflight["source_fingerprint"] = worker_image_attestation.get("source_fingerprint")
        return results

    command_output = getattr(args, "output", Path("runs/multitenant/fault-injection.json"))
    output = Path(command_output)
    project = str(getattr(args, "fault_project", "") or "").strip()
    worker_container = str(getattr(args, "fault_worker_container", "") or "").strip()
    if not project or not worker_container:
        reason = "fault-stage requires --fault-project and --fault-worker-container"
        return _not_run_for_requested(reason)
    if not _valid_fault_project(project):
        reason = "fault-stage project must be a bounded trpc-fault-* Compose project"
        return _not_run_for_requested(reason)
    if not _valid_container_selector(worker_container):
        reason = "fault-stage worker container selector is unsafe or unbounded"
        return _not_run_for_requested(reason)
    if project == str(getattr(args, "project", "") or "").strip():
        reason = "fault-stage project must be distinct from the normal Compose project"
        return _not_run_for_requested(reason)
    expected_run_id = str(os.getenv("TRPC_FAULT_RUN_ID", "") or "").strip()
    if not expected_run_id:
        reason = "TRPC_FAULT_RUN_ID is required for fault-stage report identity"
        return _not_run_for_requested(reason)
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", expected_run_id) is None:
        reason = "TRPC_FAULT_RUN_ID contains unsafe path characters"
        return _not_run_for_requested(reason)
    try:
        output = _assert_safe_report_path(output)
        report_directory = output.parent
        staging_scope = _staging_scope(report_directory, expected_run_id)
        child_output = _assert_safe_report_path(
            staging_scope / f"{output.stem}.fault-stage.child.json",
            within=staging_scope,
        )
        staging_scope.mkdir(parents=True, exist_ok=True)
        staging_scope = _assert_safe_report_path(staging_scope, within=report_directory)
        retained_scope = _run_scoped_evidence_scope(report_directory, expected_run_id)
        retained_child_output = _assert_safe_report_path(
            retained_scope / "fault-stage.child.json",
            within=retained_scope,
        )
    except RuntimeError as error:
        return _not_run_for_requested(str(error))
    if child_output.exists():
        reason = "unique fault-stage child report path already exists; refusing to reuse it"
        return {
            scenario: _fault_stage_result(
                scenario,
                status="fail",
                reason=reason,
                child_output=retained_child_output,
                run_id=expected_run_id,
            )
            for scenario in requested_scenarios
        }
    command = _fault_stage_command(
        args,
        child_output,
        scenario=(requested_scenarios[0] if len(requested_scenarios) == 1 else None),
    )
    if command is None:
        reason = "fault-stage command selectors are incomplete or not dedicated"
        return _not_run_for_requested(reason)
    if expected_source_fingerprint is not None:
        expected_source = expected_source_fingerprint.get("value")
        if not isinstance(expected_source, str):
            return _not_run_for_requested("fault-stage current source fingerprint is unavailable")
        worker_image_attestation = _fault_stage_worker_image_attestation(
            project,
            worker_container,
            expected_source,
        )
        if worker_image_attestation.get("status") != "pass":
            return _not_run_for_requested(str(worker_image_attestation.get("reason")))
    # Bind the child report to this exact invocation.  The digest is emitted
    # instead of the nonce, so reports never contain a reusable secret.
    evidence_nonce = secrets.token_urlsafe(32)
    expected_provenance = _expected_child_provenance(
        nonce=evidence_nonce,
        run_id=expected_run_id,
        project=project,
        worker_container=worker_container,
    )
    started_ns = time.time_ns()
    child_started_at = _utc_timestamp()
    child_environment = os.environ.copy()
    child_environment.update(
        {
            "TRPC_FAULT_EVIDENCE_NONCE": evidence_nonce,
            "TRPC_FAULT_PROJECT": project,
            "TRPC_FAULT_WORKER_CONTAINER": worker_container,
            "TRPC_FAULT_SCHEDULER_VERSION": "v2",
            "TRPC_FAULT_REDIS_STREAM": "trpc:session-ready:v2",
            "TRPC_FAULT_REDIS_GROUP": "trpc-session-ready-v2",
        }
    )
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local acceptance runner and arguments
            command,
            cwd=ROOT,
            env=child_environment,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=max(float(args.timeout_seconds) * len(requested_scenarios) + 60.0, 120.0),
        )
    except subprocess.TimeoutExpired:
        child_ended_at = _utc_timestamp()
        restored = _restore_fault_stage_workers(project, worker_container)
        reason = "fault-stage acceptance child timed out; no result is trusted; " + (
            "worker containers were restored"
            if restored
            else "worker restoration was not confirmed"
        )
        return _finish(
            {
                scenario: _fault_stage_result(
                    scenario,
                    status="fail",
                    reason=reason,
                    child_output=retained_child_output,
                    run_id=expected_run_id,
                    observation=_child_observation(
                        child_output=retained_child_output,
                        child=None,
                        report_directory=retained_scope,
                        started_at=child_started_at,
                        ended_at=child_ended_at,
                        observed_exit_code=None,
                    ),
                )
                for scenario in requested_scenarios
            }
        )
    except OSError as error:
        child_ended_at = _utc_timestamp()
        reason = f"fault-stage acceptance child could not start: {type(error).__name__}"
        return _finish(
            {
                scenario: _fault_stage_result(
                    scenario,
                    status="fail",
                    reason=reason,
                    child_output=retained_child_output,
                    run_id=expected_run_id,
                    observation=_child_observation(
                        child_output=retained_child_output,
                        child=None,
                        report_directory=retained_scope,
                        started_at=child_started_at,
                        ended_at=child_ended_at,
                        observed_exit_code=None,
                    ),
                )
                for scenario in requested_scenarios
            }
        )
    child_ended_at = _utc_timestamp()
    if completed.returncode != 0:
        restored = _restore_fault_stage_workers(project, worker_container)
        reason = f"fault-stage acceptance child exited with code {completed.returncode}; " + (
            "worker containers were restored"
            if restored
            else "worker restoration was not confirmed"
        )
        return _finish(
            {
                scenario: _fault_stage_result(
                    scenario,
                    status="fail",
                    reason=reason,
                    child_output=retained_child_output,
                    run_id=expected_run_id,
                    exit_code=completed.returncode,
                    observation=_child_observation(
                        child_output=retained_child_output,
                        child=None,
                        report_directory=retained_scope,
                        started_at=child_started_at,
                        ended_at=child_ended_at,
                        observed_exit_code=completed.returncode,
                    ),
                )
                for scenario in requested_scenarios
            }
        )
    try:
        child_output = _assert_safe_report_path(child_output, within=report_directory)
        child_stat = child_output.stat()
        if not stat.S_ISREG(child_stat.st_mode):
            raise RuntimeError("fault-stage child report is not a regular file")
        if child_stat.st_mtime_ns + 5_000_000_000 < started_ns:
            raise RuntimeError("fault-stage child report predates this invocation")
        child = _strict_json_loads(child_output.read_text(encoding="utf-8"))
        if not isinstance(child, dict):
            raise RuntimeError("fault-stage child report root must be a JSON object")
        retained_child_output, retained_scope = _retained_child_report(
            child_output,
            report_directory=report_directory,
            run_id=expected_run_id,
            filename="fault-stage.child.json",
            child=child,
        )
        child_stat = retained_child_output.stat()
        observation = _child_observation(
            child_output=retained_child_output,
            child=child,
            report_directory=retained_scope,
            started_at=child_started_at,
            ended_at=child_ended_at,
            observed_exit_code=completed.returncode,
        )
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        restored = _restore_fault_stage_workers(project, worker_container)
        reason = f"fault-stage child report is invalid: {type(error).__name__}"
        reason += (
            "; worker containers were restored"
            if restored
            else "; worker restoration was not confirmed"
        )
        return _finish(
            {
                scenario: _fault_stage_result(
                    scenario,
                    status="fail",
                    reason=reason,
                    child_output=retained_child_output,
                    run_id=expected_run_id,
                    exit_code=completed.returncode,
                    observation=_child_observation(
                        child_output=retained_child_output,
                        child=None,
                        report_directory=retained_scope,
                        started_at=child_started_at,
                        ended_at=child_ended_at,
                        observed_exit_code=completed.returncode,
                    ),
                )
                for scenario in requested_scenarios
            }
        )
    by_stage, validation_errors = _validate_fault_stage_child(
        child,
        expected_run_id=expected_run_id,
        expected_provenance=expected_provenance,
        expected_stages=expected_stages,
    )
    child_gate = child.get("gate") if isinstance(child, dict) else None
    if validation_errors:
        status = "not_run" if child_gate == "not_run" else "fail"
        reason = "; ".join(validation_errors)
        return _finish(
            {
                scenario: _fault_stage_result(
                    scenario,
                    status=status,
                    reason=reason,
                    child=child,
                    child_output=retained_child_output,
                    child_mtime_ns=child_stat.st_mtime_ns,
                    exit_code=completed.returncode,
                    run_id=expected_run_id,
                    markers=(
                        by_stage.get(FAULT_STAGE_SCENARIOS[scenario], {}).get("markers")
                        if isinstance(by_stage.get(FAULT_STAGE_SCENARIOS[scenario]), dict)
                        else None
                    ),
                    observation=observation,
                )
                for scenario in requested_scenarios
            }
        )
    result: dict[str, dict[str, Any]] = {}
    for scenario in requested_scenarios:
        stage = FAULT_STAGE_SCENARIOS[scenario]
        case = by_stage[stage]
        markers = [marker for marker in case["markers"] if isinstance(marker, dict)]
        case_status = str(case["status"])
        # The child gate is an aggregate and can be non-pass because a later
        # selected case timed out.  Preserve each case's independently
        # validated status instead of dragging earlier evidence down with it.
        status = case_status if case_status in {"pass", "fail", "not_run"} else "fail"
        reason = (
            ""
            if status == "pass"
            else str(case.get("reason") or child.get("reason") or "fault-stage child did not run")
        )
        result[scenario] = _fault_stage_result(
            scenario,
            status=status,
            reason=reason,
            child=child,
            child_output=retained_child_output,
            child_mtime_ns=child_stat.st_mtime_ns,
            exit_code=completed.returncode,
            run_id=expected_run_id,
            markers=markers,
            observation=observation,
        )
        result[scenario]["case_id"] = case.get("case", {}).get("case_id")
        result[scenario]["case_status"] = case_status
        case_identity = case.get("case")
        if isinstance(case_identity, dict):
            encoded_identity = json.dumps(
                case_identity,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            result[scenario]["case_identity_sha256"] = hashlib.sha256(encoded_identity).hexdigest()
        for source_key, result_key in (
            ("control_id", "control_id_sha256"),
            ("killed_container_id", "killed_container_sha256"),
        ):
            value = case.get(source_key)
            if isinstance(value, str) and value:
                result[scenario][result_key] = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return _finish(result)


def _merge_stage_markers(phase_markers: Any, selected_markers: Any) -> list[dict[str, Any]]:
    """Merge phase and component evidence by marker name without inventing it."""

    by_name: dict[str, dict[str, Any]] = {}
    for source in (phase_markers, selected_markers):
        if not isinstance(source, list):
            continue
        for marker in source:
            if not isinstance(marker, dict) or not marker.get("name"):
                continue
            by_name[str(marker["name"])] = marker
    return list(by_name.values())


def _worker_preflight_summary(child: Any) -> dict[str, Any]:
    """Validate and reduce the real-runtime worker inventory from a child report.

    The parent must not infer process independence from a caller-supplied
    ``TRPC_REAL_WORKER_IDENTITIES`` list.  The child preflight is the evidence
    boundary that contains Docker container IDs, positive PIDs, health state,
    immutable image IDs, and the current source label.  This helper returns
    only aggregate, report-safe metadata while rejecting incomplete evidence.
    """

    candidate = child.get("candidate") if isinstance(child, dict) else None
    preflight = candidate.get("preflight") if isinstance(candidate, dict) else None
    if not isinstance(preflight, dict) or preflight.get("status") != "pass":
        return {"status": "not_run", "reason": "child preflight did not pass"}
    workers = preflight.get("worker_containers")
    if workers is None:
        workers = preflight.get("worker_processes")
    if (
        not isinstance(workers, Sequence)
        or isinstance(workers, (str, bytes, bytearray))
        or len(workers) < MIN_REAL_WORKERS
    ):
        return {
            "status": "not_run",
            "reason": f"child preflight must expose at least {MIN_REAL_WORKERS} workers",
        }
    image_attestation = preflight.get("image_attestation")
    if not isinstance(image_attestation, dict) or image_attestation.get("status") != "pass":
        return {"status": "not_run", "reason": "child image attestation did not pass"}
    expected_source = image_attestation.get("source_fingerprint")
    current_source = source_fingerprint(ROOT)
    if (
        not isinstance(expected_source, str)
        or not expected_source.strip()
        or current_source.get("status") != "available"
        or current_source.get("value") != expected_source
    ):
        return {"status": "not_run", "reason": "child source fingerprint is stale or unavailable"}
    container_ids: set[str] = set()
    pids: set[int] = set()
    image_ids: set[str] = set()
    healthy_count = 0
    for worker in workers:
        if not isinstance(worker, dict):
            return {"status": "not_run", "reason": "child worker identity is malformed"}
        container_id = worker.get("container_id")
        pid = worker.get("pid")
        image_id = worker.get("image_id")
        if (
            not isinstance(container_id, str)
            or not container_id.strip()
            or not isinstance(pid, int)
            or isinstance(pid, bool)
            or pid <= 0
            or not isinstance(image_id, str)
            or IMAGE_DIGEST_RE.fullmatch(image_id) is None
            or worker.get("status") != "running"
            or worker.get("health") != "healthy"
            or worker.get("source_fingerprint") != expected_source
        ):
            return {
                "status": "not_run",
                "reason": "child worker health/identity evidence is incomplete",
            }
        container_ids.add(container_id)
        pids.add(pid)
        image_ids.add(image_id)
        healthy_count += 1
    if len(container_ids) != len(workers) or len(pids) != len(workers):
        return {"status": "not_run", "reason": "child worker identities are not independent"}
    expected_image = image_attestation.get("image_id")
    if len(image_ids) != 1 or expected_image not in image_ids:
        return {"status": "not_run", "reason": "child workers use mixed or unverified images"}
    if image_attestation.get("worker_count") != len(workers):
        return {"status": "not_run", "reason": "child image attestation count does not match"}
    return {
        "status": "pass",
        "worker_count": len(workers),
        "healthy_worker_count": healthy_count,
        "independent_processes": True,
        "image_id": next(iter(image_ids)),
        "source_fingerprint": expected_source,
    }


def _run_real_scenario(args: argparse.Namespace, scenario: str) -> dict[str, Any]:
    if scenario not in SUPPORTED_REAL:
        definition = SCENARIOS[scenario]
        reason = definition.get("blocked_reason", "scenario has no real runner in this release")
        return {
            "status": "not_run",
            "label": definition["label"],
            "reason": reason,
            "assertions": list(definition["assertions"]),
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason=reason,
            ),
        }
    if scenario == "fencing" and not args.allow_process_kill:
        return {
            "status": "not_run",
            "label": SCENARIOS[scenario]["label"],
            "reason": "fencing requires --allow-process-kill",
            "assertions": list(SCENARIOS[scenario]["assertions"]),
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="fencing requires --allow-process-kill",
            ),
        }
    if not args.compose_file.is_file() or not args.toxiproxy_override.is_file():
        return {
            "status": "not_run",
            "label": SCENARIOS[scenario]["label"],
            "reason": "Compose file or Toxiproxy override is missing",
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="Compose file or Toxiproxy override is missing",
            ),
        }
    run_id = f"{scenario}-{uuid4().hex}"
    try:
        parent_output = _assert_safe_report_path(Path(args.output))
        report_directory = parent_output.parent
        staging_scope = _staging_scope(report_directory, run_id)
        child_output = _assert_safe_report_path(
            staging_scope / f"{parent_output.stem}.{scenario}.child.json",
            within=staging_scope,
        )
        staging_scope.mkdir(parents=True, exist_ok=True)
        staging_scope = _assert_safe_report_path(staging_scope, within=report_directory)
        retained_scope = _run_scoped_evidence_scope(report_directory, run_id)
        retained_child_output = _assert_safe_report_path(
            retained_scope / f"{scenario}.child.json",
            within=retained_scope,
        )
    except RuntimeError as error:
        return {
            "status": "not_run",
            "label": SCENARIOS[scenario]["label"],
            "reason": str(error),
            "run_id": run_id,
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason=str(error),
            ),
        }
    if child_output.exists():
        return {
            "status": "fail",
            "label": SCENARIOS[scenario]["label"],
            "reason": "unique child report path already exists; refusing to reuse it",
            "run_id": run_id,
            "child_report": str(retained_child_output),
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="unique child report path was not available",
            ),
        }
    started_ns = time.time_ns()
    child_started_at = _utc_timestamp()
    command = _real_command(args, scenario, child_output)
    started = time.perf_counter()
    child_environment = os.environ.copy()
    child_environment["TRPC_REAL_RUN_ID"] = run_id
    try:
        completed = subprocess.run(  # noqa: S603 - fixed local runner and audited arguments
            command,
            cwd=ROOT,
            env=child_environment,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=max(args.timeout_seconds + 60, 120),
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        child_ended_at = _utc_timestamp()
        return {
            "status": "fail",
            "label": SCENARIOS[scenario]["label"],
            "error_type": type(error).__name__,
            **_child_observation(
                child_output=retained_child_output,
                child=None,
                report_directory=retained_scope,
                started_at=child_started_at,
                ended_at=child_ended_at,
                observed_exit_code=None,
            ),
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="real scenario subprocess did not produce evidence",
            ),
        }
    try:
        child_output = _assert_safe_report_path(child_output, within=report_directory)
        child_stat = child_output.stat()
        if not stat.S_ISREG(child_stat.st_mode):
            raise RuntimeError("real child report is not a regular file")
        # Windows filesystem timestamp granularity can make a just-created
        # file appear a few milliseconds before the wall-clock sample.
        if child_stat.st_mtime_ns + 5_000_000_000 < started_ns:
            raise RuntimeError("child report predates this scenario invocation")
        child = _strict_json_loads(child_output.read_text(encoding="utf-8"))
        if not isinstance(child, dict):
            raise RuntimeError("real child report root must be a JSON object")
        retained_child_output, retained_scope = _retained_child_report(
            child_output,
            report_directory=report_directory,
            run_id=run_id,
            filename=f"{scenario}.child.json",
            child=child,
        )
        child_stat = retained_child_output.stat()
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        return {
            "status": "fail",
            "label": SCENARIOS[scenario]["label"],
            "error_type": type(error).__name__,
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="real scenario report could not be read",
            ),
        }
    child_ended_at = _utc_timestamp()
    observation = _child_observation(
        child_output=retained_child_output,
        child=child,
        report_directory=retained_scope,
        started_at=child_started_at,
        ended_at=child_ended_at,
        observed_exit_code=completed.returncode,
    )
    worker_preflight = (
        _parent_worker_preflight(args.project, args.workers)
        if scenario == "ambiguous"
        else _worker_preflight_summary(child)
    )
    (
        runtime_worker_container_ids,
        killed_worker_container_id,
        surviving_healthy_worker_ids,
    ) = _runtime_fencing_worker_evidence(child)
    if child.get("run_id") != run_id:
        return {
            "status": "fail",
            "label": SCENARIOS[scenario]["label"],
            "reason": "child report run_id does not match this invocation",
            "run_id": run_id,
            "child_run_id": child.get("run_id"),
            **_child_observation(
                child_output=retained_child_output,
                child=child,
                report_directory=retained_scope,
                started_at=child_started_at,
                ended_at=child_ended_at,
                observed_exit_code=completed.returncode,
            ),
            "stage_markers": _scenario_stage_markers(
                scenario,
                status="not_run",
                reason="child report identity was not verified",
            ),
        }
    selected: dict[str, Any]
    if scenario == "ambiguous":
        selected = child.get("candidate", {}).get(
            "ambiguous",
            {"status": "not_run", "reason": "child report has no ambiguous evidence"},
        )
    elif scenario in {"redis_interrupt", "republish", "dlq"}:
        selected = (
            child.get("candidate", {})
            .get("faults", {})
            .get(
                {"redis_interrupt": "redis", "republish": "redis", "dlq": "dlq"}[scenario],
                {"status": "not_run", "reason": "child report has no selected evidence"},
            )
        )
    else:
        load = child.get("candidate", {}).get("load", {})
        selected = load.get(
            "fencing_takeover",
            {"status": "not_run", "reason": "child report has no fencing evidence"},
        )
        selected = {
            **selected,
            "stage_markers": load.get(
                "stage_markers",
                _scenario_stage_markers(
                    scenario,
                    status="not_run",
                    reason="child report has no stage-marker evidence",
                ),
            ),
        }
    phase_name = (
        "ambiguous" if scenario == "ambiguous" else "load" if scenario == "fencing" else "fault"
    )
    child_phase_key = (
        phase_name if scenario == "ambiguous" else ("load" if scenario == "fencing" else "faults")
    )
    child_phase = child.get("candidate", {}).get(child_phase_key, {})
    phase_markers = child_phase.get("stage_markers", []) if isinstance(child_phase, dict) else []
    selected_markers = selected.get("stage_markers", [])
    merged_markers = _merge_stage_markers(phase_markers, selected_markers)
    selected = {**selected, "stage_markers": merged_markers}
    if scenario == "republish" and isinstance(child_phase, dict):
        selected["duplicate_publish_probe"] = child_phase.get(
            "republish_duplicate_publish_probe",
            {"status": "not_run", "reason": "child report omitted duplicate publish probe"},
        )
    validation_errors: list[str] = []
    if not observation.get("child_identity_verified"):
        validation_errors.append("child nonce/run_id evidence is missing or invalid")
    if not observation.get("child_timestamps_verified"):
        validation_errors.append("child started_at/ended_at evidence is missing or invalid")
    if completed.returncode != 0:
        validation_errors.append(f"child exited with code {completed.returncode}")
    if child.get("gate") != "pass":
        validation_errors.append(f"child gate={child.get('gate', 'missing')}")
    if child.get("production_gate") != "not_run":
        validation_errors.append(
            "scoped child production_gate must be not_run; observed "
            f"{child.get('production_gate', 'missing')}"
        )
    if not isinstance(child_phase, dict) or child_phase.get("status") != "pass":
        validation_errors.append(f"child {phase_name} phase did not pass")
    marker_by_name = {
        str(marker["name"]): marker
        for marker in merged_markers
        if isinstance(marker, dict) and marker.get("name")
    }
    required_markers = SCENARIO_STAGE_MARKERS[scenario]
    missing_markers = [name for name in required_markers if name not in marker_by_name]
    nonpassing_markers = [
        name
        for name in required_markers
        if name in marker_by_name and marker_by_name[name].get("status") != "pass"
    ]
    if missing_markers:
        validation_errors.append(f"missing required stage markers: {','.join(missing_markers)}")
    if nonpassing_markers:
        validation_errors.append(
            "required stage markers are not pass: " + ",".join(nonpassing_markers)
        )
    if scenario == "republish":
        probe = (
            child_phase.get("republish_duplicate_publish_probe")
            if isinstance(child_phase, dict)
            else None
        )
        if not isinstance(probe, dict) or probe.get("status") != "pass":
            validation_errors.append(
                "republish requires an explicit active duplicate Redis publish probe"
            )
    phase_status = child_phase.get("status") if isinstance(child_phase, dict) else None
    status = (
        "not_run"
        if (
            child.get("gate") == "not_run"
            or phase_status == "not_run"
            or missing_markers
            or nonpassing_markers
            or (scenario == "republish" and validation_errors)
        )
        else "fail"
        if validation_errors
        else str(selected.get("status", "not_run"))
    )
    return {
        "status": status if status in {"pass", "fail", "not_run"} else "fail",
        "label": SCENARIOS[scenario]["label"],
        "duration_seconds": time.perf_counter() - started,
        "run_id": run_id,
        "child_run_id": child.get("run_id"),
        **_child_observation(
            child_output=retained_child_output,
            child=child,
            report_directory=retained_scope,
            started_at=child_started_at,
            ended_at=child_ended_at,
            observed_exit_code=completed.returncode,
        ),
        "child_report_mtime_ns": child_stat.st_mtime_ns,
        "child_phase": phase_name,
        "child_phase_status": child_phase.get("status") if isinstance(child_phase, dict) else None,
        "reason": "; ".join(validation_errors) if validation_errors else "",
        "child_gate": child.get("gate", "not_run"),
        "child_production_gate": child.get("production_gate", "not_run"),
        "evidence": selected,
        **(
            {"duplicate_publish_probe": selected.get("duplicate_publish_probe")}
            if scenario == "republish"
            else {}
        ),
        "stage_markers": merged_markers
        or _scenario_stage_markers(
            scenario,
            status="not_run",
            reason="child report has no stage-marker evidence",
        ),
        "child_report": str(retained_child_output),
        "exit_code": completed.returncode,
        "worker_preflight": worker_preflight,
        # This is consumed immediately by _real_report for post-fencing
        # recovery and removed before the machine-readable report is written.
        "_runtime_worker_containers": list(runtime_worker_container_ids),
        "_runtime_killed_worker_container": killed_worker_container_id,
        "_runtime_surviving_healthy_worker_containers": list(surviving_healthy_worker_ids),
    }


def _offline_report(args: argparse.Namespace) -> dict[str, Any]:
    test = _pytest_offline(args.output)
    requested = _required_scenarios(args.scenario)
    scenarios = {
        name: {
            "status": test["status"],
            "label": SCENARIOS[name]["label"],
            "mode": "deterministic_contract",
            "assertions": list(SCENARIOS[name]["assertions"]),
            "stage_markers": _scenario_stage_markers(
                name,
                status="simulated",
                reason="offline contract only; no process or network fault was injected",
                evidence="deterministic contract",
            ),
        }
        for name in requested
    }
    gate = _status([str(item["status"]) for item in scenarios.values()])
    return {
        "schema_version": FAULT_REPORT_SCHEMA_VERSION,
        "production_contract": _production_contract(),
        "baseline": {
            "scenario_names": list(SCENARIOS),
            "all_required_for_production": True,
            "production_gate_must_not_be_upgraded_by_mock": True,
        },
        "candidate": {
            "mode": "deterministic_contract",
            "requested_scenario": args.scenario,
            "offline_test": test,
            "scenarios": scenarios,
        },
        "case_deltas": {
            "requested": list(requested),
            "passed": [name for name, item in scenarios.items() if item["status"] == "pass"],
        },
        "gate": gate,
        "simulation_gate": gate,
        "production_gate": "not_run",
        "rejection_reasons": [] if gate == "pass" else ["offline fault contracts failed"],
        "production_rejection_reasons": [
            "deterministic contracts do not prove Toxiproxy network faults or OS process kills",
            (
                "stage-specific enqueue/tool/commit markers and a provider ambiguous endpoint "
                "remain required"
            ),
        ],
        "runbook": _runbook(args),
    }


def _real_report(
    args: argparse.Namespace,
    *,
    expected_source_fingerprint: dict[str, Any],
    expected_release_binding: dict[str, str],
) -> dict[str, Any]:
    requested = _required_scenarios(args.scenario)
    scenarios: dict[str, dict[str, Any]] = {}
    stage_results: dict[str, dict[str, Any]] = {}
    if any(name in FAULT_STAGE_SCENARIOS for name in requested):
        stage_results = _run_fault_stage_acceptance(
            args,
            expected_source_fingerprint=expected_source_fingerprint,
        )
    runtime_recovery_blocked = False
    for name in requested:
        if name in FAULT_STAGE_SCENARIOS:
            scenarios[name] = stage_results[name]
        elif runtime_recovery_blocked:
            reason = (
                "previous runtime worker recovery was not confirmed; "
                "dependent scenario was not started"
            )
            scenarios[name] = {
                "status": "not_run",
                "label": SCENARIOS[name]["label"],
                "reason": reason,
                "assertions": list(SCENARIOS[name]["assertions"]),
                "stage_markers": _scenario_stage_markers(
                    name,
                    status="not_run",
                    reason=reason,
                ),
            }
        else:
            scenario_result = _run_real_scenario(args, name)
            if name == "fencing" and "child_report" in scenario_result:
                recovery = _runtime_worker_recovery(args, scenario_result)
                scenario_result["runtime_worker_recovery"] = recovery
                if recovery.get("status") != "pass":
                    runtime_recovery_blocked = True
                    if scenario_result.get("status") == "pass":
                        scenario_result["status"] = "fail"
                        existing_reason = str(scenario_result.get("reason", "") or "")
                        recovery_reason = str(
                            recovery.get("reason", "runtime worker recovery was not confirmed")
                        )
                        scenario_result["reason"] = "; ".join(
                            item for item in (existing_reason, recovery_reason) if item
                        )
            scenarios[name] = scenario_result
    # Internal raw container selectors are used only during the immediate
    # recovery boundary and must never become report evidence.
    for scenario in scenarios.values():
        scenario.pop("_runtime_worker_containers", None)
        scenario.pop("_runtime_killed_worker_container", None)
        scenario.pop("_runtime_surviving_healthy_worker_containers", None)
    statuses = [str(item.get("status", "not_run")) for item in scenarios.values()]
    gate = _status(statuses)
    reasons = [
        f"{name}: {item.get('reason', 'scenario did not pass')}"
        for name, item in scenarios.items()
        if item.get("status") != "pass"
    ]
    image_digest = os.getenv("TRPC_REAL_IMAGE_DIGEST", "").strip()
    full_scope = args.scenario == "all" and set(requested) == set(SCENARIOS)
    basic_ready = (
        full_scope and gate == "pass" and IMAGE_DIGEST_RE.fullmatch(image_digest) is not None
    )
    worker_identities = _runtime_worker_identities(args.workers) if basic_ready else ()
    runtime = None
    if basic_ready and worker_identities:
        runtime = runtime_fingerprint(
            mode="fault_injection",
            worker_identities=worker_identities,
            stream=os.getenv("TRPC_REAL_REDIS_URL", "fault-runtime"),
            group=args.project,
            parameters={
                "scenario_count": len(requested),
                "messages": args.messages,
                "duplicates": args.duplicates,
                "fault_messages": args.fault_messages,
                "image_digest": image_digest,
            },
        )
    evidence = build_evidence(root=ROOT, producer=PRODUCER, runtime=runtime)
    lineage_reasons = validate_current_candidate_evidence(
        evidence,
        current_source=expected_source_fingerprint,
        expected_release_binding=expected_release_binding,
        require_release_binding=True,
    )
    case_deltas = {
        "requested": list(requested),
        "passed": [name for name, item in scenarios.items() if item.get("status") == "pass"],
    }
    candidate = {
        "mode": FAULT_REPORT_MODE,
        "requested_scenario": args.scenario,
        "scenarios": scenarios,
        "lineage": {
            "status": "pass" if basic_ready else "not_run",
            "image_digest": image_digest if basic_ready else None,
        },
    }
    contract_reasons = _fault_production_contract_errors(
        args=args,
        requested=requested,
        scenarios=scenarios,
        gate=gate,
        image_digest=image_digest,
        candidate=candidate,
        case_deltas=case_deltas,
        evidence=evidence,
    )
    for reason in lineage_reasons:
        if reason not in contract_reasons:
            contract_reasons.append(reason)
    production_ready = basic_ready and not contract_reasons
    if not production_ready:
        candidate["lineage"] = {"status": "not_run", "image_digest": None}
    production_reasons = list(reasons)
    if not full_scope:
        production_reasons.append(
            "fault production evidence requires --scenario all and every required scenario"
        )
    elif gate != "pass":
        production_reasons.append("one or more required real fault scenarios did not pass")
    elif IMAGE_DIGEST_RE.fullmatch(image_digest) is None:
        production_reasons.append(
            "TRPC_REAL_IMAGE_DIGEST must be sha256:<64 hex> for production evidence"
        )
    if basic_ready and not worker_identities:
        production_reasons.append(
            f"{REAL_WORKER_IDENTITIES_ENV} must contain at least the inspected worker identities"
        )
    for reason in contract_reasons:
        if reason not in production_reasons:
            production_reasons.append(reason)
    return {
        "schema_version": FAULT_REPORT_SCHEMA_VERSION,
        "production_contract": _production_contract(),
        "baseline": {
            "scenario_names": list(SCENARIOS),
            "all_required_for_production": True,
            "production_gate_must_not_be_upgraded_by_mock": True,
        },
        "candidate": candidate,
        "case_deltas": case_deltas,
        "gate": gate,
        "production_gate": "pass" if production_ready else "not_run",
        "run_id": evidence["run_id"],
        "evidence": evidence,
        "rejection_reasons": reasons,
        "production_rejection_reasons": production_reasons
        if production_ready
        else [
            *production_reasons,
            (
                "this scoped report does not include migration, Kubernetes, performance, or "
                "real IM gates"
            ),
        ],
        "runbook": _runbook(args),
    }


def _write(output: Path, report: dict[str, Any]) -> None:
    output = _assert_safe_report_path(output)
    rendered = atomic_write_json(output, report)
    print(rendered, end="")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.offline:
        report = _offline_report(args)
    elif not args.execute:
        report = _not_run_report(
            ["real fault acceptance requires --execute and TRPC_RUN_REAL_MULTINODE=1"], args
        )
    else:
        requested = _required_scenarios(args.scenario)
        # The dedicated enqueue/tool/commit child uses only TRPC_FAULT_* data.
        # Ordinary runtime credentials are required only when this invocation
        # also requests a scenario backed by the normal real-runtime runner.
        missing = _missing_real_environment(
            require_runtime_credentials=any(name in SUPPORTED_REAL for name in requested)
        )
        if missing:
            report = _not_run_report(
                [f"missing real prerequisite: {item}" for item in missing], args
            )
        elif (
            args.scenario in {"all", "redis_interrupt", "republish", "dlq"}
            and not args.toxiproxy_override.is_file()
        ):
            report = _not_run_report(
                [f"missing Toxiproxy override: {args.toxiproxy_override}"], args
            )
        else:
            try:
                expected_release_binding = current_release_binding(required=True)
            except ValueError as error:
                report = _not_run_report([str(error)], args)
            else:
                if expected_release_binding is None:
                    report = _not_run_report(["current release binding is unavailable"], args)
                else:
                    report = _real_report(
                        args,
                        expected_source_fingerprint=source_fingerprint(ROOT),
                        expected_release_binding=expected_release_binding,
                    )
    _write(args.output, report)
    if report.get("gate") == "fail":
        return 1
    if args.require_production and report.get("production_gate") != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
