#!/usr/bin/env python3
"""Aggregate machine-readable evidence without upgrading missing gates to pass."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from scripts.evidence_lineage import (
    DEFAULT_EVIDENCE_TTL_SECONDS,
    canonical_sha256,
    source_fingerprint,
    validate_current_candidate_evidence,
)
from scripts.evidence_lineage import (
    FINGERPRINT_MAX_BYTES as DEFAULT_FINGERPRINT_MAX_BYTES,
)
from scripts.evidence_lineage import (
    FINGERPRINT_MAX_FILES as DEFAULT_FINGERPRINT_MAX_FILES,
)
from scripts.evidence_lineage import (
    SOURCE_FINGERPRINT_ROOTS as DEFAULT_SOURCE_FINGERPRINT_ROOTS,
)
from scripts.real_runtime_gate import _role_evidence_check
from scripts.release_manifest import validate_manifest
from scripts.report_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FINGERPRINT_ROOTS = DEFAULT_SOURCE_FINGERPRINT_ROOTS
FINGERPRINT_MAX_FILES = DEFAULT_FINGERPRINT_MAX_FILES
FINGERPRINT_MAX_BYTES = DEFAULT_FINGERPRINT_MAX_BYTES

REPORTS = {
    "sdk": ("sdk-upgrade.json", False),
    "coverage": ("coverage-gate.json", False),
    "backend": ("backend-compose.json", True),
    "compose": ("compose-e2e.json", False),
    "supply_chain": ("supply-chain.json", False),
    "simulation": ("production-mock.json", False),
    # The formal report is the sole source for the production performance
    # gate. Historical ramp reports must never override it.
    "performance": ("real-performance.json", True),
    "real_runtime": ("real-runtime.json", True),
    # Keep deterministic fault contracts separate from the production fault
    # injection gate; the latter also requires stage-specific runtime evidence.
    "fault_contract": ("fault-offline.json", False),
    # The production fault gate has a separate output from the deterministic
    # offline contract below.  Keeping these paths distinct prevents an
    # offline scenario report from being treated as process-fault evidence.
    "fault_injection": ("fault-injection.json", True),
    "migration_contract": ("migration-offline.json", False),
    "migration_acceptance": ("migration-acceptance.json", False),
    "migration_full_acceptance": ("migration-full-acceptance.json", False),
    "migration": ("migration-live.json", True),
    # Static deployment checks do not substitute for the live Kubernetes
    # runtime report, whose production_gate is the release evidence.
    "deployment": ("kubernetes-runtime.json", True),
    "im_resilience_contract": ("im-resilience-offline.json", False),
    "privacy_leak": ("privacy-leak-offline.json", False),
    "online_im": ("im-online.json", True),
    "disaster_recovery": ("disaster-recovery.json", True),
}

FUNCTIONAL_DR_REPORT = (
    "disaster-recovery-functional.json",
    "scripts.functional_disaster_recovery_gate",
)

# A current-candidate envelope is useful only when it was emitted by the
# gate which owns the report.  This allowlist is intentionally explicit: a
# valid performance envelope copied into a Kubernetes, IM, or fault report
# must remain ``not_run``.  Entries whose emitter is not implemented yet are
# reserved names, not permission to fabricate a report.
PRODUCTION_EVIDENCE_PRODUCERS = {
    "backend-compose.json": "scripts.contract_gate",
    "real-performance.json": "scripts.real_performance_gate",
    "real-runtime.json": "scripts.real_runtime_gate",
    "fault-injection.json": "scripts.fault_injection_gate",
    "migration-live.json": "scripts.migrate_data",
    "kubernetes-runtime.json": "scripts.kubernetes_runtime_gate",
    "im-online.json": "scripts.im_online_gate",
    "disaster-recovery.json": "scripts.disaster_recovery_gate",
}

BACKEND_REQUIRED_SELECTORS = ("tests/integration",)
PRODUCTION_IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REAL_RUNTIME_COMPOSE_START_MODES = {"gate-owned", "wrapper-prestarted-owned"}
REAL_RUNTIME_WORKER_SERVICES = {
    "worker",
    "outbox-dispatcher",
    "channel-dispatcher",
    "post-turn-projector",
    "session-recovery",
}


def _real_runtime_worker_route_valid(connection: Mapping[str, Any]) -> bool:
    worker_database = connection.get("worker_database")
    return (
        isinstance(worker_database, Mapping)
        and worker_database.get("role") == "trpc_worker"
        and worker_database.get("host") == "toxiproxy"
        and worker_database.get("port") == 15432
    )


def _runtime_fingerprint_matches(
    value: object,
    *,
    mode: str,
    worker_identities: Sequence[Any],
    participating_identities: Sequence[Any] | None = None,
    stream: str,
    group: str,
    parameters: Mapping[str, Any],
) -> bool:
    """Recompute every runtime fingerprint field from accepted report inputs."""

    if not isinstance(value, Mapping):
        return False
    worker_hash = canonical_sha256(list(worker_identities))
    stream_group_hash = canonical_sha256({"group": group, "stream": stream})
    parameters_hash = canonical_sha256(dict(parameters))
    material = {
        "mode": mode,
        "worker_identity_summary_sha256": worker_hash,
        "stream_group_sha256": stream_group_hash,
        "parameters_sha256": parameters_hash,
    }
    participating_hash: str | None = None
    if participating_identities is not None:
        participating = list(participating_identities)
        participating_hash = canonical_sha256(participating)
        material["participating_identity_summary_sha256"] = participating_hash
    return (
        value.get("algorithm") == "sha256"
        and value.get("status") == "available"
        and value.get("value") == canonical_sha256(material)
        and value.get("mode") == mode
        and value.get("worker_count") == len(worker_identities)
        and value.get("worker_identity_summary_sha256") == worker_hash
        and value.get("stream_group_sha256") == stream_group_hash
        and value.get("parameters_sha256") == parameters_hash
        and (
            participating_identities is None
            or (
                value.get("participating_identity_summary_sha256") == participating_hash
                and value.get("participating_service_count")
                == len(
                    {
                        item.get("role")
                        for item in participating_identities
                        if isinstance(item, Mapping) and isinstance(item.get("role"), str)
                    }
                )
                and value.get("participating_container_count") == len(participating_identities)
            )
        )
    )


def _safe_http_api_endpoint(value: object) -> bool:
    """Accept a credential-free HTTP(S) API origin and reject ambiguous URLs."""

    if not isinstance(value, str):
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return bool(
        parsed.scheme in {"http", "https"}
        and parsed.hostname in {"toxiproxy", "localhost", "127.0.0.1", "::1"}
        and port is not None
        and parsed.username is None
        and parsed.password is None
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment
    )


FAULT_REQUIRED_MARKERS: dict[str, tuple[str, ...]] = {
    "redis_interrupt": (
        "proxy.disable_requested",
        "proxy.disabled",
        "work_pending_while_disabled",
        "proxy.restore_requested",
        "proxy.restored",
        "post_restore.commit_verified",
        "duplicate_turn_verified",
    ),
    "worker_enqueue": (
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
    "worker_tool": (
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
    "worker_commit": (
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
FAULT_REQUIRED_SCENARIOS = tuple(FAULT_REQUIRED_MARKERS)

# ``real-performance.json`` is a production acceptance artifact rather than a
# free-form status file.  Keep these values local to the release aggregator so
# an old or hand-written report cannot silently redefine the production bar.
PERFORMANCE_REQUIRED_CALLBACKS = 200
PERFORMANCE_REQUIRED_CALLBACK_RATE = 100.0
PERFORMANCE_REQUIRED_BURST_TURNS = 200
PERFORMANCE_REQUIRED_WORKERS = 4
PERFORMANCE_REQUIRED_WORKER_CONCURRENCY = 50
# The formal run must use the same bounded producer topology as the gate
# defaults.  Keeping these values in the release validator prevents an older
# low-capacity report from being promoted by changing only production_gate.
PERFORMANCE_REQUIRED_DB_POOL_SIZE = 32
PERFORMANCE_REQUIRED_INFLIGHT = 64
PERFORMANCE_MAX_ACK_P95_MS = 200.0
PERFORMANCE_REQUIRED_WARMUP_STEPS = (1, 4, 8)
# Keep the producer's hard safety envelope duplicated here.  A release report
# is untrusted input, so changing only the producer cannot widen the amount of
# work that the release gate is willing to promote.
PERFORMANCE_MAX_CALLBACKS = 2_000
PERFORMANCE_MAX_CALLBACK_RATE = 200.0
PERFORMANCE_MAX_BURST_TURNS = 500
PERFORMANCE_MAX_WORKERS = 64
PERFORMANCE_MAX_DB_POOL_SIZE = 64
PERFORMANCE_MAX_INFLIGHT = 64
PERFORMANCE_MAX_TIMEOUT_SECONDS = 600.0
PERFORMANCE_MAX_CONNECTIONS = 128
PERFORMANCE_GATEWAY_POOL_MAX_SIZE = 24
PERFORMANCE_WORKER_POOL_MAX_SIZE = 8
PERFORMANCE_OUTBOX_POOL_MAX_SIZE = 4
PERFORMANCE_RECOVERY_POOL_MAX_SIZE = 2
PERFORMANCE_PROBE_CONNECTION_HEADROOM = 8
PERFORMANCE_MAX_KUBERNETES_MEMORY_BYTES = 2**60 - 1
PERFORMANCE_MAX_KUBERNETES_MEMORY_IDENTITIES = 128
PERFORMANCE_REQUIRED_MEMORY_ROLES = (
    "worker",
    "outbox-dispatcher",
)

IM_REQUIRED_CHANNELS = ("feishu", "wecom")
IM_REQUIRED_CASES = (
    "round_trip",
    "idempotency",
    "media",
    "reconnect",
    "rate_limit_retry_after",
    "credential_rotation",
    "prolonged_outage",
    "ambiguous",
)
IM_EVIDENCE_CONTRACT = {
    "feishu": ("feishu_api_and_webhook", ("provider_callback", "provider_send_ack"), 4),
    "wecom": ("wecom_ws_and_send_ack", ("provider_ws_event", "provider_send_ack"), 2),
}
IM_RATE_LIMIT_CODES = {
    "feishu": frozenset({"429", "99991400", "99991401", "99991402", "99991672"}),
    "wecom": frozenset({"429", "45009", "45011"}),
}
IM_MIN_PROLONGED_OUTAGE_SECONDS = 60.0
IM_MAX_RETRY_ATTEMPTS = 100
IM_PROBE_TRUST_PATH = ROOT / "deploy" / "im-probe-trust.json"
IM_PROBE_TRUST_MAX_BYTES = 8 * 1024
IM_PROBE_TRUST_KEY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
IM_PROBE_URL_MAX_LENGTH = 2048
IM_RESPONSE_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
IM_RUNTIME_ATTESTATION_FIELDS = frozenset(
    {
        "status",
        "run_nonce",
        "image_digest",
        "release_id",
        "release_nonce_sha256",
        "source_fingerprint",
    }
)
IM_ARTIFACT_ATTESTATION_FIELDS = frozenset(
    {
        "schema_version",
        "runner_sha256",
        "runner_contract_version",
        "driver_sha256",
        "driver_contract_version",
    }
)
IM_CASE_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "round_trip": ("callback_event_id_hash", "outbound_request_id_hash", "provider_code"),
    "idempotency": ("duplicate_event_id_hash", "unique_inbound_id_hash", "duplicate_count"),
    "media": ("bytes",),
    "reconnect": (),
    "rate_limit_retry_after": (
        "provider_error_code",
        "retry_after_seconds",
        "retry_request_id_hash",
        "retry_attempts",
        "retry_elapsed_seconds",
    ),
    "credential_rotation": (
        "old_credential_event_id_hash",
        "new_credential_event_id_hash",
        "post_rotation_event_id_hash",
        "old_credential_rejected",
    ),
    "prolonged_outage": ("outage_event_id_hash", "recovery_event_id_hash", "outage_seconds"),
    "ambiguous": (
        "ambiguous_event_id_hash",
        "manual_review_id_hash",
        "drop_response_observed",
        "auto_replay_count",
    ),
}
IM_FEISHU_RECONNECT_FIELDS = (
    "failed_endpoint_id_hash",
    "replacement_endpoint_id_hash",
    "endpoint_set_observed",
    "received_after_failover_event_id_hash",
    "outbound_request_id_hash",
    "acknowledged_request_id_hash",
    "ready_endpoint_count",
    "unready_endpoint_count",
    "terminating_endpoint_count",
)
IM_WECOM_RECONNECT_FIELDS = (
    "disconnect_event_id_hash",
    "reconnect_event_id_hash",
    "received_after_reconnect_event_id_hash",
    "lock_takeover_event_id_hash",
    "old_lock_owner_released",
    "new_lock_owner_acquired",
    "lock_epoch",
    "outbound_request_id_hash",
    "acknowledged_request_id_hash",
    "provider_code",
)
IM_WECOM_ROTATION_ACK_FIELDS = (
    "outbound_request_id_hash",
    "acknowledged_request_id_hash",
    "provider_code",
)
IM_WECOM_SERVICE_FAILOVER_FIELDS = (
    "outage_mode",
    "failed_instance_id_hash",
    "takeover_instance_id_hash",
    "old_lock_owner_released",
    "new_lock_owner_acquired",
    "connection_epoch",
    "event_during_outage_id_hash",
    "reply_for_event_id_hash",
    "outbound_request_id_hash",
    "acknowledged_request_id_hash",
    "reply_count",
    "ack_count",
    "pending_count",
    "dlq_count",
)

REAL_RUNTIME_REQUIRED_PHASE = "all"
REAL_RUNTIME_REQUIRED_WORKERS = 4
REAL_RUNTIME_REQUIRED_MESSAGES = 200
REAL_RUNTIME_REQUIRED_DUPLICATES = 20
REAL_RUNTIME_REQUIRED_FAULT_MESSAGES = 8
REAL_RUNTIME_MAX_WORKERS = 16
REAL_RUNTIME_MAX_MESSAGES = 2_000
REAL_RUNTIME_MAX_DUPLICATES = 2_000
REAL_RUNTIME_MAX_FAULT_MESSAGES = 200
REAL_RUNTIME_MAX_TIMEOUT_SECONDS = 900.0
REAL_RUNTIME_REQUIRED_LOAD_MARKERS = (
    "acceptance.persisted",
    "turn.processing_observed",
    "worker.kill_requested",
    "worker.kill_completed",
    "worker.survivors_observed",
    "lease.takeover_observed",
    "stale_token_rejection_verified",
    "turn.commit_verified",
)
REAL_RUNTIME_REQUIRED_FAULT_MARKERS = (
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
REAL_RUNTIME_TOXIPROXY_ENDPOINTS: dict[str, tuple[str, str]] = {
    "postgres": ("0.0.0.0:15432", "postgres:5432"),
    "redis": ("0.0.0.0:16379", "redis:6379"),
}
_TOXIPROXY_WILDCARD_HOSTS = frozenset(
    {REAL_RUNTIME_TOXIPROXY_ENDPOINTS["postgres"][0].partition(":")[0], "[::]"}
)


def _toxiproxy_listen_matches(expected: object, observed: object) -> bool:
    """Match Toxiproxy listen endpoints, allowing only equivalent wildcards."""

    if not isinstance(expected, str) or not isinstance(observed, str):
        return False
    if expected == observed:
        return True
    expected_host, expected_separator, expected_port = expected.rpartition(":")
    observed_host, observed_separator, observed_port = observed.rpartition(":")
    return bool(
        expected_separator
        and observed_separator
        and expected_host in _TOXIPROXY_WILDCARD_HOSTS
        and observed_host in _TOXIPROXY_WILDCARD_HOSTS
        and expected_host != observed_host
        and expected_port.isdecimal()
        and expected_port == observed_port
    )


# A migration report may only promote the live gate after the complete,
# operator-controlled Redis -> PostgreSQL cutover has been observed.  The
# final value is a rollback capability (not a second static execution branch).
# These values are deliberately kept in the release gate rather than trusting
# a producer supplied list, so a partial phase report cannot be relabelled as a
# production migration by changing ``production_gate``.
MIGRATION_REQUIRED_PHASES = (
    "prepare",
    "backfill",
    "shadow-read",
    "dual-write",
    "cutover",
    "verify",
    "cleanup",
    "rollback",
)
MIGRATION_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRODUCTION_MIGRATION_PRODUCER = "scripts.migrate_data"
MIGRATION_PRODUCTION_TENANT_MARKERS = frozenset(
    {
        "test",
        "testing",
        "simulation",
        "simulated",
        "fixture",
        "mock",
        "example",
        "dummy",
        "acceptance",
    }
)

# Kubernetes runtime reports are intentionally validated here instead of
# trusting ``production_gate`` from the producer.  Keep this contract local
# to the release decision so a static/partial report cannot promote itself.
K8S_REQUIRED_CHECKS = (
    "kube_context",
    "kustomize_render",
    "production_manifest_contract",
    "image_pull_secret_contract",
    "namespace_create",
    "server_side_dry_run",
    "manifest_contract",
    "secret_server_side_dry_run",
    "secret_apply",
    "apply",
    "schema_migration",
    "schema_migration_head",
    "readiness",
    "scheduler_cutover_guard",
    "rolling_upgrade",
    "worker_scale_and_hpa",
    "hpa_driver_rbac_bind",
    "hpa_driver_trust",
    "hpa_load_observation",
    "pdb_eviction",
    "node_eviction",
    "graceful_termination",
    "namespace_cleanup",
)
K8S_REQUIRED_ACTIONS = (
    "server_side_dry_run",
    "schema_migration",
    "schema_migration_head",
    "readiness",
    "scheduler_cutover_guard",
    "rolling_upgrade",
    "hpa_observed",
    "hpa_load_observed",
    "pod_eviction",
    "node_eviction",
    "graceful_termination",
    "namespace_cleanup",
)
K8S_REQUIRED_DEPLOYMENTS = (
    "trpc-gateway",
    "trpc-session-recovery",
    "trpc-artifact-gc",
    "trpc-backlog-exporter",
    "trpc-admin",
    "trpc-worker",
    "trpc-outbox-dispatcher",
    "trpc-channel-dispatcher",
    "trpc-post-turn-projector",
)
K8S_PRODUCTION_DEPLOYMENTS = (*K8S_REQUIRED_DEPLOYMENTS, "trpc-wecom-connector")
K8S_PROVIDER_DISABLED_DEPLOYMENTS = ("trpc-wecom-connector",)
K8S_RUNTIME_SCOPE = "unified_cluster_runtime"
K8S_IM_DEPLOYMENT = "production_cluster"
K8S_IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
K8S_RUN_NONCE_RE = re.compile(r"^[0-9a-f]{32}$")
K8S_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
K8S_HPA_DRIVER_RELATIVE_PATH = "scripts/kubernetes_hpa_load_driver.py"
K8S_HPA_DRIVER_MAX_BYTES = 1024 * 1024
K8S_HPA_JOB_UID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
MIGRATION_ALLOWED_FACTORIES = frozenset(
    {
        "production_migration_control.create",
        "trpc_service.storage.production_migration_control:create",
        "trpc_service.storage.migration_control:create",
    }
)
MIGRATION_TARGET_EMPTY_TABLES = (
    "inbound_messages",
    "outbound_messages",
    "delivery_attempts",
    "sessions",
    "session_turns",
    "turn_intents",
    "session_events",
    "memories",
    "session_summaries",
    "artifacts",
    "knowledge_items",
    "knowledge_embeddings",
    "outbox_events",
    "dead_letters",
    "tool_executions",
    "confirmation_challenges",
    "audit_logs",
    "session_mailboxes",
    "session_mailbox_items",
    "wecom_connection_state",
    "im_acceptance_evidence_events",
    "im_acceptance_runs",
    "migration_checkpoints",
    "migration_scope_manifests",
    "migration_leases",
)


def _current_candidate_source_fingerprint() -> dict[str, Any]:
    """Return the same content-addressed candidate fingerprint as perf reports.

    ``real-performance.json`` is intentionally allowed to be produced by an
    older process, so the release gate recomputes the digest from the current
    checkout before accepting its production result.  Keep the input roots and
    framing byte-for-byte aligned with ``real_performance_gate``.  Only the
    digest and safe metadata are returned; source contents never enter a
    release report.
    """

    return source_fingerprint(
        ROOT,
        SOURCE_FINGERPRINT_ROOTS,
        max_files=FINGERPRINT_MAX_FILES,
        max_bytes=FINGERPRINT_MAX_BYTES,
    )


def _strict_int(value: Any) -> int | None:
    """Parse a JSON integer without accepting bools or numeric strings."""

    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return int(value)


def _schema_version_is(value: Any, expected: int) -> bool:
    """Compare schema versions without Python's bool-is-int coercion."""

    return type(value) is int and value == expected


def _strict_number(value: Any) -> float | None:
    """Parse a finite JSON number without accepting bools or numeric strings."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _is_json_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _missing_semantic(path: str) -> tuple[str, str]:
    return "not_run", f"real performance evidence is missing or invalid {path}"


def _failed_semantic(reason: str) -> tuple[str, str]:
    return "fail", f"real performance acceptance failed: {reason}"


def _mapping_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[Mapping[str, Any] | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        return None, _missing_semantic(path)
    return value, None


def _list_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[list[Any] | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not _is_json_sequence(value):
        return None, _missing_semantic(path)
    return list(cast(Sequence[Any], value)), None


def _int_field(
    parent: Mapping[str, Any],
    key: str,
    path: str,
    *,
    minimum: int | None = None,
    exact: int | None = None,
) -> tuple[int | None, tuple[str, str] | None]:
    if key not in parent:
        return None, _missing_semantic(path)
    value = _strict_int(parent.get(key))
    if value is None:
        return None, _missing_semantic(path)
    if exact is not None and value != exact:
        return value, _failed_semantic(f"{path} must equal {exact}")
    if minimum is not None and value < minimum:
        return value, _failed_semantic(f"{path} must be at least {minimum}")
    return value, None


def _number_field(
    parent: Mapping[str, Any],
    key: str,
    path: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> tuple[float | None, tuple[str, str] | None]:
    if key not in parent:
        return None, _missing_semantic(path)
    value = _strict_number(parent.get(key))
    if value is None:
        return None, _missing_semantic(path)
    if minimum is not None and value < minimum:
        return value, _failed_semantic(f"{path} must be at least {minimum:g}")
    if maximum is not None and value >= maximum:
        return value, _failed_semantic(f"{path} must be below {maximum:g}")
    return value, None


def _status_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[str | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not isinstance(value, str):
        return None, _missing_semantic(path)
    if value == "fail":
        return value, _failed_semantic(f"{path} reported fail")
    if value != "pass":
        return value, _missing_semantic(path)
    return value, None


def _unique_id_field(
    parent: Mapping[str, Any],
    key: str,
    path: str,
    *,
    expected_count: int,
) -> tuple[list[str] | None, tuple[str, str] | None]:
    values, problem = _list_field(parent, key, path)
    if problem is not None or values is None:
        return None, problem
    if any(not isinstance(item, str) or not item for item in values):
        return None, _missing_semantic(path)
    identifiers = [str(item) for item in values]
    if len(identifiers) != expected_count:
        return identifiers, _failed_semantic(f"{path} count does not match accepted count")
    if len(identifiers) != len(set(identifiers)):
        return identifiers, _failed_semantic(f"{path} must be unique")
    return identifiers, None


def _validate_completion(
    phase: Mapping[str, Any], *, path: str, expected_count: int
) -> tuple[str, str] | None:
    completion, problem = _mapping_field(phase, "completion", f"{path}.completion")
    if problem is not None or completion is None:
        return problem
    status, problem = _status_field(completion, "status", f"{path}.completion.status")
    if problem is not None:
        return problem
    if status != "pass":
        return _missing_semantic(f"{path}.completion.status")
    state, problem = _mapping_field(completion, "state", f"{path}.completion.state")
    if problem is not None or state is None:
        return problem
    for status_key in ("inbound_statuses", "turn_statuses"):
        statuses, problem = _mapping_field(
            state, status_key, f"{path}.completion.state.{status_key}"
        )
        if problem is not None or statuses is None:
            return problem
        _, problem = _int_field(
            statuses,
            "committed",
            f"{path}.completion.state.{status_key}.committed",
            exact=expected_count,
        )
        if problem is not None:
            return problem
    if state.get("scheduler_version") != "v2":
        return _missing_semantic(f"{path}.completion.state.scheduler_version=v2")
    for key, exact in (
        ("published_scheduler_outbox", expected_count),
        ("leased_sessions", 0),
        ("mailbox_expected_count", expected_count),
        ("mailbox_row_count", expected_count),
        ("mailbox_idle_count", expected_count),
        ("mailbox_settled_count", expected_count),
        ("mailbox_unresolved_item_count", 0),
    ):
        _, problem = _int_field(
            state,
            key,
            f"{path}.completion.state.{key}",
            exact=exact,
        )
        if problem is not None:
            return problem
    return None


def _validate_http_status_counts(
    phase: Mapping[str, Any], *, path: str, expected_count: int
) -> tuple[str, str] | None:
    status_counts, problem = _mapping_field(
        phase, "http_status_counts", f"{path}.http_status_counts"
    )
    if problem is not None or status_counts is None:
        return problem
    total = 0
    for raw_status, raw_count in status_counts.items():
        if isinstance(raw_status, bool):
            return _missing_semantic(f"{path}.http_status_counts")
        if isinstance(raw_status, int):
            code = raw_status
        elif isinstance(raw_status, str) and raw_status.isdigit():
            code = int(raw_status)
        else:
            return _missing_semantic(f"{path}.http_status_counts")
        count = _strict_int(raw_count)
        if count is None or count < 0:
            return _missing_semantic(f"{path}.http_status_counts")
        if not 200 <= code < 300:
            return _failed_semantic(f"{path} contains a non-2xx HTTP response")
        total += count
    if total != expected_count:
        return _failed_semantic(f"{path} does not account for every accepted callback")
    failure_counts, problem = _mapping_field(
        phase, "http_failure_counts", f"{path.rsplit('.', 1)[0]}.http_failure_counts"
    )
    if problem is not None or failure_counts is None:
        return problem
    for raw_count in failure_counts.values():
        count = _strict_int(raw_count)
        if count is None or count < 0:
            return _missing_semantic(f"{path.rsplit('.', 1)[0]}.http_failure_counts")
        if count:
            return _failed_semantic("HTTP callback failures were recorded")
    return None


def _validate_kubernetes_performance_memory_observation(
    observation: Mapping[str, Any],
    preflight: Mapping[str, Any],
    resources: Mapping[str, Any],
    *,
    worker_processes: Sequence[Any],
) -> tuple[str | None, str | None] | None:
    """Validate metrics-server memory evidence against Pod identities."""

    if observation.get("metrics_api") != "metrics.k8s.io/v1beta1":
        return _missing_semantic("candidate.memory_observation.metrics_api")
    kubernetes = preflight.get("kubernetes")
    if kubernetes is not None:
        if (
            not isinstance(kubernetes, Mapping)
            or kubernetes.get("metrics_api") != "metrics.k8s.io/v1beta1"
            or kubernetes.get("namespace_bound") is not True
        ):
            return _missing_semantic("candidate.preflight.kubernetes metrics binding")
    sample_count, problem = _int_field(
        observation,
        "sample_count",
        "candidate.memory_observation.sample_count",
        exact=1,
    )
    if problem is not None or sample_count is None:
        return problem or _missing_semantic("candidate.memory_observation.sample_count")
    sample_interval = _strict_number(observation.get("sampling_interval_seconds"))
    if sample_interval is None or sample_interval <= 0 or sample_interval > 60:
        return _missing_semantic("candidate.memory_observation.sampling_interval_seconds")

    required_roles = observation.get("required_roles")
    if not _is_json_sequence(required_roles):
        return _missing_semantic("candidate.memory_observation.required_roles")
    if tuple(cast(Sequence[Any], required_roles)) != PERFORMANCE_REQUIRED_MEMORY_ROLES:
        return _missing_semantic("candidate.memory_observation.required_roles")
    if observation.get("coverage_complete") is not True:
        return _missing_semantic("candidate.memory_observation.coverage_complete=true")

    sample_timestamps, problem = _list_field(
        observation,
        "sample_timestamps",
        "candidate.memory_observation.sample_timestamps",
    )
    if problem is not None or sample_timestamps is None:
        return problem or _missing_semantic("candidate.memory_observation.sample_timestamps")
    if not sample_timestamps or any(
        not isinstance(timestamp, str) or not timestamp.strip() for timestamp in sample_timestamps
    ):
        return _missing_semantic("candidate.memory_observation.sample_timestamps")
    if len({str(timestamp) for timestamp in sample_timestamps}) != len(sample_timestamps):
        return _missing_semantic("candidate.memory_observation.sample_timestamps")
    for timestamp in sample_timestamps:
        try:
            parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return _missing_semantic("candidate.memory_observation.sample_timestamps")
        if parsed_timestamp.tzinfo is None:
            return _missing_semantic("candidate.memory_observation.sample_timestamps")

    participating, problem = _mapping_field(
        preflight,
        "participating_processes",
        "candidate.preflight.participating_processes",
    )
    if problem is not None or participating is None:
        return problem or _missing_semantic("candidate.preflight.participating_processes")
    if set(participating) != set(PERFORMANCE_REQUIRED_MEMORY_ROLES):
        return _missing_semantic("candidate.preflight.participating_processes exact required roles")

    # Pod UID, container name, and container ID together identify the sampled
    # runtime object without relying on a host PID that is not visible in
    # Kubernetes mode.
    expected_identity_keys: dict[str, set[tuple[str, str, str]]] = {}
    expected_pod_names: dict[tuple[str, tuple[str, str, str]], str] = {}
    expected_limits: dict[tuple[str, str, str], int] = {}
    all_identity_keys: set[tuple[str, str, str]] = set()
    all_pod_uids: set[str] = set()
    all_container_ids: set[str] = set()
    for role in PERFORMANCE_REQUIRED_MEMORY_ROLES:
        identities = participating.get(role)
        if not _is_json_sequence(identities) or not identities:
            return _missing_semantic(f"candidate.preflight.participating_processes.{role}")
        role_keys: set[tuple[str, str, str]] = set()
        for identity in cast(Sequence[Any], identities):
            if not isinstance(identity, Mapping) or identity.get("role") != role:
                return _missing_semantic(
                    f"candidate.preflight.participating_processes.{role} identity"
                )
            pod_name = identity.get("pod_name")
            pod_uid = identity.get("pod_uid")
            container_name = identity.get("container_name")
            container_id = identity.get("container_id")
            if (
                not isinstance(pod_name, str)
                or not pod_name.strip()
                or not isinstance(pod_uid, str)
                or not pod_uid.strip()
                or not isinstance(container_name, str)
                or not container_name.strip()
                or container_name != role
                or not isinstance(container_id, str)
                or not container_id.strip()
            ):
                return _missing_semantic(
                    f"candidate.preflight.participating_processes.{role} Pod identity"
                )
            limit = _strict_int(identity.get("memory_limit_bytes"))
            if limit is None or limit <= 0 or limit > PERFORMANCE_MAX_KUBERNETES_MEMORY_BYTES:
                return _missing_semantic(
                    f"candidate.preflight.participating_processes.{role}.memory_limit_bytes"
                )
            key = (pod_uid, container_name, container_id)
            if (
                key in role_keys
                or key in all_identity_keys
                or pod_uid in all_pod_uids
                or container_id in all_container_ids
            ):
                return _failed_semantic("Kubernetes memory Pod identities are duplicated")
            role_keys.add(key)
            all_identity_keys.add(key)
            all_pod_uids.add(pod_uid)
            all_container_ids.add(container_id)
            expected_pod_names[(role, key)] = pod_name
            expected_limits[key] = limit
        expected_identity_keys[role] = role_keys
    if len(all_identity_keys) > PERFORMANCE_MAX_KUBERNETES_MEMORY_IDENTITIES:
        return _failed_semantic("Kubernetes memory Pod identity count exceeds safety cap")

    worker_details: dict[tuple[str, str, str], tuple[str, int]] = {}
    for worker in worker_processes:
        if not isinstance(worker, Mapping):
            return _missing_semantic("candidate.preflight.worker_processes")
        pod_name = worker.get("pod_name")
        pod_uid = worker.get("pod_uid")
        container_name = worker.get("container_name")
        container_id = worker.get("container_id")
        limit = _strict_int(worker.get("memory_limit_bytes"))
        if (
            not isinstance(pod_name, str)
            or not pod_name.strip()
            or not isinstance(pod_uid, str)
            or not pod_uid.strip()
            or not isinstance(container_name, str)
            or not container_name.strip()
            or container_name != "worker"
            or not isinstance(container_id, str)
            or not container_id.strip()
            or limit is None
            or limit <= 0
            or limit > PERFORMANCE_MAX_KUBERNETES_MEMORY_BYTES
        ):
            return _missing_semantic("candidate.preflight.worker_processes Pod identity")
        key = (pod_uid, container_name, container_id)
        if key in worker_details:
            return _failed_semantic("independent worker Pod identities are duplicated")
        worker_details[key] = (pod_name, limit)
    expected_worker_details = {
        key: (expected_pod_names[("worker", key)], expected_limits[key])
        for key in expected_identity_keys["worker"]
    }
    if worker_details != expected_worker_details:
        return _missing_semantic("worker Pod identities matching participating_processes")

    role_observations, problem = _mapping_field(
        observation,
        "role_observations",
        "candidate.memory_observation.role_observations",
    )
    if problem is not None or role_observations is None:
        return problem or _missing_semantic("candidate.memory_observation.role_observations")
    if set(role_observations) != set(PERFORMANCE_REQUIRED_MEMORY_ROLES):
        return _missing_semantic(
            "candidate.memory_observation.role_observations exact required roles"
        )

    observed_identity_keys: set[tuple[str, str, str]] = set()
    total_memory_bytes = 0
    total_identity_count = 0
    for role in PERFORMANCE_REQUIRED_MEMORY_ROLES:
        role_value, problem = _mapping_field(
            role_observations,
            role,
            f"candidate.memory_observation.role_observations.{role}",
        )
        if problem is not None or role_value is None:
            return problem or _missing_semantic(f"memory role {role}")
        identity_count, problem = _int_field(
            role_value,
            "identity_count",
            f"candidate.memory_observation.{role}.identity_count",
            exact=len(expected_identity_keys[role]),
        )
        if problem is not None or identity_count is None:
            return problem or _missing_semantic(f"memory role {role} identity count")
        observed_count, problem = _int_field(
            role_value,
            "observed_count",
            f"candidate.memory_observation.{role}.observed_count",
            exact=identity_count,
        )
        if problem is not None or observed_count is None:
            return problem or _missing_semantic(f"memory role {role} observed count")
        observations, problem = _list_field(
            role_value,
            "observations",
            f"candidate.memory_observation.{role}.observations",
        )
        if problem is not None or observations is None:
            return problem or _missing_semantic(f"memory role {role} observations")
        if len(observations) != identity_count:
            return _failed_semantic(f"memory role {role} observation count mismatch")
        role_observed_keys: set[tuple[str, str, str]] = set()
        role_memory_bytes = 0
        for item in observations:
            if not isinstance(item, Mapping) or item.get("role") != role:
                return _missing_semantic(f"memory role {role} identity binding")
            pod_name = item.get("pod_name")
            pod_uid = item.get("pod_uid")
            container_name = item.get("container_name")
            container_id = item.get("container_id")
            if (
                not isinstance(pod_uid, str)
                or not pod_uid.strip()
                or not isinstance(container_name, str)
                or not container_name.strip()
                or container_name != role
                or not isinstance(container_id, str)
                or not container_id.strip()
            ):
                return _missing_semantic(f"memory role {role} Pod identity")
            key = (pod_uid, container_name, container_id)
            if key in role_observed_keys or key in observed_identity_keys:
                return _failed_semantic(f"memory role {role} identities are duplicated")
            if key not in expected_identity_keys[role]:
                return _missing_semantic(f"memory role {role} identity binding")
            if item.get("pod_name") != expected_pod_names[(role, key)]:
                return _missing_semantic(f"memory role {role} Pod identity binding")
            role_observed_keys.add(key)
            observed_identity_keys.add(key)
            memory_bytes = _strict_int(item.get("memory_bytes"))
            if (
                memory_bytes is None
                or memory_bytes <= 0
                or memory_bytes > PERFORMANCE_MAX_KUBERNETES_MEMORY_BYTES
            ):
                return _missing_semantic(f"memory role {role} memory_bytes")
            if item.get("sampled_memory_bytes") != memory_bytes:
                return _missing_semantic(f"memory role {role} sampled_memory_bytes")
            if item.get("sampling_source") != "kubernetes_metrics_api":
                return _missing_semantic(f"memory role {role} sampling_source")
            if item.get("metrics_timestamp") not in sample_timestamps:
                return _missing_semantic(f"memory role {role} metrics_timestamp")
            metrics_window = item.get("metrics_window")
            if (
                not isinstance(metrics_window, str)
                or not metrics_window.strip()
                or re.fullmatch(r"\d+(?:\.\d+)?(?:s|m|h)", metrics_window.strip()) is None
            ):
                return _missing_semantic(f"memory role {role} metrics_window")
            limit = _strict_int(item.get("memory_limit_bytes"))
            if (
                limit is None
                or limit <= 0
                or limit > PERFORMANCE_MAX_KUBERNETES_MEMORY_BYTES
                or limit != expected_limits[key]
            ):
                return _missing_semantic(f"memory role {role} pod_resource_limits")
            role_memory_bytes += memory_bytes
        if role_observed_keys != expected_identity_keys[role]:
            return _missing_semantic(f"memory role {role} identity binding")
        if role_memory_bytes <= 0:
            return _missing_semantic(f"memory role {role} memory_bytes")
        total_memory_bytes += role_memory_bytes
        total_identity_count += identity_count

    if observed_identity_keys != all_identity_keys:
        return _missing_semantic("candidate.memory_observation observed Pod identities")
    observed_total, problem = _int_field(
        observation,
        "observed_identity_count",
        "candidate.memory_observation.observed_identity_count",
        exact=total_identity_count,
    )
    if problem is not None or observed_total is None:
        return problem or _missing_semantic("memory observed identity total")
    sampled_total, problem = _int_field(
        observation,
        "sampled_memory_bytes",
        "candidate.memory_observation.sampled_memory_bytes",
        exact=total_memory_bytes,
    )
    if problem is not None or sampled_total is None:
        return problem or _missing_semantic("sampled memory bytes")
    peak_bytes, problem = _int_field(
        observation,
        "peak_bytes",
        "candidate.memory_observation.peak_bytes",
        exact=total_memory_bytes,
    )
    if problem is not None or peak_bytes is None:
        return problem or _missing_semantic("memory peak bytes")
    threshold, problem = _int_field(
        observation,
        "safety_threshold_bytes",
        "candidate.memory_observation.safety_threshold_bytes",
        minimum=1,
    )
    if problem is not None or threshold is None:
        return problem or _missing_semantic("memory safety threshold")
    threshold_source = observation.get("threshold_source")
    limits_total = sum(expected_limits.values())
    if threshold_source == "pod_resource_limits":
        if threshold != limits_total:
            return _failed_semantic("Kubernetes memory threshold does not match Pod limits")
    elif threshold_source == "configured":
        kubernetes = preflight.get("kubernetes")
        configured_limit = (
            kubernetes.get("memory_limit_bytes") if isinstance(kubernetes, Mapping) else None
        )
        configured_value = _strict_int(configured_limit)
        if (
            configured_value is None
            or configured_value <= 0
            or configured_value != threshold
            or configured_value > limits_total
        ):
            return _missing_semantic("candidate.memory_observation configured memory limit")
    else:
        return _missing_semantic("candidate.memory_observation.threshold_source")
    if observation.get("within_safety_threshold") is not True or peak_bytes > threshold:
        return _failed_semantic("observed peak memory exceeds the safety threshold")
    available_memory = _strict_int(resources.get("available_memory_bytes"))
    if available_memory is None or available_memory <= 0:
        return _missing_semantic("candidate.preflight.resources.available_memory_bytes")
    return None


def _validate_performance_memory_observation(
    candidate: Mapping[str, Any],
    preflight: Mapping[str, Any],
    resources: Mapping[str, Any],
) -> tuple[str | None, str | None] | None:
    """Validate peak-memory evidence against preflight process identities."""

    observation, problem = _mapping_field(
        candidate, "memory_observation", "candidate.memory_observation"
    )
    if problem is not None or observation is None:
        return problem or _missing_semantic("candidate.memory_observation")
    if observation.get("status") == "fail":
        return _failed_semantic("candidate.memory_observation reported fail")
    if observation.get("status") != "pass":
        return _missing_semantic("candidate.memory_observation.status=pass")
    if observation.get("sampling_method") == "kubernetes_metrics_api":
        preflight_observation = preflight.get("memory_observation")
        if isinstance(preflight_observation, Mapping) and preflight_observation != observation:
            return _missing_semantic("candidate.preflight.memory_observation matching candidate")
        workers, problem = _list_field(
            preflight, "worker_processes", "candidate.preflight.worker_processes"
        )
        if problem is not None or workers is None:
            return problem or _missing_semantic("candidate.preflight.worker_processes")
        return _validate_kubernetes_performance_memory_observation(
            observation,
            preflight,
            resources,
            worker_processes=workers,
        )
    if observation.get("sampling_method") != "kernel_peak_counters":
        return _missing_semantic("candidate.memory_observation.sampling_method")
    sample_count = _strict_int(observation.get("sample_count"))
    if sample_count is None or sample_count < 1:
        return _missing_semantic("candidate.memory_observation.sample_count")
    sample_interval = _strict_number(observation.get("sampling_interval_seconds"))
    if sample_interval is None or sample_interval <= 0 or sample_interval > 10:
        return _missing_semantic("candidate.memory_observation.sampling_interval_seconds")
    required_roles = observation.get("required_roles")
    if not _is_json_sequence(required_roles):
        return _missing_semantic("candidate.memory_observation.required_roles")
    roles_sequence = cast(Sequence[Any], required_roles)
    if tuple(roles_sequence) != PERFORMANCE_REQUIRED_MEMORY_ROLES:
        return _missing_semantic("candidate.memory_observation.required_roles")
    if observation.get("coverage_complete") is not True:
        return _missing_semantic("candidate.memory_observation.coverage_complete=true")
    role_observations, problem = _mapping_field(
        observation,
        "role_observations",
        "candidate.memory_observation.role_observations",
    )
    if problem is not None or role_observations is None:
        return problem or _missing_semantic("candidate.memory_observation.role_observations")
    if set(role_observations) != set(PERFORMANCE_REQUIRED_MEMORY_ROLES):
        return _missing_semantic(
            "candidate.memory_observation.role_observations exact required roles"
        )
    participating, problem = _mapping_field(
        preflight,
        "participating_processes",
        "candidate.preflight.participating_processes",
    )
    if problem is not None or participating is None:
        return problem or _missing_semantic("candidate.preflight.participating_processes")
    if set(participating) != set(PERFORMANCE_REQUIRED_MEMORY_ROLES):
        return _missing_semantic("candidate.preflight.participating_processes exact required roles")
    expected_identity_keys: dict[str, set[tuple[int, str | None]]] = {}
    all_expected_keys: set[tuple[int, str | None]] = set()
    for role in PERFORMANCE_REQUIRED_MEMORY_ROLES:
        identities = participating.get(role)
        if not _is_json_sequence(identities) or not identities:
            return _missing_semantic(f"candidate.preflight.participating_processes.{role}")
        role_keys: set[tuple[int, str | None]] = set()
        for identity in cast(Sequence[Any], identities):
            if not isinstance(identity, Mapping):
                return _missing_semantic(f"candidate.preflight.participating_processes.{role}")
            pid = _strict_int(identity.get("pid"))
            container_id = identity.get("container_id")
            if pid is None or pid < 2:
                return _missing_semantic(f"candidate.preflight.participating_processes.{role} pid")
            if container_id is not None and (
                not isinstance(container_id, str) or not container_id.strip()
            ):
                return _missing_semantic(
                    f"candidate.preflight.participating_processes.{role} container identity"
                )
            identity_key = (pid, container_id)
            if identity_key in role_keys or identity_key in all_expected_keys:
                return _failed_semantic("preflight memory process identities are duplicated")
            role_keys.add(identity_key)
            all_expected_keys.add(identity_key)
        expected_identity_keys[role] = role_keys
    total_observed = 0
    for role in PERFORMANCE_REQUIRED_MEMORY_ROLES:
        role_value, problem = _mapping_field(
            role_observations,
            role,
            f"candidate.memory_observation.role_observations.{role}",
        )
        if problem is not None or role_value is None:
            return problem or _missing_semantic(f"memory role {role}")
        identity_count, problem = _int_field(
            role_value,
            "identity_count",
            f"candidate.memory_observation.{role}.identity_count",
            minimum=1,
        )
        if problem is not None or identity_count is None:
            return problem or _missing_semantic(f"memory role {role} identity count")
        observed_count, problem = _int_field(
            role_value,
            "observed_count",
            f"candidate.memory_observation.{role}.observed_count",
            exact=identity_count,
        )
        if problem is not None or observed_count is None:
            return problem or _missing_semantic(f"memory role {role} observed count")
        observations, problem = _list_field(
            role_value,
            "observations",
            f"candidate.memory_observation.{role}.observations",
        )
        if problem is not None or observations is None:
            return problem or _missing_semantic(f"memory role {role} observations")
        if len(observations) != identity_count:
            return _failed_semantic(f"memory role {role} observation count mismatch")
        observed_identity_keys: set[tuple[int, str | None]] = set()
        for item in observations:
            if not isinstance(item, Mapping) or item.get("role") != role:
                return _missing_semantic(f"memory role {role} identity binding")
            pid = _strict_int(item.get("pid"))
            if pid is None or pid < 2:
                return _missing_semantic(f"memory role {role} pid")
            container_id = item.get("container_id")
            if container_id is not None and (
                not isinstance(container_id, str) or not container_id.strip()
            ):
                return _missing_semantic(f"memory role {role} container identity")
            identity_key = (pid, container_id)
            if identity_key in observed_identity_keys:
                return _failed_semantic(f"memory role {role} identities are duplicated")
            observed_identity_keys.add(identity_key)
            rss_peak = _strict_int(item.get("rss_peak_bytes"))
            cgroup_peak = _strict_int(item.get("cgroup_peak_bytes"))
            if (rss_peak is None or rss_peak <= 0) and (cgroup_peak is None or cgroup_peak <= 0):
                return _missing_semantic(f"memory role {role} real peak bytes")
        if observed_identity_keys != expected_identity_keys[role]:
            return _missing_semantic(f"memory role {role} identity binding")
        total_observed += observed_count
    reported_total, problem = _int_field(
        observation,
        "observed_identity_count",
        "candidate.memory_observation.observed_identity_count",
        exact=total_observed,
    )
    if problem is not None or reported_total is None:
        return problem or _missing_semantic("memory observed identity total")
    peak_bytes, problem = _int_field(
        observation,
        "peak_bytes",
        "candidate.memory_observation.peak_bytes",
        minimum=1,
    )
    if problem is not None or peak_bytes is None:
        return problem or _missing_semantic("memory peak bytes")
    threshold, problem = _int_field(
        observation,
        "safety_threshold_bytes",
        "candidate.memory_observation.safety_threshold_bytes",
        minimum=1,
    )
    if problem is not None or threshold is None:
        return problem or _missing_semantic("memory safety threshold")
    if observation.get("threshold_source") not in {"cgroup_limit", "available_memory"}:
        return _missing_semantic("candidate.memory_observation.threshold_source")
    if observation.get("within_safety_threshold") is not True or peak_bytes > threshold:
        return _failed_semantic("observed peak memory exceeds the safety threshold")
    available_memory = _strict_int(resources.get("available_memory_bytes"))
    if available_memory is None or available_memory <= 0:
        return _missing_semantic("candidate.preflight.resources.available_memory_bytes")
    return None


def _validate_real_performance_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Validate the actual workload evidence before release aggregation.

    The producer's source fingerprint proves which checkout emitted a report;
    it does not prove that the report contains the locked workload.  This
    second, deliberately strict validation keeps an old/direct-only report
    from being promoted merely by changing ``production_gate`` to ``pass``.
    """

    if not _schema_version_is(report.get("schema_version"), 1):
        return _missing_semantic("schema_version=1")
    gate = report.get("gate")
    if gate == "fail":
        return _failed_semantic("top-level gate reported fail")
    if gate != "pass":
        return _missing_semantic("top-level gate=pass")

    candidate, problem = _mapping_field(report, "candidate", "candidate")
    if problem is not None or candidate is None:
        return problem or _missing_semantic("candidate")
    if candidate.get("mode") != "real_postgresql_redis_multiprocess":
        return _missing_semantic("candidate.mode=real_postgresql_redis_multiprocess")
    candidate_run_id = candidate.get("run_id")
    evidence_run_id = evidence.get("run_id")
    if not isinstance(candidate_run_id, str) or not candidate_run_id:
        return _missing_semantic("candidate.run_id")
    if candidate_run_id != evidence_run_id:
        return _missing_semantic("candidate.run_id matching evidence.run_id")

    parameters, problem = _mapping_field(candidate, "parameters", "candidate.parameters")
    if problem is not None or parameters is None:
        return problem or _missing_semantic("candidate.parameters")
    _, problem = _int_field(
        parameters,
        "min_workers",
        "candidate.parameters.min_workers",
        minimum=PERFORMANCE_REQUIRED_WORKERS,
    )
    if problem is not None:
        return problem
    min_workers = _strict_int(parameters.get("min_workers"))
    if min_workers is None or min_workers > PERFORMANCE_MAX_WORKERS:
        return _failed_semantic("candidate.parameters.min_workers exceeds the safety cap")
    for key, expected in (
        ("callbacks", PERFORMANCE_REQUIRED_CALLBACKS),
        ("burst_turns", PERFORMANCE_REQUIRED_BURST_TURNS),
        ("target_max_turn_overlap", PERFORMANCE_REQUIRED_BURST_TURNS),
    ):
        _, problem = _int_field(
            parameters,
            key,
            f"candidate.parameters.{key}",
            minimum=expected,
        )
        if problem is not None:
            return problem
    callbacks = _strict_int(parameters.get("callbacks"))
    burst_turns = _strict_int(parameters.get("burst_turns"))
    target_overlap = _strict_int(parameters.get("target_max_turn_overlap"))
    if callbacks is None or callbacks > PERFORMANCE_MAX_CALLBACKS:
        return _failed_semantic("candidate.parameters.callbacks exceeds the safety cap")
    if burst_turns is None or burst_turns > PERFORMANCE_MAX_BURST_TURNS:
        return _failed_semantic("candidate.parameters.burst_turns exceeds the safety cap")
    if target_overlap is None or target_overlap > PERFORMANCE_MAX_BURST_TURNS:
        return _failed_semantic(
            "candidate.parameters.target_max_turn_overlap exceeds the safety cap"
        )
    if target_overlap != burst_turns:
        return _failed_semantic("target_max_turn_overlap must equal burst_turns")
    callback_rate, problem = _number_field(
        parameters,
        "callback_rate_per_second",
        "candidate.parameters.callback_rate_per_second",
        minimum=PERFORMANCE_REQUIRED_CALLBACK_RATE,
    )
    if problem is not None:
        return problem
    if callback_rate is None or callback_rate > PERFORMANCE_MAX_CALLBACK_RATE:
        return _failed_semantic(
            "candidate.parameters.callback_rate_per_second exceeds the safety cap"
        )
    db_pool_size, problem = _int_field(
        parameters,
        "db_pool_size",
        "candidate.parameters.db_pool_size",
        minimum=2,
    )
    if problem is not None or db_pool_size is None:
        return problem or _missing_semantic("candidate.parameters.db_pool_size")
    if db_pool_size > PERFORMANCE_MAX_DB_POOL_SIZE:
        return _failed_semantic("candidate.parameters.db_pool_size exceeds the safety cap")
    if db_pool_size != PERFORMANCE_REQUIRED_DB_POOL_SIZE:
        return _failed_semantic(
            "candidate.parameters.db_pool_size must equal the locked formal value "
            f"{PERFORMANCE_REQUIRED_DB_POOL_SIZE}"
        )
    max_inflight, problem = _int_field(
        parameters, "max_inflight", "candidate.parameters.max_inflight", minimum=1
    )
    if problem is not None or max_inflight is None:
        return problem or _missing_semantic("candidate.parameters.max_inflight")
    if max_inflight > PERFORMANCE_MAX_INFLIGHT:
        return _failed_semantic("candidate.parameters.max_inflight exceeds the safety cap")
    if max_inflight != PERFORMANCE_REQUIRED_INFLIGHT:
        return _failed_semantic(
            "candidate.parameters.max_inflight must equal the locked formal value "
            f"{PERFORMANCE_REQUIRED_INFLIGHT}"
        )
    if db_pool_size > max_inflight:
        return _failed_semantic("database pool exceeds the bounded in-flight limit")
    max_inflight_accepts, problem = _int_field(
        parameters,
        "max_inflight_accepts",
        "candidate.parameters.max_inflight_accepts",
        minimum=1,
    )
    if problem is not None or max_inflight_accepts is None:
        return problem or _missing_semantic("candidate.parameters.max_inflight_accepts")
    if max_inflight_accepts > PERFORMANCE_MAX_INFLIGHT:
        return _failed_semantic("candidate.parameters.max_inflight_accepts exceeds the safety cap")
    if max_inflight_accepts != max_inflight:
        return _failed_semantic("max_inflight_accepts must equal max_inflight")
    timeout_seconds, problem = _number_field(
        parameters,
        "timeout_seconds",
        "candidate.parameters.timeout_seconds",
        minimum=0.001,
    )
    if problem is not None or timeout_seconds is None:
        return problem or _missing_semantic("candidate.parameters.timeout_seconds")
    if timeout_seconds > PERFORMANCE_MAX_TIMEOUT_SECONDS:
        return _failed_semantic("candidate.parameters.timeout_seconds exceeds the safety cap")
    if parameters.get("db_pool_scope") != "load_generator_only":
        return _missing_semantic("candidate.parameters.db_pool_scope=load_generator_only")

    preflight, problem = _mapping_field(candidate, "preflight", "candidate.preflight")
    if problem is not None or preflight is None:
        return problem or _missing_semantic("candidate.preflight")
    preflight_status = preflight.get("status")
    if preflight_status == "fail":
        return _failed_semantic("candidate.preflight reported fail")
    if preflight_status != "pass":
        return _missing_semantic("candidate.preflight.status=pass")
    preflight_worker_count, problem = _int_field(
        preflight,
        "worker_count",
        "candidate.preflight.worker_count",
        exact=PERFORMANCE_REQUIRED_WORKERS,
    )
    if problem is not None:
        return problem
    _, problem = _int_field(
        preflight,
        "worker_concurrency",
        "candidate.preflight.worker_concurrency",
        exact=PERFORMANCE_REQUIRED_WORKER_CONCURRENCY,
    )
    if problem is not None:
        return problem
    resources, problem = _mapping_field(preflight, "resources", "candidate.preflight.resources")
    if problem is not None or resources is None:
        return problem or _missing_semantic("candidate.preflight.resources")
    resource_values: dict[str, int] = {}
    for key in (
        "cpu_count",
        "available_memory_bytes",
        "required_memory_bytes",
        "estimated_runtime_connections",
        "max_estimated_runtime_connections",
    ):
        value = _strict_int(resources.get(key))
        if value is None or value <= 0:
            return _missing_semantic(f"candidate.preflight.resources.{key}")
        resource_values[key] = value
    if resource_values["cpu_count"] < PERFORMANCE_REQUIRED_WORKERS:
        return _failed_semantic("preflight CPU capacity is below the worker requirement")
    if resource_values["available_memory_bytes"] < resource_values["required_memory_bytes"]:
        return _failed_semantic("preflight memory capacity is below the safety threshold")
    if (
        resource_values["estimated_runtime_connections"]
        > resource_values["max_estimated_runtime_connections"]
    ):
        return _failed_semantic("preflight PostgreSQL connection budget is exceeded")
    if resource_values["max_estimated_runtime_connections"] != PERFORMANCE_MAX_CONNECTIONS:
        return _missing_semantic(
            "candidate.preflight.resources.max_estimated_runtime_connections locked cap"
        )
    preflight_source = preflight.get("source_fingerprint")
    evidence_source = evidence.get("source_fingerprint")
    if (
        not isinstance(preflight_source, Mapping)
        or not isinstance(evidence_source, Mapping)
        or preflight_source.get("status") != "available"
        or preflight_source.get("value") != evidence_source.get("value")
    ):
        return _missing_semantic("preflight source fingerprint matching evidence")
    attestation, problem = _mapping_field(
        preflight, "worker_image_attestation", "candidate.preflight.worker_image_attestation"
    )
    if problem is not None or attestation is None:
        return problem or _missing_semantic("worker image attestation")
    attestation_status = attestation.get("status")
    if attestation_status == "fail":
        return _failed_semantic("worker image attestation reported fail")
    if attestation_status != "pass":
        return _missing_semantic("worker image attestation status=pass")
    worker_count, problem = _int_field(
        attestation,
        "worker_count",
        "worker image attestation worker_count",
        minimum=PERFORMANCE_REQUIRED_WORKERS,
    )
    if problem is not None:
        return problem
    if worker_count is None or worker_count > PERFORMANCE_MAX_WORKERS:
        return _failed_semantic("worker image attestation worker count exceeds safety cap")
    image_count, problem = _int_field(
        attestation, "image_count", "worker image attestation image_count", exact=1
    )
    if problem is not None:
        return problem
    if attestation.get("source_fingerprint_matches") is not True:
        return _failed_semantic("worker image source fingerprint does not match")
    workers, problem = _list_field(
        preflight, "worker_processes", "candidate.preflight.worker_processes"
    )
    if problem is not None or workers is None:
        return problem or _missing_semantic("candidate.preflight.worker_processes")
    memory_observation = candidate.get("memory_observation")
    kubernetes_memory_mode = (
        isinstance(memory_observation, Mapping)
        and memory_observation.get("sampling_method") == "kubernetes_metrics_api"
    )
    worker_ids: list[str] = []
    worker_pids: list[int] = []
    worker_pod_uids: set[str] = set()
    worker_images: set[str] = set()
    for worker in workers:
        if not isinstance(worker, Mapping) or not isinstance(worker.get("container_id"), str):
            return _missing_semantic("candidate.preflight.worker_processes")
        container_id = worker["container_id"]
        if kubernetes_memory_mode:
            pod_uid = worker.get("pod_uid")
            container_name = worker.get("container_name")
            pod_name = worker.get("pod_name")
            memory_limit = _strict_int(worker.get("memory_limit_bytes"))
            if (
                worker.get("role") != "worker"
                or not container_id.strip()
                or not isinstance(pod_name, str)
                or not pod_name.strip()
                or not isinstance(pod_uid, str)
                or not pod_uid.strip()
                or not isinstance(container_name, str)
                or not container_name.strip()
                or container_name != "worker"
                or memory_limit is None
                or memory_limit <= 0
            ):
                return _missing_semantic("candidate.preflight worker Pod identity")
            if pod_uid in worker_pod_uids:
                return _failed_semantic("independent worker Pod identities are duplicated")
            worker_pod_uids.add(pod_uid)
        worker_ids.append(container_id)
        pid = _strict_int(worker.get("pid"))
        image_id = worker.get("image_id")
        source_value = worker.get("source_fingerprint")
        if (
            (not kubernetes_memory_mode and (pid is None or pid < 2))
            or not isinstance(image_id, str)
            or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(image_id.lower()) is None
            or not isinstance(evidence_source, Mapping)
            or (
                source_value != evidence_source.get("value")
                and (source_value is not None or not kubernetes_memory_mode)
            )
        ):
            return _missing_semantic("candidate.preflight worker process image/source identity")
        if not kubernetes_memory_mode:
            assert pid is not None
            worker_pids.append(pid)
        worker_images.add(image_id.lower())
    if len(worker_ids) < PERFORMANCE_REQUIRED_WORKERS or len(worker_ids) != len(set(worker_ids)):
        return _failed_semantic("independent worker identities are incomplete or duplicated")
    if preflight_worker_count != len(worker_ids):
        return _failed_semantic("preflight worker_count does not match worker identities")
    if not kubernetes_memory_mode and len(worker_pids) != len(set(worker_pids)):
        return _failed_semantic("worker process identities are inconsistent")
    if len(worker_images) != 1:
        return _failed_semantic("worker process or immutable image identities are inconsistent")
    if resource_values["cpu_count"] < len(worker_ids):
        return _failed_semantic("preflight CPU capacity is below discovered worker count")
    if worker_count != len(worker_ids):
        return _failed_semantic("worker image attestation count does not match worker processes")
    if image_count != 1:
        return _failed_semantic("worker image count must be 1")
    expected_connections = (
        PERFORMANCE_GATEWAY_POOL_MAX_SIZE
        + (len(worker_ids) * PERFORMANCE_WORKER_POOL_MAX_SIZE)
        + PERFORMANCE_OUTBOX_POOL_MAX_SIZE
        + PERFORMANCE_RECOVERY_POOL_MAX_SIZE
        + db_pool_size
        + PERFORMANCE_PROBE_CONNECTION_HEADROOM
    )
    if resource_values["estimated_runtime_connections"] != expected_connections:
        return _failed_semantic(
            "preflight PostgreSQL connection estimate does not match the locked topology"
        )
    if expected_connections > PERFORMANCE_MAX_CONNECTIONS:
        return _failed_semantic("preflight PostgreSQL connection estimate exceeds safety cap")
    memory_problem = _validate_performance_memory_observation(candidate, preflight, resources)
    if memory_problem is not None:
        return memory_problem
    runtime_fingerprint = evidence.get("runtime_fingerprint")
    redis_stream = parameters.get("redis_stream")
    redis_group = parameters.get("redis_group")
    if (
        parameters.get("scheduler_version") != "v2"
        or redis_stream != "trpc:session-ready:v2"
        or redis_group != "trpc-session-ready-v2"
        or not _runtime_fingerprint_matches(
            runtime_fingerprint,
            mode=str(candidate.get("mode")),
            worker_identities=workers,
            stream=redis_stream,
            group=redis_group,
            parameters=parameters,
        )
    ):
        return _missing_semantic("runtime fingerprint bound to workers and parameters")
    if candidate.get("pool_prewarmed") is not True:
        return _missing_semantic("candidate.pool_prewarmed=true")
    warmup, problem = _mapping_field(candidate, "warmup", "candidate.warmup")
    if problem is not None or warmup is None:
        return problem or _missing_semantic("candidate.warmup")
    if warmup.get("passed") is not True or warmup.get("excluded_from_burst_overlap") is not True:
        return _missing_semantic("candidate.warmup passed and excluded_from_burst_overlap")
    stages, problem = _list_field(warmup, "stages", "candidate.warmup.stages")
    if problem is not None or stages is None:
        return problem or _missing_semantic("candidate.warmup.stages")
    if len(stages) != len(PERFORMANCE_REQUIRED_WARMUP_STEPS):
        return _missing_semantic("candidate.warmup.stages complete safe ramp")
    for index, expected in enumerate(PERFORMANCE_REQUIRED_WARMUP_STEPS):
        stage = stages[index]
        if not isinstance(stage, Mapping):
            return _missing_semantic(f"candidate.warmup.stages[{index}]")
        for key in ("requested", "accepted"):
            value = _strict_int(stage.get(key))
            if value != expected:
                return _missing_semantic(f"candidate.warmup.stages[{index}].{key}={expected}")
        errors = _strict_int(stage.get("errors"))
        if errors != 0:
            return _failed_semantic(f"candidate.warmup.stages[{index}] recorded errors")
        completion = stage.get("completion")
        if not isinstance(completion, Mapping) or completion.get("status") != "pass":
            return _missing_semantic(f"candidate.warmup.stages[{index}].completion.status=pass")

    sustained, problem = _mapping_field(candidate, "sustained", "candidate.sustained")
    if problem is not None or sustained is None:
        return problem or _missing_semantic("candidate.sustained")
    if sustained.get("ingress_mode") != "synthetic_encrypted_feishu_http":
        return _missing_semantic("candidate.sustained.ingress_mode=synthetic_encrypted_feishu_http")
    if sustained.get("timed_out") is not False:
        return _failed_semantic("candidate.sustained timed out or lacks timeout evidence")
    requested, problem = _int_field(
        sustained,
        "requested_callbacks",
        "candidate.sustained.requested_callbacks",
        minimum=PERFORMANCE_REQUIRED_CALLBACKS,
    )
    if problem is not None or requested is None:
        return problem or _missing_semantic("candidate.sustained.requested_callbacks")
    _, problem = _number_field(
        sustained,
        "offered_callback_rate_per_second",
        "candidate.sustained.offered_callback_rate_per_second",
        minimum=PERFORMANCE_REQUIRED_CALLBACK_RATE,
    )
    if problem is not None:
        return problem
    try:
        evidence_generated_at = datetime.fromisoformat(
            str(evidence.get("generated_at")).replace("Z", "+00:00")
        )
        sustained_started_at = datetime.fromisoformat(
            str(sustained.get("callback_submission_started_at")).replace("Z", "+00:00")
        )
        sustained_last_started_at = datetime.fromisoformat(
            str(sustained.get("callback_submission_last_started_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _missing_semantic("candidate.sustained callback submission timestamps")
    if (
        evidence_generated_at.tzinfo is None
        or sustained_started_at.tzinfo is None
        or sustained_last_started_at.tzinfo is None
        or sustained_last_started_at < sustained_started_at
        or sustained_last_started_at > evidence_generated_at + timedelta(seconds=5)
    ):
        return _missing_semantic("candidate.sustained callback submission timestamp ordering")
    _, problem = _int_field(
        sustained, "accepted_callbacks", "candidate.sustained.accepted_callbacks", exact=requested
    )
    if problem is not None:
        return problem
    errors = sustained.get("errors")
    if errors is not None:
        _, problem = _int_field(sustained, "errors", "candidate.sustained.errors", exact=0)
        if problem is not None:
            return problem
    _, problem = _number_field(
        sustained,
        "ack_p95_ms",
        "candidate.sustained.ack_p95_ms",
        maximum=PERFORMANCE_MAX_ACK_P95_MS,
    )
    if problem is not None:
        return problem
    _, problem = _number_field(
        sustained,
        "actual_submission_start_rate_per_second",
        "candidate.sustained.actual_submission_start_rate_per_second",
        minimum=PERFORMANCE_REQUIRED_CALLBACK_RATE,
    )
    if problem is not None:
        return problem
    accepted_ids, problem = _unique_id_field(
        sustained,
        "accepted_inbound_ids",
        "candidate.sustained.accepted_inbound_ids",
        expected_count=requested,
    )
    if problem is not None or accepted_ids is None:
        return problem or _missing_semantic("candidate.sustained.accepted_inbound_ids")
    session_ids, problem = _unique_id_field(
        sustained, "session_ids", "candidate.sustained.session_ids", expected_count=requested
    )
    if problem is not None or session_ids is None:
        return problem or _missing_semantic("candidate.sustained.session_ids")
    _, problem = _int_field(
        sustained,
        "unique_inbound_id_count",
        "candidate.sustained.unique_inbound_id_count",
        exact=requested,
    )
    if problem is not None:
        return problem
    _, problem = _int_field(
        sustained,
        "duplicate_inbound_id_count",
        "candidate.sustained.duplicate_inbound_id_count",
        exact=0,
    )
    if problem is not None:
        return problem
    _, problem = _int_field(
        sustained,
        "unique_session_id_count",
        "candidate.sustained.unique_session_id_count",
        exact=requested,
    )
    if problem is not None:
        return problem
    _, problem = _int_field(
        sustained,
        "duplicate_session_id_count",
        "candidate.sustained.duplicate_session_id_count",
        exact=0,
    )
    if problem is not None:
        return problem
    _, problem = _int_field(
        sustained,
        "accepted_external_message_id_count",
        "candidate.sustained.accepted_external_message_id_count",
        exact=requested,
    )
    if problem is not None:
        return problem

    gateway, problem = _mapping_field(sustained, "gateway", "candidate.sustained.gateway")
    if problem is not None or gateway is None:
        return problem or _missing_semantic("candidate.sustained.gateway")
    gateway_class = gateway.get("host_class")
    if gateway_class not in {"loopback", "kubernetes_service"} or gateway.get("scheme") not in {
        "http",
        "https",
    }:
        return _missing_semantic("candidate.sustained.gateway endpoint proof")
    if gateway_class == "kubernetes_service":
        kubernetes, problem = _mapping_field(
            preflight, "kubernetes", "candidate.preflight.kubernetes"
        )
        if problem is not None or kubernetes is None:
            return problem or _missing_semantic("candidate.preflight.kubernetes")
        if (
            gateway.get("service_name") != "trpc-gateway"
            or gateway.get("namespace") != kubernetes.get("namespace")
            or kubernetes.get("namespace_bound") is not True
        ):
            return _missing_semantic("candidate.sustained.gateway Kubernetes Service binding")
    _, problem = _int_field(gateway, "port", "candidate.sustained.gateway.port", minimum=1)
    if problem is not None:
        return problem
    problem = _validate_http_status_counts(
        sustained, path="candidate.sustained.http_status_counts", expected_count=requested
    )
    if problem is not None:
        return problem

    lookup, problem = _mapping_field(
        sustained, "authoritative_lookup", "candidate.sustained.authoritative_lookup"
    )
    if problem is not None or lookup is None:
        return problem or _missing_semantic("candidate.sustained.authoritative_lookup")
    lookup_status = lookup.get("status")
    if lookup_status == "fail":
        return _failed_semantic("authoritative inbound lookup reported fail")
    if lookup_status != "pass":
        return _missing_semantic("candidate.sustained.authoritative_lookup.status=pass")
    for key, exact in (
        ("requested_count", requested),
        ("row_count", requested),
        ("missing_count", 0),
        ("duplicate_count", 0),
    ):
        _, problem = _int_field(
            lookup, key, f"candidate.sustained.authoritative_lookup.{key}", exact=exact
        )
        if problem is not None:
            return problem
    lookup_inbound_ids, problem = _unique_id_field(
        lookup,
        "inbound_ids",
        "candidate.sustained.authoritative_lookup.inbound_ids",
        expected_count=requested,
    )
    if problem is not None or lookup_inbound_ids is None:
        return problem or _missing_semantic("candidate.sustained.authoritative_lookup.inbound_ids")
    lookup_session_ids, problem = _unique_id_field(
        lookup,
        "session_ids",
        "candidate.sustained.authoritative_lookup.session_ids",
        expected_count=requested,
    )
    if problem is not None or lookup_session_ids is None:
        return problem or _missing_semantic("candidate.sustained.authoritative_lookup.session_ids")
    if set(lookup_inbound_ids) != set(accepted_ids) or set(lookup_session_ids) != set(session_ids):
        return _failed_semantic("authoritative IDs do not match accepted callback IDs")
    problem = _validate_completion(sustained, path="candidate.sustained", expected_count=requested)
    if problem is not None:
        return problem
    sustained_gate, problem = _mapping_field(sustained, "gate", "candidate.sustained.gate")
    if problem is not None or sustained_gate is None:
        return problem or _missing_semantic("candidate.sustained.gate")
    _, problem = _status_field(sustained_gate, "status", "candidate.sustained.gate.status")
    if problem is not None:
        return problem

    burst, problem = _mapping_field(candidate, "burst", "candidate.burst")
    if problem is not None or burst is None:
        return problem or _missing_semantic("candidate.burst")
    burst_requested, problem = _int_field(
        burst,
        "requested_concurrent_turns",
        "candidate.burst.requested_concurrent_turns",
        minimum=PERFORMANCE_REQUIRED_BURST_TURNS,
    )
    if problem is not None or burst_requested is None:
        return problem or _missing_semantic("candidate.burst.requested_concurrent_turns")
    if (
        parameters.get("callbacks") != requested
        or parameters.get("callback_rate_per_second")
        != sustained.get("offered_callback_rate_per_second")
        or parameters.get("burst_turns") != burst_requested
    ):
        return _failed_semantic("candidate workload parameters do not match phase evidence")
    if burst.get("timed_out") is not False:
        return _failed_semantic("candidate.burst timed out or lacks timeout evidence")
    try:
        burst_started_at = datetime.fromisoformat(
            str(burst.get("callback_submission_started_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _missing_semantic("candidate.burst.callback_submission_started_at")
    if (
        burst_started_at.tzinfo is None
        or burst_started_at < sustained_started_at
        or burst_started_at > evidence_generated_at + timedelta(seconds=5)
    ):
        return _missing_semantic("candidate.burst callback submission timestamp ordering")
    _, problem = _int_field(
        burst, "accepted_callbacks", "candidate.burst.accepted_callbacks", exact=burst_requested
    )
    if problem is not None:
        return problem
    _, problem = _int_field(burst, "errors", "candidate.burst.errors", exact=0)
    if problem is not None:
        return problem
    _, problem = _number_field(
        burst,
        "actual_submission_start_rate_per_second",
        "candidate.burst.actual_submission_start_rate_per_second",
        minimum=PERFORMANCE_REQUIRED_CALLBACK_RATE,
    )
    if problem is not None:
        return problem
    _, problem = _number_field(
        burst,
        "ack_p95_ms",
        "candidate.burst.ack_p95_ms",
        maximum=PERFORMANCE_MAX_ACK_P95_MS,
    )
    if problem is not None:
        return problem
    _, problem = _number_field(
        burst,
        "max_turn_overlap_observed",
        "candidate.burst.max_turn_overlap_observed",
        minimum=PERFORMANCE_REQUIRED_BURST_TURNS,
    )
    if problem is not None:
        return problem
    burst_ids, problem = _unique_id_field(
        burst,
        "accepted_inbound_ids",
        "candidate.burst.accepted_inbound_ids",
        expected_count=burst_requested,
    )
    if problem is not None or burst_ids is None:
        return problem or _missing_semantic("candidate.burst.accepted_inbound_ids")
    burst_sessions, problem = _unique_id_field(
        burst, "session_ids", "candidate.burst.session_ids", expected_count=burst_requested
    )
    if problem is not None or burst_sessions is None:
        return problem or _missing_semantic("candidate.burst.session_ids")
    if set(burst_ids) & set(accepted_ids) or set(burst_sessions) & set(session_ids):
        return _failed_semantic("sustained and burst batches reuse durable identities")
    for key, exact in (
        ("unique_inbound_id_count", burst_requested),
        ("duplicate_inbound_id_count", 0),
        ("unique_session_id_count", burst_requested),
        ("duplicate_session_id_count", 0),
    ):
        _, problem = _int_field(burst, key, f"candidate.burst.{key}", exact=exact)
        if problem is not None:
            return problem
    problem = _validate_completion(burst, path="candidate.burst", expected_count=burst_requested)
    if problem is not None:
        return problem
    burst_gate, problem = _mapping_field(burst, "gate", "candidate.burst.gate")
    if problem is not None or burst_gate is None:
        return problem or _missing_semantic("candidate.burst.gate")
    _, problem = _status_field(burst_gate, "status", "candidate.burst.gate.status")
    if problem is not None:
        return problem

    redis, problem = _mapping_field(candidate, "redis", "candidate.redis")
    if problem is not None or redis is None:
        return problem or _missing_semantic("candidate.redis")
    baseline, problem = _mapping_field(redis, "baseline", "candidate.redis.baseline")
    if problem is not None or baseline is None:
        return problem or _missing_semantic("candidate.redis.baseline")
    after_burst, problem = _mapping_field(redis, "after_burst", "candidate.redis.after_burst")
    if problem is not None or after_burst is None:
        return problem or _missing_semantic("candidate.redis.after_burst")
    _, problem = _int_field(baseline, "pending", "candidate.redis.baseline.pending", exact=0)
    if problem is not None:
        return problem
    _, problem = _int_field(after_burst, "pending", "candidate.redis.after_burst.pending", exact=0)
    if problem is not None:
        return problem
    for key in ("baseline_pending_is_zero", "final_pending_is_zero"):
        if redis.get(key) is not True:
            return _failed_semantic(f"candidate.redis.{key} is not true")
    return None, None


def _runtime_missing(path: str) -> tuple[str, str]:
    return "not_run", f"real runtime evidence is missing or invalid {path}"


def _runtime_failed(reason: str) -> tuple[str, str]:
    return "fail", f"real runtime acceptance failed: {reason}"


def _runtime_mapping_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[Mapping[str, Any] | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not isinstance(value, Mapping):
        return None, _runtime_missing(path)
    return value, None


def _runtime_list_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[list[Any] | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not _is_json_sequence(value):
        return None, _runtime_missing(path)
    return list(cast(Sequence[Any], value)), None


def _runtime_int_field(
    parent: Mapping[str, Any],
    key: str,
    path: str,
    *,
    minimum: int | None = None,
    exact: int | None = None,
    scope: bool = False,
) -> tuple[int | None, tuple[str, str] | None]:
    if key not in parent:
        return None, _runtime_missing(path)
    value = _strict_int(parent.get(key))
    if value is None:
        return None, _runtime_missing(path)
    if exact is not None and value != exact:
        return value, _runtime_failed(f"{path} must equal {exact}")
    if minimum is not None and value < minimum:
        if scope:
            return value, (
                "not_run",
                f"real runtime production scope requires {path} >= {minimum}; requested {value}",
            )
        return value, _runtime_failed(f"{path} must be at least {minimum}")
    return value, None


def _runtime_status_field(
    parent: Mapping[str, Any], key: str, path: str
) -> tuple[str | None, tuple[str, str] | None]:
    value = parent.get(key)
    if not isinstance(value, str):
        return None, _runtime_missing(path)
    if value == "fail":
        return value, _runtime_failed(f"{path} reported fail")
    if value != "pass":
        return value, _runtime_missing(path)
    return value, None


def _validate_runtime_markers(
    parent: Mapping[str, Any],
    *,
    path: str,
    required: Sequence[str],
    started_at: datetime,
    ended_at: datetime,
    run_id: str,
    run_nonce: str,
) -> tuple[str, str] | None:
    markers, problem = _runtime_list_field(parent, "stage_markers", f"{path}.stage_markers")
    if problem is not None or markers is None:
        return problem
    by_name: dict[str, list[Mapping[str, Any]]] = {}
    for marker in markers:
        if not isinstance(marker, Mapping) or not isinstance(marker.get("name"), str):
            return _runtime_missing(f"{path}.stage_markers")
        try:
            observed_at = datetime.fromisoformat(
                str(marker.get("observed_at")).replace("Z", "+00:00")
            )
        except ValueError:
            return _runtime_missing(f"{path}.stage_markers.observed_at")
        if observed_at.tzinfo is None or observed_at < started_at or observed_at > ended_at:
            return _runtime_missing(f"{path}.stage_markers observed_at run window")
        if marker.get("run_id") != run_id or marker.get("run_nonce") != run_nonce:
            return _runtime_missing(f"{path}.stage_markers run identity binding")
        by_name.setdefault(marker["name"], []).append(marker)
    missing = [name for name in required if name not in by_name]
    if missing:
        return _runtime_missing(f"{path}.stage_markers missing {','.join(missing)}")
    for name in required:
        for marker in by_name[name]:
            status = marker.get("status")
            if status == "fail":
                return _runtime_failed(f"{path}.stage_markers.{name} reported fail")
            if status != "pass":
                return _runtime_missing(f"{path}.stage_markers.{name}=pass")
    return None


def _validate_runtime_batch(
    batch: Mapping[str, Any], *, path: str, expected_count: int, expected_duplicates: int
) -> tuple[str, str] | None:
    accepted_calls, problem = _runtime_int_field(
        batch,
        "accepted_calls",
        f"{path}.accepted_calls",
        exact=expected_count + expected_duplicates,
    )
    if problem is not None:
        return problem
    if accepted_calls is None:
        return _runtime_missing(f"{path}.accepted_calls")
    _, problem = _runtime_int_field(
        batch,
        "duplicate_calls",
        f"{path}.duplicate_calls",
        exact=expected_duplicates,
    )
    if problem is not None:
        return problem
    inbound_ids, problem = _runtime_list_field(
        batch, "unique_inbound_ids", f"{path}.unique_inbound_ids"
    )
    if problem is not None or inbound_ids is None:
        return problem or _runtime_missing(f"{path}.unique_inbound_ids")
    if len(inbound_ids) != expected_count or any(
        not isinstance(item, str) or not item for item in inbound_ids
    ):
        return _runtime_failed(f"{path}.unique_inbound_ids count does not match expected count")
    if len(set(inbound_ids)) != expected_count:
        return _runtime_failed(f"{path}.unique_inbound_ids must be unique")
    tenant_id = batch.get("tenant_id")
    session_id = batch.get("session_id")
    if not isinstance(tenant_id, str) or not tenant_id:
        return _runtime_missing(f"{path}.tenant_id")
    if not isinstance(session_id, str) or not session_id:
        return _runtime_missing(f"{path}.session_id")
    message_order, problem = _runtime_list_field(batch, "message_order", f"{path}.message_order")
    if problem is not None or message_order is None:
        return problem or _runtime_missing(f"{path}.message_order")
    if (
        len(message_order) != expected_count
        or any(_strict_int(item) is None for item in message_order)
        or sorted(cast(list[int], message_order)) != list(range(expected_count))
    ):
        return _runtime_failed(f"{path}.message_order must be one complete acceptance permutation")
    return None


def _validate_runtime_completion(
    phase: Mapping[str, Any], *, path: str, expected_count: int
) -> tuple[str, str] | None:
    completion, problem = _runtime_mapping_field(phase, "completion", f"{path}.completion")
    if problem is not None or completion is None:
        return problem
    _, problem = _runtime_status_field(completion, "status", f"{path}.completion.status")
    if problem is not None:
        return problem
    state, problem = _runtime_mapping_field(completion, "state", f"{path}.completion.state")
    if problem is not None or state is None:
        return problem
    statuses, problem = _runtime_mapping_field(
        state, "inbound_statuses", f"{path}.completion.state.inbound_statuses"
    )
    if problem is not None or statuses is None:
        return problem
    _, problem = _runtime_int_field(
        statuses,
        "committed",
        f"{path}.completion.state.inbound_statuses.committed",
        exact=expected_count,
    )
    if problem is not None:
        return problem
    _, problem = _runtime_int_field(
        state, "turn_count", f"{path}.completion.state.turn_count", exact=expected_count
    )
    if problem is not None:
        return problem
    if state.get("lease_owner_present") is not False:
        return _runtime_failed(f"{path}.completion.state.lease_owner_present is not false")
    if state.get("event_sequences_contiguous") is not True:
        return _runtime_failed(f"{path}.completion.state.event_sequences_contiguous is not true")
    if state.get("scheduler_version") != "v2":
        return _runtime_missing(f"{path}.completion.state.scheduler_version=v2")
    published_outbox, problem = _runtime_int_field(
        state,
        "published_scheduler_outbox",
        f"{path}.completion.state.published_scheduler_outbox",
        minimum=1,
    )
    if problem is not None:
        return problem
    if state.get("published_inbound_outbox") != published_outbox:
        return _runtime_failed(f"{path}.completion.state scheduler outbox aliases differ")
    _, problem = _runtime_int_field(
        state, "event_count", f"{path}.completion.state.event_count", minimum=expected_count
    )
    if problem is not None:
        return problem
    return _validate_runtime_mailbox_state(
        state,
        path=f"{path}.completion.state",
        expected_count=expected_count,
    )


def _validate_runtime_mailbox_state(
    state: Mapping[str, Any], *, path: str, expected_count: int
) -> tuple[str, str] | None:
    mailbox = state.get("mailbox_v2_completion")
    if not isinstance(mailbox, Mapping) or mailbox != state.get("mailbox_v2"):
        return _runtime_missing(f"{path}.mailbox_v2_completion")
    if (
        mailbox.get("status") != "pass"
        or not _schema_version_is(mailbox.get("schema_version"), 2)
        or mailbox.get("mailbox_row_present") is not True
        or mailbox.get("status_value") != "IDLE"
        or mailbox.get("processing_sequence") is not None
        or mailbox.get("completion_verified") is not True
    ):
        return _runtime_failed(f"{path}.mailbox_v2_completion is not settled")
    for key, exact in (
        ("accepted_sequence", expected_count),
        ("resolved_sequence", expected_count),
        ("item_count", expected_count),
        ("resolved_item_count", expected_count),
        ("unresolved_item_count", 0),
    ):
        _, problem = _runtime_int_field(
            mailbox, key, f"{path}.mailbox_v2_completion.{key}", exact=exact
        )
        if problem is not None:
            return problem
    for key in ("queue_generation", "lease_epoch", "published_ready_outbox"):
        _, problem = _runtime_int_field(
            mailbox, key, f"{path}.mailbox_v2_completion.{key}", minimum=1
        )
        if problem is not None:
            return problem
    _, problem = _runtime_int_field(state, "lease_epoch", f"{path}.lease_epoch", minimum=1)
    return problem


def _validate_real_runtime_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Validate the full production scope of a real multi-process runtime run."""

    if not _schema_version_is(report.get("schema_version"), 1):
        return _runtime_missing("schema_version=1")
    if report.get("gate") == "fail":
        return _runtime_failed("top-level gate reported fail")
    if report.get("gate") != "pass":
        return _runtime_missing("top-level gate=pass")
    if report.get("run_id") != evidence.get("run_id"):
        return _runtime_missing("run_id matching evidence.run_id")
    run_id = report.get("run_id")
    run_nonce = report.get("run_nonce")
    if (
        not isinstance(run_id, str)
        or not isinstance(run_nonce, str)
        or re.fullmatch(r"[A-Za-z0-9._:-]{16,128}", run_nonce) is None
        or evidence.get("run_nonce") != run_nonce
        or not _schema_version_is(evidence.get("report_schema_version"), 1)
    ):
        return _runtime_missing("run nonce and schema binding")
    try:
        generated_at = datetime.fromisoformat(
            str(evidence.get("generated_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _runtime_missing("evidence.generated_at")
    if generated_at.tzinfo is None:
        return _runtime_missing("evidence.generated_at")
    try:
        report_generated_at = datetime.fromisoformat(
            str(report.get("generated_at")).replace("Z", "+00:00")
        )
        started_at = datetime.fromisoformat(str(report.get("started_at")).replace("Z", "+00:00"))
        ended_at = datetime.fromisoformat(str(report.get("ended_at")).replace("Z", "+00:00"))
    except ValueError:
        return _runtime_missing("report run timestamps")
    if (
        report.get("generated_at") != evidence.get("generated_at")
        or any(item.tzinfo is None for item in (report_generated_at, started_at, ended_at))
        or not started_at <= report_generated_at <= ended_at
        or ended_at > generated_at + timedelta(seconds=5)
    ):
        return _runtime_missing("report run timestamp ordering")
    recorded_source = report.get("source_fingerprint")
    evidence_source = evidence.get("source_fingerprint")
    if recorded_source != evidence_source:
        return _runtime_missing("report source fingerprint matching evidence")
    candidate, problem = _runtime_mapping_field(report, "candidate", "candidate")
    if problem is not None or candidate is None:
        return problem or _runtime_missing("candidate")
    if candidate.get("mode") != "real_compose_postgresql_redis":
        return _runtime_missing("candidate.mode=real_compose_postgresql_redis")
    if (
        candidate.get("run_id") != run_id
        or candidate.get("run_nonce") != run_nonce
        or candidate.get("generated_at") != report.get("generated_at")
        or candidate.get("started_at") != report.get("started_at")
        or candidate.get("ended_at") != report.get("ended_at")
    ):
        return _runtime_missing("candidate run identity and timestamp binding")

    role_status, role_reason = _role_evidence_check(candidate.get("database_role_evidence"))
    if role_status != "pass":
        if role_status == "fail":
            return _runtime_failed(role_reason or "database role evidence failed")
        return _runtime_missing(role_reason or "candidate.database_role_evidence")

    parameters, problem = _runtime_mapping_field(candidate, "parameters", "candidate.parameters")
    if problem is not None or parameters is None:
        return problem or _runtime_missing("candidate.parameters")
    phase = parameters.get("phase")
    if phase != REAL_RUNTIME_REQUIRED_PHASE:
        return (
            "not_run",
            "real runtime production scope requires "
            f"candidate.parameters.phase={REAL_RUNTIME_REQUIRED_PHASE}",
        )
    for key, minimum in (
        ("workers", REAL_RUNTIME_REQUIRED_WORKERS),
        ("messages", REAL_RUNTIME_REQUIRED_MESSAGES),
        ("duplicates", REAL_RUNTIME_REQUIRED_DUPLICATES),
        ("fault_messages", REAL_RUNTIME_REQUIRED_FAULT_MESSAGES),
    ):
        _, problem = _runtime_int_field(
            parameters,
            key,
            f"candidate.parameters.{key}",
            minimum=minimum,
            scope=True,
        )
        if problem is not None:
            return problem
    bounded_values = {
        "workers": REAL_RUNTIME_MAX_WORKERS,
        "messages": REAL_RUNTIME_MAX_MESSAGES,
        "duplicates": REAL_RUNTIME_MAX_DUPLICATES,
        "fault_messages": REAL_RUNTIME_MAX_FAULT_MESSAGES,
    }
    for key, maximum in bounded_values.items():
        value = _strict_int(parameters.get(key))
        if value is None or value > maximum:
            return _runtime_failed(f"candidate.parameters.{key} exceeds the safety cap")
    timeout_seconds = _strict_number(parameters.get("timeout_seconds"))
    if (
        timeout_seconds is None
        or not math.isfinite(timeout_seconds)
        or timeout_seconds < 1
        or timeout_seconds > REAL_RUNTIME_MAX_TIMEOUT_SECONDS
    ):
        return _runtime_failed("candidate.parameters.timeout_seconds exceeds the safety cap")
    compose_start_mode = parameters.get("compose_start_mode")
    if compose_start_mode not in REAL_RUNTIME_COMPOSE_START_MODES:
        return _runtime_missing(
            "candidate.parameters.compose_start_mode must be gate-owned or wrapper-prestarted-owned"
        )
    for key in ("use_toxiproxy", "kill_worker", "compose_up", "republish_probe"):
        if parameters.get(key) is not True:
            return (
                "not_run",
                f"real runtime production scope requires candidate.parameters.{key}=true",
            )

    worker_count = int(parameters["workers"])
    message_count = int(parameters["messages"])
    duplicate_count = int(parameters["duplicates"])
    fault_message_count = int(parameters["fault_messages"])

    preflight, problem = _runtime_mapping_field(candidate, "preflight", "candidate.preflight")
    if problem is not None or preflight is None:
        return problem or _runtime_missing("candidate.preflight")
    _, problem = _runtime_status_field(preflight, "status", "candidate.preflight.status")
    if problem is not None:
        return problem
    workers, problem = _runtime_list_field(
        preflight, "worker_containers", "candidate.preflight.worker_containers"
    )
    if problem is not None or workers is None:
        return problem or _runtime_missing("candidate.preflight.worker_containers")
    if len(workers) < worker_count:
        return (
            "not_run",
            "real runtime production scope requires at least the requested worker containers",
        )
    evidence_source = evidence.get("source_fingerprint")
    if not isinstance(evidence_source, Mapping):
        return _runtime_missing("evidence.source_fingerprint")
    source_value = evidence_source.get("value")
    worker_ids: list[str] = []
    worker_pids: list[int] = []
    worker_images: set[str] = set()
    runtime_worker_identities: list[dict[str, Any]] = []
    runtime_participating_identities: list[dict[str, Any]] = []
    for worker in workers:
        if (
            not isinstance(worker, Mapping)
            or not isinstance(worker.get("container_id"), str)
            or not worker["container_id"].strip()
        ):
            return _runtime_missing("candidate.preflight.worker_containers")
        worker_ids.append(worker["container_id"])
        if worker.get("status") != "running" or worker.get("health") != "healthy":
            return _runtime_failed("real runtime worker containers are not running and healthy")
        pid = _strict_int(worker.get("pid"))
        image_id = worker.get("image_id")
        connection = worker.get("connection_env")
        if (
            pid is None
            or pid < 2
            or not isinstance(image_id, str)
            or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(image_id.lower()) is None
            or worker.get("source_fingerprint") != source_value
            or not isinstance(connection, Mapping)
            or connection.get("valid") is not True
            or connection.get("role") != "worker"
        ):
            return _runtime_missing("candidate.preflight worker process/image/route identity")
        if not _real_runtime_worker_route_valid(connection):
            return _runtime_missing("candidate.preflight worker trpc_worker Toxiproxy route")
        database_route = connection.get("database")
        redis_route = connection.get("redis")
        if (
            not isinstance(database_route, Mapping)
            or not isinstance(redis_route, Mapping)
            or database_route.get("host") != "toxiproxy"
            or database_route.get("port") != 15432
            or redis_route.get("host") != "toxiproxy"
            or redis_route.get("port") != 16379
        ):
            return _runtime_missing("candidate.preflight worker Toxiproxy routing")
        worker_pids.append(pid)
        worker_images.add(image_id.lower())
        runtime_worker_identities.append(
            {
                "container_id": worker["container_id"],
                "pid": pid,
                "image_id": image_id,
                "source_fingerprint": source_value,
            }
        )
    if len(worker_ids) != len(set(worker_ids)):
        return _runtime_failed("real runtime worker container identities are duplicated")
    if len(worker_pids) != len(set(worker_pids)) or len(worker_images) != 1:
        return _runtime_failed("real runtime worker process/image identities are inconsistent")
    image_attestation, problem = _runtime_mapping_field(
        preflight, "image_attestation", "candidate.preflight.image_attestation"
    )
    if problem is not None or image_attestation is None:
        return problem or _runtime_missing("candidate.preflight.image_attestation")
    if (
        image_attestation.get("status") != "pass"
        or image_attestation.get("worker_count") != len(workers)
        or str(image_attestation.get("image_id", "")).lower() not in worker_images
        or image_attestation.get("source_fingerprint") != source_value
    ):
        return _runtime_missing("candidate.preflight.image_attestation contract")
    participating = preflight.get("participating_services")
    expected_services = (
        "worker",
        "outbox-dispatcher",
        "channel-dispatcher",
        "post-turn-projector",
        "session-recovery",
    )
    if not isinstance(participating, Mapping) or set(participating) != set(expected_services):
        return _runtime_missing("candidate.preflight.participating_services exact inventory")
    all_participating_ids: list[str] = []
    all_participating_pids: list[int] = []
    for service_name in expected_services:
        service_containers = participating.get(service_name)
        if not _is_json_sequence(service_containers) or not service_containers:
            return _runtime_missing(f"candidate.preflight.participating_services.{service_name}")
        service_ids: list[str] = []
        for container in cast(Sequence[Any], service_containers):
            pid = _strict_int(container.get("pid")) if isinstance(container, Mapping) else None
            if (
                not isinstance(container, Mapping)
                or not isinstance(container.get("container_id"), str)
                or not container["container_id"].strip()
                or pid is None
                or pid <= 0
                or container.get("status") != "running"
                or container.get("health") != "healthy"
                or container.get("source_fingerprint") != source_value
                or str(container.get("image_id", "")).lower() not in worker_images
            ):
                return _runtime_missing(
                    f"candidate.preflight.participating_services.{service_name} identity"
                )
            assert pid is not None
            route = container.get("connection_env")
            database_route = route.get("database") if isinstance(route, Mapping) else None
            redis_route = route.get("redis") if isinstance(route, Mapping) else None
            if (
                not isinstance(route, Mapping)
                or route.get("valid") is not True
                or route.get("role") != service_name
                or not isinstance(database_route, Mapping)
                or not isinstance(redis_route, Mapping)
                or database_route.get("host") != "toxiproxy"
                or database_route.get("port") != 15432
                or redis_route.get("host") != "toxiproxy"
                or redis_route.get("port") != 16379
            ):
                return _runtime_missing(
                    f"candidate.preflight.participating_services.{service_name} routing"
                )
            worker_route_valid = _real_runtime_worker_route_valid(route)
            if service_name in REAL_RUNTIME_WORKER_SERVICES and not worker_route_valid:
                return _runtime_missing(
                    f"candidate.preflight.participating_services.{service_name} "
                    "trpc_worker Toxiproxy route"
                )
            service_ids.append(container["container_id"])
            all_participating_ids.append(container["container_id"])
            all_participating_pids.append(pid)
            runtime_participating_identities.append(
                {
                    "role": service_name,
                    "container_id": container["container_id"],
                    "pid": pid,
                    "image_id": container["image_id"],
                    "source_fingerprint": source_value,
                }
            )
        if len(service_ids) != len(set(service_ids)):
            return _runtime_failed(f"participating service {service_name} identities duplicated")
    if len(all_participating_ids) != len(set(all_participating_ids)):
        return _runtime_failed("participating service container identities are globally duplicated")
    if len(all_participating_pids) != len(set(all_participating_pids)):
        return _runtime_failed("participating service process identities are globally duplicated")
    participating_worker_identities = [
        item for item in runtime_participating_identities if item["role"] == "worker"
    ]
    expected_worker_identity_keys = {
        (
            item["container_id"],
            item["pid"],
            item["image_id"],
            item["source_fingerprint"],
        )
        for item in runtime_worker_identities
    }
    actual_worker_identity_keys = {
        (
            item["container_id"],
            item["pid"],
            item["image_id"],
            item["source_fingerprint"],
        )
        for item in participating_worker_identities
    }
    if actual_worker_identity_keys != expected_worker_identity_keys:
        return _runtime_failed("participating worker identity does not match worker preflight")
    participating_workers = cast(Sequence[Any], participating["worker"])
    if {
        item.get("container_id") for item in participating_workers if isinstance(item, Mapping)
    } != set(worker_ids):
        return _runtime_failed("participating worker set does not match preflight worker set")
    runtime_fingerprint = evidence.get("runtime_fingerprint")
    redis_stream = parameters.get("redis_stream")
    redis_group = parameters.get("redis_group")
    if (
        redis_stream != "trpc:session-ready:v2"
        or redis_group != "trpc-session-ready-v2"
        or not _runtime_fingerprint_matches(
            runtime_fingerprint,
            mode=str(candidate.get("mode")),
            worker_identities=runtime_worker_identities,
            participating_identities=runtime_participating_identities,
            stream=redis_stream,
            group=redis_group,
            parameters=parameters,
        )
    ):
        return _runtime_missing(
            "runtime fingerprint bound to participating services and parameters"
        )

    load, problem = _runtime_mapping_field(candidate, "load", "candidate.load")
    if problem is not None or load is None:
        return problem or _runtime_missing("candidate.load")
    _, problem = _runtime_status_field(load, "status", "candidate.load.status")
    if problem is not None:
        return problem
    load_batch, problem = _runtime_mapping_field(load, "batch", "candidate.load.batch")
    if problem is not None or load_batch is None:
        return problem or _runtime_missing("candidate.load.batch")
    problem = _validate_runtime_batch(
        load_batch,
        path="candidate.load.batch",
        expected_count=message_count,
        expected_duplicates=duplicate_count,
    )
    if problem is not None:
        return problem
    problem = _validate_runtime_completion(
        load, path="candidate.load", expected_count=message_count
    )
    if problem is not None:
        return problem
    worker_kill, problem = _runtime_mapping_field(load, "worker_kill", "candidate.load.worker_kill")
    if problem is not None or worker_kill is None:
        return problem or _runtime_missing("candidate.load.worker_kill")
    _, problem = _runtime_status_field(worker_kill, "status", "candidate.load.worker_kill.status")
    if problem is not None:
        return problem
    killed_container_id = worker_kill.get("killed_container_id")
    active_worker_id = worker_kill.get("active_worker_id")
    if not isinstance(killed_container_id, str) or killed_container_id not in worker_ids:
        return _runtime_failed("worker kill target is not one of the attested workers")
    if not isinstance(active_worker_id, str) or not active_worker_id:
        return _runtime_missing("candidate.load.worker_kill.active_worker_id")
    killed_worker = next(
        (
            worker
            for worker in workers
            if isinstance(worker, Mapping) and worker.get("container_id") == killed_container_id
        ),
        None,
    )
    if (
        not isinstance(killed_worker, Mapping)
        or worker_kill.get("killed_container_pid") != killed_worker.get("pid")
        or worker_kill.get("killed_container_image_id") != killed_worker.get("image_id")
        or worker_kill.get("killed_container_source_fingerprint")
        != killed_worker.get("source_fingerprint")
    ):
        return _runtime_failed("worker kill process/image/source identity is not attested")
    try:
        kill_requested_at = datetime.fromisoformat(
            str(worker_kill.get("kill_requested_at")).replace("Z", "+00:00")
        )
        kill_completed_at = datetime.fromisoformat(
            str(worker_kill.get("kill_completed_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _runtime_missing("candidate.load.worker_kill timestamps")
    if (
        kill_requested_at.tzinfo is None
        or kill_completed_at.tzinfo is None
        or not started_at <= kill_requested_at <= kill_completed_at <= ended_at
    ):
        return _runtime_failed("worker kill timestamps are outside the run window")
    _, problem = _runtime_int_field(
        worker_kill,
        "active_turns_observed_before_kill",
        "candidate.load.worker_kill.active_turns_observed_before_kill",
        minimum=1,
    )
    if problem is not None:
        return problem

    fencing, problem = _runtime_mapping_field(
        load, "fencing_takeover", "candidate.load.fencing_takeover"
    )
    if problem is not None or fencing is None:
        return problem or _runtime_missing("candidate.load.fencing_takeover")
    _, problem = _runtime_status_field(fencing, "status", "candidate.load.fencing_takeover.status")
    if problem is not None:
        return problem
    _, problem = _runtime_int_field(
        fencing,
        "attempts_after_takeover",
        "candidate.load.fencing_takeover.attempts_after_takeover",
        minimum=1,
    )
    if problem is not None:
        return problem
    for key in (
        "killed_container_excluded",
        "takeover_observed",
        "takeover_owner_differs",
        "lease_epoch_monotonic",
        "takeover_owner_mapped_to_healthy_survivor",
        "all_survivors_running_healthy",
        "final_commit_contiguous",
    ):
        if fencing.get(key) is not True:
            return _runtime_failed(f"candidate.load.fencing_takeover.{key} is not true")
    killed_owner = fencing.get("killed_owner")
    takeover_owner = fencing.get("takeover_owner")
    if (
        not isinstance(killed_owner, str)
        or not killed_owner
        or killed_owner != active_worker_id
        or not isinstance(takeover_owner, str)
        or not takeover_owner
        or takeover_owner == killed_owner
    ):
        return _runtime_failed("fencing takeover owner identity is invalid")
    epoch_before = _strict_int(fencing.get("lease_epoch_before"))
    epoch_after = _strict_int(fencing.get("lease_epoch_after"))
    if epoch_before is None or epoch_after is None or epoch_after <= epoch_before:
        return _runtime_failed("fencing lease epoch did not increase")
    if (
        worker_kill.get("old_owner") != killed_owner
        or _strict_int(worker_kill.get("old_lease_epoch")) != epoch_before
        or _strict_int(worker_kill.get("old_fencing_token")) != epoch_before
        or worker_kill.get("old_token_rejected") is not True
        or worker_kill.get("new_owner") != takeover_owner
        or _strict_int(worker_kill.get("new_lease_epoch")) != epoch_after
        or fencing.get("old_token_rejected") is not True
        or _strict_int(fencing.get("old_fencing_token")) != epoch_before
        or fencing.get("new_owner") != takeover_owner
        or _strict_int(fencing.get("new_lease_epoch")) != epoch_after
    ):
        return _runtime_failed("worker kill and fencing epoch evidence are not bound")
    survivors = fencing.get("surviving_healthy_worker_containers")
    if (
        not _is_json_sequence(survivors)
        or not survivors
        or killed_container_id in survivors
        or any(not isinstance(item, str) or item not in worker_ids for item in survivors)
    ):
        return _runtime_failed("healthy survivor evidence is invalid")
    stale_token, problem = _runtime_mapping_field(
        fencing,
        "old_token_rejection",
        "candidate.load.fencing_takeover.old_token_rejection",
    )
    if problem is not None or stale_token is None:
        return problem or _runtime_missing("candidate.load.fencing_takeover.old_token_rejection")
    _, problem = _runtime_status_field(
        stale_token,
        "status",
        "candidate.load.fencing_takeover.old_token_rejection.status",
    )
    if problem is not None:
        return problem
    if (
        stale_token.get("fencing_conflict_caught") is not True
        or stale_token.get("owner_still_current") is not True
        or stale_token.get("old_owner") != killed_owner
        or stale_token.get("new_owner") != takeover_owner
        or _strict_int(stale_token.get("old_lease_epoch")) != epoch_before
        or stale_token.get("old_token_rejected") is not True
        or _strict_int(stale_token.get("old_fencing_token")) != epoch_before
        or _strict_int(stale_token.get("new_lease_epoch")) != epoch_after
    ):
        return _runtime_failed("stale fencing token rejection is not bound to the takeover")
    problem = _validate_runtime_markers(
        load,
        path="candidate.load",
        required=REAL_RUNTIME_REQUIRED_LOAD_MARKERS,
        started_at=started_at,
        ended_at=ended_at,
        run_id=run_id,
        run_nonce=run_nonce,
    )
    if problem is not None:
        return problem

    faults, problem = _runtime_mapping_field(candidate, "faults", "candidate.faults")
    if problem is not None or faults is None:
        return problem or _runtime_missing("candidate.faults")
    _, problem = _runtime_status_field(faults, "status", "candidate.faults.status")
    if problem is not None:
        return problem
    toxiproxy, problem = _runtime_mapping_field(faults, "toxiproxy", "candidate.faults.toxiproxy")
    if problem is not None or toxiproxy is None:
        return problem or _runtime_missing("candidate.faults.toxiproxy")
    _, problem = _runtime_status_field(toxiproxy, "status", "candidate.faults.toxiproxy.status")
    if problem is not None:
        return problem
    proxy_names = toxiproxy.get("proxies")
    proxy_details = toxiproxy.get("proxy_details")
    proxy_endpoints = toxiproxy.get("proxy_endpoints")
    api_endpoint = toxiproxy.get("api_endpoint")
    if (
        not _is_json_sequence(proxy_names)
        or not set(REAL_RUNTIME_TOXIPROXY_ENDPOINTS).issubset(set(cast(Sequence[Any], proxy_names)))
        or not isinstance(proxy_details, Mapping)
        or not isinstance(proxy_endpoints, Mapping)
        or not _safe_http_api_endpoint(api_endpoint)
    ):
        return _runtime_missing("candidate.faults.toxiproxy proxy inventory")
    for proxy_name, (listen, upstream) in REAL_RUNTIME_TOXIPROXY_ENDPOINTS.items():
        detail = proxy_details.get(proxy_name)
        endpoint = proxy_endpoints.get(proxy_name)
        if (
            not isinstance(detail, Mapping)
            or detail.get("enabled") is not True
            or not _toxiproxy_listen_matches(listen, detail.get("listen"))
            or detail.get("upstream") != upstream
            or not isinstance(endpoint, Mapping)
            or endpoint.get("api_endpoint") != api_endpoint
            or endpoint.get("enabled") is not True
            or not _toxiproxy_listen_matches(listen, endpoint.get("listen"))
            or endpoint.get("upstream") != upstream
        ):
            return _runtime_failed(f"Toxiproxy {proxy_name} endpoint attestation is invalid")
    fault_batches: dict[str, Mapping[str, Any]] = {}
    for component in ("redis", "postgres"):
        component_report, problem = _runtime_mapping_field(
            faults, component, f"candidate.faults.{component}"
        )
        if problem is not None or component_report is None:
            return problem or _runtime_missing(f"candidate.faults.{component}")
        _, problem = _runtime_status_field(
            component_report, "status", f"candidate.faults.{component}.status"
        )
        if problem is not None:
            return problem
        component_batch, problem = _runtime_mapping_field(
            component_report, "batch", f"candidate.faults.{component}.batch"
        )
        if problem is not None or component_batch is None:
            return problem or _runtime_missing(f"candidate.faults.{component}.batch")
        fault_batches[component] = component_batch
        problem = _validate_runtime_batch(
            component_batch,
            path=f"candidate.faults.{component}.batch",
            expected_count=fault_message_count,
            expected_duplicates=0,
        )
        if problem is not None:
            return problem
        disable, problem = _runtime_mapping_field(
            component_report, "disable", f"candidate.faults.{component}.disable"
        )
        if problem is not None or disable is None:
            return problem or _runtime_missing(f"candidate.faults.{component}.disable")
        enable, problem = _runtime_mapping_field(
            component_report, "enable", f"candidate.faults.{component}.enable"
        )
        if problem is not None or enable is None:
            return problem or _runtime_missing(f"candidate.faults.{component}.enable")
        for action, action_report, expected_enabled in (
            ("disable", disable, False),
            ("enable", enable, True),
        ):
            _, problem = _runtime_status_field(
                action_report,
                "status",
                f"candidate.faults.{component}.{action}.status",
            )
            if problem is not None:
                return problem
            expected_listen, expected_upstream = REAL_RUNTIME_TOXIPROXY_ENDPOINTS[component]
            if (
                action_report.get("api_endpoint") != api_endpoint
                or action_report.get("name") != component
                or action_report.get("enabled") is not expected_enabled
                or not _toxiproxy_listen_matches(expected_listen, action_report.get("listen"))
                or action_report.get("upstream") != expected_upstream
            ):
                return _runtime_failed(f"candidate.faults.{component}.{action} readback is invalid")
        _, problem = _runtime_int_field(
            component_report,
            "uncommitted_while_proxy_down",
            f"candidate.faults.{component}.uncommitted_while_proxy_down",
            minimum=1,
        )
        if problem is not None:
            return problem
        completion, problem = _runtime_mapping_field(
            component_report,
            "completion_after_restore",
            f"candidate.faults.{component}.completion_after_restore",
        )
        if problem is not None or completion is None:
            return problem or _runtime_missing(
                f"candidate.faults.{component}.completion_after_restore"
            )
        _, problem = _runtime_status_field(
            completion, "status", f"candidate.faults.{component}.completion_after_restore.status"
        )
        if problem is not None:
            return problem
        state, problem = _runtime_mapping_field(
            completion,
            "state",
            f"candidate.faults.{component}.completion_after_restore.state",
        )
        if problem is not None or state is None:
            return problem or _runtime_missing(
                f"candidate.faults.{component}.completion_after_restore.state"
            )
        statuses, problem = _runtime_mapping_field(
            state,
            "inbound_statuses",
            f"candidate.faults.{component}.completion_after_restore.state.inbound_statuses",
        )
        if problem is not None or statuses is None:
            return problem or _runtime_missing(
                f"candidate.faults.{component}.completion_after_restore.state.inbound_statuses"
            )
        _, problem = _runtime_int_field(
            statuses,
            "committed",
            f"candidate.faults.{component}.completion_after_restore.state.inbound_statuses.committed",
            exact=fault_message_count,
        )
        if problem is not None:
            return problem
        _, problem = _runtime_int_field(
            state,
            "turn_count",
            f"candidate.faults.{component}.completion_after_restore.state.turn_count",
            exact=fault_message_count,
        )
        if problem is not None:
            return problem
        if state.get("lease_owner_present") is not False:
            return _runtime_failed(
                f"candidate.faults.{component}.completion_after_restore retains a lease owner"
            )
        if state.get("event_sequences_contiguous") is not True:
            return _runtime_failed(
                f"candidate.faults.{component}.completion_after_restore events are not contiguous"
            )
        if state.get("scheduler_version") != "v2":
            return _runtime_missing(
                f"candidate.faults.{component}.completion_after_restore.state.scheduler_version=v2"
            )
        published_outbox, problem = _runtime_int_field(
            state,
            "published_scheduler_outbox",
            f"candidate.faults.{component}.completion_after_restore.state.published_scheduler_outbox",
            minimum=1,
        )
        if problem is not None:
            return problem
        if state.get("published_inbound_outbox") != published_outbox:
            return _runtime_failed(
                "candidate.faults."
                f"{component}.completion_after_restore scheduler outbox aliases differ"
            )
        problem = _validate_runtime_mailbox_state(
            state,
            path=f"candidate.faults.{component}.completion_after_restore.state",
            expected_count=fault_message_count,
        )
        if problem is not None:
            return problem
        if component_report.get("duplicate_turns_verified") is not True:
            return _runtime_failed(
                f"candidate.faults.{component}.duplicate_turns_verified is not true"
            )

    durable_batches = {
        "load": load_batch,
        "redis": fault_batches["redis"],
        "postgres": fault_batches["postgres"],
    }
    seen_inbound_ids: set[str] = set()
    seen_session_ids: set[str] = set()
    for batch_name, durable_batch in durable_batches.items():
        raw_ids = durable_batch.get("unique_inbound_ids")
        session_id = durable_batch.get("session_id")
        if not _is_json_sequence(raw_ids) or not isinstance(session_id, str):
            return _runtime_missing(f"runtime batch {batch_name} durable identities")
        batch_ids = set(cast(Sequence[str], raw_ids))
        if seen_inbound_ids & batch_ids or session_id in seen_session_ids:
            return _runtime_failed(f"runtime batch {batch_name} reuses durable identities")
        seen_inbound_ids.update(batch_ids)
        seen_session_ids.add(session_id)

    for key in ("dlq", "republish_duplicate_publish_probe"):
        nested, problem = _runtime_mapping_field(faults, key, f"candidate.faults.{key}")
        if problem is not None or nested is None:
            return problem or _runtime_missing(f"candidate.faults.{key}")
        _, problem = _runtime_status_field(nested, "status", f"candidate.faults.{key}.status")
        if problem is not None:
            return problem
    dlq = cast(Mapping[str, Any], faults["dlq"])
    dead_letter = dlq.get("dead_letter")
    if (
        not isinstance(dead_letter, Mapping)
        or dead_letter.get("status") != "open"
        or not isinstance(dead_letter.get("source_id"), str)
        or not dead_letter.get("source_id")
        or dlq.get("retry_attempts_increased") is not True
        or dlq.get("terminal_status_open") is not True
        or dlq.get("terminal_path") != "exhausted_retry_terminal_path"
    ):
        return _runtime_failed("candidate.faults.dlq terminal evidence is invalid")
    attempts_before = _strict_int(dlq.get("attempts_before"))
    attempts_after = _strict_int(dlq.get("attempts_after"))
    if attempts_before is None or attempts_after is None or attempts_after <= attempts_before:
        return _runtime_failed("candidate.faults.dlq retry attempts did not increase")
    duplicate_probe = cast(Mapping[str, Any], faults["republish_duplicate_publish_probe"])
    if not isinstance(duplicate_probe.get("duplicate_stream_id"), str) or not duplicate_probe.get(
        "duplicate_stream_id"
    ):
        return _runtime_missing(
            "candidate.faults.republish_duplicate_publish_probe.duplicate_stream_id"
        )
    _, problem = _runtime_int_field(
        duplicate_probe,
        "turn_count",
        "candidate.faults.republish_duplicate_publish_probe.turn_count",
        exact=1,
    )
    if problem is not None:
        return problem
    if duplicate_probe.get("turn_count_exactly_one") is not True:
        return _runtime_failed(
            "candidate.faults.republish_duplicate_publish_probe.turn_count_exactly_one is not true"
        )
    if duplicate_probe.get("pending_duplicate") is not False:
        return _runtime_failed(
            "candidate.faults.republish_duplicate_publish_probe.pending_duplicate is not false"
        )
    redis_batch_ids = fault_batches["redis"].get("unique_inbound_ids")
    if (
        duplicate_probe.get("stream") != parameters.get("redis_stream")
        or duplicate_probe.get("group") != parameters.get("redis_group")
        or not _is_json_sequence(redis_batch_ids)
        or duplicate_probe.get("inbound_id") not in cast(Sequence[Any], redis_batch_ids)
        or duplicate_probe.get("session_id") != fault_batches["redis"].get("session_id")
        or not isinstance(duplicate_probe.get("outbox_id"), str)
        or not duplicate_probe.get("outbox_id")
    ):
        return _runtime_failed("duplicate Redis publish probe is not bound to the fault batch")

    problem = _validate_runtime_markers(
        faults,
        path="candidate.faults",
        required=REAL_RUNTIME_REQUIRED_FAULT_MARKERS,
        started_at=started_at,
        ended_at=ended_at,
        run_id=run_id,
        run_nonce=run_nonce,
    )
    if problem is not None:
        return problem
    return _validate_runtime_markers(
        candidate,
        path="candidate",
        required=(*REAL_RUNTIME_REQUIRED_LOAD_MARKERS, *REAL_RUNTIME_REQUIRED_FAULT_MARKERS),
        started_at=started_at,
        ended_at=ended_at,
        run_id=run_id,
        run_nonce=run_nonce,
    ) or (None, None)


def _k8s_missing(path: str) -> tuple[str, str]:
    return "not_run", f"Kubernetes runtime evidence is missing or invalid {path}"


def _k8s_failed(reason: str) -> tuple[str, str]:
    return "fail", f"Kubernetes runtime acceptance failed: {reason}"


def _k8s_hash(value: Any, path: str) -> tuple[str | None, tuple[str, str] | None]:
    if not isinstance(value, str) or K8S_HASH_RE.fullmatch(value) is None:
        return None, _k8s_missing(path)
    return value, None


def _k8s_candidate_image_digest(candidate: Mapping[str, Any]) -> str | None:
    checks = candidate.get("checks")
    initial = checks.get("initial_image_ids") if isinstance(checks, Mapping) else None
    if not isinstance(initial, Mapping):
        return None
    for values in initial.values():
        if not _is_json_sequence(values):
            continue
        for value in values:
            if isinstance(value, str) and K8S_IMAGE_RE.fullmatch(value.lower()) is not None:
                return value.lower()
    return None


def _validate_k8s_image_evidence_binding(
    checks: Mapping[str, Any],
    canonical_images: Mapping[str, Any],
) -> tuple[str | None, str | None]:
    """Require duplicated check image evidence to equal runtime attestation.

    ``runtime_attestation.image_ids`` is the canonical producer observation.
    The check fields are accepted only when they have the exact same
    deployment keys and digest lists, and when ``changed`` is the exact
    per-deployment comparison derived from those canonical maps.
    """

    expected_deployments = set(K8S_REQUIRED_DEPLOYMENTS)
    canonical: dict[str, Mapping[str, Any]] = {}
    for phase in ("initial", "upgrade"):
        value = canonical_images.get(phase)
        if not isinstance(value, Mapping) or set(value) != expected_deployments:
            return _k8s_missing(f"candidate.runtime_attestation.image_ids.{phase} canonical map")
        canonical[phase] = value

    rolling = checks.get("rolling_upgrade")
    rolling_image_ids = rolling.get("image_ids") if isinstance(rolling, Mapping) else None
    if not isinstance(rolling_image_ids, Mapping):
        return _k8s_missing("candidate.checks.rolling_upgrade.image_ids")

    comparisons = (
        (
            "candidate.checks.initial_image_ids",
            checks.get("initial_image_ids"),
            canonical["initial"],
        ),
        (
            "candidate.checks.rolling_upgrade.image_ids.initial",
            rolling_image_ids.get("initial"),
            canonical["initial"],
        ),
        (
            "candidate.checks.rolling_upgrade.image_ids.upgrade",
            rolling_image_ids.get("upgrade"),
            canonical["upgrade"],
        ),
    )
    for label, observed, expected in comparisons:
        if not isinstance(observed, Mapping) or set(observed) != expected_deployments:
            return _k8s_missing(f"{label} deployment set does not match canonical image map")
        if observed != expected:
            return _k8s_failed(f"{label} does not match canonical image map")

    changed = rolling_image_ids.get("changed")
    expected_changed = {
        deployment: canonical["initial"][deployment] != canonical["upgrade"][deployment]
        for deployment in expected_deployments
    }
    if not isinstance(changed, Mapping) or set(changed) != expected_deployments:
        return _k8s_missing("candidate.checks.rolling_upgrade.image_ids.changed deployment set")
    if any(not isinstance(value, bool) for value in changed.values()):
        return _k8s_missing("candidate.checks.rolling_upgrade.image_ids.changed booleans")
    if dict(changed) != expected_changed:
        return _k8s_failed(
            "candidate.checks.rolling_upgrade.image_ids.changed does not match canonical image map"
        )
    return None, None


def _current_k8s_hpa_driver_sha256() -> tuple[str | None, str | None]:
    """Hash the exact bounded Job driver in this checkout.

    A report cannot promote a modified or missing driver by carrying the hash
    produced by an older run.  Symlink/path escape is rejected before reading.
    """

    path = ROOT / K8S_HPA_DRIVER_RELATIVE_PATH
    scripts_root = (ROOT / "scripts").resolve()
    if path.is_symlink() or any(parent.is_symlink() for parent in path.parents):
        return None, "Kubernetes HPA driver path must not use symlinks"
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(scripts_root)
        stat = resolved.stat()
        if not resolved.is_file() or stat.st_size <= 0 or stat.st_size > K8S_HPA_DRIVER_MAX_BYTES:
            return None, "Kubernetes HPA driver is not a bounded regular file"
        return hashlib.sha256(resolved.read_bytes()).hexdigest(), None
    except (OSError, ValueError):
        return None, "Kubernetes HPA driver is missing or outside scripts/"


def _validate_k8s_hpa_job_evidence(
    observed: Mapping[str, Any], *, namespace: str, nonce: str, cluster_fingerprint: str
) -> tuple[str | None, str | None]:
    evidence = observed.get("driver_evidence")
    if not isinstance(evidence, Mapping):
        return _k8s_missing("candidate.checks.hpa_load_observation.driver_evidence")
    expected_name = f"trpc-hpa-load-{nonce[:20]}"
    load = evidence.get("load")
    clear = evidence.get("clear")
    if not isinstance(load, Mapping) or not isinstance(clear, Mapping):
        return _k8s_missing("candidate.checks.hpa_load_observation.driver_evidence load/clear")
    load_uid = load.get("job_uid")
    clear_uid = clear.get("job_uid")
    for phase, value in (("load", load), ("clear", clear)):
        if value.get("api_observed") is not True:
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase}.api_observed"
            )
        if value.get("namespace") != namespace or value.get("run_nonce") != nonce:
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase} nonce/namespace"
            )
        if value.get("cluster_fingerprint") != cluster_fingerprint:
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase}.cluster"
            )
        if value.get("job_name") != expected_name:
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase}.job_name"
            )
        uid = value.get("job_uid")
        if not isinstance(uid, str) or K8S_HPA_JOB_UID_RE.fullmatch(uid) is None:
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase}.job_uid"
            )
        labels = value.get("job_labels")
        if (
            not isinstance(labels, Mapping)
            or labels.get("trpc.io/hpa-gate") != "bounded-job-driver"
            or labels.get("trpc.io/hpa-run") != nonce
            or labels.get("trpc.io/hpa-phase") != "load"
            or labels.get("trpc.io/hpa-cluster") != cluster_fingerprint[:63]
        ):
            return _k8s_missing(
                f"candidate.checks.hpa_load_observation.driver_evidence.{phase}.labels"
            )
    if load_uid != clear_uid:
        return _k8s_failed("HPA load and clear Job API evidence used different UIDs")
    if clear.get("job_deleted") is not True:
        return _k8s_missing("candidate.checks.hpa_load_observation.driver_evidence.clear deletion")
    return None, None


def _validate_k8s_hpa_observation(
    check: Mapping[str, Any], *, namespace: str, nonce: str, cluster_fingerprint: str
) -> tuple[str | None, str | None]:
    if check.get("status") != "pass" or check.get("observed_live") is not True:
        return _k8s_missing("candidate.checks.hpa_load_observation live status")
    observed = check.get("observation")
    if not isinstance(observed, Mapping):
        return _k8s_missing("candidate.checks.hpa_load_observation.observation")
    expected = {
        "source": "kubectl_api",
        "hpa_name": "trpc-worker",
        "metric_name": "trpc_session_ready_backlog",
        "namespace": namespace,
        "run_nonce": nonce,
    }
    if any(observed.get(key) != value for key, value in expected.items()):
        return _k8s_missing("candidate.checks.hpa_load_observation observation binding")
    identity = observed.get("cluster_identity")
    trigger = observed.get("trigger")
    if (
        not isinstance(identity, Mapping)
        or identity.get("fingerprint_sha256") != cluster_fingerprint
    ):
        return _k8s_missing("candidate.checks.hpa_load_observation cluster binding")
    if (
        not isinstance(trigger, Mapping)
        or trigger.get("kind") != "controlled_backlog"
        or trigger.get("source") != "bounded-driver"
    ):
        return _k8s_missing("candidate.checks.hpa_load_observation controlled trigger")
    job_problem = _validate_k8s_hpa_job_evidence(
        observed,
        namespace=namespace,
        nonce=nonce,
        cluster_fingerprint=cluster_fingerprint,
    )
    if job_problem != (None, None):
        return job_problem
    numeric: dict[str, float] = {}
    for phase in ("before", "during", "after"):
        value = observed.get(phase)
        if not isinstance(value, Mapping):
            return _k8s_missing(f"candidate.checks.hpa_load_observation.{phase}")
        for key in ("metric_value", "desired_replicas", "current_replicas", "ready_replicas"):
            number = _strict_number(value.get(key))
            if number is None or number < 0:
                return _k8s_missing(f"candidate.checks.hpa_load_observation.{phase}.{key}")
            numeric[f"{phase}.{key}"] = number
    if numeric["during.metric_value"] <= numeric["before.metric_value"]:
        return _k8s_failed("HPA backlog metric did not increase under controlled load")
    if numeric["during.desired_replicas"] <= numeric["before.desired_replicas"]:
        return _k8s_failed("HPA desired replicas did not scale up")
    if (
        numeric["during.current_replicas"] < numeric["during.desired_replicas"]
        or numeric["during.ready_replicas"] < numeric["during.desired_replicas"]
    ):
        return _k8s_failed("HPA scale-up replicas were not current and ready")
    if numeric["after.metric_value"] >= numeric["during.metric_value"]:
        return _k8s_failed("HPA backlog metric did not fall after load removal")
    if (
        numeric["after.desired_replicas"] > numeric["before.desired_replicas"]
        or numeric["after.current_replicas"] > numeric["during.current_replicas"]
    ):
        return _k8s_failed("HPA replicas did not scale down after load removal")
    if numeric["after.ready_replicas"] < numeric["after.desired_replicas"]:
        return _k8s_failed("HPA scale-down replicas were not ready")
    for key in ("scale_up_timeout_seconds", "scale_down_timeout_seconds"):
        timeout = _strict_number(observed.get(key))
        if timeout is None or timeout <= 0 or timeout > 3600:
            return _k8s_missing(f"candidate.checks.hpa_load_observation.{key}")
    return None, None


def _validate_kubernetes_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Fail closed unless the complete live Kubernetes runtime is evidenced."""

    if report.get("gate") == "fail":
        return _k8s_failed("top-level gate reported fail")
    if report.get("gate") != "pass":
        return _k8s_missing("top-level gate=pass")
    if not _schema_version_is(report.get("schema_version"), 1):
        return _k8s_missing("schema_version=1")
    run_id = evidence.get("run_id")
    if not isinstance(run_id, str) or not run_id or report.get("run_id") != run_id:
        return _k8s_missing("run_id matching evidence.run_id")
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return _k8s_missing("candidate")
    if (
        candidate.get("mode") != "live_kubernetes_control_plane"
        or candidate.get("enabled") is not True
    ):
        return _k8s_missing("candidate live mode/enabled")
    topology = candidate.get("topology")
    if not isinstance(topology, Mapping):
        return _k8s_missing("candidate.topology")
    if (
        topology.get("scope") != K8S_RUNTIME_SCOPE
        or topology.get("im_deployment") != K8S_IM_DEPLOYMENT
        or topology.get("production_deployments") != list(K8S_PRODUCTION_DEPLOYMENTS)
        or topology.get("tested_deployments") != list(K8S_REQUIRED_DEPLOYMENTS)
        or topology.get("provider_disabled_deployments") != list(K8S_PROVIDER_DISABLED_DEPLOYMENTS)
    ):
        return _k8s_failed(
            "ACK runtime topology is not the reviewed unified-cluster runtime topology"
        )
    namespace = candidate.get("namespace")
    nonce = candidate.get("run_nonce")
    if (
        not isinstance(namespace, str)
        or re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", namespace) is None
    ):
        return _k8s_missing("candidate.namespace")
    if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        return _k8s_missing("candidate.run_nonce")
    checks = candidate.get("checks")
    if not isinstance(checks, Mapping):
        return _k8s_missing("candidate.checks")
    for name in K8S_REQUIRED_CHECKS:
        value = checks.get(name)
        if not isinstance(value, Mapping) or value.get("status") != "pass":
            return _k8s_missing(f"candidate.checks.{name}.status=pass")
    driver_trust = checks.get("hpa_driver_trust")
    if (
        not isinstance(driver_trust, Mapping)
        or driver_trust.get("dedicated_kubeconfig") is not True
        or driver_trust.get("scope") != "namespace_jobs_only"
        or driver_trust.get("rbac_verified") is not True
        or driver_trust.get("reasons") != []
    ):
        return _k8s_missing("candidate.checks.hpa_driver_trust least privilege binding")
    for field in ("driver_sha256", "kubeconfig_sha256", "subject_sha256"):
        value = driver_trust.get(field)
        if (
            not isinstance(value, str)
            or K8S_HASH_RE.fullmatch(value) is None
            or value in {"0" * 64, "f" * 64}
        ):
            return _k8s_missing(f"candidate.checks.hpa_driver_trust.{field}")
    current_driver_sha256, driver_error = _current_k8s_hpa_driver_sha256()
    if current_driver_sha256 is None:
        return _k8s_missing(driver_error or "current Kubernetes HPA driver could not be hashed")
    if driver_trust.get("driver_sha256") != current_driver_sha256:
        return _k8s_failed("Kubernetes HPA driver digest does not match the current checkout")
    for field in ("driver_context_sha256", "cluster_fingerprint_sha256"):
        value = driver_trust.get(field)
        if not isinstance(value, str) or K8S_HASH_RE.fullmatch(value) is None:
            return _k8s_missing(f"candidate.checks.hpa_driver_trust.{field}")
    if (
        driver_trust.get("identity_verified") is not True
        or driver_trust.get("cluster_fingerprint_sha256") is None
    ):
        return _k8s_missing("candidate.checks.hpa_driver_trust verified identity")
    rule_audit = driver_trust.get("rule_audit")
    if (
        not isinstance(rule_audit, Mapping)
        or rule_audit.get("complete") is not True
        or rule_audit.get("scope") != "target_namespace_jobs_pods_only"
        or rule_audit.get("target_namespace") != namespace
    ):
        return _k8s_missing("candidate.checks.hpa_driver_trust complete SelfSubjectRulesReview")
    for field in (
        "target_rules_sha256",
        "default_rules_sha256",
        "kube_system_rules_sha256",
        "cluster_rules_sha256",
    ):
        value = rule_audit.get(field)
        if not isinstance(value, str) or K8S_HASH_RE.fullmatch(value) is None:
            return _k8s_missing(f"candidate.checks.hpa_driver_trust.rule_audit.{field}")
    deltas = report.get("case_deltas")
    if (
        not isinstance(deltas, Mapping)
        or deltas.get("failed_checks") != 0
        or deltas.get("not_run_checks") != 0
    ):
        return _k8s_missing("case_deltas zero failed/not_run checks")
    baseline = report.get("baseline")
    if (
        not isinstance(baseline, Mapping)
        or list(baseline.get("required_checks", ())) != list(K8S_REQUIRED_CHECKS)
        or list(baseline.get("required_runtime_actions", ())) != list(K8S_REQUIRED_ACTIONS)
    ):
        return _k8s_missing("baseline required checks/actions")
    hpa_policy = baseline.get("hpa_load_policy")
    if (
        not isinstance(hpa_policy, Mapping)
        or hpa_policy.get("metric") != "trpc_session_ready_backlog"
        or hpa_policy.get("required_phases") != ["before", "during", "after"]
    ):
        return _k8s_missing("baseline.hpa_load_policy")
    attestation = candidate.get("runtime_attestation")
    if (
        not isinstance(attestation, Mapping)
        or attestation.get("status") != "pass"
        or attestation.get("namespace_isolated") is not True
    ):
        return _k8s_missing("candidate.runtime_attestation status/namespace isolation")
    if attestation.get("namespace") != namespace:
        return _k8s_missing("candidate.runtime_attestation namespace binding")
    if attestation.get("run_nonce") != nonce:
        return _k8s_missing("candidate.runtime_attestation run_nonce binding")
    cluster = attestation.get("cluster_identity")
    if not isinstance(cluster, Mapping) or cluster.get("server_observed") is not True:
        return _k8s_missing("candidate.runtime_attestation.cluster_identity")
    cluster_fp, problem = _k8s_hash(
        cluster.get("fingerprint_sha256"),
        "candidate.runtime_attestation.cluster_identity.fingerprint_sha256",
    )
    if problem is not None or cluster_fp is None:
        return problem or _k8s_missing(
            "candidate.runtime_attestation.cluster_identity.fingerprint_sha256"
        )
    _, problem = _k8s_hash(
        cluster.get("context_sha256"),
        "candidate.runtime_attestation.cluster_identity.context_sha256",
    )
    if problem is not None:
        return problem
    controlled_node = candidate.get("controlled_node")
    node = attestation.get("node_identity")
    if (
        not isinstance(controlled_node, Mapping)
        or not isinstance(node, Mapping)
        or node.get("fingerprint_sha256") != controlled_node.get("fingerprint_sha256")
    ):
        return _k8s_missing("candidate controlled node identity binding")
    _, problem = _k8s_hash(
        node.get("fingerprint_sha256"),
        "candidate.runtime_attestation.node_identity.fingerprint_sha256",
    )
    if problem is not None:
        return problem
    actions = attestation.get("actions")
    if not isinstance(actions, Mapping) or any(
        actions.get(name) is not True for name in K8S_REQUIRED_ACTIONS
    ):
        return _k8s_missing("candidate.runtime_attestation.actions")
    if (
        attestation.get("eviction_scope") != "namespace_pod_eviction+controlled_node"
        or attestation.get("node_eviction_status") != "pass"
    ):
        return _k8s_missing("candidate.runtime_attestation eviction scope/status")
    hpa_problem = _validate_k8s_hpa_observation(
        cast(Mapping[str, Any], checks["hpa_load_observation"]),
        namespace=namespace,
        nonce=nonce,
        cluster_fingerprint=cluster_fp,
    )
    if hpa_problem != (None, None):
        return hpa_problem
    if driver_trust.get("cluster_fingerprint_sha256") != cluster_fp:
        return _k8s_missing("candidate.checks.hpa_driver_trust cluster fingerprint binding")
    images = attestation.get("image_ids")
    if (
        not isinstance(images, Mapping)
        or not isinstance(images.get("initial"), Mapping)
        or not isinstance(images.get("upgrade"), Mapping)
    ):
        return _k8s_missing("candidate.runtime_attestation.image_ids")
    initial = cast(Mapping[str, Any], images["initial"])
    upgrade = cast(Mapping[str, Any], images["upgrade"])
    for label, collection in (("initial", initial), ("upgrade", upgrade)):
        if set(collection) != set(K8S_REQUIRED_DEPLOYMENTS):
            return _k8s_missing(f"candidate.runtime_attestation.image_ids.{label} deployments")
        for deployment, ids in collection.items():
            if (
                not _is_json_sequence(ids)
                or len(ids) != 1
                or any(
                    not isinstance(item, str) or K8S_IMAGE_RE.fullmatch(item.lower()) is None
                    for item in ids
                )
            ):
                return _k8s_missing(f"candidate.runtime_attestation.image_ids.{label}.{deployment}")
    for deployment in K8S_REQUIRED_DEPLOYMENTS:
        initial_id = cast(list[str], initial[deployment])[0]
        upgrade_id = cast(list[str], upgrade[deployment])[0]
        if initial_id == upgrade_id:
            return _k8s_failed(f"rolling upgrade image ID did not change for {deployment}")
    image_problem = _validate_k8s_image_evidence_binding(checks, images)
    if image_problem != (None, None):
        return image_problem
    image_digest = _k8s_candidate_image_digest(candidate)
    lineage = candidate.get("lineage")
    if (
        not isinstance(lineage, Mapping)
        or lineage.get("status") != "pass"
        or lineage.get("checkout_current") is not True
        or lineage.get("producer") != PRODUCTION_EVIDENCE_PRODUCERS["kubernetes-runtime.json"]
        or not isinstance(image_digest, str)
        or lineage.get("image_digest") != image_digest
        or K8S_IMAGE_RE.fullmatch(image_digest) is None
    ):
        return _k8s_missing("candidate.lineage current image binding")
    runtime = evidence.get("runtime_fingerprint")
    expected_parameters = {
        "required_checks": len(K8S_REQUIRED_CHECKS),
        "image_digest": image_digest,
    }
    if not _runtime_fingerprint_matches(
        runtime,
        mode="kubernetes_runtime",
        worker_identities=[image_digest],
        stream="trpc:session-ready:v2",
        group="trpc-session-ready-v2",
        parameters=expected_parameters,
    ):
        return _k8s_missing("evidence.runtime_fingerprint binding")
    node_check = cast(Mapping[str, Any], checks["node_eviction"])
    preflight = node_check.get("preflight")
    drain = node_check.get("drain")
    if (
        not isinstance(preflight, Mapping)
        or preflight.get("node_label_verified") is not True
        or preflight.get("node_ready") is not True
        or preflight.get("node_schedulable") is not True
    ):
        return _k8s_missing("candidate.checks.node_eviction.preflight dedicated node")
    if not isinstance(drain, Mapping):
        return _k8s_missing("candidate.checks.node_eviction.drain")
    for key in ("cordon", "drain", "uncordon"):
        value = drain.get(key)
        if not isinstance(value, Mapping) or value.get("status") != "pass":
            return _k8s_missing(f"candidate.checks.node_eviction.drain.{key}")
    post_cordon = drain.get("post_cordon_preflight")
    post_drain = drain.get("post_drain")
    if (
        not isinstance(post_cordon, Mapping)
        or post_cordon.get("node_label_verified") is not True
        or post_cordon.get("node_ready") is not True
        or not isinstance(post_cordon.get("gate_namespace_pod_count"), int)
        or post_cordon.get("gate_namespace_pod_count", 0) < 1
    ):
        return _k8s_missing("candidate.checks.node_eviction.drain.post_cordon_preflight")
    if not isinstance(post_drain, Mapping) or post_drain.get("node_cordoned") is not True:
        return _k8s_missing("candidate.checks.node_eviction.drain.post_drain")
    post_ready = node_check.get("post_drain_readiness")
    if (
        not isinstance(post_ready, Mapping)
        or set(post_ready) != set(K8S_REQUIRED_DEPLOYMENTS)
        or any(
            not isinstance(value, Mapping) or value.get("status") != "pass"
            for value in post_ready.values()
        )
    ):
        return _k8s_missing("candidate.checks.node_eviction.post_drain_readiness")
    rolling = checks.get("rolling_upgrade")
    if (
        not isinstance(rolling, Mapping)
        or rolling.get("status") != "pass"
        or rolling.get("upgrade_image_supplied") is not True
    ):
        return _k8s_missing("candidate.checks.rolling_upgrade")
    return None, None


def _migration_missing(path: str) -> tuple[str, str]:
    return "not_run", f"real migration evidence is missing or invalid {path}"


def _migration_failed(reason: str) -> tuple[str, str]:
    return "fail", f"real migration acceptance failed: {reason}"


def _validate_migration_sha(value: Any, path: str) -> tuple[str | None, tuple[str, str] | None]:
    if not isinstance(value, str) or MIGRATION_SHA256_RE.fullmatch(value) is None:
        return None, _migration_missing(path)
    return value, None


def _validate_real_migration_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Validate an operator-confirmed, complete live migration report.

    A current-candidate envelope proves the checkout, not the migration.  The
    second evidence object therefore binds one source snapshot and immutable
    manifest to every phase, requires an observable control state, and proves
    that the approval was generated by the live producer.  This deliberately
    rejects a report made production-looking by editing JSON by hand.
    """

    if not _schema_version_is(report.get("schema_version"), 1):
        return _migration_missing("schema_version=1")
    # The former ``candidate``-only migration report was an offline/partial
    # contract.  It is intentionally never accepted as production evidence;
    # only the current migration_evidence envelope can reach this gate.
    if not isinstance(report.get("migration_evidence"), Mapping):
        return _migration_missing("migration_evidence (candidate fallback is not accepted)")
    if report.get("gate") == "fail":
        return _migration_failed("top-level gate reported fail")
    if report.get("gate") != "pass":
        return _migration_missing("top-level gate=pass")
    migration = report.get("migration_evidence")
    if not isinstance(migration, Mapping):
        return _migration_missing("migration_evidence")
    if migration.get("status") != "pass":
        return _migration_missing("migration_evidence.status=pass")
    if migration.get("scope") != "production":
        return _migration_missing("migration_evidence.scope=production")
    if migration.get("is_simulation") is True or migration.get("is_test") is True:
        return _migration_missing("migration_evidence tenant is marked simulation/test")
    run_id = evidence.get("run_id")
    if migration.get("run_id") != run_id or report.get("run_id") != run_id:
        return _migration_missing("run_id matching evidence.run_id")
    try:
        run_started_at = datetime.fromisoformat(
            str(migration.get("run_started_at")).replace("Z", "+00:00")
        )
        run_finished_at = datetime.fromisoformat(
            str(migration.get("run_finished_at")).replace("Z", "+00:00")
        )
        evidence_generated_at = datetime.fromisoformat(
            str(evidence.get("generated_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _migration_missing("migration_evidence run time window")
    if (
        run_started_at.tzinfo is None
        or run_finished_at.tzinfo is None
        or evidence_generated_at.tzinfo is None
        or run_started_at > run_finished_at
        or abs((evidence_generated_at - run_finished_at).total_seconds()) > 1
    ):
        return _migration_failed("migration_evidence run time window is invalid")

    source = migration.get("source")
    target = migration.get("target")
    manifest = migration.get("manifest")
    if not isinstance(source, Mapping):
        return _migration_missing("migration_evidence.source")
    if not isinstance(target, Mapping):
        return _migration_missing("migration_evidence.target")
    if not isinstance(manifest, Mapping):
        return _migration_missing("migration_evidence.manifest")
    if source.get("kind") != "redis":
        return _migration_missing("migration_evidence.source.kind=redis")
    if target.get("kind") != "postgresql":
        return _migration_missing("migration_evidence.target.kind=postgresql")
    for name, backend in (("source", source), ("target", target)):
        if backend.get("is_real") is not True:
            return _migration_missing(f"migration_evidence.{name}.is_real=true")
    source_endpoint, problem = _validate_migration_sha(
        source.get("endpoint_sha256"), "migration_evidence.source.endpoint_sha256"
    )
    if problem is not None:
        return problem
    target_endpoint, problem = _validate_migration_sha(
        target.get("endpoint_sha256"), "migration_evidence.target.endpoint_sha256"
    )
    if problem is not None:
        return problem
    if source_endpoint == target_endpoint:
        return _migration_failed("source and target endpoint identities are identical")
    target_count, problem = _int_field(
        target, "target_count", "migration_evidence.target.target_count", minimum=1
    )
    if problem is not None or target_count is None:
        return problem or _migration_missing("migration_evidence.target.target_count")
    target_checksum, problem = _validate_migration_sha(
        target.get("target_checksum"), "migration_evidence.target.target_checksum"
    )
    if problem is not None or target_checksum is None:
        return problem or _migration_missing("migration_evidence.target.target_checksum")
    source_snapshot_id = source.get("snapshot_id")
    if not isinstance(source_snapshot_id, str) or not source_snapshot_id:
        return _migration_missing("migration_evidence.source.snapshot_id")
    source_count, problem = _int_field(
        source, "source_count", "migration_evidence.source.source_count", minimum=1
    )
    if problem is not None or source_count is None:
        return problem or _migration_missing("migration_evidence.source.source_count")
    source_checksum, problem = _validate_migration_sha(
        source.get("source_checksum"), "migration_evidence.source.source_checksum"
    )
    if problem is not None or source_checksum is None:
        return problem or _migration_missing("migration_evidence.source.source_checksum")

    required_manifest = (
        "tenant_id",
        "migration_id",
        "source_kind",
        "kinds",
        "source_snapshot_id",
        "source_count",
        "source_checksum",
        "app_id",
        "app_revision",
        "config_version",
        "binding_id",
        "binding_revision",
    )
    if any(not isinstance(manifest.get(key), (str, int, list, tuple)) for key in required_manifest):
        return _migration_missing("migration_evidence.manifest fields")
    for key in ("tenant_id", "migration_id", "app_id", "binding_id"):
        if not isinstance(manifest.get(key), str) or not manifest[key].strip():
            return _migration_missing(f"migration_evidence.manifest.{key}")
    tenant_id = str(manifest["tenant_id"]).lower()
    if any(marker in tenant_id.split("-") for marker in MIGRATION_PRODUCTION_TENANT_MARKERS):
        return _migration_missing("migration_evidence.manifest.tenant_id production scope")
    if manifest.get("source_kind") != "redis":
        return _migration_missing("migration_evidence.manifest.source_kind=redis")
    kinds = manifest.get("kinds")
    if not _is_json_sequence(kinds):
        return _migration_missing("migration_evidence.manifest.kinds")
    kind_values = list(cast(Sequence[Any], kinds))
    if (
        not kind_values
        or any(not isinstance(kind, str) for kind in kind_values)
        or len(set(kind_values)) != len(kind_values)
        or set(kind_values) - {"session", "memory"}
    ):
        return _migration_missing("migration_evidence.manifest.kinds")
    for key in ("app_revision", "config_version", "binding_revision"):
        value, problem = _int_field(manifest, key, f"migration_evidence.manifest.{key}", minimum=1)
        if problem is not None or value is None:
            return problem or _migration_missing(f"migration_evidence.manifest.{key}")
    manifest_snapshot = manifest.get("source_snapshot_id")
    if manifest_snapshot != source_snapshot_id:
        return _migration_failed("manifest source snapshot does not match source evidence")
    manifest_count, problem = _int_field(
        manifest, "source_count", "migration_evidence.manifest.source_count", minimum=1
    )
    if problem is not None or manifest_count is None:
        return problem or _migration_missing("migration_evidence.manifest.source_count")
    manifest_checksum, problem = _validate_migration_sha(
        manifest.get("source_checksum"), "migration_evidence.manifest.source_checksum"
    )
    if problem is not None or manifest_checksum is None:
        return problem or _migration_missing("migration_evidence.manifest.source_checksum")
    if manifest_count != source_count or manifest_checksum != source_checksum:
        return _migration_failed("manifest and source snapshot counts/checksums differ")
    if target_count != source_count or target_checksum != source_checksum:
        return _migration_failed("target snapshot counts/checksums differ from source")
    top_deltas = report.get("case_deltas")
    preflight = migration.get("target_empty_preflight")
    if not isinstance(preflight, Mapping) and isinstance(top_deltas, Mapping):
        preflight = top_deltas.get("target_empty_preflight")
    if not isinstance(preflight, Mapping):
        return _migration_missing("target_empty_preflight")
    if preflight.get("tenant_id") != manifest.get("tenant_id"):
        return _migration_missing("target_empty_preflight.tenant_id")
    non_empty = preflight.get("non_empty_tables")
    table_counts = preflight.get("table_counts")
    checked_tables = preflight.get("checked_tables")
    checked_table_values = (
        list(cast(Sequence[Any], checked_tables)) if _is_json_sequence(checked_tables) else None
    )
    non_empty_values = (
        list(cast(Sequence[Any], non_empty)) if _is_json_sequence(non_empty) else None
    )
    if (
        checked_table_values != list(MIGRATION_TARGET_EMPTY_TABLES)
        or non_empty_values is None
        or any(not isinstance(table, str) for table in non_empty_values)
        or len(non_empty_values)
        != len({table for table in non_empty_values if isinstance(table, str)})
        or set(non_empty_values) - set(MIGRATION_TARGET_EMPTY_TABLES)
        or not isinstance(table_counts, Mapping)
        or set(table_counts) != set(MIGRATION_TARGET_EMPTY_TABLES)
        or preflight.get("empty") is not True
    ):
        return _migration_failed("target tenant was not empty before migration")
    if non_empty_values:
        return _migration_failed("target tenant was not empty before migration")
    normalized_table_counts = [_strict_int(value) for value in table_counts.values()]
    if any(value is None or value < 0 for value in normalized_table_counts):
        return _migration_failed("target empty preflight table counts are invalid")
    if any(value != 0 for value in normalized_table_counts):
        return _migration_failed("target tenant was not empty before migration")

    phases = migration.get("phases")
    if not isinstance(phases, Mapping):
        return _migration_missing("migration_evidence.phases")
    # Rollback must be an observed phase of this run.  A boolean
    # ``rollback_supported`` capability is not evidence that rollback was
    # actually executed and verified.
    execution_phases = MIGRATION_REQUIRED_PHASES
    missing_phases = [phase for phase in execution_phases if phase not in phases]
    if missing_phases:
        return _migration_missing("migration_evidence.phases missing " + ",".join(missing_phases))
    expected_control = {
        "prepare": (False, "source", False, False, "ready"),
        "backfill": (False, "source", False, False, "ready"),
        "shadow-read": (False, "source", False, False, "ready"),
        "dual-write": (True, "source", False, False, "dual-write"),
        "cutover": (True, "target", False, False, "target"),
        "verify": (True, "target", False, False, "target"),
        "cleanup": (False, "target", True, False, "target"),
        "rollback": (False, "source", False, True, "source"),
    }
    previous_phase_completed_at = run_started_at
    for phase in execution_phases:
        phase_record = phases.get(phase)
        if not isinstance(phase_record, Mapping):
            return _migration_missing(f"migration_evidence.phases.{phase}")
        if (
            phase_record.get("tenant_id") != manifest.get("tenant_id")
            or phase_record.get("migration_id") != manifest.get("migration_id")
            or phase_record.get("run_id") != run_id
        ):
            return _migration_missing(f"migration_evidence.phases.{phase} tenant/run binding")
        if phase_record.get("gate") != "pass":
            return _migration_missing(f"migration_evidence.phases.{phase}.gate=pass")
        if (
            phase_record.get("phase") != phase
            or phase_record.get("source_snapshot_id") != source_snapshot_id
            or _strict_int(phase_record.get("source_count")) != source_count
            or phase_record.get("source_checksum") != source_checksum
        ):
            return _migration_failed(
                f"migration_evidence.phases.{phase} source snapshot binding is invalid"
            )
        try:
            phase_started_at = datetime.fromisoformat(
                str(phase_record.get("started_at")).replace("Z", "+00:00")
            )
            phase_completed_at = datetime.fromisoformat(
                str(phase_record.get("completed_at")).replace("Z", "+00:00")
            )
        except ValueError:
            return _migration_missing(f"migration_evidence.phases.{phase} time window")
        if (
            phase_started_at.tzinfo is None
            or phase_completed_at.tzinfo is None
            or not run_started_at
            <= previous_phase_completed_at
            <= phase_started_at
            <= phase_completed_at
            <= run_finished_at
        ):
            return _migration_failed(f"migration_evidence.phases.{phase} time window is invalid")
        previous_phase_completed_at = phase_completed_at
        case_deltas = phase_record.get("case_deltas")
        if not isinstance(case_deltas, Mapping):
            return _migration_missing(f"migration_evidence.phases.{phase}.case_deltas")
        _, problem = _validate_migration_sha(
            case_deltas.get("checksum"), f"migration_evidence.phases.{phase}.case_deltas.checksum"
        )
        if problem is not None:
            return problem
        differences = case_deltas.get("differences")
        if not _is_json_sequence(differences):
            return _migration_missing(f"migration_evidence.phases.{phase}.case_deltas.differences")
        if differences:
            return _migration_failed(f"migration phase {phase} reported differences")
        for key in ("source_count", "target_count"):
            value, problem = _int_field(
                case_deltas,
                key,
                f"migration_evidence.phases.{phase}.case_deltas.{key}",
                exact=source_count,
            )
            if problem is not None or value is None:
                return problem or _migration_missing(
                    f"migration_evidence.phases.{phase}.case_deltas.{key}"
                )
        phase_checksum, problem = _validate_migration_sha(
            case_deltas.get("checksum"),
            f"migration_evidence.phases.{phase}.case_deltas.checksum",
        )
        if problem is not None or phase_checksum != source_checksum:
            return problem or _migration_failed(
                f"migration phase {phase} checksum differs from source snapshot"
            )
        target_checksum, problem = _validate_migration_sha(
            case_deltas.get("target_checksum"),
            f"migration_evidence.phases.{phase}.case_deltas.target_checksum",
        )
        if problem is not None or target_checksum != source_checksum:
            return problem or _migration_failed(
                f"migration phase {phase} target checksum differs from source snapshot"
            )
        if case_deltas.get("phase") != phase:
            return _migration_missing(f"migration_evidence.phases.{phase}.case_deltas.phase")
        state = phase_record.get("control_state")
        if not isinstance(state, Mapping) or state.get("status") != "pass":
            return _migration_missing(f"migration_evidence.phases.{phase}.control_state")
        dual_write, active_profile, cleaned, rolled_back, mailbox_v2 = expected_control[phase]
        if any(
            state.get(key) != value
            for key, value in {
                "dual_write": dual_write,
                "active_profile": active_profile,
                "cleaned": cleaned,
                "rolled_back": rolled_back,
                "mailbox_v2": mailbox_v2,
            }.items()
        ):
            return _migration_failed(f"control state does not match phase {phase}")
        if phase == "cleanup" and (
            state.get("atomic_cutover") is not True or state.get("rollback_verified") is not True
        ):
            return _migration_failed("cleanup control state lacks atomic cutover/rollback proof")
        if phase == "rollback" and state.get("rollback_verified") is not True:
            return _migration_failed("rollback control state lacks rollback proof")
    verify = cast(Mapping[str, Any], phases["verify"])["case_deltas"]
    if cast(Mapping[str, Any], verify).get("source_count") != cast(Mapping[str, Any], verify).get(
        "target_count"
    ):
        return _migration_failed("verify source and target counts differ")

    control = migration.get("control")
    if not isinstance(control, Mapping):
        return _migration_missing("migration_evidence.control")
    if (
        control.get("complete") is not True
        or control.get("rollback_supported") is not True
        or control.get("rollback_observed") is not True
    ):
        return _migration_missing(
            "migration_evidence.control complete rollback_supported rollback_observed"
        )
    phase_count, problem = _int_field(
        control,
        "phase_count",
        "migration_evidence.control.phase_count",
        exact=len(execution_phases),
    )
    if problem is not None or phase_count is None:
        return problem or _migration_missing("migration_evidence.control.phase_count")
    if control.get("factory") not in MIGRATION_ALLOWED_FACTORIES:
        return _migration_missing("migration_evidence.control.factory")
    runtime = evidence.get("runtime_fingerprint")
    if (
        not isinstance(runtime, Mapping)
        or runtime.get("algorithm") != "sha256"
        or runtime.get("status") != "available"
        or runtime.get("mode") != "real_migration"
        or runtime.get("worker_count") != 1
        or runtime.get("parameters_sha256")
        != canonical_sha256({"phase_count": len(execution_phases), "kinds": list(kind_values)})
    ):
        return _migration_missing("evidence.runtime_fingerprint migration phase binding")

    approval = migration.get("operator_confirmation")
    if not isinstance(approval, Mapping):
        return _migration_missing("migration_evidence.operator_confirmation")
    if (
        approval.get("status") != "confirmed"
        or approval.get("method") != "cli_flag_and_environment_acknowledgement"
    ):
        return _migration_missing("migration_evidence.operator_confirmation.status/method")
    for key in ("operator_id_sha256", "change_ticket_sha256"):
        _, problem = _validate_migration_sha(
            approval.get(key), f"migration_evidence.operator_confirmation.{key}"
        )
        if problem is not None:
            return problem
    timestamp = approval.get("confirmed_at")
    if not isinstance(timestamp, str):
        return _migration_missing("migration_evidence.operator_confirmation.confirmed_at")
    try:
        confirmed_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return _migration_missing("migration_evidence.operator_confirmation.confirmed_at")
    generated_raw = evidence.get("generated_at")
    try:
        generated_at = datetime.fromisoformat(str(generated_raw).replace("Z", "+00:00"))
    except ValueError:
        return _migration_missing("evidence.generated_at")
    if (
        confirmed_at.tzinfo is None
        or generated_at.tzinfo is None
        or confirmed_at.astimezone(UTC) > generated_at.astimezone(UTC) + timedelta(seconds=5)
        or confirmed_at.astimezone(UTC) < generated_at.astimezone(UTC) - timedelta(hours=24)
    ):
        return _migration_missing("migration_evidence.operator_confirmation.confirmed_at")
    lineage = migration.get("lineage")
    if not isinstance(lineage, Mapping) or lineage.get("status") != "pass":
        return _migration_missing("migration_evidence.lineage.status=pass")
    if lineage.get("checkout_current") is not True:
        return _migration_missing("migration_evidence.lineage.checkout_current=true")
    if lineage.get("producer") != PRODUCTION_EVIDENCE_PRODUCERS["migration-live.json"]:
        return _migration_missing("migration_evidence.lineage.producer")
    if lineage.get("run_id") != run_id:
        return _migration_missing("migration_evidence.lineage.run_id matching evidence.run_id")
    for key in ("source_fingerprint", "runtime_fingerprint"):
        recorded = evidence.get(key)
        expected = recorded.get("value") if isinstance(recorded, Mapping) else None
        actual = lineage.get(key)
        if isinstance(actual, Mapping):
            actual = actual.get("value")
        if not isinstance(expected, str) or actual != expected:
            return _migration_missing(f"migration_evidence.lineage.{key} matching evidence")
    image_digest = lineage.get("image_digest")
    if (
        not isinstance(image_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest) is None
        or image_digest in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}
    ):
        return _migration_missing("migration_evidence.lineage.image_digest")
    return None, None


def _validate_migration_candidate_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Validate the producer-facing candidate contract for live migration."""

    if report.get("gate") != "pass":
        return _migration_missing("top-level gate=pass")
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return _migration_missing("candidate")
    if candidate.get("mode") != "real_redis_to_postgresql":
        return _migration_missing("candidate.mode=real_redis_to_postgresql")
    if candidate.get("scope") != "production":
        return _migration_missing("candidate.scope=production")
    tenant_id = candidate.get("tenant_id")
    migration_id = candidate.get("migration_id")
    if not isinstance(tenant_id, str) or not tenant_id.strip():
        return _migration_missing("candidate.tenant_id")
    if not isinstance(migration_id, str) or not migration_id.strip():
        return _migration_missing("candidate.migration_id")
    tenant_tokens = set(re.findall(r"[a-z0-9]+", tenant_id.lower()))
    if candidate.get("is_simulation") is True or candidate.get("is_test") is True:
        return _migration_missing("candidate tenant is marked simulation/test")
    if tenant_tokens & MIGRATION_PRODUCTION_TENANT_MARKERS:
        return _migration_missing("candidate.tenant_id is a simulation/test tenant")

    evidence_run_id = evidence.get("run_id")
    if not isinstance(evidence_run_id, str) or not evidence_run_id:
        return _migration_missing("evidence.run_id")
    if candidate.get("run_id") != evidence_run_id or report.get("run_id") != evidence_run_id:
        return _migration_missing("candidate/report.run_id matching evidence.run_id")

    backend_ids: dict[str, str] = {}
    for name, kind in (("source", "redis"), ("target", "postgresql")):
        backend = candidate.get(name)
        if not isinstance(backend, Mapping):
            return _migration_missing(f"candidate.{name}")
        if backend.get("kind") != kind or backend.get("is_real") is not True:
            return _migration_missing(f"candidate.{name}.kind/is_real")
        backend_id = backend.get("backend_id")
        if not isinstance(backend_id, str) or not backend_id.strip():
            return _migration_missing(f"candidate.{name}.backend_id")
        endpoint = backend.get("endpoint_identity")
        if not isinstance(endpoint, str) or MIGRATION_SHA256_RE.fullmatch(endpoint) is None:
            return _migration_missing(f"candidate.{name}.endpoint_identity")
        backend_ids[name] = f"{backend_id}:{endpoint}"
    if backend_ids["source"] == backend_ids["target"]:
        return _migration_failed("candidate source/target must be independent")

    phases = candidate.get("phases")
    if not isinstance(phases, Mapping):
        return _migration_missing("candidate.phases")
    phase_order = candidate.get("phase_order")
    if phase_order != list(MIGRATION_REQUIRED_PHASES):
        return _migration_missing("candidate.phase_order is incomplete or out of order")
    for index, phase_name in enumerate(MIGRATION_REQUIRED_PHASES, start=1):
        phase = phases.get(phase_name)
        if not isinstance(phase, Mapping):
            return _migration_missing(f"candidate.phases.{phase_name}")
        if phase.get("status") != "pass" or phase.get("completed") is not True:
            return _migration_missing(f"candidate.phases.{phase_name}.completed=true")
        if phase.get("order") != index:
            return _migration_missing(f"candidate.phases.{phase_name}.order={index}")

    verification = candidate.get("verification")
    if not isinstance(verification, Mapping) or verification.get("status") != "pass":
        return _migration_missing("candidate.verification.status=pass")
    source_count = verification.get("source_count")
    target_count = verification.get("target_count")
    if (
        isinstance(source_count, bool)
        or not isinstance(source_count, int)
        or source_count < 1
        or isinstance(target_count, bool)
        or not isinstance(target_count, int)
        or target_count < 1
    ):
        return _migration_missing("candidate.verification source/target counts")
    if source_count != target_count:
        return _migration_failed("candidate.verification source/target counts differ")
    source_checksum = verification.get("source_checksum")
    target_checksum = verification.get("target_checksum")
    if (
        not isinstance(source_checksum, str)
        or MIGRATION_SHA256_RE.fullmatch(source_checksum) is None
        or not isinstance(target_checksum, str)
        or MIGRATION_SHA256_RE.fullmatch(target_checksum) is None
    ):
        return _migration_missing("candidate.verification source/target checksums")
    if source_checksum != target_checksum:
        return _migration_failed("candidate.verification source/target checksums differ")
    differences = verification.get("differences")
    if (
        not isinstance(differences, Sequence)
        or isinstance(differences, (str, bytes, bytearray))
        or list(differences)
    ):
        return _migration_failed("candidate.verification.differences is not empty")

    control = candidate.get("control")
    if not isinstance(control, Mapping) or control.get("status") != "pass":
        return _migration_missing("candidate.control.status=pass")
    if control.get("tenant_id") != tenant_id or control.get("migration_id") != migration_id:
        return _migration_missing("candidate.control tenant/migration scope")
    for key in (
        "tenant_scoped",
        "atomic_cutover",
        "dual_write_verified",
        "cleanup_after_verify",
        "rollback_verified",
    ):
        if control.get(key) is not True:
            return _migration_missing(f"candidate.control.{key}=true")

    attestation = candidate.get("operator_attestation")
    if not isinstance(attestation, Mapping) or attestation.get("status") != "pass":
        return _migration_missing("candidate.operator_attestation.status=pass")
    if attestation.get("scope") != "production":
        return _migration_missing("candidate.operator_attestation.scope=production")
    for key in ("operator_id", "attested_at"):
        value = attestation.get(key)
        if not isinstance(value, str) or not value.strip():
            return _migration_missing(f"candidate.operator_attestation.{key}")
    for key in ("source_target_reviewed", "checksums_reviewed", "control_reviewed"):
        if attestation.get(key) is not True:
            return _migration_missing(f"candidate.operator_attestation.{key}=true")

    lineage = candidate.get("lineage")
    if not isinstance(lineage, Mapping) or lineage.get("status") != "pass":
        return _migration_missing("candidate.lineage.status=pass")
    if (
        lineage.get("checkout_current") is not True
        or lineage.get("producer") != _PRODUCTION_MIGRATION_PRODUCER
        or lineage.get("run_id") != evidence_run_id
    ):
        return _migration_missing("candidate.lineage current checkout/producer/run_id")
    recorded_source = evidence.get("source_fingerprint")
    recorded_runtime = evidence.get("runtime_fingerprint")
    source_value = recorded_source.get("value") if isinstance(recorded_source, Mapping) else None
    runtime_value = recorded_runtime.get("value") if isinstance(recorded_runtime, Mapping) else None
    for key, expected in (
        ("source_fingerprint", source_value),
        ("runtime_fingerprint", runtime_value),
    ):
        actual = lineage.get(key)
        if isinstance(actual, Mapping):
            actual = actual.get("value")
        if not isinstance(expected, str) or actual != expected:
            return _migration_missing(f"candidate.lineage.{key} matching evidence")
    image_digest = lineage.get("image_digest")
    if (
        not isinstance(image_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", image_digest) is None
        or image_digest in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}
    ):
        return _migration_missing("candidate.lineage.image_digest")
    return None, None


def _backend_missing(path: str) -> tuple[str, str]:
    return "not_run", f"live backend evidence is missing or invalid {path}"


def _backend_failed(reason: str) -> tuple[str, str]:
    return "fail", f"live backend acceptance failed: {reason}"


def _validate_backend_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Reject a current-looking backend report without a real contract run."""

    if not _schema_version_is(report.get("schema_version"), 1):
        return _backend_missing("schema_version=1")
    if report.get("gate") != "pass":
        return _backend_missing("top-level gate=pass")
    run_id = evidence.get("run_id")
    if not isinstance(run_id, str) or report.get("run_id") != run_id:
        return _backend_missing("run_id matching evidence.run_id")
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return _backend_missing("candidate")
    if candidate.get("kind") != "backend":
        return _backend_missing("candidate.kind=backend")
    selectors = candidate.get("selectors")
    if not _is_json_sequence(selectors) or list(cast(Sequence[Any], selectors)) != list(
        BACKEND_REQUIRED_SELECTORS
    ):
        return _backend_missing("candidate.selectors=tests/integration")
    exit_code = _strict_int(candidate.get("exit_code"))
    if exit_code is None:
        return _backend_missing("candidate.exit_code")
    if exit_code != 0:
        return _backend_failed("integration test process exited non-zero")
    duration = _strict_number(candidate.get("duration_seconds"))
    if duration is None or duration < 0:
        return _backend_missing("candidate.duration_seconds")
    test_counts = candidate.get("test_counts")
    if not isinstance(test_counts, Mapping):
        return _backend_missing("candidate.test_counts")
    parsed_counts: dict[str, int] = {}
    for key in ("tests", "passed", "failures", "errors", "skipped"):
        count = _strict_int(test_counts.get(key))
        if count is None or count < 0:
            return _backend_missing(f"candidate.test_counts.{key}")
        parsed_counts[key] = count
    if parsed_counts["tests"] < 1 or parsed_counts["passed"] < 1:
        return _backend_missing("candidate.test_counts includes executed passing tests")
    if parsed_counts["tests"] != sum(
        parsed_counts[key] for key in ("passed", "failures", "errors", "skipped")
    ):
        return _backend_missing("candidate.test_counts are internally consistent")
    if any(parsed_counts[key] for key in ("failures", "errors", "skipped")):
        return _backend_failed("backend integration results are incomplete or failed")
    identities = candidate.get("backend_identities")
    if not isinstance(identities, Mapping) or set(identities) != {"postgres", "redis", "s3"}:
        return _backend_missing("candidate.backend_identities exact inventory")
    for backend_name in ("postgres", "redis", "s3"):
        identity = identities.get(backend_name)
        if not isinstance(identity, Mapping) or set(identity) != {
            "endpoint_sha256",
            "resource_sha256",
        }:
            return _backend_missing(f"candidate.backend_identities.{backend_name}")
        for key in ("endpoint_sha256", "resource_sha256"):
            value = identity.get(key)
            if (
                not isinstance(value, str)
                or re.fullmatch(r"[0-9a-f]{64}", value) is None
                or value in {"0" * 64, "f" * 64}
            ):
                return _backend_missing(f"candidate.backend_identities.{backend_name}.{key}")
    lineage = candidate.get("lineage")
    if not isinstance(lineage, Mapping) or lineage.get("status") != "pass":
        return _backend_missing("candidate.lineage.status=pass")
    image_digest = lineage.get("image_digest")
    if (
        not isinstance(image_digest, str)
        or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(image_digest.lower()) is None
        or image_digest.lower() in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}
    ):
        return _backend_missing("candidate.lineage.image_digest")
    runtime = evidence.get("runtime_fingerprint")
    runtime_value = runtime.get("value") if isinstance(runtime, Mapping) else None
    if (
        not isinstance(runtime, Mapping)
        or runtime.get("status") != "available"
        or runtime.get("mode") != "backend_contract"
        or runtime.get("worker_count") != 1
        or not isinstance(runtime_value, str)
        or re.fullmatch(r"[0-9a-f]{64}", runtime_value) is None
    ):
        return _backend_missing("evidence.runtime_fingerprint backend contract")
    if (
        lineage.get("run_id") != run_id
        or lineage.get("runtime_fingerprint_sha256") != runtime_value
    ):
        return _backend_missing("candidate.lineage run/runtime binding")
    attestation = candidate.get("runtime_attestation")
    if not isinstance(attestation, Mapping):
        return _backend_missing("candidate.runtime_attestation")
    if (
        attestation.get("status") != "pass"
        or attestation.get("run_id") != run_id
        or attestation.get("selectors") != list(BACKEND_REQUIRED_SELECTORS)
        or attestation.get("junit_counts") != dict(test_counts)
        or str(attestation.get("image_digest", "")).lower() != image_digest.lower()
        or attestation.get("runtime_fingerprint_sha256") != runtime_value
    ):
        return _backend_missing("candidate.runtime_attestation binding")
    deltas = report.get("case_deltas")
    if not isinstance(deltas, Mapping):
        return _backend_missing("case_deltas")
    failed_processes = _strict_int(deltas.get("failed_processes"))
    if failed_processes is None:
        return _backend_missing("case_deltas.failed_processes")
    if failed_processes != 0:
        return _backend_failed("one or more backend contract processes failed")
    return None, None


def _fault_missing(path: str) -> tuple[str, str]:
    return "not_run", f"fault injection evidence is missing or invalid {path}"


def _fault_failed(reason: str) -> tuple[str, str]:
    return "fail", f"fault injection acceptance failed: {reason}"


def _strict_json_loads(raw: str) -> Any:
    """Parse release evidence without duplicate keys or non-standard numbers."""

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
        raw,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )


def _fault_path_has_symlink(path: Path) -> bool:
    current = path.absolute()
    while True:
        if current.is_symlink():
            return True
        parent = current.parent
        if parent == current:
            return False
        current = parent


def _strict_child_json(path: Path) -> Any:
    """Read a retained child report without JSON extensions or duplicate keys."""

    return _strict_json_loads(path.read_text(encoding="utf-8"))


def _validate_fault_semantics(
    report: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    report_path: Path | None = None,
) -> tuple[str | None, str | None]:
    """Require every real fault and its observed stage markers."""

    def _validate_child_observation(
        scenario_name: str, scenario_value: Mapping[str, Any]
    ) -> tuple[datetime | None, datetime | None, tuple[str, str] | None]:
        """Validate parent-observed child lineage before trusting any marker.

        The child report is a separate process/artifact.  A parent report that
        only says ``child_gate=pass`` is insufficient because a stale or
        unrelated child result could otherwise be spliced into a passing
        scenario.  The producer emits the opaque nonce hash and canonical
        child-report hash; this validator binds those values to the observed
        process window, exit status, and confined report path.
        """

        prefix = f"candidate.scenarios.{scenario_name}"
        child_run_id = scenario_value.get("child_run_id")
        if (
            not isinstance(child_run_id, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", child_run_id) is None
            or scenario_value.get("run_id") != child_run_id
        ):
            return None, None, _fault_missing(f"{prefix}.child_run_id binding")
        for field in ("child_report_sha256", "child_nonce_sha256"):
            value = scenario_value.get(field)
            if (
                not isinstance(value, str)
                or re.fullmatch(r"[0-9a-f]{64}", value.lower()) is None
                or value.lower() in {"0" * 64, "f" * 64}
            ):
                return None, None, _fault_missing(f"{prefix}.{field}")

        child_report = scenario_value.get("child_report")
        path_scope = scenario_value.get("child_report_path_scope")
        if (
            not isinstance(child_report, str)
            or not isinstance(path_scope, str)
            or not child_report
            or not path_scope
            or not Path(child_report).is_absolute()
            or not Path(path_scope).is_absolute()
            or scenario_value.get("child_report_path_confined") is not True
        ):
            return None, None, _fault_missing(f"{prefix}.child_report path confinement")
        if report_path is None:
            return None, None, _fault_missing(f"{prefix}.child_report trusted root")
        try:
            child_path = Path(child_report)
            scope_path = Path(path_scope)
            if _fault_path_has_symlink(report_path):
                return None, None, _fault_missing(f"{prefix}.release report path symlink")
            if _fault_path_has_symlink(child_path) or _fault_path_has_symlink(scope_path):
                return None, None, _fault_missing(f"{prefix}.child_report path symlink")
            if not child_path.is_file() or child_path.is_dir():
                return None, None, _fault_missing(f"{prefix}.child_report missing")
            if child_path.stat().st_size <= 0:
                return None, None, _fault_missing(f"{prefix}.child_report empty")
            resolved_child = child_path.resolve(strict=False)
            resolved_scope = scope_path.resolve(strict=False)
            if resolved_child.parent != resolved_scope:
                return None, None, _fault_missing(f"{prefix}.child_report path scope")
            if child_path.parent.name != child_run_id:
                return None, None, _fault_missing(f"{prefix}.child_report run scope")
            if child_path.name not in {"fault-stage.child.json", f"{scenario_name}.child.json"}:
                return None, None, _fault_missing(f"{prefix}.child_report filename")
            trusted_root = report_path.parent / "fault-evidence"
            if child_path.parent.parent.resolve(strict=False) != trusted_root.resolve(strict=False):
                return None, None, _fault_missing(f"{prefix}.child_report trusted root")
            if resolved_scope != (trusted_root / child_run_id).resolve(strict=False):
                return None, None, _fault_missing(f"{prefix}.child_report run scope")
            if _fault_path_has_symlink(trusted_root):
                return None, None, _fault_missing(f"{prefix}.child_report trusted root symlink")
            recorded_mtime = _strict_int(scenario_value.get("child_report_mtime_ns"))
            if (
                recorded_mtime is None
                or recorded_mtime <= 0
                or child_path.stat().st_mtime_ns != recorded_mtime
            ):
                return None, None, _fault_missing(f"{prefix}.child_report mtime mismatch")
        except (OSError, RuntimeError, ValueError):
            return None, None, _fault_missing(f"{prefix}.child_report path confinement")

        if report_path is None:
            return None, None, _fault_missing(f"{prefix}.child_report trusted root")
        trusted_root = report_path.parent / "fault-evidence"
        if _fault_path_has_symlink(trusted_root):
            return None, None, _fault_missing(f"{prefix}.child_report trusted root symlink")

        try:
            child_report_value = _strict_child_json(child_path)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            return None, None, _fault_missing(f"{prefix}.child_report strict JSON")
        if not isinstance(child_report_value, Mapping):
            return None, None, _fault_missing(f"{prefix}.child_report root")
        child_hash = canonical_sha256(dict(child_report_value))
        if child_hash != scenario_value.get("child_report_sha256"):
            return None, None, _fault_missing(f"{prefix}.child_report_sha256 mismatch")
        child_report = child_report_value

        def _parse_child_time(field: str) -> datetime | None:
            raw = scenario_value.get(field)
            if not isinstance(raw, str):
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
            return parsed if parsed.tzinfo is not None else None

        child_started = _parse_child_time("child_started_at")
        child_ended = _parse_child_time("child_ended_at")
        if (
            child_started is None
            or child_ended is None
            or child_ended < child_started
            or scenario_value.get("child_identity_verified") is not True
            or scenario_value.get("child_timestamps_verified") is not True
        ):
            return None, None, _fault_missing(f"{prefix}.child timestamps/identity")
        report_started = _parse_child_time("child_report_started_at")
        report_ended = _parse_child_time("child_report_ended_at")
        if (
            report_started is None
            or report_ended is None
            or report_ended < report_started
            or report_started < child_started - timedelta(seconds=5)
            or report_ended > child_ended + timedelta(seconds=5)
        ):
            return None, None, _fault_missing(f"{prefix}.child_report timestamps")

        def _parse_report_time(field: str) -> datetime | None:
            raw = child_report.get(field)
            if not isinstance(raw, str):
                return None
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
            return parsed if parsed.tzinfo is not None else None

        child_report_started = _parse_report_time("started_at")
        child_report_ended = _parse_report_time("ended_at")
        if (
            child_report_started is None
            or child_report_ended is None
            or child_report_ended < child_report_started
            or child_report_started != report_started
            or child_report_ended != report_ended
        ):
            return None, None, _fault_missing(f"{prefix}.child_report timestamps mismatch")
        observed_exit_code = _strict_int(scenario_value.get("observed_exit_code"))
        exit_code = _strict_int(scenario_value.get("exit_code"))
        if observed_exit_code != 0 or exit_code != 0 or observed_exit_code != exit_code:
            return None, None, _fault_missing(f"{prefix}.observed_exit_code")
        if child_report.get("run_id") != child_run_id:
            return None, None, _fault_missing(f"{prefix}.child_report run_id mismatch")
        child_nonce = child_report.get("run_nonce_sha256")
        raw_child_nonce = child_report.get("run_nonce")
        if not isinstance(child_nonce, str) and isinstance(raw_child_nonce, str):
            child_nonce = hashlib.sha256(raw_child_nonce.encode("utf-8")).hexdigest()
        child_provenance = child_report.get("execution_provenance")
        if isinstance(child_provenance, Mapping):
            if child_provenance.get("run_id") != child_run_id:
                return None, None, _fault_missing(f"{prefix}.child_report provenance run_id")
            if isinstance(child_provenance.get("nonce_sha256"), str):
                if child_nonce != child_provenance.get("nonce_sha256"):
                    return None, None, _fault_missing(f"{prefix}.child_report nonce mismatch")
                child_nonce = child_provenance.get("nonce_sha256")
        if child_nonce != scenario_value.get("child_nonce_sha256"):
            return None, None, _fault_missing(f"{prefix}.child_report nonce mismatch")
        if child_report.get("gate") != scenario_value.get("child_gate"):
            return None, None, _fault_missing(f"{prefix}.child_report gate mismatch")
        expected_child_production = scenario_value.get("child_production_gate")
        if (
            expected_child_production is not None
            and child_report.get("production_gate") != expected_child_production
        ):
            return None, None, _fault_missing(f"{prefix}.child_report production_gate mismatch")
        provenance = scenario_value.get("child_provenance")
        if isinstance(provenance, Mapping) and (
            provenance.get("run_id") != child_run_id
            or provenance.get("nonce_sha256") != scenario_value.get("child_nonce_sha256")
        ):
            return None, None, _fault_missing(f"{prefix}.child_provenance lineage")
        return child_started, child_ended, None

    if not _schema_version_is(report.get("schema_version"), 1):
        return _fault_missing("schema_version=1")
    if report.get("gate") != "pass":
        return _fault_missing("top-level gate=pass")
    run_id = evidence.get("run_id")
    if not isinstance(run_id, str) or report.get("run_id") != run_id:
        return _fault_missing("run_id matching evidence.run_id")
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return _fault_missing("candidate")
    if (
        candidate.get("mode") != "real_compose_fault_injection"
        or candidate.get("requested_scenario") != "all"
    ):
        return _fault_missing("candidate real all-scenario mode")
    scenarios = candidate.get("scenarios")
    if not isinstance(scenarios, Mapping) or set(scenarios) != set(FAULT_REQUIRED_SCENARIOS):
        return _fault_missing("candidate.scenarios complete inventory")
    try:
        generated_at = datetime.fromisoformat(
            str(evidence.get("generated_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _fault_missing("evidence.generated_at")
    if generated_at.tzinfo is None:
        return _fault_missing("evidence.generated_at")
    worker_provenance: tuple[Any, ...] | None = None
    worker_stages = {
        "worker_enqueue": "enqueue",
        "worker_tool": "tool",
        "worker_commit": "commit_txn_open",
    }
    for scenario_name, required_markers in FAULT_REQUIRED_MARKERS.items():
        scenario = scenarios.get(scenario_name)
        if not isinstance(scenario, Mapping) or scenario.get("status") != "pass":
            return _fault_missing(f"candidate.scenarios.{scenario_name}.status=pass")
        scenario_started, scenario_ended, child_problem = _validate_child_observation(
            scenario_name, scenario
        )
        if child_problem is not None:
            return child_problem
        try:
            child_path = Path(str(scenario.get("child_report")))
            child_report = _strict_child_json(child_path)
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
            return _fault_missing(f"candidate.scenarios.{scenario_name}.child_report strict JSON")
        if not isinstance(child_report, Mapping):
            return _fault_missing(f"candidate.scenarios.{scenario_name}.child_report root")
        markers = scenario.get("stage_markers")
        if not _is_json_sequence(markers):
            return _fault_missing(f"candidate.scenarios.{scenario_name}.stage_markers")
        marker_map: dict[str, Mapping[str, Any]] = {}
        for raw_marker in cast(Sequence[Any], markers):
            if not isinstance(raw_marker, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.stage_markers")
            marker_name = raw_marker.get("name")
            if not isinstance(marker_name, str) or marker_name in marker_map:
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers unique names"
                )
            marker_map[marker_name] = raw_marker
        if scenario_started is not None and (
            scenario_started > generated_at + timedelta(seconds=5)
            or scenario_ended is None
            or scenario_ended > generated_at + timedelta(seconds=5)
        ):
            return _fault_missing(f"candidate.scenarios.{scenario_name}.child run window")
        for marker_name in required_markers:
            marker = marker_map.get(marker_name)
            if not isinstance(marker, Mapping) or marker.get("status") != "pass":
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers.{marker_name}=pass"
                )
            observed_at = marker.get("observed_at")
            if not isinstance(observed_at, str):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers.{marker_name}.observed_at"
                )
            try:
                observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            except ValueError:
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers.{marker_name}.observed_at"
                )
            if (
                observed.tzinfo is None
                or observed > generated_at + timedelta(minutes=5)
                or (
                    scenario_started is not None
                    and observed < scenario_started - timedelta(seconds=5)
                )
                or (scenario_ended is not None and observed > scenario_ended + timedelta(seconds=5))
            ):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers.{marker_name}.observed_at"
                )
        child_marker_map: dict[str, Mapping[str, Any]] = {}
        child_candidate = child_report.get("candidate")
        if scenario_name not in worker_stages and not isinstance(child_candidate, Mapping):
            return _fault_missing(f"candidate.scenarios.{scenario_name}.child candidate")
        if scenario_name in worker_stages:
            stage = worker_stages[scenario_name]
            raw_cases = child_report.get("cases")
            if not _is_json_sequence(raw_cases):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child cases")
            matching_cases = [
                case
                for case in cast(Sequence[Any], raw_cases)
                if isinstance(case, Mapping) and case.get("stage") == stage
            ]
            if len(matching_cases) != 1:
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child stage")
            child_case = matching_cases[0]
            if child_case.get("status") != scenario.get("case_status"):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child case status")
            identity = child_case.get("case")
            if not isinstance(identity, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child case identity")
            if identity.get("case_id") != scenario.get("case_id"):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.case_id mismatch")
            if canonical_sha256(dict(identity)) != scenario.get("case_identity_sha256"):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.case_identity_sha256 mismatch"
                )
            for source, parent_hash in (
                ("control_id", scenario.get("control_id_sha256")),
                ("killed_container_id", scenario.get("killed_container_sha256")),
            ):
                raw_value = child_case.get(source)
                if (
                    not isinstance(raw_value, str)
                    or hashlib.sha256(raw_value.encode("utf-8")).hexdigest() != parent_hash
                ):
                    return _fault_missing(f"candidate.scenarios.{scenario_name}.{source} mismatch")
            raw_child_markers = child_case.get("markers")
            if not _is_json_sequence(raw_child_markers):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child markers")
            for raw_marker in cast(Sequence[Any], raw_child_markers):
                if not isinstance(raw_marker, Mapping) or not isinstance(
                    raw_marker.get("name"), str
                ):
                    return _fault_missing(f"candidate.scenarios.{scenario_name}.child markers")
                if raw_marker["name"] in child_marker_map:
                    return _fault_missing(
                        f"candidate.scenarios.{scenario_name}.child marker duplicates"
                    )
                child_marker_map[raw_marker["name"]] = raw_marker
        else:
            expected_phase = (
                "ambiguous"
                if scenario_name == "ambiguous"
                else "load"
                if scenario_name == "fencing"
                else "fault"
            )
            if not isinstance(child_candidate, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child candidate")
            child_deltas = child_report.get("case_deltas")
            if (
                not isinstance(child_deltas, Mapping)
                or child_deltas.get("requested_phase") != expected_phase
            ):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child phase")
            child_candidate_key = "faults" if expected_phase == "fault" else expected_phase
            child_phase = child_candidate.get(child_candidate_key)
            if not isinstance(child_phase, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child phase")
            if child_phase.get("status") != scenario.get("child_phase_status"):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child phase status")
            selected_child: object
            if scenario_name == "ambiguous":
                selected_child = child_phase
            elif scenario_name == "fencing":
                selected_child = child_phase.get("fencing_takeover")
            else:
                selected_child = child_phase.get(
                    {
                        "redis_interrupt": "redis",
                        "republish": "redis",
                        "dlq": "dlq",
                    }[scenario_name]
                )
            if not isinstance(selected_child, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child evidence")
            if not isinstance(scenario.get("evidence"), Mapping) or scenario["evidence"].get(
                "status"
            ) != selected_child.get("status"):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child evidence status")
            parent_evidence = dict(cast(Mapping[str, Any], scenario.get("evidence", {})))
            child_evidence = dict(selected_child)
            parent_evidence.pop("stage_markers", None)
            child_evidence.pop("stage_markers", None)
            if scenario_name == "republish":
                child_evidence["duplicate_publish_probe"] = child_phase.get(
                    "republish_duplicate_publish_probe"
                )
            if canonical_sha256(parent_evidence) != canonical_sha256(child_evidence):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.child evidence mismatch"
                )
            selected_marker_names: set[str] = set()
            selected_markers = selected_child.get("stage_markers")
            if _is_json_sequence(selected_markers):
                for raw_marker in cast(Sequence[Any], selected_markers):
                    if not isinstance(raw_marker, Mapping) or not isinstance(
                        raw_marker.get("name"), str
                    ):
                        return _fault_missing(f"candidate.scenarios.{scenario_name}.child markers")
                    marker_name = raw_marker["name"]
                    if marker_name in selected_marker_names:
                        return _fault_missing(
                            f"candidate.scenarios.{scenario_name}.child marker duplicates"
                        )
                    selected_marker_names.add(marker_name)
                    child_marker_map[marker_name] = raw_marker

            aggregate_markers = child_phase.get("stage_markers")
            if _is_json_sequence(aggregate_markers):
                for raw_marker in cast(Sequence[Any], aggregate_markers):
                    if not isinstance(raw_marker, Mapping) or not isinstance(
                        raw_marker.get("name"), str
                    ):
                        return _fault_missing(f"candidate.scenarios.{scenario_name}.child markers")
                    marker_name = raw_marker["name"]
                    if marker_name in selected_marker_names:
                        continue
                    if marker_name not in required_markers:
                        continue
                    if marker_name in child_marker_map and canonical_sha256(
                        dict(child_marker_map[marker_name])
                    ) != canonical_sha256(dict(raw_marker)):
                        return _fault_missing(
                            f"candidate.scenarios.{scenario_name}.child marker mismatch"
                        )
                    child_marker_map[marker_name] = raw_marker
        evidence_payload = scenario.get("evidence")
        for marker_name in required_markers:
            child_marker = child_marker_map.get(marker_name)
            parent_marker = marker_map.get(marker_name)
            if (
                child_marker is None
                or parent_marker is None
                or canonical_sha256(dict(child_marker)) != canonical_sha256(dict(parent_marker))
            ):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.stage_markers."
                    f"{marker_name} child mismatch"
                )
        if scenario_name in worker_stages:
            if (
                scenario.get("mode") != "real_fault_stage_acceptance"
                or scenario.get("stage") != worker_stages[scenario_name]
                or not _schema_version_is(scenario.get("child_schema_version"), 1)
                or scenario.get("child_mode") != "fault_stage_acceptance"
                or scenario.get("child_gate") != "pass"
                or scenario.get("child_production_gate") != "pass"
                or scenario.get("exit_code") != 0
                or scenario.get("case_status") != "pass"
            ):
                return _fault_missing(f"candidate.scenarios.{scenario_name} real child contract")
            child_run_id = scenario.get("run_id")
            if not isinstance(child_run_id, str) or not child_run_id:
                return _fault_missing(f"candidate.scenarios.{scenario_name}.run_id")
            provenance = scenario.get("child_provenance")
            if not isinstance(provenance, Mapping):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child_provenance")
            if (
                not _schema_version_is(provenance.get("schema_version"), 1)
                or provenance.get("run_id") != child_run_id
                or provenance.get("scheduler_version") != "v2"
                or provenance.get("redis_stream") != "trpc:session-ready:v2"
                or provenance.get("redis_group") != "trpc-session-ready-v2"
                or "project" in provenance
                or "worker_container" in provenance
            ):
                return _fault_missing(
                    f"candidate.scenarios.{scenario_name}.child_provenance contract"
                )
            for field in (
                "nonce_sha256",
                "project_sha256",
                "worker_container_sha256",
            ):
                if re.fullmatch(r"[0-9a-f]{64}", str(provenance.get(field))) is None:
                    return _fault_missing(
                        f"candidate.scenarios.{scenario_name}.child_provenance.{field}"
                    )
            pid = _strict_int(provenance.get("pid"))
            if pid is None or pid < 1:
                return _fault_missing(f"candidate.scenarios.{scenario_name}.child_provenance.pid")
            signature = (
                provenance.get("run_id"),
                provenance.get("nonce_sha256"),
                provenance.get("project_sha256"),
                provenance.get("worker_container_sha256"),
                pid,
            )
            if worker_provenance is None:
                worker_provenance = signature
            elif worker_provenance != signature:
                return _fault_missing("worker fault stages share one child provenance")
            for field in (
                "case_identity_sha256",
                "control_id_sha256",
                "killed_container_sha256",
            ):
                if re.fullmatch(r"[0-9a-f]{64}", str(scenario.get(field))) is None:
                    return _fault_missing(f"candidate.scenarios.{scenario_name}.{field}")
            if not isinstance(scenario.get("case_id"), str) or not scenario.get("case_id"):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.case_id")
        else:
            child_run_id = scenario.get("child_run_id")
            if (
                not isinstance(child_run_id, str)
                or scenario.get("run_id") != child_run_id
                or scenario.get("child_gate") != "pass"
                or scenario.get("exit_code") != 0
                or not isinstance(scenario.get("child_report_mtime_ns"), int)
                or scenario.get("child_report_mtime_ns", 0) <= 0
            ):
                return _fault_missing(f"candidate.scenarios.{scenario_name} real child execution")
            if (
                not isinstance(evidence_payload, Mapping)
                or evidence_payload.get("status") != "pass"
            ):
                return _fault_missing(f"candidate.scenarios.{scenario_name}.evidence.status=pass")
            if scenario_name in {"redis_interrupt", "republish"}:
                completion = evidence_payload.get("completion_after_restore")
                if not isinstance(completion, Mapping) or completion.get("status") != "pass":
                    return _fault_missing(
                        f"candidate.scenarios.{scenario_name}.completion_after_restore"
                    )
                if evidence_payload.get("duplicate_turns_verified") is not True:
                    return _fault_missing(
                        f"candidate.scenarios.{scenario_name}.duplicate_turns_verified"
                    )
            if scenario_name == "republish":
                probe = evidence_payload.get("duplicate_publish_probe")
                if not isinstance(probe, Mapping) or probe.get("status") != "pass":
                    return _fault_missing("candidate.scenarios.republish duplicate publish probe")
            if scenario_name == "fencing":
                if (
                    evidence_payload.get("takeover_observed") is not True
                    or evidence_payload.get("takeover_owner_differs") is not True
                    or not isinstance(evidence_payload.get("old_token_rejection"), Mapping)
                    or evidence_payload["old_token_rejection"].get("status") != "pass"
                ):
                    return _fault_missing("candidate.scenarios.fencing takeover/fencing evidence")
            if scenario_name == "dlq" and (
                evidence_payload.get("retry_attempts_increased") is not True
                or evidence_payload.get("terminal_path") != "exhausted_retry_terminal_path"
            ):
                return _fault_missing("candidate.scenarios.dlq terminal evidence")
            if scenario_name == "ambiguous" and (
                evidence_payload.get("manual_confirmation_required") is not True
                or evidence_payload.get("automatic_replay_count") != 0
                or evidence_payload.get("confirmed_replay_status") != "pass"
            ):
                return _fault_missing("candidate.scenarios.ambiguous manual replay evidence")
            if scenario_name == "ambiguous":
                provider_ledger = evidence_payload.get("provider_ledger")
                if (
                    not isinstance(provider_ledger, Mapping)
                    or provider_ledger.get("accepted_count") != 1
                    or provider_ledger.get("side_effect_count") != 1
                    or provider_ledger.get("duplicate_replay_count") != 1
                ):
                    return _fault_missing(
                        "candidate.scenarios.ambiguous provider ledger idempotency evidence"
                    )
    deltas = report.get("case_deltas")
    if not isinstance(deltas, Mapping):
        return _fault_missing("case_deltas")
    for key in ("requested", "passed"):
        value = deltas.get(key)
        if not _is_json_sequence(value):
            return _fault_missing(f"case_deltas.{key} complete inventory")
        inventory = list(cast(Sequence[Any], value))
        if (
            any(not isinstance(item, str) for item in inventory)
            or len(inventory) != len(FAULT_REQUIRED_SCENARIOS)
            or len(set(inventory)) != len(inventory)
            or set(inventory) != set(FAULT_REQUIRED_SCENARIOS)
        ):
            return _fault_missing(f"case_deltas.{key} complete inventory")
    lineage = candidate.get("lineage")
    if not isinstance(lineage, Mapping) or lineage.get("status") != "pass":
        return _fault_missing("candidate.lineage.status=pass")
    digest = lineage.get("image_digest")
    if (
        not isinstance(digest, str)
        or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(digest.lower()) is None
        or digest.lower() in {"sha256:" + "0" * 64, "sha256:" + "f" * 64}
    ):
        return _fault_missing("candidate.lineage.image_digest")
    return None, None


def _im_missing(path: str) -> tuple[str, str]:
    return "not_run", f"online IM evidence is missing or invalid {path}"


def _canonical_im_probe_url(value: object) -> str | None:
    if not isinstance(value, str) or not value or len(value) > IM_PROBE_URL_MAX_LENGTH:
        return None
    if any(character.isspace() or ord(character) < 0x20 for character in value):
        return None
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme.lower() != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "\\" in value
        or "%" in parsed.netloc
    ):
        return None
    hostname = hostname.lower()
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = hostname if port in (None, 443) else f"{hostname}:{port}"
    return f"https://{netloc}{parsed.path.rstrip('/')}"


def _load_current_im_probe_trust() -> tuple[dict[str, Any] | None, str | None]:
    """Read the current deploy trust snapshot without following symlinks."""

    path = IM_PROBE_TRUST_PATH
    deploy_root = path.parent
    if deploy_root.name != "deploy" or path.name != "im-probe-trust.json":
        return None, "probe trust file is outside deploy"
    if _fault_path_has_symlink(path) or _fault_path_has_symlink(deploy_root):
        return None, "probe trust path contains a symlink"
    try:
        resolved_root = deploy_root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        if resolved != resolved_root / "im-probe-trust.json":
            return None, "probe trust file is outside deploy"
        before = resolved.stat()
        raw = resolved.read_bytes()
        after = resolved.stat()
    except (OSError, RuntimeError, ValueError):
        return None, "deploy/im-probe-trust.json is missing or unreadable"
    before_snapshot = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_snapshot = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if before_snapshot != after_snapshot:
        return None, "probe trust file changed while it was being read"
    if not raw or len(raw) > IM_PROBE_TRUST_MAX_BYTES:
        return None, "probe trust file size is invalid"
    try:
        value = _strict_json_loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError, json.JSONDecodeError):
        return None, "probe trust file is not strict JSON"
    if not isinstance(value, Mapping) or set(value) != {
        "schema_version",
        "probe_url",
        "key_id",
        "ed25519_public_key",
    }:
        return None, "probe trust file has an invalid schema"
    if not _schema_version_is(value.get("schema_version"), 1):
        return None, "probe trust schema_version must be integer 1"
    probe_url = _canonical_im_probe_url(value.get("probe_url"))
    key_id = value.get("key_id")
    encoded_key = value.get("ed25519_public_key")
    if probe_url is None:
        return None, "probe trust URL is invalid"
    if not isinstance(key_id, str) or IM_PROBE_TRUST_KEY_ID_RE.fullmatch(key_id) is None:
        return None, "probe trust key_id is invalid"
    if not isinstance(encoded_key, str):
        return None, "probe trust public key is invalid"
    try:
        public_key = base64.b64decode(encoded_key, validate=True)
    except (TypeError, ValueError):
        return None, "probe trust public key is not valid base64"
    if len(public_key) != 32 or public_key in {b"\0" * 32, b"\xff" * 32}:
        return None, "probe trust public key is not a valid Ed25519 key"
    projection = {
        "schema_version": 1,
        "probe_url": probe_url,
        "key_id": key_id,
        "ed25519_public_key": encoded_key,
    }
    return {
        "probe_url": probe_url,
        "key_id": key_id,
        "key_sha256": hashlib.sha256(public_key).hexdigest(),
        "config_sha256": canonical_sha256(projection),
        "file_sha256": hashlib.sha256(raw).hexdigest(),
    }, None


def _im_response_digest_binding(
    *,
    channel: str,
    run_id: str,
    run_nonce: str,
    response_sha256: str,
    trust: Mapping[str, Any],
) -> str:
    return canonical_sha256(
        {
            "channel": channel,
            "run_id": run_id,
            "run_nonce": run_nonce,
            "response_sha256": response_sha256,
            "trust_key_id": trust.get("key_id"),
            "trust_key_sha256": trust.get("key_sha256"),
            "trust_config_sha256": trust.get("config_sha256"),
            "trust_file_sha256": trust.get("file_sha256"),
        }
    )


def _validate_online_im_semantics(
    report: Mapping[str, Any], evidence: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """Require fresh, provider-originated observations for both IM channels."""

    def _contains_nonfinite(value: object) -> bool:
        if isinstance(value, float):
            return not math.isfinite(value)
        if isinstance(value, Mapping):
            return any(
                _contains_nonfinite(key) or _contains_nonfinite(item) for key, item in value.items()
            )
        if _is_json_sequence(value):
            return any(_contains_nonfinite(item) for item in cast(Sequence[Any], value))
        return False

    def _bounded_int(value: object, *, minimum: int, maximum: int) -> bool:
        return (
            isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum
        )

    def _bounded_number(value: object, *, minimum: float, maximum: float) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
            and minimum <= float(value) <= maximum
        )

    if _contains_nonfinite(report):
        return _im_missing("non-finite numeric evidence")

    if not _schema_version_is(report.get("schema_version"), 1) or report.get("gate") != "pass":
        return _im_missing("schema_version=1 and gate=pass")
    run_id = evidence.get("run_id")
    if not isinstance(run_id, str) or report.get("run_id") != run_id:
        return _im_missing("run_id matching evidence.run_id")
    candidate = report.get("candidate")
    if not isinstance(candidate, Mapping):
        return _im_missing("candidate")
    if (
        candidate.get("mode") != "real_feishu_wecom_online"
        or candidate.get("runtime_configured") is not True
    ):
        return _im_missing("candidate real online mode")
    probe_trust, probe_trust_error = _load_current_im_probe_trust()
    if probe_trust_error is not None or probe_trust is None:
        return _im_missing(probe_trust_error or "current probe trust is unavailable")
    current_source = _current_candidate_source_fingerprint()
    recorded_source = evidence.get("source_fingerprint")
    if (
        not isinstance(recorded_source, Mapping)
        or recorded_source.get("status") != "available"
        or recorded_source.get("value") != current_source.get("value")
    ):
        return _im_missing("source fingerprint changed while reading probe trust")
    runtime_digest = candidate.get("runtime_image_digest")
    if (
        not isinstance(runtime_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", runtime_digest.lower()) is None
        or runtime_digest.lower() in {"0" * 64, "f" * 64}
    ):
        return _im_missing("candidate.runtime_image_digest")
    runtime_fingerprint = evidence.get("runtime_fingerprint")
    if (
        not isinstance(runtime_fingerprint, Mapping)
        or runtime_fingerprint.get("algorithm") != "sha256"
        or runtime_fingerprint.get("status") != "available"
        or runtime_fingerprint.get("value") != runtime_digest.lower()
    ):
        return _im_missing("evidence.runtime_fingerprint matching runtime image")
    probe = candidate.get("probe")
    if (
        not isinstance(probe, Mapping)
        or probe.get("status") != "pass"
        or probe.get("endpoint_configured") is not True
        or probe.get("endpoint_allowlisted") is not True
    ):
        return _im_missing("candidate.probe.status=pass")
    identity_attestation = probe.get("identity_attestation")
    if not isinstance(identity_attestation, Mapping):
        return _im_missing("candidate.probe.identity_attestation")
    probe_nonce = identity_attestation.get("run_nonce")
    identity_sha256 = identity_attestation.get("identity_sha256")
    if (
        identity_attestation.get("status") != "pass"
        or not isinstance(probe_nonce, str)
        or re.fullmatch(r"[A-Za-z0-9._:-]{16,256}", probe_nonce) is None
        or not isinstance(identity_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", identity_sha256.lower()) is None
        or identity_sha256.lower() in {"0" * 64, "f" * 64}
        or identity_attestation.get("identity_source")
        not in {"TRPC_IM_ONLINE_PROBE_IDENTITY", "TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256"}
        or identity_attestation.get("channels") != list(IM_REQUIRED_CHANNELS)
        or identity_attestation.get("signature_verified") is not True
        or identity_attestation.get("signed_channels") != list(IM_REQUIRED_CHANNELS)
        or identity_attestation.get("signature_algorithm") != "ed25519"
    ):
        return _im_missing("candidate.probe.identity_attestation binding")
    if (
        identity_attestation.get("trust_key_id") != probe_trust["key_id"]
        or identity_attestation.get("trust_probe_url") != probe_trust["probe_url"]
    ):
        return _im_missing("candidate.probe.identity_attestation trust identity")
    trust_field_names = {
        "trust_key_sha256": "key_sha256",
        "trust_config_sha256": "config_sha256",
        "trust_file_sha256": "file_sha256",
    }
    for field, trust_field in trust_field_names.items():
        value = identity_attestation.get(field)
        if (
            not isinstance(value, str)
            or re.fullmatch(r"[0-9a-f]{64}", value.lower()) is None
            or value.lower() in {"0" * 64, "f" * 64}
            or value.lower() != probe_trust[trust_field]
        ):
            return _im_missing(f"candidate.probe.identity_attestation.{field}")
    channels = candidate.get("channels")
    if not isinstance(channels, Mapping) or set(channels) != set(IM_REQUIRED_CHANNELS):
        return _im_missing("candidate.channels exact inventory")
    try:
        generated_at = datetime.fromisoformat(
            str(evidence.get("generated_at")).replace("Z", "+00:00")
        )
    except ValueError:
        return _im_missing("evidence.generated_at")
    if generated_at.tzinfo is None:
        return _im_missing("evidence.generated_at")
    run_nonce: str | None = None
    release_binding = evidence.get("release_binding")
    if not isinstance(release_binding, Mapping):
        return _im_missing("evidence.release_binding")
    release_id = release_binding.get("release_id")
    release_nonce_sha256 = release_binding.get("nonce_sha256")
    source_value = recorded_source.get("value") if isinstance(recorded_source, Mapping) else None
    if (
        not isinstance(release_id, str)
        or not isinstance(release_nonce_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", release_nonce_sha256) is None
        or not isinstance(source_value, str)
        or re.fullmatch(r"[0-9a-f]{64}", source_value) is None
    ):
        return _im_missing("evidence release or source binding")
    runner_sha256: str | None = None
    for channel_name in IM_REQUIRED_CHANNELS:
        channel = channels.get(channel_name)
        if not isinstance(channel, Mapping) or channel.get("status") != "pass":
            return _im_missing(f"candidate.channels.{channel_name}.status=pass")
        cases = channel.get("cases")
        if not isinstance(cases, Mapping) or set(cases) != set(IM_REQUIRED_CASES):
            return _im_missing(f"candidate.channels.{channel_name}.cases exact inventory")
        if any(
            not isinstance(cases.get(case), Mapping) or cases[case].get("status") != "pass"
            for case in IM_REQUIRED_CASES
        ):
            return _im_missing(f"candidate.channels.{channel_name}.cases all pass")
        signed_response = channel.get("signature_response")
        if not isinstance(signed_response, Mapping):
            return _im_missing(f"candidate.channels.{channel_name}.signature_response")
        response_digest = signed_response.get("response_sha256")
        binding_digest = signed_response.get("binding_sha256")
        if (
            signed_response.get("algorithm") != "sha256"
            or not isinstance(response_digest, str)
            or IM_RESPONSE_DIGEST_RE.fullmatch(response_digest) is None
            or response_digest in {"0" * 64, "f" * 64}
            or not isinstance(binding_digest, str)
            or IM_RESPONSE_DIGEST_RE.fullmatch(binding_digest) is None
            or binding_digest in {"0" * 64, "f" * 64}
        ):
            return _im_missing(f"candidate.channels.{channel_name}.signature_response digest")
        provider = channel.get("provider_evidence")
        if not isinstance(provider, Mapping):
            return _im_missing(f"candidate.channels.{channel_name}.provider_evidence")
        expected_source, expected_paths, credential_count = IM_EVIDENCE_CONTRACT[channel_name]
        if provider.get("source") != expected_source or provider.get("independent_paths") != list(
            expected_paths
        ):
            return _im_missing(f"candidate.channels.{channel_name}.provider paths")
        channel_nonce = provider.get("run_nonce")
        if (
            not isinstance(channel_nonce, str)
            or re.fullmatch(r"[A-Za-z0-9._:-]{16,256}", channel_nonce) is None
            or channel_nonce != probe_nonce
        ):
            return _im_missing(f"candidate.channels.{channel_name}.provider run_nonce")
        if binding_digest != _im_response_digest_binding(
            channel=channel_name,
            run_id=run_id,
            run_nonce=channel_nonce,
            response_sha256=response_digest,
            trust=probe_trust,
        ):
            return _im_missing(f"candidate.channels.{channel_name}.signature_response binding")
        if run_nonce is None:
            run_nonce = channel_nonce
        elif run_nonce != channel_nonce:
            return _im_missing("provider run_nonce shared across channels")
        runtime_attestation = channel.get("runtime_attestation")
        if (
            not isinstance(runtime_attestation, Mapping)
            or set(runtime_attestation) != IM_RUNTIME_ATTESTATION_FIELDS
            or runtime_attestation.get("status") != "pass"
            or runtime_attestation.get("run_nonce") != channel_nonce
            or runtime_attestation.get("image_digest") != f"sha256:{runtime_digest.lower()}"
            or runtime_attestation.get("release_id") != release_id
            or runtime_attestation.get("release_nonce_sha256") != release_nonce_sha256
            or runtime_attestation.get("source_fingerprint") != source_value
        ):
            return _im_missing(f"candidate.channels.{channel_name}.runtime_attestation")
        artifact_attestation = channel.get("artifact_attestation")
        if (
            not isinstance(artifact_attestation, Mapping)
            or set(artifact_attestation) != IM_ARTIFACT_ATTESTATION_FIELDS
            or not _schema_version_is(artifact_attestation.get("schema_version"), 1)
            or not _schema_version_is(artifact_attestation.get("runner_contract_version"), 1)
            or not _schema_version_is(artifact_attestation.get("driver_contract_version"), 1)
        ):
            return _im_missing(f"candidate.channels.{channel_name}.artifact_attestation")
        if provider.get("artifact_attestation") != artifact_attestation:
            return _im_missing(
                f"candidate.channels.{channel_name}.provider artifact_attestation binding"
            )
        channel_runner_sha256 = artifact_attestation.get("runner_sha256")
        driver_sha256 = artifact_attestation.get("driver_sha256")
        if (
            not isinstance(channel_runner_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", channel_runner_sha256) is None
            or channel_runner_sha256 in {"0" * 64, "f" * 64}
            or not isinstance(driver_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", driver_sha256) is None
            or driver_sha256 in {"0" * 64, "f" * 64}
        ):
            return _im_missing(f"candidate.channels.{channel_name}.artifact hashes")
        if runner_sha256 is None:
            runner_sha256 = channel_runner_sha256
        elif runner_sha256 != channel_runner_sha256:
            return _im_missing("candidate.channels shared runner artifact")
        started_raw = provider.get("run_started_at")
        try:
            started_at = datetime.fromisoformat(str(started_raw).replace("Z", "+00:00"))
        except ValueError:
            return _im_missing(f"candidate.channels.{channel_name}.run_started_at")
        if started_at.tzinfo is None or started_at > generated_at + timedelta(seconds=5):
            return _im_missing(f"candidate.channels.{channel_name}.run_started_at")
        account_fingerprint = provider.get("account_fingerprint")
        if (
            not isinstance(account_fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", account_fingerprint.lower()) is None
            or account_fingerprint.lower() in {"0" * 64, "f" * 64}
        ):
            return _im_missing(f"candidate.channels.{channel_name}.account_fingerprint")
        attestation = provider.get("credential_attestation")
        if (
            not isinstance(attestation, Mapping)
            or attestation.get("status") != "pass"
            or attestation.get("run_nonce") != channel_nonce
            or not _bounded_int(
                attestation.get("credential_count"),
                minimum=credential_count,
                maximum=credential_count,
            )
            or "fingerprints" in attestation
        ):
            return _im_missing(f"candidate.channels.{channel_name}.credential_attestation")
        observations = provider.get("observations")
        if not isinstance(observations, Mapping) or set(observations) != set(IM_REQUIRED_CASES):
            return _im_missing(f"candidate.channels.{channel_name}.observations exact inventory")
        for case_name, required_fields in IM_CASE_REQUIRED_FIELDS.items():
            if case_name == "reconnect":
                required_fields += (
                    IM_FEISHU_RECONNECT_FIELDS
                    if channel_name == "feishu"
                    else IM_WECOM_RECONNECT_FIELDS
                )
            if channel_name == "wecom" and case_name == "credential_rotation":
                required_fields += IM_WECOM_ROTATION_ACK_FIELDS
            if channel_name == "wecom" and case_name == "prolonged_outage":
                required_fields += IM_WECOM_SERVICE_FAILOVER_FIELDS
            observation = observations.get(case_name)
            if not isinstance(observation, Mapping) or observation.get("status") != "pass":
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.status=pass"
                )
            if observation.get("run_nonce") != channel_nonce:
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.run_nonce"
                )
            provider_hash = observation.get("provider_event_id_hash")
            if (
                not isinstance(provider_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", provider_hash) is None
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.provider hash"
                )
            try:
                observed_at = datetime.fromisoformat(
                    str(observation.get("observed_at")).replace("Z", "+00:00")
                )
            except ValueError:
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.observed_at"
                )
            if (
                observed_at.tzinfo is None
                or observed_at < started_at - timedelta(seconds=5)
                or observed_at > generated_at + timedelta(seconds=5)
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.observed_at"
                )
            if any(field not in observation for field in required_fields):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name} fields"
                )
            if case_name == "idempotency" and not _bounded_int(
                observation.get("duplicate_count"), minimum=1, maximum=1_000_000
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations."
                    f"{case_name}.duplicate_count bounds"
                )
            if case_name == "media" and not _bounded_int(
                observation.get("bytes"), minimum=1, maximum=64 * 1024 * 1024
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name}.bytes bounds"
                )
            if case_name == "reconnect" and channel_name == "wecom":
                released = observation.get("old_lock_owner_released")
                acquired = observation.get("new_lock_owner_acquired")
                if acquired is not True or type(released) is not bool:
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations.{case_name} lock takeover"
                    )
                if not _bounded_int(
                    observation.get("lock_epoch"),
                    minimum=2,
                    maximum=2**63 - 1,
                ):
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations.{case_name}.lock_epoch"
                    )
            if case_name == "reconnect" and channel_name == "feishu":
                failed_endpoint = observation.get("failed_endpoint_id_hash")
                replacement_endpoint = observation.get("replacement_endpoint_id_hash")
                received_after_failover = observation.get("received_after_failover_event_id_hash")
                outbound_request = observation.get("outbound_request_id_hash")
                acknowledged_request = observation.get("acknowledged_request_id_hash")
                if any(
                    not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None
                    for value in (
                        failed_endpoint,
                        replacement_endpoint,
                        received_after_failover,
                        outbound_request,
                        acknowledged_request,
                    )
                ):
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect endpoint or ACK hash"
                    )
                if failed_endpoint == replacement_endpoint:
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect distinct endpoints"
                    )
                if outbound_request != acknowledged_request:
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect ACK binding"
                    )
                if observation.get("endpoint_set_observed") is not True:
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect EndpointSlice observation"
                    )
                if not _bounded_int(
                    observation.get("ready_endpoint_count"), minimum=1, maximum=1_000_000
                ):
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect ready endpoints"
                    )
                if any(
                    not _bounded_int(observation.get(field), minimum=0, maximum=0)
                    for field in ("unready_endpoint_count", "terminating_endpoint_count")
                ):
                    return _im_missing(
                        "candidate.channels.feishu.observations.reconnect unstable endpoints"
                    )
            if channel_name == "wecom" and case_name in {"reconnect", "credential_rotation"}:
                outbound_request = observation.get("outbound_request_id_hash")
                acknowledged_request = observation.get("acknowledged_request_id_hash")
                if (
                    not isinstance(outbound_request, str)
                    or re.fullmatch(r"[0-9a-f]{64}", outbound_request) is None
                    or not isinstance(acknowledged_request, str)
                    or re.fullmatch(r"[0-9a-f]{64}", acknowledged_request) is None
                    or acknowledged_request != outbound_request
                ):
                    return _im_missing(
                        f"candidate.channels.wecom.observations.{case_name} ACK binding"
                    )
                if str(observation.get("provider_code")) not in {"0", "200"}:
                    return _im_missing(
                        f"candidate.channels.wecom.observations.{case_name} provider ACK code"
                    )
            if case_name == "rate_limit_retry_after":
                provider_code = observation.get("provider_error_code")
                if str(provider_code) not in IM_RATE_LIMIT_CODES[channel_name]:
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations."
                        f"{case_name}.provider rate-limit code"
                    )
                retry_after = observation.get("retry_after_seconds")
                if (
                    not _bounded_number(retry_after, minimum=0.001, maximum=3600.0)
                    or not isinstance(retry_after, (int, float))
                    or isinstance(retry_after, bool)
                ):
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations."
                        f"{case_name}.retry_after_seconds bounds"
                    )
                if not _bounded_int(
                    observation.get("retry_attempts"), minimum=2, maximum=IM_MAX_RETRY_ATTEMPTS
                ):
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations."
                        f"{case_name}.retry_attempts bounds"
                    )
                elapsed = observation.get("retry_elapsed_seconds")
                if (
                    not _bounded_number(elapsed, minimum=0.001, maximum=3600.0)
                    or not isinstance(elapsed, (int, float))
                    or isinstance(elapsed, bool)
                ):
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations."
                        f"{case_name}.retry_elapsed_seconds bounds"
                    )
                if float(elapsed) < float(retry_after) * 0.9:
                    return _im_missing(
                        f"candidate.channels.{channel_name}.observations."
                        f"{case_name} did not honor Retry-After"
                    )
            if case_name == "prolonged_outage" and not _bounded_number(
                observation.get("outage_seconds"),
                minimum=IM_MIN_PROLONGED_OUTAGE_SECONDS,
                maximum=7 * 24 * 60 * 60.0,
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations."
                    f"{case_name}.outage_seconds bounds"
                )
            if channel_name == "wecom" and case_name == "prolonged_outage":
                if observation.get("outage_mode") != "service_failover":
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage."
                        "outage_mode=service_failover"
                    )
                if observation.get("failed_instance_id_hash") == observation.get(
                    "takeover_instance_id_hash"
                ):
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage."
                        "distinct failover instances"
                    )
                released = observation.get("old_lock_owner_released")
                if (
                    type(released) is not bool
                    or observation.get("new_lock_owner_acquired") is not True
                ):
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage."
                        "lock handoff evidence"
                    )
                if not _bounded_int(
                    observation.get("connection_epoch"), minimum=2, maximum=2**63 - 1
                ):
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage.connection_epoch"
                    )
                if observation.get("event_during_outage_id_hash") != observation.get(
                    "reply_for_event_id_hash"
                ):
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage.reply event binding"
                    )
                if observation.get("outbound_request_id_hash") != observation.get(
                    "acknowledged_request_id_hash"
                ):
                    return _im_missing(
                        "candidate.channels.wecom.observations.prolonged_outage."
                        "send acknowledgement binding"
                    )
                for field, expected in (
                    ("reply_count", 1),
                    ("ack_count", 1),
                    ("pending_count", 0),
                    ("dlq_count", 0),
                ):
                    if not _bounded_int(observation.get(field), minimum=expected, maximum=expected):
                        return _im_missing(
                            "candidate.channels.wecom.observations.prolonged_outage."
                            f"{field}={expected}"
                        )
            if case_name == "ambiguous" and observation.get("drop_response_observed") is not True:
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations.{case_name} drop-response"
                )
            if case_name == "ambiguous" and not _bounded_int(
                observation.get("auto_replay_count"), minimum=0, maximum=0
            ):
                return _im_missing(
                    f"candidate.channels.{channel_name}.observations."
                    f"{case_name} requires zero automatic replay"
                )
            if (
                case_name == "credential_rotation"
                and observation.get("old_credential_rejected") is not True
            ):
                return _im_missing("credential rotation old credential rejection")
            if case_name == "ambiguous" and observation.get("auto_replay_count") != 0:
                return _im_missing("ambiguous delivery requires zero automatic replay")
    deltas = report.get("case_deltas")
    if not isinstance(deltas, Mapping) or deltas.get("failed_cases") != []:
        return _im_missing("case_deltas.failed_cases empty")
    return None, None


def _validate_disaster_recovery_semantics(
    report: Mapping[str, Any], *, report_path: Path | None
) -> tuple[str | None, str | None]:
    def bounded(value: object, *, minimum: float, maximum: float) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
            and minimum <= float(value) <= maximum
        )

    candidate = report.get("candidate")
    baseline = report.get("baseline")
    deltas = report.get("case_deltas")
    if not isinstance(candidate, Mapping) or candidate.get("mode") != "isolated_restore_drill":
        return "not_run", "disaster recovery candidate mode is not a real isolated drill"
    if not isinstance(baseline, Mapping):
        return "not_run", "disaster recovery objectives are missing"
    max_rpo = baseline.get("max_rpo_seconds")
    max_rto = baseline.get("max_rto_seconds")
    if not bounded(max_rpo, minimum=0, maximum=86_400) or not bounded(
        max_rto, minimum=0, maximum=7 * 86_400
    ):
        return "not_run", "disaster recovery RPO/RTO objectives are invalid"
    assert isinstance(max_rpo, (int, float))
    assert isinstance(max_rto, (int, float))
    required = ("postgres_pitr", "artifact_restore", "key_restore")
    if baseline.get("required_components") != list(required):
        return "not_run", "disaster recovery component contract is incomplete"
    components = candidate.get("components")
    if not isinstance(components, Mapping) or set(components) != set(required):
        return "not_run", "disaster recovery component evidence is incomplete"
    for name in required:
        component = components.get(name)
        if (
            not isinstance(component, Mapping)
            or component.get("status") != "pass"
            or not isinstance(component.get("run_id"), str)
            or not bounded(component.get("rpo_seconds"), minimum=0, maximum=float(max_rpo))
            or not bounded(component.get("rto_seconds"), minimum=0, maximum=float(max_rto))
        ):
            return "not_run", f"disaster recovery component {name} is not production-valid"
    if not isinstance(deltas, Mapping) or deltas.get("failed_components") != []:
        return "not_run", "disaster recovery failed_components is not empty"
    lineage = candidate.get("lineage")
    image_digest = lineage.get("image_digest") if isinstance(lineage, Mapping) else None
    if (
        not isinstance(image_digest, str)
        or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(image_digest) is None
    ):
        return "not_run", "disaster recovery image digest is invalid"
    if report_path is None:
        return "not_run", "disaster recovery report path is unavailable"
    lock_path = report_path.parent / "candidate-lock.json"
    binding_path = report_path.parent / "registry-image-binding.json"
    if (
        lock_path.is_symlink()
        or binding_path.is_symlink()
        or not lock_path.is_file()
        or not binding_path.is_file()
    ):
        return "not_run", "disaster recovery candidate lock or image binding is missing"
    try:
        lock = _strict_json_loads(lock_path.read_text(encoding="utf-8"))
        binding = _strict_json_loads(binding_path.read_text(encoding="utf-8"))
        from scripts.candidate_lock import verify_candidate_lock

        lock_reasons = verify_candidate_lock(lock, binding)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, TypeError) as error:
        return "not_run", f"disaster recovery candidate lock is invalid: {type(error).__name__}"
    if lock_reasons:
        return "not_run", lock_reasons[0]
    if candidate.get("candidate_lock_sha256") != hashlib.sha256(lock_path.read_bytes()).hexdigest():
        return "not_run", "disaster recovery candidate lock content changed"
    if lock.get("image_digest") != image_digest:
        return "not_run", "disaster recovery candidate lock image digest changed"
    return None, None


def _validate_functional_disaster_recovery_semantics(
    report: Mapping[str, Any], *, report_path: Path | None
) -> tuple[str | None, str | None]:
    def bounded(value: object, *, minimum: float, maximum: float) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
            and minimum <= float(value) <= maximum
        )

    baseline = report.get("baseline")
    candidate = report.get("candidate")
    deltas = report.get("case_deltas")
    required = ("postgres_pitr", "artifact_restore", "key_restore")
    if report.get("production_gate") != "not_run":
        return "not_run", "functional disaster recovery must not claim production DR pass"
    if not isinstance(baseline, Mapping) or baseline.get("required_components") != list(required):
        return "not_run", "functional disaster recovery component contract is incomplete"
    max_rto = baseline.get("max_rto_seconds")
    if not bounded(max_rto, minimum=0.001, maximum=7 * 86_400):
        return "not_run", "functional disaster recovery RTO objective is invalid"
    assert isinstance(max_rto, (int, float))
    if (
        not isinstance(candidate, Mapping)
        or candidate.get("mode") != "same_cluster_zero_cost_functional"
        or candidate.get("platform") != "kubernetes"
    ):
        return "not_run", "functional disaster recovery mode is invalid"
    components = candidate.get("components")
    if not isinstance(components, Mapping) or set(components) != set(required):
        return "not_run", "functional disaster recovery component evidence is incomplete"
    for name in required:
        component = components.get(name)
        if (
            not isinstance(component, Mapping)
            or component.get("status") != "pass"
            or not isinstance(component.get("run_id"), str)
            or not component.get("run_id")
            or not bounded(component.get("rpo_seconds"), minimum=0, maximum=86_400)
            or not bounded(component.get("rto_seconds"), minimum=0, maximum=float(max_rto))
            or not isinstance(component.get("backend"), str)
            or not component.get("backend")
            or not isinstance(component.get("restore_mode"), str)
            or not component.get("restore_mode")
        ):
            return "not_run", f"functional disaster recovery component {name} is invalid"
    if not isinstance(deltas, Mapping) or deltas.get("failed_components") != []:
        return "not_run", "functional disaster recovery failed_components is not empty"
    orchestration = candidate.get("orchestration")
    if (
        not isinstance(orchestration, Mapping)
        or orchestration.get("failure_stage") is not None
        or orchestration.get("failure_code") is not None
        or orchestration.get("namespace_created") is not True
        or orchestration.get("jobs_submitted_together") is not True
        or orchestration.get("cleanup_completed") is not True
        or not isinstance(orchestration.get("namespace_sha256"), str)
        or MIGRATION_SHA256_RE.fullmatch(str(orchestration.get("namespace_sha256"))) is None
        or not isinstance(orchestration.get("namespace_uid_sha256"), str)
        or MIGRATION_SHA256_RE.fullmatch(str(orchestration.get("namespace_uid_sha256"))) is None
    ):
        return "not_run", "functional disaster recovery orchestration or cleanup is invalid"
    lineage = candidate.get("lineage")
    image_digest = lineage.get("image_digest") if isinstance(lineage, Mapping) else None
    if (
        not isinstance(image_digest, str)
        or PRODUCTION_IMAGE_DIGEST_RE.fullmatch(image_digest) is None
    ):
        return "not_run", "functional disaster recovery image digest is invalid"
    if report_path is None:
        return "not_run", "functional disaster recovery report path is unavailable"
    lock_path = report_path.parent / "candidate-lock.json"
    binding_path = report_path.parent / "registry-image-binding.json"
    if (
        lock_path.is_symlink()
        or binding_path.is_symlink()
        or not lock_path.is_file()
        or not binding_path.is_file()
    ):
        return "not_run", "functional disaster recovery candidate lock or image binding is missing"
    try:
        lock = _strict_json_loads(lock_path.read_text(encoding="utf-8"))
        binding = _strict_json_loads(binding_path.read_text(encoding="utf-8"))
        from scripts.candidate_lock import verify_candidate_lock

        lock_reasons = verify_candidate_lock(lock, binding)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, TypeError) as error:
        return (
            "not_run",
            f"functional disaster recovery candidate lock is invalid: {type(error).__name__}",
        )
    if lock_reasons:
        return "not_run", lock_reasons[0]
    if candidate.get("candidate_lock_sha256") != hashlib.sha256(lock_path.read_bytes()).hexdigest():
        return "not_run", "functional disaster recovery candidate lock content changed"
    if lock.get("image_digest") != image_digest:
        return "not_run", "functional disaster recovery candidate lock image digest changed"
    runtime_attestation = candidate.get("runtime_attestation")
    if not isinstance(runtime_attestation, Mapping):
        return "not_run", "functional disaster recovery runtime attestation is missing"
    cluster_uid = runtime_attestation.get("cluster_uid_sha256")
    namespace_uid = runtime_attestation.get("namespace_uid_sha256")
    jobs = runtime_attestation.get("jobs")
    if (
        not isinstance(cluster_uid, str)
        or MIGRATION_SHA256_RE.fullmatch(cluster_uid) is None
        or not isinstance(namespace_uid, str)
        or MIGRATION_SHA256_RE.fullmatch(namespace_uid) is None
        or not _is_json_sequence(jobs)
    ):
        return "not_run", "functional disaster recovery runtime attestation is invalid"
    worker_identities = list(cast(Sequence[Any], jobs))
    if len(worker_identities) != len(required):
        return "not_run", "functional disaster recovery runtime Job evidence is incomplete"
    for expected_component, worker in zip(required, worker_identities, strict=True):
        if (
            not isinstance(worker, Mapping)
            or worker.get("component") != expected_component
            or worker.get("image_digest") != image_digest
            or not isinstance(worker.get("job_uid_sha256"), str)
            or MIGRATION_SHA256_RE.fullmatch(str(worker.get("job_uid_sha256"))) is None
            or not isinstance(worker.get("pod_uid_sha256"), str)
            or MIGRATION_SHA256_RE.fullmatch(str(worker.get("pod_uid_sha256"))) is None
        ):
            return "not_run", "functional disaster recovery runtime Job evidence is invalid"
    evidence = report.get("evidence")
    parameters = {
        "candidate_lock_binding_sha256": lock.get("binding_sha256"),
        "components": list(required),
        "image_digest": image_digest,
    }
    if not isinstance(evidence, Mapping) or not _runtime_fingerprint_matches(
        evidence.get("runtime_fingerprint"),
        mode="same_cluster_zero_cost_functional",
        worker_identities=worker_identities,
        stream=cluster_uid,
        group=namespace_uid,
        parameters=parameters,
    ):
        return "not_run", "functional disaster recovery runtime fingerprint is invalid"
    return None, None


def _production_evidence_result(
    report: dict[str, Any], *, report_name: str, report_path: Path | None = None
) -> tuple[str | None, str | None]:
    """Return an optional status/reason pair for production evidence."""

    reasons = validate_current_candidate_evidence(
        report.get("evidence"),
        current_source=_current_candidate_source_fingerprint(),
        require_release_binding=True,
        ttl_seconds=DEFAULT_EVIDENCE_TTL_SECONDS,
    )
    if not reasons:
        evidence = report.get("evidence")
        expected_producer = (
            FUNCTIONAL_DR_REPORT[1]
            if report_name == FUNCTIONAL_DR_REPORT[0]
            else PRODUCTION_EVIDENCE_PRODUCERS.get(report_name)
        )
        actual_producer = evidence.get("producer") if isinstance(evidence, Mapping) else None
        if expected_producer is not None and actual_producer != expected_producer:
            return (
                "not_run",
                f"production evidence producer is not allowed for {report_name}",
            )
        if expected_producer is not None:
            try:
                # Producer identifiers are module paths (for example,
                # ``scripts.kubernetes_runtime_gate``), not merely package
                # names.  Import the complete identifier so a missing gate
                # implementation cannot be hidden by the importable
                # ``scripts`` package.
                importlib.import_module(expected_producer)
            except (ImportError, ModuleNotFoundError):
                return (
                    "not_run",
                    f"production evidence producer is unavailable: {expected_producer}",
                )
        if report_name == REPORTS["performance"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "real performance evidence is missing current-candidate lineage"
            return _validate_real_performance_semantics(report, evidence)
        if report_name == REPORTS["backend"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "live backend evidence is missing current-candidate lineage"
            return _validate_backend_semantics(report, evidence)
        if report_name == REPORTS["real_runtime"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "real runtime evidence is missing current-candidate lineage"
            return _validate_real_runtime_semantics(report, evidence)
        if report_name == REPORTS["fault_injection"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "fault injection evidence is missing current-candidate lineage"
            return _validate_fault_semantics(report, evidence, report_path=report_path)
        if report_name == REPORTS["migration"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "migration evidence is missing current-candidate lineage"
            real_result = _validate_real_migration_semantics(report, evidence)
            if real_result != (None, None):
                return real_result
            return _validate_migration_candidate_semantics(report, evidence)
        if report_name == REPORTS["deployment"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "Kubernetes runtime evidence is missing current-candidate lineage"
            return _validate_kubernetes_semantics(report, evidence)
        if report_name == REPORTS["online_im"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "online IM evidence is missing current-candidate lineage"
            return _validate_online_im_semantics(report, evidence)
        if report_name == REPORTS["disaster_recovery"][0]:
            if not isinstance(evidence, Mapping):
                return "not_run", "disaster recovery evidence is missing current-candidate lineage"
            return _validate_disaster_recovery_semantics(report, report_path=report_path)
        if report_name == FUNCTIONAL_DR_REPORT[0]:
            if not isinstance(evidence, Mapping):
                return (
                    "not_run",
                    "functional disaster recovery evidence is missing current-candidate lineage",
                )
            return _validate_functional_disaster_recovery_semantics(
                report,
                report_path=report_path,
            )
        return None, None
    # Keep the historical performance wording stable for existing operators;
    # all production reports use the same validator underneath.
    if report_name == REPORTS["performance"][0]:
        reason = reasons[0]
        replacements = {
            "production evidence is missing current-candidate lineage": (
                "real performance evidence is missing current-candidate lineage"
            ),
            "production evidence is not marked current_candidate": (
                "real performance evidence is not marked current_candidate"
            ),
            "production evidence source fingerprint is missing or invalid": (
                "real performance source fingerprint is unavailable"
            ),
            "production evidence source fingerprint belongs to a different candidate": (
                "real performance evidence belongs to a different candidate"
            ),
        }
        return "not_run", replacements.get(reason, reason)
    return "not_run", reasons[0]


def _production_evidence_reason(report: dict[str, Any], *, report_name: str) -> str | None:
    """Explain why a production pass cannot be promoted for this candidate."""

    return _production_evidence_result(report, report_name=report_name)[1]


def _status(path: Path, *, production_field: bool) -> tuple[str, list[str]]:
    if not path.is_file():
        return "not_run", [f"missing report: {path.name}"]
    try:
        report_value: Any = _strict_json_loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        return "fail", [f"invalid report {path.name}: {type(error).__name__}"]
    if not isinstance(report_value, dict):
        return "fail", [f"invalid report {path.name}: root must be a JSON object"]
    report: dict[str, Any] = report_value
    if "schema_version" in report and not _schema_version_is(report.get("schema_version"), 1):
        return "fail", [f"invalid schema_version in {path.name}"]
    field = "production_gate" if production_field else "gate"
    value = report.get(field, "not_run")
    if value not in {"pass", "fail", "not_run"}:
        return "fail", [f"invalid {field} in {path.name}"]
    gate_value = report.get("gate", "not_run")
    if production_field and gate_value not in {"pass", "fail", "not_run"}:
        return "fail", [f"invalid gate in {path.name}"]
    reason_field = "production_rejection_reasons" if production_field else "rejection_reasons"
    reasons = [str(reason) for reason in report.get(reason_field, [])]
    if value == "pass":
        if production_field:
            evidence_status, evidence_reason = _production_evidence_result(
                report, report_name=path.name, report_path=path
            )
            if evidence_reason is not None:
                return evidence_status or "not_run", [evidence_reason]
            if gate_value != "pass":
                gate_reasons = [str(reason) for reason in report.get("rejection_reasons", [])]
                if not gate_reasons:
                    gate_reasons = [f"{path.name} reported gate={gate_value}"]
                return str(gate_value), gate_reasons
        return value, []
    if production_field and gate_value == "fail":
        gate_reasons = [str(reason) for reason in report.get("rejection_reasons", [])]
        if not gate_reasons:
            gate_reasons = [f"{path.name} reported gate=fail"]
        return "fail", gate_reasons
    if not reasons:
        reasons = [f"{path.name} reported {field}={value}"]
    return value, reasons


def _functional_dr_status(path: Path) -> tuple[str, list[str]]:
    status, reasons = _status(path, production_field=False)
    if status != "pass":
        return status, reasons
    try:
        report_value: Any = _strict_json_loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        return "fail", [f"invalid report {path.name}: {type(error).__name__}"]
    if not isinstance(report_value, dict):
        return "fail", [f"invalid report {path.name}: root must be a JSON object"]
    evidence_status, evidence_reason = _production_evidence_result(
        report_value,
        report_name=path.name,
        report_path=path,
    )
    if evidence_reason is not None:
        return evidence_status or "not_run", [evidence_reason]
    return "pass", []


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, default=Path("runs/multitenant"))
    parser.add_argument("--output", type=Path, default=Path("runs/multitenant/release-gate.json"))
    parser.add_argument("--require-production", action="store_true")
    parser.add_argument(
        "--allow-functional-dr",
        action="store_true",
        help="explicitly authorize a validated functional DR pass for destructive DR=not_run",
    )
    args = parser.parse_args()

    candidate: dict[str, str] = {}
    reasons: list[str] = []
    for name, (filename, production_field) in REPORTS.items():
        status, report_reasons = _status(
            args.directory / filename,
            production_field=production_field,
        )
        candidate[name] = status
        reasons.extend(f"{name}: {reason}" for reason in report_reasons)

    destructive_dr_authorizable_not_run = False
    if args.allow_functional_dr and candidate["disaster_recovery"] == "not_run":
        destructive_path = args.directory / REPORTS["disaster_recovery"][0]
        if not destructive_path.is_file():
            destructive_dr_authorizable_not_run = True
        else:
            try:
                destructive_report = _strict_json_loads(
                    destructive_path.read_text(encoding="utf-8")
                )
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                destructive_report = None
            if isinstance(destructive_report, Mapping) and destructive_report.get("gate") == "fail":
                candidate["disaster_recovery"] = "fail"
                reasons.append(
                    "disaster_recovery: failed destructive disaster recovery cannot be waived"
                )
            elif isinstance(destructive_report, Mapping):
                destructive_dr_authorizable_not_run = (
                    destructive_report.get("production_gate", "not_run") == "not_run"
                )

    if args.allow_functional_dr:
        functional_status, functional_reasons = _functional_dr_status(
            args.directory / FUNCTIONAL_DR_REPORT[0]
        )
        candidate["functional_disaster_recovery"] = functional_status
        reasons.extend(f"functional_disaster_recovery: {reason}" for reason in functional_reasons)

    authorized_not_run_gates: list[str] = []
    if (
        args.allow_functional_dr
        and candidate["disaster_recovery"] == "not_run"
        and candidate.get("functional_disaster_recovery") == "pass"
        and destructive_dr_authorizable_not_run
    ):
        authorized_not_run_gates.append("disaster_recovery")

    production_report_contract = {
        name: (filename, PRODUCTION_EVIDENCE_PRODUCERS[filename])
        for name, (filename, production_field) in REPORTS.items()
        if production_field and filename in PRODUCTION_EVIDENCE_PRODUCERS
    }
    if args.allow_functional_dr:
        production_report_contract["functional_disaster_recovery"] = FUNCTIONAL_DR_REPORT
        if "disaster_recovery" in authorized_not_run_gates:
            production_report_contract.pop("disaster_recovery")
    release_bundle_status, release_bundle_reasons = validate_manifest(
        args.directory,
        reports=production_report_contract,
        current_source=_current_candidate_source_fingerprint(),
        allow_functional_dr=args.allow_functional_dr,
        authorized_not_run_gates=tuple(authorized_not_run_gates),
    )
    candidate["release_bundle"] = release_bundle_status
    reasons.extend(f"release_bundle: {reason}" for reason in release_bundle_reasons)

    development_names = (
        "sdk",
        "coverage",
        "backend",
        "compose",
        "supply_chain",
        "simulation",
        "migration_acceptance",
        "migration_full_acceptance",
        "im_resilience_contract",
        "privacy_leak",
    )
    development_contract_names = development_names + (
        ("functional_disaster_recovery",) if args.allow_functional_dr else ()
    )
    development_failed = [name for name in development_contract_names if candidate[name] == "fail"]
    development_missing = [
        name for name in development_contract_names if candidate[name] == "not_run"
    ]
    development_gate = (
        "fail" if development_failed else "not_run" if development_missing else "pass"
    )
    production_names = tuple(
        name for name, (_, production_field) in REPORTS.items() if production_field
    )
    production_failed = [name for name in production_names if candidate[name] == "fail"]
    production_missing = [
        name
        for name in production_names
        if candidate[name] == "not_run" and name not in authorized_not_run_gates
    ]
    if release_bundle_status == "fail":
        production_failed.append("release_bundle")
    elif release_bundle_status == "not_run":
        production_missing.append("release_bundle")
    runtime_production_gate = (
        "fail" if production_failed else "not_run" if production_missing else "pass"
    )
    gate = (
        "fail"
        if runtime_production_gate == "fail" or development_gate == "fail"
        else "not_run"
        if runtime_production_gate == "not_run" or development_gate == "not_run"
        else "pass"
    )
    baseline = {**{name: "pass" for name in REPORTS}, "release_bundle": "pass"}
    if args.allow_functional_dr:
        baseline["functional_disaster_recovery"] = "pass"
    result = {
        "baseline": baseline,
        "candidate": candidate,
        "case_deltas": {
            "failed_gates": len(production_failed),
            "not_run_gates": len(production_missing),
            "development_failed_gates": len(development_failed),
            "development_not_run_gates": len(development_missing),
            "development_gate": development_gate,
        },
        "gate": gate,
        "runtime_production_gate": runtime_production_gate,
        "development_gate": development_gate,
        "authorized_not_run_gates": authorized_not_run_gates,
        "rejection_reasons": reasons,
    }
    rendered = json.dumps(result, indent=2)
    atomic_write_json(args.output, result)
    print(rendered)
    if development_gate == "fail" or (args.require_production and gate != "pass"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
