from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from redis.exceptions import RedisError

from scripts.real_performance_gate import (
    CONFIRM_REAL_LOAD_ENV,
    CONFIRM_REAL_LOAD_VALUE,
    DEFAULT_DB_POOL_SIZE,
    DEFAULT_GROUP,
    DEFAULT_MAX_INFLIGHT,
    DEFAULT_MIN_WORKERS,
    DEFAULT_STREAM,
    DEFAULT_WARMUP_STEPS,
    FINGERPRINT_MAX_FILES,
    IMAGE_SOURCE_FINGERPRINT_LABEL,
    KUBERNETES_GATEWAY_SERVICE,
    KUBERNETES_HTTP_WARMUP_REQUESTS,
    KUBERNETES_METRICS_API,
    KUBERNETES_OUTBOX_SELECTOR,
    KUBERNETES_SERVICE_CONFIGMAP,
    KUBERNETES_WORKER_CONCURRENCY_KEY,
    KUBERNETES_WORKER_SELECTOR,
    MAX_BURST_TURNS,
    MAX_CALLBACK_RATE,
    MAX_CALLBACKS,
    MAX_DB_POOL_SIZE,
    MAX_LOAD_PROCESS_TIMEOUT_SECONDS,
    PARENT_WATCH_INTERVAL_SECONDS,
    PRODUCTION_MIN_WORKERS,
    V1_GROUP,
    V1_STREAM,
    _actual_start_rate,
    _batch_state,
    _compose_worker_processes,
    _gateway_metadata,
    _hard_process_timeout,
    _kubernetes_configuration,
    _kubernetes_image_attestation,
    _kubernetes_image_digest,
    _kubernetes_metrics_memory_observation,
    _kubernetes_pod_records,
    _kubernetes_preflight,
    _kubernetes_quantity_bytes,
    _kubernetes_worker_concurrency,
    _load_kubernetes_preflight_evidence,
    _load_worker_command,
    _load_worker_parent_pid,
    _lookup_authoritative_inbound_batch,
    _loopback_gateway_metadata,
    _max_overlap,
    _memory_observation,
    _not_run_report,
    _parser,
    _percentile,
    _phase_gate,
    _pid_alive,
    _preflight,
    _prewarm_pool,
    _production_gate_reasons,
    _production_gate_status,
    _redis_pending_count,
    _redis_pending_delta,
    _required_memory_bytes,
    _run_external_load,
    _run_real,
    _scheduler_transport,
    _session_hmac_key_bytes,
    _source_fingerprint,
    _staged_warmup,
    _submit_batch,
    _submit_feishu_http_batch,
    _wait_for_batch,
    _watch_parent_process,
    _worker_image_attestation,
    _write_report,
    build_kubernetes_preflight_evidence,
    main,
)
from trpc_service.config.settings import SchedulerVersion


@pytest.fixture(autouse=True)
def _clear_inherited_real_load_environment(monkeypatch) -> None:
    """Prevent a developer shell from turning unit tests into live load."""

    for name in tuple(os.environ):
        if name.startswith(("TRPC_RUN_REAL_", "TRPC_REAL_", "TRPC_PERF_")):
            monkeypatch.delenv(name, raising=False)


def _passed_image_attestation() -> dict[str, object]:
    return {
        "status": "pass",
        "worker_count": 4,
        "image_count": 1,
        "source_fingerprint_matches": True,
    }


def _passed_http_phase() -> dict[str, object]:
    return {
        "mode": "synthetic_encrypted_feishu_http",
        "requested": 200,
        "accepted": 200,
        "errors": 0,
        "ack_p95_ms": 50.0,
        "warmup_requested": KUBERNETES_HTTP_WARMUP_REQUESTS,
        "warmup_expected_requests": KUBERNETES_HTTP_WARMUP_REQUESTS,
        "warmup_accepted": KUBERNETES_HTTP_WARMUP_REQUESTS,
        "warmup_failed": 0,
        "p90_latency_ms": 45.0,
        "p99_latency_ms": 75.0,
        "over_threshold_count": 0,
        "latency_histogram": {"0-10": 180, "10-200": 20},
        "actual_submission_start_rate_per_second": 100.0,
        "accepted_external_message_id_count": 200,
        "gateway": {"host_class": "loopback", "scheme": "http", "port": 18080},
        "authoritative_lookup": {"status": "pass"},
    }


def _kubernetes_pod_payload(
    *,
    role: str,
    name: str,
    image_digest: str,
    source_fingerprint: str | None = None,
    memory_limit: str = "1Gi",
) -> dict[str, object]:
    labels = {"app.kubernetes.io/component": role}
    if source_fingerprint is not None:
        labels[IMAGE_SOURCE_FINGERPRINT_LABEL] = source_fingerprint
    return {
        "items": [
            {
                "metadata": {"name": name, "uid": f"uid-{name}", "labels": labels},
                "spec": {
                    "nodeName": "acceptance-node",
                    "containers": [
                        {
                            "name": role,
                            "resources": {"limits": {"memory": memory_limit}},
                        }
                    ],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {
                            "name": role,
                            "ready": True,
                            "state": {"running": {"startedAt": "2026-08-26T00:00:00Z"}},
                            "containerID": f"containerd://{name}",
                            "imageID": f"docker-pullable://registry.example/trpc@{image_digest}",
                        }
                    ],
                },
            }
        ]
    }


def _parent_attested_preflight(source: str, image: str) -> dict[str, object]:
    return {
        "status": "pass",
        "worker_count": 4,
        "worker_concurrency": 50,
        "source_fingerprint": {"status": "available", "value": source},
        "worker_image_attestation": {
            "status": "pass",
            "worker_count": 4,
            "image_count": 1,
            "image_id": image,
            "source_fingerprint": source,
            "source_fingerprint_matches": True,
        },
        "service_image_attestation": {
            "worker": {
                "status": "pass",
                "image_id": image,
                "source_fingerprint": source,
            },
            "outbox-dispatcher": {
                "status": "pass",
                "image_id": image,
                "source_fingerprint": source,
            },
        },
        "kubernetes": {
            "namespace": "acceptance",
            "context": "ack-context",
            "namespace_bound": True,
        },
    }


def test_real_performance_gate_is_not_run_without_explicit_opt_in(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MULTINODE", raising=False)
    output = tmp_path / "real-performance.json"

    assert main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["schema_version"] == 1

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert "--execute" in report["rejection_reasons"][0]


def test_parser_help_uses_configured_default_db_pool_size() -> None:
    action = next(action for action in _parser()._actions if action.dest == "db_pool_size")

    assert str(DEFAULT_DB_POOL_SIZE) in action.help
    assert "64" not in action.help


def test_warmup_steps_match_release_acceptance_contract() -> None:
    assert DEFAULT_WARMUP_STEPS == (1, 4, 8)


def test_performance_gate_defaults_to_mailbox_v2_transport() -> None:
    args = _parser().parse_args([])

    assert args.scheduler_version == "v2"
    assert args.redis_stream == DEFAULT_STREAM
    assert args.redis_group == DEFAULT_GROUP
    assert _scheduler_transport(args) == (SchedulerVersion.V2, DEFAULT_STREAM, DEFAULT_GROUP)


def test_performance_gate_allows_explicit_legacy_v1_transport() -> None:
    args = _parser().parse_args(["--scheduler-version", "v1"])

    assert args.scheduler_version == "v1"
    assert args.redis_stream == V1_STREAM
    assert args.redis_group == V1_GROUP
    assert _scheduler_transport(args) == (SchedulerVersion.V1, V1_STREAM, V1_GROUP)


def test_kubernetes_configuration_requires_explicit_namespace_and_context() -> None:
    with pytest.raises(ValueError, match="namespace"):
        _kubernetes_configuration(SimpleNamespace())

    with pytest.raises(ValueError, match="context"):
        _kubernetes_configuration(
            SimpleNamespace(kubernetes_namespace="acceptance", kubernetes_context="")
        )


def test_kubernetes_configuration_validates_candidate_bindings() -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    config = _kubernetes_configuration(
        SimpleNamespace(
            kubernetes_namespace="acceptance",
            kubernetes_context="ack-context",
            kubernetes_kubeconfig="C:/Users/test/.kube/config",
            kubernetes_source_fingerprint=source,
            kubernetes_image_digest=image,
            kubernetes_memory_limit_bytes=1024,
        )
    )

    assert config == {
        "namespace": "acceptance",
        "context": "ack-context",
        "kubeconfig": "C:/Users/test/.kube/config",
        "expected_image_digest": image,
        "expected_source_fingerprint": source,
        "memory_limit_bytes": 1024,
    }

    with pytest.raises(ValueError, match="image"):
        _kubernetes_configuration(
            SimpleNamespace(
                kubernetes_namespace="acceptance",
                kubernetes_context="ack-context",
                kubernetes_image_digest="latest",
            )
        )


def test_kubernetes_worker_concurrency_reads_the_bound_configmap(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_json(arguments, _configuration):
        calls.append(list(arguments))
        return {"data": {KUBERNETES_WORKER_CONCURRENCY_KEY: "50"}}, None

    monkeypatch.setattr("scripts.real_performance_gate._kubernetes_json", fake_json)
    observed, error = _kubernetes_worker_concurrency(
        {"namespace": "acceptance", "context": "ack-context"}
    )

    assert observed == 50
    assert error is None
    assert calls == [
        [
            "get",
            f"configmap/{KUBERNETES_SERVICE_CONFIGMAP}",
            "--namespace",
            "acceptance",
            "--output",
            "json",
        ]
    ]


@pytest.mark.parametrize(
    "payload",
    (
        {"data": {}},
        {"data": {KUBERNETES_WORKER_CONCURRENCY_KEY: "49"}},
        {"data": {KUBERNETES_WORKER_CONCURRENCY_KEY: "50 "}},
    ),
)
def test_kubernetes_worker_concurrency_rejects_missing_or_wrong_configmap_value(
    monkeypatch, payload
) -> None:
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_json",
        lambda _arguments, _configuration: (payload, None),
    )

    observed, error = _kubernetes_worker_concurrency(
        {"namespace": "acceptance", "context": "ack-context"}
    )

    assert error is not None
    assert "must equal exactly 50" in error
    assert observed in {None, 49, 50}


@pytest.mark.parametrize("worker_count", (3, 5))
def test_kubernetes_preflight_requires_exactly_four_ready_workers(
    monkeypatch, worker_count: int
) -> None:
    workers = tuple({"role": "worker"} for _ in range(worker_count))
    outbox = ({"role": "outbox-dispatcher"},)
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_worker_concurrency",
        lambda _configuration: (50, None),
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_pod_records",
        lambda _configuration, *, role, selector: (
            (workers, None) if role == "worker" else (outbox, None)
        ),
    )
    args = SimpleNamespace(
        kubernetes_namespace="acceptance",
        kubernetes_context="ack-context",
        kubernetes_kubeconfig=None,
        kubernetes_source_fingerprint=None,
        kubernetes_image_digest=None,
        kubernetes_memory_limit_bytes=None,
    )

    result = _kubernetes_preflight(args, min_workers=4)

    assert result["status"] == "not_run"
    assert result["worker_count"] == worker_count
    assert result["worker_concurrency"] == 50
    assert "exactly 4" in result["reason"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("sha256:" + "a" * 64, "sha256:" + "a" * 64),
        ("docker-pullable://repo/image@sha256:" + "b" * 64, "sha256:" + "b" * 64),
        ("containerd://sha256:" + "c" * 64, "sha256:" + "c" * 64),
        ("repo/image:latest", None),
        ("sha256:bad", None),
    ],
)
def test_kubernetes_image_digest_only_accepts_immutable_cri_ids(
    value: str, expected: str | None
) -> None:
    assert _kubernetes_image_digest(value) == expected


@pytest.mark.parametrize(
    "argv",
    (
        [
            "--scheduler-version",
            "v1",
            "--redis-stream",
            DEFAULT_STREAM,
            "--redis-group",
            DEFAULT_GROUP,
        ],
        ["--scheduler-version", "v2", "--redis-stream", V1_STREAM, "--redis-group", V1_GROUP],
        ["--scheduler-version", "v1", "--redis-stream", V1_STREAM],
        ["--scheduler-version", "v2", "--redis-group", DEFAULT_GROUP],
    ),
)
def test_performance_gate_rejects_cross_version_or_partial_transport(argv: list[str]) -> None:
    args = _parser().parse_args(argv)

    with pytest.raises(ValueError, match=r"matching set|configured together"):
        _scheduler_transport(args)


def test_load_worker_command_propagates_the_resolved_scheduler_transport(tmp_path) -> None:
    args = _parser().parse_args(["--scheduler-version", "v2"])
    command = _load_worker_command(args, tmp_path / "child.json")

    assert command[command.index("--scheduler-version") + 1] == "v2"
    assert command[command.index("--redis-stream") + 1] == DEFAULT_STREAM
    assert command[command.index("--redis-group") + 1] == DEFAULT_GROUP


def test_load_worker_command_forwards_explicit_kubernetes_configuration(tmp_path) -> None:
    args = _parser().parse_args(
        [
            "--kubernetes",
            "--kubernetes-namespace",
            "acceptance",
            "--kubernetes-context",
            "ack-context",
            "--kubernetes-kubeconfig",
            "C:/Users/test/.kube/config",
            "--kubernetes-image-digest",
            "sha256:" + "b" * 64,
            "--kubernetes-source-fingerprint",
            "a" * 64,
            "--kubernetes-memory-limit-bytes",
            "4096",
        ]
    )

    command = _load_worker_command(args, tmp_path / "child.json")

    assert "--kubernetes" in command
    for option, expected in (
        ("--kubernetes-namespace", "acceptance"),
        ("--kubernetes-context", "ack-context"),
        ("--kubernetes-kubeconfig", "C:/Users/test/.kube/config"),
        ("--kubernetes-image-digest", "sha256:" + "b" * 64),
        ("--kubernetes-source-fingerprint", "a" * 64),
        ("--kubernetes-memory-limit-bytes", "4096"),
    ):
        assert command[command.index(option) + 1] == expected


def test_default_report_may_collect_git_evidence_but_never_starts_load_worker(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.delenv("TRPC_RUN_REAL_MULTINODE", raising=False)
    monkeypatch.delenv(CONFIRM_REAL_LOAD_ENV, raising=False)
    popen_calls: list[object] = []
    real_popen = subprocess.Popen

    def forbidden_popen(*args, **kwargs):
        command = args[0] if args else kwargs.get("args", ())
        if "--load-worker" in command:
            popen_calls.append((args, kwargs))
            raise AssertionError("default report must not start the load-worker")
        return real_popen(*args, **kwargs)

    monkeypatch.setattr("scripts.real_performance_gate.subprocess.Popen", forbidden_popen)
    output = tmp_path / "real-performance.json"

    assert main(["--output", str(output)]) == 0
    assert popen_calls == []
    assert json.loads(output.read_text(encoding="utf-8"))["evidence"]["git"]


@pytest.mark.parametrize("raw_timeout", ["nan", "inf", "-inf"])
def test_report_normalizes_non_finite_user_timeout_to_json_null(tmp_path, raw_timeout) -> None:
    output = tmp_path / f"real-performance-{raw_timeout.replace('-', 'neg-')}.json"
    args = _parser().parse_args([f"--timeout-seconds={raw_timeout}"])

    _write_report(output, _not_run_report(["offline test"], args))
    rendered = output.read_text(encoding="utf-8")
    report = json.loads(rendered)

    assert report["candidate"]["parameters"]["timeout_seconds"] is None
    assert "NaN" not in rendered
    assert "Infinity" not in rendered


def test_report_binds_current_candidate_source_and_git_evidence(tmp_path) -> None:
    output = tmp_path / "real-performance.json"

    _write_report(output, _not_run_report(["offline test"]))
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["evidence"]["kind"] == "current_candidate"
    source = report["evidence"]["source_fingerprint"]
    assert source["algorithm"] == "sha256"
    assert source["status"] == "available"
    assert len(source["value"]) == 64
    assert source["file_count"] > 0
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"


def test_source_fingerprint_is_reproducible_for_unchanged_candidate() -> None:
    first = _source_fingerprint()
    second = _source_fingerprint()

    assert first == second


def test_source_fingerprint_excludes_symlinks(monkeypatch, tmp_path) -> None:
    import scripts.real_performance_gate as gate

    source = tmp_path / "src"
    source.mkdir()
    target = source / "target.txt"
    target.write_text("target", encoding="utf-8")
    link = source / "link.txt"
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f"symlinks unavailable in test environment: {error}")

    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "SOURCE_FINGERPRINT_ROOTS", ("src",))

    result = _source_fingerprint()

    assert result["status"] == "available"
    assert result["file_count"] == 1


def test_source_fingerprint_fails_closed_at_file_and_byte_limits(monkeypatch, tmp_path) -> None:
    import scripts.real_performance_gate as gate

    source = tmp_path / "src"
    source.mkdir()
    (source / "a.txt").write_text("a", encoding="utf-8")
    (source / "b.txt").write_text("b", encoding="utf-8")
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "SOURCE_FINGERPRINT_ROOTS", ("src",))

    monkeypatch.setattr(gate, "FINGERPRINT_MAX_FILES", 1)
    result = _source_fingerprint()
    assert result["status"] == "unavailable"
    assert result["reason"] == "source_file_count_limit_exceeded"

    monkeypatch.setattr(gate, "FINGERPRINT_MAX_FILES", FINGERPRINT_MAX_FILES)
    monkeypatch.setattr(gate, "FINGERPRINT_MAX_BYTES", 1)
    result = _source_fingerprint()
    assert result["status"] == "unavailable"
    assert result["reason"] == "source_byte_limit_exceeded"


def test_write_boundary_replaces_historical_evidence(tmp_path) -> None:
    output = tmp_path / "historical-looking.json"
    report = {
        "evidence": {"kind": "historical", "source_fingerprint": {"value": "old"}},
        "gate": "not_run",
        "production_gate": "not_run",
    }

    _write_report(output, report)
    rendered = json.loads(output.read_text(encoding="utf-8"))

    assert rendered["evidence"]["kind"] == "current_candidate"
    assert rendered["evidence"]["source_fingerprint"]["value"] != "old"


def test_missing_real_performance_environment_never_becomes_pass(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    monkeypatch.setenv(CONFIRM_REAL_LOAD_ENV, CONFIRM_REAL_LOAD_VALUE)
    for name in (
        "TRPC_REAL_DATABASE_DSN",
        "TRPC_REAL_REDIS_URL",
        "TRPC_REAL_TENANT_ID",
        "TRPC_REAL_BINDING_ID",
        "TRPC_REAL_SESSION_HMAC_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    output = tmp_path / "real-performance.json"

    assert main(["--execute", "--confirm-real-load", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert any("TRPC_REAL_DATABASE_DSN" in reason for reason in report["rejection_reasons"])


def test_live_performance_entry_requires_current_release_binding(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    monkeypatch.setenv(CONFIRM_REAL_LOAD_ENV, CONFIRM_REAL_LOAD_VALUE)
    for name in (
        "TRPC_REAL_DATABASE_DSN",
        "TRPC_REAL_REDIS_URL",
        "TRPC_REAL_TENANT_ID",
        "TRPC_REAL_BINDING_ID",
        "TRPC_REAL_SESSION_HMAC_KEY",
        "TRPC_PERF_GATEWAY_BASE_URL",
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY",
    ):
        monkeypatch.setenv(name, "test-value-" + name.lower())
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)
    output = tmp_path / "real-performance.json"

    assert main(["--execute", "--confirm-real-load", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert any(
        "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE" in reason for reason in report["rejection_reasons"]
    )


def test_real_load_requires_confirmation_even_when_environment_opt_in_is_present(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("TRPC_RUN_REAL_MULTINODE", "1")
    monkeypatch.delenv(CONFIRM_REAL_LOAD_ENV, raising=False)
    monkeypatch.setattr(
        "scripts.real_performance_gate._run_external_load",
        lambda *_args: (_ for _ in ()).throw(AssertionError("real load must not start")),
    )

    output = tmp_path / "real-performance.json"
    assert main(["--execute", "--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))

    assert report["gate"] == "not_run"
    assert any(CONFIRM_REAL_LOAD_ENV in reason for reason in report["rejection_reasons"])


def test_percentile_is_safe_for_empty_and_unsorted_values() -> None:
    assert _percentile([], 0.95) == 0.0
    assert _percentile([8.0, 1.0, 4.0, 2.0], 0.50) == 2.0
    assert _percentile([8.0, 1.0, 4.0, 2.0], 0.95) == 8.0


def test_max_overlap_counts_independent_turn_intervals() -> None:
    intervals = (
        (0.0, 2.0),
        (0.5, 1.5),
        (1.0, 3.0),
        (3.0, 4.0),
    )

    assert _max_overlap(intervals) == 3


def test_phase_gate_rejects_loss_or_uncommitted_turns() -> None:
    phase = {"requested": 4, "accepted": 3, "errors": 1, "ack_p95_ms": 2.0}
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 3},
            "turn_statuses": {"committed": 3},
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "fail"
    assert result["accepted_message_loss"] == 1
    assert result["uncommitted_turns"] == 1


def test_phase_gate_keeps_strict_p95_boundary() -> None:
    phase = {"requested": 1, "accepted": 1, "errors": 0, "ack_p95_ms": 200.0}
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    assert _phase_gate(phase, completion, required_p95_ms=200.0)["status"] == "fail"


@pytest.mark.parametrize("missing_p95", (None, "not-a-number"))
def test_phase_gate_rejects_missing_or_invalid_p95(missing_p95) -> None:
    phase = {
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": missing_p95,
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "fail"
    assert result["ack_p95_headroom_ms"] is None


def test_phase_gate_rejects_http_phase_without_authoritative_lookup() -> None:
    phase = {
        "mode": "synthetic_encrypted_feishu_http",
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": 10.0,
        "authoritative_lookup": {"status": "fail"},
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    assert _phase_gate(phase, completion, required_p95_ms=200.0)["status"] == "fail"


def test_phase_gate_rejects_incomplete_http_warmup() -> None:
    phase = {
        "mode": "synthetic_encrypted_feishu_http",
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": 10.0,
        "warmup_requested": 16,
        "warmup_expected_requests": 16,
        "warmup_accepted": 15,
        "warmup_failed": 1,
        "authoritative_lookup": {"status": "pass"},
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "fail"
    assert result["http_warmup_passed"] is False


def test_phase_gate_rejects_http_warmup_count_mismatch_even_if_helper_reports_success() -> None:
    phase = {
        "mode": "synthetic_encrypted_feishu_http",
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": 10.0,
        "warmup_expected_requests": 16,
        "warmup_requested": 0,
        "warmup_accepted": 0,
        "warmup_failed": 0,
        "authoritative_lookup": {"status": "pass"},
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "fail"
    assert result["http_warmup_passed"] is False


def test_phase_gate_keeps_legacy_zero_http_warmup_compatibility() -> None:
    phase = {
        "mode": "synthetic_encrypted_feishu_http",
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": 10.0,
        "authoritative_lookup": {"status": "pass"},
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "pass"
    assert result["http_warmup_passed"] is True


def test_phase_gate_rejects_v2_when_mailbox_is_not_settled() -> None:
    phase = {
        "requested": 1,
        "accepted": 1,
        "errors": 0,
        "ack_p95_ms": 10.0,
    }
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
            "scheduler_version": "v2",
            "mailbox_expected_count": 1,
            "mailbox_row_count": 1,
            "mailbox_idle_count": 1,
            "mailbox_settled_count": 0,
            "mailbox_unresolved_item_count": 0,
        },
    }

    result = _phase_gate(phase, completion, required_p95_ms=200.0)

    assert result["status"] == "fail"
    assert result["mailbox_complete"] is False


def test_phase_gate_rejects_v2_with_unresolved_mailbox_item() -> None:
    phase = {"requested": 1, "accepted": 1, "errors": 0, "ack_p95_ms": 10.0}
    completion = {
        "status": "pass",
        "state": {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
            "scheduler_version": "v2",
            "mailbox_expected_count": 1,
            "mailbox_row_count": 1,
            "mailbox_idle_count": 1,
            "mailbox_settled_count": 1,
            "mailbox_unresolved_item_count": 1,
        },
    }

    assert _phase_gate(phase, completion, required_p95_ms=200.0)["status"] == "fail"


def test_redis_pending_count_supports_resp2_and_resp3_shapes() -> None:
    assert _redis_pending_count([3, b"first", b"last", []]) == 3
    assert _redis_pending_count({"pending": 4, "consumers": []}) == 4
    assert _redis_pending_count({b"pending": 5, b"consumers": []}) == 5
    assert _redis_pending_count(None) == 0


def test_redis_pending_delta_is_unknown_without_final_observation() -> None:
    assert _redis_pending_delta({"pending": 3}, None) is None
    assert _redis_pending_delta({"pending": 3}, {"stream_length": 4}) is None
    assert _redis_pending_delta({"pending": 3}, {"pending": 5, "stream_length": 4}) == 2


def test_loopback_gateway_metadata_rejects_external_or_credentialed_urls() -> None:
    assert _loopback_gateway_metadata("http://127.0.0.1:18080/base") == {
        "scheme": "http",
        "host_class": "loopback",
        "port": 18080,
        "path_present": True,
    }
    assert _loopback_gateway_metadata("http://[::1]:18080")["host_class"] == "loopback"
    for value in (
        "https://example.test",
        "http://127.0.0.1:18080/callback?secret=hidden",
        "http://user:password@127.0.0.1:18080",
    ):
        with pytest.raises(ValueError):
            _loopback_gateway_metadata(value)


def test_kubernetes_gateway_metadata_is_bound_to_the_attested_service() -> None:
    metadata = _gateway_metadata(
        "http://trpc-gateway.acceptance.svc.cluster.local:8080/base",
        allow_kubernetes_service=True,
        kubernetes_namespace="acceptance",
    )

    assert metadata == {
        "scheme": "http",
        "host_class": "kubernetes_service",
        "service_name": KUBERNETES_GATEWAY_SERVICE,
        "namespace": "acceptance",
        "port": 8080,
        "path_present": True,
    }
    with pytest.raises(ValueError, match="attested gateway Service"):
        _gateway_metadata(
            "http://untrusted.acceptance.svc.cluster.local:8080",
            allow_kubernetes_service=True,
            kubernetes_namespace="acceptance",
        )
    with pytest.raises(ValueError, match="attested gateway Service"):
        _gateway_metadata(
            "http://trpc-gateway.other.svc.cluster.local:8080",
            allow_kubernetes_service=True,
            kubernetes_namespace="acceptance",
        )


def test_kubernetes_load_worker_accepts_only_parent_signed_preflight(tmp_path, monkeypatch) -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    token = "load-worker-token"
    run_id = "real-performance-run-1"
    preflight = _parent_attested_preflight(source, image)
    envelope = build_kubernetes_preflight_evidence(
        preflight,
        run_id=run_id,
        run_token=token,
        source_fingerprint=source,
        image_digest=image,
    )
    evidence_path = tmp_path / "preflight.json"
    evidence_path.write_text(json.dumps(envelope), encoding="utf-8")
    monkeypatch.setenv("TRPC_REAL_PERFORMANCE_WORKER_TOKEN", token)
    monkeypatch.setenv("TRPC_REAL_RUN_ID", run_id)
    args = SimpleNamespace(
        kubernetes=True,
        kubernetes_preflight_evidence=str(evidence_path),
        kubernetes_namespace="acceptance",
        kubernetes_context="ack-context",
        kubernetes_kubeconfig=None,
        kubernetes_source_fingerprint=source,
        kubernetes_image_digest=image,
        kubernetes_memory_limit_bytes=None,
    )

    result = _load_kubernetes_preflight_evidence(args)

    assert result["status"] == "pass"
    assert result["kubernetes"]["preflight_evidence"]["status"] == "parent_attested"  # type: ignore[index]
    assert result["kubernetes"]["preflight_evidence"]["run_id"] == run_id  # type: ignore[index]

    envelope["signature"] = "0" * 64
    evidence_path.write_text(json.dumps(envelope), encoding="utf-8")
    with pytest.raises(ValueError, match="signature"):
        _load_kubernetes_preflight_evidence(args)


def test_session_hmac_key_decoding_matches_service_base64url_contract() -> None:
    raw = bytes(range(32))
    encoded = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

    assert _session_hmac_key_bytes(encoded) == raw


def test_session_hmac_key_accepts_at_least_32_raw_bytes() -> None:
    raw = "r" * 32

    assert _session_hmac_key_bytes(raw) == raw.encode()


@pytest.mark.parametrize("value", ("", "short", "!" * 31))
def test_session_hmac_key_rejects_short_or_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        _session_hmac_key_bytes(value)


def test_authoritative_inbound_lookup_is_tenant_channel_account_scoped(monkeypatch) -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []

    async def fake_scoped_fetch(_pool, tenant_id, query, *args):
        calls.append((query, (tenant_id, *args)))
        return [
            {
                "inbound_id": "inbound-1",
                "session_id": "session-1",
                "channel": "feishu",
                "account_id": "account-1",
                "external_message_id": "message-1",
            }
        ]

    monkeypatch.setattr("scripts.real_performance_gate._scoped_fetch", fake_scoped_fetch)
    route = SimpleNamespace(
        binding=SimpleNamespace(channel=SimpleNamespace(value="feishu"), account_id="account-1")
    )
    result = asyncio.run(
        _lookup_authoritative_inbound_batch(object(), "tenant-1", route, ["message-1"])
    )

    assert result["status"] == "pass"
    assert result["inbound_ids"] == ("inbound-1",)
    assert result["session_ids"] == ("session-1",)
    assert calls and "tenant_id=$1" in calls[0][0]
    assert calls[0][1] == ("tenant-1", "tenant-1", "feishu", "account-1", ["message-1"])


def test_authoritative_inbound_lookup_fails_closed_for_missing_or_duplicate_rows(
    monkeypatch,
) -> None:
    async def fake_scoped_fetch(*_args, **_kwargs):
        return [
            {
                "inbound_id": "inbound-1",
                "session_id": "session-1",
                "channel": "feishu",
                "account_id": "account-1",
                "external_message_id": "message-1",
            },
            {
                "inbound_id": "inbound-2",
                "session_id": "session-2",
                "channel": "feishu",
                "account_id": "account-1",
                "external_message_id": "message-1",
            },
        ]

    monkeypatch.setattr("scripts.real_performance_gate._scoped_fetch", fake_scoped_fetch)
    route = SimpleNamespace(
        binding=SimpleNamespace(channel=SimpleNamespace(value="feishu"), account_id="account-1")
    )
    result = asyncio.run(
        _lookup_authoritative_inbound_batch(object(), "tenant-1", route, ["message-1", "message-2"])
    )

    assert result["status"] == "fail"
    assert result["duplicate_count"] == 1
    assert result["missing_count"] == 1
    assert result["inbound_ids"] == ()


def test_feishu_http_batch_uses_mocked_helper_and_authoritative_rows(monkeypatch) -> None:
    seen: dict[str, object] = {}

    async def fake_http(options, *, warmup_requests, latency_threshold_ms):
        seen["options"] = options
        seen["warmup_requests"] = warmup_requests
        seen["latency_threshold_ms"] = latency_threshold_ms
        return SimpleNamespace(
            requested=2,
            accepted=2,
            failed=0,
            status_counts={200: 2},
            failure_counts={},
            elapsed_ms=10.0,
            p50_latency_ms=3.0,
            p95_latency_ms=5.0,
            max_latency_ms=7.0,
            warmup_requested=16,
            warmup_accepted=16,
            warmup_failed=0,
            p90_latency_ms=4.0,
            p99_latency_ms=6.0,
            over_threshold_count=0,
            latency_histogram={"0-10": 2},
            offered_rate_per_second=100.0,
            observed_rate_per_second=200.0,
            submission_span_seconds=0.01,
            actual_submission_start_rate_per_second=100.0,
            callback_submission_started_at="2026-08-24T09:00:00Z",
            callback_submission_last_started_at="2026-08-24T09:00:00.010000Z",
            max_inflight=2,
            accepted_external_message_ids=("message-1", "message-2"),
        )

    async def fake_lookup(*_args, **_kwargs):
        return {
            "status": "pass",
            "requested_count": 2,
            "row_count": 2,
            "missing_count": 0,
            "duplicate_count": 0,
            "inbound_ids": ("inbound-1", "inbound-2"),
            "session_ids": ("session-1", "session-2"),
        }

    monkeypatch.setattr("scripts.real_performance_gate.run_feishu_http_performance", fake_http)
    monkeypatch.setattr(
        "scripts.real_performance_gate._lookup_authoritative_inbound_batch", fake_lookup
    )
    route = SimpleNamespace(
        binding=SimpleNamespace(
            channel=SimpleNamespace(value="feishu"), account_id="account-1", app_id="app-1"
        )
    )
    result = asyncio.run(
        _submit_feishu_http_batch(
            object(),
            route,
            tenant_id="tenant-1",
            binding_id="binding-1",
            gateway_base_url="http://127.0.0.1:18080",
            verification_token="synthetic-token",
            encrypt_key="synthetic-encrypt-key",
            run_id="run-1",
            count=2,
            offered_rate=100.0,
            max_inflight=2,
            timeout_seconds=10.0,
            warmup_requests=16,
        )
    )

    assert result["mode"] == "synthetic_encrypted_feishu_http"
    assert result["accepted_inbound_ids"] == ("inbound-1", "inbound-2")
    assert result["authoritative_lookup"]["status"] == "pass"
    assert result["warmup_requested"] == 16
    assert result["warmup_expected_requests"] == 16
    assert result["warmup_accepted"] == 16
    assert result["warmup_failed"] == 0
    assert result["http_warmup_passed"] is True
    assert result["p90_latency_ms"] == 4.0
    assert result["p99_latency_ms"] == 6.0
    assert result["over_threshold_count"] == 0
    assert result["latency_histogram"] == {"0-10": 2}
    options = seen["options"]
    assert options.base_url == "http://127.0.0.1:18080"
    assert options.binding_id == "binding-1"
    assert options.app_id == "account-1"
    assert seen["warmup_requests"] == 16
    assert seen["latency_threshold_ms"] == 200.0


def test_production_gate_requires_locked_load_target_after_workload_passes() -> None:
    low_callbacks = SimpleNamespace(callbacks=199, callback_rate=100.0, burst_turns=200)
    low_rate = SimpleNamespace(callbacks=200, callback_rate=99.0, burst_turns=200)
    low_burst = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=199)
    target = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=200)

    assert _production_gate_status(low_callbacks, True) == "not_run"
    assert _production_gate_status(low_rate, True) == "not_run"
    assert _production_gate_status(low_burst, True) == "not_run"
    evidence = {
        "actual_submission_start_rate_per_second": 100.0,
        "burst_actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "burst_session_ids": [f"session-{index}" for index in range(200)],
        "burst_accepted": 200,
        "accepted_inbound_ids": [f"inbound-{index}" for index in range(400)],
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "worker_image_attestation": _passed_image_attestation(),
        "sustained_http_phase": _passed_http_phase(),
    }

    assert _production_gate_status(target, True, **evidence) == "pass"


def test_production_gate_fails_when_sustained_http_warmup_is_incomplete() -> None:
    args = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=200)
    phase = _passed_http_phase()
    phase["warmup_accepted"] = KUBERNETES_HTTP_WARMUP_REQUESTS - 1
    phase["warmup_failed"] = 1
    evidence = {
        "actual_submission_start_rate_per_second": 100.0,
        "burst_actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "burst_session_ids": [f"session-{index}" for index in range(200)],
        "burst_accepted": 200,
        "accepted_inbound_ids": [f"inbound-{index}" for index in range(400)],
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "worker_image_attestation": _passed_image_attestation(),
        "sustained_http_phase": phase,
    }

    assert _production_gate_status(args, True, **evidence) == "fail"
    assert any(
        "HTTP warmup" in reason for reason in _production_gate_reasons(args, True, **evidence)
    )


def test_production_gate_rejects_legacy_direct_only_sustained_evidence() -> None:
    args = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=200)
    evidence = {
        "actual_submission_start_rate_per_second": 100.0,
        "burst_actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "burst_session_ids": [f"session-{index}" for index in range(200)],
        "burst_accepted": 200,
        "accepted_inbound_ids": [f"inbound-{index}" for index in range(400)],
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "worker_image_attestation": _passed_image_attestation(),
    }

    assert _production_gate_status(args, True, **evidence) == "not_run"
    assert any(
        "HTTP callback phase" in reason
        for reason in _production_gate_reasons(args, True, **evidence)
    )


def test_production_gate_rejects_missing_or_invalid_runtime_evidence() -> None:
    args = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=200)
    session_ids = [f"session-{index}" for index in range(200)]
    inbound_ids = [f"inbound-{index}" for index in range(400)]
    common = {
        "actual_submission_start_rate_per_second": 100.0,
        "burst_actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "burst_session_ids": session_ids,
        "burst_accepted": 200,
        "accepted_inbound_ids": inbound_ids,
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "worker_image_attestation": _passed_image_attestation(),
        "sustained_http_phase": _passed_http_phase(),
    }

    assert _production_gate_status(args, True) == "not_run"
    assert (
        _production_gate_status(args, True, **{**common, "max_turn_overlap_observed": 199})
        == "fail"
    )
    assert (
        _production_gate_status(
            args,
            True,
            **{**common, "burst_session_ids": [*session_ids[:-1], session_ids[-2]]},
        )
        == "fail"
    )
    assert (
        _production_gate_status(
            args,
            True,
            **{**common, "accepted_inbound_ids": [*inbound_ids[:-1], inbound_ids[-2]]},
        )
        == "fail"
    )
    assert _production_gate_status(args, True, **{**common, "baseline_redis_pending": 1}) == "fail"
    assert _production_gate_status(args, True, **{**common, "final_redis_pending": 1}) == "fail"

    reasons = _production_gate_reasons(
        args,
        True,
        **{**common, "max_turn_overlap_observed": 199},
    )
    assert any("max_turn_overlap_observed" in reason for reason in reasons)


def test_production_gate_requires_sustained_actual_submission_start_rate() -> None:
    args = SimpleNamespace(callbacks=200, callback_rate=100.0, burst_turns=200)
    evidence = {
        "actual_submission_start_rate_per_second": 100.0,
        "burst_actual_submission_start_rate_per_second": 100.0,
        "max_turn_overlap_observed": 200,
        "burst_session_ids": [f"session-{index}" for index in range(200)],
        "burst_accepted": 200,
        "accepted_inbound_ids": [f"inbound-{index}" for index in range(400)],
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "worker_image_attestation": _passed_image_attestation(),
        "sustained_http_phase": _passed_http_phase(),
    }

    assert (
        _production_gate_status(
            args,
            True,
            **{**evidence, "actual_submission_start_rate_per_second": 99.999},
        )
        == "fail"
    )
    assert (
        _production_gate_status(
            args,
            True,
            **{**evidence, "actual_submission_start_rate_per_second": float("nan")},
        )
        == "fail"
    )
    assert (
        _production_gate_status(
            args,
            True,
            **{**evidence, "burst_actual_submission_start_rate_per_second": 99.999},
        )
        == "fail"
    )
    assert _production_gate_status(args, True, **evidence) == "pass"


def test_actual_start_rate_uses_inter_arrival_count_and_zero_for_one() -> None:
    assert _actual_start_rate([10.0]) == 0.0
    assert _actual_start_rate([10.0, 10.5, 11.0]) == 2.0


def test_low_pressure_workload_remains_ordinary_pass_only() -> None:
    args = SimpleNamespace(callbacks=100, callback_rate=50.0, burst_turns=100)

    assert _production_gate_status(args, True) == "not_run"


def test_default_run_rate_has_headroom_above_production_minimum() -> None:
    args = _parser().parse_args([])
    assert args.callback_rate == 105.0
    assert args.db_pool_size == 32
    assert args.max_inflight == 64
    assert args.min_workers == PRODUCTION_MIN_WORKERS == DEFAULT_MIN_WORKERS == 4
    assert args.db_pool_size <= args.max_inflight


def test_parser_explains_that_burst_turns_are_measured_not_client_concurrency() -> None:
    actions = {action.dest: action for action in _parser()._actions}

    assert "independent session" in actions["burst_turns"].help
    assert "actual" in actions["burst_turns"].help
    assert "accept" in actions["max_inflight"].help


def test_report_parameters_record_workload_and_load_generator_scope() -> None:
    args = _parser().parse_args(
        [
            "--callbacks",
            "240",
            "--callback-rate",
            "120",
            "--burst-turns",
            "220",
            "--max-inflight",
            "24",
            "--db-pool-size",
            "16",
            "--min-workers",
            "4",
            "--timeout-seconds",
            "17.5",
        ]
    )

    report = _not_run_report(["offline environment"], args)

    assert report["candidate"]["parameters"] == {
        "callbacks": 240,
        "callback_rate_per_second": 120.0,
        "burst_turns": 220,
        "target_max_turn_overlap": 220,
        "max_inflight": 24,
        "max_inflight_accepts": 24,
        "db_pool_size": 16,
        "db_pool_scope": "load_generator_only",
        "min_workers": 4,
        "timeout_seconds": 17.5,
        "scheduler_version": "v2",
        "redis_stream": DEFAULT_STREAM,
        "redis_group": DEFAULT_GROUP,
    }


def test_staged_warmup_caps_each_stage_to_requested_burst_target(monkeypatch) -> None:
    submitted: list[int] = []

    async def fake_submit(*_args, **kwargs):
        submitted.append(int(kwargs["count"]))
        return {
            "accepted": kwargs["count"],
            "errors": 0,
            "inbound_ids": (),
            "session_ids": (),
        }

    async def fake_wait(*_args, **_kwargs):
        return {"status": "pass"}

    monkeypatch.setattr("scripts.real_performance_gate._submit_batch", fake_submit)
    monkeypatch.setattr("scripts.real_performance_gate._wait_for_batch", fake_wait)

    observations, passed = asyncio.run(
        _staged_warmup(
            object(),
            SimpleNamespace(binding=SimpleNamespace(channel="feishu", account_id="account")),
            object(),
            object(),
            binding_id="binding",
            tenant_id="tenant",
            baseline_pending=0,
            stream="stream",
            group="group",
            run_id="run",
            max_inflight=32,
            timeout_seconds=1.0,
            target_turns=1,
        )
    )

    assert passed is True
    assert submitted == [1, 1, 1]
    assert [item["requested"] for item in observations] == [1, 1, 1]


def test_two_worker_full_load_cannot_upgrade_to_production() -> None:
    args = SimpleNamespace(
        callbacks=200,
        callback_rate=105.0,
        burst_turns=200,
        min_workers=2,
    )
    evidence = {
        "max_turn_overlap_observed": 200,
        "burst_session_ids": [f"session-{index}" for index in range(200)],
        "burst_accepted": 200,
        "accepted_inbound_ids": [f"inbound-{index}" for index in range(400)],
        "baseline_redis_pending": 0,
        "final_redis_pending": 0,
        "actual_submission_start_rate_per_second": 105.0,
        "burst_actual_submission_start_rate_per_second": 105.0,
        "worker_image_attestation": _passed_image_attestation(),
        "sustained_http_phase": _passed_http_phase(),
    }

    assert _production_gate_status(args, True, **evidence) == "not_run"


def test_preflight_rejects_database_pool_above_inflight_limit(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ())
    args = SimpleNamespace(
        callbacks=200,
        burst_turns=200,
        callback_rate=100.0,
        min_workers=2,
        db_pool_size=33,
        max_inflight=32,
        timeout_seconds=300.0,
    )

    result = _preflight(args)

    assert result == {
        "status": "fail",
        "reason": "database pool size must not exceed the in-flight limit",
    }


def test_production_gate_fails_when_workload_fails_even_below_target() -> None:
    args = SimpleNamespace(callbacks=1, callback_rate=1.0, burst_turns=1)

    assert _production_gate_status(args, False) == "fail"


def test_preflight_rejects_invalid_counts_before_process_inspection(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ())
    args = type(
        "Args",
        (),
        {
            "callbacks": 0,
            "burst_turns": 200,
            "callback_rate": 100.0,
            "min_workers": 2,
            "db_pool_size": 64,
            "timeout_seconds": 300.0,
        },
    )()

    result = _preflight(args)

    assert result == {
        "status": "fail",
        "reason": "callback and burst counts must be positive",
    }


def test_preflight_rejects_non_finite_callback_rates_before_process_inspection(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ())
    for callback_rate in (float("nan"), float("inf"), float("-inf")):
        args = SimpleNamespace(
            callbacks=200,
            burst_turns=200,
            callback_rate=callback_rate,
            min_workers=2,
            db_pool_size=64,
            timeout_seconds=300.0,
        )

        assert _preflight(args) == {
            "status": "fail",
            "reason": "callback rate must be finite and positive",
        }


def test_preflight_rejects_unsafe_load_limits_before_process_inspection(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ())
    cases = (
        {"callbacks": MAX_CALLBACKS + 1, "reason": "callback count exceeds"},
        {"burst_turns": MAX_BURST_TURNS + 1, "reason": "burst turn count exceeds"},
        {"callback_rate": MAX_CALLBACK_RATE + 1, "reason": "callback rate exceeds"},
        {"db_pool_size": MAX_DB_POOL_SIZE + 1, "reason": "database pool size exceeds"},
        {"max_inflight": DEFAULT_MAX_INFLIGHT * 2 + 1, "reason": "in-flight limit exceeds"},
    )
    for update in cases:
        values = {
            "callbacks": 200,
            "burst_turns": 200,
            "callback_rate": 100.0,
            "min_workers": 2,
            "db_pool_size": 64,
            "max_inflight": DEFAULT_MAX_INFLIGHT,
            "timeout_seconds": 300.0,
        }
        values.update({key: value for key, value in update.items() if key != "reason"})
        result = _preflight(SimpleNamespace(**values))
        assert result["status"] == "fail"
        assert update["reason"] in result["reason"]


def test_preflight_reports_resource_snapshot(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ({}, {}))
    monkeypatch.setattr(
        "scripts.real_performance_gate._resource_snapshot",
        lambda: {
            "cpu_count": 8,
            "available_memory_bytes": 4 * 1024**3,
        },
    )
    args = SimpleNamespace(
        callbacks=200,
        burst_turns=200,
        callback_rate=100.0,
        min_workers=2,
        db_pool_size=32,
        max_inflight=DEFAULT_MAX_INFLIGHT,
        timeout_seconds=300.0,
    )

    result = _preflight(args)

    assert result["status"] == "pass"
    assert result["resources"]["cpu_count"] == 8
    assert result["resources"]["required_memory_bytes"] == _required_memory_bytes(
        db_pool_size=32,
        max_inflight=DEFAULT_MAX_INFLIGHT,
        burst_turns=200,
    )


def test_preflight_kubernetes_uses_pod_and_metrics_evidence_without_local_pid_or_docker(
    monkeypatch,
) -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    workers = tuple(
        {
            "role": "worker",
            "pod_name": f"worker-{index}",
            "pod_uid": f"uid-worker-{index}",
            "container_name": "worker",
            "container_id": f"containerd://worker-{index}",
            "image_id": image,
            "image_digest": image,
            "source_fingerprint": source,
            "node_name": "acceptance-node",
            "process_count": 1,
            "memory_limit_bytes": 2 * 1024**3,
            "ready": True,
        }
        for index in range(4)
    )
    outbox = (
        {
            "role": "outbox-dispatcher",
            "pod_name": "outbox-0",
            "pod_uid": "uid-outbox-0",
            "container_name": "outbox-dispatcher",
            "container_id": "containerd://outbox-0",
            "image_id": image,
            "image_digest": image,
            "source_fingerprint": source,
            "node_name": "acceptance-node",
            "process_count": 1,
            "memory_limit_bytes": 1024**3,
            "ready": True,
        },
    )
    metrics = {
        "items": [
            {
                "metadata": {"name": f"worker-{index}"},
                "timestamp": "2026-08-26T00:00:15Z",
                "window": "15s",
                "containers": [{"name": "worker", "usage": {"memory": "128Mi"}}],
            }
            for index in range(4)
        ]
        + [
            {
                "metadata": {"name": "outbox-0"},
                "timestamp": "2026-08-26T00:00:15Z",
                "window": "15s",
                "containers": [{"name": "outbox-dispatcher", "usage": {"memory": "64Mi"}}],
            }
        ]
    }

    monkeypatch.setattr(
        "scripts.real_performance_gate._resource_snapshot",
        lambda: {"cpu_count": 8, "available_memory_bytes": 8 * 1024**3},
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._source_fingerprint",
        lambda: {"algorithm": "sha256", "status": "available", "value": source},
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_pod_records",
        lambda _configuration, *, role, selector: (
            (workers, None) if role == "worker" else (outbox, None)
        ),
    )

    def fake_kubernetes_json(arguments, _configuration):
        if arguments[1].startswith("configmap/"):
            return {"data": {"TRPC_SERVICE_WORKER_CONCURRENCY": "50"}}, None
        return metrics, None

    monkeypatch.setattr("scripts.real_performance_gate._kubernetes_json", fake_kubernetes_json)
    monkeypatch.setattr(
        "scripts.real_performance_gate._worker_processes",
        lambda: pytest.fail("Kubernetes preflight must not inspect local PIDs"),
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._compose_worker_processes",
        lambda _project: pytest.fail("Kubernetes preflight must not inspect Docker"),
    )

    args = SimpleNamespace(
        kubernetes=True,
        kubernetes_namespace="acceptance",
        kubernetes_context="ack-context",
        kubernetes_kubeconfig="C:/Users/test/.kube/config",
        kubernetes_source_fingerprint=source,
        kubernetes_image_digest=image,
        kubernetes_memory_limit_bytes=None,
        callbacks=200,
        burst_turns=200,
        callback_rate=100.0,
        min_workers=4,
        db_pool_size=32,
        max_inflight=DEFAULT_MAX_INFLIGHT,
        timeout_seconds=300.0,
    )

    result = _preflight(args)

    assert result["status"] == "pass"
    assert len(result["worker_processes"]) == 4
    assert result["worker_count"] == 4
    assert result["worker_concurrency"] == 50
    assert all("pid" not in worker for worker in result["worker_processes"])
    assert result["worker_image_attestation"]["status"] == "pass"
    assert result["service_image_attestation"]["outbox-dispatcher"]["status"] == "pass"
    assert result["memory_observation"]["sampling_method"] == "kubernetes_metrics_api"
    assert result["memory_observation"]["coverage_complete"] is True


def test_preflight_rejects_runtime_connection_budget_before_worker_inspection(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.real_performance_gate._worker_processes",
        lambda: pytest.fail("worker inspection must not start after a connection-budget refusal"),
    )
    args = SimpleNamespace(
        callbacks=200,
        burst_turns=200,
        callback_rate=100.0,
        min_workers=8,
        db_pool_size=64,
        max_inflight=64,
        timeout_seconds=300.0,
    )

    result = _preflight(args)

    assert result["status"] == "not_run"
    assert "PostgreSQL connections" in result["reason"]
    assert (
        result["resources"]["estimated_runtime_connections"]
        > result["resources"]["max_estimated_runtime_connections"]
    )


def test_required_memory_is_conservative_monotonic_and_bounded() -> None:
    baseline = _required_memory_bytes(db_pool_size=2, max_inflight=1, burst_turns=1)
    default = _required_memory_bytes(db_pool_size=32, max_inflight=32, burst_turns=200)
    larger = _required_memory_bytes(db_pool_size=64, max_inflight=64, burst_turns=500)
    extreme = _required_memory_bytes(db_pool_size=10**9, max_inflight=10**9, burst_turns=10**9)

    assert baseline >= 2 * 1024**3
    assert baseline < default < larger < extreme
    assert extreme == 4 * 1024**3


def test_memory_observation_requires_every_performance_service_role(monkeypatch) -> None:
    participating = {
        "worker": ({"role": "worker", "pid": 101},),
        "outbox-dispatcher": ({"role": "outbox-dispatcher", "pid": 102},),
    }

    monkeypatch.setattr(
        "scripts.real_performance_gate._process_memory_snapshot",
        lambda pid: {
            "pid": pid,
            "rss_bytes": 32 * 1024**2,
            "rss_peak_bytes": 64 * 1024**2,
            "cgroup_current_bytes": None,
            "cgroup_peak_bytes": None,
            "cgroup_limit_bytes": None,
            "cgroup_key": None,
        },
    )

    result = _memory_observation(
        participating,
        resources={"available_memory_bytes": 8 * 1024**3},
    )

    assert result["status"] == "pass"
    assert result["coverage_complete"] is True
    assert result["observed_identity_count"] == 2
    assert result["peak_bytes"] == 2 * 64 * 1024**2


def test_memory_observation_fails_closed_when_a_role_has_no_peak(monkeypatch) -> None:
    participating = {
        "worker": ({"role": "worker", "pid": 101},),
        "outbox-dispatcher": ({"role": "outbox-dispatcher", "pid": 102},),
    }

    monkeypatch.setattr(
        "scripts.real_performance_gate._process_memory_snapshot",
        lambda pid: {
            "pid": pid,
            "rss_bytes": None,
            "rss_peak_bytes": None,
            "cgroup_current_bytes": None,
            "cgroup_peak_bytes": None,
            "cgroup_limit_bytes": None,
            "cgroup_key": None,
        },
    )

    result = _memory_observation(
        participating,
        resources={"available_memory_bytes": 8 * 1024**3},
    )

    assert result["status"] == "not_run"
    assert result["coverage_complete"] is False


def test_memory_observation_uses_container_kernel_counters_when_host_proc_is_hidden(
    monkeypatch,
) -> None:
    participating = {
        "worker": ({"role": "worker", "pid": 101, "container_id": "a" * 64},),
        "outbox-dispatcher": ({"role": "outbox-dispatcher", "pid": 102, "container_id": "b" * 64},),
    }
    missing = {
        "rss_peak_bytes": None,
        "cgroup_peak_bytes": None,
    }
    monkeypatch.setattr(
        "scripts.real_performance_gate._process_memory_snapshot", lambda _pid: missing
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._container_memory_snapshot",
        lambda container_id: {
            "rss_peak_bytes": 64 * 1024**2,
            "cgroup_peak_bytes": 96 * 1024**2,
            "cgroup_current_bytes": 48 * 1024**2,
            "cgroup_limit_bytes": 512 * 1024**2,
            "cgroup_key": f"docker:{container_id}",
            "sampling_source": "container_procfs",
        },
    )

    result = _memory_observation(participating)

    assert result["status"] == "pass"
    assert result["coverage_complete"] is True
    assert result["peak_cgroup_bytes"] == 2 * 96 * 1024**2
    assert all(
        item["sampling_source"] == "container_procfs"
        for role in result["role_observations"].values()
        for item in role["observations"]
    )


def test_hard_process_timeout_is_bounded() -> None:
    assert _hard_process_timeout(SimpleNamespace(timeout_seconds=1.0)) >= 30.0
    assert _hard_process_timeout(SimpleNamespace(timeout_seconds=10_000.0)) == (
        MAX_LOAD_PROCESS_TIMEOUT_SECONDS
    )
    assert _hard_process_timeout(SimpleNamespace(timeout_seconds=float("nan"))) == 0.0
    assert _hard_process_timeout(SimpleNamespace(timeout_seconds=float("inf"))) == 0.0


def test_preflight_rejects_non_finite_timeout_before_process_inspection(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate._worker_processes", lambda: ())
    for timeout_seconds in (float("nan"), float("inf"), float("-inf")):
        args = SimpleNamespace(
            callbacks=200,
            burst_turns=200,
            callback_rate=100.0,
            min_workers=2,
            db_pool_size=32,
            max_inflight=DEFAULT_MAX_INFLIGHT,
            timeout_seconds=timeout_seconds,
        )

        assert _preflight(args) == {
            "status": "fail",
            "reason": "timeout must be finite and positive",
        }


def test_external_load_supervisor_starts_one_child_and_records_supervision(
    monkeypatch, tmp_path
) -> None:
    calls: list[list[str]] = []

    class FakeProcess:
        def __init__(self, command, **_kwargs) -> None:
            calls.append(command)
            output = Path(command[command.index("--output") + 1])
            output.write_text(
                json.dumps(_not_run_report(["offline child fixture"])), encoding="utf-8"
            )

        def wait(self, timeout=None) -> int:
            del timeout
            return 0

        def poll(self) -> int:
            return 0

        def kill(self) -> None:
            raise AssertionError("completed child must not be killed")

    monkeypatch.setattr("scripts.real_performance_gate.subprocess.Popen", FakeProcess)
    args = _parser().parse_args(
        [
            "--execute",
            "--confirm-real-load",
            "--output",
            str(tmp_path / "parent.json"),
        ]
    )
    report = _run_external_load(args, {"status": "pass"})

    assert len(calls) == 1
    assert "--load-worker" in calls[0]
    assert report["candidate"]["supervision"]["load_generator"] == ("single_external_process")


def test_external_load_supervisor_kills_only_timed_out_child(monkeypatch) -> None:
    class TimeoutProcess:
        killed = False

        def __init__(self, _command, **_kwargs) -> None:
            pass

        def wait(self, timeout=None) -> int:
            if not self.killed:
                raise subprocess.TimeoutExpired(cmd="load-worker", timeout=timeout)
            return -9

        def poll(self) -> int | None:
            return -9 if self.killed else None

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr("scripts.real_performance_gate.subprocess.Popen", TimeoutProcess)
    args = _parser().parse_args(["--execute", "--confirm-real-load"])

    report = _run_external_load(args, {"status": "pass"})

    assert report["gate"] == "fail"
    assert "hard timeout" in report["rejection_reasons"][0]


def test_external_load_supervisor_reports_kill_wait_timeout(monkeypatch) -> None:
    class StuckProcess:
        def __init__(self, _command, **_kwargs) -> None:
            self.killed = False

        def wait(self, timeout=None) -> int:
            del timeout
            raise subprocess.TimeoutExpired(cmd="load-worker", timeout=1)

        def poll(self) -> int | None:
            return None

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr("scripts.real_performance_gate.subprocess.Popen", StuckProcess)
    args = _parser().parse_args(["--execute", "--confirm-real-load"])

    report = _run_external_load(args, {"status": "pass"})

    assert report["gate"] == "fail"
    assert "did not terminate" in report["rejection_reasons"][0]
    assert report["candidate"]["supervision"]["termination"] == {
        "status": "unconfirmed",
        "wait_seconds": 10.0,
        "error_type": "TimeoutExpired",
    }


def test_external_load_supervisor_reports_residual_child_from_finally(
    monkeypatch,
) -> None:
    class LingeringProcess:
        def __init__(self, command, **_kwargs) -> None:
            output = Path(command[command.index("--output") + 1])
            output.write_text(
                json.dumps(_not_run_report(["offline child fixture"])), encoding="utf-8"
            )
            self.wait_calls = 0

        def wait(self, timeout=None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1:
                return 0
            raise subprocess.TimeoutExpired(cmd="load-worker", timeout=timeout)

        def poll(self) -> int | None:
            return None

        def kill(self) -> None:
            return None

    monkeypatch.setattr("scripts.real_performance_gate.subprocess.Popen", LingeringProcess)
    args = _parser().parse_args(["--execute", "--confirm-real-load"])

    report = _run_external_load(args, {"status": "pass"})

    assert report["gate"] == "fail"
    assert report["production_gate"] == "fail"
    assert report["candidate"]["supervision"]["cleanup_termination"] == {
        "status": "unconfirmed",
        "wait_seconds": 10.0,
        "error_type": "TimeoutExpired",
    }
    assert "termination could not be confirmed" in report["rejection_reasons"][-1]


def test_parent_pid_requires_a_distinct_valid_process(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_REAL_PERFORMANCE_PARENT_PID", raising=False)
    assert _load_worker_parent_pid() is None
    monkeypatch.setenv("TRPC_REAL_PERFORMANCE_PARENT_PID", "not-a-pid")
    assert _load_worker_parent_pid() is None
    monkeypatch.setenv("TRPC_REAL_PERFORMANCE_PARENT_PID", str(os.getpid()))
    assert _load_worker_parent_pid() is None
    monkeypatch.setenv("TRPC_REAL_PERFORMANCE_PARENT_PID", "123456")
    assert _load_worker_parent_pid() == 123456


def test_load_worker_refuses_to_run_without_parent_watchdog(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_REAL_PERFORMANCE_PARENT_PID", raising=False)
    args = _parser().parse_args(["--load-worker", "--execute", "--confirm-real-load"])

    report = asyncio.run(_run_real(args, {"status": "pass"}))

    assert report["gate"] == "not_run"
    assert "parent PID" in report["rejection_reasons"][0]


def test_kubernetes_load_worker_uses_attested_job_supervision_without_host_pid(monkeypatch) -> None:
    called = False

    async def fake_run_real_once(_args, preflight):
        nonlocal called
        called = True
        assert preflight["status"] == "pass"
        return _not_run_report(["job fixture"])

    monkeypatch.setattr("scripts.real_performance_gate._run_real_once", fake_run_real_once)
    monkeypatch.setenv("TRPC_REAL_PERFORMANCE_WORKER_TOKEN", "job-token")
    args = _parser().parse_args(
        ["--load-worker", "--kubernetes-load-worker", "--execute", "--confirm-real-load"]
    )

    report = asyncio.run(_run_real(args, {"status": "pass"}))

    assert called is True
    assert report["gate"] == "not_run"


def test_parent_watchdog_cancels_worker_when_parent_disappears(monkeypatch) -> None:
    class FakeWorkerTask:
        def __init__(self) -> None:
            self.cancelled = False

        def cancel(self) -> bool:
            self.cancelled = True
            return True

    async def run() -> None:
        worker = FakeWorkerTask()
        parent_gone = asyncio.Event()
        monkeypatch.setattr("scripts.real_performance_gate._pid_alive", lambda _pid: False)
        watchdog = asyncio.create_task(
            _watch_parent_process(123456, parent_gone, worker),
        )
        await asyncio.wait_for(watchdog, timeout=PARENT_WATCH_INTERVAL_SECONDS * 4)
        assert parent_gone.is_set()
        assert worker.cancelled is True

    asyncio.run(run())


def test_wait_for_batch_does_not_fabricate_redis_pending_after_error(monkeypatch) -> None:
    async def fake_batch_state(*_args, **_kwargs):
        return {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
            "published_inbound_outbox": 1,
            "leased_sessions": 0,
        }

    class FailingRedis:
        async def execute_command(self, *_args):
            raise RedisError("redis unavailable")

    monkeypatch.setattr("scripts.real_performance_gate._batch_state", fake_batch_state)
    result = asyncio.run(
        _wait_for_batch(
            None,
            FailingRedis(),
            tenant_id="tenant",
            inbound_ids=["inbound"],
            session_ids=["session"],
            baseline_pending=0,
            stream="stream",
            group="group",
            wait_seconds=0.01,
        )
    )

    assert result["status"] == "fail"
    assert result["redis"] is None
    assert result["redis_failure"] == {"error_type": "RedisError"}


def test_wait_for_batch_reports_sanitized_postgres_failure(monkeypatch) -> None:
    async def failing_batch_state(*_args, **_kwargs):
        raise asyncpg.PostgresError("postgresql://user:secret@example.invalid/database")

    class HealthyRedis:
        async def execute_command(self, *_args):
            return [0]

        async def xlen(self, _stream):
            return 0

    monkeypatch.setattr("scripts.real_performance_gate._batch_state", failing_batch_state)
    result = asyncio.run(
        _wait_for_batch(
            None,
            HealthyRedis(),
            tenant_id="tenant",
            inbound_ids=["inbound"],
            session_ids=["session"],
            baseline_pending=0,
            stream="stream",
            group="group",
            wait_seconds=0.01,
        )
    )

    assert result["status"] == "fail"
    assert result["postgres_failure"] == {"error_type": "PostgresError"}
    assert "secret" not in json.dumps(result)


def test_v2_batch_state_counts_published_session_ready_outbox(monkeypatch) -> None:
    queries: list[str] = []
    session_arguments: list[list[str]] = []

    async def fake_scoped_fetch(_pool, _tenant_scope, query, *_args):
        queries.append(query)
        if "FROM inbound_messages" in query:
            return [{"status": "committed", "count": 2}]
        if "FROM outbox_events" in query:
            session_arguments.append(list(_args[-1]))
            return [(1,)]
        if "FROM sessions" in query:
            return [(0,)]
        if "FROM session_mailboxes" in query:
            return [
                {
                    "row_count": 1,
                    "idle_count": 1,
                    "settled_count": 1,
                    "unresolved_item_count": 0,
                }
            ]
        if "SELECT started_at,committed_at" in query:
            return []
        if "FROM session_turns" in query:
            return [{"status": "committed", "count": 2}]
        return []

    monkeypatch.setattr("scripts.real_performance_gate._scoped_fetch", fake_scoped_fetch)
    state = asyncio.run(
        _batch_state(
            None,
            "tenant",
            [str(uuid4()), str(uuid4())],
            ["session-a", "session-a"],
            scheduler_version=SchedulerVersion.V2,
        )
    )

    outbox_query = next(query for query in queries if "FROM outbox_events" in query)
    turn_query = next(query for query in queries if "FROM session_turns" in query)
    assert "aggregate_type='session'" in outbox_query
    assert "event_type='session.ready.v2'" in outbox_query
    assert "GROUP BY normalized_status" in turn_query
    assert session_arguments == [["session-a"]]
    assert state["published_scheduler_outbox"] == 1
    assert state["scheduler_version"] == "v2"
    assert state["mailbox_expected_count"] == 1
    assert state["mailbox_row_count"] == 1
    assert state["mailbox_idle_count"] == 1
    assert state["mailbox_settled_count"] == 1
    assert state["mailbox_unresolved_item_count"] == 0


def test_wait_for_v2_batch_uses_pending_not_stream_history(monkeypatch) -> None:
    async def fake_batch_state(*_args, **_kwargs):
        return {
            "inbound_statuses": {"committed": 1},
            "turn_statuses": {"committed": 1},
            "published_inbound_outbox": 1,
            "published_scheduler_outbox": 1,
            "scheduler_version": "v2",
            "leased_sessions": 0,
            "mailbox_expected_count": 1,
            "mailbox_row_count": 1,
            "mailbox_idle_count": 1,
            "mailbox_settled_count": 1,
            "mailbox_unresolved_item_count": 0,
        }

    class HistoricalRedis:
        async def execute_command(self, *_args):
            return [0]

        async def xlen(self, _stream):
            return 500

    monkeypatch.setattr("scripts.real_performance_gate._batch_state", fake_batch_state)
    result = asyncio.run(
        _wait_for_batch(
            None,
            HistoricalRedis(),
            tenant_id="tenant",
            inbound_ids=["inbound"],
            session_ids=["session"],
            baseline_pending=0,
            stream="trpc:session-ready:v2",
            group="trpc-session-ready-v2",
            wait_seconds=0.1,
            scheduler_version=SchedulerVersion.V2,
        )
    )

    assert result["status"] == "pass"
    assert result["redis"] == {"pending": 0, "stream_length": 500}


def test_submit_batch_reports_actual_max_inflight_accepts(monkeypatch) -> None:
    async def fake_accept(*_args, **_kwargs):
        await asyncio.sleep(0.01)
        return {"status": "fail", "latency_ms": 1.0}

    monkeypatch.setattr("scripts.real_performance_gate._accept_one", fake_accept)
    route = SimpleNamespace(binding=SimpleNamespace(channel="feishu", account_id="account"))

    result = asyncio.run(
        _submit_batch(
            object(),
            route,
            binding_id="binding",
            prefix="batch",
            count=3,
            offered_rate=None,
        )
    )

    assert result["max_inflight_accepts"] == 3
    assert result["concurrency_scope"] == "load_generator_accept_requests"
    assert result["agent_turn_overlap_source"] == "authoritative_session_turn_intervals"
    assert result["callback_submission_started_at"] is not None
    assert result["actual_offered_start_rate_per_second"] > 0
    assert "observed_submission_rate_per_second" not in result


def test_submit_batch_hard_timeout_cancels_pending_requests(monkeypatch) -> None:
    async def hanging_accept(*_args, **_kwargs):
        await asyncio.sleep(60)

    monkeypatch.setattr("scripts.real_performance_gate._accept_one", hanging_accept)
    route = SimpleNamespace(binding=SimpleNamespace(channel="feishu", account_id="account"))

    result = asyncio.run(
        _submit_batch(
            object(),
            route,
            binding_id="binding",
            prefix="timeout",
            count=4,
            offered_rate=None,
            max_inflight=2,
            timeout_seconds=0.01,
        )
    )

    assert result["timed_out"] is True
    assert result["errors"] == 4
    assert result["max_inflight_accepts"] <= 2


def test_not_run_report_exposes_locked_performance_baseline() -> None:
    report = _not_run_report(["offline environment"])

    assert report["baseline"]["sustained_callbacks"] == 200
    assert report["baseline"]["offered_callback_rate_per_second"] == 100.0
    assert report["baseline"]["callback_ack_p95_ms_max"] == 200.0
    assert report["baseline"]["concurrent_turns"] == 200
    assert report["baseline"]["independent_worker_processes_min"] == 4
    assert report["production_gate"] == "not_run"


def test_report_records_performance_gate_parameters() -> None:
    args = SimpleNamespace(db_pool_size=32, min_workers=4, timeout_seconds=17.5)

    report = _not_run_report(["offline environment"], args)

    assert report["candidate"]["parameters"] == {
        "callbacks": 200,
        "callback_rate_per_second": 105.0,
        "burst_turns": 200,
        "target_max_turn_overlap": 200,
        "db_pool_size": 32,
        "min_workers": 4,
        "max_inflight": DEFAULT_MAX_INFLIGHT,
        "max_inflight_accepts": DEFAULT_MAX_INFLIGHT,
        "db_pool_scope": "load_generator_only",
        "timeout_seconds": 17.5,
        "scheduler_version": "v2",
        "redis_stream": DEFAULT_STREAM,
        "redis_group": DEFAULT_GROUP,
    }


def test_pool_prewarm_runs_before_load_generation() -> None:
    executed: list[str] = []

    class Connection:
        async def execute(self, query: str) -> None:
            executed.append(query)

    class Acquire:
        async def __aenter__(self) -> Connection:
            return Connection()

        async def __aexit__(self, *_args) -> None:
            return None

    class Pool:
        def acquire(self) -> Acquire:
            return Acquire()

    asyncio.run(_prewarm_pool(Pool()))

    assert executed == ["SELECT 1"]


def test_explicit_worker_pid_override_uses_only_live_pids(monkeypatch) -> None:
    monkeypatch.setenv(
        "TRPC_REAL_WORKER_PIDS",
        f"{os.getpid()},{os.getpid()},not-a-pid,1",
    )

    from scripts.real_performance_gate import _worker_processes

    assert _worker_processes() == ({"pid": os.getpid()},)


def test_worker_discovery_uses_the_performance_compose_project(monkeypatch) -> None:
    discovered = ({"container_id": "worker-1", "pid": 101},)
    seen: list[str] = []

    def fake_compose_workers(project: str):
        seen.append(project)
        return discovered

    monkeypatch.setenv("TRPC_PERF_COMPOSE_PROJECT", "trpc-perf-current")
    monkeypatch.setattr(
        "scripts.real_performance_gate._compose_worker_processes", fake_compose_workers
    )

    from scripts.real_performance_gate import _worker_processes

    assert _worker_processes() == discovered
    assert seen == ["trpc-perf-current"]


def test_worker_discovery_rejects_conflicting_compose_projects(monkeypatch) -> None:
    monkeypatch.setenv("TRPC_REAL_COMPOSE_PROJECT", "trpc-perf-old")
    monkeypatch.setenv("TRPC_PERF_COMPOSE_PROJECT", "trpc-perf-current")
    monkeypatch.setenv("TRPC_REAL_WORKER_PIDS", str(os.getpid()))
    monkeypatch.setattr(
        "scripts.real_performance_gate._compose_worker_processes",
        lambda _project: (_ for _ in ()).throw(
            AssertionError("conflicting projects must fail before Docker discovery")
        ),
    )

    from scripts.real_performance_gate import _worker_processes

    assert _worker_processes() == ()


def test_windows_pid_probe_never_sends_a_signal(monkeypatch) -> None:
    monkeypatch.setattr("scripts.real_performance_gate.os.name", "nt")
    monkeypatch.setattr("scripts.real_performance_gate._windows_pid_alive", lambda pid: pid == 123)

    def forbidden_kill(_pid: int, _signal: int) -> None:
        raise AssertionError("Windows PID probing must not call os.kill")

    monkeypatch.setattr("scripts.real_performance_gate.os.kill", forbidden_kill)

    assert _pid_alive(123) is True
    assert _pid_alive(456) is False


def test_posix_pid_probe_reads_proc_stat_and_rejects_zombies(monkeypatch) -> None:
    import scripts.real_performance_gate as gate

    class FakeProcStat:
        state = "R"

        def __init__(self, _path: str) -> None:
            pass

        def exists(self) -> bool:
            return True

        def read_text(self, *, encoding: str) -> str:
            assert encoding == "ascii"
            return f"123 (worker with ) name) {self.state} 1 1 1"

    monkeypatch.setattr(gate.os, "name", "posix")
    monkeypatch.setattr(gate, "Path", FakeProcStat)

    FakeProcStat.state = "R"
    assert _pid_alive(123) is True
    FakeProcStat.state = "Z"
    assert _pid_alive(123) is False


def test_compose_worker_discovery_uses_running_independent_containers(monkeypatch) -> None:
    class Completed:
        def __init__(self, stdout: str, returncode: int = 0) -> None:
            self.returncode = 0
            self.stdout = stdout

    def fake_run(command, **_kwargs):
        if command[1] == "ps":
            return Completed("short-a\nshort-b\n")
        if command[-1] == "short-a":
            container = {
                "Id": "full-a",
                "Image": "sha256:" + "a" * 64,
                "State": {"Pid": 101, "Running": True},
                "Config": {"Labels": {IMAGE_SOURCE_FINGERPRINT_LABEL: "a" * 64}},
            }
            return Completed(json.dumps(container))
        container = {
            "Id": "full-b",
            "Image": "sha256:" + "a" * 64,
            "State": {"Pid": 202, "Running": True},
            "Config": {"Labels": {IMAGE_SOURCE_FINGERPRINT_LABEL: "a" * 64}},
        }
        return Completed(json.dumps(container))

    monkeypatch.setattr("scripts.real_performance_gate.shutil.which", lambda _name: "docker")
    monkeypatch.setattr("scripts.real_performance_gate.subprocess.run", fake_run)

    assert _compose_worker_processes("isolated-performance") == (
        {
            "container_id": "full-a",
            "pid": 101,
            "image_id": "sha256:" + "a" * 64,
            "image_digest": "sha256:" + "a" * 64,
            "source_fingerprint": "a" * 64,
        },
        {
            "container_id": "full-b",
            "pid": 202,
            "image_id": "sha256:" + "a" * 64,
            "image_digest": "sha256:" + "a" * 64,
            "source_fingerprint": "a" * 64,
        },
    )


@pytest.mark.parametrize(
    ("images", "labels", "expected_status"),
    [
        (("sha256:" + "a" * 64,) * 4, (None,) * 4, "not_run"),
        (("sha256:" + "a" * 64,) * 4, ("b" * 64,) * 4, "not_run"),
        (
            (
                "sha256:" + "a" * 64,
                "sha256:" + "a" * 64,
                "sha256:" + "b" * 64,
                "sha256:" + "b" * 64,
            ),
            ("a" * 64,) * 4,
            "not_run",
        ),
        (("sha256:" + "a" * 64,) * 4, ("a" * 64,) * 4, "pass"),
    ],
)
def test_fake_docker_inspect_image_evidence_covers_missing_stale_mixed_and_current(
    monkeypatch,
    images: tuple[str, ...],
    labels: tuple[str | None, ...],
    expected_status: str,
) -> None:
    class Completed:
        returncode = 0

        def __init__(self, stdout: str) -> None:
            self.stdout = stdout

    payloads = {
        f"short-{index}": {
            "Id": f"full-{index}",
            "Image": image,
            "State": {"Pid": 100 + index, "Running": True},
            "Config": {
                "Labels": ({IMAGE_SOURCE_FINGERPRINT_LABEL: label} if label is not None else {})
            },
        }
        for index, (image, label) in enumerate(zip(images, labels, strict=True))
    }

    def fake_run(command, **_kwargs):
        if command[1] == "ps":
            return Completed("\n".join(payloads))
        return Completed(json.dumps(payloads[command[-1]]))

    monkeypatch.setattr("scripts.real_performance_gate.shutil.which", lambda _name: "docker")
    monkeypatch.setattr("scripts.real_performance_gate.subprocess.run", fake_run)

    workers = _compose_worker_processes("isolated-performance")
    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result["status"] == expected_status


def _attested_workers(
    *, image_ids: tuple[str, ...], labels: tuple[str, ...]
) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "container_id": f"container-{index}",
            "pid": index + 100,
            "image_id": image_id,
            "source_fingerprint": label,
        }
        for index, (image_id, label) in enumerate(zip(image_ids, labels, strict=True))
    )


def test_worker_image_attestation_rejects_missing_label() -> None:
    workers = _attested_workers(image_ids=("sha256:" + "a" * 64,) * 4, labels=("",) * 4)
    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result["status"] == "not_run"
    assert "source_fingerprint" in result["reason"]


def test_worker_image_attestation_rejects_stale_label() -> None:
    workers = _attested_workers(image_ids=("sha256:" + "a" * 64,) * 4, labels=("b" * 64,) * 4)
    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result["status"] == "not_run"
    assert "stale" in result["reason"]


def test_worker_image_attestation_rejects_mixed_images() -> None:
    workers = _attested_workers(
        image_ids=(
            "sha256:" + "a" * 64,
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
            "sha256:" + "b" * 64,
        ),
        labels=("a" * 64,) * 4,
    )
    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result["status"] == "not_run"
    assert "mixed image" in result["reason"]


def test_worker_image_attestation_accepts_four_identical_current_workers() -> None:
    workers = _attested_workers(image_ids=("sha256:" + "a" * 64,) * 4, labels=("a" * 64,) * 4)
    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result == {
        "status": "pass",
        "worker_count": 4,
        "image_count": 1,
        "source_fingerprint_matches": True,
    }


def test_worker_image_attestation_rejects_non_digest_image_id() -> None:
    workers = _attested_workers(image_ids=("sha256:image-a",) * 4, labels=("a" * 64,) * 4)

    result = _worker_image_attestation(
        workers,
        {"algorithm": "sha256", "status": "available", "value": "a" * 64},
    )

    assert result["status"] == "not_run"
    assert "image_id" in result["reason"]


def test_kubernetes_pod_records_use_only_ready_pod_api_identity(monkeypatch) -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    payload = _kubernetes_pod_payload(
        role="worker",
        name="trpc-worker-0",
        image_digest=image,
        source_fingerprint=source,
        memory_limit="2Gi",
    )
    calls: list[list[str]] = []

    def fake_json(arguments, _configuration):
        calls.append(list(arguments))
        return payload, None

    monkeypatch.setattr("scripts.real_performance_gate._kubernetes_json", fake_json)
    records, error = _kubernetes_pod_records(
        {"namespace": "acceptance", "context": "ack-context"},
        role="worker",
        selector=KUBERNETES_WORKER_SELECTOR,
    )

    assert error is None
    assert records == (
        {
            "role": "worker",
            "pod_name": "trpc-worker-0",
            "pod_uid": "uid-trpc-worker-0",
            "container_name": "worker",
            "container_id": "containerd://trpc-worker-0",
            "image_id": image,
            "image_digest": image,
            "source_fingerprint": source,
            "node_name": "acceptance-node",
            "process_count": 1,
            "memory_limit_bytes": 2 * 1024**3,
            "ready": True,
        },
    )
    assert calls == [
        [
            "get",
            "pods",
            "--namespace",
            "acceptance",
            "--selector",
            KUBERNETES_WORKER_SELECTOR,
            "--output",
            "json",
        ]
    ]


def test_kubernetes_pod_records_fail_closed_for_unready_or_tagged_image(monkeypatch) -> None:
    payload = _kubernetes_pod_payload(
        role="outbox-dispatcher",
        name="trpc-outbox-0",
        image_digest="sha256:" + "b" * 64,
    )
    pod = payload["items"][0]
    assert isinstance(pod, dict)
    status = pod["status"]
    assert isinstance(status, dict)
    statuses = status["containerStatuses"]
    assert isinstance(statuses, list)
    assert isinstance(statuses[0], dict)
    statuses[0]["ready"] = False
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_json", lambda *_args: (payload, None)
    )

    records, error = _kubernetes_pod_records(
        {"namespace": "acceptance", "context": "ack-context"},
        role="outbox-dispatcher",
        selector=KUBERNETES_OUTBOX_SELECTOR,
    )
    assert records == ()
    assert error is not None and "not ready" in error


@pytest.mark.parametrize(
    ("value", "expected"),
    [("128Mi", 128 * 1024**2), ("1Gi", 1024**3), ("250M", 250_000_000), ("0", None)],
)
def test_kubernetes_quantity_bytes_is_bounded(value: str, expected: int | None) -> None:
    assert _kubernetes_quantity_bytes(value) == expected


def test_kubernetes_image_attestation_binds_expected_digest_and_source() -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    workers = (
        {"container_id": "container-0", "image_id": image, "source_fingerprint": source},
        {"container_id": "container-1", "image_id": image, "source_fingerprint": source},
    )

    result = _kubernetes_image_attestation(
        workers,
        expected_source=source,
        expected_image=image,
    )

    assert result["status"] == "pass"
    assert result["independent_process_count"] == 2
    assert result["image_id"] == image
    assert result["source_fingerprint"] == source
    assert result["binding_method"] == "configured_candidate"

    mismatch = _kubernetes_image_attestation(
        workers,
        expected_source=source,
        expected_image="sha256:" + "c" * 64,
    )
    assert mismatch["status"] == "not_run"
    assert "does not match" in mismatch["reason"]


def test_kubernetes_image_attestation_can_use_consistent_pod_labels() -> None:
    source = "a" * 64
    image = "sha256:" + "b" * 64
    result = _kubernetes_image_attestation(
        (
            {"container_id": "container-0", "image_id": image, "source_fingerprint": source},
            {"container_id": "container-1", "image_id": image, "source_fingerprint": source},
        ),
        expected_source=None,
        expected_image=None,
    )

    assert result["status"] == "pass"
    assert result["binding_method"] == "pod_label"


def test_worker_discovery_uses_kubernetes_pods_when_kubernetes_env_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("TRPC_PERF_K8S_ENABLED", "true")
    monkeypatch.setenv("TRPC_PERF_K8S_NAMESPACE", "acceptance")
    monkeypatch.setenv("TRPC_PERF_K8S_CONTEXT", "ack-context")
    discovered = (
        {
            "role": "worker",
            "pod_name": "worker-0",
            "pod_uid": "uid-worker-0",
            "container_name": "worker",
            "container_id": "containerd://worker-0",
            "image_id": "sha256:" + "a" * 64,
            "process_count": 1,
        },
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._kubernetes_pod_records",
        lambda _configuration, *, role, selector: (discovered, None),
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate._compose_worker_processes",
        lambda _project: pytest.fail("Kubernetes discovery must not inspect Docker"),
    )
    monkeypatch.setattr(
        "scripts.real_performance_gate.shutil.which",
        lambda name: pytest.fail(f"Kubernetes discovery must not inspect local tools: {name}"),
    )

    from scripts.real_performance_gate import _worker_processes

    assert _worker_processes() == discovered


def test_kubernetes_metrics_memory_observation_binds_role_pods() -> None:
    image = "sha256:" + "b" * 64
    participating = {
        "worker": (
            {
                "role": "worker",
                "pod_name": "worker-0",
                "pod_uid": "uid-worker-0",
                "container_name": "worker",
                "container_id": "containerd://worker-0",
                "image_id": image,
                "memory_limit_bytes": 2 * 1024**3,
            },
        ),
        "outbox-dispatcher": (
            {
                "role": "outbox-dispatcher",
                "pod_name": "outbox-0",
                "pod_uid": "uid-outbox-0",
                "container_name": "outbox-dispatcher",
                "container_id": "containerd://outbox-0",
                "image_id": image,
                "memory_limit_bytes": 1024**3,
            },
        ),
    }
    metrics = {
        "items": [
            {
                "metadata": {"name": "worker-0"},
                "timestamp": "2026-08-26T00:00:15Z",
                "window": "15s",
                "containers": [{"name": "worker", "usage": {"memory": "128Mi"}}],
            },
            {
                "metadata": {"name": "outbox-0"},
                "timestamp": "2026-08-26T00:00:15Z",
                "window": "15s",
                "containers": [{"name": "outbox-dispatcher", "usage": {"memory": "64Mi"}}],
            },
        ]
    }

    result = _kubernetes_metrics_memory_observation(participating, metrics)

    assert result["status"] == "pass"
    assert result["sampling_method"] == "kubernetes_metrics_api"
    assert result["metrics_api"] == KUBERNETES_METRICS_API
    assert result["coverage_complete"] is True
    assert result["observed_identity_count"] == 2
    assert result["peak_bytes"] == 192 * 1024**2
    assert result["safety_threshold_bytes"] == 3 * 1024**3
    assert result["sampling_interval_seconds"] == 15.0


def test_kubernetes_metrics_memory_observation_missing_container_is_not_run() -> None:
    participating = {
        "worker": (
            {"pod_name": "worker-0", "container_name": "worker", "memory_limit_bytes": 1024},
        ),
        "outbox-dispatcher": (
            {
                "pod_name": "outbox-0",
                "container_name": "outbox-dispatcher",
                "memory_limit_bytes": 1024,
            },
        ),
    }

    result = _kubernetes_metrics_memory_observation(
        participating,
        {
            "items": [
                {
                    "metadata": {"name": "worker-0"},
                    "containers": [{"name": "worker", "usage": {"memory": "1Mi"}}],
                }
            ]
        },
    )

    assert result["status"] == "not_run"
    assert result["coverage_complete"] is False
    assert "coverage" in result["reason"]
