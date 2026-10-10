from __future__ import annotations

import base64
import json
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import scripts.ack_performance_acceptance as acceptance
import scripts.kubernetes_performance_job as performance_job


def _spec(tmp_path: Path, **overrides: object) -> performance_job.PerformanceJobSpec:
    values: dict[str, Any] = {
        "namespace": "runtime-gate",
        "context": "ack-test",
        "kubeconfig": tmp_path / "config",
        "image": "registry.example/trpc-agent@sha256:" + "a" * 64,
        "image_pull_secret": "registry-pull",
        "source_fingerprint": "b" * 64,
        "run_id": "c" * 32,
        "secret_env": {
            "TRPC_SERVICE_DATABASE_DSN": "postgresql://runtime@postgres/db",
            "TRPC_SERVICE_REDIS_URL": "redis://redis:6379/0",
            "TRPC_SERVICE_SESSION_HMAC_KEY": "h" * 32,
            performance_job.WORKER_TOKEN_ENV: "t" * 32,
        },
        "config_env": {
            "TRPC_PERF_GATEWAY_BASE_URL": "http://trpc-gateway.runtime-gate.svc:8080",
            "TRPC_RUN_REAL_MULTINODE": "1",
            "TRPC_K8S_PERF_RUN_ID": "c" * 32,
        },
        "command": ("python", "scripts/kubernetes_performance_job.py", "--worker"),
        "preflight_evidence": {
            "schema_version": 1,
            "run_id": "c" * 32,
            "source_fingerprint": "b" * 64,
            "image_digest": "sha256:" + "a" * 64,
            "preflight": {"status": "pass"},
        },
    }
    values.update(overrides)
    return performance_job.build_spec(**values)


def test_acceptance_preflight_projects_runtime_values_to_formal_gate_args(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, object] = {}

    class FakeParser:
        def parse_args(self, argv: list[str]) -> object:
            captured["argv"] = argv
            return object()

    monkeypatch.setattr(acceptance.real_performance_gate, "_parser", lambda: FakeParser())
    monkeypatch.setattr(
        acceptance.real_performance_gate,
        "_preflight",
        lambda _args: {"status": "pass", "namespace": "trpc-service"},
    )

    result = acceptance._preflight(
        namespace="trpc-service",
        context="prod-context",
        kubeconfig=tmp_path / "config",
        image_digest="sha256:" + "a" * 64,
        source="b" * 64,
        max_inflight=19,
        db_pool_size=23,
        min_workers=7,
        timeout_seconds=42.0,
    )

    assert result["status"] == "pass"
    argv = captured["argv"]
    assert isinstance(argv, list)
    expected = {
        "--kubernetes-namespace": "trpc-service",
        "--kubernetes-context": "prod-context",
        "--kubernetes-image-digest": "sha256:" + "a" * 64,
        "--kubernetes-source-fingerprint": "b" * 64,
        "--max-inflight": "19",
        "--db-pool-size": "23",
        "--min-workers": "7",
    }
    for option, value in expected.items():
        assert argv[argv.index(option) + 1] == value


def test_acceptance_reads_fixture_values_from_gateway_secret_ref(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    required = {
        "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "secret-app",
        "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "secret-token",
        "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "secret-encrypt",
    }
    deployment = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "envFrom": [{"secretRef": {"name": "trpc-service-secrets"}}],
                            "env": [
                                {
                                    "name": name,
                                    "value": "stale-literal",
                                }
                                for name in required
                            ],
                        }
                    ]
                }
            }
        }
    }
    secret = {
        "data": {
            name: base64.b64encode(value.encode()).decode() for name, value in required.items()
        }
    }
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        calls.append(command)
        payload = deployment if "deployment" in command else secret
        return SimpleNamespace(stdout=json.dumps(payload))

    monkeypatch.setattr(acceptance.shutil, "which", lambda _name: "kubectl")
    monkeypatch.setattr(acceptance.subprocess, "run", fake_run)

    result = acceptance._gateway_fixture_secrets(
        namespace="trpc-service",
        context="ack",
        kubeconfig=tmp_path / "config",
        deployment_name="trpc-gateway",
    )

    assert result == required
    assert len(calls) == 2
    assert calls[1][-4:] == ["secret", "trpc-service-secrets", "-o", "json"]


def test_acceptance_rejects_invalid_gateway_fixture_secret_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deployment = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"envFrom": [{"secretRef": {"name": "trpc-service-secrets"}}]}]
                }
            }
        }
    }
    responses = iter(
        (
            SimpleNamespace(stdout=json.dumps(deployment)),
            SimpleNamespace(
                stdout=json.dumps(
                    {
                        "data": {
                            "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "not-base64!",
                            "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "dmVyaWZ5",
                            "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "ZW5jcnlwdA==",
                        }
                    }
                )
            ),
        )
    )
    monkeypatch.setattr(acceptance.shutil, "which", lambda _name: "kubectl")
    monkeypatch.setattr(acceptance.subprocess, "run", lambda *_args, **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match="fixture secrets are unavailable"):
        acceptance._gateway_fixture_secrets(
            namespace="trpc-service",
            context="ack",
            kubeconfig=tmp_path / "config",
            deployment_name="trpc-gateway",
        )


def test_acceptance_projects_unified_config_into_job_spec_and_gate_command(
    tmp_path: Path,
) -> None:
    digest = "sha256:" + "a" * 64
    source = "b" * 64
    run_id = "c" * 32
    performance = SimpleNamespace(
        gateway_url="http://trpc-gateway.trpc-service.svc.cluster.local:18080",
        postgres_service="postgres.trpc-support.svc.cluster.local",
        redis_service="redis.trpc-support.svc.cluster.local",
        postgres_port=15432,
        redis_port=16379,
        max_inflight=19,
        db_pool_size=23,
        workers=7,
        node_selector={"trpc-role": "load-driver"},
        taint_key="trpc-role",
        taint_value="load-driver",
        taint_effect="NoSchedule",
        resources=SimpleNamespace(
            request_cpu="3",
            request_memory="3Gi",
            limit_cpu="5",
            limit_memory="5Gi",
        ),
    )
    config = SimpleNamespace(release_id="release-under-test")
    service_values = {
        "TRPC_SERVICE_DATABASE_DSN": "postgresql://db/service",
        "TRPC_SERVICE_REDIS_URL": "redis://redis:6379/0",
        "TRPC_SERVICE_SESSION_HMAC_KEY": "h" * 32,
    }
    service = {
        key: base64.b64encode(value.encode()).decode() for key, value in service_values.items()
    }
    evidence = {
        "schema_version": 1,
        "run_id": run_id,
        "source_fingerprint": source,
        "image_digest": digest,
        "preflight": {"status": "pass"},
    }

    spec = acceptance._build_job_spec(
        config=config,
        performance=performance,
        namespace="trpc-service",
        context="prod-context",
        kubeconfig=tmp_path / "config",
        image_reference="registry.example/trpc-agent@" + digest,
        image_digest=digest,
        image_pull_secret="registry-pull",
        source=source,
        run_id=run_id,
        worker_token="t" * 32,
        service=service,
        fixture_secrets={
            "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "app",
            "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "verify",
            "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "encrypt",
        },
        preflight_evidence=evidence,
        job_timeout=42.0,
    )

    assert spec.node_selector == performance.node_selector
    assert spec.requests == {"cpu": "3", "memory": "3Gi"}
    assert spec.limits == {"cpu": "5", "memory": "5Gi"}
    assert spec.config_env["TRPC_K8S_PERF_POSTGRES_SERVICE_DNS"] == performance.postgres_service
    assert spec.config_env["TRPC_K8S_PERF_REDIS_SERVICE_DNS"] == performance.redis_service
    assert spec.config_env["TRPC_K8S_PERF_POSTGRES_PORT"] == "15432"
    assert spec.config_env["TRPC_K8S_PERF_REDIS_PORT"] == "16379"
    command = json.loads(spec.config_env["TRPC_K8S_PERF_GATE_COMMAND"])
    assert command[command.index("--max-inflight") + 1] == "19"
    assert command[command.index("--db-pool-size") + 1] == "23"
    assert command[command.index("--min-workers") + 1] == "7"
    assert command[command.index("--kubernetes-namespace") + 1] == "trpc-service"
    assert command[command.index("--kubernetes-image-digest") + 1] == digest
    assert command[command.index("--kubernetes-source-fingerprint") + 1] == source
    assert command[command.index("--kubernetes-preflight-evidence") + 1] == (
        performance_job.PREFLIGHT_EVIDENCE_PATH
    )
    assert spec.secret_env[performance_job.WORKER_TOKEN_ENV] == "t" * 32


def test_build_spec_has_explicit_load_driver_placement_and_bounds(tmp_path: Path) -> None:
    spec = _spec(tmp_path)

    assert spec.node_selector == {"trpc-role": "load-driver"}
    assert spec.toleration == {
        "key": "trpc-role",
        "operator": "Equal",
        "value": "load-driver",
        "effect": "NoSchedule",
    }
    assert spec.requests == {"cpu": "2", "memory": "2Gi"}
    assert spec.limits == {"cpu": "4", "memory": "4Gi"}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("image", "registry.example/trpc-agent:latest", "immutable sha256"),
        ("source_fingerprint", "not-a-fingerprint", "source fingerprint"),
        ("image_pull_secret", "registry/pull", "Secret name"),
        ("node_selector", {"trpc-role": "workload"}, "nodeSelector"),
        ("toleration", {"key": "trpc-role", "effect": "NoSchedule"}, "toleration"),
        ("limits", {"cpu": "not-a-quantity"}, "Kubernetes quantity"),
    ],
)
def test_build_spec_rejects_unsafe_or_unbounded_inputs(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _spec(tmp_path, **{field: value})


def test_build_spec_rejects_secret_values_in_config_map(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unsupported environment name"):
        _spec(tmp_path, config_env={"TRPC_SERVICE_DATABASE_DSN": "should-be-secret"})


def test_build_spec_rejects_unbound_preflight_evidence(tmp_path: Path) -> None:
    evidence = {
        "schema_version": 1,
        "run_id": "different-run",
        "source_fingerprint": "b" * 64,
        "image_digest": "sha256:" + "a" * 64,
        "preflight": {"status": "pass"},
    }
    with pytest.raises(ValueError, match="binding mismatches"):
        _spec(tmp_path, preflight_evidence=evidence)


def test_secret_manifest_only_contains_base64_values(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    manifest = performance_job.secret_manifest(spec)

    assert manifest["kind"] == "Secret"
    encoded = manifest["data"]["TRPC_SERVICE_DATABASE_DSN"]
    assert encoded != spec.secret_env["TRPC_SERVICE_DATABASE_DSN"]
    assert base64.b64decode(encoded).decode() == spec.secret_env["TRPC_SERVICE_DATABASE_DSN"]
    assert "postgresql://runtime@postgres/db" not in json.dumps(manifest)


def test_config_map_mounts_signed_preflight_evidence_as_a_file(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    manifest = performance_job.config_map_manifest(spec)
    assert manifest["data"][performance_job.PREFLIGHT_EVIDENCE_FILE_NAME] == (
        spec.preflight_evidence_json
    )


def test_job_manifest_is_single_completion_and_uses_secret_config_refs(tmp_path: Path) -> None:
    spec = _spec(tmp_path)
    manifest = performance_job.job_manifest(spec)
    job_spec = manifest["spec"]
    pod_spec = job_spec["template"]["spec"]
    container = pod_spec["containers"][0]

    assert job_spec["completions"] == 1
    assert job_spec["parallelism"] == 1
    assert job_spec["backoffLimit"] == 0
    assert job_spec["activeDeadlineSeconds"] == 1260
    assert job_spec["ttlSecondsAfterFinished"] == 300
    assert pod_spec["nodeSelector"] == {"trpc-role": "load-driver"}
    assert pod_spec["tolerations"] == [spec.toleration]
    assert pod_spec["automountServiceAccountToken"] is False
    volumes = {volume["name"]: volume for volume in pod_spec["volumes"]}
    assert volumes["tmp"] == {"name": "tmp", "emptyDir": {}}
    assert volumes["preflight-evidence"]["configMap"]["items"] == [
        {
            "key": performance_job.PREFLIGHT_EVIDENCE_FILE_NAME,
            "path": performance_job.PREFLIGHT_EVIDENCE_FILE_NAME,
        }
    ]
    assert volumes["preflight-evidence"]["configMap"]["defaultMode"] == 0o440
    assert manifest["metadata"]["labels"]["trpc.io/performance-runner"] == "true"
    assert (
        manifest["spec"]["template"]["metadata"]["labels"]["trpc.io/performance-runner"] == "true"
    )
    assert pod_spec["imagePullSecrets"] == [{"name": "registry-pull"}]
    assert container["image"] == spec.image
    assert container["envFrom"] == [
        {"secretRef": {"name": "trpc-perf-secret-" + spec.run_id[:24]}},
        {"configMapRef": {"name": "trpc-perf-config-" + spec.run_id[:24]}},
    ]
    assert container["resources"] == {"requests": spec.requests, "limits": spec.limits}
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["volumeMounts"] == [
        {"name": "tmp", "mountPath": "/tmp"},  # noqa: S108
        {
            "name": "preflight-evidence",
            "mountPath": performance_job.PREFLIGHT_EVIDENCE_PATH,
            "subPath": performance_job.PREFLIGHT_EVIDENCE_FILE_NAME,
            "readOnly": True,
        },
    ]
    assert container["envFrom"][0]["secretRef"]["name"].startswith("trpc-perf-secret-")
    assert all(
        variable["name"] != performance_job.WORKER_TOKEN_ENV for variable in container["env"]
    )


def test_parse_worker_report_requires_one_marker_and_version() -> None:
    payload = {"schema_version": 1, "status": "pass", "report": {"gate": "pass"}}
    logs = "diagnostic line\n" + performance_job.REPORT_MARKER + json.dumps(payload)

    assert performance_job.parse_worker_report(logs) == payload


@pytest.mark.parametrize(
    "logs",
    [
        "no report",
        performance_job.REPORT_MARKER + "{}\n" + performance_job.REPORT_MARKER + "{}",
        performance_job.REPORT_MARKER + "not-json",
        performance_job.REPORT_MARKER + '{"schema_version": 2}',
    ],
)
def test_parse_worker_report_fails_closed(logs: str) -> None:
    with pytest.raises(performance_job.PerformanceJobError):
        performance_job.parse_worker_report(logs)


def test_gate_command_requires_signed_kubernetes_handoff_arguments() -> None:
    command = (
        "python",
        "scripts/real_performance_gate.py",
        "--load-worker",
        "--kubernetes-load-worker",
        "--kubernetes",
        "--kubernetes-namespace",
        "runtime-gate",
        "--kubernetes-context",
        "ack-test",
        "--kubernetes-image-digest",
        "sha256:" + "a" * 64,
        "--kubernetes-source-fingerprint",
        "b" * 64,
        "--kubernetes-preflight-evidence",
        performance_job.PREFLIGHT_EVIDENCE_PATH,
    )
    assert (
        performance_job._validate_gate_command(
            command,
            namespace="runtime-gate",
            context="ack-test",
            source_fingerprint="b" * 64,
            image_digest="sha256:" + "a" * 64,
            evidence_path=performance_job.PREFLIGHT_EVIDENCE_PATH,
        )
        == command
    )

    with pytest.raises(performance_job.PerformanceJobError, match="not bound"):
        performance_job._validate_gate_command(
            (*command[:-1], "/var/run/trpc-performance/other.json"),
            namespace="runtime-gate",
            context="ack-test",
            source_fingerprint="b" * 64,
            image_digest="sha256:" + "a" * 64,
            evidence_path=performance_job.PREFLIGHT_EVIDENCE_PATH,
        )


def test_service_url_rewrites_only_host_and_port() -> None:
    rewritten = performance_job._service_url(
        "postgresql+asyncpg://user:pass@postgres:5432/db?sslmode=disable",
        hostname="postgres.trpc-runtime-support.svc.cluster.local",
        port="5432",
        field="PostgreSQL",
    )
    assert rewritten == (
        "postgresql+asyncpg://user:pass@postgres.trpc-runtime-support.svc.cluster.local:5432/"
        "db?sslmode=disable"
    )


def test_preflight_binding_requires_job_source_digest_and_run_id(tmp_path: Path) -> None:
    evidence_path = tmp_path / performance_job.PREFLIGHT_EVIDENCE_FILE_NAME
    evidence_path.write_text(
        json.dumps(
            {
                "run_id": "c" * 32,
                "source_fingerprint": "b" * 64,
                "image_digest": "sha256:" + "a" * 64,
                "preflight": {"status": "pass"},
                "preflight_sha256": "d" * 64,
            }
        ),
        encoding="utf-8",
    )
    assert (
        performance_job._preflight_binding(
            evidence_path,
            run_id="c" * 32,
            source_fingerprint="b" * 64,
            image_digest="sha256:" + "a" * 64,
        )["status"]
        == "verified"
    )

    with pytest.raises(performance_job.PerformanceJobError, match="binding mismatches"):
        performance_job._preflight_binding(
            evidence_path,
            run_id="e" * 32,
            source_fingerprint="b" * 64,
            image_digest="sha256:" + "a" * 64,
        )


def test_gate_report_identity_rejects_stale_source_or_image() -> None:
    digest = "sha256:" + "a" * 64
    source = "b" * 64
    report = {
        "candidate": {
            "run_id": "c" * 32,
            "preflight": {
                "source_fingerprint": {"value": source},
                "worker_image_attestation": {"image_id": digest},
                "service_image_attestation": {
                    "worker": {"image_id": digest},
                    "outbox-dispatcher": {"image_id": digest},
                },
            },
        },
        "evidence": {"source_fingerprint": {"value": source}},
    }
    performance_job._validate_gate_report_identity(
        report,
        run_id="c" * 32,
        source_fingerprint=source,
        image_digest=digest,
    )
    with pytest.raises(performance_job.PerformanceJobError, match="worker image"):
        performance_job._validate_gate_report_identity(
            report,
            run_id="c" * 32,
            source_fingerprint=source,
            image_digest="sha256:" + "d" * 64,
        )


def test_run_performance_job_collects_report_and_confirms_all_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    calls: list[tuple[str, str]] = []
    job_payload = {"metadata": {"uid": "job-uid"}, "status": {"succeeded": 1}}
    worker = {
        "schema_version": 1,
        "status": "pass",
        "report": {"gate": "pass", "production_gate": "pass"},
    }

    def fake_apply(_spec: object, document: dict[str, object]) -> None:
        calls.append(("apply", str(document["kind"])))

    def fake_delete(_spec: object, kind: str, name: str) -> dict[str, object]:
        calls.append(("delete", kind))
        return {"status": "pass", "kind": kind, "name": name}

    monkeypatch.setattr(performance_job, "_apply", fake_apply)
    monkeypatch.setattr(performance_job, "_wait_for_job", lambda *_args: job_payload)
    monkeypatch.setattr(
        performance_job,
        "_logs",
        lambda *_args: performance_job.REPORT_MARKER + json.dumps(worker),
    )
    monkeypatch.setattr(performance_job, "_delete_and_confirm", fake_delete)

    output = tmp_path / "job.json"
    result = performance_job.run_performance_job(spec, output)

    assert result["status"] == "pass"
    assert result["report"] == worker
    assert result["job"]["uid"] == "job-uid"
    assert [kind for action, kind in calls if action == "apply"] == [
        "Secret",
        "ConfigMap",
        "Job",
    ]
    assert [kind for action, kind in calls if action == "delete"] == [
        "job",
        "configmap",
        "secret",
    ]
    assert json.loads(output.read_text(encoding="utf-8")) == result


def test_run_performance_job_recovers_from_transient_read_timeouts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    job_payload = {"metadata": {"uid": "job-uid"}, "status": {"succeeded": 1}}
    worker = {
        "schema_version": 1,
        "status": "pass",
        "report": {"gate": "pass", "production_gate": "pass"},
    }
    outcomes: list[object] = [
        performance_job._KubectlTimeoutError("kubectl request timed out"),
        subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(job_payload), stderr=""
        ),
        performance_job._KubectlTimeoutError("kubectl request timed out"),
        subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=performance_job.REPORT_MARKER + json.dumps(worker),
            stderr="",
        ),
    ]
    sleeps: list[float] = []

    def fake_kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, subprocess.CompletedProcess)
        return outcome

    monkeypatch.setattr(performance_job, "_apply", lambda *_args: None)
    monkeypatch.setattr(performance_job, "_kubectl", fake_kubectl)
    monkeypatch.setattr(time, "sleep", sleeps.append)
    monkeypatch.setattr(
        performance_job,
        "_delete_and_confirm",
        lambda _spec, kind, name: {"status": "pass", "kind": kind, "name": name},
    )

    result = performance_job.run_performance_job(spec, tmp_path / "job.json")

    assert result["status"] == "pass"
    assert result["report"] == worker
    assert result["job"]["uid"] == "job-uid"
    assert result["rejection_reasons"] == []
    assert sleeps == [0.25, 0.25]
    assert outcomes == []


def test_read_retry_is_bounded_and_preserves_typed_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    calls = 0
    sleeps: list[float] = []

    def always_timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        raise performance_job._KubectlTimeoutError("kubectl request timed out")

    monkeypatch.setattr(performance_job, "_kubectl", always_timeout)
    monkeypatch.setattr(time, "sleep", sleeps.append)

    with pytest.raises(performance_job._KubectlTimeoutError, match="request timed out"):
        performance_job._get_job(
            spec,
            "job-name",
            deadline=time.monotonic() + 100.0,
        )

    assert calls == 3
    assert sleeps == [0.25, 0.5]


@pytest.mark.parametrize(
    "result",
    [
        subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="forbidden"),
        subprocess.CompletedProcess(args=[], returncode=0, stdout="not-json", stderr=""),
    ],
)
def test_job_inspection_does_not_retry_non_timeout_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    result: subprocess.CompletedProcess[str],
) -> None:
    spec = _spec(tmp_path)
    calls = 0

    def fake_kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        return result

    monkeypatch.setattr(performance_job, "_kubectl", fake_kubectl)

    with pytest.raises(performance_job.PerformanceJobError):
        performance_job._get_job(
            spec,
            "job-name",
            deadline=time.monotonic() + 100.0,
        )

    assert calls == 1


def test_cleanup_read_recovers_from_transient_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    outcomes: list[object] = [
        performance_job._KubectlTimeoutError("kubectl request timed out"),
        subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="Error from server (NotFound)"
        ),
    ]
    sleeps: list[float] = []

    def fake_kubectl(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        assert isinstance(outcome, subprocess.CompletedProcess)
        return outcome

    monkeypatch.setattr(performance_job, "_kubectl", fake_kubectl)
    monkeypatch.setattr(time, "sleep", sleeps.append)

    result = performance_job._delete_and_confirm(spec, "job", "job-name")

    assert result == {
        "status": "pass",
        "kind": "job",
        "name": "job-name",
        "already_absent": True,
    }
    assert sleeps == [0.25]
    assert outcomes == []


def test_run_performance_job_reports_failure_but_still_cleans_created_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    deleted: list[str] = []

    def fake_apply(_spec: object, document: dict[str, object]) -> None:
        if document["kind"] == "Job":
            raise performance_job.PerformanceJobError("Job creation failed")

    monkeypatch.setattr(performance_job, "_apply", fake_apply)

    def fake_delete(_spec: object, kind: str, name: str) -> dict[str, object]:
        deleted.append(kind)
        return {"status": "pass", "kind": kind, "name": name}

    monkeypatch.setattr(
        performance_job,
        "_delete_and_confirm",
        fake_delete,
    )

    result = performance_job.run_performance_job(spec, tmp_path / "job.json")

    assert result["status"] == "fail"
    assert result["report"] is None
    assert "Job creation failed" in result["rejection_reasons"]
    assert deleted == ["job", "configmap", "secret"]


def test_run_performance_job_keeps_failed_worker_report_before_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _spec(tmp_path)
    deleted: list[str] = []
    worker = {
        "schema_version": 1,
        "status": "fail",
        "report": {"gate": "fail", "production_gate": "fail"},
    }
    job_payload = {"metadata": {"uid": "failed-job-uid"}, "status": {"failed": 1}}

    monkeypatch.setattr(performance_job, "_apply", lambda *_args: None)
    monkeypatch.setattr(performance_job, "_wait_for_job", lambda *_args: job_payload)
    monkeypatch.setattr(
        performance_job,
        "_logs",
        lambda *_args: performance_job.REPORT_MARKER + json.dumps(worker),
    )

    def fake_delete(_spec: object, kind: str, name: str) -> dict[str, object]:
        deleted.append(kind)
        return {"status": "pass", "kind": kind, "name": name}

    monkeypatch.setattr(performance_job, "_delete_and_confirm", fake_delete)

    result = performance_job.run_performance_job(spec, tmp_path / "job.json")

    assert result["status"] == "fail"
    assert result["report"] == worker
    assert result["job"]["uid"] == "failed-job-uid"
    assert "performance Job worker reported failure" in result["rejection_reasons"]
    assert deleted == ["job", "configmap", "secret"]


def test_worker_report_keeps_gate_and_cleanup_evidence_separate() -> None:
    payload = performance_job._worker_report(
        status="fail",
        gate_report={"gate": "fail"},
        fixture_report={"synthetic": True, "tenant_id": "tenant"},
        cleanup_report={"gate": "pass"},
        reasons=("gate failed",),
        evidence_binding={
            "status": "verified",
            "run_id": "c" * 32,
            "source_fingerprint": "b" * 64,
            "image_digest": "sha256:" + "a" * 64,
        },
        secrets=("secret-token",),
    )

    assert payload["report"] == {"gate": "fail"}
    assert payload["fixture"]["synthetic"] is True
    assert payload["cleanup"] == {"gate": "pass"}
    assert payload["rejection_reasons"] == ["gate failed"]
    assert payload["evidence_binding"]["status"] == "verified"


def test_worker_report_redacts_secret_values() -> None:
    payload = performance_job._worker_report(
        status="fail",
        gate_report={"reason": "token=secret-token"},
        fixture_report={"value": "secret-token"},
        cleanup_report=None,
        reasons=("secret-token leaked",),
        secrets=("secret-token",),
    )

    rendered = json.dumps(payload)
    assert "secret-token" not in rendered
    assert "[redacted]" in rendered


def test_worker_main_cleans_up_cleanup_ready_partial_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture_path = tmp_path / "fixture.json"
    gate_path = tmp_path / "gate.json"
    cleanup_path = tmp_path / "cleanup.json"
    tenant_id = "perf-" + "d" * 32
    run_id = "c" * 32
    partial_report = {
        "schema_version": 1,
        "kind": "performance_fixture",
        "gate": "partial",
        "cleanup_ready": True,
        "tenant_id": tenant_id,
        "run_id": run_id,
    }
    environment = {
        "TRPC_K8S_PERF_IMAGE_DIGEST": "sha256:" + "a" * 64,
        "TRPC_K8S_PERF_SOURCE_FINGERPRINT": "b" * 64,
        "TRPC_K8S_PERF_RUN_ID": run_id,
    }
    child_commands: list[tuple[str, ...]] = []

    monkeypatch.setenv("TRPC_K8S_PERF_FIXTURE_PATH", str(fixture_path))
    monkeypatch.setenv("TRPC_K8S_PERF_REPORT_PATH", str(gate_path))
    monkeypatch.setenv("TRPC_K8S_PERF_CLEANUP_PATH", str(cleanup_path))
    monkeypatch.setattr(performance_job, "_child_environment", lambda: environment.copy())
    monkeypatch.setattr(
        performance_job,
        "_preflight_binding",
        lambda *_args, **_kwargs: {"status": "verified"},
    )

    def fake_run_child(command: tuple[str, ...], _environment: object, _timeout: float) -> int:
        child_commands.append(command)
        if command[2] == "create":
            fixture_path.write_text(json.dumps(partial_report), encoding="utf-8")
            return 1
        assert command[2] == "cleanup"
        cleanup_path.write_text(json.dumps({"status": "pass"}), encoding="utf-8")
        return 0

    monkeypatch.setattr(performance_job, "_run_child", fake_run_child)

    assert performance_job.worker_main() == 1

    assert [command[2] for command in child_commands] == ["create", "cleanup"]
    cleanup_command = child_commands[1]
    assert cleanup_command[cleanup_command.index("--tenant-id") + 1] == tenant_id
    assert cleanup_command[cleanup_command.index("--run-id") + 1] == run_id
    report = performance_job.parse_worker_report(capsys.readouterr().out)
    assert report["fixture"] == partial_report
    assert report["cleanup"] == {"status": "pass"}


def test_worker_main_preserves_failed_cleanup_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fixture_path = tmp_path / "fixture.json"
    gate_path = tmp_path / "gate.json"
    cleanup_path = tmp_path / "cleanup.json"
    tenant_id = "perf-" + "e" * 32
    fixture_run_id = "perf-fixture-" + "e" * 32
    job_run_id = "d" * 32
    fixture_report = {
        "schema_version": 1,
        "kind": "performance_fixture",
        "gate": "pass",
        "tenant_id": tenant_id,
        "run_id": fixture_run_id,
    }
    gate_report = {"gate": "pass", "production_gate": "pass"}
    cleanup_report = {
        "schema_version": 1,
        "kind": "performance_fixture",
        "gate": "fail",
        "error_type": "InsufficientPrivilegeError",
    }
    environment = {
        "TRPC_K8S_PERF_IMAGE_DIGEST": "sha256:" + "a" * 64,
        "TRPC_K8S_PERF_SOURCE_FINGERPRINT": "b" * 64,
        "TRPC_K8S_PERF_RUN_ID": job_run_id,
    }

    monkeypatch.setenv("TRPC_K8S_PERF_FIXTURE_PATH", str(fixture_path))
    monkeypatch.setenv("TRPC_K8S_PERF_REPORT_PATH", str(gate_path))
    monkeypatch.setenv("TRPC_K8S_PERF_CLEANUP_PATH", str(cleanup_path))
    monkeypatch.setattr(performance_job, "_child_environment", lambda: environment.copy())
    monkeypatch.setattr(
        performance_job,
        "_preflight_binding",
        lambda *_args, **_kwargs: {"status": "verified"},
    )
    monkeypatch.setattr(performance_job, "_supervision_parent", lambda _timeout: (1, None))
    monkeypatch.setattr(
        performance_job, "_validate_gate_command", lambda command, **_kwargs: command
    )
    monkeypatch.setattr(performance_job, "_strict_command_from_env", lambda _name: ["gate"])
    monkeypatch.setattr(performance_job, "_validate_gate_report_identity", lambda *_a, **_k: None)

    def fake_run_child(command: tuple[str, ...], _environment: object, _timeout: float) -> int:
        is_fixture = len(command) > 2 and command[1].endswith("performance_fixture.py")
        if is_fixture and command[2] == "create":
            fixture_path.write_text(json.dumps(fixture_report), encoding="utf-8")
            return 0
        if is_fixture and command[2] == "cleanup":
            cleanup_path.write_text(json.dumps(cleanup_report), encoding="utf-8")
            return 1
        gate_path.write_text(json.dumps(gate_report), encoding="utf-8")
        return 0

    monkeypatch.setattr(performance_job, "_run_child", fake_run_child)

    assert performance_job.worker_main() == 1

    report = performance_job.parse_worker_report(capsys.readouterr().out)
    assert report["cleanup"] == cleanup_report, report
    assert report["rejection_reasons"] == ["performance fixture cleanup failed"]
