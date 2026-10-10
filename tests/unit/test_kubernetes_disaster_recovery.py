import json
import subprocess
from pathlib import Path

import pytest

from scripts import kubernetes_disaster_recovery as dr


def _lock() -> dict[str, object]:
    return {
        "release_binding": {"release_id": "release-dr", "nonce_sha256": "a" * 64},
        "source_fingerprint": {"status": "available", "value": "b" * 64},
        "image_digest": "sha256:" + "c" * 64,
        "binding_sha256": "d" * 64,
    }


def _job_payload() -> dict[str, object]:
    return {
        "metadata": {"uid": "job-uid", "namespace": "trpc-drill"},
        "status": {
            "succeeded": 1,
            "failed": 0,
            "active": 0,
            "startTime": "2026-08-26T10:00:00Z",
            "completionTime": "2026-08-26T10:02:00Z",
            "conditions": [{"type": "Complete", "status": "True"}],
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "restore",
                            "image": "registry.example/acme/service@sha256:" + "c" * 64,
                        }
                    ]
                }
            }
        },
    }


def _pod_payload() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {"uid": "pod-uid"},
                "status": {
                    "phase": "Succeeded",
                    "containerStatuses": [
                        {
                            "imageID": "docker-pullable://service@sha256:" + "c" * 64,
                            "state": {"terminated": {"exitCode": 0}},
                        }
                    ],
                },
            }
        ]
    }


def _result_line(component: str) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "component": component,
            "status": "pass",
            "mode": "same_cluster_zero_cost_functional",
            "drill_id": "drill-one",
            "run_id": component + "-run",
            "tenant_id_hash": "e" * 64,
            "canary_sha256": "f" * 64,
            "restored_canary_sha256": "f" * 64,
            "rpo_seconds": 30,
            "rto_seconds": 120,
            "point_in_time_recovery": component == "postgres_pitr",
            "backup_integrity_verified": component == "postgres_pitr",
            "versioned_restore": component != "postgres_pitr",
            "checksum_verified": component == "artifact_restore",
            "key_version_restored": component == "key_restore",
            "decrypt_verified": component == "key_restore",
            "backup": {
                "backend": "postgresql" if component == "postgres_pitr" else "s3",
                "storage_tier": "cross_region_redundant",
                "disaster_redundant": True,
                "replication_verified": True,
                "pitr_enabled": component == "postgres_pitr",
                "versioning_enabled": component != "postgres_pitr",
                "key_versioned": component == "key_restore",
                "backup_id_sha256": "1" * 64,
                "restore_id_sha256": "2" * 64,
                "created_at": "2026-08-26T09:59:30Z",
                "restore_started_at": "2026-08-26T10:00:00Z",
            },
            "validation": {
                "source": "restore_job_output",
                "status": "pass",
                "production_data_touched": False,
            },
        },
        separators=(",", ":"),
    )


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _collector_kwargs(tmp_path: Path, lock: dict[str, object]) -> dict[str, object]:
    kubeconfig = tmp_path / "config"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    return {
        "job_name": "artifact-restore",
        "namespace": "trpc-drill",
        "context": "ack-acceptance",
        "kubeconfig": kubeconfig,
        "timeout_seconds": 3,
        "lock": lock,
        "cluster_uid_sha256": "3" * 64,
        "namespace_uid_sha256": "4" * 64,
    }


def _kubectl_job_or_pod_or_logs(
    arguments: list[str], *, pod: dict[str, object], logs: str
) -> subprocess.CompletedProcess[str]:
    if arguments[0:2] == ["get", "job"]:
        return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
    if arguments[0:2] == ["get", "pods"]:
        return subprocess.CompletedProcess(arguments, 0, json.dumps(pod), "")
    if arguments[0] == "logs":
        return subprocess.CompletedProcess(arguments, 0, logs, "")
    raise AssertionError(arguments)


def test_collect_component_uses_api_status_and_secret_free_job_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kubeconfig = tmp_path / "config"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        if arguments[0:2] == ["get", "job"]:
            payload = _job_payload()
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")
        if arguments[0:2] == ["get", "pods"]:
            payload = _pod_payload()
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")
        if arguments[0] == "logs":
            return subprocess.CompletedProcess(arguments, 0, _result_line("artifact_restore"), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    observation = dr.collect_component(
        "artifact_restore",
        job_name="artifact-restore",
        namespace="trpc-drill",
        context="ack-acceptance",
        kubeconfig=kubeconfig,
        timeout_seconds=30,
        lock=lock,
        cluster_uid_sha256="3" * 64,
        namespace_uid_sha256="4" * 64,
    )

    assert observation["status"] == "pass"
    assert observation["execution"]["source"] == "kubectl_api"
    assert observation["execution"]["succeeded"] == 1
    assert observation["mode"] == "same_cluster_zero_cost_functional"
    assert "password" not in json.dumps(observation).lower()


def test_collect_component_retries_until_succeeded_pod_is_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    pod_reads = 0
    pod = _pod_payload()
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal pod_reads
        if arguments[0:2] == ["get", "job"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
        if arguments[0:2] == ["get", "pods"]:
            pod_reads += 1
            payload = {"items": []} if pod_reads == 1 else pod
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")
        if arguments[0] == "logs":
            return subprocess.CompletedProcess(arguments, 0, _result_line("artifact_restore"), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    observation = dr.collect_component("artifact_restore", **_collector_kwargs(tmp_path, lock))

    assert observation["status"] == "pass"
    assert pod_reads == 2
    assert clock.now == 1


def test_collect_component_retries_until_completed_job_status_is_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    job_reads = 0
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal job_reads
        if arguments[0:2] == ["get", "job"]:
            job_reads += 1
            payload = _job_payload()
            if job_reads == 1:
                payload["status"] = {"active": 1}
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")
        if arguments[0:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_pod_payload()), "")
        if arguments[0] == "logs":
            return subprocess.CompletedProcess(arguments, 0, _result_line("artifact_restore"), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    observation = dr.collect_component("artifact_restore", **_collector_kwargs(tmp_path, lock))

    assert observation["status"] == "pass"
    assert job_reads == 2
    assert clock.now == 1


def test_collect_component_retries_until_container_status_is_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    pod_reads = 0
    incomplete_pod = {
        "items": [
            {
                "metadata": {"uid": "pod-uid"},
                "status": {"phase": "Succeeded", "containerStatuses": []},
            }
        ]
    }
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal pod_reads
        if arguments[0:2] == ["get", "job"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
        if arguments[0:2] == ["get", "pods"]:
            pod_reads += 1
            payload = incomplete_pod if pod_reads == 1 else _pod_payload()
            return subprocess.CompletedProcess(arguments, 0, json.dumps(payload), "")
        if arguments[0] == "logs":
            return subprocess.CompletedProcess(arguments, 0, _result_line("artifact_restore"), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    observation = dr.collect_component("artifact_restore", **_collector_kwargs(tmp_path, lock))

    assert observation["status"] == "pass"
    assert pod_reads == 2
    assert clock.now == 1


def test_collect_component_retries_until_job_logs_are_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    log_reads = 0
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal log_reads
        if arguments[0:2] == ["get", "job"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
        if arguments[0:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_pod_payload()), "")
        if arguments[0] == "logs":
            log_reads += 1
            output = "" if log_reads == 1 else _result_line("artifact_restore")
            return subprocess.CompletedProcess(arguments, 0, output, "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    observation = dr.collect_component("artifact_restore", **_collector_kwargs(tmp_path, lock))

    assert observation["status"] == "pass"
    assert log_reads == 2
    assert clock.now == 1


def test_collect_component_times_out_on_permanently_incomplete_pod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    pod_reads = 0
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal pod_reads
        if arguments[0:2] == ["get", "job"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
        if arguments[0:2] == ["get", "pods"]:
            pod_reads += 1
            return subprocess.CompletedProcess(arguments, 0, json.dumps({"items": []}), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    with pytest.raises(dr.DisasterRecoveryCollectionTimeout):
        dr.collect_component(
            "artifact_restore",
            **{**_collector_kwargs(tmp_path, lock), "timeout_seconds": 0.5},
        )

    assert pod_reads == 1
    assert clock.now == 0.5


def test_collect_component_does_not_retry_permanent_pod_semantic_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(dr.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(dr.time, "sleep", clock.sleep)
    pod = json.loads(json.dumps(_pod_payload()))
    pod["items"][0]["status"]["containerStatuses"][0]["state"]["terminated"]["exitCode"] = 1
    pod_reads = 0
    lock = _lock()

    def fake_kubectl(arguments, **_kwargs):
        nonlocal pod_reads
        if arguments[0:2] == ["get", "job"]:
            return subprocess.CompletedProcess(arguments, 0, json.dumps(_job_payload()), "")
        if arguments[0:2] == ["get", "pods"]:
            pod_reads += 1
            return subprocess.CompletedProcess(arguments, 0, json.dumps(pod), "")
        if arguments[0] == "logs":
            return subprocess.CompletedProcess(arguments, 0, _result_line("artifact_restore"), "")
        raise AssertionError(arguments)

    monkeypatch.setattr(dr, "_kubectl", fake_kubectl)
    with pytest.raises(RuntimeError, match="image or exit status"):
        dr.collect_component("artifact_restore", **_collector_kwargs(tmp_path, lock))

    assert pod_reads == 1
    assert clock.now == 0


def test_job_result_rejects_sensitive_fields() -> None:
    with pytest.raises(RuntimeError, match="not secret-safe"):
        dr._job_output(
            json.dumps(
                {
                    "schema_version": 1,
                    "component": "artifact_restore",
                    "status": "pass",
                    "password": "must-not-be-recorded",
                }
            ),
            component="artifact_restore",
        )


def test_kubectl_error_does_not_expose_stderr(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(["kubectl"], 1, "", "secret-value-must-not-escape")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(dr.shutil, "which", lambda _name: "kubectl")

    with pytest.raises(RuntimeError) as error:
        dr._kubectl(
            ["get", "namespace", "trpc-drill", "-o", "json"],
            kubeconfig=Path("config"),
            context="ack-acceptance",
            timeout_seconds=30,
        )
    assert "secret-value" not in str(error.value)


def test_job_names_must_cover_exactly_the_three_components() -> None:
    assert set(
        dr._job_name_args(
            [
                "postgres_pitr=postgres-pitr",
                "artifact_restore=artifact-restore",
                "key_restore=key-restore",
            ]
        )
    ) == set(dr.COMPONENTS)
    with pytest.raises(ValueError, match="include postgres_pitr"):
        dr._job_name_args(["postgres_pitr=postgres-pitr"])


def test_main_is_inert_without_explicit_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TRPC_DR_DRILL_ENABLED", raising=False)
    output = tmp_path / "disaster-recovery.json"

    assert dr.main(["--output", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
