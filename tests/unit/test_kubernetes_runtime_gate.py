from __future__ import annotations

import hashlib
import json
import os
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

import scripts.kubernetes_runtime_gate as runtime_gate
from scripts.kubernetes_runtime_gate import (
    _REQUIRED_RUNTIME_ACTIONS,
    _REQUIRED_RUNTIME_CHECKS,
    CommandResult,
    _controlled_node_drain,
    _deployment_image_ids,
    _evict_pod,
    _hpa_load_observation_contract,
    _hpa_load_report_payload,
    _hpa_observation_from_api,
    _image_transform,
    _migration_head_check,
    _observe_hpa_state,
    _rendered_manifest_contract,
    _report,
    _rolling_upgrade_serial,
    _run_live,
    _runtime_attestation_contract,
    _scheduler_runtime_contract,
    _schema_head_check_manifest,
    _split_migration_manifests,
    _write_overlay,
)


@pytest.fixture(autouse=True)
def _clear_inherited_kubernetes_environment(monkeypatch) -> None:
    for name in tuple(os.environ):
        if name.startswith("TRPC_K8S_RUNTIME_"):
            monkeypatch.delenv(name, raising=False)


def _job_evidence(namespace: str, nonce: str, fingerprint: str) -> dict[str, dict[str, object]]:
    uid = "job-uid-1"
    common = {
        "api_observed": True,
        "job_name": f"trpc-hpa-load-{nonce[:20]}",
        "job_uid": uid,
        "job_labels": {
            "trpc.io/hpa-gate": "bounded-job-driver",
            "trpc.io/hpa-run": nonce,
            "trpc.io/hpa-phase": "load",
            "trpc.io/hpa-cluster": fingerprint[:63],
        },
        "namespace": namespace,
        "run_nonce": nonce,
        "cluster_fingerprint": fingerprint,
        "phase": "load",
    }
    return {"load": dict(common), "clear": {**common, "job_deleted": True}}


def _externalize_hpa_observation(observation: dict[str, object]) -> dict[str, object]:
    """Attach the direct external-metrics API evidence required by the gate."""

    namespace = str(observation["namespace"])
    metric_name = "trpc_session_ready_backlog"
    api_path = f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{namespace}/{metric_name}"
    for phase in ("before", "during", "after"):
        phase_observation = observation[phase]
        assert isinstance(phase_observation, dict)
        phase_observation["external_metric"] = {
            "api_observed": True,
            "api_version": "v1beta1",
            "api_path": api_path,
            "metric_name": metric_name,
            "namespace": namespace,
            "label_namespace": namespace,
            "item_count": 1,
            "value": phase_observation["metric_value"],
        }
    return observation


def test_image_transform_requires_immutable_reference_shape() -> None:
    assert _image_transform("registry.example/trpc-service:2026-08-21") == {
        "newName": "registry.example/trpc-service",
        "newTag": "2026-08-21",
    }
    assert _image_transform("registry.example/trpc-service@sha256:abc") == {
        "newName": "registry.example/trpc-service",
        "digest": "sha256:abc",
    }
    with pytest.raises(ValueError):
        _image_transform("registry.example/trpc-service")


def test_production_image_contract_requires_registry_digest_references() -> None:
    initial = "ghcr.io/acme/trpc-service@sha256:" + "a" * 64
    upgrade = "ghcr.io/acme/trpc-service@sha256:" + "b" * 64
    valid, reasons = runtime_gate._production_image_contract(initial, upgrade)
    assert valid
    assert reasons == ()

    valid, reasons = runtime_gate._production_image_contract(
        "trpc-service@sha256:" + "a" * 64,
        upgrade,
    )
    assert not valid
    assert any("registry-qualified" in reason for reason in reasons)

    valid, reasons = runtime_gate._production_image_contract(
        "ghcr.io/acme/trpc-service@sha256:abc",
        upgrade,
    )
    assert not valid
    assert any("registry-qualified" in reason for reason in reasons)

    for invalid in (
        "https://ghcr.io/acme/trpc-service@sha256:" + "a" * 64,
        "registry.example:abc/acme/trpc-service@sha256:" + "a" * 64,
        "ghcr.io@sha256:" + "a" * 64,
    ):
        valid, reasons = runtime_gate._production_image_contract(invalid, upgrade)
        assert not valid
        assert any("registry-qualified" in reason for reason in reasons)


def test_runtime_object_store_override_updates_only_configmap_and_records_hashes() -> None:
    rendered = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: trpc-service-config
data:
  TRPC_SERVICE_S3_ENDPOINT: https://oss.example.invalid
  TRPC_SERVICE_S3_BUCKET: old-bucket
---
apiVersion: v1
kind: Service
metadata:
  name: gateway
"""

    updated, evidence = runtime_gate._runtime_object_store_override(
        rendered,
        endpoint="http://minio.runtime-support.svc.cluster.local:9000/",
        bucket="trpc-artifacts",
    )
    documents = list(yaml.safe_load_all(updated))

    assert documents[0]["data"]["TRPC_SERVICE_S3_ENDPOINT"] == (
        "http://minio.runtime-support.svc.cluster.local:9000"
    )
    assert documents[0]["data"]["TRPC_SERVICE_S3_BUCKET"] == "trpc-artifacts"
    assert documents[1]["metadata"]["name"] == "gateway"
    assert evidence["status"] == "pass"
    assert "minio" not in json.dumps(evidence)


def test_rollback_probe_keeps_registry_and_uses_unavailable_digest() -> None:
    image = "registry.internal:5000/team/trpc-service@sha256:" + "a" * 64
    probe = runtime_gate._rollback_probe_image(image)
    assert probe == (
        "registry.internal:5000/team/trpc-service/__trpc_runtime_gate_failure__@sha256:" + "0" * 64
    )


def test_failure_rollback_requires_failed_rollout_and_restores_known_good_image(
    monkeypatch,
) -> None:
    kubectl_calls: list[list[str]] = []

    def fake_kubectl(arguments, **_kwargs):
        kubectl_calls.append(arguments)
        return CommandResult(status="pass")

    rollout_results = iter(
        [
            CommandResult(status="fail", reason="ImagePullBackOff"),
            CommandResult(status="pass"),
        ]
    )
    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    monkeypatch.setattr(
        runtime_gate,
        "_rollout_deployment",
        lambda *args, **kwargs: next(rollout_results),
    )
    known_good = ("sha256:" + "b" * 64,)
    image_observations = iter(
        [
            (CommandResult(status="fail", reason="terminating rollback pod"), ()),
            (CommandResult(status="pass"), known_good),
        ]
    )
    monkeypatch.setattr(
        runtime_gate,
        "_deployment_image_ids",
        lambda *args, **kwargs: next(image_observations),
    )
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, details = runtime_gate._failure_rollback(
        "trpc-worker",
        "worker",
        "ghcr.io/acme/trpc-service@sha256:" + "b" * 64,
        known_good,
        namespace="runtime-gate",
        context="prod",
        timeout_seconds=60,
    )

    assert result.status == "pass"
    assert details["failure_injected"] is True
    assert details["failure_observed"] is True
    assert details["undo_observed"] is True
    assert details["readiness_recovered"] is True
    assert details["rollback_image_poll_count"] == 2
    assert [call[0] for call in kubectl_calls] == ["set", "rollout"]
    assert kubectl_calls[0][3].startswith("worker=ghcr.io/acme/trpc-service/")


def test_deployment_image_ids_normalise_common_cri_image_id_forms(monkeypatch) -> None:
    payload = {
        "items": [
            {
                "metadata": {},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {
                            "imageID": "docker-pullable://registry.example/trpc-service@sha256:"
                            + "A" * 64,
                        },
                        {"imageID": "containerd://sha256:" + "b" * 64},
                    ],
                },
            }
        ]
    }

    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda *args, **kwargs: (CommandResult(status="pass"), payload),
    )
    result, image_ids = _deployment_image_ids(
        "trpc-worker", namespace="runtime-gate", context="test", timeout_seconds=5
    )

    assert result.status == "pass"
    assert image_ids == ("sha256:" + "a" * 64, "sha256:" + "b" * 64)


def test_deployment_image_ids_reject_terminating_pods(monkeypatch) -> None:
    payload = {
        "items": [
            {
                "metadata": {"deletionTimestamp": "2026-08-24T00:00:00Z"},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [{"imageID": "sha256:" + "a" * 64}],
                },
            }
        ]
    }
    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda *args, **kwargs: (CommandResult(status="pass"), payload),
    )
    result, image_ids = _deployment_image_ids(
        "trpc-worker", namespace="runtime-gate", context="test", timeout_seconds=5
    )
    assert result.status == "fail"
    assert image_ids == ()


@pytest.mark.parametrize("image_id", [None, "not-a-digest", "sha256:" + "a" * 64 + "junk"])
def test_deployment_image_ids_rejects_missing_or_malformed_container_ids(
    monkeypatch, image_id: str | None
) -> None:
    payload = {
        "items": [
            {
                "metadata": {},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "containerStatuses": [
                        {"imageID": "sha256:" + "a" * 64},
                        {"imageID": image_id},
                    ],
                },
            }
        ]
    }
    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda *args, **kwargs: (CommandResult(status="pass"), payload),
    )

    result, image_ids = _deployment_image_ids(
        "trpc-worker", namespace="runtime-gate", context="test", timeout_seconds=5
    )

    assert result.status == "fail"
    assert image_ids == ()


def test_wait_for_deployment_image_ids_requires_one_new_digest(monkeypatch) -> None:
    old_digest = ("sha256:" + "a" * 64,)
    new_digest = ("sha256:" + "b" * 64,)
    observations = iter(
        [
            (CommandResult(status="fail", reason="terminating pod"), ()),
            (CommandResult(status="pass"), old_digest),
            (CommandResult(status="pass"), new_digest),
        ]
    )
    monkeypatch.setattr(
        runtime_gate,
        "_deployment_image_ids",
        lambda *args, **kwargs: next(observations),
    )
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, image_ids, poll_count = runtime_gate._wait_for_deployment_image_ids(
        "trpc-worker",
        namespace="runtime-gate",
        context="test",
        timeout_seconds=5,
        previous_image_ids=old_digest,
    )

    assert result.status == "pass"
    assert image_ids == new_digest
    assert poll_count == 3


def test_generated_overlay_is_namespace_scoped(tmp_path) -> None:
    path = _write_overlay(
        tmp_path,
        namespace="trpc-runtime-gate-test",
        image="registry.example/trpc-service:test",
    )
    rendered = path.read_text(encoding="utf-8")
    assert "namespace: trpc-runtime-gate-test" in rendered
    assert "newTag: test" in rendered
    assert "namespace.yaml" in rendered
    assert "replicas-patch.yaml" in rendered
    assert "provider-disabled-patch.yaml" in rendered
    provider_disabled = list(
        yaml.safe_load_all((tmp_path / "provider-disabled-patch.yaml").read_text(encoding="utf-8"))
    )
    assert provider_disabled == [
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "trpc-wecom-connector"},
            "spec": {"replicas": 0},
        }
    ]


def test_generated_overlay_pins_runtime_pods_to_controlled_label(tmp_path) -> None:
    path = _write_overlay(
        tmp_path,
        namespace="trpc-runtime-gate-test",
        image="registry.example/trpc-service:test",
        node_label=("trpc-runtime-gate", "acceptance"),
    )
    rendered = path.read_text(encoding="utf-8")
    patch = (tmp_path / "controlled-node-patch.yaml").read_text(encoding="utf-8")
    assert "controlled-node-patch.yaml" in rendered
    assert "kind: Deployment" in rendered
    assert 'trpc-runtime-gate: "acceptance"' in patch


def test_generated_overlay_configures_private_registry_for_deployments_and_jobs(
    tmp_path,
) -> None:
    path = _write_overlay(
        tmp_path,
        namespace="trpc-runtime-gate-test",
        image="registry.example/trpc-service:test",
        image_pull_secret="ghcr-pull",
    )

    rendered = path.read_text(encoding="utf-8")
    assert rendered.count("path: image-pull-secret-patch.yaml") == 2
    assert "kind: Deployment" in rendered
    assert "kind: Job" in rendered
    patch = yaml.safe_load((tmp_path / "image-pull-secret-patch.yaml").read_text(encoding="utf-8"))
    assert patch == [
        {
            "op": "add",
            "path": "/spec/template/spec/imagePullSecrets",
            "value": [{"name": "ghcr-pull"}],
        }
    ]


def test_generated_overlay_rejects_invalid_image_pull_secret_name(tmp_path) -> None:
    with pytest.raises(ValueError, match="image pull Secret name is invalid"):
        _write_overlay(
            tmp_path,
            namespace="trpc-runtime-gate-test",
            image="registry.example/trpc-service:test",
            image_pull_secret="registry/secret",
        )


def test_image_pull_secret_metadata_contract_reports_metadata_only(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(arguments, **_kwargs):
        calls.append(arguments)
        return CommandResult(
            status="pass",
            stdout=(
                "Secret\ttrpc-service-secrets\t\tOpaque\n"
                "Secret\tghcr-pull\t\tkubernetes.io/dockerconfigjson\n"
            ),
        )

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    result = runtime_gate._image_pull_secret_metadata_contract(
        "C:/secure/runtime-secrets.yaml",
        "ghcr-pull",
        namespace="runtime-gate",
        context="prod",
        timeout_seconds=30,
    )

    assert result.status == "pass"
    assert result.evidence == {
        "configured": True,
        "secret_name": "ghcr-pull",
        "secret_type": "kubernetes.io/dockerconfigjson",
        "namespace_bound": False,
    }
    report_payload = runtime_gate._result_payload(result)
    assert "stdout" not in report_payload
    assert "data" not in report_payload
    assert "stringData" not in report_payload
    assert calls[0][:2] == ["create", "--dry-run=client"]


@pytest.mark.parametrize(
    ("stdout", "reason"),
    [
        ("Secret\tother\t\tkubernetes.io/dockerconfigjson\n", "exactly one"),
        ("Secret\tghcr-pull\t\tOpaque\n", "dockerconfigjson"),
        (
            "Secret\tghcr-pull\tother\tkubernetes.io/dockerconfigjson\n",
            "different namespace",
        ),
    ],
)
def test_image_pull_secret_metadata_contract_fails_closed(monkeypatch, stdout, reason) -> None:
    monkeypatch.setattr(
        runtime_gate,
        "_kubectl",
        lambda *_args, **_kwargs: CommandResult(status="pass", stdout=stdout),
    )
    result = runtime_gate._image_pull_secret_metadata_contract(
        "C:/secure/runtime-secrets.yaml",
        "ghcr-pull",
        namespace="runtime-gate",
        context="prod",
        timeout_seconds=30,
    )
    assert result.status == "fail"
    assert reason in result.reason


def test_generated_kind_overlay_uses_bounded_local_capacity(tmp_path) -> None:
    path = _write_overlay(
        tmp_path,
        namespace="trpc-runtime-gate-test",
        image="registry.example/trpc-service:test",
        local_kind=True,
    )

    rendered = path.read_text(encoding="utf-8")
    assert "kind-capacity-patch.yaml" in rendered
    assert "replicas-patch.yaml" not in rendered
    documents = list(
        yaml.safe_load_all((tmp_path / "kind-capacity-patch.yaml").read_text(encoding="utf-8"))
    )
    deployments = {
        item["metadata"]["name"]: item["spec"]["replicas"]
        for item in documents
        if item["kind"] == "Deployment"
    }
    hpas = {
        item["metadata"]["name"]: (item["spec"]["minReplicas"], item["spec"]["maxReplicas"])
        for item in documents
        if item["kind"] == "HorizontalPodAutoscaler"
    }
    assert deployments["trpc-worker"] == 2
    assert all(deployments[name] == 2 for name in runtime_gate._PDB_PROTECTED_DEPLOYMENTS)
    assert deployments["trpc-session-recovery"] == 1
    assert deployments["trpc-admin"] == 1
    assert hpas == {"trpc-worker": (2, 4), "trpc-gateway": (2, 2)}


def test_generated_production_overlay_keeps_production_replica_patch(tmp_path) -> None:
    path = _write_overlay(
        tmp_path,
        namespace="trpc-runtime-gate-test",
        image="registry.example/trpc-service:test",
        local_kind=False,
    )

    rendered = path.read_text(encoding="utf-8")
    assert "replicas-patch.yaml" in rendered
    assert "kind-capacity-patch.yaml" not in rendered
    assert not (tmp_path / "kind-capacity-patch.yaml").exists()


def test_rolling_upgrade_sets_and_waits_for_each_deployment_serially(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_kubectl(arguments, **_kwargs):
        action = arguments[0]
        deployment = arguments[2].split("/", 1)[1]
        calls.append((action, deployment))
        return CommandResult(status="pass")

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    image_updates, rollouts = _rolling_upgrade_serial(
        "trpc-agent-service:upgrade",
        namespace="runtime-gate",
        context="kind-runtime-gate",
        timeout_seconds=30,
    )

    expected: list[tuple[str, str]] = []
    for deployment, _container in runtime_gate.ACK_RUNTIME_DEPLOYMENTS:
        expected.extend([("set", deployment), ("rollout", deployment)])
    assert calls == expected
    assert set(image_updates) == {name for name, _container in runtime_gate.ACK_RUNTIME_DEPLOYMENTS}
    assert set(rollouts) == set(image_updates)
    assert "trpc-wecom-connector" not in image_updates


def test_live_gate_without_kubectl_is_not_run(tmp_path, monkeypatch) -> None:
    secret = tmp_path / "secrets.yaml"
    secret.write_text(
        "apiVersion: v1\nkind: Secret\nmetadata:\n  name: trpc-service-secrets\n", encoding="utf-8"
    )
    output = tmp_path / "kubernetes-runtime.json"
    monkeypatch.setenv("TRPC_K8S_RUNTIME_IMAGE", "registry.example/trpc-service:test")
    monkeypatch.setenv("TRPC_K8S_RUNTIME_UPGRADE_IMAGE", "registry.example/trpc-service:next")
    monkeypatch.setenv("TRPC_K8S_RUNTIME_SECRET_MANIFEST", str(secret))
    monkeypatch.setattr("scripts.kubernetes_runtime_gate.shutil.which", lambda _name: None)

    exit_code = _run_live(
        output=output,
        context=None,
        timeout_seconds=1,
        require_runtime=False,
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert "kube_context" in report["candidate"]["checks"]
    assert report["candidate"]["checks"]["kube_context"]["status"] == "not_run"


def test_live_gate_requires_opt_in(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("TRPC_K8S_RUNTIME_TESTS_ENABLED", raising=False)
    output = tmp_path / "kubernetes-runtime.json"
    monkeypatch.setattr(sys, "argv", ["kubernetes_runtime_gate.py", "--output", str(output)])
    from scripts.kubernetes_runtime_gate import main

    assert main() == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"


def test_config_preflight_failure_stops_before_live_gate(monkeypatch, tmp_path) -> None:
    output = tmp_path / "kubernetes-runtime.json"
    preflight_output = tmp_path / "deployment-preflight.json"

    def unexpected_live_gate(**_kwargs):
        raise AssertionError("live gate must not run after configuration preflight failure")

    monkeypatch.setattr(runtime_gate, "_run_live", unexpected_live_gate)

    assert (
        runtime_gate.main(
            [
                "--config",
                str(tmp_path / "missing-runtime-gate.yaml"),
                "--preflight-output",
                str(preflight_output),
                "--output",
                str(output),
                "--require-runtime",
            ]
        )
        == 1
    )
    preflight = json.loads(preflight_output.read_text(encoding="utf-8"))
    report = json.loads(output.read_text(encoding="utf-8"))
    assert preflight["gate"] == "fail"
    assert report["gate"] == "not_run"
    assert report["candidate"]["checks"]["configuration_preflight"]["status"] == "fail"


def test_config_projects_context_and_timeout_once_before_live_gate(monkeypatch, tmp_path) -> None:
    output = tmp_path / "kubernetes-runtime.json"
    preflight_output = tmp_path / "deployment-preflight.json"
    calls: list[dict[str, object]] = []
    projected = {
        "TRPC_K8S_RUNTIME_TESTS_ENABLED": "true",
        "TRPC_K8S_RUNTIME_CONTEXT": "config-context",
        "TRPC_K8S_RUNTIME_TIMEOUT_SECONDS": "123",
    }

    monkeypatch.setattr(
        runtime_gate,
        "build_preflight",
        lambda *_args, **_kwargs: (
            {"gate": "pass", "rejection_reasons": [], "checks": []},
            projected,
        ),
    )

    def fake_live_gate(**kwargs):
        calls.append(kwargs)
        output.write_text('{"gate":"pass"}\n', encoding="utf-8")
        return 0

    monkeypatch.setattr(runtime_gate, "_run_live", fake_live_gate)

    assert (
        runtime_gate.main(
            [
                "--config",
                str(tmp_path / "runtime-gate.yaml"),
                "--preflight-output",
                str(preflight_output),
                "--output",
                str(output),
                "--require-runtime",
            ]
        )
        == 0
    )
    assert len(calls) == 1
    assert calls[0]["context"] == "config-context"
    assert calls[0]["timeout_seconds"] == 123.0


def test_config_rejects_duplicate_cli_context(tmp_path) -> None:
    with pytest.raises(SystemExit):
        runtime_gate.main(
            [
                "--config",
                str(tmp_path / "runtime-gate.yaml"),
                "--context",
                "other-context",
            ]
        )


def test_required_runtime_checks_include_schema_migration(tmp_path) -> None:
    from scripts.kubernetes_runtime_gate import _report

    output = tmp_path / "kubernetes-runtime.json"
    result = _report(
        output,
        gate="not_run",
        candidate={"mode": "test", "checks": {"schema_migration": {"status": "not_run"}}},
        rejection_reasons=["schema migration was not run"],
    )

    assert "schema_migration" in result["baseline"]["required_checks"]
    assert "manifest_contract" in result["baseline"]["required_checks"]
    assert "scheduler_cutover_guard" in result["baseline"]["required_checks"]
    assert result["schema_version"] == 1


def test_production_prerequisites_require_release_binding(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)

    missing = runtime_gate._missing_prerequisites(
        allow_local_images=True,
        require_release_binding=True,
    )

    assert "TRPC_RELEASE_ID is required for a production runtime acceptance" in missing
    assert "TRPC_RELEASE_NONCE is required for a production runtime acceptance" in missing


def test_kind_prerequisites_can_skip_production_release_binding(monkeypatch) -> None:
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)

    missing = runtime_gate._missing_prerequisites(
        allow_local_images=True,
        require_release_binding=False,
    )

    assert not any("TRPC_RELEASE_" in reason for reason in missing)


def test_runtime_prerequisites_reject_invalid_image_pull_secret(monkeypatch) -> None:
    monkeypatch.setenv("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET", "registry/secret")
    missing = runtime_gate._missing_prerequisites(
        allow_local_images=True,
        require_release_binding=False,
    )
    assert "TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET is invalid" in missing


def test_runtime_report_rejects_symlink_output(tmp_path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    output = tmp_path / "runtime.json"
    try:
        output.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable in this environment")
    with pytest.raises(ValueError, match="must not be a symlink"):
        _report(
            output,
            gate="not_run",
            candidate={"mode": "test", "checks": {}},
            rejection_reasons=["not run"],
        )


def test_live_wrapper_uses_final_report_for_required_exit_code(monkeypatch, tmp_path) -> None:
    output = tmp_path / "runtime.json"

    def fake_run_live_once(**kwargs):
        del kwargs
        output.write_text(
            json.dumps({"schema_version": 1, "gate": "fail", "production_gate": "not_run"}),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(runtime_gate, "_run_live_once", fake_run_live_once)
    assert (
        _run_live(
            output=output,
            context=None,
            timeout_seconds=1,
            require_runtime=True,
        )
        == 1
    )


def test_runtime_report_fails_closed_without_attestation(tmp_path) -> None:
    output = tmp_path / "kubernetes-runtime.json"
    candidate = {
        "mode": "live_kubernetes_control_plane",
        "namespace": "isolated",
        "checks": {name: {"status": "pass"} for name in _REQUIRED_RUNTIME_CHECKS},
    }
    result = _report(
        output,
        gate="pass",
        candidate=candidate,
        rejection_reasons=[],
    )
    assert result["gate"] == "fail"
    assert result["production_gate"] == "fail"
    assert "runtime_attestation is missing" in result["rejection_reasons"]


def test_runtime_report_requires_controlled_node_eviction(tmp_path) -> None:
    output = tmp_path / "kubernetes-runtime.json"
    checks = {name: {"status": "pass"} for name in _REQUIRED_RUNTIME_CHECKS}
    checks["initial_image_ids"] = {"trpc-worker": ["sha256:" + "1" * 64]}
    checks["rolling_upgrade"] = {
        "status": "pass",
        "image_ids": {"upgrade": {"trpc-worker": ["sha256:" + "2" * 64]}},
    }
    candidate = {
        "mode": "live_kubernetes_control_plane",
        "namespace": "isolated",
        "run_nonce": "a" * 32,
        "checks": checks,
        "runtime_attestation": {
            "status": "pass",
            "namespace_isolated": True,
            "namespace": "isolated",
            "run_nonce": "a" * 32,
            "cluster_identity": {
                "context_sha256": "b" * 64,
                "fingerprint_sha256": "a" * 64,
                "server_observed": True,
            },
            "node_identity": {"fingerprint_sha256": "c" * 64},
            "actions": {
                **{name: True for name in _REQUIRED_RUNTIME_ACTIONS},
                "node_eviction": False,
            },
            "image_ids": {
                "initial": checks["initial_image_ids"],
                "upgrade": checks["rolling_upgrade"]["image_ids"]["upgrade"],
            },
            "eviction_scope": "namespace_pod_eviction",
            "node_eviction_status": "not_run",
        },
    }
    result = _report(output, gate="pass", candidate=candidate, rejection_reasons=[])
    assert result["gate"] == "fail"
    assert result["production_gate"] == "fail"
    assert "node eviction was not observed" in result["rejection_reasons"]
    assert result["candidate"]["runtime_attestation"]["actions"]["node_eviction"] is False


def test_runtime_attestation_does_not_promote_node_preflight_as_eviction() -> None:
    candidate = {
        "namespace": "runtime-gate",
        "run_nonce": "a" * 32,
        "controlled_node": {"fingerprint_sha256": "b" * 64},
        "checks": {
            "node_eviction": {
                "status": "not_run",
                "preflight": {"status": "pass", "node_schedulable": True},
            }
        },
    }

    attestation = runtime_gate._build_runtime_attestation(
        candidate,
        context="ack-test",
        cluster_stdout="{}",
    )

    assert attestation["actions"]["node_eviction"] is False
    assert attestation["node_eviction_status"] == "not_run"
    assert attestation["eviction_scope"] == "namespace_pod_eviction"


def test_runtime_attestation_contract_requires_all_actions() -> None:
    hpa_observation = _externalize_hpa_observation(
        {
            "status": "pass",
            "observed_live": True,
            "source": "kubectl_api",
            "hpa_name": "trpc-worker",
            "metric_name": "trpc_session_ready_backlog",
            "run_nonce": "a" * 32,
            "namespace": "isolated",
            "cluster_identity": {"fingerprint_sha256": "a" * 64},
            "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
            "driver_evidence": _job_evidence("isolated", "a" * 32, "a" * 64),
            "scale_up_timeout_seconds": 120,
            "scale_down_timeout_seconds": 360,
            "before": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
            },
            "during": {
                "metric_value": 25,
                "desired_replicas": 4,
                "current_replicas": 4,
                "ready_replicas": 4,
            },
            "after": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
            },
        }
    )
    checks = {name: {"status": "pass"} for name in _REQUIRED_RUNTIME_CHECKS}
    checks["hpa_load_observation"] = {"status": "pass", "observation": hpa_observation}
    initial_image_ids = {
        deployment: ["sha256:" + "1" * 64]
        for deployment, _container in runtime_gate.ACK_RUNTIME_DEPLOYMENTS
    }
    upgrade_image_ids = {
        deployment: ["sha256:" + "2" * 64]
        for deployment, _container in runtime_gate.ACK_RUNTIME_DEPLOYMENTS
    }
    checks["initial_image_ids"] = deepcopy(initial_image_ids)
    checks["rolling_upgrade"]["image_ids"] = {
        "initial": deepcopy(initial_image_ids),
        "upgrade": deepcopy(upgrade_image_ids),
        "changed": {deployment: True for deployment in initial_image_ids},
    }
    checks["rolling_upgrade"]["rollback"] = {
        "status": "pass",
        "deployment": "trpc-worker",
        "failure_injected": True,
        "failure_observed": True,
        "undo_observed": True,
        "readiness_recovered": True,
        "restored_image_ids": deepcopy(upgrade_image_ids["trpc-worker"]),
    }
    candidate = {
        "namespace": "isolated",
        "run_nonce": "a" * 32,
        "topology": {
            "scope": "unified_cluster_runtime",
            "im_deployment": "production_cluster",
            "production_deployments": [
                name for name, _container in runtime_gate.PRODUCTION_DEPLOYMENTS
            ],
            "tested_deployments": [
                name for name, _container in runtime_gate.ACK_RUNTIME_DEPLOYMENTS
            ],
            "provider_disabled_deployments": ["trpc-wecom-connector"],
        },
        "checks": checks,
        "runtime_attestation": {
            "status": "pass",
            "namespace_isolated": True,
            "namespace": "isolated",
            "run_nonce": "a" * 32,
            "cluster_identity": {
                "context_sha256": "b" * 64,
                "fingerprint_sha256": "a" * 64,
                "server_observed": True,
            },
            "node_identity": {"fingerprint_sha256": "c" * 64},
            "actions": {
                **{name: True for name in _REQUIRED_RUNTIME_ACTIONS},
            },
            "image_ids": {
                "initial": initial_image_ids,
                "upgrade": upgrade_image_ids,
            },
            "eviction_scope": "namespace_pod_eviction+controlled_node",
            "node_eviction_status": "pass",
        },
    }
    valid, reasons = _runtime_attestation_contract(candidate)
    assert valid
    assert reasons == ()

    candidate_without_provider_isolation = deepcopy(candidate)
    candidate_without_provider_isolation["topology"]["provider_disabled_deployments"] = []
    valid, reasons = _runtime_attestation_contract(candidate_without_provider_isolation)
    assert not valid
    assert any("provider-disabled deployment set" in reason for reason in reasons)

    candidate_without_rollback = deepcopy(candidate)
    candidate_without_rollback["checks"]["rolling_upgrade"].pop("rollback")
    valid, reasons = _runtime_attestation_contract(candidate_without_rollback)
    assert not valid
    assert any("failure rollback" in reason for reason in reasons)

    candidate_with_stale_check = deepcopy(candidate)
    candidate_with_stale_check["checks"]["initial_image_ids"]["trpc-worker"] = [
        "sha256:" + "3" * 64
    ]
    valid, reasons = _runtime_attestation_contract(candidate_with_stale_check)
    assert not valid
    assert any("checks.initial_image_ids" in reason for reason in reasons)

    candidate_with_extra_deployment = deepcopy(candidate)
    candidate_with_extra_deployment["checks"]["rolling_upgrade"]["image_ids"]["upgrade"][
        "unexpected-deployment"
    ] = ["sha256:" + "3" * 64]
    valid, reasons = _runtime_attestation_contract(candidate_with_extra_deployment)
    assert not valid
    assert any("deployment set" in reason for reason in reasons)

    candidate_with_false_changed = deepcopy(candidate)
    candidate_with_false_changed["checks"]["rolling_upgrade"]["image_ids"]["changed"][
        "trpc-worker"
    ] = False
    valid, reasons = _runtime_attestation_contract(candidate_with_false_changed)
    assert not valid
    assert any("changed" in reason for reason in reasons)

    candidate["runtime_attestation"]["image_ids"]["upgrade"]["trpc-worker"] = ["sha256:" + "1" * 64]
    valid, reasons = _runtime_attestation_contract(candidate)
    assert not valid
    assert any("did not change for trpc-worker" in reason for reason in reasons)


def test_hpa_load_observation_rejects_manual_scale_only() -> None:
    valid, reasons = _hpa_load_observation_contract(
        {
            "status": "pass",
            "observed_live": True,
            "source": "kubectl_api",
            "hpa_name": "trpc-worker",
            "metric_name": "trpc_session_ready_backlog",
            "scale_up_timeout_seconds": 120,
            "scale_down_timeout_seconds": 360,
            "trigger": {"kind": "manual_scale", "source": "kubectl"},
            "cluster_identity": {"fingerprint_sha256": "a" * 64},
            "before": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
            },
            "during": {
                "metric_value": 0,
                "desired_replicas": 4,
                "current_replicas": 4,
                "ready_replicas": 4,
            },
            "after": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
            },
        }
    )
    assert not valid
    assert any("controlled backlog" in reason for reason in reasons)


def test_hpa_load_observation_requires_scale_up_and_down() -> None:
    valid, reasons = _hpa_load_observation_contract(
        _externalize_hpa_observation(
            {
                "status": "pass",
                "observed_live": True,
                "source": "kubectl_api",
                "hpa_name": "trpc-worker",
                "metric_name": "trpc_session_ready_backlog",
                "scale_up_timeout_seconds": 120,
                "scale_down_timeout_seconds": 360,
                "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
                "run_nonce": "a" * 32,
                "namespace": "isolated",
                "driver_evidence": _job_evidence("isolated", "a" * 32, "a" * 64),
                "cluster_identity": {"fingerprint_sha256": "a" * 64},
                "before": {
                    "metric_value": 0,
                    "desired_replicas": 2,
                    "current_replicas": 2,
                    "ready_replicas": 2,
                },
                "during": {
                    "metric_value": 25,
                    "desired_replicas": 4,
                    "current_replicas": 4,
                    "ready_replicas": 4,
                },
                "after": {
                    "metric_value": 0,
                    "desired_replicas": 2,
                    "current_replicas": 2,
                    "ready_replicas": 2,
                },
            }
        ),
        cluster_fingerprint="a" * 64,
    )
    assert valid
    assert reasons == ()


def test_hpa_load_observation_accepts_cached_metric_and_status_lag() -> None:
    nonce = "a" * 32
    fingerprint = "a" * 64
    evidence = _externalize_hpa_observation(
        {
            "status": "pass",
            "observed_live": True,
            "source": "kubectl_api",
            "hpa_name": "trpc-worker",
            "metric_name": "trpc_session_ready_backlog",
            "scale_up_timeout_seconds": 120,
            "scale_down_timeout_seconds": 360,
            "trigger": {"kind": "controlled_backlog", "source": "bounded-driver"},
            "run_nonce": nonce,
            "namespace": "isolated",
            "driver_evidence": _job_evidence("isolated", nonce, fingerprint),
            "cluster_identity": {"fingerprint_sha256": fingerprint},
            "before": {
                "metric_value": 25,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
            },
            "during": {
                "metric_value": 25,
                "desired_replicas": 2,
                "current_replicas": 4,
                "ready_replicas": 4,
            },
            "after": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 4,
                "ready_replicas": 2,
            },
        }
    )
    valid, reasons = _hpa_load_observation_contract(
        evidence,
        cluster_fingerprint=fingerprint,
        run_nonce=nonce,
        namespace="isolated",
    )

    assert valid, reasons
    assert reasons == ()

    evidence["after"]["ready_replicas"] = 4
    valid, reasons = _hpa_load_observation_contract(
        evidence,
        cluster_fingerprint=fingerprint,
        run_nonce=nonce,
        namespace="isolated",
    )

    assert not valid
    assert any("ready replicas did not return" in reason for reason in reasons)


def test_hpa_load_report_payload_is_fixed_and_content_free() -> None:
    payload = _hpa_load_report_payload(
        {
            "status": "pass",
            "observed_live": True,
            "source": "kubectl_api",
            "hpa_name": "trpc-worker",
            "metric_name": "trpc_session_ready_backlog",
            "run_nonce": "run-1",
            "namespace": "gate-1",
            "cluster_identity": {"fingerprint_sha256": "a" * 64},
            "trigger": {"kind": "controlled_backlog", "source": "driver-secret"},
            "scale_up_timeout_seconds": 120,
            "scale_down_timeout_seconds": 360,
            "payload": "should-not-be-reported",
            "before": {
                "metric_value": 0,
                "desired_replicas": 2,
                "current_replicas": 2,
                "ready_replicas": 2,
                "secret": "should-not-be-reported",
            },
            "during": {"metric_value": 25, "desired_replicas": 4},
            "after": {"metric_value": 0, "desired_replicas": 2},
        }
    )
    assert payload["trigger"] == {"kind": "controlled_backlog", "source": "bounded-driver"}
    assert "payload" not in payload
    assert "secret" not in json.dumps(payload)
    assert payload["before"] == {
        "metric_value": 0,
        "desired_replicas": 2,
        "current_replicas": 2,
        "ready_replicas": 2,
    }


def test_hpa_observation_is_derived_from_kubernetes_objects() -> None:
    observation, reasons = _hpa_observation_from_api(
        {
            "metadata": {"name": "trpc-worker"},
            "status": {
                "currentReplicas": 4,
                "desiredReplicas": 5,
                "currentMetrics": [
                    {
                        "type": "External",
                        "external": {
                            "metric": {"name": "trpc_session_ready_backlog"},
                            "current": {"value": "25"},
                        },
                    }
                ],
            },
        },
        {"status": {"readyReplicas": 5}},
    )
    assert reasons == ()
    assert observation == {
        "metric_value": 25.0,
        "desired_replicas": 5.0,
        "current_replicas": 4.0,
        "ready_replicas": 5.0,
    }


def test_hpa_observation_rejects_nonfinite_api_values() -> None:
    observation, reasons = _hpa_observation_from_api(
        {
            "metadata": {"name": "trpc-worker"},
            "status": {
                "currentReplicas": 2,
                "desiredReplicas": 2,
                "currentMetrics": [
                    {
                        "type": "External",
                        "external": {
                            "metric": {"name": "trpc_session_ready_backlog"},
                            "current": {"value": "NaN"},
                        },
                    }
                ],
            },
        },
        {"status": {"readyReplicas": 2}},
    )
    assert observation is None
    assert any("finite backlog" in reason for reason in reasons)


def test_external_metric_api_parser_requires_one_namespace_bound_value() -> None:
    payload = {
        "kind": "ExternalMetricValueList",
        "apiVersion": "external.metrics.k8s.io/v1beta1",
        "items": [
            {
                "metricName": "trpc_session_ready_backlog",
                "metricLabels": {"namespace": "runtime-gate"},
                "value": "25",
            }
        ],
    }
    value, evidence, reasons = runtime_gate._external_metric_from_api(
        payload, namespace="runtime-gate"
    )
    assert value == 25.0
    assert reasons == ()
    assert evidence == {
        "api_observed": True,
        "api_version": "v1beta1",
        "metric_name": "trpc_session_ready_backlog",
        "namespace": "runtime-gate",
        "item_count": 1,
        "label_namespace": "runtime-gate",
        "value": 25.0,
    }

    for invalid in (
        {**payload, "items": []},
        {
            **payload,
            "items": [
                {
                    **payload["items"][0],
                    "metricLabels": {"namespace": "other"},
                }
            ],
        },
        {
            **payload,
            "items": [
                {
                    **payload["items"][0],
                    "value": "NaN",
                }
            ],
        },
        {**payload, "apiVersion": "external.metrics.k8s.io/v1alpha1"},
    ):
        value, _evidence, reasons = runtime_gate._external_metric_from_api(
            invalid, namespace="runtime-gate"
        )
        assert value is None
        assert reasons


def test_observe_hpa_state_uses_external_total_for_average_value_hpa(monkeypatch) -> None:
    hpa = {
        "metadata": {"name": "trpc-worker"},
        "status": {
            "currentReplicas": 2,
            "desiredReplicas": 3,
            "currentMetrics": [
                {
                    "type": "External",
                    "external": {
                        "metric": {"name": "trpc_session_ready_backlog"},
                        "current": {"averageValue": "10"},
                    },
                }
            ],
        },
    }
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        calls.append(arguments)
        if arguments[1] == "hpa":
            return CommandResult(status="pass"), hpa
        if arguments[1] == "--raw":
            return CommandResult(status="pass"), {
                "kind": "ExternalMetricValueList",
                "apiVersion": "external.metrics.k8s.io/v1beta1",
                "items": [
                    {
                        "metricName": "trpc_session_ready_backlog",
                        "metricLabels": {"namespace": "runtime-gate"},
                        "value": "40",
                    }
                ],
            }
        return CommandResult(status="pass"), {"status": {"readyReplicas": 3}}

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    result, observation = runtime_gate._observe_hpa_state(
        namespace="runtime-gate", context="kind-test", timeout_seconds=5
    )
    assert result.status == "pass"
    assert observation is not None
    assert observation["metric_value"] == 40.0
    assert observation["hpa_metric_value"] == 10.0
    assert observation["external_metric"]["value"] == 40.0
    assert calls[-1][1] == "--raw"


@pytest.mark.parametrize(
    ("quantity", "expected"),
    (("25e0", 25.0), ("2500m", 2.5), ("1Ki", 1024.0)),
)
def test_hpa_observation_parses_kubernetes_quantity_forms(quantity: str, expected: float) -> None:
    observation, reasons = _hpa_observation_from_api(
        {
            "metadata": {"name": "trpc-worker"},
            "status": {
                "currentReplicas": 4,
                "desiredReplicas": 4,
                "currentMetrics": [
                    {
                        "type": "External",
                        "external": {
                            "metric": {"name": "trpc_session_ready_backlog"},
                            "current": {"value": quantity},
                        },
                    }
                ],
            },
        },
        {"status": {"readyReplicas": 4}},
    )

    assert reasons == ()
    assert observation is not None
    assert observation["metric_value"] == expected


def test_observe_hpa_state_reads_hpa_and_deployment_from_api(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        calls.append(arguments)
        if arguments[1] == "hpa":
            return CommandResult(status="pass"), {
                "metadata": {"name": "trpc-worker"},
                "status": {
                    "currentReplicas": 2,
                    "desiredReplicas": 3,
                    "currentMetrics": [
                        {
                            "type": "External",
                            "external": {
                                "metric": {"name": "trpc_session_ready_backlog"},
                                "current": {"value": "10"},
                            },
                        }
                    ],
                },
            }
        if arguments[1] == "--raw":
            return CommandResult(status="pass"), {
                "kind": "ExternalMetricValueList",
                "apiVersion": "external.metrics.k8s.io/v1beta1",
                "items": [
                    {
                        "metricName": "trpc_session_ready_backlog",
                        "metricLabels": {"namespace": "runtime-gate"},
                        "value": "10",
                    }
                ],
            }
        return CommandResult(status="pass"), {"status": {"readyReplicas": 3}}

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    result, observation = _observe_hpa_state(
        namespace="runtime-gate", context="kind-test", timeout_seconds=5
    )
    assert result.status == "pass"
    assert observation == {
        "metric_value": 10.0,
        "hpa_metric_value": 10.0,
        "desired_replicas": 3.0,
        "current_replicas": 2.0,
        "ready_replicas": 3.0,
        "external_metric": {
            "api_observed": True,
            "api_version": "v1beta1",
            "api_path": (
                "/apis/external.metrics.k8s.io/v1beta1/namespaces/runtime-gate/"
                "trpc_session_ready_backlog"
            ),
            "metric_name": "trpc_session_ready_backlog",
            "namespace": "runtime-gate",
            "label_namespace": "runtime-gate",
            "item_count": 1,
            "value": 10.0,
        },
    }
    assert calls == [
        ["get", "hpa", "trpc-worker", "--namespace", "runtime-gate", "-o", "json"],
        ["get", "deployment", "trpc-worker", "--namespace", "runtime-gate", "-o", "json"],
        [
            "get",
            "--raw",
            "/apis/external.metrics.k8s.io/v1beta1/namespaces/runtime-gate/"
            "trpc_session_ready_backlog",
        ],
    ]


def test_wait_for_hpa_status_retries_startup_race_until_contract_is_healthy(monkeypatch) -> None:
    unhealthy = {
        "status": {
            "conditions": [
                {"type": "AbleToScale", "status": "True"},
                {"type": "ScalingActive", "status": "False"},
            ],
            "currentMetrics": [],
            "currentReplicas": 0,
            "desiredReplicas": 0,
        },
        "spec": {
            "metrics": [
                {
                    "type": "External",
                    "external": {"metric": {"name": "trpc_session_ready_backlog"}},
                }
            ]
        },
    }
    healthy = {
        "status": {
            "conditions": [
                {"type": "AbleToScale", "status": "True"},
                {"type": "ScalingActive", "status": "True"},
            ],
            "currentMetrics": [
                {
                    "type": "External",
                    "external": {
                        "metric": {"name": "trpc_session_ready_backlog"},
                        "current": {"value": "0"},
                    },
                }
            ],
            "currentReplicas": 2,
            "desiredReplicas": 2,
        },
        "spec": {
            "metrics": [
                {
                    "type": "External",
                    "external": {"metric": {"name": "trpc_session_ready_backlog"}},
                }
            ]
        },
    }
    responses = iter([unhealthy, healthy])
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        calls.append(arguments)
        return CommandResult(status="pass"), next(responses)

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, payload = runtime_gate._wait_for_hpa_status(
        namespace="runtime-gate", context="kind-test", timeout_seconds=5
    )

    assert result.status == "pass"
    assert payload == healthy
    assert result.evidence == {"poll_count": 2}
    assert len(calls) == 2


def test_wait_for_hpa_status_times_out_with_last_snapshot_and_reasons(monkeypatch) -> None:
    unhealthy = {
        "status": {
            "conditions": [
                {"type": "AbleToScale", "status": "True"},
                {"type": "ScalingActive", "status": "False"},
            ],
            "currentMetrics": [],
            "currentReplicas": 0,
            "desiredReplicas": 0,
        },
        "spec": {"metrics": [{"type": "External"}]},
    }
    clock = [0.0]

    def fake_json(*_args, **_kwargs):
        return CommandResult(status="pass"), unhealthy

    def fake_monotonic() -> float:
        return clock[0]

    def fake_sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    monkeypatch.setattr(runtime_gate.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(runtime_gate.time, "sleep", fake_sleep)

    result, payload = runtime_gate._wait_for_hpa_status(
        namespace="runtime-gate", context="kind-test", timeout_seconds=2
    )

    assert result.status == "fail"
    assert payload == unhealthy
    assert "worker HPA did not become healthy before timeout" in result.reason
    assert "worker HPA did not expose ScalingActive=True" in result.reason
    assert result.evidence == {
        "poll_count": 2,
        "last_snapshot_available": True,
        "last_reasons": [
            "worker HPA did not expose ScalingActive=True",
            "worker HPA has no current metric samples",
            "worker HPA does not configure the exact backlog external metric",
            "worker HPA currentReplicas is below the configured minimum",
            "worker HPA desiredReplicas is below the configured minimum",
        ],
    }


def _hpa_snapshot(metric: float, desired: float, current: float, ready: float) -> dict[str, float]:
    return {
        "metric_value": metric,
        "desired_replicas": desired,
        "current_replicas": current,
        "ready_replicas": ready,
    }


def test_hpa_during_wait_retries_transient_missing_metric_until_scale_up(monkeypatch) -> None:
    before = _hpa_snapshot(0, 2, 2, 2)
    responses = iter(
        [
            (
                CommandResult(
                    status="fail", reason="HPA API response has no finite backlog metric value"
                ),
                None,
            ),
            (CommandResult(status="pass"), before),
            (CommandResult(status="pass"), _hpa_snapshot(25, 4, 4, 4)),
        ]
    )

    monkeypatch.setattr(runtime_gate, "_observe_hpa_state", lambda **kwargs: next(responses))
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, observation = runtime_gate._wait_for_hpa_phase(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=5,
        phase="during",
        before=before,
    )

    assert result.status == "pass"
    assert observation == _hpa_snapshot(25, 4, 4, 4)


def test_hpa_during_wait_accepts_cached_baseline_and_lagging_hpa_status(monkeypatch) -> None:
    # The external-metrics API can still expose the positive value captured
    # before the trigger while Deployment has already reached 4/4 Ready.  In
    # the same window HPA desiredReplicas may still report the old floor.
    before = _hpa_snapshot(25, 2, 2, 2)
    during = _hpa_snapshot(25, 2, 4, 4)
    monkeypatch.setattr(
        runtime_gate,
        "_observe_hpa_state",
        lambda **kwargs: (CommandResult(status="pass"), during),
    )
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, observation = runtime_gate._wait_for_hpa_phase(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=5,
        phase="during",
        before=before,
    )

    assert result.status == "pass"
    assert observation == during


def test_hpa_phase_contract_tolerates_hpa_current_status_lag_after_clear() -> None:
    before = _hpa_snapshot(0, 2, 2, 2)
    during = _hpa_snapshot(25, 4, 2, 4)
    after = _hpa_snapshot(0, 2, 4, 2)

    during_ok, during_reasons = runtime_gate._hpa_phase_transition_contract(
        before, during, phase="during"
    )
    after_ok, after_reasons = runtime_gate._hpa_phase_transition_contract(
        before, after, phase="after", during=during
    )

    assert during_ok, during_reasons
    assert after_ok, after_reasons


def test_hpa_phase_contract_rejects_ready_replicas_above_pre_load_bound() -> None:
    before = _hpa_snapshot(0, 2, 2, 2)
    during = _hpa_snapshot(25, 4, 4, 4)
    after = _hpa_snapshot(0, 2, 4, 4)

    result, reasons = runtime_gate._hpa_phase_transition_contract(
        before, after, phase="after", during=during
    )

    assert not result
    assert "ready replicas did not return" in " ".join(reasons)


def test_hpa_after_wait_retries_until_metric_and_replicas_recover(monkeypatch) -> None:
    before = _hpa_snapshot(0, 2, 2, 2)
    during = _hpa_snapshot(25, 4, 4, 4)
    responses = iter(
        [
            (
                CommandResult(status="fail", reason="metrics API temporarily unavailable"),
                None,
            ),
            (CommandResult(status="pass"), _hpa_snapshot(0, 3, 3, 3)),
            (CommandResult(status="pass"), _hpa_snapshot(0, 2, 2, 2)),
        ]
    )

    monkeypatch.setattr(runtime_gate, "_observe_hpa_state", lambda **kwargs: next(responses))
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)

    result, observation = runtime_gate._wait_for_hpa_phase(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=5,
        phase="after",
        before=before,
        during=during,
    )

    assert result.status == "pass"
    assert observation == _hpa_snapshot(0, 2, 2, 2)


def test_hpa_phase_wait_fails_closed_when_transition_times_out(monkeypatch) -> None:
    before = _hpa_snapshot(0, 2, 2, 2)
    calls = 0

    def fake_monotonic() -> float:
        nonlocal calls
        calls += 1
        return 0.0 if calls <= 3 else 2.0

    monkeypatch.setattr(runtime_gate.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(runtime_gate.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        runtime_gate,
        "_observe_hpa_state",
        lambda **kwargs: (CommandResult(status="pass"), before),
    )

    result, observation = runtime_gate._wait_for_hpa_phase(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=1,
        phase="during",
        before=before,
    )

    assert result.status == "fail"
    assert "before timeout" in result.reason
    assert observation == before


def test_worker_eviction_capacity_patches_hpa_and_waits_without_manual_scale(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        calls.append(arguments)
        return CommandResult(status="pass")

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        assert arguments[:2] == ["get", "hpa"]
        return CommandResult(status="pass"), {"spec": {"minReplicas": 4, "maxReplicas": 8}}

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)

    result, details = runtime_gate._prepare_worker_eviction_capacity(
        namespace="runtime-gate", context="kind-test", timeout_seconds=30
    )

    assert result.status == "pass"
    assert details["min_replicas"] == 4
    assert details["max_replicas"] == 8
    assert details["ready_wait"]["status"] == "pass"
    assert calls[0][:5] == [
        "patch",
        "hpa",
        "trpc-worker",
        "--namespace",
        "runtime-gate",
    ]
    assert calls[0][5] == "--type=merge"
    assert json.loads(calls[0][7]) == {"spec": {"minReplicas": 4}}
    assert calls[1][0] == "wait"
    assert "manual_scale" not in json.dumps(details)
    assert all(arguments[0] != "scale" for arguments in calls)


def test_worker_eviction_capacity_rejects_hpa_max_below_floor(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda arguments, *, context, timeout_seconds: (
            CommandResult(status="pass"),
            {"spec": {"minReplicas": 4, "maxReplicas": 3}},
        ),
    )

    result, details = runtime_gate._prepare_worker_eviction_capacity(
        namespace="runtime-gate", context="kind-test", timeout_seconds=30
    )

    assert result.status == "fail"
    assert "maxReplicas" in result.reason
    assert details["max_replicas"] == 3
    assert len(calls) == 1


def test_local_worker_eviction_capacity_verifies_all_pdb_protected_deployments(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        calls.append(arguments)
        return CommandResult(status="pass")

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "hpa"]:
            return CommandResult(status="pass"), {"spec": {"minReplicas": 4, "maxReplicas": 8}}
        deployment = arguments[2]
        return CommandResult(status="pass"), {
            "spec": {"replicas": 2},
            "status": {"readyReplicas": 2},
            "metadata": {"name": deployment},
        }

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)

    result, details = runtime_gate._prepare_worker_eviction_capacity(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=30,
        local_kind=True,
    )

    assert result.status == "pass"
    assert details["pdb_capacity"]["status"] == "pass"
    assert set(details["pdb_capacity"]["deployments"]) == (runtime_gate._PDB_PROTECTED_DEPLOYMENTS)
    assert calls[0][0:3] == ["patch", "hpa", "trpc-worker"]
    assert calls[1][0] == "wait"
    assert all(arguments[0] != "scale" for arguments in calls)


def test_local_worker_eviction_capacity_fails_closed_on_underprovisioned_pdb(
    monkeypatch,
) -> None:
    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del arguments, context, timeout_seconds, input_text
        return CommandResult(status="pass")

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "hpa"]:
            return CommandResult(status="pass"), {"spec": {"minReplicas": 4, "maxReplicas": 8}}
        ready = 1 if arguments[2] == "trpc-channel-dispatcher" else 2
        return CommandResult(status="pass"), {
            "spec": {"replicas": 2},
            "status": {"readyReplicas": ready},
        }

    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)
    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)

    result, details = runtime_gate._prepare_worker_eviction_capacity(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=30,
        local_kind=True,
    )

    assert result.status == "fail"
    assert "trpc-channel-dispatcher" in result.reason
    assert details["pdb_capacity"]["deployments"]["trpc-channel-dispatcher"] == {
        "status": "fail",
        "desired_replicas": 2,
        "ready_replicas": 1,
        "reason": "PDB-protected Deployment has fewer than two ready replicas",
    }


@pytest.mark.parametrize("timeout", [True, False, 0, -1, 3600.1, float("nan"), float("inf")])
def test_live_timeout_is_rejected_before_runner(monkeypatch, tmp_path, timeout) -> None:
    called = False

    def unexpected_runner(**kwargs):
        nonlocal called
        called = True
        del kwargs
        return 0

    monkeypatch.setattr(runtime_gate, "_run_live_once", unexpected_runner)
    with pytest.raises(ValueError, match="timeout_seconds"):
        _run_live(
            output=tmp_path / "runtime.json",
            context=None,
            timeout_seconds=timeout,
            require_runtime=False,
        )
    assert not called


def test_hpa_driver_path_is_repository_bound(tmp_path) -> None:
    driver = tmp_path / "driver.py"
    driver.write_text("raise SystemExit(0)\n", encoding="utf-8")
    result = runtime_gate._run_hpa_driver(
        str(driver),
        namespace="runtime-gate",
        run_nonce="a" * 32,
        cluster_fingerprint="b" * 64,
        context=None,
        timeout_seconds=5,
        phase="load",
    )
    assert result.status == "not_run"
    assert "repository scripts" in result.reason


def test_hpa_driver_cannot_supply_evidence_file(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    driver = Path(runtime_gate.ROOT) / "scripts" / "kubernetes_runtime_gate.py"
    driver_kubeconfig = tmp_path / "driver-kubeconfig"
    driver_kubeconfig.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
    main_kubeconfig = tmp_path / "main-kubeconfig"
    main_kubeconfig.write_text("apiVersion: v1\nkind: Config\nclusters: []\n", encoding="utf-8")
    monkeypatch.setenv("KUBECONFIG", str(main_kubeconfig))
    monkeypatch.setenv("TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG", str(driver_kubeconfig))
    monkeypatch.setenv(
        "TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256", hashlib.sha256(driver.read_bytes()).hexdigest()
    )
    for name, value in {
        "PATH": r"C:\Windows\System32",
        "HOME": r"C:\Users\runner",
        "USERPROFILE": r"C:\Users\runner",
        "SYSTEMROOT": r"C:\Windows",
        "WINDIR": r"C:\Windows",
        "TEMP": r"C:\Users\runner\AppData\Local\Temp",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TMP", raising=False)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-inherit")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql://secret")
    monkeypatch.setenv("TRPC_K8S_RUNTIME_IMAGE_PULL_SECRET", "ghcr-pull")

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        payload = {
            "status": "pass",
            "phase": "load",
            "namespace": "runtime-gate",
            "run_nonce": "a" * 32,
            "cluster_fingerprint": "b" * 64,
            "job_uid": "job-uid-1",
        }
        return type(
            "Completed", (), {"returncode": 0, "stdout": json.dumps(payload), "stderr": ""}
        )()

    monkeypatch.setattr(runtime_gate.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runtime_gate,
        "_observe_hpa_driver_job",
        lambda **kwargs: (
            CommandResult(status="pass"),
            {
                "api_observed": True,
                "job_name": f"trpc-hpa-load-{'a' * 20}",
                "job_uid": "job-uid-1",
                "job_labels": {},
                "namespace": kwargs["namespace"],
                "run_nonce": kwargs["run_nonce"],
                "cluster_fingerprint": kwargs["cluster_fingerprint"],
                "phase": "load",
            },
        ),
    )
    result = runtime_gate._run_hpa_driver(
        str(driver),
        namespace="runtime-gate",
        run_nonce="a" * 32,
        cluster_fingerprint="b" * 64,
        context=None,
        timeout_seconds=5,
        phase="load",
    )
    assert result.status == "pass"
    assert "TRPC_K8S_HPA_EVIDENCE_PATH" not in captured["env"]
    assert captured["env"]["TRPC_K8S_HPA_PHASE"] == "load"
    assert captured["env"]["KUBECONFIG"] == str(driver_kubeconfig)
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert set(environment) == {
        "PATH",
        "KUBECONFIG",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TRPC_K8S_HPA_RUN_NONCE",
        "TRPC_K8S_HPA_NAMESPACE",
        "TRPC_K8S_HPA_CLUSTER_FINGERPRINT",
        "TRPC_K8S_HPA_PHASE",
        "TRPC_K8S_HPA_DRIVER_SUBJECT",
        "TRPC_K8S_HPA_DRIVER_JOB_IMAGE",
        "TRPC_K8S_HPA_DRIVER_JOB_COMMAND",
        "TRPC_K8S_HPA_DRIVER_IMAGE_PULL_SECRET",
    }
    assert "TMP" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "TRPC_SERVICE_DATABASE_DSN" not in environment
    assert environment["TRPC_K8S_HPA_DRIVER_IMAGE_PULL_SECRET"] == "ghcr-pull"


def test_driver_kubectl_keeps_required_os_runtime_env_without_parent_secrets(monkeypatch) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(runtime_gate.shutil, "which", lambda _name: "kubectl.exe")
    for name, value in {
        "PATH": r"C:\Windows\System32",
        "HOME": r"C:\Users\runner",
        "USERPROFILE": r"C:\Users\runner",
        "SYSTEMROOT": r"C:\Windows",
        "WINDIR": r"C:\Windows",
        "TEMP": r"C:\Users\runner\AppData\Local\Temp",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TMP", raising=False)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "must-not-inherit")
    monkeypatch.setenv("TRPC_SERVICE_DATABASE_DSN", "postgresql://secret")
    monkeypatch.setenv("GITHUB_TOKEN", "must-not-inherit")

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return type("Completed", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()

    monkeypatch.setattr(runtime_gate.subprocess, "run", fake_run)
    result = runtime_gate._driver_kubectl(
        ["auth", "whoami", "-o", "json"],
        kubeconfig_path="C:\\driver\\kubeconfig",
        context="driver-context",
        timeout_seconds=5,
    )

    assert result.status == "pass"
    assert captured["command"] == [
        "kubectl.exe",
        "--kubeconfig",
        "C:\\driver\\kubeconfig",
        "--context",
        "driver-context",
        "auth",
        "whoami",
        "-o",
        "json",
    ]
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert set(environment) == {
        "PATH",
        "KUBECONFIG",
        "HOME",
        "USERPROFILE",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
    }
    assert environment["KUBECONFIG"] == "C:\\driver\\kubeconfig"
    assert "TMP" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "TRPC_SERVICE_DATABASE_DSN" not in environment
    assert "GITHUB_TOKEN" not in environment


def _install_driver_scope_mocks(
    monkeypatch,
    *,
    non_resource_rules: list[dict[str, object]] | None = None,
    rolebindings: list[dict[str, object]] | None = None,
    clusterrolebindings: list[dict[str, object]] | None = None,
    extra_target_rules: list[dict[str, object]] | None = None,
    transient_review_failures: dict[str, int] | None = None,
) -> tuple[str, str, list[str], list[list[str]]]:
    subject = "system:serviceaccount:trpc-hpa-driver:trpc-hpa-driver"
    namespace = "runtime-gate"
    review_namespaces: list[str] = []
    admin_calls: list[list[str]] = []
    non_resource_rules = non_resource_rules or [
        {"verbs": ["get"], "nonResourceURLs": sorted(runtime_gate._HPA_DRIVER_DISCOVERY_URLS)}
    ]
    rolebindings = rolebindings or [
        {
            "metadata": {"namespace": namespace, "name": "trpc-hpa-load-driver"},
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "namespace": "trpc-hpa-driver",
                    "name": "trpc-hpa-driver",
                }
            ],
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": "trpc-hpa-load-driver",
            },
        }
    ]
    clusterrolebindings = clusterrolebindings or []
    remaining_review_failures = dict(transient_review_failures or {})

    def fake_driver_json(arguments, *, kubeconfig_path, context, timeout_seconds, input_text=None):
        del kubeconfig_path, context, timeout_seconds
        if arguments[:2] == ["auth", "whoami"]:
            return CommandResult(status="pass"), {"status": {"userInfo": {"username": subject}}}
        if arguments[0] == "version":
            return CommandResult(status="pass"), {
                "serverVersion": {
                    "gitVersion": "v1.33.1",
                    "gitCommit": "commit",
                    "platform": "linux/amd64",
                }
            }
        assert arguments[:2] == ["create", "--raw"]
        body = json.loads(str(input_text))
        review_namespace = body["spec"]["namespace"]
        review_namespaces.append(review_namespace)
        if remaining_review_failures.get(review_namespace, 0) > 0:
            remaining_review_failures[review_namespace] -= 1
            return CommandResult(status="fail"), None
        identity_rules = [
            {
                "apiGroups": ["authorization.k8s.io"],
                "resources": ["selfsubjectaccessreviews", "selfsubjectrulesreviews"],
                "verbs": ["create"],
            },
            {
                "apiGroups": ["authentication.k8s.io"],
                "resources": ["selfsubjectreviews"],
                "verbs": ["create"],
            },
        ]
        target_rules = [
            {"apiGroups": ["batch"], "resources": ["jobs"], "verbs": ["create", "get", "delete"]},
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]},
            {"apiGroups": [""], "resources": ["pods/log"], "verbs": ["get"]},
        ]
        resource_rules = identity_rules + (
            target_rules + (extra_target_rules or []) if review_namespace == namespace else []
        )
        return CommandResult(status="pass"), {
            "status": {
                "incomplete": False,
                "resourceRules": resource_rules,
                "nonResourceRules": non_resource_rules,
            }
        }

    def fake_json_command(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        admin_calls.append(arguments)
        if arguments[:2] == ["get", "rolebindings"]:
            return CommandResult(status="pass"), {"items": rolebindings}
        if arguments[:2] == ["get", "clusterrolebindings"]:
            return CommandResult(status="pass"), {"items": clusterrolebindings}
        raise AssertionError(arguments)

    monkeypatch.setattr(runtime_gate, "_driver_json", fake_driver_json)
    monkeypatch.setattr(runtime_gate, "_json_command", fake_json_command)
    return subject, namespace, review_namespaces, admin_calls


def test_driver_scope_allows_standard_discovery_and_audits_bindings(monkeypatch) -> None:
    subject, namespace, review_namespaces, admin_calls = _install_driver_scope_mocks(monkeypatch)
    server_identity = "v1.33.1|commit|linux/amd64"
    cluster_fingerprint = hashlib.sha256(server_identity.encode("utf-8")).hexdigest()

    allowed, reasons, attestation = runtime_gate._driver_identity_and_scope(
        kubeconfig_path="driver.kubeconfig",
        driver_context="trpc-hpa-driver-gate2",
        admin_context="kind-trpc-runtime-gate2",
        subject=subject,
        namespace=namespace,
        cluster_fingerprint=cluster_fingerprint,
        timeout_seconds=5,
    )

    assert allowed, reasons
    assert reasons == []
    assert review_namespaces == [namespace, "default", "kube-system"]
    assert "" not in review_namespaces
    assert admin_calls == [
        ["get", "rolebindings", "--all-namespaces", "-o", "json"],
        ["get", "clusterrolebindings", "-o", "json"],
    ]
    assert attestation["rule_audit"]["complete"] is True
    assert attestation["rule_audit"]["binding_audit"] == {
        "complete": True,
        "matching_rolebinding_count": 1,
        "matching_clusterrolebinding_count": 0,
    }


def test_driver_scope_retries_transient_rules_review_failure(monkeypatch) -> None:
    subject, namespace, review_namespaces, _ = _install_driver_scope_mocks(
        monkeypatch,
        transient_review_failures={"kube-system": 1},
    )
    server_identity = "v1.33.1|commit|linux/amd64"
    cluster_fingerprint = hashlib.sha256(server_identity.encode("utf-8")).hexdigest()

    allowed, reasons, attestation = runtime_gate._driver_identity_and_scope(
        kubeconfig_path="driver.kubeconfig",
        driver_context="trpc-hpa-driver-gate2",
        admin_context="kind-trpc-runtime-gate2",
        subject=subject,
        namespace=namespace,
        cluster_fingerprint=cluster_fingerprint,
        timeout_seconds=5,
    )

    assert allowed, reasons
    assert reasons == []
    assert review_namespaces == [namespace, "default", "kube-system", "kube-system"]
    assert attestation["rule_audit"]["complete"] is True


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        (
            "write_verb",
            "HPA driver non-resource rule is not GET-only",
        ),
        (
            "unexpected_url",
            "HPA driver non-resource URL is not allowlisted",
        ),
        (
            "dangerous_wildcard",
            "HPA driver non-resource URL is not allowlisted",
        ),
        (
            "extra_rolebinding",
            "HPA driver subject has extra or missing RoleBindings",
        ),
        (
            "clusterrolebinding",
            "HPA driver subject has a ClusterRoleBinding",
        ),
        (
            "extra_target_rule",
            "HPA driver target namespace rules are broader or incomplete",
        ),
    ],
)
def test_driver_scope_rejects_broader_discovery_or_bindings(
    monkeypatch, case, expected_reason
) -> None:
    kwargs: dict[str, object] = {}
    if case == "write_verb":
        kwargs["non_resource_rules"] = [{"verbs": ["get", "post"], "nonResourceURLs": ["/api"]}]
    elif case == "unexpected_url":
        kwargs["non_resource_rules"] = [{"verbs": ["get"], "nonResourceURLs": ["/metrics"]}]
    elif case == "dangerous_wildcard":
        kwargs["non_resource_rules"] = [{"verbs": ["get"], "nonResourceURLs": ["*"]}]
    elif case == "extra_rolebinding":
        kwargs["rolebindings"] = [
            {
                "metadata": {"namespace": "runtime-gate", "name": "one"},
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": "trpc-hpa-driver",
                        "name": "trpc-hpa-driver",
                    }
                ],
                "roleRef": {"kind": "Role", "name": "trpc-hpa-load-driver"},
            },
            {
                "metadata": {"namespace": "other", "name": "two"},
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": "trpc-hpa-driver",
                        "name": "trpc-hpa-driver",
                    }
                ],
                "roleRef": {"kind": "Role", "name": "trpc-hpa-load-driver"},
            },
        ]
    elif case == "clusterrolebinding":
        kwargs["clusterrolebindings"] = [
            {
                "metadata": {"name": "unsafe"},
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "namespace": "trpc-hpa-driver",
                        "name": "trpc-hpa-driver",
                    }
                ],
                "roleRef": {"kind": "ClusterRole", "name": "view"},
            }
        ]
    else:
        kwargs["extra_target_rules"] = [
            {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": ["get"]}
        ]
    subject, namespace, review_namespaces, _ = _install_driver_scope_mocks(monkeypatch, **kwargs)
    server_identity = "v1.33.1|commit|linux/amd64"
    cluster_fingerprint = hashlib.sha256(server_identity.encode("utf-8")).hexdigest()

    allowed, reasons, _ = runtime_gate._driver_identity_and_scope(
        kubeconfig_path="driver.kubeconfig",
        driver_context="trpc-hpa-driver-gate2",
        admin_context="kind-trpc-runtime-gate2",
        subject=subject,
        namespace=namespace,
        cluster_fingerprint=cluster_fingerprint,
        timeout_seconds=5,
    )

    assert not allowed
    assert expected_reason in reasons
    assert review_namespaces == [namespace, "default", "kube-system"]
    assert "" not in review_namespaces


def test_hpa_driver_rejects_gate_kubeconfig_reuse(monkeypatch, tmp_path) -> None:
    driver = Path(runtime_gate.ROOT) / "scripts" / "kubernetes_runtime_gate.py"
    kubeconfig = tmp_path / "shared-kubeconfig"
    kubeconfig.write_text("apiVersion: v1\n", encoding="utf-8")
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    identity, reason = runtime_gate._hpa_driver_identity(
        str(driver),
        expected_sha256=hashlib.sha256(driver.read_bytes()).hexdigest(),
        kubeconfig_path=str(kubeconfig),
    )
    assert identity is None
    assert reason == "HPA driver kubeconfig must be distinct from the gate kubeconfig"


def test_controlled_node_drain_requires_dedicated_node_and_uncordons(monkeypatch) -> None:
    cordoned = False
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": cordoned},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "runtime-gate",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                }
            ]
        }

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        nonlocal cordoned
        calls.append(arguments)
        if arguments[0] == "cordon":
            cordoned = True
        elif arguments[0] == "uncordon":
            cordoned = False
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "pass"
    assert [call[0] for call in calls] == ["cordon", "drain", "uncordon"]
    drain_args = calls[1]
    assert "--delete-emptydir-data=true" in drain_args
    assert "--force=false" in drain_args
    assert details["post_drain"]["node_cordoned"] is True
    assert details["uncordon"]["status"] == "pass"


def test_controlled_node_drain_retries_transient_post_cordon_node_read(monkeypatch) -> None:
    node_reads = 0
    cordoned = False
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        nonlocal node_reads
        if arguments[:2] == ["get", "node"]:
            node_reads += 1
            if node_reads == 2:
                return CommandResult(status="fail", exit_code=1), None
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": cordoned},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "runtime-gate",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                }
            ]
        }

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        nonlocal cordoned
        calls.append(arguments)
        if arguments[0] == "cordon":
            cordoned = True
        elif arguments[0] == "uncordon":
            cordoned = False
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate.time.sleep", lambda _seconds: None)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "pass"
    assert details["post_cordon_preflight"]["node_read_attempts"] == 2
    assert details["post_cordon_preflight"]["node_schedulable"] is False
    assert [call[0] for call in calls] == ["cordon", "drain", "uncordon"]


def test_controlled_node_drain_fails_closed_after_post_cordon_read_retries(
    monkeypatch,
) -> None:
    node_reads = 0
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        nonlocal node_reads
        if arguments[:2] == ["get", "node"]:
            node_reads += 1
            if node_reads > 1:
                return CommandResult(status="fail", exit_code=1), None
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": False},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        return CommandResult(status="pass"), {"items": []}

    def fake_kubectl(arguments, **kwargs):
        del kwargs
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate.time.sleep", lambda _seconds: None)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status != "pass"
    assert details["post_cordon_preflight"]["node_read_attempts"] == 3
    assert [call[0] for call in calls] == ["cordon", "uncordon"]


def test_controlled_node_drain_requires_observed_cordon_before_drain(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": False},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "runtime-gate",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                }
            ]
        }

    def fake_kubectl(arguments, **kwargs):
        del kwargs
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status != "pass"
    assert details["post_cordon_preflight"]["node_schedulable"] is True
    assert details["post_cordon_preflight"]["node_read_attempts"] == 1
    assert [call[0] for call in calls] == ["cordon", "uncordon"]


def test_controlled_node_drain_refuses_other_workloads(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": False},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "other-workload",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                }
            ]
        }

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "not_run"
    assert details["preflight"]["blocking_pod_count"] == 1
    assert calls == []


def test_controlled_node_drain_allows_ready_pdb_protected_system_replica(
    monkeypatch,
) -> None:
    cordoned = False
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": cordoned},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        if arguments[:2] == ["get", "pdb"]:
            return CommandResult(status="pass"), {
                "items": [
                    {
                        "metadata": {"namespace": "kube-system", "generation": 3},
                        "spec": {"selector": {"matchLabels": {"k8s-app": "kube-dns"}}},
                        "status": {
                            "observedGeneration": 3,
                            "disruptionsAllowed": 1,
                            "currentHealthy": 2,
                            "desiredHealthy": 1,
                        },
                    }
                ]
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "runtime-gate",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                },
                {
                    "metadata": {
                        "namespace": "kube-system",
                        "labels": {"k8s-app": "kube-dns"},
                        "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
                    },
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                },
            ]
        }

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        nonlocal cordoned
        calls.append(arguments)
        if arguments[0] == "cordon":
            cordoned = True
        elif arguments[0] == "uncordon":
            cordoned = False
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "pass"
    assert details["blocking_pod_count"] == 0
    assert details["pdb_protected_system_pod_count"] == 1
    assert [call[0] for call in calls] == ["cordon", "drain", "uncordon"]


def test_controlled_node_drain_rejects_system_replica_without_pdb_headroom(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": False},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        if arguments[:2] == ["get", "pdb"]:
            return CommandResult(status="pass"), {
                "items": [
                    {
                        "metadata": {"namespace": "kube-system", "generation": 1},
                        "spec": {"selector": {"matchLabels": {"k8s-app": "kube-dns"}}},
                        "status": {
                            "observedGeneration": 1,
                            "disruptionsAllowed": 0,
                            "currentHealthy": 1,
                            "desiredHealthy": 1,
                        },
                    }
                ]
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "kube-system",
                        "labels": {"k8s-app": "kube-dns"},
                        "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
                    },
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                }
            ]
        }

    def fake_kubectl(arguments, **kwargs):
        del kwargs
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "not_run"
    assert details["preflight"]["blocking_pod_count"] == 1
    assert details["preflight"]["pdb_protected_system_pod_count"] == 0
    assert calls == []


def test_controlled_node_drain_rechecks_inventory_after_cordon(monkeypatch) -> None:
    cordoned = False
    inventory_reads = 0
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        nonlocal inventory_reads
        if arguments[:2] == ["get", "node"]:
            return CommandResult(status="pass"), {
                "metadata": {"labels": {"trpc-runtime-gate": "true"}},
                "spec": {"unschedulable": cordoned},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        inventory_reads += 1
        if inventory_reads == 1:
            return CommandResult(status="pass"), {"items": []}
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "namespace": "other-workload",
                        "ownerReferences": [{"kind": "Deployment"}],
                    }
                }
            ]
        }

    def fake_kubectl(arguments, *, context, timeout_seconds, input_text=None):
        del context, timeout_seconds, input_text
        nonlocal cordoned
        calls.append(arguments)
        if arguments[0] == "cordon":
            cordoned = True
        elif arguments[0] == "uncordon":
            cordoned = False
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._json_command", fake_json)
    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result, details = _controlled_node_drain(
        "node-1",
        namespace="runtime-gate",
        label_key="trpc-runtime-gate",
        label_value="true",
        context="test",
        timeout_seconds=5,
    )
    assert result.status == "not_run"
    assert details["post_cordon_preflight"]["blocking_pod_count"] == 1
    assert [call[0] for call in calls] == ["cordon", "uncordon"]


def test_evict_pod_uses_policy_api(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_kubectl(arguments, **kwargs):
        captured["arguments"] = arguments
        captured.update(kwargs)
        return CommandResult(status="pass")

    monkeypatch.setattr("scripts.kubernetes_runtime_gate._kubectl", fake_kubectl)
    result = _evict_pod(
        "worker-abc", namespace="runtime-test", context="kind-test", timeout_seconds=30
    )

    assert result.status == "pass"
    assert captured["arguments"] == [
        "create",
        "--raw",
        "/api/v1/namespaces/runtime-test/pods/worker-abc/eviction",
        "-f",
        "-",
    ]
    payload = json.loads(str(captured["input_text"]))
    assert payload["apiVersion"] == "policy/v1"
    assert payload["kind"] == "Eviction"


def test_first_worker_pod_selects_ready_non_terminating_candidate(monkeypatch) -> None:
    payload = {
        "items": [
            {
                "metadata": {
                    "name": "worker-terminating",
                    "deletionTimestamp": "2026-08-27T00:00:00Z",
                },
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            },
            {
                "metadata": {"name": "worker-pending"},
                "status": {
                    "phase": "Pending",
                    "conditions": [{"type": "Ready", "status": "False"}],
                },
            },
            {
                "metadata": {"name": "worker-ready"},
                "status": {
                    "phase": "Running",
                    "conditions": [{"type": "Ready", "status": "True"}],
                },
            },
        ]
    }
    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda *_args, **_kwargs: (CommandResult(status="pass"), payload),
    )

    result, pod_name = runtime_gate._first_worker_pod(
        namespace="runtime-test", context="kind-test", timeout_seconds=30
    )

    assert result.status == "pass"
    assert pod_name == "worker-ready"


def test_first_worker_pod_fails_closed_without_ready_candidate(monkeypatch) -> None:
    payload = {
        "items": [
            {
                "metadata": {"name": "worker-pending"},
                "status": {"phase": "Pending", "conditions": []},
            }
        ]
    }
    monkeypatch.setattr(
        runtime_gate,
        "_json_command",
        lambda *_args, **_kwargs: (CommandResult(status="pass"), payload),
    )

    result, pod_name = runtime_gate._first_worker_pod(
        namespace="runtime-test", context="kind-test", timeout_seconds=30
    )

    assert result.status == "fail"
    assert "ready non-terminating" in result.reason
    assert pod_name is None


def test_rendered_manifest_contract_requires_v2_and_resource_protection() -> None:
    root = Path("deploy/kustomize/base")
    rendered = "\n---\n".join(
        (root / name).read_text(encoding="utf-8")
        for name in (
            "config.yaml",
            "deployments.yaml",
            "disruption.yaml",
            "network-policy.yaml",
            "autoscaling.yaml",
            "migration-job.yaml",
        )
    )

    valid, reasons = _rendered_manifest_contract(rendered)
    assert valid
    assert reasons == ()

    invalid, reasons = _rendered_manifest_contract(
        rendered.replace(
            "TRPC_SERVICE_SCHEDULER_VERSION: v2", "TRPC_SERVICE_SCHEDULER_VERSION: v1", 1
        )
    )
    assert not invalid
    assert any("scheduler" in reason for reason in reasons)

    wrong_probe_role = rendered.replace(
        '["python", "-m", "trpc_service.probe", "--role", "session-recovery"]',
        '["python", "-m", "trpc_service.probe", "--role", "worker"]',
        1,
    )
    invalid, reasons = _rendered_manifest_contract(wrong_probe_role)
    assert not invalid
    assert any("exact lightweight readiness probe" in reason for reason in reasons)

    fake_drain = rendered.replace(
        '["trpc-service", "drain", "--role", "gateway"]',
        '["echo", "drain"]',
        1,
    )
    invalid, reasons = _rendered_manifest_contract(fake_drain)
    assert not invalid
    assert any("exact drain" in reason for reason in reasons)


def test_manifest_contract_uses_ten_second_timeout_for_database_exec_probes() -> None:
    deployments = [
        document
        for document in yaml.safe_load_all(
            (Path("deploy/kustomize/base") / "deployments.yaml").read_text(encoding="utf-8")
        )
        if isinstance(document, dict)
    ]
    deployment_items = {
        item["metadata"]["name"]: item for item in deployments if item.get("kind") == "Deployment"
    }
    expected_backend_probes = {
        "trpc-session-recovery": ("session-recovery", 10, 10, 6, 15, 3),
        "trpc-worker": ("worker", 10, 30, 2, 15, 3),
        "trpc-outbox-dispatcher": ("outbox-dispatcher", 10, 10, 6, 15, 3),
        "trpc-channel-dispatcher": ("channel-dispatcher", 10, 10, 6, 15, 3),
        "trpc-post-turn-projector": ("post-turn-projector", 10, 10, 6, 15, 3),
        "trpc-wecom-connector": ("wecom-connector", 10, 10, 6, 15, 3),
    }
    for deployment_name, probe_contract in expected_backend_probes.items():
        (
            role,
            readiness_timeout,
            readiness_period,
            readiness_failures,
            liveness_period,
            liveness_failures,
        ) = probe_contract
        container = deployment_items[deployment_name]["spec"]["template"]["spec"]["containers"][0]
        readiness = container["readinessProbe"]
        liveness = container["livenessProbe"]
        assert readiness["exec"]["command"] == [
            "python",
            "-m",
            "trpc_service.probe",
            "--role",
            role,
        ]
        assert liveness["exec"]["command"] == [
            "python",
            "-m",
            "trpc_service.probe",
            "--role",
            role,
            "--liveness",
        ]
        assert readiness["timeoutSeconds"] >= readiness_timeout
        assert liveness["timeoutSeconds"] >= readiness_timeout
        assert readiness["periodSeconds"] == readiness_period
        assert readiness["failureThreshold"] == readiness_failures
        assert liveness["periodSeconds"] == liveness_period
        assert liveness["failureThreshold"] == liveness_failures

    session_container = deployment_items["trpc-session-recovery"]["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert session_container["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "1000m", "memory": "1Gi"},
    }

    for deployment_name in ("trpc-gateway", "trpc-admin"):
        container = deployment_items[deployment_name]["spec"]["template"]["spec"]["containers"][0]
        assert container["readinessProbe"]["httpGet"]
        assert container["livenessProbe"]["httpGet"]
        assert container["readinessProbe"]["timeoutSeconds"] == 3
        assert container["livenessProbe"]["timeoutSeconds"] == 3


def test_migration_job_read_only_root_has_writable_tmp_mount() -> None:
    root = Path("deploy/kustomize/base")
    migration = yaml.safe_load((root / "migration-job.yaml").read_text(encoding="utf-8"))
    pod_spec = migration["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]

    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert {"name": "tmp", "mountPath": "/tmp"} in container["volumeMounts"]  # noqa: S108
    assert {"name": "tmp", "emptyDir": {}} in pod_spec["volumes"]


def test_migration_manifest_is_applied_before_schema_dependent_workloads() -> None:
    rendered = yaml.safe_dump_all(
        [
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "runtime-gate"},
            },
            {
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {"name": "trpc-service"},
            },
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "trpc-service-config"},
            },
            {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "trpc-schema-migration"},
            },
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "trpc-worker"},
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": "trpc-gateway"},
            },
        ],
        sort_keys=False,
    )

    migration_manifest, runtime_manifest = _split_migration_manifests(rendered)
    migration_documents = list(yaml.safe_load_all(migration_manifest))
    runtime_documents = list(yaml.safe_load_all(runtime_manifest))

    assert [document["kind"] for document in migration_documents] == [
        "ServiceAccount",
        "ConfigMap",
        "Job",
    ]
    assert all(document["kind"] != "Deployment" for document in migration_documents)
    assert all(document["kind"] != "Job" for document in runtime_documents)
    assert all(document["kind"] != "Namespace" for document in runtime_documents)
    assert [document["kind"] for document in runtime_documents] == ["Deployment", "Service"]


def _migration_manifest_for_head_check() -> str:
    return yaml.safe_dump(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "trpc-schema-migration"},
            "spec": {
                "backoffLimit": 3,
                "template": {
                    "metadata": {"labels": {"source": "migration"}},
                    "spec": {
                        "restartPolicy": "Never",
                        "serviceAccountName": "trpc-service",
                        "securityContext": {"runAsNonRoot": True},
                        "volumes": [{"name": "tmp", "emptyDir": {}}],
                        "containers": [
                            {
                                "name": "migrate",
                                "image": "example/trpc@sha256:" + "a" * 64,
                                "command": ["trpc-service"],
                                "args": ["migrate", "--revision", "head"],
                                "env": [{"name": "TRPC_ENV", "value": "test"}],
                                "envFrom": [{"secretRef": {"name": "trpc-migration-secrets"}}],
                                "volumeMounts": [
                                    {"name": "tmp", "mountPath": "/tmp"}  # noqa: S108
                                ],
                                "securityContext": {"readOnlyRootFilesystem": True},
                            }
                        ],
                    },
                },
            },
        },
        sort_keys=False,
    )


def test_schema_head_check_manifest_reuses_migration_pod_template() -> None:
    migration_manifest = _migration_manifest_for_head_check()

    result = _schema_head_check_manifest(migration_manifest, namespace="runtime-gate")
    source_job = yaml.safe_load(migration_manifest)
    source_pod = source_job["spec"]["template"]["spec"]
    source_container = source_pod["containers"][0]
    head_spec = result["spec"]
    head_pod = head_spec["template"]["spec"]
    head_container = head_pod["containers"][0]

    assert result["metadata"]["name"] == "trpc-schema-head-check"
    assert result["metadata"]["namespace"] == "runtime-gate"
    assert head_spec["backoffLimit"] == 0
    assert head_spec["completions"] == 1
    assert head_spec["parallelism"] == 1
    assert head_pod["serviceAccountName"] == source_pod["serviceAccountName"]
    assert head_pod["securityContext"] == source_pod["securityContext"]
    assert head_pod["volumes"] == source_pod["volumes"]
    assert head_container["image"] == source_container["image"]
    assert head_container["env"] == source_container["env"]
    assert head_container["envFrom"] == source_container["envFrom"]
    assert head_container["volumeMounts"] == source_container["volumeMounts"]
    assert head_container["securityContext"] == source_container["securityContext"]
    assert head_container["command"] == ["trpc-service"]
    assert head_container["args"] == ["migrate", "--check"]


def test_migration_head_check_runs_fresh_job_and_records_uids(monkeypatch) -> None:
    migration_manifest = _migration_manifest_for_head_check()
    calls: list[tuple[list[str], str | None]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[1:3] == ["job", "trpc-schema-migration"]:
            return CommandResult(status="pass"), {"metadata": {"uid": "migration-job-uid"}}
        if arguments[1:3] == ["job", "trpc-schema-head-check"]:
            return CommandResult(status="pass"), {
                "metadata": {"uid": "head-job-uid"},
                "status": {"succeeded": 1},
            }
        return CommandResult(status="pass"), {
            "items": [
                {
                    "metadata": {
                        "name": "head-check-failed",
                        "uid": "head-pod-failed-uid",
                        "ownerReferences": [
                            {
                                "kind": "Job",
                                "name": "trpc-schema-head-check",
                                "uid": "head-job-uid",
                            }
                        ],
                    },
                    "status": {"phase": "Failed"},
                },
                {
                    "metadata": {
                        "name": "head-check-success",
                        "uid": "head-pod-uid",
                        "ownerReferences": [
                            {
                                "kind": "Job",
                                "name": "trpc-schema-head-check",
                                "uid": "head-job-uid",
                            }
                        ],
                    },
                    "status": {
                        "phase": "Succeeded",
                        "containerStatuses": [
                            {
                                "name": "migrate",
                                "state": {"terminated": {"exitCode": 0, "reason": "Completed"}},
                            }
                        ],
                    },
                },
            ]
        }

    def fake_kubectl(arguments, *, input_text=None, **kwargs):
        del kwargs
        calls.append((arguments, input_text))
        return CommandResult(status="pass", exit_code=0)

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)

    result = _migration_head_check(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=5,
        migration_manifest=migration_manifest,
    )

    assert result.status == "pass"
    assert result.evidence["migration_job_uid"] == "migration-job-uid"
    assert result.evidence["head_check_job_uid"] == "head-job-uid"
    assert result.evidence["head_check_pod_uid"] == "head-pod-uid"
    assert result.evidence["container_exit_code"] == 0
    assert result.evidence["failed_pod_names"] == ["head-check-failed"]
    assert len(calls) == 2
    assert calls[0][0] == ["apply", "--server-side", "-f", "-"]
    applied = json.loads(calls[0][1])
    container = applied["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "example/trpc@sha256:" + "a" * 64
    assert container["command"] == ["trpc-service"]
    assert container["args"] == ["migrate", "--check"]
    assert calls[1][0] == [
        "wait",
        "--for=condition=complete",
        "job/trpc-schema-head-check",
        "--namespace",
        "runtime-gate",
        "--timeout=5s",
    ]
    assert all("exec" not in arguments for arguments, _ in calls)


@pytest.mark.parametrize(
    ("items", "reason"),
    [
        (
            [
                {
                    "metadata": {
                        "name": "head-check-failed",
                        "ownerReferences": [
                            {
                                "kind": "Job",
                                "name": "trpc-schema-head-check",
                                "uid": "head-job-uid",
                            }
                        ],
                    },
                    "status": {"phase": "Failed"},
                }
            ],
            "no successful Pod",
        ),
        (
            [
                {
                    "metadata": {
                        "name": "head-check-success-1",
                        "ownerReferences": [
                            {
                                "kind": "Job",
                                "name": "trpc-schema-head-check",
                                "uid": "head-job-uid",
                            }
                        ],
                    },
                    "status": {"phase": "Succeeded"},
                },
                {
                    "metadata": {
                        "name": "head-check-success-2",
                        "ownerReferences": [
                            {
                                "kind": "Job",
                                "name": "trpc-schema-head-check",
                                "uid": "head-job-uid",
                            }
                        ],
                    },
                    "status": {"phase": "Succeeded"},
                },
            ],
            "multiple successful Pods",
        ),
    ],
)
def test_migration_head_check_fails_closed_without_unique_success(
    monkeypatch, items, reason
) -> None:
    migration_manifest = _migration_manifest_for_head_check()
    calls: list[list[str]] = []

    def fake_json(arguments, *, context, timeout_seconds):
        del context, timeout_seconds
        if arguments[1:3] == ["job", "trpc-schema-migration"]:
            return CommandResult(status="pass"), {"metadata": {"uid": "migration-job-uid"}}
        if arguments[1:3] == ["job", "trpc-schema-head-check"]:
            return CommandResult(status="pass"), {
                "metadata": {"uid": "head-job-uid"},
                "status": {"succeeded": 1},
            }
        return CommandResult(status="pass"), {"items": items}

    def fake_kubectl(arguments, **kwargs):
        del kwargs
        calls.append(arguments)
        return CommandResult(status="pass")

    monkeypatch.setattr(runtime_gate, "_json_command", fake_json)
    monkeypatch.setattr(runtime_gate, "_kubectl", fake_kubectl)

    result = _migration_head_check(
        namespace="runtime-gate",
        context="kind-test",
        timeout_seconds=5,
        migration_manifest=migration_manifest,
    )

    assert result.status == "fail"
    assert reason in result.reason
    assert calls == [
        ["apply", "--server-side", "-f", "-"],
        [
            "wait",
            "--for=condition=complete",
            "job/trpc-schema-head-check",
            "--namespace",
            "runtime-gate",
            "--timeout=5s",
        ],
    ]


def test_scheduler_runtime_contract_rejects_mixed_versions() -> None:
    configmap = {
        "data": {
            "TRPC_SERVICE_SCHEDULER_VERSION": "v2",
            "TRPC_SERVICE_REDIS_STREAM": "trpc:session-ready:v2",
            "TRPC_SERVICE_REDIS_CONSUMER_GROUP": "trpc-session-ready-v2",
        }
    }
    deployments = {
        "items": [
            {
                "metadata": {"name": "trpc-worker", "labels": {}},
                "spec": {"template": {"metadata": {"labels": {"scheduler-version": "v1"}}}},
            }
        ]
    }
    valid, reasons = _scheduler_runtime_contract(configmap, deployments)
    assert not valid
    assert any("mixed" in reason or "v1" in reason for reason in reasons)
