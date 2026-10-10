from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import scripts.ack_performance_acceptance as acceptance

VALUES = {
    "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "fixture-app-secret",
    "TRPC_PERF_FIXTURE_UNUSED_VERIFICATION_TOKEN": "fixture-verification-token",
    "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "fixture-encrypt-key",
}


def _deployment(*, annotations: dict[str, str] | None = None, ready: bool = True) -> dict[str, Any]:
    replicas = 4
    status = {
        "observedGeneration": 2,
        "updatedReplicas": replicas if ready else replicas - 1,
        "readyReplicas": replicas if ready else replicas - 1,
        "availableReplicas": replicas if ready else replicas - 1,
    }
    return {
        "metadata": {"generation": 2},
        "spec": {
            "replicas": replicas,
            "template": {"metadata": {"annotations": annotations or {}}},
        },
        "status": status,
    }


def _secret(*, version: str = "7", values: dict[str, str] | None = None) -> dict[str, Any]:
    selected = values or VALUES
    import base64

    return {
        "metadata": {"resourceVersion": version},
        "data": {
            name: base64.b64encode(value.encode("utf-8")).decode("ascii")
            for name, value in selected.items()
        },
    }


def _binding(
    *, version: str = "7", values: dict[str, str] | None = None
) -> acceptance.GatewayFixtureSecretBinding:
    selected = values or VALUES
    return acceptance.GatewayFixtureSecretBinding(
        values=selected,
        secret_name="trpc-service-secrets",
        checksum=acceptance._fixture_secret_checksum(selected),
        resource_version=version,
    )


def _endpoint_slices(
    *, ready: int = 4, terminating: int = 0, address_prefix: str = "10.0.0"
) -> dict[str, Any]:
    endpoints = []
    for index in range(4):
        endpoints.append(
            {
                "addresses": [f"{address_prefix}.{index + 1}"],
                "conditions": {
                    "ready": index < ready,
                    "terminating": index < terminating,
                },
            }
        )
    return {"items": [{"metadata": {"name": "gateway-slice"}, "endpoints": endpoints}]}


def _endpoint_stability_evidence() -> dict[str, object]:
    return {
        "status": "pass",
        "service": "trpc-gateway",
        "expected_replicas": 4,
        "ready_endpoints": 4,
        "unready_endpoints": 0,
        "terminating_endpoints": 0,
        "total_endpoints": 4,
        "endpoint_set_sha256": "endpoint-set",
        "observations": 2,
        "stable_observations": 2,
        "required_stable_observations": 2,
        "interval_seconds": acceptance.GATEWAY_ENDPOINT_STABILITY_INTERVAL_SECONDS,
    }


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_fixture_binding_checksum_is_deterministic_and_public_evidence_has_no_values() -> None:
    binding = _binding()
    reversed_values = dict(reversed(tuple(VALUES.items())))

    assert binding.checksum == acceptance._fixture_secret_checksum(reversed_values)
    public = json.dumps(binding.public_evidence, sort_keys=True)
    assert binding.checksum in public
    assert binding.resource_version in public
    for value in VALUES.values():
        assert value not in public
    assert repr(binding).find("fixture-app-secret") == -1


def test_gateway_fixture_secret_binding_requires_secret_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses: Iterator[dict[str, Any]] = iter(
        (
            {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {"envFrom": [{"secretRef": {"name": "trpc-service-secrets"}}]}
                            ]
                        }
                    }
                }
            },
            {"data": {}},
        )
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match="fixture secrets are unavailable"):
        acceptance._gateway_fixture_secret_binding(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            deployment_name="trpc-gateway",
        )


def test_first_run_patches_template_and_confirms_rollout(monkeypatch: pytest.MonkeyPatch) -> None:
    binding = _binding()
    desired_annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: binding.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: binding.resource_version,
    }
    responses: Iterator[dict[str, Any]] = iter(
        (_deployment(), _deployment(annotations=desired_annotations), _secret())
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))
    patches: list[acceptance.GatewayFixtureSecretBinding] = []
    waits: list[str] = []
    monkeypatch.setattr(
        acceptance,
        "_patch_gateway_fixture_binding",
        lambda **kwargs: patches.append(kwargs["binding"]),
    )
    monkeypatch.setattr(
        acceptance,
        "_wait_for_gateway_fixture_rollout",
        lambda **kwargs: waits.append(kwargs["deployment_name"]),
    )
    monkeypatch.setattr(
        acceptance,
        "_wait_for_gateway_endpoint_stability",
        lambda **_kwargs: _endpoint_stability_evidence(),
    )

    result = acceptance._ensure_gateway_fixture_rollout(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        deployment_name="trpc-gateway",
        binding=binding,
        timeout_seconds=30.0,
    )

    assert result["status"] == "pass"
    assert result["template_updated"] is True
    assert result["rollout_confirmed"] is True
    assert result["endpoint_stability"] == _endpoint_stability_evidence()
    assert patches == [binding]
    assert waits == ["trpc-gateway"]


def test_binding_patch_is_targeted_and_never_a_blind_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    calls: list[list[str]] = []
    monkeypatch.setattr(acceptance.shutil, "which", lambda _name: "kubectl")  # type: ignore[attr-defined]

    def fake_run(command: list[str], **_kwargs: object) -> Any:
        calls.append(command)
        return type("Completed", (), {"returncode": 0})()

    monkeypatch.setattr(acceptance.subprocess, "run", fake_run)  # type: ignore[attr-defined]

    acceptance._patch_gateway_fixture_binding(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        deployment_name="trpc-gateway",
        binding=binding,
        timeout_seconds=30.0,
    )

    command = calls[0]
    assert "patch" in command
    assert "restart" not in command
    patch_payload = json.loads(command[command.index("--patch") + 1])
    annotations = patch_payload["spec"]["template"]["metadata"]["annotations"]
    assert annotations[acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION] == binding.checksum
    assert (
        annotations[acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION]
        == binding.resource_version
    )
    rendered = json.dumps(command)
    for value in VALUES.values():
        assert value not in rendered


def test_same_image_secret_update_changes_template_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    first = _binding(version="7")
    second_values = {**VALUES, "TRPC_PERF_FIXTURE_UNUSED_ENCRYPT_KEY": "rotated-encrypt-key"}
    second = _binding(version="8", values=second_values)
    first_annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: first.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: first.resource_version,
    }
    second_annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: second.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: second.resource_version,
    }
    responses: Iterator[dict[str, Any]] = iter(
        (
            _deployment(),
            _deployment(annotations=first_annotations),
            _secret(version="7"),
            _deployment(annotations=first_annotations),
            _deployment(annotations=second_annotations),
            _secret(version="8", values=second_values),
        )
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))
    patches: list[acceptance.GatewayFixtureSecretBinding] = []
    monkeypatch.setattr(
        acceptance,
        "_patch_gateway_fixture_binding",
        lambda **kwargs: patches.append(kwargs["binding"]),
    )
    monkeypatch.setattr(acceptance, "_wait_for_gateway_fixture_rollout", lambda **_kwargs: None)
    monkeypatch.setattr(
        acceptance,
        "_wait_for_gateway_endpoint_stability",
        lambda **_kwargs: _endpoint_stability_evidence(),
    )

    first_result = acceptance._ensure_gateway_fixture_rollout(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        deployment_name="trpc-gateway",
        binding=first,
        timeout_seconds=30.0,
    )
    second_result = acceptance._ensure_gateway_fixture_rollout(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        deployment_name="trpc-gateway",
        binding=second,
        timeout_seconds=30.0,
    )

    assert first_result["template_updated"] is True
    assert second_result["template_updated"] is True
    assert patches == [first, second]
    assert first.checksum != second.checksum


def test_matching_binding_does_not_trigger_patch_but_still_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: binding.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: binding.resource_version,
    }
    responses: Iterator[dict[str, Any]] = iter(
        (
            _deployment(annotations=annotations),
            _deployment(annotations=annotations),
            _secret(),
        )
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))
    monkeypatch.setattr(
        acceptance,
        "_patch_gateway_fixture_binding",
        lambda **_kwargs: pytest.fail("matching Secret binding must not be patched"),
    )
    waits: list[bool] = []
    monkeypatch.setattr(
        acceptance,
        "_wait_for_gateway_fixture_rollout",
        lambda **_kwargs: waits.append(True),
    )
    monkeypatch.setattr(
        acceptance,
        "_wait_for_gateway_endpoint_stability",
        lambda **_kwargs: _endpoint_stability_evidence(),
    )

    result = acceptance._ensure_gateway_fixture_rollout(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        deployment_name="trpc-gateway",
        binding=binding,
        timeout_seconds=30.0,
    )

    assert result["template_updated"] is False
    assert waits == [True]


def test_unconverged_gateway_rollout_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    binding = _binding()
    annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: binding.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: binding.resource_version,
    }
    responses: Iterator[dict[str, Any]] = iter(
        (_deployment(), _deployment(annotations=annotations, ready=False))
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))
    monkeypatch.setattr(acceptance, "_patch_gateway_fixture_binding", lambda **_kwargs: None)
    monkeypatch.setattr(acceptance, "_wait_for_gateway_fixture_rollout", lambda **_kwargs: None)

    with pytest.raises(RuntimeError, match="rollout has not converged"):
        acceptance._ensure_gateway_fixture_rollout(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            deployment_name="trpc-gateway",
            binding=binding,
            timeout_seconds=30.0,
        )


def test_secret_rotation_during_rollout_fails_closed_without_leaking_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding(version="7")
    annotations = {
        acceptance.GATEWAY_FIXTURE_SECRET_CHECKSUM_ANNOTATION: binding.checksum,
        acceptance.GATEWAY_FIXTURE_SECRET_VERSION_ANNOTATION: binding.resource_version,
    }
    rotated_values = {**VALUES, "TRPC_PERF_FIXTURE_UNUSED_APP_SECRET": "rotated-app-secret"}
    responses: Iterator[dict[str, Any]] = iter(
        (
            _deployment(annotations=annotations),
            _deployment(annotations=annotations),
            _secret(version="8", values=rotated_values),
        )
    )
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))
    monkeypatch.setattr(acceptance, "_wait_for_gateway_fixture_rollout", lambda **_kwargs: None)

    with pytest.raises(RuntimeError, match="changed during rollout") as error:
        acceptance._ensure_gateway_fixture_rollout(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            deployment_name="trpc-gateway",
            binding=binding,
            timeout_seconds=30.0,
        )
    assert "rotated-app-secret" not in str(error.value)


def test_gateway_endpoint_stability_requires_two_ready_observations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    responses: Iterator[dict[str, Any]] = iter((_endpoint_slices(), _endpoint_slices()))
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    evidence = acceptance._wait_for_gateway_endpoint_stability(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        service_name="trpc-gateway",
        expected_replicas=4,
        timeout_seconds=5.0,
    )

    assert evidence["status"] == "pass"
    assert evidence["ready_endpoints"] == 4
    assert evidence["unready_endpoints"] == 0
    assert evidence["terminating_endpoints"] == 0
    assert evidence["observations"] == 2
    assert evidence["stable_observations"] == 2
    assert evidence["required_stable_observations"] == 2


def test_gateway_endpoint_stability_rejects_unready_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    responses: Iterator[dict[str, Any]] = iter([_endpoint_slices(ready=3)] * 3)
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match=r"ready endpoints 3/4.*unready=1"):
        acceptance._wait_for_gateway_endpoint_stability(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            service_name="trpc-gateway",
            expected_replicas=4,
            timeout_seconds=5.0,
        )


def test_gateway_endpoint_stability_rejects_terminating_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    responses: Iterator[dict[str, Any]] = iter([_endpoint_slices(terminating=1)] * 3)
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match=r"terminating=1"):
        acceptance._wait_for_gateway_endpoint_stability(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            service_name="trpc-gateway",
            expected_replicas=4,
            timeout_seconds=5.0,
        )


def test_gateway_endpoint_stability_resets_when_endpoint_set_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    responses: Iterator[dict[str, Any]] = iter(
        (
            _endpoint_slices(address_prefix="10.0.0"),
            _endpoint_slices(address_prefix="10.0.1"),
            _endpoint_slices(address_prefix="10.0.1"),
        )
    )
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    evidence = acceptance._wait_for_gateway_endpoint_stability(
        namespace="trpc-service",
        context="ack",
        kubeconfig=Path("config"),
        service_name="trpc-gateway",
        expected_replicas=4,
        timeout_seconds=7.0,
    )

    assert evidence["observations"] == 3
    assert evidence["stable_observations"] == 2


def test_gateway_endpoint_stability_timeout_reports_last_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    responses: Iterator[dict[str, Any]] = iter(
        (
            _endpoint_slices(address_prefix="10.0.0"),
            _endpoint_slices(address_prefix="10.0.1"),
        )
    )
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    monkeypatch.setattr(acceptance, "_kubectl_json", lambda **_kwargs: next(responses))

    with pytest.raises(RuntimeError, match=r"timed out.*stable observations 1/2"):
        acceptance._wait_for_gateway_endpoint_stability(
            namespace="trpc-service",
            context="ack",
            kubeconfig=Path("config"),
            service_name="trpc-gateway",
            expected_replicas=4,
            timeout_seconds=3.0,
        )


def test_failure_report_omits_unavailable_performance_samples() -> None:
    report = acceptance._failure_report(
        "job observation failed",
        image_digest="sha256:" + "a" * 64,
        source_fingerprint="b" * 64,
    )

    candidate = report["candidate"]
    assert isinstance(candidate, dict)
    assert "burst" not in candidate
    assert "sustained" not in candidate
    assert report["gate"] == "fail"
    assert report["production_gate"] == "fail"
