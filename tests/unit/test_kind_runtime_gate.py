from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.kind_runtime_gate import (
    METRICS_SERVER_MANIFEST_URL,
    METRICS_SERVER_VERSION,
    _hpa_metrics_contract,
    _image_source_contract,
    _install_kind_metrics_server,
    _kind_context_contract,
    _kind_node_safety_contract,
    _metrics_server_patch_operations,
    _prerequisite_manifest,
)


def test_documented_direct_invocation_resolves_scripts_package() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/kind_runtime_gate.py", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "usage: kind_runtime_gate.py" in result.stdout


def test_kind_prerequisites_are_isolated_and_complete() -> None:
    manifest = _prerequisite_manifest()
    items = manifest["items"]
    resources = {(item["kind"], item["metadata"]["name"]): item for item in items}

    runtime = resources[("Secret", "trpc-service-secrets")]["stringData"]
    migration = resources[("Secret", "trpc-migration-secrets")]["stringData"]
    worker = resources[("Secret", "trpc-worker-secrets")]["stringData"]
    assert "trpc_runtime" in runtime["TRPC_SERVICE_DATABASE_DSN"]
    assert "trpc_migration" in migration["TRPC_SERVICE_DATABASE_DSN"]
    assert "trpc_worker" in worker["TRPC_SERVICE_WORKER_DATABASE_DSN"]
    assert worker["TRPC_SERVICE_WORKER_DATABASE_DSN_REF"] == (
        "env://TRPC_SERVICE_WORKER_DATABASE_DSN"
    )
    assert worker["TRPC_SERVICE_WORKER_DATABASE_PASSWORD_REF"] == (
        "env://TRPC_SERVICE_WORKER_DATABASE_PASSWORD"
    )
    assert len(runtime["TRPC_SERVICE_SESSION_HMAC_KEY"]) >= 32
    assert len(runtime["TRPC_SERVICE_EMERGENCY_QUEUE_KEY"]) == 32
    init_sql = resources[("ConfigMap", "trpc-gate-postgres-init")]["data"]["001-runtime-role.sql"]
    assert "CREATE ROLE trpc_worker LOGIN NOSUPERUSER" in init_sql
    assert "NOINHERIT BYPASSRLS" in init_sql
    assert ("Deployment", "postgres") in resources
    assert ("Deployment", "redis") in resources
    assert ("NetworkPolicy", "trpc-gate-allow-dependencies") in resources

    expected_selector = {"node-role.kubernetes.io/control-plane": ""}
    expected_toleration = {
        "key": "node-role.kubernetes.io/control-plane",
        "operator": "Exists",
        "effect": "NoSchedule",
    }
    for name in ("postgres", "redis"):
        pod_spec = resources[("Deployment", name)]["spec"]["template"]["spec"]
        assert pod_spec["nodeSelector"] == expected_selector
        assert pod_spec["tolerations"] == [expected_toleration]
        for container in pod_spec["containers"]:
            security_context = container.get("securityContext", {})
            assert security_context.get("privileged", False) is False
            resources_block = container["resources"]
            assert resources_block["requests"]["cpu"]
            assert resources_block["requests"]["memory"]
            assert resources_block["limits"]["cpu"]
            assert resources_block["limits"]["memory"]


def test_kind_node_safety_contract_matches_context_nodes_and_limits(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    nodes = {
        "items": [
            {"metadata": {"name": "gate-control-plane"}},
            {"metadata": {"name": "gate-worker"}},
            {"metadata": {"name": "gate-worker2"}},
        ]
    }
    containers = [
        {
            "Name": "/gate-control-plane",
            "Config": {
                "Hostname": "gate-control-plane",
                "Labels": {"io.x-k8s.kind.cluster": "gate", "io.x-k8s.kind.role": "control-plane"},
            },
            "State": {"Running": True},
            "HostConfig": {
                "RestartPolicy": {"Name": "no"},
                "NanoCpus": 3_000_000_000,
                "Memory": 6 * 1024 * 1024 * 1024,
                "MemorySwap": 6 * 1024 * 1024 * 1024,
                "PidsLimit": 1024,
            },
        },
        {
            "Name": "/gate-worker",
            "Config": {
                "Hostname": "gate-worker",
                "Labels": {"io.x-k8s.kind.cluster": "gate", "io.x-k8s.kind.role": "worker"},
            },
            "State": {"Running": True},
            "HostConfig": {
                "RestartPolicy": {"Name": "no"},
                "NanoCpus": 3_000_000_000,
                "Memory": 6 * 1024 * 1024 * 1024,
                "MemorySwap": 6 * 1024 * 1024 * 1024,
                "PidsLimit": 1024,
            },
        },
        {
            "Name": "/gate-worker2",
            "Config": {
                "Hostname": "gate-worker2",
                "Labels": {"io.x-k8s.kind.cluster": "gate", "io.x-k8s.kind.role": "worker"},
            },
            "State": {"Running": True},
            "HostConfig": {
                "RestartPolicy": {"Name": "no"},
                "NanoCpus": 3_000_000_000,
                "Memory": 6 * 1024 * 1024 * 1024,
                "MemorySwap": 6 * 1024 * 1024 * 1024,
                "PidsLimit": 1024,
            },
        },
    ]

    def fake_kubectl(context, arguments, *, timeout_seconds):
        del timeout_seconds
        calls.append((context, arguments))
        return 0, json.dumps(nodes)

    def fake_docker(arguments, *, timeout_seconds):
        del timeout_seconds
        if arguments[:2] == ["ps", "-aq"]:
            assert "label=io.x-k8s.kind.cluster=gate" in arguments
            assert "label=io.x-k8s.kind.role=node" not in arguments
            return 0, "node-a\nnode-b\nnode-c\n"
        assert arguments[:1] == ["inspect"]
        return 0, json.dumps(containers)

    monkeypatch.setattr("scripts.kind_runtime_gate._run_kubectl", fake_kubectl)
    monkeypatch.setattr("scripts.kind_runtime_gate._run_docker", fake_docker)
    valid, evidence, reason = _kind_node_safety_contract(context="kind-gate", timeout_seconds=10)

    assert valid, reason
    assert evidence["status"] == "pass"
    assert evidence["kubernetes_nodes"] == [
        "gate-control-plane",
        "gate-worker",
        "gate-worker2",
    ]
    assert evidence["role_counts"] == {"control-plane": 1, "worker": 2}
    assert calls == [("kind-gate", ["get", "nodes", "-o", "json"])]


def test_kind_node_safety_contract_rejects_unbounded_or_mismatched_nodes(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.kind_runtime_gate._run_kubectl",
        lambda *_args, **_kwargs: (
            0,
            json.dumps({"items": [{"metadata": {"name": "gate-control-plane"}}]}),
        ),
    )
    monkeypatch.setattr(
        "scripts.kind_runtime_gate._run_docker",
        lambda arguments, **_kwargs: (
            (0, "node-a\n")
            if arguments[:2] == ["ps", "-aq"]
            else (
                0,
                json.dumps(
                    [
                        {
                            "Name": "/gate-control-plane",
                            "Config": {
                                "Hostname": "gate-control-plane",
                                "Labels": {
                                    "io.x-k8s.kind.cluster": "gate",
                                    "io.x-k8s.kind.role": "control-plane",
                                },
                            },
                            "State": {"Running": True},
                            "HostConfig": {
                                "RestartPolicy": {"Name": "always"},
                                "NanoCpus": 0,
                                "Memory": 0,
                                "MemorySwap": 0,
                                "PidsLimit": 0,
                            },
                        }
                    ]
                ),
            )
        ),
    )

    valid, evidence, reason = _kind_node_safety_contract(context="kind-gate", timeout_seconds=10)

    assert not valid
    assert evidence["status"] == "fail"
    assert "restart policy is not no" in (reason or "")
    assert "below the safe CPU limit" in (reason or "")
    assert "below the safe memory limit" in (reason or "")
    assert "below the safe PIDs limit" in (reason or "")
    assert "at least two worker nodes" in (reason or "")


def test_image_source_contract_requires_current_checkout_fingerprint() -> None:
    current = "a" * 64
    initial = {
        "Id": "sha256:initial",
        "Config": {
            "Labels": {
                "io.trpc.agent-service.source-fingerprint": current,
            }
        },
    }
    upgrade = {
        "Id": "sha256:upgrade",
        "Config": {
            "Labels": {
                "io.trpc.agent-service.source-fingerprint": current,
            }
        },
    }

    assert _image_source_contract(
        initial,
        upgrade,
        image="trpc-agent-service:a",
        upgrade_image="trpc-agent-service:b",
        current_source_fingerprint=current,
    ) == (True, ())

    valid, reasons = _image_source_contract(
        {**initial, "Config": {"Labels": {}}},
        upgrade,
        image="trpc-agent-service:a",
        upgrade_image="trpc-agent-service:b",
        current_source_fingerprint=current,
    )
    assert not valid
    assert any("source fingerprint" in reason for reason in reasons)


def test_hpa_metrics_contract_rejects_missing_metrics_server_evidence() -> None:
    hpa = {
        "status": "pass",
        "conditions": {
            "AbleToScale": "True",
            "ScalingActive": "False",
        },
        "current_replicas": 2,
        "desired_replicas": 2,
    }
    valid, reasons = _hpa_metrics_contract(hpa, metrics_api_available=True)
    assert not valid
    assert any("ScalingActive" in reason for reason in reasons)

    valid, reasons = _hpa_metrics_contract(hpa, metrics_api_available=False)
    assert not valid
    assert any("metrics-server" in reason for reason in reasons)


def test_kind_metrics_server_install_is_pinned_and_context_scoped() -> None:
    assert METRICS_SERVER_VERSION == "v0.9.0"
    assert METRICS_SERVER_MANIFEST_URL.endswith("/v0.9.0/components.yaml")
    assert _kind_context_contract("kind-trpc-runtime-gate")
    assert not _kind_context_contract("production-context")
    assert not _kind_context_contract(None)


def test_metrics_server_patch_is_idempotent_and_preserves_existing_args() -> None:
    deployment = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {"name": "other", "args": ["--other"]},
                        {"name": "metrics-server", "args": ["--secure-port=10250"]},
                    ]
                }
            }
        }
    }
    patch = _metrics_server_patch_operations(deployment)
    assert patch == (
        {
            "op": "add",
            "path": "/spec/template/spec/containers/1/args/-",
            "value": "--kubelet-insecure-tls",
        },
    )
    deployment["spec"]["template"]["spec"]["containers"][1]["args"].append("--kubelet-insecure-tls")
    assert _metrics_server_patch_operations(deployment) == ()


def test_metrics_server_install_refuses_non_kind_context(monkeypatch) -> None:
    called = False

    def fail_if_called(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("kubectl must not run for a non-kind context")

    monkeypatch.setattr("scripts.kind_runtime_gate._run_kubectl", fail_if_called)
    valid, evidence, reason = _install_kind_metrics_server(
        context="production-context", timeout_seconds=10
    )
    assert not valid
    assert evidence["scope"] == "kind-only"
    assert "kind-*" in (reason or "")
    assert called is False


def test_metrics_server_install_applies_patch_and_waits_for_rollout(monkeypatch) -> None:
    calls: list[list[str]] = []
    deployment = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "metrics-server", "args": ["--secure-port=10250"]}]
                }
            }
        }
    }

    def fake_run(_context, arguments, *, timeout_seconds):
        del timeout_seconds
        calls.append(arguments)
        if arguments[:2] == ["get", "deployment"]:
            import json

            return 0, json.dumps(deployment)
        return 0, ""

    monkeypatch.setattr("scripts.kind_runtime_gate._run_kubectl", fake_run)
    valid, evidence, reason = _install_kind_metrics_server(
        context="kind-trpc-runtime-gate", timeout_seconds=10
    )
    assert valid, reason
    assert evidence["status"] == "pass"
    assert any(arguments[:1] == ["apply"] for arguments in calls)
    assert any(arguments[:1] == ["patch"] for arguments in calls)
    assert any(arguments[:1] == ["rollout"] for arguments in calls)
