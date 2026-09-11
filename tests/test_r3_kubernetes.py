"""R3 production topology contracts: manifests stay intentionally minimal."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "deploy" / "k8s"


def _documents(name: str) -> list[dict]:
    return [doc for doc in yaml.safe_load_all((K8S / name).read_text(encoding="utf-8")) if doc]


def _all_documents() -> list[dict]:
    docs: list[dict] = []
    for resource in yaml.safe_load((K8S / "kustomization.yaml").read_text(encoding="utf-8"))["resources"]:
        docs.extend(_documents(resource))
    return docs


def _container(document: dict) -> dict:
    spec = document["spec"]["template"]["spec"]
    return spec["containers"][0]


def test_kustomization_declares_the_complete_small_topology():
    config = yaml.safe_load((K8S / "kustomization.yaml").read_text(encoding="utf-8"))
    assert config["kind"] == "Kustomization"
    assert set(config["resources"]) == {
        "namespace.yaml",
        "configmap.yaml",
        "secret.example.yaml",
        "postgres.yaml",
        "redis.yaml",
        "minio.yaml",
        "otel-collector.yaml",
        "init-job.yaml",
        "worker.yaml",
        "gateway.yaml",
        "admin.yaml",
        "worker-hpa.yaml",
    }


def test_only_init_job_runs_migration_and_backend_initialization():
    docs = _all_documents()
    commands = []
    for doc in docs:
        for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
            commands.append(" ".join(container.get("command", []) + container.get("args", [])))
    migration = [command for command in commands if "db-migrate" in command or "backend-init" in command]
    assert len(migration) == 1
    assert "tenant-config-import" in migration[0]


def test_app_workloads_use_secret_refs_and_have_safe_lifecycle_settings():
    docs = _all_documents()
    deployments = {doc["metadata"]["name"]: doc for doc in docs if doc.get("kind") == "Deployment"}
    assert {"gateway", "worker", "admin"} <= set(deployments)
    assert deployments["gateway"]["spec"]["replicas"] == 1
    assert deployments["worker"]["spec"]["replicas"] == 2
    for name in ("gateway", "worker", "admin"):
        doc = deployments[name]
        pod = doc["spec"]["template"]["spec"]
        container = _container(doc)
        assert pod["terminationGracePeriodSeconds"] >= 20
        assert container["resources"]["requests"]
        assert container["resources"]["limits"]
        assert container["livenessProbe"] and container["readinessProbe"]
        secret_keys = [entry for entry in container.get("env", []) if "valueFrom" in entry]
        assert secret_keys, name
        assert all("secretKeyRef" in entry["valueFrom"] for entry in secret_keys)


def test_worker_hpa_and_stateful_dependencies_are_present():
    docs = _all_documents()
    hpa = next(doc for doc in docs if doc.get("kind") == "HorizontalPodAutoscaler")
    assert hpa["spec"]["scaleTargetRef"]["name"] == "worker"
    assert hpa["spec"]["minReplicas"] == 2
    assert hpa["spec"]["maxReplicas"] > 2
    stateful = {doc["metadata"]["name"] for doc in docs if doc.get("kind") == "StatefulSet"}
    assert {"postgres", "redis", "minio"} <= stateful


def test_collector_config_accepts_and_exports_otlp_metrics():
    config_map = next(doc for doc in _documents("configmap.yaml") if doc["metadata"]["name"] == "otel-collector-config")
    config = yaml.safe_load(config_map["data"]["config.yaml"])
    pipeline = config["service"]["pipelines"]["metrics"]
    assert "otlp" in pipeline["receivers"]
    assert pipeline["exporters"]


def test_secret_example_contains_placeholders_only():
    secret = _documents("secret.example.yaml")[0]
    assert secret["kind"] == "Secret"
    assert all(value.startswith("REPLACE_") for value in secret["stringData"].values())


def test_worker_has_complete_non_secret_model_configuration():
    config = _documents("configmap.yaml")[0]["data"]
    assert config["TRPC_MODEL_PROVIDER"] == "openai-compatible"
    assert config["TRPC_MODEL_NAME"]
    assert config["TRPC_MODEL_BASE_URL"]

    worker = next(doc for doc in _documents("worker.yaml") if doc.get("kind") == "Deployment")
    secret_names = {entry["name"] for entry in _container(worker).get("env", [])}
    assert "TRPC_MODEL_API_KEY" in secret_names
