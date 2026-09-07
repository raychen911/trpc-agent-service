from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.render_k8s import render_documents


def load(path: Path) -> list[dict[str, object]]:
    return [
        document for document in yaml.safe_load_all(path.read_text(encoding="utf-8")) if document is not None
    ]


def test_all_deployment_yaml_parses_and_image_renderer_preserves_mixed_documents() -> None:
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "docker-compose.yml",
        *sorted((root / "deploy").rglob("*.yaml")),
        *sorted((root / "deploy").rglob("*.yml")),
    ]
    assert sum(len(load(path)) for path in paths) == 46

    compose = load(root / "docker-compose.yml")[0]
    compose_services = compose["services"]  # type: ignore[index]
    common_database_url = compose["x-app"]["environment"]["TAP_CONTROL_DATABASE_URL"]  # type: ignore[index]
    migration_database_url = compose_services["migrate"]["environment"][  # type: ignore[index]
        "TAP_CONTROL_DATABASE_URL"
    ]
    assert "tenant_agent_app" in common_database_url
    assert "tenant_agent_admin" in migration_database_url
    role_script = (root / "deploy/postgres/init-app-role.sh").read_text(encoding="utf-8")
    assert "NOSUPERUSER" in role_script and "NOBYPASSRLS" in role_script
    secret_examples = load(root / "deploy/k8s/secrets.example.yaml")
    runtime_secret_dsns = [
        document["stringData"]["TAP_CONTROL_DATABASE_URL"]  # type: ignore[index]
        for document in secret_examples
        if document["metadata"]["name"] != "tenant-agent-migration-secrets"  # type: ignore[index]
        and "TAP_CONTROL_DATABASE_URL" in document.get("stringData", {})
    ]
    migration_secret = next(
        document
        for document in secret_examples
        if document["metadata"]["name"] == "tenant-agent-migration-secrets"  # type: ignore[index]
    )
    assert runtime_secret_dsns and all("NOBYPASSRLS" in dsn for dsn in runtime_secret_dsns)
    assert "MIGRATION_OWNER" in migration_secret["stringData"]["TAP_CONTROL_DATABASE_URL"]  # type: ignore[index]
    for role in ("channel-adapter", "gateway", "worker", "outbox", "admin-api"):
        limits = compose_services[role]["deploy"]["resources"]["limits"]  # type: ignore[index]
        assert limits["cpus"] and limits["memory"]

    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert "HEALTHCHECK" in dockerfile and "/health/ready" in dockerfile
    assert any((root / "scripts").iterdir())
    rls = (root / "migrations/postgres_rls.example.sql").read_text(encoding="utf-8")
    assert "'usage_reservations'" in rls
    assert "DROP POLICY IF EXISTS" in rls
    reservation_migration = (root / "migrations/versions/8b6d1e4f2a90_usage_reservations.py").read_text(
        encoding="utf-8"
    )
    assert "ENABLE ROW LEVEL SECURITY" not in reservation_migration
    corrective_rls = (root / "migrations/versions/d4e5f607a1b2_disable_implicit_rls.py").read_text(
        encoding="utf-8"
    )
    assert "DISABLE ROW LEVEL SECURITY" in corrective_rls

    digest = "registry.example/tenant-agent@sha256:" + "a" * 64
    platform = load(root / "deploy/k8s/platform.yaml")
    original_kinds = [document["kind"] for document in platform]
    assert render_documents(platform, digest) == 5
    assert [document["kind"] for document in platform] == original_kinds
    workload_images = [
        container["image"]
        for document in platform
        if document["kind"] == "Deployment"
        for container in document["spec"]["template"]["spec"]["containers"]  # type: ignore[index]
    ]
    assert workload_images and set(workload_images) == {digest}

    migration = load(root / "deploy/k8s/migration.yaml")
    assert render_documents(migration, digest, release_id="release-a") == 1
    assert migration[0]["metadata"]["name"] == "schema-migrate-d4e5f607a1b2-release-a"  # type: ignore[index]
    with pytest.raises(ValueError, match="immutable"):
        render_documents(migration, "tenant-agent:latest")

    kubernetes_documents = [
        document for path in sorted((root / "deploy/k8s").glob("*.yaml")) for document in load(path)
    ]
    deployments = {
        document["metadata"]["name"]: document  # type: ignore[index]
        for document in kubernetes_documents
        if document["kind"] == "Deployment"
    }
    for role in ("channel-adapter", "gateway", "worker", "outbox", "admin-api"):
        container = deployments[role]["spec"]["template"]["spec"]["containers"][0]  # type: ignore[index]
        assert container["resources"]["requests"] and container["resources"]["limits"]
        assert container["readinessProbe"] and container["livenessProbe"]
        assert container["startupProbe"]["failureThreshold"] >= 24
    for role in ("worker", "outbox"):
        pod_spec = deployments[role]["spec"]["template"]["spec"]  # type: ignore[index]
        assert pod_spec["terminationGracePeriodSeconds"] >= 130
        assert pod_spec["containers"][0]["lifecycle"]["preStop"]

    for document in kubernetes_documents:
        if document["kind"] not in {"Deployment", "StatefulSet", "DaemonSet", "Job"}:
            continue
        containers = document["spec"]["template"]["spec"]["containers"]  # type: ignore[index]
        assert all(
            container["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
            for container in containers
        )
        if document["kind"] == "Deployment":
            assert all(container.get("startupProbe") for container in containers)
