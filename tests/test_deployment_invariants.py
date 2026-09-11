from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_initial_migration_uses_its_immutable_schema_snapshots() -> None:
    migration = (ROOT / "alembic/versions/0001_platform_schema.py").read_text(encoding="utf-8")
    assert 'with_name("_snapshots")' in migration
    assert ' / "docs"' not in migration
    assert (ROOT / "alembic/versions/_snapshots/0001_platform_schema.sql").is_file()
    assert (ROOT / "alembic/versions/_snapshots/0001_platform_rls.sql").is_file()

    audit_dedup = (ROOT / "alembic/versions/0009_audit_dedup.py").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS audit_dedup" in audit_dedup
    assert "DROP POLICY IF EXISTS tenant_isolation ON audit_dedup" in audit_dedup


def test_kustomize_uses_per_workload_database_secrets_and_restricted_roles() -> None:
    base = ROOT / "deploy/kubernetes/base"
    workloads = {
        "api.yaml": ("TRPC_SERVICE_API_DATABASE_URL", "agent_api_gateway"),
        "worker.yaml": ("TRPC_SERVICE_WORKER_DATABASE_URL", "agent_worker"),
        "dispatcher.yaml": ("TRPC_SERVICE_DISPATCHER_DATABASE_URL", "agent_dispatcher"),
        "migration-job.yaml": ("TRPC_SERVICE_MIGRATOR_DATABASE_URL", "platform_schema_owner"),
        "wecom-aibot.yaml": ("TRPC_SERVICE_AIBOT_DATABASE_URL", "agent_wecom_aibot"),
    }
    for name, (secret_key, role) in workloads.items():
        manifest = (base / name).read_text(encoding="utf-8")
        assert secret_key in manifest
        assert f"value: {role}" in manifest
        assert "secretRef: {name: trpc-agent-service-secrets}" not in manifest

    config = (base / "configmap.yaml").read_text(encoding="utf-8")
    assert "TRPC_SERVICE_ENVIRONMENT: production" in config
