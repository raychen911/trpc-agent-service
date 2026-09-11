"""R2A PostgreSQL evidence for audit policy and immutable channel bindings."""

from __future__ import annotations

import uuid

import pytest

from trpc_service.storage import schema

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker, run_alembic

pytestmark = requires_docker

_REVISION_0009 = "0009_add_artifact_knowledge"
_GOVERNANCE = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],"tool_decisions":{},'
               '"content_policy":{"enabled":false,"input_action":"block","output_action":"block"},'
               '"limits":null}')
_PROFILE = '{"state_backend":"redis","artifact_backend":"s3","knowledge_backend":"sql","audit_backend":"sql"}'
_POLICY = '{"retention_days":365,"delivery_events":"all"}'


def _tenant() -> str:
    return f"t{uuid.uuid4().hex[:12]}"


def _binding() -> str:
    return str(uuid.uuid4())


def _insert_legacy_tenant(pg: PostgreSQLContainer, tenant_id: str, version: int = 7) -> None:
    head = pg.run_sql(
        "INSERT INTO tenant_configs "
        "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance,backend_profile) "
        f"VALUES ('{tenant_id}',true,{version},'app_demo','legacy','default','[]'::jsonb,"
        f"'{_GOVERNANCE}'::jsonb,'{_PROFILE}'::jsonb)")
    assert head.success, head.output
    history = pg.run_sql(
        "INSERT INTO tenant_config_versions "
        "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,governance,backend_profile) "
        f"VALUES ('{tenant_id}',{version},true,'app_demo','legacy','default','[]'::jsonb,"
        f"'{_GOVERNANCE}'::jsonb,'{_PROFILE}'::jsonb)")
    assert history.success, history.output


def _insert_tenant_at_head(pg: PostgreSQLContainer, tenant_id: str) -> None:
    result = pg.run_sql("INSERT INTO tenant_configs "
                        "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance,"
                        "backend_profile,audit_policy) "
                        f"VALUES ('{tenant_id}',true,1,'app_demo','test','default','[]'::jsonb,"
                        f"'{_GOVERNANCE}'::jsonb,'{_PROFILE}'::jsonb,'{_POLICY}'::jsonb)")
    assert result.success, result.output


@pytest.fixture(scope="module")
def r2_pg():
    if not docker_is_available():
        pytest.skip("Docker not available")
    pg = PostgreSQLContainer(name_prefix="trpc-r2a-pg")
    pg.start()
    try:
        assert run_alembic(pg.url, "upgrade", "head").returncode == 0
        yield pg
    finally:
        pg.stop()


class TestMigration0010ChannelBindings:

    @pytest.fixture(autouse=True)
    def _at_head(self, r2_pg):
        run_alembic(r2_pg.url, "upgrade", "head", check=True)

    def test_backfills_head_and_history_without_changing_versions(self, r2_pg):
        assert run_alembic(r2_pg.url, "downgrade", _REVISION_0009).returncode == 0
        tenant_id = _tenant()
        _insert_legacy_tenant(r2_pg, tenant_id)

        upgraded = run_alembic(r2_pg.url, "upgrade", "head")
        assert upgraded.returncode == 0, upgraded.stderr
        for table in ("tenant_configs", "tenant_config_versions"):
            row = r2_pg.run_sql(f"SELECT audit_policy->>'retention_days', audit_policy->>'delivery_events', version "
                                f"FROM {table} WHERE tenant_id='{tenant_id}'")
            assert row.success, row.output
            assert row.stdout.strip() == "365|all|7"
            default = r2_pg.run_sql("SELECT coalesce(column_default, '') FROM information_schema.columns "
                                    f"WHERE table_name='{table}' AND column_name='audit_policy'")
            assert default.success and default.stdout.strip() in ("", "NULL")

    def test_binding_constraints_and_history_are_immutable(self, r2_pg):
        tenant_a, tenant_b = _tenant(), _tenant()
        _insert_tenant_at_head(r2_pg, tenant_a)
        _insert_tenant_at_head(r2_pg, tenant_b)
        binding_id = _binding()
        inserted = r2_pg.run_sql(
            "INSERT INTO channel_bindings "
            "(binding_id,tenant_id,app_id,channel,external_account_id,secret_ref,enabled,version) "
            f"VALUES ('{binding_id}','{tenant_a}','app_demo','wecom','corp-robot','env:TRPC_WECOM_A',true,1)")
        assert inserted.success, inserted.output
        version = r2_pg.run_sql(
            "INSERT INTO channel_binding_versions "
            "(binding_id,tenant_id,app_id,channel,external_account_id,secret_ref,enabled,version) "
            f"VALUES ('{binding_id}','{tenant_a}','app_demo','wecom','corp-robot','env:TRPC_WECOM_A',true,1)")
        assert version.success, version.output

        duplicate = r2_pg.run_sql(
            "INSERT INTO channel_bindings "
            "(binding_id,tenant_id,app_id,channel,external_account_id,secret_ref,enabled,version) "
            f"VALUES ('{_binding()}','{tenant_b}','app_demo','wecom','corp-robot','env:TRPC_WECOM_B',true,1)")
        assert not duplicate.success
        non_normalized = r2_pg.run_sql(
            "INSERT INTO channel_bindings "
            "(binding_id,tenant_id,app_id,channel,external_account_id,secret_ref,enabled,version) "
            f"VALUES ('{_binding()}','{tenant_b}','app_demo','feishu',' Upper ','env:TRPC_FEISHU_B',true,1)")
        assert not non_normalized.success
        missing_tenant = r2_pg.run_sql(
            "INSERT INTO channel_bindings "
            "(binding_id,tenant_id,app_id,channel,external_account_id,secret_ref,enabled,version) "
            f"VALUES ('{_binding()}','{_tenant()}','app_demo','feishu','bot','env:TRPC_FEISHU_B',true,1)")
        assert not missing_tenant.success
        assert not r2_pg.run_sql(
            f"UPDATE channel_binding_versions SET enabled=false WHERE binding_id='{binding_id}'").success
        assert not r2_pg.run_sql(f"DELETE FROM channel_binding_versions WHERE binding_id='{binding_id}'").success

        error_check = r2_pg.run_sql("SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                                    "WHERE conname='execution_audit_events_error_code_valid'")
        assert error_check.success and "channel_delivery_failed" in error_check.stdout

    def test_schema_metadata_matches_live_binding_contract(self, r2_pg):
        assert schema.channel_bindings.name == "channel_bindings"
        assert schema.channel_binding_versions.name == "channel_binding_versions"
        metadata_constraints = {constraint.name for constraint in schema.channel_bindings.constraints}
        assert {"channel_bindings_channel_account_uk", "channel_bindings_tenant_fk"} <= metadata_constraints
        live_constraints = r2_pg.run_sql(
            "SELECT conname FROM pg_constraint WHERE conrelid='channel_bindings'::regclass")
        assert live_constraints.success
        assert {"channel_bindings_channel_account_uk",
                "channel_bindings_tenant_fk"} <= set(live_constraints.stdout.splitlines())

    def test_downgrade_and_reupgrade_are_symmetric(self, r2_pg):
        assert run_alembic(r2_pg.url, "downgrade", _REVISION_0009).returncode == 0
        absent = r2_pg.run_sql("SELECT count(*) FROM information_schema.tables "
                               "WHERE table_name IN ('channel_bindings','channel_binding_versions')")
        assert absent.success and absent.stdout.strip() == "0"
        policy = r2_pg.run_sql("SELECT count(*) FROM information_schema.columns "
                               "WHERE table_name='tenant_configs' AND column_name='audit_policy'")
        assert policy.success and policy.stdout.strip() == "0"
        assert run_alembic(r2_pg.url, "upgrade", "head").returncode == 0
        restored = r2_pg.run_sql("SELECT count(*) FROM information_schema.tables "
                                 "WHERE table_name IN ('channel_bindings','channel_binding_versions')")
        assert restored.success and restored.stdout.strip() == "2"
