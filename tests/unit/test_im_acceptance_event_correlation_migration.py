from pathlib import Path

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "migrations"
    / "versions"
    / "0021_im_acceptance_event_correlation.py"
)
INDEX_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "migrations"
    / "versions"
    / "0022_inbound_provider_event_unique_index.py"
)


def test_im_acceptance_event_correlation_migration_is_current_and_bounded() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "0021_im_acceptance_event_correlation"' in source
    assert 'down_revision = "0020_fault_stage_fixture_cleanup"' in source
    assert "ADD COLUMN provider_event_hash text" in source
    assert "provider_event_hash ~ '^[0-9a-f]{64}$'" in source
    assert "ADD COLUMN delivery_count integer NOT NULL DEFAULT 1" in source
    assert "CHECK (delivery_count >= 1)" in source
    assert "ADD COLUMN retry_after_seconds double precision" in source
    assert "retry_after_seconds <= 3600" in source
    assert "CREATE TABLE public.im_acceptance_runs" in source
    assert "run_id_sha256 text NOT NULL" in source
    assert "run_nonce_sha256 text NOT NULL" in source
    assert "run_binding_sha256 text NOT NULL" in source
    assert "expires_at timestamptz NOT NULL" in source
    assert "uq_im_acceptance_run_provider_event" in source
    assert "UNIQUE (tenant_id, binding_id, channel, provider_event_hash)" in source
    assert "ON DELETE CASCADE" in source
    assert "ENABLE ROW LEVEL SECURITY" in source
    assert "tenant_isolation_im_acceptance_runs" in source
    assert "FROM PUBLIC, trpc_runtime, trpc_worker" in source
    assert "TO trpc_runtime" in source
    assert "TO trpc_worker" not in source
    assert "SELECT, INSERT, UPDATE, DELETE" in source
    assert "public.wecom_connection_state" in source
    assert "public.im_acceptance_evidence_events" in source
    assert "GRANT SELECT ON TABLE" in source
    assert source.count("CREATE OR REPLACE FUNCTION public.migration_protected_target_counts") == 2
    upgrade_source, downgrade_source = source.split("def downgrade() -> None:", maxsplit=1)
    assert "SELECT 'im_acceptance_runs'::text" in upgrade_source
    assert "FROM public.im_acceptance_runs" in upgrade_source
    assert "SELECT 'im_acceptance_runs'::text" not in downgrade_source
    assert "OWNER TO trpc_migration" in source
    assert "GRANT EXECUTE ON FUNCTION public.migration_protected_target_counts(text)" in source
    assert "Migrations 0021 and 0022 must finish" in source


def test_im_acceptance_event_correlation_downgrade_removes_only_owned_columns() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    assert "DROP COLUMN IF EXISTS retry_after_seconds" in source
    assert "DROP TABLE IF EXISTS public.im_acceptance_runs" in source
    assert "DROP COLUMN IF EXISTS delivery_count" in source
    assert "DROP COLUMN IF EXISTS provider_event_hash" in source


def test_inbound_provider_event_index_is_a_separate_retryable_migration() -> None:
    source = INDEX_MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "0022_inbound_provider_event_unique_index"' in source
    assert 'down_revision = "0021_im_acceptance_event_correlation"' in source
    assert "autocommit_block()" in source
    assert "DROP INDEX CONCURRENTLY IF EXISTS" in source
    assert "CREATE UNIQUE INDEX CONCURRENTLY" in source
    assert "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS" not in source
    assert "ux_inbound_provider_event_hash" in source
    assert "WHERE provider_event_hash IS NOT NULL" in source
    assert "DROP INDEX CONCURRENTLY IF EXISTS" in source
