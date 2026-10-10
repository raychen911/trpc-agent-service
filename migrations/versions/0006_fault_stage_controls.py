"""Add tenant-scoped one-shot fault stage controls.

Revision ID: 0006_fault_stage_controls
Revises: 0005_add_feishu_channel
"""

from __future__ import annotations

from alembic import op

revision = "0006_fault_stage_controls"
down_revision = "0005_add_feishu_channel"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE fault_stage_controls (
            tenant_id text NOT NULL CHECK (char_length(tenant_id) BETWEEN 1 AND 256)
                REFERENCES tenants(tenant_id),
            control_id uuid NOT NULL DEFAULT gen_random_uuid(),
            run_id text NOT NULL CHECK (char_length(run_id) BETWEEN 1 AND 128),
            stage text NOT NULL CHECK (stage IN ('enqueue','tool','commit_txn_open')),
            target_fingerprint text NOT NULL CHECK (target_fingerprint ~ '^[0-9a-f]{64}$'),
            target_worker_id text NOT NULL CHECK (char_length(target_worker_id) BETWEEN 1 AND 256),
            target_inbound_id text CHECK (target_inbound_id IS NULL OR char_length(target_inbound_id) BETWEEN 1 AND 256),
            target_turn_id text CHECK (target_turn_id IS NULL OR char_length(target_turn_id) BETWEEN 1 AND 256),
            target_execution_key text CHECK (target_execution_key IS NULL OR char_length(target_execution_key) BETWEEN 1 AND 256),
            target_stream_id text CHECK (target_stream_id IS NULL OR char_length(target_stream_id) BETWEEN 1 AND 256),
            target_fencing_token bigint CHECK (target_fencing_token >= 0),
            token_hash text NOT NULL CHECK (token_hash ~ '^[0-9a-f]{64}$'),
            status text NOT NULL DEFAULT 'armed'
                CHECK (status IN ('armed','entered','released','expired')),
            armed_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL CHECK (
                expires_at > armed_at
                AND expires_at <= armed_at + interval '5 minutes'
            ),
            entered_at timestamptz,
            released_at timestamptz,
            marker_id uuid,
            marker_status text CHECK (marker_status IS NULL OR marker_status = 'entered'),
            marker_worker_id text,
            marker_inbound_id text,
            marker_turn_id text,
            marker_execution_key text,
            marker_stream_id text,
            marker_fencing_token bigint CHECK (marker_fencing_token >= 0),
            marker_at timestamptz,
            PRIMARY KEY (tenant_id, control_id),
            UNIQUE (tenant_id, run_id, stage, target_fingerprint)
        );
        COMMENT ON COLUMN fault_stage_controls.marker_id IS
            'Transient evidence; cleanup_expired removes the row. Acceptance reports must read markers before cleanup.';
        CREATE INDEX ix_fault_stage_controls_tenant_lookup
            ON fault_stage_controls (tenant_id, run_id, stage, status, expires_at);
        CREATE INDEX ix_fault_stage_controls_tenant_expiry
            ON fault_stage_controls (tenant_id, expires_at);
        ALTER TABLE fault_stage_controls ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_fault_stage_controls
            ON fault_stage_controls
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        REVOKE ALL ON TABLE fault_stage_controls FROM PUBLIC;
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE
                    ON TABLE fault_stage_controls TO trpc_runtime;
            END IF;
        END
        $block$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fault_stage_controls CASCADE")
