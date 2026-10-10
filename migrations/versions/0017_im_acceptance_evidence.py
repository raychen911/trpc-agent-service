"""Add fenced WeCom connection state and redacted acceptance evidence.

Revision ID: 0017_im_acceptance_evidence
Revises: 0016_session_ready_backlog_metric
"""

from __future__ import annotations

from alembic import op

revision = "0017_im_acceptance_evidence"
down_revision = "0016_session_ready_backlog_metric"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE public.wecom_connection_state (
            tenant_id text NOT NULL,
            binding_id text NOT NULL,
            owner_hash text NOT NULL CHECK (owner_hash ~ '^[0-9a-f]{64}$'),
            epoch bigint NOT NULL CHECK (epoch >= 1),
            phase text NOT NULL CHECK (
                phase IN ('acquired','authenticated','disconnected','released')
            ),
            acquired_at timestamptz NOT NULL,
            authenticated_at timestamptz,
            disconnected_at timestamptz,
            released_at timestamptz,
            last_provider_event_hash text
                CHECK (
                    last_provider_event_hash IS NULL
                    OR last_provider_event_hash ~ '^[0-9a-f]{64}$'
                ),
            last_provider_event_at timestamptz,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (tenant_id, binding_id),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES public.channel_bindings(tenant_id, binding_id)
        );

        CREATE TABLE public.im_acceptance_evidence_events (
            event_id uuid NOT NULL DEFAULT gen_random_uuid(),
            tenant_id text NOT NULL,
            binding_id text NOT NULL,
            channel text NOT NULL CHECK (channel = 'wecom_ai_bot'),
            connection_epoch bigint NOT NULL CHECK (connection_epoch >= 1),
            event_type text NOT NULL CHECK (length(event_type) BETWEEN 1 AND 64),
            owner_hash text NOT NULL CHECK (owner_hash ~ '^[0-9a-f]{64}$'),
            provider_event_hash text
                CHECK (
                    provider_event_hash IS NULL
                    OR provider_event_hash ~ '^[0-9a-f]{64}$'
                ),
            occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (tenant_id, event_id),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES public.channel_bindings(tenant_id, binding_id)
        );

        CREATE INDEX ix_wecom_connection_state_epoch
            ON public.wecom_connection_state (tenant_id, binding_id, epoch);
        CREATE INDEX ix_im_acceptance_evidence_timeline
            ON public.im_acceptance_evidence_events (
                tenant_id, binding_id, connection_epoch, occurred_at, event_id
            );
        CREATE INDEX ix_im_acceptance_evidence_provider_hash
            ON public.im_acceptance_evidence_events (
                tenant_id, binding_id, provider_event_hash
            )
            WHERE provider_event_hash IS NOT NULL;

        ALTER TABLE public.wecom_connection_state ENABLE ROW LEVEL SECURITY;
        ALTER TABLE public.im_acceptance_evidence_events ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_wecom_connection_state
            ON public.wecom_connection_state
            USING (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            )
            WITH CHECK (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            );
        CREATE POLICY tenant_isolation_im_acceptance_evidence_events
            ON public.im_acceptance_evidence_events
            USING (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            )
            WITH CHECK (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            );

        REVOKE ALL ON TABLE public.wecom_connection_state FROM PUBLIC, trpc_runtime;
        REVOKE ALL ON TABLE public.im_acceptance_evidence_events FROM PUBLIC, trpc_runtime;
        GRANT SELECT, INSERT, UPDATE
            ON TABLE public.wecom_connection_state TO trpc_worker;
        GRANT SELECT, INSERT
            ON TABLE public.im_acceptance_evidence_events TO trpc_worker;

        CREATE TRIGGER migration_write_barrier_wecom_connection_state
            BEFORE INSERT OR UPDATE OR DELETE ON public.wecom_connection_state
            FOR EACH ROW EXECUTE FUNCTION public.migration_write_barrier_guard();
        CREATE TRIGGER migration_write_barrier_im_acceptance_evidence_events
            BEFORE INSERT OR UPDATE OR DELETE ON public.im_acceptance_evidence_events
            FOR EACH ROW EXECUTE FUNCTION public.migration_write_barrier_guard();
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE ALL ON TABLE public.im_acceptance_evidence_events
            FROM PUBLIC, trpc_runtime, trpc_worker;
        REVOKE ALL ON TABLE public.wecom_connection_state
            FROM PUBLIC, trpc_runtime, trpc_worker;
        DROP TABLE IF EXISTS public.im_acceptance_evidence_events;
        DROP TABLE IF EXISTS public.wecom_connection_state;
        """
    )
