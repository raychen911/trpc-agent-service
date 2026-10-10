"""Split WeChat AppID from the Official Account original ID.

Revision ID: 0004_wechat_binding_identifiers
Revises: 0003_tenant_budget_usage
"""

from __future__ import annotations

from alembic import op

revision = "0004_wechat_binding_identifiers"
down_revision = "0003_tenant_budget_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE channel_bindings
            ADD COLUMN IF NOT EXISTS wechat_app_id text,
            ADD COLUMN IF NOT EXISTS wechat_original_id text;
        UPDATE channel_bindings
           SET wechat_original_id=account_id
         WHERE channel='wechat_official' AND wechat_original_id IS NULL;

        DROP FUNCTION IF EXISTS resolve_channel_binding(text);
        CREATE FUNCTION resolve_channel_binding(p_binding_id text)
        RETURNS TABLE (
            tenant_id text,
            binding_id text,
            app_id text,
            channel text,
            account_id text,
            wechat_app_id text,
            wechat_original_id text,
            secret_refs jsonb,
            capabilities jsonb,
            enabled boolean,
            control_version bigint,
            tenant_active boolean,
            active_config_version bigint,
            candidate_config_version bigint,
            candidate_percent numeric
        )
        LANGUAGE sql
        SECURITY DEFINER
        STABLE
        SET search_path = pg_catalog, public
        AS $$
            SELECT b.tenant_id, b.binding_id, b.app_id, b.channel, b.account_id,
                   b.wechat_app_id, b.wechat_original_id, b.secret_refs,
                   b.capabilities, b.enabled, b.control_version,
                   (t.status = 'active'), a.active_config_version,
                   a.candidate_config_version, a.candidate_percent
              FROM public.channel_bindings b
              JOIN public.tenants t ON t.tenant_id = b.tenant_id
              JOIN public.agent_apps a
                ON a.tenant_id = b.tenant_id AND a.app_id = b.app_id
             WHERE b.binding_id = p_binding_id
        $$;
        REVOKE ALL ON FUNCTION resolve_channel_binding(text) FROM PUBLIC;
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT EXECUTE ON FUNCTION resolve_channel_binding(text) TO trpc_runtime;
            END IF;
        END
        $block$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP FUNCTION IF EXISTS resolve_channel_binding(text);
        ALTER TABLE channel_bindings
            DROP COLUMN IF EXISTS wechat_original_id,
            DROP COLUMN IF EXISTS wechat_app_id;
        CREATE FUNCTION resolve_channel_binding(p_binding_id text)
        RETURNS TABLE (
            tenant_id text,
            binding_id text,
            app_id text,
            channel text,
            account_id text,
            secret_refs jsonb,
            capabilities jsonb,
            enabled boolean,
            control_version bigint,
            tenant_active boolean,
            active_config_version bigint,
            candidate_config_version bigint,
            candidate_percent numeric
        )
        LANGUAGE sql
        SECURITY DEFINER
        STABLE
        SET search_path = pg_catalog, public
        AS $$
            SELECT b.tenant_id, b.binding_id, b.app_id, b.channel, b.account_id,
                   b.secret_refs, b.capabilities, b.enabled, b.control_version,
                   (t.status = 'active'), a.active_config_version,
                   a.candidate_config_version, a.candidate_percent
              FROM public.channel_bindings b
              JOIN public.tenants t ON t.tenant_id = b.tenant_id
              JOIN public.agent_apps a
                ON a.tenant_id = b.tenant_id AND a.app_id = b.app_id
             WHERE b.binding_id = p_binding_id
        $$;
        REVOKE ALL ON FUNCTION resolve_channel_binding(text) FROM PUBLIC;
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT EXECUTE ON FUNCTION resolve_channel_binding(text) TO trpc_runtime;
            END IF;
        END
        $block$;
        """
    )
