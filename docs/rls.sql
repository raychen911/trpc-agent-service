-- PostgreSQL tenant isolation policy.
-- Workload groups and login roles are created by
-- deploy/postgres/init/00-create-workload-roles.sh before this migration runs.
-- Keeping role administration outside Alembic lets the migrator remain a
-- non-superuser schema owner.

CREATE SCHEMA IF NOT EXISTS app_security AUTHORIZATION platform_schema_owner;
ALTER SCHEMA app_security OWNER TO platform_schema_owner;
REVOKE ALL ON SCHEMA app_security FROM PUBLIC;
GRANT USAGE ON SCHEMA app_security
    TO agent_gateway, agent_worker, agent_dispatcher, agent_admin, agent_auditor;

CREATE OR REPLACE FUNCTION app_security.current_tenant_id()
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog
AS $function$
DECLARE
    scoped_tenant text;
BEGIN
    scoped_tenant := pg_catalog.btrim(
        pg_catalog.current_setting('app.tenant_id', true)
    );
    IF scoped_tenant IS NULL OR scoped_tenant = '' THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            MESSAGE = 'tenant context is required for this transaction';
    END IF;
    RETURN scoped_tenant;
END
$function$;

ALTER FUNCTION app_security.current_tenant_id() OWNER TO platform_schema_owner;
REVOKE ALL ON FUNCTION app_security.current_tenant_id() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app_security.current_tenant_id()
    TO agent_gateway, agent_worker, agent_dispatcher, agent_admin, agent_auditor;

CREATE OR REPLACE FUNCTION app_security.resolve_binding(
    requested_webhook_key_hash text,
    requested_provider text
)
RETURNS TABLE (resolved_tenant_id text, resolved_binding_id text)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    SELECT locator.resolved_tenant_id, locator.binding_id
    FROM public.channel_binding_locator AS locator
    WHERE locator.webhook_key_hash = requested_webhook_key_hash
      AND locator.provider = requested_provider
      AND locator.enabled
    LIMIT 1
$function$;

CREATE OR REPLACE FUNCTION app_security.sync_binding_locator()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
    IF TG_OP = 'DELETE'
       OR (TG_OP = 'UPDATE' AND OLD.webhook_key_hash <> NEW.webhook_key_hash) THEN
        DELETE FROM public.channel_binding_locator
        WHERE webhook_key_hash = OLD.webhook_key_hash;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    INSERT INTO public.channel_binding_locator (
        webhook_key_hash, resolved_tenant_id, binding_id, provider, enabled
    ) VALUES (
        NEW.webhook_key_hash, NEW.tenant_id, NEW.binding_id, NEW.provider,
        NEW.status <> 'disabled'
    )
    ON CONFLICT (webhook_key_hash) DO UPDATE
    SET resolved_tenant_id = EXCLUDED.resolved_tenant_id,
        binding_id = EXCLUDED.binding_id,
        provider = EXCLUDED.provider,
        enabled = EXCLUDED.enabled;

    RETURN NEW;
END
$function$;

INSERT INTO channel_binding_locator (
    webhook_key_hash, resolved_tenant_id, binding_id, provider, enabled
)
SELECT webhook_key_hash, tenant_id, binding_id, provider, status <> 'disabled'
FROM channel_binding
ON CONFLICT (webhook_key_hash) DO UPDATE
SET resolved_tenant_id = EXCLUDED.resolved_tenant_id,
    binding_id = EXCLUDED.binding_id,
    provider = EXCLUDED.provider,
    enabled = EXCLUDED.enabled;

ALTER TABLE channel_binding_locator OWNER TO platform_schema_owner;
REVOKE ALL ON channel_binding_locator FROM PUBLIC;
ALTER TABLE tenant_locator OWNER TO platform_schema_owner;
REVOKE ALL ON tenant_locator FROM PUBLIC;
ALTER FUNCTION app_security.resolve_binding(text, text) OWNER TO platform_schema_owner;
ALTER FUNCTION app_security.sync_binding_locator() OWNER TO platform_schema_owner;
REVOKE ALL ON FUNCTION app_security.resolve_binding(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION app_security.sync_binding_locator() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION app_security.resolve_binding(text, text) TO agent_gateway;

DROP TRIGGER IF EXISTS channel_binding_locator_sync ON channel_binding;
CREATE TRIGGER channel_binding_locator_sync
AFTER INSERT OR UPDATE OR DELETE ON channel_binding
FOR EACH ROW EXECUTE FUNCTION app_security.sync_binding_locator();

DO $policies$
DECLARE
    table_name text;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'tenant',
        'tenant_runtime_state',
        'storage_route',
        'storage_migration',
        'agent_app',
        'agent_release',
        'channel_binding',
        'identity_mapping',
        'session',
        'session_event',
        'session_summary',
        'memory',
        'memory_projection',
        'knowledge_document',
        'artifact',
        'inbox',
        'outbox',
        'execution_attempt',
        'budget_account',
        'budget_reservation',
        'tool_execution',
        'delivery_attempt',
        'audit_log',
        'audit_dedup',
        -- Partitions do not inherit ENABLE/FORCE RLS from their parent. The
        -- default audit partition must be protected against direct reads too.
        'audit_log_default'
    ]
    LOOP
        EXECUTE pg_catalog.format('ALTER TABLE public.%I OWNER TO platform_schema_owner', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('ALTER TABLE public.%I FORCE ROW LEVEL SECURITY', table_name);
        EXECUTE pg_catalog.format('DROP POLICY IF EXISTS tenant_isolation ON public.%I', table_name);
        EXECUTE pg_catalog.format(
            'CREATE POLICY tenant_isolation ON public.%I USING '
            || '(tenant_id = app_security.current_tenant_id()) WITH CHECK '
            || '(tenant_id = app_security.current_tenant_id())',
            table_name
        );
        EXECUTE pg_catalog.format('REVOKE ALL ON public.%I FROM PUBLIC', table_name);
    END LOOP;
END
$policies$;

-- Gateway: resolve bindings and durably accept/inspect Inbox + Outbox records.
GRANT SELECT ON tenant, tenant_runtime_state, agent_app, agent_release,
    channel_binding, identity_mapping, session TO agent_gateway;
GRANT SELECT, INSERT, UPDATE ON inbox, outbox TO agent_gateway;

-- Worker: all Session-side operations remain tenant scoped; no tenant/config mutation.
GRANT SELECT ON tenant, tenant_runtime_state, storage_route, storage_migration, agent_app, agent_release,
    channel_binding, identity_mapping, knowledge_document TO agent_worker;
-- Migration workers enumerate only tenant identifiers, then open a separate
-- RLS-scoped transaction per tenant.  They never receive a cross-tenant data
-- query capability.
GRANT SELECT ON tenant_locator TO agent_worker;
GRANT SELECT, INSERT, UPDATE ON session, session_summary, memory,
    memory_projection, artifact, inbox, outbox, execution_attempt, budget_account,
    budget_reservation, tool_execution TO agent_worker;
GRANT SELECT, INSERT ON session_event TO agent_worker;
-- ``audit_dedup`` contains only deterministic audit IDs/timestamps. PostgreSQL
-- requires SELECT to evaluate its ON CONFLICT key under RLS; this does not
-- expose the audit ledger's compliance facts to workers.
GRANT INSERT ON audit_log, audit_log_default TO agent_worker;
GRANT SELECT, INSERT ON audit_dedup TO agent_worker;
GRANT UPDATE (status, source_watermark, target_watermark, error, updated_at)
    ON storage_migration TO agent_worker;
GRANT UPDATE (execution_mode, security_epoch, updated_at) ON tenant_runtime_state TO agent_worker;

-- Dispatcher/Reconciler: publish Outbox and update projections/delivery attempts.
GRANT SELECT ON tenant_locator TO agent_dispatcher;
GRANT SELECT ON tenant, tenant_runtime_state, storage_route, storage_migration, channel_binding,
    session, inbox, memory TO agent_dispatcher;
GRANT SELECT, INSERT, UPDATE ON outbox, memory_projection, delivery_attempt,
    execution_attempt TO agent_dispatcher;
GRANT SELECT, UPDATE ON budget_reservation TO agent_dispatcher;
GRANT SELECT, UPDATE (reserved_units, version, updated_at) ON budget_account TO agent_dispatcher;
GRANT INSERT ON audit_log, audit_log_default TO agent_dispatcher;
GRANT SELECT, INSERT ON audit_dedup TO agent_dispatcher;

-- Admin is still tenant scoped. Cross-tenant jobs iterate one SET LOCAL scope at a time.
GRANT INSERT, UPDATE ON tenant_locator TO agent_admin;
GRANT SELECT, INSERT, UPDATE ON tenant, tenant_runtime_state,
    agent_app, channel_binding, identity_mapping, knowledge_document, budget_account
    TO agent_admin;
GRANT SELECT, INSERT ON agent_release, storage_route, storage_migration TO agent_admin;
GRANT UPDATE (release_status) ON agent_release TO agent_admin;
GRANT UPDATE (route_status, source_watermark, target_watermark, activated_at)
    ON storage_route TO agent_admin;
GRANT UPDATE (status, target_routing_epoch, source_watermark, target_watermark, error, updated_at)
    ON storage_migration TO agent_admin;
GRANT SELECT, INSERT ON audit_log TO agent_admin;
GRANT INSERT ON audit_log_default TO agent_admin;
GRANT SELECT, INSERT ON audit_dedup TO agent_admin;
GRANT SELECT, INSERT, UPDATE ON tool_execution, delivery_attempt TO agent_admin;

GRANT SELECT ON audit_log TO agent_auditor;

-- The bootstrap role script grants ``platform_schema_owner`` only to the
-- dedicated migrator login; application workload logins cannot assume it.
