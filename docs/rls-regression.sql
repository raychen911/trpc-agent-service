-- Destructive only to two transaction-local fixture rows; the final ROLLBACK keeps
-- the database unchanged. Run as a deployment-test identity allowed to SET ROLE.
\set ON_ERROR_STOP on

BEGIN;

DO $role_assertions$
DECLARE
    unsafe_count integer;
BEGIN
    SELECT count(*) INTO unsafe_count
    FROM pg_roles
    WHERE rolname IN (
        'platform_schema_owner', 'platform_migrator', 'agent_gateway',
        'agent_worker', 'agent_dispatcher', 'agent_admin', 'agent_auditor'
    )
      AND (rolcanlogin OR rolsuper OR rolbypassrls);

    IF unsafe_count <> 0 THEN
        RAISE EXCEPTION 'platform owner/migrator/service group roles must be NOLOGIN, NOSUPERUSER, NOBYPASSRLS';
    END IF;

    SELECT count(*) INTO unsafe_count
    FROM pg_roles
    WHERE rolname IN ('agent_gateway', 'agent_worker', 'agent_dispatcher', 'agent_admin', 'agent_auditor')
      AND (
          pg_catalog.has_table_privilege(rolname, 'public.channel_binding_locator', 'SELECT')
          OR pg_catalog.has_table_privilege(rolname, 'public.channel_binding_locator', 'INSERT')
          OR pg_catalog.has_table_privilege(rolname, 'public.channel_binding_locator', 'UPDATE')
          OR pg_catalog.has_table_privilege(rolname, 'public.channel_binding_locator', 'DELETE')
      );

    IF unsafe_count <> 0 THEN
        RAISE EXCEPTION 'service roles must not have direct locator table access';
    END IF;

    SELECT count(*) INTO unsafe_count
    FROM information_schema.columns AS cols
    JOIN pg_catalog.pg_namespace AS ns ON ns.nspname = cols.table_schema
    JOIN pg_catalog.pg_class AS cls
      ON cls.relnamespace = ns.oid AND cls.relname = cols.table_name
    WHERE cols.table_schema = 'public'
      AND cols.column_name = 'tenant_id'
      AND cls.relkind IN ('r', 'p')
      -- The scheduler index contains only IDs/enabled bits and deliberately
      -- has no tenant business rows; it is the sole controlled enumeration
      -- surface for dispatchers before they enter a tenant-scoped transaction.
      AND cols.table_name <> 'tenant_locator'
      AND NOT (cls.relrowsecurity AND cls.relforcerowsecurity);

    IF unsafe_count <> 0 THEN
        RAISE EXCEPTION 'representative tenant tables must have ENABLE/FORCE RLS';
    END IF;
END
$role_assertions$;

SET LOCAL ROLE platform_schema_owner;
SELECT pg_catalog.set_config('app.tenant_id', 't_rls_fixture_a', true);
INSERT INTO tenant (tenant_id, display_name, status)
VALUES ('t_rls_fixture_a', 'RLS fixture A', 'active');

SELECT pg_catalog.set_config('app.tenant_id', 't_rls_fixture_b', true);
INSERT INTO tenant (tenant_id, display_name, status)
VALUES ('t_rls_fixture_b', 'RLS fixture B', 'active');

SET LOCAL ROLE agent_gateway;
SELECT pg_catalog.set_config('app.tenant_id', '', true);

DO $missing_context$
BEGIN
    BEGIN
        PERFORM count(*) FROM tenant;
        RAISE EXCEPTION 'query unexpectedly succeeded without tenant context';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END
$missing_context$;

SELECT pg_catalog.set_config('app.tenant_id', 't_rls_fixture_a', true);
DO $tenant_a$
DECLARE
    visible_count integer;
BEGIN
    SELECT count(*) INTO visible_count FROM tenant;
    IF visible_count <> 1 THEN
        RAISE EXCEPTION 'tenant A saw % rows, expected exactly 1', visible_count;
    END IF;
END
$tenant_a$;

SET LOCAL ROLE agent_admin;
DO $cross_tenant_write$
BEGIN
    BEGIN
        INSERT INTO tenant (tenant_id, display_name, status)
        VALUES ('t_rls_forbidden', 'must not be written', 'active');
        RAISE EXCEPTION 'cross-tenant insert unexpectedly succeeded';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END
$cross_tenant_write$;

-- FORCE RLS subjects even the table owner to the tenant policy.
SET LOCAL ROLE platform_schema_owner;
SELECT pg_catalog.set_config('app.tenant_id', 't_rls_fixture_b', true);
DO $forced_owner$
DECLARE
    visible_count integer;
BEGIN
    SELECT count(*) INTO visible_count FROM tenant;
    IF visible_count <> 1 THEN
        RAISE EXCEPTION 'FORCE RLS owner saw % rows, expected exactly 1', visible_count;
    END IF;
END
$forced_owner$;

ROLLBACK;

-- A new transaction on the same session must not inherit the prior tenant GUC.
BEGIN;
SET LOCAL ROLE agent_gateway;
DO $pool_reset$
BEGIN
    BEGIN
        PERFORM count(*) FROM tenant;
        RAISE EXCEPTION 'transaction-local tenant context leaked across rollback';
    EXCEPTION
        WHEN insufficient_privilege THEN NULL;
    END;
END
$pool_reset$;
ROLLBACK;
