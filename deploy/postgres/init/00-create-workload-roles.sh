#!/bin/sh
# Create only non-privileged application login roles.  This script runs during
# first PostgreSQL initialization and may also be invoked by the explicit
# provisioning runbook for an existing volume.
set -eu

: "${TRPC_DB_API_PASSWORD:?TRPC_DB_API_PASSWORD is required}"
: "${TRPC_DB_WORKER_PASSWORD:?TRPC_DB_WORKER_PASSWORD is required}"
: "${TRPC_DB_DISPATCHER_PASSWORD:?TRPC_DB_DISPATCHER_PASSWORD is required}"
: "${TRPC_DB_MIGRATOR_PASSWORD:?TRPC_DB_MIGRATOR_PASSWORD is required}"
: "${TRPC_DB_AIBOT_PASSWORD:?TRPC_DB_AIBOT_PASSWORD is required}"

export PGPASSWORD="${TRPC_DB_BOOTSTRAP_PASSWORD:-${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}}"
bootstrap_user="${TRPC_DB_BOOTSTRAP_USER:-${POSTGRES_USER:-postgres}}"

psql --set=ON_ERROR_STOP=1 --username "$bootstrap_user" --dbname "$POSTGRES_DB" \
  --set=bootstrap_role="$bootstrap_user" \
  --set=api_password="$TRPC_DB_API_PASSWORD" \
  --set=worker_password="$TRPC_DB_WORKER_PASSWORD" \
  --set=dispatcher_password="$TRPC_DB_DISPATCHER_PASSWORD" \
  --set=migrator_password="$TRPC_DB_MIGRATOR_PASSWORD" \
  --set=aibot_password="$TRPC_DB_AIBOT_PASSWORD" <<'SQL'
DO $roles$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY[
        'platform_schema_owner', 'platform_migrator',
        'agent_gateway', 'agent_worker', 'agent_dispatcher', 'agent_admin',
        'agent_auditor', 'agent_api_gateway', 'agent_wecom_aibot'
    ]
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format(
                'CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                role_name
            );
        END IF;
        EXECUTE format(
            'ALTER ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
            role_name
        );
    END LOOP;
END
$roles$;

-- Alembic creates its metadata table before executing the RLS SQL, so a
-- legacy Compose database leaves this one table owned by the old bootstrap
-- role even though the application tables are already transferred by
-- docs/rls.sql.  Transfer only that known metadata relation.  This is an
-- ownership-only transition: it neither copies nor deletes tenant data, and
-- is a no-op on a freshly initialized cluster.
SELECT 'ALTER TABLE public.alembic_version OWNER TO platform_schema_owner'
WHERE to_regclass('public.alembic_version') IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM pg_class relation
      JOIN pg_roles owner_role ON owner_role.oid = relation.relowner
      WHERE relation.oid = 'public.alembic_version'::regclass
        AND owner_role.rolname = :'bootstrap_role'
  )
\gexec

GRANT agent_admin, agent_gateway TO agent_api_gateway;
GRANT agent_gateway, agent_dispatcher TO agent_wecom_aibot;
GRANT platform_schema_owner TO platform_migrator;

SELECT format(
    'CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'trpc_api_gateway', :'api_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_api_gateway')
\gexec
SELECT format(
    'CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'trpc_worker', :'worker_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker')
\gexec
SELECT format(
    'CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'trpc_dispatcher', :'dispatcher_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_dispatcher')
\gexec
SELECT format(
    'CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'trpc_migrator', :'migrator_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_migrator')
\gexec
SELECT format(
    'CREATE ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
    'trpc_wecom_aibot', :'aibot_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_wecom_aibot')
\gexec

SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', 'trpc_api_gateway', :'api_password')
\gexec
SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', 'trpc_worker', :'worker_password')
\gexec
SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', 'trpc_dispatcher', :'dispatcher_password')
\gexec
SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', 'trpc_migrator', :'migrator_password')
\gexec
SELECT format('ALTER ROLE %I LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L', 'trpc_wecom_aibot', :'aibot_password')
\gexec

GRANT agent_api_gateway TO trpc_api_gateway;
GRANT agent_worker TO trpc_worker;
GRANT agent_dispatcher TO trpc_dispatcher;
GRANT platform_schema_owner TO trpc_migrator;
GRANT agent_wecom_aibot TO trpc_wecom_aibot;

REVOKE ALL ON DATABASE trpc_agent FROM PUBLIC;
GRANT CONNECT ON DATABASE trpc_agent TO trpc_api_gateway, trpc_worker,
    trpc_dispatcher, trpc_migrator, trpc_wecom_aibot;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO trpc_api_gateway, trpc_worker, trpc_dispatcher,
    trpc_wecom_aibot;
GRANT USAGE, CREATE ON SCHEMA public TO platform_schema_owner;
SQL
