#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=app_password="$POSTGRES_APP_PASSWORD" \
  --set=admin_role="$POSTGRES_USER" <<'SQL'
SELECT format(
  'CREATE ROLE tenant_agent_app LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS',
  :'app_password'
) WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenant_agent_app') \gexec
ALTER ROLE tenant_agent_app NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
GRANT CONNECT ON DATABASE tenant_agent TO tenant_agent_app;
GRANT USAGE ON SCHEMA public TO tenant_agent_app;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_role" IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO tenant_agent_app;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_role" IN SCHEMA public
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO tenant_agent_app;
SQL
