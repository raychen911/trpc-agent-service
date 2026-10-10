#!/bin/sh
set -eu

cleanup_passwords() {
    unset runtime_password migration_password worker_password metrics_password \
        TRPC_INIT_RUNTIME_PASSWORD TRPC_INIT_MIGRATION_PASSWORD \
        TRPC_INIT_WORKER_PASSWORD TRPC_INIT_METRICS_PASSWORD
}
trap cleanup_passwords EXIT

runtime_password="$(tr -d '\r\n' </run/secrets/runtime_database_password)"
migration_password="$(tr -d '\r\n' </run/secrets/migration_database_password)"
worker_password_file=/run/secrets/worker_database_password
worker_password_set=false
if [ -r "$worker_password_file" ]; then
    worker_password="$(tr -d '\r\n' <"$worker_password_file")"
    export TRPC_INIT_WORKER_PASSWORD="$worker_password"
    worker_password_set=true
fi
metrics_password_file=/run/secrets/metrics_database_password
metrics_password_set=false
if [ -r "$metrics_password_file" ]; then
    metrics_password="$(tr -d '\r\n' <"$metrics_password_file")"
    export TRPC_INIT_METRICS_PASSWORD="$metrics_password"
    metrics_password_set=true
fi

# Keep passwords out of psql's argv.  psql imports them from the environment
# while the SQL itself is supplied on stdin.
export TRPC_INIT_RUNTIME_PASSWORD="$runtime_password"
export TRPC_INIT_MIGRATION_PASSWORD="$migration_password"
{
    printf '%s\n' \
        '\getenv runtime_password TRPC_INIT_RUNTIME_PASSWORD' \
        '\getenv migration_password TRPC_INIT_MIGRATION_PASSWORD'
    if [ "$worker_password_set" = true ]; then
        printf '%s\n' '\getenv worker_password TRPC_INIT_WORKER_PASSWORD'
    fi
    if [ "$metrics_password_set" = true ]; then
        printf '%s\n' '\getenv metrics_password TRPC_INIT_METRICS_PASSWORD'
    fi
    cat <<'SQL'
DO $block$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_migration') THEN
        CREATE ROLE trpc_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
        CREATE ROLE trpc_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    ELSE
        ALTER ROLE trpc_runtime NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker') THEN
        CREATE ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS;
    ELSE
        ALTER ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT BYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_metrics') THEN
        CREATE ROLE trpc_metrics LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    ELSE
        ALTER ROLE trpc_metrics LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
END
$block$;
SELECT format('ALTER ROLE trpc_runtime PASSWORD %L', :'runtime_password') \gexec
SELECT format('ALTER ROLE trpc_migration PASSWORD %L', :'migration_password') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_runtime', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_migration', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_worker', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_metrics', current_database()) \gexec
-- Extension installation is an owner/bootstrap responsibility.  The migration
-- role is intentionally non-owner and must not need elevated database rights.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO trpc_migration;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trpc_runtime;
SQL
    if [ "$worker_password_set" = true ]; then
        cat <<'SQL'
SELECT format('ALTER ROLE trpc_worker PASSWORD %L', :'worker_password') \gexec
SQL
    fi
    if [ "$metrics_password_set" = true ]; then
        cat <<'SQL'
SELECT format('ALTER ROLE trpc_metrics PASSWORD %L', :'metrics_password') \gexec
SQL
    fi
} | psql --set=ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"
