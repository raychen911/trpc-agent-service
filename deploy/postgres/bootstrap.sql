-- Run as a database administrator before Alembic. The role reconciliation is
-- idempotent. Passwords are supplied
-- by psql variables and never embedded in this file:
-- psql -v runtime_password=... -v migration_password=... \
--   [-v worker_password=...] [-v metrics_password=...] -f bootstrap.sql
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_migration') THEN
        CREATE ROLE trpc_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    ELSE
        ALTER ROLE trpc_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
        CREATE ROLE trpc_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    ELSE
        ALTER ROLE trpc_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker') THEN
        CREATE ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION BYPASSRLS;
    ELSE
        ALTER ROLE trpc_worker LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION BYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_metrics') THEN
        CREATE ROLE trpc_metrics LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    ELSE
        ALTER ROLE trpc_metrics LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;
END
$$;

SELECT format('ALTER ROLE trpc_runtime PASSWORD %L', :'runtime_password') \gexec
SELECT format('ALTER ROLE trpc_migration PASSWORD %L', :'migration_password') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_runtime', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_migration', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_worker', current_database()) \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO trpc_metrics', current_database()) \gexec
\if :{?worker_password}
SELECT format('ALTER ROLE trpc_worker PASSWORD %L', :'worker_password') \gexec
\endif
\if :{?metrics_password}
SELECT format('ALTER ROLE trpc_metrics PASSWORD %L', :'metrics_password') \gexec
\endif
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO trpc_runtime;
GRANT USAGE, CREATE ON SCHEMA public TO trpc_migration;
ALTER DEFAULT PRIVILEGES FOR ROLE trpc_migration IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO trpc_runtime;
