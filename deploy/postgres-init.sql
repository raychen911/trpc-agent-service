-- Local Compose only. Production credentials and roles must come from the platform/KMS.
CREATE ROLE agent_runtime
    LOGIN
    PASSWORD 'agent-runtime-dev'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT;

GRANT CONNECT ON DATABASE agent TO agent_runtime;
GRANT USAGE ON SCHEMA public TO agent_runtime;

ALTER DEFAULT PRIVILEGES FOR ROLE agent_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO agent_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE agent_owner IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO agent_runtime;
