"""Fail closed unless configured database roles have safe RLS attributes.

Revision ID: 0015_worker_database_role_contract
Revises: 0014_worker_database_role

Migration ``0014`` moved cross-tenant SQL privileges to ``trpc_worker``.
Existing databases may already be at that revision, so this follow-up keeps
the role contract as an applied migration that is checked on every upgrade.
It requires login-enabled, non-owner roles with explicit RLS attributes.  It
intentionally performs no grants or role changes; provisioning remains an
administrator/bootstrap responsibility.
"""

from __future__ import annotations

from alembic import op

revision = "0015_worker_database_role_contract"
down_revision = "0014_worker_database_role"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $roles$
        DECLARE
            worker_is_superuser boolean;
            worker_bypasses_rls boolean;
            worker_can_login boolean;
            worker_owned_rls_tables bigint;
            runtime_is_superuser boolean;
            runtime_bypasses_rls boolean;
            runtime_can_login boolean;
            runtime_owned_rls_tables bigint;
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker') THEN
                RAISE EXCEPTION
                    'trpc_worker role must be provisioned before migration 0015';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
                RAISE EXCEPTION
                    'trpc_runtime role must be provisioned before migration 0015';
            END IF;

            SELECT rolcanlogin, rolsuper, rolbypassrls
              INTO worker_can_login, worker_is_superuser, worker_bypasses_rls
              FROM pg_roles
             WHERE rolname = 'trpc_worker';
            SELECT count(*)
              INTO worker_owned_rls_tables
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relrowsecurity
               AND pg_get_userbyid(c.relowner) = 'trpc_worker';
            IF worker_can_login IS DISTINCT FROM TRUE
               OR worker_is_superuser IS DISTINCT FROM FALSE
               OR worker_bypasses_rls IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION
                    'trpc_worker must be LOGIN NOSUPERUSER with explicit BYPASSRLS before migration 0015';
            END IF;
            IF worker_owned_rls_tables <> 0 THEN
                RAISE EXCEPTION
                    'trpc_worker must not own RLS tables before migration 0015';
            END IF;

            SELECT rolcanlogin, rolsuper, rolbypassrls
              INTO runtime_can_login, runtime_is_superuser, runtime_bypasses_rls
              FROM pg_roles
             WHERE rolname = 'trpc_runtime';
            SELECT count(*)
              INTO runtime_owned_rls_tables
              FROM pg_class AS c
              JOIN pg_namespace AS n ON n.oid = c.relnamespace
             WHERE n.nspname = 'public' AND c.relrowsecurity
               AND pg_get_userbyid(c.relowner) = 'trpc_runtime';
            IF runtime_can_login IS DISTINCT FROM TRUE
               OR runtime_is_superuser IS DISTINCT FROM FALSE
               OR runtime_bypasses_rls IS DISTINCT FROM FALSE THEN
                RAISE EXCEPTION
                    'trpc_runtime must be LOGIN NOSUPERUSER NOBYPASSRLS before migration 0015';
            END IF;
            IF runtime_owned_rls_tables <> 0 THEN
                RAISE EXCEPTION
                    'trpc_runtime must not own RLS tables before migration 0015';
            END IF;
        END
        $roles$;
        """
    )


def downgrade() -> None:
    """Role validation has no reversible database state."""
