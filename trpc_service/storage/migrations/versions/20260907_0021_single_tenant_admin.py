"""Collapse tenant management roles into one administrator account.

Revision ID: 20260907_0021
Revises: 20260907_0020
Create Date: 2026-09-07
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260907_0021"
down_revision: str | None = "20260907_0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Canonicalize legacy grants and enforce one administrator per tenant."""

    # Never promote an arbitrary delegated role. A legacy tenant is migrated only
    # when exactly one active human owner/admin already has a password credential.
    # Ambiguous installations stop here so an operator can make the ownership
    # decision explicitly instead of the migration deleting or elevating accounts.
    bind = op.get_bind()
    ambiguous_tenants = bind.execute(
        sa.text("""
        WITH eligible AS (
            SELECT assignment.tenant_id, assignment.management_principal_id
            FROM management_role_assignment AS assignment
            JOIN management_principal AS principal
              ON principal.management_principal_id = assignment.management_principal_id
            JOIN management_password_credential AS password
              ON password.management_principal_id = assignment.management_principal_id
            WHERE assignment.tenant_id IS NOT NULL
              AND assignment.role IN ('tenant_owner', 'tenant_admin')
              AND principal.principal_type = 'human'
              AND principal.status = 'active'
              AND NOT EXISTS (
                  SELECT 1
                  FROM management_role_assignment AS platform_assignment
                  WHERE platform_assignment.management_principal_id =
                        assignment.management_principal_id
                    AND platform_assignment.tenant_id IS NULL
                    AND platform_assignment.role = 'platform_admin'
              )
        ), assigned_tenant AS (
            SELECT DISTINCT tenant_id
            FROM management_role_assignment
            WHERE tenant_id IS NOT NULL
        )
        SELECT assigned_tenant.tenant_id
        FROM assigned_tenant
        LEFT JOIN eligible ON eligible.tenant_id = assigned_tenant.tenant_id
        GROUP BY assigned_tenant.tenant_id
        HAVING COUNT(DISTINCT eligible.management_principal_id) <> 1
        ORDER BY assigned_tenant.tenant_id
    """)).scalars().all()
    if ambiguous_tenants:
        values = ", ".join(str(item) for item in ambiguous_tenants)
        raise RuntimeError("single tenant administrator migration requires exactly one active "
                           f"human owner/admin with a password for tenant(s): {values}")

    reused_principals = bind.execute(
        sa.text("""
        SELECT assignment.management_principal_id
        FROM management_role_assignment AS assignment
        JOIN management_principal AS principal
          ON principal.management_principal_id = assignment.management_principal_id
        JOIN management_password_credential AS password
          ON password.management_principal_id = assignment.management_principal_id
        WHERE assignment.tenant_id IS NOT NULL
          AND assignment.role IN ('tenant_owner', 'tenant_admin')
          AND principal.principal_type = 'human'
          AND principal.status = 'active'
          AND NOT EXISTS (
              SELECT 1
              FROM management_role_assignment AS platform_assignment
              WHERE platform_assignment.management_principal_id =
                    assignment.management_principal_id
                AND platform_assignment.tenant_id IS NULL
                AND platform_assignment.role = 'platform_admin'
          )
        GROUP BY assignment.management_principal_id
        HAVING COUNT(DISTINCT assignment.tenant_id) > 1
        ORDER BY assignment.management_principal_id
    """)).scalars().all()
    if reused_principals:
        values = ", ".join(str(item) for item in reused_principals)
        raise RuntimeError("one tenant administrator identity cannot manage multiple tenants: " +
                           values)

    # Remove delegated grants and duplicate legacy owner/admin rows only after the
    # checks above establish the exact login identity retained for each tenant.
    op.execute(
        sa.text("""
        WITH ranked_eligible AS (
            SELECT assignment.role_assignment_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY assignment.tenant_id
                       ORDER BY assignment.created_at, assignment.role_assignment_id
                   ) AS position
            FROM management_role_assignment AS assignment
            JOIN management_principal AS principal
              ON principal.management_principal_id = assignment.management_principal_id
            JOIN management_password_credential AS password
              ON password.management_principal_id = assignment.management_principal_id
            WHERE assignment.tenant_id IS NOT NULL
              AND assignment.role IN ('tenant_owner', 'tenant_admin')
              AND principal.principal_type = 'human'
              AND principal.status = 'active'
              AND NOT EXISTS (
                  SELECT 1
                  FROM management_role_assignment AS platform_assignment
                  WHERE platform_assignment.management_principal_id =
                        assignment.management_principal_id
                    AND platform_assignment.tenant_id IS NULL
                    AND platform_assignment.role = 'platform_admin'
              )
        ), retained AS (
            SELECT role_assignment_id
            FROM ranked_eligible
            WHERE position = 1
        )
        DELETE FROM management_role_assignment AS assignment
        WHERE assignment.tenant_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM retained
              WHERE retained.role_assignment_id = assignment.role_assignment_id
          )
    """))
    op.execute(
        sa.text("""
        UPDATE management_role_assignment
        SET role = 'tenant_admin'
        WHERE tenant_id IS NOT NULL
    """))
    subject_conflicts = bind.execute(
        sa.text("""
        SELECT principal.management_principal_id
        FROM management_role_assignment AS assignment
        JOIN management_principal AS principal
          ON principal.management_principal_id = assignment.management_principal_id
        JOIN management_password_credential AS password
          ON password.management_principal_id = assignment.management_principal_id
        JOIN management_principal AS conflicting
          ON conflicting.external_subject = 'tenant-console:' || password.username
         AND conflicting.management_principal_id <> principal.management_principal_id
        WHERE assignment.role = 'tenant_admin'
        ORDER BY principal.management_principal_id
    """)).scalars().all()
    if subject_conflicts:
        values = ", ".join(str(item) for item in subject_conflicts)
        raise RuntimeError("tenant administrator login subject conflicts with another principal: " +
                           values)
    op.execute(
        sa.text("""
        UPDATE management_principal AS principal
        SET external_subject = 'tenant-console:' || password.username
        FROM management_role_assignment AS assignment
        JOIN management_password_credential AS password
          ON password.management_principal_id = assignment.management_principal_id
        WHERE assignment.management_principal_id = principal.management_principal_id
          AND assignment.role = 'tenant_admin'
    """))
    with op.batch_alter_table("management_role_assignment") as batch:
        batch.drop_constraint("management_role_scope", type_="check")
        batch.drop_constraint("management_role_value", type_="check")
        batch.create_check_constraint(
            "management_role_scope",
            "(role = 'platform_admin' AND tenant_id IS NULL) OR "
            "(role = 'tenant_admin' AND tenant_id IS NOT NULL)",
        )
        batch.create_check_constraint(
            "management_role_value",
            "role IN ('platform_admin', 'tenant_admin')",
        )
    # Revision 0020 used a constraint name that PostgreSQL had to truncate.
    # Normalize it while this migration is already reconciling control-plane
    # constraints so Alembic drift checks remain deterministic.
    with op.batch_alter_table("management_password_credential") as batch:
        batch.drop_constraint("management_password_failed_nonnegative", type_="check")
        batch.create_check_constraint("password_failed_nonnegative", "failed_attempts >= 0")
    op.create_index(
        "uq_management_role_assignment_tenant_admin",
        "management_role_assignment",
        ["tenant_id"],
        unique=True,
        postgresql_where=sa.text("tenant_id IS NOT NULL"),
        sqlite_where=sa.text("tenant_id IS NOT NULL"),
    )
    op.create_index(
        "uq_management_role_assignment_tenant_principal",
        "management_role_assignment",
        ["management_principal_id"],
        unique=True,
        postgresql_where=sa.text("tenant_id IS NOT NULL"),
        sqlite_where=sa.text("tenant_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Restore the former role vocabulary without recreating removed grants."""

    op.drop_index(
        "uq_management_role_assignment_tenant_principal",
        table_name="management_role_assignment",
    )
    op.drop_index(
        "uq_management_role_assignment_tenant_admin",
        table_name="management_role_assignment",
    )
    with op.batch_alter_table("management_role_assignment") as batch:
        batch.drop_constraint("management_role_scope", type_="check")
        batch.drop_constraint("management_role_value", type_="check")
        batch.create_check_constraint(
            "management_role_scope",
            "(role = 'platform_admin' AND tenant_id IS NULL) OR "
            "(role <> 'platform_admin' AND tenant_id IS NOT NULL)",
        )
        batch.create_check_constraint(
            "management_role_value",
            "role IN ('platform_admin', 'tenant_owner', 'tenant_admin', "
            "'agent_manager', 'channel_manager', 'auditor', 'viewer')",
        )
    with op.batch_alter_table("management_password_credential") as batch:
        batch.drop_constraint("password_failed_nonnegative", type_="check")
        batch.create_check_constraint(
            "management_password_failed_nonnegative",
            "failed_attempts >= 0",
        )
