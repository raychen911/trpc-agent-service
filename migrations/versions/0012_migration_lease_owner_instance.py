"""Add a non-reusable process-instance nonce to migration leases."""

from __future__ import annotations

from alembic import op

revision = "0012_migration_lease_owner_instance"
down_revision = "0011_mailbox_recovery_fencing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Alembic creates ``alembic_version.version_num`` as varchar(32), while
    # this revision identifier is 35 characters.  Widen it before Alembic
    # records this revision so a fresh PostgreSQL upgrade can reach head.
    op.execute("ALTER TABLE alembic_version ALTER COLUMN version_num TYPE varchar(64)")
    # Existing rows are backfilled before the column becomes NOT NULL.  The
    # value is deliberately independent of owner_id so a restarted process
    # with the same configured owner cannot reuse an active lease instance.
    op.execute(
        """
        ALTER TABLE migration_leases
            ADD COLUMN owner_instance text;
        UPDATE migration_leases
           SET owner_instance = md5(
               tenant_id || ':' || migration_id || ':' || owner_id || ':' ||
               lease_epoch::text || ':' || clock_timestamp()::text || ':' ||
               random()::text
           )
         WHERE owner_instance IS NULL;
        ALTER TABLE migration_leases
            ALTER COLUMN owner_instance SET NOT NULL,
            ADD CONSTRAINT migration_leases_owner_instance_length
                CHECK (char_length(owner_instance) BETWEEN 1 AND 256);
        CREATE INDEX ix_migration_leases_tenant_instance
            ON migration_leases (tenant_id, owner_instance, lease_epoch);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS ix_migration_leases_tenant_instance;
        ALTER TABLE migration_leases
            DROP CONSTRAINT IF EXISTS migration_leases_owner_instance_length,
            DROP COLUMN IF EXISTS owner_instance;
        """
    )
