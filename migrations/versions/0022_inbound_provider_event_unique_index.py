"""Build the inbound provider-event uniqueness index without blocking writes.

This migration is deliberately separate from 0021: Alembic's autocommit block
commits before a concurrent index operation, so mixing schema DDL and the index
would leave a partially applied, unversioned migration if index creation failed.

Revision ID: 0022_inbound_provider_event_unique_index
Revises: 0021_im_acceptance_event_correlation
"""

from __future__ import annotations

from alembic import op

revision = "0022_inbound_provider_event_unique_index"
down_revision = "0021_im_acceptance_event_correlation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            DROP INDEX CONCURRENTLY IF EXISTS
                public.ux_inbound_provider_event_hash
            """
        )
        op.execute(
            """
            CREATE UNIQUE INDEX CONCURRENTLY
                ux_inbound_provider_event_hash
                ON public.inbound_messages (
                    tenant_id, binding_id, provider_event_hash
                )
                WHERE provider_event_hash IS NOT NULL
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            DROP INDEX CONCURRENTLY IF EXISTS
                public.ux_inbound_provider_event_hash
            """
        )
