"""Persist inline knowledge text and grant its authenticated control-plane path."""

from __future__ import annotations

from alembic import op

revision = "0013_knowledge_projection"
down_revision = "0012_audit_default_grants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing object-backed rows are retained as empty-content metadata. New
    # control-plane documents always require text before a projection is queued.
    op.execute(
        "ALTER TABLE knowledge_document ADD COLUMN IF NOT EXISTS content text NOT NULL DEFAULT ''"
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'knowledge_document_content_size_ck'
              AND conrelid = 'public.knowledge_document'::regclass
          ) THEN
            ALTER TABLE knowledge_document
              ADD CONSTRAINT knowledge_document_content_size_ck
              CHECK (char_length(content) <= 200000);
          END IF;
        END $$
        """
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON knowledge_document TO agent_admin")


def downgrade() -> None:
    op.execute("REVOKE SELECT, INSERT, UPDATE ON knowledge_document FROM agent_admin")
    op.execute(
        "ALTER TABLE knowledge_document DROP CONSTRAINT IF EXISTS knowledge_document_content_size_ck"
    )
    op.execute("ALTER TABLE knowledge_document DROP COLUMN IF EXISTS content")
