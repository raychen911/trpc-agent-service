"""add message_receipts and message_audit_events tables

Revision ID: 0003_add_message_receipts_audit
Revises: 0002_add_tenant_config_versions
Create Date: 2026-09-02

Creates the message idempotency receipt table and the associated audit event
table. Both tables are created in a single transaction; an interrupted upgrade
can never leave the audit table without its receipt parent.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003_add_message_receipts_audit"
down_revision: Union[str, None] = "0002_add_tenant_config_versions"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "message_receipts",
        sa.Column("receipt_id", sa.UUID, primary_key=True),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("channel", sa.TEXT, nullable=False),
        sa.Column("user_id", sa.TEXT, nullable=False),
        sa.Column("session_id", sa.TEXT, nullable=False),
        sa.Column("message_id", sa.TEXT, nullable=False),
        sa.Column("app_id", sa.TEXT, nullable=False),
        sa.Column("config_version", sa.BIGINT, nullable=False),
        sa.Column("request_id", sa.UUID, nullable=False),
        sa.Column("message_digest", sa.CHAR(64), nullable=False),
        sa.Column("state", sa.TEXT, nullable=False),
        sa.Column("response_text", sa.TEXT, nullable=True),
        sa.Column("error_code", sa.TEXT, nullable=True),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.BIGINT, nullable=True),
        sa.UniqueConstraint(
            "tenant_id",
            "channel",
            "user_id",
            "session_id",
            "message_id",
            name="message_receipts_business_key",
        ),
        sa.CheckConstraint(
            "state IN ('processing', 'completed', 'failed')",
            name="message_receipts_state_valid",
        ),
        sa.CheckConstraint(
            "config_version >= 1",
            name="message_receipts_config_version_positive",
        ),
        sa.CheckConstraint(
            "message_digest ~ '^[0-9a-f]{64}$'",
            name="message_receipts_digest_format",
        ),
        sa.CheckConstraint(
            "(state = 'completed' AND response_text IS NOT NULL) OR (state != 'completed')",
            name="message_receipts_completed_has_response",
        ),
        sa.CheckConstraint(
            "(state = 'failed' AND error_code IS NOT NULL) OR (state != 'failed')",
            name="message_receipts_failed_has_error",
        ),
        sa.CheckConstraint(
            "(state = 'processing' AND response_text IS NULL AND error_code IS NULL) OR (state != 'processing')",
            name="message_receipts_processing_no_terminal_data",
        ),
    )
    op.create_table(
        "message_audit_events",
        sa.Column("audit_id", sa.UUID, primary_key=True),
        sa.Column("receipt_id", sa.UUID, nullable=False),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("app_id", sa.TEXT, nullable=False),
        sa.Column("channel", sa.TEXT, nullable=False),
        sa.Column("user_id", sa.TEXT, nullable=False),
        sa.Column("session_id", sa.TEXT, nullable=False),
        sa.Column("message_id", sa.TEXT, nullable=False),
        sa.Column("event_type", sa.TEXT, nullable=False),
        sa.Column("request_id", sa.UUID, nullable=False),
        sa.Column("config_version", sa.BIGINT, nullable=False),
        sa.Column("error_code", sa.TEXT, nullable=True),
        sa.Column("latency_ms", sa.BIGINT, nullable=True),
        sa.Column("message_digest", sa.CHAR(64), nullable=False),
        sa.Column("response_digest", sa.CHAR(64), nullable=True),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["receipt_id"],
            ["message_receipts.receipt_id"],
            ondelete="RESTRICT",
            name="message_audit_events_receipt_fk",
        ),
        sa.CheckConstraint(
            "event_type IN ('accepted', 'completed', 'failed')",
            name="message_audit_events_type_valid",
        ),
        sa.CheckConstraint(
            "config_version >= 1",
            name="message_audit_events_config_version_positive",
        ),
        sa.CheckConstraint(
            "message_digest ~ '^[0-9a-f]{64}$'",
            name="message_audit_events_digest_format",
        ),
        sa.CheckConstraint(
            "(response_digest IS NULL OR response_digest ~ '^[0-9a-f]{64}$')",
            name="message_audit_events_response_digest_format",
        ),
    )
    op.create_index(
        "message_audit_events_tenant_message_idx",
        "message_audit_events",
        ["tenant_id", "message_id", "occurred_at"],
    )


def downgrade() -> None:
    op.drop_index("message_audit_events_tenant_message_idx", table_name="message_audit_events")
    op.drop_table("message_audit_events")
    op.drop_table("message_receipts")
