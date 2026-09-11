"""add tenant-scoped request usage facts for the unified audit read model."""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0011_request_usage_audit"
down_revision: Union[str, None] = "0010_channel_binding_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "request_usage_records",
        sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
        sa.Column("request_id", sa.UUID, primary_key=True),
        sa.Column("receipt_id", sa.UUID, nullable=True),
        sa.Column("config_version", sa.BIGINT, nullable=False),
        sa.Column("model_profile", sa.TEXT, nullable=False),
        sa.Column("input_tokens", sa.BIGINT, nullable=True),
        sa.Column("output_tokens", sa.BIGINT, nullable=True),
        sa.Column("cost_microunits", sa.BIGINT, nullable=True),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["receipt_id", "tenant_id", "request_id", "config_version"],
                                ["message_receipts.receipt_id", "message_receipts.tenant_id",
                                 "message_receipts.request_id", "message_receipts.config_version"],
                                ondelete="RESTRICT", name="request_usage_records_receipt_identity_fk"),
        sa.CheckConstraint("config_version >= 1", name="request_usage_records_config_version_positive"),
        sa.CheckConstraint("btrim(model_profile) <> ''", name="request_usage_records_profile_not_blank"),
    )
    op.create_index("request_usage_records_tenant_time_idx", "request_usage_records", ["tenant_id", "occurred_at"])


def downgrade() -> None:
    op.drop_index("request_usage_records_tenant_time_idx", table_name="request_usage_records")
    op.drop_table("request_usage_records")
