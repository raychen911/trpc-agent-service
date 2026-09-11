"""add optional WeCom webhook secret references."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0014_wecom_webhook_refs"
down_revision: Union[str, None] = "0013_channel_account_case"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLUMNS = (
    ("webhook_token_ref", "webhook_token_ref IS NULL OR webhook_token_ref ~ '^env:TRPC_[A-Z0-9_]+$'"),
    ("webhook_aes_key_ref", "webhook_aes_key_ref IS NULL OR webhook_aes_key_ref ~ '^env:TRPC_[A-Z0-9_]+$'"),
)


def upgrade() -> None:
    for table in ("channel_bindings", "channel_binding_versions"):
        for column, check in _COLUMNS:
            op.add_column(table, sa.Column(column, sa.TEXT(), nullable=True))
            op.create_check_constraint(f"{table}_{column}_valid", table, check)


def downgrade() -> None:
    for table in ("channel_bindings", "channel_binding_versions"):
        for column, _ in reversed(_COLUMNS):
            op.drop_constraint(f"{table}_{column}_valid", table, type_="check")
            op.drop_column(table, column)
