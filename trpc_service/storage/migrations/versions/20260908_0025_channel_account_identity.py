"""Normalize provider account identity independently of presentation settings.

Revision ID: 20260908_0025
Revises: 20260908_0024
Create Date: 2026-09-08
"""

from collections.abc import Sequence
import hashlib
import json
from typing import Any

from alembic import op
import sqlalchemy as sa

revision: str = "20260908_0025"
down_revision: str | None = "20260908_0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACCOUNT_IDENTITY_FIELDS = {
    "wecom": ("bot_id", ),
    "feishu": ("app_id", ),
}


def _identity_hash(channel_type: str, account_config: dict[str, Any]) -> str:
    """Match the service's provider identity hash during data normalization."""

    fields = _ACCOUNT_IDENTITY_FIELDS.get(channel_type)
    identity = ({
        field: account_config.get(field)
        for field in fields
    } if fields is not None else account_config)
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{channel_type}:{canonical}".encode()).hexdigest()


def upgrade() -> None:
    """Keep only the newest duplicate active, then enforce active uniqueness."""

    op.drop_constraint(
        "uq_channel_binding_external_account",
        "channel_binding",
        type_="unique",
    )
    connection = op.get_bind()
    rows = connection.execute(
        sa.text("SELECT binding_id, channel_type, account_config, status, created_at "
                "FROM channel_binding ORDER BY created_at DESC, binding_id DESC")).mappings()
    normalized: list[tuple[object, str, str]] = []
    active_owners: set[tuple[str, str]] = set()
    for row in rows:
        channel_type = str(row["channel_type"])
        account_hash = _identity_hash(channel_type, dict(row["account_config"] or {}))
        status = str(row["status"])
        identity = (channel_type, account_hash)
        if status == "active" and identity in active_owners:
            status = "disabled"
        elif status == "active":
            active_owners.add(identity)
        normalized.append((row["binding_id"], account_hash, status))
    statement = sa.text("UPDATE channel_binding SET external_account_hash = :account_hash, "
                        "status = :status WHERE binding_id = :binding_id")
    for binding_id, account_hash, status in normalized:
        connection.execute(statement, {
            "binding_id": binding_id,
            "account_hash": account_hash,
            "status": status,
        })
    op.create_index(
        "uq_channel_binding_active_external_account",
        "channel_binding",
        ["channel_type", "external_account_hash"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )


def downgrade() -> None:
    """Return to uniqueness across both active and disabled bindings."""

    # Disabled historical duplicates cannot coexist under the former contract.
    # Preserve every row while giving older disabled records archival hashes.
    op.drop_index(
        "uq_channel_binding_active_external_account",
        table_name="channel_binding",
    )
    op.execute("""
        WITH ranked AS (
            SELECT binding_id,
                   row_number() OVER (
                       PARTITION BY channel_type, external_account_hash
                       ORDER BY created_at DESC, binding_id DESC
                   ) AS position
            FROM channel_binding
        )
        UPDATE channel_binding AS binding
        SET external_account_hash =
            left(binding.external_account_hash, 80) || ':' || binding.binding_id::text
        FROM ranked
        WHERE binding.binding_id = ranked.binding_id
          AND ranked.position > 1
        """)
    op.create_unique_constraint(
        "uq_channel_binding_external_account",
        "channel_binding",
        ["channel_type", "external_account_hash"],
    )
