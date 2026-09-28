"""Tighten channel identity and Runner attempt constraints.

Revision ID: 20260903_0017
Revises: 20260903_0016
Create Date: 2026-09-03
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260903_0017"
down_revision: str | None = "20260903_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Reject invalid identity states and orphan Runner attempts."""

    op.create_check_constraint(
        "channel_principal_type",
        "channel_principal",
        "principal_type IN ('USER', 'SERVICE', 'BOT')",
    )
    op.create_check_constraint(
        "channel_identity_status",
        "channel_identity",
        "status IN ('ACTIVE', 'DISABLED', 'DELETED')",
    )
    op.create_check_constraint(
        "conversation_member_role",
        "conversation_member",
        "role IN ('MEMBER', 'OWNER', 'ADMIN', 'BOT')",
    )
    op.create_check_constraint(
        "conversation_member_status",
        "conversation_member",
        "status IN ('ACTIVE', 'LEFT', 'REMOVED')",
    )
    op.create_foreign_key(
        "fk_runner_attempt_task",
        "runner_attempt",
        "agent_task",
        ["task_id"],
        ["task_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Restore the preceding permissive development schema."""

    op.drop_constraint("fk_runner_attempt_task", "runner_attempt", type_="foreignkey")
    op.drop_constraint("conversation_member_status", "conversation_member", type_="check")
    op.drop_constraint("conversation_member_role", "conversation_member", type_="check")
    op.drop_constraint("channel_identity_status", "channel_identity", type_="check")
    op.drop_constraint("channel_principal_type", "channel_principal", type_="check")
