"""Add channel identities, Tool Ledger, Runner attempts and Feishu catalog.

Revision ID: 20260903_0016
Revises: 20260902_0015
Create Date: 2026-09-03
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260903_0016"
down_revision: str | None = "20260902_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    return (
        sa.Column("created_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
    )


def upgrade() -> None:
    """Create durable identity, conversation and execution recovery facts."""

    op.add_column(
        "agent_task",
        sa.Column("lease_token", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.alter_column("agent_task", "lease_token", server_default=None)

    op.create_table(
        "channel_principal",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("principal_type", sa.String(length=30), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"],
                                name="fk_channel_principal_tenant_id_tenant",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "principal_id", name="pk_channel_principal"),
        sa.UniqueConstraint("tenant_id", "principal_id", name="uq_channel_principal_tenant_id"),
        sa.CheckConstraint("status IN ('ACTIVE', 'DISABLED', 'DELETED')",
                           name="channel_principal_status"),
    )
    op.create_table(
        "channel_identity",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("identity_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("provider_principal_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(["tenant_id", "binding_id"],
                                ["channel_binding.tenant_id", "channel_binding.binding_id"],
                                name="fk_channel_identity_tenant_binding",
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id", "principal_id"],
                                ["channel_principal.tenant_id", "channel_principal.principal_id"],
                                name="fk_channel_identity_tenant_principal",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "identity_id", name="pk_channel_identity"),
        sa.UniqueConstraint("tenant_id",
                            "binding_id",
                            "provider_principal_id",
                            name="uq_channel_identity_provider"),
    )
    op.create_index("ix_channel_identity_principal", "channel_identity",
                    ["tenant_id", "principal_id"])
    op.create_table(
        "channel_conversation",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("provider_conversation_id", sa.String(length=255), nullable=False),
        sa.Column("kind", sa.String(length=20), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("last_message_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(["tenant_id", "binding_id"],
                                ["channel_binding.tenant_id", "channel_binding.binding_id"],
                                name="fk_channel_conversation_tenant_binding",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "conversation_id", name="pk_channel_conversation"),
        sa.UniqueConstraint("tenant_id",
                            "binding_id",
                            "provider_conversation_id",
                            name="uq_channel_conversation_provider"),
        sa.CheckConstraint("kind IN ('DIRECT', 'GROUP', 'THREAD')",
                           name="channel_conversation_kind"),
        sa.CheckConstraint("status IN ('ACTIVE', 'CLOSED', 'DELETED')",
                           name="channel_conversation_status"),
    )
    op.create_table(
        "conversation_member",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "conversation_id"],
            ["channel_conversation.tenant_id", "channel_conversation.conversation_id"],
            name="fk_conversation_member_tenant_conversation",
            ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id", "principal_id"],
                                ["channel_principal.tenant_id", "channel_principal.principal_id"],
                                name="fk_conversation_member_tenant_principal",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id",
                                "conversation_id",
                                "principal_id",
                                name="pk_conversation_member"),
    )

    op.create_table(
        "tool_call_ledger",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("call_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("logical_call_index", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("kind", sa.String(length=30), nullable=False),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("resource", sa.String(length=1000), nullable=True),
        sa.Column("intent_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("result_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.ForeignKeyConstraint(["tenant_id", "agent_app_id"],
                                ["agent_app.tenant_id", "agent_app.agent_app_id"],
                                name="fk_tool_call_ledger_tenant_agent",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "agent_app_id", "call_id", name="pk_tool_call_ledger"),
        sa.UniqueConstraint("tenant_id",
                            "agent_app_id",
                            "request_id",
                            "logical_call_index",
                            name="uq_tool_call_ledger_position"),
        sa.CheckConstraint("logical_call_index >= 0", name="tool_call_ledger_index_nonnegative"),
        sa.CheckConstraint("status IN ('PREPARED', 'SUCCEEDED', 'FAILED', 'UNKNOWN')",
                           name="tool_call_ledger_status"),
    )
    op.create_index("ix_tool_call_ledger_request", "tool_call_ledger", ["tenant_id", "request_id"])

    op.create_table(
        "runner_attempt",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.String(length=320), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column("node_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("started_at",
                  sa.DateTime(timezone=True),
                  nullable=False,
                  server_default=sa.func.now()),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id", "agent_app_id"],
                                ["agent_app.tenant_id", "agent_app.agent_app_id"],
                                name="fk_runner_attempt_tenant_agent",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "attempt_id", name="pk_runner_attempt"),
        sa.UniqueConstraint("tenant_id", "task_id", "attempt_no", name="uq_runner_attempt_number"),
        sa.CheckConstraint("attempt_no > 0", name="runner_attempt_number_positive"),
        sa.CheckConstraint("fencing_token > 0", name="runner_attempt_fence_positive"),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'SUCCEEDED', 'RETRYABLE_FAILED', "
            "'PERMANENT_FAILED', 'UNKNOWN')",
            name="runner_attempt_status"),
    )
    op.create_index("ix_runner_attempt_task_fence", "runner_attempt",
                    ["tenant_id", "task_id", "fencing_token"])
    op.create_table(
        "runner_checkpoint",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.String(length=320), nullable=False),
        sa.Column("sequence_no", sa.BigInteger(), nullable=False),
        sa.Column("stage", sa.String(length=100), nullable=False),
        sa.Column("state_ref", sa.String(length=1000), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id", "attempt_id"],
                                ["runner_attempt.tenant_id", "runner_attempt.attempt_id"],
                                name="fk_runner_checkpoint_attempt",
                                ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id",
                                "attempt_id",
                                "sequence_no",
                                name="pk_runner_checkpoint"),
        sa.CheckConstraint("sequence_no > 0", name="runner_checkpoint_sequence_positive"),
    )

    # Pass JSON through bound values. Embedding ``:true``-like fragments in a
    # SQLAlchemy text literal makes the colon look like a named bind parameter.
    op.get_bind().execute(
        sa.text("INSERT INTO channel_adapter_type "
                "(channel_type, display_name, adapter_version, config_schema, secret_schema, "
                "capabilities, status) VALUES "
                "(:channel_type, :display_name, :adapter_version, "
                "CAST(:config_schema AS jsonb), CAST(:secret_schema AS jsonb), "
                "CAST(:capabilities AS jsonb), :status) "
                "ON CONFLICT (channel_type) DO NOTHING"),
        {
            "channel_type": "feishu",
            "display_name": "Feishu/Lark",
            "adapter_version": "1.0",
            "config_schema": '{"type":"object","required":["app_id"]}',
            "secret_schema": '{"required":["app_secret"]}',
            "capabilities": '{"long_connection":true,"streaming":true,"media":true}',
            "status": "active",
        },
    )


def downgrade() -> None:
    """Remove reliability extensions in reverse dependency order."""

    op.execute(sa.text("DELETE FROM channel_adapter_type WHERE channel_type = 'feishu'"))
    op.drop_table("runner_checkpoint")
    op.drop_index("ix_runner_attempt_task_fence", table_name="runner_attempt")
    op.drop_table("runner_attempt")
    op.drop_index("ix_tool_call_ledger_request", table_name="tool_call_ledger")
    op.drop_table("tool_call_ledger")
    op.drop_table("conversation_member")
    op.drop_table("channel_conversation")
    op.drop_index("ix_channel_identity_principal", table_name="channel_identity")
    op.drop_table("channel_identity")
    op.drop_table("channel_principal")
    op.drop_column("agent_task", "lease_token")
