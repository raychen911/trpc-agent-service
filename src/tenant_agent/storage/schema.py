"""Portable control/data-plane schema with tenant IDs on every owned row."""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

metadata = MetaData()

tenants = Table(
    "tenants",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("display_name", String(255), nullable=False),
    Column("status", String(32), nullable=False),
    Column("active_config_revision", Integer),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

tenant_config_versions = Table(
    "tenant_config_versions",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("revision", Integer, primary_key=True),
    Column("status", String(32), nullable=False),
    Column("config_json", JSON, nullable=False),
    Column("checksum_sha256", String(64), nullable=False),
    Column("created_by", String(255), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("activated_at", DateTime(timezone=True)),
    ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"], ondelete="CASCADE"),
    UniqueConstraint("tenant_id", "checksum_sha256", name="uq_config_checksum"),
)

agent_apps = Table(
    "agent_apps",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("app_id", String(63), primary_key=True),
    Column("agent_name", String(63), nullable=False),
    Column("config_revision", Integer, nullable=False),
    Column("config_json", JSON, nullable=False),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"], ondelete="CASCADE"),
)

channel_bindings = Table(
    "channel_bindings",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("binding_id", String(64), primary_key=True),
    Column("channel", String(32), nullable=False),
    Column("app_id", String(63), nullable=False),
    Column("external_account_id", String(255), nullable=False),
    Column("config_revision", Integer, nullable=False),
    Column("credential_refs_json", JSON, nullable=False),
    Column("settings_json", JSON, nullable=False),
    Column("enabled", Boolean, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(["tenant_id", "app_id"], ["agent_apps.tenant_id", "agent_apps.app_id"]),
    UniqueConstraint("channel", "binding_id", name="uq_channel_binding_route"),
    UniqueConstraint("tenant_id", "channel", "external_account_id", name="uq_tenant_channel_account"),
)

identity_links = Table(
    "identity_links",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("channel", String(32), primary_key=True),
    Column("external_user_id_hash", String(64), primary_key=True),
    Column("canonical_user_id", String(80), nullable=False),
    Column("verified_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"], ondelete="CASCADE"),
)

sessions = Table(
    "sessions",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("session_id", String(80), primary_key=True),
    Column("app_id", String(63), nullable=False),
    Column("user_id", String(80), nullable=False),
    Column("channel", String(32), nullable=False),
    Column("state_json", JSON, nullable=False),
    Column("revision", BigInteger, nullable=False, default=0),
    Column("last_event_sequence", BigInteger, nullable=False, default=0),
    Column("summary_version", Integer, nullable=False, default=0),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index("ix_sessions_tenant_user_updated", sessions.c.tenant_id, sessions.c.user_id, sessions.c.updated_at)

session_events = Table(
    "session_events",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("event_id", String(80), primary_key=True),
    Column("session_id", String(80), nullable=False),
    Column("sequence", BigInteger, nullable=False),
    Column("kind", String(64), nullable=False),
    Column("actor_id", String(128), nullable=False),
    Column("payload_json", JSON, nullable=False),
    Column("state_delta_json", JSON, nullable=False),
    Column("trace_id", String(32), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    ForeignKeyConstraint(
        ["tenant_id", "session_id"],
        ["sessions.tenant_id", "sessions.session_id"],
        ondelete="CASCADE",
    ),
    UniqueConstraint("tenant_id", "session_id", "sequence", name="uq_session_event_sequence"),
)
Index(
    "ix_events_tenant_session_sequence",
    session_events.c.tenant_id,
    session_events.c.session_id,
    session_events.c.sequence,
)

memories = Table(
    "memories",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("user_id", String(80), primary_key=True),
    Column("memory_id", String(80), primary_key=True),
    Column("content", Text, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("revision", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index("ix_memory_tenant_user_updated", memories.c.tenant_id, memories.c.user_id, memories.c.updated_at)

summaries = Table(
    "summaries",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("session_id", String(80), primary_key=True),
    Column("version", Integer, nullable=False),
    Column("through_event_sequence", BigInteger, nullable=False),
    Column("content", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

artifacts = Table(
    "artifacts",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("artifact_id", String(80), primary_key=True),
    Column("session_id", String(80), nullable=False),
    Column("filename", String(512), nullable=False),
    Column("content_type", String(255), nullable=False),
    Column("size_bytes", BigInteger, nullable=False),
    Column("checksum_sha256", String(64), nullable=False),
    Column("storage_uri", String(2048), nullable=False),
    Column("version", Integer, nullable=False),
    Column("content_blob", LargeBinary),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

knowledge_chunks = Table(
    "knowledge_chunks",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("document_id", String(255), primary_key=True),
    Column("chunk_id", String(255), primary_key=True),
    Column("text", Text, nullable=False),
    Column("embedding_json", JSON, nullable=False),
    Column("metadata_json", JSON, nullable=False),
    Column("embedding_model", String(255)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

audit_logs = Table(
    "audit_logs",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("audit_id", String(80), primary_key=True),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("channel", String(32), nullable=False),
    Column("user_id", String(80), nullable=False),
    Column("session_id", String(80), nullable=False),
    Column("agent_name", String(63), nullable=False),
    Column("tool_name", String(255)),
    Column("decision", String(64), nullable=False),
    Column("latency_ms", Float, nullable=False),
    Column("error_type", String(128)),
    Column("cost_usd", Float, nullable=False),
    Column("token_input", BigInteger, nullable=False),
    Column("token_output", BigInteger, nullable=False),
    Column("trace_id", String(32), nullable=False),
    Column("message_id", String(255)),
    Column("details_json", JSON, nullable=False),
)
Index("ix_audit_tenant_occurred", audit_logs.c.tenant_id, audit_logs.c.occurred_at)
Index("ix_audit_trace", audit_logs.c.trace_id)

inbound_receipts = Table(
    "inbound_receipts",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("dedupe_key", String(64), primary_key=True),
    Column("status", String(32), nullable=False),
    Column("owner", String(128), nullable=False),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
    Column("response_json", JSON, nullable=False),
    Column("error_type", String(128)),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_receipts_status_updated",
    inbound_receipts.c.status,
    inbound_receipts.c.updated_at,
)

tenant_usage = Table(
    "tenant_usage",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("period", String(7), primary_key=True),
    Column("input_tokens", BigInteger, nullable=False, default=0),
    Column("output_tokens", BigInteger, nullable=False, default=0),
    Column("cost_usd", Float, nullable=False, default=0.0),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

usage_reservations = Table(
    "usage_reservations",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("reservation_id", String(80), primary_key=True),
    Column("period", String(7), nullable=False),
    Column("reserved_tokens", BigInteger, nullable=False),
    Column("reserved_cost_usd", Float, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_usage_reservations_period_expiry",
    usage_reservations.c.tenant_id,
    usage_reservations.c.period,
    usage_reservations.c.expires_at,
)

tenant_concurrency_slots = Table(
    "tenant_concurrency_slots",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("owner", String(128), primary_key=True),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
)
Index(
    "ix_tenant_concurrency_expiry",
    tenant_concurrency_slots.c.tenant_id,
    tenant_concurrency_slots.c.lease_expires_at,
)

outbox = Table(
    "outbox",
    metadata,
    Column("outbox_id", String(80), primary_key=True),
    Column("tenant_id", String(63), nullable=False),
    Column("kind", String(64), nullable=False),
    Column("payload_json", JSON, nullable=False),
    Column("status", String(32), nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("available_at", DateTime(timezone=True), nullable=False),
    Column("owner", String(128)),
    Column("last_error_type", String(128)),
    Column("lease_expires_at", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
Index("ix_outbox_ready", outbox.c.status, outbox.c.available_at)
Index("ix_outbox_status_updated", outbox.c.status, outbox.c.updated_at)

session_leases = Table(
    "session_leases",
    metadata,
    Column("tenant_id", String(63), primary_key=True),
    Column("session_id", String(80), primary_key=True),
    Column("owner", String(128), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("fencing_token", BigInteger, nullable=False, default=1),
)
