"""Add tenant-owned RAG metadata and pgvector index.

Revision ID: 20260902_0015
Revises: 20260901_0014
Create Date: 2026-09-02
"""

from collections.abc import Sequence

from alembic import op
from pgvector.sqlalchemy import VECTOR
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260902_0015"
down_revision: str | None = "20260901_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def upgrade() -> None:
    """Create isolated Knowledge, Artifact, chunk metadata, and vector tables."""

    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "knowledge_base",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("knowledge_base_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("embedding_model", sa.String(length=120), nullable=False),
        sa.Column("embedding_dimensions", sa.Integer(), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name="fk_knowledge_base_tenant_id_tenant",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "knowledge_base_id",
            name="pk_knowledge_base",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "name",
            name="uq_knowledge_base_tenant_name",
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'DISABLED', 'DELETED')",
            name="knowledge_base_status",
        ),
    )
    op.create_table(
        "knowledge_artifact",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.String(length=255), nullable=False),
        sa.Column("uploaded_by_agent_id", sa.Uuid(), nullable=False),
        sa.Column("uploaded_by_principal_id", sa.String(length=255), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("media_type", sa.String(length=160), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("object_uri", sa.String(length=1000), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name="fk_knowledge_artifact_tenant_id_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "uploaded_by_agent_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_knowledge_artifact_tenant_id_agent_app",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "artifact_id", name="pk_knowledge_artifact"),
        sa.UniqueConstraint(
            "tenant_id",
            "checksum",
            name="uq_knowledge_artifact_checksum",
        ),
        sa.CheckConstraint(
            "size_bytes >= 0",
            name="knowledge_artifact_size_nonnegative",
        ),
    )
    op.create_table(
        "knowledge_source_document",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("document_id", sa.Uuid(), nullable=False),
        sa.Column("knowledge_base_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.String(length=255), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("chunk_count", sa.Integer(), nullable=False),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "knowledge_base_id"],
            ["knowledge_base.tenant_id", "knowledge_base.knowledge_base_id"],
            name="fk_knowledge_source_document_tenant_id_knowledge_base",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "artifact_id"],
            ["knowledge_artifact.tenant_id", "knowledge_artifact.artifact_id"],
            name="fk_knowledge_source_document_tenant_id_knowledge_artifact",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "document_id",
            name="pk_knowledge_source_document",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "checksum",
            name="uq_knowledge_document_base_checksum",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "filename",
            "version",
            name="uq_knowledge_document_filename_version",
        ),
        sa.CheckConstraint(
            "version > 0",
            name="doc_version_positive",
        ),
        sa.CheckConstraint(
            "chunk_count >= 0",
            name="doc_chunks_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('INGESTING', 'READY', 'FAILED', 'SUPERSEDED', 'DELETED')",
            name="doc_status",
        ),
    )
    op.create_index(
        "ix_knowledge_document_scope_status",
        "knowledge_source_document",
        ["tenant_id", "knowledge_base_id", "status"],
    )
    op.create_table(
        "knowledge_chunk",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("chunk_id", sa.String(length=255), nullable=False),
        sa.Column("document_id", sa.Uuid(), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("start_char", sa.Integer(), nullable=False),
        sa.Column("end_char", sa.Integer(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.ForeignKeyConstraint(
            ["tenant_id", "document_id"],
            ["knowledge_source_document.tenant_id", "knowledge_source_document.document_id"],
            name="fk_knowledge_chunk_tenant_id_knowledge_source_document",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "chunk_id", name="pk_knowledge_chunk"),
        sa.UniqueConstraint(
            "tenant_id",
            "document_id",
            "chunk_index",
            name="uq_knowledge_chunk_document_index",
        ),
        sa.CheckConstraint(
            "chunk_index >= 0",
            name="knowledge_chunk_index_nonnegative",
        ),
        sa.CheckConstraint(
            "start_char >= 0 AND end_char >= start_char",
            name="knowledge_chunk_offsets",
        ),
    )
    op.create_table(
        "knowledge_chunk_vector",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("document_id", sa.String(length=255), nullable=False),
        sa.Column("knowledge_base_id", sa.String(length=255), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("embedding", VECTOR(dim=1024), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "document_id",
            name="pk_knowledge_chunk_vector",
        ),
    )
    op.create_index(
        "ix_knowledge_chunk_vector_scope_base",
        "knowledge_chunk_vector",
        ["tenant_id", "knowledge_base_id"],
    )
    op.create_index(
        "ix_knowledge_chunk_vector_embedding_hnsw",
        "knowledge_chunk_vector",
        ["embedding"],
        postgresql_using="hnsw",
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )


def downgrade() -> None:
    """Remove tenant knowledge data structures in dependency order."""

    op.drop_index(
        "ix_knowledge_chunk_vector_embedding_hnsw",
        table_name="knowledge_chunk_vector",
        postgresql_using="hnsw",
    )
    op.drop_index("ix_knowledge_chunk_vector_scope_base", table_name="knowledge_chunk_vector")
    op.drop_table("knowledge_chunk_vector")
    op.drop_table("knowledge_chunk")
    op.drop_index("ix_knowledge_document_scope_status", table_name="knowledge_source_document")
    op.drop_table("knowledge_source_document")
    op.drop_table("knowledge_artifact")
    op.drop_table("knowledge_base")
