"""Tenant-owned knowledge and Artifact metadata persisted in PostgreSQL/SQL."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from pgvector.sqlalchemy import VECTOR

from trpc_service.agent.models import AgentApp  # noqa: F401
from trpc_service.storage.orm import Base, TimestampMixin
from trpc_service.tenant.models import Tenant  # noqa: F401

JSON_VALUE = JSON().with_variant(JSONB(), "postgresql")


class KnowledgeBaseRow(Base, TimestampMixin):
    """A named knowledge corpus owned by exactly one Tenant."""

    __tablename__ = "knowledge_base"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="RESTRICT"),
        UniqueConstraint("tenant_id", "name", name="uq_knowledge_base_tenant_name"),
        CheckConstraint("status IN ('ACTIVE', 'DISABLED', 'DELETED')",
                        name="knowledge_base_status"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    knowledge_base_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    embedding_model: Mapped[str] = mapped_column(
        String(120),
        nullable=False,
        default="text-embedding-v4",
    )
    embedding_dimensions: Mapped[int] = mapped_column(Integer, nullable=False, default=1024)


class KnowledgeArtifactRow(Base, TimestampMixin):
    """Database metadata for a tenant-owned object stored outside SQL."""

    __tablename__ = "knowledge_artifact"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="RESTRICT"),
        ForeignKeyConstraint(
            ["tenant_id", "uploaded_by_agent_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "checksum", name="uq_knowledge_artifact_checksum"),
        CheckConstraint("size_bytes >= 0", name="knowledge_artifact_size_nonnegative"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    uploaded_by_agent_id: Mapped[UUID] = mapped_column(nullable=False)
    uploaded_by_principal_id: Mapped[str] = mapped_column(String(255), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    media_type: Mapped[str] = mapped_column(String(160), nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    object_uri: Mapped[str] = mapped_column(String(1000), nullable=False)


class KnowledgeDocumentRow(Base, TimestampMixin):
    """Versioned ingestion state for one source file in a knowledge base."""

    __tablename__ = "knowledge_source_document"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "knowledge_base_id"],
            ["knowledge_base.tenant_id", "knowledge_base.knowledge_base_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "artifact_id"],
            ["knowledge_artifact.tenant_id", "knowledge_artifact.artifact_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "checksum",
            name="uq_knowledge_document_base_checksum",
        ),
        UniqueConstraint(
            "tenant_id",
            "knowledge_base_id",
            "filename",
            "version",
            name="uq_knowledge_document_filename_version",
        ),
        CheckConstraint("version > 0", name="doc_version_positive"),
        CheckConstraint("chunk_count >= 0", name="doc_chunks_nonnegative"),
        CheckConstraint(
            "status IN ('INGESTING', 'READY', 'FAILED', 'SUPERSEDED', 'DELETED')",
            name="doc_status",
        ),
        Index(
            "ix_knowledge_document_scope_status",
            "tenant_id",
            "knowledge_base_id",
            "status",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    document_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    knowledge_base_id: Mapped[UUID] = mapped_column(nullable=False)
    artifact_id: Mapped[str] = mapped_column(String(255), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="INGESTING")
    chunk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_summary: Mapped[str | None] = mapped_column(Text)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KnowledgeChunkRow(Base, TimestampMixin):
    """Citation and lifecycle metadata for one embedded source range."""

    __tablename__ = "knowledge_chunk"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "document_id"],
            ["knowledge_source_document.tenant_id", "knowledge_source_document.document_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "document_id",
            "chunk_index",
            name="uq_knowledge_chunk_document_index",
        ),
        CheckConstraint("chunk_index >= 0", name="knowledge_chunk_index_nonnegative"),
        CheckConstraint("start_char >= 0 AND end_char >= start_char",
                        name="knowledge_chunk_offsets"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    chunk_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    document_id: Mapped[UUID] = mapped_column(nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    start_char: Mapped[int] = mapped_column(Integer, nullable=False)
    end_char: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict, nullable=False)


class KnowledgeChunkVectorRow(Base):
    """Local pgvector index; external vector backends implement the same port."""

    __tablename__ = "knowledge_chunk_vector"
    __table_args__ = (
        Index(
            "ix_knowledge_chunk_vector_scope_base",
            "tenant_id",
            "knowledge_base_id",
        ),
        Index(
            "ix_knowledge_chunk_vector_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    document_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    knowledge_base_id: Mapped[str] = mapped_column(String(255), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(VECTOR(1024), nullable=False)
