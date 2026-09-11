"""add tenant scoped Artifact catalogue and SQL Knowledge (R1C)."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0009_add_artifact_knowledge"
down_revision: Union[str, None] = "0008_add_tenant_backend_profile"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "artifact_metadata",
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("artifact_path", sa.TEXT, nullable=False),
        sa.Column("state", sa.TEXT, nullable=False),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"], ondelete="RESTRICT",
                                name="artifact_metadata_tenant_fk"),
        sa.PrimaryKeyConstraint("tenant_id", "artifact_path"),
        sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="artifact_metadata_tenant_id_format"),
        sa.CheckConstraint("btrim(artifact_path) <> ''", name="artifact_metadata_path_not_blank"),
        sa.CheckConstraint("state IN ('active', 'deleted')", name="artifact_metadata_state_valid"),
    )
    op.create_table(
        "artifact_versions",
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("artifact_path", sa.TEXT, nullable=False),
        sa.Column("version", sa.BIGINT, nullable=False),
        sa.Column("object_key", sa.TEXT, nullable=False),
        sa.Column("content_digest", sa.CHAR(64), nullable=False),
        sa.Column("size_bytes", sa.BIGINT, nullable=False),
        sa.Column("mime_type", sa.TEXT, nullable=False),
        sa.Column("custom_metadata", JSONB, nullable=False),
        sa.Column("state", sa.TEXT, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["tenant_id", "artifact_path"],
                                ["artifact_metadata.tenant_id", "artifact_metadata.artifact_path"],
                                ondelete="RESTRICT", name="artifact_versions_metadata_fk"),
        sa.PrimaryKeyConstraint("tenant_id", "artifact_path", "version"),
        sa.UniqueConstraint("object_key", name="artifact_versions_object_key_uk"),
        sa.CheckConstraint("version >= 0", name="artifact_versions_version_non_negative"),
        sa.CheckConstraint("btrim(object_key) <> ''", name="artifact_versions_object_key_not_blank"),
        sa.CheckConstraint("content_digest ~ '^[0-9a-f]{64}$'", name="artifact_versions_digest_format"),
        sa.CheckConstraint("size_bytes >= 0", name="artifact_versions_size_non_negative"),
        sa.CheckConstraint("btrim(mime_type) <> ''", name="artifact_versions_mime_type_not_blank"),
        sa.CheckConstraint("jsonb_typeof(custom_metadata) = 'object'", name="artifact_versions_metadata_is_object"),
        sa.CheckConstraint("state IN ('pending', 'available', 'deleted')", name="artifact_versions_state_valid"),
    )
    op.create_index("artifact_versions_tenant_path_state_idx", "artifact_versions",
                    ["tenant_id", "artifact_path", "state"])
    op.create_table(
        "knowledge_documents",
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("document_id", sa.TEXT, nullable=False),
        sa.Column("content", sa.TEXT, nullable=False),
        sa.Column("metadata", JSONB, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"], ondelete="RESTRICT",
                                name="knowledge_documents_tenant_fk"),
        sa.PrimaryKeyConstraint("tenant_id", "document_id"),
        sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="knowledge_documents_tenant_id_format"),
        sa.CheckConstraint("btrim(document_id) <> ''", name="knowledge_documents_id_not_blank"),
        sa.CheckConstraint("btrim(content) <> ''", name="knowledge_documents_content_not_blank"),
        sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="knowledge_documents_metadata_is_object"),
    )
    op.execute("CREATE INDEX knowledge_documents_content_fts_idx ON knowledge_documents "
               "USING GIN (to_tsvector('simple', content))")


def downgrade() -> None:
    op.execute("DROP INDEX knowledge_documents_content_fts_idx")
    op.drop_table("knowledge_documents")
    op.drop_index("artifact_versions_tenant_path_state_idx", table_name="artifact_versions")
    op.drop_table("artifact_versions")
    op.drop_table("artifact_metadata")
