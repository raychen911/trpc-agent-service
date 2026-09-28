"""Alembic environment for the service's asynchronous SQLAlchemy models."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

# Import every ORM model so Base.metadata contains the complete schema during
# autogeneration and drift checks.
from trpc_service.agent.models import AgentApp  # noqa: F401
from trpc_service.admin.models import (  # noqa: F401
    ChannelAdapterType, ManagementAuditLog, ManagementCredential, ManagementPasswordCredential,
    ManagementPrincipal, ManagementWebSession, ModelCatalogEntry, ModelProfile, RoleAssignment,
    TenantSecret,
)
from trpc_service.channels.models import (  # noqa: F401
    ChannelBinding, ChannelConversation, ChannelIdentity, ChannelPrincipal, ConversationMember,
)
from trpc_service.config import get_settings
from trpc_service.storage.orm import Base
from trpc_service.storage.knowledge_orm import (  # noqa: F401
    KnowledgeArtifactRow, KnowledgeBaseRow, KnowledgeChunkRow, KnowledgeChunkVectorRow,
    KnowledgeDocumentRow,
)
from trpc_service.storage.runtime_orm import (  # noqa: F401
    AgentSession, AgentTaskRow, AuditLogRow, InboxMessageRow, MemoryRecordRow, OutboxAttemptRow,
    OutboxMessageRow, RunnerAttemptRow, RunnerCheckpointRow, RunnerRequestRow, RuntimeNodeRow,
    SessionEventRow, SessionExecutionFence, SessionSummaryRow, ToolCallLedgerRow,
    WorkerPoolControlRow,
)
from trpc_service.tenant.models import Tenant  # noqa: F401
from trpc_service.mcp.models import MCPConnection  # noqa: F401

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = get_settings().resolved_database_url.render_as_string(hide_password=False)
config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Render migrations without opening a database connection."""

    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: object) -> None:
    """Configure Alembic on the synchronous connection bridge."""

    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Run online migrations through SQLAlchemy's asynchronous engine."""

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Bridge Alembic's synchronous entry point into the async migration loop."""

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
