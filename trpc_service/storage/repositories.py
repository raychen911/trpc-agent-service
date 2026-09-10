"""Tenant-scoped repositories over the SQLite models."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import Select, desc, select, update
from sqlalchemy.exc import IntegrityError

from trpc_service.config.models import (
    AgentAppRecord,
    ArtifactRecord,
    AuditLogRecord,
    ChannelBindingRecord,
    ChannelMode,
    ChannelType,
    ExecutionOutboxRecord,
    InboundMessageRecord,
    KnowledgeRecord,
    MemoryRecord,
    OutboxStatus,
    SessionEventRecord,
    SessionRecord,
    SummaryRecord,
    TenantRecord,
)
from trpc_service.storage.database import Database
from trpc_service.storage.models import (
    AgentAppModel,
    ArtifactModel,
    AuditLogModel,
    ChannelBindingModel,
    ExecutionOutboxModel,
    InboundMessageModel,
    KnowledgeModel,
    MemoryModel,
    SessionEventModel,
    SessionModel,
    SummaryModel,
    TenantModel,
)


def _now() -> datetime:
    return datetime.now(UTC)


class TenantRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: TenantRecord) -> TenantRecord:
        model = TenantModel(
            tenant_id=record.tenant_id,
            name=record.name,
            status=record.status.value,
            audit_policy=record.audit_policy,
            storage_config=record.storage_config.model_dump(mode="json"),
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return TenantRecord.model_validate(model)

    async def get(self, tenant_id: str) -> TenantRecord | None:
        async with self.database.session() as session:
            model = await session.get(TenantModel, tenant_id)
            return TenantRecord.model_validate(model) if model else None

    async def list(self) -> list[TenantRecord]:
        async with self.database.session() as session:
            result = await session.scalars(select(TenantModel).order_by(TenantModel.tenant_id))
            return [TenantRecord.model_validate(model) for model in result]

    async def update_storage(
        self, tenant_id: str, storage_config: dict[str, object]
    ) -> TenantRecord | None:
        statement = (
            update(TenantModel)
            .where(TenantModel.tenant_id == tenant_id)
            .values(storage_config=storage_config, updated_at=_now())
        )
        async with self.database.session() as session:
            result = await session.execute(statement)
            if result.rowcount != 1:
                await session.rollback()
                return None
            await session.commit()
        return await self.get(tenant_id)


class AgentAppRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: AgentAppRecord) -> AgentAppRecord:
        model = AgentAppModel(
            tenant_id=record.tenant_id,
            app_id=record.app_id,
            name=record.name,
            system_prompt=record.system_prompt,
            model_config_data=record.model_config_data,
            tool_policy=record.tool_policy,
            active_config_version=record.active_config_version,
            is_active=record.is_active,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return AgentAppRecord.model_validate(model)

    async def get(self, tenant_id: str, app_id: str) -> AgentAppRecord | None:
        async with self.database.session() as session:
            model = await session.get(AgentAppModel, (tenant_id, app_id))
            return AgentAppRecord.model_validate(model) if model else None

    async def list_for_tenant(self, tenant_id: str) -> list[AgentAppRecord]:
        statement = select(AgentAppModel).where(AgentAppModel.tenant_id == tenant_id)
        async with self.database.session() as session:
            result = await session.scalars(statement.order_by(AgentAppModel.app_id))
            return [AgentAppRecord.model_validate(model) for model in result]


class ChannelBindingRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: ChannelBindingRecord) -> ChannelBindingRecord:
        model = ChannelBindingModel(
            tenant_id=record.tenant_id,
            binding_id=record.binding_id,
            app_id=record.app_id,
            channel_type=record.channel_type.value,
            connection_mode=record.connection_mode.value,
            account_id=record.account_id,
            token_ref=record.token_ref,
            secret_ref=record.secret_ref,
            aes_key_ref=record.aes_key_ref,
            webhook_path=record.webhook_path,
            is_active=record.is_active,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return ChannelBindingRecord.model_validate(model)

    async def get(self, tenant_id: str, binding_id: str) -> ChannelBindingRecord | None:
        async with self.database.session() as session:
            model = await session.get(ChannelBindingModel, (tenant_id, binding_id))
            return ChannelBindingRecord.model_validate(model) if model else None

    async def find_active(
        self,
        channel_type: ChannelType,
        account_id: str,
        connection_mode: ChannelMode | None = None,
    ) -> ChannelBindingRecord | None:
        statement = select(ChannelBindingModel).where(
            ChannelBindingModel.channel_type == channel_type.value,
            ChannelBindingModel.account_id == account_id,
            ChannelBindingModel.is_active.is_(True),
        )
        if connection_mode is not None:
            statement = statement.where(
                ChannelBindingModel.connection_mode == connection_mode.value
            )
        async with self.database.session() as session:
            model = await session.scalar(statement)
            return ChannelBindingRecord.model_validate(model) if model else None

    async def list_for_tenant(self, tenant_id: str) -> list[ChannelBindingRecord]:
        statement = (
            select(ChannelBindingModel)
            .where(ChannelBindingModel.tenant_id == tenant_id)
            .order_by(ChannelBindingModel.binding_id)
        )
        async with self.database.session() as session:
            result = await session.scalars(statement)
            return [ChannelBindingRecord.model_validate(model) for model in result]

    async def list_active(
        self, connection_mode: ChannelMode | None = None
    ) -> list[ChannelBindingRecord]:
        statement = select(ChannelBindingModel).where(ChannelBindingModel.is_active.is_(True))
        if connection_mode is not None:
            statement = statement.where(
                ChannelBindingModel.connection_mode == connection_mode.value
            )
        statement = statement.order_by(
            ChannelBindingModel.channel_type,
            ChannelBindingModel.account_id,
        )
        async with self.database.session() as session:
            result = await session.scalars(statement)
            return [ChannelBindingRecord.model_validate(model) for model in result]


class InboundMessageRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def register(self, record: InboundMessageRecord) -> tuple[InboundMessageRecord, bool]:
        model = InboundMessageModel(
            inbound_id=record.inbound_id,
            tenant_id=record.tenant_id,
            binding_id=record.binding_id,
            external_message_id=record.external_message_id,
            session_id=record.session_id,
            trace_id=record.trace_id,
            status=record.status,
            payload=record.payload,
            created_at=record.created_at,
        )
        async with self.database.session() as session:
            session.add(model)
            try:
                await session.commit()
                await session.refresh(model)
                return InboundMessageRecord.model_validate(model), True
            except IntegrityError:
                await session.rollback()
                statement = select(InboundMessageModel).where(
                    InboundMessageModel.tenant_id == record.tenant_id,
                    InboundMessageModel.binding_id == record.binding_id,
                    InboundMessageModel.external_message_id == record.external_message_id,
                )
                existing = await session.scalar(statement)
                if existing is None:
                    raise
                return InboundMessageRecord.model_validate(existing), False

    async def update_status(self, inbound_id: str, status: str) -> bool:
        statement = (
            update(InboundMessageModel)
            .where(InboundMessageModel.inbound_id == inbound_id)
            .values(status=status)
        )
        async with self.database.session() as session:
            result = await session.execute(statement)
            await session.commit()
            return result.rowcount == 1


class SessionRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: SessionRecord) -> SessionRecord:
        model = SessionModel(
            tenant_id=record.tenant_id,
            session_id=record.session_id,
            app_id=record.app_id,
            principal_id=record.principal_id,
            channel_type=record.channel_type.value,
            state=record.state,
            version=record.version,
            last_event_sequence=record.last_event_sequence,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return SessionRecord.model_validate(model)

    async def get(self, tenant_id: str, session_id: str) -> SessionRecord | None:
        async with self.database.session() as session:
            model = await session.get(SessionModel, (tenant_id, session_id))
            return SessionRecord.model_validate(model) if model else None

    async def update_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict[str, object],
    ) -> bool:
        statement = (
            update(SessionModel)
            .where(
                SessionModel.tenant_id == tenant_id,
                SessionModel.session_id == session_id,
                SessionModel.version == expected_version,
            )
            .values(
                state=state,
                version=expected_version + 1,
                updated_at=_now(),
            )
        )
        async with self.database.session() as session:
            result = await session.execute(statement)
            await session.commit()
            return result.rowcount == 1

    async def update_progress(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        last_event_sequence: int,
        state: dict[str, object],
    ) -> bool:
        statement = (
            update(SessionModel)
            .where(
                SessionModel.tenant_id == tenant_id,
                SessionModel.session_id == session_id,
                SessionModel.version == expected_version,
            )
            .values(
                state=state,
                last_event_sequence=last_event_sequence,
                version=expected_version + 1,
                updated_at=_now(),
            )
        )
        async with self.database.session() as session:
            result = await session.execute(statement)
            await session.commit()
            return result.rowcount == 1


class SessionEventRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def append(self, record: SessionEventRecord) -> SessionEventRecord:
        model = SessionEventModel(
            event_id=record.event_id,
            tenant_id=record.tenant_id,
            session_id=record.session_id,
            sequence=record.sequence,
            event_type=record.event_type.value,
            payload=record.payload,
            trace_id=record.trace_id,
            created_at=record.created_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return SessionEventRecord.model_validate(model)

    async def list_for_session(
        self, tenant_id: str, session_id: str, limit: int = 100
    ) -> list[SessionEventRecord]:
        statement = (
            select(SessionEventModel)
            .where(
                SessionEventModel.tenant_id == tenant_id,
                SessionEventModel.session_id == session_id,
            )
            .order_by(SessionEventModel.sequence)
            .limit(limit)
        )
        async with self.database.session() as session:
            result = await session.scalars(statement)
            return [SessionEventRecord.model_validate(model) for model in result]


class MemoryRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: MemoryRecord) -> MemoryRecord:
        model = MemoryModel(
            tenant_id=record.tenant_id,
            memory_id=record.memory_id,
            principal_id=record.principal_id,
            content=record.content,
            source_event_id=record.source_event_id,
            metadata_data=record.metadata_data,
            created_at=record.created_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return MemoryRecord.model_validate(model)

    async def list_for_principal(
        self, tenant_id: str, principal_id: str, limit: int = 20
    ) -> list[MemoryRecord]:
        statement = (
            select(MemoryModel)
            .where(
                MemoryModel.tenant_id == tenant_id,
                MemoryModel.principal_id == principal_id,
            )
            .order_by(desc(MemoryModel.created_at))
            .limit(limit)
        )
        async with self.database.session() as session:
            result = await session.scalars(statement)
            return [MemoryRecord.model_validate(model) for model in result]


class SummaryRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: SummaryRecord) -> SummaryRecord:
        model = SummaryModel(
            tenant_id=record.tenant_id,
            summary_id=record.summary_id,
            session_id=record.session_id,
            content=record.content,
            source_end_sequence=record.source_end_sequence,
            version=record.version,
            created_at=record.created_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return SummaryRecord.model_validate(model)

    async def latest(self, tenant_id: str, session_id: str) -> SummaryRecord | None:
        statement = (
            select(SummaryModel)
            .where(
                SummaryModel.tenant_id == tenant_id,
                SummaryModel.session_id == session_id,
            )
            .order_by(desc(SummaryModel.source_end_sequence))
            .limit(1)
        )
        async with self.database.session() as session:
            model = await session.scalar(statement)
            return SummaryRecord.model_validate(model) if model else None


class KnowledgeRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: KnowledgeRecord) -> KnowledgeRecord:
        model = KnowledgeModel(**record.model_dump())
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return KnowledgeRecord.model_validate(model)

    async def search(
        self,
        tenant_id: str,
        query: str,
        app_id: str | None = None,
        limit: int = 20,
    ) -> list[KnowledgeRecord]:
        statement = select(KnowledgeModel).where(
            KnowledgeModel.tenant_id == tenant_id,
            KnowledgeModel.content.contains(query),
        )
        if app_id is not None:
            statement = statement.where(KnowledgeModel.app_id == app_id)
        statement = statement.order_by(desc(KnowledgeModel.updated_at)).limit(limit)
        async with self.database.session() as session:
            result = await session.scalars(statement)
            return [KnowledgeRecord.model_validate(model) for model in result]


class ArtifactRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: ArtifactRecord) -> ArtifactRecord:
        model = ArtifactModel(**record.model_dump())
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return ArtifactRecord.model_validate(model)

    async def get(self, tenant_id: str, artifact_id: str) -> ArtifactRecord | None:
        async with self.database.session() as session:
            model = await session.get(ArtifactModel, (tenant_id, artifact_id))
            return ArtifactRecord.model_validate(model) if model else None


class AuditLogRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: AuditLogRecord) -> AuditLogRecord:
        model = AuditLogModel(
            log_id=record.log_id,
            trace_id=record.trace_id,
            tenant_id=record.tenant_id,
            channel=record.channel.value,
            user_id=record.user_id,
            session_id=record.session_id,
            agent_name=record.agent_name,
            tool_name=record.tool_name,
            decision=record.decision.value,
            latency_ms=record.latency_ms,
            error_type=record.error_type,
            cost=record.cost,
            created_at=record.created_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return AuditLogRecord.model_validate(model)

    async def list_for_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditLogRecord]:
        statement: Select[tuple[AuditLogModel]] = (
            select(AuditLogModel)
            .where(AuditLogModel.tenant_id == tenant_id)
            .order_by(desc(AuditLogModel.created_at))
            .limit(limit)
        )
        async with self.database.session() as session:
            result: Sequence[AuditLogModel] = (await session.scalars(statement)).all()
            return [AuditLogRecord.model_validate(model) for model in result]


class ExecutionOutboxRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def create(self, record: ExecutionOutboxRecord) -> ExecutionOutboxRecord:
        model = ExecutionOutboxModel(
            outbox_id=record.outbox_id,
            trace_id=record.trace_id,
            tenant_id=record.tenant_id,
            payload=record.payload,
            status=record.status.value,
            attempts=record.attempts,
            error_type=record.error_type,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
        async with self.database.session() as session:
            session.add(model)
            await session.commit()
            await session.refresh(model)
        return ExecutionOutboxRecord.model_validate(model)

    async def get(self, outbox_id: str) -> ExecutionOutboxRecord | None:
        async with self.database.session() as session:
            model = await session.get(ExecutionOutboxModel, outbox_id)
            return ExecutionOutboxRecord.model_validate(model) if model else None

    async def mark(
        self,
        outbox_id: str,
        status: OutboxStatus,
        *,
        error_type: str | None = None,
        increment_attempts: bool = False,
    ) -> bool:
        values: dict[str, object] = {
            "status": status.value,
            "error_type": error_type,
            "updated_at": _now(),
        }
        if increment_attempts:
            values["attempts"] = ExecutionOutboxModel.attempts + 1
        statement = (
            update(ExecutionOutboxModel)
            .where(ExecutionOutboxModel.outbox_id == outbox_id)
            .values(**values)
        )
        async with self.database.session() as session:
            result = await session.execute(statement)
            await session.commit()
            return result.rowcount == 1


__all__ = [
    "AgentAppRepository",
    "ArtifactRepository",
    "AuditLogRepository",
    "ChannelBindingRepository",
    "ExecutionOutboxRepository",
    "InboundMessageRepository",
    "KnowledgeRepository",
    "MemoryRepository",
    "SessionEventRepository",
    "SessionRepository",
    "SummaryRepository",
    "TenantRepository",
]
