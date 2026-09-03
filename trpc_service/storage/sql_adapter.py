"""
SQL (SQLite/PostgreSQL) Storage Adapter Implementation using SQLAlchemy.
"""

import json
import time
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    BigInteger,
    Float,
    Boolean,
    Text,
    DateTime,
    select,
    delete,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from trpc_service.config.models import (
    Session,
    SessionEvent,
    MemoryItem,
    SummaryItem,
    ArtifactItem,
    AuditLogEntry,
    EventType,
    AuditDecision,
)
from trpc_service.storage.base import BaseStorageAdapter

Base = declarative_base()


def get_utc_now():
    return datetime.now(timezone.utc)


class SessionModel(Base):
    __tablename__ = "sessions"
    session_id = Column(String(255), primary_key=True)
    tenant_id = Column(String(100), index=True, nullable=False)
    app_id = Column(String(100), nullable=False)
    channel_type = Column(String(50), nullable=False)
    external_user_id = Column(String(100), nullable=False)
    external_chat_id = Column(String(100), nullable=True)
    is_group = Column(Boolean, default=False)
    current_version = Column(Integer, default=0)
    state_metadata = Column(Text, default="{}")
    created_at = Column(DateTime, default=get_utc_now)
    updated_at = Column(DateTime, default=get_utc_now)


class SessionEventModel(Base):
    __tablename__ = "session_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(255), index=True, nullable=False)
    tenant_id = Column(String(100), index=True, nullable=False)
    sequence_num = Column(Integer, nullable=False)
    event_type = Column(String(50), nullable=False)
    payload = Column(Text, default="{}")
    created_at = Column(DateTime, default=get_utc_now)


class MemoryModel(Base):
    __tablename__ = "memories"
    memory_id = Column(String(100), primary_key=True)
    tenant_id = Column(String(100), index=True, nullable=False)
    user_id = Column(String(100), index=True, nullable=False)
    content = Column(Text, nullable=False)
    embedding_json = Column(Text, nullable=True)
    metadata_json = Column(Text, default="{}")
    created_at = Column(DateTime, default=get_utc_now)


class SummaryModel(Base):
    __tablename__ = "summaries"
    summary_id = Column(String(100), primary_key=True)
    tenant_id = Column(String(100), index=True, nullable=False)
    session_id = Column(String(255), index=True, nullable=False)
    summary_text = Column(Text, nullable=False)
    covered_sequence_end = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=get_utc_now)


class AuditLogModel(Base):
    __tablename__ = "audit_logs"
    log_id = Column(String(100), primary_key=True)
    trace_id = Column(String(100), index=True, nullable=False)
    tenant_id = Column(String(100), index=True, nullable=False)
    channel = Column(String(50), nullable=False)
    user_id = Column(String(100), nullable=False)
    session_id = Column(String(255), index=True, nullable=False)
    agent_name = Column(String(100), nullable=False)
    tool_name = Column(String(100), nullable=True)
    decision = Column(String(50), nullable=False)
    prompt_tokens = Column(Integer, default=0)
    completion_tokens = Column(Integer, default=0)
    cost_usd = Column(Float, default=0.0)
    latency_ms = Column(Integer, default=0)
    error_type = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=get_utc_now)


class LockModel(Base):
    __tablename__ = "distributed_locks"
    key = Column(String(255), primary_key=True)
    expires_at = Column(Float, nullable=False)


class SQLStorageAdapter(BaseStorageAdapter):
    """
    Relational Database Storage Adapter supporting SQLite, PostgreSQL, and MySQL.
    """

    def __init__(self, db_url: str = "sqlite:///./data/trpc_agent.db"):
        self.engine = create_engine(db_url, echo=False)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    async def acquire_session_lock(self, session_id: str, ttl_seconds: float = 30.0) -> bool:
        now = time.time()
        with self.SessionLocal() as session:
            # Clean expired
            session.query(LockModel).filter(LockModel.expires_at < now).delete()
            lock = session.get(LockModel, f"lock:{session_id}")
            if lock and lock.expires_at > now:
                return False
            if lock:
                lock.expires_at = now + ttl_seconds
            else:
                session.add(LockModel(key=f"lock:{session_id}", expires_at=now + ttl_seconds))
            session.commit()
            return True

    async def release_session_lock(self, session_id: str) -> bool:
        with self.SessionLocal() as session:
            res = session.query(LockModel).filter(LockModel.key == f"lock:{session_id}").delete()
            session.commit()
            return res > 0

    async def check_and_set_idempotency(self, idempotency_key: str, ttl_seconds: float = 60.0) -> bool:
        now = time.time()
        with self.SessionLocal() as session:
            session.query(LockModel).filter(LockModel.expires_at < now).delete()
            lock = session.get(LockModel, f"idempotency:{idempotency_key}")
            if lock and lock.expires_at > now:
                return False  # Already processed
            session.add(LockModel(key=f"idempotency:{idempotency_key}", expires_at=now + ttl_seconds))
            try:
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    async def get_session(self, tenant_id: str, session_id: str) -> Optional[Session]:
        with self.SessionLocal() as session:
            obj = session.get(SessionModel, session_id)
            if not obj or obj.tenant_id != tenant_id:
                return None
            return Session(
                session_id=obj.session_id,
                tenant_id=obj.tenant_id,
                app_id=obj.app_id,
                channel_type=obj.channel_type,
                external_user_id=obj.external_user_id,
                external_chat_id=obj.external_chat_id,
                is_group=obj.is_group,
                current_version=obj.current_version,
                state_metadata=json.loads(obj.state_metadata or "{}"),
                created_at=obj.created_at.replace(tzinfo=timezone.utc),
                updated_at=obj.updated_at.replace(tzinfo=timezone.utc),
            )

    async def save_session(self, s: Session) -> Session:
        with self.SessionLocal() as session:
            obj = session.get(SessionModel, s.session_id)
            if obj:
                obj.current_version = s.current_version
                obj.state_metadata = json.dumps(s.state_metadata)
                obj.updated_at = get_utc_now()
            else:
                obj = SessionModel(
                    session_id=s.session_id,
                    tenant_id=s.tenant_id,
                    app_id=s.app_id,
                    channel_type=s.channel_type,
                    external_user_id=s.external_user_id,
                    external_chat_id=s.external_chat_id,
                    is_group=s.is_group,
                    current_version=s.current_version,
                    state_metadata=json.dumps(s.state_metadata),
                )
                session.add(obj)
            session.commit()
            return s

    async def append_events(
        self,
        tenant_id: str,
        session_id: str,
        events: List[SessionEvent],
        expected_version: int,
    ) -> bool:
        with self.SessionLocal() as session:
            s_obj = session.get(SessionModel, session_id)
            if s_obj and s_obj.current_version != expected_version:
                return False  # Concurrency conflict

            # Get current max sequence
            count = session.query(SessionEventModel).filter(SessionEventModel.session_id == session_id).count()
            for idx, evt in enumerate(events):
                evt.sequence_num = count + idx + 1
                model = SessionEventModel(
                    session_id=session_id,
                    tenant_id=tenant_id,
                    sequence_num=evt.sequence_num,
                    event_type=evt.event_type.value if isinstance(evt.event_type, EventType) else str(evt.event_type),
                    payload=json.dumps(evt.payload),
                )
                session.add(model)

            if s_obj:
                s_obj.current_version += len(events)
                s_obj.updated_at = get_utc_now()
            session.commit()
            return True

    async def get_events(
        self,
        tenant_id: str,
        session_id: str,
        limit: int = 50,
    ) -> List[SessionEvent]:
        with self.SessionLocal() as session:
            records = (
                session.query(SessionEventModel)
                .filter(SessionEventModel.session_id == session_id, SessionEventModel.tenant_id == tenant_id)
                .order_by(SessionEventModel.sequence_num.asc())
                .limit(limit)
                .all()
            )
            return [
                SessionEvent(
                    id=r.id,
                    session_id=r.session_id,
                    tenant_id=r.tenant_id,
                    sequence_num=r.sequence_num,
                    event_type=EventType(r.event_type),
                    payload=json.loads(r.payload or "{}"),
                    created_at=r.created_at.replace(tzinfo=timezone.utc),
                )
                for r in records
            ]

    async def add_memory(self, memory: MemoryItem) -> MemoryItem:
        with self.SessionLocal() as session:
            obj = MemoryModel(
                memory_id=memory.memory_id,
                tenant_id=memory.tenant_id,
                user_id=memory.user_id,
                content=memory.content,
                embedding_json=json.dumps(memory.embedding) if memory.embedding else None,
                metadata_json=json.dumps(memory.metadata),
            )
            session.merge(obj)
            session.commit()
            return memory

    async def search_memory(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        top_k: int = 5,
    ) -> List[MemoryItem]:
        with self.SessionLocal() as session:
            records = (
                session.query(MemoryModel)
                .filter(MemoryModel.tenant_id == tenant_id, MemoryModel.user_id == user_id)
                .order_by(MemoryModel.created_at.desc())
                .limit(top_k)
                .all()
            )
            return [
                MemoryItem(
                    memory_id=r.memory_id,
                    tenant_id=r.tenant_id,
                    user_id=r.user_id,
                    content=r.content,
                    embedding=json.loads(r.embedding_json) if r.embedding_json else None,
                    metadata=json.loads(r.metadata_json or "{}"),
                    created_at=r.created_at.replace(tzinfo=timezone.utc),
                )
                for r in records
            ]

    async def save_summary(self, summary: SummaryItem) -> SummaryItem:
        with self.SessionLocal() as session:
            obj = SummaryModel(
                summary_id=summary.summary_id,
                tenant_id=summary.tenant_id,
                session_id=summary.session_id,
                summary_text=summary.summary_text,
                covered_sequence_end=summary.covered_sequence_end,
            )
            session.add(obj)
            session.commit()
            return summary

    async def get_latest_summary(self, tenant_id: str, session_id: str) -> Optional[SummaryItem]:
        with self.SessionLocal() as session:
            r = (
                session.query(SummaryModel)
                .filter(SummaryModel.tenant_id == tenant_id, SummaryModel.session_id == session_id)
                .order_by(SummaryModel.created_at.desc())
                .first()
            )
            if not r:
                return None
            return SummaryItem(
                summary_id=r.summary_id,
                tenant_id=r.tenant_id,
                session_id=r.session_id,
                summary_text=r.summary_text,
                covered_sequence_end=r.covered_sequence_end,
                created_at=r.created_at.replace(tzinfo=timezone.utc),
            )

    async def save_artifact(self, artifact: ArtifactItem) -> ArtifactItem:
        return artifact

    async def record_audit_log(self, entry: AuditLogEntry) -> None:
        with self.SessionLocal() as session:
            obj = AuditLogModel(
                log_id=entry.log_id,
                trace_id=entry.trace_id,
                tenant_id=entry.tenant_id,
                channel=entry.channel,
                user_id=entry.user_id,
                session_id=entry.session_id,
                agent_name=entry.agent_name,
                tool_name=entry.tool_name,
                decision=entry.decision.value if isinstance(entry.decision, AuditDecision) else str(entry.decision),
                prompt_tokens=entry.prompt_tokens,
                completion_tokens=entry.completion_tokens,
                cost_usd=entry.cost_usd,
                latency_ms=entry.latency_ms,
                error_type=entry.error_type,
            )
            session.add(obj)
            session.commit()

    async def query_audit_logs(
        self,
        tenant_id: str,
        limit: int = 50,
    ) -> List[AuditLogEntry]:
        with self.SessionLocal() as session:
            records = (
                session.query(AuditLogModel)
                .filter(AuditLogModel.tenant_id == tenant_id)
                .order_by(AuditLogModel.created_at.desc())
                .limit(limit)
                .all()
            )
            return [
                AuditLogEntry(
                    log_id=r.log_id,
                    trace_id=r.trace_id,
                    tenant_id=r.tenant_id,
                    channel=r.channel,
                    user_id=r.user_id,
                    session_id=r.session_id,
                    agent_name=r.agent_name,
                    tool_name=r.tool_name,
                    decision=AuditDecision(r.decision),
                    prompt_tokens=r.prompt_tokens,
                    completion_tokens=r.completion_tokens,
                    cost_usd=r.cost_usd,
                    latency_ms=r.latency_ms,
                    error_type=r.error_type,
                    created_at=r.created_at.replace(tzinfo=timezone.utc),
                )
                for r in records
            ]
