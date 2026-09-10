"""平台侧数据表的 SQLAlchemy 模型（唯一事实源）。"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

MYSQL_TABLE_ARGS = {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"}


class Base(DeclarativeBase):
    """平台表共同基类（与框架自身的 ORM metadata 互不相干）。"""


class AuditLogRow(Base):
    """审计日志（audit_log）。字段对齐 docs/data-model.md 的 DDL。"""

    __tablename__ = "audit_log"
    __table_args__ = (
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),  # SQLite 仅对 INTEGER PK 自增
        primary_key=True,
        autoincrement=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False)
    channel: Mapped[str] = mapped_column(String(16), nullable=False, default="web")
    user_id: Mapped[str] = mapped_column(String(64), default="")
    session_id: Mapped[str] = mapped_column(String(64), default="")
    agent_name: Mapped[str] = mapped_column(String(64), default="")
    tool_name: Mapped[str] = mapped_column(String(128), default="")
    decision: Mapped[str] = mapped_column(String(16), nullable=False, default="allow")
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    error_type: Mapped[str] = mapped_column(String(64), default="")
    cost: Mapped[float] = mapped_column(Numeric(10, 4), default=0.0)
    trace_id: Mapped[str] = mapped_column(String(32), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class ChannelBindingRow(Base):
    """IM 用户与租户的绑定（channel_binding）。写入方在⑤幂等批次接线。"""

    __tablename__ = "channel_binding"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "channel_type", "external_user_id", "chat_id",
            name="uk_binding",
        ),
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),  # SQLite 仅对 INTEGER PK 自增
        primary_key=True,
        autoincrement=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False)
    channel_type: Mapped[str] = mapped_column(String(16), nullable=False)
    external_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    chat_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    session_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class IdempotencyRow(Base):
    """消息幂等键（idempotency）。第三层兜底（SQL 唯一索引）。"""

    __tablename__ = "idempotency"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uk_idem"),
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),  # SQLite 仅对 INTEGER PK 自增
        primary_key=True,
        autoincrement=True,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    dedupe_status: Mapped[str] = mapped_column(String(16), nullable=False, default="processed")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class TenantRow(Base):
    """租户（tenant）：元数据 + TenantConfig 完整 JSON 快照。"""

    __tablename__ = "tenant"
    __table_args__ = (
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    tenant_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, onupdate=datetime.now)


class TenantRevisionRow(Base):
    """租户配置历史版本（tenant_revision）：只增不改，支持按租户回滚。"""

    __tablename__ = "tenant_revision"
    __table_args__ = (
        UniqueConstraint("tenant_id", "revision", name="uk_tenant_revision"),
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),  # SQLite 仅对 INTEGER PK 自增
        primary_key=True,
        autoincrement=True,
    )
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)


class AgentAppRow(Base):
    """Agent 应用（agent_app）：app_name 唯一注册表（租户配置的应用级投影）。"""

    __tablename__ = "agent_app"
    __table_args__ = (
        UniqueConstraint("app_name", name="uk_agent_app_name"),
        {"mysql_engine": MYSQL_TABLE_ARGS["mysql_engine"], "mysql_charset": MYSQL_TABLE_ARGS["mysql_charset"]},
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer(), "sqlite"),  # SQLite 仅对 INTEGER PK 自增
        primary_key=True,
        autoincrement=True,
    )
    app_name: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    instruction: Mapped[str] = mapped_column(Text, nullable=False, default="")
    model_name: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
