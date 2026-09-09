# ===================================================================
# storage.sql_store - SQL 存储实现（租户 / 审计 / Summary 持久化）
# ===================================================================
# 说明: 承载 Audit Log 与 Tenant 持久化（强一致，PRD 2.2 / 2.5），
#   表结构与 PRD 2.5 DDL 对应。
#   使用 SQLAlchemy 2.0 async 引擎（sqlite+aiosqlite 本地自测，
#   mysql+aiomysql / postgresql+asyncpg 生产）。
# 规范: 所有查询强制 WHERE tenant_id = ?（行级隔离，PRD 1.4 / 风险 1）。
# ===================================================================

from __future__ import annotations

import uuid
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Float, Index, Integer, MetaData, String, Table, Text, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.dialects.sqlite import JSON as SQLiteJSON

import hashlib
from datetime import datetime

from .base import AuditStore, SummaryStore
from ..tenant.models import TenantConfig, tenant_from_dict

# 兼容不同方言的 JSON 类型
try:
    from sqlalchemy.dialects.mysql import JSON as MySQLJSON
except ImportError:  # pragma: no cover
    MySQLJSON = SQLiteJSON

_JSON = SQLiteJSON

_META = MetaData()

# 与 PRD 2.5 audit_log 表对应（最小字段集）
audit_log = Table(
    "audit_log",
    _META,
    Column("log_id", String(64), primary_key=True),
    Column("tenant_id", String(64), nullable=False),
    Column("trace_id", String(64)),
    Column("channel", String(32)),
    Column("user_id", String(64)),
    Column("session_id", String(64)),
    Column("agent_name", String(128)),
    Column("tool_name", String(128)),
    Column("decision", String(32)),
    Column("latency_ms", Integer),
    Column("error_type", String(64)),
    Column("cost", String(32)),
    Column("payload", _JSON),
    Column("created_at", DateTime),
    Index("idx_tenant_time", "tenant_id", "created_at"),
)

# 与 PRD 2.5 tenant 表对应（含 JSON 配置列；补充预算/限流配额列）
tenant = Table(
    "tenant",
    _META,
    Column("tenant_id", String(64), primary_key=True),
    Column("name", String(128), nullable=False),
    Column("status", String(32), nullable=False, default="active"),
    Column("app_config", _JSON),
    Column("model_config", _JSON),
    Column("tool_permissions", _JSON),
    Column("im_channel_config", _JSON),
    Column("data_backend_config", _JSON),
    Column("audit_policy", _JSON),
    Column("gray_config", _JSON),
    Column("monthly_budget_usd", Float),
    Column("used_budget_usd", Float),
    Column("rate_limit_per_min", Integer),
    Column("created_at", DateTime),
    Column("updated_at", DateTime),
    Index("idx_status", "status"),
)

# 与 PRD 2.5 summary 表对应（每 session 一条，低频更新）
summary = Table(
    "summary",
    _META,
    Column("summary_id", String(64), primary_key=True),
    Column("session_id", String(64), nullable=False),
    Column("tenant_id", String(64), nullable=False),
    Column("content", Text),
    Column("updated_at", DateTime),
    Index("idx_tenant_session", "tenant_id", "session_id"),
)

# 租户配置历史快照（PRD 5.2 配置回滚）：Admin 每次 update 前落一份旧配置，
# 共享存储——多节点 Admin 实例回滚语义一致，进程重启不丢（与内存环形
# 快照 _history 同语义，MAX_KEEP_VERSIONS 条上限由 push_history 截断）。
tenant_config_history = Table(
    "tenant_config_history",
    _META,
    Column("history_id", Integer, primary_key=True, autoincrement=True),
    Column("tenant_id", String(64), nullable=False),
    Column("config_json", _JSON, nullable=False),
    Column("created_at", DateTime),
    Index("idx_history_tenant", "tenant_id"),
)


class SqlAuditStore(AuditStore):
    """基于 SQLAlchemy 的审计日志存储（强一致，独立审计表）。"""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def write_log(self, tenant_id: str, log: dict[str, Any]) -> None:
        row = {
            "log_id": log.get("log_id") or uuid.uuid4().hex,
            "tenant_id": tenant_id,
            "trace_id": log.get("trace_id"),
            "channel": log.get("channel"),
            "user_id": log.get("user_id"),
            "session_id": log.get("session_id"),
            "agent_name": log.get("agent_name"),
            "tool_name": log.get("tool_name"),
            "decision": log.get("decision"),
            "latency_ms": log.get("latency_ms"),
            "error_type": log.get("error_type"),
            "cost": str(log["cost"]) if log.get("cost") is not None else None,
            "payload": log.get("payload"),
            "created_at": log.get("created_at") or func.now(),
        }
        async with self._engine.begin() as conn:
            await conn.execute(audit_log.insert().values(**row))

    async def query_logs(self, tenant_id: str, filters: dict[str, Any], limit: int = 100) -> list[dict[str, Any]]:
        stmt = select(audit_log).where(audit_log.c.tenant_id == tenant_id)
        for key, value in filters.items():
            if key in audit_log.c:
                stmt = stmt.where(audit_log.c[key] == value)
        stmt = stmt.order_by(audit_log.c.created_at.desc()).limit(limit)
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()
        return [_json_safe_row(dict(row._mapping)) for row in rows]

    async def close(self) -> None:
        await self._engine.dispose()


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    """把 SQL 行转为 JSON 可序列化 dict（datetime -> ISO 字符串）。

    Admin 审计查询直接经 JSONResponse 返回（web/admin.py:257），datetime
    不可序列化会 500（联调发现）。payload 等嵌套结构可能含 datetime 一并处理。
    """
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        elif isinstance(value, dict):
            out[key] = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in value.items()}
        else:
            out[key] = value
    return out


class SqlTenantStore:
    """基于 SQLAlchemy 的租户配置存储（行级隔离，PRD 1.4 / 2.5 tenant 表）。

    密钥字段（api_key_ref / token_ref）不落库（PRD 4.5），运行时环境注入；
    输出时 model_dump 排除密钥字段，禁止明文回显。
    """

    _SECRET_EXCLUDE = {
        "model": {"api_key_ref"},
        "im": {
            "__all__": {"token_ref", "secret_ref", "aes_key_ref"}
        },
    }

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @classmethod
    def _to_row(cls, config: TenantConfig) -> dict[str, Any]:
        """TenantConfig -> SQL 行（JSON 列存结构化配置，密钥字段排除）。"""
        d = config.model_dump(exclude=cls._SECRET_EXCLUDE)
        return {
            "tenant_id": config.tenant_id,
            "name": config.name,
            "status": config.status,
            "app_config": d["app"],
            "model_config": d["model"],
            "tool_permissions": d["tools"],
            "im_channel_config": d["im"],
            "data_backend_config": d["backends"],
            "audit_policy": d["audit"],
            "gray_config": d["gray"],
            "monthly_budget_usd": config.monthly_budget_usd,
            "used_budget_usd": config.used_budget_usd,
            "rate_limit_per_min": config.rate_limit_per_min,
        }

    @classmethod
    def _from_row(cls, row: dict[str, Any]) -> TenantConfig:
        """SQL 行 -> TenantConfig（扁平命名经 tenant_from_dict 自动展开）。"""
        return tenant_from_dict({
            "tenant_id":
            row["tenant_id"],
            "name":
            row["name"],
            "status":
            row["status"],
            "app_config":
            row.get("app_config") or {},
            "model_config":
            row.get("model_config") or {},
            "tool_permissions":
            row.get("tool_permissions") or {},
            "im_channel_config":
            row.get("im_channel_config") or [],
            "data_backend_config":
            row.get("data_backend_config") or {},
            "audit_policy":
            row.get("audit_policy") or {},
            "gray_config":
            row.get("gray_config") or {},
            "monthly_budget_usd":
            row.get("monthly_budget_usd") or 0.0,
            "used_budget_usd":
            row.get("used_budget_usd") or 0.0,
            "rate_limit_per_min":
            row.get("rate_limit_per_min") if row.get("rate_limit_per_min") is not None else 60,
        })

    async def list(self) -> list[TenantConfig]:
        async with self._engine.connect() as conn:
            result = await conn.execute(select(tenant).order_by(tenant.c.tenant_id))
            rows = result.fetchall()
        return [self._from_row(dict(r._mapping)) for r in rows]

    async def get(self, tenant_id: str) -> Optional[TenantConfig]:
        stmt = select(tenant).where(tenant.c.tenant_id == tenant_id)
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            row = result.first()
        return self._from_row(dict(row._mapping)) if row is not None else None

    async def create(self, config: TenantConfig) -> None:
        row = self._to_row(config)
        row["created_at"] = func.now()
        row["updated_at"] = func.now()
        async with self._engine.begin() as conn:
            await conn.execute(tenant.insert().values(**row))

    async def update(self, tenant_id: str, config: TenantConfig) -> None:
        row = self._to_row(config)
        row["updated_at"] = func.now()
        async with self._engine.begin() as conn:
            await conn.execute(tenant.update().where(tenant.c.tenant_id == tenant_id).values(**row))

    async def delete(self, tenant_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(tenant.delete().where(tenant.c.tenant_id == tenant_id))

    async def increment_usage(self, tenant_id: str, delta_usd: float) -> None:
        """原子累加已用预算（used_budget_usd += delta，PRD 4.2/6-10）。

        仅允许正向累加（负 delta 直接忽略，防越权改写）；SQL 侧原子
        UPDATE，多节点并发累加不丢更新（预算为最终一致，见 PRD §2.4）。
        """
        if delta_usd <= 0:
            return
        async with self._engine.begin() as conn:
            await conn.execute(tenant.update().where(tenant.c.tenant_id == tenant_id).values(
                used_budget_usd=tenant.c.used_budget_usd + delta_usd,
                updated_at=func.now(),
            ))

    async def close(self) -> None:
        await self._engine.dispose()

    async def push_history(self, tenant_id: str, config: TenantConfig, keep: int = 5) -> None:
        """落一份租户配置历史快照（update 前调用，PRD 5.2 回滚数据源）。

        Args:
            keep: 保留最近 keep 份（超出即删，与 Admin 内存环形上限对齐）。
        """
        snapshot = config.model_dump(exclude=self._SECRET_EXCLUDE)
        async with self._engine.begin() as conn:
            await conn.execute(tenant_config_history.insert().values(
                tenant_id=tenant_id,
                config_json=snapshot,
                created_at=func.now(),
            ))
            # 环形上限：仅保留最近 keep 条（子查询选新 id，防多节点并发膨胀）
            subq = (select(
                tenant_config_history.c.history_id).where(tenant_config_history.c.tenant_id == tenant_id).order_by(
                    tenant_config_history.c.history_id.desc()).limit(keep))
            await conn.execute(tenant_config_history.delete().where(
                tenant_config_history.c.tenant_id == tenant_id,
                tenant_config_history.c.history_id.not_in(subq),
            ))

    async def pop_latest_history(self, tenant_id: str) -> Optional[TenantConfig]:
        """弹出最近一份历史快照并删除（rollback 语义，与内存 hist.pop() 对齐）。"""
        async with self._engine.begin() as conn:
            result = await conn.execute(
                select(tenant_config_history).where(tenant_config_history.c.tenant_id == tenant_id).order_by(
                    tenant_config_history.c.history_id.desc()).limit(1).with_for_update())
            row = result.first()
            if row is None:
                return None
            await conn.execute(
                tenant_config_history.delete().where(tenant_config_history.c.history_id == row._mapping["history_id"]))
        return tenant_from_dict(row._mapping["config_json"])


class SqlSummaryStore(SummaryStore):
    """基于 SQLAlchemy 的 Summary 存储（每 session 一条，PRD 2.2 / 2.5）。"""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _summary_id(tenant_id: str, session_id: str) -> str:
        """确定性 summary_id（同租户同 session 恒等，便于幂等 upsert）。"""
        return hashlib.sha256(f"{tenant_id}:{session_id}".encode()).hexdigest()[:32]

    async def get_summary(self, tenant_id: str, session_id: str) -> Optional[str]:
        stmt = select(summary.c.content).where(summary.c.tenant_id == tenant_id, summary.c.session_id == session_id)
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            row = result.first()
        return row[0] if row is not None else None

    async def save_summary(self, tenant_id: str, session_id: str, content: str) -> None:
        # 先删后插（同一事务内），保证每 session 仅一条（幂等）
        async with self._engine.begin() as conn:
            await conn.execute(summary.delete().where(summary.c.tenant_id == tenant_id,
                                                      summary.c.session_id == session_id))
            await conn.execute(summary.insert().values(
                summary_id=self._summary_id(tenant_id, session_id),
                session_id=session_id,
                tenant_id=tenant_id,
                content=content,
                updated_at=func.now(),
            ))

    async def delete_summary(self, tenant_id: str, session_id: str) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(summary.delete().where(summary.c.tenant_id == tenant_id,
                                                      summary.c.session_id == session_id))

    async def list_summaries(self, tenant_id: str) -> list[tuple[str, str]]:
        stmt = select(summary.c.session_id, summary.c.content).where(summary.c.tenant_id == tenant_id)
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()
        return [(row[0], row[1]) for row in rows]

    async def close(self) -> None:
        await self._engine.dispose()


async def create_sql_engine(dsn: str, echo: bool = False) -> AsyncEngine:
    """按 DSN 创建异步引擎。

    Args:
        dsn: sqlite+aiosqlite:///... / mysql+aiomysql://... / postgresql+asyncpg://...
        echo: 是否输出 SQL 日志（调试用）

    Returns:
        AsyncEngine: 已建表（sqlite 自动建表）的引擎
    """
    if dsn.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
        # sqlite 文件路径可能为相对路径，需确保父目录存在
        if ":///" in dsn:
            path = dsn.split(":///", 1)[1]
            if path and path != ":memory:":
                import os
                from pathlib import Path

                parent = Path(path).parent
                if parent and not parent.exists():
                    os.makedirs(parent, exist_ok=True)
    else:
        connect_args = {}
    engine = create_async_engine(dsn, echo=echo, connect_args=connect_args)
    if dsn.startswith("sqlite"):
        async with engine.begin() as conn:
            await conn.run_sync(_META.create_all)
            # 轻量迁移：既有库若缺新增列则补（不重建丢数据）
            await conn.run_sync(_sqlite_ensure_columns, {
                "tenant": {
                    "gray_config": "JSON",
                },
            })
    return engine


def _sqlite_ensure_columns(sync_conn: Any, table_columns: dict[str, dict[str, str]]) -> None:
    """为已有 sqlite 表补齐缺失列（PRD 数据模型演进）。

    Args:
        sync_conn: SQLAlchemy 同步连接（run_sync 内）
        table_columns: {表名: {列名: SQLAlchemy 类型名}}
    """
    from sqlalchemy import inspect, text

    inspector = inspect(sync_conn)
    for table_name, columns in table_columns.items():
        existing = {col["name"] for col in inspector.get_columns(table_name)}
        for column_name, col_type in columns.items():
            if column_name not in existing:
                sync_conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {col_type}"))
