"""审计服务：异步缓冲批量落 SQL，失败降级 JSONL 文件。"""
from __future__ import annotations

import asyncio
import json
import os
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from trpc_service.tenant.audit.model import AuditEvent
from trpc_service.tenant.audit.redact import redact
from trpc_service.log import get_logger

logger = get_logger("audit")

# 项目根：trpc_service/tenant/audit/service.py → parents[3] = 项目根
# （parents[2] 是 trpc_service 包目录，会把兜底文件写进源码树）
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AUDIT_DIR = PROJECT_ROOT / "data" / "audit"


class AuditService:
    """审计写入器：SQL 缓冲批量（有 db 时）/ 文件直写（无 db 时）。"""

    def __init__(
        self,
        audit_dir: Optional[Path] = None,
        db=None,
        batch_size: int = 100,
        flush_interval: float = 2.0,
        max_queue: int = 10000,
    ):
        self._dir = Path(audit_dir) if audit_dir else DEFAULT_AUDIT_DIR
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / "audit.jsonl"
        self._db = db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_queue = max_queue
        self._buffer: deque[dict] = deque()
        self._lock = threading.Lock()   # emit 在事件循环线程、测试可能在线程池调用
        self._task: Optional[asyncio.Task] = None

    # ---- 生命周期 ----

    def attach(self, db) -> None:
        """注入平台 Database（lifespan 启动时调用；测试可直接构造注入）。"""
        self._db = db

    async def start(self) -> None:
        """启动后台批量落库循环（有 db 才有意义）。"""
        if self._db is None or self._task is not None:
            return
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """停循环并把缓冲余量刷净（优雅关停的关键动作）。"""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self.flush()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("audit flush loop error")

    # ---- 写入 ----

    def emit(self, event: AuditEvent) -> AuditEvent:
        """记录一条审计（脱敏后进缓冲或直接落文件）。"""
        record = redact(event.model_dump(mode="json"))
        if self._db is not None:
            with self._lock:
                self._buffer.append(record)
                overflow = len(self._buffer) - self._max_queue
            if overflow > 0:
                # 缓冲超限：把最旧的一批溢出到文件，保护内存
                self._spill_to_file([self._buffer.popleft() for _ in range(overflow)])
            return event
        self._spill_to_file([record])
        return event

    async def flush(self) -> int:
        """把当前缓冲批量写入 SQL；失败则整批落文件。返回落库条数。"""
        if self._db is None:
            return 0
        batch = self._drain(self._batch_size)
        if not batch:
            return 0
        try:
            from trpc_service.tenant.storage.tables import AuditLogRow

            rows = [
                AuditLogRow(**{**row, "created_at": datetime.fromisoformat(row["created_at"])})
                for row in batch
            ]
            with self._db.session() as s:
                s.add_all(rows)
            return len(batch)
        except Exception:  # noqa: BLE001
            logger.exception("audit batch insert failed, spilling to file")
            self._spill_to_file(batch)
            return 0

    def _drain(self, limit: int) -> List[dict]:
        with self._lock:
            batch = []
            while self._buffer and len(batch) < limit:
                batch.append(self._buffer.popleft())
        return batch

    def _spill_to_file(self, records: List[dict]) -> None:
        if not records:
            return
        with self._file.open("a", encoding="utf-8") as fp:
            for record in records:
                fp.write(json.dumps(record, ensure_ascii=False) + "\n")

    # ---- 查询与清理 ----

    def tail(self, limit: int = 20) -> List[dict]:
        """最近 limit 条审计：SQL 优先，失败降级文件。"""
        if self._db is not None:
            try:
                from trpc_service.tenant.storage.tables import AuditLogRow

                with self._db.session() as s:
                    rows = (
                        s.query(AuditLogRow)
                        .order_by(AuditLogRow.id.desc())
                        .limit(limit)
                        .all()
                    )
                    return [
                        {
                            "tenant_id": r.tenant_id,
                            "channel": r.channel,
                            "user_id": r.user_id,
                            "session_id": r.session_id,
                            "agent_name": r.agent_name,
                            "tool_name": r.tool_name,
                            "decision": r.decision,
                            "latency_ms": r.latency_ms,
                            "error_type": r.error_type,
                            "cost": float(r.cost or 0),
                            "trace_id": r.trace_id,
                            "created_at": r.created_at.isoformat(),
                        }
                        for r in reversed(rows)
                    ]
            except Exception:  # noqa: BLE001
                logger.exception("audit tail from SQL failed, falling back to file")
        if not self._file.exists():
            return []
        lines = self._file.read_text(encoding="utf-8").strip().splitlines()
        return [json.loads(line) for line in lines[-limit:]]

    def cleanup(self, retention_map: dict[str, int]) -> int:
        """按租户保留期清理**文件后端**的过期行（SQL 后端的清理在②收尾接 DELETE）。"""
        if not self._file.exists():
            return 0
        now = datetime.now()
        kept, removed = [], 0
        for line in self._file.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            days = retention_map.get(record.get("tenant_id", ""), 90)
            created = datetime.fromisoformat(record["created_at"])
            if now - created > timedelta(days=days):
                removed += 1
            else:
                kept.append(line)
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text("\n".join(kept) + "\n", encoding="utf-8")
        os.replace(tmp, self._file)
        return removed


audit_service = AuditService()
