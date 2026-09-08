# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Append-only audit log with a pluggable sink.

The process keeps only the most recent 500 entries for tests and diagnostics.
The production sink is append-only SQL so durable history is not tied to the
process cache.
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime
from typing import Awaitable
from typing import Callable
from typing import Optional

from ._models import AuditLogEntry

AuditSink = Callable[[AuditLogEntry], Awaitable[None]]
"""Async callback that persists a single audit entry."""
AuditSource = Callable[..., Awaitable[list[AuditLogEntry]]]

_MAX_IN_MEMORY_ENTRIES = 500
"""Maximum recent audit entries retained by each process."""


class AuditLogger:
    """Collects and queries audit entries."""

    def __init__(self, sink: Optional[AuditSink] = None, source: Optional[AuditSource] = None) -> None:
        self._entries: deque[AuditLogEntry] = deque(maxlen=_MAX_IN_MEMORY_ENTRIES)
        self._sink = sink
        self._source = source
        self._lock = threading.RLock()

    async def log(self, entry: AuditLogEntry) -> None:
        """Append an entry to the in-memory log and forward to the sink (if any)."""
        with self._lock:
            self._entries.append(entry)
        if self._sink is not None:
            await self._sink(entry)

    async def query(
        self,
        *,
        tenant_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        decision: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> list[AuditLogEntry]:
        """Return entries matching the given filters (newest first)."""
        if self._source is not None:
            return await self._source(
                tenant_id=tenant_id,
                tool_name=tool_name,
                decision=decision,
                since=since,
            )
        with self._lock:
            entries = list(self._entries)
        if tenant_id is not None:
            entries = [e for e in entries if e.tenant_id == tenant_id]
        if tool_name is not None:
            entries = [e for e in entries if e.tool_name == tool_name]
        if decision is not None:
            entries = [e for e in entries if e.decision == decision]
        if since is not None:
            entries = [e for e in entries if e.created_at >= since]
        return list(reversed(entries))

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
