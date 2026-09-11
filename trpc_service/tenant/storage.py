"""Tenant-scoped state, event, memory and audit stores."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Any

from .models import SessionEvent, SessionSnapshot, utcnow


def scoped_key(tenant_id: str, value: str) -> str:
    return f"{tenant_id}:{value}"


class IdempotencyStore:
    def __init__(self) -> None:
        self._seen: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def claim(self, key: str) -> bool:
        async with self._lock:
            if key in self._seen:
                return False
            self._seen[key] = None
            return True

    async def complete(self, key: str, result: Any) -> None:
        async with self._lock:
            self._seen[key] = result

    async def release(self, key: str) -> None:
        async with self._lock:
            self._seen.pop(key, None)

    async def result(self, key: str) -> Any:
        async with self._lock:
            return self._seen.get(key)

    async def contains(self, key: str) -> bool:
        async with self._lock:
            return key in self._seen

    async def fail(self, key: str, error_type: str) -> None:
        async with self._lock:
            current = self._seen.get(key)
            if isinstance(current, dict) and current.get("status") == "completed":
                return
            if isinstance(current, dict) and current.get("status") == "prepared":
                self._seen[key] = {**current, "status": "delivery_failed", "error_type": error_type}
                return
            self._seen[key] = {"status": "failed", "error_type": error_type}


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionSnapshot] = {}
        self._events: dict[str, list[SessionEvent]] = defaultdict(list)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    @asynccontextmanager
    async def lock(self, tenant_id: str, session_id: str) -> AsyncIterator[None]:
        async with self._locks[scoped_key(tenant_id, session_id)]:
            yield

    async def get_or_create(self, tenant_id: str, session_id: str, app_id: str, user_id: str) -> SessionSnapshot:
        key = scoped_key(tenant_id, session_id)
        if key not in self._sessions:
            self._sessions[key] = SessionSnapshot(tenant_id, session_id, app_id, user_id)
        return self._sessions[key]

    async def append_turn(
        self,
        snapshot: SessionSnapshot,
        user_text: str,
        assistant_text: str,
        trace_id: str,
        lease: Any = None,
        inbox_key: str | None = None,
        config_version: int | None = None,
    ) -> list[SessionEvent]:
        _ = lease, inbox_key, config_version
        key = scoped_key(snapshot.tenant_id, snapshot.session_id)
        start = snapshot.version + 1
        events = [
            SessionEvent(snapshot.tenant_id, snapshot.session_id, start, "user_message", {"text": user_text}, trace_id),
            SessionEvent(
                snapshot.tenant_id,
                snapshot.session_id,
                start + 1,
                "assistant_message",
                {"text": assistant_text},
                trace_id,
            ),
        ]
        self._events[key].extend(events)
        snapshot.version += len(events)
        snapshot.state["last_user_text"] = user_text
        snapshot.state["last_assistant_text"] = assistant_text
        snapshot.updated_at = utcnow()
        return events

    async def events(self, tenant_id: str, session_id: str) -> list[SessionEvent]:
        return list(self._events.get(scoped_key(tenant_id, session_id), []))

    async def export(self) -> dict[str, Any]:
        return {
            "sessions": {key: asdict(value) for key, value in self._sessions.items()},
            "events": {key: [asdict(item) for item in value] for key, value in self._events.items()},
        }


class MemoryStore:
    def __init__(self) -> None:
        self._values: dict[str, list[str]] = defaultdict(list)
        self._sources: set[tuple[str, str]] = set()

    async def add(self, tenant_id: str, user_id: str, value: str, source_id: str | None = None) -> None:
        if source_id is not None:
            source = (tenant_id, source_id)
            if source in self._sources:
                return
            self._sources.add(source)
        self._values[scoped_key(tenant_id, user_id)].append(value)

    async def list(self, tenant_id: str, user_id: str) -> list[str]:
        return list(self._values.get(scoped_key(tenant_id, user_id), []))


class AuditStore:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self._ids: set[tuple[str, str]] = set()

    async def write(self, record: dict[str, Any]) -> None:
        audit_id = record.get("audit_id")
        identity = (str(record.get("tenant_id", "")), str(audit_id))
        if audit_id and identity in self._ids:
            return
        if audit_id:
            self._ids.add(identity)
        self.records.append(dict(record))
