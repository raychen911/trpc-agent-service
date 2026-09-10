"""SQLite adapter implementing stable storage protocols."""

from __future__ import annotations

from typing import Any

from trpc_agent_sdk.sessions import SqlSessionService

from trpc_service.config.models import KnowledgeRecord, MemoryRecord
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import KnowledgeRepository, MemoryRepository


class SQLiteMemoryStore:
    def __init__(self, database: Database) -> None:
        self._repository = MemoryRepository(database)

    async def create(self, record: MemoryRecord) -> MemoryRecord:
        return await self._repository.create(record)

    async def list_for_principal(
        self, tenant_id: str, principal_id: str, limit: int = 20
    ) -> list[MemoryRecord]:
        return await self._repository.list_for_principal(tenant_id, principal_id, limit)


class SQLiteKnowledgeStore:
    def __init__(self, database: Database) -> None:
        self._repository = KnowledgeRepository(database)

    async def create(self, record: KnowledgeRecord) -> KnowledgeRecord:
        return await self._repository.create(record)

    async def search(
        self,
        tenant_id: str,
        query: str,
        app_id: str | None = None,
        limit: int = 20,
    ) -> list[KnowledgeRecord]:
        return await self._repository.search(tenant_id, query, app_id, limit)


class SQLiteSessionService(SqlSessionService):
    """Work around an async SDK TTL-refresh expiration on session reads."""

    async def _get_session(
        self,
        sql_session: Any,
        app_name: str,
        user_id: str,
        session_id: str,
    ) -> Any:
        storage_session = await super()._get_session(
            sql_session,
            app_name,
            user_id,
            session_id,
        )
        if storage_session is not None:
            await self._sql_storage.refresh(sql_session, storage_session)
        return storage_session


__all__ = ["SQLiteKnowledgeStore", "SQLiteMemoryStore", "SQLiteSessionService"]
