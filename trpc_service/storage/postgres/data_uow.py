"""Shared PostgreSQL transaction boundary for data and mutation audit."""
from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from .data_repositories import PostgresDataRepositoryFactory
from .database import PostgresDatabase


class PostgresDataUnitOfWork:
    def __init__(self, database: PostgresDatabase, *, audit: Any = None, migrations: Any = None) -> None:
        self.database = database
        self.audit = audit
        self.migrations = migrations
        self.session: AsyncSession | None = None

    async def __aenter__(self) -> "PostgresDataUnitOfWork":
        self.session = AsyncSession(self.database.engine)
        await self.session.begin()
        factory = PostgresDataRepositoryFactory(self.database)
        self.events = factory.events(self.session, audit=self.audit)
        self.memories = factory.memories(self.session)
        self.summaries = factory.summaries(self.session, audit=self.audit)
        self.audits = self.audit
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.session is None:
            return
        if exc_type is None:
            await self.session.commit()
        else:
            await self.session.rollback()
        await self.session.close()
        self.session = None

    async def commit(self) -> None:
        if self.session is not None:
            await self.session.commit()

    async def rollback(self) -> None:
        if self.session is not None:
            await self.session.rollback()


class PostgresDataUnitOfWorkFactory:
    def __init__(self, database: PostgresDatabase, *, audit: Any = None, migrations: Any = None) -> None:
        self.database = database
        self.audit = audit
        self.migrations = migrations

    def __call__(self, scope: Any) -> PostgresDataUnitOfWork:
        del scope
        return PostgresDataUnitOfWork(self.database, audit=self.audit, migrations=self.migrations)
