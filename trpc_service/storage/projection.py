"""Versioned summary and memory projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Summary:
    tenant_id: str
    session_id: str
    source_version: int
    content: str


class ProjectionStore:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    async def put_summary(self, summary: Summary) -> bool:
        async with self.repository._scoped(summary.tenant_id) as connection:
            value = await connection.fetchval(
                """
                INSERT INTO summaries(tenant_id,session_id,source_version,content)
                VALUES($1,$2,$3,$4)
                ON CONFLICT (tenant_id,session_id) DO UPDATE SET source_version=EXCLUDED.source_version,
                    content=EXCLUDED.content,updated_at=now()
                WHERE summaries.source_version <= EXCLUDED.source_version
                RETURNING source_version
                """,
                summary.tenant_id,
                summary.session_id,
                summary.source_version,
                summary.content,
            )
        return value is not None

    async def get_summary(self, tenant_id: str, session_id: str) -> Summary | None:
        async with self.repository._scoped(tenant_id) as connection:
            row = await connection.fetchrow(
                "SELECT tenant_id,session_id,source_version,content FROM summaries WHERE tenant_id=$1 AND session_id=$2",
                tenant_id,
                session_id,
            )
        return Summary(**dict(row)) if row else None


class InMemoryProjectionStore:
    def __init__(self) -> None:
        self._summaries: dict[tuple[str, str], Summary] = {}

    async def put_summary(self, summary: Summary) -> bool:
        key = (summary.tenant_id, summary.session_id)
        current = self._summaries.get(key)
        if current is not None and current.source_version > summary.source_version:
            return False
        self._summaries[key] = summary
        return True

    async def get_summary(self, tenant_id: str, session_id: str) -> Summary | None:
        return self._summaries.get((tenant_id, session_id))
