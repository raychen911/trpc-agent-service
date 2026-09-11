"""Monotonic summary version and covered-event watermark commits."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import desc
from sqlalchemy import select

from trpc_service.persistence import Database
from trpc_service.persistence.models import SessionSummaryRow
from trpc_service.storage import ConflictError


@dataclass(frozen=True)
class SummarySnapshot:
    version: int
    covered_event_seq: int
    summary_text: str
    model_version: str


class SummaryRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def latest(self, tenant_id: str, session_id: str) -> SummarySnapshot | None:
        with self._database.sessions() as session:
            row = session.scalar(
                select(SessionSummaryRow).where(
                    SessionSummaryRow.tenant_id == tenant_id,
                    SessionSummaryRow.session_id == session_id,
                ).order_by(desc(SessionSummaryRow.version)).limit(1))
            if row is None:
                return None
            return SummarySnapshot(row.version, row.covered_event_seq, row.summary_text, row.model_version)

    def commit(self, tenant_id, session_id, covered_event_seq, summary_text, model_version) -> int:
        with self._database.sessions.begin() as session:
            current = session.scalar(
                select(SessionSummaryRow).where(
                    SessionSummaryRow.tenant_id == tenant_id,
                    SessionSummaryRow.session_id == session_id,
                ).order_by(desc(SessionSummaryRow.version)).limit(1).with_for_update())
            if current is not None and covered_event_seq < current.covered_event_seq:
                raise ConflictError("summary watermark cannot move backwards")
            if current is not None and covered_event_seq == current.covered_event_seq:
                if current.summary_text != summary_text or current.model_version != model_version:
                    raise ConflictError("summary watermark was reused with different content")
                return current.version
            version = 1 if current is None else current.version + 1
            session.add(
                SessionSummaryRow(
                    tenant_id=tenant_id,
                    session_id=session_id,
                    version=version,
                    summary_id=uuid.uuid4().hex,
                    covered_event_seq=covered_event_seq,
                    summary_text=summary_text,
                    model_version=model_version,
                    replaces_version=current.version if current else None,
                ))
            return version
