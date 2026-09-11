"""Durable, leased Summary and Memory jobs."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from trpc_service.persistence import Database
from trpc_service.persistence.models import PostTurnTaskRow
from trpc_service.persistence.models import utcnow
from .execution import SessionBusyError


@dataclass(frozen=True)
class PostTurnTask:
    task_id: str
    tenant_id: str
    session_id: str
    task_type: str
    source_event_seq: int
    attempt: int


class PostTurnRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def enqueue(self, tenant_id: str, session_id: str, task_type: str, source_event_seq: int) -> str:
        if task_type not in {"summary", "memory"}:
            raise ValueError("post-turn task type must be summary or memory")
        task_id = uuid.uuid4().hex
        with self._database.sessions() as session:
            session.add(
                PostTurnTaskRow(
                    task_id=task_id,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    task_type=task_type,
                    source_event_seq=source_event_seq,
                    status="pending",
                ))
            try:
                session.commit()
                return task_id
            except IntegrityError:
                session.rollback()
                row = session.scalar(
                    select(PostTurnTaskRow).where(
                        PostTurnTaskRow.tenant_id == tenant_id,
                        PostTurnTaskRow.session_id == session_id,
                        PostTurnTaskRow.task_type == task_type,
                        PostTurnTaskRow.source_event_seq == source_event_seq,
                    ))
                assert row is not None
                return row.task_id

    def claim(self, *, limit: int, worker_id: str, lease_seconds: float) -> list[PostTurnTask]:
        now = utcnow()
        with self._database.sessions.begin() as session:
            rows = list(
                session.scalars(
                    select(PostTurnTaskRow).where(
                        or_(
                            PostTurnTaskRow.status.in_(("pending", "retry")),
                            and_(
                                PostTurnTaskRow.status == "running",
                                PostTurnTaskRow.lease_expires_at < now,
                            ),
                        ),
                        or_(PostTurnTaskRow.next_retry_at.is_(None), PostTurnTaskRow.next_retry_at <= now),
                    ).order_by(PostTurnTaskRow.created_at).limit(limit).with_for_update(skip_locked=True)))
            for row in rows:
                row.status = "running"
                row.lease_owner = worker_id
                row.lease_expires_at = now + timedelta(seconds=lease_seconds)
                row.attempt += 1
            return [
                PostTurnTask(row.task_id, row.tenant_id, row.session_id, row.task_type, row.source_event_seq,
                             row.attempt) for row in rows
            ]

    def complete(self, task_id: str, worker_id: str) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(PostTurnTaskRow, task_id)
            if row is None or row.lease_owner != worker_id:
                raise RuntimeError("post-turn task is not owned by this worker")
            row.status = "succeeded"
            row.lease_owner = None
            row.lease_expires_at = None

    def fail(self, task_id: str, worker_id: str, error_type: str, *, retry: bool, delay_seconds: float,
             count_attempt: bool = True) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(PostTurnTaskRow, task_id)
            if row is None or row.lease_owner != worker_id:
                raise RuntimeError("post-turn task is not owned by this worker")
            row.status = "retry" if retry else "failed"
            if not count_attempt:
                row.attempt -= 1
            row.error_type = error_type
            row.next_retry_at = utcnow() + timedelta(seconds=delay_seconds) if retry else None
            row.lease_owner = None
            row.lease_expires_at = None


class DurablePostTurnWorker:
    """Claim and execute persisted Summary/Memory jobs after process restarts."""

    def __init__(self, repository: PostTurnRepository, processor, *, worker_id: str, max_attempts: int = 3) -> None:
        self._repository = repository
        self._processor = processor
        self.worker_id = worker_id
        self.max_attempts = max_attempts

    async def run_once(self) -> bool:
        tasks = self._repository.claim(limit=1, worker_id=self.worker_id, lease_seconds=60)
        if not tasks:
            return False
        task = tasks[0]
        try:
            import asyncio

            await asyncio.wait_for(self._processor(task), timeout=45)
        except SessionBusyError:
            self._repository.fail(task.task_id, self.worker_id, "session_busy", retry=True,
                                  delay_seconds=0.5, count_attempt=False)
            return True
        except Exception as error:
            self._repository.fail(
                task.task_id,
                self.worker_id,
                type(error).__name__,
                retry=task.attempt < self.max_attempts,
                delay_seconds=min(2**task.attempt, 60),
            )
            return True
        self._repository.complete(task.task_id, self.worker_id)
        return True
