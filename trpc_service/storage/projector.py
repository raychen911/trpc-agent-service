"""Ordered post-turn projection into rebuildable backends."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from trpc_service.storage.models import OutboxRecord
from trpc_service.storage.protocols import ProjectionStore, RuntimeRepository, SessionStore


class PostTurnProjector:
    _MAX_ATTEMPTS = 5

    def __init__(
        self,
        repository: RuntimeRepository,
        session_projection: ProjectionStore,
        *,
        owner_id: str,
        session_store: SessionStore | None = None,
    ) -> None:
        self._repository = repository
        self._session_projection = session_projection
        self._owner_id = owner_id
        self._session_store = session_store

    async def project_once(self, *, stop_event: asyncio.Event | None = None) -> int:
        if stop_event is not None and stop_event.is_set():
            return 0
        records = await self._repository.claim_outbox(
            event_type="post_turn.ready",
            owner_id=self._owner_id,
            # A graceful-stop run claims one record at a time.  This keeps a
            # stop signal from leaving an unprocessed batch of new claims
            # behind; the default one-shot API retains its throughput.
            limit=1 if stop_event is not None else 100,
            lease_for=timedelta(seconds=60),
        )
        completed = 0
        for record in records:
            try:
                session_id = str(record.payload["session_id"])
                if self._session_store is None:
                    snapshot = await self._repository.get_session_snapshot(
                        record.tenant_id, session_id
                    )
                else:
                    snapshot = await self._session_store.get_snapshot(record.tenant_id, session_id)
                if snapshot is None:
                    raise LookupError("session snapshot is not visible")
                await self._session_projection.put_session(
                    record.tenant_id,
                    session_id,
                    sequence=snapshot.next_sequence - 1,
                    value=snapshot.model_dump(mode="json"),
                )
                await self._repository.mark_outbox_published(
                    record.tenant_id, record.outbox_id, owner_id=self._owner_id
                )
                completed += 1
            except Exception as error:
                await self._recover_failed_record(record, error)
        return completed

    async def _recover_failed_record(self, record: OutboxRecord, error: Exception) -> None:
        reason = (
            "session_not_visible"
            if isinstance(error, LookupError)
            else (f"projection:{type(error).__name__}")
        )
        try:
            if record.attempts >= self._MAX_ATTEMPTS:
                await self._repository.dead_letter_outbox(
                    record,
                    owner_id=self._owner_id,
                    reason=reason,
                )
            else:
                delay = timedelta(seconds=min(60, 2 ** max(0, record.attempts - 1)))
                await self._repository.release_outbox(
                    record.tenant_id,
                    record.outbox_id,
                    owner_id=self._owner_id,
                    delay=delay,
                    error_type=reason,
                )
        except Exception:
            # A stale claim is already recoverable by the outbox lease.  Do
            # not allow one record's cleanup race to terminate the projector.
            return

    async def run(
        self, *, poll_seconds: float = 0.5, stop_event: asyncio.Event | None = None
    ) -> None:
        while stop_event is None or not stop_event.is_set():
            try:
                claimed = await self.project_once(stop_event=stop_event)
            except asyncio.CancelledError:
                raise
            except Exception:
                claimed = 0
            if claimed == 0:
                if stop_event is None:
                    await asyncio.sleep(poll_seconds)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=poll_seconds)
                    except TimeoutError:
                        pass


__all__ = ["PostTurnProjector"]
