"""Per-session asynchronous leases without a global execution lock."""

from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import uuid4

from trpc_service.storage.contracts import LeaseLost, SessionBusy
from trpc_service.storage.models import NodeIdentity, SessionFence


@dataclass(slots=True)
class InMemoryLease:
    """Process-local implementation of the shared lease contract for contract tests.

    Correctness in the shared profile never depends on this class.  It exists so
    local and shared adapters expose the same async ownership semantics.
    """

    manager: "SessionLockManager"
    session_id: str
    fence: SessionFence
    _lock: asyncio.Lock
    released: bool = False

    async def renew(self, lease_ms: int) -> SessionFence:
        if self.released or not self._lock.locked():
            raise LeaseLost("The local session lease is no longer owned.")
        self.fence = self.fence.model_copy(
            update={"expires_at": datetime.now(timezone.utc) + timedelta(milliseconds=lease_ms)}
        )
        return self.fence

    async def release(self) -> None:
        if not self.released:
            self.released = True
            if self._lock.locked():
                self._lock.release()

    async def __aenter__(self) -> "InMemoryLease":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.release()


class SessionLockManager:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()

        self._generations: dict[str, int] = {}

    @asynccontextmanager
    async def _legacy_acquire(self, platform_session_id: str) -> AsyncIterator[None]:
        if re.fullmatch(r"sess_[0-9a-f]{64}", platform_session_id) is None:
            raise ValueError("a tenant-scoped platform session id is required")
        async with self._guard:
            lock = self._locks.setdefault(platform_session_id, asyncio.Lock())
        await lock.acquire()
        try:
            yield
        finally:
            if lock.locked():
                lock.release()

    def acquire(self, platform_session_id: str, *args: object, **kwargs: object):
        """Acquire either the 002 context-manager lock or the 003 lease port.

        The compact one-argument form preserves the established Gateway API.
        Supplying the shared-port arguments returns an acquired lease coroutine.
        """

        if not args and not kwargs:
            return self._legacy_acquire(platform_session_id)
        return self._acquire_lease(platform_session_id, *args, **kwargs)

    async def _acquire_lease(
        self,
        platform_session_id: str,
        agent_id: str,
        session_key: str,
        message_key_digest: str,
        node_identity: NodeIdentity,
        lease_ms: int,
        wait_ms: int,
    ) -> InMemoryLease:
        del agent_id, message_key_digest
        if re.fullmatch(r"sess_[0-9a-f]{64}", session_key) is None:
            raise ValueError("a tenant-scoped platform session id is required")
        async with self._guard:
            lock = self._locks.setdefault(session_key, asyncio.Lock())
        try:
            await asyncio.wait_for(lock.acquire(), timeout=max(wait_ms, 1) / 1000)
        except TimeoutError:
            raise SessionBusy("The session is currently being processed.") from None
        generation = self._generations.get(session_key, 0) + 1
        self._generations[session_key] = generation
        digest = sha256(platform_session_id.encode("utf-8")).hexdigest()
        return InMemoryLease(
            manager=self,
            session_id=session_key,
            fence=SessionFence(
                session_key_digest=digest,
                generation=generation,
                owner_node=node_identity,
                owner_token=uuid4().hex,
                expires_at=datetime.now(timezone.utc) + timedelta(milliseconds=lease_ms),
            ),
            _lock=lock,
        )
