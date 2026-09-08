import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from trpc_service.metrics import PlatformMetrics, tracer
from trpc_service.storage.contracts import (
    CoordinationStore,
    DataPlaneStore,
    SessionIdentity,
    TurnCommit,
    TurnCommitResult,
)
from trpc_service.storage.exceptions import DuplicateMessageError
from trpc_service.storage.keys import idempotency_key, session_lock_key


class TurnCoordinator:
    """Enforce locking and idempotency around an atomic turn transaction."""

    def __init__(
        self,
        data_plane: DataPlaneStore,
        coordination: CoordinationStore,
        *,
        lock_ttl_seconds: float = 60,
        lock_wait_seconds: float = 10,
        idempotency_ttl_seconds: int = 86_400,
        metrics: PlatformMetrics | None = None,
    ) -> None:
        self._data_plane = data_plane
        self._coordination = coordination
        self._lock_ttl = lock_ttl_seconds
        self._lock_wait = lock_wait_seconds
        self._idempotency_ttl = idempotency_ttl_seconds
        self._metrics = metrics

    async def _commit_observed(self, request: TurnCommit) -> TurnCommitResult:
        started = time.perf_counter()
        status = "ok"
        try:
            with tracer.start_as_current_span("storage.commit_turn") as span:
                span.set_attribute("trpc.tenant_id", request.identity.tenant_id)
                span.set_attribute("trpc.session_id", request.identity.session_id)
                return await self._data_plane.commit_turn(request)
        except Exception:
            status = "error"
            raise
        finally:
            if self._metrics is not None:
                self._metrics.storage_latency.labels(
                    type(self._data_plane).__name__, "commit_turn", status
                ).observe(time.perf_counter() - started)

    async def commit(self, request: TurnCommit) -> TurnCommitResult:
        event = request.event
        if not event.channel or not event.external_message_id:
            raise ValueError("channel and external_message_id are required for coordinated commits")
        message_key = idempotency_key(
            request.identity.tenant_id,
            event.channel,
            event.external_message_id,
        )
        lock_key = session_lock_key(
            request.identity.tenant_id,
            request.identity.agent_app_id,
            request.identity.session_id,
        )
        async with self._coordination.acquire(
            lock_key,
            ttl_seconds=self._lock_ttl,
            wait_timeout_seconds=self._lock_wait,
        ):
            claimed = await self._coordination.claim(message_key, ttl_seconds=self._idempotency_ttl)
            if not claimed:
                raise DuplicateMessageError("external message is already processing or completed")
            try:
                result = await self._commit_observed(request)
            except Exception:
                await self._coordination.abandon(message_key)
                raise
            await self._coordination.complete(
                message_key,
                {
                    "event_id": result.event.id,
                    "session_version": result.session.version,
                },
                ttl_seconds=self._idempotency_ttl,
            )
            return result

    async def execute(
        self,
        identity: SessionIdentity,
        channel: str,
        external_message_id: str,
        build_turn: Callable[[Mapping[str, Any], int], Awaitable[TurnCommit]],
    ) -> TurnCommitResult:
        """Run an Agent and commit its turn while holding one session lock."""

        message_key = idempotency_key(identity.tenant_id, channel, external_message_id)
        lock_key = session_lock_key(
            identity.tenant_id,
            identity.agent_app_id,
            identity.session_id,
        )
        async with self._coordination.acquire(
            lock_key,
            ttl_seconds=self._lock_ttl,
            wait_timeout_seconds=self._lock_wait,
        ):
            claimed = await self._coordination.claim(message_key, ttl_seconds=self._idempotency_ttl)
            if not claimed:
                raise DuplicateMessageError("external message is already processing or completed")
            try:
                session_started = time.perf_counter()
                session_status = "ok"
                try:
                    with tracer.start_as_current_span("storage.session.read") as span:
                        span.set_attribute("trpc.tenant_id", identity.tenant_id)
                        span.set_attribute("trpc.session_id", identity.session_id)
                        current = await self._data_plane.get_session(identity)
                        if current is None:
                            current = await self._data_plane.create_session(identity)
                            span.set_attribute("trpc.session.created", True)
                except Exception:
                    session_status = "error"
                    raise
                finally:
                    if self._metrics is not None:
                        self._metrics.storage_latency.labels(
                            type(self._data_plane).__name__, "session_read", session_status
                        ).observe(time.perf_counter() - session_started)
                request = await build_turn(current.state, current.version)
                if request.identity != identity:
                    raise ValueError("turn builder changed the session identity")
                if (
                    request.event.channel != channel
                    or request.event.external_message_id != external_message_id
                ):
                    raise ValueError("turn builder changed the idempotency identity")
                result = await self._commit_observed(request)
            except Exception:
                await self._coordination.abandon(message_key)
                raise
            await self._coordination.complete(
                message_key,
                {
                    "event_id": result.event.id,
                    "session_version": result.session.version,
                },
                ttl_seconds=self._idempotency_ttl,
            )
            return result
