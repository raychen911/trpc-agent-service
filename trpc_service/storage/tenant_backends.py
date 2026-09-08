import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.domain import BackendKind
from trpc_service.storage.contracts import (
    OutboxRecord,
    OutboxStore,
    SessionIdentity,
    TurnCommit,
    TurnCommitResult,
)
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.models import AgentApp, BackendConfig
from trpc_service.tenant.errors import InvalidStateError


@dataclass(frozen=True, slots=True)
class EffectiveBackend:
    kind: BackendKind
    configured_type: str | None
    effective_type: str
    source: str
    runtime_supported: bool
    secret_ref: str | None = None
    options: Mapping[str, Any] | None = None


class TenantBackendResolver:
    """Resolve the active, versioned backend choice for one tenant Agent App."""

    SUPPORTED_SESSION_BACKENDS = frozenset({"sql", "inmemory"})

    def __init__(
        self,
        factory: sessionmaker[Session],
        default_session_backend: str,
        *,
        allow_inmemory_session: bool = True,
    ) -> None:
        self._factory = factory
        self._default_session_backend = default_session_backend
        self._allow_inmemory_session = allow_inmemory_session

    async def session_backend(self, identity: SessionIdentity) -> str:
        selected = await self.resolve(
            identity.tenant_id, identity.agent_app_id, BackendKind.SESSION
        )
        return selected.effective_type

    async def resolve(
        self, tenant_id: str, agent_app_id: str, kind: BackendKind
    ) -> EffectiveBackend:
        return await asyncio.to_thread(self._resolve_sync, tenant_id, agent_app_id, kind)

    def _resolve_sync(
        self, tenant_id: str, agent_app_id: str, kind: BackendKind
    ) -> EffectiveBackend:
        with self._factory() as session:
            row = session.scalar(
                select(BackendConfig)
                .join(
                    AgentApp,
                    (AgentApp.id == BackendConfig.agent_app_id)
                    & (AgentApp.tenant_id == BackendConfig.tenant_id),
                )
                .where(
                    AgentApp.id == agent_app_id,
                    AgentApp.tenant_id == tenant_id,
                    BackendConfig.config_version == AgentApp.active_version,
                    BackendConfig.backend_kind == kind,
                )
            )
        configured = row.backend_type.lower() if row is not None else None
        if kind == BackendKind.SESSION:
            if configured == "inmemory" and not self._allow_inmemory_session:
                raise InvalidStateError(
                    "production does not allow tenant session backend=inmemory; use sql"
                )
            supported = configured in self.SUPPORTED_SESSION_BACKENDS if configured else True
            effective = configured if supported and configured else self._default_session_backend
        else:
            # Other backend kinds are exposed for configuration/audit. Their runtime
            # adapters continue to use the platform default until a routed adapter exists.
            supported = not configured
            effective = configured or "platform-default"
        return EffectiveBackend(
            kind=kind,
            configured_type=configured,
            effective_type=effective,
            source="agent-app" if configured and supported else "platform-default",
            runtime_supported=supported,
            secret_ref=row.secret_ref if row else None,
            options=dict(row.options) if row else {},
        )

    async def all_effective(self, tenant_id: str, agent_app_id: str) -> list[EffectiveBackend]:
        return [
            await self.resolve(tenant_id, agent_app_id, kind)
            for kind in BackendKind
        ]


class TenantTurnCoordinator:
    """Route each turn to the coordinator selected by the active app revision."""

    def __init__(
        self,
        resolver: TenantBackendResolver,
        coordinators: Mapping[str, TurnCoordinator],
    ) -> None:
        self._resolver = resolver
        self._coordinators = dict(coordinators)

    async def _for(self, identity: SessionIdentity) -> TurnCoordinator:
        backend = await self._resolver.session_backend(identity)
        return self._coordinators[backend]

    async def commit(self, request: TurnCommit) -> TurnCommitResult:
        return await (await self._for(request.identity)).commit(request)

    async def execute(
        self,
        identity: SessionIdentity,
        channel: str,
        external_message_id: str,
        build_turn: Callable[[Mapping[str, Any], int], Awaitable[TurnCommit]],
    ) -> TurnCommitResult:
        return await (await self._for(identity)).execute(
            identity, channel, external_message_id, build_turn
        )


class CompositeOutboxStore:
    """Poll SQL and in-memory outboxes so either tenant session backend can sync."""

    def __init__(self, stores: Sequence[OutboxStore]) -> None:
        self._stores = tuple(stores)
        self._owners: dict[str, OutboxStore] = {}
        self._guard = asyncio.Lock()

    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[OutboxRecord]:
        claimed: list[OutboxRecord] = []
        for store in self._stores:
            if len(claimed) >= limit:
                break
            records = await store.claim_batch(worker_id, limit - len(claimed))
            async with self._guard:
                for record in records:
                    self._owners[record.id] = store
            claimed.extend(records)
        return tuple(claimed)

    async def _owner(self, record_id: str) -> OutboxStore:
        async with self._guard:
            return self._owners[record_id]

    async def mark_processed(self, record_id: str) -> None:
        store = await self._owner(record_id)
        await store.mark_processed(record_id)

    async def mark_failed(self, record_id: str, error: str) -> None:
        store = await self._owner(record_id)
        await store.mark_failed(record_id, error)

    async def mark_dead_letter(self, record_id: str, error: str) -> None:
        store = await self._owner(record_id)
        handler = getattr(store, "mark_dead_letter", None)
        if handler is not None:
            await handler(record_id, error)
        else:
            await store.mark_failed(record_id, error)
