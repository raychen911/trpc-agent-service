"""Lifecycle ownership for the IM services selected by channel bindings."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeAlias
from uuid import UUID

from trpc_service.channels.binding import ChannelBinding
from trpc_service.config.secret_resolver import EnvSecretResolver
from trpc_service.storage.channel_binding_repository import ChannelBindingRepository

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_SECONDS = 5.0
_STARTUP_ERROR = "channel services could not be started"


class ChannelServiceManagerStartupError(RuntimeError):
    """The initial authenticated channel-service set cannot be installed safely."""


class ManagedChannelService(Protocol):

    async def start(self) -> None:
        ...

    async def close(self) -> None:
        ...


ChannelServiceFactory: TypeAlias = Callable[[ChannelBinding, str],
                                            ManagedChannelService | Awaitable[ManagedChannelService]]


class ChannelServiceManager:
    """Keep exactly one authenticated long-connection service per binding.

    The repository is intentionally queried through ``list_enabled``.  It is
    a small global read needed by a gateway process; all message authorization
    remains the repository's exact channel/account lookup.
    """

    def __init__(
        self,
        repository: ChannelBindingRepository,
        secret_resolver: EnvSecretResolver,
        service_factory: ChannelServiceFactory,
        *,
        refresh_interval: float = REFRESH_INTERVAL_SECONDS,
    ) -> None:
        if refresh_interval <= 0:
            raise ValueError("invalid channel service refresh interval")
        self._repository = repository
        self._secret_resolver = secret_resolver
        self._service_factory = service_factory
        self._refresh_interval = refresh_interval
        self._services: dict[UUID, tuple[int, ManagedChannelService]] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """Install every available service; repository failure blocks startup."""
        if self._closed:
            raise ChannelServiceManagerStartupError(_STARTUP_ERROR)
        if self._started:
            return
        if not await self._refresh(initial=True):
            raise ChannelServiceManagerStartupError(_STARTUP_ERROR)
        self._started = True
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def refresh_once(self) -> bool:
        """Refresh bindings once; failures deliberately leave old services live."""
        if self._closed:
            return False
        return await self._refresh(initial=False)

    async def close(self) -> None:
        """Stop the background refresher and close every owned service once."""
        if self._closed:
            return
        self._closed = True
        task = self._refresh_task
        self._refresh_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        services = tuple(self._services.values())
        self._services.clear()
        for _, service in services:
            await self._close_quietly(service)

    async def _refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._refresh_interval)
                await self.refresh_once()
        except asyncio.CancelledError:
            raise

    async def _refresh(self, *, initial: bool) -> bool:
        try:
            bindings = await self._list_enabled()
            desired = {binding.binding_id: binding for binding in bindings}
            if len(desired) != len(bindings):
                raise ValueError
        except Exception:
            if initial:
                return False
            logger.warning("channel service refresh failed")
            return False

        candidates: dict[UUID, tuple[int, ManagedChannelService]] = {}
        for binding_id, binding in desired.items():
            current = self._services.get(binding_id)
            if current is not None and current[0] == binding.version:
                continue
            service: ManagedChannelService | None = None
            try:
                secret = self._secret_resolver.resolve(binding.secret_ref)
                created = self._service_factory(binding, secret)
                service = await created if inspect.isawaitable(created) else created
                await service.start()
            except Exception:
                if service is not None:
                    await self._close_quietly(service)
                logger.warning("channel binding service unavailable")
                continue
            candidates[binding_id] = (binding.version, service)

        replaced_or_removed: list[ManagedChannelService] = []
        for binding_id in tuple(self._services):
            replacement = candidates.get(binding_id)
            if binding_id not in desired:
                _, service = self._services.pop(binding_id)
                replaced_or_removed.append(service)
            elif replacement is not None:
                _, service = self._services.pop(binding_id)
                replaced_or_removed.append(service)
        self._services.update(candidates)
        for service in replaced_or_removed:
            await self._close_quietly(service)
        return True

    async def _list_enabled(self) -> tuple[ChannelBinding, ...]:
        list_enabled = getattr(self._repository, "list_enabled", None)
        if list_enabled is None:
            raise RuntimeError
        bindings = await list_enabled()
        if not isinstance(bindings, tuple) or any(not isinstance(binding, ChannelBinding) or not binding.enabled
                                                  for binding in bindings):
            raise ValueError
        return bindings

    @staticmethod
    async def _close_quietly(service: ManagedChannelService) -> None:
        try:
            await service.close()
        except Exception:
            logger.warning("channel service close failed")


__all__ = [
    "ChannelServiceFactory",
    "ChannelServiceManager",
    "ChannelServiceManagerStartupError",
    "ManagedChannelService",
    "REFRESH_INTERVAL_SECONDS",
]
