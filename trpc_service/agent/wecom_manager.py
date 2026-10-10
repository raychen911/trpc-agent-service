"""Reconcile one authenticated WeCom connection per active binding."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Any, cast

from trpc_service.channels.base import InboundSink
from trpc_service.channels.envelopes import InboundEnvelope
from trpc_service.channels.wecom import WeComBindingLeaseUnavailable, WeComConnector
from trpc_service.storage.models import BindingRoute
from trpc_service.storage.protocols import RuntimeRepository
from trpc_service.tenant.models import Channel, ChannelBinding

logger = logging.getLogger(__name__)

EmergencyPreparedSink = Callable[[BindingRoute, InboundEnvelope], Awaitable[None]]


class WeComConnectionManager:
    def __init__(
        self,
        repository: RuntimeRepository,
        connector: WeComConnector,
        sink: InboundSink,
        emergency_sink: EmergencyPreparedSink | None = None,
        reconnect_jitter_ratio: float = 0.2,
        standby_retry_seconds: float = 0.5,
        random_fn: Callable[[], float] = random.random,
    ) -> None:
        if not 0 <= reconnect_jitter_ratio <= 1:
            raise ValueError("reconnect_jitter_ratio must be between zero and one")
        if standby_retry_seconds <= 0:
            raise ValueError("standby retry seconds must be positive")
        self._repository = repository
        self._connector = connector
        self._sink = sink
        self._emergency_sink = emergency_sink
        self._tasks: dict[str, asyncio.Task[None]] = {}
        # A binding's secret reference and control version are part of the
        # connection identity.  Keeping a digest here lets reconciliation
        # rotate credentials without ever retaining or logging the secret
        # value itself.
        self._binding_signatures: dict[str, str] = {}
        self._routes: dict[str, BindingRoute] = {}
        self._stop_event = asyncio.Event()
        self._reconnect_jitter_ratio = reconnect_jitter_ratio
        self._standby_retry_seconds = standby_retry_seconds
        self._random = random_fn

    async def reconcile_once(self) -> None:
        listed = await self._repository.list_bindings(Channel.WECOM_AI_BOT)
        bindings = {binding.binding_id: binding for binding in listed}
        if self._emergency_sink is not None:
            refreshed: dict[str, BindingRoute] = {}
            for binding in listed:
                route = await self._repository.resolve_binding(binding.binding_id)
                if (
                    route is not None
                    and route.tenant_active
                    and route.binding.enabled
                    and route.binding.channel == Channel.WECOM_AI_BOT
                ):
                    refreshed[binding.binding_id] = route
            bindings = {binding_id: route.binding for binding_id, route in refreshed.items()}
            self._routes.update(refreshed)
            for binding_id in set(self._routes) - set(bindings):
                self._routes.pop(binding_id, None)
        for binding_id, task in list(self._tasks.items()):
            binding_changed = binding_id in bindings and self._binding_signatures.get(
                binding_id
            ) != _binding_signature(bindings[binding_id])
            if binding_id not in bindings or task.done() or binding_changed:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                elif not task.cancelled() and task.exception():
                    logger.warning(
                        "WeCom connector stopped",
                        extra={
                            "binding_id": binding_id,
                            "error_type": type(task.exception()).__name__,
                        },
                    )
                self._tasks.pop(binding_id, None)
                self._binding_signatures.pop(binding_id, None)
                if binding_id not in bindings:
                    self._routes.pop(binding_id, None)
        for binding_id, binding in bindings.items():
            if binding_id not in self._tasks:
                self._tasks[binding_id] = asyncio.create_task(
                    self._run_binding(binding),
                    name=f"wecom:{binding_id}",
                )
                self._binding_signatures[binding_id] = _binding_signature(binding)

    async def _run_binding(self, binding: ChannelBinding) -> None:
        delay = 0.5
        while not self._stop_event.is_set():
            try:
                runner = self._connector.run
                if _accepts_emergency_sink(runner):
                    await runner(
                        binding,
                        self._sink,
                        self._stop_event,
                        self._emergency_for_binding,
                    )
                elif _accepts_stop_event(runner):
                    await runner(binding, self._sink, self._stop_event)
                else:
                    await runner(binding, self._sink)
            except asyncio.CancelledError:
                raise
            except WeComBindingLeaseUnavailable:
                # A held binding lease means this replica is a healthy standby,
                # not a degraded connector.  Keep takeover polling short and
                # bounded; exponential backoff here used to leave the binding
                # without an authenticated WebSocket for up to 30 seconds after
                # the active replica died.
                delay = 0.5
                await _wait_or_stop(
                    self._stop_event,
                    self._backoff_delay(self._standby_retry_seconds),
                )
            except Exception as error:
                logger.warning(
                    "WeCom connector degraded",
                    extra={"binding_id": binding.binding_id, "error_type": type(error).__name__},
                )
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._backoff_delay(delay)
                    )
                except TimeoutError:
                    delay = min(delay * 2, 30.0)
                else:
                    return
            else:
                delay = 0.5
                if self._stop_event.is_set():
                    return
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._backoff_delay(delay)
                    )
                except TimeoutError:
                    pass

    def _backoff_delay(self, base_seconds: float) -> float:
        """Add bounded positive jitter to reconnect/reconcile backoff.

        The lower bound remains the requested exponential delay.  This avoids
        a fleet of connectors reconnecting in lockstep after a provider outage
        while preserving the provider's minimum Retry-After/backoff signal.
        """

        jitter = base_seconds * self._reconnect_jitter_ratio * self._random()
        return min(base_seconds + max(0.0, jitter), 60.0)

    async def run(
        self,
        *,
        refresh_seconds: float = 30,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        if refresh_seconds < 0:
            raise ValueError("refresh_seconds must be non-negative")
        delay = 0.5
        try:
            while stop_event is None or not stop_event.is_set():
                try:
                    await self.reconcile_once()
                    delay = 0.5
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    logger.warning(
                        "WeCom binding reconciliation degraded",
                        extra={"error_type": type(error).__name__},
                    )
                    await _wait_or_stop(stop_event, self._backoff_delay(delay))
                    delay = min(delay * 2, 30.0)
                    continue
                if stop_event is None:
                    await asyncio.sleep(refresh_seconds)
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=refresh_seconds)
                    except TimeoutError:
                        pass
        finally:
            self._stop_event.set()
            tasks = list(self._tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _emergency_for_binding(
        self,
        binding_id: str,
        envelope: InboundEnvelope,
    ) -> None:
        if self._emergency_sink is None:
            raise RuntimeError("WeCom emergency sink is not configured")
        route = self._routes.get(binding_id)
        if route is None:
            raise RuntimeError("authenticated WeCom route is unavailable")
        await self._emergency_sink(route, envelope)


__all__ = ["WeComConnectionManager"]


def _binding_signature(binding: ChannelBinding) -> str:
    """Return a non-secret identity for a connector configuration.

    Secret values are intentionally not resolved here.  A changed
    ``SecretRef`` URI or control version is enough to force the old client to
    close and the next loop to construct a fresh authenticated connection.
    """

    refs = {key: ref.uri for key, ref in sorted(binding.secret_refs.items())}
    projection = {
        "binding_id": binding.binding_id,
        "tenant_id": binding.tenant_id,
        "app_id": binding.app_id,
        "channel": binding.channel.value,
        "account_id": binding.account_id,
        "secret_refs": refs,
        "enabled": binding.enabled,
        "control_version": binding.control_version,
        "capabilities": sorted(binding.capabilities),
    }
    return hashlib.sha256(
        json.dumps(projection, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _accepts_stop_event(runner: object) -> bool:
    try:
        parameters = tuple(inspect.signature(cast(Callable[..., Any], runner)).parameters.values())
    except (TypeError, ValueError):
        return False
    accepts_varargs = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in parameters
    )
    return accepts_varargs or len(parameters) >= 3


def _accepts_emergency_sink(runner: object) -> bool:
    try:
        parameters = tuple(inspect.signature(cast(Callable[..., Any], runner)).parameters.values())
    except (TypeError, ValueError):
        return False
    accepts_varargs = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL for parameter in parameters
    )
    return accepts_varargs or len(parameters) >= 4


async def _wait_or_stop(stop_event: asyncio.Event | None, seconds: float) -> None:
    if stop_event is None:
        await asyncio.sleep(seconds)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass
