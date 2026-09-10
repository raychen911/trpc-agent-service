"""Channel process composition for the shared-state runtime."""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Mapping

from trpc_service.audit.models import PreAuthScope
from trpc_service.channels.contracts import Channel
from trpc_service.channels.delivery import DeliveryService
from trpc_service.channels.feishu import FeishuChannelAdapter, FeishuProviderClient
from trpc_service.channels.identity import ChannelIdentity
from trpc_service.channels.service import ChannelMessageService
from trpc_service.channels.wecom import WeComChannelAdapter, WeComProviderClient
from trpc_service.config.settings import EnvironmentSecretProvider, load_channel_credentials
from trpc_service.storage.contracts import ConfigurationUnavailable
from trpc_service.storage.contracts import LeaseBusy, LeaseLost, StateBackendUnavailable
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.postgres.repositories import PostgresDeliveryRepository
from trpc_service.web.app import build_shared_runtime
from trpc_service.channels.base import AdapterReadiness
from trpc_service.storage.redis_adapter_leases import RedisAdapterOwnershipRepository


class ConnectionRetryPolicy:
    def __init__(self, *, random_source: random.Random | None = None) -> None:
        self._random = random_source or random.Random()
        self._attempt = 0
        self._auth_failed_version: int | None = None
        self.cancelled = False

    def next_delay(self) -> float:
        base = min(30, 2 ** self._attempt)
        self._attempt += 1
        return base * (1 + self._random.random() * 0.2)

    def mark_stable(self, seconds: float) -> None:
        if seconds >= 60:
            self._attempt = 0

    def authentication_failed(self, *, config_version: int) -> None:
        self._auth_failed_version = config_version

    def may_authenticate(self, *, config_version: int) -> bool:
        return self._auth_failed_version is None or config_version != self._auth_failed_version

    def cancel(self) -> None:
        self.cancelled = True


class ManagedChannelRuntime:
    """Own one SDK connection only while holding the shared adapter lease."""

    def __init__(
        self,
        adapter,
        identity: ChannelIdentity,
        node: NodeIdentity,
        ownership,
        *,
        lease_ms: int = 10_000,
        heartbeat_ms: int = 3_000,
        metrics=None,
    ) -> None:
        self.adapter, self.identity, self.node = adapter, identity, node
        self.ownership = ownership
        self.lease_ms, self.heartbeat_ms = lease_ms, heartbeat_ms
        self.metrics = metrics
        self.handle = None
        self.fence = None
        self._readiness = AdapterReadiness.NOT_READY
        self._stopped = False
        self._startup_renew_task: asyncio.Task[None] | None = None
        self._startup_lease_lost = False

    def readiness(self) -> AdapterReadiness:
        return self._readiness

    def _observe(self, *, stage: str, outcome: str, generation: int | None = None) -> None:
        observe = getattr(self.metrics, "observe_channel", None)
        if observe is None:
            return
        try:
            observe(
                PreAuthScope(),
                channel=self.identity.channel,
                stage=stage,
                outcome=outcome,
                duration_ms=0,
                generation=generation,
            )
        except Exception:
            return

    async def start_once(self) -> AdapterReadiness:
        if self._stopped:
            self._stopped = False
        try:
            self.handle = await self.ownership.acquire(
                self.identity.identity_digest, self.node.node_id, self.lease_ms
            )
        except LeaseBusy:
            self._readiness = AdapterReadiness.STANDBY
            self._observe(stage="adapter_connection", outcome="standby")
            return self._readiness
        try:
            self._startup_lease_lost = False
            self._startup_renew_task = asyncio.create_task(
                self._renew_during_start()
            )
            await self.adapter.start(self.identity, self.node)
            if self._startup_lease_lost:
                raise LeaseLost("Adapter ownership lease was lost during startup.")
            bot = self.adapter.runtime_bot_identity
            if bot is None:
                raise LeaseLost("Adapter authentication produced no identity.")
            self.fence = await self.handle.mark_ready(bot)
            self.adapter.replace_adapter_fence(self.fence)
            self._readiness = AdapterReadiness.READY
            self._observe(
                stage="adapter_connection",
                outcome="ready",
                generation=self.fence.generation,
            )
            if self.fence.generation > 1:
                self._observe(
                    stage="takeover",
                    outcome="acquired",
                    generation=self.fence.generation,
                )
            return self._readiness
        except Exception:
            await self.adapter.stop("start_failed")
            await self.handle.release("start_failed")
            self._readiness = AdapterReadiness.NOT_READY
            raise
        finally:
            if self._startup_renew_task is not None:
                self._startup_renew_task.cancel()
                await asyncio.gather(self._startup_renew_task, return_exceptions=True)
                self._startup_renew_task = None

    async def _renew_during_start(self) -> None:
        """Keep ownership alive while a provider SDK establishes its socket."""

        try:
            while True:
                await asyncio.sleep(max(0.01, self.heartbeat_ms / 2000))
                if self.handle is None:
                    return
                try:
                    self.fence = await self.handle.renew(self.lease_ms)
                except (LeaseLost, StateBackendUnavailable):
                    self._startup_lease_lost = True
                    return
        except asyncio.CancelledError:
            return

    async def renew_once(self) -> AdapterReadiness:
        if self.handle is None or self._readiness != AdapterReadiness.READY:
            return self._readiness
        try:
            self.fence = await self.handle.renew(self.lease_ms)
            self.adapter.replace_adapter_fence(self.fence)
        except (LeaseLost, StateBackendUnavailable):
            await self.adapter.stop("lease_lost")
            self._readiness = AdapterReadiness.STANDBY
            self._observe(
                stage="adapter_connection",
                outcome="lost",
                generation=getattr(self.fence, "generation", None),
            )
        return self._readiness

    async def stop(self, reason: str) -> None:
        self._stopped = True
        self._readiness = AdapterReadiness.NOT_READY
        await self.adapter.stop(reason)
        if self.handle is not None:
            await self.handle.release(reason)
        self.handle = None

    async def run_forever(self) -> None:
        while not self._stopped:
            if self._readiness != AdapterReadiness.READY:
                previous = self._readiness
                readiness = await self.start_once()
                if (
                    readiness == AdapterReadiness.READY
                    and previous != AdapterReadiness.READY
                ):
                    print(
                        json.dumps(
                            {
                                "channel": self.identity.channel.value,
                                "readiness": readiness.value,
                                "generation": getattr(self.fence, "generation", None),
                            },
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )
            await asyncio.sleep(
                self.heartbeat_ms / 1000
                if self._readiness == AdapterReadiness.READY
                else min(1.0, self.heartbeat_ms / 1000)
            )
            if self._readiness == AdapterReadiness.READY:
                await self.renew_once()


async def run_channel_process(
    channel: Channel,
    node: NodeIdentity,
    environ: Mapping[str, str],
) -> int:
    """Run one adapter without printing identity values or credential material."""

    tenant_key_names = {
        Channel.FEISHU: "LARK_TENANT_KEY",
        Channel.WECOM: "WECOM_CORP_ID",
    }
    tenant_key = environ.get(tenant_key_names[channel], "").strip()
    if not tenant_key:
        raise ConfigurationUnavailable("Trusted channel identity is unavailable.")
    credentials = load_channel_credentials(
        channel,
        EnvironmentSecretProvider(environ),
    )
    app_or_bot_id = credentials.app_or_bot_id.reveal().decode("utf-8")
    identity = ChannelIdentity(
        channel=channel,
        provider_tenant_key=tenant_key,
        provider_app_or_bot_id=app_or_bot_id,
    )
    runtime_environ = dict(environ)
    runtime_environ["TRPC_RUNTIME_PROFILE"] = "shared"
    runtime_environ["TRPC_NODE_ID"] = node.node_id
    runtime = await build_shared_runtime(runtime_environ)
    ownership = RedisAdapterOwnershipRepository(runtime.adapters.redis)
    delivery = DeliveryService(
        PostgresDeliveryRepository(
            runtime.adapters.database, fence_validator=ownership.validate_fence
        )
    )
    service = ChannelMessageService(
        runtime.adapters.configuration,
        runtime.gateway,
        delivery,
        preauth_audit=runtime.adapters.audit,
    )
    if channel == Channel.FEISHU:
        provider = FeishuProviderClient(identity, credentials.app_or_bot_id)
        adapter = FeishuChannelAdapter(
            provider=provider,
            credential_secret=credentials.secret,
            message_service=service,
        )
    else:
        provider = WeComProviderClient(identity, credentials.app_or_bot_id)
        adapter = WeComChannelAdapter(
            provider=provider,
            credential_secret=credentials.secret,
            message_service=service,
        )
    try:
        managed = ManagedChannelRuntime(
            adapter, identity, node, ownership, metrics=runtime.metrics
        )
        readiness = await managed.start_once()
        print(json.dumps({"channel": channel.value, "readiness": readiness.value}, separators=(",", ":")))
        await managed.run_forever()
    finally:
        if "managed" in locals():
            await managed.stop("process_exit")
        else:
            await adapter.stop("process_exit")
        await runtime.close()
    return 0


__all__ = ["run_channel_process"]
