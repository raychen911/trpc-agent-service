from __future__ import annotations

import os
from datetime import UTC, datetime

from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.models import BindingRoute
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
)

_LIVE_TEST_ENV_PREFIXES = (
    "TRPC_RUN_REAL_",
    "TRPC_REAL_",
    "TRPC_FAULT_",
    "TRPC_RUN_FAULT_",
    "TRPC_K8S_RUNTIME_",
    "TRPC_IM_ONLINE_",
    "TRPC_MIGRATION_",
    "TRPC_PERF_",
    "TRPC_TEST_",
    "TRPC_E2E_",
)


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--allow-real-tests",
        action="store_true",
        default=False,
        help="preserve explicitly supplied real acceptance environment variables",
    )


def pytest_configure(config) -> None:
    if config.getoption("--allow-real-tests"):
        return
    for name in tuple(os.environ):
        if name.startswith(_LIVE_TEST_ENV_PREFIXES) or name == "TRPC_SERVICE_ONLINE_TESTS_ENABLED":
            os.environ.pop(name, None)


def tenant_config(*, tenant_id: str = "tenant-a", version: int = 1) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        app_id="support",
        version=version,
        model=ModelPolicy(provider="openai", model="fake-model"),
        storage=StorageSelection(profile_id="default"),
    )


def binding(
    *,
    tenant_id: str = "tenant-a",
    channel: Channel = Channel.FEISHU,
    binding_id: str = "binding-unpredictable-a",
    account_id: str = "cli_account_a",
) -> ChannelBinding:
    return ChannelBinding(
        binding_id=binding_id,
        tenant_id=tenant_id,
        app_id="support",
        channel=channel,
        account_id=account_id,
    )


def repository(*, tenant_id: str = "tenant-a") -> InMemoryRuntimeRepository:
    value = InMemoryRuntimeRepository()
    config = tenant_config(tenant_id=tenant_id)
    route_binding = binding(tenant_id=tenant_id)
    value.add_config(config)
    value.add_route(BindingRoute(binding=route_binding, active_config_version=1))
    return value


def envelope(
    message_id: str = "message-1",
    *,
    user_id: str = "user-1",
    account_id: str = "cli_account_a",
    text: str = "hello",
) -> InboundEnvelope:
    return InboundEnvelope(
        channel=Channel.FEISHU,
        account_id=account_id,
        external_message_id=message_id,
        external_user_id=user_id,
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text=text,
        occurred_at=datetime(2026, 8, 21, tzinfo=UTC),
    )
