from __future__ import annotations

import pytest

from tests.conftest import envelope, repository, tenant_config
from trpc_service.runtime import BindingMismatch, TenantRuntime, UnknownBinding
from trpc_service.storage.models import BindingRoute
from trpc_service.tenant.models import Channel, ChannelBinding, ConversationKind
from trpc_service.tenant.session_id import (
    make_principal_id,
    make_session_id,
    rollout_bucket,
    select_config_version,
)

KEY = b"r" * 32


def test_session_ids_are_stable_and_isolated() -> None:
    common = {
        "key": KEY,
        "binding_id": "binding",
        "kind": ConversationKind.DIRECT,
        "external_user_id": "same-user",
    }
    first = make_session_id(tenant_id="tenant-a", **common)
    assert first == make_session_id(tenant_id="tenant-a", **common)
    assert first != make_session_id(tenant_id="tenant-b", **common)
    assert "same-user" not in first
    assert make_principal_id(
        KEY,
        tenant_id="tenant-a",
        binding_id="binding",
        external_user_id="same-user",
    ).startswith("p1_")


def test_group_session_requires_and_uses_chat_id() -> None:
    with pytest.raises(ValueError, match="group"):
        make_session_id(
            KEY,
            tenant_id="tenant",
            binding_id="binding",
            kind=ConversationKind.GROUP,
            external_user_id="user",
        )
    left = make_session_id(
        KEY,
        tenant_id="tenant",
        binding_id="binding",
        kind=ConversationKind.GROUP,
        external_user_id="user-a",
        external_conversation_id="chat",
    )
    right = make_session_id(
        KEY,
        tenant_id="tenant",
        binding_id="binding",
        kind=ConversationKind.GROUP,
        external_user_id="user-b",
        external_conversation_id="chat",
    )
    assert left == right


def test_rollout_is_stable_and_boundaries_are_exact() -> None:
    bucket = rollout_bucket(KEY, tenant_id="t", app_id="a", session_id="s")
    assert bucket == rollout_bucket(KEY, tenant_id="t", app_id="a", session_id="s")
    assert (
        select_config_version(
            active_version=1, candidate_version=2, candidate_percent=0, bucket=bucket
        )
        == 1
    )
    assert (
        select_config_version(
            active_version=1, candidate_version=2, candidate_percent=100, bucket=bucket
        )
        == 2
    )
    with pytest.raises(ValueError):
        select_config_version(
            active_version=1, candidate_version=2, candidate_percent=101, bucket=0
        )


@pytest.mark.asyncio
async def test_runtime_pins_config_and_deduplicates() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=KEY)
    first = await runtime.accept("binding-unpredictable-a", envelope())
    duplicate = await runtime.accept("binding-unpredictable-a", envelope())
    assert first.inbound_id == duplicate.inbound_id
    assert duplicate.duplicate
    assert first.context.config_version == 1
    assert first.context.tenant_id == "tenant-a"


@pytest.mark.asyncio
async def test_runtime_rejects_unknown_and_mismatched_bindings() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=KEY)
    with pytest.raises(UnknownBinding):
        await runtime.accept("missing", envelope())
    with pytest.raises(BindingMismatch):
        await runtime.accept("binding-unpredictable-a", envelope(account_id="wrong"))

    other = ChannelBinding(
        binding_id="disabled-binding",
        tenant_id="tenant-a",
        app_id="support",
        channel=Channel.FEISHU,
        account_id="cli_account_a",
        enabled=False,
    )
    repo.add_route(BindingRoute(binding=other, active_config_version=1))
    repo.add_config(tenant_config())
    with pytest.raises(UnknownBinding):
        await runtime.accept("disabled-binding", envelope())
