"""Tenant and session routing tests."""

from datetime import datetime
from datetime import timezone

import pytest
from pydantic import ValidationError

from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.tenant import ChannelBindingRegistry
from trpc_service.tenant import InboundRouteRequest
from trpc_service.tenant import MessageRouter
from trpc_service.tenant import RouteNotFoundError
from trpc_service.tenant import SessionIdentityFactory
from trpc_service.tenant import verify_parse_and_route_callback

from .conftest import build_tenant


def build_router(*tenants):
    keys = {tenant.tenant_id: (tenant.tenant_id.encode("utf-8") * 32)[:32] for tenant in tenants}
    return MessageRouter(ChannelBindingRegistry(tenants), SessionIdentityFactory(keys.__getitem__))


def direct_request(webhook_public_id: str = "callback_acme") -> InboundRouteRequest:
    return InboundRouteRequest(
        channel=ChannelType.WECOM,
        webhook_public_id=webhook_public_id,
        external_user_id="external_user_42",
        conversation_type=ConversationType.DIRECT,
    )


def test_direct_message_route_is_stable_and_partitioned_by_session(tenant):
    router = build_router(tenant)
    first = router.route(direct_request())
    second = router.route(direct_request())

    assert first == second
    assert first.tenant_id == tenant.tenant_id
    assert first.agent_app_id == "customer_support"
    assert first.partition_key == f"{tenant.tenant_id}:{first.session_id}"


def test_different_groups_get_different_sessions(tenant):
    router = build_router(tenant)
    base = {
        "channel": ChannelType.WECOM,
        "webhook_public_id": "callback_acme",
        "external_user_id": "external_user_42",
        "conversation_type": ConversationType.GROUP,
    }
    group_a = router.route(InboundRouteRequest(**base, external_chat_id="group_a"))
    group_b = router.route(InboundRouteRequest(**base, external_chat_id="group_b"))

    assert group_a.actor_id == group_b.actor_id
    assert group_a.internal_user_id != group_b.internal_user_id
    assert group_a.session_id != group_b.session_id


def test_same_external_user_is_isolated_across_tenants():
    tenant_a = build_tenant("tenant_acme", "callback_acme")
    tenant_b = build_tenant("tenant_beta", "callback_beta")
    router = build_router(tenant_a, tenant_b)

    route_a = router.route(direct_request("callback_acme"))
    route_b = router.route(direct_request("callback_beta"))

    assert route_a.internal_user_id != route_b.internal_user_id
    assert route_a.session_id != route_b.session_id


def test_unregistered_callback_cannot_choose_a_tenant(tenant):
    router = build_router(tenant)
    with pytest.raises(RouteNotFoundError):
        router.route(direct_request("unknown_callback"))


def test_group_route_requires_chat_identity():
    with pytest.raises(ValidationError, match="external_chat_id"):
        InboundRouteRequest(
            channel=ChannelType.WECOM,
            webhook_public_id="callback_acme",
            external_user_id="external_user_42",
            conversation_type=ConversationType.GROUP,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "suspended"),
        ("status", "disabled"),
        ("binding_enabled", False),
        ("application_enabled", False),
    ],
)
def test_inactive_tenant_binding_or_application_cannot_route(tenant, field, value):
    data = tenant.model_dump()
    if field == "status":
        data["status"] = value
    elif field == "binding_enabled":
        data["channel_bindings"][0]["enabled"] = value
    else:
        data["applications"][0]["enabled"] = value
    inactive_tenant = type(tenant).model_validate(data)

    with pytest.raises(RouteNotFoundError):
        build_router(inactive_tenant).route(direct_request())


def test_threads_in_same_group_get_different_sessions(tenant):
    router = build_router(tenant)
    base = {
        "channel": ChannelType.WECOM,
        "webhook_public_id": "callback_acme",
        "external_user_id": "external_user_42",
        "conversation_type": ConversationType.THREAD,
        "external_chat_id": "group_a",
    }

    thread_a = router.route(InboundRouteRequest(**base, thread_id="thread_a"))
    thread_b = router.route(InboundRouteRequest(**base, thread_id="thread_b"))

    assert thread_a.session_id != thread_b.session_id


def test_short_tenant_hmac_key_fails_without_fallback(tenant):
    router = MessageRouter(ChannelBindingRegistry([tenant]), SessionIdentityFactory(lambda _: b"short"))

    with pytest.raises(ValueError, match="at least 32 bytes"):
        router.route(direct_request())


@pytest.mark.asyncio
async def test_callback_is_verified_before_parsing_and_routing(tenant):
    calls = []

    class FakeAdapter:
        async def verify_callback(self, request, binding):
            calls.append(("verify", binding.binding_id))

        async def parse_inbound(self, request, binding):
            calls.append(("parse", binding.binding_id))
            return NormalizedInboundMessage(
                channel=ChannelType.WECOM,
                webhook_public_id="callback_acme",
                external_message_id="message_1",
                external_user_id="external_user_42",
                conversation_type=ConversationType.DIRECT,
                text="hello",
                received_at=datetime.now(timezone.utc),
                request_id="request_1",
            )

    registry = ChannelBindingRegistry([tenant])
    router = MessageRouter(
        registry,
        SessionIdentityFactory(lambda _: b"a" * 32),
    )
    _, resolved = await verify_parse_and_route_callback(
        adapter=FakeAdapter(),
        raw_request=object(),
        channel=ChannelType.WECOM,
        webhook_public_id="callback_acme",
        registry=registry,
        router=router,
    )

    assert calls == [("verify", "binding_primary"), ("parse", "binding_primary")]
    assert resolved.tenant_id == "tenant_acme"
