"""Authenticated media staging before durable IM ingress."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from trpc_service.channels.acceptance import test_tenant as build_tenant
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.channels.runtime import ChannelRuntime
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory


def image_frame():
    return {
        "header": {
            "app_id": "cli_test",
            "tenant_key": "enterprise",
            "event_id": "event-media",
            "event_type": "im.message.receive_v1",
        },
        "event": {
            "sender": {
                "sender_type": "user",
                "tenant_key": "enterprise",
                "sender_id": {"open_id": "ou_user"},
            },
            "message": {
                "message_id": "om_media",
                "chat_id": "oc_chat",
                "chat_type": "p2p",
                "message_type": "image",
                "content": json.dumps({"image_key": "img_synthetic"}),
            },
        },
    }


@pytest.mark.asyncio
async def test_authorized_media_is_hashed_staged_and_replaced_by_internal_uri():
    tenant = build_tenant("feishu", "cli_test")
    binding = tenant.channel_bindings[0]
    identities = SessionIdentityFactory(lambda _: b"k" * 32)
    router = MessageRouter(ChannelBindingRegistry([tenant]), identities)
    adapter = FeishuAdapter("cli_test", "synthetic")
    adapter.download_media = AsyncMock(return_value=(b"image-bytes", "photo.png", "image/png"))
    event = adapter.normalize(image_frame(), binding)
    route = router.route_message(event.message, expected_binding=binding)
    service = SimpleNamespace(load_artifact=AsyncMock(return_value=None),
                              save_artifact=AsyncMock(return_value=3))
    runtime = SimpleNamespace(
        ingress=SimpleNamespace(authorized=Mock(return_value=True)),
        store=SimpleNamespace(route_message=Mock(return_value=route)),
        artifact_service=AsyncMock(return_value=service),
    )
    try:
        staged = await ChannelRuntime.materialize_media(runtime, event, adapter, tenant, binding, router)
        attachment = staged.message.attachments[0]
        assert staged.pending_media == () and staged.message.text == ""
        assert attachment.artifact_id.startswith("memory://")
        assert attachment.filename.endswith("-photo.png")
        assert attachment.size_bytes == len(b"image-bytes")
        assert len(attachment.sha256) == 64
        service.save_artifact.assert_awaited_once()

        service.load_artifact.return_value = SimpleNamespace(
            version=SimpleNamespace(version=3, custom_metadata={"sha256": attachment.sha256}))
        repeated = await ChannelRuntime.materialize_media(runtime, event, adapter, tenant, binding, router)
        assert repeated.message.attachments == staged.message.attachments
        service.save_artifact.assert_awaited_once()
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_unauthorized_media_never_downloads_or_reaches_object_storage():
    tenant = build_tenant("feishu", "cli_test")
    binding = tenant.channel_bindings[0]
    router = MessageRouter(ChannelBindingRegistry([tenant]), SessionIdentityFactory(lambda _: b"k" * 32))
    adapter = FeishuAdapter("cli_test", "synthetic")
    adapter.download_media = AsyncMock()
    event = adapter.normalize(image_frame(), binding)
    runtime = SimpleNamespace(
        ingress=SimpleNamespace(authorized=Mock(return_value=False)),
        store=SimpleNamespace(route_message=Mock()),
        artifact_service=AsyncMock(),
    )
    try:
        rejected = await ChannelRuntime.materialize_media(runtime, event, adapter, tenant, binding, router)
        assert rejected.pending_media == ()
        adapter.download_media.assert_not_awaited()
        runtime.artifact_service.assert_not_awaited()
    finally:
        await adapter.close()
