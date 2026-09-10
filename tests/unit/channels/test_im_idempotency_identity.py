from __future__ import annotations

from uuid import uuid4

import pytest

from tests.support_channels import dual_im_settings
from trpc_service.channels.contracts import Channel
from trpc_service.channels.contracts import VerifiedBindingScope
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.models import IdempotencyKey
from trpc_service.storage.redis_codec import RedisKeyCodec
from trpc_service.tenant.session_identity import (
    assert_session_ownership,
    derive_session_identity,
)


def test_im_idempotency_key_contains_channel_and_cannot_collide_across_scope() -> None:
    codec = RedisKeyCodec(namespace="phase5")
    common = {
        "tenant_id": "tenant-alpha",
        "binding_id": "binding-shared",
        "external_message_id": "provider-message-shared",
    }
    feishu = IdempotencyKey(channel=Channel.FEISHU, **common)
    wecom = IdempotencyKey(channel=Channel.WECOM, **common)
    other_binding = feishu.model_copy(update={"binding_id": "binding-other"})
    other_tenant = feishu.model_copy(update={"tenant_id": "tenant-beta"})
    other_message = feishu.model_copy(
        update={"external_message_id": "provider-message-other"}
    )

    encoded = {
        codec.idempotency(feishu),
        codec.idempotency(wecom),
        codec.idempotency(other_binding),
        codec.idempotency(other_tenant),
        codec.idempotency(other_message),
    }
    assert len(encoded) == 5
    assert all("provider-message" not in value for value in encoded)


def test_local_http_idempotency_key_remains_backwards_compatible() -> None:
    key = IdempotencyKey(
        tenant_id="tenant-alpha",
        binding_id="binding-alpha",
        external_message_id="local-message",
    )
    assert key.channel == Channel.LOCAL_HTTP


def test_session_ownership_explicitly_rejects_cross_channel_identity() -> None:
    settings, identities = dual_im_settings()
    platform = InMemoryPlatformAdapters(settings)
    feishu = platform._resolve_active_context(
        VerifiedBindingScope._issue(
            binding_id="binding-feishu-alpha", channel=Channel.FEISHU
        ),
        external_user_id="shared-user",
        trace_id=uuid4(),
    )
    identity = derive_session_identity(feishu, "direct", "conversation-001")

    assert identity.channel == Channel.FEISHU
    with pytest.raises(ValueError, match="ownership"):
        assert_session_ownership(
            feishu.model_copy(update={"channel": Channel.WECOM}), identity
        )
