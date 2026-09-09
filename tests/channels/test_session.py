"""Tenant/channel isolation tests for HMAC-derived identities."""

from __future__ import annotations

import pytest

from tests.channels.helpers import IDENTITY_KEY
from trpc_service.channels.contracts import Channel, ConversationKind
from trpc_service.channels.session import ChannelIdentityDeriver


def _derive(
    deriver: ChannelIdentityDeriver,
    *,
    user: str = "user-a",
    conversation: str = "chat-a",
    kind: ConversationKind = ConversationKind.GROUP,
    tenant: str = "tenant-a",
    app: str = "app-a",
    app_revision: int = 1,
    binding: str = "binding-a",
    channel: Channel = Channel.TELEGRAM,
    thread: str | None = None,
):
    return deriver.derive(
        tenant_id=tenant,
        app_id=app,
        app_revision=app_revision,
        binding_id=binding,
        channel=channel,
        conversation_kind=kind,
        external_user_id=user,
        external_conversation_id=conversation,
        external_thread_id=thread,
    )


def test_derivation_is_deterministic_and_does_not_expose_source_ids() -> None:
    deriver = ChannelIdentityDeriver(IDENTITY_KEY, key_version=7)

    first = _derive(deriver)
    second = _derive(deriver)

    assert first == second
    assert first.principal_id.startswith("usr_v7_")
    assert first.conversation_id.startswith("conv_v7_")
    assert first.session_id.startswith("sess_v7_")
    serialized = repr(first)
    assert "user-a" not in serialized
    assert "chat-a" not in serialized


@pytest.mark.parametrize(
    ("field", "override"),
    [
        ("tenant", "tenant-b"),
        ("app", "app-b"),
        ("app_revision", 2),
        ("binding", "binding-b"),
        ("channel", Channel.WECOM),
        ("conversation", "chat-b"),
    ],
)
def test_session_isolated_by_every_trust_boundary(field: str, override: object) -> None:
    deriver = ChannelIdentityDeriver(IDENTITY_KEY)
    baseline = _derive(deriver)

    changed = _derive(deriver, **{field: override})

    assert changed.session_id != baseline.session_id


def test_group_session_is_shared_but_principals_are_distinct() -> None:
    deriver = ChannelIdentityDeriver(IDENTITY_KEY)

    alice = _derive(deriver, user="alice", kind=ConversationKind.GROUP)
    bob = _derive(deriver, user="bob", kind=ConversationKind.GROUP)

    assert alice.principal_id != bob.principal_id
    assert alice.conversation_id == bob.conversation_id
    assert alice.session_id == bob.session_id


def test_private_session_includes_principal_and_thread_isolates_group_topic() -> None:
    deriver = ChannelIdentityDeriver(IDENTITY_KEY)
    alice = _derive(
        deriver,
        user="alice",
        conversation="private-chat",
        kind=ConversationKind.PRIVATE,
    )
    bob = _derive(
        deriver,
        user="bob",
        conversation="private-chat",
        kind=ConversationKind.PRIVATE,
    )
    topic_a = _derive(deriver, thread="100")
    topic_b = _derive(deriver, thread="200")

    assert alice.session_id != bob.session_id
    assert topic_a.thread_id != topic_b.thread_id
    assert topic_a.session_id != topic_b.session_id


def test_key_version_and_key_material_change_outputs() -> None:
    first = _derive(ChannelIdentityDeriver(IDENTITY_KEY, key_version=1))
    second = _derive(ChannelIdentityDeriver(IDENTITY_KEY, key_version=2))
    third = _derive(ChannelIdentityDeriver(b"x" * 32, key_version=1))

    assert len({first.session_id, second.session_id, third.session_id}) == 3


def test_external_message_reference_is_hmac_opaque_and_tenant_scoped() -> None:
    deriver = ChannelIdentityDeriver(IDENTITY_KEY)
    values = {
        "tenant_id": "tenant-a",
        "app_id": "app-a",
        "app_revision": 1,
        "binding_id": "binding-a",
        "channel": Channel.TELEGRAM,
        "external_message_id": "9",
    }

    first = deriver.derive_message_id(**values)
    second = deriver.derive_message_id(**values)
    cross_tenant = deriver.derive_message_id(**{**values, "tenant_id": "tenant-b"})

    assert first == second
    assert first.startswith("msg_v1_")
    assert first != "9"
    assert cross_tenant != first

    with pytest.raises(ValueError, match="must not be empty"):
        deriver.derive_message_id(**{**values, "external_message_id": " "})


@pytest.mark.parametrize("secret", [b"", b"short", "too-short"])
def test_deriver_rejects_short_keys(secret: str | bytes) -> None:
    with pytest.raises(ValueError, match="at least 32"):
        ChannelIdentityDeriver(secret)


def test_deriver_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="positive"):
        ChannelIdentityDeriver(IDENTITY_KEY, key_version=0)

    deriver = ChannelIdentityDeriver(IDENTITY_KEY)
    with pytest.raises(ValueError, match="must not be empty"):
        _derive(deriver, user=" ")
    with pytest.raises(ValueError, match="must not be blank"):
        _derive(deriver, thread=" ")
