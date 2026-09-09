# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

from trpc_service.config import ChannelType
from trpc_service.gateway import NormalizedInboundMessage
from trpc_service.gateway import internal_session_id
from trpc_service.gateway import internal_user_id


def test_identity_is_deterministic_and_tenant_scoped():
    assert internal_user_id("a", "web", "user") == internal_user_id("a", "web", "user")
    assert internal_user_id("a", "web", "user") != internal_user_id("b", "web", "user")


def test_group_session_mode_changes_subject():
    first = NormalizedInboundMessage(message_id="1",
                                     binding_id="b",
                                     channel=ChannelType.TELEGRAM,
                                     external_user_id="u1",
                                     external_conversation_id="group",
                                     text="hi",
                                     is_group=True)
    second = first.model_copy(update={"external_user_id": "u2"})
    assert internal_session_id("t", "a", first, "shared") == internal_session_id("t", "a", second, "shared")
    assert internal_session_id("t", "a", first, "per_user") != internal_session_id("t", "a", second, "per_user")
