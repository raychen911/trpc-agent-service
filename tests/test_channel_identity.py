"""Tests for Stage 5A channel identity projection."""

from __future__ import annotations

import re

import pytest


def test_project_identity_returns_channel_identity():
    from trpc_service.channels.identity import ChannelIdentity, project_identity

    ident = project_identity("web_console", "alice", "conv-1")
    assert isinstance(ident, ChannelIdentity)
    assert ident.user_id.startswith("usr_v1_")
    assert ident.session_id.startswith("ses_v1_")


def test_project_identity_fixed_length_ascii():
    from trpc_service.channels.identity import project_identity

    ident = project_identity("web_console", "alice", "conv-1")
    assert re.fullmatch(r"usr_v1_[0-9a-f]{48}", ident.user_id)
    assert re.fullmatch(r"ses_v1_[0-9a-f]{48}", ident.session_id)


def test_project_identity_is_stable():
    from trpc_service.channels.identity import project_identity

    a = project_identity("web_console", "alice", "conv-1")
    b = project_identity("web_console", "alice", "conv-1")
    assert a.user_id == b.user_id
    assert a.session_id == b.session_id


def test_project_identity_changes_with_each_dimension():
    from trpc_service.channels.identity import project_identity

    base_user = project_identity("web_console", "alice", "conv-1").user_id
    base_session = project_identity("web_console", "alice", "conv-1").session_id

    assert project_identity("wecom", "alice", "conv-1").user_id != base_user
    assert project_identity("web_console", "bob", "conv-1").user_id != base_user
    assert project_identity("web_console", "alice", "conv-2").session_id != base_session


def test_project_identity_isolation_across_tenants_via_sdk_user_id():
    """Different channels produce different internal user_ids; combined with
    tenant_id in TenantContext.sdk_user_id this gives per-tenant isolation."""
    from trpc_service.channels.identity import project_identity

    web = project_identity("web_console", "alice", "conv-1")
    wecom = project_identity("wecom", "alice", "conv-1")
    assert web.user_id != wecom.user_id
    assert web.session_id != wecom.session_id


def test_project_identity_does_not_embed_raw_external_id():
    from trpc_service.channels.identity import project_identity

    external_user = "very-specific-external-user-xyz"
    ident = project_identity("web_console", external_user, "conv-1")
    assert external_user not in ident.user_id
    assert external_user not in ident.session_id


def test_project_identity_rejects_blank_inputs():
    from trpc_service.channels.identity import project_identity

    for args in [
        ("", "u", "c"),
        ("web_console", "", "c"),
        ("web_console", "u", ""),
        ("   ", "u", "c"),
    ]:
        with pytest.raises(ValueError):
            project_identity(*args)


def test_project_identity_rejects_bad_channel_grammar():
    from trpc_service.channels.identity import project_identity

    for bad in ["WebConsole", "web-console", "0abc", "_abc", "a" * 33]:
        with pytest.raises(ValueError):
            project_identity(bad, "u", "c")


def test_project_identity_handles_unicode_without_collision():
    from trpc_service.channels.identity import project_identity

    a = project_identity("web_console", "alice\x00bob", "conv")
    b = project_identity("web_console", "alice", "\x00bobconv")
    assert a.user_id != b.user_id or a.session_id != b.session_id


def test_project_identity_scopes_both_dimensions_by_binding_and_direct_user():
    from uuid import uuid4

    from trpc_service.channels.identity import project_identity

    first = uuid4()
    second = uuid4()
    direct = project_identity("wecom", "alice", "ignored", binding_id=first, conversation_kind="direct")
    other = project_identity("wecom", "alice", "ignored", binding_id=second, conversation_kind="direct")

    assert direct.user_id != other.user_id
    assert direct.session_id != other.session_id
    assert direct.session_id == project_identity("wecom",
                                                 "alice",
                                                 "different",
                                                 binding_id=first,
                                                 conversation_kind="direct").session_id
