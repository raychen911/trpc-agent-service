from __future__ import annotations

from uuid import uuid4

import pytest

from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.tenant import session_identity


def _context(binding: str = "binding-alpha", user: str = "shared-user"):
    return InMemoryPlatformAdapters(build_demo_settings()).context_for_test(binding, user, uuid4())


def test_identity_is_stable_collision_safe_and_every_scope_component_matters() -> None:
    alpha = _context()
    baseline = session_identity.derive_session_identity(alpha, "direct", "shared-conversation")
    assert baseline == session_identity.derive_session_identity(alpha, "direct", "shared-conversation")
    variants = [
        session_identity.derive_session_identity(_context("binding-beta"), "direct", "shared-conversation"),
        session_identity.derive_session_identity(_context(user="other-user"), "direct", "shared-conversation"),
        session_identity.derive_session_identity(alpha, "group", "shared-conversation"),
        session_identity.derive_session_identity(alpha, "direct", "other-conversation"),
        session_identity.derive_session_identity(alpha, "direct", "ab|c"),
        session_identity.derive_session_identity(alpha, "direct", "a|bc"),
    ]
    assert len({baseline.platform_session_id, *(item.platform_session_id for item in variants)}) == 7
    rendered = baseline.model_dump_json()
    assert "shared-user" not in rendered and "shared-conversation" not in rendered


def test_explicit_session_ownership_rejects_mismatched_context() -> None:
    alpha = _context()
    beta_identity = session_identity.derive_session_identity(_context("binding-beta"), "direct", "shared-conversation")
    with pytest.raises(ValueError, match="ownership"):
        session_identity.assert_session_ownership(alpha, beta_identity)

    valid = session_identity.derive_session_identity(alpha, "direct", "shared-conversation")
    for field, value in (
        ("sdk_app_name", "app_" + "0" * 32),
        ("sdk_user_id", "user_" + "0" * 32),
        ("external_user_digest", "sha256:" + "0" * 64),
    ):
        with pytest.raises(ValueError, match="ownership"):
            session_identity.assert_session_ownership(alpha, valid.model_copy(update={field: value}))


def test_real_im_group_session_is_scoped_by_group_and_sender() -> None:
    context = _context(user="group-user-a")
    first = session_identity.derive_session_identity(
        context, "group", "group-001", "group-user-a"
    )
    same = session_identity.derive_session_identity(
        context, "group", "group-001", "group-user-a"
    )
    other_sender = session_identity.derive_session_identity(
        context, "group", "group-001", "group-user-b"
    )
    other_group = session_identity.derive_session_identity(
        context, "group", "group-002", "group-user-a"
    )

    assert first.platform_session_id == same.platform_session_id
    assert len(
        {
            first.platform_session_id,
            other_sender.platform_session_id,
            other_group.platform_session_id,
        }
    ) == 3
