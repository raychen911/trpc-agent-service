from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from trpc_service.governance import confirmation


def test_confirmation_binds_identity_and_reservation_and_expires() -> None:
    pending = confirmation.create_pending(
        tenant_id="tenant-a", channel="feishu", binding_id="b",
        principal_digest="a" * 64, session_id="s", tool_name="delete",
        arguments_digest="x" * 64, policy_version=1, reservation_id="r1",
        now=datetime.now(timezone.utc), ttl_seconds=30,
    )
    assert pending.reservation_id == "r1"
    with pytest.raises(Exception):
        confirmation.consume(pending, token="wrong", now=datetime.now(timezone.utc))
    expired = pending.model_copy(update={"expires_at": datetime.now(timezone.utc) - timedelta(seconds=1)})
    with pytest.raises(Exception) as exc:
        confirmation.consume(expired, token=pending.token_digest, now=datetime.now(timezone.utc))
    assert getattr(exc.value, "code", "") == "confirmation_expired"
