from __future__ import annotations
from datetime import timedelta
from uuid import UUID

import pytest

from tests.support import FIXED_UTC
from trpc_service.storage.models import (
    ExecutionPhase,
    MessageFence,
    NodeIdentity,
    RecoveryMarker,
    RecoveryState,
    SessionFence,
)


def test_fences_require_positive_generation_and_hide_owner_token() -> None:
    node = NodeIdentity(node_id="worker-a")
    message = MessageFence(
        key_digest="a" * 64,
        generation=1,
        owner_node=node,
        owner_token="runtime-" + "owner-token",
        owner_trace_id=UUID(int=1),
        expires_at=FIXED_UTC + timedelta(seconds=10),
    )
    session = SessionFence(
        session_key_digest="b" * 64,
        generation=2,
        owner_node=node,
        owner_token="runtime-" + "session-token",
        expires_at=FIXED_UTC + timedelta(seconds=10),
    )
    assert "runtime-owner-token" not in repr(message)
    assert "runtime-session-token" not in repr(session)
    with pytest.raises(ValueError):
        message.model_copy(update={"generation": 0}).model_validate(
            message.model_copy(update={"generation": 0}).model_dump()
        )


def test_recovery_marker_only_moves_terminal_pending_to_final_state() -> None:
    marker = RecoveryMarker(
        recovery_id=UUID(int=10),
        tenant_id="tenant-alpha",
        idempotency_key_digest="c" * 64,
        message_generation=1,
        session_generation=1,
        execution_trace_id=UUID(int=11),
        result_digest="d" * 64,
        state=RecoveryState.TERMINAL_PENDING,
        created_at=FIXED_UTC,
        updated_at=FIXED_UTC,
    )
    reconciled = marker.transition(RecoveryState.RECONCILED, FIXED_UTC)
    assert reconciled.state == RecoveryState.RECONCILED
    with pytest.raises(ValueError):
        reconciled.transition(RecoveryState.TERMINAL_PENDING, FIXED_UTC)


def test_execution_started_is_explicit_and_ordered() -> None:
    assert ExecutionPhase.PREPARED < ExecutionPhase.EXECUTION_STARTED
    assert ExecutionPhase.EXECUTION_STARTED < ExecutionPhase.FINALIZING
