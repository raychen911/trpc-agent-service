"""Protocol-level failure coverage for mirrored storage adapters.

These tests use small programmable doubles.  They verify compensation and
fallback behavior without claiming that either real backend was available.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary
from trpc_service.storage.durable import OutboxRecord
from trpc_service.storage.mailbox import MailboxRecord
from trpc_service.storage.mirror import (
    _MirrorArtifact,
    _MirrorAudit,
    _MirrorIdempotency,
    _MirrorInboxOutbox,
    _MirrorKnowledge,
    _MirrorMailbox,
    _MirrorMemory,
    _MirrorSession,
    _MirrorSessionMailboxV2,
    _MirrorSummary,
    _MirrorToolGovernance,
)
from trpc_service.storage.session_mailbox import SessionMailboxClaimStatus, SessionMailboxLease
from trpc_service.storage.vector_store import KnowledgeChunk

NOW = datetime(2030, 1, 1, tzinfo=UTC)


def _failure(_message="secondary failed"):
    def fail(*_args, **_kwargs):
        raise RuntimeError(_message)

    return fail


def _events(value=None):
    return value or [SessionEvent("tenant", "session", "event", "message", {}, "trace")]


def _lease(sequence=1):
    return SessionMailboxLease(
        "tenant", "session", "message", sequence, "worker", 1,
        NOW, 1, 0, 3,
    )


def test_mirror_session_fallbacks_and_write_mismatch_are_explicit():
    events = _events()
    primary = SimpleNamespace(
        acquire_session_lock=lambda *args: "lock",
        release_session_lock=lambda *args: None,
        acquire_session_lease=lambda *args: "lease",
        release_session_lease=lambda *args: None,
        renew_session_lease=lambda lease: lease,
        validate_session_lease=lambda lease: None,
        append_event=lambda *args, **kwargs: 1,
        load_events=lambda *args: [],
        load_state=lambda *args: SimpleNamespace(state_version=0, latest_event_seq=0, state={}),
        compare_and_set_state=lambda *args, **kwargs: False,
    )
    secondary = SimpleNamespace(
        append_event=lambda *args, **kwargs: 2,
        load_events=lambda *args: events,
        load_state=lambda *args: SimpleNamespace(state_version=1, latest_event_seq=2, state={"x": 1}),
        compare_and_set_state=lambda *args, **kwargs: False,
    )
    calls = []
    session = _MirrorSession(primary, secondary, lambda op, payload: calls.append((op, payload)))
    assert session.acquire_session_lock("tenant", "session", 1) == "lock"
    session.release_session_lock("tenant", "session", "lock")
    assert session.acquire_session_lease("tenant", "session", 1) == "lease"
    session.release_session_lease("tenant", "session", "lease")
    assert session.renew_session_lease("lease") == "lease"
    session.validate_session_lease("lease")
    assert session.load_events("tenant", "session") == events
    assert session.load_state("tenant", "session").state == {"x": 1}
    assert not session.compare_and_set_state("tenant", "session", 0, {"x": 1})

    primary.append_event = _failure("primary failed")
    with pytest.raises(RuntimeError, match="primary failed"):
        session.append_event(events[0])
    primary.append_event = lambda *args, **kwargs: 1
    secondary.append_event = _failure()
    with pytest.raises(RuntimeError, match="secondary failed"):
        session.append_event(events[0])
    assert calls[-1][0] == "mirror.session.append_event"
    secondary.append_event = lambda *args, **kwargs: 2
    with pytest.raises(RuntimeError, match="sequence mismatch"):
        session.append_event(events[0])

    primary.load_events = _failure("read failed")
    assert session.load_events("tenant", "session") == events
    primary.load_state = _failure("state failed")
    assert session.load_state("tenant", "session").state == {"x": 1}

    primary.compare_and_set_state = lambda *args, **kwargs: True
    secondary.compare_and_set_state = _failure()
    with pytest.raises(RuntimeError, match="secondary failed"):
        session.compare_and_set_state("tenant", "session", 0, {"x": 2})
    secondary.compare_and_set_state = lambda *args, **kwargs: False
    secondary.load_state = lambda *args: SimpleNamespace(state_version=1, latest_event_seq=1, state={"x": 3})
    with pytest.raises(RuntimeError, match="state mismatch"):
        session.compare_and_set_state("tenant", "session", 0, {"x": 2})

    primary.acquire_session_lock = None
    with pytest.raises(AttributeError, match="distributed lock"):
        session.acquire_session_lock("tenant", "session", 1)


@pytest.mark.parametrize(
    "adapter, method, value",
    [
        (_MirrorMemory, "put", MemoryItem("tenant", "memory", "scope", "content")),
        (_MirrorSummary, "put", Summary("tenant", "session", "summary", 1)),
        (_MirrorAudit, "append", AuditRecord("audit", "tenant", "allow", "trace")),
    ],
)
def test_mirror_projection_writes_enqueue_on_secondary_failure(adapter, method, value):
    calls = []
    primary = SimpleNamespace(**{method: lambda *_args, **_kwargs: None})
    secondary = SimpleNamespace(**{method: _failure()})
    instance = adapter(primary, secondary, lambda op, payload: calls.append((op, payload)))
    with pytest.raises(RuntimeError, match="secondary failed"):
        getattr(instance, method)(value)
    assert calls


def test_mirror_projection_reads_fallback_after_empty_or_failed_primary():
    item = MemoryItem("tenant", "memory", "scope", "content")
    summary = Summary("tenant", "session", "summary", 1)
    record = AuditRecord("audit", "tenant", "allow", "trace")
    for adapter, method, value in (
        (_MirrorMemory, "search", [item]),
        (_MirrorSummary, "latest", summary),
        (_MirrorAudit, "list_by_tenant", [record]),
    ):
        primary = SimpleNamespace(**{method: lambda *_args, **_kwargs: [] if method != "latest" else None})
        secondary = SimpleNamespace(**{method: lambda *_args, value=value, **_kwargs: value})
        instance = adapter(primary, secondary, lambda *_args: None)
        result = getattr(instance, method)("tenant", "session") if method == "latest" else getattr(instance, method)("tenant", "query")
        assert result == value
        setattr(primary, method, _failure("read failed"))
        result = getattr(instance, method)("tenant", "session") if method == "latest" else getattr(instance, method)("tenant", "query")
        assert result == value


def test_mirror_idempotency_secondary_failures_and_read_fallbacks():
    record = SimpleNamespace(status="completed")
    calls = []
    primary = SimpleNamespace(
        start=lambda *args: record,
        complete=lambda *args: record,
        fail=lambda *args: record,
        get=lambda *args: None,
        claim_delivery=lambda *args: True,
        release_delivery=lambda *args: None,
    )
    secondary = SimpleNamespace(
        start=_failure(), complete=_failure(), fail=_failure(), get=lambda *args: record,
        claim_delivery=_failure(), release_delivery=_failure(),
    )
    instance = _MirrorIdempotency(primary, secondary, lambda op, payload: calls.append(op))
    for method, args in (
        ("start", ("tenant", "key", "trace")),
        ("complete", ("tenant", "key", "response", {})),
        ("fail", ("tenant", "key", "error")),
        ("claim_delivery", ("tenant", "key")),
        ("release_delivery", ("tenant", "key")),
    ):
        with pytest.raises(RuntimeError):
            getattr(instance, method)(*args)
    assert instance.get("tenant", "key") == record
    primary.get = _failure("read failed")
    assert instance.get("tenant", "key") == record
    assert len(calls) == 5


def test_mirror_mailbox_failure_paths_record_compensation():
    mailbox = MailboxRecord("tenant", "session", 1, "message", "dedupe", {})
    calls = []
    primary = SimpleNamespace(
        enqueue=lambda *args: mailbox,
        get=lambda *args: mailbox,
        claim_next=lambda *args: mailbox,
        renew=lambda *args: mailbox,
        complete=lambda *args: None,
        fail=lambda *args: None,
        recover_expired=lambda *args: 1,
    )
    secondary = SimpleNamespace(
        enqueue=_failure(), get=lambda *args: None, claim_next=_failure(), renew=_failure(),
        complete=_failure(), fail=_failure(), recover_expired=_failure(),
    )
    instance = _MirrorMailbox(primary, secondary, lambda op, payload: calls.append(op))
    with pytest.raises(RuntimeError):
        instance.enqueue("tenant", "session", "message", "dedupe", {})
    with pytest.raises(RuntimeError):
        instance.claim_next("tenant", "session", "worker")
    assert instance.get("tenant", "dedupe") is mailbox
    primary.get = _failure("read failed")
    assert instance.get("tenant", "dedupe") is None
    primary.get = _failure("read failed")
    assert instance.get("tenant", "dedupe") is None
    assert calls[:2] == ["mirror.mailbox.enqueue", "mirror.mailbox.claim_next"]


def test_mirror_session_mailbox_v2_mismatch_and_failure_paths():
    lease = _lease()
    result = SimpleNamespace(status=SessionMailboxClaimStatus.CLAIMED, claimed=True, lease=lease)
    calls = []
    primary = SimpleNamespace(
        get=lambda *args: None, export_by_tenant=lambda *args: {"items": []}, restore_export=lambda *args: True,
        has_unresolved_message=lambda *args: True, accept=lambda *args, **kwargs: SimpleNamespace(accepted_sequence=1, queue_generation=1),
        claim=lambda *args: lease, claim_session=lambda *args, **kwargs: result, renew=lambda *args: lease,
        commit=lambda *args: True, retry=lambda *args, **kwargs: True, dead_letter=lambda *args: True,
        recover=lambda *args: None, sweep_expired_leases=lambda **kwargs: 1,
        schedule_retries=lambda **kwargs: 1, reconcile_sessions=lambda **kwargs: 1, reconcile=lambda *args: None,
    )
    secondary = SimpleNamespace(
        get=lambda *args: None, export_by_tenant=lambda *args: {"items": []}, restore_export=_failure(),
        has_unresolved_message=lambda *args: False, accept=_failure(), claim=_failure(), claim_session=_failure(),
        renew=_failure(), commit=_failure(), retry=_failure(), dead_letter=_failure(), recover=_failure(),
        sweep_expired_leases=lambda **kwargs: 2, schedule_retries=lambda **kwargs: 1,
        reconcile_sessions=lambda **kwargs: 1, reconcile=_failure(),
    )
    instance = _MirrorSessionMailboxV2(primary, secondary, lambda op, payload: calls.append(op))
    with pytest.raises(RuntimeError, match="message state mismatch"):
        instance.has_unresolved_message("tenant", "session", "message")
    with pytest.raises(RuntimeError):
        instance.restore_export({})
    with pytest.raises(RuntimeError):
        instance.accept("tenant", "session", "message")
    with pytest.raises(RuntimeError):
        instance.claim("tenant", "session", "worker", 30)
    with pytest.raises(RuntimeError):
        instance.claim_session("tenant", "session", "worker", 30)
    assert instance.get("tenant", "session") is None
    primary.get = _failure("read failed")
    assert instance.get("tenant", "session") is None
    assert calls


def test_mirror_tool_and_inbox_outbox_failures_are_recorded():
    calls = []
    primary = SimpleNamespace(
        create_or_get=lambda *args, **kwargs: "ok", get=lambda *args, **kwargs: None,
        approve=lambda *args, **kwargs: "ok", consume=lambda *args, **kwargs: "ok",
        reserve_call=lambda *args, **kwargs: "ok", begin_execution=lambda *args, **kwargs: "ok",
        get_execution=lambda *args, **kwargs: None, complete_execution=lambda *args, **kwargs: "ok",
        fail_execution=lambda *args, **kwargs: "ok", list_executions_by_tenant=lambda *args, **kwargs: [],
        restore_execution=lambda *args, **kwargs: "ok",
    )
    secondary = SimpleNamespace(**{name: _failure() for name in (
        "create_or_get", "approve", "consume", "reserve_call", "begin_execution",
        "complete_execution", "fail_execution", "restore_execution",
    )}, get=lambda *args, **kwargs: "fallback", get_execution=lambda *args, **kwargs: "fallback",
        list_executions_by_tenant=_failure())
    governance = _MirrorToolGovernance(primary, secondary, lambda op, payload: calls.append(op))
    for method in ("create_or_get", "approve", "consume", "reserve_call", "begin_execution", "complete_execution", "fail_execution", "restore_execution"):
        with pytest.raises(RuntimeError):
            getattr(governance, method)("tenant")
    assert governance.get("tenant") == "fallback"
    assert governance.get_execution("tenant") == "fallback"
    primary.list_executions_by_tenant = _failure("read failed")
    secondary.list_executions_by_tenant = lambda *args, **kwargs: "fallback"
    assert governance.list_executions_by_tenant("tenant") == "fallback"

    inbox_primary = SimpleNamespace(
        accept_inbox=lambda *args, **kwargs: (SimpleNamespace(message_id="message"), True),
        complete_inbox=lambda *args, **kwargs: None,
        complete_inbox_and_enqueue_outbox=lambda *args, **kwargs: OutboxRecord("event", "tenant", "topic", "session", {}),
        fail_inbox=lambda *args, **kwargs: None, dead_inbox=lambda *args, **kwargs: None,
        enqueue_outbox=lambda *args, **kwargs: OutboxRecord("event", "tenant", "topic", "session", {}),
        claim_outbox=lambda *args, **kwargs: [OutboxRecord("event", "tenant", "topic", "session", {})],
        complete_outbox=lambda *args, **kwargs: None, fail_outbox=lambda *args, **kwargs: None,
        replay_outbox=lambda *args, **kwargs: "ok",
    )
    inbox_secondary = SimpleNamespace(
        accept_inbox=_failure(), complete_inbox=_failure(), complete_inbox_and_enqueue_outbox=_failure(),
        fail_inbox=_failure(), dead_inbox=_failure(), enqueue_outbox=_failure(), claim_outbox=_failure(),
        complete_outbox=_failure(), fail_outbox=_failure(), replay_outbox=_failure(),
    )
    inbox = _MirrorInboxOutbox(inbox_primary, inbox_secondary, lambda op, payload: calls.append(op))
    with pytest.raises(RuntimeError):
        inbox.accept_inbox("tenant", "dedupe", "session", {}, "worker")
    for method in ("complete_inbox", "complete_inbox_and_enqueue_outbox", "fail_inbox", "dead_inbox", "enqueue_outbox", "claim_outbox", "complete_outbox", "fail_outbox", "replay_outbox"):
        if method == "complete_outbox":
            args = ("event", "worker")
        elif method == "fail_outbox":
            args = ("event", "worker", "error")
        elif method == "replay_outbox":
            args = ("event", "tenant")
        else:
            args = ("tenant",)
        with pytest.raises(RuntimeError):
            getattr(inbox, method)(*args)
    assert calls


def test_mirror_artifact_and_knowledge_failure_paths():
    calls = []
    stored = SimpleNamespace(object_id="object", tenant_id="tenant", content_type="application/octet-stream")
    primary = SimpleNamespace(
        put=lambda *args, **kwargs: stored, put_with_id=lambda *args, **kwargs: stored,
        get=lambda *args, **kwargs: None, list_by_tenant=lambda *args, **kwargs: [],
        upsert=lambda *args, **kwargs: None, search=lambda *args, **kwargs: [],
    )
    secondary = SimpleNamespace(
        put=_failure(), put_with_id=_failure(), get=lambda *args, **kwargs: "fallback",
        list_by_tenant=lambda *args, **kwargs: ["fallback"], upsert=_failure(),
        search=lambda *args, **kwargs: ["fallback"],
    )
    artifact = _MirrorArtifact(primary, secondary, lambda op, payload: calls.append(op))
    with pytest.raises(RuntimeError):
        artifact.put("tenant", b"data")
    with pytest.raises(RuntimeError):
        artifact.put_with_id("tenant", "id", b"data")
    primary.get = _failure("read failed")
    assert artifact.get("tenant", "id") == "fallback"
    assert artifact.list_by_tenant("tenant") == ["fallback"]
    primary.get = _failure("read failed")
    primary.list_by_tenant = _failure("read failed")
    assert artifact.get("tenant", "id") == "fallback"
    assert artifact.list_by_tenant("tenant") == ["fallback"]

    knowledge = _MirrorKnowledge(primary, secondary, lambda op, payload: calls.append(op))
    with pytest.raises(RuntimeError):
        knowledge.upsert(KnowledgeChunk("tenant", "collection", "chunk", "content"))
    assert knowledge.search("tenant", "collection", "query") == ["fallback"]
    assert knowledge.list_by_tenant("tenant") == ["fallback"]
    primary.search = _failure("read failed")
    primary.list_by_tenant = _failure("read failed")
    assert knowledge.search("tenant", "collection", "query") == ["fallback"]
    assert calls
