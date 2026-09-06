"""Expanded contract and adversarial coverage for production boundaries.

These tests intentionally exercise the same contracts with several provider
shapes and tenant states.  They are cheap local tests; live provider and
infrastructure checks remain opt-in acceptance gates.
"""

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from trpc_service.channels.base import (
    ChannelVerificationError,
    normalize_attachment,
    normalize_event_type,
    parse_attachments,
    parse_webhook_body,
    revoke_target_message_id,
    sanitize_event_metadata,
)
from trpc_service.channels.feishu import FeishuAdapter, FeishuVerificationError
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.channels.telegram import TelegramAdapter
from trpc_service.channels.wecom_ai_bot import parse_wecom_ai_bot_frame
from trpc_service.policy.tenant_filter import PolicyDenied, TenantPolicy
from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary
from trpc_service.storage.compensation import (
    InMemoryCompensationStore,
    apply_compensation_task,
    replay_compensations,
)
from trpc_service.storage.durable import InboxStatus, InMemoryInboxOutbox, OutboxStatus
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.mirror import MirroredStorageBundle
from trpc_service.storage.object_store import FileObjectStore
from trpc_service.storage.session_mailbox import (
    InMemorySessionMailboxStore,
    SessionMailboxClaimStatus,
    SessionMailboxStatus,
)
from trpc_service.storage.tool_governance import (
    ApprovalStatus,
    InMemoryToolGovernanceStore,
    ToolExecutionStatus,
    arguments_hash,
)
from trpc_service.storage.vector_store import KnowledgeChunk, LocalVectorStore
from trpc_service.tenant.models import ChannelBinding, ToolPolicy, default_demo_config


def binding(channel="simple", tenant="tenant-a", **config):
    return ChannelBinding(
        tenant_id=tenant,
        binding_id=f"{channel}:account",
        channel=channel,
        account_id="account",
        agent_app_id="app_support",
        config=config,
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({}, "message"),
        ({"event_type": "text"}, "message"),
        ({"event_type": "revoke"}, "revoke"),
        ({"action": "deleted"}, "revoke"),
        ({"ChangeType": "Recall"}, "revoke"),
        ({"MsgType": "image"}, "message"),
        ({"normalized_event_type": "withdraw"}, "revoke"),
        ({"event": "MESSAGE"}, "message"),
        ({"event_type": "unknown", "action": "recall"}, "revoke"),
    ],
)
def test_event_type_normalization_matrix(payload, expected):
    assert normalize_event_type(payload) == expected


@pytest.mark.parametrize(
    ("body", "content_type", "expected"),
    [
        (b"{}", "application/json", {}),
        (b'{"x": 1}', "application/json; charset=utf-8", {"x": 1}),
        (b"x=1&x=2&empty=", "application/x-www-form-urlencoded", {"x": "2", "empty": ""}),
        (
            b"<root><Id>m-1</Id><Text>hello</Text></root>",
            "text/xml",
            {"raw_body": "<root><Id>m-1</Id><Text>hello</Text></root>", "Id": "m-1", "Text": "hello"},
        ),
        (b'{"x": true}', "application/vnd.provider+json", {"x": True}),
    ],
)
def test_webhook_body_parser_supported_content_matrix(body, content_type, expected):
    result = parse_webhook_body(body, content_type)
    assert result == expected


@pytest.mark.parametrize(
    ("body", "content_type", "max_bytes", "error"),
    [
        (b"[]", "application/json", None, "object"),
        (b"null", "application/json", None, "object"),
        (b"{bad", "application/json", None, "invalid"),
        (b"<root>", "text/xml", None, "invalid"),
        (b"hello", "text/plain", None, "unsupported"),
        (b"{}", "application/json", 1, "exceeds"),
        (b"\xff", "application/json", None, "UTF-8"),
    ],
)
def test_webhook_body_parser_rejects_invalid_inputs(body, content_type, max_bytes, error):
    with pytest.raises(ValueError, match=error):
        parse_webhook_body(body, content_type, max_bytes=max_bytes)


@pytest.mark.parametrize(
    "raw",
    [
        {"kind": "file", "name": "a.txt"},
        {"type": "image", "filename": "a.png", "content_type": "image/png"},
        {"kind": "file", "file_id": "f-1", "metadata": {}},
        {"kind": "file", "content_base64": base64.b64encode(b"payload").decode()},
        {"kind": "file", "metadata": {"media_id": "m-1", "source": "provider"}},
    ],
)
def test_attachment_normalization_preserves_provider_identity(raw):
    attachment = normalize_attachment(raw)
    assert attachment.kind
    assert isinstance(attachment.metadata, dict)


@pytest.mark.parametrize(
    ("raw", "error"),
    [
        (None, "list"),
        ({}, "list"),
        ([{"kind": "file", "content_base64": "bad"}], "invalid"),
        ([{"kind": "file", "metadata": []}], "object"),
        ([{"kind": "file"}] * 33, "at most 32"),
    ],
)
def test_attachment_parser_rejects_malformed_provider_shapes(raw, error):
    if raw is None:
        assert parse_attachments(raw) == []
        return
    with pytest.raises(ValueError, match=error):
        parse_attachments(raw)


def test_attachment_parser_rejects_oversized_metadata_and_duplicate_sensitive_content():
    with pytest.raises(ValueError, match="metadata exceeds"):
        normalize_attachment({"kind": "file", "metadata": {"note": "x" * 33_000}})
    payload = {"token": "secret", "safe": {"value": "kept"}, "content_base64": "hidden"}
    sanitized = sanitize_event_metadata(payload)
    assert sanitized == {"safe": {"value": "kept"}}


@pytest.mark.parametrize(
    ("payload", "message_id", "user_id", "group_id", "text"),
    [
        ({"message_id": "m", "user_id": "u", "text": "hello"}, "m", "u", None, "hello"),
        ({"MsgId": "wx", "FromUserName": "wx-u", "Content": "wx-text"}, "wx", "wx-u", None, "wx-text"),
        ({"update_id": 3, "from": "u-3", "chat_id": "g-3", "message": "group"}, "3", "u-3", "g-3", "group"),
        (
            {"message_id": "m", "user_id": "u", "group_id": "g", "event_type": "revoke", "msg_id": "old"},
            "m",
            "u",
            "g",
            None,
        ),
        ({"message_id": "m", "user_id": "u", "text": ""}, "m", "u", None, None),
    ],
)
def test_simple_adapter_field_precedence_and_revoke_contract(payload, message_id, user_id, group_id, text):
    inbound = SimpleJsonChannelAdapter().parse_event(payload, binding())
    assert inbound.external_message_id == message_id
    assert inbound.external_user_id == user_id
    assert inbound.group_id == group_id
    assert inbound.text == text
    if payload.get("event_type") == "revoke":
        assert inbound.is_revoke
        assert inbound.raw_event["target_message_id"] == "old"


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"target_message_id": "a"}, "a"),
        ({"revoke_message_id": "b"}, "b"),
        ({"MsgId": "c"}, "c"),
        ({"msg_id": 4}, "4"),
        ({}, None),
    ],
)
def test_revoke_target_field_matrix(payload, expected):
    assert revoke_target_message_id(payload) == expected


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"update_id": 1}, True),
        ({"message": {"message_id": 1}}, False),
        ({"edited_message": {"message_id": 1}}, False),
        ({"channel_post": {"message_id": 1}}, False),
        ({"callback_query": {"data": "ok"}}, False),
        ({"my_chat_member": {}}, True),
    ],
)
def test_telegram_noop_matrix(payload, expected):
    assert TelegramAdapter().is_noop(payload, binding("telegram")) is expected


@pytest.mark.parametrize(
    ("payload", "text", "group_id"),
    [
        (
            {
                "update_id": 1,
                "message": {"message_id": 2, "from": {"id": 3}, "chat": {"id": 3}, "text": "hello"},
            },
            "hello",
            "3",
        ),
        (
            {
                "update_id": 2,
                "edited_message": {
                    "message_id": 4,
                    "from": {"id": 5},
                    "chat": {"id": -5},
                    "caption": "edited",
                },
            },
            "edited",
            "-5",
        ),
        (
            {
                "update_id": 3,
                "callback_query": {
                    "id": "q",
                    "from": {"id": 6},
                    "message": {"chat": {"id": 6}},
                    "data": "button",
                },
            },
            "button",
            "6",
        ),
    ],
)
def test_telegram_inbound_update_matrix(payload, text, group_id):
    inbound = TelegramAdapter().parse_event(payload, binding("telegram"))
    assert inbound.text == text
    assert inbound.group_id == group_id


def feishu_payload(message_type="text", chat_type="group", content=None):
    return {
        "_raw_body": json.dumps({
            "header": {"event_id": "event-1", "event_type": "im.message.receive_v1", "app_id": "app-1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou-1"}, "sender_type": "user"},
                "message": {
                    "message_id": "om-1", "chat_id": "oc-1", "chat_type": chat_type,
                    "message_type": message_type,
                    "content": json.dumps(content or {"text": "hello"}),
                },
            },
        }),
        "_headers": {},
    }


@pytest.mark.parametrize(
    ("message_type", "content", "text", "attachment_kind"),
    [
        ("text", {"text": "hello"}, "hello", None),
        ("text", {"text": "@user hello"}, "@user hello", None),
        ("post", {"title": "title", "content": [[{"tag": "text", "text": "body"}]]}, "title\nbody", None),
        ("image", {"image_key": "img-1", "file_name": "x.png"}, None, "image"),
        ("file", {"file_key": "file-1", "file_name": "x.pdf"}, None, "file"),
        ("audio", {"file_key": "audio-1"}, None, "file"),
    ],
)
def test_feishu_message_content_matrix(message_type, content, text, attachment_kind):
    inbound = FeishuAdapter().parse_event(
        feishu_payload(message_type, "group", content),
        binding("feishu", app_id="app-1"),
    )
    assert inbound.text == text
    assert inbound.group_id == "oc-1"
    assert (inbound.attachments[0].kind if inbound.attachments else None) == attachment_kind


@pytest.mark.parametrize("chat_type", ["p2p", "group", "topic", ""], ids=["private", "group", "topic", "unknown"])
def test_feishu_chat_scope_matrix(chat_type):
    inbound = FeishuAdapter().parse_event(
        feishu_payload("text", chat_type, {"text": "hello"}),
        binding("feishu", app_id="app-1"),
    )
    assert inbound.group_id is None if chat_type == "p2p" else inbound.group_id == "oc-1"


@pytest.mark.parametrize(
    "payload_patch",
    [
        {"header": {"app_id": "wrong"}},
        {"event": {}},
        {"event": {"sender": {}}},
        {"event": {"message": {}}},
    ],
)
def test_feishu_parser_fails_closed_for_incomplete_callbacks(payload_patch):
    payload = json.loads(feishu_payload()["_raw_body"])
    for key, value in payload_patch.items():
        if key == "header":
            payload[key].update(value)
        elif key == "event":
            payload[key] = value
    callback = {"_raw_body": json.dumps(payload), "_headers": {}}
    with pytest.raises(FeishuVerificationError):
        FeishuAdapter().parse_event(callback, binding("feishu", app_id="app-1"))


@pytest.mark.parametrize(
    ("message_type", "expected_text", "expected_attachments"),
    [
        ("text", "hello", 0),
        ("voice", "spoken", 0),
        ("image", None, 1),
        ("file", None, 1),
        ("mixed", "hello", 1),
    ],
)
def test_wecom_ai_bot_frame_message_matrix(message_type, expected_text, expected_attachments):
    body = {
        "msgid": f"msg-{message_type}", "aibotid": "bot-1", "msgtype": message_type,
        "from": {"userid": "user-1"},
    }
    if message_type == "text":
        body["text"] = {"content": "hello"}
    elif message_type == "voice":
        body["voice"] = {"content": "spoken"}
    elif message_type in {"image", "file"}:
        body[message_type] = {"url": "https://media.example.test/item", "aeskey": "key"}
    else:
        body["mixed"] = {"msg_item": [
            {"msgtype": "text", "text": {"content": "hello"}},
            {"msgtype": "image", "image": {"url": "https://media.example.test/item", "aeskey": "key"}},
        ]}
    inbound = parse_wecom_ai_bot_frame({"body": body}, binding("wecom_ai_bot", bot_id="bot-1"))
    assert inbound.text == expected_text
    assert len(inbound.attachments) == expected_attachments


@pytest.mark.parametrize("bad_body", [{}, {"from": {}}, {"from": {"userid": "u"}, "aibotid": "wrong"}])
def test_wecom_ai_bot_frame_rejects_identity_and_shape_errors(bad_body):
    with pytest.raises(ValueError):
        parse_wecom_ai_bot_frame({"body": bad_body}, binding("wecom_ai_bot", bot_id="bot-1"))


@pytest.mark.parametrize(
    ("allowed", "external", "internal", "allowed_result"),
    [
        ([], "external", None, True),
        (["external"], "external", None, True),
        (["internal"], "external", "internal", True),
        (["other"], "external", "internal", False),
        (["external", "other"], "external", "internal", True),
    ],
)
def test_tenant_im_identity_scope_matrix(allowed, external, internal, allowed_result):
    current = binding()
    current.config["allowed_user_ids"] = allowed
    if internal:
        current.config["identity_mapping"] = {"external_to_internal": {external: internal}}
    if allowed_result:
        TenantPolicy.check_im_user(current, external, internal)
    else:
        with pytest.raises(PolicyDenied):
            TenantPolicy.check_im_user(current, external, internal)


@pytest.mark.parametrize(
    ("tool", "policy", "approved", "allowed"),
    [
        ("search_knowledge", ToolPolicy(), False, True),
        ("send", ToolPolicy(allowlist=["send"]), False, True),
        ("other", ToolPolicy(allowlist=["send"]), False, False),
        ("delete", ToolPolicy(denylist=["delete"]), True, False),
        ("send", ToolPolicy(risk_levels={"send": "critical"}), False, False),
        ("send", ToolPolicy(risk_levels={"send": "critical"}), True, True),
    ],
)
def test_tenant_tool_policy_matrix(tool, policy, approved, allowed):
    config = default_demo_config()
    config.apps[0].tool_policy = policy
    checker = TenantPolicy(config)
    if allowed:
        checker.check_tool(tool, approval_granted=approved)
    else:
        with pytest.raises(PolicyDenied):
            checker.check_tool(tool, approval_granted=approved)


@pytest.mark.parametrize("value", [20001, 25000, 100000])
def test_tenant_input_limit_is_enforced(value):
    with pytest.raises(PolicyDenied, match="input exceeds"):
        TenantPolicy(default_demo_config()).check_input("x" * value)


@pytest.mark.parametrize("args", [("", "session", "message"), ("tenant", "", "message"), ("tenant", "session", "")])
def test_session_mailbox_rejects_empty_identifiers(args):
    with pytest.raises(ValueError, match="identifiers"):
        InMemorySessionMailboxStore().accept(*args)


@pytest.mark.parametrize("priority", [-1, -5, True, "high"])
def test_session_mailbox_rejects_invalid_priority(priority):
    with pytest.raises(ValueError, match="priority"):
        InMemorySessionMailboxStore().accept("tenant", "session", "message", priority=priority)


@pytest.mark.parametrize("lease_seconds", [0, -1, "invalid"])
def test_session_mailbox_rejects_invalid_lease(lease_seconds):
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    with pytest.raises(ValueError):
        mailbox.claim("tenant", "session", "worker", lease_seconds)


def test_session_mailbox_duplicate_accept_does_not_advance_sequence_or_generation():
    mailbox = InMemorySessionMailboxStore()
    first = mailbox.accept("tenant", "session", "message", trace_id="trace-1")
    duplicate = mailbox.accept("tenant", "session", "message", trace_id="trace-2")
    assert duplicate.accepted_sequence == first.accepted_sequence == 1
    assert duplicate.queue_generation == first.queue_generation == 1
    assert len(mailbox.outbox) == 1


def test_session_mailbox_tenant_scope_and_missing_session_claim_are_isolated():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant-a", "session", "message-a")
    mailbox.accept("tenant-b", "session", "message-b")
    assert mailbox.claim("tenant-a", "session", "worker", 30).message_id == "message-a"
    assert mailbox.claim("tenant-b", "other-session", "worker", 30) is None
    assert mailbox.get("tenant-a", "session").accepted_sequence == 1
    assert mailbox.get("tenant-b", "session").accepted_sequence == 1


@pytest.mark.parametrize("retry_at", [None, timedelta(seconds=0)])
def test_session_mailbox_retry_requeues_and_increments_attempts(retry_at):
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker", 30)
    state = mailbox.retry(lease, retry_at=None if retry_at is None else mailbox.get("tenant", "session").updated_at)
    assert state.status == SessionMailboxStatus.QUEUED
    assert state.retry_count == 1
    next_lease = mailbox.claim("tenant", "session", "worker-2", 30)
    assert next_lease.attempt == 2


def test_session_mailbox_delayed_retry_stays_blocked_until_scheduler():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker", 30)
    retry_at = mailbox.get("tenant", "session").updated_at + timedelta(hours=1)
    state = mailbox.retry(lease, retry_at=retry_at)
    assert state.status == SessionMailboxStatus.RETRY_WAIT
    assert mailbox.claim("tenant", "session", "worker-2", 30) is None
    assert mailbox.schedule_retries(tenant_id="other") == 0
    assert mailbox.schedule_retries(tenant_id="tenant") == 0


def test_session_mailbox_expected_generation_and_empty_statuses_are_distinct():
    mailbox = InMemorySessionMailboxStore()
    stale = mailbox.claim_session("tenant", "missing", "worker", 30, expected_generation=1)
    empty = mailbox.claim_session("tenant", "missing", "worker", 30)
    assert stale.status == SessionMailboxClaimStatus.STALE
    assert empty.status == SessionMailboxClaimStatus.EMPTY


def test_session_mailbox_recovery_emits_one_wakeup_and_fences_expired_owner():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    lease = mailbox.claim("tenant", "session", "worker-a", 30)
    current = mailbox.get("tenant", "session")
    current.lease_until = current.lease_until - timedelta(seconds=60)
    mailbox._mailboxes[("tenant", "session")] = current
    assert mailbox.recover("tenant", "session").status == SessionMailboxStatus.QUEUED
    with pytest.raises(SessionLeaseLost):
        mailbox.commit(lease)
    assert [item.topic for item in mailbox.outbox].count("session.ready.v2") == 2


def test_session_mailbox_concurrent_claim_has_single_winner():
    mailbox = InMemorySessionMailboxStore()
    mailbox.accept("tenant", "session", "message")
    with ThreadPoolExecutor(max_workers=12) as pool:
        claims = list(pool.map(lambda i: mailbox.claim("tenant", "session", f"worker-{i}", 30), range(12)))
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert mailbox.get("tenant", "session").lease_owner == winners[0].owner


def test_session_mailbox_export_restore_is_tenant_scoped():
    source = InMemorySessionMailboxStore()
    source.accept("tenant-a", "session", "a")
    source.accept("tenant-b", "session", "b")
    exported = source.export_by_tenant("tenant-a")
    target = InMemorySessionMailboxStore()
    target.restore_export(exported)
    assert target.get("tenant-a", "session").accepted_sequence == 1
    assert target.get("tenant-b", "session") is None


@pytest.mark.parametrize("tenant", ["tenant-a", "tenant-b", "tenant-c", "tenant-d"])
def test_inbox_outbox_acceptance_is_idempotent_per_tenant(tenant):
    store = InMemoryInboxOutbox()
    first, created = store.accept_inbox(tenant, "dedupe", "session", {"tenant": tenant}, "worker")
    second, duplicate = store.accept_inbox(tenant, "dedupe", "session", {"different": True}, "worker-2")
    assert created is True
    assert duplicate is False
    assert second.message_id == first.message_id
    assert second.payload == {"tenant": tenant}


def test_inbox_outbox_terminal_records_cannot_be_reclaimed_or_cross_tenant_completed():
    store = InMemoryInboxOutbox()
    record, _ = store.accept_inbox("tenant-a", "dedupe", "session", {"x": 1}, "worker")
    store.complete_inbox("tenant-a", "dedupe", "worker", {"ok": True})
    duplicate, created = store.accept_inbox("tenant-a", "dedupe", "session", {"x": 2}, "worker-2")
    assert created is False
    assert duplicate.status == InboxStatus.COMPLETED
    with pytest.raises(KeyError):
        store.complete_inbox("tenant-b", "dedupe", "worker", {})
    assert record.message_id == duplicate.message_id


def test_inbox_outbox_atomic_completion_is_idempotent_by_event_id():
    store = InMemoryInboxOutbox()
    store.accept_inbox("tenant", "dedupe", "session", {"x": 1}, "worker")
    first = store.complete_inbox_and_enqueue_outbox(
        "tenant", "dedupe", "worker", {"answer": "ok"}, "agent.response", "session", "event-1"
    )
    second = store.enqueue_outbox("tenant", "agent.response", "session", {"answer": "changed"}, event_id="event-1")
    assert first.event_id == second.event_id
    assert second.payload == {"answer": "ok"}
    assert len(store.list_outbox_by_tenant("tenant")) == 1


@pytest.mark.parametrize("tenant", ["tenant-a", "tenant-b", "tenant-c"])
def test_outbox_claim_filter_never_returns_another_tenant(tenant):
    store = InMemoryInboxOutbox()
    for current in [tenant, "other"]:
        store.enqueue_outbox(current, "topic", "session", {"tenant": current}, event_id=f"event-{current}")
    claimed = store.claim_outbox("worker", tenant_id=tenant)
    assert [item.tenant_id for item in claimed] == [tenant]


def test_outbox_expired_lease_is_reclaimable_and_wrong_owner_is_fenced():
    store = InMemoryInboxOutbox()
    store.enqueue_outbox("tenant", "topic", "session", {})
    claimed = store.claim_outbox("worker-a", lease_seconds=1)[0]
    with pytest.raises(RuntimeError, match="ownership"):
        store.complete_outbox(claimed.event_id, "worker-b")
    record = store._outbox[claimed.event_id]
    record.locked_until = record.locked_until - timedelta(seconds=60)
    reclaimed = store.claim_outbox("worker-b")
    assert reclaimed[0].event_id == claimed.event_id


def test_outbox_dead_letter_replay_resets_delivery_state_and_keeps_tenant():
    store = InMemoryInboxOutbox()
    store.enqueue_outbox("tenant", "topic", "session", {}, event_id="event-1")
    for _ in range(10):
        claimed = store.claim_outbox("worker")[0]
        store.fail_outbox(claimed.event_id, "worker", "api_key=secret", 0)
    dead = store._outbox["event-1"]
    assert dead.status == OutboxStatus.DEAD
    assert "api_key=secret" not in (dead.last_error or "")
    replayed = store.replay_outbox("event-1", "tenant")
    assert replayed.status == OutboxStatus.PENDING
    assert replayed.attempts == 0
    with pytest.raises(KeyError):
        store.replay_outbox("event-1", "other")


@pytest.mark.parametrize("tenant", ["tenant-a", "tenant-b", "tenant-c"])
def test_session_events_and_memory_are_tenant_scoped(tenant):
    storage = InMemoryStorage()
    storage.session.append_event(SessionEvent(tenant, "session", "event", "message", {"x": tenant}, "trace"))
    storage.memory.put(MemoryItem(tenant, "memory", "session", f"private-{tenant}"))
    assert len(storage.session.load_events(tenant, "session")) == 1
    assert storage.session.load_events("other", "session") == []
    assert storage.memory.search(tenant, "private")
    assert storage.memory.search("other", "private") == []


def test_session_event_idempotency_and_compare_and_set_versioning():
    storage = InMemoryStorage()
    event = SessionEvent("tenant", "session", "event-1", "message", {"x": 1}, "trace", "dedupe")
    assert storage.session.append_event(event) == 1
    assert storage.session.append_event(event) == 1
    assert storage.session.compare_and_set_state("tenant", "session", 0, {"value": 1})
    assert storage.session.compare_and_set_state("tenant", "session", 0, {"value": 2}) is False
    assert storage.session.load_state("tenant", "session").state == {"value": 1}


def test_mirrored_bundle_dual_writes_all_local_projections(tmp_path):
    primary = InMemoryStorage()
    secondary = InMemoryStorage()
    first = StorageBundle(
        primary,
        objects=FileObjectStore(tmp_path / "primary-artifacts"),
        knowledge=LocalVectorStore(tmp_path / "primary-knowledge"),
    )
    second = StorageBundle(
        secondary,
        objects=FileObjectStore(tmp_path / "secondary-artifacts"),
        knowledge=LocalVectorStore(tmp_path / "secondary-knowledge"),
    )
    mirror = MirroredStorageBundle(first, second)
    try:
        event = SessionEvent("tenant", "session", "event-1", "message", {"text": "hello"}, "trace")
        assert mirror.session.append_event(event) == 1
        assert len(mirror.session.load_events("tenant", "session")) == 1
        assert mirror.session.compare_and_set_state("tenant", "session", 0, {"ok": True})
        assert mirror.session.load_state("tenant", "session").state == {"ok": True}

        mirror.memory.put(MemoryItem("tenant", "memory", "session", "hello memory"))
        assert mirror.memory.search("tenant", "hello")
        mirror.summary.put(Summary("tenant", "session", "hello summary", 1))
        assert mirror.summary.latest("tenant", "session").content == "hello summary"
        mirror.audit.append(AuditRecord("audit", "tenant", "allow", "trace"))
        assert len(mirror.audit.list_by_tenant("tenant")) == 1

        mirror.idempotency.start("tenant", "idem", "trace")
        assert mirror.idempotency.complete("tenant", "idem", "response", {"ok": True}).status.value == "completed"
        assert mirror.idempotency.get("tenant", "idem").response_ref == "response"
        assert mirror.idempotency.claim_delivery("tenant", "idem")
        mirror.idempotency.release_delivery("tenant", "idem")
        mirror.idempotency.start("tenant", "failed", "trace")
        assert mirror.idempotency.fail("tenant", "failed", "provider").status.value == "failed"

        record = mirror.mailbox.enqueue("tenant", "session", "message", "dedupe", {"text": "hello"})
        assert mirror.mailbox.get("tenant", "dedupe").message_id == record.message_id
        claimed = mirror.mailbox.claim_next("tenant", "session", "worker")
        assert claimed is not None
        mirror.mailbox.renew(claimed)
        mirror.mailbox.fail(claimed, "temporary", retry_after_seconds=0)
        assert mirror.mailbox.recover_expired("tenant", "session") == 0

        mirror.session_mailbox_v2.accept("tenant", "v2-session", "v2-message")
        v2_lease = mirror.session_mailbox_v2.claim("tenant", "v2-session", "worker", 30)
        assert v2_lease is not None
        mirror.session_mailbox_v2.renew(v2_lease, 30)
        mirror.session_mailbox_v2.commit(v2_lease)
        assert mirror.session_mailbox_v2.get("tenant", "v2-session").status == SessionMailboxStatus.IDLE

        args = arguments_hash({"target": "user"})
        mirror.tool_governance.create_or_get("tenant", "approval", "session", "request", "send", args)
        mirror.tool_governance.approve("tenant", "approval", "operator")
        mirror.tool_governance.consume("tenant", "approval", "request", args)
        execution = mirror.tool_governance.begin_execution(
            "tenant", "execution", "request", "session", "send", "call", args, True
        )
        assert mirror.tool_governance.complete_execution("tenant", "call", {"ok": True}).status == "succeeded"
        assert execution.status == "running"

        accepted, created = mirror.inbox_outbox.accept_inbox("tenant", "inbox", "session", {"x": 1}, "worker")
        assert created
        outbox = mirror.inbox_outbox.complete_inbox_and_enqueue_outbox(
            "tenant", "inbox", "worker", {"ok": True}, "response", "session", "outbox"
        )
        assert accepted.message_id
        claimed_outbox = mirror.inbox_outbox.claim_outbox("worker")[0]
        mirror.inbox_outbox.complete_outbox(claimed_outbox.event_id, "worker", tenant_id="tenant")
        assert outbox.status == "pending"

        artifact = mirror.artifacts.put_with_id("tenant", "artifact", b"bytes", "text/plain")
        assert mirror.artifacts.get("tenant", artifact.object_id) == b"bytes"
        assert len(mirror.artifacts.list_by_tenant("tenant")) == 1
        mirror.knowledge.upsert(KnowledgeChunk("tenant", "default", "chunk", "hello knowledge"))
        assert mirror.knowledge.search("tenant", "default", "hello")
        assert len(mirror.knowledge.list_by_tenant("tenant")) == 1
    finally:
        mirror.close()


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        ({"nested": {"x": [1, 2]}}, {"nested": {"x": [1, 2]}}),
        ({"value": "中文"}, {"value": "中文"}),
    ],
)
def test_tool_arguments_hash_is_canonical(left, right):
    assert arguments_hash(left) == arguments_hash(right)


def test_tool_execution_lifecycle_is_idempotent_and_tenant_scoped():
    store = InMemoryToolGovernanceStore()
    args_hash = arguments_hash({"target": "user-1"})
    created = store.begin_execution(
        "tenant-a", "execution-1", "request-1", "session-1", "send", "call-1", args_hash, True, 7
    )
    duplicate = store.begin_execution(
        "tenant-a", "execution-2", "request-2", "session-2", "send", "call-1", args_hash, True, 7
    )
    assert created.status == ToolExecutionStatus.RUNNING
    assert duplicate.execution_id == "execution-1"
    assert store.get_execution("tenant-b", "call-1") is None
    completed = store.complete_execution("tenant-a", "call-1", {"sent": True}, fencing_token=7)
    assert completed.status == ToolExecutionStatus.SUCCEEDED
    assert store.complete_execution("tenant-a", "call-1", {"changed": True}, fencing_token=7).result == {"sent": True}


def test_tool_execution_failure_can_retry_but_stale_fence_is_rejected():
    store = InMemoryToolGovernanceStore()
    args_hash = arguments_hash({"x": 1})
    store.begin_execution("tenant", "execution", "request", "session", "tool", "call", args_hash, False, 3)
    failed = store.fail_execution("tenant", "call", "provider_error", "api_key=secret", fencing_token=3)
    assert failed.status == ToolExecutionStatus.FAILED
    assert failed.error_message == "api_key=secret"
    with pytest.raises(RuntimeError, match="stale"):
        store.begin_execution("tenant", "execution-2", "request", "session", "tool", "call", args_hash, False, 4)
    retried = store.begin_execution("tenant", "execution-2", "request", "session", "tool", "call", args_hash, False, 3)
    assert retried.status == ToolExecutionStatus.RUNNING
    assert retried.attempt == 2


def test_tool_execution_identity_conflict_is_terminal_until_operator_intervenes():
    store = InMemoryToolGovernanceStore()
    store.begin_execution("tenant", "execution", "request", "session", "tool", "call", "hash-a", False)
    conflict = store.begin_execution("tenant", "execution-2", "request", "session", "other", "call", "hash-b", False)
    assert conflict.status == ToolExecutionStatus.AMBIGUOUS
    with pytest.raises(RuntimeError, match="ambiguous"):
        store.complete_execution("tenant", "call", {})
    assert [item.call_key for item in store.list_executions_by_tenant("tenant")] == ["call"]


def test_tool_approval_lifecycle_supports_duplicate_requests_and_consumption():
    store = InMemoryToolGovernanceStore()
    record = store.create_or_get("tenant", "approval", "session", "request", "send", "hash", expires_seconds=60)
    duplicate = store.create_or_get("tenant", "approval", "other-session", "other-request", "send", "hash")
    assert record.status == ApprovalStatus.PENDING
    assert duplicate.created_at == record.created_at
    approved = store.approve("tenant", "approval", "operator")
    assert approved.status == ApprovalStatus.APPROVED
    consumed = store.consume("tenant", "approval", "request", "hash")
    assert consumed.status == ApprovalStatus.CONSUMED
    assert store.consume("tenant", "approval", "request", "hash").status == ApprovalStatus.CONSUMED
    with pytest.raises(RuntimeError, match="not pending"):
        store.approve("tenant", "approval")


def test_tool_approval_argument_mismatch_is_ambiguous_and_cross_tenant_is_hidden():
    store = InMemoryToolGovernanceStore()
    store.create_or_get("tenant-a", "approval", "session", "request", "send", "hash-a")
    with pytest.raises(RuntimeError, match="ambiguous"):
        store.consume("tenant-a", "approval", "request", "hash-b")
    assert store.get("tenant-a", "approval").status == ApprovalStatus.AMBIGUOUS
    assert store.get("tenant-b", "approval") is None
    with pytest.raises(KeyError):
        store.approve("tenant-b", "approval")


def test_tool_approval_expiration_and_restore_are_deterministic():
    store = InMemoryToolGovernanceStore()
    record = store.create_or_get("tenant", "approval", "session", "request", "send", "hash", expires_seconds=1)
    record.expires_at = record.created_at - timedelta(seconds=1)
    store._approvals[("tenant", "approval")] = record
    assert store.get("tenant", "approval").status == ApprovalStatus.EXPIRED
    with pytest.raises(RuntimeError, match="not pending"):
        store.approve("tenant", "approval")


@pytest.mark.parametrize(
    ("calls", "side_effect", "max_calls", "max_side_effect"),
    [("call-1", False, 1, 0), ("call-1", True, 1, 1), ("call-2", False, 2, 0)],
)
def test_tool_budget_reservation_matrix(calls, side_effect, max_calls, max_side_effect):
    store = InMemoryToolGovernanceStore()
    first = store.reserve_call("tenant", "request", calls, side_effect, max_calls, max_side_effect)
    duplicate = store.reserve_call("tenant", "request", calls, side_effect, max_calls, max_side_effect)
    assert first.total_calls == duplicate.total_calls == 1
    assert duplicate.call_keys == [calls]


def test_tool_budget_rejects_total_and_side_effect_overflow():
    store = InMemoryToolGovernanceStore()
    store.reserve_call("tenant", "request", "call-1", False, 1, 0)
    with pytest.raises(RuntimeError, match="call budget"):
        store.reserve_call("tenant", "request", "call-2", False, 1, 0)
    with pytest.raises(RuntimeError, match="side-effect"):
        store.reserve_call("tenant-2", "request", "call-1", True, 2, 0)


def test_compensation_store_is_tenant_scoped_and_deduplicated():
    store = InMemoryCompensationStore()
    first = store.enqueue("tenant-a", "memory.put", {"x": 1}, task_id="task-1")
    duplicate = store.enqueue("tenant-a", "memory.put", {"x": 2}, task_id="task-1")
    store.enqueue("tenant-b", "memory.put", {"x": 3}, task_id="task-2")
    assert duplicate.payload == first.payload
    assert [task.task_id for task in store.claim(10, tenant_id="tenant-a")] == ["task-1"]
    assert store.claim(10, tenant_id="tenant-a") == []


@pytest.mark.parametrize("operation", ["memory.put", "summary.put", "audit.append"])
def test_compensation_tasks_apply_to_the_matching_storage_projection(operation):
    storage = InMemoryStorage()
    payloads = {
        "memory.put": {"tenant_id": "tenant", "memory_id": "m", "scope_key": "session", "content": "hello"},
        "summary.put": {"tenant_id": "tenant", "session_id": "session", "content": "summary", "source_event_seq": 1},
        "audit.append": {"audit_id": "audit", "tenant_id": "tenant", "decision": "allow", "trace_id": "trace"},
    }
    task = storage.compensation.enqueue("tenant", operation, payloads[operation], task_id=f"task-{operation}")
    assert replay_compensations(storage, tenant_id="tenant") == 1
    apply_compensation_task(storage, task)
    if operation == "memory.put":
        assert storage.memory.search("tenant", "hello")
    elif operation == "summary.put":
        assert storage.summary.latest("tenant", "session").content == "summary"
    else:
        assert storage.audit.list_by_tenant("tenant")[0].audit_id == "audit"


def test_compensation_failure_uses_retry_and_dead_letter_replay():
    store = InMemoryCompensationStore(max_attempts=1)
    task = store.enqueue("tenant", "unsupported.operation", {}, task_id="task-1")
    assert replay_compensations(InMemoryStorage(), tenant_id="tenant") == 0
    claimed = store.claim()[0]
    store.fail(claimed.task_id, "api_key=secret", 0, tenant_id="tenant")
    assert store._tasks[task.task_id].status == "dead"
    assert "api_key=secret" not in store._tasks[task.task_id].last_error
    assert store.replay(task.task_id, "tenant").status == "pending"


@pytest.mark.parametrize(
    ("email", "phone", "token"),
    [
        ("user@example.com", "13800138000", "Bearer abc"),
        ("admin@example.org", "+8613800138000", "api_key=xyz"),
        ("plain", "plain", "plain"),
    ],
)
def test_tenant_audit_redaction_matrix(email, phone, token):
    result = TenantPolicy(default_demo_config()).redact(f"{email} {phone} {token}")
    if "@" in email:
        assert email not in result
    if any(char.isdigit() for char in phone):
        assert phone not in result
    if token != "plain":
        assert token not in result


def test_unused_channel_verification_error_contract_remains_value_error():
    assert issubclass(ChannelVerificationError, ValueError)
    assert FeishuVerificationError.__mro__[1] is ChannelVerificationError
