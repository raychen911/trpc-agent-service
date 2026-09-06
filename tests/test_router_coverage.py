"""Focused branch coverage for the gateway and local worker contracts."""

from datetime import timedelta
from types import SimpleNamespace

import pytest

from trpc_service.agent.model_client import ModelResponse
from trpc_service.gateway.router import (
    AgentWorker,
    GatewayError,
    _combine_usage,
    _mailbox_failure_is_terminal,
    _max_tool_rounds,
    _model_tool_messages,
    _SessionMailboxV2Adapter,
)
from trpc_service.storage.base import MemoryItem, now_utc
from trpc_service.storage.factory import create_storage
from trpc_service.storage.session_mailbox import SessionMailboxLease
from trpc_service.tenant.models import AgentEvent, default_demo_config


class RecordingMailbox:
    def __init__(self):
        self.calls = []
        self.claim_result = SimpleNamespace(claimed=False, lease=None)

    def accept(self, *args, **kwargs):
        self.calls.append(("accept", args, kwargs))
        return "accepted"

    def claim_session(self, *args, **kwargs):
        self.calls.append(("claim", args, kwargs))
        return self.claim_result

    def has_unresolved_message(self, *args):
        self.calls.append(("unresolved", args))
        return True

    def commit(self, lease):
        self.calls.append(("commit", lease))
        return "committed"

    def retry(self, lease, *, retry_at):
        self.calls.append(("retry", lease, retry_at))
        return "retried"

    def dead_letter(self, lease, error):
        self.calls.append(("dead", lease, error))
        return SimpleNamespace(resolved_sequence=lease.sequence)


def lease(*, retry_count=0, sequence=1):
    return SessionMailboxLease(
        "tenant",
        "session",
        "message",
        sequence,
        "worker",
        1,
        now_utc() + timedelta(seconds=30),
        1,
        retry_count,
        0,
    )


def test_session_mailbox_adapter_delegates_and_fences_terminal_failures(monkeypatch):
    store = RecordingMailbox()
    adapter = _SessionMailboxV2Adapter(store)
    assert adapter.enqueue("tenant", "session", "message", "dedupe", {"priority": "3", "trace_id": "trace"}) == "accepted"
    assert store.calls[-1][2] == {"priority": 3, "trace_id": "trace"}

    assert adapter.claim_next("tenant", "session", "worker") is None
    claim_lease = lease()
    store.claim_result = SimpleNamespace(claimed=True, lease=claim_lease)
    assert adapter.claim_next("tenant", "session", "worker", lease_seconds=9) == claim_lease
    assert adapter.has_unresolved_message("tenant", "session", "message") is True
    assert adapter.complete(claim_lease) == "committed"

    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "bad")
    with pytest.raises(GatewayError, match="must be an integer"):
        adapter.fail(claim_lease, "error")
    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "0")
    with pytest.raises(GatewayError, match="must be positive"):
        adapter.fail(claim_lease, "error")

    monkeypatch.setenv("SESSION_MAILBOX_MAX_ATTEMPTS", "2")
    assert adapter.fail(lease(retry_count=1), "permanent").resolved_sequence == 1
    retry_lease = lease(retry_count=0)
    assert adapter.fail(retry_lease, "temporary", retry_after_seconds=0.1) == "retried"
    assert store.calls[-1][0] == "retry"
    assert _mailbox_failure_is_terminal(SimpleNamespace(resolved_sequence=1), retry_lease)
    assert not _mailbox_failure_is_terminal(None, retry_lease)


def test_router_configuration_and_tool_message_helpers(monkeypatch):
    monkeypatch.delenv("AGENT_MAX_TOOL_ROUNDS", raising=False)
    assert _max_tool_rounds() == 8
    monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", "4")
    assert _max_tool_rounds() == 4
    for value, message in (("bad", "integer"), ("0", "between"), ("33", "between")):
        monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", value)
        with pytest.raises(GatewayError, match=message):
            _max_tool_rounds()

    calls = [{"call_id": "c1", "name": "lookup", "arguments": {"q": "x"}}]
    events = [AgentEvent("tool_call", "answer", {"call_id": "c1"})]
    messages = _model_tool_messages(calls, events)
    assert messages[0]["tool_calls"][0]["function"]["name"] == "lookup"
    assert messages[1] == {"role": "tool", "tool_call_id": "c1", "content": "answer"}
    with pytest.raises(GatewayError, match="did not return"):
        _model_tool_messages(calls, [])

    previous = ModelResponse("old", 1, 2, 3, "model-a")
    current = ModelResponse("new", 4, 5, 9, "model-b")
    combined = _combine_usage(previous, current)
    assert (combined.text, combined.input_tokens, combined.output_tokens, combined.total_tokens, combined.model) == (
        "new", 5, 7, 12, "model-b"
    )


def test_worker_local_generation_conversation_search_and_error_audit(monkeypatch):
    storage = create_storage()
    try:
        monkeypatch.setattr(
            "trpc_service.gateway.router.ResponsesModelClient.from_config",
            lambda *args, **kwargs: None,
        )
        worker = AgentWorker(storage)
        config = default_demo_config()
        app = config.app("app_support")
        assert isinstance(worker._generate_answer("", app, []), str)
        assert isinstance(worker._generate_answer("/help", app, []), str)
        response = worker._generate_answer("hello", app, [])
        assert "hello" in response

        prior = [
            SimpleNamespace(event_type="user_message", payload={"text": "old user"}),
            SimpleNamespace(event_type="assistant_message", payload={"text": "old answer"}),
            SimpleNamespace(event_type="ignored", payload={"text": "skip"}),
        ]
        conversation = worker._build_conversation(
            "new",
            prior,
            "knowledge",
            "summary",
            "memory",
            lambda value: f"redacted:{value}",
        )
        assert conversation[-1] == {"role": "user", "content": "redacted:new"}
        assert len(conversation) == 6
        local = worker._generate_from_conversation(app, conversation, None)
        assert isinstance(local, ModelResponse)
        assert "redacted:new" in local.text

        result = worker._search_knowledge(storage, "tenant_demo", "missing")
        assert result.name == "search_knowledge"
        with pytest.raises(GatewayError, match="tool failure"):
            worker._raise_model_tool_error(
                storage,
                SimpleNamespace(
                    tenant_context=SimpleNamespace(
                        tenant_id="tenant_demo",
                        channel="web",
                        user_id="user",
                        session_id="session",
                        trace_id="trace",
                    )
                ),
                app,
                "tool_error",
                "tool failure",
            )
        assert storage.audit.list_by_tenant("tenant_demo")[-1].error_type == "tool_error"
    finally:
        storage.close()


def test_worker_compensation_serializes_datetimes():
    storage = create_storage()
    try:
        worker = AgentWorker(storage)
        value = MemoryItem("tenant_demo", "memory-1", "session", "text")
        worker._write_with_compensation(
            storage,
            "memory.put",
            value,
            lambda: (_ for _ in ()).throw(RuntimeError("backend down")),
            SimpleNamespace(tenant_id="tenant_demo", channel="web"),
        )
        task = storage.compensation.claim(1)[0]
        assert task.operation == "memory.put"
        assert task.payload["created_at"]
    finally:
        storage.close()
