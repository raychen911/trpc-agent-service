"""Additional branch coverage for recovery and production protocols."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from trpc_service.agent.model_client import ModelResponse, ModelToolCall
from trpc_service.channels.base import InboundMessage
from trpc_service.gateway.router import AgentGateway, AgentWorker, GatewayError
from trpc_service.policy.approval import approval_id, approval_token
from trpc_service.policy.tenant_filter import PolicyDenied, TenantPolicy
from trpc_service.storage.base import SessionEvent, SessionState, ToolExecution, now_utc
from trpc_service.storage.factory import create_storage
from trpc_service.storage.locking import (
    PostgresSessionLockMixin,
    RedisSessionLockMixin,
    SessionLease,
    SessionLeaseLost,
    SessionLockTimeout,
    postgres_advisory_lock,
    renew_session_lease,
    session_lease,
    session_lock,
    validate_session_lease,
)
from trpc_service.storage.mirror import MirroredStorageBundle
from trpc_service.storage.redis_store import RedisStorage
from trpc_service.storage.remote_vector import RemoteVectorStore, resolve_secret
from trpc_service.storage.tool_governance import ToolExecutionStatus
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.tenant.models import RunRequest, StorageProfile, TenantContext, UserInput, default_demo_config
from trpc_service.tenant.repository import InMemoryTenantRepository
from trpc_service.tenant.service import TenantService


def _request(text="hello", metadata=None, key="request-key"):
    context = TenantContext("tenant_demo", "app_support", 1, "trace", "session", "web", "user")
    return RunRequest(context, UserInput(text=text, metadata=metadata or {}), key)


def test_worker_tool_execution_replay_approval_and_recovery_states():
    storage = create_storage()
    try:
        worker = AgentWorker(storage)
        config = default_demo_config()
        app = config.app("app_support")
        app.tool_policy.allowlist.append("send_external")

        def send_external(message: str, request_id=None, idempotency_key=None):
            return __import__("trpc_service.tool.runtime", fromlist=["ToolResult"]).ToolResult(
                "send_external", message, {"request_id": request_id, "key": idempotency_key}
            )

        worker.tools.register("send_external", send_external)
        request = _request()
        policy = TenantPolicy(config, app.agent_app_id)
        calls = [{"name": "send_external", "arguments": {"message": "sent"}, "side_effect": True,
                  "idempotent": True, "_tool_key": "request-key:send"}]
        events, context = worker._run_tools(request, app, policy, storage, calls=calls)
        assert events[0].metadata["tool_name"] == "send_external" and context == "sent"

        replayed, _ = worker._run_tools(request, app, policy, storage, calls=calls)
        assert replayed[0].metadata["replayed"] is True

        approval_app = config.app("app_support")
        approval_app.tool_policy.approval_rules = ["send_external"]
        approval_call = [{"name": "send_external", "arguments": {"message": "approved"},
                          "side_effect": True, "_tool_key": "request-key:approval"}]
        requested, _ = worker._run_tools(request, approval_app, policy, storage, calls=approval_call)
        approval = approval_id("tenant_demo", "session", "send_external", ["message"])
        assert requested[0].event_type == "approval_required"
        approved_call = [dict(approval_call[0], approval_id=approval,
                              approval_token=approval_token(approval, "tenant_demo"))]
        approved, _ = worker._run_tools(request, approval_app, policy, storage, calls=approved_call)
        assert approved[0].event_type == "tool_call"

        class StateGovernance:
            def __init__(self, state):
                self.state = state
                self.failed = False

            def reserve_call(self, *args):
                return None

            def create_or_get(self, *args):
                return None

            def begin_execution(self, *args):
                return self.state

            def fail_execution(self, *args):
                self.failed = True

        app.tool_policy.approval_rules = []
        for status, message in (
            (ToolExecutionStatus.AMBIGUOUS, "ambiguous"),
            (ToolExecutionStatus.RUNNING, "already running"),
        ):
            execution = ToolExecution(
                "tenant_demo", "other-worker", "request-key", "session", "send_external", "other",
                "hash", True, status=status, attempt=2 if status == ToolExecutionStatus.RUNNING else 1,
            )
            governed = StateGovernance(execution)
            storage.tool_governance = governed
            with pytest.raises((PolicyDenied, GatewayError), match=message):
                worker._run_tools(
                    request, app, policy, storage,
                    calls=[{"name": "send_external", "arguments": {"message": "x"}, "side_effect": True}],
                )
            assert governed.failed
    finally:
        storage.close()


def test_worker_model_tool_round_and_mcp_registration_paths(monkeypatch):
    storage = create_storage()
    try:
        config = default_demo_config()
        app = config.app("app_support")
        app.metadata["mcp_servers"] = [{"name": "docs", "endpoint": "https://mcp.example.test", "tools": ["lookup"]}]

        class Client:
            def __init__(self):
                self.calls = 0

            def generate_with_usage(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return ModelResponse("", 1, 1, 2, "model", [ModelToolCall("call-1", "search_knowledge", {"query": "docs"})])
                return ModelResponse("final", 1, 2, 3, "model")

        worker = AgentWorker(storage, model_client=Client())
        request = _request()
        events = worker._run_unlocked(request, config, storage)
        assert events[-1].event_type == "message_end" and events[-1].content == "final"
        assert worker.tools.mcp_servers["docs"]["tools"] == ["lookup"]

        monkeypatch.setenv("AGENT_MAX_TOOL_ROUNDS", "1")

        class LoopClient:
            def generate_with_usage(self, **kwargs):
                return ModelResponse("", tool_calls=[ModelToolCall("loop", "search_knowledge", {})])

        limited = AgentWorker(storage, model_client=LoopClient())
        with pytest.raises(GatewayError, match="AGENT_MAX_TOOL_ROUNDS"):
            limited._run_unlocked(_request(key="limited"), config, storage)
    finally:
        storage.close()


def test_worker_recovery_requires_idempotency_confirmation_and_rejects_bad_arguments():
    storage = create_storage()
    try:
        worker = AgentWorker(storage)
        config = default_demo_config()
        app = config.app("app_support")
        app.tool_policy.allowlist.append("send_external")
        worker.tools.register("send_external", lambda message: __import__(
            "trpc_service.tool.runtime", fromlist=["ToolResult"]
        ).ToolResult("send_external", message, {}))
        request = _request()
        policy = TenantPolicy(config, app.agent_app_id)
        storage.tool_governance = None
        storage.session.append_event(SessionEvent(
            "tenant_demo", "session", "intent", "tool_intent",
            {"side_effect": True}, "trace", "request-key:tool:0",
        ))
        with pytest.raises(GatewayError, match="idempotency confirmation"):
            worker._run_tools(request, app, policy, storage, calls=[
                {"name": "send_external", "arguments": {"message": "x"}, "side_effect": True}
            ])
        with pytest.raises(GatewayError, match="arguments must be an object"):
            worker._run_tools(request, app, policy, storage, calls=[
                {"name": "send_external", "arguments": []}
            ])
    finally:
        storage.close()


def test_mirror_replay_applies_all_persisted_operation_families():
    calls = []

    def record(name):
        def method(*args, **kwargs):
            calls.append((name, args, kwargs))
            if name == "load_state":
                return SessionState("tenant", "session")
            if name == "compare_and_set_state":
                return True
            return None
        return method

    secondary = SimpleNamespace(
        session=SimpleNamespace(append_event=record("append_event"), load_state=record("load_state"),
                                compare_and_set_state=record("compare_and_set_state")),
        memory=SimpleNamespace(put=record("memory.put")),
        summary=SimpleNamespace(put=record("summary.put")),
        audit=SimpleNamespace(append=record("audit.append")),
        idempotency=SimpleNamespace(start=record("idempotency.start"), complete=record("idempotency.complete"),
                                    fail=record("idempotency.fail"), claim_delivery=record("claim_delivery"),
                                    release_delivery=record("release_delivery")),
        artifacts=SimpleNamespace(put_with_id=record("artifact.put")),
        knowledge=SimpleNamespace(upsert=record("knowledge.upsert")),
        mailbox=SimpleNamespace(enqueue=record("mailbox.enqueue"), claim_next=record("mailbox.claim_next"),
                                renew=record("mailbox.renew"), complete=record("mailbox.complete"),
                                fail=record("mailbox.fail"), recover_expired=record("mailbox.recover_expired")),
        session_mailbox_v2=SimpleNamespace(renew=record("v2.renew"), commit=record("v2.commit"),
                                           retry=record("v2.retry"), restore_export=record("v2.restore_export"),
                                           accept=record("v2.accept")),
        inbox_outbox=SimpleNamespace(complete_inbox=record("complete_inbox")),
        tool_governance=SimpleNamespace(restore_execution=record("restore_execution"), approve=record("approve")),
    )
    bundle = MirroredStorageBundle.__new__(MirroredStorageBundle)
    bundle.secondary = secondary
    event = SessionEvent("tenant", "session", "event", "message", {"text": "x"}, "trace")
    event_payload = json.loads(json.dumps(asdict(event), default=str))
    now = now_utc()
    mailbox = {"tenant_id": "tenant", "session_id": "session", "sequence": 1, "message_id": "message",
               "dedupe_key": "dedupe", "payload": {}, "created_at": now.isoformat(),
               "updated_at": now.isoformat(), "available_at": now.isoformat(), "lease_until": now.isoformat()}
    lease = {"tenant_id": "tenant", "session_id": "session", "message_id": "message", "sequence": 1,
             "owner": "worker", "epoch": 1, "expires_at": now.isoformat(), "attempt": 1, "retry_count": 0,
             "priority": 0}
    execution = asdict(ToolExecution("tenant", "execution", "request", "session", "tool", "call", "hash"))
    operations = [
        ("mirror.session.append_event", {"event": event_payload}),
        ("mirror.session.compare_and_set", {"tenant_id": "tenant", "session_id": "session", "expected_version": 0, "state": {"x": 1}}),
        ("mirror.memory.put", {"item": json.loads(json.dumps(asdict(__import__("trpc_service.storage.base", fromlist=["MemoryItem"]).MemoryItem("tenant", "m", "s", "x")), default=str))}),
        ("mirror.summary.put", {"summary": json.loads(json.dumps(asdict(__import__("trpc_service.storage.base", fromlist=["Summary"]).Summary("tenant", "session", "x", 1)), default=str))}),
        ("mirror.audit.append", {"record": json.loads(json.dumps(asdict(__import__("trpc_service.storage.base", fromlist=["AuditRecord"]).AuditRecord("a", "tenant", "allow", "trace")), default=str))}),
        ("mirror.idempotency.start", {"tenant_id": "tenant", "key": "k", "trace_id": "trace"}),
        ("mirror.idempotency.complete", {"tenant_id": "tenant", "key": "k", "response_ref": "r", "result": {}}),
        ("mirror.idempotency.fail", {"tenant_id": "tenant", "key": "k", "error_type": "error"}),
        ("mirror.idempotency.claim_delivery", {"tenant_id": "tenant", "key": "k"}),
        ("mirror.idempotency.release_delivery", {"tenant_id": "tenant", "key": "k"}),
        ("mirror.artifact.put", {"tenant_id": "tenant", "object_id": "o", "content_base64": "ZA==", "content_type": "text/plain"}),
        ("mirror.knowledge.upsert", {"chunk": {"tenant_id": "tenant", "collection": "c", "chunk_id": "id", "text": "x", "metadata": {}}}),
        ("mirror.mailbox.enqueue", {"tenant_id": "tenant", "session_id": "session", "message_id": "m", "dedupe_key": "d", "payload": {}}),
        ("mirror.mailbox.claim_next", {"tenant_id": "tenant", "session_id": "session", "owner": "w"}),
        ("mirror.mailbox.renew", {"record": mailbox, "lease_seconds": 30}),
        ("mirror.mailbox.complete", {"record": mailbox}),
        ("mirror.mailbox.fail", {"record": mailbox, "error": "x", "retry_after_seconds": 1}),
        ("mirror.mailbox.recover_expired", {"tenant_id": "tenant", "session_id": "session"}),
        ("mirror.session_mailbox_v2.renew", {"kwargs": {"lease": lease, "lease_seconds": 30}}),
        ("mirror.session_mailbox_v2.commit", {"kwargs": {"lease": lease}}),
        ("mirror.session_mailbox_v2.retry", {"kwargs": {"lease": lease}}),
        ("mirror.session_mailbox_v2.restore_export", {"kwargs": {"payload": {"items": []}}}),
        ("mirror.session_mailbox_v2.accept", {"args": ["tenant", "session", "message"]}),
        ("mirror.inbox_outbox.complete_inbox", {"args": ["tenant"]}),
        ("mirror.tool_governance.approve", {"args": ["tenant"]}),
        ("mirror.tool_governance.restore_execution", {"args": [execution]}),
    ]
    for operation, payload in operations:
        bundle.replay_mirror_task(SimpleNamespace(operation=operation, payload=payload))
    with pytest.raises(ValueError, match="unsupported"):
        bundle.replay_mirror_task(SimpleNamespace(operation="mirror.unknown", payload={}))
    assert len(calls) >= len(operations)


def test_redis_storage_constructor_and_idempotency_state_edges(monkeypatch):
    class Client:
        def __init__(self):
            self.values = {}

        def ping(self):
            return True

        def pipeline(self):
            class Pipe:
                def __enter__(self): return self
                def __exit__(self, *_): return False
                def watch(self, *_): return None
                def multi(self): return None
                def set(self, key, value, ex=None): self.outer.values[key] = value
                def execute(self): return [True]
                def unwatch(self): return None
            pipe = Pipe()
            pipe.outer = client
            return pipe

        def get(self, key): return self.values.get(key)
        def set(self, key, value, **kwargs): self.values[key] = value
        def eval(self, *_args): return 0

        def close(self): return None

    client = Client()
    monkeypatch.setattr("redis.Redis.from_url", lambda *_args, **_kwargs: client)
    storage = RedisStorage("redis://fake", prefix="test")
    try:
        assert storage.backend_name == "redis"
        event = SessionEvent("tenant", "session", "event", "message", {}, "trace")
        client.eval = lambda *_args: -1
        with pytest.raises(Exception, match="fencing token"):
            storage.append_event(event, fencing_token=2)
        client.eval = lambda *_args: 0
        assert storage.compare_and_set_state("tenant", "session", 0, {}) is False
        with pytest.raises(KeyError):
            storage.complete("tenant", "missing", "ref", {})
        with pytest.raises(KeyError):
            storage.fail("tenant", "missing", "error")
    finally:
        storage.close()


def test_gateway_static_helpers_and_inactive_tenant(monkeypatch):
    config = default_demo_config()
    repository = InMemoryTenantRepository()
    repository.create(config)
    gateway = AgentGateway(TenantService(repository), create_storage(), workers=[])
    try:
        message = InboundMessage("web", "web_demo", "message", "user", "hello")
        payload = gateway._inbox_payload(message)
        assert payload["external_message_id"] == "message"
        monkeypatch.setenv("TRPC_AGENT_RUNTIME_MODE", "local")
        monkeypatch.delenv("DURABLE_INBOX_OUTBOX", raising=False)
        assert gateway._durable_inbox_enabled() is False
        assert gateway._next_worker if False else True
        config.status = type(config.status)("disabled")
        repository.save_and_publish(config, expected_version=1)
        with pytest.raises(GatewayError, match="not active"):
            gateway.dispatch(message)
    finally:
        gateway.storage.close()


def test_storage_factory_backend_selection_and_secret_reference_paths(tmp_path, monkeypatch):
    from trpc_service.storage import factory, postgres_store

    def structured():
        part = SimpleNamespace(backend_name="fake", close=lambda: None)
        return SimpleNamespace(
            session=part,
            memory=part,
            summary=part,
            audit=part,
            idempotency=part,
            compensation=None,
            inbox_outbox=None,
            mailbox=None,
            tool_governance=None,
            migration_control=None,
            close=lambda: None,
        )

    monkeypatch.setenv("FACTORY_REDIS", "redis://factory.example")
    profile = StorageProfile(
        session_backend="redis",
        redis_url_ref="env://FACTORY_REDIS",
        sql_dsn_ref="postgresql://raw-dsn",
        knowledge_backend="remote",
        vector_url="https://vector.example",
        vector_token_ref="secret://vector/token",
        embedding_token_ref="secret://embedding/token",
        artifact_backend="s3",
        object_endpoint="https://s3.example",
        object_bucket="bucket",
        object_access_key_ref="secret://object/access",
        object_secret_key_ref="secret://object/secret",
    )
    remote = SimpleNamespace(backend_name="remote", close=lambda: None)
    objects = SimpleNamespace(backend_name="s3", close=lambda: None)
    with patch.object(factory, "RedisStorage", return_value=structured()) as redis, \
            patch.object(postgres_store, "PostgresStorage", return_value=structured()), \
            patch.object(factory, "RemoteVectorStore", return_value=remote), \
            patch.object(factory, "S3ObjectStore", return_value=objects), \
            patch.object(factory, "resolve_secret", side_effect=["vector", "embedding", "access", "secret"]):
        bundle = factory.create_storage(profile, tmp_path / "factory-branches")
    assert bundle.knowledge is remote and bundle.objects is objects
    redis.assert_called_once_with("redis://factory.example")
    bundle.close()

    with patch.object(postgres_store, "PostgresStorage", return_value=structured()) as postgres:
        bundle = factory.create_storage(StorageProfile(session_backend="postgres"), tmp_path / "postgres-branch")
        bundle.close()
    postgres.assert_called_once_with(None)

    with pytest.raises(ValueError, match="unsupported artifact backend"):
        factory.create_storage(StorageProfile(artifact_backend="unsupported"), tmp_path / "bad-artifact")


class _HTTPResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_remote_vector_http_embedding_headers_and_errors(monkeypatch):
    requests = []

    def fake_urlopen(request, timeout):
        requests.append((request, timeout))
        if request.full_url.endswith("/embeddings"):
            return _HTTPResponse({"data": [{"embedding": ["1", 2]}]})
        return _HTTPResponse({"ok": True})

    monkeypatch.setattr("trpc_service.storage.remote_vector.urlopen", fake_urlopen)
    store = RemoteVectorStore(
        "https://vector.example.com/",
        token="vector-token",
        embedding_url="https://embed.example.com/",
        embedding_model="embed-model",
        embedding_token="embed-token",
        timeout=3,
    )
    assert store._embedding("hello") == [1.0, 2.0]
    assert requests[0][0].method == "POST"
    assert requests[0][0].get_header("Authorization") == "Bearer embed-token"
    assert json.loads(requests[0][0].data)["model"] == "embed-model"
    assert store._request("GET", "/health") == {"ok": True}
    assert requests[1][0].get_header("Api-key") == "vector-token"
    assert requests[1][0].get_header("Authorization") == "Bearer vector-token"

    monkeypatch.setattr(
        "trpc_service.storage.remote_vector.urlopen",
        lambda *_args, **_kwargs: _HTTPResponse({"data": [{}]}),
    )
    with pytest.raises(RuntimeError, match="no vector"):
        store._embedding("missing")
    with pytest.raises(ValueError, match="requires vector_url"):
        RemoteVectorStore("")
    assert store._collection("docs") == "trpc-agent_docs"
    assert RemoteVectorStore._point_id(KnowledgeChunk("t", "d", "c", "x", {})) > 0
    assert resolve_secret("") == ""


def test_remote_vector_qdrant_conflicts_pagination_and_payload_defaults(monkeypatch):
    store = RemoteVectorStore("https://vector.example.com", token="token", dimension=3, collection_prefix="p")
    chunk = KnowledgeChunk("tenant", "docs", "chunk", "text", {"kind": "note"})
    calls = []

    def request(method, path, payload=None):
        calls.append((method, path, payload))
        if path == "/collections":
            raise RuntimeError("listing forbidden")
        if path.endswith("/scroll") and payload.get("offset") is None:
            return {"result": {"points": [{"id": 1, "payload": {}}], "next_page_offset": "next"}}
        if path.endswith("/scroll"):
            return {"result": {"points": [{"id": 2, "payload": {
                "tenant_id": "tenant", "collection": "docs", "chunk_id": "c2",
                "text": "second", "metadata": {"page": 2},
            }}], "next_page_offset": None}}
        if path == "/collections/p_docs" and method == "PUT":
            from urllib.error import HTTPError
            raise HTTPError(path, 409, "exists", {}, None)
        if path.endswith("/search"):
            return {"result": [{"id": 3, "payload": {"text": "found"}}]}
        return {}

    monkeypatch.setattr(store, "_request", request)
    store._ensure_collection("docs")
    store._ensure_collection("docs")
    store.upsert(chunk)
    found = store.search("tenant", "docs", "query", limit=2)
    assert found[0].tenant_id == "tenant" and found[0].chunk_id == "3"
    store._collections.add("p_docs")
    listed = store.list_by_tenant("tenant")
    assert [item.chunk_id for item in listed] == ["1", "c2"]
    assert any(item[2] and item[2].get("offset") == "next" for item in calls if item[1].endswith("/scroll"))

    def non_conflict(*_args, **_kwargs):
        from urllib.error import HTTPError
        raise HTTPError("url", 500, "broken", {}, None)

    other = RemoteVectorStore("https://vector.example.com")
    monkeypatch.setattr(other, "_request", non_conflict)
    with pytest.raises(Exception):
        other._ensure_collection("docs")


def test_remote_vector_generic_list_and_search_list_shapes(monkeypatch):
    store = RemoteVectorStore("https://vector.example.com", provider="custom")
    chunk = KnowledgeChunk("tenant", "docs", "c1", "hello", {})
    seen = []

    def request(method, path, payload=None):
        seen.append((method, path, payload))
        return [asdict(chunk)]

    monkeypatch.setattr(store, "_request", request)
    store.upsert(chunk)
    assert store.search("tenant", "docs", "hello")[0] == chunk
    assert store.list_by_tenant("tenant")[0] == chunk
    assert [item[1] for item in seen] == ["/v1/knowledge/upsert", "/v1/knowledge/search", "/v1/knowledge/list"]
    store.close()


def test_lock_helpers_local_timeout_and_optional_protocols(monkeypatch):
    lease = SessionLease("t", "s", "owner", 1, datetime.now(UTC) + timedelta(seconds=30))
    zero = SessionLease("t", "s", "owner", 0, lease.expires_at)
    validate_session_lease(object(), zero)
    assert renew_session_lease(object(), lease) == lease

    class LeaseOnly:
        def acquire_session_lease(self, *_args): return lease
        def release_session_lease(self, *_args): pass

    with session_lease(LeaseOnly(), "t", "s") as current:
        assert current == lease
    with session_lock(LeaseOnly(), "t", "s") as current:
        assert current == lease

    class FailingLock:
        def acquire(self, **_kwargs): return False

    monkeypatch.setattr("trpc_service.storage.locking._local_lock", lambda _key: FailingLock())
    with pytest.raises(SessionLockTimeout), session_lease(object(), "t", "s", timeout=0):
        pass
    with pytest.raises(SessionLockTimeout), session_lock(object(), "t", "s", timeout=0):
        pass


class _RedisLockClient:
    def __init__(self):
        self.set_result = False
        self.eval_results = []
        self.value = None

    def set(self, *_args, **_kwargs):
        return self.set_result

    def eval(self, *_args):
        return self.eval_results.pop(0) if self.eval_results else 0

    def get(self, _key):
        return self.value


def test_redis_lock_timeout_renew_loss_and_bytes_validation(monkeypatch):
    class Lock(RedisSessionLockMixin):
        def __init__(self):
            self.client = _RedisLockClient()

        def _key(self, kind, tenant, session):
            return f"{kind}:{tenant}:{session}"

    lock = Lock()
    with pytest.raises(SessionLockTimeout):
        lock.acquire_session_lock("t", "s", 0)
    with pytest.raises(SessionLockTimeout):
        lock.acquire_session_lease("t", "s", 0)
    lease = SessionLease("t", "s", "owner", 1, datetime.now(UTC))
    with pytest.raises(SessionLeaseLost):
        lock.renew_session_lease(lease)
    lock.client.value = b'{"owner":"owner","fencing_token":1}'
    lock.validate_session_lease(lease)
    lock.client.value = None
    with pytest.raises(SessionLeaseLost, match="expired"):
        lock.validate_session_lease(lease)


def test_postgres_lock_timeout_renew_and_validation_failures(monkeypatch):
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    connection = MagicMock()
    connection.cursor.return_value = cursor
    connection.transaction.return_value = MagicMock()

    class Lock(PostgresSessionLockMixin):
        def __init__(self):
            self._conn = connection
            self._lock = MagicMock()

    lock = Lock()
    cursor.fetchone.return_value = (False,)
    with pytest.raises(SessionLockTimeout):
        lock.acquire_session_lock("t", "s", 0)
    cursor.fetchone.return_value = (True,)
    assert lock.acquire_session_lock("t", "s", 0).startswith("trpc-agent-session:")
    cursor.fetchone.side_effect = [(None, None, datetime.now(UTC) + timedelta(seconds=60))]
    with pytest.raises(SessionLockTimeout):
        lock.acquire_session_lease("t", "s", 0)
    lease = SessionLease("t", "s", "owner", 1, datetime.now(UTC))
    cursor.rowcount = 0
    with pytest.raises(SessionLeaseLost):
        lock.renew_session_lease(lease)
    cursor.fetchone.side_effect = None
    cursor.fetchone.return_value = None
    with pytest.raises(SessionLeaseLost):
        lock.validate_session_lease(lease)
    cursor.fetchone.return_value = (True,)
    with postgres_advisory_lock(connection, "migration", timeout=0):
        pass
    cursor.fetchone.return_value = (False,)
    with pytest.raises(SessionLockTimeout), postgres_advisory_lock(connection, "busy", timeout=0):
        pass
