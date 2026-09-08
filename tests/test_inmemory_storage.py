import asyncio

import pytest

from trpc_service.storage.artifacts import LocalArtifactStore
from trpc_service.storage.contracts import (
    EventInput,
    MemoryInput,
    SessionIdentity,
    TurnCommit,
)
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.exceptions import (
    DuplicateMessageError,
    InvalidArtifactKeyError,
    LockNotAcquiredError,
    VersionConflictError,
)
from trpc_service.storage.inmemory import (
    InMemoryConversationStore,
    InMemoryCoordinationStore,
)
from trpc_service.storage.keys import idempotency_key, session_lock_key
from trpc_service.storage.outbox import OutboxWorker, VectorOutboxHandlers
from trpc_service.storage.vector import InMemoryVectorStore, SemanticStore


def test_coordination_contracts() -> None:
    async def scenario() -> None:
        store = InMemoryCoordinationStore()
        key = idempotency_key("tenant-1", "wecom", "message-1")
        assert key == "tenant-1:wecom:message-1"
        assert await store.claim(key, ttl_seconds=30)
        assert not await store.claim(key, ttl_seconds=30)
        await store.complete(key, {"reply_id": "reply-1"})
        completed = await store.get(key)
        assert completed is not None
        assert completed.status == "completed"
        assert completed.result == {"reply_id": "reply-1"}

        await store.set_state("typing", {"active": True}, ttl_seconds=30)
        assert await store.get_state("typing") == {"active": True}
        await store.delete_state("typing")
        assert await store.get_state("typing") is None

        first = await store.check("tenant-1", limit=2, window_seconds=60)
        second = await store.check("tenant-1", limit=2, window_seconds=60)
        third = await store.check("tenant-1", limit=2, window_seconds=60)
        assert (first.allowed, second.allowed, third.allowed) == (True, True, False)

        lock_key = session_lock_key("tenant-1", "app-1", "session-1")
        async with store.acquire(lock_key):
            with pytest.raises(LockNotAcquiredError):
                async with store.acquire(lock_key, wait_timeout_seconds=0.01):
                    pass

    asyncio.run(scenario())


def test_inmemory_turn_order_cas_and_outbox_vector_sync() -> None:
    async def scenario() -> None:
        store = InMemoryConversationStore()
        identity = SessionIdentity("tenant-1", "app-1", "user-1", "session-1")
        result = await store.commit_turn(
            TurnCommit(
                identity=identity,
                expected_version=0,
                event=EventInput(
                    event_type="user_message",
                    role="user",
                    payload={"text": "I prefer tea"},
                    trace_id="trace-1",
                    channel="wecom",
                    external_message_id="message-1",
                ),
                next_state={"turn": 1},
                summary_content="The user prefers tea.",
                memories=(
                    MemoryInput(
                        memory_key="preference-1",
                        text="The user prefers tea",
                        metadata={"source": "conversation"},
                    ),
                ),
            )
        )
        assert result.event.sequence_no == 1
        assert result.session.version == 1
        assert result.summary is not None
        assert result.summary.through_sequence == result.event.sequence_no

        with pytest.raises(VersionConflictError):
            await store.compare_and_swap_state(identity, 0, {"turn": 2})
        with pytest.raises(DuplicateMessageError):
            await store.commit_turn(
                TurnCommit(
                    identity=identity,
                    expected_version=1,
                    event=EventInput(
                        "user_message",
                        "user",
                        {"text": "duplicate"},
                        "trace-2",
                        "wecom",
                        "message-1",
                    ),
                    next_state={"turn": 2},
                )
            )

        semantic = SemanticStore(InMemoryVectorStore())
        worker = OutboxWorker(
            store,
            VectorOutboxHandlers(semantic).handlers(),
            worker_id="test-worker",
        )
        assert await worker.poll_once() == 1
        matches = await semantic.search_memories("tenant-1", "app-1", "user-1", "tea preference")
        assert matches[0].document.id == "preference-1"
        assert matches[0].score > 0

    asyncio.run(scenario())


def test_turn_coordinator_abandons_failed_claim_and_rejects_duplicate() -> None:
    async def scenario() -> None:
        data_plane = InMemoryConversationStore()
        coordination = InMemoryCoordinationStore()
        coordinator = TurnCoordinator(data_plane, coordination)
        identity = SessionIdentity("tenant-1", "app-1", "user-1", "session-1")

        def request(expected_version: int) -> TurnCommit:
            return TurnCommit(
                identity=identity,
                expected_version=expected_version,
                event=EventInput(
                    "user_message",
                    "user",
                    {"text": "hello"},
                    "trace-1",
                    "wecom",
                    "message-1",
                ),
                next_state={"turn": 1},
            )

        with pytest.raises(VersionConflictError):
            await coordinator.commit(request(expected_version=1))
        key = idempotency_key("tenant-1", "wecom", "message-1")
        assert await coordination.get(key) is None

        result = await coordinator.commit(request(expected_version=0))
        assert result.session.version == 1
        with pytest.raises(DuplicateMessageError):
            await coordinator.commit(request(expected_version=1))

    asyncio.run(scenario())


def test_semantic_store_isolates_tenants() -> None:
    async def scenario() -> None:
        semantic = SemanticStore(InMemoryVectorStore())
        await semantic.upsert_knowledge("tenant-a", "app", "doc-a", "refund policy")
        await semantic.upsert_knowledge("tenant-b", "app", "doc-b", "secret policy")

        matches = await semantic.search_knowledge("tenant-a", "app", "policy", limit=10)
        assert [match.document.id for match in matches] == ["doc-a"]

    asyncio.run(scenario())


def test_local_artifact_store(tmp_path) -> None:
    async def scenario() -> None:
        store = LocalArtifactStore(tmp_path)
        saved = await store.put(
            "tenant-1",
            "reports/result.txt",
            b"hello",
            "text/plain",
            {"source": "test"},
        )
        loaded = await store.get("tenant-1", "reports/result.txt")
        assert loaded.content == b"hello"
        assert loaded.metadata.checksum == saved.checksum
        assert loaded.metadata.metadata == {"source": "test"}

        with pytest.raises(InvalidArtifactKeyError):
            await store.put("tenant-1", "../escape", b"bad", "text/plain")
        await store.delete("tenant-1", "reports/result.txt")

    asyncio.run(scenario())
