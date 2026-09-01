"""Contract tests for the explicitly development-only InMemory backend."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace

import pytest

from trpc_service.backends import (
    ArtifactObject,
    BackendConflictError,
    InMemoryBackend,
    KnowledgeDocument,
    MemoryProjection,
    ScopedStateProjection,
    SessionProjection,
    SummaryProjection,
    WatermarkRegressionError,
    WriteDisposition,
)


def session_projection(
    tenant_id: str = "tenant-a",
    *,
    version: int = 0,
    watermark: int = 0,
    state: dict[str, object] | None = None,
) -> SessionProjection:
    return SessionProjection(
        tenant_id=tenant_id,
        session_id="shared-session",
        version=version,
        committed_through=watermark,
        state=state or {"turn": version},
    )


@pytest.mark.asyncio
async def test_session_cas_is_atomic_monotonic_and_tenant_isolated() -> None:
    backend = InMemoryBackend()
    assert backend.consistency.development_only is True
    assert (
        await backend.compare_and_set_session(
            session_projection("tenant-a"),
            expected_version=None,
        )
    ).disposition is WriteDisposition.APPLIED
    assert (
        await backend.compare_and_set_session(
            session_projection("tenant-b", state={"owner": "b"}),
            expected_version=None,
        )
    ).disposition is WriteDisposition.APPLIED

    tenant_a = await backend.get_session("tenant-a", "shared-session")
    tenant_b = await backend.get_session("tenant-b", "shared-session")
    assert tenant_a is not None and tenant_a.state == {"turn": 0}
    assert tenant_b is not None and tenant_b.state == {"owner": "b"}
    assert await backend.get_session("tenant-c", "shared-session") is None

    candidates = (
        session_projection(version=1, watermark=1, state={"winner": "one"}),
        session_projection(version=1, watermark=1, state={"winner": "two"}),
    )
    outcomes = await asyncio.gather(
        *(
            backend.compare_and_set_session(candidate, expected_version=0)
            for candidate in candidates
        )
    )
    assert [outcome.disposition for outcome in outcomes].count(WriteDisposition.APPLIED) == 1
    assert [outcome.disposition for outcome in outcomes].count(WriteDisposition.CONFLICT) == 1
    current = await backend.get_session("tenant-a", "shared-session")
    assert current is not None and current.version == current.committed_through == 1
    current.state["local"] = "mutation"
    persisted = await backend.get_session("tenant-a", "shared-session")
    assert persisted is not None and "local" not in persisted.state
    current = persisted

    assert (
        await backend.compare_and_set_session(current, expected_version=1)
    ).disposition is WriteDisposition.UNCHANGED
    with pytest.raises(BackendConflictError):
        await backend.compare_and_set_session(
            replace(current, state={"tampered": True}),
            expected_version=1,
        )
    with pytest.raises(WatermarkRegressionError):
        await backend.compare_and_set_session(
            session_projection(version=0, watermark=0),
            expected_version=1,
        )


@pytest.mark.asyncio
async def test_scoped_state_has_independent_tenant_aware_occ() -> None:
    backend = InMemoryBackend()
    initial = ScopedStateProjection(
        tenant_id="tenant-a",
        app_id="support",
        app_revision=1,
        scope="user",
        subject_id="same-user",
        version=0,
        state={"locale": "zh-CN"},
    )
    assert (
        await backend.compare_and_set_scoped_state(initial, expected_version=None)
    ).disposition is WriteDisposition.APPLIED
    other_tenant = replace(initial, tenant_id="tenant-b", state={"locale": "en-US"})
    await backend.compare_and_set_scoped_state(other_tenant, expected_version=None)

    assert (await backend.get_scoped_state("tenant-a", "support", "user", "same-user")) == initial
    assert (
        await backend.get_scoped_state("tenant-b", "support", "user", "same-user")
    ) == other_tenant
    assert (
        await backend.compare_and_set_scoped_state(
            replace(initial, version=1, state={"locale": "fr"}),
            expected_version=99,
        )
    ).disposition is WriteDisposition.CONFLICT
    updated = replace(initial, version=1, state={"locale": "fr"})
    assert (
        await backend.compare_and_set_scoped_state(updated, expected_version=0)
    ).disposition is WriteDisposition.APPLIED
    assert (
        await backend.compare_and_set_scoped_state(updated, expected_version=1)
    ).disposition is WriteDisposition.UNCHANGED
    with pytest.raises(BackendConflictError):
        await backend.compare_and_set_scoped_state(
            replace(updated, state={"locale": "conflict"}),
            expected_version=1,
        )
    with pytest.raises(WatermarkRegressionError):
        await backend.compare_and_set_scoped_state(initial, expected_version=1)
    with pytest.raises(ValueError, match="scope"):
        await backend.get_scoped_state("tenant-a", "support", "session", "same-user")


@pytest.mark.asyncio
async def test_summary_and_memory_are_monotonic_idempotent_and_isolated() -> None:
    backend = InMemoryBackend()
    summary = SummaryProjection(
        tenant_id="tenant-a",
        session_id="session-1",
        through_seq=4,
        content="summary four",
        summarizer_version="v1",
    )
    assert (await backend.put_summary_if_newer(summary)).disposition is WriteDisposition.APPLIED
    assert (await backend.put_summary_if_newer(summary)).disposition is WriteDisposition.UNCHANGED
    assert await backend.get_summary("tenant-a", "session-1") == summary
    assert await backend.get_summary("tenant-b", "session-1") is None
    with pytest.raises(BackendConflictError):
        await backend.put_summary_if_newer(replace(summary, content="different"))
    with pytest.raises(WatermarkRegressionError):
        await backend.put_summary_if_newer(replace(summary, through_seq=3))

    memory = MemoryProjection(
        tenant_id="tenant-a",
        memory_id="memory-1",
        principal_id="principal-1",
        session_id="session-1",
        source_event_id="event-1",
        extractor_version="extractor-v1",
        record_version=4,
        content="customer prefers concise answers",
        metadata={"kind": "preference"},
    )
    assert (await backend.put_memory_once(memory)).disposition is WriteDisposition.APPLIED
    assert (await backend.put_memory_once(memory)).disposition is WriteDisposition.UNCHANGED
    with pytest.raises(BackendConflictError):
        await backend.put_memory_once(replace(memory, content="conflicting"))
    with pytest.raises(BackendConflictError, match="memory_id"):
        await backend.put_memory_once(
            replace(memory, source_event_id="event-2", extractor_version="extractor-v2")
        )
    await backend.put_memory_once(replace(memory, tenant_id="tenant-b"))
    assert await backend.list_memories("tenant-a", "principal-1") == (memory,)
    assert len(await backend.list_memories("tenant-b", "principal-1")) == 1
    assert await backend.list_memories("tenant-c", "principal-1") == ()
    with pytest.raises(ValueError, match="after_version"):
        await backend.list_memories("tenant-a", "principal-1", after_version=-2)
    with pytest.raises(ValueError, match="limit"):
        await backend.list_memories("tenant-a", "principal-1", limit=0)


@pytest.mark.asyncio
async def test_knowledge_and_artifact_validate_hashes_and_versions() -> None:
    backend = InMemoryBackend()
    content = "tenant handbook"
    document = KnowledgeDocument(
        tenant_id="tenant-a",
        document_id="handbook",
        version=1,
        content=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
        indexed_version=1,
    )
    assert (await backend.put_document_if_newer(document)).disposition is WriteDisposition.APPLIED
    assert (await backend.put_document_if_newer(document)).disposition is WriteDisposition.UNCHANGED
    assert await backend.get_latest_document("tenant-a", "handbook") == document
    next_content = "tenant handbook revision two"
    document_v2 = replace(
        document,
        version=2,
        indexed_version=2,
        content=next_content,
        content_hash=hashlib.sha256(next_content.encode()).hexdigest(),
    )
    assert (
        await backend.put_document_if_newer(document_v2)
    ).disposition is WriteDisposition.APPLIED
    with pytest.raises(BackendConflictError):
        await backend.put_document_if_newer(replace(document_v2, metadata={"conflict": True}))
    with pytest.raises(ValueError, match="content_hash"):
        await backend.put_document_if_newer(replace(document, version=2, content_hash="bad"))
    with pytest.raises(WatermarkRegressionError):
        await backend.put_document_if_newer(replace(document, version=0, indexed_version=0))

    payload = b"artifact bytes"
    artifact = ArtifactObject(
        tenant_id="tenant-a",
        artifact_id="report",
        version=1,
        media_type="application/pdf",
        content=payload,
        content_hash=hashlib.sha256(payload).hexdigest(),
    )
    assert (await backend.put_artifact_if_newer(artifact)).disposition is WriteDisposition.APPLIED
    assert (await backend.put_artifact_if_newer(artifact)).disposition is WriteDisposition.UNCHANGED
    assert await backend.get_latest_artifact("tenant-a", "report") == artifact
    assert await backend.get_latest_artifact("tenant-b", "report") is None
    with pytest.raises(BackendConflictError):
        await backend.put_artifact_if_newer(replace(artifact, metadata={"changed": True}))
    with pytest.raises(WatermarkRegressionError):
        await backend.put_artifact_if_newer(replace(artifact, version=0))
    with pytest.raises(ValueError, match="content_hash"):
        await backend.put_artifact_if_newer(replace(artifact, version=2, content_hash="not-a-hash"))


@pytest.mark.asyncio
async def test_inmemory_validation_rejects_cross_boundary_ambiguity() -> None:
    backend = InMemoryBackend()
    with pytest.raises(ValueError, match="tenant_id"):
        await backend.get_session("unsafe:tenant", "session")
    with pytest.raises(ValueError, match="session_id"):
        await backend.get_session("tenant-a", "")
    with pytest.raises(ValueError, match="version"):
        await backend.compare_and_set_session(
            session_projection(version=-1),
            expected_version=None,
        )
    with pytest.raises(ValueError, match="committed_through"):
        await backend.compare_and_set_session(
            session_projection(version=1, watermark=2),
            expected_version=None,
        )
