"""Actual Qdrant Python local engine; this does not claim remote-server coverage."""

from dataclasses import replace
from pathlib import Path

import pytest

from tenant_agent.models import KnowledgeRecord
from tenant_agent.services.migration import DataMigrator, GoldenQuery
from tenant_agent.storage.base import TenantDataPlane
from tenant_agent.storage.external import QdrantKnowledgeRepository
from tenant_agent.storage.sql import SqlPlane


async def test_sql_to_real_qdrant_local_migration_and_restart(tmp_path: Path) -> None:
    qdrant = pytest.importorskip("qdrant_client")
    from qdrant_client.local.persistence import CollectionPersistence

    # Avoid the SDK's leaked temporary thread-safety probe connection on Windows;
    # the local engine is used only from this async test thread.
    CollectionPersistence.CHECK_SAME_THREAD = False
    source = SqlPlane(f"sqlite+aiosqlite:///{(tmp_path / 'source.db').as_posix()}")
    await source.initialize()
    target = QdrantKnowledgeRepository('{"url":"http://unused.invalid"}')
    # Supply the real SDK's on-disk local engine. No HTTP stub or fake vector
    # implementation participates in this test.
    target._client = qdrant.AsyncQdrantClient(path=str(tmp_path / "qdrant"))
    target._models = qdrant.models
    source_plane = TenantDataPlane(
        sessions=source,
        memories=source,
        summaries=source,
        artifacts=source,
        knowledge=source,
        audit=source,
        receipts=source,
        usage=source,
        concurrency=source,
        outbox=source,
        leases=source,
    )
    original = KnowledgeRecord(
        tenant_id="alpha",
        document_id="document",
        chunk_id="chunk",
        text="alpha content",
        embedding=(0.1, 0.2, 0.3),
        metadata={"kind": "test"},
    )
    try:
        await source.put_knowledge(original)
        report = await DataMigrator(source_plane, replace(source_plane, knowledge=target)).migrate(
            "alpha",
            resources=("knowledge",),
            golden_queries=(GoldenQuery(embedding=(0.1, 0.2, 0.3), expected_ids=("document/chunk",)),),
        )
        assert report.verified and report.recall_checks == 1
        assert report.verification_modes["knowledge"] == "cosine-normalized-content"
        await target.put_knowledge(
            original.model_copy(update={"tenant_id": "bravo", "text": "bravo content"})
        )
        alpha = await target.search_knowledge("alpha", (0.1, 0.2, 0.3), metadata_filter={"kind": "test"})
        assert len(alpha) == 1 and alpha[0].text == "alpha content"
        assert alpha[0].embedding != original.embedding
        await target.close()
        target._client = qdrant.AsyncQdrantClient(path=str(tmp_path / "qdrant"))
        persisted = [row async for row in target.iter_knowledge("alpha")]
        assert len(persisted) == 1 and persisted[0].tenant_id == "alpha"
        assert await target.healthcheck()
    finally:
        await target.close()
        await source.close()
