import pytest

from trpc_service.storage import KnowledgeIngestionService, LocalVectorKnowledgeStore, TextChunker


@pytest.mark.asyncio
async def test_ingestion_chunks_embeds_and_scopes_document():
    store = LocalVectorKnowledgeStore()
    seen = []

    async def embed(text):
        seen.append(text)
        return [float(len(text)), 1.0]

    service = KnowledgeIngestionService(store,
                                        tenant_id="tenant_a",
                                        knowledge_base_id="kb_a",
                                        index_version=3,
                                        embedder=embed,
                                        chunker=TextChunker(chunk_size=8, overlap=2))
    result = await service.ingest_document("doc-1", "alpha beta gamma", {"source": "test"})
    assert result.chunk_count == len(seen) >= 2
    assert len(store.iter_index("tenant_a", "kb_a", 3)) == result.chunk_count
    assert all(item.metadata["content_digest"] == result.content_digest
               for item in store.search("tenant_a", "kb_a", 3, [1.0, 1.0]))
    assert store.search("tenant_b", "kb_a", 3, [1.0, 1.0]) == []


def test_chunker_rejects_invalid_overlap():
    with pytest.raises(ValueError):
        TextChunker(chunk_size=4, overlap=4)


@pytest.mark.asyncio
async def test_embedding_failure_preserves_previous_document_and_replacement_removes_old_chunks():
    store = LocalVectorKnowledgeStore()
    service = KnowledgeIngestionService(store, tenant_id="tenant_a", knowledge_base_id="kb_a", index_version=1,
                                        embedder=lambda text: [1.0, 0.0], chunker=TextChunker(8, 0))
    await service.ingest_document("doc", "old content with several chunks")
    previous = store.search("tenant_a", "kb_a", 1, [1.0, 0.0])
    service.embedder = lambda text: [float("nan"), 0.0]
    with pytest.raises(ValueError):
        await service.ingest_document("doc", "replacement")
    assert store.search("tenant_a", "kb_a", 1, [1.0, 0.0]) == previous
    service.embedder = lambda text: [1.0, 0.0]
    await service.ingest_document("doc", "new")
    assert [hit.text for hit in store.search("tenant_a", "kb_a", 1, [1.0, 0.0])] == ["new"]
