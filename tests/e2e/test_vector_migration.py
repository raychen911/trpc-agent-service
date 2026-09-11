"""Real Qdrant writes, tombstones, readback and adversarial cutover checks."""

import uuid

import pytest

from trpc_service.migration import VectorMigration
from trpc_service.storage import LocalVectorKnowledgeStore


def seed_index():
    source = LocalVectorKnowledgeStore()
    for tenant in ("tenant_a", "tenant_b"):
        for index in range(6):
            source.upsert(tenant, "kb", 1, f"doc-{index}", str(uuid.uuid4()),
                          [1.0, 0.0] if index % 2 == 0 else [0.0, 1.0], f"{tenant} 文档 {index}",
                          {"revision": 1, "source": "synthetic"})
        source.delete_document(tenant, "kb", 1, "doc-5")
    return source


def migration(source, target):
    return VectorMigration(tenant_id="tenant_a", knowledge_base_id="kb", source_index_version=1,
                           target_index_version=2, source=source, target=target,
                           source_embedding=("deterministic", 2), target_embedding=("deterministic", 2))


def test_vector_copy_tombstones_incremental_changes_and_query_equivalence(vector_backend):
    source = seed_index()
    flow = migration(source, vector_backend)
    report = flow.copy_and_verify()
    assert (report.copied, report.tombstones) == (5, 1)
    source.delete_document("tenant_a", "kb", 1, "doc-0")
    source.upsert("tenant_a", "kb", 1, "new-doc", str(uuid.uuid4()), [1.0, 0.0], "新增知识", {"revision": 2})
    # Duplicate catchup must not duplicate chunks or resurrect deleted documents.
    flow.copy_and_verify()
    flow.copy_and_verify()
    expected = {c.chunk_id: (c.document_id, c.text, c.deleted, c.metadata)
                for c in source.iter_index("tenant_a", "kb", 1)}
    actual = {c.chunk_id: (c.document_id, c.text, c.deleted, c.metadata)
              for c in vector_backend.iter_index("tenant_a", "kb", 2)}
    assert actual == expected
    assert vector_backend.iter_index("tenant_b", "kb", 2) == ()
    flow.cutover()
    for query in ([1.0, 0.0], [0.0, 1.0]):
        local = {hit.chunk_id: hit.score for hit in source.search("tenant_a", "kb", 1, query)}
        remote = {hit.chunk_id: hit.score for hit in vector_backend.search("tenant_a", "kb", 2, query)}
        assert remote == pytest.approx(local)
    flow.rollback()
    assert flow.active_index_version == 1
    assert len(source.search("tenant_b", "kb", 1, [1.0, 0.0])) == 5


@pytest.mark.parametrize("collision", ["tenant", "index_version", "knowledge_base"])
def test_same_chunk_id_must_not_overwrite_another_scope(vector_backend, collision):
    chunk_id = str(uuid.uuid4())
    vector_backend.upsert("tenant_a", "kb", 1, "doc", chunk_id, [1.0, 0.0], "original")
    tenant, version = ("tenant_b", 1) if collision == "tenant" else ("tenant_a", 2)
    kb = "other-kb" if collision == "knowledge_base" else "kb"
    if collision == "knowledge_base":
        tenant, version = "tenant_a", 1
    vector_backend.upsert(tenant, kb, version, "doc", chunk_id, [1.0, 0.0], "new")
    original = vector_backend.search("tenant_a", "kb", 1, [1.0, 0.0])
    assert [hit.text for hit in original] == ["original"]
    assert [hit.text for hit in vector_backend.search(tenant, kb, version, [1.0, 0.0])] == ["new"]


def test_corrupt_payload_with_same_count_must_block_cutover(vector_backend):
    source = seed_index()

    class CorruptingTarget:
        def __getattr__(self, name):
            return getattr(vector_backend, name)

        def upsert(self, tenant, kb, version, doc, chunk, vector, text, metadata):
            vector_backend.upsert(tenant, kb, version, doc, chunk, vector, "corrupted", metadata)

    flow = migration(source, CorruptingTarget())
    try:
        flow.copy_and_verify()
        flow.cutover()
    except ValueError:
        return
    assert flow.active_index_version == 1, "corrupt index was accepted for cutover"


@pytest.mark.parametrize("side,field", [
    ("source", "text"), ("target", "text"), ("target", "metadata"),
    ("target", "vector"), ("target", "deleted"), ("target", "document_id"),
])
def test_changes_after_verification_invalidate_cutover(vector_backend, side, field):
    source = seed_index()
    flow = migration(source, vector_backend)
    flow.copy_and_verify()
    store, version = (source, 1) if side == "source" else (vector_backend, 2)
    chunk = next(c for c in store.iter_index("tenant_a", "kb", version) if not c.deleted)
    if field == "deleted":
        store.delete_document("tenant_a", "kb", version, chunk.document_id)
    else:
        vector = [-v for v in chunk.vector] if field == "vector" else list(chunk.vector)
        store.upsert("tenant_a", "kb", version,
                     "wrong-doc" if field == "document_id" else chunk.document_id, chunk.chunk_id, vector,
                     "corrupted" if field == "text" else chunk.text,
                     {"revision": 999} if field == "metadata" else chunk.metadata)
    with pytest.raises(ValueError, match="changed after verification"):
        flow.cutover()
    assert flow.active_index_version == 1
    assert not flow.verified


def test_failed_reverification_revokes_previous_permission(vector_backend):
    source = seed_index()
    flow = migration(source, vector_backend)
    flow.copy_and_verify()
    vector_backend.upsert("tenant_a", "kb", 2, "extra", "non-uuid-extra", [1.0, 0.0], "unexpected")
    with pytest.raises(ValueError, match="verification failed"):
        flow.copy_and_verify()
    with pytest.raises(ValueError, match="must verify"):
        flow.cutover()


def test_reembedding_verifies_new_dimension_and_tombstones(vector_backend):
    source = LocalVectorKnowledgeStore()
    for doc in ("live", "deleted"):
        source.upsert("tenant_a", "kb", 1, doc, doc, [1.0, 2.0, 3.0], doc)
    source.delete_document("tenant_a", "kb", 1, "deleted")
    flow = VectorMigration(tenant_id="tenant_a", knowledge_base_id="kb", source_index_version=1,
                           target_index_version=2, source=source, target=vector_backend,
                           source_embedding=("old", 3), target_embedding=("new", 2),
                           reembed=lambda text: [3.0, 4.0])
    flow.copy_and_verify()
    flow.cutover()
    assert flow.active_index_version == 2
    assert len(vector_backend.search("tenant_a", "kb", 2, [3.0, 4.0])) == 1
