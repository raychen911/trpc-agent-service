import pytest

from trpc_service.storage import (
    LocalArtifactStore,
    LocalVectorStore,
    MemoryProjectionWorker,
    StorageProfileRouter,
)
from trpc_service.storage.adapters import StorageError


def test_vector_memory_projection_is_tenant_scoped_and_read_your_write():
    vector = LocalVectorStore()
    projection = MemoryProjectionWorker(vector)
    projection.project(tenant_id="a", memory_id="mem-a", content="Alice likes tea", version=1)
    projection.project(tenant_id="b", memory_id="mem-b", content="Alice likes coffee", version=1)

    assert [match.document_id for match in vector.search("a", "tea")] == ["mem-a"]
    merged = projection.read_your_write("a", "tea", [("fresh", "fresh tea preference", {})])
    assert {match.document_id for match in merged} == {"mem-a", "fresh"}
    assert projection.projection_versions("a", "mem-a") == (1, 1)


def test_local_artifact_checksum_and_tenant_boundary(tmp_path):
    store = LocalArtifactStore(tmp_path)
    metadata = store.put("a", "art-1", b"safe artifact", content_type="text/plain")
    assert metadata.checksum
    assert store.get("a", "art-1") == b"safe artifact"
    with pytest.raises(StorageError):
        store.get("b", "art-1")


def test_storage_profile_copy_verifies_manifest_and_selects_active_profile(tmp_path):
    router = StorageProfileRouter(root=tmp_path)
    snapshot = {
        "memories": [
            {
                "memory_id": "preference",
                "memory_type": "fact",
                "version": 2,
                "content": "Alice prefers tea",
            }
        ],
        "summaries": [
            {
                "session_id": "session-1",
                "based_on_seq": 4,
                "content": "user: I like tea",
            }
        ],
    }
    profile = {"profile": "filesystem-secondary"}

    copied = router.copy("tenant-a", profile, snapshot)
    verified, source, target = router.verify("tenant-a", profile, snapshot)

    assert verified is True
    assert copied == source == target
    assert router.search("tenant-a", profile, "tea")[0]["record_id"] == "memory:preference"

    changed = {**snapshot, "memories": []}
    verified, _, _ = router.verify("tenant-a", profile, changed)
    assert verified is False
