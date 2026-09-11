"""Versioned local-to-remote vector migration and rollback."""

from __future__ import annotations

import math
from copy import deepcopy

from .models import VectorMigrationReport


def _manifest(chunks):
    result = {}
    for chunk in chunks:
        if chunk.chunk_id in result:
            raise ValueError("duplicate chunk identity in index")
        vector = tuple(chunk.vector)
        if not vector or not all(math.isfinite(value) for value in vector):
            raise ValueError("invalid embedding vector")
        # Knowledge search uses cosine; Qdrant normalizes stored vectors to unit length.
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        result[chunk.chunk_id] = (
            chunk.document_id, chunk.text, deepcopy(chunk.metadata), chunk.deleted,
            tuple(value / norm for value in vector),
        )
    return result


def _matches(expected, actual):
    if expected.keys() != actual.keys():
        return False
    for key, left in expected.items():
        right = actual[key]
        if left[:4] != right[:4] or len(left[4]) != len(right[4]):
            return False
        if not all(math.isclose(a, b, rel_tol=1e-5, abs_tol=1e-6) for a, b in zip(left[4], right[4])):
            return False
    return True


class VectorMigration:
    def __init__(
        self,
        *,
        tenant_id,
        knowledge_base_id,
        source_index_version,
        target_index_version,
        source,
        target,
        source_embedding,
        target_embedding,
        reembed=None,
    ) -> None:
        if source_index_version == target_index_version:
            raise ValueError("vector migration requires a new index version")
        self.tenant_id = tenant_id
        self.knowledge_base_id = knowledge_base_id
        self.source_index_version = source_index_version
        self.target_index_version = target_index_version
        self.source = source
        self.target = target
        self.source_embedding = source_embedding
        self.target_embedding = target_embedding
        self.reembed = reembed
        self.active_index_version = source_index_version
        self.verified = False
        self._source_manifest = None
        self._target_manifest = None

    def copy_and_verify(self) -> VectorMigrationReport:
        self.verified = False
        self._source_manifest = None
        self._target_manifest = None
        copied = 0
        tombstones = 0
        compatible = self.source_embedding == self.target_embedding
        if not compatible and self.reembed is None:
            raise ValueError("incompatible embedding schema requires re-embedding from source documents")
        chunks = deepcopy(self.source.iter_index(self.tenant_id, self.knowledge_base_id, self.source_index_version))
        source_manifest = _manifest(chunks)
        expected_chunks = []
        deleted_documents = set()
        for chunk in chunks:
            vector = list(chunk.vector) if compatible else self.reembed(chunk.text)
            if len(vector) != self.target_embedding[1]:
                raise ValueError("embedding dimension does not match target schema")
            expected = deepcopy(chunk)
            expected.vector = tuple(vector)
            expected_chunks.append(expected)
            self.target.upsert(
                self.tenant_id,
                self.knowledge_base_id,
                self.target_index_version,
                chunk.document_id,
                chunk.chunk_id,
                vector,
                chunk.text,
                chunk.metadata,
            )
            if chunk.deleted:
                deleted_documents.add(chunk.document_id)
                tombstones += 1
            else:
                copied += 1
        for document_id in deleted_documents:
            self.target.delete_document(self.tenant_id, self.knowledge_base_id, self.target_index_version, document_id)
        target_chunks = self.target.iter_index(self.tenant_id, self.knowledge_base_id, self.target_index_version)
        expected_manifest = _manifest(expected_chunks)
        if not _matches(expected_manifest, _manifest(target_chunks)):
            raise ValueError("vector migration content verification failed")
        if not _matches(source_manifest, _manifest(self.source.iter_index(
                self.tenant_id, self.knowledge_base_id, self.source_index_version))):
            raise ValueError("source index changed during verification")
        self._source_manifest = source_manifest
        self._target_manifest = expected_manifest
        self.verified = True
        return VectorMigrationReport(copied, tombstones)

    def cutover(self) -> None:
        if not self.verified:
            raise ValueError("vector migration must verify before cutover")
        # Re-read both sides: a previous successful check is not a permanent permit.
        self.verified = False
        if not _matches(self._source_manifest, _manifest(self.source.iter_index(
                self.tenant_id, self.knowledge_base_id, self.source_index_version))):
            raise ValueError("source index changed after verification")
        if not _matches(self._target_manifest, _manifest(self.target.iter_index(
                self.tenant_id, self.knowledge_base_id, self.target_index_version))):
            raise ValueError("target index changed after verification")
        self.active_index_version = self.target_index_version

    def rollback(self) -> None:
        self.active_index_version = self.source_index_version
        self.verified = False
