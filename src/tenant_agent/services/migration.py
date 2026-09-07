"""Idempotent tenant-scoped backfill and verification across storage backends."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from tenant_agent.models import KnowledgeRecord
from tenant_agent.storage.base import TenantDataPlane


class MigrationPhase(StrEnum):
    PREPARE = "prepare"
    DUAL_WRITE = "dual_write"
    BACKFILL = "backfill"
    VERIFY = "verify"
    CUTOVER = "cutover"
    RETIRE = "retire"


@dataclass(slots=True)
class MigrationReport:
    tenant_id: str
    resources: tuple[str, ...]
    copied: dict[str, int] = field(default_factory=dict)
    source_hashes: dict[str, str] = field(default_factory=dict)
    target_hashes: dict[str, str] = field(default_factory=dict)
    verification_modes: dict[str, str] = field(default_factory=dict)
    recall_checks: int = 0
    mismatches: list[str] = field(default_factory=list)

    @property
    def verified(self) -> bool:
        return not self.mismatches and self.source_hashes == self.target_hashes


EmbeddingTransform = Callable[[KnowledgeRecord], Awaitable[KnowledgeRecord]]


@dataclass(frozen=True, slots=True)
class GoldenQuery:
    embedding: tuple[float, ...]
    expected_ids: tuple[str, ...]
    min_matches: int = 1
    limit: int = 10
    metadata_filter: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class NativeHistoryReport:
    copied: int
    sessions: int
    source_hash: str
    target_hash: str


class DataMigrator:
    """Replay stable IDs, then compare canonical content hashes before cutover."""

    def __init__(self, source: TenantDataPlane, target: TenantDataPlane) -> None:
        self.source = source
        self.target = target

    async def migrate(
        self,
        tenant_id: str,
        *,
        resources: Iterable[str] = ("sessions", "summaries", "memories"),
        embedding_transform: EmbeddingTransform | None = None,
        golden_queries: Iterable[GoldenQuery] = (),
    ) -> MigrationReport:
        selected = tuple(dict.fromkeys(resources))
        recall_queries = tuple(golden_queries)
        if embedding_transform is not None and "knowledge" in selected and not recall_queries:
            raise ValueError("re-embedding migration requires golden-query recall validation")
        report = MigrationReport(tenant_id=tenant_id, resources=selected)
        if "sessions" in selected:
            report.copied["sessions"] = await self._sessions(tenant_id)
        if "summaries" in selected:
            report.copied["summaries"] = await self._summaries(tenant_id)
        if "memories" in selected:
            report.copied["memories"] = await self._memories(tenant_id)
        if "artifacts" in selected:
            report.copied["artifacts"] = await self._artifacts(tenant_id)
        if "knowledge" in selected:
            report.copied["knowledge"] = await self._knowledge(
                tenant_id, embedding_transform=embedding_transform
            )
        await self._verify(
            report,
            transformed_knowledge=embedding_transform is not None,
        )
        if "knowledge" in selected and recall_queries:
            await self._verify_recall(report, recall_queries)
        return report

    async def _sessions(self, tenant_id: str) -> int:
        copied = 0
        async for source_session in self.source.sessions.iter_sessions(tenant_id):
            target_session = await self.target.sessions.get_or_create_session(
                tenant_id=source_session.tenant_id,
                app_id=source_session.app_id,
                session_id=source_session.session_id,
                user_id=source_session.user_id,
                channel=source_session.channel,
            )
            source_events = await self.source.sessions.list_events(tenant_id, source_session.session_id)
            target_events = await self.target.sessions.list_events(
                tenant_id,
                source_session.session_id,
            )
            source_ids = [event.event_id for event in source_events]
            target_ids = [event.event_id for event in target_events]
            if target_ids != source_ids[: len(target_ids)]:
                raise RuntimeError(
                    "target session history is not a source prefix; use an empty shadow namespace"
                )
            for event in source_events[len(target_events) :]:
                target_session, _ = await self.target.sessions.append_event(
                    snapshot=target_session,
                    event_id=event.event_id,
                    kind=event.kind,
                    actor_id=event.actor_id,
                    payload=event.payload,
                    state_delta=event.state_delta,
                    trace_id=event.trace_id,
                )
                copied += 1
        return copied

    async def _summaries(self, tenant_id: str) -> int:
        copied = 0
        async for summary in self.source.summaries.iter_summaries(tenant_id):
            await self.target.summaries.put_summary(summary)
            copied += 1
        return copied

    async def _memories(self, tenant_id: str) -> int:
        copied = 0
        async for memory in self.source.memories.iter_memories(tenant_id):
            await self.target.memories.put_memory(memory)
            copied += 1
        return copied

    async def _artifacts(self, tenant_id: str) -> int:
        copied = 0
        async for record in self.source.artifacts.iter_artifacts(tenant_id):
            loaded = await self.source.artifacts.get_artifact(tenant_id, record.artifact_id)
            if loaded is None:
                raise RuntimeError(f"artifact disappeared during migration: {record.artifact_id}")
            _, content = loaded
            await self.target.artifacts.put_artifact(record, content)
            copied += 1
        return copied

    async def _knowledge(
        self,
        tenant_id: str,
        *,
        embedding_transform: EmbeddingTransform | None,
    ) -> int:
        copied = 0
        async for record in self.source.knowledge.iter_knowledge(tenant_id):
            target_record = await embedding_transform(record) if embedding_transform else record
            if target_record.tenant_id != tenant_id:
                raise ValueError("embedding transform changed tenant scope")
            immutable_fields = ("document_id", "chunk_id", "text", "metadata")
            if any(getattr(target_record, field) != getattr(record, field) for field in immutable_fields):
                raise ValueError("embedding transform changed immutable knowledge content")
            if embedding_transform is not None and not target_record.embedding:
                raise ValueError("embedding transform returned an empty vector")
            await self.target.knowledge.put_knowledge(target_record)
            copied += 1
        return copied

    async def _verify(
        self,
        report: MigrationReport,
        *,
        transformed_knowledge: bool,
    ) -> None:
        for resource in report.resources:
            identity_only = resource == "knowledge" and transformed_knowledge
            cosine_normalized = resource == "knowledge" and any(
                getattr(plane.knowledge, "backend_name", "") == "qdrant"
                for plane in (self.source, self.target)
            )
            source_hash = await self._hash_resource(
                self.source,
                report.tenant_id,
                resource,
                knowledge_identity_only=identity_only,
                knowledge_cosine_normalized=cosine_normalized,
            )
            target_hash = await self._hash_resource(
                self.target,
                report.tenant_id,
                resource,
                knowledge_identity_only=identity_only,
                knowledge_cosine_normalized=cosine_normalized,
            )
            report.source_hashes[resource] = source_hash
            report.target_hashes[resource] = target_hash
            report.verification_modes[resource] = (
                "identity-and-recall"
                if identity_only
                else "cosine-normalized-content"
                if cosine_normalized
                else "exact-content"
            )
            if source_hash != target_hash:
                report.mismatches.append(resource)

    async def _verify_recall(
        self,
        report: MigrationReport,
        queries: tuple[GoldenQuery, ...],
    ) -> None:
        for index, query in enumerate(queries):
            if not query.embedding or not query.expected_ids:
                raise ValueError("golden queries require an embedding and expected IDs")
            if not 1 <= query.min_matches <= len(query.expected_ids):
                raise ValueError("golden-query min_matches is outside the expected-ID set")
            matches = await self.target.knowledge.search_knowledge(
                report.tenant_id,
                query.embedding,
                limit=query.limit,
                metadata_filter=query.metadata_filter,
            )
            actual = {f"{item.document_id}/{item.chunk_id}" for item in matches}
            if len(actual & set(query.expected_ids)) < query.min_matches:
                report.mismatches.append(f"knowledge_recall:{index}")
            report.recall_checks += 1

    async def _hash_resource(
        self,
        plane: TenantDataPlane,
        tenant_id: str,
        resource: str,
        *,
        knowledge_identity_only: bool = False,
        knowledge_cosine_normalized: bool = False,
    ) -> str:
        rows: list[dict[str, Any]] = []
        if resource == "sessions":
            async for session in plane.sessions.iter_sessions(tenant_id):
                rows.append(
                    {
                        "record_type": "session",
                        **session.model_dump(
                            mode="json",
                            exclude={"created_at", "updated_at"},
                        ),
                    }
                )
                events = await plane.sessions.list_events(tenant_id, session.session_id)
                rows.extend(
                    {
                        "record_type": "event",
                        "session_id": session.session_id,
                        "event_id": event.event_id,
                        "sequence": event.sequence,
                        "kind": event.kind,
                        "actor": event.actor_id,
                        "payload": event.payload,
                        "state_delta": event.state_delta,
                        "trace_id": event.trace_id,
                    }
                    for event in events
                )
        elif resource == "summaries":
            async for summary_row in plane.summaries.iter_summaries(tenant_id):
                rows.append(summary_row.model_dump(mode="json", exclude={"created_at"}))
        elif resource == "memories":
            async for memory_row in plane.memories.iter_memories(tenant_id):
                rows.append(memory_row.model_dump(mode="json", exclude={"created_at", "updated_at"}))
        elif resource == "artifacts":
            async for artifact_row in plane.artifacts.iter_artifacts(tenant_id):
                loaded = await plane.artifacts.get_artifact(tenant_id, artifact_row.artifact_id)
                if loaded is None:
                    raise RuntimeError(
                        f"artifact disappeared during verification: {artifact_row.artifact_id}"
                    )
                _, content = loaded
                rows.append(
                    {
                        **artifact_row.model_dump(
                            mode="json",
                            exclude={"storage_uri", "created_at"},
                        ),
                        "verified_content_sha256": hashlib.sha256(content).hexdigest(),
                    }
                )
        elif resource == "knowledge":
            async for knowledge_row in plane.knowledge.iter_knowledge(tenant_id):
                excluded = {"updated_at"}
                if knowledge_identity_only:
                    excluded.update({"embedding", "embedding_model"})
                row = knowledge_row.model_dump(mode="json", exclude=excluded)
                if knowledge_cosine_normalized and not knowledge_identity_only:
                    embedding = tuple(float(value) for value in knowledge_row.embedding)
                    norm = math.sqrt(sum(value * value for value in embedding))
                    row["embedding"] = (
                        [round(value / norm, 6) for value in embedding] if norm else [0.0 for _ in embedding]
                    )
                rows.append(row)
        else:
            raise ValueError(f"unsupported migration resource {resource!r}")
        rows.sort(key=lambda row: json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        canonical = json.dumps(rows, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()


async def migrate_trpc_session_history(
    *,
    tenant_id: str,
    manifest_sessions: Any,
    source_service: Any,
    target_service: Any,
) -> NativeHistoryReport:
    """Replay native tRPC events after the normalized platform backfill.

    The normalized session manifest supplies app/user/session enumeration, which
    native tRPC Redis keys intentionally do not expose as a global tenant scan.
    """

    copied = 0
    session_count = 0
    source_rows: list[dict[str, Any]] = []
    target_rows: list[dict[str, Any]] = []
    tenant_scope = hashlib.sha256(tenant_id.encode()).hexdigest()[:16]
    async for manifest in manifest_sessions.iter_sessions(tenant_id):
        app_name = f"tap:{tenant_scope}:{manifest.app_id}"
        source = await source_service.get_session(
            app_name=app_name,
            user_id=manifest.user_id,
            session_id=manifest.session_id,
        )
        if source is None:
            unexpected = await target_service.get_session(
                app_name=app_name,
                user_id=manifest.user_id,
                session_id=manifest.session_id,
            )
            if unexpected is not None:
                raise RuntimeError("native target session exists without a source session")
            continue
        session_count += 1
        target = await target_service.get_session(
            app_name=app_name,
            user_id=manifest.user_id,
            session_id=manifest.session_id,
        )
        if target is None:
            target = await target_service.create_session(
                app_name=app_name,
                user_id=manifest.user_id,
                session_id=manifest.session_id,
                state=dict(source.state),
            )
        source_events = (*source.historical_events, *source.events)
        target_events = (*target.historical_events, *target.events)
        if [event.id for event in target_events] != [
            event.id for event in source_events[: len(target_events)]
        ]:
            raise RuntimeError("native target history is not a source prefix")
        for source_event, target_event in zip(source_events, target_events, strict=False):
            if _canonical_native_value(source_event) != _canonical_native_value(target_event):
                raise RuntimeError("native target event payload differs from the source")
        for event in source_events[len(target_events) :]:
            await target_service.append_event(session=target, event=event)
            copied += 1
        refreshed = await target_service.get_session(
            app_name=app_name,
            user_id=manifest.user_id,
            session_id=manifest.session_id,
        )
        if refreshed is None:
            raise RuntimeError("native target session disappeared during verification")
        source_rows.append(_canonical_native_session(manifest, source))
        target_rows.append(_canonical_native_session(manifest, refreshed))
    source_hash = _canonical_native_hash(source_rows)
    target_hash = _canonical_native_hash(target_rows)
    if source_hash != target_hash:
        raise RuntimeError("native Session history failed canonical verification")
    return NativeHistoryReport(
        copied=copied,
        sessions=session_count,
        source_hash=source_hash,
        target_hash=target_hash,
    )


def _canonical_native_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "__dict__"):
        return {
            str(key): _canonical_native_value(item)
            for key, item in sorted(vars(value).items())
            if not str(key).startswith("_")
        }
    if isinstance(value, dict):
        return {str(key): _canonical_native_value(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical_native_value(item) for item in value]
    return value


def _canonical_native_session(manifest: Any, session: Any) -> dict[str, Any]:
    return {
        "app_id": manifest.app_id,
        "user_id": manifest.user_id,
        "session_id": manifest.session_id,
        "state": _canonical_native_value(dict(session.state)),
        "events": [_canonical_native_value(event) for event in (*session.historical_events, *session.events)],
    }


def _canonical_native_hash(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        sorted(rows, key=lambda row: (row["app_id"], row["user_id"], row["session_id"])),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
