"""Storage backend factory."""

from __future__ import annotations

from pathlib import Path

from trpc_service.security.secrets import SecretManager
from trpc_service.storage.external_memory import ExternalMemoryStore
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.object_store import FileObjectStore, RedisObjectStore, S3ObjectStore
from trpc_service.storage.redis_store import RedisStorage
from trpc_service.storage.remote_vector import RemoteVectorStore, resolve_secret
from trpc_service.storage.sql_store import SQLiteStorage
from trpc_service.storage.vector_store import LocalVectorStore
from trpc_service.tenant.models import StorageProfile


class StorageBundle:
    def __init__(
        self,
        structured,
        memory_backend=None,
        summary_backend=None,
        audit_backend=None,
        inbox_outbox=None,
        mailbox=None,
        tool_governance=None,
        migration_control=None,
        knowledge: LocalVectorStore | None = None,
        objects: FileObjectStore | None = None,
        extra_backends: list[object] | None = None,
    ) -> None:
        self.structured = structured
        self.session = structured.session
        self.memory = memory_backend or structured.memory
        self.summary = summary_backend or structured.summary
        self.audit = audit_backend or structured.audit
        self.idempotency = structured.idempotency
        self.compensation = getattr(structured, "compensation", None)
        if self.compensation is None:
            from trpc_service.storage.compensation import InMemoryCompensationStore

            self.compensation = InMemoryCompensationStore()
        self.inbox_outbox = inbox_outbox or getattr(structured, "inbox_outbox", None)
        self.mailbox = mailbox or getattr(structured, "mailbox", None)
        # Durable Inbox/Outbox and the ordered session mailbox may live on a
        # different structured backend than the primary session backend (for
        # example Redis sessions with PostgreSQL coordination state).  Resolve
        # the mailbox from the first backend that provides it instead of only
        # inspecting ``structured``.
        self.session_mailbox_v2 = next(
            (
                getattr(backend, "session_mailbox_v2", None)
                for backend in (structured, memory_backend, summary_backend, audit_backend)
                if getattr(backend, "session_mailbox_v2", None) is not None
            ),
            None,
        )
        self.tool_governance = tool_governance or getattr(structured, "tool_governance", None)
        self.migration_control = migration_control or getattr(structured, "migration_control", None)
        self.knowledge = knowledge or LocalVectorStore()
        self.objects = objects or FileObjectStore()
        self.artifacts = self.objects
        self.memory_backend = self.memory
        self.summary_backend = self.summary
        self.audit_backend = self.audit
        self.knowledge_collection = "default"
        self.extra_backends = list(extra_backends or [])

    def close(self) -> None:
        closed: set[int] = set()
        for value in (
            self.structured,
            *self.extra_backends,
            self.knowledge,
            self.objects,
        ):
            if id(value) in closed:
                continue
            close = getattr(value, "close", None)
            if close:
                close()
            closed.add(id(value))


def create_storage(profile: StorageProfile | None = None, data_dir: str | Path = "data") -> StorageBundle:
    profile = profile or StorageProfile()
    data_dir = Path(data_dir)

    def resolve_connection(value: str, reference: str, fallback_env: str) -> str:
        if value:
            return value
        if reference.startswith("env://"):
            import os

            return os.getenv(reference.removeprefix("env://"), "")
        if reference.startswith("secret://"):
            return SecretManager().resolve(reference)
        return reference or __import__("os").getenv(fallback_env, "")

    redis_url = resolve_connection(profile.redis_url, profile.redis_url_ref, "REDIS_URL")
    sql_dsn = resolve_connection(profile.sql_dsn, profile.sql_dsn_ref, "POSTGRES_DSN")

    def structured_backend(name: str):
        backend = name.lower()
        if backend in {"memory", "inmemory"}:
            return InMemoryStorage()
        if backend == "redis":
            return RedisStorage(redis_url or None)
        if backend in {"sql", "sqlite"}:
            return SQLiteStorage(data_dir / "trpc_service.sqlite3")
        if backend in {"postgres", "postgresql"}:
            from trpc_service.storage.postgres_store import PostgresStorage

            return PostgresStorage(sql_dsn or None)
        raise ValueError(f"unsupported storage backend: {backend}")

    session_structured = structured_backend(profile.session_backend)
    external_memory = None
    if profile.memory_backend == "external":
        token = SecretManager().resolve(profile.external_memory_token_ref) if profile.external_memory_token_ref else ""
        external_memory = ExternalMemoryStore(profile.external_memory_url, token)
        memory_structured = session_structured
    else:
        memory_structured = (
            session_structured
            if profile.memory_backend == profile.session_backend
            else structured_backend(profile.memory_backend)
        )
    summary_structured = (
        session_structured
        if profile.summary_backend == profile.session_backend
        else structured_backend(profile.summary_backend)
    )
    audit_structured = (
        session_structured
        if profile.audit_backend == profile.session_backend
        else structured_backend(profile.audit_backend)
    )
    knowledge_backend = profile.knowledge_backend.lower()
    if knowledge_backend in {"vector", "memory", "local"}:
        knowledge = LocalVectorStore(data_dir / "knowledge")
    elif knowledge_backend == "redis":
        from trpc_service.storage.redis_knowledge import RedisKnowledgeStore

        knowledge = RedisKnowledgeStore(redis_url or None)
    elif knowledge_backend in {"postgres", "postgresql", "sql"}:
        from trpc_service.storage.postgres_knowledge import PostgresKnowledgeStore

        knowledge = PostgresKnowledgeStore(sql_dsn or None)
    elif knowledge_backend in {"remote", "qdrant", "vector_remote"}:
        knowledge = RemoteVectorStore(
            profile.vector_url,
            resolve_secret(profile.vector_token_ref),
            provider=profile.vector_provider,
            dimension=profile.vector_dimension,
            collection_prefix=profile.knowledge_collection,
            embedding_url=profile.embedding_url,
            embedding_model=profile.embedding_model,
            embedding_token=resolve_secret(profile.embedding_token_ref),
        )
    else:
        raise ValueError(f"unsupported knowledge backend: {knowledge_backend}")
    artifact_backend = profile.artifact_backend.lower()
    if artifact_backend in {"object", "file", "filesystem"}:
        objects = FileObjectStore(data_dir / "artifacts")
    elif artifact_backend == "redis":
        objects = RedisObjectStore(redis_url or None)
    elif artifact_backend in {"s3", "oss", "minio"}:
        objects = S3ObjectStore(
            profile.object_endpoint,
            profile.object_bucket,
            profile.object_region or "us-east-1",
            resolve_secret(profile.object_access_key_ref),
            resolve_secret(profile.object_secret_key_ref),
        )
    else:
        raise ValueError(f"unsupported artifact backend: {artifact_backend}")

    extra_backends = []
    if external_memory is not None:
        extra_backends.append(external_memory)
    for backend in (memory_structured, summary_structured, audit_structured):
        if backend is not session_structured:
            extra_backends.append(backend)
    bundle = StorageBundle(
        structured=session_structured,
        memory_backend=external_memory or memory_structured.memory,
        summary_backend=summary_structured.summary,
        audit_backend=audit_structured.audit,
        inbox_outbox=next(
            (
                getattr(backend, "inbox_outbox", None)
                for backend in (session_structured, memory_structured, summary_structured, audit_structured)
                if getattr(backend, "inbox_outbox", None) is not None
            ),
            None,
        ),
        mailbox=next(
            (
                getattr(backend, "mailbox", None)
                for backend in (session_structured, memory_structured, summary_structured, audit_structured)
                if getattr(backend, "mailbox", None) is not None
            ),
            None,
        ),
        tool_governance=next(
            (
                getattr(backend, "tool_governance", None)
                for backend in (session_structured, memory_structured, summary_structured, audit_structured)
                if getattr(backend, "tool_governance", None) is not None
            ),
            None,
        ),
        migration_control=next(
            (
                getattr(backend, "migration_control", None)
                for backend in (session_structured, memory_structured, summary_structured, audit_structured)
                if getattr(backend, "migration_control", None) is not None
            ),
            None,
        ),
        knowledge=knowledge,
        objects=objects,
        extra_backends=extra_backends,
    )
    bundle.knowledge_collection = profile.knowledge_collection
    return bundle


def create_mirrored_storage(
    primary: StorageProfile,
    secondary: StorageProfile,
    data_dir: str | Path = "data",
):
    """Create a tenant-scoped dual-write bundle for backend migration."""
    from trpc_service.storage.mirror import mirrored_storage

    return mirrored_storage(primary, secondary, Path(data_dir))
