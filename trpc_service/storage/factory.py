"""Configuration-driven composition of concrete storage backends."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import cast
from urllib.parse import urlparse

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine
from openai import AsyncOpenAI

from trpc_service.config.settings import Settings
from trpc_service.config.storage import (
    InMemoryBackendConfig,
    PgVectorBackendConfig,
    PostgreSQLBackendConfig,
    S3BackendConfig,
    resolve_local_secret,
)
from trpc_service.storage.adapters.inmemory import build_inmemory_backend
from trpc_service.storage.adapters.pgvector import PgVectorKnowledgeStore
from trpc_service.storage.adapters.postgresql import PostgreSQLExecutionStore
from trpc_service.storage.adapters.postgresql_auxiliary import (
    PostgreSQLAuditStore,
    PostgreSQLMemoryStore,
    PostgreSQLSummaryStore,
)
from trpc_service.storage.adapters.s3 import S3ArtifactStore, S3Client
from trpc_service.storage.database import build_session_factory, configured_engine
from trpc_service.storage.embedding import (
    BailianEmbeddingProvider,
    EmbeddingClient,
    EmbeddingProvider,
)
from trpc_service.storage.registry import StorageBackend, StorageBackendRegistry
from trpc_service.storage.router import StorageRouter

Initializer = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class StorageComposition:
    """Own registered stores and resources that share the process lifecycle."""

    registry: StorageBackendRegistry
    router: StorageRouter
    engines: tuple[AsyncEngine, ...] = ()
    initializers: tuple[Initializer, ...] = ()
    provisioners: tuple[Initializer, ...] = ()
    finalizers: tuple[Initializer, ...] = ()

    async def initialize(self) -> None:
        """Validate provider infrastructure using the runtime identity."""

        for initializer in self.initializers:
            await initializer()

    async def provision(self) -> None:
        """Create provider infrastructure only in the explicit deployment step."""

        for provisioner in self.provisioners:
            await provisioner()

    async def close(self) -> None:
        """Dispose every external SQL connection pool."""

        errors: list[Exception] = []
        for close in (*self.finalizers, *(engine.dispose for engine in self.engines)):
            try:
                await close()
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("storage cleanup failed", errors)


def _resolved_sql_url(url: str, password_ref: str | None) -> URL:
    """Inject a referenced password without putting it in environment JSON."""

    parsed = make_url(url)
    if password_ref is None:
        return parsed
    return parsed.set(password=resolve_local_secret(password_ref))


def _build_s3_store(config: S3BackendConfig) -> S3ArtifactStore:
    """Keep boto3-specific construction at the outer composition boundary."""

    access_key = (resolve_local_secret(config.access_key_ref)
                  if config.access_key_ref is not None else None)
    secret_key = (resolve_local_secret(config.secret_key_ref)
                  if config.secret_key_ref is not None else None)
    hostname = urlparse(config.endpoint_url).hostname if config.endpoint_url is not None else None
    signature_version = None
    if config.anonymous or (access_key is None and secret_key is None
                            and hostname in {"127.0.0.1", "::1", "localhost"}):
        # Explicit anonymous mode supports in-cluster S3-compatible services;
        # loopback development retains its safe automatic convenience.
        signature_version = UNSIGNED
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version=signature_version,
            connect_timeout=5,
            read_timeout=30,
            retries={
                "mode": "standard",
                "max_attempts": 2
            },
            s3={"addressing_style": "path" if config.path_style else "virtual"},
        ),
    )
    return S3ArtifactStore(
        cast(S3Client, client),
        bucket=config.bucket,
        region=config.region,
    )


def build_storage_composition(
    settings: Settings,
    embedding_providers: Mapping[str, EmbeddingProvider] | None = None,
) -> StorageComposition:
    """Build configured adapters and fail fast on missing embedding providers."""

    providers = dict(embedding_providers or {})
    registry = StorageBackendRegistry()
    engines: list[AsyncEngine] = []
    engines_by_url: dict[URL, AsyncEngine] = {}
    initializers: list[Initializer] = []
    provisioners: list[Initializer] = []
    finalizers: list[Initializer] = []

    def sql_engine(config: PostgreSQLBackendConfig | PgVectorBackendConfig) -> AsyncEngine:
        """Share one connection pool when capabilities target the same database."""

        resolved_url = _resolved_sql_url(config.url, config.password_ref)
        engine = engines_by_url.get(resolved_url)
        if engine is None:
            engine = configured_engine(resolved_url, settings)
            engines_by_url[resolved_url] = engine
            engines.append(engine)
        return engine

    for name, config in settings.storage_backends.items():
        if isinstance(config, InMemoryBackendConfig):
            registry.register(build_inmemory_backend(name))
            continue
        if isinstance(config, PostgreSQLBackendConfig):
            engine = sql_engine(config)
            sessions = build_session_factory(engine)
            # Capabilities share one pool, but remain independently replaceable
            # when an external store is selected by a later Backend Profile.
            execution_store = PostgreSQLExecutionStore(sessions)
            registry.register(
                StorageBackend(
                    name=name,
                    session=execution_store,
                    outbox=execution_store,
                    memory=PostgreSQLMemoryStore(sessions),
                    summary=PostgreSQLSummaryStore(sessions),
                    audit=PostgreSQLAuditStore(sessions),
                ))
            continue
        if isinstance(config, PgVectorBackendConfig):
            if (config.embedding_provider not in providers
                    and config.embedding_provider == settings.embedding.provider_name):
                embedding_config = settings.embedding
                api_key = (settings.dashscope_api_key.get_secret_value()
                           if embedding_config.api_key_ref == "env://DASHSCOPE_API_KEY" else
                           resolve_local_secret(embedding_config.api_key_ref))
                if not api_key.strip():
                    raise ValueError("Bailian embedding API key is not configured")
                client = AsyncOpenAI(
                    api_key=api_key,
                    base_url=embedding_config.base_url,
                )
                finalizers.append(client.close)
                providers[config.embedding_provider] = BailianEmbeddingProvider(
                    cast(EmbeddingClient, client),
                    model=embedding_config.model_name,
                    dimensions=embedding_config.dimensions,
                    batch_size=embedding_config.batch_size,
                )
            try:
                provider = providers[config.embedding_provider]
            except KeyError as error:
                raise ValueError(
                    f"embedding provider is not registered: {config.embedding_provider}") from error
            engine = sql_engine(config)
            vector_store = PgVectorKnowledgeStore(build_session_factory(engine), provider)
            initializers.append(vector_store.validate_schema)
            provisioners.append(vector_store.ensure_schema)
            registry.register(StorageBackend(name=name, knowledge=vector_store))
            continue
        if isinstance(config, S3BackendConfig):
            artifact_store = _build_s3_store(config)
            initializers.append(artifact_store.validate_bucket)
            provisioners.append(artifact_store.ensure_bucket)
            finalizers.append(artifact_store.close)
            registry.register(StorageBackend(name=name, artifact=artifact_store))
            continue
        raise TypeError(f"unsupported storage backend configuration: {type(config).__name__}")

    # Resolve once during startup so invalid profiles fail before serving traffic.
    router = StorageRouter(registry)
    router.resolve(settings.storage_profile.to_domain())
    return StorageComposition(
        registry=registry,
        router=router,
        engines=tuple(engines),
        initializers=tuple(initializers),
        provisioners=tuple(provisioners),
        finalizers=tuple(finalizers),
    )
