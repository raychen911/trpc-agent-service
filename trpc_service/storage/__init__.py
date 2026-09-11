"""Tenant storage profiles, resolution, and guarded services."""

from .bundle import TenantScopedService
from .bundle import TenantStorageBundle
from .artifact import S3ArtifactService
from .errors import ConflictError
from .errors import PermanentStorageError
from .errors import StorageError
from .errors import TenantBoundaryError
from .errors import TransientStorageError
from .errors import UnavailableError
from .factory import TrpcServiceFactory
from .memory import GuardedMemoryService
from .knowledge import KnowledgeResult
from .knowledge import LocalVectorKnowledgeStore
from .knowledge import QdrantKnowledgeStore
from .knowledge_ingestion import IngestionResult, KnowledgeIngestionService, OpenRouterEmbeddingClient, TextChunker
from .models import BackendCapability
from .models import BackendHealth
from .models import BackendProfile
from .models import BackendProfileRegistry
from .models import ResourceType
from .models import TenantBackendBinding
from .models import validate_tenant_bindings
from .resolver import TenantStorageResolver
from .session import GuardedSessionService
from .session import StorageExecutionContext
from .session import execution_scope

__all__ = [
    "BackendCapability",
    "BackendHealth",
    "BackendProfile",
    "BackendProfileRegistry",
    "ConflictError",
    "GuardedMemoryService",
    "GuardedSessionService",
    "KnowledgeResult",
    "IngestionResult",
    "KnowledgeIngestionService",
    "OpenRouterEmbeddingClient",
    "LocalVectorKnowledgeStore",
    "PermanentStorageError",
    "ResourceType",
    "QdrantKnowledgeStore",
    "S3ArtifactService",
    "StorageError",
    "TenantBackendBinding",
    "TenantBoundaryError",
    "TenantScopedService",
    "TenantStorageBundle",
    "TenantStorageResolver",
    "TrpcServiceFactory",
    "TransientStorageError",
    "TextChunker",
    "UnavailableError",
    "StorageExecutionContext",
    "execution_scope",
    "validate_tenant_bindings",
]
