"""Provider-neutral embedding contract used by vector storage adapters."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Protocol


class EmbeddingProvider(ABC):
    """Convert text to fixed-size vectors without coupling storage to an LLM SDK."""

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Return the exact vector width produced by this provider."""

        ...

    @abstractmethod
    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed a batch of documents in stable input order."""

        ...

    @abstractmethod
    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed one retrieval query using the provider's query mode."""

        ...


class _EmbeddingItem(Protocol):
    """Provider response item needed by the adapter."""

    index: int
    embedding: list[float]


class _EmbeddingResponse(Protocol):
    """Provider response envelope needed by the adapter."""

    data: list[_EmbeddingItem]


class EmbeddingsEndpoint(Protocol):
    """Small OpenAI-compatible embeddings endpoint used at composition."""

    async def create(
        self,
        *,
        model: str,
        input: Sequence[str],
        dimensions: int,
        encoding_format: str,
    ) -> _EmbeddingResponse:
        """Create one bounded embedding batch."""

        ...


class EmbeddingClient(Protocol):
    """Client surface implemented by the OpenAI-compatible SDK."""

    embeddings: EmbeddingsEndpoint


class BailianEmbeddingProvider(EmbeddingProvider):
    """Call Bailian's OpenAI-compatible text embedding endpoint in batches."""

    def __init__(
        self,
        client: EmbeddingClient,
        *,
        model: str = "text-embedding-v4",
        dimensions: int = 1_024,
        batch_size: int = 10,
    ) -> None:
        if not model.strip() or dimensions < 1 or batch_size < 1 or batch_size > 10:
            raise ValueError("embedding model, dimensions, and batch size are invalid")
        self._client = client
        self._model = model
        self._dimensions = dimensions
        self._batch_size = batch_size

    @property
    def dimensions(self) -> int:
        """Return the vector width configured for both API and pgvector."""

        return self._dimensions

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        """Embed non-empty text in provider-sized batches and stable input order."""

        if any(not isinstance(value, str) or not value.strip() for value in texts):
            raise ValueError("embedding inputs must be non-empty strings")
        vectors: list[Sequence[float]] = []
        for offset in range(0, len(texts), self._batch_size):
            batch = list(texts[offset:offset + self._batch_size])
            response = await self._client.embeddings.create(
                model=self._model,
                input=batch,
                dimensions=self._dimensions,
                encoding_format="float",
            )
            ordered = sorted(response.data, key=lambda item: item.index)
            if len(ordered) != len(batch):
                raise ValueError("embedding provider returned an incomplete batch")
            vectors.extend(item.embedding for item in ordered)
        return vectors

    async def embed_query(self, text: str) -> Sequence[float]:
        """Embed one query through the same model and vector dimensions."""

        vectors = await self.embed_documents([text])
        return vectors[0]
