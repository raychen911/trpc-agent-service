from collections.abc import Sequence
from dataclasses import dataclass

import pytest

from trpc_service.storage.embedding import BailianEmbeddingProvider


@dataclass
class _EmbeddingItem:
    index: int
    embedding: list[float]


@dataclass
class _EmbeddingResponse:
    data: list[_EmbeddingItem]


class _EmbeddingsEndpoint:

    def __init__(self) -> None:
        self.batches: list[list[str]] = []

    async def create(
        self,
        *,
        model: str,
        input: Sequence[str],
        dimensions: int,
        encoding_format: str,
    ) -> _EmbeddingResponse:
        assert model == "text-embedding-v4"
        assert dimensions == 3
        assert encoding_format == "float"
        values = list(input)
        self.batches.append(values)
        return _EmbeddingResponse([
            _EmbeddingItem(index=index, embedding=[float(len(text)), 1.0, 2.0])
            for index, text in reversed(list(enumerate(values)))
        ])


class _EmbeddingClient:

    def __init__(self) -> None:
        self.embeddings = _EmbeddingsEndpoint()


@pytest.mark.anyio
async def test_bailian_embedding_batches_requests_and_restores_provider_order() -> None:
    client = _EmbeddingClient()
    provider = BailianEmbeddingProvider(
        client,  # type: ignore[arg-type]
        model="text-embedding-v4",
        dimensions=3,
        batch_size=2,
    )

    vectors = await provider.embed_documents(["a", "bb", "ccc"])
    query = await provider.embed_query("dddd")

    assert vectors == [[1.0, 1.0, 2.0], [2.0, 1.0, 2.0], [3.0, 1.0, 2.0]]
    assert query == [4.0, 1.0, 2.0]
    assert client.embeddings.batches == [["a", "bb"], ["ccc"], ["dddd"]]
