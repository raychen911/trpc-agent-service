"""Vector and object backend contract, isolation and routing tests."""

from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

import pytest
from pydantic import SecretStr
from pydantic import ValidationError
from qdrant_client import AsyncQdrantClient

import trpc_service.workspace._router as router_module
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import ObjectBackendConfig
from trpc_service.tenant import StorageBackendConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import VectorBackendConfig
from trpc_service import EnterpriseMetrics
from trpc_service.workspace import InMemoryVectorStore
from trpc_service.workspace import LocalObjectStore
from trpc_service.workspace import QdrantVectorStore
from trpc_service.workspace import S3CompatibleObjectStore
from trpc_service.workspace import TenantObjectStore
from trpc_service.workspace import TenantStorageRouter
from trpc_service.workspace import TenantVectorStore
from trpc_service.workspace import VectorRecord


async def test_in_memory_vector_store_search_filter_delete_and_validation():
    store = InMemoryVectorStore()
    await store.upsert("kb", [])
    await store.upsert(
        "kb",
        [
            VectorRecord(record_id="a", embedding=[1.0, 0.0], content="A", metadata={"type": "faq"}),
            VectorRecord(record_id="b", embedding=[0.0, 1.0], content="B", metadata={"type": "manual"}),
            VectorRecord(record_id="c", embedding=[0.0, 0.0], content="C", metadata={"type": "faq"}),
        ],
    )

    matches = await store.search("kb", [1.0, 0.0], metadata_filter={"type": "faq"})
    assert [(item.record.record_id, item.score) for item in matches] == [("a", 1.0), ("c", 0.0)]
    assert await store.search("kb", [1.0, 0.0], limit=0) == []

    await store.delete("kb", ["a", "missing"])
    assert [item.record.record_id for item in await store.search("kb", [1.0, 0.0])] == ["b", "c"]
    await store.delete("kb")
    assert await store.search("kb", [1.0, 0.0]) == []

    await store.upsert("dimensions", [VectorRecord(record_id="a", embedding=[1.0])])
    with pytest.raises(ValueError, match="same dimensions"):
        await store.upsert("dimensions", [VectorRecord(record_id="b", embedding=[1.0, 2.0])])
    with pytest.raises(ValueError, match="dimensions do not match"):
        await store.search("dimensions", [1.0, 2.0])
    with pytest.raises(ValueError, match="must not be empty"):
        await store.search("dimensions", [])
    await store.close()


async def test_tenant_vector_store_namespaces_are_isolated_and_idempotent():
    backend = InMemoryVectorStore()
    metrics = EnterpriseMetrics(meter=False)
    tenant_a = TenantVectorStore(backend, "tenant_a", metrics=metrics, backend_name="memory")
    tenant_b = TenantVectorStore(backend, "tenant_b")
    record = VectorRecord(record_id="doc", embedding=[1.0], content="private")

    await tenant_a.upsert("knowledge", [record])
    assert len(await tenant_a.search("tenant_a:knowledge", [1.0])) == 1
    assert await tenant_b.search("knowledge", [1.0]) == []
    await tenant_a.delete("knowledge", ["doc"])
    assert await tenant_a.search("knowledge", [1.0]) == []
    operations = {
        item["attributes"]["operation"]
        for item in metrics.snapshot("tenant_a")["counters"] if item["name"] == "agent_storage_operation_total"
    }
    assert operations == {"upsert", "search", "delete"}
    await tenant_b.close()


class FakeQdrantClient:

    def __init__(self):
        self.exists = False
        self.created = []
        self.upserts = []
        self.queries = []
        self.deletes = []
        self.closed = False

    async def collection_exists(self, _collection):
        return self.exists

    async def create_collection(self, **kwargs):
        self.created.append(kwargs)
        self.exists = True

    async def upsert(self, **kwargs):
        self.upserts.append(kwargs)

    async def query_points(self, **kwargs):
        self.queries.append(kwargs)
        point = self.upserts[0]["points"][0]
        return SimpleNamespace(
            points=[SimpleNamespace(
                id=point.id,
                vector=point.vector,
                payload=point.payload,
                score=0.95,
            )])

    async def delete(self, **kwargs):
        self.deletes.append(kwargs)

    async def close(self):
        self.closed = True


async def test_qdrant_vector_store_lifecycle_and_tenant_filter():
    client = FakeQdrantClient()
    store = QdrantVectorStore(
        url="http://qdrant:6333",
        collection="knowledge",
        dimensions=2,
        client=client,
    )
    record = VectorRecord(record_id="doc-1", embedding=[1.0, 0.0], content="hello", metadata={"lang": "zh"})

    await store.upsert("tenant_a:kb", [])
    await store.upsert("tenant_a:kb", [record])
    assert len(client.created) == 1
    assert client.upserts[0]["points"][0].payload["tenant_namespace"] == "tenant_a:kb"
    assert QdrantVectorStore._point_id("tenant_a:kb", "doc-1") == QdrantVectorStore._point_id("tenant_a:kb", "doc-1")

    matches = await store.search("tenant_a:kb", [1.0, 0.0], metadata_filter={"lang": "zh"})
    assert matches[0].record == record
    assert matches[0].score == 0.95
    query_filter = client.queries[0]["query_filter"]
    assert [condition.key for condition in query_filter.must] == ["tenant_namespace", "metadata.lang"]

    await store.delete("tenant_a:kb", ["doc-1"])
    await store.delete("tenant_a:kb")
    assert len(client.deletes) == 2
    await store.close()
    assert client.closed is True


async def test_qdrant_vector_store_validation_and_empty_collection():
    client = FakeQdrantClient()
    store = QdrantVectorStore(url="", collection="knowledge", dimensions=2, client=client)
    assert await store.search("tenant:kb", [1.0, 0.0]) == []
    assert await store.search("tenant:kb", [1.0, 0.0], limit=0) == []
    await store.delete("tenant:kb")
    with pytest.raises(ValueError, match="must not be empty"):
        await store.search("tenant:kb", [])
    with pytest.raises(ValueError, match="query dimensions"):
        client.exists = True
        await store.search("tenant:kb", [1.0])
    with pytest.raises(ValueError, match="configured Qdrant"):
        await store.upsert("tenant:kb", [VectorRecord(record_id="a", embedding=[1.0])])
    with pytest.raises(ValueError, match="same dimensions"):
        await QdrantVectorStore(url="", collection="knowledge", client=client).upsert(
            "tenant:kb",
            [
                VectorRecord(record_id="a", embedding=[1.0]),
                VectorRecord(record_id="b", embedding=[1.0, 2.0]),
            ],
        )
    with pytest.raises(ValueError, match="requires a URL"):
        QdrantVectorStore(url="", collection="knowledge")
    with pytest.raises(ValueError, match="collection"):
        QdrantVectorStore(url="url", collection="", client=client)


async def test_qdrant_vector_store_works_with_official_local_client():
    client = AsyncQdrantClient(location=":memory:")
    store = QdrantVectorStore(url="", collection="knowledge", dimensions=2, client=client)
    record = VectorRecord(record_id="doc", embedding=[1.0, 0.0], content="isolated")

    await store.upsert("tenant_a:kb", [record])
    assert [match.record.record_id for match in await store.search("tenant_a:kb", [1.0, 0.0])] == ["doc"]
    assert await store.search("tenant_b:kb", [1.0, 0.0]) == []

    await store.delete("tenant_a:kb", ["doc"])
    assert await store.search("tenant_a:kb", [1.0, 0.0]) == []
    await store.close()


async def test_local_object_store_is_atomic_scoped_and_path_safe(tmp_path):
    backend = LocalObjectStore(str(tmp_path))
    metrics = EnterpriseMetrics(meter=False)
    tenant_a = TenantObjectStore(backend, "tenant_a", metrics=metrics, backend_name="local")
    tenant_b = TenantObjectStore(backend, "tenant_b")

    info = await tenant_a.put("reports/a.txt", b"hello", "text/plain", {"source": "test"})
    assert info.key == "reports/a.txt"
    assert info.size == 5
    assert len(info.checksum) == 64
    assert await tenant_a.get("reports/a.txt") == b"hello"
    assert (await tenant_a.head("reports/a.txt")).metadata == {"source": "test"}
    assert await tenant_b.get("reports/a.txt") is None
    assert await tenant_b.head("reports/a.txt") is None

    # Supplying an already scoped key must not produce a duplicate prefix.
    assert await tenant_a.get("tenant_a/reports/a.txt") == b"hello"
    await tenant_a.delete("reports/a.txt")
    assert await tenant_a.get("reports/a.txt") is None
    await tenant_a.delete("missing.txt")
    operations = {
        item["attributes"]["operation"]
        for item in metrics.snapshot("tenant_a")["counters"] if item["name"] == "agent_storage_operation_total"
    }
    assert operations == {"put", "get", "head", "delete"}
    await tenant_a.close()

    with pytest.raises(ValueError, match="relative POSIX"):
        await backend.get("/etc/passwd")
    with pytest.raises(ValueError, match="escape"):
        await backend.get("../outside")
    with pytest.raises(TypeError, match="must be bytes"):
        await backend.put("bad", "text")


class ObjectMissingError(Exception):

    def __init__(self, code="NoSuchKey"):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3Client:

    def __init__(self):
        self.objects = {}
        self.closed = False

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = kwargs

    def get_object(self, **kwargs):
        item = self.objects.get(kwargs["Key"])
        if item is None:
            raise ObjectMissingError()
        return {"Body": BytesIO(item["Body"])}

    def head_object(self, **kwargs):
        item = self.objects.get(kwargs["Key"])
        if item is None:
            raise ObjectMissingError("404")
        return {
            "ContentLength": len(item["Body"]),
            "ContentType": item["ContentType"],
            "Metadata": item["Metadata"],
        }

    def delete_object(self, **kwargs):
        self.objects.pop(kwargs["Key"], None)

    def close(self):
        self.closed = True


async def test_s3_compatible_object_store_round_trip_and_missing():
    client = FakeS3Client()
    store = S3CompatibleObjectStore(bucket="artifacts", client=client)

    saved = await store.put("tenant/file.bin", b"payload", metadata={"version": 2})
    assert saved.metadata == {"version": 2}
    assert await store.get("tenant/file.bin") == b"payload"
    loaded = await store.head("tenant/file.bin")
    assert loaded.checksum == saved.checksum
    assert loaded.metadata == {"version": 2}

    await store.delete("tenant/file.bin")
    assert await store.get("tenant/file.bin") is None
    assert await store.head("tenant/file.bin") is None
    await store.close()
    assert client.closed is True

    with pytest.raises(ValueError, match="bucket"):
        S3CompatibleObjectStore(bucket="", client=client)
    error = ObjectMissingError("AccessDenied")
    assert S3CompatibleObjectStore._missing(error) is False


def _tenant(tmp_path, vector_backend="memory", object_backend="local") -> Tenant:
    return Tenant(
        tenant_id="tenant_a",
        name="A",
        model=ModelEndpoint(model_name="model"),
        storage_config=StorageBackendConfig(
            vector=VectorBackendConfig(
                backend=vector_backend,
                url=SecretStr("https://vector.example"),
                collection="tenant-knowledge",
                dimensions=3,
            ),
            object=ObjectBackendConfig(
                backend=object_backend,
                endpoint_url="https://objects.example",
                bucket="tenant-artifacts",
                access_key=SecretStr("access"),
                secret_key=SecretStr("secret"),
                local_path=str(tmp_path),
            ),
        ),
    )


async def test_storage_router_routes_caches_registers_and_closes_extended_backends(tmp_path):
    tenant = _tenant(tmp_path)
    router = TenantStorageRouter()
    vector = router.vector_store(tenant)
    objects = router.object_store(tenant)
    assert router.vector_store(tenant) is vector
    assert router.object_store(tenant) is objects
    await objects.put("file", b"data")
    assert await objects.get("file") == b"data"

    custom_vector = InMemoryVectorStore()
    custom_object = LocalObjectStore(str(tmp_path / "custom"))
    router.register_vector_factory("qdrant", lambda _config: custom_vector)
    router.register_object_factory("s3", lambda _config: custom_object)
    external = _tenant(tmp_path, vector_backend="qdrant", object_backend="s3")
    assert isinstance(router.vector_store(external), TenantVectorStore)
    assert isinstance(router.object_store(external), TenantObjectStore)

    invalid_vector = _tenant(tmp_path)
    invalid_vector.storage_config.vector.backend = "unknown"
    with pytest.raises(ValueError, match="registered factory"):
        router.vector_store(invalid_vector)
    invalid_object = _tenant(tmp_path)
    invalid_object.storage_config.object.backend = "unknown"
    with pytest.raises(ValueError, match="registered factory"):
        router.object_store(invalid_object)
    await router.close()


def test_extended_backend_config_validation_and_secret_repr():
    config = StorageBackendConfig()
    assert config.vector.backend == "memory"
    assert config.object.backend == "local"
    with pytest.raises(ValidationError):
        VectorBackendConfig(backend="unknown")
    with pytest.raises(ValidationError):
        VectorBackendConfig(dimensions=0)
    with pytest.raises(ValidationError):
        ObjectBackendConfig(backend="ftp")

    secure = ObjectBackendConfig(access_key="visible-access", secret_key="visible-secret")
    assert "visible-access" not in repr(secure)
    assert "visible-secret" not in repr(secure)
    assert router_module._secret_value(secure.secret_key) == "visible-secret"
