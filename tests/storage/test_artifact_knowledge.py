"""Object and vector backends preserve tenant scope."""

from dataclasses import dataclass
from io import BytesIO

import pytest
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from trpc_service.storage import LocalVectorKnowledgeStore
from trpc_service.storage import S3ArtifactService
from trpc_service.storage import TenantBoundaryError


@dataclass
class ObjectInfo:
    object_name: str


class Response(BytesIO):
    def close(self):
        super().close()

    def release_conn(self):
        pass


class FakeMinio:
    def __init__(self):
        self.objects = {}

    def bucket_exists(self, bucket):
        return True

    def put_object(self, bucket, name, stream, length, **kwargs):
        self.objects[(bucket, name)] = (stream.read(length), kwargs)

    def get_object(self, bucket, name):
        return Response(self.objects[(bucket, name)][0])

    def list_objects(self, bucket, prefix, recursive=True):
        return [
            ObjectInfo(name) for stored_bucket, name in self.objects
            if stored_bucket == bucket and name.startswith(prefix)
        ]

    def remove_object(self, bucket, name):
        self.objects.pop((bucket, name), None)

    def stat_object(self, bucket, name):
        data, kwargs = self.objects[(bucket, name)]
        return type(
            "Stat",
            (),
            {
                "size": len(data),
                "content_type": kwargs.get("content_type"),
                "metadata": kwargs.get("metadata", {}),
            },
        )()


@pytest.mark.asyncio
async def test_s3_artifact_implements_trpc_contract_and_tenant_prefix(tmp_path) -> None:
    from trpc_service.persistence import Database
    database = Database(f"sqlite:///{tmp_path / 'artifact.db'}")
    database.create_schema()
    client = FakeMinio()
    service = S3ArtifactService(tenant_id="tenant_acme", bucket="artifacts", client=client, database=database)
    artifact_id = ArtifactId(
        app_name="tenant_acme:support_app",
        user_id="usr_1",
        session_id="ses_1",
        filename="answer.txt",
    )
    version = await service.save_artifact(
        artifact_id=artifact_id,
        artifact=Part.from_text(text="hello"),
        metadata={"request_id": "req-1"},
    )
    loaded = await service.load_artifact(artifact_id=artifact_id, version=version)
    assert loaded.data.text == "hello"
    assert loaded.version.custom_metadata["request_id"] == "req-1"
    assert next(iter(client.objects))[1].startswith("tenants/tenant_acme/")

    with pytest.raises(TenantBoundaryError):
        await service.save_artifact(
            artifact_id=artifact_id.model_copy(update={"app_name": "tenant_other:support_app"}),
            artifact=Part.from_text(text="blocked"),
            metadata={},
        )


def test_local_vector_queries_always_apply_tenant_kb_and_index_scope() -> None:
    store = LocalVectorKnowledgeStore()
    store.upsert("tenant_acme", "kb_support", 2, "doc-a", "chunk-a", [1.0, 0.0], "acme")
    store.upsert("tenant_beta", "kb_support", 2, "doc-b", "chunk-b", [1.0, 0.0], "beta")
    store.upsert("tenant_acme", "kb_support", 1, "doc-old", "chunk-old", [1.0, 0.0], "old")

    results = store.search("tenant_acme", "kb_support", 2, [1.0, 0.0], limit=10)
    assert [item.text for item in results] == ["acme"]
    with pytest.raises(TenantBoundaryError):
        store.search("", "kb_support", 2, [1.0, 0.0])
