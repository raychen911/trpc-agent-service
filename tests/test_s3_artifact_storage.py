import hashlib
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from trpc_service.storage import ArtifactIntegrityError, ArtifactMetadata
from trpc_service.storage.adapters.s3 import S3ArtifactStore
from trpc_service.tenant import TenantContext


class FakeBody:

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self, amount: int) -> bytes:
        chunk, self._payload = self._payload[:amount], self._payload[amount:]
        return chunk

    def close(self) -> None:
        pass


class FakeS3Client:

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.extra_args: list[object] = []
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def upload_fileobj(self, file: object, bucket: str, key: str, ExtraArgs: object) -> None:
        self.extra_args.append(ExtraArgs)
        self.objects[(bucket, key)] = file.read()  # type: ignore[attr-defined]

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def generate_presigned_url(
        self,
        operation: str,
        *,
        Params: dict[str, str],
        ExpiresIn: int,
    ) -> str:
        return f"https://objects.test/{Params['Bucket']}/{Params['Key']}?ttl={ExpiresIn}"


class ConcurrentBucketClient(FakeS3Client):
    """Model another process creating the bucket after this process checks it."""

    def head_bucket(self, *, Bucket: str) -> dict[str, object]:
        del Bucket
        raise ClientError(
            {"Error": {
                "Code": "404",
                "Message": "Not Found"
            }},
            "HeadBucket",
        )

    def create_bucket(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        raise ClientError(
            {"Error": {
                "Code": "BucketAlreadyOwnedByYou",
                "Message": "Concurrent creator won",
            }},
            "CreateBucket",
        )


async def _content(payload: bytes) -> AsyncIterator[bytes]:
    yield payload


@pytest.mark.anyio
async def test_s3_validation_does_not_create_missing_buckets_and_close_releases_client() -> None:
    client = ConcurrentBucketClient()
    store = S3ArtifactStore(client, bucket="existing")  # type: ignore[arg-type]
    with pytest.raises(ClientError, match="HeadBucket"):
        await store.validate_bucket()
    await store.close()
    assert client.closed


@pytest.mark.anyio
async def test_s3_artifact_store_uses_scoped_keys_and_validates_content() -> None:
    client = FakeS3Client()
    store = S3ArtifactStore(client, bucket="artifacts", chunk_size=3)
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )
    payload = b"seaweedfs"
    metadata = ArtifactMetadata(
        filename="artifact.txt",
        media_type="text/plain",
        checksum=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
    )

    reference = await store.put(context, _content(payload), metadata)

    assert b"".join([chunk
                     async for chunk in store.open(context, reference.artifact_id)]) == payload
    assert str(context.tenant_id) in reference.uri
    assert "ttl=60" in await store.create_download_url(context, reference.artifact_id, 60)
    with pytest.raises(ArtifactIntegrityError):
        await store.put(context, _content(payload + b"!"), metadata)


@pytest.mark.anyio
async def test_s3_artifact_store_encodes_unicode_filename_as_header_safe_metadata() -> None:
    """IM filenames may be Unicode, while S3 user-metadata headers must be ASCII."""

    client = FakeS3Client()
    store = S3ArtifactStore(client, bucket="artifacts")
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-image",
        trace_id="trace-image",
    )
    payload = b"image"

    await store.put(
        context,
        _content(payload),
        ArtifactMetadata(
            filename="企业微信图片.png",
            media_type="image/png",
            checksum=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
        ),
    )

    extra_args = client.extra_args[0]
    assert isinstance(extra_args, dict)
    metadata = extra_args["Metadata"]
    assert isinstance(metadata, dict)
    assert metadata["filename"].isascii()


@pytest.mark.anyio
async def test_s3_bucket_initialization_is_idempotent_across_processes() -> None:
    """Gateway and Workers may race to create the same configured bucket."""

    store = S3ArtifactStore(ConcurrentBucketClient(), bucket="artifacts")

    await store.ensure_bucket()
