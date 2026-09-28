"""Generic S3-compatible Artifact storage, used locally with SeaweedFS."""

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, Protocol, cast
from uuid import NAMESPACE_URL, uuid5

from botocore.exceptions import ClientError

from trpc_service.storage.errors import ArtifactIntegrityError, StoredObjectNotFound
from trpc_service.storage.ports import ArtifactStore
from trpc_service.storage.types import ArtifactMetadata, ArtifactRef
from trpc_service.tenant.context import TenantContext


class _StreamingBody(Protocol):
    """Small subset of botocore's streaming response used by this adapter."""

    def read(self, amount: int) -> bytes:
        """Read at most amount bytes."""

        ...

    def close(self) -> None:
        """Release the underlying HTTP connection."""

        ...


class S3Client(Protocol):
    """Structural client contract that keeps boto3 dynamic types at composition."""

    def close(self) -> None:
        """Close the client-owned HTTP connection pools."""

        ...

    def upload_fileobj(
        self,
        file: BinaryIO,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, object],
    ) -> None:
        """Upload a seekable object."""

        ...

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        """Open an object body."""

        ...

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        """Verify that an object exists."""

        ...

    def head_bucket(self, *, Bucket: str) -> dict[str, object]:
        """Verify that a bucket exists."""

        ...

    def create_bucket(
        self,
        *,
        Bucket: str,
        CreateBucketConfiguration: dict[str, str] | None = None,
    ) -> dict[str, object]:
        """Create a bucket."""

        ...

    def generate_presigned_url(
        self,
        operation: str,
        *,
        Params: dict[str, str],
        ExpiresIn: int,
    ) -> str:
        """Create a provider-generated temporary URL."""

        ...


class S3ArtifactStore(ArtifactStore):
    """Store validated files under mandatory tenant and Agent key prefixes."""

    def __init__(
        self,
        client: S3Client,
        *,
        bucket: str,
        region: str = "us-east-1",
        chunk_size: int = 1024 * 1024,
        spool_limit: int = 8 * 1024 * 1024,
    ) -> None:
        if chunk_size <= 0 or spool_limit <= 0:
            raise ValueError("S3 stream and spool sizes must be positive")
        self._client = client
        self._bucket = bucket
        self._region = region
        self._chunk_size = chunk_size
        self._spool_limit = spool_limit

    async def ensure_bucket(self) -> None:
        """Create the configured local/external bucket when it is absent."""

        try:
            await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)
        except ClientError as error:
            error_code = str(error.response.get("Error", {}).get("Code", ""))
            if error_code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            try:
                if self._region == "us-east-1":
                    await asyncio.to_thread(self._client.create_bucket, Bucket=self._bucket)
                else:
                    await asyncio.to_thread(
                        self._client.create_bucket,
                        Bucket=self._bucket,
                        CreateBucketConfiguration={"LocationConstraint": self._region},
                    )
            except ClientError as create_error:
                create_code = str(create_error.response.get("Error", {}).get("Code", ""))
                # Gateway and Worker processes initialize concurrently. The
                # configured owner winning this race is an idempotent success;
                # BucketAlreadyExists remains fatal because it may be foreign.
                if create_code != "BucketAlreadyOwnedByYou":
                    raise

    async def validate_bucket(self) -> None:
        """Require an existing accessible bucket without creating infrastructure."""

        await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)

    async def close(self) -> None:
        """Release the synchronous SDK's pooled HTTP connections off the event loop."""

        await asyncio.to_thread(self._client.close)

    @staticmethod
    def _artifact_id(context: TenantContext, checksum: str) -> str:
        return str(uuid5(
            NAMESPACE_URL,
            f"{context.tenant_id}:{checksum}",
        ))

    @staticmethod
    def _key(context: TenantContext, artifact_id: str) -> str:
        return f"{context.tenant_id}/{artifact_id}"

    @staticmethod
    def _metadata_filename(filename: str) -> str:
        """Encode filenames that cannot be represented in HTTP metadata headers."""

        # S3 user metadata is transported as HTTP headers. Botocore rejects
        # Unicode header values before contacting SeaweedFS, which is common
        # for filenames supplied by Chinese IM clients.
        if filename.isascii() and filename.isprintable():
            return filename
        encoded = base64.urlsafe_b64encode(filename.encode("utf-8")).decode("ascii")
        return f"base64url:{encoded}"

    @staticmethod
    def _translate_not_found(error: Exception, artifact_id: str) -> StoredObjectNotFound:
        return StoredObjectNotFound(f"Artifact does not exist: {artifact_id}")

    async def put(
        self,
        context: TenantContext,
        content: AsyncIterator[bytes],
        metadata: ArtifactMetadata,
    ) -> ArtifactRef:
        """Validate streamed bytes before publishing the object under a stable key."""

        digest = hashlib.sha256()
        size = 0
        spool = SpooledTemporaryFile(max_size=self._spool_limit, mode="w+b")
        try:
            async for chunk in content:
                digest.update(chunk)
                size += len(chunk)
                spool.write(chunk)
            checksum = digest.hexdigest()
            if size != metadata.size_bytes or checksum != metadata.checksum:
                raise ArtifactIntegrityError(
                    "Artifact size or SHA-256 checksum does not match metadata")
            spool.seek(0)
            artifact_id = self._artifact_id(context, checksum)
            key = self._key(context, artifact_id)
            await asyncio.to_thread(
                self._client.upload_fileobj,
                cast(BinaryIO, spool),
                self._bucket,
                key,
                ExtraArgs={
                    "ContentType": metadata.media_type,
                    "Metadata": {
                        "checksum": checksum,
                        "filename": self._metadata_filename(metadata.filename),
                    },
                },
            )
        finally:
            spool.close()
        return ArtifactRef(
            artifact_id=artifact_id,
            uri=f"s3://{self._bucket}/{key}",
            checksum=checksum,
        )

    async def open(
        self,
        context: TenantContext,
        artifact_id: str,
    ) -> AsyncIterator[bytes]:
        """Stream an object without exposing another tenant's key prefix."""

        try:
            response = await asyncio.to_thread(
                self._client.get_object,
                Bucket=self._bucket,
                Key=self._key(context, artifact_id),
            )
        except (ClientError, KeyError) as error:
            raise self._translate_not_found(error, artifact_id) from error
        body = cast(_StreamingBody, response["Body"])
        try:
            while chunk := await asyncio.to_thread(body.read, self._chunk_size):
                yield chunk
        finally:
            await asyncio.to_thread(body.close)

    async def create_download_url(
        self,
        context: TenantContext,
        artifact_id: str,
        ttl_seconds: int,
    ) -> str:
        """Verify existence before returning a short-lived scoped download URL."""

        if ttl_seconds <= 0:
            raise ValueError("download URL TTL must be positive")
        params = {
            "Bucket": self._bucket,
            "Key": self._key(context, artifact_id),
        }
        try:
            await asyncio.to_thread(self._client.head_object, **params)
        except (ClientError, KeyError) as error:
            raise self._translate_not_found(error, artifact_id) from error
        return await asyncio.to_thread(
            self._client.generate_presigned_url,
            "get_object",
            Params=params,
            ExpiresIn=ttl_seconds,
        )
