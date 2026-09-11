"""S3-compatible artifact storage with checksums and tenant-safe keys."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class Artifact:
    tenant_id: str
    artifact_id: str
    key: str
    checksum: str
    size: int
    content_type: str


class S3ArtifactStore:
    def __init__(self, bucket: str, *, client: Any | None = None, endpoint_url: str | None = None) -> None:
        if not bucket:
            raise ValueError("artifact bucket is required")
        if client is None:
            import boto3

            client = boto3.client("s3", endpoint_url=endpoint_url)
        self.client = client
        self.bucket = bucket

    def uri(self, artifact: Artifact) -> str:
        return f"s3://{self.bucket}/{artifact.key}"

    async def put(self, tenant_id: str, data: bytes, content_type: str, artifact_id: str | None = None) -> Artifact:
        identifier = artifact_id or uuid4().hex
        checksum = hashlib.sha256(data).hexdigest()
        key = f"tenants/{tenant_id}/artifacts/{identifier}"
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            Metadata={"sha256": checksum, "tenant-id": tenant_id},
        )
        return Artifact(tenant_id, identifier, key, checksum, len(data), content_type)

    async def get(self, artifact: Artifact) -> bytes:
        if artifact.key != f"tenants/{artifact.tenant_id}/artifacts/{artifact.artifact_id}":
            raise ValueError("artifact key is outside its tenant scope")
        response = await asyncio.to_thread(self.client.get_object, Bucket=self.bucket, Key=artifact.key)
        data = await asyncio.to_thread(response["Body"].read)
        if hashlib.sha256(data).hexdigest() != artifact.checksum:
            raise ValueError("artifact checksum mismatch")
        return data

    async def delete(self, artifact: Artifact) -> None:
        if artifact.key != f"tenants/{artifact.tenant_id}/artifacts/{artifact.artifact_id}":
            raise ValueError("artifact key is outside its tenant scope")
        await asyncio.to_thread(self.client.delete_object, Bucket=self.bucket, Key=artifact.key)
