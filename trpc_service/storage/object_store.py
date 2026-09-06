"""Filesystem object store for artifacts and file replies."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from trpc_service.security.identifiers import filesystem_component


@dataclass(slots=True)
class StoredObject:
    tenant_id: str
    object_id: str
    path: str
    content_type: str
    size: int


class FileObjectStore:
    backend_name = "object"

    def __init__(self, root: str | Path = "data/artifacts") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, tenant_id: str, content: bytes, content_type: str = "application/octet-stream") -> StoredObject:
        object_id = str(uuid4())
        return self.put_with_id(tenant_id, object_id, content, content_type)

    def put_with_id(
        self, tenant_id: str, object_id: str, content: bytes, content_type: str = "application/octet-stream"
    ) -> StoredObject:
        tenant_dir = self.root / filesystem_component(tenant_id, "tenant_id")
        tenant_dir.mkdir(parents=True, exist_ok=True)
        path = tenant_dir / filesystem_component(object_id, "object_id")
        path.write_bytes(content)
        # Keep content type beside the payload so migrations from metadata-
        # preserving stores (for example Redis/S3) do not lose it.
        metadata_path = tenant_dir / f"{filesystem_component(object_id, 'object_id')}.meta.json"
        metadata_path.write_text(json.dumps({"content_type": content_type}), encoding="utf-8")
        return StoredObject(
            tenant_id=tenant_id,
            object_id=object_id,
            path=str(path),
            content_type=content_type,
            size=len(content),
        )

    def get(self, tenant_id: str, object_id: str) -> bytes:
        path = self.root / filesystem_component(tenant_id, "tenant_id") / filesystem_component(object_id, "object_id")
        return path.read_bytes()

    def list_by_tenant(self, tenant_id: str) -> list[StoredObject]:
        tenant_dir = self.root / filesystem_component(tenant_id, "tenant_id")
        if not tenant_dir.exists():
            return []
        result = []
        for path in sorted(tenant_dir.iterdir()):
            if not path.is_file() or path.name.endswith(".meta.json"):
                continue
            metadata_path = path.with_name(f"{path.name}.meta.json")
            content_type = "application/octet-stream"
            if metadata_path.is_file():
                try:
                    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                    content_type = str(metadata.get("content_type") or content_type)
                except (OSError, ValueError, TypeError):
                    pass
            result.append(StoredObject(tenant_id, path.name, str(path), content_type, path.stat().st_size))
        return result


class RedisObjectStore:
    """Redis object store for small artifacts and provider-independent tests."""

    backend_name = "redis"

    def __init__(self, url: str | None = None, prefix: str = "trpc-agent") -> None:
        import redis

        self.client = redis.Redis.from_url(url or os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        self.prefix = prefix

    def put(self, tenant_id: str, content: bytes, content_type: str = "application/octet-stream") -> StoredObject:
        object_id = str(uuid4())
        return self.put_with_id(tenant_id, object_id, content, content_type)

    def put_with_id(
        self, tenant_id: str, object_id: str, content: bytes, content_type: str = "application/octet-stream"
    ) -> StoredObject:
        key = f"{self.prefix}:artifact:{tenant_id}:{object_id}"
        self.client.hset(key, mapping={"content": base64.b64encode(content), "content_type": content_type})
        return StoredObject(tenant_id, object_id, f"redis://{key}", content_type, len(content))

    def get(self, tenant_id: str, object_id: str) -> bytes:
        key = f"{self.prefix}:artifact:{tenant_id}:{object_id}"
        value = self.client.hget(key, "content")
        if value is None:
            raise FileNotFoundError(object_id)
        return base64.b64decode(value)

    def list_by_tenant(self, tenant_id: str) -> list[StoredObject]:
        prefix = f"{self.prefix}:artifact:{tenant_id}:"
        result = []
        for key in self.client.scan_iter(f"{prefix}*"):
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            object_id = key_text[len(prefix) :]
            content = self.client.hget(key, "content") or b""
            content_type = self.client.hget(key, "content_type") or b"application/octet-stream"
            if isinstance(content_type, bytes):
                content_type = content_type.decode("utf-8")
            result.append(
                StoredObject(
                    tenant_id, object_id, f"redis://{key_text}", str(content_type), len(base64.b64decode(content))
                )
            )
        return sorted(result, key=lambda item: item.object_id)


class S3ObjectStore:
    """S3-compatible artifact storage for AWS S3, MinIO, and OSS gateways."""

    backend_name = "s3"

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        region: str = "us-east-1",
        access_key: str = "",
        secret_key: str = "",
        prefix: str = "trpc-agent",
    ) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("S3 backend requires boto3") from exc
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint or None,
            region_name=region or None,
            aws_access_key_id=access_key or None,
            aws_secret_access_key=secret_key or None,
        )

    def _key(self, tenant_id: str, object_id: str) -> str:
        return f"{self.prefix}/{tenant_id}/{object_id}"

    def put(self, tenant_id: str, content: bytes, content_type: str = "application/octet-stream") -> StoredObject:
        object_id = str(uuid4())
        return self.put_with_id(tenant_id, object_id, content, content_type)

    def put_with_id(
        self, tenant_id: str, object_id: str, content: bytes, content_type: str = "application/octet-stream"
    ) -> StoredObject:
        key = self._key(tenant_id, object_id)
        checksum = hashlib.sha256(content).hexdigest()
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
            Metadata={"tenant_id": tenant_id, "sha256": checksum},
        )
        return StoredObject(tenant_id, object_id, f"s3://{self.bucket}/{key}", content_type, len(content))

    def get(self, tenant_id: str, object_id: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(tenant_id, object_id))
        return response["Body"].read()

    def list_by_tenant(self, tenant_id: str) -> list[StoredObject]:
        prefix = self._key(tenant_id, "")
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        result = []
        for item in response.get("Contents", []):
            object_id = str(item["Key"])[len(prefix) :]
            result.append(
                StoredObject(
                    tenant_id,
                    object_id,
                    f"s3://{self.bucket}/{item['Key']}",
                    "application/octet-stream",
                    int(item.get("Size", 0)),
                )
            )
        return sorted(result, key=lambda item: item.object_id)

    def close(self) -> None:
        return None
