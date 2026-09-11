"""S3-compatible ArtifactService using tenant-prefixed immutable versions."""

from __future__ import annotations
import asyncio

import hashlib
import json
from io import BytesIO
from urllib.parse import quote
from urllib.parse import unquote
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from trpc_service.persistence.models import ArtifactMetadataRow, ArtifactSequenceRow

from trpc_agent_sdk.abc import ArtifactEntry
from trpc_agent_sdk.abc import ArtifactServiceABC
from trpc_agent_sdk.abc import ArtifactVersion
from trpc_agent_sdk.artifacts import create_artifact_uri
from trpc_agent_sdk.types import Part

from .errors import TenantBoundaryError


class S3ArtifactService(ArtifactServiceABC):
    """Persist tRPC artifacts in MinIO/S3 without storing credentials."""

    def __init__(self, *, tenant_id: str, bucket: str, client, database) -> None:
        self.tenant_id = tenant_id
        self.bucket = bucket
        self._client = client
        self._database = database
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

    def _scope(self, artifact_id) -> str:
        if not artifact_id.app_name.startswith(f"{self.tenant_id}:"):
            raise TenantBoundaryError("artifact app_name is outside the tenant scope")
        scope = artifact_id.session_id or "user"
        values = (artifact_id.app_name, artifact_id.user_id, scope, artifact_id.filename)
        if not artifact_id.filename or any(".." in value or "/" in value or "\\" in value for value in values):
            raise ValueError("artifact identifiers must not contain path separators")
        encoded = "/".join(quote(value, safe="-_.:") for value in values)
        return f"tenants/{self.tenant_id}/{encoded}"

    def _version_keys(self, artifact_id) -> list[tuple[int, str]]:
        identity = self._identity(artifact_id)
        with self._database.sessions() as session:
            rows = session.scalars(select(ArtifactMetadataRow).where(
                ArtifactMetadataRow.tenant_id == self.tenant_id,
                ArtifactMetadataRow.artifact_id == identity,
                ArtifactMetadataRow.status == "published").order_by(ArtifactMetadataRow.version))
            return [(row.version, row.object_uri) for row in rows]

    def _identity(self, artifact_id):
        return hashlib.sha256(f"{self.bucket}/{self._scope(artifact_id)}".encode()).hexdigest()

    def _reserve(self, artifact_id, payload, mime_type):
        identity = self._identity(artifact_id)
        # A uniqueness conflict means another process created the counter.
        try:
            with self._database.sessions.begin() as session:
                session.add(ArtifactSequenceRow(tenant_id=self.tenant_id, artifact_id=identity, next_version=0))
                session.flush()
                prefix = f"{self._scope(artifact_id)}/versions/"
                if next(iter(self._client.list_objects(self.bucket, prefix=prefix, recursive=True)), None) is not None:
                    raise ValueError("unregistered artifact objects exist; refusing to overwrite")
        except IntegrityError:
            with self._database.sessions() as session:
                if session.get(ArtifactSequenceRow, (self.tenant_id, identity)) is None:
                    raise
        with self._database.sessions.begin() as session:
            version = session.scalar(update(ArtifactSequenceRow).where(
                ArtifactSequenceRow.tenant_id == self.tenant_id,
                ArtifactSequenceRow.artifact_id == identity).values(
                    next_version=ArtifactSequenceRow.next_version + 1).returning(ArtifactSequenceRow.next_version)) - 1
            key = f"{self._scope(artifact_id)}/versions/{version}"
            session.add(ArtifactMetadataRow(
                tenant_id=self.tenant_id, artifact_id=identity, version=version,
                session_id=artifact_id.session_id, user_id=artifact_id.user_id, filename=artifact_id.filename,
                object_uri=key, mime_type=mime_type, size=len(payload),
                content_hash=hashlib.sha256(payload).hexdigest(), status="reserved"))
        return version, key

    def _publish(self, artifact_id, version):
        with self._database.sessions.begin() as session:
            row = session.get(ArtifactMetadataRow, (self.tenant_id, self._identity(artifact_id), version))
            if row is None or row.status not in {"reserved", "published"}:
                raise ValueError("artifact reservation is unavailable")
            stat = self._client.stat_object(self.bucket, row.object_uri)
            digest = stat.metadata.get("sha256") or stat.metadata.get("x-amz-meta-sha256")
            if stat.size != row.size or digest != row.content_hash:
                raise ValueError("artifact upload verification failed")
            row.status = "published"

    async def recover_upload(self, *, artifact_id, version):
        """Finish publication after an object write succeeded but SQL confirmation failed."""
        self._publish(artifact_id, version)
        return version

    async def save_artifact(self, *, artifact_id, artifact: Part, metadata=None) -> int:
        return await asyncio.to_thread(self._save, artifact_id=artifact_id, artifact=artifact, metadata=metadata)

    def _save(self, *, artifact_id, artifact, metadata):
        self._scope(artifact_id)
        if artifact.inline_data is not None:
            payload = bytes(artifact.inline_data.data)
            mime_type = artifact.inline_data.mime_type
        elif artifact.text is not None:
            payload = artifact.text.encode("utf-8")
            mime_type = "text/plain"
        else:
            raise ValueError("S3 artifacts must contain inline data or text")
        version, key = self._reserve(artifact_id, payload, mime_type)
        safe_metadata = dict(metadata or {})
        safe_metadata["sha256"] = hashlib.sha256(payload).hexdigest()
        safe_metadata["artifact-metadata"] = json.dumps(metadata or {}, sort_keys=True)
        self._client.put_object(
            self.bucket,
            key,
            BytesIO(payload),
            len(payload),
            content_type=mime_type,
            metadata=safe_metadata,
        )
        self._publish(artifact_id, version)
        return version

    async def load_artifact(self, *, artifact_id, version=None):
        return await asyncio.to_thread(self._load, artifact_id=artifact_id, version=version)

    def _load(self, *, artifact_id, version):
        versions = self._version_keys(artifact_id)
        if not versions:
            return None
        selected = versions[-1] if version is None else next((item for item in versions if item[0] == version), None)
        if selected is None:
            return None
        response = self._client.get_object(self.bucket, selected[1])
        try:
            payload = response.read()
        finally:
            response.close()
            response.release_conn()
        stat = self._client.stat_object(self.bucket, selected[1])
        stored_hash = stat.metadata.get("sha256") or stat.metadata.get("x-amz-meta-sha256")
        with self._database.sessions() as session:
            row = session.get(ArtifactMetadataRow, (self.tenant_id, self._identity(artifact_id), selected[0]))
            expected_hash = row.content_hash
        if stored_hash != expected_hash or expected_hash != hashlib.sha256(payload).hexdigest():
            raise ValueError("artifact content hash mismatch")
        mime_type = stat.content_type or "application/octet-stream"
        if mime_type.startswith("text/"):
            data = Part.from_text(text=payload.decode("utf-8"))
        else:
            data = Part.from_bytes(data=payload, mime_type=mime_type)
        raw_metadata = stat.metadata.get("artifact-metadata") or stat.metadata.get("x-amz-meta-artifact-metadata")
        custom_metadata = json.loads(raw_metadata) if raw_metadata else {}
        return ArtifactEntry(
            data=data,
            version=ArtifactVersion(
                version=selected[0],
                canonical_uri=create_artifact_uri(artifact_id, selected[0]),
                custom_metadata=custom_metadata,
                mime_type=mime_type,
            ),
        )

    async def list_artifact_keys(self, *, artifact_id) -> list[str]:
        if artifact_id.session_id is None:
            raise ValueError("session_id is required when listing artifact keys")
        prefix_id = artifact_id.model_copy(update={"filename": "placeholder"})
        prefix = self._scope(prefix_id).rsplit("/", 1)[0] + "/"
        names = set()
        for item in self._client.list_objects(self.bucket, prefix=prefix, recursive=True):
            remainder = item.object_name.removeprefix(prefix)
            if "/versions/" in remainder:
                filename = unquote(remainder.split("/versions/", 1)[0])
                if self._version_keys(artifact_id.model_copy(update={"filename": filename})):
                    names.add(filename)
        return sorted(names)

    async def delete_artifact(self, *, artifact_id) -> None:
        identity = self._identity(artifact_id)
        with self._database.sessions.begin() as session:
            rows = list(session.scalars(select(ArtifactMetadataRow).where(
                ArtifactMetadataRow.tenant_id == self.tenant_id, ArtifactMetadataRow.artifact_id == identity)))
            keys = [row.object_uri for row in rows]
            for row in rows:
                row.status = "deleted"
        for key in keys:
            self._client.remove_object(self.bucket, key)

    async def list_versions(self, *, artifact_id) -> list[int]:
        return [version for version, _ in self._version_keys(artifact_id)]

    async def list_artifact_versions(self, *, artifact_id) -> list[ArtifactVersion]:
        result = []
        for version in await self.list_versions(artifact_id=artifact_id):
            entry = await self.load_artifact(artifact_id=artifact_id, version=version)
            if entry is not None:
                result.append(entry.version)
        return result

    async def get_artifact_version(self, *, artifact_id, version=None):
        entry = await self.load_artifact(artifact_id=artifact_id, version=version)
        return entry.version if entry else None

    async def close(self) -> None:
        return None
