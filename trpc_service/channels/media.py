"""Provider-neutral ingress media persistence boundary."""

from collections.abc import AsyncIterator, Mapping
import hashlib
from typing import Protocol

from trpc_service.channels.contracts import ChannelBindingConfig
from trpc_service.storage.ports import ArtifactStore
from trpc_service.storage.router import BackendProfile, StorageRouter
from trpc_service.storage.types import ArtifactMetadata
from trpc_service.tenant.context import TenantContext


def detect_image_media_type(content: bytes) -> str:
    """Identify image bytes without trusting a provider filename or MIME header."""

    signatures = (
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"BM", "image/bmp"),
        (b"II*\x00", "image/tiff"),
        (b"MM\x00*", "image/tiff"),
    )
    for signature, media_type in signatures:
        if content.startswith(signature):
            return media_type
    if len(content) >= 12 and content[0:4] == b"RIFF" and content[8:12] == b"WEBP":
        return "image/webp"
    raise ValueError("inbound image format is unsupported or does not match its message kind")


class ArtifactBackendProfileProvider(Protocol):
    """Resolve the immutable Backend Profile selected for an IM message."""

    async def load_backends(self, context: TenantContext) -> Mapping[str, object]:
        ...


class ChannelMediaStore:
    """Persist downloaded IM media in the configured tenant artifact store."""

    def __init__(
        self,
        artifacts: ArtifactStore,
        *,
        storage_router: StorageRouter | None = None,
        backend_profiles: ArtifactBackendProfileProvider | None = None,
    ) -> None:
        self._artifacts = artifacts
        self._storage_router = storage_router
        self._backend_profiles = backend_profiles

    async def _resolve_artifacts(self, context: TenantContext) -> ArtifactStore:
        """Route uploads by the same versioned profile later used for ingestion."""

        if self._storage_router is None or self._backend_profiles is None:
            return self._artifacts
        backends = await self._backend_profiles.load_backends(context)
        resolved = self._storage_router.resolve(BackendProfile.from_mapping(backends))
        if resolved.artifact is None:
            raise ValueError("Agent backend profile requires artifact storage")
        return resolved.artifact

    async def put(
        self,
        binding: ChannelBindingConfig,
        *,
        principal_id: str,
        message_id: str,
        content: bytes,
        filename: str,
        media_type: str,
        context: TenantContext | None = None,
    ) -> str:
        checksum = hashlib.sha256(content).hexdigest()

        async def blocks() -> AsyncIterator[bytes]:
            yield content

        context = context or TenantContext(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            config_version=1,
            request_id=message_id[:128],
            trace_id=f"channel-media:{message_id}"[:128],
        )
        if (context.tenant_id != binding.tenant_id or context.agent_app_id != binding.agent_app_id):
            raise PermissionError("Channel media context does not match its binding")
        metadata = ArtifactMetadata(
            filename=filename,
            media_type=media_type,
            checksum=checksum,
            size_bytes=len(content),
            attributes={
                "source": binding.channel_type,
                "message_id": message_id,
                "principal_id": principal_id,
            },
        )
        artifacts = await self._resolve_artifacts(context)
        reference = await artifacts.put(
            context,
            blocks(),
            metadata,
        )
        return reference.artifact_id

    async def read(self, binding: ChannelBindingConfig, artifact_id: str) -> bytes:
        """Read a tenant-owned artifact for provider upload."""

        context = TenantContext(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            config_version=1,
            request_id="channel-media-read",
            trace_id="channel-media-read",
        )
        return b"".join([block async for block in self._artifacts.open(context, artifact_id)])
