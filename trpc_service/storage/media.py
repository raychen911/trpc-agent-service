"""Materialize provider media locators into tenant-scoped artifact storage."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from typing import Any

from ..tenant import ChannelBinding, InboundMessage, TenantConfig


class InboundMediaMaterializer:
    def __init__(self, router: Any, *, repository: Any | None = None, max_bytes: int = 30 * 1024 * 1024) -> None:
        self.router = router
        self.repository = repository
        self.max_bytes = max_bytes

    async def materialize(
        self,
        config: TenantConfig,
        message: InboundMessage,
        adapter: Any,
        binding: ChannelBinding,
    ) -> InboundMessage:
        media = message.raw.get("normalized_media")
        if not isinstance(media, dict):
            return message
        store = self.router.route(config).object_store
        if store is None:
            return message
        locator = str(media.get("file_id") or media.get("media_id") or media.get("image_key") or media.get("file_key") or "")
        if not locator:
            return message
        artifact_id = hashlib.sha256(
            f"{message.tenant_id}:{message.channel}:{message.account_id}:{message.external_message_id}:{locator}".encode()
        ).hexdigest()
        existing = None
        if self.repository is not None and hasattr(self.repository, "get_artifact"):
            existing = await self.repository.get_artifact(message.tenant_id, artifact_id)
        if existing is None:
            data, content_type = await self._download(adapter, binding, message, media, locator)
            if not data or len(data) > self.max_bytes:
                raise ValueError("inbound media is empty or exceeds the configured limit")
            artifact = await store.put(message.tenant_id, data, content_type, artifact_id=artifact_id)
            if self.repository is not None and hasattr(self.repository, "record_artifact"):
                await self.repository.record_artifact(artifact)
        else:
            artifact = existing
        artifact_data = asdict(artifact)
        uri = getattr(store, "uri", None)
        if callable(uri):
            artifact_data["uri"] = uri(artifact)
        raw = dict(message.raw)
        raw["normalized_media"] = {**media, "artifact": artifact_data}
        raw["_artifact_materialized"] = True
        return replace(message, raw=raw)

    async def _download(
        self,
        adapter: Any,
        binding: ChannelBinding,
        message: InboundMessage,
        media: dict[str, Any],
        locator: str,
    ) -> tuple[bytes, str]:
        if message.channel == "feishu":
            resource_type = "image" if media.get("type") == "image" else "file"
            data = await adapter.download_resource(
                binding,
                message.external_message_id,
                locator,
                resource_type=resource_type,
                max_bytes=self.max_bytes,
            )
            return data, "image/jpeg" if resource_type == "image" else "application/octet-stream"
        download = getattr(adapter, "download_media", None)
        if not callable(download):
            raise TypeError(f"{message.channel} adapter does not support media download")
        return await download(binding, locator, max_bytes=self.max_bytes)
