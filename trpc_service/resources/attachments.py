"""Download only trusted Telegram file references, then persist tenant objects."""

from trpc_service.channels.base import InboundAttachmentDownloader
from trpc_service.config import ChannelType


class UnsupportedAttachmentError(ValueError):
    pass


class AttachmentIngestor:

    def __init__(self, artifacts, adapters):
        self._artifacts = artifacts
        self._adapters = adapters

    async def materialize_inbound(self, tenant_id, app_id, message):
        """Persist transport-owned files before a request crosses a process boundary."""
        if not message.attachments:
            return message
        adapter = self._adapters.get(message.binding_id)
        if not isinstance(adapter, InboundAttachmentDownloader):
            raise UnsupportedAttachmentError("channel attachment downloader is not configured")
        for attachment in message.attachments:
            if attachment.object_uri:
                continue
            content, mime_type, original_name = await adapter.download_attachment(attachment)
            metadata = await self._artifacts.put(tenant_id, app_id, original_name or attachment.name, mime_type
                                                 or attachment.mime_type, content)
            attachment.attachment_id = metadata.artifact_id
            attachment.name = metadata.original_name
            attachment.mime_type = metadata.mime_type
            attachment.object_uri = metadata.object_uri
            attachment.checksum_sha256 = metadata.checksum_sha256
            attachment.size_bytes = metadata.size_bytes
            attachment.source_url = ""
        return message

    async def materialize(self, request):
        for attachment in request.attachments:
            if attachment.object_uri:
                metadata, _ = await self._artifacts.get(request.tenant_id, attachment.attachment_id)
                if metadata.app_id != request.app_id:
                    raise UnsupportedAttachmentError("attachment app mismatch")
                continue
            adapter = self._adapters.get(request.binding_id)
            if isinstance(adapter, InboundAttachmentDownloader):
                content, mime, original_name = await adapter.download_attachment(attachment)
                metadata = await self._artifacts.put(request.tenant_id, request.app_id, original_name
                                                     or attachment.name, mime or attachment.mime_type, content)
                attachment.attachment_id = metadata.artifact_id
                attachment.name = metadata.original_name
                attachment.mime_type = metadata.mime_type
                attachment.object_uri = metadata.object_uri
                attachment.checksum_sha256 = metadata.checksum_sha256
                attachment.size_bytes = metadata.size_bytes
                attachment.source_url = ""
                continue
            prefixes = {ChannelType.TELEGRAM: "telegram://file/", ChannelType.WECOM_KF: "wecom-kf://media/"}
            prefix = prefixes.get(request.channel, "")
            if not prefix or adapter is None or not attachment.source_url.startswith(prefix):
                raise UnsupportedAttachmentError("attachment download provider is not configured")
            content, mime = await adapter.download_file(attachment.source_url.removeprefix(prefix))
            if request.channel == ChannelType.WECOM_KF:
                attachment.mime_type = mime
            metadata = await self._artifacts.put(request.tenant_id, request.app_id, attachment.name,
                                                 attachment.mime_type, content)
            attachment.attachment_id = metadata.artifact_id
            attachment.object_uri = metadata.object_uri
            attachment.checksum_sha256 = metadata.checksum_sha256
            attachment.size_bytes = metadata.size_bytes
        return request
