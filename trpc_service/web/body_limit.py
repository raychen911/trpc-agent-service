"""Bound JSON/form input before framework parsing without buffering file uploads."""

import asyncio
import re

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from trpc_service.storage.knowledge import MAX_KNOWLEDGE_FILE_BYTES


class RequestBodyLimitMiddleware:
    """Stop oversized control requests before JSON decoding and authentication work."""

    def __init__(self, app: ASGIApp, *, api_prefix: str, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes
        base = re.escape(api_prefix) + r"/tenants/[^/]+/knowledge-bases/[^/]+/documents"
        self.upload_paths = {
            "POST": re.compile(base + r"/?$"),
            "PUT": re.compile(base + r"/[^/]+/?$")
        }

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        upload_pattern = self.upload_paths.get(scope["method"])
        upload = upload_pattern is not None and upload_pattern.fullmatch(scope["path"]) is not None
        limit = MAX_KNOWLEDGE_FILE_BYTES if upload else self.max_bytes

        async def reject(status: int, code: str, message: str) -> None:
            response = JSONResponse({"error": {
                "code": code,
                "message": message
            }},
                                    status_code=status)
            await response(scope, receive, send)

        lengths = [value for name, value in scope["headers"] if name.lower() == b"content-length"]
        if lengths:
            if len(lengths) != 1 or not lengths[0].isdigit():
                await reject(400, "invalid_content_length", "invalid Content-Length")
                return
            if len(lengths[0]) > 20 or int(lengths[0]) > limit:
                await reject(413, "payload_too_large", "request body exceeds the allowed size")
                return
        if upload:
            # These two raw-file routes authenticate before reading and enforce
            # the 10 MiB bound in TenantKnowledgeService.upload for every chunk,
            # including requests without Content-Length. Do not spool anonymous
            # files here or raise their allowance on unrelated JSON endpoints.
            await self.app(scope, receive, send)
            return
        payload = bytearray()
        total = 0
        try:
            async with asyncio.timeout(30):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    total += len(message.get("body", b""))
                    if total > limit:
                        await reject(413, "payload_too_large",
                                     "request body exceeds the allowed size")
                        return
                    # Coalesce tiny/empty chunks to keep container overhead
                    # bounded independently of how the sender frames its body.
                    payload.extend(message.get("body", b""))
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            await reject(408, "request_timeout", "request body timed out")
            return

        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {"type": "http.request", "body": bytes(payload), "more_body": False}

        await self.app(scope, replay, send)
