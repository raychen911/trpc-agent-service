"""Attachment materialization helpers for outbound IM media."""

from __future__ import annotations

import base64
import mimetypes
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterator
from urllib.request import Request, urlopen
from uuid import uuid4

from trpc_service.channels.base import Attachment


@dataclass(slots=True)
class PreparedAttachment:
    path: Path
    filename: str
    content_type: str


def attachment_kind(attachment: Attachment | None) -> str:
    if attachment is None:
        return "text"
    kind = str(attachment.kind or "").lower()
    content_type = str(attachment.content_type or "").lower()
    if kind in {"image", "photo"} or content_type.startswith("image/"):
        return "image"
    return "file"


@contextmanager
def prepare_attachment_file(
    attachment: Attachment | None,
    *,
    max_bytes: int = 10 * 1024 * 1024,
) -> Iterator[PreparedAttachment | None]:
    if attachment is None:
        yield None
        return

    raw: bytes | None = None
    source = attachment.metadata.get("content_base64")
    if source:
        raw = base64.b64decode(str(source))
    elif attachment.url:
        if str(attachment.url).startswith(("http://", "https://")):
            request = Request(
                attachment.url,
                headers={"User-Agent": "trpc-agent-service"},
                method="GET",
            )
            with urlopen(request, timeout=15) as response:
                raw = response.read(max_bytes + 1)
        else:
            path = Path(str(attachment.url).removeprefix("file://"))
            raw = path.read_bytes()
    if raw is None:
        yield None
        return
    if len(raw) > max_bytes:
        raise ValueError("attachment exceeds MAX_ATTACHMENT_BYTES")

    filename = attachment.name or f"attachment-{uuid4().hex}"
    suffix = Path(filename).suffix
    if not suffix and attachment.content_type:
        suffix = mimetypes.guess_extension(attachment.content_type) or ""

    temp_path: Path | None = None
    try:
        with NamedTemporaryFile(
            prefix="trpc-agent-attachment-",
            suffix=suffix or "",
            delete=False,
        ) as file_obj:
            file_obj.write(raw)
            temp_path = Path(file_obj.name)
        yield PreparedAttachment(
            path=temp_path,
            filename=filename,
            content_type=attachment.content_type or "application/octet-stream",
        )
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def post_multipart_json(
    url: str,
    *,
    fields: dict[str, str | int | float],
    file_field: str,
    file_path: Path,
    content_type: str | None = None,
) -> dict:
    import json

    boundary = f"trpc-agent-{uuid4().hex}"
    body = bytearray()

    def add_text(name: str, value: str) -> None:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"))
        body.extend(value.encode("utf-8"))
        body.extend(b"\r\n")

    for key, value in fields.items():
        add_text(str(key), str(value))

    file_bytes = file_path.read_bytes()
    mime = content_type or mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(
        (f'Content-Disposition: form-data; name="{file_field}"; ' f'filename="{file_path.name}"\r\n').encode("utf-8")
    )
    body.extend(f"Content-Type: {mime}\r\n\r\n".encode("utf-8"))
    body.extend(file_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    request = Request(
        url,
        data=bytes(body),
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))
