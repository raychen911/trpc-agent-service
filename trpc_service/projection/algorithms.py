# mypy: disable-error-code="import-untyped"
"""Deterministic, privacy-conscious baseline projection algorithms.

These algorithms make the durable projector executable without introducing a
second model call or silently inventing user memories.  Deployments can inject a
versioned semantic summarizer/extractor behind the same contracts later.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from trpc_agent_sdk.events import Event

from trpc_service.reliability import ProjectionInput
from trpc_service.worker import EnvelopeEventCodec, EventCodecContext

from .contracts import MemoryCandidate

_WHITESPACE = re.compile(r"\s+")
_MEMORY_PREFIXES = (
    "remember:",
    "remember\uff1a",
    "请记住:",
    "请记住\uff1a",
    "记住:",
    "记住\uff1a",
)


@dataclass(frozen=True, slots=True)
class ProjectionText:
    """Minimal decrypted text view used only during one projection attempt."""

    event_id: str
    seq: int
    role: str
    text: str


class EncryptedProjectionTextReader:
    """Open authenticated SDK event objects at their exact canonical context."""

    def __init__(self, codec: EnvelopeEventCodec) -> None:
        self._codec = codec

    async def read(self, projection_input: ProjectionInput) -> tuple[ProjectionText, ...]:
        values: list[ProjectionText] = []
        for item in projection_input.events:
            if item.content_ref is None:
                continue
            event = await self._codec.open(
                item.content_ref,
                context=EventCodecContext(
                    tenant_id=projection_input.tenant_id,
                    session_id=projection_input.session_id,
                    event_id=item.event_id,
                    seq=item.seq,
                ),
            )
            text = _visible_text(event)
            if not text:
                continue
            values.append(
                ProjectionText(
                    event_id=item.event_id,
                    seq=item.seq,
                    role=item.role or _event_role(event),
                    text=text,
                )
            )
        return tuple(values)


class ExtractiveWindowSummary:
    """Bounded replay summary that never adds claims absent from committed events."""

    version = "extractive-window-v1"

    def __init__(
        self,
        reader: EncryptedProjectionTextReader,
        *,
        max_events: int = 24,
        max_chars: int = 6_000,
        max_event_chars: int = 1_000,
    ) -> None:
        if max_events < 1 or max_chars < 128 or max_event_chars < 32:
            raise ValueError("extractive summary limits are invalid")
        self._reader = reader
        self._max_events = max_events
        self._max_chars = max_chars
        self._max_event_chars = max_event_chars

    async def summarize(self, projection_input: ProjectionInput) -> str | None:
        texts = await self._reader.read(projection_input)
        if not texts:
            return None
        lines = [
            f"[{item.role}] {_bounded(item.text, self._max_event_chars)}"
            for item in texts[-self._max_events :]
        ]
        while lines and len("\n".join(lines)) > self._max_chars:
            lines.pop(0)
        if not lines:
            last = texts[-1]
            return f"[{last.role}] {_bounded(last.text, self._max_chars - len(last.role) - 3)}"
        return "\n".join(lines)


class ExplicitInstructionMemoryExtractor:
    """Persist only user text explicitly introduced as a memory instruction."""

    version = "explicit-instruction-v1"

    def __init__(
        self,
        reader: EncryptedProjectionTextReader,
        *,
        max_memory_chars: int = 2_000,
    ) -> None:
        if max_memory_chars < 32:
            raise ValueError("max_memory_chars must be at least 32")
        self._reader = reader
        self._max_memory_chars = max_memory_chars

    async def extract(self, projection_input: ProjectionInput) -> tuple[MemoryCandidate, ...]:
        candidates: list[MemoryCandidate] = []
        for item in await self._reader.read(projection_input):
            if item.role != "user":
                continue
            content = _explicit_memory(item.text)
            if content is None:
                continue
            candidates.append(
                MemoryCandidate(
                    source_event_id=item.event_id,
                    content=_bounded(content, self._max_memory_chars),
                    metadata={
                        "source": "explicit_user_instruction",
                        "source_seq": item.seq,
                    },
                )
            )
        return tuple(candidates)


def _visible_text(event: Event) -> str:
    if event.content is None:
        return ""
    text = "".join(
        part.text
        for part in event.content.parts
        if part.text and not bool(getattr(part, "thought", False))
    )
    return _WHITESPACE.sub(" ", text).strip()


def _event_role(event: Event) -> str:
    if event.content is None or not event.content.role:
        return "unknown"
    return "assistant" if event.content.role == "model" else event.content.role


def _explicit_memory(text: str) -> str | None:
    folded = text.casefold()
    for prefix in _MEMORY_PREFIXES:
        if folded.startswith(prefix.casefold()):
            content = text[len(prefix) :].strip()
            return content or None
    return None


def _bounded(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(1, limit - 1)].rstrip() + "…"
