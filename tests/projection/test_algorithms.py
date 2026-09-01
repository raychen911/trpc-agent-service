# mypy: disable-error-code="import-untyped"
"""Executable deterministic projection algorithm contracts."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.projection import (
    EncryptedProjectionTextReader,
    ExplicitInstructionMemoryExtractor,
    ExtractiveWindowSummary,
)
from trpc_service.reliability import ProjectionEvent, ProjectionInput
from trpc_service.security import EnvelopeCipher
from trpc_service.worker import EnvelopeEventCodec, EventCodecContext


class MemoryObjectStore:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        del ciphertext_sha256
        key = (tenant_id, object_key)
        existing = self.values.setdefault(key, ciphertext)
        if existing != ciphertext:
            raise ValueError("content address collision")

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        return self.values[(tenant_id, object_key)]


async def _input() -> tuple[ProjectionInput, EnvelopeEventCodec]:
    codec = EnvelopeEventCodec(
        cipher=EnvelopeCipher(b"p" * 32),
        store=MemoryObjectStore(),
    )
    raw = (
        (
            "event-user",
            Event(
                author="user",
                content=Content(
                    role="user",
                    parts=[Part.from_text(text="请记住:我的报表时区是 Asia/Shanghai")],
                ),
            ),
        ),
        (
            "event-agent",
            Event(
                author="assistant",
                content=Content(
                    role="model",
                    parts=[
                        Part(text="内部推理", thought=True),
                        Part.from_text(text="好的, 我会按该时区生成报表。"),
                    ],
                ),
            ),
        ),
    )
    events: list[ProjectionEvent] = []
    for seq, (event_id, event) in enumerate(raw, start=1):
        reference = await codec.seal(
            event,
            context=EventCodecContext("tenant-a", "session-a", event_id, seq),
        )
        events.append(
            ProjectionEvent(
                seq=seq,
                event_id=event_id,
                event_type="message",
                role="user" if seq == 1 else "assistant",
                content_ref=reference,
                payload={},
                state_delta={},
                created_at=datetime(2026, 8, 27, tzinfo=UTC),
            )
        )
    return (
        ProjectionInput(
            tenant_id="tenant-a",
            job_id="job-a",
            run_id="run-a",
            session_id="session-a",
            principal_id="principal-a",
            config_revision=1,
            through_seq=2,
            state_version=2,
            events=tuple(events),
        ),
        codec,
    )


@pytest.mark.asyncio
async def test_extractive_summary_reads_encrypted_events_without_thought_text() -> None:
    projection_input, codec = await _input()
    summary = await ExtractiveWindowSummary(EncryptedProjectionTextReader(codec)).summarize(
        projection_input
    )

    assert summary is not None
    assert "Asia/Shanghai" in summary
    assert "好的" in summary
    assert "内部推理" not in summary


@pytest.mark.asyncio
async def test_memory_extractor_only_accepts_explicit_user_instruction() -> None:
    projection_input, codec = await _input()
    memories = await ExplicitInstructionMemoryExtractor(
        EncryptedProjectionTextReader(codec)
    ).extract(projection_input)

    assert len(memories) == 1
    assert memories[0].source_event_id == "event-user"
    assert memories[0].content == "我的报表时区是 Asia/Shanghai"
    assert memories[0].metadata["source"] == "explicit_user_instruction"
