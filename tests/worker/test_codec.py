# mypy: disable-error-code="import-untyped"
"""Authenticated encryption and context-binding tests for full SDK events."""

from __future__ import annotations

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.security import EnvelopeCipher
from trpc_service.worker import (
    EnvelopeEventCodec,
    EventCodecContext,
    EventCodecError,
)


class MemoryObjectStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        del tenant_id, ciphertext_sha256
        existing = self.values.setdefault(object_key, ciphertext)
        if existing != ciphertext:
            raise ValueError("content address collision")

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        del tenant_id
        return self.values[object_key]


@pytest.mark.asyncio
async def test_envelope_codec_round_trip_has_no_plaintext_reference() -> None:
    store = MemoryObjectStore()
    codec = EnvelopeEventCodec(
        cipher=EnvelopeCipher(b"k" * 32),
        store=store,
    )
    context = EventCodecContext(
        tenant_id="tenant-a",
        session_id="session-a",
        event_id="run-a:attempt:1:event:1",
        seq=1,
    )
    event = Event(
        id="sdk-a",
        author="user",
        content=Content(role="user", parts=[Part.from_text(text="private prompt")]),
    )
    reference = await codec.seal(event, context=context)
    assert reference.startswith("evt+enc://v1/")
    assert "private" not in reference
    assert all("private prompt" not in value for value in store.values.values())
    restored = await codec.open(reference, context=context)
    assert restored.get_text() == "private prompt"


@pytest.mark.asyncio
async def test_envelope_codec_rejects_cross_session_replay_and_tamper() -> None:
    store = MemoryObjectStore()
    codec = EnvelopeEventCodec(
        cipher=EnvelopeCipher(b"k" * 32),
        store=store,
    )
    context = EventCodecContext("tenant-a", "session-a", "event-a", 1)
    reference = await codec.seal(Event(author="user"), context=context)
    with pytest.raises(EventCodecError, match="context"):
        await codec.open(
            reference,
            context=EventCodecContext("tenant-a", "session-b", "event-a", 1),
        )

    object_key = next(iter(store.values))
    store.values[object_key] += "tampered"
    with pytest.raises(EventCodecError, match="digest"):
        await codec.open(reference, context=context)


@pytest.mark.asyncio
async def test_codec_does_not_chain_secret_bearing_store_errors() -> None:
    class UnsafeFailingStore(MemoryObjectStore):
        async def put_if_absent(
            self,
            object_key: str,
            ciphertext: str,
            *,
            tenant_id: str,
            ciphertext_sha256: str,
        ) -> None:
            del object_key, tenant_id, ciphertext_sha256
            raise ValueError(f"unsafe backend echoed {ciphertext}")

    codec = EnvelopeEventCodec(
        cipher=EnvelopeCipher(b"k" * 32),
        store=UnsafeFailingStore(),
    )
    with pytest.raises(EventCodecError) as captured:
        await codec.seal(
            Event(
                author="user",
                content=Content(
                    role="user",
                    parts=[Part.from_text(text="must-never-escape")],
                ),
            ),
            context=EventCodecContext("tenant-a", "session-a", "event-a", 1),
        )
    assert captured.value.__cause__ is None
    assert "must-never-escape" not in str(captured.value)


@pytest.mark.asyncio
async def test_codec_rejects_partial_malformed_missing_and_wrong_key_content() -> None:
    context = EventCodecContext("tenant-a", "session-a", "event-a", 1)
    store = MemoryObjectStore()
    codec = EnvelopeEventCodec(cipher=EnvelopeCipher(b"a" * 32), store=store)
    with pytest.raises(EventCodecError, match="partial"):
        await codec.seal(Event(author="user", partial=True), context=context)
    with pytest.raises(EventCodecError, match="malformed"):
        await codec.open("not-an-encrypted-reference", context=context)
    with pytest.raises(EventCodecError, match="context"):
        await codec.seal(
            Event(author="user"),
            context=EventCodecContext("tenant-a", "session-a", "event-a", 0),
        )

    reference = await codec.seal(Event(author="user"), context=context)
    unavailable = EnvelopeEventCodec(
        cipher=EnvelopeCipher(b"a" * 32),
        store=MemoryObjectStore(),
    )
    with pytest.raises(EventCodecError, match="unavailable"):
        await unavailable.open(reference, context=context)
    wrong_key = EnvelopeEventCodec(cipher=EnvelopeCipher(b"b" * 32), store=store)
    with pytest.raises(EventCodecError, match="authentication"):
        await wrong_key.open(reference, context=context)
