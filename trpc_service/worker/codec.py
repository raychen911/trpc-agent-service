# mypy: disable-error-code="import-untyped"
"""Authenticated SDK-event codec backed by an injected encrypted object store."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Protocol

from trpc_agent_sdk.events import Event

from trpc_service.security import EnvelopeCipher
from trpc_service.worker.contracts import EventCodecContext

_REFERENCE = re.compile(r"\Aevt\+enc://v1/([0-9a-f]{64})/([0-9a-f]{64})\Z")


class EventCodecError(RuntimeError):
    """Encrypted event content is unavailable, inconsistent, or unauthentic."""


class EventObjectStore(Protocol):
    """Minimal durable blob contract for encrypted SDK event envelopes."""

    async def put_if_absent(
        self,
        object_key: str,
        ciphertext: str,
        *,
        tenant_id: str,
        ciphertext_sha256: str,
    ) -> None:
        """Create a content-addressed object or verify an identical existing value."""

    async def get(self, object_key: str, *, tenant_id: str) -> str:
        """Read an encrypted envelope by opaque, non-secret object key."""


class EnvelopeEventCodec:
    """Seal complete SDK events with AES-GCM before external persistence.

    The reference contains only hashes.  Authenticated context binds an object to the
    tenant, session, canonical event id, and sequence, preventing cross-session replay.
    """

    def __init__(self, *, cipher: EnvelopeCipher, store: EventObjectStore) -> None:
        # The cipher must be provisioned with an event-dedicated root secret.  AAD
        # separates every tenant/session/event, while a distinct root key also
        # separates this data class from short-lived IM reply credentials.
        self._cipher = cipher
        self._store = store

    async def seal(self, event: Event, *, context: EventCodecContext) -> str:
        if event.partial:
            raise EventCodecError("partial SDK events cannot be sealed")
        aad = _encryption_context(context)
        try:
            serialized = event.model_dump_json(
                by_alias=True,
                exclude_none=False,
            )
            envelope = self._cipher.encrypt(serialized, context=aad)
        except Exception:
            raise EventCodecError("SDK event could not be sealed") from None
        context_hash = _context_hash(aad)
        ciphertext_hash = hashlib.sha256(envelope.encode()).hexdigest()
        object_key = f"sdk-events/v1/{context_hash}/{ciphertext_hash}"
        try:
            await self._store.put_if_absent(
                object_key,
                envelope,
                tenant_id=context.tenant_id,
                ciphertext_sha256=ciphertext_hash,
            )
        except Exception:
            raise EventCodecError("encrypted SDK event could not be stored") from None
        return f"evt+enc://v1/{context_hash}/{ciphertext_hash}"

    async def open(self, content_ref: str, *, context: EventCodecContext) -> Event:
        match = _REFERENCE.fullmatch(content_ref)
        if match is None:
            raise EventCodecError("SDK event reference is malformed")
        aad = _encryption_context(context)
        expected_context_hash = _context_hash(aad)
        context_hash, ciphertext_hash = match.groups()
        if context_hash != expected_context_hash:
            raise EventCodecError("SDK event reference context does not match")
        object_key = f"sdk-events/v1/{context_hash}/{ciphertext_hash}"
        try:
            envelope = await self._store.get(object_key, tenant_id=context.tenant_id)
        except Exception:
            raise EventCodecError("encrypted SDK event is unavailable") from None
        if hashlib.sha256(envelope.encode()).hexdigest() != ciphertext_hash:
            raise EventCodecError("encrypted SDK event digest does not match")
        try:
            plaintext = self._cipher.decrypt(envelope, context=aad).get_secret_value()
            event = Event.model_validate_json(plaintext)
        except Exception:
            raise EventCodecError("encrypted SDK event authentication failed") from None
        if event.partial:
            raise EventCodecError("partial SDK event appeared in durable storage")
        return event


def _encryption_context(context: EventCodecContext) -> dict[str, str]:
    if not context.tenant_id or not context.session_id or not context.event_id or context.seq < 1:
        raise EventCodecError("SDK event encryption context is invalid")
    return {
        "purpose": "sdk-event-v1",
        "tenant_id": context.tenant_id,
        "session_id": context.session_id,
        "event_id": context.event_id,
        "seq": str(context.seq),
    }


def _context_hash(context: dict[str, str]) -> str:
    canonical = json.dumps(
        context,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()
