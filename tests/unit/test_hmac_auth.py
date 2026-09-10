from __future__ import annotations

from datetime import timedelta
import secrets

import pytest

from tests.support import FIXED_UTC
from trpc_service.channels import hmac_auth
from trpc_service.storage.contracts import Unauthorized


def test_v1_canonical_string_and_signature_use_exact_body_bytes() -> None:
    raw = b'{"external_message_id":"message-001","text":"hello"}'
    digest = hmac_auth.body_sha256(raw)
    canonical = hmac_auth.canonical_string("1788595200", "binding-alpha", "message-001", digest)
    runtime_key = secrets.token_bytes(32)
    signature = hmac_auth.sign_request(runtime_key, canonical)

    assert digest == __import__("hashlib").sha256(raw).hexdigest()
    assert canonical == f"v1\n1788595200\nbinding-alpha\nmessage-001\n{digest}"
    assert signature.startswith("v1=") and len(signature) == 67


@pytest.mark.parametrize("offset", [-300, 300])
def test_signature_accepts_exact_time_window_boundary(offset: int) -> None:
    runtime_key = secrets.token_bytes(32)
    raw = b'{"external_message_id":"message-001"}'
    timestamp = str(int((FIXED_UTC + timedelta(seconds=offset)).timestamp()))
    digest = hmac_auth.body_sha256(raw)
    signature = hmac_auth.sign_request(
        runtime_key,
        hmac_auth.canonical_string(timestamp, "binding-alpha", "message-001", digest),
    )
    scope = hmac_auth.verify_signature(
        binding_id="binding-alpha",
        timestamp=timestamp,
        signature=signature,
        external_message_id="message-001",
        raw_body=raw,
        secret=runtime_key,
        now=FIXED_UTC,
    )
    assert scope.binding_id == "binding-alpha"


@pytest.mark.parametrize(
    "timestamp,signature",
    [
        ("not-a-time", "v1=" + "a" * 64),
        (str(int(FIXED_UTC.timestamp()) + 301), "v1=" + "a" * 64),
        (str(int(FIXED_UTC.timestamp())), "v2=" + "a" * 64),
        (str(int(FIXED_UTC.timestamp())), "v1=bad"),
    ],
)
def test_invalid_auth_inputs_share_one_safe_error(timestamp: str, signature: str) -> None:
    with pytest.raises(Unauthorized) as caught:
        hmac_auth.verify_signature(
            binding_id="unknown-candidate",
            timestamp=timestamp,
            signature=signature,
            external_message_id="message-001",
            raw_body=b"{}",
            secret=secrets.token_bytes(32),
            now=FIXED_UTC,
        )
    assert str(caught.value) == "Request authentication failed."
    assert "unknown-candidate" not in str(caught.value)


async def test_malformed_public_auth_fields_are_rejected_before_registry_lookup() -> None:
    class Registry:
        def get_auth_material(self, *_args):
            raise AssertionError("registry must not be queried")

    with pytest.raises(Unauthorized):
        await hmac_auth.verify_request(
            binding_id="candidate", timestamp="not-a-time", signature="bad",
            external_message_id="message-001", raw_body=b"{}",
            registry=Registry(), resolver=object(), now=FIXED_UTC,
        )


def test_signed_body_tampering_is_rejected() -> None:
    runtime_key = secrets.token_bytes(32)
    original = b'{"external_message_id":"message-001","text":"original"}'
    timestamp = str(int(FIXED_UTC.timestamp()))
    signature = hmac_auth.sign_request(
        runtime_key,
        hmac_auth.canonical_string(timestamp, "binding-alpha", "message-001", hmac_auth.body_sha256(original)),
    )
    with pytest.raises(Unauthorized):
        hmac_auth.verify_signature(
            binding_id="binding-alpha", timestamp=timestamp, signature=signature,
            external_message_id="message-001",
            raw_body=b'{"external_message_id":"message-001","text":"tampered"}',
            secret=runtime_key, now=FIXED_UTC,
        )
