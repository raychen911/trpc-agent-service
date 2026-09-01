"""Secret-reference and authenticated-envelope tests."""

from __future__ import annotations

import pytest

from trpc_service.security import (
    EnvelopeCipher,
    EnvironmentSecretResolver,
    InvalidCiphertextError,
    SecretResolutionError,
)


def test_envelope_round_trip_is_bound_to_context() -> None:
    cipher = EnvelopeCipher("a" * 32)
    context = {"tenant_id": "t-1", "binding_id": "b-1", "kind": "response_url"}

    envelope = cipher.encrypt("https://example.test/reply?key=secret", context=context)

    assert "example.test" not in envelope
    assert cipher.decrypt(envelope, context=context).get_secret_value().startswith("https://")
    with pytest.raises(InvalidCiphertextError):
        cipher.decrypt(envelope, context={**context, "tenant_id": "t-2"})


def test_tampered_envelope_is_rejected() -> None:
    cipher = EnvelopeCipher("b" * 32)
    context = {"tenant_id": "t-1"}
    envelope = cipher.encrypt("one-time-value", context=context)
    replacement = "A" if envelope[-1] != "A" else "B"

    with pytest.raises(InvalidCiphertextError):
        cipher.decrypt(envelope[:-1] + replacement, context=context)


def test_environment_resolver_requires_explicit_allowlist(monkeypatch) -> None:
    monkeypatch.setenv("TEST_ALLOWED_SECRET", "resolved-value")
    monkeypatch.setenv("TEST_BLOCKED_SECRET", "must-not-be-readable")
    resolver = EnvironmentSecretResolver({"TEST_ALLOWED_SECRET"})

    value = resolver.resolve("secret://env/TEST_ALLOWED_SECRET")
    assert value.get_secret_value() == "resolved-value"
    with pytest.raises(SecretResolutionError):
        resolver.resolve("secret://env/TEST_BLOCKED_SECRET")
