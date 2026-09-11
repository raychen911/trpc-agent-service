"""RED tests for internal Token loading and constant-time comparison."""

from __future__ import annotations

import pytest

from trpc_service.transport.auth import InternalToken


def _valid_token_str() -> str:
    return "a" * 48


class TestInternalTokenFromEnv:

    def test_loads_from_explicit_environ(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert token.header_value() == _valid_token_str()

    def test_loads_from_process_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TRPC_INTERNAL_TOKEN", _valid_token_str())

        token = InternalToken.from_env()

        assert token.header_value() == _valid_token_str()

    def test_strips_whitespace(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": f"  {_valid_token_str()}  "})
        assert token.header_value() == _valid_token_str()

    def test_rejects_missing_variable(self) -> None:
        with pytest.raises(Exception) as exc_info:
            InternalToken.from_env({})
        msg = str(exc_info.value)
        assert _valid_token_str() not in msg
        assert "TRPC_INTERNAL_TOKEN" in msg or "token" in msg.lower()

    def test_rejects_blank_variable(self) -> None:
        with pytest.raises(Exception):
            InternalToken.from_env({"TRPC_INTERNAL_TOKEN": "   "})

    def test_rejects_short_variable(self) -> None:
        with pytest.raises(Exception):
            InternalToken.from_env({"TRPC_INTERNAL_TOKEN": "too_short"})

    def test_rejects_exactly_31_chars(self) -> None:
        with pytest.raises(Exception):
            InternalToken.from_env({"TRPC_INTERNAL_TOKEN": "x" * 31})

    def test_accepts_exactly_32_chars(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": "x" * 32})
        assert len(token.header_value()) == 32

    def test_error_does_not_contain_token_value(self) -> None:
        short = "short_value"
        with pytest.raises(Exception) as exc_info:
            InternalToken.from_env({"TRPC_INTERNAL_TOKEN": short})
        assert short not in str(exc_info.value)

    def test_repr_does_not_contain_token_value(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert _valid_token_str() not in repr(token)


class TestInternalTokenMatches:

    def test_correct_candidate(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert token.matches(_valid_token_str()) is True

    def test_incorrect_candidate(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert token.matches("wrong" * 10) is False

    def test_none_candidate(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert token.matches(None) is False

    def test_empty_candidate(self) -> None:
        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        assert token.matches("") is False

    def test_uses_compare_digest(self) -> None:
        import secrets

        token = InternalToken.from_env({"TRPC_INTERNAL_TOKEN": _valid_token_str()})
        original = secrets.compare_digest
        calls = []

        def tracking_compare_digest(a, b):
            calls.append((a, b))
            return original(a, b)

        secrets.compare_digest = tracking_compare_digest
        try:
            token.matches(_valid_token_str())
        finally:
            secrets.compare_digest = original
        assert len(calls) == 1
