"""Admin Token loading and constant-time comparison."""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from typing import Final

_MIN_TOKEN_LENGTH: Final[int] = 32
_ENV_VAR: Final[str] = "TRPC_ADMIN_TOKEN"


class AdminToken:
    """Holds the Admin API authentication token.

    Never reuse TRPC_INTERNAL_TOKEN: the Admin plane is a separate trust
    domain from the Gateway ↔ Worker channel.
    """

    def __init__(self, value: str) -> None:
        self._value = value

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "AdminToken":
        if environ is None:
            import os
            environ = os.environ
        raw = environ.get(_ENV_VAR, "")
        stripped = raw.strip()
        if len(stripped) < _MIN_TOKEN_LENGTH:
            raise ValueError(f"{_ENV_VAR} must be at least {_MIN_TOKEN_LENGTH} characters"
                             " after stripping whitespace")
        return cls(stripped)

    def header_value(self) -> str:
        return self._value

    def matches(self, candidate: str | None) -> bool:
        if candidate is None:
            return False
        return secrets.compare_digest(self._value, candidate)

    def __repr__(self) -> str:
        return "AdminToken(***)"


__all__ = ["AdminToken"]
