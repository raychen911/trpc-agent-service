"""Secret-reference resolution with a deliberately non-disclosing contract."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

_ENV_REFERENCE = re.compile(r"^env:TRPC_[A-Z0-9_]+$")
_FIXED_ERROR = "secret reference could not be resolved"


class SecretResolutionError(ValueError):
    """A reference was malformed or unavailable without exposing its details."""


class EnvSecretResolver:
    """Resolve the small, auditable ``env:TRPC_*`` secret-reference subset.

    Environment variable names and values are both sensitive operational
    details.  Every failure has one fixed message so callers can safely log
    it without accidentally exposing either.
    """

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = os.environ if environ is None else environ

    def resolve(self, secret_ref: str) -> str:
        """Return a non-empty environment secret for a valid reference."""
        try:
            if not isinstance(secret_ref, str) or _ENV_REFERENCE.fullmatch(secret_ref) is None:
                raise ValueError
            value = self._environ.get(secret_ref.removeprefix("env:"))
            if not isinstance(value, str) or not value:
                raise ValueError
            return value
        except (AttributeError, TypeError, ValueError):
            raise SecretResolutionError(_FIXED_ERROR) from None


__all__ = ["EnvSecretResolver", "SecretResolutionError"]
