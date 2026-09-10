"""Deterministic, safe representations for phase-seven data.

The canonical form is deliberately small: JSON objects are encoded with stable
key ordering, compact separators and UTF-8 (without ASCII escaping).  Callers
store the resulting digest rather than the original representation in audit or
diagnostic surfaces.
"""

from __future__ import annotations

import json
import math
from hashlib import sha256
from typing import Any


class CanonicalizationError(ValueError):
    """Raised when a value cannot be safely represented as canonical JSON."""


def _reject_non_finite(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        raise CanonicalizationError("non-finite numeric value is not allowed")
    if isinstance(value, dict):
        return {str(key): _reject_non_finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_reject_non_finite(item) for item in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    # Datetime/UUID/bytes and arbitrary objects must be converted by the
    # domain layer explicitly; silently stringifying them makes digests unsafe.
    raise CanonicalizationError("value is not canonical JSON")


def canonical_json(value: Any) -> str:
    """Return a deterministic JSON string or raise a safe validation error."""

    try:
        normalized = _reject_non_finite(value)
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        if isinstance(exc, CanonicalizationError):
            raise
        raise CanonicalizationError("value is not canonical JSON") from None


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def content_digest(value: Any) -> str:
    """Return a lower-case SHA-256 digest of canonical JSON or supplied text."""

    if isinstance(value, str):
        payload = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray)):
        payload = bytes(value)
    else:
        payload = canonical_bytes(value)
    return sha256(payload).hexdigest()


def safe_size_bytes(value: Any) -> int:
    return len(canonical_bytes(value))


__all__ = ["CanonicalizationError", "canonical_json", "canonical_bytes", "content_digest", "safe_size_bytes"]
