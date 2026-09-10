"""Canonical digests and contract-version ordering for operations payloads."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_payload_digest(payload: dict[str, Any]) -> str:
    """Stable sha256 digest over the canonical JSON form of a payload."""

    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def contract_rank(version: str) -> int:
    """Order contract versions like v1 < v2 < v10; unknown parts rank 0."""

    try:
        return int(str(version).lstrip("vV") or 0)
    except ValueError:
        return 0
