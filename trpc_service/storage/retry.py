"""Deterministic bounded backoff shared by durable recovery queues."""

from __future__ import annotations

import os
from hashlib import sha256


def retry_delay_seconds(
    attempt: int,
    *,
    identity: str,
    base_env: str,
    cap_env: str,
    default_base: float = 5.0,
    default_cap: float = 300.0,
) -> float:
    """Return exponential backoff with stable per-record jitter."""

    if isinstance(attempt, bool) or int(attempt) < 1:
        raise ValueError("retry attempt must be a positive integer")
    base = _positive_env(base_env, default_base)
    cap = _positive_env(cap_env, default_cap)
    if cap < base:
        raise ValueError(f"{cap_env} must be greater than or equal to {base_env}")
    exponent = min(int(attempt) - 1, 30)
    raw = min(cap, base * (2**exponent))
    digest = sha256(f"{identity}:{attempt}".encode()).digest()
    jitter = 0.8 + (int.from_bytes(digest[:2], "big") / 65535) * 0.4
    return min(cap, max(0.001, raw * jitter))


def _positive_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


__all__ = ["retry_delay_seconds"]
