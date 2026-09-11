"""Immutable tenant configuration rollout domain and deterministic selector."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TenantConfigRollout:
    tenant_id: str
    active_version: int
    candidate_version: int
    candidate_percent: int
    started_at: datetime
    status: str = "running"

    def __post_init__(self) -> None:
        if self.active_version < 1 or self.candidate_version < 1:
            raise ValueError("rollout version must be positive")
        if self.active_version == self.candidate_version:
            raise ValueError("rollout versions must differ")
        if type(self.candidate_percent) is not int or not 1 <= self.candidate_percent <= 99:
            raise ValueError("candidate_percent must be between 1 and 99")
        if self.status != "running":
            raise ValueError("rollout status must be running")


def select_config_version(
    tenant_id: str,
    channel: str,
    message_id: str,
    *,
    active_version: int,
    candidate_version: int,
    candidate_percent: int,
) -> int:
    """Choose one immutable config snapshot without mutable process state.

    The digest includes each boundary as length-prefixed UTF-8 so tuples
    cannot collide through separator content.  The first eight bytes are
    sufficient and map uniformly into the documented 0..99 bucket.
    """
    if not 1 <= candidate_percent <= 99:
        raise ValueError("candidate_percent must be between 1 and 99")
    pieces = (tenant_id, channel, message_id)
    payload = b"".join(len(piece.encode("utf-8")).to_bytes(4, "big") + piece.encode("utf-8") for piece in pieces)
    bucket = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % 100
    return candidate_version if bucket < candidate_percent else active_version


__all__ = ["TenantConfigRollout", "select_config_version"]
