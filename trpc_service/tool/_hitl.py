# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Human-in-the-loop (HITL) confirmation registry for dangerous tools.

When a tenant marks a tool as ``dangerous_tools``, the governance filter asks
the :class:`ConfirmationManager` to mint a short-lived token. The user echoes
the token back (typically via IM) to authorize the operation; the manager then
resolves and consumes the pending request.
"""

from __future__ import annotations

import re
import secrets
import time
import threading
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

DEFAULT_CONFIRMATION_TTL_SECONDS = 300.0

_CONFIRMATION_RE = re.compile(r"^(?:确认|confirm)\s+([A-Za-z0-9_\-]{10,})", re.IGNORECASE)


def parse_confirmation_token(text: Optional[str]) -> Optional[str]:
    """Extract a confirmation token from a user message like ``确认 <token>``.

    Returns ``None`` when the message is not a confirmation reply.
    """
    if not text:
        return None
    match = _CONFIRMATION_RE.search(text.strip())
    return match.group(1) if match else None


class PendingConfirmation(BaseModel):
    """A single pending confirmation request."""

    model_config = {"extra": "forbid"}

    token: str
    tenant_id: str
    tool_name: str
    tool_args: dict[str, Any] = Field(default_factory=dict)
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    expires_at: float = Field(default_factory=lambda: time.time() + DEFAULT_CONFIRMATION_TTL_SECONDS)

    def is_expired(self, now: Optional[float] = None) -> bool:
        return (now or time.time()) >= self.expires_at


class ConfirmationManager:
    """Mints, stores and consumes short-lived confirmation tokens."""

    def __init__(self, ttl_seconds: float = DEFAULT_CONFIRMATION_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        self._pending: dict[str, PendingConfirmation] = {}
        self._lock = threading.Lock()

    def request(
        self,
        tenant_id: str,
        tool_name: str,
        tool_args: Optional[dict[str, Any]] = None,
        *,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> PendingConfirmation:
        """Create a pending confirmation and return it (with its token)."""
        token = secrets.token_urlsafe(16)
        pending = PendingConfirmation(
            token=token,
            tenant_id=tenant_id,
            tool_name=tool_name,
            tool_args=tool_args or {},
            user_id=user_id,
            session_id=session_id,
            expires_at=time.time() + self._ttl,
        )
        with self._lock:
            self._pending[token] = pending
        return pending

    def get(self, token: str) -> Optional[PendingConfirmation]:
        """Return the pending confirmation for ``token``, or ``None`` if absent/expired."""
        with self._lock:
            pending = self._pending.get(token)
        if pending is None:
            return None
        if pending.is_expired():
            with self._lock:
                self._pending.pop(token, None)
            return None
        return pending

    def resolve(self, token: str, approve: bool) -> Optional[PendingConfirmation]:
        """Consume a token and return the pending confirmation.

        Returns ``None`` when the token is unknown or already expired; the
        confirmation is removed either way so a token can only be used once.
        """
        with self._lock:
            pending = self._pending.pop(token, None)
        if pending is None or pending.is_expired():
            return None
        return pending
