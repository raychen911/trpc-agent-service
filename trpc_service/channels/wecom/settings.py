"""WeCom AI Bot configuration parsed from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WeComSettings:
    """Immutable WeCom configuration.

    The authenticated bot account and its secret contain no tenant authority.
    A persisted ``ChannelBinding`` assigns the tenant/application.
    """

    bot_id: str
    secret: str

    def __repr__(self) -> str:
        return "WeComSettings(bot_id=***, secret=***)"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> WeComSettings | None:
        """Parse WeCom settings from environment variables.

        When *environ* is ``None`` the real ``os.environ`` is used.
        Returns ``None`` when bot credentials are absent (feature disabled).
        Legacy ``TRPC_WECOM_TENANT_ID`` is ignored; tenant authority never
        comes from process environment.
        """
        if environ is None:
            environ = os.environ

        bot_id = environ.get("TRPC_WECOM_BOT_ID", "").strip()
        secret = environ.get("TRPC_WECOM_BOT_SECRET", "").strip()
        any_set = bool(bot_id or secret)
        if not any_set:
            return None

        missing = []
        if not bot_id:
            missing.append("TRPC_WECOM_BOT_ID")
        if not secret:
            missing.append("TRPC_WECOM_BOT_SECRET")
        if missing:
            raise ValueError(f"WeCom configuration incomplete: missing {', '.join(missing)}.")

        return cls(bot_id=bot_id, secret=secret)


__all__ = ["WeComSettings"]
