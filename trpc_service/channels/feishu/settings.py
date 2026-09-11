"""Feishu AI Bot configuration parsed from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeishuSettings:
    """Immutable Feishu configuration.

    It contains SDK credentials only.  Tenant authority comes exclusively from
    a persisted ``ChannelBinding`` in the service layer.
    """

    app_id: str
    app_secret: str

    def __repr__(self) -> str:
        return "FeishuSettings(app_id=***, app_secret=***)"

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> FeishuSettings | None:
        """Parse Feishu settings from environment variables.

        When *environ* is ``None`` the real ``os.environ`` is used.
        Returns ``None`` when both variables are absent (feature disabled).
        Raises ``ValueError`` when only some are set or values are invalid.
        """
        if environ is None:
            environ = os.environ

        app_id = environ.get("TRPC_FEISHU_APP_ID", "").strip()
        app_secret = environ.get("TRPC_FEISHU_APP_SECRET", "").strip()
        any_set = bool(app_id or app_secret)
        if not any_set:
            return None

        missing = []
        if not app_id:
            missing.append("TRPC_FEISHU_APP_ID")
        if not app_secret:
            missing.append("TRPC_FEISHU_APP_SECRET")
        if missing:
            raise ValueError(f"Feishu configuration incomplete: missing {', '.join(missing)}.")

        return cls(app_id=app_id, app_secret=app_secret)


__all__ = ["FeishuSettings"]
