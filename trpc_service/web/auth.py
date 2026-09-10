"""API-Key 鉴权（Admin API 与 chat 端点）。"""
from __future__ import annotations

import os
import secrets

from fastapi import Header, HTTPException


def _verify(provided: str, env_name: str) -> None:
    expected = os.getenv(env_name, "")
    if not expected:
        return  # 未配置密钥 = 开发模式放行
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="invalid or missing API key")


async def require_admin_key(x_api_key: str = Header(default="")) -> None:
    """Admin API 鉴权依赖（tenants CRUD / audit / metrics）。"""
    _verify(x_api_key, "ADMIN_API_KEY")


async def require_chat_key(x_api_key: str = Header(default="")) -> None:
    """chat 端点鉴权依赖（CHAT_API_KEY 配置后生效）。"""
    _verify(x_api_key, "CHAT_API_KEY")
