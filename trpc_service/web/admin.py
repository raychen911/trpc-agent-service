"""Admin authentication, RBAC and tenant payload conversion."""

from __future__ import annotations

import hmac
import re
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from fastapi import HTTPException, Request

from ..tenant import AgentApp, AuditPolicy, ChannelBinding, StorageProfile, TenantConfig, TenantPolicy


def validate_secret_references(config: TenantConfig, *, production: bool) -> None:
    """Reject development-only secret references before a revision is activated."""
    if not production:
        return
    for binding in config.channels.values():
        for field in ("verify_token", "secret_ref", "encrypt_key_ref"):
            value = getattr(binding, field)
            if value and not value.startswith(("env://", "file://")):
                raise HTTPException(
                    status_code=422,
                    detail=f"production secret {binding.channel}.{field} must use env:// or file://",
                )
    for app in config.apps.values():
        for field in ("model_api_key_ref", "fallback_api_key_ref"):
            value = getattr(app, field)
            if value and not value.startswith(("env://", "file://")):
                raise HTTPException(
                    status_code=422,
                    detail=f"production secret app.{app.app_id}.{field} must use env:// or file://",
                )
        for field in ("model_base_url", "fallback_base_url"):
            value = getattr(app, field)
            if value and not value.startswith("https://"):
                raise HTTPException(
                    status_code=422,
                    detail=f"production endpoint app.{app.app_id}.{field} must use HTTPS",
                )


def require_role(
    request: Request,
    token: str | None,
    roles: set[str],
    role_tokens: Mapping[str, str] | None = None,
) -> str:
    configured = dict(role_tokens or ({"admin": token} if token else {}))
    if not configured:
        return "admin"
    supplied = request.headers.get("x-admin-token", "")
    matched = next(
        (role for role, expected in configured.items() if supplied and hmac.compare_digest(supplied, expected)),
        None,
    )
    if matched is None:
        raise HTTPException(status_code=403, detail="admin authentication required")
    if matched not in roles:
        raise HTTPException(status_code=403, detail="admin role is not authorized")
    return matched


def etag(config: TenantConfig) -> str:
    return f'"tenant-{config.tenant_id}-v{config.version}"'


def expected_version(request: Request) -> int:
    value = request.headers.get("if-match", "").strip('"')
    if "-v" not in value:
        raise HTTPException(status_code=428, detail="If-Match tenant ETag is required")
    try:
        return int(value.rsplit("-v", 1)[1])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid If-Match ETag") from exc


def public_config(config: TenantConfig) -> dict[str, Any]:
    value = asdict(config)
    for binding in value["channels"].values():
        for field in ("verify_token", "secret_ref", "encrypt_key_ref"):
            if binding.get(field):
                binding[field] = "[SECRET_REF]"
    for field in ("allowed_tools", "dangerous_tools"):
        value["policy"][field] = sorted(value["policy"][field])
    for app in value["apps"].values():
        for field in ("model_api_key_ref", "fallback_api_key_ref"):
            if app.get(field):
                app[field] = "[SECRET_REF]"
        app["tools"] = sorted(app["tools"])
        app["knowledge_collections"] = sorted(app["knowledge_collections"])
    return value


def parse_config(
    payload: Mapping[str, Any], *, version: int = 1, existing: TenantConfig | None = None
) -> TenantConfig:
    try:
        tenant_id = str(payload["tenant_id"])
        apps = {}
        for key, value in dict(payload["apps"]).items():
            app_data = dict(value)
            for field in ("tools", "knowledge_collections"):
                if field in app_data:
                    app_data[field] = frozenset(app_data[field])
            old = existing.apps.get(str(key)) if existing is not None else None
            if old is not None:
                for field in ("model_api_key_ref", "fallback_api_key_ref"):
                    if app_data.get(field) == "[SECRET_REF]":
                        app_data[field] = getattr(old, field)
            apps[str(key)] = AgentApp(**app_data)
        channels = {}
        for key, value in dict(payload["channels"]).items():
            channel_data = dict(value)
            if "allowed_users" in channel_data:
                channel_data["allowed_users"] = frozenset(channel_data["allowed_users"])
            old = existing.channels.get(str(key)) if existing is not None else None
            if old is not None:
                for field in ("verify_token", "secret_ref", "encrypt_key_ref"):
                    if channel_data.get(field) == "[SECRET_REF]":
                        channel_data[field] = getattr(old, field)
            channels[str(key)] = ChannelBinding(**channel_data)
        policy_data = dict(payload.get("policy") or {})
        for key in ("allowed_tools", "dangerous_tools"):
            if key in policy_data:
                policy_data[key] = frozenset(policy_data[key])
        storage_data = dict(payload.get("storage") or {})
        audit_data = dict(payload.get("audit") or {})
        config = TenantConfig(
            tenant_id=tenant_id,
            name=str(payload["name"]),
            apps=apps,
            channels=channels,
            policy=TenantPolicy(**policy_data),
            audit=AuditPolicy(**audit_data),
            storage=StorageProfile(**storage_data),
            version=version,
            status=str(payload.get("status", "active")),
        )
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", tenant_id) or not config.name or not config.apps:
            raise ValueError("tenant identity and at least one app are required")
        if any(key != app.app_id or not app.name for key, app in config.apps.items()):
            raise ValueError("app keys must match non-empty app_id values")
        if any(key != binding.channel or not binding.account_id for key, binding in config.channels.items()):
            raise ValueError("channel keys must match bindings with non-empty account IDs")
        if not config.policy.dangerous_tools <= config.policy.allowed_tools:
            raise ValueError("dangerous tools must be included in allowed_tools")
        if any(not app.tools <= config.policy.allowed_tools for app in config.apps.values()):
            raise ValueError("app tools must be included in the tenant allowlist")
        if any(not name or len(name) > 128 for app in config.apps.values() for name in app.knowledge_collections):
            raise ValueError("knowledge collection names must be non-empty and at most 128 characters")
        if any(app.knowledge_collections for app in config.apps.values()) and config.storage.vector == "none":
            raise ValueError("knowledge collections require a vector storage backend")
        if any(
            app.input_cost_per_million < 0
            or app.output_cost_per_million < 0
            or (app.timeout_seconds is not None and app.timeout_seconds <= 0)
            for app in config.apps.values()
        ):
            raise ValueError("model token prices must not be negative and timeout must be positive")
        if (
            config.policy.max_input_chars <= 0
            or config.policy.daily_token_budget <= 0
            or config.policy.requests_per_minute <= 0
        ):
            raise ValueError("tenant limits must be positive")
        if config.audit.retention_days < 1 or config.audit.retention_days > 3650:
            raise ValueError("audit retention_days must be between 1 and 3650")
        if config.status not in {"active", "disabled"}:
            raise ValueError("tenant status must be active or disabled")
        return config
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="invalid tenant configuration") from exc
