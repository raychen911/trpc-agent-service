"""Admin operations independent from the HTTP framework."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from trpc_service.tenant.models import ChannelBinding, TenantConfig
from trpc_service.tenant.service import TenantService


class AdminService:
    def __init__(self, tenants: TenantService) -> None:
        self.tenants = tenants

    def create_tenant(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.tenants.create_tenant(TenantConfig.from_dict(payload)).to_public_dict()

    def get_tenant(self, tenant_id: str, version: int | None = None) -> dict[str, Any]:
        return self.tenants.get_tenant(tenant_id, version=version).to_public_dict()

    def update_config(
        self,
        tenant_id: str,
        payload: dict[str, Any],
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        self.tenants.check_expected_version(tenant_id, expected_version)
        current = self.tenants.get_tenant(tenant_id).to_dict()
        payload = _merge_public_config(payload, current)
        return self.tenants.create_config_version(
            tenant_id,
            TenantConfig.from_dict(payload),
            expected_version=expected_version,
        ).to_public_dict()

    def publish(self, tenant_id: str, version: int, expected_version: int | None = None) -> dict[str, Any]:
        return self.tenants.publish(tenant_id, version, expected_version=expected_version).to_public_dict()

    def rollback(self, tenant_id: str, version: int, expected_version: int | None = None) -> dict[str, Any]:
        return self.tenants.rollback(tenant_id, version, expected_version=expected_version).to_public_dict()

    def configure_gray_release(
        self,
        tenant_id: str,
        payload: dict[str, Any],
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        return self.tenants.configure_gray_release(
            tenant_id,
            payload.get("candidate_version"),
            int(payload.get("percent", 0)),
            session_overrides=payload.get("session_overrides") or {},
            enabled=bool(payload.get("enabled", True)),
            expected_version=expected_version,
        ).to_public_dict()

    def add_channel(
        self,
        tenant_id: str,
        payload: dict[str, Any],
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        payload = dict(payload)
        payload["tenant_id"] = tenant_id
        payload.setdefault("binding_id", f"{payload['channel']}:{payload['account_id']}")
        binding = ChannelBinding.from_dict(payload)
        return self.tenants.add_channel_binding(
            tenant_id,
            binding,
            expected_version=expected_version,
        ).to_public_dict()


def _merge_public_config(value: Any, current: Any) -> Any:
    """Make a redacted GET response safe to round-trip through PUT."""

    if value == "[configured]":
        return deepcopy(current)
    if isinstance(value, dict) and isinstance(current, dict):
        result = deepcopy(current)
        for key, item in value.items():
            result[key] = _merge_public_config(item, current.get(key))
        return result
    if isinstance(value, list) and isinstance(current, list):
        if not value:
            return []
        if all(isinstance(item, dict) for item in value) and all(isinstance(item, dict) for item in current):
            identity_keys = ("agent_app_id", "binding_id", "memory_id")
            current_by_identity = {
                (identity_key, str(item.get(identity_key))): item
                for identity_key in identity_keys
                for item in current
                if item.get(identity_key) is not None
            }
            result = []
            for index, item in enumerate(value):
                current_item = current[index] if index < len(current) else {}
                for identity_key in identity_keys:
                    identity = item.get(identity_key)
                    if identity is not None:
                        current_item = current_by_identity.get((identity_key, str(identity)), current_item)
                        break
                result.append(_merge_public_config(item, current_item))
            return result
        return [
            _merge_public_config(item, current[index] if index < len(current) else None)
            for index, item in enumerate(value)
        ]
    return deepcopy(value)
