"""Composable tenant activation checks."""

from __future__ import annotations

from pydantic import BaseModel
from pydantic import SecretStr

from trpc_service.tenant import Tenant
from ._secrets import is_secret_ref
from ._settings import ServiceSettings


class ProductionTenantPreflight:
    """Reject single-node backends and inline secrets in production."""

    def __init__(self, settings: ServiceSettings) -> None:
        self._settings = settings

    def __call__(self, tenant: Tenant) -> None:
        if self._settings.environment != "production":
            return
        if tenant.storage_config.vector.backend == "memory":
            raise ValueError("production tenants cannot use the in-memory vector backend")
        if tenant.storage_config.object.backend == "local":
            raise ValueError("production tenants cannot use the local object backend")
        self._validate_secret_refs(tenant)

    @staticmethod
    def _validate_secret_refs(model: BaseModel) -> None:
        for name in model.__class__.model_fields:
            value = getattr(model, name)
            if isinstance(value, SecretStr):
                if not is_secret_ref(value.get_secret_value()):
                    raise ValueError(f"production secret field '{name}' must use a SecretRef")
            elif isinstance(value, BaseModel):
                ProductionTenantPreflight._validate_secret_refs(value)
            elif isinstance(value, dict):
                for item in value.values():
                    if isinstance(item, BaseModel):
                        ProductionTenantPreflight._validate_secret_refs(item)
