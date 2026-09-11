"""Fixed-domain errors for tenant agent configuration."""

from __future__ import annotations

TENANT_AGENT_CONFIG_ERROR_TEXT = "Tenant agent configuration is not available."


class TenantAgentConfigurationError(ValueError):
    """Raised when a tenant's agent configuration cannot be resolved."""

    def __init__(self) -> None:
        super().__init__(TENANT_AGENT_CONFIG_ERROR_TEXT)


__all__ = [
    "TENANT_AGENT_CONFIG_ERROR_TEXT",
    "TenantAgentConfigurationError",
]
