"""
Tenant Manager and Context handling for Multi-tenant isolation.
"""

from contextvars import ContextVar
from typing import Dict, List, Optional
import logging
from trpc_service.config.models import (
    TenantConfig,
    AgentAppConfig,
    ChannelBindingConfig,
    QuotaPolicy,
)

logger = logging.getLogger(__name__)

# ContextVars for request-scoped tenant and trace isolation
_current_tenant_id: ContextVar[Optional[str]] = ContextVar("current_tenant_id", default=None)
_current_trace_id: ContextVar[Optional[str]] = ContextVar("current_trace_id", default=None)
_current_user_id: ContextVar[Optional[str]] = ContextVar("current_user_id", default=None)


class TenantContext:
    """Helper to set and retrieve context variables safely in async workflows."""

    @staticmethod
    def set_context(tenant_id: str, trace_id: Optional[str] = None, user_id: Optional[str] = None):
        t_token = _current_tenant_id.set(tenant_id)
        tr_token = _current_trace_id.set(trace_id)
        u_token = _current_user_id.set(user_id)
        return (t_token, tr_token, u_token)

    @staticmethod
    def reset_context(tokens):
        t_token, tr_token, u_token = tokens
        _current_tenant_id.reset(t_token)
        _current_trace_id.reset(tr_token)
        _current_user_id.reset(u_token)

    @staticmethod
    def get_tenant_id() -> Optional[str]:
        return _current_tenant_id.get()

    @staticmethod
    def get_trace_id() -> Optional[str]:
        return _current_trace_id.get()

    @staticmethod
    def get_user_id() -> Optional[str]:
        return _current_user_id.get()


class TenantManager:
    """Manages tenant configurations, agent apps, channel bindings, and policies."""

    def __init__(self):
        self._tenants: Dict[str, TenantConfig] = {}
        self._apps: Dict[str, AgentAppConfig] = {}  # key: app_id
        self._tenant_apps: Dict[str, List[str]] = {}  # key: tenant_id -> list of app_ids
        self._channel_bindings: Dict[str, ChannelBindingConfig] = {}  # key: binding_id
        self._token_usage_daily: Dict[str, int] = {}  # key: tenant_id -> daily token count

    def register_tenant(self, config: TenantConfig) -> TenantConfig:
        """Register or update a tenant configuration."""
        self._tenants[config.tenant_id] = config
        if config.tenant_id not in self._tenant_apps:
            self._tenant_apps[config.tenant_id] = []
        logger.info("Tenant registered/updated: %s (%s)", config.tenant_id, config.name)
        return config

    def get_tenant(self, tenant_id: str) -> Optional[TenantConfig]:
        """Retrieve tenant configuration by ID."""
        return self._tenants.get(tenant_id)

    def list_tenants(self) -> List[TenantConfig]:
        """List all registered tenants."""
        return list(self._tenants.values())

    def register_app(self, app_config: AgentAppConfig) -> AgentAppConfig:
        """Register an agent app under a specific tenant."""
        if app_config.tenant_id not in self._tenants:
            raise ValueError(f"Cannot register app. Tenant '{app_config.tenant_id}' does not exist.")
        self._apps[app_config.app_id] = app_config
        if app_config.app_id not in self._tenant_apps[app_config.tenant_id]:
            self._tenant_apps[app_config.tenant_id].append(app_config.app_id)
        logger.info("Agent App registered: %s for tenant: %s", app_config.app_id, app_config.tenant_id)
        return app_config

    def get_app(self, app_id: str) -> Optional[AgentAppConfig]:
        """Retrieve agent app config by app_id."""
        return self._apps.get(app_id)

    def get_tenant_apps(self, tenant_id: str) -> List[AgentAppConfig]:
        """Get all agent apps belonging to a tenant."""
        app_ids = self._tenant_apps.get(tenant_id, [])
        return [self._apps[aid] for aid in app_ids if aid in self._apps]

    def register_channel_binding(self, binding: ChannelBindingConfig) -> ChannelBindingConfig:
        """Register an IM channel binding to a tenant."""
        if binding.tenant_id not in self._tenants:
            raise ValueError(f"Cannot bind channel. Tenant '{binding.tenant_id}' does not exist.")
        self._channel_bindings[binding.binding_id] = binding
        logger.info("Channel binding registered: %s (Type: %s, Tenant: %s)",
                    binding.binding_id, binding.channel_type, binding.tenant_id)
        return binding

    def get_channel_binding(self, binding_id: str) -> Optional[ChannelBindingConfig]:
        """Get channel binding by ID."""
        return self._channel_bindings.get(binding_id)

    def find_channel_binding_by_bot(self, channel_type: str, bot_id: str) -> Optional[ChannelBindingConfig]:
        """Find channel binding for inbound webhook matching bot_id and channel_type."""
        for b in self._channel_bindings.values():
            if b.channel_type == channel_type and b.bot_id == bot_id and b.is_active:
                return b
        return None

    def record_token_usage(self, tenant_id: str, tokens: int):
        """Track tenant token consumption for quota enforcement."""
        self._token_usage_daily[tenant_id] = self._token_usage_daily.get(tenant_id, 0) + tokens

    def check_quota(self, tenant_id: str) -> bool:
        """Check whether tenant has exceeded daily token budget."""
        tenant = self.get_tenant(tenant_id)
        if not tenant:
            return False
        used = self._token_usage_daily.get(tenant_id, 0)
        return used < tenant.quota_policy.daily_token_budget


# Global singleton tenant manager instance
tenant_manager = TenantManager()
