"""
Tenant Canary / Gray Release and Dynamic Config Rollback Engine.
Allows percentage-based or user-whitelisted canary releases and instant version rollback.
"""

import copy
import hashlib
from typing import Dict, List, Optional
import logging
from trpc_service.config.models import TenantConfig, AgentAppConfig
from trpc_service.tenant.manager import tenant_manager

logger = logging.getLogger(__name__)


class GrayReleaseRule:
    """Defines how traffic is split between baseline (v1) and canary (v2)."""

    def __init__(
        self,
        tenant_id: str,
        canary_percent: int = 10,
        whitelist_users: Optional[List[str]] = None,
        whitelist_groups: Optional[List[str]] = None,
        canary_app_id: Optional[str] = None,
    ):
        self.tenant_id = tenant_id
        self.canary_percent = canary_percent  # 0 to 100
        self.whitelist_users = set(whitelist_users or [])
        self.whitelist_groups = set(whitelist_groups or [])
        self.canary_app_id = canary_app_id

    def should_route_to_canary(self, user_id: str, chat_id: Optional[str] = None) -> bool:
        """Evaluate whether a request routes to canary."""
        if user_id in self.whitelist_users:
            return True
        if chat_id and chat_id in self.whitelist_groups:
            return True

        if self.canary_percent <= 0:
            return False
        if self.canary_percent >= 100:
            return True

        # Consistent hashing based on user_id
        hash_val = int(hashlib.md5(f"{self.tenant_id}:{user_id}".encode("utf-8")).hexdigest(), 16)
        bucket = hash_val % 100
        return bucket < self.canary_percent


class GrayReleaseManager:
    """
    Manages active canary rules and maintains configuration history for instant rollback.
    """

    def __init__(self):
        self._rules: Dict[str, GrayReleaseRule] = {}
        # History version storage: tenant_id -> list of (version_num, TenantConfig)
        self._config_history: Dict[str, List[TenantConfig]] = {}

    def set_gray_rule(self, rule: GrayReleaseRule):
        """Set or update canary release rule."""
        self._rules[rule.tenant_id] = rule
        logger.info("[GrayRelease] Set canary rule for tenant %s: percent=%d%%",
                    rule.tenant_id, rule.canary_percent)

    def get_gray_rule(self, tenant_id: str) -> Optional[GrayReleaseRule]:
        return self._rules.get(tenant_id)

    def should_use_canary(self, tenant_id: str, user_id: str, chat_id: Optional[str] = None) -> bool:
        rule = self._rules.get(tenant_id)
        if not rule:
            return False
        return rule.should_route_to_canary(user_id, chat_id)

    def save_config_snapshot(self, tenant_config: TenantConfig):
        """Save snapshot of current configuration for future rollback."""
        tid = tenant_config.tenant_id
        if tid not in self._config_history:
            self._config_history[tid] = []
        # Append deepcopy
        self._config_history[tid].append(copy.deepcopy(tenant_config))

    def rollback_to_previous(self, tenant_id: str) -> Optional[TenantConfig]:
        """Rollback tenant configuration to the preceding snapshot."""
        history = self._config_history.get(tenant_id, [])
        if len(history) < 2:
            logger.warning("[Rollback] No previous version to rollback for tenant %s", tenant_id)
            return None

        # Pop latest current version
        history.pop()
        # Restore previous version
        prev_config = history[-1]
        tenant_manager.register_tenant(copy.deepcopy(prev_config))
        logger.info("[Rollback] Successfully rolled back tenant %s to snapshot updated at %s",
                    tenant_id, prev_config.updated_at)
        return prev_config


gray_release_manager = GrayReleaseManager()
