"""租户配置管理器。"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from trpc_service.config.loader import DEFAULT_CONFIG_PATH, load_config
from trpc_service.config.tenant_config import TenantConfig


class ConfigManager:
    """租户配置缓存管理器。"""

    def __init__(self, config_path: Optional[str] = None):
        self._config_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
        self._mtime: Optional[float] = None
        self._configs: Dict[str, TenantConfig] = {}
        self.reload(force=True)

    def _current_mtime(self) -> Optional[float]:
        try:
            return self._config_path.stat().st_mtime
        except OSError:
            return None

    def reload(self, force: bool = False) -> Dict[str, TenantConfig]:
        """重新加载配置。force=True 时忽略 mtime 强制加载。"""
        self._configs = load_config(str(self._config_path))
        self._assert_unique_app_name()
        self._mtime = self._current_mtime()
        return self._configs

    def _assert_unique_app_name(self) -> None:
        """聚合级不变量：规范化后的 app_name 全局唯一（模型层管不了跨租户）。"""
        seen = {}
        for cfg in self._configs.values():
            if cfg.app.app_name in seen:
                raise ValueError(
                    f"app_name 冲突: {cfg.app.app_name} 同时属于 "
                    f"{seen[cfg.app.app_name]} 和 {cfg.tenant_id}"
                )
            seen[cfg.app.app_name] = cfg.tenant_id

    def maybe_reload(self) -> Dict[str, TenantConfig]:
        """如果配置文件有变更则热加载，否则返回缓存。"""
        if self._current_mtime() != self._mtime:
            return self.reload()
        return self._configs

    def get(self, tenant_id: str) -> Optional[TenantConfig]:
        return self._configs.get(tenant_id)

    def register(self, tenant: TenantConfig) -> TenantConfig:
        """注册（或覆盖）一个租户配置。"""
        self._configs[tenant.tenant_id] = tenant
        self._assert_unique_app_name()
        return tenant

    def remove(self, tenant_id: str) -> bool:
        """移除租户配置。"""
        return self._configs.pop(tenant_id, None) is not None

    def all(self) -> Dict[str, TenantConfig]:
        return self._configs
