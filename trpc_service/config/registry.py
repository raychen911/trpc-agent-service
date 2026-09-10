"""跨模块共享的进程级单例注册表。"""
from __future__ import annotations

from trpc_service.config.manager import ConfigManager

config_manager = ConfigManager()
