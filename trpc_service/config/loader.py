"""租户配置加载。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import yaml
from dotenv import load_dotenv

from trpc_service.config.tenant_config import TenantConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "tenants.yaml"
ENV_FILE = PROJECT_ROOT / ".env"


def load_dotenv_if_present() -> None:
    """加载项目根目录的 .env（不存在则跳过）。"""
    if ENV_FILE.exists():
        load_dotenv(dotenv_path=ENV_FILE)


def _apply_env_overrides(cfg: TenantConfig) -> TenantConfig:
    """用环境变量覆盖租户配置（模型 + 通道凭证：yaml 留空占位，凭证不进仓库）。"""
    key = os.getenv("KEY") or os.getenv("API_KEY")
    url = os.getenv("URL") or os.getenv("BASE_URL")
    model = os.getenv("MODEL")
    sql_url = os.getenv("SQL_URL")
    if key:
        cfg.model.api_key = key
    if url:
        cfg.model.base_url = url
    if model:
        cfg.model.model_name = model
    if sql_url and not cfg.storage.sql_url:
        cfg.storage.sql_url = sql_url

    channel_env = {
        "wecom": {
            "token": "WECOM_TOKEN",
            "encoding_aes_key": "WECOM_AES_KEY",
            "corp_id": "WECOM_CORP_ID",
            "bot_id": "WECOM_AGENT_ID",
        },
        "wecom_smartbot": {"secret": "WECOM_BOT_SECRET", "bot_id": "WECOM_SMARTBOT_ID"},
        "feishu": {
            "app_secret": "FEISHU_APP_SECRET",
            "token": "FEISHU_TOKEN",
            "encrypt_key": "FEISHU_ENCRYPT_KEY",
        },
    }
    for name, cfg_channel in cfg.channels.items():
        env_map = channel_env.get(name)
        if not env_map:
            continue
        for field, env_name in env_map.items():
            if getattr(cfg_channel, field, "") == "":
                value = os.getenv(env_name, "")
                if value:
                    setattr(cfg_channel, field, value)
    return cfg


def load_config(path: Optional[str] = None) -> Dict[str, TenantConfig]:
    """加载全部租户配置，返回 tenant_id -> TenantConfig 的映射。"""
    load_dotenv_if_present()
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"租户配置文件不存在: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    tenants = (raw or {}).get("tenants") or []
    result: Dict[str, TenantConfig] = {}
    for item in tenants:
        cfg = TenantConfig(**item)
        _apply_env_overrides(cfg)
        result[cfg.tenant_id] = cfg
    return result


def get_tenant_config(tenant_id: str, configs: Optional[Dict[str, TenantConfig]] = None) -> Optional[TenantConfig]:
    """按 tenant_id 获取单个租户配置。"""
    return (configs or load_config()).get(tenant_id)
