"""Tenant channel-binding registry and adapter construction."""

from __future__ import annotations

from typing import Callable
from typing import Optional

from trpc_service.channels import ChannelAdapter
from trpc_service.channels import DingTalkAdapter
from trpc_service.channels import FeishuAdapter
from trpc_service.channels import QQAdapter
from trpc_service.channels import WecomAdapter
from trpc_service.channels import WechatCustomerServiceAdapter
from trpc_service.config import DEFAULT_SECRET_RESOLVER
from trpc_service.config import SecretResolver
from trpc_service.config import resolve_model_secrets
from trpc_service.tenant import ChannelConfig
from trpc_service.tenant import DingTalkChannelConfig
from trpc_service.tenant import FeishuChannelConfig
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import WeComChannelConfig
from trpc_service.tenant import WechatCustomerServiceChannelConfig

ChannelAdapterFactory = Callable[[ChannelConfig], ChannelAdapter]


def _wecom_factory(cfg: WeComChannelConfig) -> WecomAdapter:
    return WecomAdapter(
        token=cfg.token.get_secret_value(),
        encoding_aes_key=cfg.aes_key.get_secret_value(),
        corp_id=cfg.corp_id,
        agent_id=cfg.agent_id,
        access_token=cfg.access_token.get_secret_value() if cfg.access_token else None,
        corp_secret=cfg.secret.get_secret_value() if cfg.secret else None,
    )


def _wechat_kf_factory(cfg: WechatCustomerServiceChannelConfig) -> WechatCustomerServiceAdapter:
    return WechatCustomerServiceAdapter(
        corp_id=cfg.corp_id,
        open_kfid=cfg.open_kfid,
        token=cfg.token.get_secret_value(),
        webhook_url=cfg.webhook_url,
    )


def _dingtalk_factory(cfg: DingTalkChannelConfig) -> DingTalkAdapter:
    return DingTalkAdapter(
        client_id=cfg.app_id,
        robot_code=cfg.robot_code,
        secret=cfg.secret.get_secret_value(),
        webhook_url=cfg.webhook_url,
    )


def _feishu_factory(cfg: FeishuChannelConfig) -> FeishuAdapter:
    return FeishuAdapter(
        app_id=cfg.app_id,
        verification_token=(cfg.verification_token.get_secret_value() if cfg.verification_token else ""),
        encrypt_key=cfg.encrypt_key.get_secret_value() if cfg.encrypt_key else "",
        secret=cfg.secret.get_secret_value() if cfg.secret else None,
        webhook_url=cfg.webhook_url,
    )


def _qq_factory(cfg: QQChannelConfig) -> QQAdapter:
    return QQAdapter(
        app_id=cfg.app_id,
        app_secret=cfg.secret.get_secret_value(),
        access_token=cfg.access_token.get_secret_value() if cfg.access_token else None,
    )


def default_channel_factories() -> dict[str, ChannelAdapterFactory]:
    return {
        "wecom": _wecom_factory,
        "wechat_kf": _wechat_kf_factory,
        "dingtalk": _dingtalk_factory,
        "feishu": _feishu_factory,
        "qq": _qq_factory,
    }


class ChannelRegistry:
    """Build and cache adapters for tenant-owned binding identities."""

    def __init__(
        self,
        factories: Optional[dict[str, ChannelAdapterFactory]] = None,
        secret_resolver: Optional[SecretResolver] = None,
    ) -> None:
        self._factories = factories or default_channel_factories()
        self._secret_resolver = secret_resolver or DEFAULT_SECRET_RESOLVER
        self._cache: dict[tuple[str, str], ChannelAdapter] = {}

    def register_factory(self, channel: str, factory: ChannelAdapterFactory) -> None:
        self._factories[channel] = factory

    def invalidate(self, tenant_id: Optional[str] = None) -> None:
        if tenant_id is None:
            self._cache.clear()
            return
        for key in [key for key in self._cache if key[0] == tenant_id]:
            self._cache.pop(key, None)

    def get(self, tenant: Tenant, channel: str) -> Optional[ChannelAdapter]:
        key = (tenant.tenant_id, channel)
        if key in self._cache:
            return self._cache[key]
        config = tenant.channel_configs.get(channel)
        if config is None:
            return None
        factory = self._factories.get(channel) or self._factories.get(config.channel_type)
        if factory is None:
            return None
        resolved = resolve_model_secrets(
            config,
            tenant_id=tenant.tenant_id,
            resolver=self._secret_resolver,
        )
        adapter = factory(resolved)
        self._cache[key] = adapter
        return adapter
