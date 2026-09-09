# ===================================================================
# channels.factory - 通道工厂（按租户 im_channel_config 创建）
# ===================================================================
# 说明: 每类 IM 一个适配器实例（PRD 3.1），工厂按租户绑定配置创建。
#   adapter 缓存: (tenant_id, channel_type) -> IMAdapter，复用实例。
# 规范: 未安装可选依赖（httpx/cryptography）的通道创建时给出明确错误。
# ===================================================================

from __future__ import annotations

from typing import Optional

from ..tenant.models import ChannelType, ImChannelConfig
from .base import IMAdapter
from .feishu import FeishuAdapter
from .web import WebImAdapter
from .wechat_work import WechatWorkAdapter


class ChannelFactory:
    """IM 通道工厂（实例缓存 + 按配置创建）。"""

    def __init__(self) -> None:
        self._adapters: dict[tuple[str, str], IMAdapter] = {}

    def create(self, tenant_id: str, config: ImChannelConfig, channel_type: Optional[ChannelType] = None) -> IMAdapter:
        """创建/获取通道适配器（同一租户同通道复用实例）。"""
        ctype = channel_type or config.channel_type
        key = (tenant_id, ctype)
        cached = self._adapters.get(key)
        if cached is not None:
            return cached
        adapter = self._build(ctype, config)
        self._adapters[key] = adapter
        return adapter

    def get(self, tenant_id: str, channel_type: ChannelType) -> Optional[IMAdapter]:
        """获取已创建的适配器（无则 None）。"""
        return self._adapters.get((tenant_id, channel_type))

    def invalidate(self, tenant_id: str) -> None:
        """失效某租户的全部适配器缓存（租户配置热更新后调用）。

        此前缓存不随配置变更失效——Admin 改 token/secret/通道绑定后旧实例
        （带旧凭据）继续服役，须重启节点才生效（多节点下各节点一致）。
        仅丢引用不 await close（本方法为同步接口）：旧 httpx 客户端由 GC
        回收，进程关闭时统一走 close()。
        """
        stale = [k for k in self._adapters if k[0] == tenant_id]
        for key in stale:
            self._adapters.pop(key, None)

    def _build(self, channel_type: ChannelType, config: ImChannelConfig) -> IMAdapter:
        if channel_type == "web":
            return WebImAdapter(config)
        if channel_type == "wechat_work":
            return WechatWorkAdapter(config)
        if channel_type == "feishu":
            return FeishuAdapter(config)
        raise ValueError(f"不支持的通道类型: {channel_type}")

    async def close(self) -> None:
        for adapter in self._adapters.values():
            close = getattr(adapter, "close", None)
            if close is not None:
                await close()
        self._adapters.clear()
