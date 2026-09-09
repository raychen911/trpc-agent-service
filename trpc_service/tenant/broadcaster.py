# ===================================================================
# tenant.broadcaster - 跨进程租户配置失效通知（Redis pub/sub）
# ===================================================================
# 说明: Admin 进程更新租户配置后，Gateway 进程的本地 Registry LRU
#   不会自动感知 —— 本模块用 Redis pub/sub 广播失效通知，各节点
#   订阅后 invalidate 本地缓存，下一请求回源加载新配置（PRD 5.2
#   灰度/回滚的跨节点一致性基础）。
# 规范: 尽力而为（best-effort）—— Redis 不可用时发布静默降级（仅告警），
#   配置最终以共享存储为准；单进程 / InMemory 模式可不启用。
# ===================================================================

from __future__ import annotations

import asyncio
from typing import Any, Optional

from ..log.logger import get_logger
from .registry import TenantRegistry

log = get_logger("tenant.broadcaster")

CHANNEL = "teneuris:tenant_changed"
"""租户配置变更通知频道（消息体 = tenant_id）。"""

_DEBOUNCE_S = 0.5
"""get_message 轮询间隔（秒），兼顾时效与空转开销。"""


class ConfigBroadcaster:
    """基于 Redis pub/sub 的配置失效广播器。

    - Admin 侧: `publish_invalidated(tenant_id)` 通知各节点
    - Gateway 侧: `subscribe_loop(registry, stop)` 后台任务订阅并失效本地缓存
    """

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    async def publish_invalidated(self, tenant_id: str) -> None:
        """发布失效通知。失败仅告警不抛错（配置以共享存储为准，不影响主流程）。"""
        try:
            await self._redis.publish(CHANNEL, tenant_id)
        except Exception as exc:  # noqa: BLE001 - 通知失败不阻塞配置写入
            log.warning("tenant change publish failed", extra={"tenant_id": tenant_id, "error": str(exc)})

    async def subscribe_loop(self,
                             registry: TenantRegistry,
                             stop: asyncio.Event,
                             ready: Optional[asyncio.Event] = None,
                             storage_manager: Any = None,
                             channel_factory: Any = None) -> None:
        """订阅循环：收到通知即失效本地 Registry 缓存（下一请求回源加载）。

        Args:
            registry: TenantRegistry（调用 invalidate）
            stop: 停止事件（进程退出时置位，循环在下一轮询周期退出）
            ready: 可选就绪事件；订阅建立后置位（避免测试在订阅完成前发布丢消息）
            storage_manager: 可选 StorageManager；租户配置变更后一并失效
                按租户缓存的 Storage（PRD 2.1 后端切换生效）
            channel_factory: 可选 ChannelFactory；一并失效适配器缓存，
                避免旧凭据实例继续服役（C6：热更新对通道绑定生效）
        """
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(CHANNEL)
        if ready is not None:
            ready.set()
        log.info("tenant change subscriber started", extra={"channel": CHANNEL})
        try:
            while not stop.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=_DEBOUNCE_S)
                if not msg:
                    continue
                tenant_id = self._decode(msg.get("data"))
                if tenant_id:
                    registry.invalidate(tenant_id)
                    if storage_manager is not None:
                        storage_manager.invalidate(tenant_id)
                    if channel_factory is not None:
                        channel_factory.invalidate(tenant_id)
                    log.info("tenant cache invalidated", extra={"tenant_id": tenant_id})
        except asyncio.CancelledError:  # pragma: no cover - 进程退出路径
            pass
        finally:
            try:
                await pubsub.aclose()
            except Exception:  # noqa: BLE001 - 退出清理尽力而为
                pass
            log.info("tenant change subscriber stopped")

    @staticmethod
    def _decode(data: Any) -> Optional[str]:
        """pub/sub 消息体 -> tenant_id（兼容 str / bytes / 空）。"""
        if data is None:
            return None
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="ignore") or None
        text = str(data).strip()
        return text or None
