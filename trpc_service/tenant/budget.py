# ===================================================================
# tenant.budget - 租户成本累计（Runtime 执行后结算用）
# ===================================================================
# 说明: PRD 4.2/6-10「成本累计 + BudgetFilter 硬限」。Runtime 在 Agent
#   执行完、算得真实 token 消耗与成本后调用 BudgetTracker.record:
#     tenant_store.increment_usage(SQL 原子累加 used_budget_usd)
#     -> registry.invalidate(本进程缓存失效，下一请求回源读新预算)
#     -> broadcaster.publish_invalidated(跨节点失效通知，尽力而为)
#   BudgetFilter 在下一请求读到已累加的 used_budget_usd，超限即 block。
# 一致性: 多节点间为**最终一致**（执行后累加 + 广播失效，Filter 读到
#   的是缓存快照，可容忍瞬时越界）——与 PRD §2.4 一致。
# 规范: 无 tenant_store（纯内存 demo 模式）时 no-op 并告警，metrics /
#   审计不受影响；record 失败不抛错，不阻塞回复主链路。
# ===================================================================

from __future__ import annotations

import asyncio
from typing import Any

from ..log.logger import get_logger

log = get_logger("tenant.budget")

_RETRY_SLEEP_S = 0.2
"""持久化失败后重试前的等待（秒）；预算累计尽力而为，重试一次防瞬时 DB 抖动）。"""


class BudgetTracker:
    """把单次执行成本累加到租户预算（SQL 原子累加 + 缓存/广播失效）。

    Args:
        tenant_store: SqlTenantStore（含 increment_usage）；None 时仅记录日志
        registry: TenantRegistry（累加后失效本地缓存，下一请求回源）
        broadcaster: ConfigBroadcaster（跨节点失效通知）；None 时仅本进程
    """

    def __init__(self, *, tenant_store: Any = None, registry: Any = None, broadcaster: Any = None) -> None:
        self._tenant_store = tenant_store
        self._registry = registry
        self._broadcaster = broadcaster

    async def record(self, tenant_id: str, cost_usd: float) -> None:
        """累加单次执行成本到租户预算。

        成本 <= 0 时跳过（无消耗不必记账）。持久化失败**重试一次**后仍失败
        仅告警返回（预算累计是尽力而为，不能因记账失败拖垮回复链路）。
        """
        if cost_usd <= 0:
            return
        if self._tenant_store is None:
            log.warning("budget tracker without tenant_store, skip persist",
                        extra={
                            "tenant_id": tenant_id,
                            "cost_usd": cost_usd
                        })
            return
        # 注（审查 09-04）: 重试是非幂等的 `+=`——若首次实际成功而响应丢失，
        # 会多计一次成本。这是**有意取舍**：多计使预算更紧（fail-safe），
        # 不会放松 BudgetFilter 硬限；反之少计才是危险方向。
        persisted = False
        try:
            await self._tenant_store.increment_usage(tenant_id, cost_usd)
            persisted = True
        except Exception as exc:  # noqa: BLE001 - 首次失败重试一次（防瞬时抖动）
            log.warning("budget persist failed, retrying", extra={"tenant_id": tenant_id, "error": str(exc)})
            await asyncio.sleep(_RETRY_SLEEP_S)
            try:
                await self._tenant_store.increment_usage(tenant_id, cost_usd)
                persisted = True
            except Exception as exc2:  # noqa: BLE001 - 重试仍失败仅告警
                log.warning("budget persist failed after retry", extra={"tenant_id": tenant_id, "error": str(exc2)})
        # 缓存/广播失效无论持久化成败都执行（审查 09-04：此前失败路径不失效，
        # 各节点缓存会一直拿着旧预算）；失败仅丢「已持久化」的增量，最终一致。
        if self._registry is not None:
            self._registry.invalidate(tenant_id)
        if self._broadcaster is not None:
            try:
                await self._broadcaster.publish_invalidated(tenant_id)
            except Exception:  # noqa: BLE001 - 通知失败不影响本进程已失效
                pass
        return persisted
