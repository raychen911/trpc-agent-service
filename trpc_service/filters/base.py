# ===================================================================
# filters.base - 治理 Filter 基类与链（洋葱模型）
# ===================================================================
# 说明: 平台层治理 Filter 链（PRD 0.3-2 / 4.1），复用框架 Filter 的
#   洋葱模型（_before -> handle -> _after），在 Gateway 侧按序执行:
#   Trace -> Audit -> TenantResolve -> Signature -> UserAuth
#   -> RateLimit -> Budget -> ToolWhitelist -> PII
# 规范: Filter 抛 FilterBlocked 即中断链路（短路的治理决策）；
#   Audit 置于洋葱外层（Trace 之内），保证被阻断 / 异常流量同样留痕。
# ===================================================================

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Awaitable, Callable, Optional

from ..events import AgentEvent

if TYPE_CHECKING:
    from .context import GatewayContext

# handle 类型: 执行下一个 Filter（洋葱内层）
HandleType = Callable[[], Awaitable["FilterResult"]]


class FilterBlocked(Exception):
    """治理决策: 阻断请求。

    Attributes:
        reason: 阻断原因（audit_log.decision = block）
        error_type: 错误类型（metrics agent_errors label）
    """

    def __init__(self, reason: str, error_type: str = "filter_blocked") -> None:
        super().__init__(reason)
        self.reason = reason
        self.error_type = error_type


@dataclass
class FilterResult:
    """Filter 链执行结果。"""

    event: Optional[AgentEvent] = None
    error: Optional[Exception] = None
    decisions: list[str] = field(default_factory=list)
    """链路累计决策记录（如 allow / block / confirm_required）。"""
    duration_ms: int = 0
    """链路总耗时（审计用）。"""

    @property
    def passed(self) -> bool:
        return self.error is None


class GatewayFilter(ABC):
    """治理 Filter 基类（洋葱模型）。"""

    name: str = "base"

    def __init__(self) -> None:
        self._chain_index = 0

    @abstractmethod
    async def _before(self, ctx: "GatewayContext", event: AgentEvent, result: FilterResult) -> None:
        """前置校验 / 注入（放行则返回；阻断抛 FilterBlocked）。"""

    async def _after(self, ctx: "GatewayContext", event: AgentEvent, result: FilterResult) -> None:
        """后置处理（审计等，默认无操作）。"""

    async def run(self, ctx: "GatewayContext", event: AgentEvent, handle: HandleType) -> FilterResult:
        """执行完整生命周期: before -> handle -> after。"""
        result = FilterResult(event=event)
        start = time.perf_counter()
        try:
            await self._before(ctx, event, result)
        except FilterBlocked as exc:
            result.error = exc
            result.decisions.append(f"block:{self.name}:{exc.reason}")
            result.duration_ms = int((time.perf_counter() - start) * 1000)
            return result
        except Exception as exc:  # noqa: BLE001 - 治理层统一兜底
            result.error = exc
            result.decisions.append(f"error:{self.name}")
            result.duration_ms = int((time.perf_counter() - start) * 1000)
            return result

        # 内层链路
        inner = await handle()
        result.event = inner.event or result.event
        result.decisions.extend(inner.decisions)
        result.error = inner.error

        # _after 无论链路成败都执行（审计需覆盖被阻断/异常流量，PRD 4.4）；
        # _after 自身异常不吞掉内层原有错误。
        try:
            await self._after(ctx, result.event, result)
        except Exception as exc:  # noqa: BLE001
            if result.error is None:
                result.error = exc
        result.duration_ms = int((time.perf_counter() - start) * 1000)
        return result


async def _noop_handle() -> FilterResult:
    """链路末端（无更多 Filter）。"""
    return FilterResult()


class FilterChain:
    """按注册顺序执行 Filter（洋葱嵌套）。"""

    def __init__(self, filters: list[GatewayFilter]) -> None:
        self._filters = filters
        for index, flt in enumerate(filters):
            flt._chain_index = index

    async def run(self, ctx: "GatewayContext", event: AgentEvent) -> FilterResult:

        async def build(index: int) -> HandleType:
            if index >= len(self._filters):
                return _noop_handle

            async def handle() -> FilterResult:
                return await self._filters[index].run(ctx, event, await build(index + 1))

            return handle

        start = time.perf_counter()
        result = await (await build(0))()
        result.duration_ms = int((time.perf_counter() - start) * 1000)
        return result

    def __len__(self) -> int:
        return len(self._filters)
