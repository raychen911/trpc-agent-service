# ===================================================================
# runtime.pipeline - 消息处理主链路（幂等 -> Filter 链 -> Runtime）
# ===================================================================
# 说明: 原为 web/app.py 内 webhook 处理闭包（_process），抽取为模块级
#   函数，使 HTTP webhook 与 wecom_bot 长连接驱动（channels/wecom_bot.py）
#   共用同一条「幂等去重 -> 治理 Filter 链 -> Runtime 执行」链路。
#   - 逻辑与 web/app.py 历史版本零差异（纯重构，行为不变）
#   - ctx 由 build_filter_chain 产出的 GatewayContext，内含 storage/registry
# ===================================================================

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..events import AgentEvent, AgentResponse, ResponseType

if TYPE_CHECKING:  # 仅类型检查期导入，避免 runtime <-> filters 运行期耦合
    from ..filters.base import FilterChain
    from .runtime import Runtime
from ..tenant.resolver import generate_session_id


async def process_event(
    event: AgentEvent,
    *,
    chain: "FilterChain",
    ctx: Any,
    runtime: "Runtime",
) -> AgentResponse:
    """处理单个 AgentEvent（webhook 与 wecom_bot 长连接共用入口）。

    Args:
        event: 已带 tenant_id/channel_type/user_id/msg_id/content 的入站事件
        chain: build_filter_chain() 产出的 FilterChain
        ctx: build_filter_chain() 产出的 GatewayContext（含 storage）
        runtime: Runtime 实例（框架 Runner 封装）

    Returns:
        AgentResponse: 回复（TEXT/ERROR），由调用方决定如何投递
    """
    # Session ID 确定性生成（PRD 1.3-3）: 客户端未传时按
    # (tenant, channel, channel_id, user, group?) 恒定映射，
    # 同一会话后续请求无需 sticky 即可路由到共享后端的同一 session。
    if not event.session_id:
        event.session_id = generate_session_id(
            tenant_id=event.tenant_id,
            channel_type=event.channel_type,  # type: ignore[arg-type]
            channel_id=event.channel_id,
            external_user_id=event.user_id,
            is_group=event.is_group,
        )

    # 幂等去重（PRD 2.3-E；key 带 tenant_id 前缀，跨租户隔离）。
    # 语义（审查 09-04 修复）: 成功处理后保持占用（TTL 过期前重复 msg_id 判
    # duplicate）；**处理失败（治理阻断/执行错误）时释放**，让 IM 平台的
    # 消息重试能真正重新处理，而非 24h 内一律被幂等挡掉。
    idem_key = None
    if event.msg_id:
        idem_key = f"idempotency:{event.tenant_id}:{event.channel_type}:{event.msg_id}"
        acquired = await ctx.storage.idempotency.try_acquire(idem_key)
        if not acquired:
            return AgentResponse(response_type=ResponseType.ERROR,
                                 content="duplicate message",
                                 session_id=event.session_id,
                                 tenant_id=event.tenant_id,
                                 channel_type=event.channel_type,
                                 trace_id=event.trace_id)

    result = await chain.run(ctx, event)
    if not result.passed:
        if idem_key:
            await ctx.storage.idempotency.release(idem_key)
        error = result.error
        reason = getattr(error, "reason", str(error))
        return AgentResponse(response_type=ResponseType.ERROR,
                             content=reason,
                             session_id=event.session_id,
                             tenant_id=event.tenant_id,
                             channel_type=event.channel_type,
                             trace_id=event.trace_id)

    response = await runtime.handle(event)
    if response.response_type == ResponseType.ERROR and idem_key:
        await ctx.storage.idempotency.release(idem_key)
    return response
