# ===================================================================
# storage.migration - 后端数据迁移（PRD 2.3-D 可执行版）
# ===================================================================
# 说明: PRD 2.3-D 四阶段（双写→回填→切读→下线）的**数据回填/校验**部分。
#   当前范围 = Summary 数据域跨后端拷贝（Redis↔SQL↔InMemory，三者均有真实
#   实现，最贴题面「从 Redis 迁移到 SQL」例子）。Session/Memory 因缺 SQL
#   后端暂不纳入（目标后端实现后可扩展本模块，不虚构"支持"）。
#
#   使用方式（双写期 + 切读仍由运维按 Admin 租户 backends 热切执行，
#   StorageManager.invalidate 已支持）:
#     1. 旧后端继续写（双写期可选，本工具不新增双写代码）
#     2. 本工具 copy_summaries: 旧后端 → 新后端（回填）
#     3. verify_summaries: 校验两遍一致（重复执行可收敛）
#     4. Admin 切换租户 backends 到新后端（切读）→ 观察 → 下线旧后端
# 规范: copy 逐条 save（save 语义幂等：先删后插）；单条失败记录并继续，
#   最终由 verify 暴露差异——Summary 低频少量，重跑 copy 即可收敛。
# ===================================================================

from __future__ import annotations

from typing import Any

from ..log.logger import get_logger

log = get_logger("storage.migration")


async def copy_summaries(src: Any, dst: Any, tenant_id: str) -> int:
    """把源后端某租户的全部 Summary 拷贝到目标后端（幂等回填）。

    Args:
        src: SummaryStore（源，需实现 list_summaries）
        dst: SummaryStore（目标）
        tenant_id: 租户

    Returns:
        拷贝条数；单条失败记录日志并跳过（最终以 verify 暴露差异）。
    """
    entries = await src.list_summaries(tenant_id)
    copied = 0
    for session_id, content in entries:
        try:
            await dst.save_summary(tenant_id, session_id, content)
            copied += 1
        except Exception as exc:  # noqa: BLE001 - 单条失败不中断整体迁移
            log.warning("summary migrate item failed",
                        extra={
                            "tenant_id": tenant_id,
                            "session_id": session_id,
                            "error": str(exc)
                        })
    log.info("summary migrated", extra={"tenant_id": tenant_id, "source_total": len(entries), "copied": copied})
    return copied


async def verify_summaries(a: Any, b: Any, tenant_id: str) -> dict[str, Any]:
    """校验两端 Summary 集合一致（迁移正确性检查）。

    Returns:
        {"source": int, "target": int, "matched": int, "mismatched": list[(session_id,)]}
    """
    set_a = set(await a.list_summaries(tenant_id))
    set_b = set(await b.list_summaries(tenant_id))
    matched = len(set_a & set_b)
    mismatched = [sid for sid, _ in (set_a ^ set_b)]
    return {
        "source": len(set_a),
        "target": len(set_b),
        "matched": matched,
        "mismatched": mismatched,
    }
