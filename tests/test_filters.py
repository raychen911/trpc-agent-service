"""治理过滤器测试：白名单、脱敏、预算、危险确认。"""
from trpc_agent_sdk.filter import get_tool_filter
from trpc_agent_sdk.tools._context_var import set_tool_var, reset_tool_var

import trpc_service.tenant.governance  # noqa: F401  # 注册过滤器
from trpc_service.tenant.governance.budget_limit import budget_manager
from trpc_service.tenant.governance.pii_mask import mask_text
from trpc_service.metrics.context import build_agent_context


class _FakeTool:
    def __init__(self, name):
        self.name = name


async def _ok():
    return {"ok": True}


async def _run_filter(filter_name, tool_name, args=None, tenant_id="tenant_001", confirmed_meta=False):
    """在 fake 工具上下文中执行过滤器，返回 (rsp, is_continue)。"""
    tool = _FakeTool(tool_name)
    token = set_tool_var(tool)
    try:
        f = get_tool_filter(filter_name)
        assert f is not None, f"过滤器 {filter_name} 未注册"
        ctx = build_agent_context(tenant_id, "u1", "s1")
        if confirmed_meta:
            ctx.with_metadata("tool_confirmed", True)
        result = await f.run(ctx, args or {}, _ok)
        return result.rsp, result.is_continue
    finally:
        reset_tool_var(token)


async def test_whitelist_allows_listed_tool():
    rsp, cont = await _run_filter("tool_whitelist", "query_order")
    assert cont is True
    assert rsp == {"ok": True}


async def test_whitelist_blocks_unlisted_tool():
    # tenant_001 的 allowed_tools 不含 search_code
    rsp, cont = await _run_filter("tool_whitelist", "search_code")
    assert cont is False
    assert rsp["error"] == "tool_not_allowed"


async def test_whitelist_blocks_blocked_tool():
    rsp, cont = await _run_filter("tool_whitelist", "modify_order", args={"confirm": False})
    # modify_order 不在 tenant_001 的 allowed_tools → 白名单先拦
    assert cont is False
    assert "blocked" in rsp["status"]


def test_mask_text():
    masked = mask_text("手机 13812345678 邮箱 a@b.com 密钥 sk-abcdef1234567890")
    assert "13812345678" not in masked
    assert "sk-abcdef1234567890" not in masked
    assert "****" in masked


async def test_budget_blocks_when_exhausted():
    budget_manager.reset("tenant_002")
    budget_manager.record("tenant_002", api_calls=999999, tokens=99999999)
    rsp, cont = await _run_filter("budget_limit", "query_docs", tenant_id="tenant_002")
    assert cont is False
    assert rsp["error"] == "budget_exceeded"
    budget_manager.reset("tenant_002")


async def test_dangerous_requires_confirm():
    # modify_order 属危险名单；tenant_002 名单里有 search_code/query_docs，
    # modify_order 未在 allowed 中，白名单会先拦，这里直接用 tenant_002 且放行名单外不行 ——
    # 危险过滤器单独验证：confirm=false 时拦截
    rsp, cont = await _run_filter("dangerous_confirm", "modify_order", args={"confirm": False}, tenant_id="tenant_002")
    assert cont is False
    assert rsp["error"] == "confirmation_required"


async def test_dangerous_passes_with_confirm_arg():
    rsp, cont = await _run_filter("dangerous_confirm", "modify_order", args={"confirm": True}, tenant_id="tenant_002")
    assert cont is True


async def test_dangerous_passes_with_meta_confirm():
    rsp, cont = await _run_filter(
        "dangerous_confirm", "modify_order", args={}, tenant_id="tenant_002", confirmed_meta=True
    )
    assert cont is True
