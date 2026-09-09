# tenant 模块单元测试
import asyncio

import pytest

from trpc_service.tenant import (
    TenantConfig,
    TenantRegistry,
    generate_session_id,
    parse_webhook_path,
    resolve_tenant_id,
    tenant_from_dict,
)


def test_tenant_model_defaults():
    t = TenantConfig(tenant_id="t1", name="x")
    assert t.is_active
    assert not t.budget_exceeded()
    assert t.tools.is_allowed("any")


def test_tenant_from_flat_dict_prd_style():
    flat = {
        "tenant_id": "t2",
        "name": "扁平",
        "app_config": {
            "system_prompt": "你好"
        },
        "tool_permissions": {
            "allowlist": ["search"]
        },
        "data_backend_config": {
            "session": "redis"
        },
    }
    t = tenant_from_dict(flat)
    assert t.app.system_prompt == "你好"
    assert t.backends.session == "redis"
    assert not t.tools.is_allowed("other")


def test_tool_permissions_priority():
    t = TenantConfig(tenant_id="t", name="x", tools={"allowlist": ["a"], "blocklist": ["a"]})
    assert not t.tools.is_allowed("a")  # 黑名单优先


def test_session_id_deterministic():
    a = generate_session_id("t1", "wechat_work", "corp1", "u1")
    b = generate_session_id("t1", "wechat_work", "corp1", "u1")
    c = generate_session_id("t1", "wechat_work", "corp1", "u2")
    assert a == b and a != c and len(a) == 32


def test_resolve_tenant_priority():
    assert resolve_tenant_id(headers={"X-Tenant-ID": "h1"}) == "h1"
    assert resolve_tenant_id(host="tenantA.gateway.example.com") == "tenantA"
    assert resolve_tenant_id(path="/webhook/wechat_work/t1__cb1") == "t1"
    assert resolve_tenant_id(query={"tenant_id": "q1"}) == "q1"


def test_parse_webhook_path():
    assert parse_webhook_path("/webhook/wechat_work/t1__cb1") == ("wechat_work", "t1__cb1")
    assert parse_webhook_path("/nope") == (None, "")


@pytest.mark.asyncio
async def test_registry_lru_and_reload():
    loaded = []

    async def load_fn(tid):
        loaded.append(tid)
        return {"tenant_id": tid, "name": f"租户{tid}"}

    reg = TenantRegistry(load_fn=load_fn, capacity=2)
    await reg.get("a")
    await reg.get("b")
    await reg.get("a")
    await reg.get("c")
    assert len(reg) == 2  # b 被 LRU 挤出
    reg.invalidate("c")
    assert len(reg) == 1


@pytest.mark.asyncio
async def test_registry_get_or_raise():
    reg = TenantRegistry(load_fn=lambda tid: asyncio.sleep(0) or None)
    with pytest.raises(KeyError):
        await reg.get_or_raise("missing")
