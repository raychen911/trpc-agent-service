# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Offline simulation of the 3-tenant SaaS demo (no IM / LLM credentials).

Demonstrates multi-tenant isolation and deterministic session routing by
sending the same user message to all three tenants through the worker.

Run::

    python simulate.py
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trpc_service import InboundMessage  # noqa: E402
from trpc_service import TenantConfigManager  # noqa: E402
from trpc_service import TenantWorker  # noqa: E402
from trpc_service import generate_session_id  # noqa: E402
from trpc_service import load_tenants  # noqa: E402
from trpc_service.channels import CHAT_PRIVATE  # noqa: E402
from trpc_service.web.app import create_session_service  # noqa: E402

from agent import create_agent  # noqa: E402

TENANTS_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tenants.yaml")


async def main() -> None:
    manager = TenantConfigManager()
    for tenant in load_tenants(TENANTS_CONFIG):
        manager.register(tenant)

    worker = TenantWorker(
        manager=manager,
        agent_factory=create_agent,
        session_service_factory=create_session_service,
    )

    print("=" * 60)
    print("3 租户 SaaS 客服中台演示（离线 mock 模式）")
    print("=" * 60)

    # 同一个用户 u1 向三个租户发同样的问题 —— 隔离 + 每租户不同指令
    message = "我的订单到哪了？"
    for tenant_id in ["tenant_ecom", "tenant_logi", "tenant_fin"]:
        inbound = InboundMessage(
            channel="wecom",
            chat_id="u1",
            chat_type=CHAT_PRIVATE,
            sender_id="u1",
            message_id=f"{tenant_id}-1",
            text=message,
        )
        reply = await worker.handle(tenant_id, "wecom", inbound)
        session_id = generate_session_id(tenant_id, "wecom", CHAT_PRIVATE, "u1", "u1")
        print(f"\n[{tenant_id}] session={session_id[:12]}…")
        print(f"  回复: {reply!r}")

    # session 路由确定性：同一租户+用户 会话 id 稳定，跨租户不同
    print("\n" + "-" * 60)
    print("session_id 路由验证（同一用户 u1）：")
    ids = {
        tid: generate_session_id(tid, "wecom", CHAT_PRIVATE, "u1", "u1")
        for tid in ["tenant_ecom", "tenant_logi", "tenant_fin"]
    }
    for tid, sid in ids.items():
        print(f"  {tid}: {sid[:16]}…")
    assert len(set(ids.values())) == 3, "跨租户 session 必须隔离"
    print("  ✓ 三个租户的 session_id 互不相同（隔离生效）")

    print("\n" + "=" * 60)
    print("演示完成。")
    print("接入真实 IM/LLM：设置 TRPC_AGENT_API_KEY 后运行 run_gateway.py，")


if __name__ == "__main__":
    asyncio.run(main())
