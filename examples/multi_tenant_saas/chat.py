# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Interactive CLI for the 3-tenant SaaS customer-service demo.

Lets you chat with each tenant's agent interactively, demonstrating
multi-tenant isolation (separate session / instruction / tools per tenant).

Runs fully offline with a deterministic mock agent. Set TRPC_SERVICE_MODEL_API_KEY to
talk to a real LLM instead.

Run::

    python chat.py

Commands inside the REPL:
    /tenant            list tenants and show current
    /switch <id|name>  switch active tenant (e.g. /switch tenant_logi)
    /model             show whether using mock or real LLM
    /help              show this help
    /quit              exit
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trpc_service import CHAT_PRIVATE
from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import load_tenants
from trpc_service.web.app import create_session_service

from agent import create_agent

TENANTS_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tenants.yaml")
CLI_USER = "cli_user"
CHANNEL = "cli"


def _short(sid: str) -> str:
    return sid[:16] + "…" if len(sid) > 16 else sid


async def main() -> None:
    manager = TenantConfigManager()
    tenants = load_tenants(TENANTS_CONFIG)
    for t in tenants:
        manager.register(t)

    worker = TenantWorker(
        manager=manager,
        agent_factory=create_agent,
        session_service_factory=create_session_service,
    )

    tenant_by_key = {t.tenant_id: t for t in tenants}
    for t in tenants:  # also index by display name
        tenant_by_key.setdefault(t.name, t)

    current = tenants[0]
    real_llm = bool(os.environ.get("TRPC_SERVICE_MODEL_API_KEY"))

    def render_tenants() -> str:
        lines = []
        for t in tenants:
            mark = "▶" if t.tenant_id == current.tenant_id else " "
            lines.append(f"  {mark} {t.tenant_id:<14} {t.name}  (model={t.model.model_name})")
        return "\n".join(lines)

    print("=" * 64)
    print("  多租户 SaaS 客服中台 · 交互式 CLI")
    print("=" * 64)
    print(f"  模式: {'真实 LLM' if real_llm else '离线 Mock（无需 API key）'}")
    print("  可用租户:")
    print(render_tenants())
    print("  输入 /help 查看命令。直接打字即可发消息。")
    print("=" * 64)

    while True:
        try:
            text = input(f"\n[{current.name}] 你> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见。")
            break

        if not text:
            continue

        if text.startswith("/"):
            parts = text[1:].split(None, 1)
            cmd, arg = parts[0], (parts[1] if len(parts) > 1 else "")
            if cmd in ("quit", "exit", "q"):
                print("再见。")
                break
            elif cmd == "help":
                print("命令:\n"
                      "  /tenant            列出租户并显示当前\n"
                      "  /switch <id|name>  切换当前租户\n"
                      "  /model             显示当前模型模式\n"
                      "  /help              显示帮助\n"
                      "  /quit              退出")
            elif cmd == "tenant":
                print("可用租户:")
                print(render_tenants())
                print(f"当前: {current.tenant_id} ({current.name})")
            elif cmd == "model":
                print("真实 LLM" if real_llm else "离线 Mock（设置 TRPC_SERVICE_MODEL_API_KEY 切换）")
            elif cmd == "switch":
                target = tenant_by_key.get(arg.strip())
                if not target:
                    print(f"未知租户: {arg!r}。用 /tenant 查看可用租户。")
                else:
                    current = target
                    print(f"已切换到: {current.tenant_id} ({current.name})")
            else:
                print(f"未知命令: /{cmd}。输入 /help 查看帮助。")
            continue

        inbound = InboundMessage(
            channel=CHANNEL,
            chat_id=CLI_USER,
            chat_type=CHAT_PRIVATE,
            sender_id=CLI_USER,
            message_id=f"{current.tenant_id}-{abs(hash(text)) % 10**9}",
            text=text,
        )
        try:
            reply = await worker.handle(current.tenant_id, CHANNEL, inbound)
        except Exception as exc:  # noqa: BLE001
            print(f"  [worker 错误] {exc!r}")
            continue
        print(f"[{current.name}] 客服> {reply}")


if __name__ == "__main__":
    asyncio.run(main())
