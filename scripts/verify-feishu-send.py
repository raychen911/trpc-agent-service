#!/usr/bin/env python3
"""飞书真实发送联调: 直接驱动 FeishuAdapter.send_message 发一条真实消息。"""
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import trpc_service.config.loader as _cl  # noqa: F401,E402 - 触发 load_dotenv


async def main() -> None:
    app_id = os.environ.get("FEISHU_APP_ID", "")
    secret = os.environ.get("FEISHU_APP_SECRET", "")
    target = os.environ.get("FEISHU_DEMO_USER_OPEN_ID", "")
    if not (app_id and secret and target):
        print("[错误] 未配置 FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_DEMO_USER_OPEN_ID")
        sys.exit(1)

    from trpc_service.channels.feishu import FeishuAdapter
    from trpc_service.events import AgentResponse
    from trpc_service.tenant.models import ImChannelConfig

    config = ImChannelConfig(channel_type="feishu", app_id=app_id, secret_ref=secret, default_target=target)
    adapter = FeishuAdapter(config)
    msg = AgentResponse.text("Teneuris 飞书真实联调: 这条消息来自真实 im/v1/messages 接口。",
                             tenant_id="demo",
                             channel_type="feishu")
    msg.metadata["user_id"] = target
    print(f"app_id: {app_id}")
    print(f"target: {target}")
    try:
        await adapter.send_message("demo", msg)
        print("✅ 飞书发送成功，请到飞书 App 查看是否收到。")
    except Exception as exc:  # noqa: BLE001 - 联调脚本打印友好错误（对齐 wecom 脚本）
        print(f"❌ 发送失败: {type(exc).__name__}: {exc}")
        sys.exit(1)
    finally:
        await adapter.close()


if __name__ == "__main__":
    asyncio.run(main())
