#!/usr/bin/env python3
# ===================================================================
# scripts/verify-wecom-send.py - 企微真实发送联调（阶段二.13 遗留闭环）
# ===================================================================
# 前置: .env 中已配置企微凭证（WECOM_CORP_ID / WECOM_AGENT_ID /
#       WECOM_SECRET / WECOM_TOKEN / WECOM_AES_KEY），见 .env.example
# 用途: 不经 HTTP 服务，直接驱动 WechatWorkAdapter.send_message 发起
#       一次真实消息投递 —— 验证「gettoken + message/send」真实链路。
# 用法: python scripts/verify-wecom-send.py [企微成员userid] [消息内容]
#       不传成员时读取环境变量 WECOM_TO_USER；两者都缺失则报错退出
#       （不设无效兜底默认值——发给不存在的成员只会得到 81013 误导排查）
# ===================================================================

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# 确保可从仓库根目录 import trpc_service
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 触发 config.loader 的 load_dotenv()（不覆盖已 export 的环境变量）
import trpc_service.config.loader as _config_loader  # noqa: F401,E402


def _require(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        print(f"[错误] 未配置 {name}。请在 .env 中填写（参考 .env.example），或 export {name}=xxx")
        sys.exit(1)
    return value


async def main() -> None:
    corp_id = _require("WECOM_CORP_ID")
    agent_id = _require("WECOM_AGENT_ID")
    secret = _require("WECOM_SECRET")
    token = os.environ.get("WECOM_TOKEN", "")
    aes_key = os.environ.get("WECOM_AES_KEY", "")

    touser = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("WECOM_TO_USER", "")
    if not touser:
        print("[错误] 未指定目标成员。用法: python scripts/verify-wecom-send.py <成员userid>"
              " 或在 .env 配置 WECOM_TO_USER（企业微信管理后台 -> 通讯录 -> 成员账号）")
        sys.exit(1)
    content = sys.argv[2] if len(sys.argv) > 2 else "Teneuris 企微真实联调: 这条消息来自真实 message/send 接口。"

    from trpc_service.channels.wechat_work import WechatWorkAdapter
    from trpc_service.events import AgentResponse
    from trpc_service.tenant.models import ImChannelConfig

    config = ImChannelConfig(
        channel_type="wechat_work",
        app_id=corp_id,
        agent_id=agent_id,
        secret_ref=secret,
        token_ref=token,
        aes_key_ref=aes_key,
    )
    adapter = WechatWorkAdapter(config)
    msg = AgentResponse.text(content, tenant_id="demo", channel_type="wechat_work")
    msg.metadata["user_id"] = touser

    print("=== 企微真实发送联调 ===")
    print(f"corp_id : {corp_id}")
    print(f"agent_id: {agent_id}")
    print(f"touser  : {touser}")
    print(f"content : {content[:60]}{'...' if len(content) > 60 else ''}")
    print()

    try:
        await adapter.send_message("demo", msg)
    except Exception as exc:  # noqa: BLE001 - 联调脚本打印完整错误
        print(f"❌ 发送失败: {type(exc).__name__}: {exc}")
        sys.exit(1)
    finally:
        await adapter.close()

    print("✅ 发送成功。请到企业微信 App 里查看是否收到这条消息。")
    print("   收到 = 发送链路（gettoken + message/send）真实打通，可进入老师代验阶段。")
    print("   未收到 = 检查成员 userid 是否正确（企业微信管理后台 -> 通讯录），或代理/网络可达性。")


if __name__ == "__main__":
    asyncio.run(main())
