# 微信 / 企微 / 飞书 IM SDK 调研分析

> 项目代号 **本平台**。本文档只保留对候选 SDK 的**调研与分析结论**；
> 平台通道的实际实现/落地口径见 `PRD.md` §3。
> 调研日期：2026-09-02。方法：逐仓库 GitHub/PyPI 元数据核实 + 隔离 venv import 冒烟。
>
> ⚠️ **调研留档说明（2026-09-06）**：本文档为历史调研记录。**公众号（wechat_mp）与
> Telegram 通道已从平台移除**（账号未认证无法真连 / 国内网络不可达，不满足「代码实现 +
> 真实可用」的交付口径）；微信客服从未实现为代码。保留通道为企微（自建应用 + 智能机器人
> 长连接）与飞书（手写 webhook + SDK 长连接）。本文下述公众号/微信客服/Telegram 相关
> SDK 候选分析不再对应在库代码，仅作决策留档。

## 平台约束（选型判据）

| 约束 | 说明 |
| --- | --- |
| 全异步 | 平台为 FastAPI + httpx/asyncio；同步 SDK 需线程池桥接，破坏链路 |
| 双向能力 | 既要「接收：回调/事件 验签解密」也要「主动投递回复」，不只推送 |
| 低依赖 | 密钥不落库、可无网单测、依赖面可控 |
| 账号资质墙 | 公众号=个人未认证订阅号；微信客服=需认证企业微信——SDK 只能解决协议层，解不了资质层 |

## SDK 全景核对表（2026-09-02 实测）

| 生态 | 仓库 | 状态（当日实测） | 同步/异步 | 覆盖能力 | 分析结论 |
| --- | --- | --- | --- | --- | --- |
| 企微 | WecomTeam/wecom-aibot-python-sdk | ✅ 2026-03 活跃 | **asyncio** | 智能机器人长连接收发/流式/卡片/事件 | 官方可用 → 已采用（wecom_bot 通道） |
| 企微 | WecomTeam/wecom-unified | ✅ 2026-08 活跃 | 同步 CLI | 消息/邮件/文档/日程/通讯录等业务 API | CLI 套件，非嵌入式回调 IM SDK |
| 企微 | WecomTeam/wecom-openclaw-plugin | ✅ 2026-08 活跃 | TypeScript | OpenClaw 接入企微工作流 | 非本 Python 平台组件 |
| 企微 | wechatpy（enterprise） | ✅ 2026-05 活跃 ★4.3k | 同步 requests | 企微能力仅 2.0 **alpha** | 不稳定 + 同步，不采用 |
| 企微 | quanttide/wecom-sdk-py | ⚠️ 2022 停更 | 同步 | 基础 OpenAPI | 停更，排除 |
| 企微 | GentleCP/corpwechatbot | ✅ 2025-05 ★349 | 同步 | **只推不收** | 缺回调/解密，排除 |
| 公众号 | wechatpy | ✅ 2026-05 活跃 ★4.3k | **同步 requests** | 回调/验签/AES/客服消息全功能 | 唯一成熟候选但同步；且账号资质墙挡真连 |
| 公众号 | heyshop/python-weixin | ❌ 2016 停更 ★0 | 同步 | 公众号+开放平台 | 死库，排除 |
| 公众号 | penxxy/wechat-publisher | ✅ 2025-06 ★9 | 同步 | 只做内容发布 | 非 IM 收发，排除 |
| 微信客服 | wechatpy 2.0 alpha（wework） | 不稳定 | 同步 | 需认证企业微信 | 不稳定 + 资质墙，排除 |
| 微信客服 | RaphuStudio/SmartServe-Engine | ⚠️ 2026-05 新建 ★0 | — | 客服会话引擎 | 过新/定位待观察，不作依赖 |
| 飞书 | **larksuite/oapi-sdk-python（lark-oapi）** | ✅ 2026-08 活跃 ★551 | **异步可用**（httpx+websockets；`lark_oapi.channel` 长连接 + 事件，实测可 import） | IM 收发/长连接事件订阅/文档/审批全量 OpenAPI | 官方且满足全部约束 → 已采用（feishu_sdk 通道） |
| 飞书 | HUST-wjc/feishu_tools | ⚠️ ★7 | 同步 requests | 仅多维表格/文档 | 无 IM，排除 |
| 飞书 | go-lark/awesome-lark | ✅ | 清单 | 汇总资源 | 非 SDK |
| 个人号 | python-wechaty / WeChatFerry / ItChat | 活跃但个人号 | — | 个人号自动化 | ToS/封号风险，生产不适用 |
| 飞书(存疑) | larksuite/lark-channel-sdk | ❌ GitHub 404 | — | 用户提供仓库名不存在 | 排除；真实为 larksuite/channel-sdk-python（lark-oapi 同源独立包） |

> 实测证据：`wechatpy 1.8.18` 依赖 `requests`（同步）；`lark-oapi 1.7.3` 依赖 `websockets+httpx`（异步传输入口），
> 隔离 venv 中 `lark_oapi.channel.FeishuChannel` 可 import/构造（无需补 protobuf）。

## 逐通道分析结论

### 公众号 —— 资质墙，SDK 无法解锁
- 账号现状为个人未认证订阅号（无服务器配置入口/客服消息权限）→ 任何 SDK 都无法真连。
- 候选分析：唯一成熟 `wechatpy` 能力全但**同步**；`python-weixin` 死库；`wechat-publisher` 只做发布。
- 结论：维持手写协议实现（等账号升级认证后可直接用）；异步架构下不引入同步 SDK。

### 微信客服 —— 资质墙 + 无稳定 Python SDK
- 需要**认证企业微信**（当前不具备）；企微客服无稳定官方 Python SDK（wechatpy work 在 alpha；第三方过新）。
- 结论：维持「代码实现 + 文档」降级口径。

### 企业微信（自建应用 / 智能机器人）
- **自建应用**（HTTP 回调 + 应用消息推送）：无官方 Python SDK → 手写官方协议（SHA1 验签 + AES-256-CBC）。
- **智能机器人·长连接**：官方 `wecom-aibot-python-sdk` 为 WSS 私有协议、asyncio 原生 → 官方 SDK 已采用。
- 结论：形态决定实现——HTTP 回调手写，长连接用官方 SDK（见 LOG 对应章节）。

### 飞书 —— 官方 lark-oapi 满足全部约束，双实现并存
- 手写 webhook 版：收发双向已闭环（小型化、零额外依赖）；
- 官方 SDK 版（`lark_oapi.channel.FeishuChannel` 长连接）：免公网回调域名 + 官方背书 → 已作为并存实现落地。
- 分析要点：飞书不存在「企微那种应用 vs 独立机器人」并列形态，机器人为应用能力之一；
  长连接是事件订阅通道选择（与 webhook 二选一，不可双活）。

## 结论一句话
> 公众号/微信客服 = **账号资质墙**，SDK 无法解锁；异步约束下官方可用路线仅两条——
> 企微智能机器人长连接（`wecom-aibot-python-sdk`）与飞书（`lark-oapi`），均已落地。
