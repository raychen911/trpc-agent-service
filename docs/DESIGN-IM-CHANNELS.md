# 详设 3 · IM 软件接入

> 主文档：[PRD.md §3](PRD.md)　|　验证证据：[VERIFICATION.md](VERIFICATION.md)
> 本文为该章节的完整详设（spec 深度层）；与代码/实测不一致时，以后者为准。
> 小节编号沿用原 PRD 章号（如本篇 §N.x）；跨篇 § 引用指向对应编号的详设文件。

### 3.1 Channel Adapter 抽象

统一接口，每类 IM 一个实现（**抽象一个 IM 类，适配企微/微信/飞书**——老师建议）：

```python
from abc import ABC, abstractmethod

class IMAdapter(ABC):
    @abstractmethod
    async def parse_webhook(self, body: bytes, headers: dict) -> AgentEvent: ...
    @abstractmethod
    async def send_message(self, tenant_id: str, msg: AgentResponse) -> None: ...
    @abstractmethod
    async def send_streaming(self, tenant_id: str, chunk: AgentResponseChunk) -> None: ...
    @abstractmethod
    def verify_signature(self, body: bytes, signature: str, secret: str) -> bool: ...
    @abstractmethod
    def platform_limits(self) -> PlatformLimits: ...   # max_len / rate_limit / types

    # ---- 多模态（图片 / 文件，对应 §3.6「上传素材」）----
    async def send_media(self, tenant_id: str, msg: MediaMessage) -> None:
        """发送图片 / 文件消息：先经平台上传媒素材（企微 upload media /
        飞书 media API），再发送引用。默认抛 NotImplementedError，
        各通道按平台能力实现（§3.6）。"""
        raise NotImplementedError(f"{self.channel_type} 未实现图片/文件发送")

    # ---- 撤回事件（对应 §3.7）----
    def parse_recall_event(self, body: bytes, headers: dict) -> Optional[RecallEvent]:
        """解析撤回事件回调（企微/飞书支持）。返回 None 表示非撤回事件。
        默认按普通消息解析处理（不识别撤回），通道可覆写。"""
        return None
```

### 3.2 消息转换（外部 IM ↔ Agent 输入 ↔ IM 回复）

- **IM → Agent 输入**：`parse_webhook` 解析平台消息为 `AgentEvent`（tenant_id / user_id / session_id / content / msg_type），`content` 直接作为 `Runner.run_async` 的 `new_message`。
- **Agent 事件流 → IM 回复**：Runtime 订阅 Runner 事件流：
  - `assistant_message` 文本 → 普通文本消息
  - 流式 chunk → `send_streaming` 分片（企微智能机器人流式回复）
  - 结构化结果 → 卡片消息（企业微信 textcard / markdown）

### 3.3 候选通道与选择依据

| 通道                | SDK 成熟度                                | 自测门槛                        | 本方案定位             |
| ------------------- | ----------------------------------------- | ------------------------------- | ---------------------- |
| **企业微信**  | 高（`wechatpy`/官方）                   | 需 corp_id/secret（已自测通过） | **正式通道（已真发闭环）** |
| **企微智能机器人·长连接** | 高（官方 `wecom-aibot-python-sdk`，asyncio 原生，`wecom_bot` 形态） | 工作台建机器人得 bot_id/secret（已真连闭环） | **企微第二接入形态（支持群聊 @，09-02 真连）** |
| **飞书**      | 高（`lark-oapi` 官方；本表手写 webhook 协议） | 个人开发者应用即可（App ID/Secret，无企业认证、无 IP 白名单） | **正式通道（手写 webhook，已真发闭环）** |
| **飞书·SDK 长连接** | 高（官方 `lark-oapi` `FeishuChannel`，asyncio 原生，`feishu_sdk` 形态） | 同一自建应用凭证（已真连闭环） | **第二实现形态（官方 SDK + 长连接，免公网回调）** |
| **Web UI IM** | 自研                                      | 零门槛                          | 本地自测用，**不计入正式 IM** |

> **企微智能机器人·长连接（`wecom_bot`，2026-09-02 接入）**：企微面向 AI 助手的官方 chatbot 形态，
> 创建于「工作台 → 智能机器人（API 模式 · 长连接）」，凭证 `bot_id + secret`。平台以**出站 WSS**
> 连接 `openws.work.weixin.qq.com`（官方 asyncio SDK，`wecom-aibot-python-sdk`，import 名 `aibot`），
> **无需公网回调 URL / echostr 验签**；回调帧携带 `chattype=single|group` + `chatid`（仅群），
> 文本回复走 `msgtype=stream`（`text` 仅欢迎语，实测 40008）。驱动 `channels/wecom_bot.py` 与
> webhook 共用 `runtime.pipeline.process_event` 治理链；`--wecom-bot` 显式启动，**同一 bot 只允许单副本连接**。
> 09-02 实测：认证成功 + 单聊回声 + **群聊 @ 收到回复（不带 @ 不触发）**，单/群 session 按
> `channel_id`（aibotid / chatid）隔离。
> 实现状态：代码 + 单测 + 真实长连接冒烟闭环；仅入站被动回复场景实测，主动推送 API 已具备未演示。

> **飞书官方 SDK 长连接（`feishu_sdk`，2026-09-02 接入）**：与手写 webhook `feishu` **并存的第二实现形态**。
> 基于官方 `lark-oapi` 的 `lark_oapi.channel.FeishuChannel`（`transport="ws"`），出站 WebSocket 长连接
> **免公网回调域名**（解决「回调接收需正式域名」痛点）；归一化 `InboundMessage`（`chat_type=p2p/group/topic`、
> `sender_id`、`content_text`、`mentioned_bot`），回复 `channel.send(chat_id, {"text":…}, {"reply_to": message_id})`。
> SDK 内置去重关闭（`SafetyConfig(dedup=DedupConfig(enabled=False))`）让位平台 msg_id 幂等；
> 驱动 `channels/feishu_sdk.py` 与 webhook 共用 `process_event` 治理链；`--feishu-sdk` 显式启动。
> ⚠️ **同一飞书应用的事件订阅投递方式（长连接 vs webhook）二选一，不可双活**——代码并存 ≠ 同应用双活；
> 如需同时展示两种形态请用两个飞书应用。
> 实现状态：代码 + 11 条无网单测闭环；真实长连接冒烟见 `DEVELOPMENT_LOG`。

> **最终通道（交付口径）**：**企业微信 + 飞书**两种真实可连 IM 通道，均完成代码实现并**真发闭环**
> （企微手机 App 收到 `message/send` 消息；飞书个人应用 1-on-1 私聊收到），满足「至少两种 IM 通道（含微信/企微）」
> 的**代码实现 + 真实可用**要求；企微另有**智能机器人·长连接第二形态**（`wecom_bot`）真连闭环并支持群聊 @；
> 飞书为**手写 webhook + 官方 SDK 长连接双实现并存**（`feishu_sdk`，免公网回调）。
> 公众号 / 微信客服 / Telegram 因账号未认证 / 网络不可达无法真连，**已从平台移除**（代码删除，
> 调研留档见 `docs/IM-SDK-RESEARCH.md`）。
> 回调接收方向（企微自建应用/飞书 webhook）代码已接线 + 单元/模拟 E2E，公网正式域名留老师代验；
> `wecom_bot` / `feishu_sdk` 长连接为出站连接，无公网回调依赖。
> **Web UI IM 仅作为本地自行验证手段，不计入正式 IM 之列**
> （老师 08-28 群答疑明确：Web UI 不能替代「至少两种 IM」）。

### 3.4 IM 账号与租户绑定

- **Webhook URL**：`https://gateway.example.com/webhook/{channel_type}/{binding_id}`，`binding_id` 隐含 tenant_id。
- **验签（各通道不同，不可复用同一份实现）**：
  - **企业微信**：`msg_signature = SHA1(sort(token, timestamp, nonce, encrypt))` + 消息体
    **AES-256-CBC** 加解密（EncodingAESKey → 32 字节密钥，IV 取前 16 字节，填充为官方
    **PKCS7-32 分组**——pad 值 1..32，勿用标准 16 字节 unpadder，09-02 实测真实回调修复）；
  - **飞书**：事件体 `header.token` 比对 `verification_token`；或 `X-Lark-Signature`
    = base64(HMAC-SHA256(timestamp+nonce+body, token))（`feishu.py::_parse_lark_header`
    支持逗号分隔与 query 两种形态；09-02 修复「base64 提取含参数后缀致恒失败」缺陷）；
- **长连接形态（`wecom_bot`，无 HTTP webhook）**：连接由 SDK 建立后自动认证
  （`bot_id + secret` 握手），**无 URL 配置 / echostr / 签名验签**；入站帧以 `msgid` 幂等去重、
  `user_id_mapping` 身份映射、`user_acl` 用户级权限照常生效（与 webhook 共用治理链）。
- **去重**：`msg_id` 幂等（2.3-E）。
- **身份映射**：`user_id_mapping` JSON 规则（external_field → 内部 user_id）——
  企微用 `FromUserName`（成员 UserID），飞书用 `open_id`，语义因平台而异。

### 3.5 群聊与单聊的 Session 隔离

- 单聊：`tenant + channel + user` 唯一确定 session。
- 群聊：每群一个 session（群内共享上下文），或按 `group_id + user_id` 用户级隔离。
- 跨租户隔离：`tenant_id` 是 session_id 前缀因子 + 所有查询强制 tenant 过滤。

> ⚠️ **接入限制（2026-09-02 校准）**：企微**自建应用消息回调**不携带群聊标识（`FromUserName`
> 始终为成员 userid，XML 无 ChatId）——该形态下单聊/群聊不可区分，群聊需群机器人/客户群等形态。
> **企微智能机器人·长连接（`wecom_bot`）已解决此缺口**：回调帧携带 `chattype=single|group`
> 与 `chatid`（仅群聊），群聊 @ 消息按「(群, 人) 用户级隔离」（`channel_id=chatid`）落 session，
> 09-02 真实长连接冒烟验证通过。飞书 SDK 长连接亦实现群聊（`chat_type=group/topic`）。

### 3.6 平台限制处理

| 限制      | 企业微信                   | 飞书                         |
| --------- | -------------------------- | ---------------------------- |
| 消息长度  | 文本 2048 字节，自动分段   | 单条 2000 字，超长拆分回复   |
| 频率      | ~20 次/秒/应用，令牌桶限流 | 5 条/秒                      |
| 异步回复  | 5 秒内先回 ack，再经应用消息接口异步推送 | 支持异步推送（im/v1/messages） |
| 图片/文件 | 需上传素材（media_id）     | 需上传素材（media API）      |
| 失败重试  | 指数退避 + 死信队列        | 失败返回错误码，幂等兜底     |

> **关键差异**：企微回调是「**先 ack 再异步推送**」语义（5 秒内回 ack）；
> 飞书 webhook 支持 URL challenge 验签短路，消息事件经 `verify_signature` 校验后进 Filter 链。
> `send_streaming` 两平台均不支持，长文本累积后整条发（飞书见 LOG「IM 真实可用化：飞书通道」§B）。

### 3.7 消息撤回处理

> ✅ **实现状态（2026-09-06 落地）**：`IMAdapter.parse_recall_event` 已落地
> （`channels/base.py`，默认返回 None=不识别撤回），飞书 `im.message.recalled_v1` 已按官方
> 协议实现识别（`channels/feishu.py`）；webhook 处理链路（`web/app.py`）识别撤回后
> **不触发 Agent**，写审计 `decision=recall` + 尽力标记会话历史 revoked（`mark_message_revoked`
> 纯函数，user 消息落库带 msg_id）。含协议级单测。
>
> ⚠️ **边界如实标注**：企微自建应用消息回调**无撤回事件推送**（用户撤回后应用收不到通知，
> 属平台限制），其 `parse_recall_event` 维持默认 None；飞书撤回事件仅携带 message_id、
> 无会话上下文时仅记审计不做历史定位（RecallEvent.session_id 为空）。**真实平台撤回回调
> 格式最终以公网接收方向代验为准**（老师正式域名）。撤回非 Problem 硬验收，本轮按
> 「接口 + 单测 + 飞书协议就绪」交付，不做"虚构已验证"。

平台按「不阻断、可溯源」原则处理：

```
撤回事件回调 → IMAdapter.parse_recall_event（识别 msg_id 与撤回人）
   → Gateway 不触发 Agent 执行（撤回不是新输入），返回 ack
   → 写审计 decision=recall（含被撤回消息 id / event_type）
   → 若 RecallEvent 能定位 session：历史中该 msg_id 的 user 消息标记 revoked=true
     （保留原始内容，供审计追溯；不从历史中物理删除）——不能定位则仅审计留痕
   → 已发出的对应回复**不强行撤回**（多数 IM 不允许应用侧撤回他人消息）
```

| 决策 | 结论 | 理由 |
|---|---|---|
| 撤回后历史是否删除 | 标记 `revoked=true` 而非物理删除 | 审计要求全量留痕（PRD 4.4）；物理删除会让审计与历史对不上 |
| 撤回是否触发 Agent | 不触发 | 撤回是事件通知，不是用户新输入；触发会产生无意义回复 |
| 已发回复是否撤回 | 不撤回 | IM 平台一般不允许应用撤回已送达消息；平台语义是「知悉并留痕」 |

---
