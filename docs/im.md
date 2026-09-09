# IM 接入与本地可视化验证

项目支持三类 IM，并统一按以下顺序展示：企业微信智能机器人 `wecom`、微信客服 `wecom_kf`、Telegram `telegram`。每个通道负责自己的认证、协议转换、附件收发和错误分类。消息进入平台后，共用租户绑定、身份映射、幂等、任务队列、Runner、Session/Memory、Outbox 和 Delivery。

三类实现遵守同一个接口。`normalize()` 把平台消息转成统一结构，`deliver()` 把平台回复发送出去，主流程不需要判断具体通道：

```python
class ChannelAdapter(Protocol):
    async def normalize(
        self, binding_id: str, payload: dict, headers: dict
    ) -> NormalizedInboundMessage: ...

    async def deliver(self, message: OutboundMessage) -> DeliveryResult: ...
    async def close(self) -> None: ...
```

## 本地页面

在 Anaconda Prompt 中运行：

```cmd
python -m trpc_service._cli im-demo --config examples\config\im-demo.yaml --env-file .env
```

浏览器打开 `http://127.0.0.1:8080/im`。页面默认使用 Fake Model，不联网，也不会产生模型费用。选择“当前配置的真实模型”时，Agent 会读取 `.env` 的模型配置，但 IM 收件和发件仍由 Fake Client 承接。

这个页面不会绕过 IM 逻辑直接调用 Chat API。它先生成对应平台的消息结构，再交给正式 Adapter：

- 企业微信使用 `aibot_msg_callback`、`headers.req_id`、`body.msgid/aibotid/chattype/from/msgtype`；
- 微信客服使用 `open_kfid`、`external_userid`、`msgid`、`origin`、`send_time` 和消息类型字段；
- Telegram 使用 `update_id`、`message.message_id/from/chat/text/photo/document` 和 Webhook secret header。

页面可以查看脱敏后的原始消息、`NormalizedInboundMessage`、`request_id`、配置版本、任务状态、Outbox 状态以及各处理阶段。模拟入口创建 W3C trace context 并随 Queue 和 Outbox 传播。重复发送会复用原请求；测试附件会先通过通道下载协议进入租户 Artifact，再进入队列。

附件框支持多选、逐个移除和全部取消。微信客服、Telegram 等平台通常会把多个附件拆成多条入站消息，模拟器也按这种方式生成并校验每个文件。

同一次页面提交的文字和附件通过批次 ID 聚合为一个 `AgentRequest`。页面保留每条原始消息，但整批内容只生成一个 `request_id`、调用一次 Agent、回复一次。聚合发生在 Adapter 之后，不会改造平台协议。

`im-demo` 会在同一进程中启动本地 Gateway、Worker 和 Delivery，不受终端中 `TRPC_SERVICE_ROLES` 的遗留值影响。页面每 700 毫秒查询一次状态，任务结束后自动停止，也可以手动点击“停止状态查询”。该按钮只停止浏览器轮询，不会取消已经提交的 Agent 任务。

## 三类协议边界

### 企业微信智能机器人

真实运行使用 `wecom-aibot-python-sdk>=1.0.1,<1.1.0`。`WSClient` 负责认证、心跳和重连，Adapter 校验 `aibotid` 是否属于当前 Binding。

单聊以成员 ID 为会话对象；群聊以 `chatid` 为会话对象，同时保留真实发言人。图片和文件按 SDK 给出的 `url + aeskey` 下载，并在入队前保存。回复帧有效时使用 `reply_stream`，过期后使用 `send_message`。

### 微信客服

回调只表示有新消息：平台先验证 SHA1 签名并完成 AES-CBC 解密，再由后台通过 `sync_msg` 分页拉取内容。只有客户消息进入 Agent；坐席消息和会话事件只更新状态。发送前重新检查接待状态，人工接管、回复窗口过期或额度不足时停止自动回复。网络超时且无法判断是否已经发送时，Outbox 进入 `unknown`，不会盲目重发。

### Telegram

Webhook 校验 `X-Telegram-Bot-Api-Secret-Token`，并使用 `update_id` 做幂等。附件根据 `getFile` 返回的路径下载，出站支持 `sendMessage`、`sendPhoto` 和 `sendDocument`。

发送结果同时检查 HTTP 状态和 JSON 中的 `ok`。429 按 `parameters.retry_after` 延迟重试，5xx 可以重试，明确的 4xx 视为永久失败；发送响应丢失时记录为结果未知。

## 平台限制与处理方式

- **消息长度**：入站文字先经过 Binding 的 `max_text_chars` 检查。Telegram 出站按 4096 个 Unicode 字符拆分；微信客服按 2048 字节拆分；企业微信长文本按 2048 个 UTF-8 字节拆成多条主动消息。短企业微信回复仍优先使用原始 Frame 流式回复。
- **频率限制**：三类 Adapter 都会把平台限流识别为可重试错误。平台给出 `retry_after` 时按该时间等待，否则由 Outbox 使用有上限的指数退避；达到最大次数后进入 `dead`，不会无限重试。
- **异步回复**：回调或长连接收件只负责认证、转换和入队。Agent 执行与回复发送由 Worker 和 Delivery 异步完成，因此模型耗时不会占住 Webhook 请求。
- **图片和文件**：三类通道都支持接收图片、下载文件并按租户保存 Artifact。
- **媒体回复**：Telegram 可以发送图片和文件，微信客服每条消息可以发送一个媒体附件。
- **企业微信附件**：项目锁定的 SDK 稳定接口只明确提供流式文本、Markdown、卡片和文件下载。发送附件时会返回 `wecom_outbound_attachment_not_supported`，不会静默丢弃。
- **发送失败**：认证或格式错误属于永久失败；网络连接失败、限流和服务端 5xx 可以重试；已经开始发送但响应丢失时记为 `unknown`，交由人工核查，避免盲目重发。长消息发送部分分片后失败也按 `unknown` 处理。
- **撤回与编辑**：首版不提供跨平台统一撤回 API，也不把未识别的撤回事件当作普通消息执行。各平台撤回期限、权限和接口能力不同，而且撤回回复不能逆转已经发生的 Tool 副作用。后续若增加撤回，必须由各 Adapter 显式声明能力，保存平台送达的编辑或撤回事件及审计，并使用外部消息 ID 调用对应平台接口。

## 本地验证边界

本地 UI 能证明协议解析、身份映射、附件入库、幂等、Agent 执行、会话保存、Outbox 和投递状态机能够连通，也能稳定复现限流、超时、人工接管等分支。它不能证明真实平台的账号权限、公网回调、实际限额或网络质量。

老师提供真实凭据后，只需在正式租户配置中增加对应 Channel Binding，并通过环境变量提供 Secret 引用；无需修改 Adapter。企业微信智能机器人还需要用带 `wecom` 角色的进程建立长连接，微信客服需要公网 HTTPS 回调，Telegram 需要向 Bot API 注册 Webhook。

### 统一真实账号测试入口

复制 `.env.example` 为 `.env`，只填写实际需要验证的通道。密钥只放在 `.env`，不要写进 YAML 或提交 Git。

```env
WECOM_BOT_ID=
WECOM_BOT_SECRET=

WECOM_KF_CORP_ID=
WECOM_KF_OPEN_KFID=
WECOM_KF_SECRET=
WECOM_KF_CALLBACK_TOKEN=
WECOM_KF_ENCODING_AES_KEY=
WECOM_KF_TEST_EXTERNAL_USER_ID=

TELEGRAM_BOT_TOKEN=
TELEGRAM_TEST_CHAT_ID=
```

三个通道均有凭据时运行：

```cmd
python -m trpc_service._cli demo im-live --env-file .env --channels all --confirm --json
```

只有部分账号时，使用一个通道名称或逗号分隔的列表：

```cmd
python -m trpc_service._cli demo im-live --env-file .env --channels wecom --confirm --json
python -m trpc_service._cli demo im-live --env-file .env --channels wecom,wecom-kf --confirm --json
```

执行顺序固定为企业微信、微信客服、Telegram。某个通道失败时，命令仍会继续检查其余通道，最后统一输出 `passed`、`failed` 和每个通道的结果。全部成功时进程退出码为 0；任一通道失败时退出码为 1，便于验收脚本判断。

正确结果示例：

```json
{
  "scenario": "im-live",
  "passed": 3,
  "failed": 0,
  "results": [
    {"channel": "wecom", "status": "passed", "detail": {"authenticated": true}},
    {"channel": "wecom-kf", "status": "passed", "detail": {"delivered": true}},
    {"channel": "telegram", "status": "passed", "detail": {"delivered": true}}
  ]
}
```

企业微信项目验证真实 BotID/Secret 能否建立并认证长连接。微信客服和 Telegram 会向 `.env` 指定的测试用户真实发送 `trpc-agent-service live test`，所以测试对象必须属于验收环境；`--confirm` 用于避免误发。

原有的单通道命令继续保留，主要用于开发排错：

```cmd
python -m trpc_service._cli demo wecom-live --json
python -m trpc_service._cli demo wecom-kf-live --json
python -m trpc_service._cli demo telegram-live --json
```

单通道旧命令直接读取当前进程环境变量，不负责加载 `.env`。评审时优先使用上面的 `im-live --env-file .env`。所有 Live 命令都不会被默认测试调用。

### 完整真实收发验证边界

统一入口用于快速验证账号、网络和真实发送能力，不代替公网回调的端到端测试。需要验证真实收件时，还要完成以下配置：

- 企业微信：在租户配置中增加 `wecom` Binding，并启动带 `wecom` 角色的进程；认证成功后从企业微信向机器人发送消息。
- 微信客服：使用 `examples\config\wecom-kf.yaml`，把公网 HTTPS 回调配置为 `/api/v1/channels/{binding_id}/webhook`，再由测试客户发送消息。
- Telegram：增加 `telegram` Binding，把 Bot Webhook 注册到同一路径，并配置 Webhook Secret。

收到真实消息后，应看到 Request 进入 `succeeded`、Outbox 进入 `delivered`，并能用同一 `request_id` 对齐审计与 Trace。若验收环境只提供 BotID/Secret，不提供公网回调条件，可用统一 Live 入口验证账号连接，再用本地 UI 验证完整协议链路。

## 自动测试

```cmd
python -m pytest tests\test_channels.py tests\test_customer_service.py tests\test_v3_channels_migration.py tests\test_im_visual_demo.py -p no:cacheprovider -vv
```

该命令只测试本地协议和完整 Fake 闭环。正确结果是全部通过：三类消息均完成 Adapter、Queue、Runner、Outbox 和 Fake Delivery；重复消息只产生一个请求；附件在入队前落入 Artifact；开发接口只在 `im-demo` 中开放。
