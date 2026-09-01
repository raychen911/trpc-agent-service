# 核心链路时序图

本文档展示“企业微信用户发消息 -> Agent 执行 -> Tool 调用 -> Session / Memory 写入 -> IM 回复”的完整链路。其他 IM 通道，如微信公众号、微信客服、Telegram，也遵循同一标准入站消息和出站消息模型，只是在 Channel Adapter 层处理各自的平台协议。

```mermaid
sequenceDiagram
    participant U as 企业微信用户
    participant IM as 企业微信平台
    participant CA as 企业微信 Channel Adapter
    participant GW as Agent Gateway
    participant RQ as Redis Run Queue
    participant W as Agent Worker
    participant P as Tenant Filter / Policy
    participant K as Tool / MCP / Knowledge
    participant M as Model Provider
    participant S as Redis / PostgreSQL / Vector Store
    participant O as IM Outbound Delivery
    participant OT as OpenTelemetry Collector

    U->>IM: 发送文本/图片/文件消息
    IM->>CA: Webhook XML/JSON + signature + timestamp + nonce
    CA->>CA: 校验 Token/签名，按需解密，提取 MsgId 和用户身份
    CA->>GW: 标准 InboundMessage + trace_id
    GW->>S: 通过 channel + account_id 查询 ChannelBinding
    GW->>S: 生成 session_id，检查 idempotency_key
    alt 重复消息且已完成
        S-->>GW: 返回已保存 response_ref / result_json
        GW-->>O: 复用已完成回复
        O-->>IM: 平台出站回复
        IM-->>U: 用户收到回复
    else 新消息
        GW->>S: 检查并预留 QPS / Token / Cost 配额
        GW->>RQ: 写入带租户和配置版本的 RunRequest
        RQ->>W: Worker 消费请求
        W->>S: 加载租户配置、Session event/state、Memory、Summary
        W->>P: 输入长度、敏感信息、IM 用户权限校验
        opt 需要知识检索
            W->>K: 按租户 Knowledge 后端检索 top-k chunk
            K-->>W: 返回检索结果和引用元数据
        end
        opt 需要工具或 MCP 调用
            W->>P: 校验工具白名单、审批规则、危险操作策略
            W->>K: 执行已授权 Tool / MCP Server
            K-->>W: 返回工具结果或审批状态
        end
        W->>M: 携带上下文、工具结果和模型配置调用模型
        M-->>W: 返回 Agent Event、回复文本和 token usage
        W->>S: 追加 user / assistant / tool message_event
        W->>S: CAS 更新 session_state
        W->>S: 写入 Memory、Summary、Audit Log；失败则写入 compensation_task
        W-->>RQ: 返回 AgentEvents 和执行结果
        RQ-->>GW: Gateway 获取执行结果
        GW->>S: 完成幂等状态，提交实际 token/cost 用量
        GW->>O: 按平台限制切分文本、卡片或文件消息并投递
        O-->>IM: 失败则重试，超限进入 DLQ
        IM-->>U: 用户收到 Agent 回复
    end
    CA-->>OT: IM callback span
    GW-->>OT: binding / routing / quota / queue span
    W-->>OT: runner / model / tool / storage span
    O-->>OT: IM outbound delivery span
```

## Trace 串联

每条入站消息在 Channel Adapter 处生成或继承 `trace_id`。该 ID 会写入 `InboundMessage`、`TenantContext`、`RunRequest`、模型调用 Span、工具调用 Span、存储 Span、审计日志和出站投递 Span。这样可以在故障排查时从一次 IM callback 反查到租户绑定、配额决策、模型耗时、工具审批、Session 写入、IM 投递结果和错误类型。

关键 Span 建议如下：

| Span | 关键属性 |
| --- | --- |
| `im.callback` | `tenant_id`、`channel`、`account_id`、`external_message_id`、验签结果。 |
| `gateway.route` | `tenant_id`、`agent_app_id`、`session_id`、配置版本、幂等状态。 |
| `gateway.quota` | QPS、预留 token、预留成本、拒绝原因。 |
| `worker.run` | Worker ID、队列等待时间、执行耗时、结果状态。 |
| `model.call` | 模型供应商、模型名、耗时、token usage、重试次数、错误类型。 |
| `tool.call` | 工具名、审批结果、耗时、错误类型，参数只记录脱敏摘要。 |
| `storage.write` | 后端类型、表/Key 类型、CAS 版本、延迟、冲突次数。 |
| `im.outbound` | 平台、消息类型、分片数量、重试次数、投递结果。 |

## IM 消息转换规则

入站方向由各平台 Adapter 转换为统一 `InboundMessage`，字段包括 `channel`、`account_id`、`external_message_id`、`conversation_type`、`conversation_id`、`user_id`、`content_type`、`text`、`attachments`、`raw_headers` 和 `received_at`。平台 XML、JSON、签名字段和加密字段不会向 Worker 泄漏，Worker 只处理标准化后的消息。

出站方向由 Worker 产生 `AgentEvent`，Gateway 按平台能力转换为文本、卡片、图片、文件或异步消息。超过平台长度限制的文本会被切分；频率限制触发时进入延迟重试；文件和图片使用 Artifact 后端生成可访问 URL；撤回或投递失败会写入审计和死信队列。
