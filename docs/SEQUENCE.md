# 核心链路时序图

本文档展示“企业微信用户发消息 -> Agent 执行 -> Tool 调用 -> Session / Memory 写入 -> IM 回复”的完整链路。飞书和 Telegram 也遵循同一标准入站消息和出站消息模型，只是在 Channel Adapter 层处理各自的平台协议。

```mermaid
sequenceDiagram
    participant U as 用户
    participant IM as 企业微信
    participant CA as Channel Adapter
    participant GW as Gateway
    participant Q as Redis Queue
    participant W as Worker
    participant P as Filter / Policy
    participant T as Tool / Knowledge
    participant M as Model
    participant D as Session / Memory DB
    participant O as Outbound
    participant OT as OTel

    rect rgb(232, 241, 255)
      U->>IM: 发送文本 / 图片 / 文件
      IM->>CA: 回调 + 签名 + 时间戳
      CA->>CA: 验签、解密、提取 MsgId / 用户
      CA->>GW: InboundMessage + trace_id
    end

    GW->>D: ChannelBinding 路由
    GW->>D: 生成 session_id，检查幂等键
    alt 重复消息且已完成
        D-->>GW: 返回已保存 response / result
        GW-->>O: 复用已完成回复
        O-->>IM: 平台出站回复
        IM-->>U: 用户收到回复
    else 新消息
        GW->>D: 预留 QPS / Token / Cost 配额
        GW->>Q: 写入租户 + 配置版本的 RunRequest
        Q->>W: Worker 消费
        W->>D: 加载 Session event/state、Memory、Summary
        W->>P: 输入长度、敏感信息、IM 用户权限校验
        opt 需要知识检索
            W->>T: 按租户检索 top-k
            T-->>W: 结果 + 引用元数据
        end
        opt 需要工具或 MCP 调用
            W->>P: 校验工具白名单、审批规则、危险操作策略
            W->>T: 执行已授权 Tool / MCP
            T-->>W: 工具结果或审批状态
        end
        W->>M: 上下文 + 工具结果 + 模型配置
        M-->>W: Agent Event + 回复 + token usage
        W->>D: 追加 message_event，CAS 更新 session_state
        W->>D: 写入 Memory / Summary / Audit
        Note right of D: 派生写失败 -> compensation_task
        W-->>Q: 返回 AgentEvents 和结果
        Q-->>GW: Gateway 获取结果
        GW->>D: 完成幂等，提交 token / cost
        GW->>O: 按平台限制分片并投递
        O-->>IM: 失败重试，超限进入 DLQ
        IM-->>U: 用户收到 Agent 回复
    end
    CA-->>OT: callback span
    GW-->>OT: routing / quota / queue span
    W-->>OT: runner / model / tool / storage span
    O-->>OT: outbound span
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
