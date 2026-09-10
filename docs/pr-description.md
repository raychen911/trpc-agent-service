# 作品简介

本作品基于 tRPC-Agent Python 实现了一套面向企业场景的**多租户节点化 Agent 部署平台**。

系统支持多租户配置、可自定义大模型、共享 Session/Memory（Redis/SQL 双后端）、多节点无状态 Worker、飞书和企业微信接入（HTTP 回调 + 智能机器人长连接双形态）、工具治理、审计追踪、故障恢复、灰度发布，以及从本地 Docker Compose 到 Kubernetes 的完整部署方案。

本作品不是单进程聊天机器人，而是一套能够承载多个租户、多个 Agent 应用、多个 IM 入口和多个 Worker 节点的 Agent 服务平台。

## 一、总体架构

```mermaid
%% 系统架构图：多租户节点化 Agent 部署平台
flowchart TB
    subgraph Clients["接入方"]
        WEB["Web UI"]
        FEISHU["飞书"]
        WECOM["企业微信<br/>(HTTP 回调 / 智能机器人长连接)"]
    end

    subgraph GatewayLayer["Agent Gateway（无状态，可水平扩展）"]
        AUTH["验签 → 去重 → 限流/预算校验"]
        ROUTER["SessionRouter"]
        ADMIN["Admin API"]
    end

    subgraph WorkerLayer["Agent Worker（无状态）"]
        AGENT["LlmAgent + 治理 Filter 链<br/>(白名单/脱敏/预算/二次确认)"]
        TOOLS["FunctionTool×N"]
    end

    subgraph StorageLayer["存储与可观测"]
        BACKEND[("Redis / MySQL<br/>Session·Memory·审计")]
        OTEL["OTel<br/>trace_id 贯穿"]
    end

    WEB --> AUTH
    FEISHU --> AUTH
    WECOM --> AUTH
    SBOT["企微智能机器人"] --> AUTH
    AUTH --> ROUTER --> AGENT --> TOOLS --> BACKEND
    ADMIN --> BACKEND
    AGENT -.-> OTEL
```

## 二、核心时序链路（trace_id 贯穿）

```mermaid
%% 核心时序图（关键链路）：企微用户发消息 → Agent → Tool → Session/Memory → 回复
sequenceDiagram
    autonumber
    participant U as 企微用户
    participant GW as 通道适配 + 网关
    participant AG as AgentRunner<br/>(LlmAgent + 治理 Filter)
    participant T as Tool
    participant S as Redis/SQL<br/>Session·Memory
    participant AU as 审计/指标

    U->>GW: 发消息（HTTP 回调 / 长连接）
    GW->>GW: 验签 → 幂等去重 → 限流/预算校验
    GW->>S: 读会话上下文
    GW->>AG: run(agent_context{trace_id})
    AG->>AG: 决定调用 query_order<br/>(白名单✓ 脱敏✓ 预算✓)
    AG->>T: execute_tool
    T-->>AG: 订单结果
    AG->>S: append_event → state → 异步 summary → Memory 写入
    AG-->>GW: 回复文本（输出脱敏）
    GW->>AU: 审计落盘 + 指标（同一 trace_id）
    GW-->>U: 回复消息
```

## 三、最终完成内容

- **多租户平台**：租户配置七要素聚合、配置热加载、动态 CRUD、app_name 前缀数据隔离
- **IM 三通道**：飞书事件订阅 v2.0（模拟验证）、企微 HTTP 回调被动回复（模拟验证）、企微智能机器人长连接（**真机验证通过**，免公网域名）
- **治理与安全**：工具白名单/PII 脱敏/预算双层拦截/危险工具二次确认；API-Key 鉴权、CORS 白名单、凭证全部环境变量注入
- **多节点**：Worker 无状态（确定性 session 路由）、inline/Redis 队列双模式（崩溃任务可重放）
- **可观测**：trace_id 贯穿全链路；业务指标（请求量/错误率/耗时/工具耗时/Session 后端延迟/IM 投递成功率/token），进程内聚合 + OTel 双通道
- **数据模型**：平台六表（tenant/agent_app/tenant_revision/audit_log/channel_binding/idempotency）+ 三层幂等
- **运维**：租户配置版本化回滚 + release_stage 灰度发布、审计 JSONL 兜底、Docker Compose/K8s 部署方案

## 四、测试与交付

- 测试：**123 passed + 1 skipped**，覆盖率 77%（协议回环、治理拦截、并发写、容器沙箱、长连接回环）
- 交付物：架构设计文档、架构图、双时序图、数据模型、同步与幂等策略、多后端适配方案、风险清单 12 条（详见 docs/）
