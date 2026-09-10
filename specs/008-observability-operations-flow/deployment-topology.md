# 部署拓扑（第八阶段，FR-027/FR-036，SC-011）

本文档给出最小可观察部署（已交付）与生产推荐拓扑（设计建议）。组件名称与 `plan.md` 架构图及前七阶段一致；本地演示与生产建议的差距在文末逐条列出。**本文档不声明任何生产 HA/SLA。**

## 1. 最小可观察部署（已实现：`deploy/local-observable/`）

```
local-http 客户端
      │
      ▼
┌──────────┐    OTLP HTTP (4318)    ┌─────────────────┐
│ Gateway  │ ─────────────────────► │ OTel Collector  │
└──────────┘                        │ (tail sampling, │
      │                             │  trace-affinity,│
      ▼                             │  debug exporter)│
┌──────────┐    ┌──────────┐        └─────────────────┘
│ Worker-A │    │ Worker-B │
└──────────┘    └──────────┘
      │              │
      ▼              ▼
┌──────────────────────────────┐
│ Redis (lease/fence/cache)    │
│ PostgreSQL (authority: 配置、 │
│ 幂等、审计、发布、pin、告警)  │
└──────────────────────────────┘
```

- 核心服务 healthcheck 使用 `/health/ready`；真实飞书/企业微信 Adapter 在显式 `real-channels` profile。
- Secret 仅通过当前进程环境或 `.env.local` 注入；唯一 Compose project name。

## 2. 生产推荐拓扑（设计建议，未实现）

```
                    ┌────────────┐
                    │     LB     │  (L4/L7, health=/health/ready)
                    └────────────┘
            ┌───────┼────────┬─────────┐
            ▼       ▼        ▼         ▼
        Gateway×N  Feishu   WeCom    Local-HTTP
                   Adapter×2 Adapter×2 Adapter
            │       │        │         │
            └───────┴────────┴─────────┘
                        │
                  Worker Pool×M (stateless, 快照 pin 感知)
                        │
        ┌───────────────┼─────────────────┐
        ▼               ▼                 ▼
  HA PostgreSQL    HA Redis         OTel Collector 层一
  (权威: 租户/审计/ (lease/fence/    (trace-affinity 路由)
   幂等/配置/发布/  cache)                │
   pin/告警)                           ▼
                                  OTel Collector 层二
                                  (tail sampling: 关键全保留,
                                   普通成功默认 10%)
                        │
        ┌───────────────┼─────────────────┐
        ▼               ▼                 ▼
   Recovery/Operator  外部 Secret      发布控制面
   (独立进程,          Provider        (ReleaseCoordinator,
    终态对账)         (secret_ref)      硬门槛 enforcement)
```

- 每渠道至少 2 个 Adapter 实例（单渠道故障只 degraded）；Worker 无状态可水平扩缩。
- 发布顺序：schema-init（forward-only 迁移）→ Recovery/Operator → Worker → Adapter → Gateway。
- 故障域：共享数据库、共享缓存、渠道网络、配置发布、遥测出口、密钥提供者各自独立。
- 备份恢复：PostgreSQL PITR；Redis 仅缓存与租约，不持久化业务事实。

## 3. 本地演示与生产建议的差距与补齐路径

| 能力 | 本地演示 | 生产建议 | 补齐路径 |
| --- | --- | --- | --- |
| 数据库 | 单实例 PostgreSQL | HA + PITR | 托管 PG/主备 + 定期恢复演练 |
| 缓存 | 单实例 Redis | HA Sentinel/Cluster | 托管 Redis + failover 演练 |
| 遥测 | 单 Collector + debug | 两层 Collector + 持久导出 | Collector 池 + OTLP 后端 |
| 渠道 | local-http / echo | 真实飞书/企业微信 | `real-channels` profile + 真实租户凭据 |
| 发布 | 内存权威 + PG 仓库 | PG 权威 + Redis fence | `TRPC_SHARED_DATABASE_URL` 环境验收 |
| 编排 | Compose | 编排器（K8s 等） | 健康端点已就绪，编排属后续范围 |

**范围声明**：本文档为设计建议；本阶段交付的是最小可观察部署与离线演练证据，不含生产 K8s、真实模型 API 与生产 SLA。
