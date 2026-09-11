# 生产运行手册

## 1. 运行角色和凭据

| 角色 | 环境变量 | 职责 |
|---|---|---|
| Gateway | runtime PostgreSQL、control PostgreSQL、Redis、IM Secret、可选 embedding | Webhook、Inbox/Outbox、限流、投影 relay |
| Worker | runtime PostgreSQL、Redis、模型/工具 Secret、embedding、对象存储 | Agent turn、Session/Memory、Tool/MCP、IM 回复 |
| Admin | control PostgreSQL、三类 Admin token | 配置、审计、Knowledge、DLQ 恢复 |
| Migration | owner PostgreSQL | 仅执行 DDL migration |

生产必须设置 `TRPC_SERVICE_ENV=production`、`TRPC_SERVICE_BACKEND=postgres-redis` 和对应 `TRPC_SERVICE_ROLE`。Gateway/Worker 禁止持有 migration DSN；Admin 不需要 runtime DSN 或 Redis。

Admin 凭据分别使用 `TRPC_SERVICE_ADMIN_TOKEN_REF`、`TRPC_SERVICE_ADMIN_OPERATOR_TOKEN_REF`、`TRPC_SERVICE_ADMIN_VIEWER_TOKEN_REF`。服务端根据 token 决定角色，不读取客户端声明的角色。生产 SecretRef 只允许 `env://` 或 `file://`。

## 2. 初始化与部署

```bash
bash build.sh
uv run trpc-service migrate
docker compose up --build --scale worker=2
kubectl apply -k deploy/kustomize/base
```

Migration DSN 必须是 DDL owner；runtime DSN 使用启用 RLS 的非 owner；control DSN 使用仅供 Gateway relay/Admin 的 `trpc_control` 角色。migration 会只在业务租户表上为该角色创建显式跨租户 policy，不应授予集群级 `BYPASSRLS`。数据库角色必须先于 migration 创建，`TRPC_SERVICE_AUTO_MIGRATE` 在生产必须为 `false`。

生产 Worker 还必须设置 `TRPC_AGENT_MODEL_NAME` 和 `TRPC_AGENT_API_KEY` 作为默认模型。租户 App revision 可用 `model_name`、`model_api_key_ref`、`model_base_url`、`fallback_*` 和 `timeout_seconds` 覆盖默认值；密钥字段只允许 `env://`/`file://` SecretRef，生产 endpoint 必须使用 HTTPS。启用 `storage.vector=pgvector` 或 Knowledge 时设置 `TRPC_AGENT_EMBEDDING_MODEL`；启用 `object_store=s3` 时设置 `TRPC_SERVICE_ARTIFACT_BUCKET` 及 S3/MinIO 凭据。`TRPC_SERVICE_IM_DRY_RUN=true` 在生产会拒绝启动。

## 3. 健康和观测

- `/health/live` 只检查进程。
- `/health/ready` 检查 PostgreSQL、Redis、Stream pending/DLQ 和飞书长连接；异常返回 HTTP 503。
- `/metrics` 提供 callback/model/storage/tool 延迟、入站/出站、队列、token、成本和 fencing 指标。
- `OTEL_EXPORTER_OTLP_ENDPOINT` 启用 OTLP；队列中的 32 位 trace id 用于跨 Gateway/Worker 关联 span。

## 4. 配置与知识库

配置读取返回 ETag，更新和回滚必须携带 `If-Match`：

```bash
curl -H "X-Admin-Token: $ADMIN_TOKEN" http://admin/admin/tenants/acme
curl -X POST -H "X-Admin-Token: $OPERATOR_TOKEN" -H 'If-Match: "tenant-acme-v2"' \
  http://admin/admin/tenants/acme/rollback/1
```

Agent App 通过 `knowledge_collections` 声明可检索集合，StorageProfile 使用 `vector=pgvector`。写入文档后由 Outbox 异步索引：

```bash
curl -X PUT -H "X-Admin-Token: $OPERATOR_TOKEN" -H "Content-Type: application/json" \
  -d '{"content":"退款申请需在订单完成后 7 天内提交。"}' \
  http://admin/admin/knowledge/acme/faq/refund-policy
```

## 5. 恢复操作

查看和重放 PostgreSQL Outbox 死信：

```bash
curl -H "X-Admin-Token: $VIEWER_TOKEN" http://admin/admin/outbox/acme/dead-letters
curl -X POST -H "X-Admin-Token: $OPERATOR_TOKEN" \
  http://admin/admin/outbox/acme/dead-letters/OUTBOX_ID/replay
```

查看和重放 Worker 失败消息：

```bash
curl -H "X-Admin-Token: $VIEWER_TOKEN" http://admin/admin/inbox/acme/failed
curl -X POST -H "X-Admin-Token: $OPERATOR_TOKEN" \
  http://admin/admin/inbox/acme/acme%3Awecom%3Abot%3Amessage-id/replay
```

`delivery_failed` 会保留已生成回复，重放只重试 IM；`failed` 会重新执行 turn，因此操作前必须确认外部副作用工具已经过账。Outbox relay 每 30 秒扫描长时间处于 `accepted/queued` 的 Inbox，以恢复 Redis 通知丢失。

审计按租户 revision 的 `audit.retention_days` 清理：

```bash
curl -X POST -H "X-Admin-Token: $OPERATOR_TOKEN" http://admin/admin/audit/acme/purge
```

## 6. 上线门禁

1. 同一 callback 重放 100 次，只存在一条 Inbox 和一次 Agent turn，重复请求保持 2xx。
2. 模型调用中终止 Worker，pending 由另一 Worker reclaim；旧 fencing epoch 不能提交。
3. 暂停 Redis 后 callback 保留在 SQL，恢复后 Outbox 补发；清空 Stream 后 stranded Inbox 被重建。
4. 模拟 provider 响应不确定，outbound 标为 ambiguous 或 delivery_failed，不盲目重复副作用。
5. 验证 RLS 跨租户拒绝、配置 CAS、Secret 轮换、Knowledge 版本投影和租户回滚。
6. 压测 callback P95 < 500 ms、正常 IM 投递率 >= 99.9%，记录 Redis/SQL QPS 和 Worker 稳态吞吐。

## 7. 应用回滚

先停止新 Gateway 流量，等待 Outbox 与 Stream pending 清空，再回滚镜像。配置回滚使用租户 revision API；数据库采用 expand-contract，不在事故窗口执行破坏性 downgrade。回滚后确认 `/health/ready`、投递成功率、DLQ、fencing conflict 和模型错误率恢复正常。
