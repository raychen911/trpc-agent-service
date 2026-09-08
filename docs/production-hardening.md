# 生产安全、迁移、富媒体与运维

## Admin OIDC 与 RBAC

启用 `TRPC_SERVICE_ADMIN_OIDC_ENABLED=true` 后，Admin API 只接受 Bearer JWT。服务从 JWKS
获取签名公钥，并校验签名、`iss`、`aud`、`exp`、`iat`、`sub`。支持三种角色：

- `platform-admin`：全平台读写，包括创建租户。
- `tenant-admin`：只可读写 `tenant_ids` claim 中的租户。
- `auditor`：只读；带 tenant claim 时，租户列表也会过滤。

开发和测试默认关闭 OIDC，并映射为本地 platform admin。生产环境应在入口网关同时设置访问策略、
短 Token TTL 和 MFA。

## 内部 mTLS 与密钥

`internal_tls_*` 为跨节点 HTTP 客户端加载 CA 与客户端证书；`tls_server_*` 和
`tls_client_ca_file` 让 Uvicorn 要求客户端证书。共享 `x-internal-token` 继续保留作为纵深防御。
若公开接口与内部接口使用不同信任域，生产部署应由 Envoy/Istio 创建独立 internal listener。

SecretResolver 支持：

- `env://NAME`
- `vault://secret/data/team/app#api_key`（兼容 KV v2 响应）
- `aws-kms://BASE64_CIPHERTEXT`

日志只记录引用，不记录解析值。Kubernetes 中推荐 Vault Agent/CSI 或 AWS IRSA，避免长期 Vault
Token。

## Qdrant 与数据迁移

设置 `VECTOR_BACKEND=qdrant` 后，Memory/Knowledge 写入 Qdrant。Qdrant point payload 保留完整
namespace，并在每次搜索中强制 namespace filter。默认 HashingEmbedder 适合验证链路；生产应替换
为与模型一致的 embedding 服务后创建新 collection，再双写、回填、校验、切读、停止旧写。

迁移命令：

```powershell
python scripts/migrate_data.py redis-to-sql `
  --redis-url "$env:REDIS_URL" `
  --database-url "$env:DATABASE_URL"

$env:MINIO_ACCESS_KEY="..."
$env:MINIO_SECRET_KEY="..."
python scripts/migrate_data.py local-to-minio `
  --local-root ./data/artifacts --endpoint 127.0.0.1:9000
```

Redis→SQL 默认跳过已存在目标，`--overwrite` 仍不会用较旧版本覆盖较新 SQL Session。
Local→MinIO 会逐对象验证 SHA-256；任何失败返回退出码 2。生产切换使用“停止写入→最后增量→校验
数量/版本/校验和→切换读取→保留旧数据观察期”。

## 富媒体、Artifact 和死信

`AgentReply` 支持 `attachments`、`card`、`stream_updates`。Telegram 支持图片/文档 URL、
inline keyboard 和 `editMessageText`；企业微信支持 template card 以及预上传 `media_id`。文本会按
Telegram 4000 字符、企业微信 1900 字符安全拆分。

Artifact API：

```http
POST /gateway/v1/artifacts
GET  /gateway/v1/artifacts/{tenant_id}/{object_key}
```

上传采用 Base64，单请求限制 10 MiB；更大文件应使用 MinIO 预签名直传。Outbox 默认重试 8 次，
随后复制到 `outbox_dead_letters` 并结束原消息。重新投递前必须核对通道幂等能力，再创建新的
Outbox dedupe key。

## Alembic、Compose 与 Kubernetes

本地生产拓扑：

```bash
docker compose up --build
```

Compose 包含 PostgreSQL、Redis、MinIO、Qdrant、独立 Alembic migration job 和应用。
Kubernetes 示例位于 `deploy/kubernetes.yaml`，包含 stable/canary Deployment、Service、HPA、PDB
和 migration Job。`CANARY_TENANT_IDS` 中的租户优先路由到注册为 canary 的节点；没有健康 canary
时自动回退。

升级数据库：

```bash
alembic upgrade head
alembic downgrade -1  # 仅在 revision 明确支持且已验证数据兼容时
```

生产发布顺序为：向后兼容 schema → canary 节点 → 指定测试租户 → 观察错误率、P95、成本与死信
→ 扩大租户 → stable rolling update → 最后清理旧 schema。

## 备份恢复和容量测试

SQLite 在线一致性备份及校验恢复：

```bash
python scripts/backup_sqlite.py data/trpc_service.db backups/trpc.db
python scripts/restore_sqlite.py backups/trpc.db data/trpc_service.db \
  --manifest backups/trpc.db.manifest.json --force
```

恢复前必须停写；工具会先校验 SHA-256，并为现有数据库创建 `pre-restore` 副本。PostgreSQL 使用
`pg_dump --format=custom`，恢复到新库后执行 `pg_restore --clean --if-exists`，完成行数、外键、
Session version 和 Outbox 状态校验后再切换连接串。MinIO/Qdrant 需同时启用版本化/快照，恢复点
应与 SQL Outbox 水位对应。

容量测试：

```bash
python scripts/capacity_test.py --token "$GATEWAY_TOKEN" \
  --concurrency 100 --requests 5000
```

脚本输出失败数、均值、P95 和 P99。压测租户必须提前创建并使用独立模型配额；同时观察每节点
并发、Token/s、Redis lock 等待、SQL QPS/连接池、Inbox backlog、Outbox dead letter 和 IM 限流。
> 本文中的 Compose/Kubernetes 示例已经包含 OpenTelemetry Collector；完整多租户和节点说明见
> `../deliverables/system-architecture-design.md`。
