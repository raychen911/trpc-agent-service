# 验收证据说明

本目录保存脱敏后的本地联调、测试和真实企业微信验证证据，不保存 API Key、企业微信 Token、
EncodingAESKey、数据库生产密码或未脱敏聊天数据。

## 环境与后端

- `compose-services-redacted.png`、`compose-services.txt`：Compose 服务及端口；截图中的本机路径已遮挡。
- `health-ready.png`、`health-ready.json`：应用与数据库就绪检查。
- `postgres-tables.txt`：PostgreSQL 20 张平台表。
- `redis-info.txt`：Redis 运行信息。
- `minio-console.png`：MinIO Console 可访问；画面为空 Bucket，不代表已完成 Artifact 写入验证。
- `qdrant-collections.png`：Qdrant API 可访问；画面为空 Collection，不代表已完成向量写入验证。

## 测试

- `im-adapter-validation.txt`：31 项 IM、Webhook、Gateway、队列和恢复测试通过。
- `full-regression-after-aibot.txt`：63 项通过，真实双节点项在该次全量运行中被跳过。
- `two-node-test.txt`：连接真实 PostgreSQL/Redis 的双节点测试单独执行并通过。

## 企业微信真实链路

- `wecom-aibot-url-verified-redacted.png`：URL 回调模式配置证据；URL、Token 和 EncodingAESKey 已不可逆遮挡。
- `wecom-aibot-real-reply.jpg`：真实企业微信消息与模型回复。
- `wecom-aibot-database.txt`：脱敏后的 Inbox/Execution 状态。
- `wecom-aibot-outbox.txt`：回复 Outbox 已处理状态。
- `wecom-aibot-trace.json`：脱敏后的完整 Trace；保留 trace/span 父子关系、耗时和模型统计。

真实消息调用了 DeepSeek 模型，但未触发 Tool，因此这些证据不能单独证明真实 Tool 外部副作用链路。
