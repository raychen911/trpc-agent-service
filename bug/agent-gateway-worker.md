# Gateway / Worker 问题记录

## 结论

这次现象中确实存在问题，但 QQ 回调地址本身不是主要问题。Gateway 能收到 QQ 请求；真正导致“收到消息但没有回复”的直接原因是 Worker 退出，Redis Streams 中的任务没有消费者处理。

2026-09-08 已完成代码修复和回归测试。修复范围包括：

1. Worker 捕获 Redis 读取超时和断连，重新建立 consumer group 后继续消费；
2. Worker 发布带 TTL 的存活心跳，Gateway 在入队前检查心跳，无可用 Worker 时返回 503；
3. Compose 强制显式提供稳定的 `TENANT_CONFIG_ENCRYPTION_KEY`，错误密钥返回可理解的启动错误；
4. 脱敏日志保留 Uvicorn `AccessFormatter` 所需的五元参数，并继续对 URL 中的密钥脱敏；
5. Compose 增加自动重启和健康检查，Kubernetes 区分 Gateway readiness/liveness；
6. Prometheus 增加 Worker 不可用和 Redis 重连告警。

## 1. 已确认的运行证据

### Gateway

QQ 回调请求已经到达 Gateway，并返回成功：

```text
POST /webhook/local_demo/qq HTTP/1.1" 200
```

访问错误租户路径时则返回 404：

```text
POST /webhook/tenant_demo/qq HTTP/1.1" 404
```

因此当前正确的租户路径是：

```text
/webhook/local_demo/qq
```

### Worker

Worker 容器退出状态为 `Exited (1)`，日志末尾为：

```text
redis.exceptions.TimeoutError: Timeout reading from redis:6379
```

问题发生时的消费循环位于 `trpc_service/agent/_consumer.py`：

```python
while True:
    await self.run_once(count=10, block=block)
```

当时 `run_once()` 调用 Redis Streams 的阻塞读取；读取抛出 `TimeoutError` 后没有在外层捕获，因此异常一路退出 `asyncio.run()`，容器结束。

## 2. Worker 的 Redis 超时退出问题（已修复）

### 影响

- Worker 空闲等待任务时可能退出；
- Gateway 仍然可以把 QQ 消息写入 Redis；
- 消息停留在 pending 或 stream 中，用户看不到回复；
- Docker Compose 没有 Worker 健康检查，也没有有效的自动恢复策略。

### 已实现

- 阻塞 `XREADGROUP` 的 socket timeout 作为一次空轮询处理；
- `StreamWorker.run()` 捕获 Redis `TimeoutError`/`ConnectionError`，退避后重新执行 `ensure_group()`；
- 单条消息错误仍走 retry / dead-letter，不会结束消费进程；
- 每个 Worker 使用唯一 consumer id，并持续刷新 Worker 心跳和执行中任务的 pending idle；
- Compose 使用 `restart: unless-stopped` 并检查进程和 Redis，Kubernetes readiness 校验当前 Pod 的 Redis 心跳；
- `test_stream_queue_treats_blocking_read_timeout_as_empty_poll` 和
  `test_stream_worker_recovers_queue_connection_errors` 覆盖超时、断线恢复。

## 3. Gateway 队列模式的可用性问题（已修复）

Gateway 在 `trpc_service/web/app.py` 中默认启用队列：

```python
queue_enabled = os.environ.get("AGENT_QUEUE_ENABLED", "1").lower() not in {"0", "false", "no"}
```

Gateway 现在通过 Redis sorted set 检查最近 30 秒内是否存在 Worker 心跳。队列模式下没有活跃 Worker
时，`/readyz` 和消息回调都返回 503，且不会占用消息幂等键，IM 平台重试后仍可正常入队。
`/healthz` 仅表示 Gateway 进程存活，避免把依赖故障误判为进程崩溃。

Prometheus 同时记录 `agent_worker_available`、`agent_worker_unavailable_total`，并配置
`AgentWorkerUnavailable` 告警。

## 4. 加密密钥导致启动失败（已修复部署风险）

Gateway 启动时会从 MySQL 读取租户配置，并使用 `TENANT_CONFIG_ENCRYPTION_KEY` 解密敏感字段。使用了错误的密钥时会出现：

```text
cryptography.fernet.InvalidToken
```

这不是可以通过重新生成密钥解决的问题。必须恢复创建或保存租户配置时使用的原始密钥，否则已有密文无法解密。

Compose 不再提供弱默认值，未设置变量时在创建容器前直接失败：

```yaml
TENANT_CONFIG_ENCRYPTION_KEY=${TENANT_CONFIG_ENCRYPTION_KEY:?set a stable TENANT_CONFIG_ENCRYPTION_KEY}
```

Gateway 和 Worker 读取同一环境变量；Kubernetes 继续从同一个 Secret key 注入。解密旧数据时密钥
不匹配会转换为明确的 `ValueError`，提示必须恢复持久化数据所使用的原密钥，而不是暴露模糊的
`cryptography.fernet.InvalidToken`。

## 5. 脱敏日志破坏 Uvicorn access log（已修复）

日志中反复出现：

```text
ValueError: not enough values to unpack (expected 5, got 0)
```

请求实际仍能返回 200，但 Uvicorn 的 access log 格式化失败。问题发生时，`trpc_service/log/_masker.py` 会这样处理 `LogRecord`：

```python
record.msg = SecretMasker.mask_value(rendered_message)
record.args = ()
```

Uvicorn 的 `AccessFormatter` 需要从 access log 的 `record.args` 中读取请求信息；脱敏处理器提前把这些参数渲染并清空，导致 formatter 无法解包。

脱敏处理器现在识别 `uvicorn.access`，保留 formatter 所需的五元 tuple，只对 tuple 中的 URL 等字符串
做脱敏。`test_redacting_log_filter_preserves_uvicorn_access_formatter_arguments` 使用真实
`uvicorn.logging.AccessFormatter` 验证不会再发生解包错误，并验证 query token 不会出现在输出中。

## 6. 当前可用运行方式

队列模式现在可以直接运行：

```bash
TENANT_CONFIG_ENCRYPTION_KEY='原来的稳定密钥' \
docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d --force-recreate
```

然后确认：

```bash
docker ps
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl -sS http://127.0.0.1:8889/metrics | grep '^agent_'
```

QQ 回调地址使用：

```text
https://公网域名/webhook/local_demo/qq
```

## 7. 优先级

| 优先级 | 问题 | 状态 |
|---|---|---|
| P0 | Worker Redis 超时后退出 | 已修复并测试 |
| P0 | 密钥未注入导致 Gateway 启动失败 | 已修复并测试 |
| P1 | Gateway 不感知 Worker 不可用 | 已增加心跳门禁、readiness、指标和告警 |
| P1 | access log formatter 失败 | 已用真实 AccessFormatter 回归测试 |
| P2 | 临时 cloudflared 隧道不稳定且暴露全部 Gateway | 运维事项：仍应使用 Named Tunnel / 正式域名并限制 Admin 访问 |
