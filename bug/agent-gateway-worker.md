# Gateway / Worker 问题记录

## 结论

这次现象中确实存在问题，但 QQ 回调地址本身不是主要问题。Gateway 能收到 QQ 请求；真正导致“收到消息但没有回复”的直接原因是 Worker 退出，Redis Streams 中的任务没有消费者处理。

此外还发现两个部署和可观测性问题：

1. Worker 在 Redis 阻塞读取超时后没有恢复或重试，直接退出。
2. Gateway 默认启用队列，但没有检查 Worker 是否可用；Worker 挂掉后，Gateway 仍可能成功返回 webhook ACK，造成消息已经入队但没有回复。
3. Gateway / Worker 启动时使用错误的 `TENANT_CONFIG_ENCRYPTION_KEY` 会直接因为 `InvalidToken` 启动失败。
4. 脱敏日志处理器破坏了 Uvicorn access log 的参数，日志中出现 `ValueError: not enough values to unpack`。

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

当前消费循环位于 `trpc_service/agent/_consumer.py`：

```python
while True:
    await self.run_once(count=10, block=block)
```

`run_once()` 调用 Redis Streams 的阻塞读取；读取抛出 `TimeoutError` 后没有在外层捕获，因此异常一路退出 `asyncio.run()`，容器结束。

## 2. Worker 的 Redis 超时退出问题

### 影响

- Worker 空闲等待任务时可能退出；
- Gateway 仍然可以把 QQ 消息写入 Redis；
- 消息停留在 pending 或 stream 中，用户看不到回复；
- Docker Compose 没有 Worker 健康检查，也没有有效的自动恢复策略。

### 建议修复

在 `StreamWorker.run()` 外层捕获 Redis 连接和读取超时，记录日志后延迟重连，而不是让进程退出。例如：

```python
async def run(self, block: int = 5000) -> None:
    await self._queue.ensure_group()
    while True:
        try:
            await self.run_once(count=10, block=block)
        except (TimeoutError, ConnectionError) as exc:
            logger.exception("worker queue read failed; retrying")
            await asyncio.sleep(1)
```

实际实现应使用 Redis 客户端对应的异常类型，并区分“可重试的连接错误”和“任务处理错误”。任务处理错误已有 retry / dead-letter 逻辑，不能因为单条坏消息退出整个 Worker。

同时建议：

- 给 Worker 增加 Docker `restart: unless-stopped`；
- 增加 Worker 健康检查；
- 配置合适的 Redis socket/connect timeout；
- 为队列读取增加单元测试，覆盖 Redis 超时后继续消费的行为。

## 3. Gateway 队列模式的可用性问题

Gateway 在 `trpc_service/web/app.py` 中默认启用队列：

```python
queue_enabled = os.environ.get("AGENT_QUEUE_ENABLED", "1").lower() not in {"0", "false", "no"}
```

Docker Compose 也默认将 `AGENT_QUEUE_ENABLED` 设为 `1`。Gateway 将消息写入 Redis 后即可返回 QQ 所需的成功 ACK，但它不会确认 Worker 是否正在消费。

这符合异步队列的设计，但缺少运行状态保护，容易产生“平台显示发送成功、Agent 实际没有回复”的假成功。

### 临时规避

单机验证时可关闭队列，让 Gateway 在本进程内执行 Agent：

```bash
AGENT_QUEUE_ENABLED=0 \
TENANT_CONFIG_ENCRYPTION_KEY='原来的稳定密钥' \
docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d --force-recreate gateway
```

生产环境应保留 Gateway / Worker 解耦模式，并修复 Worker 的重连、健康检查和告警。

## 4. 加密密钥导致启动失败

Gateway 启动时会从 MySQL 读取租户配置，并使用 `TENANT_CONFIG_ENCRYPTION_KEY` 解密敏感字段。使用了错误的密钥时会出现：

```text
cryptography.fernet.InvalidToken
```

这不是可以通过重新生成密钥解决的问题。必须恢复创建或保存租户配置时使用的原始密钥，否则已有密文无法解密。

Compose 文件当前提供了本地默认值：

```yaml
TENANT_CONFIG_ENCRYPTION_KEY=${TENANT_CONFIG_ENCRYPTION_KEY:-local-compose-change-me}
```

这会造成一个部署风险：如果启动命令没有重新注入真实稳定密钥，容器可能使用默认值并启动失败，或在新环境中产生不可迁移的密文。

### 建议修复

- 生产环境强制要求该变量存在，不使用弱默认值；
- 使用 Docker Secret、Kubernetes Secret 或受控 `.env` 文件；
- 在 Gateway 和 Worker 中保持完全相同的密钥；
- 在启动检查阶段给出明确错误信息。

## 5. 脱敏日志破坏 Uvicorn access log

日志中反复出现：

```text
ValueError: not enough values to unpack (expected 5, got 0)
```

请求实际仍能返回 200，但 Uvicorn 的 access log 格式化失败。问题与 `trpc_service/log/_masker.py` 中对 `LogRecord` 的处理有关：

```python
record.msg = SecretMasker.mask_value(rendered_message)
record.args = ()
```

Uvicorn 的 `AccessFormatter` 需要从 access log 的 `record.args` 中读取请求信息；脱敏处理器提前把这些参数渲染并清空，导致 formatter 无法解包。

### 建议修复

不要对 Uvicorn access logger 的结构化参数做破坏性修改。可选方案：

- 只对业务日志做脱敏；
- 为 access logger 使用独立 formatter / filter；
- 保留 Uvicorn 需要的 5 个 access-log 参数，只脱敏其中的 URL 或 header 值；
- 增加一次真实 Uvicorn access log 的集成测试。

## 6. 当前可用运行方式

在 Worker 修复前，单机测试可使用：

```bash
AGENT_QUEUE_ENABLED=0 \
TENANT_CONFIG_ENCRYPTION_KEY='原来的稳定密钥' \
docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d --force-recreate gateway otel-collector prometheus
```

然后确认：

```bash
docker ps
curl http://127.0.0.1:8080/healthz
curl -sS http://127.0.0.1:8889/metrics | grep '^agent_'
```

QQ 回调地址使用：

```text
https://公网域名/webhook/local_demo/qq
```

## 7. 优先级

| 优先级 | 问题 | 处理建议 |
|---|---|---|
| P0 | Worker Redis 超时后退出 | 增加重连循环，并添加回归测试 |
| P0 | 密钥未注入导致 Gateway 启动失败 | 统一 Secret 注入，移除生产默认密钥 |
| P1 | Gateway 不感知 Worker 不可用 | 增加健康检查、告警和队列积压监控 |
| P1 | access log formatter 失败 | 修复脱敏 filter 与 Uvicorn formatter 的兼容性 |
| P2 | 临时 cloudflared 隧道不稳定且暴露全部 Gateway | 生产使用 Named Tunnel / 正式域名并限制 Admin 访问 |

