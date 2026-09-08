s# Docker 部署 Agent 并接入 QQ Bot

本文记录在 Docker Compose 中启动 Agent、使用 `cloudflared` 暴露 webhook、接入 QQ 机器人，以及查看 Metrics 的完整流程。

以下命令默认在仓库根目录执行：

```bash
cd /home/peanut/myCode/trpc-agent-service
```

## 1. 准备环境变量

### 1.1 模型和管理密钥

`TRPC_AGENT_API_KEY` 使用模型服务商提供的 API Key；另外两个密钥在本地生成：

```bash
export TRPC_AGENT_API_KEY='你的模型API密钥'
export TENANT_CONFIG_ENCRYPTION_KEY="$(openssl rand -hex 32)"
export ADMIN_API_KEY="$(openssl rand -hex 32)"
```

`TENANT_CONFIG_ENCRYPTION_KEY` 必须长期保持不变。它用于解密数据库中已有的租户敏感配置，不能在每次启动时重新生成。
建议将三个值保存到密码管理器或部署平台的 Secret 中，不要提交到 Git。

### 1.2 QQ 机器人密钥

在 QQ 机器人开放平台创建机器人并获取 `AppID` 和 `AppSecret`：

```bash
export QQBOT_APP_ID='你的QQ机器人AppID'
export QQBOT_APP_SECRET='你的QQ机器人AppSecret'
```

这里使用的是 QQ 机器人的 `AppSecret`，不是模型 API Key，也不是旧版 Token。

如果关闭终端后还要继续部署，需要重新设置同样的值，或将它们配置到服务器的 Secret / `.env` 文件中。`.env` 文件不要提交到 Git。

## 2. 确认租户配置

打开 `deploy/tenants.yaml`，确认 QQ 通道配置存在：

```yaml
tenants:
  - tenant_id: local_demo
    channel_configs:
      qq:
        channel_type: qq
        app_id: ${QQBOT_APP_ID}
        secret: ${QQBOT_APP_SECRET}
```

回调 URL 中的租户 ID 必须与这里一致。本项目示例使用 `local_demo`，不是 `tenant_demo`。

不要把真实的 `AppSecret` 直接写入 `tenants.yaml`。

## 3. 启动 Docker 服务

### 3.1 进程内模式（适合先验证 QQ）

进程内模式不依赖独立 Worker，Gateway 会直接执行 Agent：

```bash
AGENT_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  up -d --build gateway redis mysql
```

检查状态：

```bash
docker compose -f deploy/docker-compose.minimal.yml ps
curl http://127.0.0.1:8080/healthz
```

健康检查应返回 HTTP `200`。

### 3.2 指标链路模式

需要查看跨进程 Metrics 时，叠加 observability 配置：

```bash
AGENT_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d --build gateway otel-collector prometheus
```

此模式会启动：

```text
Gateway → OpenTelemetry Collector → Prometheus
```

查看状态：

```bash
docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  ps
```

## 4. 暴露公网 webhook

本项目的 Gateway 只监听宿主机的 `127.0.0.1:8080`。如果已经安装 `cloudflared`，运行临时公网隧道：

```bash
cloudflared tunnel --url http://127.0.0.1:8080
```

命令会输出一个临时 HTTPS 地址，例如：

```text
https://随机名称.trycloudflare.com
```

保持这个终端和进程持续运行。终止 `cloudflared` 后，公网地址立即失效；重新启动通常会得到新的地址。

先测试公网转发：

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  https://你的临时域名.trycloudflare.com/healthz
```

应返回 `200`。

## 5. 在 QQ 开放平台配置回调

在 QQ 机器人开放平台的消息接收 / HTTP 回调设置中填写：

```text
https://你的临时域名.trycloudflare.com/webhook/local_demo/qq
```

注意：

- 使用 `https`；
- 路径必须是 `/webhook/local_demo/qq`；
- 不要使用 `/webhook/tenant_demo/qq`；
- AppID 和 AppSecret 必须属于同一个 QQ 机器人；
- 启用所需事件和权限，例如私聊事件、群聊 `@` 事件；
- 测试群和测试账号需要加入 QQ 机器人的沙箱配置。

服务会自动处理 QQ 的 `op=13` 地址验证、Ed25519 签名和后续事件接收，不需要手工填写 AccessToken。

配置时可实时查看 Gateway 日志：

```bash
docker logs -f deploy-gateway-1
```

保存成功时通常可以看到：

```text
POST /webhook/local_demo/qq ... 200
```

## 6. 测试 Agent

在 QQ 私聊机器人发送：

```text
你好，请介绍一下你自己
```

群聊中需要按照 QQ 平台权限要求 `@` 机器人。

同时查看日志：

```bash
docker logs -f deploy-gateway-1
```

如果使用队列模式，再查看 Worker：

```bash
docker logs -f deploy-worker-1
```

## 7. 查看 Metrics

### 7.1 Gateway 进程内快照

```bash
curl -sS \
  -H "X-Admin-API-Key: $ADMIN_API_KEY" \
  http://127.0.0.1:8080/admin/metrics
```

返回中的 `scope` 固定为 `process`，只代表当前 Gateway 进程。它不包含独立 Worker 进程的内存快照。

### 7.2 Collector

```bash
curl -sS http://127.0.0.1:8889/metrics | grep '^agent_'
```

### 7.3 Prometheus

Prometheus 默认绑定在服务器本机的 `9090` 端口：

```text
http://127.0.0.1:9090
```

如果从远程电脑访问，使用 SSH 隧道：

```bash
ssh -L 9090:127.0.0.1:9090 user@你的服务器
```

在 Prometheus 中查询：

```promql
{__name__=~"agent_.*"}
```

按 Gateway / Worker 区分：

```promql
sum by (service_name, outcome) (
  rate(agent_callback_total[5m])
)
```

## 8. 常见问题

### 8.1 `curl: Failed to connect to 127.0.0.1:8889`

说明 Collector 没有运行，或命令是在错误的机器上执行。检查：

```bash
docker ps
docker logs deploy-otel-collector-1
```

启动指标链路：

```bash
AGENT_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d gateway otel-collector prometheus
```

### 8.2 QQ 保存回调失败

优先检查：

1. URL 是否使用 `/webhook/local_demo/qq`；
2. `AppID` 和 `AppSecret` 是否属于同一个机器人；
3. `cloudflared` 是否仍在运行；
4. Gateway 日志中是否收到 QQ 的 `POST` 请求；
5. QQ 平台的事件订阅、权限和沙箱配置是否已启用。

### 8.3 QQ 能保存，但机器人不回复

如果 Gateway 收到了消息而没有回复，先看容器状态：

```bash
docker ps -a --format 'table {{.Names}}\t{{.Status}}'
```

使用队列模式时，`worker` 必须是 `Up`。如果 Worker 反复退出，可先切换到进程内模式：

```bash
AGENT_QUEUE_ENABLED=0 docker compose \
  -f deploy/docker-compose.minimal.yml \
  -f deploy/docker-compose.observability.yml \
  up -d --force-recreate gateway
```

如果日志出现 `InvalidToken`，说明启动时使用了错误的 `TENANT_CONFIG_ENCRYPTION_KEY`。必须恢复创建租户配置时使用的原始密钥，不能重新生成一个新密钥替代。

### 8.4 临时域名失效

重新启动：

```bash
cloudflared tunnel --url http://127.0.0.1:8080
```

然后把 QQ 平台中的回调地址更新为新的 HTTPS 地址。生产环境应使用 Cloudflare Named Tunnel 或正式域名，不要依赖 `trycloudflare.com` 临时隧道。

