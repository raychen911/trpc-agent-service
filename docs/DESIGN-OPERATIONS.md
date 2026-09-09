# 详设 5 · 故障恢复与运维

> 主文档：[PRD.md §5](PRD.md)　|　验证证据：[VERIFICATION.md](VERIFICATION.md)
> 本文为该章节的完整详设（spec 深度层）；与代码/实测不一致时，以后者为准。
> 小节编号沿用原 PRD 章号（如本篇 §N.x）；跨篇 § 引用指向对应编号的详设文件。

### 5.1 降级策略矩阵

| 故障          | 检测              | 降级                                                | 恢复           |
| ------------- | ----------------- | --------------------------------------------------- | -------------- |
| 节点故障      | 健康检查/心跳超时 | 流量切健康节点，进行中 session 由 IM 重发           | 自动重建       |
| IM 重试       | 投递失败/超时     | 指数退避（1s→2s→4s→8s）最多 3 次，失败入死信队列 | 定时重投       |
| DB 短暂不可用 | 超时/错误率突增   | 读走本地缓存，写入 Redis Stream 异步刷盘            | 恢复后消费队列 |
| 模型超时      | LLM 超时（30s）   | 回「思考中」+ 后台异步继续推结果                    | 异步完成推送   |
| 工具失败      | Tool 返回 error   | 返回错误由 Agent 决策重试/换工具                    | Agent 重规划   |
| Redis 故障    | 连接断开          | Session 降级 SQL 直读写 + 布隆过滤防穿透            | 恢复后预热     |

> ⚠️ **实现状态（2026-09-02 校准）**：本节为**降级策略设计矩阵**（Problem 要求「设计……降级策略」），
> 未做故障注入实测（未真关 Redis / 未模拟节点宕机）。已落地的最小防护：
> 启动 `ping()` 探活快速失败、模型 `timeout_ms=120_000` 显式上限、工具错误归一化回传 LLM 重试、
> SQL 租户库不可用时回退内置 demo 配置。

### 5.2 灰度发布与租户级配置回滚

配置中心版本化（`version` 单调递增）。租户级回滚只影响单租户。

> ✅ **按用户比例灰度（2026-09-06 落地）**：`TenantConfig.gray`（`GrayConfig`）新增
> `enabled / percent / canary` 三段；`tenant/gray.py::apply_gray` 为**请求级纯函数**——按
> `sha256(user_id) % 100 < percent`（稳定哈希，跨节点一致，不用进程随机的内置 hash）
> 决定该请求走 canary 覆盖配置还是原配置；Runtime 在租户上下文解析后调用。canary 覆盖
> **顶层单值子配置**（model/app/tools/audit/backends：dict→子模型重校验）与标量（预算/限流）；
> im（列表）等复合字段不做覆盖（语义复杂，避免误配置）。灰度配置随租户持久化
> （`gray_config` JSON 列 + sqlite 轻量补列迁移），Admin PUT /tenants 可下发。
> 分流为请求级选版，session 历史仍按 (tenant,user) 隔离。
>
> ⚠️ 按**用户比例**灰度（灰度期新旧配置并存按用户分流）已实现；「按租户比例/金丝雀
> （双版本 Gateway 共存 + K8s 流量切分）」仍为生产演进（与代码部署灰度同层，文档如实标注）。

**生产设计要点（阶段四并入）**：
- **配置与流量解耦**：灰度变更的是租户配置（数据），不是代码/镜像 → 配置回滚秒级（Admin + pub/sub 广播即到所有副本，无需重启 Pod）；代码灰度走 K8s 滚动发布，两者互不干扰。
- **版本环形快照**：`TenantRepository` 每租户保留 ≤5 版内存快照，`POST /tenants/{id}/rollback` 弹出上一版写回（存储 + Registry + 广播）。双进程实测：热更新 suspended 跨进程即时生效 + 回滚恢复，全程不重启（见 `VERIFICATION.md`）。
- **生产演进**：内存版本环形 → 持久化 `tenant_version` 表（版本号 + 快照 JSON + 操作人/时间），可回滚到任意历史版本。
- **回滚语义**：`_save` 串行化（存储 → Registry → 广播），任一阶段失败不影响已提交部分，节点以共享存储为准最终一致收敛。
- ✅ **Admin 操作审计（2026-09-06）**：写操作（create/update/rollback/delete）记审计
  `decision=admin_create/update/rollback/delete`，payload 带 `operator`（请求头
  `X-Admin-Operator`，可选）——补「配置谁改的」归属，读操作不记。

### 5.3 容量评估

| 指标               | 参考值                                |
| ------------------ | ------------------------------------- |
| 每节点并发 session | 500-2000（受 LLM 延迟约束）           |
| 平均 token 消耗    | input 2K-4K / output 500-1K           |
| Redis QPS          | 每 1000 并发约 5K-10K                 |
| SQL QPS            | 每 1000 并发约 500-1K（审计异步批写） |
| IM 回调峰值        | 企微 20 QPS / 飞书事件订阅并发       |

扩容触发：Worker CPU>70% 持续 5 分钟 → HPA 扩容；Redis 内存>80% → 分片；LLM P99>10s → 加配额或切备用模型。

> ⚠️ **实现状态（2026-09-02 校准）**：本节为**估算方法**（Problem 要求「说明如何做容量评估」），
> 数据为理论估算值，未做真实压测/benchmark；评审按「评估方法设计」口径理解。

**估算式（阶段四并入）**：单节点并发上限 ≈ `min(CPU 核数×4, 内存/单会话常驻)`；
单会话常驻内存 ≈ session state + memory top-k ≈ 数十 KB；Redis QPS 峰值 ≈ 峰值消息率 × 每消息后端操作数（读 session+锁+写 session+幂等+审计 ≈ 5-8）；
每租户月成本 ≈ 月 token 消耗×单价 + 工具/IM 外部调用费（BudgetFilter 前置限流防失控）。
生产建议：HPA 自动扩容，Redis/SQL 预留 2× 峰值余量，LLM 按租户 `rate_limit_per_min` + 月度预算双层限流（均已实现）。

### 5.4 部署方案

> **后端硬约束**：生产与验收环境的默认后端组合**不包含 InMemory**——Session/幂等/锁/配置
> 一律 Redis，租户/审计/Summary 一律 SQL；`INMEMORY` 后端仅允许在**单元测试**中使用
> （「不建议 InMemory，推荐 Redis」落实为交付层面的硬约束，默认配置即合规）。
>
> ✅ **CLI 默认值生产化（09-06）**：`_cli.py gateway` 的 `--storage`/`--runner` 默认改为
> **`redis` / `framework`**（生产默认）；Redis 不可达 / 缺模型 key 时启动**明确报错退出**并提示
> 显式降级命令（`--storage inmemory --runner mock`），不再静默回落 demo。`start.sh` 同步
> 默认 redis+framework（可 `STORAGE=inmemory RUNNER=mock` 覆盖）；docker-compose gateway
> 默认 framework（可 `TENEURIS_RUNNER=mock` 离线验证多节点）、Redis `appendonly yes` +
> healthcheck。Web UI `/chat` 自测与本地开发仍可显式走 mock/inmemory。

- **最小可运行（Docker Compose）**：gateway + worker + channel-adapter + redis + postgres + vector + minio + otel-collector。
- **生产推荐（Kubernetes）**：Deployment 多副本 + HPA（CPU 70% / 每 Pod 100 活跃 session）+ Secret 注入 DSN + 健康探针 `/healthz` `/readyz`。

K8s 关键 YAML 骨架（详细设计见 PRD §0.3）：

```yaml
# Deployment: 无状态副本，滚动更新 + 就绪门控
apiVersion: apps/v1
kind: Deployment
metadata:
  name: teneuris-gateway
spec:
  replicas: 3
  selector: { matchLabels: { app: teneuris-gateway } }
  template:
    metadata: { labels: { app: teneuris-gateway } }
    spec:
      containers:
        - name: gateway
          image: teneuris:latest
          args: ["python", "-m", "trpc_service._cli", "gateway", "--runner", "framework", "--storage", "redis"]
          envFrom:
            - secretRef: { name: teneuris-secrets }     # DEEPSEEK_API_KEY 等，来自外部密钥注入
          env:
            - name: TENEURIS_STORAGE_REDIS_DSN
              valueFrom: { secretKeyRef: { name: teneuris-secrets, key: redis-dsn } }
          ports: [{ containerPort: 8000 }]
          livenessProbe:  { httpGet: { path: /healthz, port: 8000 }, periodSeconds: 10 }
          readinessProbe: { httpGet: { path: /readyz,  port: 8000 }, initialDelaySeconds: 3 }
          resources:
            requests: { cpu: "500m", memory: "512Mi" }
            limits:   { cpu: "2",    memory: "2Gi" }
---
# HPA: 按 CPU/请求量扩缩容（多节点弹性）
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: { name: teneuris-gateway-hpa }
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: teneuris-gateway }
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
```

> **为什么 K8s 语义与平台匹配**：无状态 Worker → Deployment 天然支持滚动发布、节点漂移、HPA 弹性，无需 StatefulSet/亲和性；共享后端（Redis/PostgreSQL）外置 → 副本可随时增减；配置热更新 → 租户级灰度**不需要重新发布 Pod**（Admin + pub/sub 即到所有副本）。多节点实测证据见 `VERIFICATION.md`。

> ⚠️ **实现状态（2026-09-02 校准）**：K8s 为 **YAML 骨架方案**（Problem 要求「生产推荐部署方案」），
> 未在真实/模拟 K8s 集群部署验证；**最小可运行方案（Docker Compose：2×Gateway + Redis）已实测**
> （`scripts/verify-multinode.sh` 多节点轮换）。

---

### 5.5 依赖锁定与构建链（uv.lock）

- **锁文件**：`uv.lock`（115 包精确解析入库，`pyproject.toml` 中 `trpc-agent-py==1.1.20`
  锁定实测基线，升级须重跑回归）；
- **构建链**：`build.sh` 与根 Dockerfile 均 `uv sync --frozen`——锁与 pyproject 不同步时
  **构建直接失败**，杜绝漂移构建；requirements.txt 保留语义版本区间作可读索引；
- **CI 联动**：开发镜像流水线触发列表含 `uv.lock`，锁变更即重建；
- **镜像验证**：生产镜像构建 1 分钟 / 594MB，容器冒烟（非 root、/readyz 真实探测、chat 带 trace_id）通过。

### 5.6 K8s kustomize 生产部署

`deploy/kustomize/`（base + overlays/production）：Gateway 2 副本 + HPA（CPU 70%，2–10 副本，
PRD 5.3 口径）+ PDB（minAvailable 1）+ 探针复用 `/healthz`（存活）/`/readyz`（就绪，
真实探测依赖）+ Redis（可换托管实例）+ Secret 注入（`TENEURIS_*` 环境变量覆盖机制，
配置文件中仅存 `{env: NAME}` 引用——PRD 4.5）。`env=prod` 使部署即触发 §4.6 fail-closed 校验。
设计要点与复现命令见 `deploy/kustomize/README.md`；NetworkPolicy / 外部指标 HPA /
Redis 持久化 PVC 列为生产演进。
