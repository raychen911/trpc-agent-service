# 验证记录（VERIFICATION）

> **定位**：本文件是系统的**验证 artifact**——汇总两轮「彻底归零 → 重建 → 部署 → 全板块实测」的证据与复现命令。
> 每当架构/依赖/配置变更后应重跑并更新本文件（对应 AI-Native SDLC 的 feedback loop 产物）。期间发现并修复 4 个缺陷（根因与回归防护见各 fix 提交）。
> 数据采集日期：2026-09-09（第二轮，全部通过；第一轮同口径，其间发现并修复 4 个问题（已全部带回归防护合入））。

---

## 1. 测试基线

| 指标 | 数值 | 复现命令 |
| --- | --- | --- |
| 单元/集成测试 | **282 passed**（Redis 开启）/ 255 passed + 27 skipped（Redis 关闭时自动跳过） | `sh coverage.sh` |
| 覆盖率 | **84%**（4395 statements） | 同上 |
| Lint | flake8 0 违规 | `bash lint_flake8.sh` |
| 构建 | `uv sync --frozen`（115 包锁定，含 `trpc-agent-py==1.1.20`） | `sh build.sh` |

## 2. 最小部署冒烟（单节点，framework 真实 LLM）

```bash
STORAGE=redis RUNNER=framework sh start.sh   # 空库首启：自动播种 demo 租户（framework 档模型）
curl -s localhost:8000/healthz   # {"status":"ok"}
curl -s localhost:8000/readyz    # {"status":"ready","checks":{"redis":"ok","sql":"ok"}}
sh stop.sh
```

- 空库首启日志：`demo 租户已自动播种到租户存储`（provider=deepseek 直接播种，无需对齐修复）。
- 隔离验证：跑完整个测试套件后 `data/teneuris.db` **不会被测试创建/污染**。

## 3. Agent 孵化验证（真实 LLM 全链路）

| 用例 | 输入 | 实测结果 |
| --- | --- | --- |
| 工具调用 | "请用计算器工具计算 456\*123" | `456 × 123 = 56,088`（calculator 真实执行） |
| 会话记忆 | "把刚才的结果再加上 1000" | `56,088 + 1,000 = 57,088`（跨轮读到工具结果） |
| 知识库 RAG | CLI 录入热线 FAQ 后提问 | Agent 经 knowledge_search 答出 `99.9%` SLA 等录入内容 |
| PII 实时脱敏 | "我的手机号是 138…，请原样复述" | 输出中号码被替换为 `[REDACTED]`（Filter 链实时生效） |

每轮响应携带独立 `trace_id`，审计日志含 `tenant_id/channel/user_id/session_id/decision/trace_id` 等字段。

## 4. 多租户治理实测（独立租户 gov）

| 用例 | 配置 | 实测结果 |
| --- | --- | --- |
| Admin 建租户 | POST /tenants（预算 $0.000001） | `created` |
| 预算熔断 | 第 1 轮成本 $0.0002 后第 2 轮 | `budget exceeded: 0.0002`（BudgetFilter 拒绝） |
| 限流 | `rate_limit_per_min=1` 同窗口连发 | `rate limited, retry in 16.5s`（Redis 固定窗口） |
| 热更新 | PUT 放宽预算/限流 → 无重启生效 | 后续请求按新配置执行（pub/sub 广播失效缓存） |
| 回滚 | POST /tenants/gov/rollback | 配置恢复上一版 |
| 删除 | DELETE /tenants/gov | `deleted`（测试数据清理） |

## 5. 多节点验证

```bash
TENEURIS_RUNNER=mock docker compose up -d --build
sh scripts/verify-multinode.sh        # gw1:8001 / gw2:8004
sh scripts/verify-integration.sh      # 六场景（本地双进程 + Admin）
```

- **无 sticky session**：同一 session 三轮请求轮换命中两节点，`trace_id` 各不相同、
  `session_id` 一致，Redis 中会话历史连续累计（version 同步递增）。
- **六场景联调**（`verify-integration.sh`）：跨节点会话累计 / 跨节点 msg_id 幂等 /
  知识库跨进程共享 / 配置热更新广播 + 一键回滚 / 审计字段完整性 / Redis 重启自愈——**7/7 通过**。

## 6. 故障恢复

| 用例 | 操作 | 结果 |
| --- | --- | --- |
| Redis 重启 | `redis-cli shutdown nosave` → 网关报错不挂死 → 重启 Redis | `readyz` 恢复，`/chat` 自愈 |
| 重复投递 | 同 `msg_id` 发往不同节点 | 第二次 `duplicate message`，不重复执行 |
| 容器级 | 生产镜像（594MB，非 root `platform`）+ 独立 Redis 容器组网 | healthz/readyz/chat 全链路通过 |

## 7. K8s 部署件

- 生产镜像构建 1 分钟 / 594MB（多阶段，`uv sync --frozen`）；
- 12 个 kustomize YAML 静态校验通过（Deployment/HPA/PDB/Service 结构与装配一致性）；
- `deploy/kustomize/base/teneuris.yaml` 经 Secret 环境变量注入后通过 `env=prod` fail-closed 校验；
- 集群渲染与应用（`kubectl apply -k`）需 kubectl 环境，命令见 `deploy/kustomize/README.md`。
