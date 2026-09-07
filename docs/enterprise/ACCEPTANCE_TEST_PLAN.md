# 多租户 Agent 工程化验收与测试搭建方案

本文给出可执行的验收口径。目标不是“接口被调用过”，而是证明租户不串、消息不丢、故障可恢复、密钥不泄漏，并以 CI 行覆盖率门禁防止回归。

## 1. 质量门禁

每个 Pull Request 必须依次通过：

1. `flake8` 的语法、未定义符号与静态错误检查；
2. 企业模块单元/组件测试；
3. `trpc_service` 行覆盖率 **≥95%**，低于阈值 CI 直接失败；
4. Redis/MySQL/Qdrant/S3 兼容接口的后端集成测试；
5. Compose 配置校验、镜像启动与健康检查；
6. 浏览器 E2E（MyTestWeb）和 API 契约测试；
7. 主分支夜间执行性能、故障注入、迁移回放和密钥扫描。

覆盖率是防回归下限，不替代断言质量。测试必须断言结果、隔离边界或故障状态，禁止只为走行而调用函数。当前命令：

```bash
pytest tests/service \
  --cov=trpc_service \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-fail-under=95

diff-cover coverage.xml --fail-under=85
```

## 2. 测试环境

### 快速 PR 环境

- Python 3.10、3.11、3.12 矩阵；
- fakeredis 验证原子预算、锁、幂等、Streams pending/DLQ；
- SQLite 验证 SQL 审计和迁移；
- MockTransport 验证企业微信、微信客服、钉钉、飞书、QQ 的网络、限长、流式和错误响应；
- Mock Agent 验证 Runner、Session、Memory，不消耗模型费用。

### 真实后端环境

`deploy/docker-compose.test.yml` 启动 Redis、MySQL、Jaeger 与 Toxiproxy；CI 另用 Qdrant local client 和 S3 契约桩，夜间环境连接真实 Qdrant、MinIO。测试数据全部使用 `ci-${run_id}` 前缀；测试结束按 tenant_id 清理。生产凭据禁止进入该环境。

### 外部沙箱环境

- 企业微信测试企业/应用；
- 企业微信、微信客服、钉钉、飞书、QQ 各自的沙箱账号/机器人和测试群；
- DeepSeek 或兼容模型的低预算测试 key；
- 每夜最多执行一次，设置租户日 token/费用硬限额。

## 3. 逐项验收矩阵

| 需求域 | 测试方法 | 核心断言 | 自动化层级 |
|---|---|---|---|
| 租户模型全部字段 | YAML/JSON round-trip、非法字段、SecretStr repr | tenant/app/model/tool/channel/storage/audit 字段不丢，明文不出现 | 单元 |
| Gateway/Worker/Adapter/Admin/Telemetry 协作 | webhook→Streams→Worker→Session→reply 全链路 | 同一 trace_id；正确 tenant、channel、agent | E2E |
| 多节点水平扩展 | 2 Gateway + 3 Worker，随机杀 Worker | 无 sticky 仍可续聊，无跨租户事件 | 集成/故障 |
| 正确 session 路由 | 同用户跨租户、跨群、单聊/群聊组合 | session_id 稳定且互不相等 | 单元/性质测试 |
| 配置隔离 | 并发热更新两个租户并回滚 | 只影响目标租户，版本单调递增 | 并发 |
| 数据隔离 | 相同 app/user/session 写不同 tenant | 查询永远只返回本租户数据 | 单元/集成 |
| 工具权限隔离 | 同一工具在不同白/黑/危险列表 | 未授权函数调用次数为 0；危险工具只在一次性确认后执行 | 组件 |
| 日志脱敏/密钥 | 注入手机号、身份证、Bearer、API key、DB URL | log/trace/error/audit/配置历史均不含原文 | 安全 |
| 每租户多后端 | Redis/MySQL/Qdrant/S3 组合路由 | 按连接身份复用池，所有 key/namespace 强制 tenant 前缀 | 单元/集成 |
| Session 并发一致性 | 两 Worker 同时写同 session | Redis 分布式锁串行；事件均存在且顺序可解释 | 并发 |
| Event/state/summary 顺序 | 故障点分别插入 event 后、state 后、summary 前 | event 可重放，state 版本不倒退，summary 指向已提交版本 | 故障注入 |
| Memory 跨节点可见 | Worker A 写，Worker B 立即轮询 | Redis/MySQL 主库强读立即可见 | 集成 |
| Redis→SQL 迁移 | 全量复制→双写→checksum→切读→回滚 | 数量/校验和一致，迁移期写不丢，支持租户级回滚 | 迁移 |
| IM 重复投递 | 相同 message_id 并发 20 次，处理/回复阶段分别崩溃 | Agent 副作用至多一次；成功回复缓存后只重试投递 | 并发/故障 |
| 企业微信接入 | HMAC/SHA1/AES、XML、图文、群聊、2048 bytes、stream | 验签失败 401；UTF-8 不截断；开流失败自动降级 | 组件/沙箱 |
| 四类 IM 接入 | 平台签名、文本/图片/文件、群聊、分段、限流 | 身份/附件映射正确；失败可观测 | 组件/沙箱 |
| QQ Bot 扩展接入 | op=13、Ed25519、C2C/群/频道/私信、AccessToken、回复路由 | 挑战响应正确；篡改回调 401；回复进入对应会话 | 单元/E2E/沙箱 |
| IM 账号绑定 | tenant/channel 配置与 webhook 路径交叉请求 | A 的签名和 token 不能调用 B | 安全 |
| IM 身份映射 | 同用户跨群、跨平台、跨租户 | 映射表有 tenant+channel 复合域，不合并身份 | 组件 |
| Filter 治理 | 白名单、脱敏、预算、HITL、用户/群权限组合 | 先鉴权后工具；预算原子预留；审计 decision 正确 | 组件 |
| 监控指标 | 成功/失败/超时/重复投递各执行一次 | callback、queue、worker、runner、storage、IM 指标及 SDK 模型/工具/token 指标增加 | 集成 |
| OTel Trace | IM callback 到最终回复 | callback→queue→runner→tool→storage→reply 为同一 trace | 集成 |
| 审计字段 | 成功、deny、confirm、timeout 各生成记录 | 必填 12 字段完整，tenant 查询无越权 | 单元/API |
| 节点故障 | Worker 在锁内、模型后、回复前被 SIGKILL | lease 到期可接管；pending 被 reclaim；已有结果不重跑 Agent | Chaos |
| DB 短暂不可用 | Toxiproxy 断开 5/30 秒 | 有界重试、503 触发 IM 重投、恢复后 pending 清零 | Chaos |
| 模型超时 | 首 token 前超时、部分流后超时 | 首 token 前切 fallback；部分流后不重复整段回答 | 组件 |
| 工具失败 | 可重试/不可重试/超时工具 | 退避次数正确，错误类型与 trace 可查，危险副作用不盲重试 | 组件 |
| 灰度与回滚 | 5% 租户使用 canary，注入错误率 | 仅灰度租户受影响；一键回滚恢复旧 config version | 发布演练 |
| 容量 | 阶梯提升 callback 与并发 session | P95、错误率、Redis/SQL QPS、队列 lag 达标且找到拐点 | 性能 |
| 最小/生产部署 | Compose/K8s manifest 校验与 smoke | 健康探针、资源限制、滚动升级期间可服务 | 部署 |

## 4. 数据一致性口径

| 数据 | 推荐后端 | 口径 | 验收 SLA |
|---|---|---|---|
| Session event/state | Redis 原子操作或 SQL 事务 | 单 session 单写者强顺序；跨 session 并行 | 写成功后立即可读 |
| Summary | 跟随 Session 存入 SQL/Redis，与 source_version 关联 | 可最终一致，但不得覆盖新版本 | 30 秒内或任务 SLA |
| Memory | Redis/MySQL | 主库写后读一致，版本单调递增 | 立即可见 |
| Artifact | S3/MinIO/COS 内容 + MySQL metadata | 内容成功后再提交 metadata；失败走 outbox 补偿 | metadata 可见时内容必须存在 |
| Knowledge | MySQL 文档/分块 + Qdrant 向量 + Redis 缓存 | checksum/版本幂等，向量按确定性 point id upsert | SQL 主库立即可见，向量最终一致 |
| Audit Log | SQL append-only + 进程内最近 500 条 | 不允许业务更新/删除，不使用 Redis 主存储 | `log()` 成功返回前落库；内存第 501 条自动淘汰最旧记录 |

Session 的推荐提交序列为：获得 session writer lock → append user/agent events → 基于事件计算并 CAS 更新 state/version → 提交事务/原子脚本 → 异步 summary(source_version) → 异步 Memory。Summary 只可在 `source_version >= current_summary.source_version` 时更新。

## 5. 迁移演练

1. 冻结 schema 与序列化版本，创建目标索引；
2. 按 tenant/kind/key 游标全量复制，记录 count/checksum/watermark；
3. 开启 dual-write，失败写入 durable outbox；
4. 重放 watermark 后增量并做 checksum、随机语义抽样；
5. 单租户切换读路径，保留 shadow read 对比；
6. 观察一个保留周期后停止源写；
7. 回滚时把读指针切回源端并反向重放增量。

Redis→SQL 仅迁移 Session/Memory，必测空值、Unicode、大消息、乱序版本、重复 key 和迁移中并发写。知识库迁移按 SQL 文档版本重建向量索引，对象迁移以 SHA-256 对账后再切元数据指针。Audit 固定使用 MySQL，不进入迁移流程。

## 6. 性能与容量

用真实平均输入/输出 token 分布，不用固定短 prompt。分三种负载：IM 回调突发、长会话持续对话、工具/向量检索混合。

计算基线：

- `worker_concurrency ≈ 可用内存 / 单活跃 Runner 峰值内存`，再受模型连接池与工具池限制；
- `model_tokens_per_second = arrival_rate × (avg_input + avg_output)`；
- `Redis QPS ≈ callback × (dedup + queue + lock + session event/state + result cache)`；
- `SQL QPS ≈ session/audit/memory 事务数 + 迁移双写`；
- 队列容量必须覆盖峰值到达率与模型消费率差值乘以可接受恢复窗口。

建议门槛：回调 ACK P95 < 500 ms、正常对话首 token P95 按模型基线 +20%、IM 投递成功率 ≥99.9%、非模型错误率 <0.1%、队列 oldest-pending-age <60 s。超过阈值时扩 Worker；Gateway 仅在 ACK CPU/网络成为瓶颈时扩容。

## 7. 发布与故障演练

- 配置使用不可变 `config_version`，Gateway/Worker 记录实际使用版本；
- 灰度按 tenant_id 一致性哈希，不按随机请求，避免同一租户版本漂移；
- 自动回滚条件：5 分钟错误率、P95、模型成本任一超过基线阈值；
- 每季度演练 Redis 主从切换、MySQL failover、Worker SIGKILL、IM 重投和密钥轮换；
- 告警必须附 tenant_id（高基数详细信息进 trace/log，不进 Prometheus label）、trace_id、queue lag 和 config_version。

## 8. 完成定义

- PR 快速套件全绿，企业层行覆盖率 ≥95%；
- 两个 IM adapter 的契约、附件、分段、流式与失败测试通过；
- 真实 Redis/SQL 通过并发、可见性、pending reclaim 和迁移测试；Qdrant/S3 通过租户隔离、幂等 upsert/checksum 和故障补偿测试；
- MyTestWeb 的 Mock 模式、DeepSeek 模式（有 key 时）、租户切换、HITL 和场景按钮通过浏览器 E2E；
- Compose 最小部署和 K8s 推荐部署均通过 smoke/rollout；
- 日志、trace、审计和错误报告的 secret scan 为 0 命中；
- 生成 coverage.xml、JUnit、性能报告、迁移校验报告和 trace 样例作为评审证据。
