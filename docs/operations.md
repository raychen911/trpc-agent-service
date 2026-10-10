# 运维、容量与故障恢复

Compose 提供单机验收环境：Gateway、Admin、Worker、两类 Dispatcher、Projector、WeCom
Connector、PostgreSQL/pgvector、Redis、MinIO、OTel Collector、Prometheus、Jaeger 和 Toxiproxy。
Compose 默认启动一个 Worker；多节点/容量验收必须显式使用
`docker compose ... up -d --scale worker=4 --scale outbox-dispatcher=2`，不能依赖
Compose `deploy.replicas`。
Kubernetes base/production overlay 提供 HPA、PDB、NetworkPolicy、探针、非 root、只读文件系统、
滚动升级和优雅停机。生产建议使用托管高可用 PostgreSQL、Redis 与对象存储。

`session-recovery` 是独立的 PostgreSQL mailbox 恢复角色，通过
`trpc-service serve --role session-recovery` 运行 Lease Sweeper、Retry Scheduler 和
Session Reconciler 三个有界循环。它只依赖 PostgreSQL；Compose 只等待 `migrate` 完成，探针
也只检查 PostgreSQL，不要求 Redis readiness 或 Redis Secret。默认每轮每个组件最多处理 25
条记录、每 5 秒轮询，Kubernetes 默认 1 副本、请求 50m CPU/128Mi 内存、上限 250m CPU/256Mi
内存。扩容前应确认 Repository 的行锁/fencing 已实现；多个副本可以并行运行，但默认单副本能
减少无必要的数据库竞争。收到 SIGTERM 时停止新轮次并取消三个循环，终止宽限建议至少 30 秒。

Worker 接收媒体时必须配置 S3-compatible Artifact 后端，否则不会在未持久化原文件的情况下继续处理。
`artifact-gc` 使用独立 worker 数据库身份，每轮通过 `FOR UPDATE SKIP LOCKED` 清理超过 24 小时仍为
`staged` 的元数据与对象，并分页检查 S3 staging 前缀，回收上传成功但元数据事务未提交的孤儿对象。
默认每轮最多 100 项、每 60 秒轮询；对象删除成功后才把元数据 CAS 为 `deleted`，供应商故障会保留
记录供下轮幂等重试。Compose/Kubernetes 均以单副本启动，也可依靠行锁安全扩容。
Production overlay 使用 digest-pinned MinIO StatefulSet、独立 application identity 和 retained PVC；
凭据来自 `trpc-infrastructure-secrets`，应用不能使用 MinIO root 身份。小规模图片/PDF 验收时 Worker
常驻约几十 MiB，峰值额外内存主要由当前下载项、
PDF 解析和模型 SDK 决定；每个 Worker 当前顺序消费消息，默认单项 20 MiB 硬上限可避免并发倍增。
更高吞吐应通过增加无状态 Worker 副本扩展，而不是放宽单文件限制。

真实多进程验收使用 `scripts/real_runtime_gate.py`。默认 Compose 应用角色直连依赖；需要
验证网络切断时，必须叠加 `deploy/toxiproxy-runtime.override.yml`，否则切换 Toxiproxy
不会影响 Worker/Dispatcher 的实际连接。该脚本只在显式 opt-in 后执行 `docker kill`，
不会调用 `docker compose down -v`，因此不会删除已保留的数据卷。

| 故障 | 行为 | 告警/恢复 |
|---|---|---|
| Redis 不可用 | PG Outbox 保留；低速 PG 轮询 | queue publish error、outbox age；恢复后补投 |
| PostgreSQL 暂不可用 | 已缓存并验签 binding 的回调连同固定 config revision 进入 AES-GCM Redis 应急流 | consumer group 恢复回灌，PG 成功后才 ACK；若 Redis 也失败返回非 2xx 让 IM 重试 |
| Worker 被杀 | 租约过期后其他节点接管 | lease expiry/conflict；旧 token 不能 commit |
| 模型超时 | 租户 fallback model 或统一降级答复 | model timeout、fallback count |
| 工具失败 | 仅幂等工具退避重试，熔断隔离下游 | tool error/circuit state |
| IM 明确失败 | 限次退避，超过阈值入 DLQ | delivery success/error/DLQ |
| IM 结果未知 | 标记 ambiguous，不自动重发 | 人工审批 replay |
| Projector 落后 | 精确读回退 PG，检索报告 lag | projection age/depth |

配置 revision 不可变。灰度按 Session HMAC 桶选择 candidate 百分比，保证重试稳定；观察错误率、成本、
延迟和业务质量后增加比例。回滚只原子切换 active revision，旧 Worker 继续使用其 Inbox 已固定版本，
新消息立即使用历史稳定版本。数据库变更必须 expand-contract，禁止把 schema 回滚当应用回滚。

容量基线以 100 IM callback/s、200 并发 turn 为首版门槛。压测报告至少记录消息大小、模型 p50/p95、
平均输入/输出 token、工具并发、每 turn event 数、PG/Redis QPS 和 IM 额度。估算：

- Worker 数约为 `峰值 turn/s × p95 turn 秒数 ÷ 每 Worker 安全并发 × 1.3`。
- PG 写 QPS 至少为 callback 事务 + 每 turn commit + outbox/投递/审计更新，并预留 50% 峰值余量。
- Redis Stream 保留量为 `峰值 callback/s × 最长可接受恢复秒数 × 平均消息字节`。
- HPA 同时看 CPU、运行中 turn、队列 age；仅看 CPU 会漏掉外部 API 等待型负载。

发布门禁要求 callback ack p95 <200ms、已接受消息零丢失、错误率 <0.1%、突发结束后队列归零。
PDB 保持关键角色可用，preStop 停止拉新任务并等待当前 turn；终止宽限必须大于 lease 续期间隔和常见
turn p95。备份需要同时验证 PG PITR、对象版本和密钥恢复，季度执行按租户恢复演练。三个隔离恢复作业
分别输出 `runs/drill/postgres_pitr.json`、`artifact_restore.json`、`key_restore.json` 后，显式设置
`TRPC_DR_DRILL_ENABLED=true` 并运行 `scripts/disaster_recovery_gate.py --require-production`。三份证据
必须共享同一 drill/tenant canary，恢复前后 SHA-256 一致，绑定当前 candidate lock，并同时满足配置的
RPO/RTO；未执行或只写一份“通过”摘要时门禁保持 `not_run`。

没有跨区 OSS、KMS 或持久卷时，可运行零成本功能灾备检查来验证三条恢复代码路径。它从同一个
`deploy/runtime-gate.yaml` 读取 kubeconfig、context、`image_pull_secret`、support 镜像和当前
candidate lock，在集群内创建临时 `trpc-dr-functional-*` Namespace；Namespace 只使用 `emptyDir`，
不挂 PVC/hostPath，也不接触生产数据。PostgreSQL 检查是合成数据的逻辑快照恢复，MinIO 检查对象版本
恢复，密钥检查使用临时 Secret 中的合成 wrapping key；因此它只能证明功能链路，不能代替生产 PITR、
异地对象冗余或外部 KMS。三个 Job 会一起提交，完成后收集 Kubernetes API 与 Job 输出证据，成功或失败
都会按 Namespace UID 校验后清理临时 Namespace，报告固定为 `production_gate=not_run`。

执行时先准备配置文件和当前 nonce，再显式开启：

```powershell
$env:TRPC_DR_FUNCTIONAL_ENABLED = "true"
python -m scripts.kubernetes_functional_disaster_recovery `
  --config deploy/runtime-gate.yaml `
  --require-functional
```

Job 内部入口必须使用 `python -m scripts.dr_functional_job`，不要改成脚本文件路径；这样容器从
`/app` 启动时能正确解析 `scripts` 包。报告写入
`runs/multitenant/disaster-recovery-functional.json`，功能检查通过也不能用于
`scripts.disaster_recovery_gate.py --require-production`。

发布聚合默认仍要求上述破坏性生产灾备真实 `pass`。只有发布者显式给 release gate 和 manifest 都传入
`--allow-functional-dr`，才能用当前候选的功能灾备 `pass` 授权破坏性报告保持 `not_run`；聚合结果必须
记录 `authorized_not_run_gates=[disaster_recovery]`。破坏性报告为 `fail`、功能报告缺少三个组件、cleanup、
lineage 或正确 producer 时都不能授权，`online_im` 和其他门禁也不受影响。该模式的 manifest 绑定功能
灾备报告和 policy，排除未运行的破坏性报告；policy 或所绑定报告被篡改时最终门禁失败。

调度器 `v1` 与 `v2` 的 Redis stream、consumer group 和数据库处理语义不同。相同版本的
代码升级可以使用 Kubernetes RollingUpdate；`v1↔v2` 是协议切换，必须先停入站、排空旧
版本的 PostgreSQL outbox/Redis group 和执行 lease，再在所有相关 Pod 为 0 时切换配置并
按顺序启动新版本。具体 SQL 判据、Redis `XPENDING`/`XINFO` 检查、回滚前的 mailbox 条件
和应急队列处理见 [调度器切换运行手册](scheduler-cutover.md)。禁止以 `DEL`、`XTRIM`、
`XGROUP DESTROY` 或直接改状态字段的方式伪造排空。

## Kubernetes 正式发布顺序

正式发布只使用 `deploy/kustomize/overlays/production`。先创建六类 Secret 和镜像拉取凭据，再应用
基础设施并等待 PostgreSQL、Redis、MinIO、Prometheus ready；随后重建并等待 schema migration 与
MinIO bootstrap Job，最后启动 Gateway、Admin、Worker、两个 Dispatcher、Projector、Recovery、GC、
Exporter 和 WeCom Connector。裸 `kubectl` 与 Argo CD 的精确顺序、等待命令和回滚步骤见根目录
README。不得以单机 systemd、面板数据库或临时域名替代正式集群模板。

变更 ConfigMap 或 Secret 后按角色滚动，并重新验证 EndpointSlice、HPA、binding/SecretRef、
PostgreSQL/Redis/MinIO 连通性和日志脱敏。企业微信两个 Connector 副本必须跨节点，且同一 binding
只能有一个 advisory-lock owner；飞书回调必须经正式 Ingress HTTPS 验签。
