# 测试与发布门禁

默认 `pytest` 使用 deterministic FakeAgent/FakeTool/Fake IM，不访问网络、不要求 API key。测试分层：

- 单元/协议：HMAC Session、rollout、飞书签名/AES、WeCom frame、Filter、预算、确认、脱敏。
- SDK 契约：固定 1.1.19 的 Runner 参数、AgentContext metadata、Event、Tool Safety 和取消行为。
- 后端契约：InMemory/Redis/PostgreSQL/S3/pgvector 使用相同语义；真实服务测试标记 `integration`。
- 并发/故障：至少四 Worker 竞争同一 Session、乱序/重复、续租失败、阶段 kill 和依赖中断。
- 在线 IM：仅 `TRPC_IM_ONLINE_TESTS_ENABLED=true` 且 Feishu/WeCom Secret 注入时运行，不从 `.env` 打印凭证。
  生产证据还必须提供 `TRPC_IM_ONLINE_PROBE_URL`（HTTPS）和已部署探针的
  `TRPC_IM_ONLINE_IMAGE_DIGEST`；同时必须配置只含精确 HTTPS 基址的
  `TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST`（可用逗号或换行分隔多个固定地址）和固定的
  `TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256`（或仅在 Secret 管理器内使用
  `TRPC_IM_ONLINE_PROBE_IDENTITY`），以及飞书/企微各自不可变 control profile 的 SHA-256。探针必须
  回传匹配 digest、运行 nonce、身份和 profile，并逐通道证明往返、幂等、媒体、重连、真实限流退避、
  凭证轮换、至少 60 秒的单实例故障接管和供应商 ACK 后丢响应的 ambiguous 共 8 个 case。探测器拒绝
  userinfo/query/fragment，禁止 HTTP 重定向，也不会跟随重定向后的最终 URL；探针响应严格使用有限
  JSON，并且必须通过源码绑定的
  `deploy/im-probe-trust.json` Ed25519 公钥验证完整响应签名。缺少信任文件或签名时保持 `not_run`。
  `retry_after_seconds` 必须为有限的 0.001–3600 秒，
  `outage_seconds` 必须为有限的 0.001–604800 秒。
- 在线 IM 的 `reconnect`/`prolonged_outage` 只按“单个 connector 进程不可用、冗余 owner 接管并继续
  交付”验收：必须记录旧 owner 释放、新 owner 接管、重新订阅和接管后唯一 marker 的 provider 事件
  与发送 ACK；`prolonged_outage` 的 connector 故障窗口至少 60 秒。两个 WSS 同时断开属于独立的
  provider delivery gap，不能以恢复后新消息替代断线期间旧消息，也不能据此生成恢复 `pass`。
- 在线 IM 的控制动作只能由 checkout 外的 host-only control broker 按固定 profile 调度。driver 不得
  获得 Kubernetes、数据库、OIDC 或供应商管理凭据，也不得自行制造 observation。飞书入站必须与独立
  callback observer 的哈希 receipt 核对，出站、限流与 ambiguous 必须与 OpenAPI witness 的真实 ACK
  receipt 核对；企微不得建立第二条 WSS，只能把 broker 动作结果与当前 Connector 持久化的 connection
  epoch、lease lifecycle 和 provider event 哈希快照核对。任一旁路证据缺失时该 case 必须非 `pass`。
- “功能完成”的真实 IM 基础证据只要求当前候选在 Feishu/WeCom 各闭环唯一入站、唯一出站和供应商
  回执。它不能冒充上述 `online_im` 生产 8-case；生产发布和 release manifest 始终要求两个通道的完整
  8-case。默认还要求 destructive DR 真实通过；只有显式 `--allow-functional-dr` 可在当前候选功能灾备
  `pass` 时授权 destructive DR 保持 `not_run`，不能豁免 destructive DR `fail`、`online_im` 或其他门禁。
- 性能：100 callback/s、200 turn，并生成机器可读 JSON。

本地门禁：

```bash
uv sync --extra dev --locked
uv run ruff format --check .
uv run ruff check .
uv run mypy trpc_service
uv run pytest --cov=trpc_service --cov-branch
docker compose config --quiet
docker compose up -d --scale worker=4 --scale outbox-dispatcher=2
kubectl kustomize deploy/kustomize/overlays/production >/dev/null
python scripts/performance_gate.py
python scripts/mock_production_gate.py
python scripts/contract_gate.py fault
python scripts/contract_gate.py migration
python scripts/deployment_gate.py
python scripts/kubernetes_runtime_gate.py
python scripts/release_gate.py --output runs/multitenant/release-gate-current-final.json
```

`deployment_gate.py` 默认只做静态部署检查：静态清单通过时返回 0，但报告明确为
`static_gate=pass`、`gate=not_run`、`production_gate=not_run`；它不会假装执行 live Kubernetes
验收。加上 `--require-production` 会要求生产结论为 `pass`，由于该命令本身不执行运行态验收，
在没有独立 live Kubernetes 证据时应返回非零。静态失败无论是否加该参数都返回非零。

真实多进程/故障验收必须显式 opt-in，且需要至少四个独立 Worker、运行账号数据库、Redis
和 Toxiproxy；详见 [真实运行态验收](real-runtime.md)。未提供环境时应执行：

```bash
python scripts/real_runtime_gate.py --output runs/multitenant/real-runtime.json
```

该命令只生成 `gate=not_run`。真实执行需要 `TRPC_RUN_REAL_MULTINODE=1`、`--execute`、
`--use-toxiproxy` 和 `--allow-process-kill`；脚本永远不会删除 Compose 数据卷。

`trpc_service` 行和分支覆盖率均不得低于 90%。真实 PostgreSQL 测试必须使用非 owner runtime 账号，
构造两个租户相同 app/user/session 标识，验证 API、直接 SQL、Redis key、对象路径和向量 namespace 均
不能互读。泄漏扫描把测试 token、API key、密码和消息正文作为 canary，扫描日志、span、异常和 JSON
报告。

真实 Compose 后端契约通过 `TRPC_TEST_POSTGRES_DSN`、`TRPC_TEST_REDIS_URL`、
`TRPC_TEST_S3_ENDPOINT`、`TRPC_TEST_S3_ACCESS_KEY`、`TRPC_TEST_S3_SECRET_KEY` 和
`TRPC_TEST_S3_BUCKET` 以及已部署候选镜像的
`TRPC_TEST_IMAGE_DIGEST=sha256:<64-hex-digest>` 显式启用；执行
`python scripts/contract_gate.py backend`，并用 `--output`
将报告写入 `runs/multitenant/backend-compose.json`。控制面 E2E 使用
`TRPC_E2E_DEVELOPMENT_TOKEN` 和
`TRPC_E2E_POSTGRES_RUNTIME_DSN`，执行 `python scripts/compose_e2e.py`。该报告明确标记为
`scope=control_plane`，只证明控制面创建、配置、审计、绑定解析和清理，不是
callback→mailbox→worker→outbound 的消息 E2E；完整消息 E2E 仍需真实运行态验收。这些变量只能从
Secret 注入，命令和报告不能打印其值。

故障注入的 `production_gate` 只有在 `--scenario all` 的完整场景集合全部真实通过、并提供
`TRPC_REAL_IMAGE_DIGEST=sha256:<64-hex-digest>` 时才可能为 `pass`；单场景、离线契约或缺少任一
场景仍为 `not_run`。

`scripts/release_gate.py` 汇总所有 JSON 证据。默认允许开发门禁通过但保留生产 `not_run`；CI/CD
发布阶段必须使用 `--require-production`，任何真实 IM、生产负载、故障注入、迁移或 Kubernetes 运行
报告缺失都会返回非零状态。`run-current-final-acceptance.ps1` 的 Stage 8 实际把聚合结果写到
`runs/multitenant/release-gate-current-final.json`；只有这次聚合生成、通过 current-candidate lineage、
源码指纹和 24 小时 TTL 校验的最新 final 文件才是当前候选结论。历史组件 JSON 的顶层 `pass`、
旧的 `release-gate.json` 或 `release-gate-current.json` 都只是输入/历史记录，不能单独升级候选状态。
过期或来自其他 checkout 的生产 evidence 必须降级为 `not_run`。

默认的 `--require-production` 仍要求真实破坏性生产灾备 `pass`。本项目 Stage 8 固定同时传入
`--allow-functional-dr`；这时必须验证 `disaster-recovery-functional.json` 的三个恢复组件、cleanup、
lineage 和 producer，且只可把破坏性 DR 的 `not_run` 记入
`authorized_not_run_gates=[disaster_recovery]`。破坏性 DR 为 `fail` 时不能豁免，`online_im=not_run` 仍阻断。

所有真实生产报告必须使用同一个 `TRPC_RELEASE_ID`、同一个高熵
`TRPC_RELEASE_NONCE` 和同一个不可变候选镜像 digest。报告全部完成后运行
`scripts/release_manifest.py --image-digest sha256:<64-hex>`，生成
`runs/multitenant/release-manifest.json`。该 manifest 按 canonical JSON SHA-256
绑定每个生产报告的内容、producer、run ID、时间、源码指纹、release nonce 哈希和镜像。
manifest 缺失时最终生产门禁保持 `not_run`；报告被替换、混入其他运行或镜像不一致时门禁为 `fail`。
显式功能 DR 模式必须给 manifest 命令同样传入 `--allow-functional-dr`；manifest 的 `policy` 会绑定功能
灾备报告和 `authorized_not_run_gates`，并排除未运行的破坏性报告，缺失或篡改该 policy 都会失败。

Pytest 默认清除从当前 Shell 继承的真实负载、故障、迁移、Kubernetes 和在线 IM 环境变量。
只有在隔离验收环境中明确添加 `--allow-real-tests` 才会保留这些变量；普通 CI 只运行
`tests/unit`，不会因为开发机遗留变量意外启动外部验收。

当真实环境暂不可用时，`scripts/mock_production_gate.py` 会执行五组确定性虚拟验收：4 租户/8 Worker
乱序与重复负载、节点和依赖故障、双租户可恢复迁移、Kubernetes 控制器行为模型，以及企业微信/飞书
协议 Fake。结果写入 `runs/multitenant/production-mock.json`，并以独立 `simulation_gate` 汇总。报告始终把
`production_gate` 保持为 `not_run`；Mock 通过只能提前发现逻辑错误，不能替代真实网络、进程、存储、
Kubernetes 控制面和 IM 平台配额。

生产候选还要求：Compose 零状态 E2E；kubeconform、滚动升级/HPA/节点驱逐/优雅停机；迁移断点续传、
双写、checksum 和按租户回滚；Toxiproxy 中断 PG/Redis/MinIO/IM；企业微信与飞书真实凭证完成协议验收；
依赖漏洞、镜像漏洞和 SBOM 门禁。缺少任何必需外部环境时，报告必须明确 `not_run` 原因，不能把静态
清单验证表述成部署通过。

## Kubernetes 运行态验收

`tests/simulation/test_kubernetes_runtime_model.py` 只验证清单驱动的控制器行为模型，不能替代
Kubernetes 控制面。真实运行态验收由 `scripts/kubernetes_runtime_gate.py` 提供，并且必须显式开启：

```bash
export TRPC_K8S_RUNTIME_TESTS_ENABLED=true
export TRPC_K8S_RUNTIME_CONTEXT=your-context
export TRPC_K8S_RUNTIME_IMAGE=<registry-host>/<org>/trpc-agent-service@sha256:<64-hex-digest-a>
export TRPC_K8S_RUNTIME_UPGRADE_IMAGE=<registry-host>/<org>/trpc-agent-service@sha256:<64-hex-digest-b>
export TRPC_K8S_RUNTIME_SECRET_MANIFEST=/secure/trpc-runtime-secrets.yaml
export TRPC_K8S_RUNTIME_HPA_DRIVER=E:/trpc-agent-service/scripts/kubernetes_hpa_load_driver.py
export TRPC_K8S_RUNTIME_HPA_DRIVER_SHA256=<64-hex-sha256-of-driver>
export TRPC_K8S_RUNTIME_HPA_DRIVER_KUBECONFIG=/secure/hpa-driver-kubeconfig
export TRPC_K8S_RUNTIME_HPA_DRIVER_SUBJECT=system:serviceaccount:runtime-gate:hpa-driver
export TRPC_K8S_RUNTIME_HPA_DRIVER_CONTEXT=dedicated-driver-context
export TRPC_K8S_RUNTIME_HPA_JOB_IMAGE=<registry-host>/<org>/trpc-hpa-backlog@sha256:<64-hex-digest>
export TRPC_K8S_RUNTIME_HPA_JOB_COMMAND='["python","-m","your_bounded_backlog_probe"]'
export TRPC_K8S_RUNTIME_NODE_NAME=dedicated-runtime-node
export TRPC_K8S_RUNTIME_NODE_LABEL=trpc-runtime-gate=dedicated-gate
export TRPC_K8S_RUNTIME_NODE_DRAIN_CONFIRM=I_UNDERSTAND_ISOLATED_NODE_DRAIN
export TRPC_RELEASE_ID=release-<current-candidate>
export TRPC_RELEASE_NONCE=<same-high-entropy-release-nonce-for-all-gates>
python scripts/kubernetes_runtime_gate.py --timeout-seconds 900 --require-runtime
```

PowerShell 等价写法是 `$env:TRPC_K8S_RUNTIME_TESTS_ENABLED = "true"`。Secret 清单必须由外部
Secret 管理系统生成，至少包含 `trpc-service-secrets` 和 `trpc-migration-secrets`；文件内容不会被
写入报告、日志或命令输出。生产镜像必须带 registry host 的完整
`registry/repository@sha256:<64-hex-digest>` 引用；本地 Docker image ID、未限定名、tag-only、
example/replace 占位镜像会被拒绝，升级镜像必须是可拉取的不同 digest。

运行器会先对完整 production overlay 执行静态契约检查，再在随机的 `trpc-runtime-gate-*` namespace
中部署隔离运行态。正式拓扑把 Gateway、Channel Dispatcher、Feishu/WeCom Connector 全部绑定到
production cluster；隔离 namespace 为避免争抢真实 provider 连接，只把 `trpc-wecom-connector` 缩为
0，并在证据中同时记录 `scope=unified_cluster_runtime`、`im_deployment=production_cluster`、完整正式
Deployment 集与本次实际测试集。真实飞书回调和企业微信长连接由同一集群的 online IM gate 验收，
签名 provider evidence 来自独立探针主机。运行器检查其余 Deployment readiness、滚动升级、worker 扩容、HPA `AbleToScale=True`、
Prometheus Adapter/KEDA 提供的 backlog external metric 必须存在且 `ScalingActive=True`；仅有
metrics-server 的 CPU/内存指标不满足本门禁，
namespace-scoped Pod Eviction/PDB 恢复、专用节点 cordon/drain/uncordon 和非强制优雅终止，最后无论
成功失败都删除该 namespace。节点 drain 只允许在显式标签、全量 Pod inventory 和二次 cordon 后
preflight 均证明专用的节点上执行；该测试允许删除这些生产 Pod 的临时 `/tmp` `emptyDir` 数据，
不会触碰其他 namespace 的工作负载。`--require-runtime` 用于发布门禁；未设置 opt-in 或缺少集群、权限、镜像、Secret
时，报告为 `gate=not_run` 并返回非零。运行器还会在隔离 namespace 中用一个不可用的 registry
digest 注入一次失败 rollout，要求 `rollout undo` 后 readiness 和已知良好 digest 恢复，再继续
Pod 驱逐与节点 drain。默认本地调用对这种未请求的 `not_run` 返回零，避免离线开发
误触发集群操作。

HPA 负载触发器必须是当前 checkout `scripts/` 目录下的绝对路径、非符号链接 Python 文件；默认驱动
是 `scripts/kubernetes_hpa_load_driver.py`，它在独立 context 中创建一个单次完成、无重试、带
`trpc.io/hpa-gate`、`trpc.io/hpa-run`、`trpc.io/hpa-phase` 和集群指纹标签的 bounded Job，并通过
Job UID/API 状态证明 `load` 与 `clear` 属于同一 nonce。`TRPC_K8S_RUNTIME_HPA_JOB_COMMAND` 必须是
由不可变镜像实现的 JSON 参数数组，负责应用特定的有限 backlog 操作；没有这个真实命令时门禁保持
`not_run`，不会用 sleep 或静态数字伪造扩容。门禁以无 shell 的受限子进程分别调用它的
`TRPC_K8S_HPA_PHASE=load` 和 `clear` 阶段，且每次受
`--timeout-seconds` 限制（必须为有限的 `(0, 3600]` 数值）。触发器只负责产生和清理受控 backlog，
不得写入或提交 `hpa-evidence.json`。门禁会在触发前、触发后、清理后分别通过 Kubernetes API 读取
`trpc-worker` HPA 的 backlog metric、`desiredReplicas`、`currentReplicas`，以及 worker Deployment
的 `readyReplicas`；报告中的 HPA 数值全部来自这些 API 读取，触发器自报的 JSON 永远不作为证据。
因此没有可用的仓库内触发器或无法观察到三阶段 API 变化时，HPA 运行态检查必须保持失败/`not_run`。
触发器文件还必须匹配显式的 SHA-256。它只能获得独立 kubeconfig 和显式 context；门禁通过
`kubectl auth whoami -o json`/SelfSubjectReview 将实际身份绑定到声明的 ServiceAccount，再通过
`SelfSubjectRulesReview` 在目标、`default`、`kube-system` 和 cluster scope 做完整规则审计。主验收账号会在
本次随机 namespace 中临时创建最小 Role/RoleBinding，绑定显式的 service-account subject，并由服务器端
RBAC 证明该账号只能在这里创建、查询和删除负载 Job，并读取对应 Pod/日志，同时明确不能读取 Secret、
修改 Deployment、删除 Pod、访问 Node、创建 Namespace 或拥有通配权限。与主验收 kubeconfig 重用路径、
硬链接、内容或权限无法证明时，驱动不会启动。发布验证器会重新计算当前 driver digest，并核对 Job UID、
nonce、标签和删除证据，不能只信报告中自报的字符串。

namespace 同时带有 owner、run nonce、集群指纹和 Unix expiry 标签。进程被 kill -9 后，下次运行只会在
启动阶段清理本工具、当前集群、已过期且标签完整的最多 10 个残留 namespace；未过期、标签缺失或其他
集群的资源永远不会被删除。

默认报告为 `runs/multitenant/kubernetes-runtime.json`，其中 `production_gate` 与 `gate` 同步，
静态 Kustomize 渲染和控制器模型永远不会将运行态 `not_run` 升级为 `pass`。运行前请确保当前
kube context 使用专用测试权限，并确认镜像、数据库、Redis、对象存储和两个 Secret 都是可用的。

### 本机 kind 低风险运行态验收

仓库提供 `scripts/kind_runtime_gate.py`。它只接受 `kind-*` context，并在该 context 中可选安装
固定版本的 metrics-server `v0.9.0`；安装使用官方发布清单，且仅为 kind 的自签名 kubelet 增加
`--kubelet-insecure-tls`。不会修改 `deploy/kustomize` 生产清单。运行前必须先创建专用 kind 集群、
加载两个带当前 checkout 指纹的镜像；脚本会在创建验收 namespace 前验证镜像 label、metrics API
和至少一个 NodeMetrics 样本：

```powershell
Set-Location C:\path\to\trpc-agent-service
$ErrorActionPreference = "Stop"
$kindName = "trpc-runtime-gate-$PID"
$kindContext = "kind-$kindName"
$kindCleanupRequired = $false
$kindConfig = Join-Path ([IO.Path]::GetTempPath()) "$kindName.yaml"
$sourceFingerprint = .venv\Scripts\python.exe -c `
  "from pathlib import Path; from scripts.evidence_lineage import source_fingerprint; print(source_fingerprint(Path.cwd())['value'])"

try {
  docker build --pull=false --provenance=false -t trpc-agent-service:k8s-gate-a `
    --build-arg "TRPC_SOURCE_FINGERPRINT=$sourceFingerprint" `
    --label "io.trpc.agent-service.kind-rollout=initial" .
  docker build --pull=false --provenance=false -t trpc-agent-service:k8s-gate-b `
    --build-arg "TRPC_SOURCE_FINGERPRINT=$sourceFingerprint" `
    --label "io.trpc.agent-service.kind-rollout=upgrade" .
  $imageIds = @(docker image inspect --format '{{.Id}}' `
    trpc-agent-service:k8s-gate-a trpc-agent-service:k8s-gate-b)
  if ($LASTEXITCODE -ne 0 -or $imageIds.Count -ne 2 -or $imageIds[0] -eq $imageIds[1]) {
    throw "initial and upgrade images must have distinct immutable IDs"
  }

  @"
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
"@ | Set-Content -LiteralPath $kindConfig -Encoding utf8
  $kindCleanupRequired = $true
  kind create cluster --name $kindName --config $kindConfig --wait 120s
  if ($LASTEXITCODE -ne 0) { throw "kind create cluster failed" }

  # Bound every node before metrics-server or any acceptance workload exists.
  $kindNodes = @(kind get nodes --name $kindName)
  $workerNodes = @($kindNodes | Where-Object { $_ -match '-worker\d*$' })
  if ($LASTEXITCODE -ne 0 -or $kindNodes.Count -ne 3 -or $workerNodes.Count -ne 2) {
    throw "Kind acceptance requires one control-plane and two workers"
  }
  foreach ($node in $kindNodes) {
    docker update --restart=no --cpus=3 --memory=6g --memory-swap=6g `
      --pids-limit=1024 $node
    if ($LASTEXITCODE -ne 0) { throw "failed to bound Kind node $node" }
  }
  foreach ($node in $workerNodes) {
    kubectl --context $kindContext label node $node trpc-runtime-gate=acceptance --overwrite
    if ($LASTEXITCODE -ne 0) { throw "failed to label Kind worker $node" }
  }
  $env:TRPC_K8S_RUNTIME_NODE_NAME = $workerNodes[0]
  $env:TRPC_K8S_RUNTIME_NODE_LABEL = "trpc-runtime-gate=acceptance"

  # Load sequentially into the two workers only; do not duplicate application
  # layers in the control-plane node.
  foreach ($image in @("trpc-agent-service:k8s-gate-a", "trpc-agent-service:k8s-gate-b")) {
    kind load docker-image $image --name $kindName --nodes ($workerNodes -join ",")
    if ($LASTEXITCODE -ne 0) { throw "kind image load failed for $image" }
  }

  .venv\Scripts\python.exe scripts\kind_runtime_gate.py `
    --context $kindContext `
    --image trpc-agent-service:k8s-gate-a `
    --upgrade-image trpc-agent-service:k8s-gate-b `
    --source-fingerprint $sourceFingerprint `
    --install-metrics-server `
    --timeout-seconds 900 `
    --output runs\multitenant\kind-kubernetes-runtime.json
  if ($LASTEXITCODE -ne 0) { throw "Kind runtime gate failed" }
}
finally {
  if ($kindCleanupRequired) {
    # restart=no prevents Docker Desktop from reviving the acceptance nodes
    # after a host crash; this finally block removes the temporary cluster too.
    kind delete cluster --name $kindName
  }
  if (Test-Path -LiteralPath $kindConfig) {
    Remove-Item -LiteralPath $kindConfig -Force
  }
}
```

`--install-metrics-server` 是显式 kind-only 操作；缺少该参数时脚本只探测已有 metrics API，不能把
缺失的 metrics-server 当作 HPA 通过。metrics-server 清单来自官方固定发布地址：
`https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml`。
该 wrapper 仍会调用同一套 HPA bounded Job、专用节点驱逐和优雅终止运行态检查，因此运行前必须在
当前 shell 中设置上面列出的 `TRPC_K8S_RUNTIME_HPA_*`、`TRPC_K8S_RUNTIME_NODE_*` 环境变量；缺少
其中任一项会安全地写入 `not_run`，不会用静态指标冒充 HPA 通过。
验收会删除随机 `trpc-runtime-gate-*` namespace；上面的 `try/finally` 还会删除专用 kind 集群。
wrapper 会在 metrics-server 和任何验收 workload 之前，用 `kind-*` context 的 Kubernetes
Node 名称与 Docker Kind cluster/role label 做精确集合匹配，要求至少 1 个 control-plane 与 2 个
worker，并硬失败于 `restart!=no`、CPU 少于 2 核、
内存少于 4 GiB、swap 未锁定为内存上限或 PIDs 少于 768 的节点。PostgreSQL 使用
`100m/256Mi` requests、`500m/1Gi` limits，
Redis 使用 `50m/64Mi` requests、`250m/256Mi` limits。

Kind wrapper 传入本地模式后，只在临时生成的 overlay 中将固定副本压到 gateway/admin/worker=1/1/2、
其余后台角色各 1；滚动升级按 Deployment 严格串行等待，gateway HPA 范围为 1--2，worker
HPA 保留 2--4，仅覆盖本地 HPA/驱逐机制验证，不冒充 200 turn 生产容量。生产 overlay 不会被修改。不要在生产 context
上使用该安装参数；脚本会拒绝非 `kind-*` context。该 kind 验收仍不等价于托管 Kubernetes
节点驱逐或真实生产容量验收。
