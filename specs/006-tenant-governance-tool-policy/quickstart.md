# 第六阶段快速验证指南

本文定义实现完成后的本地验收步骤。自动化测试使用确定性 Agent、工具和 Channel SDK 替身，不需要真实模型密钥、真实危险工具或真实 IM Secret。

## 1. 前置条件

- Python 3.12、uv、Docker Desktop。
- 当前目录：`E:\grad_files\2026trpc-agent\trpc-agent-service-submit`。
- 已完成依赖安装：

```powershell
uv sync
```

- 使用与 `deploy/local-shared/compose.yaml` 一致的本地测试密码。密码只在当前 PowerShell 进程设置，不写入脚本、文档或 Git。

## 2. 启动共享后端

```powershell
docker compose -f deploy/local-shared/compose.yaml up -d --wait
docker compose -f deploy/local-shared/compose.yaml ps
uv run trpc-agent-shared-init
```

预期：Redis 与 PostgreSQL 为 healthy，schema 初始化成功。若认证失败，先核对当前进程中的 Redis/PostgreSQL 环境变量是否与 Compose 配置一致，不要把密码粘贴到 issue 或测试记录。

## 3. 分层自动化验证

以下测试目录由本阶段实现任务创建；每一组都应先记录失败（RED），实现后记录通过（GREEN）。

```powershell
uv run pytest tests/unit/governance -q
uv run pytest tests/contract/governance -q
uv run pytest tests/integration/governance -q
uv run pytest tests/e2e/governance -q
uv run pytest -q
```

预期：所有新增治理测试通过，第二、第三和第五阶段回归无失败。共享后端未启动时，只有明确标记的共享测试允许 skip；正式共享验收不得把连接失败当作通过。

## 4. ACA 决策专项验收

### 4.1 A：策略收紧立即生效

1. 租户 A 当前允许确定性工具，租户 B 禁止同名工具。
2. 两个租户发送相同请求，验证仅 A 的工具计数增加。
3. 在 A 的 Agent 已选择工具、尚未执行时激活收紧版本。
4. 两个 Worker 即使保留旧解析缓存，也必须在 tool callback 读取新 active 版本并拒绝。

验收证据：工具执行次数为 0；审计记录新策略版本和 `policy_stale`/`tool_denied`；已完成副作用没有治理层回滚记录。

### 4.2 C：文本与按钮共享一次性确认

1. 分别由飞书与企业微信 SDK 替身触发危险确定性工具。
2. 验证回复同时包含安全文本确认编号和最小按钮。
3. 对同一 confirmation 执行“先文本、后按钮”和“先按钮、后文本”。
4. 每种顺序至少并发/重放 10 次，并加入其他用户、Session、tenant、过期和参数变化场景。

验收证据：每个危险操作的工具执行次数最多 1；两个入口关联相同 confirmation/execution；无效请求均稳定拒绝。

### 4.3 A：按单次最大额度严格预占

1. 为测试租户设置临界预算和确定性 maximum/actual。
2. 从两个 Worker 同时发起至少 20 个请求。
3. 验证只有能完整预占四个维度 maximum 的请求获准。
4. actual 小于 maximum 时验证差额释放；重复结算、重启恢复与回复失败不得再次扣减。

验收证据：每个账户始终满足 `settled + reserved <= hard_limit`；剩余额度小于 maximum 时拒绝；相同 execution 只结算一次。

## 5. 双租户与内容安全验收

- 两租户使用相反工具、主体、预算和内容策略，分别通过飞书/企业微信测试替身发送相同文本。
- 验证 tenant、授权、Session、确认、预算、成本和审计均无串用。
- 用仅供测试的手机号、邮箱、token/secret 标记覆盖入站、工具参数、Agent 输出和回复。
- 检查 stdout/stderr、测试日志、审计样本、错误结果和数据库持久化样本。

预期：需要脱敏的内容只出现稳定占位符；必须拒绝的凭证停止传播；原始测试敏感值命中数为 0。

## 6. 故障与跨节点恢复验收

依次在以下阶段终止 owner Worker：预算已预占但未执行、确认已 claim 但未执行、Agent/工具已开始、结果已保存但未结算、结算完成但未发送回复。

预期：

- 执行前阶段允许新 generation 安全释放或接管。
- 已开始且结果未知进入 `REVIEW_REQUIRED/OUTCOME_UNKNOWN`，不自动重放。
- 已有确定结果只补齐结算、审计或回复交付。
- 旧 owner 恢复后的写入被 fencing 拒绝。
- Agent、危险工具与预算实际结算次数均不超过 1。

## 7. 审计检查

按 trace 查询允许、拒绝、待确认、确认成功、预算不足和依赖故障事件。每条证据应能关联：tenant、channel、主体摘要、Session、Agent、policy version、tool、decision/reason、reservation/actual、错误、成本及 `trace_id`、`owner_trace_id`、`execution_trace_id`。

不得记录主体原值、确认码、完整工具参数、Secret、数据库密码或高基数身份指标标签。

## 8. 停止环境

保留本地数据以便复查：

```powershell
docker compose -f deploy/local-shared/compose.yaml down
```

只有明确需要全新数据库并已确认不再需要本地证据时，才使用带 `-v` 的清理命令。

## 9. 验收记录模板

每组测试在后续 `validation-results.md` 记录：日期、Git commit、环境、执行命令、RED 失败原因、GREEN 结果、通过/失败/skip 数、关联用户故事、FR、DEC 和证据文件。禁止粘贴真实凭证、response URL 或未经脱敏的完整日志。
