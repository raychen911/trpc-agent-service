# Quickstart: tRPC-Agent SDK 最小集成验证

本指南定义 SDK 最小验证的运行和验收方式。实现已完成，以下命令可直接在项目根
目录执行；最近一次实测结果记录在 [validation-results.md](./validation-results.md)。

## Prerequisites

- Python 3.12；
- uv；
- 已进入 `trpc-agent-service-main` 项目根目录；
- 初次同步依赖时能够取得 `trpc-agent-py==1.1.19`；
- 运行验证不需要任何模型 API Key。

依赖首次下载可能需要网络，但 SDK 验证执行本身不得访问真实模型服务。

## 1. Prepare environment

```powershell
uv sync --group dev
```

预期结果：

- 命令成功；
- 锁定并安装 `trpc-agent-py==1.1.19`；
- 没有要求输入模型凭据。

## 2. Run human-readable validation

```powershell
uv run python -m trpc_service.agent.sdk_validation
```

预期摘要：

```text
SDK target: 1.1.19
SDK actual: 1.1.19
version: PASS
initialization: PASS
single_turn: PASS
event_finalization: PASS
session_continuity: PASS
session_isolation: PASS
offline_safety: PASS
credential_required: false
external_model_calls: 0
RESULT: PASS
```

字段值必须符合
[SDK Validation CLI contract](./contracts/sdk-validation-cli.md)，实际排版可以略有
不同。

## 3. Run machine-readable validation

```powershell
uv run python -m trpc_service.agent.sdk_validation --json
```

预期结果：

- 退出码为 0；
- stdout 是单个合法 JSON object；
- `status` 为 `passed`；
- 发行包版本和模块版本均为 `1.1.19`；
- 七个阶段均为 `passed`；
- 完整运行包含两个 session、每个两轮，`final_response_count` 为 4 且
  `event_count` 大于等于 4；
- `credential_required` 为 false；
- `external_model_calls` 为 0。

## 4. Run automated verification

```powershell
uv run pytest tests/sdk_validation -q
```

预期结果：

- 所有测试通过；
- 覆盖单轮执行、唯一最终回复、双轮上下文连续性、跨 session 隔离和重复运行；
- 覆盖版本不匹配、无最终 Event 和上下文丢失三类失败诊断；
- 测试不要求真实 API Key；
- 测试期间任何外部 socket 连接都会失败，从而证明没有真实模型调用。

## 5. Repeatability check

连续执行两次 JSON 验证：

```powershell
uv run python -m trpc_service.agent.sdk_validation --json
uv run python -m trpc_service.agent.sdk_validation --json
```

排除 `run_id`、时间戳、`event_id` 和 `invocation_id` 后，两次报告中的版本、
阶段状态、事件数量、最终文本和安全结论必须一致。

## 6. Timing acceptance

使用 PowerShell 分别测量首次准备到获得验证报告的总耗时，以及依赖完成后的测试
耗时：

```powershell
Measure-Command {
    uv sync --group dev
    uv run python -m trpc_service.agent.sdk_validation --json
}

Measure-Command {
    uv run pytest tests/sdk_validation -q
}
```

验收要求：

- 新开发者在满足 prerequisites 的干净项目环境中，10 分钟内完成同步并获得报告；
- 依赖已同步后，SDK 验证测试套件在普通开发机上 10 秒内结束；
- 两项实际耗时必须记录到 `validation-results.md`，不得只写“符合预期”。

## Troubleshooting

| Symptom | Expected stage | Action |
|---|---|---|
| 安装版本不是 1.1.19 | version | 重新同步锁文件并检查依赖约束 |
| SDK 无法构造 Agent 或 Runner | initialization | 对照 research.md 中的 1.1.19 接口 |
| 没有任何 Event | single_turn | 检查确定性模型是否产生 LlmResponse |
| Event 存在但无唯一最终回复 | event_finalization | 检查 is_final_response 与 get_text 处理 |
| 第二轮忘记第一轮 token | session_continuity | 检查同一 Runner/Session 服务是否复用 |
| 第二个 session 读到第一个 token | session_isolation | 检查 app、user、session 标识组合 |
| 发生网络连接尝试 | offline_safety | 检查是否误用了真实供应商模型 |

不得通过配置真实 API Key 来绕过失败；该验证必须保持离线和确定性。
