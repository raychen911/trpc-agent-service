# Data Model: tRPC-Agent SDK 最小集成验证

本功能不建立数据库表。以下模型仅存在于一次验证进程内，并通过 CLI 报告或测试
断言形成证据；Session 内容由官方 `InMemorySessionService` 管理。

## 1. SdkBaseline

表示待验证的 SDK 版本约束。

| Field | Type | Required | Rules |
|---|---|---:|---|
| package_name | string | yes | 固定为 `trpc-agent-py` |
| expected_version | string | yes | 本 Spec 固定为 `1.1.19` |
| distribution_version | string | yes | 从已安装发行包元数据读取 |
| module_version | string | yes | 从 SDK 版本模块读取 |
| matches | boolean | yes | 两个实际版本均等于 expected_version 时为 true |

### Validation rules

- 任一实际版本不可读取时，version 阶段失败。
- 发行包版本与模块版本不一致时，即使其中一个等于目标版本也必须失败。
- 版本失败后不得继续报告 Agent 执行成功。

## 2. ValidationRun

表示一次完整、隔离的验证执行。

| Field | Type | Required | Rules |
|---|---|---:|---|
| run_id | string | yes | 每次运行唯一，不作为重复性比较内容 |
| target_version | string | yes | 来自 SdkBaseline.expected_version |
| status | enum | yes | `pending`、`running`、`passed`、`failed` |
| started_at | timestamp | yes | UTC；不作为确定性内容比较 |
| completed_at | timestamp | no | 终止状态时必须存在 |
| stages | list of StageResult | yes | 阶段名唯一并保持契约顺序 |
| event_count | integer | yes | 非负，汇总所有场景可见事件 |
| final_response_count | integer | yes | 非负 |
| credential_required | boolean | yes | 成功报告必须为 false |
| external_model_calls | integer | yes | 成功报告必须为 0 |

### State transitions

```text
pending -> running -> passed
                   -> failed
```

- 一个阶段失败后，ValidationRun 最终状态必须为 failed。
- passed 状态要求所有必需阶段均为 passed。
- completed_at 只能在 passed 或 failed 时设置。
- 完整 CLI 验证固定执行两个 session、每个两轮；passed 状态要求
  final_response_count 为 4，event_count 大于等于 4。

## 3. StageResult

表示一个可独立诊断的验证阶段。

| Field | Type | Required | Rules |
|---|---|---:|---|
| stage | enum | yes | 见固定阶段列表 |
| status | enum | yes | `pending`、`passed`、`failed`、`skipped` |
| message | string | yes | 非敏感、可供开发者定位 |
| event_count | integer | yes | 非负 |
| final_text | string | no | 仅允许确定性测试回复 |
| error_type | string | no | 只记录稳定分类，不记录秘密 |

### Fixed stages

1. `version`
2. `initialization`
3. `single_turn`
4. `event_finalization`
5. `session_continuity`
6. `session_isolation`
7. `offline_safety`

### Validation rules

- stage 在同一 ValidationRun 中不可重复。
- failed 必须带 error_type 和安全 message。
- skipped 必须说明其前置阶段为何失败。
- passed 不得携带异常堆栈或环境变量内容。

## 4. ValidationConversation

描述一个由官方 Session 服务承载的测试对话。

| Field | Type | Required | Rules |
|---|---|---:|---|
| app_name | string | yes | 固定验证应用标识 |
| user_id | string | yes | 固定非敏感测试用户 |
| session_id | string | yes | 每个隔离场景唯一 |
| expected_token | string | yes | 固定测试 token，不是凭据 |
| turns | list of ValidationTurn | yes | 单轮场景 1 条，连续性场景 2 条 |

### Identity and relationship

- SDK Session 由 `app_name + user_id + session_id` 定位。
- 两轮连续性验证复用同一组合。
- 隔离验证保持 app_name 和 user_id 相同，只改变 session_id，以证明 session
  边界而非用户边界。
- expected_token 只存在于该 conversation 的第一轮输入和预期结果中。

## 5. ValidationTurn

| Field | Type | Required | Rules |
|---|---|---:|---|
| turn_index | integer | yes | 从 1 开始，同一会话严格递增 |
| input_text | string | yes | 非空、确定性、无秘密 |
| expected_text | string | yes | 非空、确定性 |
| observations | list of EventObservation | yes | 由 Runner 事件流产生 |
| final_text | string | no | 成功时必须等于 expected_text |

## 6. EventObservation

是官方 Event 的安全投影，不持久化完整 Event。

| Field | Type | Required | Rules |
|---|---|---:|---|
| event_id | string | yes | 来自 Event |
| invocation_id | string | yes | 非空 |
| author | string | yes | 非空 |
| partial | boolean | yes | 来自 Event |
| final_response | boolean | yes | 来自 `is_final_response()` |
| text | string | no | 由 `get_text()` 提取并限制为测试内容 |
| error_code | string | no | SDK 有错误结果时记录 |

### Validation rules

- 每个成功 turn 必须至少有一个 observation。
- 每个成功 turn 必须恰好有一个 final_response 为 true 且 text 非空的
  observation。
- 不保存原始请求、环境变量、API key、base URL 或完整异常对象。

## 7. ValidationEvidence

ValidationEvidence 不是独立运行时实体，而是 ValidationRun 的规范化视图：

- JSON CLI 报告；
- pytest 通过/失败结果；
- quickstart 中定义的预期输出。

比较重复运行时必须排除 run_id、时间戳、SDK 自动生成的 event_id 和
invocation_id，只比较版本、阶段状态、事件数量、最终文本和安全边界结论。
