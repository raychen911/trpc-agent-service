# Contract: SDK Validation CLI

## Purpose

为开发者、评审者和自动化环境提供同一个 SDK 最小验证入口。该入口仅验证
tRPC-Agent-Python 集成，不启动 HTTP 服务，不接收真实用户数据。

## Invocation

```powershell
uv run python -m trpc_service.agent.sdk_validation
uv run python -m trpc_service.agent.sdk_validation --json
```

### Options

| Option | Required | Meaning |
|---|---:|---|
| `--json` | no | stdout 只输出一个 UTF-8 JSON 文档 |
| `--help` | no | 显示用法并以成功状态退出 |

未知参数必须由参数解析器拒绝，并使用退出码 2。

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | 所有必需验证阶段通过 |
| 1 | 一个或多个验证阶段失败 |
| 2 | CLI 参数或调用方式错误 |

## Human-readable output

默认模式必须包含：

- 目标 SDK 版本与实际版本；
- 总体 PASS 或 FAIL；
- 七个固定阶段各自的 PASS、FAIL 或 SKIPPED；
- 事件总数和最终回复数；
- 失败阶段与安全错误分类；
- 明确的 `credential_required=false` 和 `external_model_calls=0` 结论。

不得依赖人类输出的标点、空格或颜色进行自动化判断。

## JSON output

`--json` 模式 stdout 必须只包含一个 JSON object，不得混入日志或进度文本。
JSON 使用下列稳定字段：

完整 CLI 验证固定执行两个 session、每个两轮，共 4 轮。下面示例中的
`final_response_count=4` 是规范值；`event_count=4` 是最小确定性模型当前的
示例值，SDK 若产生额外可见中间 Event 时可以更大。

```json
{
  "schema_version": "1",
  "run_id": "uuid",
  "status": "passed",
  "sdk": {
    "package": "trpc-agent-py",
    "expected_version": "1.1.19",
    "distribution_version": "1.1.19",
    "module_version": "1.1.19",
    "matches": true
  },
  "stages": [
    {
      "stage": "version",
      "status": "passed",
      "message": "SDK version matches baseline",
      "event_count": 0,
      "final_text": null,
      "error_type": null
    }
  ],
  "event_count": 4,
  "final_response_count": 4,
  "credential_required": false,
  "external_model_calls": 0
}
```

### JSON invariants

- `schema_version` 固定为字符串 `1`。
- `status` 只能为 `passed` 或 `failed`。
- `stages` 按 data-model 中的固定阶段顺序出现，每个阶段恰好一次。
- 成功时所有必需阶段为 `passed`，版本全部为 `1.1.19`。
- 成功时 `event_count` 为大于等于 4 的整数，`final_response_count` 必须为 4。
- 成功时 `credential_required` 为 false，`external_model_calls` 为 0。
- 失败时对应阶段包含稳定 `error_type`，不得包含完整异常堆栈。

动态的 run_id、时间戳或 SDK 自动标识不得用于重复性断言。

## stdout and stderr

- 正常人类摘要或 JSON 报告写入 stdout。
- 参数错误和无法构造报告的进程级错误写入 stderr。
- 验证失败若仍能构造报告，报告写入 stdout 并返回退出码 1。
- `--json` 模式的 stderr 不得复制 JSON 报告。

## Security contract

输出必须采用允许字段列表。以下内容不得出现于 stdout、stderr 或报告：

- API key、token、secret、数据库密码；
- 完整环境变量；
- 模型供应商请求头；
- 未清理的完整异常对象；
- 真实终端用户数据。

测试消息中的 validation token 是固定测试字符串，不属于凭据，但仍不得被误标记为
API token。

## Compatibility policy

- 新增可选 JSON 字段允许保持 schema_version 为 1。
- 删除字段、修改字段类型、改变退出码语义或阶段名称属于不兼容变更，必须提升
  schema_version 并更新契约测试。
- SDK 基线升级必须同时更新 Spec、Plan、锁文件、预期版本和兼容性测试。
