# 详设 4 · 治理、监控和安全

> 主文档：[PRD.md §4](PRD.md)　|　验证证据：[VERIFICATION.md](VERIFICATION.md)
> 本文为该章节的完整详设（spec 深度层）；与代码/实测不一致时，以后者为准。
> 小节编号沿用原 PRD 章号（如本篇 §N.x）；跨篇 § 引用指向对应编号的详设文件。

### 4.1 Filter 链（租户级治理）

> **本节是 Filter 链顺序的「唯一权威定义」**；§0.1 架构图与 §0.3 运行时生命周期中的
> Filter 链均为引用本节，如有出入以本节为准。

复用 tRPC-Agent-Python 的 Filter AOP 机制，按序注册：

```
TraceFilter → AuditFilter → TenantResolveFilter → SignatureFilter → UserAuthFilter
→ RateLimitFilter → BudgetFilter → ToolWhitelistFilter → PIIFilter
```

> ⚠️ **实现状态（2026-09-02 校准）**：`UserAuthFilter`（IM 用户级权限校验）是 Problem.md
> 明确要求的 Filter 治理能力之一，**已于 09-02 补实现**（`filters/impl.py`，读
> `ImChannelConfig.user_acl` 黑白名单，4 条单测覆盖 allow/block/不在白名单/disabled）。

> `SignatureFilter` / `TenantResolveFilter` 做的是**租户级**鉴权（这个 webhook 属于哪个租户、
> 签名对不对）；`UserAuthFilter` 做的是**用户级**权限校验（同一租户下，这个 IM 用户是否
> 被允许使用该 bot——如内部员工白名单、黑名单拉黑），两者层次不同，不可混为一层。

> **危险工具二次确认：网关 Filter + 运行时门控双层（09-06 补齐；09-04 默认反转）**：
> 1. 网关侧 `ToolWhitelistFilter` 只拦截**事件声明**的工具（metadata 带 tool_name/确认标记）；
> 2. 真实 LLM 动态调工具在框架 Runner 侧，不经过网关 Filter。现于 `tool/builder.py` 的 `make_tool_impl`（框架 FunctionTool 的包装
>    impl）内加**运行时门控**：未二次确认（`confirmed_tools` 不含该工具）的危险工具不执行
>    `spec.func`，返回「需确认」提示回填 LLM；`confirmed_tools` 由 Runtime 从事件
>    `metadata.tool_confirmed` 注入。单测覆盖拦截/放行/非危险工具三路径。
>    演示租户不把危险工具放入 allowlist（门控以单测证明，不依赖 demo 数据）。
> 3. **默认反转（09-04 全项目审查）**：平台在工具定义上标记 `dangerous` 即视为需确认——
>    租户 `dangerous_tools` 名单是**追加**而非前置条件（此前「平台声明危险但租户漏配就不拦」
>    违反安全默认）；租户 `require_confirmation=False` 仍可显式关闭。

关键示例（用户级权限 + 工具白名单二次确认）：

```python
class UserAuthFilter(BaseFilter):
    """IM 用户级权限校验（区别于租户级鉴权）。"""

    async def run(self, ctx, req, handle):
        tenant_cfg = get_tenant(ctx.tenant_id)
        rules = tenant_cfg.im_channel_config.get("user_acl", {})
        if req.user_id in rules.get("blocklist", []):
            raise FilterBlocked(f"user blocked: {req.user_id}", "user_blocked")
        allowlist = rules.get("allowlist", [])
        if allowlist and req.user_id not in allowlist:
            raise FilterBlocked(f"user not allowed: {req.user_id}", "user_not_allowed")
        return await handle(ctx, req)

class ToolWhitelistFilter(BaseFilter):
    async def run(self, ctx, req, handle):
        tenant_cfg = get_tenant(ctx.tenant_id)
        if req.tool_name not in tenant_cfg.tool_permissions["allowlist"]:
            raise PermissionError(f"tool {req.tool_name} not allowed")
        if is_dangerous(req.tool_name) and not req.confirmed:
            raise SecondConfirmationRequired(req.tool_name)
        return await handle(ctx, req)
```

### 4.2 监控指标

| 类别    | 指标                                             | Labels                         | 状态 |
| ------- | ------------------------------------------------ | ------------------------------ | ---- |
| 请求量  | `agent_requests_total`                         | tenant_id, channel, agent_name | ✅ 已接线 |
| 模型    | `llm_latency_seconds` / `llm_tokens_*_total` | tenant_id, model, status       | ✅ 已接线 |
| 工具    | `tool_calls_total` / `tool_latency_seconds`  | tenant_id, tool_name           | ✅ 已接线（09-05：延迟在 `make_tool_impl` 真实执行路径计时，门控拦截不计） |
| IM 投递 | `im_delivery_success/failed/retry_total`       | tenant_id, channel, msg_type   | ✅ 已接线（webhook 投递路径；retry 记未送达类连接错误重试，09-05） |
| 成本    | `tenant_cost_usd`（Counter）/ `tenant_budget_usd`（Gauge） | tenant_id（+cost_type）        | ✅ 已接线（成本经执行审计结算；预算 Gauge 由 BudgetFilter 每请求刷新，09-05） |
| 错误    | `agent_errors_total`                           | tenant_id, error_type, stage   | ✅ 已接线（Runner 错误） |
| 运维    | `active_sessions`（Gauge）                      | tenant_id                      | ✅ 已接线 |

> 注（09-05）：`storage_op_latency_seconds` 已从指标表移除——存储层逐方法埋点侵入面大、
> 且会为凑指标造无效埋点；后端延迟观测由 `/readyz` 依赖检查与 Redis/SQL 自身监控承担。
> 自研 `trace_id` 贯穿承担 tracing 职责（Problem 允许「或等价 tracing」）；OpenTelemetry
> 依赖与 `otlp_endpoint` 配置字段已删除，不再保留未接线的预留项。

> ✅ **实测状态（2026-09-02 校准）**：平台指标前缀 `teneuris_`。实测起 gateway 后访问 `/metrics`，
> 输出 `teneuris_agent_requests_total{agent_name=...,channel="web",tenant_id="demo"}` 且随请求递增。
> 09-06 校准：原先「覆盖上表 7 类」的表述与代码不符——`llm_tokens_*`/`tenant_cost`/
> `im_delivery_*`/`active_sessions` 定义但零调用；现已接线（Runtime `_settle_usage` 记 token 与
> 成本、webhook 投递记 im_delivery、handle 记 active_sessions），单测验证指标递增。
>
> **成本闭环（09-06 补齐，代码在 `runtime/runtime.py::_settle_usage` + `tenant/budget.py`）**：
> 框架事件 `usage_metadata`（实测 v1.1.19：agent 级 Event 直接暴露，`prompt_token_count`=输入 /
> `candidates_token_count`=输出）经 `translate_event` 透传到 RunnerEvent → Runtime 累计 →
> `cost = in/1e6×单价 + out/1e6×单价`（单价来自 `ModelConfig.input/output_price_per_1m_usd`，0 则 cost=0）→
> 写执行审计 + `tenant_cost` 指标 + `BudgetTracker.record`（SQL 原子累加 `used_budget_usd` +
> 缓存/广播失效，BudgetFilter 下一请求硬限生效）。缺 key/无单价时 tokens 照计、cost=0，不抛错。

### 4.3 全链路追踪（等价 tracing）

> **实现口径（2026-09-02 校准）**：Problem.md 要求「OpenTelemetry **或等价 tracing**」——
> 当前实现为**等价方案：自研 `trace_id` 贯穿**（`TraceFilter` 注入 `uuid`，贯穿 Filter 链 →
> Runtime → 审计日志 → 响应体，实测每次请求均带回 trace_id）。OTel 的 span 导出（W3C
> `traceparent` 传播、`im_webhook_receive → … → audit_log_write` span 链）列为**生产演进**，
> 接入时每段附加 `tenant_id / session_id / msg_id` attribute，现有 trace_id 可直接作为
> OTel trace_id 来源。

### 4.4 审计日志字段

`tenant_id, channel, user_id, session_id, agent_name, tool_name, decision, latency_ms, error_type, cost, trace_id, input_tokens, output_tokens, metadata, created_at`（覆盖题目要求的最小集合）。

> **审计双行语义（09-06 校准）**：链路有两层审计，互不覆盖——
> 1. **网关治理审计**：AuditFilter 记 `decision=allow/block`（含限流/预算/越权等被阻断流量），
>    在 Filter 链内、Agent 执行前落库；
> 2. **执行审计（Runtime `_settle_usage`，09-06 新增）**：Runner 真实跑完后记
>    `decision=executed / execution_error`，含真实工具列表（`tool_name` 逗号拼接）、
>    token 用量与成本（`payload.input_tokens/output_tokens`、`cost`），错误路径同样留痕
>    （可能已消耗部分 token）。两次请求各一行，trace_id 相同可串联。
> 修正前执行结果完全不留审计（cost 恒 0、`used_budget_usd` 无累加点），属「文档声称 ≠
> 代码行为」，09-06 已闭环。

### 4.5 密钥管理与脱敏

- **密钥**：KMS/Vault 加密存储，运行时解密到内存缓存（TTL），绝不落盘/入日志/入 trace/返回客户端。
- **脱敏**：Filter 层正则替换手机号、身份证、API Key 等为 `[REDACTED]`；日志结构化 + 级别可控。
- **异常/错误信息脱敏**：异常堆栈、错误报告同样禁止携带连接串或明文 key——禁止
  `except Exception as e: return str(e)` 这类把原始异常透传给客户端/审计的写法；
  错误统一经错误类型枚举（`error_type`）+ 脱敏后的简要描述返回，完整堆栈仅写内部日志
  且先过脱敏 Filter（典型泄漏点：DB 连接失败 traceback 含 DSN、模型调用失败含 api key）。

**生产演进（阶段四设计并入）**：现状为环境注入（密钥仓库/`.env` → 环境变量，已 gitignore）+ 结构化日志脱敏；
生产接 KMS/Vault，Pod 侧 `secretRef` 注入 + 动态轮换；`api_key_ref`/`token_ref` 由 `TenantRepository`/`SqlTenantStore`
`model_dump(exclude=...)` 排除，禁止明文回显（`test_admin_secret_not_leaked` 覆盖）。

---

### 4.6 生产安全 fail-closed 校验（`enforce_production_safety`）

`PlatformSettings` 的 model_validator：`env=prod` 时任一不满足即**启动即拒**（错误逐条列出）；
`dev/test` 不触发，保证本地开箱即用。

| # | 校验项 | 依据 |
| --- | --- | --- |
| 1 | `admin.api_key` 必填 | Admin 持有租户配置与审计查询，无鉴权即裸奔（PRD 4.5） |
| 2 | `log_level != DEBUG` | 详尽日志放大敏感信息泄露面（PRD 4.5） |
| 3 | `pii.enabled` 强制开启 | 脱敏是生产硬性要求（PRD 4.5） |
| 4 | SQL DSN 禁 sqlite | 单文件库与多节点水平扩展冲突（PRD 2.2） |
| 5 | Redis DSN 禁本机默认值 | 默认指向 127.0.0.1，生产须显式配置共享实例（PRD 1.3） |
| 6 | Prometheus 强制开启 | 生产必须有监控打点（PRD 4.2） |

**实测**：非法 prod 配置启动输出逐条违规并 exit=1；合规配置正常起服，
「无鉴权裸奔」警告消失。错误经 loader 包装为 `ConfigError`，报错可读。
