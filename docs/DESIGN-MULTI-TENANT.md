# 详设 1 · 多租户与节点部署

> 主文档：[PRD.md §1](PRD.md)　|　验证证据：[VERIFICATION.md](VERIFICATION.md)
> 本文为该章节的完整详设（spec 深度层）；与代码/实测不一致时，以后者为准。
> 小节编号沿用原 PRD 章号（如本篇 §N.x）；跨篇 § 引用指向对应编号的详设文件。

### 1.1 租户模型

租户是平台一等公民。核心模型（Python，Pydantic，与 SQL 表一一对应）：

```python
from pydantic import BaseModel, Field
from typing import Optional

class TenantConfig(BaseModel):
    tenant_id: str                                # 全局唯一租户标识
    name: str
    status: str = "active"                        # active / suspended / deleted

    app_config: dict                              # Agent 类型、系统提示词、最大轮数
    model_config: dict                            # provider / model_name / temperature /
                                                  # input_price_per_1m_usd / output_price_per_1m_usd
                                                  # （单价 USD/百万 token，0=不计成本；key 引用）
    tool_permissions: dict                        # allowlist / blocklist / 危险工具二次确认开关
    im_channel_config: dict                       # webhook_url / token 引用 / 消息格式模板
    data_backend_config: dict                     # session/memory/vector 后端类型与连接串
    audit_policy: dict                            # 日志保留天数、敏感字段列表、脱敏规则
```

> **模型单价（09-06 补）**：`ModelConfig` 含 `input_price_per_1m_usd`/`output_price_per_1m_usd`
> （默认 0 = 不计成本）。成本闭环据此把框架上报的 token 折算为 USD（`runtime/_settle_usage`），
> 是 `monthly_budget_usd`/`used_budget_usd` 预算硬限的数据来源；价格随 `model_config` JSON
> 列落库，经 Admin 更新后广播生效。

对应 SQL DDL（完整数据模型见 2.5）：

```sql
CREATE TABLE tenant (
    tenant_id       VARCHAR(64) PRIMARY KEY,
    name            VARCHAR(128) NOT NULL,
    status          ENUM('active','suspended','deleted') DEFAULT 'active',
    app_config      JSON NOT NULL,
    model_config    JSON NOT NULL,
    tool_permissions JSON NOT NULL,
    im_channel_config JSON NOT NULL,
    data_backend_config JSON NOT NULL,
    audit_policy    JSON NOT NULL,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_status (status)
);
```

### 1.2 节点部署拓扑与协作

- **Gateway 多实例无状态**：仅做协议接入 + Filter 治理 + 路由，不持有任何用户状态。
- **Worker 多实例无状态**：执行推理，所有状态读写共享后端；任意节点可处理任意 session。
- **协作关系**：Gateway 经服务发现/负载均衡投递事件到 Worker；Worker 经 Storage Adapter 读写后端；回复经 Channel Adapter 回投 IM；全程 trace 贯穿。

### 1.3 消息路由（无需 Sticky Session）

**核心决策：Worker 完全无状态，不依赖 Sticky Session。**

1. **状态外置**：session state、对话历史、memory 全部存共享后端（Redis/SQL），Worker 本地零状态。
2. **请求级租户上下文**：Gateway 通过 Filter 从 URL 子域名 / Header `X-Tenant-ID` / webhook path 提取 `tenant_id`，注入运行上下文。Worker 从上下文取配置，不关心自己调度到哪台机器。
3. **Session ID 生成规则**：

```python
import hashlib

def generate_session_id(tenant_id, channel_type, channel_id, external_user_id, is_group=False):
    raw = f"{tenant_id}:{channel_type}:{channel_id}:{'group' if is_group else 'user'}:{external_user_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
```

- `channel_type`: `wechat_work`（企业微信）/ `feishu`（飞书）/ `web`（本地自测，不计入正式 IM）
  / `wecom_bot`（企微智能机器人·长连接，第二接入形态，支持群聊 @，见 §3.3）
  / `feishu_sdk`（飞书官方 SDK·长连接，与手写 webhook `feishu` 并存，见 §3.3）
- 群聊：`external_user_id` 替换为 `group_id + "_" + user_id`；wecom_bot 群聊以 `chatid` 作 `channel_id`（`is_group` 按回调 `chattype`），单聊以 `aibotid` 作 `channel_id`

4. **并发写入一致性**：Redis 分布式锁（`SET lock:session:{tenant}:{sid} NX EX 30`）串行化同一 session 的并发写入（见 2.3-A）。

### 1.4 租户隔离机制

| 隔离维度               | 实现策略                      | 技术细节                                                                         |
| ---------------------- | ----------------------------- | -------------------------------------------------------------------------------- |
| **配置隔离**     | 配置表 + 本地缓存             | 启动加载全量租户配置到本地 LRU；Worker 经 ctx 读取，减少 DB 查询                 |
| **数据隔离**     | 行级隔离（Shared Schema）     | 所有表含`tenant_id`，查询强制 `WHERE tenant_id = ?`；高合规租户可独立 schema |
| **工具权限隔离** | Filter 拦截 + 动态 Agent 构建 | 按`tool_permissions` 过滤可用工具；危险工具触发二次确认                        |
| **IM 用户权限隔离** | `UserAuthFilter` 用户级校验（⚠️ 已设计、代码待补，见 §4.1） | 同一租户下按 `im_channel_config.user_acl` 白/黑名单判定该 IM 用户能否使用 bot（区别于租户级鉴权，详见 §4.1） |
| **日志脱敏**     | Filter 层正则替换             | PII 规则（手机号/身份证/银行卡）命中替换为`[REDACTED]`                         |
| **密钥管理**     | KMS + 运行时解密              | IM token、模型 key、DB 密码密文存储，启动解密到内存，**不落盘、不入日志**  |

---

### 1.5 首启自动播种与模型对齐

#### 1.5.1 首启自动播种（`ensure_demo_tenant`）

**动机**：租户配置统一回源共享 SQL 存储（§1.5 热更新/回滚基础），但空库时 Gateway 回退
内置 demo 配置而 Admin PUT 报 404「租户不存在」——两侧配置源分裂。

**语义**（bootstrap.py `ensure_demo_tenant`）：
- **仅空库播种**：`tenant_store.list()` 非空即放弃（尊重 Admin 删除 demo 的运维意图）；
- **并发首启安全**：两节点同时判定空库并 create，后到者撞唯一键 IntegrityError，捕获后放弃（对方已播种同款配置）；
- 播种内容 = 内置 demo 配置（`demo_tenant_dict`，与 load_tenant 兜底共用同一份，含随启动模式生成的 backends）。

**收益**：demo 租户首启即有 SQL 持久化行，Admin 可直接热更新/回滚，BudgetTracker 预算闭环对 demo 亦生效。

#### 1.5.2 demo 模型对齐修复（`reconcile_demo_model`）

**场景**：demo 曾在 mock runner 下播种（model.provider=mock），运维切 framework runner 重启后，
租户回源读到 mock 模型——`build_llm_model` 无对应密钥注入路径，请求时崩溃。

**修复**：启动时若 demo 的 provider 仍为 `mock` 且本次以 framework 启动，则对齐为
`DEMO_MODEL_BY_RUNNER["framework"]`（deepseek）。**边界**：运营者经 Admin 自定义过模型的租户
（provider 非 mock）一律不触碰。

> 回归防护：`tests/test_bootstrap.py`（对齐修复生效 + 自定义模型不触碰）。实测：空库首启
> 直接播种 framework 档（无需修复），被污染库存量自动收敛。
