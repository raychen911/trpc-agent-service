# 开发与源码调试入口

## 1. 阅读顺序

1. [配置类型](../trpc_service/config/models.py)：平台允许哪些模型/工具/后端。
2. [内部协议](../trpc_service/gateway/models.py) 与 [身份映射](../trpc_service/gateway/identity.py)：外部身份如何带 tenant namespace。
3. [HTTP](../trpc_service/web/app.py) → [Gateway](../trpc_service/gateway/service.py) → [Worker](../trpc_service/agent/worker.py)。
4. [Runtime](../trpc_service/agent/runtime.py) → SDK Runner：模型循环与共享 Session。
5. [租约](../trpc_service/storage/guard.py) 与 [SessionService 包装器](../trpc_service/storage/session_wrapper.py)。
6. [队列/Outbox 调度](../trpc_service/gateway/dispatcher.py) → [角色装配](../trpc_service/web/container.py)。
7. resources、tenant/approval、tool/execution_filter、migration、metrics。

## 2. 本地功能验证

这些入口使用本地 `OfflineModel`，适合在开发阶段快速检查核心流程，运行过程不产生模型调用费用。

| 命令 | 验证内容 | 正常结果 |
|---|---|---|
| `python -m trpc_service._cli demo list` | 查看已有演示场景 | 输出场景名称列表 |
| `python -m trpc_service._cli demo all` | 依次验证配置、隔离、存储、治理、迁移和消息链路 | 每个场景输出 `[PASS]` |
| `python -m trpc_service._cli demo e2e --json` | 验证一条消息从入队到模拟投递的完整过程 | `state=succeeded`、`delivered=1` |
| `python -m pytest tests/test_v3_resumption.py -vv` | 验证故障后的阶段恢复和结果复用 | 所有用例显示 `PASSED` |

`OfflineModel` 继承 SDK 的 `LLMModel`，按照固定规则返回内容。`OfflineRuntimeFactory` 会装配真实的 `LlmAgent`、Runner 和 SessionService，测试会经过用户 Event 写入、Agent 执行、历史会话读取和 post-turn 处理。

`demo e2e` 的完整路径是“任务入队 → Agent 执行 → Outbox 写入 → 企业微信模拟投递”。成功结果包含：

```text
state=succeeded
delivered=1
model_calls=1
network_calls=0
```

## 3. 推荐断点与观察项

- gateway.web_request/inbound_request：tenant、config_version、内部 ID、payload hash。
- AgentWorker._stream：Runtime borrow、Session lease、是否命中 replay。
- RequestTaggingSessionService.append_event：非 partial Event 的 request_id。
- `TenantRuntime.run` / `TurnFinalizer`：模型产生 final Event 后先记录执行阶段，再完成 Summary 和 Memory。
- Processor：result 保存→Outbox→succeeded→ACK。
- DeliveryWorker：binding 路由、HTTP Retry-After、外部 message ID。

调试时主要观察 `request_id`、`sequence`、`type`、`partial` 和 `author` 等元数据。SDK 最终 Event 可能带有完整文本，事件聚合器会据此替换此前的 partial 内容。

## 4. 新增 Tool

带有类型标注和 docstring 的函数注册到 `ToolRegistry`，再通过租户的 `allowed` 列表启用。内置示例是 `current_utc_time`。容器还提供 `knowledge_search`，它从 `tenant_scope` 读取租户和应用信息，模型只需要提交检索参数。

需要人工确认的工具加入 `confirmation_required`。审批参数 Hash 与实际调用参数相匹配，`ToolExecutionFilter` 保存每次执行记录。生产环境将记录写入 PostgreSQL：成功结果可直接复用，状态未知的操作进入人工核查。修改远端数据的工具会把平台幂等键传给业务系统，便于查询执行状态或进行补偿。

## 5. 新增存储、Channel、迁移 Provider

Storage Provider 通过 `create_session` 和 `create_memory` 返回符合 SDK 抽象基类的对象。`InvocationContext` 的 Pydantic 校验会在 Runtime 创建阶段确认类型。

Channel 实现 `normalize`、`deliver` 和 `close`，支持附件的通道再实现统一下载接口。

- Telegram 验证自己的 Webhook Secret。
- WeCom 从智能机器人 SDK 的认证回调中接收已经解码的 `WsFrame`。
- 企业微信的 `url + aeskey`、微信客服的 `media_id` 和 Telegram 的 `file_id` 均由对应 Adapter 下载。

附件在消息入队前保存到租户 Artifact，Worker 直接读取共享 Artifact 数据，无需持有通道的长连接客户端。

三类 IM 的本地页面、协议结构和测试入口见 [IM 接入与本地可视化验证](im.md)。

Migration Provider 为每个迁移阶段提供执行函数。服务根据迁移方向选择已注册的 Provider；找不到对应实现时返回 501。`records.py` 展示本地双写和对账逻辑，真实 SDK Session 导出由对应的 Redis/PostgreSQL Reader 与 Writer 完成。

## 6. 测试提交规范

综合回归测试文件以 `test_v3_*` 命名，真实依赖测试文件以 `test_integration_*` 命名。

```bat
python -m pytest -m "not integration and not live" -vv
python -m flake8 trpc_service tests examples
python -m compileall -q trpc_service tests
```

SDK 仓库保持原样。服务侧适配以 1.1.19 的源码和公开接口为准，兼容逻辑集中在平台 Adapter 中。
