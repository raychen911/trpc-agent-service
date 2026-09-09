# 开发与源码调试入口

## 1. 阅读顺序

1. [配置类型](../trpc_service/config/models.py)：平台允许哪些模型/工具/后端。
2. [内部协议](../trpc_service/gateway/models.py) 与 [身份映射](../trpc_service/gateway/identity.py)：外部身份如何带 tenant namespace。
3. [HTTP](../trpc_service/web/app.py) → [Gateway](../trpc_service/gateway/service.py) → [Worker](../trpc_service/agent/worker.py)。
4. [Runtime](../trpc_service/agent/runtime.py) → SDK Runner：模型循环与共享 Session。
5. [租约](../trpc_service/storage/guard.py) 与 [SessionService 包装器](../trpc_service/storage/session_wrapper.py)。
6. [队列/Outbox 调度](../trpc_service/gateway/dispatcher.py) → [角色装配](../trpc_service/web/container.py)。
7. resources、tenant/approval、tool/execution_filter、migration、metrics。

## 2. 不花模型费用的入口

```bat
python -m trpc_service._cli demo list
python -m trpc_service._cli demo all
python -m trpc_service._cli demo e2e --json
python -m pytest tests/test_v3_resumption.py -vv
```

OfflineModel 继承 SDK LLMModel，只返回确定性 echo；OfflineRuntimeFactory 使用真实 LlmAgent/Runner/SessionService。它比“Fake Runtime 直接吐 Event”多覆盖 SDK 类型检查、用户 Event 写入、历史与 post-turn 生命周期。

e2e Demo 实际入队、执行模型、写 Outbox、Fake WeCom 投递；预期 state=succeeded、delivered=1、model_calls=1、network_calls=0。这不是双进程或真实 IM 验收。

## 3. 推荐断点与观察项

- gateway.web_request/inbound_request：tenant、config_version、内部 ID、payload hash。
- AgentWorker._stream：Runtime borrow、Session lease、是否命中 replay。
- RequestTaggingSessionService.append_event：非 partial Event 的 request_id。
- `TenantRuntime.run` / `TurnFinalizer`：模型产生 final Event 后先记录执行阶段，再完成 Summary 和 Memory；重试时只补齐未完成的阶段。
- Processor：result 保存→Outbox→succeeded→ACK。
- DeliveryWorker：binding 路由、HTTP Retry-After、外部 message ID。

只观察 request_id/sequence/type/partial/author 等元数据；不要将 prompt、工具完整参数、Token 写日志。SDK 最终 Event 可能带完整文本，不能再和此前 partial 全量拼接。

## 4. 新增 Tool

把有类型标注、docstring 的函数注册到 ToolRegistry，租户 allowed 显式启用。内置 current_utc_time；容器提供 knowledge_search，它从 tenant_scope 取租户/应用，不让模型传 tenant 参数。

需要人工确认的工具加入 `confirmation_required`。审批参数 Hash 必须与实际调用一致，`ToolExecutionFilter` 负责保存执行记录。生产环境把记录保存在 PostgreSQL 中：成功结果可以复用，状态未知时交由人工核查。对于会修改远端数据的工具，还应把平台幂等键传给业务系统；支付、写文件或发送消息等结果未知的操作不能自动重试。

## 5. 新增存储、Channel、迁移 Provider

Storage 实现 create_session/create_memory 返回真正符合 SDK ABC 的对象；只有 __getattr__ 的普通代理会被 SDK InvocationContext 的 Pydantic 类型检查拒绝。

Channel 实现 `normalize`、`deliver` 和 `close`，支持附件的通道再实现统一下载接口。

- Telegram 验证自己的 Webhook Secret。
- WeCom 只接受智能机器人 SDK 解码后的 `WsFrame`，普通 HTTP Frame 会被拒绝。
- 企业微信的 `url + aeskey`、微信客服的 `media_id` 和 Telegram 的 `file_id` 均由对应 Adapter 下载。

附件在消息入队前保存到租户 Artifact，因此 Worker 不需要持有长连接客户端。

三类 IM 的本地页面、协议结构和测试入口见 [IM 接入与本地可视化验证](im.md)。

Migration 必须提供实际阶段函数；未注册默认 501，不能只改 phase。参考 records.py 可学习本地双写/对账，不应当作真实 SDK Session 导出。

## 6. 测试提交规范

每个测试都应说明测试目的、使用真实组件还是 Fake、前置条件、输入、核心断言、预期结果、排错位置和费用情况。综合回归测试文件以 `test_v3_*` 命名，真实依赖测试文件以 `test_integration_*` 命名。

```bat
python -m pytest -m "not integration and not live" -vv
python -m flake8 trpc_service tests examples
python -m compileall -q trpc_service tests
```

服务不修改 SDK 仓库。需要复用 SDK 能力时，以本地 1.1.19 的源码和公开接口为准，不根据设计文档推测接口是否存在。
