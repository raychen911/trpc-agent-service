# 强制 Agent Release 评测门禁

所有生产 Agent Release 必须绑定不可变 Eval Suite，并在晋级前完成可复现的 Eval Run；平台复用 `trpc-agent-py==1.1.19` Evaluation 能力，补充租户隔离的数据集、持久化结果、异步调度和发布门禁。门禁覆盖最终回复、工具轨迹、知识召回、安全拒答、时延、Token 与成本，并与当前生产 Release 做回归比较；跨租户泄露、越权工具、密钥泄露和禁用操作等确定性安全断言零容忍，LLM Judge 固定模型、提示词和评分标准且不得作为安全判定的唯一证据。Eval Run 必须记录 SDK、模型、Knowledge Revision、工具、策略和评测数据版本，评测数据受分级、加密、脱敏与授权约束；通过离线门禁后仍需灰度监测，触发安全阈值时停止推广或回滚。
