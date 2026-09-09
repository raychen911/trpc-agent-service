# 文档与测试导航

正式提交入口是[架构设计文档](../文档/架构设计文档.md)。本文档目录用于补充架构、配置、接口、测试、IM、迁移、安全和运维细节。

项目共收集 167 项测试：145 项离线测试、18 项 Redis/PostgreSQL 集成测试和 4 项 Live 条件检查。离线测试、数据库集成测试、静态检查和 wheel 构建均已通过。

模型调用、Compose 多进程、故障恢复和 Jaeger 均有对应的测试入口。三类 IM 已完成本地可视化协议闭环；真实账号可由验收环境通过统一 Live 入口验证。

| 文档 | 解决的问题 |
|---|---|
| [architecture.md](architecture.md) | 角色、请求状态、锁租约、Streams、Outbox 与恢复顺序 |
| [configuration.md](configuration.md) | 环境变量、租户快照、模型替换、存储和 Secret |
| [api.md](api.md) | Chat/SSE/异步任务、Channel、Artifact、Knowledge、Approval、Migration |
| [development.md](development.md) | 源码入口、Fake、调试和扩展 Provider |
| [testing.md](testing.md) | 每层测试测什么、准备条件、命令、预期输出和费用 |
| [customer-service.md](customer-service.md) | 微信客服配置、加密回调、分页、附件、人工接管、UNKNOWN与真实联调 |
| [im.md](im.md) | 企业微信、微信客服、Telegram 的统一协议和本地可视化验证 |
| [operations.md](operations.md) | Compose/Kubernetes、角色探针、停机和故障处理 |
| [migration.md](migration.md) | 双写、回填、校验、切读、回滚和状态机 |
| [security.md](security.md) | 租户边界、Tool Approval、预算、Secret、PII 与审计 |
| [../文档/交付清单.md](../文档/交付清单.md) | 题目逐项核对、交付文件、验证证据和已知边界 |

建议先阅读架构文档，再运行 `python -m trpc_service._cli demo all`。确认基本流程后，按照 `testing.md` 运行离线测试，并按需验证 Redis、PostgreSQL、真实模型和 IM。
