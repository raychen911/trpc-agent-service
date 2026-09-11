# 数据同步、一致性与幂等

## 写入顺序与跨节点可见性

同一 Session 的并发 Worker 先取得 Redis 分布式租约。SDK Runner 产生 Event 后，由选定的 Redis 或 SQL state backend 保存 Event，再更新 State，再生成 Summary；Memory 写在该 turn 的持久化链路后。共享 Redis/SQL 是下一 Worker 的读取来源，因此完成写入后跨节点可见；租约避免两个 Worker 同时改同一 Session。Artifact 则先上传不可变对象、后在 PostgreSQL 目录发布 `available`，失败保留非可读 pending 或清理，不把半写对象当成功。

这不是所有对象的全局强一致：同一 Session 以租约取得串行一致性，PostgreSQL 配置/收据/目录使用事务与唯一约束，S3 对象与 SQL 发布是显式状态机，指标与 exporter 是尽力而为。选择换取了多 Worker 无状态、低延迟 Redis 会话访问与可审计 SQL 终态；代价是 Redis、PostgreSQL、对象存储均需运维，且用量异步累加失败只会延后预算阻断。

## 重复、乱序与失败

`message_receipts` 的 tenant/channel/user/session/message 业务唯一键先 claim：相同正文且已终态重放、进行中拒绝、不同正文同 key 为冲突，均不重跑模型或 Tool。IM 输入在 binding 后经过 Redis 时间水位：更旧时间戳在创建 Worker task 前拒绝；相同时间戳交给 receipt 幂等。Redis 不可用时顺序门与启用限流均失败关闭。

IM 发送重试只作用于尚未发送的 SDK 操作，最多三次；已部分发送的结果不猜测、不重放执行。发送终态才写 delivery audit。模型、工具、数据库异常使用固定错误码；当前请求不跨 Worker 自动重试，避免副作用重复。

## 后端迁移

`state-backend-migrate` 的离线流程读取明确 tenant/app/version namespace，导出 canonical Event/State/Memory/Summary 快照及摘要 digest，再写入目标 Redis 或 SQL 并校验 digest；目标不匹配或源/目标不可用即失败，不切换配置。验证后用版本化 tenant config 将 `state_backend` 前向切换；回滚为新版本指向旧 backend，而非重写历史。Knowledge 向量索引迁移以 SQL 文档和版本为事实源，在目标端重建 tenant-scoped 索引，完成文档数、版本和抽样查询校验后切换索引别名；旧索引保留一个回滚窗口。
