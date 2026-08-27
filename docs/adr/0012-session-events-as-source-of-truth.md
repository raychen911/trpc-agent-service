# 以不可变 Session Event 作为权威记录

不可变 Session Event 是 Session 的权威记录，Session State 与 Summary 是带来源版本的可重建投影。提交时先校验幂等键、fencing token 和预期版本，再原子追加事件、递增 Session 版本并应用 `state_delta`，提交后通过 Outbox 发布；Summary 按确定的事件版本范围异步生成，带 `source_version` 写回且不得以旧结果覆盖新结果，Memory 只消费已提交事件。Redis 型 Session 后端必须把事件归档到 SQL 或对象存储，不能成为唯一权威副本。
