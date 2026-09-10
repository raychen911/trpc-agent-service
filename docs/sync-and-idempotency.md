# 数据同步与幂等策略

## 1. 不同数据的一致性目标

| 数据 | 一致性要求 | 策略 |
|------|-----------|------|
| Session event 流 | 会话内有序、不丢 | append 语义，seq 单调递增，唯一索引兜底 |
| Session state | 多节点并发写不覆盖 | state_delta 键值合并（可交换，并发实测验证）；同键冲突由前端串行化兜底 |
| Summary | 允许滞后 | 异步后置生成，最终一致 |
| Memory | 写后读可见 | 写路径同步提交；读前 flush 本节点写队列 |
| Audit Log | 不丢即可 | JSONL 文件兜底 + SQL 主存储 |
| IM 消息 | 恰好处理一次 | idempotency key 去重 |

## 2. 多节点并发写同一 session

- **事件追加**：`append_event` 天然 append 语义，`(session_id, seq)` 唯一索引保证不重不丢；seq 由会话内单调计数分配。
- **state 更新**：`update_session` 携带 `expected_version`（乐观锁）。冲突时重读最新 state，按 event 的 `actions.state_delta` 合并后重试（state delta 为键值合并，天然可交换）。
- **取舍（含实测结论）**：并发实测（tests/test_concurrent_session.py，InMemory 后端、5 协程×10 事件）验证：并发 append 50 事件不丢不重；并发 update 不同 state 键合并后全部存在（state_delta 键值合并可交换）。极端场景（同 session 双端同时发）允许短暂交错，最终一致；强一致需求场景由前端串行化（同一 session 同时只允许一个 in-flight 请求）。

## 3. event / state / summary 的更新顺序

固定顺序：**append event → update state（携带 event.state_delta）→ 生成 summary（异步）**。

1. 用户消息与模型/工具事件先落 event 流（会话历史的唯一事实源）；
2. state 变更随事件 delta 合并进 session.state；
3. summary 由 Runner 的 post-turn 阶段异步生成（`defer_post_turn_processing`），滞后可接受，节点崩溃最多丢一次摘要，不丢历史。

## 4. Memory 写后读的跨节点可见性

- **Redis 后端**：写即时可见（单 key 写后读强一致）；`store_session` 同步提交后再返回响应。
- **SQL 后端**：事务提交后可见；同一节点请求内先写后读无窗口问题。
- **跨节点读**：Worker 不缓存 Memory，检索直接打后端（`search_memory` key=`{app_name}/{user_id}`），避免本地副本造成的读旧。

## 5. Redis → SQL 迁移方案

四步走，可灰度可回滚：

1. **停写**：配置中心把目标租户 `session_backend` 置为维护态（请求排队）。
2. **全量导出**：SCAN Redis（禁用 KEYS），按 key 前缀 `session:` / `memory:` 分批导出。
3. **转换导入**：按 `docs/data-model.md` schema 写入 SQL 表（session/message/memory/summary），保留原时间戳与 seq；行数对账（Redis 键数 vs SQL 行数）。
4. **切读验证**：切 `session_backend=sql`，抽样比对会话历史；观察 24h 后清理 Redis 旧键。

回滚：SQL 导入期间 Redis 原数据未删，配置切回即回滚。

## 6. IM 消息重复投递的幂等

- **幂等键**：`{channel}:{external_msg_id}`（飞书 message_id / 企微 MsgId 全局唯一）。
- **第一层（多节点共享）**：`Deduper` Redis `SET dedupe:{key} 1 NX EX 300` 原子占位，多节点共享；未配置 Redis 或运行期故障时降级进程内内存 TTL 去重（拦截事件重投窗口内的重复回调）。
- **第二层（兜底）**：SQL `idempotency` 表唯一索引（`feishu:{message_id}` / `wecom:{MsgId}` / `wecom_smartbot:{msgid}`），插入冲突视为重复；进程重启丢失去重缓存后仍能拦截。
- **语义**：重复消息直接返回 ACK `success`，不触发 Agent，不重复扣预算。

## 7. 各后端一致性取舍对比

| 后端 | 一致性 | 读写延迟 | 成本 | 运维复杂度 | 适用 |
|------|--------|---------|------|-----------|------|
| InMemory | 进程内强一致 | μs | 零 | 零 | 开发/单机 demo |
| Redis | 键级强一致，持久化可配 | ~1ms | 中 | 中 | 热会话、高 QPS、多节点 |
| SQL | 事务强一致（ACID） | ~5-10ms | 中高 | 中 | 合规审计、长期保存、复杂查询 |
| 向量库 | 最终一致 | ~10-50ms | 高 | 高 | Memory 语义检索（预留 embedding 列） |
| 对象存储 | 最终一致 | ~50ms+ | 低 | 低 | Artifact 大文件（预留） |
