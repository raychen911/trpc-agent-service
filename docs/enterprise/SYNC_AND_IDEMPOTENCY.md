# 数据同步与幂等策略

## 1. 一条消息的状态边界

系统接受 IM 平台的 at-least-once 投递，不声称所有外部副作用具有天然 exactly-once。平台通过
“唯一键 + 状态机 + 结果缓存 + 业务幂等键”实现效果上的一次执行。

1. Gateway 验签后生成 `dedup_key = tenant_id:channel:message_id`；平台没有提供 `message_id` 时，
   以原始 callback body 的 SHA-256 作为稳定 fallback，避免空 ID 造成跨消息误去重；
2. Redis `SET NX EX` 成功才允许入队，失败表示重复回调并立即 ACK；
3. 入队失败释放 reservation，让 IM 重试可以再次提交；
4. Redis Streams consumer group 提供 at-least-once 投递，pending 超时后由其他 Worker reclaim；
5. Worker 先查 `agent:task-result:{dedup_key}`。已有结果时跳过 Agent/Tool，只重试 IM 回复；
6. 首次执行完成后先保存结果，再发送回复；成功后 XACK，连续失败超过阈值进入 DLQ；
7. 生产长期去重可把同一键写入 SQL `inbound_receipt`，TTL 必须大于平台重试窗口和最大任务时长。

危险工具还要把相同 dedup key 或独立 operation id 传给下游业务。网络超时意味着“结果未知”时，
必须先查询下游状态，不能直接重放创建、扣款、取消等副作用。

## 2. Session、Event、Summary 与 Memory

同一 Session 的推荐提交顺序：

```text
获取带 fencing token 的 lease lock
  → append user/tool/assistant event
  → CAS(session.version) 更新 state
  → 提交/确认
  → 释放锁
  → 异步 summary(source_version)
  → 异步 memory(version)
```

Event 是恢复依据，必须先于派生 state 提交。SQL 使用事务和唯一 sequence；Redis 使用 Lua/事务或
单写者锁。Summary 仅在 `source_version >= current_source_version` 时覆盖。Memory 写入使用稳定
memory id/version，跨节点读主库；如使用本地缓存，通过 Pub/Sub 按 tenant + entity id 失效。

## 3. 配置同步

MySQL 是租户配置事实源。当前快照、不可变版本和 `config_outbox` 在一个事务提交；随后 publisher
刷新 Redis L2 并发布版本通知。节点收到通知后清除 L1。Redis 暂时不可用时 MySQL 事务不回滚，
pending outbox 恢复后重放；节点漏消息也会在缓存 TTL 到期后回源。更新使用 config version CAS，
防止两个 Admin 节点互相覆盖。

## 4. Knowledge 与对象同步

知识导入采用 Saga，而不是跨后端分布式事务：

1. 原文写对象存储，计算并校验 SHA-256；
2. SQL 事务写 document/chunk，状态为 `indexing`，同时写 `storage_outbox`；
3. 索引 Worker 用稳定 vector id 向 Qdrant/Milvus/pgvector upsert；
4. 校验 count、embedding model/version 和固定查询集后，将文档标记为 `ready`；
5. 检索只过滤 active/ready version；旧向量在回滚窗口后异步删除。

对象删除先把 SQL metadata 标记为 `deleting`，再删除 payload，成功后标记 `deleted`。PUT 成功但
SQL 失败产生的 orphan 由定时 GC 按 checksum/key 与 SQL 引用集合比对，并等待安全窗口后删除。

## 5. 后端迁移

- Redis→SQL Session/Memory：全量复制、watermark、双写、增量追平、checksum/shadow read、
  单租户切读、保留回滚窗口；
- 向量库迁移：固定 embedding 模型和归一化逻辑，双写新旧 collection，对固定查询集比较
  Recall@K/NDCG，按租户切 alias；
- 对象存储迁移：版本化 key 批量复制，逐对象比较 size/checksum，metadata 双写；切读后保留源对象，
  直到生命周期规则确认回滚窗口结束。

迁移任务本身也必须幂等：checkpoint 包含 tenant、entity kind、cursor、source version 和 checksum；
重复执行 upsert 不增加版本，只有全部验证成功才能改变租户的读取路由。
