# 使用版本化 CloudEvents JSON 契约

Kafka 领域事件采用 CloudEvents 1.0 风格信封、版本化 JSON Schema 与兼容性 Schema Registry，不直接序列化 tRPC-Agent-Python 内部对象。信封携带事件、租户、因果、关联、Trace、Schema 和数据分级信息；生产 Schema 强制向后兼容，新增字段可选，不允许原位删除、改名或改类型，破坏性语义使用新事件类型或主版本。消费者忽略未知字段但拒绝未知主版本，消息只携带必要快照和权威记录 ID，所有 Producer/Consumer 通过历史契约测试。
