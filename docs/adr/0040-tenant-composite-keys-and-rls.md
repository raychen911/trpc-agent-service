# 使用 UUIDv7、租户复合外键与 PostgreSQL RLS

平台内部实体由应用侧生成 UUIDv7，所有租户表以 `(tenant_id, id)` 建立唯一性，跨表关系使用包含 tenant ID 的复合外键，从数据库结构上阻止跨租户引用；业务连接角色既非 Owner、超级用户也无 BYPASSRLS，并在事务级 Tenant Context 下强制 RLS。平台全局表与租户表分 Schema，Repository 显式接收租户上下文，高频事件按时间分区并经压测决定租户哈希分片，CI 持续验证错误引用同时被外键和 RLS 阻断。
