# 使用 Vault 兼容密钥管理服务

生产环境以 OpenBao/Vault HA 或兼容服务管理秘密，平台数据库和配置只保存租户范围的 `secret_ref`。Pod 通过 Kubernetes Auth 获取短期凭证，静态秘密使用 KV v2，租户数据使用 Transit 信封加密；路径和策略按租户及用途隔离，支持版本、轮换、吊销和双版本过渡。Admin API 永不返回秘密正文，日志、Trace、异常、Kafka 和 Session Event 统一脱敏；Kubernetes Secret 只承载最小启动引用，Secret Provider 接口允许企业替换为现有 KMS。
