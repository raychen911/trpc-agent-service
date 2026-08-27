# 按扩缩容与故障域拆分部署单元

平台部署为 Admin API、Web Console、Agent Gateway、Channel Gateway、Agent Worker 和 Job Worker 六类单元：管理面、标准协议入口、IM 入口、Agent 执行与异步补偿可以独立扩缩容和隔离故障。Channel Adapter 留在 Channel Gateway 内作为插件，Storage Adapter 作为 Worker 共享库，Filter 留在 Agent 执行管线中；OpenTelemetry Collector 和各种数据库属于基础设施，不为目录结构或抽象接口额外制造网络服务。
