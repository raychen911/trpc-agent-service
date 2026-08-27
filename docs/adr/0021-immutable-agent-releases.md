# 使用不可变 Agent Release 与环境 Deployment

Agent 应用先在可变 Draft 中编辑，经 Schema、密钥引用、权限、连通性和策略验证后生成包含完整快照、哈希与来源的不可变 Agent Release，再由 Development、Staging 或 Production Deployment 分配流量。灰度按规范 Session ID 确定性分配，同一执行始终使用启动时 Release，扩大比例受错误率、延迟、成本和治理指标控制；回滚只移动 Deployment 指针，Worker 按 Release ID 缓存并仅让旧版本完成已开始的执行，数据迁移独立于配置回滚。
