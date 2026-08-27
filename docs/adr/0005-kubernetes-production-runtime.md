# 使用 Kubernetes 作为生产部署基座

Kubernetes 是平台唯一正式支持的生产部署基座，生产交付必须包含 Helm Chart、健康探针、资源配额、HPA、PDB、拓扑分布、NetworkPolicy、数据库迁移 Job、灰度发布和回滚配置；Docker Compose 仅服务于本地开发、集成测试和最小演示。该选择使节点扩缩容、调度隔离、滚动升级和故障恢复成为可验证的系统能力，而不是仅存在于设计文档中的假设。
