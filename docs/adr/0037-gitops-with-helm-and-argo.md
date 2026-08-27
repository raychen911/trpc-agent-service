# 使用 Helm 与 Argo 实施 GitOps 发布

Kubernetes 期望状态由 Helm 与 Argo CD 管理，Argo Rollouts 负责 Gateway、Worker 和 Job Worker 金丝雀发布；CI 只执行测试、构建、扫描、SBOM 与签名，不直接持有生产集群管理员权限。镜像按 digest 固定，环境 values 只含密钥引用，生产推荐独立集群。数据库采用 Expand-Migrate-Contract，并以受控 Alembic PreSync Job 阻断失败发布；租户和 Agent 业务配置不进入 Git，紧急回滚只恢复服务与 Helm 状态，不假装撤销不可逆数据变更。
