# 本地存储数据

该目录只保存项目存储服务的运行数据，不保存源码：

- `postgresql/`：租户配置、Session、Inbox/Outbox、Memory、Audit 与 pgvector 数据。
- `redis/`：可选协调和通知数据；当前核心正确性不依赖 Redis。
- `seaweedfs/`：S3 兼容对象与文件数据。
- `workspaces/`：按 Tenant、Agent 和 Request 隔离的 Local Workspace 数据。
- `backups/`：由开发期备份脚本创建的数据库与对象存储校验备份。

核心运行目录由 `start.sh` 自动创建，内容不会提交到 Git。执行 `stop.sh --volumes` 会清空这些测试数据及监控组件的 Docker 数据卷；普通 `stop.sh` 会保留数据。Kubernetes 使用 PVC/`emptyDir`，不直接复用本目录；首次数据迁移由部署脚本的 `--import-compose-data` 显式完成。
