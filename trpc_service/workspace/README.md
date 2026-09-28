# Workspace

该目录只定义 Agent 本地或容器沙箱工作区的能力，不承载租户业务数据，
也不存放开发检查脚本或基础设施配置。

- `contracts.py`：Workspace 执行器的抽象接口。
- `local.py`：当前 Local 实现，按 Tenant、Agent、Request 隔离目录。
- 每次 Worker 执行 Agent 前创建 `inputs/`、`outputs/`、`tmp/`，并把
  `WorkspaceHandle` 注入 Runner Context；重试由其他本地 Worker 接管时仍定位到同一目录。
- 本地 Workspace 不归属于某个 WorkNode。两个本地 Worker 共享 `data/workspaces`；
  进程间文件租约阻止并发执行和误清理，释放后其他节点可继续恢复；
  容器或远程沙箱实现后续继续实现相同接口。
- 当前仅开放受治理的有限目录列举和 UTF-8 文本读取，不开放任意写文件或执行命令。
- 本地请求目录默认保留 7 天，并在 Worker 获取工作区时分批清理过期目录；
  保留时间和清理间隔均由环境变量配置。
- Kubernetes 当前为每个 Pod 使用独立 `emptyDir`，跨 Pod 恢复只依赖共享 Session、任务与 Artifact，不承诺恢复临时 Workspace 文件；需要该能力时实现远程 Provider 或使用经过评审的 RWX 卷。
