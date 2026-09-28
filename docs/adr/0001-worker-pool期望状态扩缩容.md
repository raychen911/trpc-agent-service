# Worker Pool 使用期望状态扩缩容

状态：已采纳并实现。

## 决策

系统管理员只在控制面修改 `agent-worker` 的期望节点数，不由 Web 请求直接创建或终止进程。独立协调器负责把实际容量收敛到目标数量：本地模式管理独立 Worker 子进程，Kubernetes 模式只修改 `agent-worker` Deployment 的 scale 子资源。

缩容统一先排空：目标外 Worker 停止领取新任务，完成或释放已领取任务后退出。管理请求携带 generation，防止两个过期页面互相覆盖。任务租约和 fencing token 仍是最终正确性边界，协调器不能绕过数据库状态。

## 结果

- 管理 API 不需要宿主机进程权限或通用 Kubernetes 管理权限。
- 本地与 Kubernetes 使用同一种控制面语义。
- 手工 `kubectl scale` 或 HPA 不能与当前期望状态协调器长期同时控制副本数；接入 HPA 时必须明确移交控制权。
- 管理员不能强杀指定节点；故障节点由心跳过期、租约接管和运维流程处理。
