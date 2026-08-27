# 使用 Kubernetes gVisor Sandbox 执行不可信代码

平台基于 tRPC-Agent-Python `BaseCodeExecutor` 新增 Kubernetes Sandbox Executor，每次不可信执行进入独立 Sandbox Pod，并默认使用 gVisor RuntimeClass；生产禁止 UnsafeLocalCodeExecutor、Docker Socket 和静默降级到普通容器。Sandbox 以非 root、只读根文件系统、零 capabilities、无 ServiceAccount Token、默认禁网和严格资源配额运行，输入输出经暂存与 Artifact Store 传递，镜像按摘要固定并扫描，结束后销毁；不具备隔离运行时的集群必须禁用不可信代码执行。
