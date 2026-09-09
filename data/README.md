# 运行数据目录

本目录是服务的本地运行数据入口，不保存业务源码。首次启动前只保留本说明文件，服务运行后会按需生成：

- `trpc-service.pid`：`start.sh` 启动的进程号；
- `trpc-service.log`：本地服务日志；
- `artifacts/`：本地 Artifact Provider 保存的测试附件；
- 其他临时状态文件。

这些运行数据可能包含用户输入或附件，因此均由 `.gitignore` 排除，不提交到仓库。生产环境使用 Redis、PostgreSQL 和对象存储，本目录不承担多节点共享存储。
