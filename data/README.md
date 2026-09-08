# 本地运行数据目录

该目录用于保存本地 SQLite、Artifact、日志和运行时临时数据。除本说明文件外，目录内容均由
`.gitignore` 排除，不属于项目交付物。

提交代码前不要把以下文件加入 Git 或压缩包：

- `trpc_service.db`、`*.db-wal`、`*.db-shm`；
- `service*.log`；
- `artifacts/` 中的用户文件；
- 数据库备份或包含真实会话的导出文件。

需要演示时，由应用和 Alembic 在本地重新创建数据库结构。
