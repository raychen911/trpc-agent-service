# 运行数据目录

此目录用于本地开发、测试和 Docker Compose 运行时数据。运行过程中可能生成
SQLite 数据库、对象文件、Prometheus 数据、补偿任务和死信记录。

除本说明文件外，运行产物均被 `.gitignore` 忽略，不应提交到 GitHub。生产环境
应使用 PostgreSQL、Redis、对象存储和外部 Prometheus 等持久化服务，不要把本地
`data/` 目录当作生产数据后端。
