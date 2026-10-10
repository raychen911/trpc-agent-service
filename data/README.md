# Local data

该目录只用于本地开发的挂载点或无敏感内容的固定测试夹具。生产数据由 PostgreSQL、Redis 和
S3/MinIO 管理；真实消息、token、密钥、数据库文件和在线测试下载不得提交。可重复生成的报告写入
`runs/multitenant/`。
