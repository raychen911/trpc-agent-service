# 第五阶段运行环境变量示例

本文档只列出变量名称和占位符。请从受控密钥位置向当前进程注入真实值；不要把
真实 Secret、token、ticket、access_key 或完整连接 URL 写入源码、文档、日志或 Git。

## 飞书 Adapter

~~~powershell
$env:LARK_APP_ID = "<由安全配置提供>"
$env:LARK_APP_SECRET = "<由安全配置提供>"
$env:LARK_TENANT_KEY = "<飞书测试企业身份键>"
~~~

## 企业微信 Adapter

~~~powershell
$env:WECOM_BOT_ID = "<由安全配置提供>"
$env:WECOM_BOT_SECRET = "<由安全配置提供>"
$env:WECOM_CORP_ID = "<企业微信企业身份键>"
~~~

## 第三阶段共享后端

~~~powershell
$env:TRPC_DEMO_REDIS_PASSWORD = "<仅本机 Compose 使用>"
$env:TRPC_DEMO_POSTGRES_PASSWORD = "<仅本机 Compose 使用>"
$env:TRPC_SHARED_REDIS_URL = "<Redis 地址由运行环境提供>"
$env:TRPC_SHARED_DATABASE_URL = "<PostgreSQL DSN 由运行环境提供>"
~~~

## 使用后清理当前终端

~~~powershell
Remove-Item Env:LARK_APP_ID -ErrorAction SilentlyContinue
Remove-Item Env:LARK_APP_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:LARK_TENANT_KEY -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_BOT_ID -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_BOT_SECRET -ErrorAction SilentlyContinue
Remove-Item Env:WECOM_CORP_ID -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_SHARED_REDIS_URL -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_SHARED_DATABASE_URL -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_DEMO_REDIS_PASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:TRPC_DEMO_POSTGRES_PASSWORD -ErrorAction SilentlyContinue
~~~

Channel Binding 只能保存变量名或 Secret Provider 引用，不能保存解析后的值。
