# Admin API

Admin API 前缀为 `/admin/v1`，完整请求模型和响应模型可在 `/docs` 查看。

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/tenants` | 创建租户 |
| `GET` | `/tenants` | 分页查询租户 |
| `GET` | `/tenants/{tenant_id}` | 查询租户 |
| `PATCH` | `/tenants/{tenant_id}` | 更新、暂停或禁用租户 |
| `POST` | `/tenants/{tenant_id}/apps` | 创建应用及 v1 草稿 |
| `GET` | `/tenants/{tenant_id}/apps` | 查询租户应用 |
| `GET` | `/tenants/{tenant_id}/apps/{app_id}` | 查询应用状态 |
| `GET/PUT` | `/tenants/{tenant_id}/apps/{app_id}/draft` | 读取或原子替换草稿配置 |
| `POST` | `/tenants/{tenant_id}/apps/{app_id}/publish` | 发布草稿并创建下一草稿 |
| `POST` | `/tenants/{tenant_id}/apps/{app_id}/rollback` | 切换到历史已发布版本 |
| `GET` | `/tenants/{tenant_id}/apps/{app_id}/backends/effective` | 查询配置值与实际生效后端 |
| `GET` | `/tenants/{tenant_id}/apps/{app_id}/channel-bindings` | 查询 active IM 绑定及完整 Webhook URL |
| `PUT` | `/tenants/{tenant_id}/im-identities` | 新增或更新外部用户身份映射 |
| `GET` | `/tenants/{tenant_id}/im-identities` | 按通道和账号查询身份映射 |

更新请求必须携带当前 `expected_version` 或 `expected_lock_version`。发生并发修改时返回
`409 conflict`，客户端应重新读取资源后再决定是否重试。

后端选择、IM Secret Reference、身份映射模式和最小数据模型详见
`docs/tenant-backend-and-im-binding.md`。Admin API 只显示密钥引用，不解析或返回密钥明文。
