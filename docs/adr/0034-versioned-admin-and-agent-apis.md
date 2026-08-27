# 分离版本化 Admin API 与 Agent 协议入口

Admin API 使用 `/api/v1` REST/OpenAPI，Web Console 仅调用该公开接口；Agent Gateway 提供 HTTP、SSE，并复用 tRPC-Agent-Python v1.1.19 的 AG-UI 与 A2A 能力，IM 协议只进入 Channel Gateway，不引入 GraphQL。写请求支持 Idempotency-Key，更新使用版本或 If-Match，长任务返回 operation ID，列表游标分页，执行返回 execution、session、release 与 trace 标识；稳定错误码、OpenAPI、SDK 示例、兼容测试和弃用周期均属于正式交付。
