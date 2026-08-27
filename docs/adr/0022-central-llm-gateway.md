# 生产模型调用统一经过 LLM Gateway

生产 Agent Worker 通过 tRPC-Agent-Python Model 接口调用集群内 LLM Gateway，不直接持有供应商密钥。网关按租户模型配置档解析模型别名、注入密钥引用、执行限流、成本计量、首个可见流式输出前的安全重试、熔断和租户允许范围内的 fallback；默认只记录模型、Token、费用、延迟、错误和内容哈希，不记录 Prompt/Response 正文。高敏租户可绑定独立网关或私有 Endpoint，LLM Gateway 作为基础设施部署而不增加业务服务边界。
