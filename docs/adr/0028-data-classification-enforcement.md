# 使用四级数据分级强制出站治理

平台采用 PUBLIC、INTERNAL、CONFIDENTIAL 与 RESTRICTED 四级数据分级，通道绑定、Knowledge、工具输出和 Artifact 声明基础等级，DLP 可以自动提高但不能降低，聚合内容取最高等级。模型配置档与工具声明可接收等级和区域，Prompt 组装、工具调用、日志及 Trace 前由 OPA/Filter 强制检查；CONFIDENTIAL 需脱敏或使用私有模型，RESTRICTED 禁止进入外部模型，检测到秘密直接阻断并审计。管理员降级需要理由，高敏降级需要双人审批。
