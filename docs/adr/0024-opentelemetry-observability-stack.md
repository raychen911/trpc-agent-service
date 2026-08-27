# 使用 OpenTelemetry 统一可观测性

平台默认交付 OpenTelemetry Collector、Prometheus、Tempo、Loki 与 Grafana，分别承载遥测管线、指标、Trace、运行日志和展示；审计数据使用独立权威存储。IM 入口生成 trace ID，经内部 HTTP、Kafka Header 和异步 Job 传播，异步 Memory、Summary 与回复投递用 Span Link 关联原执行。错误、审批、高风险工具和预算拒绝保留完整 Trace，普通成功流量尾部采样；Attribute 使用允许列表，正文、秘密和原始 IM 身份默认不采集，内部 Baggage 在调用外部模型前剥离，并交付 SLO、成本、积压、存储与 IM 投递的 Dashboard 和告警。
