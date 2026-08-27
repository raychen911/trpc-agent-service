# 多租户 Agent 部署平台

面向一个企业内部的多个组织租户，统一创建、部署、接入、治理和运营 Agent 应用，并在企业控制的共享基础设施上保证租户隔离与生产可靠性。

## 使用约定

- 下列粗体名称是项目规范术语；需求、代码、API、事件、日志和运维文档应优先使用这些名称。
- Agent、Session、Memory、Knowledge、Tool、Artifact 等框架概念保留英文，避免翻译后产生多个近义名称。
- `_Avoid_` 中列出的名称不是同义词，不得用来代指对应术语。
- `tenant_id` 必须表示平台租户边界；外部通道、模型或存储中的租户标识必须使用带来源限定的独立字段名。

## 统一语言

**生产平台**:
能够真实部署和持续运营租户 Agent 的完整系统，其生产能力必须由可执行代码、自动化测试和部署配置支撑，而不能只由示例、伪代码或设计文档表达。
_Avoid_: 参考实现、演示项目、架构样例

**租户**:
在平台内拥有独立 Agent 应用、通道绑定、数据、权限、密钥、预算和审计边界的企业或组织。
_Avoid_: 客户、账号、namespace

**Tenant Group**:
为集团、事业群等组织视角聚合多个租户的管理对象，可以汇总运营信息或分发模板，但不天然拥有成员租户的数据访问权。
_Avoid_: 父租户、共享租户、数据域

**Agent 应用**:
由租户创建和管理、包含模型、指令、工具、知识及治理配置的可部署 Agent 定义。
_Avoid_: Agent 脚本、机器人配置

**Agent Draft**:
Agent 应用中允许持续编辑但不能直接承载生产流量的工作版本；只有校验并发布后才能形成不可变 Agent Release。
_Avoid_: Agent Release、在线生产配置、代码分支

**私有化部署**:
一套平台专属于一个企业并运行在该企业控制的基础设施中，企业内部的部门、业务线或子公司作为平台租户共享该平台。
_Avoid_: 公有 SaaS、跨企业共享实例

**控制面**:
负责租户、权限、配置、Release、Deployment、策略、审批和运营管理的系统能力，不参与已接收 Agent 执行的关键数据路径。
_Avoid_: Admin API 单个进程、数据面、控制面元数据

**数据面**:
负责接收用户输入、调度及执行 Agent、持久化运行状态并投递回复的系统能力；控制面短暂不可用时，已发布配置应能继续服务。
_Avoid_: 运行时数据、Worker 集合、控制面

**平台用户**:
通过企业身份系统登录管理面的人员，其租户归属和平台操作权限由平台角色控制。
_Avoid_: IM 用户、Agent 用户、后台账号

**IM 用户**:
通过外部消息通道与 Agent 应用交互的人员，其通道身份需要映射为租户范围内的主体。
_Avoid_: 平台用户、管理员账号

**控制面元数据**:
描述租户、Agent 应用、权限、通道绑定和配置版本的权威管理数据，由平台统一持有。
_Avoid_: 运行时数据、业务数据

**运行时数据**:
Agent 执行过程中产生或读取的 Session、Memory、Summary、Knowledge、Artifact 和审计数据，其存储位置可以按租户配置。
_Avoid_: 控制面元数据、平台配置

**存储配置档**:
租户可绑定的一组运行时数据后端及其隔离方式，描述各类运行时数据写入哪些共享或独占存储。
_Avoid_: 数据库配置、Storage Adapter、后端字符串

**Storage Adapter**:
在不改变平台领域语义的前提下，将 Session、Memory、Knowledge、Artifact、审计等存储契约映射到具体后端的进程内适配模块。
_Avoid_: 存储配置档、独立存储微服务、数据库驱动

**Agent 执行**:
平台接受一条用户输入或执行请求后，到产生成功、失败、取消或超时终态之间的一次运行实例；等待模型、工具或存储响应期间仍属于执行中。
_Avoid_: HTTP 请求、Session、Worker 进程

**执行总线**:
在 Gateway、Agent Worker 和 Job Worker 之间持久传递执行命令及领域事件的 Kafka 兼容消息基础设施。
_Avoid_: Redis 队列、进程内任务队列、Session Event

**Outbox**:
与业务状态在同一数据库事务内写入、随后可靠发布到执行总线或审计链路的待发记录。
_Avoid_: Kafka Topic、重试队列、业务事件本身

**Session**:
属于一个租户和一个 Agent 应用的连续交互上下文，聚合有序事件、当前状态与摘要；它不归属于任何特定 Worker。
_Avoid_: 用户、群聊、HTTP 会话、Worker 状态

**Session Event**:
Session 中已经提交且不可修改的事实记录，是重建 Session 当前状态和摘要的权威依据。
_Avoid_: 日志行、Kafka 消息、可变消息记录

**Session State**:
由已提交 Session Event 推导出的指定版本当前状态，可以从权威事件重新构建。
_Avoid_: Event、Memory、Worker 内存

**Summary**:
对一段确定版本范围内 Session Event 的压缩表达，用于控制模型上下文长度，不是原始对话的权威副本。
_Avoid_: Memory、完整历史、审计记录

**Memory**:
从已提交交互中提取、可跨 Session 复用的长期信息，归属于租户范围内的主体并保留来源；它不是单次 Session 的压缩历史。
_Avoid_: Summary、Session State、聊天记录

**通道绑定**:
租户范围内一个外部 IM 机器人或应用与一个 Agent 应用之间的有效关联，包含解析入站路由和发送回复所需的身份及策略。
_Avoid_: Channel 配置、机器人账号、Webhook 地址

**IM 主体**:
一个外部 IM 用户在特定租户和通道绑定中的平台身份；相同外部用户标识出现在其他绑定或租户中时不是同一主体。
_Avoid_: 平台用户、原始 external_user_id、全局用户

**主体关联**:
经验证后声明多个 IM 主体代表同一自然人的租户内关系，用于在明确授权下共享跨通道 Memory。
_Avoid_: 用户猜测、自动合并、跨租户身份

**入站消息**:
外部通道事件经验证、解密、规范化并解析到唯一通道绑定后形成的不可变用户输入；重复投递仍表示同一条入站消息。
_Avoid_: 原始 Webhook、Kafka 消息、Agent Event

**Channel Adapter**:
运行在 Channel Gateway 内、负责特定 IM 协议的验签、解密、规范化、能力协商与回复转换的插件。
_Avoid_: Channel Gateway、通道绑定、独立通道微服务

**工具**:
向 Agent 暴露的版本化能力定义，声明输入输出、权限范围、副作用等级、数据分级、费用和执行约束。
_Avoid_: 工具调用、任意 Python 函数、MCP Server

**工具调用**:
一次由 Agent 提议并由平台治理的确定工具操作，绑定工具版本、规范化参数、调用主体、幂等键和最终结果状态。
_Avoid_: 函数调用文本、Tool Event、任意重试

**审批**:
授权或拒绝某个不可变工具调用意图的人工决定，只对指定工具、参数哈希、主体和有效期生效。
_Avoid_: 通用许可、聊天中的“同意”、永久白名单

**Artifact**:
Agent 执行或工具调用产生、接收或引用的具名文件对象，具有租户归属、内容摘要、来源和生命周期。
_Avoid_: Workspace 文件、日志附件、数据库大字段

**密钥引用**:
租户范围内指向秘密值的不可读句柄，可以出现在平台配置中，但不能被解析为或替代秘密值本身。
_Avoid_: 密码字段、API Key 正文、环境变量值

**环境**:
用于隔离发布、凭据、信任域和数据的 Development、Staging 或 Production 运行边界。
_Avoid_: Kubernetes namespace、Deployment、租户

**Agent Release**:
Agent 应用在某一时刻发布的不可变配置快照，具有唯一版本、内容摘要和变更来源。
_Avoid_: Draft、在线配置、代码提交

**Deployment**:
声明某个环境当前向哪些 Session 提供哪些 Agent Release 的运行分配关系。
_Avoid_: Kubernetes Deployment、Agent Release、发布脚本

**模型配置档**:
租户可绑定的一组模型别名、调用边界、预算、凭据引用和故障降级顺序；Agent 应用只依赖其中公开的模型别名。
_Avoid_: Provider API Key、模型字符串、SDK 客户端配置

**LLM Gateway**:
代表租户解析模型配置档、注入模型凭据并执行路由、限流、费用计量和安全重试的统一模型出口。
_Avoid_: Agent Gateway、模型配置档、模型供应商 Endpoint

**预算**:
租户、Agent 应用或单次 Agent 执行在指定周期内允许预留和消耗的费用及资源上限。
_Avoid_: 成本报表、余额缓存、告警阈值

**成本账本**:
记录预算预留、结算、释放、拒绝与管理员调整的不可变明细，是费用归因和预算审计的权威来源。
_Avoid_: Token 指标、月度汇总、供应商账单

**审计事件**:
描述谁在何时以什么权限对哪个平台对象作出何种决定或操作的不可变证据记录，默认只保存必要元数据和内容摘要。
_Avoid_: 运行日志、Trace Span、聊天记录

**删除请求**:
要求在规定期限内删除指定租户、主体、Session、Memory、Artifact 或 Knowledge 及其派生内容，并生成跨后端执行与对账证明的受审计操作。
_Avoid_: 数据库 DELETE、缓存失效、对象过期

**治理策略**:
租户发布的版本化规则集合，用于对模型、工具、Memory、Knowledge、预算、数据披露和人工审批作出可解释决定。
_Avoid_: Prompt 指令、代码条件、Tool 白名单文件

**Policy Bundle**:
由治理策略编译、签名和版本化后供 OPA 本地执行的不可变策略制品。
_Avoid_: 治理策略草稿、OPA Sidecar、Filter

**Filter**:
嵌入 tRPC-Agent 执行管线的治理扩展点，负责向 Policy Bundle 提交规范上下文并落实允许、拒绝、脱敏或需要审批的决定。
_Avoid_: 唯一权限边界、Prompt 规则、HTTP 中间件

**数据分级**:
对平台处理的信息施加的租户治理等级，等级随内容聚合取最高值，并约束数据可以进入哪些模型、工具、日志和存储。
_Avoid_: 日志级别、数据库权限、DLP 检测结果

**生产变更**:
会改变生产环境流量、权限、策略、凭据、数据位置、保留义务或敏感数据披露方式的受控操作。
_Avoid_: Draft 编辑、测试运行、普通查询

**降级执行**:
当非关键依赖不可用时，依据 Agent Release 和治理策略明确省略部分增强能力后继续的 Agent 执行，并向用户和审计披露降级事实。
_Avoid_: 静默失败、错误重试、完整执行

**回复投递**:
将一个 Agent 执行结果按通道能力转换、发送并确认到达外部会话的一次受追踪交付，包含占位、增量更新和最终状态。
_Avoid_: Agent Event、模型 Token、HTTP 响应

**审批请求**:
针对一个确定工具调用创建的限时授权任务，绑定请求者、风险、审批规则和不可变操作预览，并可由 Agent 执行跨进程等待。
_Avoid_: 审批决定、聊天确认、通用授权

**Knowledge Base**:
租户管理的逻辑知识集合，具有文档访问边界，但不代表某次具体索引构建结果。
_Avoid_: 向量索引、上传目录、Retriever

**Knowledge Revision**:
由确定知识源、处理策略、Embedding 模型和文档权限生成的不可变内容及索引快照。
_Avoid_: Knowledge Base、在线索引、文档版本号

**Knowledge Deployment**:
声明某个环境当前使用哪个 Knowledge Revision 的受控分配关系。
_Avoid_: Agent Deployment、索引任务、Knowledge Revision

**Eval Suite**:
用于判断一个 Agent Release 是否达到环境准入标准的版本化评测契约，包含评测用例、数据集引用、评分器、硬性安全断言和通过阈值。
_Avoid_: 单次测试、线上监控、无版本提示词集合

**Eval Run**:
针对一个确定 Agent Release 及其模型、Knowledge、工具、策略和 SDK 版本执行 Eval Suite 后形成的不可变结果与证据集合。
_Avoid_: CI Job、人工体验记录、聚合质量报表

**工作负载身份**:
由生产信任域向一个确定 Kubernetes ServiceAccount 所代表的平台服务签发并自动轮换的短期密码学身份，用于服务间双向认证与授权。
_Avoid_: Pod IP、共享 API Key、用户身份、可伪造服务名请求头

**Admin API**:
向 Web Console 和自动化客户端提供版本化租户及平台管理能力的控制面 API。
_Avoid_: Agent Gateway、内部数据库接口、Web Console

**Web Console**:
通过 Admin API 管理、审计和运营平台的浏览器控制台，不直接访问数据库或数据面内部接口。
_Avoid_: Admin API、Agent 对话页面、静态演示页

**Agent Gateway**:
接收 HTTP、SSE、AG-UI 与 A2A 请求，完成认证、租户及 Release 解析并把 Agent 执行提交到执行总线的数据面入口。
_Avoid_: Channel Gateway、Agent Worker、LLM Gateway

**Channel Gateway**:
承载 Channel Adapter、解析通道绑定并持久接收入站消息及回复投递状态的数据面 IM 入口。
_Avoid_: Channel Adapter、Agent Gateway、IM 平台

**Agent Worker**:
从执行总线取得 Agent 执行，加载固定 Agent Release，在 Session 并发控制和治理 Filter 下驱动 tRPC-Agent Runner 的无状态执行单元。
_Avoid_: Job Worker、Agent 节点、Session 所有者

**Job Worker**:
从执行总线处理 Memory、Summary、Knowledge、迁移、删除、审计归档和补偿等异步任务的无状态执行单元。
_Avoid_: Agent Worker、定时脚本、Kubernetes Job
