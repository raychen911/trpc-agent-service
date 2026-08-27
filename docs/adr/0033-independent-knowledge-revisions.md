# Knowledge 使用独立不可变 Revision 发布

Knowledge Base 表示租户逻辑知识集合，Knowledge Revision 固定源摘要、解析切分、Embedding、索引参数和文档 ACL，Knowledge Deployment 决定环境当前 Revision；Agent Release 只声明可访问的 Knowledge Base，每次执行记录实际 Revision。新 Revision 在独立索引中异步扫描、构建和验证后原子切换，旧版保留观察期，Embedding 变化不得混用向量空间。检索在存储层强制租户、Base、Revision 和文档 ACL 过滤，知识更新无需重新发布 Agent Release，但生产切换仍受审批和灰度治理。
