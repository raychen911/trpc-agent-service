# Official Practical Project Specification

## Project Context

tRPC-Agent is Tencent's open-source framework for production-grade agent
applications, with Python and Go implementations. It covers model invocation,
tools, multi-agent orchestration, GraphAgent, Session/Memory, Knowledge/RAG, A2A,
AG-UI, MCP, evaluation, optimization, and OpenTelemetry observability. The Python
implementation emphasizes rapid integration with the AI ecosystem; the Go
implementation emphasizes high concurrency, strong typing, and long-running
production runtimes.

Project mentors: Shumei Ou, Lei Chen, and Zenghao Liu.

## Practical Project: Multi-Tenant and Multi-Node Deployment

1. Design a tenant model containing at least `tenant_id`, application
   configuration, model configuration, tool permissions, IM channel configuration,
   data-backend configuration, and an audit policy.
2. Design the deployment topology and explain how Agent Gateway, Agent Worker,
   Channel Adapter, Storage Adapter, Admin API, and Telemetry Collector cooperate.
3. Support horizontal scaling across multiple nodes and explain how a user message
   is routed to the correct tenant and session.
4. State whether sticky sessions are required. If not, explain how shared Session
   and Memory backends make Workers stateless.
5. Design tenant isolation for configuration, data, tool permissions, log
   redaction, and secret management.

## Data Synchronization and Multiple Backends

1. Allow different tenants to choose different backends, including InMemory,
   Redis, SQL, a vector database, object storage, or an external Memory service.
2. Define a unified data-access abstraction and explain storage for Session,
   Memory, Summary, Artifact, Knowledge, and Audit Log.
3. Define synchronization behavior covering:
   - consistency when multiple nodes write the same session concurrently;
   - the update order for Session event, state, and summary;
   - cross-node visibility after a Memory write;
   - Redis-to-SQL and local-vector-to-remote-vector migration;
   - idempotency for duplicate IM deliveries.
4. Explain consistency tradeoffs across backends, including strong versus eventual
   consistency, read/write latency, cost, and operational complexity.
5. Provide a minimum data model or schema containing tenant, agent application,
   session, message/event, memory, summary, channel binding, and audit log.

## IM Integration

1. Implement an IM Channel Adapter supporting at least two channel classes among
   WeCom, WeChat Customer Service, WeChat Official Account, Telegram, or another IM
   platform.
2. Explain how external IM messages become tRPC-Agent user input and how Agent
   Events become IM replies, streaming messages, or card messages.
3. Design IM-account-to-tenant binding, including webhook URL, token, secret,
   callback-signature verification, message deduplication, and user identity
   mapping.
4. Define `session_id` generation for direct and group conversations and isolate
   users across groups and tenants.
5. Handle platform constraints such as message length, rate limits, asynchronous
   replies, image/file messages, recall/withdrawal, and delivery retries.

## Governance, Monitoring, and Security

1. Use Filters for tenant governance, including tool allow-lists, sensitive-data
   redaction, budgets, secondary confirmation for dangerous tools, and IM-user
   authorization.
2. Expose metrics for request volume, model latency, tool latency, IM delivery
   success, errors, token consumption, per-tenant cost, and Session-backend latency.
3. Integrate OpenTelemetry or equivalent tracing so one trace connects IM callback,
   Runner execution, Tool calls, Session/Memory reads and writes, and IM reply.
4. Audit records must contain at least `tenant_id`, `channel`, `user_id`,
   `session_id`, `agent_name`, `tool_name`, `decision`, `latency`, `error_type`,
   `cost`, and `trace_id`.
5. IM tokens, model API keys, and database passwords must never appear in plaintext
   in logs, traces, or error reports.

## Recovery and Operations

1. Define degradation and recovery for node failure, IM retry, temporary database
   unavailability, model timeout, and tool failure.
2. Explain gray/canary rollout and tenant-level configuration rollback.
3. Provide capacity estimation for concurrent sessions per node, average token
   consumption, Redis/SQL QPS, and peak IM callback traffic.
4. Provide both a minimum runnable deployment and a recommended production
   deployment using Docker Compose, Kubernetes, or an equivalent system.

## Submission Interpretation from Mentor QA

- The major directions and requirement names must remain easy to audit, while
  implementation details may be split into reusable modules.
- Governance, monitoring, and security may be implemented across IM and operations
  modules when the responsibility boundaries remain clear.
- At least two real IM adapter classes must exist in code. A browser UI is useful
  for local validation but does not replace the two-IM requirement.
- When real WeCom credentials are unavailable, the local UI and deterministic
  fixtures may validate the flow; real credentials can be supplied later by the
  mentor environment.
- The final deliverable must be a complete runnable service, not pseudocode, and
  should include design documentation explaining the engineering decisions.
