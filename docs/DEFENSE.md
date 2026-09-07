# Defense Guide

## Ninety-second opening

This is not a multi-tenant diagram wrapped around a single-node chatbot. It is a
runnable tRPC-Agent platform whose correctness boundary starts at an authenticated
IM binding and ends at a transactional reply outbox. A public Channel Adapter
verifies Telegram or encrypted WeCom, an authenticated Gateway derives an opaque
tenant/session route and writes Redis Streams, and any stateless Worker can execute
the exact tenant configuration revision. A renewable per-session lease orders
turns, an optimistic revision prevents lost updates, and duplicate callbacks reuse
the completed receipt instead of calling the model twice. The same tenant chooses
Session, Memory, Summary, Artifact, Knowledge, and Audit backends independently.
Every model/tool/storage/reply operation stays in one redacted OpenTelemetry trace.

The important claim is not "exactly-once delivery," which external IM systems do
not provide. The precise claim is: one committed normalized turn/reply intent per
dedupe receipt inside the SQL boundary, with stable recovery IDs and idempotent
built-in effects. Model calls, arbitrary external tools, and IM transport remain
at-least-once/ambiguous boundaries and are never mislabeled globally exactly-once.

## Suggested ten-minute flow

1. Show the browser demo and repeat the same `message_id`; point out the cached
   response and unchanged event count.
2. Show the topology and explain why tenant comes from channel binding, not input.
3. Show direct/group HMAC session rules and why no sticky session is needed.
4. Walk one turn's happens-before chain from receipt to outbox.
5. Show tenant YAML selecting independent data backends and tool policy.
6. Show the dangerous-tool confirmation test: wrong args or replay fails.
7. Follow one trace from callback through tRPC model/tool/storage to IM reply, then
   show that prompt/state attributes are removed before export.
8. Show config revision activation/rollback and the migration verification report.
9. End with quality evidence and the honest limitations.

## Differentiators

- **Trustworthy routing:** `tenant_id` cannot be supplied by the sender. Binding,
  external account, signature, and derived route are checked twice across the
  Channel-to-Gateway trust boundary.
- **Correct concurrency:** a session lease orders expensive turns, while the event
  revision remains a fencing safety net. Redis and SQL leases renew with owner-
  checked updates; a lease alone would not be enough.
- **Correct idempotency boundary:** receipt completion and outbox insertion are one
  transaction. This closes the classic "model finished, process crashed before
  reply" hole. Native tRPC finals carry the external request ID, closing the
  smaller crash window before normalized persistence without rerunning the model.
- **Checkpointed multi-part replies:** text chunks, cards, and media are ordered
  one-effect segments. A failed later segment resumes at its checkpoint instead of
  repeating every earlier visible message.
- **Config-consistent queues with revocation override:** normal jobs carry an
  immutable revision, while current tenant suspension or binding disable remains
  an authoritative kill switch.
- **Lossless bounded admission:** one Redis-side transaction enforces global and
  per-tenant pending limits before acknowledgement; no live stream entry is
  discarded to make room, and tenant-partitioned dead letters have audited replay.
- **Real backend plurality:** resources have separate ports; Summary and Artifact
  do not rely on a foreign key in the Session database.
- **Filter governance:** tools are not merely hidden from the prompt. They are never
  instantiated outside the allow-list and are checked again at execution.
- **Exact dangerous confirmation:** token binds tenant, user, session, tool, args,
  expiry, and a completed-terminal one-use receipt—not a vague "yes" message.
- **Hard shared budgets:** a row-locked tenant-period ledger counts settled usage
  plus live worst-case reservations. The bound is context-window × permitted LLM
  calls plus every maximum output, and tRPC enforces matching LLM/tool limits.
  Provider retries are included as `retry_count + 1` attempts per logical call.
  Completion reconciles actual tokens/cost in the receipt transaction; cancellation
  and expiry release capacity. Persisted crash output is recovered before a new
  budget decision and can never be replaced by a later denial. Error/timeout events
  retain observed billed usage, and native recovery sums the whole invocation.
- **Repairable projections, independent replies:** Summary/Memory failures create
  stable event-derived repair jobs carrying the canonical governed effective input,
  so repair preserves redaction and attachments. IM delivery resolves only its binding and Audit
  backend, so a retired Knowledge/Artifact backend cannot suppress a ready reply.
- **Fail-before-switch configuration:** activation resolves scoped secrets, rejects
  unknown tools/local production backends, constructs the tRPC model/session runtime,
  and health-checks every resource before the audited pointer change. Bootstrap uses
  the same preflight.
- **Atomic object publication:** S3 and filesystem metadata/bytes are one immutable
  versioned bundle. Conditional object create or no-overwrite atomic link prevents
  same-version writers from producing a torn `.bin`/`.json` pair. Bounded S3 409
  retries, signed-64-bit versions, and torn legacy-pair detection close provider
  and upgrade edge cases.
- **Separated SQL ownership:** platform Session rows and native tRPC Session rows
  live in different provisioned databases. One-shot migration credentials own DDL;
  startup rejects a superuser or `BYPASSRLS` runtime role.
- **End-to-end confidentiality:** root logging filter, exact resolved-secret
  registry, rendered traceback redaction, safe errors, exporter-level span
  sanitization, Collector-level removal, and safe streaming policy.
- **Failure-boundary discipline:** delivery and tool audit failures are observable
  but cannot requeue an accepted reply or rewrite the original tool outcome.
- **Migration proof, not migration hope:** canonical verification includes session
  identity/state/events and actual artifact content, so a corrupted target cannot
  pass merely because row counts match. Re-embedding additionally requires
  immutable chunk identity and golden-query recall.
- **Pinned-network dangerous tool:** HTTPS fetches connect to a DNS-validated public
  IP while retaining the original TLS SNI/Host, closing the DNS-rebinding gap left
  by a validate-then-resolve-again design.
- **Honest semantics:** no claim of global exactly-once IM delivery, no sticky
  session, no claim that local vector or InMemory is production multi-node storage.
- **WeCom Bot connection:** Bot ID/Secret uses the provider WebSocket subscribe
  protocol, a shared Bot-ID lease, authenticated callback matching, and a dedicated
  durable outbox lane. It does not pretend that CorpID callback credentials can
  authenticate this separate product.

## Likely questions and strong answers

### Why not sticky sessions?

Sticky routing reduces cache misses but makes correctness depend on a node. Here,
the shared Session/Memory backends are authoritative, and a session lease plus
revision makes any Worker safe. Connection pools and immutable Runner graphs are
local caches only. Node loss therefore causes replay, not conversation loss.

### Why both a lease and optimistic revision?

The lease prevents two expensive model/tool turns from interleaving. A lease can
expire or be lost during a partition, so the event append still compares the
expected revision. The first is coordination; the second is data integrity.

### Is this exactly once?

Inside PostgreSQL, one dedupe receipt owns the turn and completion plus reply
outbox commit atomically. External delivery remains at-least-once because a remote
platform can accept a message and lose the HTTP response. Stable platform IDs,
Telegram update IDs, WeCom duplicate-check settings, and outbox IDs minimize that
last ambiguity. Calling it globally exactly once would be incorrect.

### What if two messages for one group arrive together?

They have distinct receipts but the same group session HMAC. Different Workers may
receive them, but the lease prevents interleaved state mutation. Processing order is
lease-acquisition order, not a claim of strict callback FIFO; strict arrival order
requires a session-partitioned sequencing layer. Different sessions do not contend.

### How is tenant isolation enforced beyond `WHERE tenant_id`?

Tenant is in every PK, index, Redis namespace, object prefix, and vector collection;
the Gateway validates binding ownership; Runner caches include tenant/revision;
tools are tenant/app-filtered; audit uses opaque identities. Production adds a
non-owner/non-bypass DB role, per-prefix object IAM, per-tenant vector collections,
and NetworkPolicy. The checked-in RLS SQL is deliberately inactive because runtime
transactions do not yet set `app.tenant_id`; applying it now would cause an outage,
not improve isolation. A single missing application predicate is still not the
only barrier.

### What happens if Redis is down?

Before durable publish, the callback returns non-2xx and the IM retries. After a
job is in Redis Streams, a SQL outage leaves it unacknowledged. If Redis itself is
the Session backend, processing pauses rather than silently switching to local
memory. Correctness is preferred over an inconsistent reply.

### How do Redis-to-SQL migrations avoid lost writes?

The checked-in CLI supports a quiesced source and empty/shadow target. It enforces
source-prefix history and verifies canonical hashes, reconstructed state, summary
checkpoints, artifacts, and exact native tRPC state/ordered-event hashes before a
manual immutable cutover. Platform and native SQL DSNs are explicit and isolated;
interrupted native event-prefix replay resumes before final state/hash comparison.
SQL sources are schema-prechecked/no-DDL and Redis Cluster uses explicit clients.
An online migration additionally requires a separately implemented dual-write,
watermark/tail catch-up, repair log, and shadow-read controller; this repository
does not pretend the one-shot CLI supplies those phases.

### How is vector migration different?

If embedding model/dimension is identical, vectors may be copied and hashes/counts
checked. If it changes, text is re-embedded into a new collection and evaluated
with golden-query recall. Mixing incompatible vector versions and calling it a
migration would produce silent relevance regression.
For unchanged cosine vectors, both sides are normalized before exact hashing so
Qdrant's server-side normalization is not a false mismatch.

The CLI consumes a reviewed offline embedding map rather than importing arbitrary
code. It rejects changed text/metadata/identity and refuses a transform without a
golden-query file, so vector inequality is expected but relevance regression is not.

### Can concurrent requests overspend a tenant budget?

Admission locks the tenant-period usage row, removes expired reservations, and
checks settled usage plus all active reservations before inserting another. Each
reservation includes the full configured context window and maximum output for
every allowed LLM call and provider retry attempt, priced at the tenant profile. `RunConfig` enforces those
call/tool ceilings. Actual usage and reservation deletion commit with receipt/outbox
completion, so concurrent nodes cannot all pass against the same remaining balance.

### Can traces leak prompts or API keys?

Resolved secret values enter an exact-value redaction registry. Logs recursively
redact sensitive keys and values. All spans, including tRPC-created ones, pass
through a redacting exporter that removes Runner input/output, LLM request, state,
tool args, and exception text; the Collector repeats the removal. Trace IDs and
operation metadata remain, but prompt content and credentials do not.

### Why suppress partial streaming under redaction?

A secret such as an API key can be split across two chunks, defeating a per-chunk
regex. The default policy buffers and emits the sanitized final replacement. A
tenant that disables content redaction can opt into low-latency token streaming.
This is an explicit confidentiality/latency tradeoff, not an accidental leak.

### How do you calculate capacity?

Start with Little's Law using callback peak RPS and measured p95 turn time, add
headroom for burst and node loss, then divide by load-tested safe concurrency per
Worker. Independently size model TPM/RPM, backend operations per turn, queue buffer,
database connections, artifact bandwidth, and vector indexing. The CLI exposes the
formula so assumptions are reviewable.

## Live evidence commands

```bash
uv run tenant-agent validate-config config/tenants.example.yaml
uv run pytest -q
uv run ruff check .
uv run mypy src
uv run tenant-agent doctor
uv run tenant-agent capacity --peak-rps 100 --p95-latency 4
uv run tenant-agent probe-channel --channel telegram --binding-id tg-acme-prod-01
uv run tenant-agent probe-channel --channel wecom --binding-id wc-acme-prod-01
```

Expected current evidence is recorded in `REQUIREMENTS_MATRIX.md`. Do not claim a
Docker/Kubernetes runtime test until it has actually run in an environment with
Docker or Kubernetes.

## Final close

The project optimizes for explainable correctness: trusted tenant routing, explicit
consistency classes, no node-owned state, an exact transactional boundary, tenant-
selected backends, executable governance, and observable recovery. The remaining
limitations are stated and bounded rather than hidden. That makes the service
defensible under both code review and production failure questions.
