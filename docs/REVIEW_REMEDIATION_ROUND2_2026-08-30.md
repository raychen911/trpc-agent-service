# Second Production-Readiness Review Remediation

Date: 2026-08-30

> Historical snapshot, superseded by
> `REVIEW_REMEDIATION_ROUND3_2026-08-30.md`. Its automatic-RLS closure and gate
> totals are retracted there after the third-pass review exposed the missing
> runtime tenant context. Do not use this file as the current readiness claim.

This record resolves the nine second-round findings against the current local
tree. It does not replace credentialed infrastructure validation.

| Finding | Implemented closure | Regression evidence |
|---|---|---|
| P1 persisted reply can be overwritten by budget denial | Dispatcher probes deterministic outbound IDs before admission and again before policy denial. Recovery returns the persisted reply and settles its recorded actual usage without requiring a new reservation. Inbound events persist canonical `effective_text`. | Recovery succeeds after the monthly budget is deliberately exhausted, never reruns the engine, settles usage, and leaves no reservation. |
| P1 reservation omits history/tools/multi-call usage | `ModelConfig.context_window_tokens` bounds each prompt; reservation covers context plus maximum output for every allowed LLM call. Budget policy adds finite LLM/tool-call limits and tRPC `RunConfig` enforces them. | Reservation-size and captured-RunConfig tests; concurrent SQL/InMemory budget tests remain green. |
| P1 Memory repair bypasses redaction/attachments | Normal persistence stores governed `effective_text`; repair consumes that exact field, with a legacy raw-text fallback only for old events. | Injected Memory failure with `redact_before_model=true` preserves `[REDACTED]` and safe attachment metadata while excluding raw email/provider ID. |
| P1 env-prefix collision and tenant-local symlink escape | Separator-bearing tenant IDs use injective Base32 prefixes; simple IDs retain readable prefixes. File resolution rejects a symlinked tenant root and requires the final path to remain under the lexical top-level tenant directory. | `a-b` versus `a_b` cross-prefix rejection plus real/simulated sibling-directory symlink escape tests. |
| P2 runtime-invalid activation/bootstrap bypass | Preflight constructs and closes provider-specific model and native Session objects without provider invocation. First bootstrap receives the same preflight callback before any version is stored or activated. | Invalid LiteLLM activation leaves the active pointer unchanged; rejecting bootstrap leaves no version or active tenant. |
| P2 duplicate contention creates false dead letters | Broker defer supports non-counting waits. `duplicate_processing` keeps attempts unchanged; only actual exceptions consume the finite failure budget. | Repeated Inline and Redis duplicate waits remain attempt zero and create no dead letter. |
| P2 local-vector source mutation/empty success/native history manual | SQL/local-vector migration always uses `create_schema=False`, validates SQLite paths before connection, and rejects an all-empty source unless explicitly allowed. CLI integrates optional native tRPC Session replay after a complete SQL schema check. | Misspelled source remains absent, empty migration fails/opt-in succeeds, native replay is idempotent. |
| P2 S3 torn metadata/content writes | Metadata and bytes are encoded in one immutable versioned bundle. Conditional create makes same-version publication atomic; identical races are idempotent and conflicting races fail. Readers select the highest complete bundle; legacy pairs are read-only compatible. | Concurrent conflicting-writer test yields one conflict and one checksum-valid artifact; no `.bin`/`.json` pair is produced. |
| P2 `usage_reservations` absent from RLS | Superseded: adding the table to automatic RLS was unsafe because runtime transactions do not set tenant context. Round 3 disables the implicit policy and retains only a clearly inactive reference script. | Corrective Alembic migration and deployment-policy test. |

## Independent final-audit follow-ups

Three scoped read-only reviewers then checked the modified tree. Their bounded
follow-ups were also closed: legacy-event repair now reuses the full governed input
projection; NTFS junction/reparse traversal is rejected; the then-current RLS
claim was later invalidated and superseded by Round 3; S3 retries 409,
constrains versions, and detects torn legacy pairs; native tRPC state/event history
is prefix-checked and canonically hash-verified; budget bounds include provider
retries; Redis Cluster migration options select cluster-capable normalized/native
clients. No reviewer edited repository files.

## Current reproduced gates

```text
pytest                 108 passed, 1 skipped (external PostgreSQL URL absent)
branch coverage        85.28%
ruff lint              passed
ruff format            passed after formatting all changed files
mypy strict            44 source files passed (Python 3.11 target)
AST parse              73 Python files passed
Alembic head           8b6d1e4f2a90
configuration examples demo + production validated
locked dependency audit no known vulnerabilities
```

## Still external

The real PostgreSQL/RLS role test, Redis Cluster failover, real Telegram/WeCom,
Vault/S3/Qdrant, deployed tracing, Docker/Kubernetes rollout, production load/
chaos, and restore drill require the mentor environment. They are not claimed as
passed on this workstation.
