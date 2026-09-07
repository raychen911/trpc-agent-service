# Local Runtime Evidence

Date: 2026-09-07

All checks in this report use synthetic Web messages and isolated temporary
databases. The real WeCom Bot connection was tested separately with the supplied
credentials; the load and failure tests never send data to WeCom.

## Two-process shared-SQL check

Command:

```bash
uv run python scripts/local_runtime_check.py --requests 500 --concurrency 32
```

Results:

- Platform and isolated native Session schemas were provisioned through the CLI;
  Alembic head was `d4e5f607a1b2`.
- Two independent Uvicorn processes shared SQLite SQL adapters and routed the same
  tenant/session correctly. A cross-tenant session read returned 404.
- Sixteen duplicate deliveries committed exactly two Session events.
- Twelve concurrent messages on one hot session committed 24 contiguous events.
- Five hundred HTTP requests at concurrency 32 all returned `processed`, with zero
  HTTP/application errors, 19.39 requests per second, and p95 latency 5.73 seconds.
- A process was killed while inside a delayed model turn. The other process first
  observed `duplicate_processing`, waited for the real persisted receipt lease to
  expire, then completed the turn with exactly two events. The clock was not
  modified.
- SQLite backup API snapshots and restores of control, platform, and native
  databases passed `PRAGMA integrity_check` and canonical dump comparison.
- After restore, a completed reply remained idempotent and a new message continued
  the original session.

The complete machine-readable report is retained at
[local runtime JSON evidence](C:/Users/ROG/AppData/Local/Temp/tap-local-runtime-kjh_1sc7/report.json).

## Package check

`uv build --wheel` produced a 65-entry wheel containing the application, all four
Alembic revisions, the configuration examples, and the WeCom Bot adapter. The
wheel contained no `.env`, database, or bytecode files. Extracting it into a
directory outside the source checkout and running `tenant-agent db-init` succeeded
with migration head `d4e5f607a1b2`.

## Additional local checks

- The real Qdrant Python local engine persisted a cosine collection across client
  restart, enforced tenant filtering, and passed SQL-to-Qdrant migration plus
  recall verification.
- The loopback Bot service returned HTTP 200 for `/health/live`, `/health/ready`,
  and `/metrics`.
- The checked-in `scripts/load_smoke.py` completed a stock-demo run of 5 requests
  at concurrency 2 with 5 `processed`, zero failures, 5.44 requests per second,
  and p95 latency 433.84 ms.
- A further stock-demo run of 200 requests at concurrency 20 completed 194 turns
  and returned 6 expected budget-policy denials, with zero HTTP/transport errors.
  Total request throughput was 29.76/s, processed-turn throughput 28.86/s, and
  p95 latency 2591.71 ms. Policy denials are reported separately, not counted as
  successfully generated turns.
- The current test suite reports 133 passed and 1 PostgreSQL integration test
  skipped because no disposable PostgreSQL URL is configured. Branch-aware
  coverage is 85.13%.
- Ruff format/check, mypy strict across 48 source files, AST parsing across 83
  Python files, YAML parsing across 46 documents, `uv lock --check`, and the
  locked dependency audit all pass.

## Baseline load observation

The first 500-request run against the stock demo budget produced 274 successful
turns, 222 `monthly_token_budget` decisions, and three transient SQLite
`OperationalError` failures. This was a valid governance result but exposed that
the local SQLite writer configuration was too sensitive to concurrent locking.
The adapter now uses a bounded 30-second busy timeout and WAL journal mode. The
isolated high-budget rerun above then completed all 500 requests successfully.

## External boundary

The workstation has no PostgreSQL, Redis, Vault, S3, Qdrant server, Docker,
Kubernetes, or model endpoint configured. These remain unverified server-side
checks and require the mentor environment. The GitHub repository
`raychen911/trpc-agent-service` was inspected read-only at commit
`4cda37bfc41efc412e9ce5e38aa563859c1aa8ee`; it is a 23-file scaffold with empty
implementation placeholders. The complete implementation remains in the local
`multi-tenant-agent-platform` directory and has not been copied or submitted.
