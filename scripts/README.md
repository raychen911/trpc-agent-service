# Operations Scripts

`load_smoke.py` sends bounded concurrent requests to the deterministic local Web
adapter and reports throughput plus p50/p95/p99 latency. It is a smoke/load-shape
check, not a substitute for a production test with real model and backend quotas.

```bash
uv run python scripts/load_smoke.py --requests 1000 --concurrency 32
```

The default demo has a deliberately small budget; policy denials are reported
separately from HTTP failures. For reproducible local load and recovery checks:

```bash
uv run python scripts/local_runtime_check.py --requests 500 --concurrency 32
uv run python scripts/local_runtime_check.py --package-only
```

The runtime check creates a fresh OS temporary directory with synthetic Web
tenants, three SQLite databases, and two loopback Uvicorn processes. It validates
cross-process session/tenant isolation, duplicate delivery, a hot session, load,
kill-during-turn recovery after the real lease expires, SQLite backup/restore,
and extracted-wheel migration outside the checkout. Child processes are stopped
on exit; the temporary evidence directory is retained. `local_runtime_server.py`
is its fault-injection fixture and is never a production entrypoint. These checks
do not connect to WeCom or exercise PostgreSQL/Redis Cluster semantics.

`render_k8s.py` replaces only workload images with a reviewed immutable digest
and gives migration Jobs a release-unique name; the runbook uses it as the
production apply gate.

`security_audit.py` exports the locked dependency graph to an OS temporary file,
runs `pip-audit` through `uvx`, and removes the file even on failure:

```bash
uv run python scripts/security_audit.py
```

Database, Redis, object-store, vector-store, and Vault backups are intentionally
not implemented as ad-hoc application scripts. Production recovery must use each
managed service's snapshot/PITR mechanism and the ordered restore drill in
`docs/RUNBOOK.md`; copying live Docker volumes is not a consistent backup.
