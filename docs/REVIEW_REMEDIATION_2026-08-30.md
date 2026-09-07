# Production-Readiness Review Remediation

Date: 2026-08-30

Superseded for current gate totals by
`REVIEW_REMEDIATION_ROUND2_2026-08-30.md`; retained as the first-round audit trail.

This record maps the nine reported findings to the implemented correction and
regression evidence. It is an engineering review artifact, not a claim that
credentialed external infrastructure has been exercised on this workstation.

| Finding | Resolution | Regression evidence |
|---|---|---|
| P1 completed SQL receipts reclaimed after lease expiry | Reclaim now requires `FAILED` or expired `PROCESSING` in both the locked decision and SQL update predicate. `COMPLETED` is terminal, preserving one-use dangerous confirmations. | Expired completed receipt and SQL confirmation-token replay tests. |
| P1 tenant secret path/environment escape | File/Vault paths are percent-decoded, canonical-relative, traversal-free, tenant-scoped, and symlink-contained. Tenant env references require `TENANT_{TENANT_ID}_*`; only the Audit backend may use the control DSN. | Raw/encoded file and Vault traversal, cross-tenant env, and platform-env rejection tests. |
| P1 non-atomic monthly budget | A row-locked tenant-period ledger counts settled usage plus live worst-case reservations. Admission reserves estimated input, maximum output, and priced cost. Receipt completion reconciles actual usage and deletes the reservation atomically; failures release it and maintenance prunes expiry. | Concurrent InMemory and two-adapter SQL reservation tests, cost reservation test, completion/capacity release assertions. |
| P2 no Summary/Memory repair | Projection failure durably enqueues stable `auxiliary-repair` Outbox work. The repair service reconstructs both projections from canonical Session events with bounded retry/dead-letter behavior. | Injected dual-backend outage followed by successful repair without a second model run. |
| P2 IM delivery initializes unrelated backends | Delivery resolves the historical tenant/binding for provider output and resolves only Audit after provider success. Admin audit/session reads also use narrow adapters. | Delivery succeeds while a configured historical S3 backend is deliberately invalid. |
| P2 Redis retry delay/attempt loss | Deferral atomically leaves the PEL for a delayed sorted set, persists incremented attempts, promotes due jobs, accounts delayed work in admission limits, and dead-letters at the shared finite Worker limit. | Delay-not-early, attempt increment, delayed-capacity, and persistent-failure dead-letter tests. |
| P2 re-embedding always fails hash verification | Transforms may change only embedding/model fields. Verification hashes immutable chunk identity/content and requires golden-query recall. CLI accepts reviewed JSON embedding maps and golden cases instead of executable plugins. | Re-embedding success, missing-golden rejection, immutable-content rejection, and CLI option tests. |
| P2 activation lacks production preflight | Admin activate/rollback resolves channel/model/backend secrets, rejects unknown tools and production-local/browser backends, initializes and health-checks all resource adapters under a timeout, then audits and switches the pointer. | Unknown-tool pointer-preservation, missing-secret, unhealthy-backend, and production-local-backend tests. |
| P2 WeCom callback does not bind `AgentID` | Decrypted POST `AgentID` is mandatory and constant-time compared with the binding's resolved `agent_id`, in addition to existing signature/AES/CorpID validation. | Valid AgentID round trip and wrong-AgentID rejection tests. |

## Reproduced gates

See `REVIEW_REMEDIATION_ROUND2_2026-08-30.md` for the current post-review gate
totals. Historical numbers were removed here to prevent stale evidence from being
mistaken for the current tree.

## External gates still required

Run the opt-in real PostgreSQL test, Redis Cluster failover/retry test, real
Telegram and WeCom callback/delivery probes, Vault/S3/Qdrant checks, deployed
Collector trace inspection, Kubernetes/Compose rollout, production load/chaos,
and backup/restore drill in the mentor-provided environment. Docker and `kubectl`
are not available on this workstation, so those outcomes are not claimed here.
