# Submission metadata

- Author: Changyuan Chen
- Institution: Eastern Michigan University
- Branch: `feature/Changyuan-Chen`
- Submission date: 2026-09-07

This branch contains the reviewed multi-tenant tRPC-Agent implementation,
including the WeCom intelligent bot channel, shared-session deployment,
tenant isolation, storage adapters, governance, telemetry, recovery tooling,
and local verification evidence.

No access tokens, model API keys, database passwords, or WeCom credentials are
committed. Runtime credentials are supplied only through environment-variable
references or an external secret manager.
