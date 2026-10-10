# Ambiguous provider endpoint

This is a local/isolated acceptance dependency. It accepts a Feishu-compatible
message request, commits only its idempotency key and a request hash to SQLite,
then closes the TCP connection before writing an HTTP response. The first
attempt is therefore observed by the adapter as `transport_unknown` while
`GET /state/<uuid>` proves that the provider accepted it.

A later request with the same UUID is an explicit replay. It returns a normal
success response and increments the observation counter without creating a
second ledger row or side effect. Reusing the UUID with a different request
body returns `409`.

Run it only on loopback or an isolated acceptance network:

```powershell
\.venv\Scripts\python.exe deploy\ambiguous_provider\server.py `
  --host 127.0.0.1 --port 8791 `
  --ledger runs\multitenant\ambiguous-provider.sqlite3
```

For a self-contained local acceptance run, omit `--provider-url`; the script
starts a loopback endpoint on an ephemeral port and stops it in a bounded
cleanup path:

```powershell
\.venv\Scripts\python.exe scripts\ambiguous_provider_acceptance.py `
  --output runs\multitenant\ambiguous-provider-acceptance.json
```

When an independently deployed endpoint is available, run the bounded
acceptance sequence against it:

```powershell
\.venv\Scripts\python.exe scripts\ambiguous_provider_acceptance.py `
  --execute --project trpc-agent-service `
  --provider-url http://127.0.0.1:8791 `
  --timeout-seconds 5 `
  --output runs\multitenant\ambiguous-provider-acceptance.json
```

The acceptance report contains only hashes, statuses, counters, and the three
existing fault-gate markers:
`delivery.ambiguous_observed`,
`delivery.replay_confirmation_required`, and
`delivery.replay_verified`.
