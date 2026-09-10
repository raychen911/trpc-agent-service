# Contract: Local Message HTTP Channel

**Version**: `v1`
**Feature**: `002-multitenant-local-message-flow`
**Base URL**: `http://127.0.0.1:8000` for local validation only

## 1. Endpoint

`POST /v1/local/messages`

Accepts one signed local text message and returns a uniform delivery result. This endpoint is
not a production IM webhook and must not be exposed outside the local validation environment.

## 2. Required Headers

| Header | Format | Meaning |
|---|---|---|
| `Content-Type` | `application/json` | Body encoding |
| `X-Channel-Binding` | 1–96 safe characters | Public binding identifier |
| `X-Request-Timestamp` | Unix seconds | Included in signature; accepted within ±300 seconds |
| `X-Signature` | `v1=<64 lowercase hex>` | HMAC-SHA256 signature |
| `X-Trace-ID` | UUID, optional | Inherited if valid; otherwise server generates a UUID |

No tenant identifier in the body or headers grants tenant authority. Tenant identity is derived
only after the binding signature succeeds.

## 3. Request Body

```json
{
  "channel": "local_http",
  "external_message_id": "msg-001",
  "external_user_id": "user-001",
  "conversation_type": "direct",
  "external_conversation_id": "conversation-001",
  "text": "Remember validation token ALPHA."
}
```

| Field | Type | Required | Validation |
|---|---|---:|---|
| channel | string | yes | Exactly `local_http` |
| external_message_id | string | yes | 1–128 Unicode characters |
| external_user_id | string | yes | 1–128 Unicode characters |
| conversation_type | string | yes | `direct` or `group` |
| external_conversation_id | string | yes | 1–128 Unicode characters |
| text | string | yes | Trimmed content is 1–4000 Unicode characters |

Unknown fields are rejected in v1 to prevent unsigned assumptions from silently entering the
business contract.

## 4. Signature Algorithm

### 4.1 Secret Resolution

The binding configuration contains only a `secret_ref`, which names an environment variable.
The environment value is interpreted as UTF-8 bytes. Missing or empty values fail as
`unauthorized`; the reference and value must not appear in logs or responses.

### 4.2 Body Digest

```text
body_sha256 = lowercase_hex(SHA256(exact_raw_request_body_bytes))
```

The sender must sign the exact bytes it sends. Reformatting JSON after signing invalidates the
signature.

### 4.3 Canonical String

Join exactly five UTF-8 lines with `\n` and no final newline:

```text
v1
{X-Request-Timestamp}
{X-Channel-Binding}
{external_message_id from body}
{body_sha256}
```

### 4.4 Signature

```text
signature = lowercase_hex(HMAC_SHA256(binding_secret, canonical_string_utf8))
X-Signature = "v1=" + signature
```

The server compares decoded signatures in constant time. Invalid version, length or hex syntax
fails before comparison.

### 4.5 Time Window and Replay

- The timestamp must parse as an integer Unix second.
- `abs(server_utc_unix_seconds - request_timestamp) <= 300`.
- Requests outside the window return the same unauthorized response as bad signatures.
- Valid repeated requests inside the window continue through the idempotency contract; HMAC
  authentication alone does not claim a message.

## 5. Uniform Response Envelope

```json
{
  "status": "succeeded",
  "trace_id": "93dc1bb1-d2fa-4fac-b824-10ba176a368c",
  "original_trace_id": null,
  "data": {
    "tenant_id": "tenant-alpha",
    "platform_session_id": "sess_<64-lowercase-hex>",
    "external_message_id": "msg-001",
    "text": "stored:ALPHA",
    "delivery_action": "deliver"
  },
  "error": null
}
```

### Envelope Fields

| Field | Required | Meaning |
|---|---:|---|
| status | yes | Stable result category |
| trace_id | yes | Current HTTP delivery trace |
| original_trace_id | conditional | Current owner attempt for processing; actual execution trace for any cached terminal result |
| data | conditional | Present for success, duplicate, processing, or an authenticated cached terminal failure |
| error | conditional | Present for rejected, conflict or failed result; may coexist with data for cached terminal failure |

### Data Fields

| Field | Required | Meaning |
|---|---:|---|
| tenant_id | after authentication | Verified tenant |
| platform_session_id | after resolution | Tenant-scoped session |
| external_message_id | after body validation | Validated external message |
| text | success/duplicate | First execution's user-visible final text |
| delivery_action | yes | `deliver` for first success, `suppress` for every cached terminal result, or `none` |
| retryable | processing/failure | Whether the same external message may be retried |

### Error Fields

```json
{
  "code": "idempotency_conflict",
  "message": "The message identifier was already used for different content.",
  "retryable": false,
  "execution_started": false
}
```

Error messages are fixed safe text. Exceptions, stack traces, secrets, signatures and full request
content are never returned.

## 6. Result Matrix

| HTTP | status | error.code | delivery_action | Same-ID Retry |
|---:|---|---|---|---|
| 200 | succeeded | null | deliver | Returns duplicate result if repeated |
| 200 | duplicate | null | suppress | Allowed but never re-executes |
| 202 | processing | null | none | Yes, later |
| 400 | invalid_request | invalid_request | none | Correct fields; use appropriate ID |
| 401 | unauthorized | unauthorized | none | Correct signature/timestamp |
| 403 | access_denied | access_denied | none | After binding/tenant configuration changes |
| 409 | conflict | idempotency_conflict | none | No; use a new external message ID |
| 502 | failed | agent_failed | none | No when execution_started=true |
| 502 | failed | agent_failed | suppress | Cached terminal failure; no re-execution |
| 503 | failed | agent_unavailable | none | Yes only when execution_started=false |
| 503 | failed | audit_unavailable | none | Yes only when execution_started=false |
| 503 | failed | audit_incomplete | none | No when execution_started=true |
| 503 | failed | audit_incomplete | suppress | Cached terminal failure; no re-execution |
| 503 | failed | outcome_unknown | none | No |
| 503 | failed | outcome_unknown | suppress | Cached terminal result; no re-execution |

## 7. Duplicate Semantics

For an already completed same-fingerprint request:

```json
{
  "status": "duplicate",
  "trace_id": "<current-delivery-uuid>",
  "original_trace_id": "<first-execution-uuid>",
  "data": {
    "tenant_id": "tenant-alpha",
    "platform_session_id": "sess_<same-id>",
    "external_message_id": "msg-001",
    "text": "stored:ALPHA",
    "delivery_action": "suppress",
    "retryable": false
  },
  "error": null
}
```

The HTTP caller can recover the original business result, but a Channel Adapter must obey
`delivery_action=suppress` and not send a second IM reply.

For a cached FAILED_POST_START or OUTCOME_UNKNOWN result, the response preserves the original
HTTP class and safe error code instead of changing status to `duplicate`. It includes the current
delivery `trace_id`, the first actual `execution_trace_id` as `original_trace_id`, authenticated
identity fields in `data`, `data.delivery_action=suppress`, `data.retryable=false`, and the saved
safe `error`. It never returns a second business reply.

For `processing`, `original_trace_id` points to the current `owner_trace_id` because an execution
trace may not exist yet. A FAILED_PRE_START reclaim replaces owner_trace_id for the new attempt;
first_claim_trace_id remains internal evidence and is never substituted for execution_trace_id.

## 8. Authentication Failure Non-Disclosure

The following conditions return the same HTTP 401 body shape and safe message:

- Unknown binding ID.
- Missing binding secret in the service environment.
- Missing or malformed timestamp.
- Timestamp outside ±300 seconds.
- Missing, malformed, unsupported-version or mismatched signature.
- Body changed after signature generation.

The body must not indicate which condition occurred. Internal audit uses safe error categories
without storing candidate secrets or signatures.

## 9. Trace Rules

- A valid `X-Trace-ID` becomes current `trace_id`.
- A missing or malformed value causes a new UUID; malformed input is not copied to logs.
- Each redelivery has a new current trace unless the caller deliberately supplies a valid one.
- Processing results include the owner attempt trace; cached success/failure results include the
  execution trace as `original_trace_id`. The current trace is never replaced by it.
- All Gateway, Worker, Session and Audit calls for one delivery receive the current trace.

## 10. Health Endpoint

`GET /healthz`

- Returns HTTP 200 with `{"status":"ok"}` after all in-memory repositories and Worker runtime
  are initialized.
- Does not expose tenant, binding, secret, dependency or process details.
- Does not prove production readiness, shared storage health or external connectivity.

## 11. Contract Compatibility

- Adding optional response fields is backward compatible within v1.
- Removing fields, changing signing input, changing enum meaning or relaxing tenant verification
  requires a new contract version.
- HTTP transport code must not pass raw framework request objects beyond the Channel Adapter.
