from urllib.parse import quote


def idempotency_key(tenant_id: str, channel: str, external_message_id: str) -> str:
    """Return the canonical business idempotency key required by the gateway contract."""

    return f"{tenant_id}:{channel}:{external_message_id}"


def session_lock_key(tenant_id: str, agent_app_id: str, session_id: str) -> str:
    parts = (tenant_id, agent_app_id, session_id)
    return "session:" + ":".join(quote(part, safe="") for part in parts)
