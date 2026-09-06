"""Tenant-safe secret reference resolution.

The configuration layer stores references only. Development can use environment
variables or a JSON map; production should replace this class with Vault/KMS.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from urllib.request import Request, urlopen


class SecretResolutionError(RuntimeError):
    pass


_SECRET_KEY_PATTERN = re.compile(
    r"(?i)(?:authorization|access[_ -]?token|api[_ -]?key|token|secret|password|"
    r"passwd|corp[_ -]?secret|app[_ -]?secret)"
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(\b(?:authorization|access[_ -]?token|api[_ -]?key|token|secret|password|"
    r"passwd|corp[_ -]?secret|app[_ -]?secret)\b\s*[:=]\s*)([\"']?)([^\s,;&}\"']+)"
)
_BEARER_PATTERN = re.compile(r"(?i)(\bBearer\s+)([^\s,;]+)")
_URL_QUERY_SECRET_PATTERN = re.compile(r"(?i)([?&](?:access[_-]?token|api[_-]?key|token|secret|password)=)([^&#\s]+)")


def redact_secret_text(value: object, extra_secrets: tuple[str, ...] = ()) -> str:
    """Redact credentials from exception text, logs, traces, and dead letters."""
    text = str(value)
    known = {item for key, item in os.environ.items() if item and _SECRET_KEY_PATTERN.search(key)}
    known.update(item for item in extra_secrets if item)
    for secret in sorted(known, key=len, reverse=True):
        text = text.replace(secret, "[secret-redacted]")
    text = _BEARER_PATTERN.sub(r"\1[secret-redacted]", text)
    text = _URL_QUERY_SECRET_PATTERN.sub(r"\1[secret-redacted]", text)
    return _SECRET_ASSIGNMENT_PATTERN.sub(r"\1\2[secret-redacted]", text)


def redact_secret_data(value: object) -> object:
    """Recursively sanitize provider response metadata without losing safe fields."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if _SECRET_KEY_PATTERN.search(str(key)):
                result[str(key)] = "[secret-redacted]"
            else:
                result[str(key)] = redact_secret_data(item)
        return result
    if isinstance(value, list):
        return [redact_secret_data(item) for item in value]
    if isinstance(value, tuple):
        return [redact_secret_data(item) for item in value]
    if isinstance(value, str):
        return redact_secret_text(value)
    return value


class SecretManager:
    def __init__(self, values: dict[str, str] | None = None) -> None:
        self.values = dict(values or {})
        raw = os.getenv("SECRETS_JSON", "")
        if raw:
            try:
                self.values.update({str(k): str(v) for k, v in json.loads(raw).items()})
            except (TypeError, ValueError) as exc:
                raise SecretResolutionError("SECRETS_JSON must be a JSON object") from exc

    def resolve(self, reference: str | None) -> str:
        if not reference or not reference.startswith("secret://"):
            raise SecretResolutionError("a secret:// reference is required")
        if reference in self.values:
            return self.values[reference]
        path = reference.removeprefix("secret://")
        env_name = "SECRET_" + re.sub(r"[^A-Za-z0-9]", "_", path).upper()
        value = os.getenv(env_name)
        if value:
            return value
        digest_name = "SECRET_SHA256_" + hashlib.sha256(reference.encode()).hexdigest()[:24].upper()
        value = os.getenv(digest_name)
        if value:
            return value
        vault_value = self._resolve_vault(reference)
        if vault_value is not None:
            return vault_value
        raise SecretResolutionError(f"secret is not configured: {reference}")

    def validate(self, reference: str | None) -> None:
        if reference:
            self.resolve(reference)

    @staticmethod
    def _resolve_vault(reference: str) -> str | None:
        address = os.getenv("VAULT_ADDR", "").rstrip("/")
        token = os.getenv("VAULT_TOKEN", "")
        if not address or not token:
            return None
        mount = os.getenv("VAULT_KV_MOUNT", "secret").strip("/")
        path = reference.removeprefix("secret://").strip("/")
        request = Request(
            f"{address}/v1/{mount}/data/{path}",
            headers={"X-Vault-Token": token},
            method="GET",
        )
        try:
            with urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            data = payload.get("data", {}).get("data", {})
            value = data.get("value") or data.get("secret")
            return str(value) if value is not None else None
        except Exception:
            return None
