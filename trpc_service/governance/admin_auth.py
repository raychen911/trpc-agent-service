"""Request-scoped management identities; credentials stay in operator environment."""

from contextvars import ContextVar
import hmac
import json
import os

from fastapi import HTTPException

principal = ContextVar("management_principal", default=None)


def current_actor(default="platform_operator"):
    value = principal.get()
    return value["actor"] if value else default


def identities():
    values = json.loads(os.environ.get("TRPC_ADMIN_PRINCIPALS", "[]"))
    if not isinstance(values, list):
        raise ValueError("management identities must be a list")
    tokens = set()
    for value in values:
        if (not isinstance(value, dict)
                or set(value) - {"actor", "role", "tenant_ids", "token_ref", "profile_ids", "secret_refs"}
                or value.get("role") not in {"viewer", "tenant_admin", "tenant_auditor"}
                or not isinstance(value.get("actor"), str) or not 1 <= len(value["actor"]) <= 128
                or not isinstance(value.get("tenant_ids"), list) or not value["tenant_ids"]
                or any(not isinstance(item, str) or not item for item in value["tenant_ids"])):
            raise ValueError("invalid management identity")
        reference = value.get("token_ref", "")
        if not reference.startswith("env://") or not reference[6:].isidentifier():
            raise ValueError("management credential must be an env reference")
        token = os.environ[reference[6:]]
        if len(token) < 32 or token in tokens or token == os.environ.get("TRPC_ADMIN_TOKEN"):
            raise ValueError("management tokens must be distinct and at least 32 characters")
        tokens.add(token)
        yield value, token


def authenticate(request, credentials, operator_token, operator_actor):
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(401, "management authentication required")
    value = None
    if operator_token and hmac.compare_digest(credentials.credentials, operator_token):
        value = {"actor": operator_actor, "role": "operator"}
    else:
        for candidate, token in identities():
            if hmac.compare_digest(credentials.credentials, token):
                value = candidate
    if value is None:
        raise HTTPException(401, "management authentication required")
    if value["role"] != "operator":
        tenant = request.path_params.get("tenant_id")
        if tenant not in value["tenant_ids"]:
            raise HTTPException(403, "management resource is outside the assigned tenant scope")
        if value["role"] in {"viewer", "tenant_auditor"} and request.method not in {"GET", "HEAD"}:
            raise HTTPException(403, "read-only management identity")
        if value["role"] == "tenant_auditor" and not (request.url.path == f"/admin/tenants/{tenant}/audit" or
                                                      request.url.path.startswith(f"/admin/tenants/{tenant}/audit/")):
            raise HTTPException(403, "auditor identity can only read audit evidence")
        if request.url.path.endswith("/model-prices") and request.method != "GET":
            raise HTTPException(403, "model prices require a platform operator")
    return value


def authorize_config(config):
    """A tenant cannot select another tenant's credentials or backend registrations."""
    value = principal.get()
    if value is None or value["role"] == "operator":
        return
    allowed_profiles = set(value.get("profile_ids", []))
    allowed_secrets = set(value.get("secret_refs", []))
    if any(
            getattr(config.data_backends, role).profile_id not in allowed_profiles
            for role in type(config.data_backends).model_fields):
        raise HTTPException(403, "backend profile is not assigned to this management identity")
    references = [model.api_key_ref.uri for model in config.models]
    if config.embedding:
        references.append(config.embedding.api_key_ref.uri)
    references.extend(server.secret_ref.uri for app in config.applications for server in app.mcp_servers
                      if server.secret_ref)
    references.extend(credential.secret_ref.uri for binding in config.channel_bindings
                      for credential in binding.credential_refs)
    if set(references) - allowed_secrets:
        raise HTTPException(403, "credential reference is not assigned to this management identity")
