"""Scope model credential references before the runtime resolves them."""

from pathlib import Path
from uuid import UUID

from trpc_service.config.models import SecretRef

_PLATFORM_ENV_NAMES = frozenset({"DASHSCOPE_API_KEY"})
_PLATFORM_ENV_PREFIX = "TRPC_PLATFORM_MODEL_"


def _file_is_within(target: str, roots: tuple[Path, ...]) -> bool:
    """Compare normalized paths without requiring the secret file to exist yet."""

    path = Path(target).resolve(strict=False)
    return any(path.is_relative_to(root.resolve(strict=False)) for root in roots)


def validate_platform_model_secret_ref(reference: str) -> str:
    """Allow only operator-owned model credential locations."""

    parsed = SecretRef(uri=reference).uri
    scheme, _, target = parsed.partition("://")
    if scheme == "env" and (target in _PLATFORM_ENV_NAMES
                            or target.startswith(_PLATFORM_ENV_PREFIX)):
        return reference
    if scheme == "file" and _file_is_within(
            target, (Path(".secrets/platform"), Path("/run/secrets/platform"))):
        return reference
    raise ValueError("platform model SecretRef is outside the platform credential scope")


def tenant_model_env_prefix(tenant_id: UUID) -> str:
    """Return the environment namespace reserved for one tenant."""

    return f"TRPC_TENANT_{str(tenant_id).replace('-', '_').upper()}_MODEL_"


def validate_tenant_model_secret_ref(reference: str, tenant_id: UUID) -> str:
    """Prevent a tenant profile from reading another tenant or platform secret."""

    parsed = SecretRef(uri=reference).uri
    scheme, _, target = parsed.partition("://")
    if scheme == "env" and target.startswith(tenant_model_env_prefix(tenant_id)):
        return reference
    tenant_segment = str(tenant_id)
    tenant_roots = (
        Path(".secrets/tenants") / tenant_segment,
        Path("/run/secrets/tenants") / tenant_segment,
    )
    if scheme == "file" and _file_is_within(target, tenant_roots):
        return reference
    raise ValueError("tenant model SecretRef is outside this tenant's credential scope")


def _validate_tenant_integration_secret_ref(
    reference: str,
    tenant_id: UUID,
    scope: str,
    environment_scope: str,
    error_scope: str,
) -> str:
    """Scope one tenant integration credential without trusting URL path cleanup."""

    parsed = SecretRef(uri=reference).uri
    scheme, _, target = parsed.partition("://")
    environment_prefix = (
        f"TRPC_TENANT_{str(tenant_id).replace('-', '_').upper()}_{environment_scope}_")
    if scheme == "env" and target.startswith(environment_prefix):
        return reference
    tenant_segment = str(tenant_id)
    roots = (
        Path(".secrets/tenants") / tenant_segment / scope,
        Path("/run/secrets/tenants") / tenant_segment / scope,
    )
    if scheme == "file" and _file_is_within(target, roots):
        return reference
    external_prefix = f"tenants/{tenant_segment}/{scope}/"
    if scheme in {"vault", "secret-manager"}:
        normalized = target.strip("/")
        # External clients differ in URL decoding behavior. Forbid encoded and
        # relative path syntax instead of relying on provider-specific cleanup.
        segments = normalized.replace("\\", "/").split("/")
        unsafe_path = ("%" in normalized or "?" in normalized or "#" in normalized
                       or any(segment in {"", ".", ".."} for segment in segments))
        if not unsafe_path and normalized.startswith(external_prefix):
            return reference
    raise ValueError(f"{error_scope} SecretRef is outside this tenant's credential scope")


def validate_tenant_channel_secret_ref(reference: str, tenant_id: UUID) -> str:
    """Scope IM credentials for local and external secret backends."""

    # Keep the original singular CHANNEL namespace for backwards compatibility.
    return _validate_tenant_integration_secret_ref(
        reference,
        tenant_id,
        "channels",
        "CHANNEL",
        "channel",
    )


def validate_tenant_mcp_secret_ref(reference: str, tenant_id: UUID) -> str:
    """Scope remote MCP credentials independently from IM credentials."""

    return _validate_tenant_integration_secret_ref(reference, tenant_id, "mcp", "MCP", "MCP")
