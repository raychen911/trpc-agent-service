"""Load exact tenant/profile bindings for the built-in storage bundle.

This file format is deliberately not a connection registry.  It selects the
process-wide PostgreSQL/S3/pgvector resources that the worker already received
from its secret-backed configuration; it cannot change their physical
endpoints.  Deployments that require independently constructed resources must
register prebuilt :class:`~trpc_service.storage.services.TenantDataServices`
bundles through :class:`~trpc_service.storage.services.TenantStorageProfileRegistry`.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from trpc_service.storage.services import (
    PostgresTenantServiceFactory,
    RegisteredTenantServiceBundle,
)
from trpc_service.tenant.models import ProductionStorageSelection

_BUILT_IN_BUNDLE = "default_postgresql_s3_pgvector"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_MAX_FILE_BYTES = 64 * 1024
_MAX_PROFILES = 1_000


def _identifier(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("tenant storage profile registry identifier is invalid")
    return value


def _read_document(path: str | Path) -> object:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ValueError("tenant storage profile registry path must be absolute")
    try:
        if candidate.is_symlink() or not candidate.is_file():
            raise ValueError("tenant storage profile registry file is invalid")
        if candidate.stat().st_size > _MAX_FILE_BYTES:
            raise ValueError("tenant storage profile registry file is too large")
        payload = candidate.read_bytes()
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError("tenant storage profile registry file is unavailable") from exc
    if len(payload) > _MAX_FILE_BYTES:
        raise ValueError("tenant storage profile registry file is too large")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("tenant storage profile registry JSON is invalid") from exc


def load_default_profile_registrations(
    path: str | Path,
    default_factory: PostgresTenantServiceFactory,
) -> dict[tuple[str, str], RegisteredTenantServiceBundle]:
    """Load secret-free aliases of the worker's built-in storage bundle."""

    document = _read_document(path)
    if not isinstance(document, dict) or set(document) != {"schema_version", "profiles"}:
        raise ValueError("tenant storage profile registry schema is invalid")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ValueError("tenant storage profile registry schema version is unsupported")
    profiles = document["profiles"]
    if not isinstance(profiles, list) or not 1 <= len(profiles) <= _MAX_PROFILES:
        raise ValueError("tenant storage profile registry profiles are invalid")

    selections: dict[tuple[str, str], ProductionStorageSelection] = {}
    for entry in profiles:
        if not isinstance(entry, dict) or set(entry) != {"tenant_id", "profile_id", "bundle"}:
            raise ValueError("tenant storage profile registry profile schema is invalid")
        tenant_id = _identifier(entry["tenant_id"])
        profile_id = _identifier(entry["profile_id"])
        if entry["bundle"] != _BUILT_IN_BUNDLE:
            raise ValueError("tenant storage profile registry bundle is unsupported")
        key = (tenant_id, profile_id)
        if key in selections:
            raise ValueError("tenant storage profile registry contains a duplicate binding")
        selections[key] = ProductionStorageSelection(profile_id=profile_id)

    return {
        key: RegisteredTenantServiceBundle(
            selection=selection,
            services=default_factory.build_bundle(selection),
        )
        for key, selection in selections.items()
    }


__all__ = ["load_default_profile_registrations"]
