# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant configuration loaders (YAML / JSON) with env-var expansion."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from typing import Union

import yaml

from ._models import Tenant


def expand_env_vars(value: Any) -> Any:
    """Recursively expand ``$VAR`` / ``${VAR}`` references in strings.

    Secret values can therefore be injected from the environment instead of
    being committed to disk in plaintext.
    """
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {key: expand_env_vars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    return value


def _load_raw(path: Path) -> Any:
    raw_text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(raw_text)
    return yaml.safe_load(raw_text)


def load_tenants(path: Union[str, Path]) -> list[Tenant]:
    """Load a list of tenants from a YAML/JSON file.

    Accepted top-level shapes:

    * a mapping ``{"tenants": [...]}``
    * a plain list ``[...]``

    Environment variables in any string value are expanded before model
    validation, so secrets may be referenced as ``${WECOM_TOKEN}`` etc.
    """
    data = expand_env_vars(_load_raw(Path(path)))

    if isinstance(data, dict):
        items = data.get("tenants")
        if items is None:
            # Allow a single-tenant document keyed by tenant id.
            if "tenant_id" in data:
                items = [data]
            else:
                raise ValueError("cannot find 'tenants' key in config document")
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError(f"unsupported config document type: {type(data).__name__}")

    if not isinstance(items, list):
        raise ValueError("'tenants' must be a list")

    return [Tenant.model_validate(item) for item in items]
