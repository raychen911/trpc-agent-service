# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Shared helpers for tenant-scoped namespacing."""

from __future__ import annotations

import re

SCOPE_SEPARATOR = ":"

_INVALID_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9_]")


def to_agent_name(tenant_id: str) -> str:
    """Map a tenant id to a valid Python identifier usable as an Agent name.

    Agent names must be Python identifiers (framework constraint); tenant ids
    are more permissive. Non ``[A-Za-z0-9_]`` characters are replaced with
    ``_`` and a leading digit is prefixed with ``t_``.
    """
    name = _INVALID_IDENTIFIER_RE.sub("_", tenant_id)
    if not name:
        name = "tenant"
    if name[0].isdigit():
        name = "t_" + name
    return name


def scope_key(tenant_id: str, key: str) -> str:
    """Namespace ``key`` under ``tenant_id``, idempotently.

    The framework already uses ``app_name`` as the first component of every
    storage key (``session:{app}:{user}:{session}`` and ``save_key={app}/{user}``),
    so tenant isolation is achieved by prefixing that first component. The
    prefix is idempotent so already-scoped keys (e.g. a ``session.save_key``
    derived from a scoped ``app_name``) are not double-prefixed.

    ``tenant_id`` must not contain the framework delimiters ``:`` or ``/``
    (enforced by the :class:`Tenant` model validator), which keeps the scope
    prefix collision-free.
    """
    if not tenant_id:
        return key
    prefix = f"{tenant_id}{SCOPE_SEPARATOR}"
    if key.startswith(prefix):
        return key
    return f"{prefix}{key}"
