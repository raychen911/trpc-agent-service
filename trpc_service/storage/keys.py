"""Canonical keys shared by workers and administrative storage operations."""

from __future__ import annotations


def session_execution_key(tenant_id: str, app_id: str, session_id: str) -> str:
    """Return the single fencing identity for all writes to one SDK session."""
    return f"{tenant_id}:{app_id}:{session_id}"
