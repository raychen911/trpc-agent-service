"""Explicit registry for tenant selectable extension tools.

The registry is deliberately process local and code owned.  Tenant JSON may
select a registered tool, but it can never import or execute arbitrary Python
from configuration.  This keeps dynamic tool selection useful without making
the configuration store a code execution boundary.
"""

from __future__ import annotations

import re
from threading import RLock


_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_LOCK = RLock()
_TOOLS: dict[str, object] = {}


def register_tool(name: str, function) -> None:
    """Register a trusted callable under a stable model-facing name."""
    if not isinstance(name, str) or _NAME.fullmatch(name) is None:
        raise ValueError("tool name must be an identifier")
    if not callable(function):
        raise TypeError("tool must be callable")
    if getattr(function, "__name__", None) != name:
        raise ValueError("registered callable name must match tool name")
    with _LOCK:
        if name in _TOOLS:
            raise ValueError(f"tool already registered: {name}")
        _TOOLS[name] = function


def unregister_tool(name: str) -> None:
    with _LOCK:
        _TOOLS.pop(name, None)


def extension_tools() -> tuple:
    """Return a snapshot so runtime construction is deterministic."""
    with _LOCK:
        return tuple(_TOOLS.values())


def clear_extension_tools() -> None:
    """Test/operator helper; production code should register at startup."""
    with _LOCK:
        _TOOLS.clear()
