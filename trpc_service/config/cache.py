"""Configuration hints that can never replace the SQL authorization authority."""

from __future__ import annotations

from typing import Any, Awaitable, Callable


class AuthoritativeConfigCache:
    def __init__(self) -> None:
        self._verified: dict[str, tuple[int, Any]] = {}
        self._denied: dict[str, int] = {}

    def remember_verified(self, binding_id: str, version: int, value: Any) -> None:
        self._verified[binding_id] = (version, value)

    def remember_denied(self, binding_id: str, version: int) -> None:
        self._denied[binding_id] = version

    def verified_hint(self, binding_id: str) -> tuple[int, Any] | None:
        """Return a parsing/diagnostic hint; callers must not treat it as authority."""
        return self._verified.get(binding_id)

    async def authorize(self, binding_id: str, authoritative_read: Callable[[], Awaitable[Any]]) -> Any:
        if binding_id in self._denied:
            raise PermissionError("Binding remains denied.")
        # A positive entry is a parsing hint only. Every new authorization must
        # still complete the authority read successfully.
        value = await authoritative_read()
        return value
