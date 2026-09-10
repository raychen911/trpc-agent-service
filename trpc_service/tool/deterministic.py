"""Deterministic side-effect counter for governance validation."""
from __future__ import annotations
from typing import Any


class DeterministicTool:
    def __init__(self, result: str = "ok") -> None:
        self.result, self.call_count, self.calls = result, 0, []

    async def __call__(self, **arguments: Any) -> str:
        self.call_count += 1
        self.calls.append(dict(arguments))
        return self.result
