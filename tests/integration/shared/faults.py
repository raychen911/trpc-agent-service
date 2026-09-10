"""Explicit, test-only fault points for reproducible failure matrices."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FaultController:
    armed: set[str] = field(default_factory=set)
    observed: list[str] = field(default_factory=list)

    def arm(self, point: str) -> None:
        self.armed.add(point)

    def hit(self, point: str) -> None:
        self.observed.append(point)
        if point in self.armed:
            self.armed.remove(point)
            raise RuntimeError(f"injected:{point}")

    def assert_observed(self, point: str) -> None:
        if point not in self.observed:
            raise AssertionError(f"fault point was not reached: {point}")


class BackendFailureProxy:
    """Attribute proxy that can fail named async backend operations exactly once."""
    def __init__(self, target: Any, controller: FaultController, backend: str) -> None:
        self.target, self.controller, self.backend = target, controller, backend

    def __getattr__(self, name: str) -> Any:
        value = getattr(self.target, name)
        if not callable(value):
            return value
        async def invoke(*args: Any, **kwargs: Any) -> Any:
            self.controller.hit(f"{self.backend}.{name}")
            return await value(*args, **kwargs)
        return invoke


@dataclass
class AgentSpy:
    calls: int = 0

    async def execute(self) -> str:
        self.calls += 1
        return "executed"
