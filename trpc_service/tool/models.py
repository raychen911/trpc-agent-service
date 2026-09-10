"""Governed tool descriptors used by the callback boundary."""
from __future__ import annotations
from typing import Any
from trpc_service.governance.models import SideEffectClass, ToolRiskLevel, UsageVector


class GovernedTool:
    def __init__(self, name: str, *, risk: ToolRiskLevel = ToolRiskLevel.LOW, side_effect: SideEffectClass = SideEffectClass.NONE, handler: Any = None) -> None:
        self.name, self.risk, self.side_effect, self.handler = name, risk, side_effect, handler


__all__ = ["GovernedTool", "SideEffectClass", "ToolRiskLevel", "UsageVector"]
