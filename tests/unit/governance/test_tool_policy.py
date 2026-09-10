import asyncio
from trpc_service.governance.models import ToolDescriptor, ToolRiskLevel, SideEffectClass
from trpc_service.tool.deterministic import DeterministicTool

def test_tool_descriptor_high_risk_requires_confirmation_by_default():
    assert ToolDescriptor(tool_name='delete', side_effect_class=SideEffectClass.EXTERNAL, risk_level=ToolRiskLevel.HIGH).confirmation_required

def test_deterministic_tool_is_countable():
    t=DeterministicTool(); asyncio.run(t(x=1)); assert t.call_count == 1
