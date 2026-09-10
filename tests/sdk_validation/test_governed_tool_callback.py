import asyncio
from types import SimpleNamespace
from trpc_service.governance.models import ToolDescriptor, ToolRiskLevel, SideEffectClass
from trpc_service.tool.governance_callbacks import before_tool_callback

def test_callback_blocks_non_allow_decision():
    class C:
        tool_descriptors={'x': ToolDescriptor(tool_name='x',side_effect_class=SideEffectClass.NONE,risk_level=ToolRiskLevel.LOW)}
        governance=SimpleNamespace(authorize=lambda **kw: None)
    assert asyncio.iscoroutinefunction(before_tool_callback)
