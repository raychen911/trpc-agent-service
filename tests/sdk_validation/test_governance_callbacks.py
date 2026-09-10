from __future__ import annotations

import inspect


def test_official_callback_adapter_exports_before_tool_hook() -> None:
    from trpc_service.tool import governance_callbacks

    callback = getattr(governance_callbacks, "before_tool_callback", None)
    assert callback is not None
    assert inspect.iscoroutinefunction(callback)


def test_callback_does_not_define_a_second_runner() -> None:
    from trpc_service.tool import governance_callbacks

    assert not hasattr(governance_callbacks, "Runner")
