"""思考与回答分离测试：thought Part 收敛、<tool_call> 标签兜底、端到端透出。"""
from trpc_service.agent.runner import RunResult, split_think_tags


def test_split_think_tags():
    body, reasoning = split_think_tags("<tool_call>先查工具再回答。<tool_call>最终答案")
    assert body == "最终答案"
    assert reasoning == "先查工具再回答。"
    body2, reasoning2 = split_think_tags("无思考的正文")
    assert body2 == "无思考的正文"
    assert reasoning2 == ""


def test_run_result_has_reasoning_field():
    r = RunResult(text="答案", reasoning="思考")
    assert r.text == "答案" and r.reasoning == "思考"


def test_runner_separates_thought_parts():
    """thought=True 的 Part 进 reasoning，不进正文（模拟 SDK 事件流）。"""
    from unittest.mock import MagicMock

    from trpc_service.agent.runner import AgentRunner

    def make_part(text, thought=False):
        p = MagicMock()
        p.text = text
        p.thought = thought
        return p

    text_part = make_part("正文回复")
    thought_part = make_part("这是思考", thought=True)

    event_final = MagicMock()
    event_final.partial = False
    event_final.get_function_calls.return_value = []
    event_final.get_function_responses.return_value = []
    event_final.is_error.return_value = False
    event_final.content.parts = [thought_part, text_part]

    runner = AgentRunner.__new__(AgentRunner)
    runner.runner = MagicMock()
    runner.app_name = "t"

    async def fake_stream(*args, **kwargs):
        yield event_final

    runner.runner.run_async = fake_stream

    import asyncio

    result = asyncio.run(runner.run("u", "s", "hi"))
    assert result.text == "正文回复"
    assert result.reasoning == "这是思考"
