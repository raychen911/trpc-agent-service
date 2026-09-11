"""One short, explicit OpenRouter request through the pinned official SDK."""

import asyncio
import logging
import os

from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.models import OpenAIModel, LlmRequest
from trpc_agent_sdk.types import Content, Part, GenerateContentConfig, HttpOptions

from trpc_service.sdk_provenance import verify_official_sdk
from trpc_service.telemetry.sdk_logging import configure_sdk_logging

MODEL = "deepseek/deepseek-v3.2"
ENDPOINT = "https://openrouter.ai/api/v1"


def request():
    return LlmRequest(model=MODEL,
                      contents=[Content(role="user", parts=[Part.from_text(text="请只回复：模型连接成功")])],
                      config=GenerateContentConfig(max_output_tokens=32,
                                                   temperature=0,
                                                   http_options=HttpOptions(timeout=40000,
                                                                            extra_body={
                                                                                "reasoning": {
                                                                                    "enabled": False
                                                                                },
                                                                                "provider": {
                                                                                    "allow_fallbacks": False,
                                                                                    "max_price": {
                                                                                        "prompt": 1,
                                                                                        "completion": 2
                                                                                    }
                                                                                }
                                                                            })))


async def check(model):
    received = False
    usage = None
    responses = model.generate_async(request(), stream=False)
    try:
        async for response in responses:
            if response.error_code:
                return {"status": "model_error", "cost_status": "not_reconciled"}
            if response.content:
                received = received or any(bool(part.text) for part in response.content.parts)
            if response.usage_metadata:
                usage = response.usage_metadata
    finally:
        await responses.aclose()
    return {
        "status": "text_received" if received else "no_text",
        "input_tokens": usage.prompt_token_count if usage else None,
        "output_tokens": usage.candidates_token_count if usage else None,
        "cost_status": "not_reconciled"
    }


async def run_from_environment():
    verify_official_sdk()
    configure_sdk_logging()
    for name in ("httpx", "httpcore", "openai", "httpx2", "httpcore2"):
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)
    if not os.getenv("TRPC_MODEL_API_KEY") or os.getenv("TRPC_MODEL_BASE_URL") != ENDPOINT:
        print("缺少本次 OpenRouter 模型测试配置。")
        return False
    model = OpenAIModel(model_name=MODEL,
                        api_key=os.environ["TRPC_MODEL_API_KEY"],
                        base_url=ENDPOINT,
                        model_retry_config=ModelRetryConfig(num_retries=0),
                        client_args={
                            "timeout": 40
                        })
    print("开始一次 DeepSeek 模型连通测试：最多输出 32 tokens，关闭客户端重试与供应商回退。", flush=True)
    try:
        result = await asyncio.wait_for(check(model), 50)
    except Exception:
        print("模型测试未完成，原始异常已隐藏；费用未核算，不自动重试。")
        return False
    if result["status"] != "text_received":
        print("未取得模型文本回复；费用未核算，不自动重试。")
        return False
    print(f"官方 SDK 已收到模型文本回复；输入 tokens={result['input_tokens']}，输出 tokens={result['output_tokens']}。")
    print("这是独立模型连通测试；实际费用仍以 OpenRouter 账单为准。")
    return True
