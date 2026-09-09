# ===================================================================
# agent.model_factory - 租户模型配置 -> 框架 LLMModel 实例
# ===================================================================
# 说明: PRD 0.3-4b「按配置动态构建 LlmAgent」。经装包实测（trpc-agent-py
#   v1.1.19）：LlmAgent.model 只接受 `str | LLMModel | Callable`，
#   **不接受 dict**（传 dict 直接 ValidationError），故此处必须产出
#   LLMModel 实例，而非配置字典。
# 规范: api_key 只经 SecretStr 或环境变量注入，不落库不入日志（PRD 4.5）。
# ===================================================================

from __future__ import annotations

import os
from typing import Optional

from ..tenant.models import ModelConfig

DEFAULT_ENDPOINTS: dict[str, str] = {
    "deepseek": "https://api.deepseek.com",
    "openai": "https://api.openai.com/v1",
}
"""已知 provider 的默认 endpoint（DeepSeek 走 OpenAI 兼容协议）。"""

ENV_KEYS: dict[str, str] = {
    "deepseek": "DEEPSEEK_API_KEY",
    "openai": "OPENAI_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
}
"""provider -> 环境变量名（密钥环境注入，PRD 4.5）。"""


def resolve_model_endpoint(provider: str, base_url: Optional[str] = None) -> str:
    """解析模型 endpoint：显式 base_url 优先，其次 provider 默认值。

    Args:
        provider: 模型提供商（deepseek / openai / anthropic / custom）。
        base_url: 租户显式配置的 endpoint，优先级最高。

    Returns:
        解析后的 endpoint。

    Raises:
        ValueError: 未知 provider 且未给出 base_url（避免静默打到错误地址）。
    """
    if base_url:
        return base_url
    endpoint = DEFAULT_ENDPOINTS.get(provider)
    if endpoint:
        return endpoint
    raise ValueError(f"未知 provider '{provider}' 且未配置 base_url；请在租户 model.base_url 中显式指定 endpoint")


def resolve_api_key(cfg: ModelConfig) -> str:
    """解析模型 API key：租户 SecretStr 优先，其次环境变量。

    Returns:
        API key 明文（仅内存传递）。

    Raises:
        ValueError: 两处都没有时可抛错——显式失败好过静默回落 mock
            （阶段二 Spec 用例 6）。
    """
    if cfg.api_key_ref is not None:
        key = cfg.api_key_ref.get_secret_value()
        if key:
            return key
    env_name = ENV_KEYS.get(cfg.provider)
    if env_name:
        key = os.environ.get(env_name, "")
        if key:
            return key
    hint = f"（或设置环境变量 {env_name}）" if env_name else ""
    raise ValueError(f"租户未配置模型 api_key{hint}；请在 model.api_key_ref 中注入{hint}")


def build_llm_model(cfg: ModelConfig):
    """按租户模型配置构建框架 LLMModel 实例。

    Args:
        cfg: 租户模型配置（provider / model_name / api_key_ref / base_url）。

    Returns:
        框架 `LLMModel` 实例（OpenAIModel / AnthropicModel 等）。

    Raises:
        ValueError: 缺 api_key，或未知 provider 且无 base_url。
        ImportError: 未安装 trpc-agent-py。
    """
    from trpc_agent_sdk.models import AnthropicModel, OpenAIModel  # 懒加载：未装框架也可 import 本模块

    api_key = resolve_api_key(cfg)
    endpoint = resolve_model_endpoint(cfg.provider, cfg.base_url)

    if cfg.provider == "anthropic":
        return AnthropicModel(cfg.model_name, api_key=api_key, base_url=endpoint)

    # deepseek / openai 及所有 OpenAI 兼容协议均走 OpenAIModel：
    # 框架据 base_url 自动选用对应 adapter（实测 DeepSeek 命中 DeepSeekAdapter）。
    return OpenAIModel(cfg.model_name, api_key=api_key, base_url=endpoint)
