"""Provider configuration for large language models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from trpc_service.config.models import SecretRef


class BailianModelConfig(BaseModel):
    """Configure Bailian through its OpenAI-compatible API."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["bailian_openai"] = "bailian_openai"
    model_name: str = Field(default="qwen-max", min_length=1)
    # qwen-max remains the text path; visual requests use a model that accepts
    # OpenAI-compatible Base64 image parts.
    vision_model_name: str = Field(default="qwen-vl-max", min_length=1)
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    api_key_ref: str = "env://DASHSCOPE_API_KEY"
    temperature: float = Field(default=0.2, ge=0, le=2)
    max_output_tokens: int = Field(default=4096, ge=1)

    @field_validator("api_key_ref")
    @classmethod
    def validate_api_key_reference(cls, value: str) -> str:
        """Require a secret reference so the API key is never embedded in JSON."""

        SecretRef(uri=value)
        return value


class BailianEmbeddingConfig(BaseModel):
    """Configure tenant knowledge vectors through Bailian's compatible API."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_name: str = Field(default="bailian", min_length=1)
    model_name: str = Field(default="text-embedding-v4", min_length=1)
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    api_key_ref: str = "env://DASHSCOPE_API_KEY"
    # The current pgvector migration is physically VECTOR(1024). Supporting a
    # different width requires a versioned index migration, not a runtime knob.
    dimensions: Literal[1024] = 1024
    batch_size: int = Field(default=10, ge=1, le=10)

    @field_validator("api_key_ref")
    @classmethod
    def validate_api_key_reference(cls, value: str) -> str:
        """Reuse a SecretRef without copying the API key into settings JSON."""

        SecretRef(uri=value)
        return value
