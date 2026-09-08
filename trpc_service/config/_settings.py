"""Validated process settings for every runtime role."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import model_validator


def _boolean(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, 1/0, yes/no, on/off")


class ServiceSettings(BaseModel):
    """Single typed configuration surface shared by Gateway/Worker/Outbox."""

    model_config = ConfigDict(extra="forbid")

    environment: Literal["development", "test", "production"] = "development"
    role: Literal["gateway", "worker", "outbox", "all"] = "all"
    host: str = "0.0.0.0"
    port: int = Field(default=8080, ge=1, le=65535)
    tenants_config: Optional[str] = None
    redis_url: Optional[SecretStr] = None
    mysql_url: Optional[SecretStr] = None
    tenant_config_encryption_key: Optional[SecretStr] = None
    admin_api_key: Optional[SecretStr] = None
    queue_enabled: bool = True
    durable_delivery_enabled: bool = False
    test_api_enabled: bool = False
    test_api_key: Optional[SecretStr] = None
    model_api_key_ref: str = "env://TRPC_SERVICE_MODEL_API_KEY"
    deepseek_input_price_per_mtok: float = Field(default=0.22, ge=0)
    deepseek_output_price_per_mtok: float = Field(default=0.66, ge=0)
    secret_file_root: str = "/run/secrets"
    outbox_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    outbox_max_attempts: int = Field(default=8, ge=1, le=100)

    @model_validator(mode="after")
    def _validate_role_dependencies(self) -> "ServiceSettings":
        if self.test_api_enabled and self.test_api_key is None:
            raise ValueError("TRPC_SERVICE_TEST_API_KEY is required when the test API is enabled")
        if self.role == "worker" and self.redis_url is None:
            raise ValueError(f"role={self.role} requires TRPC_SERVICE_REDIS_URL")
        if self.role == "worker" and self.durable_delivery_enabled and self.mysql_url is None:
            raise ValueError("durable delivery requires TRPC_SERVICE_MYSQL_URL")
        if self.role == "outbox" and self.mysql_url is None:
            raise ValueError("role=outbox requires TRPC_SERVICE_MYSQL_URL")
        if self.environment == "production":
            if self.role == "all":
                raise ValueError("production requires one explicit runtime role")
            if self.role == "gateway":
                if self.admin_api_key is None:
                    raise ValueError("production gateway requires TRPC_SERVICE_ADMIN_API_KEY")
                if self.mysql_url is None:
                    raise ValueError("production gateway requires TRPC_SERVICE_MYSQL_URL")
                if self.queue_enabled and self.redis_url is None:
                    raise ValueError("queued production gateway requires TRPC_SERVICE_REDIS_URL")
            if self.role == "worker" and not self.durable_delivery_enabled:
                raise ValueError("production worker requires durable delivery")
        return self

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "ServiceSettings":
        """Load only the canonical ``TRPC_SERVICE_*`` environment variables."""
        values = environ if environ is not None else os.environ
        prefix = "TRPC_SERVICE_"
        data: dict[str, object] = {}
        text_fields = {
            "ENVIRONMENT": "environment",
            "ROLE": "role",
            "HOST": "host",
            "TENANTS_CONFIG": "tenants_config",
            "REDIS_URL": "redis_url",
            "MYSQL_URL": "mysql_url",
            "TENANT_CONFIG_ENCRYPTION_KEY": "tenant_config_encryption_key",
            "ADMIN_API_KEY": "admin_api_key",
            "TEST_API_KEY": "test_api_key",
            "MODEL_API_KEY_REF": "model_api_key_ref",
            "SECRET_FILE_ROOT": "secret_file_root",
        }
        for suffix, field in text_fields.items():
            if prefix + suffix in values:
                data[field] = values[prefix + suffix]
        integer_fields = {
            "PORT": "port",
            "OUTBOX_MAX_ATTEMPTS": "outbox_max_attempts",
        }
        float_fields = {
            "DEEPSEEK_INPUT_PRICE_PER_MTOK": "deepseek_input_price_per_mtok",
            "DEEPSEEK_OUTPUT_PRICE_PER_MTOK": "deepseek_output_price_per_mtok",
            "OUTBOX_POLL_INTERVAL_SECONDS": "outbox_poll_interval_seconds",
        }
        for suffix, field in integer_fields.items():
            if prefix + suffix in values:
                data[field] = int(values[prefix + suffix])
        for suffix, field in float_fields.items():
            if prefix + suffix in values:
                data[field] = float(values[prefix + suffix])
        for suffix, field in {
                "QUEUE_ENABLED": "queue_enabled",
                "DURABLE_DELIVERY_ENABLED": "durable_delivery_enabled",
                "TEST_API_ENABLED": "test_api_enabled",
        }.items():
            if prefix + suffix in values:
                data[field] = _boolean(values[prefix + suffix], prefix + suffix)
        return cls.model_validate(data)

    @staticmethod
    def reveal(value: Optional[SecretStr]) -> Optional[str]:
        """Reveal a process setting only at a component construction boundary."""
        return value.get_secret_value() if value is not None else None
