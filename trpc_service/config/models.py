"""Reusable configuration models and security-oriented validators."""

from collections.abc import Mapping, Sequence
import re
from typing import ClassVar, Self, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

ConfigValue = TypeVar("ConfigValue")
CHANNEL_NAME_PATTERN = r"^[a-z][a-z0-9_]*$"
CHANNEL_NAME_MAX_LENGTH = 40


def normalize_channel_type(value: object) -> str:
    """Normalize and validate a channel name used in config or persistence."""

    if not isinstance(value, str):
        raise ValueError("channel type must be a string")
    normalized = value.strip().lower()
    if (len(normalized) > CHANNEL_NAME_MAX_LENGTH
            or re.fullmatch(CHANNEL_NAME_PATTERN, normalized) is None):
        raise ValueError("invalid channel type")
    return normalized


class SecretRef(BaseModel):
    """A reference to a secret; the secret value never belongs in configuration."""

    model_config = ConfigDict(frozen=True)
    allowed_schemes: ClassVar[frozenset[str]] = frozenset({
        "env",
        "file",
        "secret-manager",
        "vault",
    })

    uri: str

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        """Accept only references that identify a non-empty external secret."""

        scheme, separator, target = value.partition("://")
        if separator == "" or scheme not in cls.allowed_schemes or target.strip("/") == "":
            allowed = ", ".join(sorted(cls.allowed_schemes))
            raise ValueError(f"secret reference must use one of these schemes: {allowed}")
        return value


def validate_secret_bearing_config(value: ConfigValue, key: str = "") -> ConfigValue:
    """Recursively reject plaintext values under secret-bearing config keys."""

    normalized_key = key.lower().replace("-", "_")
    secret_key = (normalized_key in {"api_key", "password", "secret", "token"}
                  or normalized_key.endswith(
                      ("_api_key", "_password", "_secret", "_token", "_ref")))
    secret_container = normalized_key in {"secret_refs", "secret_ref_map"}
    # Flexible JSON config is inspected recursively using conservative naming
    # conventions. Provider-specific schemas should add stricter validation.
    if secret_key:
        if not isinstance(value, str):
            raise ValueError(f"{key or 'secret'} must be a SecretRef URI")
        SecretRef(uri=value)
    elif secret_container and isinstance(value, Mapping):
        for child_value in value.values():
            if not isinstance(child_value, str):
                raise ValueError(f"{key} values must be SecretRef URIs")
            SecretRef(uri=child_value)
    elif isinstance(value, Mapping):
        for child_key, child_value in value.items():
            validate_secret_bearing_config(child_value, str(child_key))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for child_value in value:
            validate_secret_bearing_config(child_value, key)
    return value


class NonNullableUpdateModel(BaseModel):
    """PATCH base that distinguishes omitted fields from forbidden explicit nulls."""

    @model_validator(mode="after")
    def reject_explicit_null(self) -> Self:
        """Reject explicit null while still allowing fields to be omitted."""

        null_fields = [field for field in self.model_fields_set if getattr(self, field) is None]
        if null_fields:
            raise ValueError(f"fields cannot be null: {', '.join(sorted(null_fields))}")
        return self
