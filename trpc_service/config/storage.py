"""Provider configuration for capability-oriented storage backends."""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy.engine import make_url

from trpc_service.config.models import SecretRef
if TYPE_CHECKING:
    from trpc_service.storage.router import BackendProfile


class LocalSecretNotReadyError(ValueError):
    """A local SecretRef is valid but does not have a usable value yet."""


class _BackendConfig(BaseModel):
    """Freeze parsed backend configuration for one process lifetime."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class InMemoryBackendConfig(_BackendConfig):
    """Configure a process-local backend implementing every capability."""

    kind: Literal["inmemory"] = "inmemory"


class _SQLBackendConfig(_BackendConfig):
    """Share safe SQL connection configuration across PostgreSQL adapters."""

    url: str
    password_ref: str | None = None

    @field_validator("url")
    @classmethod
    def reject_embedded_password(cls, value: str) -> str:
        """Keep plaintext database passwords out of environment JSON."""

        if make_url(value).password is not None:
            raise ValueError("database URL must use password_ref instead of an embedded password")
        return value

    @field_validator("password_ref")
    @classmethod
    def validate_password_reference(cls, value: str | None) -> str | None:
        """Accept only validated references when a password is required."""

        if value is not None:
            SecretRef(uri=value)
        return value


class PostgreSQLBackendConfig(_SQLBackendConfig):
    """Configure durable Session, Memory, Summary and Audit storage."""

    kind: Literal["postgresql"] = "postgresql"


class PgVectorBackendConfig(_SQLBackendConfig):
    """Configure vector retrieval and its injected embedding provider."""

    kind: Literal["pgvector"] = "pgvector"
    embedding_provider: str


class S3BackendConfig(_BackendConfig):
    """Configure any S3-compatible Artifact backend, including SeaweedFS."""

    kind: Literal["s3"] = "s3"
    endpoint_url: str | None = None
    bucket: str
    region: str = "us-east-1"
    access_key_ref: str | None = None
    secret_key_ref: str | None = None
    path_style: bool = True
    anonymous: bool = False

    @field_validator("access_key_ref", "secret_key_ref")
    @classmethod
    def validate_credential_reference(cls, value: str | None) -> str | None:
        """Require external references instead of plaintext S3 credentials."""

        if value is not None:
            SecretRef(uri=value)
        return value

    @model_validator(mode="after")
    def require_complete_static_credentials(self) -> "S3BackendConfig":
        """Reject half-configured static credentials before boto3 starts."""

        if (self.access_key_ref is None) != (self.secret_key_ref is None):
            raise ValueError("S3 access_key_ref and secret_key_ref must be configured together")
        if self.anonymous and self.access_key_ref is not None:
            raise ValueError("anonymous S3 mode cannot include static credentials")
        return self


StorageBackendConfig = Annotated[
    Union[
        InMemoryBackendConfig,
        PostgreSQLBackendConfig,
        PgVectorBackendConfig,
        S3BackendConfig,
    ],
    Field(discriminator="kind"),
]


class StorageProfileConfig(BaseModel):
    """Select a backend independently for each storage capability."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    session: str
    memory: str | None = None
    summary: str | None = None
    knowledge: str | None = None
    artifact: str | None = None
    audit: str | None = None

    def to_domain(self) -> "BackendProfile":
        """Convert validated settings into the provider-neutral routing value."""

        # Import at the composition boundary to keep configuration independent
        # from storage package initialization.
        from trpc_service.storage.router import BackendProfile

        return BackendProfile(**self.model_dump())


def resolve_local_secret(reference: str) -> str:
    """Resolve local env/file references; managed secret providers are injected later."""

    parsed = SecretRef(uri=reference).uri
    scheme, _, target = parsed.partition("://")
    if scheme == "env":
        try:
            return os.environ[target]
        except KeyError as error:
            raise LocalSecretNotReadyError(
                f"secret environment variable is not set: {target}") from error
    if scheme == "file":
        try:
            value = Path(target).read_text(encoding="utf-8").strip()
        except FileNotFoundError as error:
            raise LocalSecretNotReadyError("secret file does not exist") from error
        if value == "":
            raise LocalSecretNotReadyError("secret file is empty")
        return value
    raise ValueError(f"secret scheme requires an external resolver: {scheme}")
