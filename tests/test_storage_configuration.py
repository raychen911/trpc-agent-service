from pathlib import Path

import pytest
from pydantic import ValidationError

from trpc_service.config import Settings
from trpc_service.config.storage import (
    InMemoryBackendConfig,
    LocalSecretNotReadyError,
    PgVectorBackendConfig,
    PostgreSQLBackendConfig,
    S3BackendConfig,
    resolve_local_secret,
)


def test_settings_load_bailian_model_and_api_key_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        'TRPC_SERVICE_LLM={"provider":"bailian_openai","model_name":"qwen-max",'
        '"base_url":"https://dashscope.aliyuncs.com/compatible-mode/v1",'
        '"api_key_ref":"env://DASHSCOPE_API_KEY","temperature":0.2,'
        '"max_output_tokens":4096}\nDASHSCOPE_API_KEY=sk-test-only\n',
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.llm.provider == "bailian_openai"
    assert settings.llm.model_name == "qwen-max"
    assert settings.dashscope_api_key.get_secret_value() == "sk-test-only"
    assert settings.embedding.model_name == "text-embedding-v4"
    assert settings.embedding.dimensions == 1024


def test_settings_parse_config_driven_storage_backends() -> None:
    settings = Settings(
        storage_backends={
            "local": {
                "kind": "inmemory"
            },
            "facts": {
                "kind": "postgresql",
                "url": "postgresql+asyncpg://trpc@127.0.0.1:55432/trpc_agent",
                "password_ref": "file://.secrets/postgres_password",
            },
            "vectors": {
                "kind": "pgvector",
                "url": "postgresql+asyncpg://trpc@127.0.0.1:55432/trpc_agent",
                "password_ref": "file://.secrets/postgres_password",
                "embedding_provider": "default",
            },
            "objects": {
                "kind": "s3",
                "endpoint_url": "http://127.0.0.1:8333",
                "bucket": "trpc-artifacts",
            },
        },
        storage_profile={
            "session": "facts",
            "memory": "facts",
            "summary": "facts",
            "knowledge": "vectors",
            "artifact": "objects",
            "audit": "facts",
        },
    )

    assert isinstance(settings.storage_backends["local"], InMemoryBackendConfig)
    assert isinstance(settings.storage_backends["facts"], PostgreSQLBackendConfig)
    assert isinstance(settings.storage_backends["vectors"], PgVectorBackendConfig)
    assert isinstance(settings.storage_backends["objects"], S3BackendConfig)
    assert settings.storage_profile.session == "facts"


def test_storage_url_rejects_an_embedded_plaintext_password() -> None:
    with pytest.raises(ValidationError):
        PostgreSQLBackendConfig(
            kind="postgresql",
            url="postgresql+asyncpg://trpc:plaintext@localhost/trpc_agent",
        )


def test_session_cache_url_accepts_redis_and_rejects_other_schemes() -> None:
    settings = Settings(session_cache_url="redis://cache.internal:6379/2")

    assert settings.resolved_session_cache_url == "redis://cache.internal:6379/2"
    with pytest.raises(ValueError, match="redis"):
        Settings(session_cache_url="postgresql://database/session").resolved_session_cache_url
    with pytest.raises(ValueError, match="host"):
        Settings(session_cache_url="redis:///0").resolved_session_cache_url


def test_s3_backend_rejects_partial_static_credentials() -> None:
    with pytest.raises(ValidationError):
        S3BackendConfig(
            kind="s3",
            bucket="artifacts",
            access_key_ref="env://S3_ACCESS_KEY",
        )


def test_s3_backend_allows_explicit_anonymous_local_service() -> None:
    config = S3BackendConfig(
        kind="s3",
        endpoint_url="http://seaweedfs:8333",
        bucket="artifacts",
        anonymous=True,
    )

    assert config.anonymous is True


def test_s3_backend_rejects_credentials_in_anonymous_mode() -> None:
    with pytest.raises(ValidationError):
        S3BackendConfig(
            kind="s3",
            bucket="artifacts",
            anonymous=True,
            access_key_ref="env://S3_ACCESS_KEY",
            secret_key_ref="env://S3_SECRET_KEY",
        )


def test_local_secret_resolver_reads_file_reference(tmp_path: Path) -> None:
    secret = tmp_path / "secret"
    secret.write_text("value\n", encoding="utf-8")

    assert resolve_local_secret(f"file://{secret}") == "value"


def test_local_secret_resolver_reports_empty_placeholder(tmp_path: Path) -> None:
    """Provisioning may create the protected file before its value is known."""

    secret = tmp_path / "secret"
    secret.write_text("\n", encoding="utf-8")

    with pytest.raises(LocalSecretNotReadyError, match="empty"):
        resolve_local_secret(f"file://{secret}")


def test_local_secret_resolver_reports_missing_environment(
    monkeypatch: pytest.MonkeyPatch, ) -> None:
    monkeypatch.delenv("TRPC_TEST_MISSING_SECRET", raising=False)

    with pytest.raises(LocalSecretNotReadyError, match="not set"):
        resolve_local_secret("env://TRPC_TEST_MISSING_SECRET")
