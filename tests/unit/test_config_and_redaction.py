from __future__ import annotations

import json
import logging

import pytest
from pydantic import ValidationError

from trpc_service.config.secrets import LocalSecretProvider, SecretRef, SecretResolutionError
from trpc_service.config.settings import (
    Environment,
    SchedulerVersion,
    ServiceSettings,
    get_settings,
)
from trpc_service.log.configure import RedactingJsonFormatter
from trpc_service.log.redaction import REDACTED, redact, sanitize_text
from trpc_service.tenant.models import MediaPolicy


def test_secret_refs_never_render_targets(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("UNIT_TEST_SECRET", "value")
    provider = LocalSecretProvider()
    ref = SecretRef(uri="env://UNIT_TEST_SECRET")
    assert provider.resolve(ref) == "value"
    assert "UNIT_TEST_SECRET" not in str(ref)

    secret_file = tmp_path / "secret"
    secret_file.write_text("mounted\n", encoding="utf-8")
    assert provider.resolve(SecretRef(uri=secret_file.as_uri())) == "mounted"
    with pytest.raises(SecretResolutionError):
        provider.resolve(SecretRef(uri="literal://plaintext"))


def test_tenant_file_secret_rejects_in_root_symlink(tmp_path) -> None:
    root = tmp_path / "secrets"
    root.mkdir()
    target = root / "target"
    target.write_text("mounted\n", encoding="utf-8")
    link = root / "alias"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable in this test environment")

    provider = LocalSecretProvider(secret_root=root)
    with pytest.raises(SecretResolutionError, match="symlinks"):
        provider.resolve_tenant(SecretRef(uri=link.as_uri()))


def test_production_settings_fail_closed() -> None:
    with pytest.raises(ValidationError):
        ServiceSettings(environment=Environment.PRODUCTION)
    with pytest.raises(ValidationError, match="content capture"):
        ServiceSettings(
            environment=Environment.PRODUCTION,
            allow_development_token=False,
            oidc_issuer="https://issuer.example",
            oidc_audience="service",
            capture_content=True,
        )


def test_settings_parse_secret_reference_environment_strings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "TRPC_SERVICE_DATABASE_PASSWORD_REF", "file:///run/secrets/database_password"
    )
    monkeypatch.setenv("TRPC_SERVICE_SESSION_HMAC_REF", "file:///run/secrets/session_hmac")
    settings = ServiceSettings(_env_file=None)
    assert settings.database_password_ref == SecretRef(uri="file:///run/secrets/database_password")
    assert settings.session_hmac_ref == SecretRef(uri="file:///run/secrets/session_hmac")


def test_worker_queue_and_database_pool_settings_are_bounded() -> None:
    settings = ServiceSettings(_env_file=None)
    assert settings.worker_concurrency == 1
    assert settings.worker_poll_seconds == 5.0
    assert settings.scheduler_version == SchedulerVersion.V2
    assert settings.redis_stream == "trpc:session-ready:v2"
    assert settings.redis_consumer_group == "trpc-session-ready-v2"
    assert settings.redis_reclaim_after_ms == 60_000
    assert settings.redis_socket_connect_timeout_seconds == 3.0
    assert settings.redis_socket_timeout_seconds == 10.0
    assert settings.redis_ack_timeout_seconds == 3.0
    assert (settings.database_pool_min_size, settings.database_pool_max_size) == (2, 20)

    for updates in (
        {"worker_concurrency": 0},
        {"worker_concurrency": 257},
        {"worker_poll_seconds": 0.04},
        {"worker_poll_seconds": 60.1},
        {"redis_reclaim_after_ms": 999},
        {"redis_reclaim_after_ms": 3_600_001},
        {"redis_socket_connect_timeout_seconds": 0},
        {"redis_socket_timeout_seconds": 5},
        {"redis_socket_timeout_seconds": 301},
        {"redis_ack_timeout_seconds": 0},
        {"lease_seconds": 10, "redis_ack_timeout_seconds": 10},
        {"database_pool_min_size": 0},
        {"database_pool_max_size": 1},
        {"database_pool_max_size": 257},
        {"database_pool_min_size": 10, "database_pool_max_size": 8},
    ):
        with pytest.raises(ValidationError):
            ServiceSettings(_env_file=None, **updates)

    explicit = ServiceSettings(_env_file=None, worker_poll_seconds=2.5)
    assert explicit.worker_poll_seconds == 2.5


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("redis_socket_connect_timeout_seconds", float("nan")),
        ("redis_socket_timeout_seconds", float("inf")),
        ("redis_ack_timeout_seconds", float("nan")),
    ],
)
def test_timeout_settings_reject_nonfinite_values(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        ServiceSettings(_env_file=None, **{field: value})


def test_timeout_validator_rejects_nonfinite_constructed_value() -> None:
    settings = ServiceSettings.model_construct(
        redis_socket_connect_timeout_seconds=float("inf"),
        redis_socket_timeout_seconds=10.0,
        redis_ack_timeout_seconds=3.0,
        lease_seconds=60,
    )
    with pytest.raises(ValueError, match="redis_socket_connect_timeout_seconds"):
        settings.validate_redis_timeouts()


def test_timeout_lease_boundary_accepts_strictly_shorter_ack_deadline() -> None:
    settings = ServiceSettings(
        _env_file=None,
        lease_seconds=10,
        redis_ack_timeout_seconds=9.999,
        redis_socket_timeout_seconds=6.0,
    )
    assert settings.redis_ack_timeout_seconds == 9.999


def test_feishu_send_api_root_is_online_only_and_canonical() -> None:
    default = ServiceSettings(_env_file=None)
    assert default.feishu_send_api_root == "https://open.feishu.cn"

    custom = ServiceSettings(
        _env_file=None,
        online_tests_enabled=True,
        feishu_send_api_root="https://probe.example/feishu-openapi",
    )
    assert custom.feishu_send_api_root.endswith("/feishu-openapi")

    with pytest.raises(ValidationError, match="requires online tests"):
        ServiceSettings(
            _env_file=None,
            feishu_send_api_root="https://probe.example/feishu-openapi",
        )
    for value in (
        "http://probe.example/feishu-openapi",
        "https://user@probe.example/feishu-openapi",
        "https://probe.example/feishu-openapi/",
        "https://probe.example/../feishu-openapi",
        "https://probe.example/feishu-openapi?token=secret",
        "https://probe.example:443/feishu-openapi",
    ):
        with pytest.raises(ValidationError, match="canonical HTTPS"):
            ServiceSettings(
                _env_file=None,
                online_tests_enabled=True,
                feishu_send_api_root=value,
            )


def test_production_and_fault_injection_safety_branches() -> None:
    production = {
        "environment": Environment.PRODUCTION,
        "allow_development_token": False,
        "oidc_issuer": "https://issuer.example",
        "oidc_audience": "service",
    }
    with pytest.raises(ValidationError, match="fault injection"):
        ServiceSettings(
            _env_file=None,
            **production,
            fault_injection_enabled=True,
            fault_injection_run_id="run",
        )
    with pytest.raises(ValidationError, match="OIDC issuer and audience"):
        ServiceSettings(environment=Environment.PRODUCTION, allow_development_token=False)
    with pytest.raises(ValidationError, match="literal secret"):
        ServiceSettings(
            _env_file=None,
            **production,
            database_dsn_ref=SecretRef(uri="literal://postgresql://user:password@db/service"),
        )
    assert ServiceSettings(_env_file=None, **production).environment == Environment.PRODUCTION
    with pytest.raises(ValidationError, match="run_id"):
        ServiceSettings(_env_file=None, fault_injection_enabled=True)


def test_settings_before_validator_and_cached_accessor_paths() -> None:
    with pytest.raises(ValidationError):
        ServiceSettings.model_validate(None)

    get_settings.cache_clear()
    assert isinstance(get_settings(), ServiceSettings)
    get_settings.cache_clear()


def test_scheduler_version_selects_one_queue_and_allows_v1_drain_override() -> None:
    legacy = ServiceSettings(_env_file=None, scheduler_version=SchedulerVersion.V1)
    assert legacy.redis_stream == "trpc:inbound:v1"
    assert legacy.redis_consumer_group == "trpc-workers-v1"

    custom = ServiceSettings(
        _env_file=None,
        scheduler_version=SchedulerVersion.V1,
        redis_stream="trpc:legacy:drain",
        redis_consumer_group="legacy-drain",
    )
    assert custom.redis_stream == "trpc:legacy:drain"
    assert custom.redis_consumer_group == "legacy-drain"

    with pytest.raises(ValidationError):
        ServiceSettings(_env_file=None, scheduler_version="v3")


@pytest.mark.parametrize(
    ("scheduler_version", "redis_stream", "redis_consumer_group"),
    [
        (SchedulerVersion.V1, "trpc:session-ready:v2", "custom-v1"),
        (SchedulerVersion.V1, "custom-v1", "trpc-session-ready-v2"),
        (SchedulerVersion.V2, "trpc:inbound:v1", "custom-v2"),
        (SchedulerVersion.V2, "custom-v2", "trpc-workers-v1"),
    ],
)
def test_scheduler_version_rejects_the_other_versions_standard_transport(
    scheduler_version: SchedulerVersion,
    redis_stream: str,
    redis_consumer_group: str,
) -> None:
    with pytest.raises(ValidationError, match="scheduler"):
        ServiceSettings(
            _env_file=None,
            scheduler_version=scheduler_version,
            redis_stream=redis_stream,
            redis_consumer_group=redis_consumer_group,
        )


def test_scheduler_version_environment_selects_legacy_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRPC_SERVICE_SCHEDULER_VERSION", "v1")
    legacy = ServiceSettings(_env_file=None)
    assert legacy.scheduler_version == SchedulerVersion.V1
    assert legacy.redis_stream == "trpc:inbound:v1"
    assert legacy.redis_consumer_group == "trpc-workers-v1"

    monkeypatch.setenv("TRPC_SERVICE_REDIS_STREAM", "trpc:drain:custom")
    monkeypatch.setenv("TRPC_SERVICE_REDIS_CONSUMER_GROUP", "drain-custom")
    custom = ServiceSettings(_env_file=None)
    assert custom.redis_stream == "trpc:drain:custom"
    assert custom.redis_consumer_group == "drain-custom"


def test_media_policy_has_bounded_defaults() -> None:
    policy = MediaPolicy()
    assert policy.max_items_per_turn <= 4
    assert policy.max_bytes_per_item <= policy.max_total_bytes
    with pytest.raises(ValidationError):
        MediaPolicy(max_bytes_per_item=101 * 1024 * 1024)


def test_recursive_redaction_and_inline_credentials() -> None:
    value = redact(
        {
            "tenant_id": "tenant",
            "api_key": "unit-api-key",
            "nested": {"tool_args": {"danger": True}},
            "items": ["Bearer abc.def", {"password": "secret"}],
        }
    )
    assert value["tenant_id"] == "tenant"
    assert value["api_key"] == REDACTED
    assert value["nested"]["tool_args"] == REDACTED
    assert value["items"][0] == f"Bearer {REDACTED}"
    assert "open-sesame" not in sanitize_text("password=open-sesame")


def test_json_log_formatter_does_not_emit_sensitive_values() -> None:
    record = logging.LogRecord(
        "unit",
        logging.INFO,
        __file__,
        1,
        "token=%s",
        ("super-secret",),
        None,
    )
    record.api_key = "key-value"
    payload = json.loads(RedactingJsonFormatter().format(record))
    rendered = json.dumps(payload)
    assert "super-secret" not in rendered
    assert "key-value" not in rendered
    assert payload["api_key"] == REDACTED
