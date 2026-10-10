from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import jwt
import pytest
from opentelemetry.sdk.trace import Event
from opentelemetry.trace import Status, StatusCode

from tests.conftest import envelope, repository, tenant_config
from trpc_service.config.secrets import (
    LocalSecretProvider,
    SecretRef,
    SecretResolutionError,
)
from trpc_service.config.settings import Environment, ServiceSettings, get_settings
from trpc_service.log.configure import RedactingJsonFormatter, configure_logging
from trpc_service.metrics.privacy import (
    PrivacySpanProcessor,
    extract_trace_context,
    inject_trace_headers,
    sanitize_attributes,
)
from trpc_service.metrics.setup import configure_tracing
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.services import (
    ProfileServiceFactory,
    RegisteredTenantServiceBundle,
    TenantDataServices,
)
from trpc_service.storage.vector import PgVectorKnowledgeStore
from trpc_service.tenant.auth import (
    AuthenticationError,
    AuthorizationError,
    DevelopmentAuthorizer,
    OidcAuthorizer,
    Principal,
    Role,
    require_role,
)
from trpc_service.workspace.manager import WorkspaceManager


class Delegate:
    def __init__(self) -> None:
        self.started = None
        self.ended = None
        self.stopped = False

    def on_start(self, span, parent_context=None):
        self.started = span

    def on_end(self, span):
        self.ended = span

    def shutdown(self):
        self.stopped = True

    def force_flush(self, timeout_millis=30_000):
        return timeout_millis == 123


def test_privacy_processor_and_trace_carriers() -> None:
    assert dict(sanitize_attributes(None)) == {}
    assert sanitize_attributes({"llm.request": "secret", "safe": 2}) == {
        "llm.request": "[REDACTED]",
        "safe": 2,
    }
    sdk_attributes = {
        "trpc.python.agent.llm_request": "request-canary",
        "trpc.python.agent.state.begin": "state-canary",
        "trpc.python.agent.tool_call_args": "tool-canary",
    }
    assert all(value == "[REDACTED]" for value in sanitize_attributes(sdk_attributes).values())
    span = SimpleNamespace(
        attributes={"tool.arguments": "canary", "latency": 1},
        status=Status(StatusCode.ERROR, "password=status-canary"),
        events=(
            Event(
                "exception",
                {
                    "exception.type": "RuntimeError",
                    "exception.message": "password=event-canary",
                    "exception.stacktrace": "event-canary",
                },
            ),
        ),
    )
    delegate = Delegate()
    processor = PrivacySpanProcessor(delegate)
    processor.on_start(span)
    processor.on_end(span)
    assert delegate.started is span
    assert delegate.ended.attributes["tool.arguments"] == "[REDACTED]"
    assert delegate.ended.status.description is None
    assert delegate.ended.events[0].attributes["exception.message"] == "[REDACTED]"
    assert delegate.ended.events[0].attributes["exception.type"] == "RuntimeError"
    assert processor.force_flush(123)
    processor.shutdown()
    assert delegate.stopped

    carrier: dict[str, str] = {}
    inject_trace_headers(carrier)
    assert extract_trace_context(carrier) is not None


def test_configure_tracing_wires_private_and_explicit_content(monkeypatch) -> None:
    added = []
    exported = []
    monkeypatch.setattr(
        "trpc_service.metrics.setup.OTLPSpanExporter",
        lambda **kwargs: exported.append(kwargs) or object(),
    )

    class Batch:
        def __init__(self, exporter):
            self.exporter = exporter

    monkeypatch.setattr("trpc_service.metrics.setup.BatchSpanProcessor", Batch)
    monkeypatch.setattr(
        "trpc_service.metrics.setup.TracerProvider.add_span_processor",
        lambda self, processor: added.append(processor),
    )
    monkeypatch.setattr(
        "trpc_service.metrics.setup.trace.set_tracer_provider", lambda provider: None
    )

    configure_tracing(service_name="gateway", endpoint=None)
    configure_tracing(service_name="worker", endpoint="http://otel:4317")
    configure_tracing(service_name="dev", endpoint="https://otel:4317", capture_content=True)
    assert exported == [
        {"endpoint": "http://otel:4317", "insecure": True},
        {"endpoint": "https://otel:4317", "insecure": False},
    ]
    assert isinstance(added[0], PrivacySpanProcessor)
    assert isinstance(added[1], Batch)


def test_json_logging_redacts_extra_and_exception() -> None:
    formatter = RedactingJsonFormatter()
    try:
        raise RuntimeError("password=canary")
    except RuntimeError:
        record = logging.LogRecord(
            "unit",
            logging.ERROR,
            __file__,
            1,
            "token=canary",
            (),
            __import__("sys").exc_info(),
        )
    record.api_key = "canary"
    payload = json.loads(formatter.format(record))
    assert "canary" not in json.dumps(payload)
    assert payload["api_key"] == "[REDACTED]"
    configure_logging("debug")
    assert logging.getLogger().level == logging.DEBUG


def test_settings_and_secret_negative_paths(monkeypatch, tmp_path) -> None:
    with pytest.raises(ValueError, match="scheme"):
        SecretRef(uri="vault://item")
    with pytest.raises(ValueError, match="empty"):
        SecretRef(uri="env://")
    provider = LocalSecretProvider()
    with pytest.raises(SecretResolutionError, match="not set"):
        provider.resolve(SecretRef(uri="env://MISSING_UNIT_SECRET"))
    empty = tmp_path / "empty"
    empty.write_text("\n", encoding="utf-8")
    with pytest.raises(SecretResolutionError, match="empty"):
        provider.resolve(SecretRef(uri=empty.as_uri()))
    with pytest.raises(SecretResolutionError, match="disabled"):
        provider.resolve(SecretRef(uri="literal://secret"))
    assert (
        LocalSecretProvider(allow_literal=True).resolve(SecretRef(uri="literal://hello")) == "hello"
    )

    common = {
        "environment": Environment.PRODUCTION,
        "allow_development_token": False,
        "oidc_issuer": "https://issuer",
        "oidc_audience": "audience",
    }
    ServiceSettings(**common)
    with pytest.raises(ValueError, match="authentication"):
        ServiceSettings(environment="production", oidc_issuer="x", oidc_audience="y")
    with pytest.raises(ValueError, match="OIDC"):
        ServiceSettings(environment="production", allow_development_token=False)
    with pytest.raises(ValueError, match="content"):
        ServiceSettings(**common, capture_content=True)
    with pytest.raises(ValueError, match="literal"):
        ServiceSettings(**common, database_dsn_ref=SecretRef(uri="literal://dsn"))
    get_settings.cache_clear()
    monkeypatch.setenv("TRPC_SERVICE_ENVIRONMENT", "test")
    assert get_settings().environment == Environment.TEST
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_oidc_development_auth_and_rbac(monkeypatch) -> None:
    with pytest.raises(ValueError, match="allow-list"):
        OidcAuthorizer(issuer="https://issuer", audience="aud", algorithms=("none",))
    authorizer = OidcAuthorizer(issuer="https://issuer/", audience="aud")
    monkeypatch.setattr(
        authorizer._jwks,
        "get_signing_key_from_jwt",
        lambda token: SimpleNamespace(key="public"),
    )
    monkeypatch.setattr(
        jwt,
        "decode",
        lambda *args, **kwargs: {
            "sub": "user",
            "roles": ["tenant_admin", "ignored"],
            "tenant_ids": ["tenant-a"],
        },
    )
    principal = await authorizer.authenticate("jwt")
    assert principal.roles == frozenset({Role.TENANT_ADMIN})
    require_role(principal, Role.TENANT_ADMIN, tenant_id="tenant-a")
    with pytest.raises(AuthorizationError, match="scope"):
        require_role(principal, Role.TENANT_ADMIN, tenant_id="tenant-b")
    with pytest.raises(AuthorizationError, match="role"):
        require_role(principal, Role.AUDITOR, tenant_id="tenant-a")
    require_role(
        Principal(subject="root", roles=frozenset({Role.PLATFORM_ADMIN})),
        Role.AUDITOR,
        tenant_id="any",
    )

    monkeypatch.setattr(jwt, "decode", lambda *args, **kwargs: {"sub": "x", "roles": "bad"})
    with pytest.raises(AuthenticationError, match="roles"):
        await authorizer.authenticate("jwt")
    monkeypatch.setattr(
        jwt,
        "decode",
        lambda *args, **kwargs: {"sub": "x", "roles": [], "tenant_ids": "bad"},
    )
    with pytest.raises(AuthenticationError, match="scope"):
        await authorizer.authenticate("jwt")
    monkeypatch.setattr(
        jwt, "decode", lambda *args, **kwargs: (_ for _ in ()).throw(jwt.DecodeError())
    )
    with pytest.raises(AuthenticationError, match="invalid"):
        await authorizer.authenticate("jwt")

    secrets = LocalSecretProvider(allow_literal=True)
    ref = SecretRef(uri="literal://dev-token")
    with pytest.raises(ValueError, match="disabled"):
        DevelopmentAuthorizer(secrets, ref, enabled=False)
    development = DevelopmentAuthorizer(secrets, ref, enabled=True)
    with pytest.raises(AuthenticationError):
        await development.authenticate("wrong")
    assert Role.PLATFORM_ADMIN in (await development.authenticate("dev-token")).roles


@pytest.mark.asyncio
async def test_profile_factory_vector_and_workspace(tmp_path) -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"s" * 32)
    accepted = await runtime.accept("binding-unpredictable-a", envelope())
    config = tenant_config()
    services = TenantDataServices(*(object() for _ in range(6)))
    registration = RegisteredTenantServiceBundle(selection=config.storage, services=services)
    factory = ProfileServiceFactory({(config.tenant_id, config.storage.profile_id): registration})
    assert await factory.for_context(accepted.context, config) is services
    with pytest.raises(ValueError, match="another tenant"):
        await factory.for_context(
            accepted.context.model_copy(update={"tenant_id": "other"}), config
        )
    with pytest.raises(LookupError, match="unavailable"):
        await ProfileServiceFactory({}).for_context(accepted.context, config)

    class Connection:
        def __init__(self):
            self.calls = []

        def transaction(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def execute(self, *args):
            self.calls.append(args)

    connection = Connection()

    class Acquire:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, *args):
            return None

    pool = SimpleNamespace(acquire=lambda: Acquire())
    vector = PgVectorKnowledgeStore(pool, dimension=1536)
    with pytest.raises(ValueError, match="dimension"):
        await vector.upsert("tenant", "item", [1.0], {})
    await vector.upsert(
        "tenant",
        "item",
        [1.0] * 1536,
        {"chunk_id": "c", "profile_id": "profile"},
    )
    assert connection.calls[-1][3] == "c"

    with pytest.raises(ValueError, match="32"):
        WorkspaceManager(tmp_path, key=b"short")
    workspace = WorkspaceManager(tmp_path, key=b"k" * 32).prepare("tenant", "session")
    assert (workspace / "inputs").is_dir() and (workspace / "outputs").is_dir()
