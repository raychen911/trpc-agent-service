"""Behavioral tests for administrative authentication and operator commands."""

from __future__ import annotations

import base64
import json
import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from trpc_service.admin import auth
from trpc_service.admin.auth import (
    AdminAuthenticationError,
    AdminPrincipal,
    OIDCAuthenticationError,
    authenticate,
    authorize,
)


def b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def test_api_key_configuration_and_authorization(monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "root-key")
    monkeypatch.setenv("ADMIN_ROLE", "operator")
    monkeypatch.setenv("ADMIN_API_KEYS", "viewer-key=viewer:t-a|t-b,broken,ops-key=operator:t-c")
    assert authenticate(api_key="root-key").subject == "env-admin"
    viewer = authenticate(api_key="viewer-key")
    assert viewer.tenants == frozenset({"t-a", "t-b"})
    authorize(viewer, "t-a", {"viewer"})
    with pytest.raises(AdminAuthenticationError):
        authorize(viewer, "t-c", {"viewer"})
    with pytest.raises(AdminAuthenticationError):
        authorize(viewer, "t-a", {"operator"})
    assert AdminPrincipal("x", "platform_admin").can_access("any")
    assert not AdminPrincipal("x", "viewer").can_access("any")
    with pytest.raises(AdminAuthenticationError):
        authenticate(api_key="wrong")
    with pytest.raises(AdminAuthenticationError):
        authenticate()
    monkeypatch.setenv("ADMIN_API_KEYS", "")
    assert authenticate(authorization="Bearer root-key").subject == "env-admin"


def test_oidc_decoding_numeric_claims_and_jwks_cache(monkeypatch):
    with pytest.raises(OIDCAuthenticationError):
        auth._b64url_decode("!")
    with pytest.raises(OIDCAuthenticationError):
        auth._b64url_decode("")
    assert auth._numeric_claim({"exp": 4}, "exp", required=True) == 4.0
    assert auth._numeric_claim({}, "nbf") is None
    with pytest.raises(OIDCAuthenticationError):
        auth._numeric_claim({}, "exp", required=True)
    with pytest.raises(OIDCAuthenticationError):
        auth._numeric_claim({"exp": True}, "exp")

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, _limit):
            return b'{"keys": []}'

    auth._JWKS_CACHE.clear()
    monkeypatch.setattr(auth, "urlopen", lambda request, timeout: Response())
    first = auth._load_jwks("https://issuer.example/jwks")
    assert first == {"keys": []}
    assert auth._load_jwks("https://issuer.example/jwks") is first

    class BadResponse(Response):
        def read(self, _limit):
            return b"[]"

    auth._JWKS_CACHE.clear()
    monkeypatch.setattr(auth, "urlopen", lambda request, timeout: BadResponse())
    with pytest.raises(OIDCAuthenticationError, match="invalid"):
        auth._load_jwks("https://issuer.example/bad")


def make_oidc_token(monkeypatch, claims):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding, rsa

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = private.public_key().public_numbers()
    header = {"alg": "RS256", "kid": "key-1", "typ": "JWT"}
    encoded_header = b64(json.dumps(header, separators=(",", ":")).encode())
    encoded_claims = b64(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{encoded_header}.{encoded_claims}".encode("ascii")
    signature = private.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    token = f"{encoded_header}.{encoded_claims}.{b64(signature)}"
    monkeypatch.setattr(
        auth,
        "_load_jwks",
        lambda url: {"keys": [{"kid": "key-1", "kty": "RSA", "e": b64(public.e.to_bytes(3, "big")), "n": b64(public.n.to_bytes((public.n.bit_length() + 7) // 8, "big"))}]},
    )
    return token


def test_oidc_principal_verifies_signature_roles_and_tenant_scope(monkeypatch):
    import time

    monkeypatch.setenv("OIDC_ISSUER", "https://issuer.example")
    monkeypatch.setenv("OIDC_AUDIENCE", "agent-api")
    claims = {
        "iss": "https://issuer.example",
        "aud": ["other", "agent-api"],
        "sub": "operator-1",
        "exp": time.time() + 300,
        "nbf": time.time() - 1,
        "roles": ["viewer", "operator"],
        "tenants": "tenant-a, tenant-b",
    }
    token = make_oidc_token(monkeypatch, claims)
    principal = authenticate(authorization=f"Bearer {token}")
    assert principal.subject == "operator-1"
    assert principal.role == "operator"
    assert principal.tenants == frozenset({"tenant-a", "tenant-b"})

    for bad_claims, message in (
        ({**claims, "iss": "wrong"}, "issuer"),
        ({**claims, "aud": "wrong"}, "audience"),
        ({**claims, "exp": 0}, "expired"),
        ({**claims, "nbf": time.time() + 300}, "active"),
        ({**claims, "sub": ""}, "subject"),
        ({**claims, "roles": [1]}, "role"),
        ({**claims, "roles": ["unknown"]}, "allowed"),
        ({**claims, "tenants": [1]}, "tenant"),
    ):
        bad_token = make_oidc_token(monkeypatch, bad_claims)
        with pytest.raises(OIDCAuthenticationError, match=message):
            auth._oidc_principal(bad_token)

    with pytest.raises(OIDCAuthenticationError):
        auth._oidc_principal("not-a-jwt")


@dataclass
class FakeRuntime:
    storage: object
    telemetry: object
    storage_manager: object | None = None


class FakeClose:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeRepository:
    def __init__(self, configs):
        self.configs = configs
        self.closed = False

    def all_active(self):
        return self.configs

    def close(self):
        self.closed = True


def cli_runtime():
    repository = FakeRepository([SimpleNamespace(tenant_id="tenant-a")])
    admin = SimpleNamespace(tenants=SimpleNamespace(repository=repository))
    gateway = SimpleNamespace(storage=FakeClose(), telemetry=FakeClose(), storage_manager=None)
    return gateway, admin


def test_cli_maintenance_commands_run_once_and_cleanup(monkeypatch, capsys):
    import trpc_service._cli as cli

    gateway, admin = cli_runtime()
    class Manager(FakeClose):
        def get(self, config):
            return storage

    manager = Manager()
    storage = SimpleNamespace(
        session_mailbox_v2=SimpleNamespace(
            sweep_expired_leases=lambda **_: 1,
            schedule_retries=lambda **_: 2,
            reconcile_sessions=lambda **_: 3,
        )
    )
    monkeypatch.setattr(cli, "_create_runtime", lambda: (gateway, admin))
    monkeypatch.setattr(cli, "TenantStorageManager", lambda: manager)
    monkeypatch.setattr(cli, "replay_compensations", lambda *args, **kwargs: 4)
    cli.run_compensate(once=True, limit=2)
    cli.run_mailbox_maintenance(once=True, limit=2)
    assert '"processed"' in capsys.readouterr().out
    assert manager.closed and gateway.storage.closed and gateway.telemetry.closed
    assert admin.tenants.repository.closed


def test_cli_replay_and_main_dispatch(monkeypatch, capsys):
    import trpc_service._cli as cli

    class Store:
        def __init__(self):
            self.audit = SimpleNamespace(append=lambda record: setattr(self, "audit_record", record))
            from trpc_service.storage.base import CompensationTask
            from trpc_service.storage.durable import OutboxRecord

            self.compensation = SimpleNamespace(replay=lambda *args, **kwargs: CompensationTask("task", "tenant-a", "op", {}))
            self.inbox_outbox = SimpleNamespace(replay_outbox=lambda *args: OutboxRecord("event", "tenant-a", "topic", "aggregate", {}))

    store = Store()
    config = SimpleNamespace(tenant_id="tenant-a")
    gateway, admin = cli_runtime()
    admin.tenants.get_tenant = lambda tenant_id: config
    manager = SimpleNamespace(get=lambda _: store, close=lambda: None)
    monkeypatch.setattr(cli, "_create_runtime", lambda: (gateway, admin))
    monkeypatch.setattr(cli, "TenantStorageManager", lambda: manager)
    cli.run_replay("outbox", "tenant-a", "event-1", "operator")
    cli.run_replay("compensation", "tenant-a", "task-1", "operator")
    with pytest.raises(SystemExit):
        cli.run_replay("outbox", "", "", "")
    with pytest.raises(ValueError):
        cli.run_replay("invalid", "tenant-a", "id", "operator")
    assert "requeued" in capsys.readouterr().out

    called = []
    monkeypatch.setattr(cli, "run_compensate", lambda *args: called.append("compensate"))
    monkeypatch.setattr(sys, "argv", ["trpc-agent", "compensate", "--once"])
    cli.main()
    assert called == ["compensate"]


def test_cli_demo_reports_model_failures(monkeypatch, capsys):
    import trpc_service._cli as cli

    class Gateway:
        storage_manager = None
        storage = FakeClose()
        telemetry = FakeClose()

        def dispatch(self, message):
            return "session", [SimpleNamespace(content="answer")], "response"

    monkeypatch.setattr(cli, "_create_runtime", lambda: (Gateway(), None))
    cli.run_demo()
    assert "answer" in capsys.readouterr().out

    from trpc_service.agent.model_client import ModelClientError

    class FailingGateway(Gateway):
        def dispatch(self, message):
            raise ModelClientError("provider down")

    monkeypatch.setattr(cli, "_create_runtime", lambda: (FailingGateway(), None))
    with pytest.raises(SystemExit) as exc:
        cli.run_demo()
    assert exc.value.code == 2
