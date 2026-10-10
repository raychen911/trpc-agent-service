from __future__ import annotations

import pytest

import trpc_service._cli as cli
from trpc_service.config import LocalSecretProvider, Role, SecretRef, ServiceSettings


def _settings(*, worker_ref: bool = True, worker_user: str = "trpc_worker") -> ServiceSettings:
    updates = {
        "_env_file": None,
        "database_dsn_ref": SecretRef(uri="literal://postgresql://trpc_runtime@db/service"),
    }
    if worker_ref:
        updates["worker_database_dsn_ref"] = SecretRef(
            uri=f"literal://postgresql://{worker_user}@db/service"
        )
    return ServiceSettings(**updates)


def test_worker_reference_requires_a_worker_dsn() -> None:
    with pytest.raises(ValueError, match="worker database password reference"):
        ServiceSettings(
            _env_file=None,
            worker_database_password_ref=SecretRef(uri="literal://password"),
        )


def test_runtime_role_rejects_worker_secret_and_worker_requires_one() -> None:
    settings = _settings()
    secrets = LocalSecretProvider(allow_literal=True)
    with pytest.raises(ValueError, match="forbidden for tenant runtime roles"):
        cli._database_dsn_for_role(Role.GATEWAY, settings, secrets)

    with pytest.raises(ValueError, match="required for cross-tenant"):
        cli._database_dsn_for_role(Role.WORKER, _settings(worker_ref=False), secrets)


def test_worker_dsn_username_is_pinned() -> None:
    with pytest.raises(ValueError, match="database DSN username must be trpc_worker"):
        cli._database_dsn_for_role(
            Role.SESSION_RECOVERY,
            _settings(worker_user="trpc_runtime"),
            LocalSecretProvider(allow_literal=True),
        )


def test_worker_table_privileges_preserve_append_only_evidence() -> None:
    assert cli._WORKER_TABLE_PRIVILEGES["wecom_connection_state"] == ("SELECT,INSERT,UPDATE")
    assert cli._WORKER_TABLE_PRIVILEGES["im_acceptance_evidence_events"] == ("SELECT,INSERT")
    assert cli._WORKER_TABLE_PRIVILEGES["inbound_messages"] == ("SELECT,INSERT,UPDATE,DELETE")


def test_production_rejects_literal_worker_database_references() -> None:
    with pytest.raises(ValueError, match="literal secret"):
        ServiceSettings(
            _env_file=None,
            environment="production",
            allow_development_token=False,
            oidc_issuer="https://issuer.example",
            oidc_audience="service",
            worker_database_dsn_ref=SecretRef(
                uri="literal://postgresql://trpc_worker:password@db/service"
            ),
        )


class _Connection:
    def __init__(
        self,
        *,
        bypasses_rls: bool,
        function_grants: bool = True,
        can_login: bool = True,
        owned_rls_tables: int = 0,
    ) -> None:
        self.identity = {
            "current_user": "trpc_worker" if bypasses_rls else "trpc_runtime",
            "session_user": "trpc_worker" if bypasses_rls else "trpc_runtime",
            "is_superuser": False,
            "bypasses_rls": bypasses_rls,
            "rolcanlogin": can_login,
            "owned_rls_table_count": owned_rls_tables,
            "schema_usage": True,
        }
        self.function_grants = function_grants

    async def fetchrow(self, _query: str):
        return self.identity

    async def fetchval(self, _query: str, *_args):
        return self.function_grants


class _Acquire:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


class _Repository:
    def __init__(self, connection: _Connection) -> None:
        self.pool = _Pool(connection)


@pytest.mark.asyncio
async def test_identity_requires_worker_bypassrls_and_explicit_grants() -> None:
    await cli._validate_database_identity(_Repository(_Connection(bypasses_rls=True)), Role.WORKER)
    await cli._validate_database_identity(_Repository(_Connection(bypasses_rls=False)), Role.ADMIN)
    wrong_bypass = _Connection(bypasses_rls=False)
    wrong_bypass.identity.update(current_user="trpc_worker", session_user="trpc_worker")
    with pytest.raises(RuntimeError, match="must bypass"):
        await cli._validate_database_identity(_Repository(wrong_bypass), Role.WORKER)
    with pytest.raises(RuntimeError, match="lacks EXECUTE"):
        await cli._validate_database_identity(
            _Repository(_Connection(bypasses_rls=True, function_grants=False)),
            Role.WORKER,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("can_login", "owned_rls_tables", "message"),
    (
        (False, 0, "login-enabled"),
        (True, 1, "must not own RLS tables"),
    ),
)
async def test_identity_rejects_non_login_or_rls_owner_worker_roles(
    can_login: bool, owned_rls_tables: int, message: str
) -> None:
    with pytest.raises(RuntimeError, match=message):
        await cli._validate_database_identity(
            _Repository(
                _Connection(
                    bypasses_rls=True,
                    can_login=can_login,
                    owned_rls_tables=owned_rls_tables,
                )
            ),
            Role.WORKER,
        )
