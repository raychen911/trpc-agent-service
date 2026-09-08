from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token

_tenant_id: ContextVar[str | None] = ContextVar("database_tenant_id", default=None)


def current_database_tenant() -> str | None:
    return _tenant_id.get()


@contextmanager
def tenant_database_scope(tenant_id: str) -> Iterator[None]:
    """Set the PostgreSQL RLS tenant for all sessions opened in this context."""

    token: Token[str | None] = _tenant_id.set(tenant_id)
    try:
        yield
    finally:
        _tenant_id.reset(token)
