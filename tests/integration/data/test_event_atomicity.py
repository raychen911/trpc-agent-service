import inspect

from trpc_service.storage.postgres import data_repositories


def test_postgres_event_repository_declares_audit_atomicity() -> None:
    repository = getattr(data_repositories, "PostgresSessionEventRepository", None)
    assert repository is not None
    assert "audit" in inspect.signature(repository).parameters
