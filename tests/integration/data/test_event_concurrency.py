from trpc_service.storage.postgres import data_repositories


def test_postgres_event_repository_has_first_write_fence_hook() -> None:
    repository = getattr(data_repositories, "PostgresSessionEventRepository", None)
    assert repository is not None
    assert hasattr(repository, "mark_first_authoritative_write")
