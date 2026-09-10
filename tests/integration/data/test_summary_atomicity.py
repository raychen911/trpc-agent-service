from trpc_service.storage.postgres import data_repositories


def test_postgres_summary_repository_exists_for_atomic_uow() -> None:
    assert hasattr(data_repositories, "PostgresSummaryRepository")
