from trpc_service.storage.postgres import data_repositories


def test_summary_repository_locks_event_watermark() -> None:
    repository = getattr(data_repositories, "PostgresSummaryRepository", None)
    assert repository is not None
    assert getattr(repository, "locks_event_stream", False) is True
