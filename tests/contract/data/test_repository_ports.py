from __future__ import annotations

import inspect

from trpc_service.storage import contracts


def test_data_scope_and_unit_of_work_are_required_ports() -> None:
    assert hasattr(contracts, "DataScope")
    assert hasattr(contracts, "DataUnitOfWork")
    assert hasattr(contracts, "DataUnitOfWorkFactory")


def test_all_data_ports_are_async_and_require_scope() -> None:
    names = (
        "SessionEventRepository", "MemoryRepository", "SummaryRepository",
        "ArtifactRepository", "KnowledgeRepository", "MigrationRepository",
        "ObjectStorePort", "VectorStorePort",
    )
    for name in names:
        port = getattr(contracts, name, None)
        assert port is not None
        methods = [value for key, value in vars(port).items() if not key.startswith("_")]
        assert methods
        for method in methods:
            if callable(method):
                assert inspect.iscoroutinefunction(method)
                assert "scope" in inspect.signature(method).parameters
