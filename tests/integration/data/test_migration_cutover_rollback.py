import inspect
from trpc_service.storage.migration import MigrationCoordinator

def test_migration_coordinator_exposes_safe_rollback_and_forward_repair() -> None:
    assert inspect.iscoroutinefunction(MigrationCoordinator.rollback)
    assert inspect.iscoroutinefunction(MigrationCoordinator.require_forward_repair)
