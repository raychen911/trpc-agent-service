import pytest
from trpc_service.storage.contracts import ForwardRepairRequired, MigrationConflict
from trpc_service.storage.data_models import MigrationState
from trpc_service.storage.sync import transition_migration

def test_migration_transitions_and_rollback_boundary() -> None:
    state=MigrationState(tenant_id="tenant-alpha",stream="s")
    state=transition_migration(state,"PAUSING",expected_generation=1)
    assert state.generation == 2
    with pytest.raises(MigrationConflict): transition_migration(state,"CUTOVER_READY",expected_generation=1)
    state=state.model_copy(update={"state":"ACTIVE_FORWARD_ONLY","authority":"POSTGRES","rollback_eligible":False})
    with pytest.raises(ForwardRepairRequired): transition_migration(state,"ROLLED_BACK",expected_generation=state.generation)
