def test_worker_restart_uses_shared_data_boundary():
    from trpc_service.storage.data_service import AuditedDataAccess
    assert AuditedDataAccess
