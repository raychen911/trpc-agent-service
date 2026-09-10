def test_recovery_repository_supports_generation_claim_and_review():
    from trpc_service.recovery.repository import InMemoryDataRecoveryRepository
    assert all(hasattr(InMemoryDataRecoveryRepository,name) for name in ("create_once","claim","mark_complete","mark_review"))
