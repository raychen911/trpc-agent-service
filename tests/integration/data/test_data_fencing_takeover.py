from trpc_service.recovery.data_decisions import can_fence_write

def test_old_generation_cannot_write():
    assert not can_fence_write(1,2)
    assert can_fence_write(2,2)
