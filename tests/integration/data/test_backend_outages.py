from trpc_service.storage.contracts import StateBackendUnavailable

def test_outage_is_mapped_to_stable_error() -> None:
    error=StateBackendUnavailable("safe")
    assert error.code == "state_backend_unavailable" and error.retryable
