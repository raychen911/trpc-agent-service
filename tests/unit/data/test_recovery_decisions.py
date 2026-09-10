from trpc_service.recovery.data_decisions import RecoveryDecision, decide_recovery

def test_recovery_decision_matrix_is_idempotent_and_fail_closed() -> None:
    assert decide_recovery("pre_commit") is RecoveryDecision.RETRY_IDEMPOTENT
    assert decide_recovery("committed_response_lost") is RecoveryDecision.REPLAY_RESULT
    assert decide_recovery("outcome_unknown") is RecoveryDecision.REVIEW
