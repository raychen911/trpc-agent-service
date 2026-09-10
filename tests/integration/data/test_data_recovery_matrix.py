from trpc_service.recovery.data_decisions import decide_recovery

def test_non_idempotent_unknown_enters_review():
    assert decide_recovery("outcome_unknown").value == "review"
