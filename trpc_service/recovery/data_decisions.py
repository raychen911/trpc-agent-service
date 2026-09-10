from enum import StrEnum

class RecoveryDecision(StrEnum):
    RETRY_IDEMPOTENT = "retry_idempotent"
    REPLAY_RESULT = "replay_result"
    REVIEW = "review"

def decide_recovery(stage: str) -> RecoveryDecision:
    return {"pre_commit": RecoveryDecision.RETRY_IDEMPOTENT, "committed_response_lost": RecoveryDecision.REPLAY_RESULT}.get(stage, RecoveryDecision.REVIEW)

def can_fence_write(owner_generation: int, current_generation: int) -> bool:
    return owner_generation == current_generation
