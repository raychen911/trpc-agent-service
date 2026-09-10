from dataclasses import dataclass

from trpc_service.observability import taxonomy


@dataclass(frozen=True, slots=True)
class OperationalEvent:
    """Bounded operational log envelope (distinct from the formal audit log).

    ``error_type`` MUST come from the central taxonomy domain; raw exception
    text, secrets or free-form values are rejected at this boundary.
    """

    component: str
    operation: str
    error_type: str
    retryable: bool
    trace_digest: str

    def __post_init__(self) -> None:
        taxonomy.validate_component(self.component)
        taxonomy.validate_error_type(self.error_type)
        if not self.trace_digest.startswith("sha256:"):
            raise ValueError("trace_digest must be a sha256:<hex> reference")

    def to_dict(self) -> dict[str, object]:
        return {"component":self.component,"operation":self.operation,"error_type":self.error_type,"retryable":self.retryable,"trace_digest":self.trace_digest}
