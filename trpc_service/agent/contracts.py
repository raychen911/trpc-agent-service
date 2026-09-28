"""Provider-neutral values exchanged through the Agent execution chain."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from trpc_service.channels.contracts import (
    ChannelBindingConfig,
    IncomingMessage,
    MessageKind,
)
from trpc_service.storage.types import (
    KnowledgeHit,
    MemoryHit,
    OutboxMessage,
    SessionEvent,
    SessionSnapshot,
    SessionSummary,
)
from trpc_service.tenant.context import TenantContext
from trpc_service.workspace.contracts import WorkspaceHandle


class PolicyAction(StrEnum):
    """Governance outcomes understood by the core execution pipeline."""

    ALLOW = "allow"
    DENY = "deny"
    REVIEW = "review"


class AgentExecutionOutcome(StrEnum):
    """Durable terminal outcomes returned by the execution coordinator."""

    SUCCEEDED = "succeeded"
    DENIED = "denied"
    REVIEW_REQUIRED = "review_required"


class AgentTaskStatus(StrEnum):
    """Durable dispatch states shared by every Gateway and Worker node."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE_FAILED = "retryable_failed"
    PERMANENT_FAILED = "permanent_failed"


class AgentToolKind(StrEnum):
    """External capability kinds routed through one governance boundary."""

    TOOL = "tool"
    MCP = "mcp"
    SKILL = "skill"
    WORKSPACE = "workspace"


@dataclass(frozen=True, slots=True)
class AgentInputArtifact:
    """Tenant-resolved binary input made available to the request-scoped Runner."""

    artifact_id: str
    media_type: str
    filename: str
    content: bytes

    def __post_init__(self) -> None:
        if not self.artifact_id.strip() or not self.filename.strip():
            raise ValueError("input Artifact identity and filename cannot be empty")
        if not self.media_type.startswith("image/"):
            raise ValueError("the multimodal Runner currently accepts image Artifacts only")
        if not self.content:
            raise ValueError("input Artifact content cannot be empty")


@dataclass(frozen=True, slots=True)
class AgentExecutionRequest:
    """One normalized request ready for a stateless Agent Worker."""

    tenant: TenantContext
    session_id: str
    incoming: IncomingMessage
    channel: ChannelBindingConfig
    attempt: int = 1
    trace_context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Tenant identity must be fixed before any storage, Tool or model call.
        if self.channel.tenant_id != self.tenant.tenant_id:
            raise ValueError("channel tenant does not match execution tenant")
        if self.channel.agent_app_id != self.tenant.agent_app_id:
            raise ValueError("channel Agent does not match execution Agent")
        if self.session_id.strip() == "":
            raise ValueError("session id cannot be empty")
        if self.attempt < 1:
            raise ValueError("execution attempt must be positive")
        allowed_trace_fields = {"traceparent", "tracestate"}
        if (set(self.trace_context) - allowed_trace_fields
                or any(not isinstance(value, str) or len(value) > 512
                       for value in self.trace_context.values())):
            # Baggage is intentionally excluded because arbitrary values can
            # carry user data or credentials across the durable queue.
            raise ValueError("execution trace context contains unsafe fields")


@dataclass(frozen=True, slots=True)
class AgentTaskClaim:
    """One durable Agent request leased to a stateless Worker node."""

    task_id: str
    request: AgentExecutionRequest
    status: AgentTaskStatus
    attempt_count: int
    fencing_token: int = 1

    def __post_init__(self) -> None:
        if self.task_id.strip() == "":
            raise ValueError("Agent task ID cannot be empty")
        if self.status is not AgentTaskStatus.RUNNING:
            raise ValueError("a claimed Agent task must be running")
        if self.attempt_count < 1:
            raise ValueError("Agent task attempt count must be positive")
        if self.fencing_token < 1:
            raise ValueError("Agent task fencing token must be positive")


@dataclass(frozen=True, slots=True)
class AgentRuntimeConfig:
    """Versioned Agent configuration loaded once at execution start."""

    config_version: int
    runner_name: str
    application: Mapping[str, object] = field(default_factory=dict)
    model: Mapping[str, object] = field(default_factory=dict)
    tools: Mapping[str, object] = field(default_factory=dict)
    knowledge: Mapping[str, object] = field(default_factory=dict)
    policy: Mapping[str, object] = field(default_factory=dict)
    backends: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.config_version < 1:
            raise ValueError("config version must be positive")
        if self.runner_name.strip() == "":
            raise ValueError("runner name cannot be empty")


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Tenant governance decision made before Agent execution."""

    action: PolicyAction
    reason: str | None = None
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentExecutionReceipt:
    """Durable outcome returned only after Session and Outbox commit."""

    session: SessionSnapshot
    committed_outbox_ids: tuple[str, ...] = ()
    replayed: bool = False
    outcome: AgentExecutionOutcome = AgentExecutionOutcome.SUCCEEDED
    policy: PolicyDecision | None = None

    def __post_init__(self) -> None:
        expected_action = {
            AgentExecutionOutcome.SUCCEEDED: PolicyAction.ALLOW,
            AgentExecutionOutcome.DENIED: PolicyAction.DENY,
            AgentExecutionOutcome.REVIEW_REQUIRED: PolicyAction.REVIEW,
        }[self.outcome]
        if self.policy is None:
            if self.outcome is not AgentExecutionOutcome.SUCCEEDED:
                raise ValueError("a rejected execution receipt requires its policy decision")
        elif self.policy.action is not expected_action:
            raise ValueError("execution outcome does not match its policy decision")


@dataclass(frozen=True, slots=True)
class AgentExecutionClaim:
    """Durable Inbox claim or previously completed result for one request."""

    claim_id: str
    request_id: str | None = None
    fencing_token: int | None = None
    session_version: int | None = None
    completed: AgentExecutionReceipt | None = None
    request: AgentExecutionRequest | None = None
    runtime_config: AgentRuntimeConfig | None = None

    def __post_init__(self) -> None:
        if self.claim_id.strip() == "":
            raise ValueError("execution claim id cannot be empty")
        if self.fencing_token is not None and self.fencing_token < 1:
            raise ValueError("execution fencing token must be positive")
        if self.session_version is not None and self.session_version < 0:
            raise ValueError("claimed Session version cannot be negative")


@dataclass(frozen=True, slots=True)
class AgentExecutionContext:
    """Shared state assembled for one Runner invocation."""

    request: AgentExecutionRequest
    config: AgentRuntimeConfig
    policy: PolicyDecision
    claim: AgentExecutionClaim
    session: SessionSnapshot | None = None
    summary: SessionSummary | None = None
    memories: tuple[MemoryHit, ...] = ()
    knowledge: tuple[KnowledgeHit, ...] = ()
    input_artifacts: tuple[AgentInputArtifact, ...] = ()
    workspace: WorkspaceHandle | None = None
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentToolCall:
    """One governed capability request with a stable replay position."""

    call_id: str
    name: str
    kind: AgentToolKind
    logical_call_index: int
    action: str = "execute"
    resource: str | None = None
    arguments: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.call_id.strip() == "" or self.name.strip() == "":
            raise ValueError("tool call id and name cannot be empty")
        if self.logical_call_index < 0:
            raise ValueError("logical tool call index cannot be negative")
        if self.action.strip() == "":
            raise ValueError("capability action cannot be empty")
        if self.resource is not None and self.resource.strip() == "":
            raise ValueError("capability resource cannot be empty")


@dataclass(frozen=True, slots=True)
class AgentToolResult:
    """Provider-neutral result returned by a governed Tool or MCP call."""

    call_id: str
    content: str | None = None
    artifact_refs: tuple[str, ...] = ()
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentReply:
    """Provider-neutral reply produced by a concrete Agent Runner."""

    kind: MessageKind
    text: str | None = None
    artifact_refs: tuple[str, ...] = ()
    attributes: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AgentUsage:
    """Provider-reported model usage retained for metrics and cost audit."""

    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated_cost: float = 0.0

    def __post_init__(self) -> None:
        if min(self.input_tokens, self.output_tokens, self.total_tokens) < 0:
            raise ValueError("model token usage cannot be negative")
        if self.estimated_cost < 0:
            raise ValueError("estimated model cost cannot be negative")


@dataclass(frozen=True, slots=True)
class AgentTaskSnapshot:
    """Durable task state readable from any Gateway node."""

    task_id: str
    external_message_id: str
    status: AgentTaskStatus
    attempt_count: int
    replies: tuple[AgentReply, ...] = ()
    safe_error: str | None = None
    delivery_status: str | None = None
    delivery_error: str | None = None

    def __post_init__(self) -> None:
        if not self.task_id.strip() or not self.external_message_id.strip():
            raise ValueError("Agent task and external message IDs cannot be empty")
        if self.attempt_count < 0:
            raise ValueError("Agent task attempt count cannot be negative")
        if self.delivery_status not in {None, "PENDING", "DELIVERED", "UNKNOWN", "DEAD_LETTER"}:
            raise ValueError("Agent task delivery status is invalid")


@dataclass(frozen=True, slots=True)
class AgentRunResult:
    """Runner output awaiting one atomic Session and Outbox commit."""

    replies: tuple[AgentReply, ...] = ()
    events: tuple[SessionEvent, ...] = ()
    state: Mapping[str, object] = field(default_factory=dict)
    derived_outbox: tuple[OutboxMessage, ...] = ()
    runner_checkpoint_id: str | None = None
    usage: AgentUsage = field(default_factory=AgentUsage)
