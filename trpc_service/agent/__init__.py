"""tRPC-Agent-Python integration with tenant and reliability boundaries."""

from trpc_service.agent.compat import (
    SUPPORTED_SDK_VERSION,
    CompatibilityReport,
    IncompatibleSdkError,
    probe_sdk_compatibility,
    require_sdk_compatibility,
)
from trpc_service.agent.events import (
    DEFAULT_PUBLIC_ERROR_MESSAGE,
    framework_event_to_event_data,
    framework_event_to_reply_intent,
)
from trpc_service.agent.factory import (
    AgentBuild,
    AgentConfigurationError,
    AgentFactory,
    FilterFactory,
    ModelResolver,
)
from trpc_service.agent.governance import (
    GovernanceViolationError,
    TenantGovernanceFilter,
    govern_agent_input,
    redact_sensitive_text,
)
from trpc_service.agent.models import ModelRouteError, TenantModelResolver
from trpc_service.agent.runtime import (
    AgentExecutionError,
    AgentTurnTimeoutError,
    ExecutionLimits,
    MissingFinalResponseError,
    TenantAgentRunner,
    TurnResult,
)

__all__ = [
    "DEFAULT_PUBLIC_ERROR_MESSAGE",
    "SUPPORTED_SDK_VERSION",
    "AgentBuild",
    "AgentConfigurationError",
    "AgentExecutionError",
    "AgentFactory",
    "AgentTurnTimeoutError",
    "CompatibilityReport",
    "ExecutionLimits",
    "FilterFactory",
    "GovernanceViolationError",
    "IncompatibleSdkError",
    "MissingFinalResponseError",
    "ModelResolver",
    "ModelRouteError",
    "TenantAgentRunner",
    "TenantGovernanceFilter",
    "TenantModelResolver",
    "TurnResult",
    "framework_event_to_event_data",
    "framework_event_to_reply_intent",
    "govern_agent_input",
    "probe_sdk_compatibility",
    "redact_sensitive_text",
    "require_sdk_compatibility",
]
