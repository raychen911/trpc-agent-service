"""Bridge governed platform Tools into tRPC-Agent-Python callables."""

from collections.abc import Awaitable, Callable, Mapping, Sequence
from urllib.parse import urlsplit

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentToolCall,
    AgentToolKind,
)
from trpc_service.agent.governance import ToolApprovalRequired
from trpc_service.agent.ports import AgentToolInvoker

TRPCToolFunction = Callable[..., Awaitable[dict[str, str]]]


class CapabilityCallSequence:
    """Allocate stable logical indexes across local Tool and dynamic MCP calls."""

    def __init__(self) -> None:
        self._next_index = 0

    def next(self) -> int:
        value = self._next_index
        self._next_index += 1
        return value


class TRPCToolBridge:
    """Expose only granted SDK functions and route calls through governance."""

    def __init__(
        self,
        context: AgentExecutionContext,
        invoker: AgentToolInvoker,
        sequence: CapabilityCallSequence,
    ) -> None:
        self._context = context
        self._invoker = invoker
        self._sequence = sequence

    def _enabled_tools(self) -> frozenset[str]:
        """Resolve visible Tool names from the unified or legacy policy shape."""

        raw_grants = self._context.config.tools.get("grants")
        if raw_grants is not None:
            if not isinstance(raw_grants, Sequence) or isinstance(raw_grants, (str, bytes)):
                raise ValueError("capability grants must be an array")
            enabled: set[str] = set()
            for raw_grant in raw_grants:
                if not isinstance(raw_grant, Mapping):
                    raise ValueError("capability grant must be an object")
                actions = raw_grant.get("actions", ())
                resources = raw_grant.get("resources", ())
                if (not isinstance(actions, Sequence) or isinstance(actions, (str, bytes))
                        or not isinstance(resources, Sequence)
                        or isinstance(resources, (str, bytes))):
                    raise ValueError("capability grant actions and resources must be arrays")
                name = raw_grant.get("name")
                expected_kind = (AgentToolKind.WORKSPACE.value if isinstance(name, str)
                                 and name.startswith("workspace.") else AgentToolKind.TOOL.value)
                if (raw_grant.get("kind") == expected_kind and isinstance(name, str)
                        and "execute" in actions):
                    enabled.add(name)
            return frozenset(enabled)

        raw = self._context.config.tools.get("allowlist", ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            raise ValueError("tool allowlist must be an array of strings")
        if any(not isinstance(name, str) or not name.strip() for name in raw):
            raise ValueError("tool allowlist must contain non-empty strings")
        return frozenset(name.strip() for name in raw)

    async def _invoke(
        self,
        name: str,
        arguments: dict[str, object],
        *,
        kind: AgentToolKind = AgentToolKind.TOOL,
        resource: str | None = None,
    ) -> dict[str, str]:
        logical_index = self._sequence.next()
        request_id = self._context.request.tenant.request_id
        try:
            result = await self._invoker.invoke(
                self._context,
                AgentToolCall(
                    call_id=f"{request_id}:{logical_index}:{name}",
                    name=name,
                    kind=kind,
                    logical_call_index=logical_index,
                    resource=resource,
                    arguments=arguments,
                ),
            )
        except ToolApprovalRequired as error:
            if error.approval is None:
                raise
            # The model only receives a single-purpose short code and cannot
            # manufacture trusted approval evidence. The Channel resolves the
            # code into a durable approval UUID after identity checks.
            return {
                "result": ("该操作需要用户确认。请停止继续调用工具，并请原请求人在当前会话回复："
                           f"确认 {error.approval.short_code}")
            }
        except PermissionError:
            # Model-generated Tool arguments are untrusted input. A rejected
            # resource is a normal Tool result, not an infrastructure failure
            # that should abort and retry the whole turn. Validation errors are
            # intentionally not swallowed because a side effect may be unknown.
            base_names = self._knowledge_base_names()
            base_hint = ("、".join(base_names) if base_names else "管理员配置的知识库")
            return {
                "result": ("工具调用未执行：参数或资源不符合当前 Agent 配置。"
                           f"请使用已授权知识库 {base_hint} 后重试。")
            }
        return {"result": result.content or ""}

    def _knowledge_base_names(self) -> tuple[str, ...]:
        """Return validated model-visible base names for safe corrective feedback."""

        raw = self._context.config.knowledge.get("knowledge_base_names", ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            return ()
        return tuple(name.strip() for name in raw if isinstance(name, str) and name.strip())

    def functions(self) -> tuple[TRPCToolFunction, ...]:
        """Return deterministic SDK callables for registered allowlisted Tools."""

        functions: list[TRPCToolFunction] = []
        if "calculate" in self._enabled_tools():

            async def calculate(expression: str) -> dict[str, str]:
                """Calculate a numeric arithmetic expression such as '(2 + 3) * 4'."""

                return await self._invoke("calculate", {"expression": expression})

            functions.append(calculate)
        if "current_time" in self._enabled_tools():

            async def current_time(timezone: str = "UTC") -> dict[str, str]:
                """Return the current date and time in an IANA timezone."""

                return await self._invoke("current_time", {"timezone": timezone})

            functions.append(current_time)
        if "http.get" in self._enabled_tools():

            async def http_get(url: str) -> dict[str, str]:
                """Read bounded JSON or text from an administrator-allowlisted HTTPS host."""

                return await self._invoke(
                    "http.get",
                    {"url": url},
                    resource=urlsplit(url).hostname,
                )

            functions.append(http_get)
        if "workspace.list" in self._enabled_tools():

            async def workspace_list(path: str = ".") -> dict[str, str]:
                """List files in this request's tenant-isolated local workspace."""

                return await self._invoke(
                    "workspace.list",
                    {"path": path},
                    kind=AgentToolKind.WORKSPACE,
                    resource=path,
                )

            functions.append(workspace_list)
        if "workspace.read" in self._enabled_tools():

            async def workspace_read(path: str) -> dict[str, str]:
                """Read a bounded UTF-8 file from this request's local workspace."""

                return await self._invoke(
                    "workspace.read",
                    {"path": path},
                    kind=AgentToolKind.WORKSPACE,
                    resource=path,
                )

            functions.append(workspace_read)
        if "knowledge.list" in self._enabled_tools():

            async def knowledge_list(knowledge_base_name: str) -> dict[str, str]:
                """List current documents in an authorized tenant knowledge base."""

                return await self._invoke(
                    "knowledge.list",
                    {"knowledge_base_name": knowledge_base_name},
                    resource=knowledge_base_name,
                )

            functions.append(knowledge_list)
        if "knowledge.search" in self._enabled_tools():

            async def knowledge_search(
                knowledge_base_name: str,
                query: str,
            ) -> dict[str, str]:
                """Search an authorized tenant knowledge base and return cited source chunks."""

                return await self._invoke(
                    "knowledge.search",
                    {
                        "knowledge_base_name": knowledge_base_name,
                        "query": query,
                    },
                    resource=knowledge_base_name,
                )

            functions.append(knowledge_search)
        return tuple(functions)
