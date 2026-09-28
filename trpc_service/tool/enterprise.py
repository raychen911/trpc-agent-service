"""Bounded enterprise utility Tools for time, HTTP and local workspaces."""

from collections.abc import Callable, Sequence
from datetime import datetime, timezone
import ipaddress
import socket
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from trpc_service.agent.contracts import AgentExecutionContext, AgentToolCall, AgentToolResult
from trpc_service.agent.ports import AgentToolInvoker
from trpc_service.workspace import WorkspaceHandle, WorkspaceProvider

_MAX_HTTP_BYTES = 256 * 1024
_MAX_WORKSPACE_READ_BYTES = 256 * 1024
_MAX_WORKSPACE_RESULTS = 200
_MAX_WORKSPACE_SCAN_ENTRIES = 2_000


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_public_addresses(host: str) -> tuple[str, ...]:
    return tuple(
        {str(entry[4][0])
         for entry in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)})


class EnterpriseToolInvoker(AgentToolInvoker):
    """Execute common read-only enterprise Tools within explicit boundaries."""

    TOOL_NAMES = frozenset({"current_time", "http.get", "workspace.list", "workspace.read"})

    def __init__(
        self,
        workspace: WorkspaceProvider,
        *,
        http_client: httpx.AsyncClient,
        clock: Callable[[], datetime] = _utc_now,
        resolve_host: Callable[[str], Sequence[str]] = _resolve_public_addresses,
    ) -> None:
        self._workspace = workspace
        self._http_client = http_client
        self._clock = clock
        self._resolve_host = resolve_host

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        if call.name == "current_time":
            return AgentToolResult(call.call_id, content=self._current_time(call))
        if call.name == "workspace.list":
            return AgentToolResult(call.call_id, content=await self._workspace_list(context, call))
        if call.name == "workspace.read":
            return AgentToolResult(call.call_id, content=await self._workspace_read(context, call))
        if call.name == "http.get":
            return AgentToolResult(call.call_id, content=await self._http_get(context, call))
        raise PermissionError(f"tool is not registered for this Agent: {call.name}")

    def _current_time(self, call: AgentToolCall) -> str:
        timezone_name = call.arguments.get("timezone", "UTC")
        if not isinstance(timezone_name, str) or not timezone_name.strip():
            raise ValueError("current_time requires an IANA timezone name")
        try:
            target = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise ValueError("current_time timezone is unknown") from error
        now = self._clock()
        if now.tzinfo is None:
            raise RuntimeError("current_time clock must return a timezone-aware value")
        return now.astimezone(target).isoformat(timespec="seconds")

    async def _workspace_handle(self, context: AgentExecutionContext) -> WorkspaceHandle:
        # Acquisition and release belong exclusively to the execution pipeline.
        # A Tool must not hide a broken Worker composition or leak a future
        # container/sandbox resource by creating an unmanaged handle itself.
        if context.workspace is None:
            raise RuntimeError("Agent execution has no bound workspace")
        return context.workspace

    async def _workspace_list(self, context: AgentExecutionContext, call: AgentToolCall) -> str:
        raw_path = call.arguments.get("path", ".")
        if not isinstance(raw_path, str):
            raise ValueError("workspace.list path must be a string")
        handle = await self._workspace_handle(context)
        files = await self._workspace.list_files(
            handle,
            raw_path,
            max_results=_MAX_WORKSPACE_RESULTS,
            max_entries=_MAX_WORKSPACE_SCAN_ENTRIES,
        )
        return "\n".join(files)

    async def _workspace_read(self, context: AgentExecutionContext, call: AgentToolCall) -> str:
        raw_path = call.arguments.get("path")
        if not isinstance(raw_path, str):
            raise ValueError("workspace.read path must be a string")
        handle = await self._workspace_handle(context)
        return await self._workspace.read_text(
            handle,
            raw_path,
            max_bytes=_MAX_WORKSPACE_READ_BYTES,
        )

    async def _http_get(self, context: AgentExecutionContext, call: AgentToolCall) -> str:
        raw_url = call.arguments.get("url")
        if not isinstance(raw_url, str):
            raise ValueError("http.get requires a URL string")
        parsed = urlsplit(raw_url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.port not in {None, 443}):
            raise PermissionError("http.get requires an HTTPS URL without credentials")
        configured = context.config.tools.get("http_allowed_hosts", ())
        if (not isinstance(configured, Sequence) or isinstance(configured, (str, bytes))
                or any(not isinstance(host, str) or not host for host in configured)):
            raise ValueError("http_allowed_hosts must be an array of host names")
        allowed_hosts = {str(host).casefold() for host in configured}
        if parsed.hostname.casefold() not in allowed_hosts:
            raise PermissionError("http.get host is not allowlisted")
        addresses = self._resolve_host(parsed.hostname)
        if not addresses:
            raise ConnectionError("http.get host did not resolve")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
                    or ip.is_reserved or ip.is_unspecified):
                raise PermissionError("http.get host resolved to a non-public address")

        response = await self._http_client.get(
            raw_url,
            headers={"Accept": "application/json,text/plain"},
            follow_redirects=False,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"http.get provider returned status {response.status_code}")
        if len(response.content) > _MAX_HTTP_BYTES:
            raise ValueError("http.get response exceeds the size limit")
        media_type = response.headers.get("content-type", "").partition(";")[0].strip().casefold()
        if media_type not in {"application/json", "text/plain", ""}:
            raise ValueError("http.get response content type is not supported")
        return response.content.decode("utf-8")
