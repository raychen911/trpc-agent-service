"""Small, auditable tool registry; unlisted tools cannot be instantiated."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import ipaddress
import math
import operator
import socket
from datetime import datetime
from typing import Any, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from tenant_agent.governance.filters import TenantToolGovernanceFilter
from tenant_agent.governance.policies import ConfirmationManager, GovernanceService
from tenant_agent.ids import stable_checksum
from tenant_agent.models import ArtifactRecord, TenantConfig
from tenant_agent.storage.base import TenantDataPlane

_BINARY_OPERATORS: dict[type[ast.operator], Any] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS: dict[type[ast.unaryop], Any] = {ast.UAdd: operator.pos, ast.USub: operator.neg}
REGISTERED_TOOL_NAMES = frozenset(
    {"calculator", "current_time", "memory_search", "save_text_artifact", "fetch_url"}
)


def _calculate(node: ast.AST, *, depth: int = 0) -> float | int:
    if depth > 20:
        raise ValueError("expression is too deeply nested")
    if isinstance(node, ast.Expression):
        return _calculate(node.body, depth=depth + 1)
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
    ):
        if not math.isfinite(float(node.value)) or abs(float(node.value)) > 1e100:
            raise ValueError("numeric literal is outside the safe range")
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
        left = _calculate(node.left, depth=depth + 1)
        right = _calculate(node.right, depth=depth + 1)
        if isinstance(node.op, ast.Pow) and abs(float(right)) > 12:
            raise ValueError("exponent is outside the safe range")
        value = _BINARY_OPERATORS[type(node.op)](left, right)
        if not math.isfinite(float(value)) or abs(float(value)) > 1e100:
            raise ValueError("result is outside the safe range")
        return cast(float | int, value)
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
        return cast(
            float | int,
            _UNARY_OPERATORS[type(node.op)](_calculate(node.operand, depth=depth + 1)),
        )
    raise ValueError("only numeric arithmetic is permitted")


def calculator(expression: str) -> dict[str, float | int | str]:
    """Safely evaluate a numeric arithmetic expression without executing code."""

    if len(expression) > 256:
        return {"error": "expression_too_long"}
    try:
        tree = ast.parse(expression, mode="eval")
        return {"expression": expression, "result": _calculate(tree)}
    except (SyntaxError, ValueError, ZeroDivisionError, OverflowError) as exc:
        return {"error": exc.__class__.__name__}


def current_time(timezone_name: str = "UTC") -> dict[str, str]:
    """Return the current ISO-8601 time for an IANA timezone name."""

    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return {"error": "unknown_timezone"}
    now = datetime.now(zone)
    return {"timezone": timezone_name, "iso8601": now.isoformat()}


async def _public_addresses(hostname: str) -> tuple[str, ...]:
    rows = await asyncio.to_thread(socket.getaddrinfo, hostname, None, type=socket.SOCK_STREAM)
    addresses = tuple(sorted({str(row[4][0]) for row in rows}))
    if not addresses:
        raise ValueError("target did not resolve")
    for raw in addresses:
        address = ipaddress.ip_address(raw)
        if not address.is_global:
            raise ValueError("target resolves to a non-public address")
    return addresses


def build_tools(
    *,
    tenant: TenantConfig,
    app_id: str,
    plane: TenantDataPlane,
    governance: GovernanceService,
    confirmations: ConfirmationManager,
) -> list[Any]:
    """Instantiate only tools selected by both app and tenant allow-lists."""

    from trpc_agent_sdk.context import InvocationContext
    from trpc_agent_sdk.tools import FunctionTool

    async def memory_search(query: str, tool_context: InvocationContext) -> dict[str, Any]:
        """Search this user's tenant-scoped long-term memory."""

        records = await plane.memories.search_memory(tenant.tenant_id, tool_context.user_id, query, limit=5)
        return {
            "items": [
                {
                    "content": record.content,
                    "metadata": record.metadata,
                    "updated_at": record.updated_at.isoformat(),
                }
                for record in records
            ]
        }

    async def save_text_artifact(
        filename: str,
        content: str,
        tool_context: InvocationContext,
    ) -> dict[str, Any]:
        """Save a UTF-8 text artifact in this tenant and session."""

        encoded = content.encode()
        if len(encoded) > 512 * 1024:
            return {"error": "artifact_too_large"}
        call_identity = str(
            getattr(tool_context, "function_call_id", None) or stable_checksum(filename, content)
        )
        artifact_id = (
            "a_"
            + stable_checksum(
                tenant.tenant_id,
                tool_context.session_id,
                call_identity,
            )[:48]
        )
        record = ArtifactRecord(
            tenant_id=tenant.tenant_id,
            session_id=tool_context.session_id,
            artifact_id=artifact_id,
            filename=filename[:512],
            content_type="text/plain; charset=utf-8",
            size_bytes=len(encoded),
            checksum_sha256=hashlib.sha256(encoded).hexdigest(),
            storage_uri=f"artifact://{artifact_id}",
        )
        await plane.artifacts.put_artifact(record, encoded)
        return {"artifact_id": artifact_id, "filename": record.filename, "size_bytes": len(encoded)}

    async def fetch_url(url: str) -> dict[str, Any]:
        """Fetch an allow-listed public HTTPS URL; redirects and private networks are blocked."""

        parsed = urlparse(url)
        hostname = (parsed.hostname or "").casefold()
        allowlist = {
            item.strip().casefold()
            for item in tenant.metadata.get("http_tool_allowlist", "").split(",")
            if item.strip()
        }
        if parsed.scheme != "https" or not hostname:
            return {"error": "https_url_required"}
        if parsed.username or parsed.password:
            return {"error": "inline_credentials_forbidden"}
        if not any(hostname == allowed or hostname.endswith(f".{allowed}") for allowed in allowlist):
            return {"error": "host_not_allowed"}
        try:
            addresses = await _public_addresses(hostname)
            selected_address = addresses[0]
            address_literal = (
                f"[{selected_address}]"
                if ipaddress.ip_address(selected_address).version == 6
                else selected_address
            )
            port = parsed.port
            authority = address_literal + (f":{port}" if port and port != 443 else "")
            pinned_url = parsed._replace(netloc=authority).geturl()
            host_header = hostname + (f":{port}" if port and port != 443 else "")
            async with httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                async with client.stream(
                    "GET",
                    pinned_url,
                    headers={
                        "Host": host_header,
                        "User-Agent": "tenant-agent-tool/1",
                    },
                    extensions={"sni_hostname": hostname},
                ) as response:
                    if 300 <= response.status_code < 400:
                        return {"error": "redirect_blocked"}
                    response.raise_for_status()
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > 1_000_000:
                            return {"error": "response_too_large"}
                        chunks.append(chunk)
            return {
                "status": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "text": b"".join(chunks).decode("utf-8", errors="replace"),
            }
        except (OSError, ValueError, httpx.HTTPError) as exc:
            return {"error": exc.__class__.__name__}

    registry = {
        "calculator": calculator,
        "current_time": current_time,
        "memory_search": memory_search,
        "save_text_artifact": save_text_artifact,
        "fetch_url": fetch_url,
    }
    assert registry.keys() == REGISTERED_TOOL_NAMES
    tools: list[Any] = []
    for name in sorted(tenant.apps[app_id].allowed_tools):
        function = registry.get(name)
        if function is None:
            raise ValueError(f"configured tool {name!r} is not registered")
        policy_filter = TenantToolGovernanceFilter(
            tenant=tenant,
            app_id=app_id,
            tool_name=name,
            governance=governance,
            confirmations=confirmations,
            audit=plane.audit,
        )
        tools.append(FunctionTool(function, filters=[policy_filter]))
    return tools
