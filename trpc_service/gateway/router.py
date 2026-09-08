import hashlib
import hmac
import ssl

import httpx
from opentelemetry.propagate import inject

from trpc_service.gateway.contracts import (
    GatewayResponse,
    MessageHandler,
    NodeDirectory,
    NodeRecord,
    NormalizedMessage,
)
from trpc_service.metrics import PlatformMetrics, tracer


class NoHealthyNodeError(RuntimeError):
    pass


class CrossNodeRouter:
    """Rendezvous-hash sessions across healthy stateless worker nodes."""

    def __init__(
        self,
        node_id: str,
        directory: NodeDirectory,
        local_handler: MessageHandler,
        internal_secret: str,
        *,
        forward_timeout_seconds: float = 30,
        client: httpx.AsyncClient | None = None,
        metrics: PlatformMetrics | None = None,
        tls_ca_file: str | None = None,
        tls_cert_file: str | None = None,
        tls_key_file: str | None = None,
        canary_tenant_ids: set[str] | None = None,
    ) -> None:
        self.node_id = node_id
        self._directory = directory
        self._handler = local_handler
        self._secret = internal_secret
        self._timeout = forward_timeout_seconds
        tls_context = None
        if tls_ca_file:
            tls_context = ssl.create_default_context(cafile=tls_ca_file)
            if tls_cert_file and tls_key_file:
                tls_context.load_cert_chain(tls_cert_file, tls_key_file)
        self._client = client or httpx.AsyncClient(verify=tls_context or True)
        self._owns_client = client is None
        self._metrics = metrics
        self._canary_tenants = canary_tenant_ids or set()

    @staticmethod
    def select_node(route_key: str, nodes: tuple[NodeRecord, ...]) -> NodeRecord:
        if not nodes:
            raise NoHealthyNodeError("no healthy gateway worker is registered")

        def score(node: NodeRecord) -> int:
            digest = hashlib.sha256(f"{route_key}|{node.node_id}".encode()).digest()
            return int.from_bytes(digest, "big")

        return max(nodes, key=score)

    async def resolve(self, route_key: str, tenant_id: str | None = None) -> NodeRecord:
        nodes = tuple(await self._directory.list_healthy())
        if self._metrics is not None:
            self._metrics.healthy_nodes.set(len(nodes))
        desired_track = "canary" if tenant_id in self._canary_tenants else "stable"
        tracked = tuple(
            node for node in nodes if node.metadata.get("release_track", "stable") == desired_track
        )
        return self.select_node(route_key, tracked or nodes)

    async def dispatch(self, message: NormalizedMessage) -> GatewayResponse:
        with tracer.start_as_current_span("gateway.route") as span:
            span.set_attribute("trpc.tenant_id", message.tenant_id)
            span.set_attribute("trpc.session_id", message.session_id)
            span.set_attribute("trpc.channel", message.channel)
            selected = await self.resolve(message.route_key, message.tenant_id)
            span.set_attribute("trpc.node_id", selected.node_id)
            if selected.node_id == self.node_id:
                result = await self._handler.handle(message, self.node_id)
                if self._metrics is not None:
                    self._metrics.gateway_messages.labels(message.channel, result.status).inc()
                return result
            headers = {"x-internal-token": self._secret}
            inject(headers)
            response = await self._client.post(
                f"{selected.base_url}/internal/v1/messages",
                json={"message": message_to_dict(message)},
                headers=headers,
                timeout=self._timeout,
            )
            response.raise_for_status()
            result = GatewayResponse(**response.json())
            if self._metrics is not None:
                self._metrics.gateway_messages.labels(message.channel, result.status).inc()
            return result

    async def handle_local(self, message: NormalizedMessage) -> GatewayResponse:
        return await self._handler.handle(message, self.node_id)

    def verify_internal_token(self, provided: str | None) -> bool:
        return provided is not None and hmac.compare_digest(provided, self._secret)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def message_to_dict(message: NormalizedMessage) -> dict[str, object]:
    return {
        "tenant_id": message.tenant_id,
        "agent_app_id": message.agent_app_id,
        "channel": message.channel,
        "account_id": message.account_id,
        "external_message_id": message.external_message_id,
        "sender_user_id": message.sender_user_id,
        "conversation_id": message.conversation_id,
        "conversation_type": message.conversation_type,
        "text": message.text,
        "trace_id": message.trace_id,
        "metadata": dict(message.metadata),
    }
