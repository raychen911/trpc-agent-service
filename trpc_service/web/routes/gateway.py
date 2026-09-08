import base64
import uuid
from dataclasses import replace
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, Field

from trpc_service.channels import (
    ChannelBindingRepository,
    IgnoredWeComAIBotEvent,
    ImIdentityRepository,
    TelegramAdapter,
    WeComAdapter,
    WeComCrypto,
)
from trpc_service.channels.bindings import ChannelBindingNotFoundError
from trpc_service.channels.identity import ImIdentityMappingNotFoundError
from trpc_service.channels.telegram import (
    ChannelAuthenticationError,
    UnsupportedChannelMessageError,
)
from trpc_service.config import SecretResolver
from trpc_service.domain import ChannelType
from trpc_service.gateway.contracts import GatewayResponse, NormalizedMessage
from trpc_service.gateway.router import NoHealthyNodeError
from trpc_service.storage.exceptions import DuplicateMessageError

router = APIRouter(tags=["gateway"])


class MessageBody(BaseModel):
    tenant_id: str
    agent_app_id: str
    channel: str = Field(min_length=1, max_length=32)
    account_id: str = Field(min_length=1, max_length=255)
    external_message_id: str = Field(min_length=1, max_length=255)
    sender_user_id: str = Field(min_length=1, max_length=255)
    conversation_id: str = Field(min_length=1, max_length=255)
    conversation_type: Literal["direct", "group"] = "direct"
    text: str = Field(min_length=1, max_length=100_000)
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = Field(default_factory=dict)

    def normalized(self) -> NormalizedMessage:
        return NormalizedMessage(**self.model_dump())


class InternalMessageBody(BaseModel):
    message: MessageBody


class ExecutionRecoveryBody(BaseModel):
    action: Literal["retry", "fail"]


class ArtifactUploadBody(BaseModel):
    tenant_id: str
    object_key: str = Field(min_length=1, max_length=1024)
    mime_type: str = Field(min_length=1, max_length=255)
    content_base64: str
    metadata: dict[str, Any] = Field(default_factory=dict)


def _services(request: Request) -> Any:
    return request.app.state.services


def _require_gateway_token(request: Request, token: str | None) -> None:
    if not _services(request).gateway_router.verify_internal_token(token):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid gateway token")


async def _resolve_optional(resolver: SecretResolver, reference: str | None) -> str | None:
    return await resolver.resolve(reference) if reference else None


def _raise_channel_error(error: Exception) -> None:
    if isinstance(error, ChannelBindingNotFoundError):
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error
    if isinstance(error, ChannelAuthenticationError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(error)) from error
    if isinstance(error, ImIdentityMappingNotFoundError):
        raise HTTPException(status.HTTP_403_FORBIDDEN, str(error)) from error
    if isinstance(error, UnsupportedChannelMessageError):
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(error)) from error
    if isinstance(error, DuplicateMessageError):
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    if isinstance(error, NoHealthyNodeError):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(error)) from error
    raise error


@router.post("/gateway/v1/messages", response_model=None)
async def gateway_message(
    payload: MessageBody,
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> dict[str, Any]:
    _require_gateway_token(request, gateway_token)
    try:
        result = await _services(request).gateway_router.dispatch(payload.normalized())
        return _response_dict(result)
    except Exception as error:
        _raise_channel_error(error)
        raise


@router.post("/internal/v1/messages", response_model=None, include_in_schema=False)
async def internal_message(
    payload: InternalMessageBody,
    request: Request,
    internal_token: Annotated[str | None, Header(alias="x-internal-token")] = None,
) -> dict[str, Any]:
    services = _services(request)
    if not services.gateway_router.verify_internal_token(internal_token):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid internal token")
    result = await services.gateway_router.handle_local(payload.message.normalized())
    return _response_dict(result)


@router.get("/gateway/v1/nodes")
async def list_nodes(
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> list[dict[str, Any]]:
    _require_gateway_token(request, gateway_token)
    nodes = await _services(request).node_directory.list_healthy()
    return [
        {
            "node_id": node.node_id,
            "base_url": node.base_url,
            "capacity": node.capacity,
            "expires_at": node.expires_at.isoformat(),
            "metadata": dict(node.metadata),
        }
        for node in nodes
    ]


@router.get("/gateway/v1/messages/{message_id}")
async def get_message_status(
    message_id: str,
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> dict[str, Any]:
    _require_gateway_token(request, gateway_token)
    record = await _services(request).inbound_queue.get(message_id)
    if record is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "inbound message not found")
    execution_id = await _services(request).inbound_queue.execution_id(message_id)
    return {
        "message_id": record.id,
        "execution_id": execution_id,
        "status": record.status,
        "attempts": record.attempts,
        "trace_id": record.message.trace_id,
    }


@router.post("/gateway/v1/artifacts", status_code=status.HTTP_201_CREATED)
async def upload_artifact(
    payload: ArtifactUploadBody,
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> dict[str, Any]:
    _require_gateway_token(request, gateway_token)
    try:
        content = base64.b64decode(payload.content_base64, validate=True)
    except ValueError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid base64") from error
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, "artifact exceeds 10 MiB")
    stored = await _services(request).artifacts.put(
        payload.tenant_id,
        payload.object_key,
        content,
        payload.mime_type,
        payload.metadata,
    )
    return {
        "tenant_id": stored.tenant_id,
        "object_key": stored.object_key,
        "mime_type": stored.mime_type,
        "size_bytes": stored.size_bytes,
        "checksum": stored.checksum,
    }


@router.get("/gateway/v1/artifacts/{tenant_id}/{object_key:path}")
async def download_artifact(
    tenant_id: str,
    object_key: str,
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> Response:
    _require_gateway_token(request, gateway_token)
    artifact = await _services(request).artifacts.get(tenant_id, object_key)
    return Response(
        artifact.content,
        media_type=artifact.metadata.mime_type,
        headers={"etag": artifact.metadata.checksum},
    )


@router.post("/gateway/v1/executions/{execution_id}/resolve")
async def resolve_uncertain_execution(
    execution_id: str,
    payload: ExecutionRecoveryBody,
    request: Request,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> dict[str, Any]:
    _require_gateway_token(request, gateway_token)
    try:
        record = await _services(request).execution_ledger.resolve_uncertain(
            execution_id, payload.action
        )
    except LookupError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error
    except ValueError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    return {"execution_id": record.id, "status": record.status}


@router.get("/gateway/v1/routes/resolve")
async def resolve_route(
    request: Request,
    tenant_id: str,
    agent_app_id: str,
    session_id: str,
    gateway_token: Annotated[str | None, Header(alias="x-gateway-token")] = None,
) -> dict[str, str]:
    _require_gateway_token(request, gateway_token)
    route_key = f"{tenant_id}:{agent_app_id}:{session_id}"
    try:
        node = await _services(request).gateway_router.resolve(route_key)
    except NoHealthyNodeError as error:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(error)) from error
    return {"route_key": route_key, "node_id": node.node_id, "base_url": node.base_url}


@router.post("/webhooks/telegram/{account_id}", response_model=None)
async def telegram_webhook(
    account_id: str,
    payload: dict[str, Any],
    request: Request,
    webhook_secret: Annotated[str | None, Header(alias="x-telegram-bot-api-secret-token")] = None,
) -> dict[str, Any]:
    repository = ChannelBindingRepository(request.app.state.database.session_factory)
    resolver = request.app.state.secret_resolver
    try:
        binding = await repository.resolve(ChannelType.TELEGRAM, account_id)
        if binding.webhook_path != request.url.path:
            raise ChannelBindingNotFoundError("webhook path does not match active binding")
        expected_secret = await _resolve_optional(resolver, binding.token_secret_ref)
        message = TelegramAdapter().normalize(
            payload,
            binding,
            webhook_secret,
            expected_secret,
            getattr(request.state, "trace_id", None),
        )
        message = await _map_im_user(request, binding, message)
        queued = await _services(request).inbound_queue.enqueue(message)
        return {
            "status": "accepted" if queued.created else queued.record.status,
            "message_id": queued.record.id,
        }
    except Exception as error:
        _raise_channel_error(error)
        raise


@router.get("/webhooks/wecom/{account_id}", response_class=Response)
async def verify_wecom_webhook(
    account_id: str,
    request: Request,
    msg_signature: Annotated[str, Query()],
    timestamp: Annotated[str, Query()],
    nonce: Annotated[str, Query()],
    echostr: Annotated[str, Query()],
) -> Response:
    repository = ChannelBindingRepository(request.app.state.database.session_factory)
    resolver = request.app.state.secret_resolver
    try:
        binding = await repository.resolve(ChannelType.WECOM, account_id)
        if binding.webhook_path != request.url.path:
            raise ChannelBindingNotFoundError("webhook path does not match active binding")
        token = await _resolve_optional(resolver, binding.token_secret_ref)
        aes_key = await _resolve_optional(resolver, binding.secret_ref)
        if token is None or aes_key is None:
            raise ChannelAuthenticationError("WeCom token and EncodingAESKey are required")
        receive_id = (
            ""
            if binding.options.get("mode", "app") == "aibot"
            else str(binding.options.get("receive_id", binding.account_id))
        )
        crypto = WeComCrypto(token, aes_key, receive_id)
        plaintext = WeComAdapter.verify_url(echostr, msg_signature, timestamp, nonce, crypto)
        return Response(plaintext, media_type="text/plain")
    except Exception as error:
        _raise_channel_error(error)
        raise


@router.post("/webhooks/wecom/{account_id}", response_class=Response)
async def wecom_webhook(
    account_id: str,
    request: Request,
    msg_signature: Annotated[str | None, Query()] = None,
    timestamp: Annotated[str | None, Query()] = None,
    nonce: Annotated[str | None, Query()] = None,
) -> Response:
    repository = ChannelBindingRepository(request.app.state.database.session_factory)
    resolver = request.app.state.secret_resolver
    try:
        binding = await repository.resolve(ChannelType.WECOM, account_id)
        if binding.webhook_path != request.url.path:
            raise ChannelBindingNotFoundError("webhook path does not match active binding")
        token = await _resolve_optional(resolver, binding.token_secret_ref)
        aes_key = await _resolve_optional(resolver, binding.secret_ref)
        message = WeComAdapter().normalize(
            await request.body(),
            binding,
            token=token,
            encoding_aes_key=aes_key,
            signature=msg_signature,
            timestamp=timestamp,
            nonce=nonce,
            trace_id=getattr(request.state, "trace_id", None),
        )
        message = await _map_im_user(request, binding, message)
        await _services(request).inbound_queue.enqueue(message)
        if binding.options.get("mode", "app") == "aibot":
            return Response("{}", media_type="application/json")
        return Response("success", media_type="text/plain")
    except IgnoredWeComAIBotEvent:
        return Response("{}", media_type="application/json")
    except Exception as error:
        _raise_channel_error(error)
        raise


def _response_dict(result: GatewayResponse) -> dict[str, Any]:
    return {
        "status": result.status,
        "node_id": result.node_id,
        "session_id": result.session_id,
        "trace_id": result.trace_id,
        "reply_text": result.reply_text,
        "session_version": result.session_version,
        "delivery": dict(result.delivery),
    }


async def _map_im_user(
    request: Request, binding: Any, message: NormalizedMessage
) -> NormalizedMessage:
    resolved = await ImIdentityRepository(request.app.state.database.session_factory).resolve(
        binding, message.sender_user_id
    )
    metadata = dict(message.metadata)
    metadata["external_sender_user_id"] = resolved.external_user_id
    metadata["identity_mapped"] = resolved.mapped
    return replace(message, sender_user_id=resolved.internal_user_id, metadata=metadata)
