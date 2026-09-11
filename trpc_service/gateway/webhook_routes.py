"""Enterprise WeChat HTTP callback route with a deliberately small boundary."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse

from trpc_service.channels.policy import bind_message
from trpc_service.channels.webhook.wecom import (
    WebhookVerificationError,
    decode_wecom_message,
    decrypt_wecom_payload,
    verify_wecom_signature,
)
from trpc_service.config.secret_resolver import EnvSecretResolver, SecretResolutionError

_NOT_FOUND = "Webhook binding not found."
_UNAUTHORIZED = "Webhook verification failed."
_INVALID = "Invalid webhook request."
_UNAVAILABLE = "Webhook service unavailable."


def register_wecom_webhook_routes(
    app: FastAPI,
    binding_repository,
    ingress,
    secret_resolver: EnvSecretResolver,
) -> None:
    """Register the authenticated WeCom GET/POST callback URL once."""
    if getattr(app.state, "_wecom_webhook_routes_registered", False):
        return
    app.state._wecom_webhook_routes_registered = True

    async def binding_for(account: str):
        try:
            binding = await binding_repository.resolve_enabled("wecom", account)
        except Exception:
            raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None
        if binding is None:
            raise HTTPException(status_code=404, detail=_NOT_FOUND)
        return binding

    def secrets_for(binding):
        try:
            if binding.webhook_token_ref is None or binding.webhook_aes_key_ref is None:
                raise ValueError
            return (
                secret_resolver.resolve(binding.webhook_token_ref),
                secret_resolver.resolve(binding.webhook_aes_key_ref),
            )
        except (SecretResolutionError, ValueError):
            raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None

    def decrypt_query(request: Request, token: str, aes_key: str, account: str) -> bytes:
        query = request.query_params
        try:
            encrypted = query["echostr"]
            verify_wecom_signature(msg_signature=query["msg_signature"],
                                   timestamp=query["timestamp"],
                                   nonce=query["nonce"],
                                   encrypted=encrypted,
                                   token=token)
            return decrypt_wecom_payload(encrypted, aes_key=aes_key, receive_id=account)
        except (KeyError, WebhookVerificationError):
            raise HTTPException(status_code=401, detail=_UNAUTHORIZED) from None

    @app.get("/webhooks/wecom/{external_account_id}")
    async def wecom_challenge(external_account_id: str, request: Request) -> PlainTextResponse:
        binding = await binding_for(external_account_id)
        token, aes_key = secrets_for(binding)
        plaintext = decrypt_query(request, token, aes_key, binding.external_account_id)
        try:
            return PlainTextResponse(plaintext.decode("utf-8"), media_type="text/plain")
        except UnicodeDecodeError:
            raise HTTPException(status_code=401, detail=_UNAUTHORIZED) from None

    @app.post("/webhooks/wecom/{external_account_id}")
    async def wecom_message(external_account_id: str, request: Request) -> PlainTextResponse:
        binding = await binding_for(external_account_id)
        token, aes_key = secrets_for(binding)
        try:
            body = await request.body()
            encrypted = _encrypted_xml_value(body)
            query = request.query_params
            verify_wecom_signature(msg_signature=query["msg_signature"],
                                   timestamp=query["timestamp"],
                                   nonce=query["nonce"],
                                   encrypted=encrypted,
                                   token=token)
            plaintext = decrypt_wecom_payload(encrypted, aes_key=aes_key, receive_id=binding.external_account_id)
        except (KeyError, WebhookVerificationError):
            raise HTTPException(status_code=401, detail=_UNAUTHORIZED) from None
        try:
            message = decode_wecom_message(plaintext, external_account_id=binding.external_account_id)
        except WebhookVerificationError:
            raise HTTPException(status_code=422, detail=_INVALID) from None
        try:
            await ingress.chat(bind_message(message, binding))
        except ValueError:
            raise HTTPException(status_code=422, detail=_INVALID) from None
        except Exception:
            raise HTTPException(status_code=503, detail=_UNAVAILABLE) from None
        return PlainTextResponse("success", media_type="text/plain")


def _encrypted_xml_value(body: bytes) -> str:
    try:
        import xml.etree.ElementTree as etree
        value = etree.fromstring(body).findtext("Encrypt")
        if not isinstance(value, str) or not value:
            raise ValueError
        return value
    except Exception:
        raise WebhookVerificationError(_UNAUTHORIZED) from None


__all__ = ["register_wecom_webhook_routes"]
