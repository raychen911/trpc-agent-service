"""Settle unknown costs only from a persisted provider receipt and authenticated GET."""

import json
from decimal import Decimal, ROUND_CEILING

import httpx
from sqlalchemy import select

from trpc_service.persistence.models import ModelAttemptRow, AuditLogRow, InboundMessageRow
from trpc_service.management import ConfigurationStore
from trpc_service.agent.openrouter import ENDPOINT, MODEL
from trpc_service.storage.runtime_resources import resolve_env
from .budget import BudgetLedger, BudgetConflict, QUANTUM, digest, token_count, money
from .model_filter import database_call


async def reconcile(database, tenant_id, attempt_id, actor):
    ledger = BudgetLedger(database)
    with database.sessions() as session:
        attempt = session.get(ModelAttemptRow, (tenant_id, attempt_id))
        if attempt is None:
            raise LookupError("model attempt not found")
        if attempt.status == "settled":
            return ledger.view(attempt)
        if attempt.status != "pending_reconciliation":
            raise BudgetConflict("attempt is not awaiting reconciliation")
        records = list(
            session.scalars(
                select(AuditLogRow).where(AuditLogRow.tenant_id == tenant_id,
                                          AuditLogRow.decision == "model_provider_receipt")))
        generation_id = next((row.metadata_json["generation_id"]
                              for row in records if row.metadata_json.get("attempt_id") == attempt_id), None)
        if generation_id is None:
            raise BudgetConflict("provider receipt unavailable; cost remains unknown")
        parent = session.scalar(
            select(InboundMessageRow).where(InboundMessageRow.tenant_id == tenant_id,
                                            InboundMessageRow.execution_id == attempt.execution_id))
        if parent is None:
            raise BudgetConflict("pinned invocation is unavailable")
        route = dict(parent.payload_json["route"])
        model_id = attempt.model_id
    config = ConfigurationStore(database).published(tenant_id, route["config_version"])
    model = next(item for item in config.models if item.model_config_id == model_id)
    if model.provider != "openrouter" or model.model_name != MODEL:
        raise BudgetConflict("unsupported provider reconciliation contract")
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
            response = await client.get(ENDPOINT + "/generation",
                                        params={"id": generation_id},
                                        headers={"Authorization": "Bearer " + resolve_env(model.api_key_ref.uri)})
            response.raise_for_status()
            value = json.loads(response.content, parse_float=Decimal)["data"]
            catalog = await client.get(ENDPOINT + "/models")
            catalog.raise_for_status()
            entry = next(item for item in catalog.json()["data"] if item["id"] == MODEL)
            canonical = entry.get("canonical_slug")
        accepted_models = {MODEL}
        if isinstance(canonical, str) and canonical:
            accepted_models.add(canonical)
        if value["id"] != generation_id or value["model"] not in accepted_models:
            raise ValueError("provider receipt mismatch")
        incoming = token_count(value["native_tokens_prompt"])
        outgoing = token_count(value["native_tokens_completion"])
        amount = money(Decimal(str(value["total_cost"])).quantize(QUANTUM, rounding=ROUND_CEILING))
    except Exception:
        raise BudgetConflict("provider cost could not be verified; reservation retained") from None
    return await database_call(ledger.settle,
                               tenant_id,
                               attempt_id,
                               incoming,
                               outgoing,
                               digest([generation_id, incoming, outgoing,
                                       str(amount)]),
                               actor=actor,
                               provider_cost=amount)
