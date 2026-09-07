"""Tenant audit export and retention maintenance."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import UTC, datetime, timedelta

import httpx

from tenant_agent.models import AuditRecord
from tenant_agent.observability import ERRORS, traced
from tenant_agent.security import CompositeSecretResolver, Redactor
from tenant_agent.services.config import TenantConfigService
from tenant_agent.settings import Settings
from tenant_agent.storage.router import StorageRouter

logger = logging.getLogger(__name__)


class AuditMaintenanceWorker:
    def __init__(
        self,
        *,
        settings: Settings,
        configs: TenantConfigService,
        storage: StorageRouter,
        secrets: CompositeSecretResolver,
        redactor: Redactor,
    ) -> None:
        self.settings = settings
        self.configs = configs
        self.storage = storage
        self.secrets = secrets
        self.redactor = redactor

    async def _export_batch(
        self,
        *,
        tenant_id: str,
        sink: str,
        authorization: str | None,
        records: tuple[AuditRecord, ...],
    ) -> None:
        audit_ids = sorted(record.audit_id for record in records)
        batch_id = hashlib.sha256("\n".join(audit_ids).encode()).hexdigest()
        headers = {"Idempotency-Key": batch_id}
        if authorization:
            headers["Authorization"] = authorization
        payload = {
            "tenant_id": tenant_id,
            "batch_id": batch_id,
            "records": [record.model_dump(mode="json") for record in records],
        }
        async with httpx.AsyncClient(timeout=self.settings.audit_export_timeout_seconds) as client:
            response = await client.post(sink, headers=headers, json=payload)
            response.raise_for_status()

    async def run_once(self) -> dict[str, int]:
        totals = {"exported": 0, "pruned": 0, "failed_tenants": 0}
        tenants = await self.configs.repository.list_configured_tenants()
        for tenant in tenants:
            cutoff = datetime.now(UTC) - timedelta(days=tenant.audit.retention_days)
            try:
                audit = await self.storage.audit_for_tenant(tenant)
                if not tenant.audit.export_sink:
                    for _ in range(self.settings.audit_maintenance_max_batches_per_tenant):
                        deleted = await audit.prune_audit(
                            tenant.tenant_id,
                            before=cutoff,
                            limit=self.settings.audit_export_batch_size,
                        )
                        totals["pruned"] += deleted
                        if deleted < self.settings.audit_export_batch_size:
                            break
                    continue
                authorization = None
                if tenant.audit.export_auth_ref:
                    authorization = await self.secrets.resolve(tenant.audit.export_auth_ref)
                for _ in range(self.settings.audit_maintenance_max_batches_per_tenant):
                    records = tuple(
                        await audit.query_audit(
                            tenant.tenant_id,
                            limit=self.settings.audit_export_batch_size,
                            before=cutoff,
                            oldest_first=True,
                        )
                    )
                    if not records:
                        break
                    with traced(
                        "audit.export",
                        {
                            "tenant.id": tenant.tenant_id,
                            "audit.batch.count": len(records),
                        },
                        redactor=self.redactor,
                    ):
                        await self._export_batch(
                            tenant_id=tenant.tenant_id,
                            sink=tenant.audit.export_sink,
                            authorization=authorization,
                            records=records,
                        )
                    deleted = await audit.delete_audit_ids(
                        tenant.tenant_id,
                        audit_ids=tuple(record.audit_id for record in records),
                    )
                    totals["exported"] += len(records)
                    totals["pruned"] += deleted
            except Exception as exc:
                error_type = exc.__class__.__name__
                totals["failed_tenants"] += 1
                ERRORS.labels(
                    tenant.tenant_id,
                    "audit_maintenance",
                    error_type,
                ).inc()
                logger.warning("Audit maintenance failed with %s", error_type)
        try:
            cutoff = datetime.now(UTC) - timedelta(seconds=self.settings.idempotency_ttl_seconds)
            for _ in range(self.settings.operational_retention_max_batches):
                operational_deleted = await self.configs.repository.prune_operational_records(
                    before=cutoff,
                    limit=self.settings.audit_export_batch_size,
                )
                if all(
                    count < self.settings.audit_export_batch_size for count in operational_deleted.values()
                ):
                    break
        except Exception as exc:
            error_type = exc.__class__.__name__
            ERRORS.labels("_system", "operational_retention", error_type).inc()
            logger.warning("Operational retention failed with %s", error_type)
        return totals

    async def run_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception as exc:
                error_type = exc.__class__.__name__
                ERRORS.labels("_system", "audit_loop", error_type).inc()
                logger.warning("Audit maintenance loop failed with %s", error_type)
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.settings.audit_maintenance_interval_seconds,
                )
            except TimeoutError:
                pass
