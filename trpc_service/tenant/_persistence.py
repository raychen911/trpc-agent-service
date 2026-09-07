# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Durable MySQL tenant repository and encrypted configuration codec."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Optional
from uuid import uuid4

from cryptography.fernet import Fernet
from pydantic import SecretStr
from sqlalchemy import JSON
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine

from ._models import Tenant


def mysql_sync_url(url: str) -> str:
    """Normalise supported MySQL URLs for synchronous repository access."""
    return url.replace("mysql+aiomysql://", "mysql+pymysql://").replace("mysql+asyncmy://", "mysql+pymysql://")


class TenantConfigCodec:
    """Split public config from secrets and encrypt the secret payload."""

    def __init__(self, encryption_key: Optional[str]) -> None:
        self._fernet = None
        if encryption_key:
            digest = hashlib.sha256(encryption_key.encode("utf-8")).digest()
            self._fernet = Fernet(base64.urlsafe_b64encode(digest))

    def encode(self, tenant: Tenant) -> tuple[dict[str, Any], Optional[str]]:
        public = tenant.model_dump(mode="python")
        secrets: dict[str, str] = {}

        def walk(value: Any, path: tuple[str, ...]) -> Any:
            if isinstance(value, SecretStr):
                secrets[".".join(path)] = value.get_secret_value()
                return None
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, dict):
                return {str(key): walk(item, (*path, str(key))) for key, item in value.items()}
            if isinstance(value, list):
                return [walk(item, (*path, str(index))) for index, item in enumerate(value)]
            if hasattr(value, "value"):
                return value.value
            return value

        sanitized = walk(public, tuple())
        encrypted = None
        if secrets:
            if self._fernet is None:
                raise ValueError("TENANT_CONFIG_ENCRYPTION_KEY is required when tenant config contains secrets")
            encrypted = self._fernet.encrypt(json.dumps(secrets, ensure_ascii=False,
                                                        sort_keys=True).encode("utf-8")).decode("ascii")
        return sanitized, encrypted

    def decode(self, public: dict[str, Any], encrypted: Optional[str]) -> Tenant:
        payload = json.loads(json.dumps(public))
        if encrypted:
            if self._fernet is None:
                raise ValueError("TENANT_CONFIG_ENCRYPTION_KEY is required to decrypt tenant config")
            secrets = json.loads(self._fernet.decrypt(encrypted.encode("ascii")).decode("utf-8"))
            for dotted_path, value in secrets.items():
                target = payload
                parts = dotted_path.split(".")
                for part in parts[:-1]:
                    target = target[int(part)] if isinstance(target, list) else target[part]
                if isinstance(target, list):
                    target[int(parts[-1])] = value
                else:
                    target[parts[-1]] = value
        return Tenant.model_validate(payload)


@dataclass(frozen=True)
class StoredConfigVersion:
    tenant_id: str
    version: int
    tenant: Tenant
    created_by: str
    reason: str
    created_at: datetime
    rolled_back: bool = False
    rolled_back_to: Optional[int] = None


metadata = MetaData()
tenant_table = Table(
    "tenant",
    metadata,
    Column("tenant_id", String(128), primary_key=True),
    Column("name", String(255), nullable=False),
    Column("status", String(32), nullable=False),
    Column("config_version", BigInteger, nullable=False),
    Column("config_snapshot", JSON, nullable=False),
    Column("encrypted_secrets", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)
version_table = Table(
    "tenant_config_version",
    metadata,
    Column("tenant_id", String(128), primary_key=True),
    Column("version", BigInteger, primary_key=True),
    Column("config_snapshot", JSON, nullable=False),
    Column("encrypted_secrets", Text, nullable=True),
    Column("created_by", String(128), nullable=False),
    Column("reason", String(512), nullable=False, default=""),
    Column("rolled_back", Boolean, nullable=False, default=False),
    Column("rolled_back_to", BigInteger, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
)
outbox_table = Table(
    "config_outbox",
    metadata,
    Column("event_id", String(64), primary_key=True),
    Column("tenant_id", String(128), nullable=False, index=True),
    Column("config_version", BigInteger, nullable=False),
    Column("event_type", String(64), nullable=False),
    Column("payload", JSON, nullable=False),
    Column("status", String(32), nullable=False, default="pending"),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
)


class MySqlTenantRepository:
    """MySQL source of truth for tenants and immutable config versions."""

    def __init__(self, db_url: str, encryption_key: Optional[str], create_schema: bool = True) -> None:
        self._engine: Engine = create_engine(mysql_sync_url(db_url), pool_pre_ping=True)
        self.codec = TenantConfigCodec(encryption_key)
        if create_schema:
            metadata.create_all(self._engine)

    @property
    def engine(self) -> Engine:
        return self._engine

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    def _version_values(self,
                        tenant: Tenant,
                        version: int,
                        by: str,
                        reason: str,
                        rolled_back: bool = False,
                        rolled_back_to: Optional[int] = None) -> dict[str, Any]:
        snapshot, secrets = self.codec.encode(tenant)
        return {
            "tenant_id": tenant.tenant_id,
            "version": version,
            "config_snapshot": snapshot,
            "encrypted_secrets": secrets,
            "created_by": by,
            "reason": reason,
            "rolled_back": rolled_back,
            "rolled_back_to": rolled_back_to,
            "created_at": self._utcnow(),
        }

    def _outbox_values(self, tenant_id: str, version: int, event_type: str) -> dict[str, Any]:
        return {
            "event_id": uuid4().hex,
            "tenant_id": tenant_id,
            "config_version": version,
            "event_type": event_type,
            "payload": {
                "tenant_id": tenant_id,
                "version": version
            },
            "status": "pending",
            "created_at": self._utcnow(),
        }

    def create(self, tenant: Tenant, by: str, reason: str) -> int:
        snapshot, secrets = self.codec.encode(tenant)
        now = self._utcnow()
        try:
            with self._engine.begin() as connection:
                last_version = connection.execute(
                    select(func.max(version_table.c.version)).where(
                        version_table.c.tenant_id == tenant.tenant_id)).scalar_one_or_none()
                version = int(last_version or 0) + 1
                connection.execute(
                    insert(tenant_table).values(
                        tenant_id=tenant.tenant_id,
                        name=tenant.name,
                        status=tenant.status.value,
                        config_version=version,
                        config_snapshot=snapshot,
                        encrypted_secrets=secrets,
                        created_at=now,
                        updated_at=now,
                    ))
                connection.execute(insert(version_table).values(self._version_values(tenant, version, by, reason)))
                connection.execute(
                    insert(outbox_table).values(self._outbox_values(tenant.tenant_id, version, "tenant.created")))
        except IntegrityError as exc:
            raise ValueError(f"tenant '{tenant.tenant_id}' already registered") from exc
        return version

    def get(self, tenant_id: str) -> Optional[tuple[Tenant, int]]:
        with self._engine.connect() as connection:
            row = connection.execute(
                select(tenant_table).where(tenant_table.c.tenant_id == tenant_id)).mappings().first()
        if row is None:
            return None
        return self.codec.decode(row["config_snapshot"], row["encrypted_secrets"]), int(row["config_version"])

    def list(self) -> list[tuple[Tenant, int]]:
        with self._engine.connect() as connection:
            rows = connection.execute(select(tenant_table).order_by(tenant_table.c.created_at)).mappings().all()
        return [(self.codec.decode(row["config_snapshot"], row["encrypted_secrets"]), int(row["config_version"]))
                for row in rows]

    def update(self,
               tenant: Tenant,
               expected_version: int,
               by: str,
               reason: str,
               rolled_back: bool = False,
               rolled_back_to: Optional[int] = None) -> int:
        snapshot, secrets = self.codec.encode(tenant)
        next_version = expected_version + 1
        with self._engine.begin() as connection:
            result = connection.execute(
                update(tenant_table).where(
                    tenant_table.c.tenant_id == tenant.tenant_id,
                    tenant_table.c.config_version == expected_version,
                ).values(
                    name=tenant.name,
                    status=tenant.status.value,
                    config_version=next_version,
                    config_snapshot=snapshot,
                    encrypted_secrets=secrets,
                    updated_at=self._utcnow(),
                ))
            if result.rowcount != 1:
                raise ValueError(f"tenant '{tenant.tenant_id}' version conflict")
            connection.execute(
                insert(version_table).values(
                    self._version_values(tenant, next_version, by, reason, rolled_back, rolled_back_to)))
            event_type = "tenant.rolled_back" if rolled_back else "tenant.updated"
            connection.execute(
                insert(outbox_table).values(self._outbox_values(tenant.tenant_id, next_version, event_type)))
        return next_version

    def delete(self, tenant_id: str, expected_version: int) -> None:
        with self._engine.begin() as connection:
            result = connection.execute(
                delete(tenant_table).where(
                    tenant_table.c.tenant_id == tenant_id,
                    tenant_table.c.config_version == expected_version,
                ))
            if result.rowcount != 1:
                raise ValueError(f"tenant '{tenant_id}' not found or version conflict")
            connection.execute(
                insert(outbox_table).values(self._outbox_values(tenant_id, expected_version + 1, "tenant.deleted")))

    def history(self, tenant_id: str) -> list[StoredConfigVersion]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                select(version_table).where(version_table.c.tenant_id == tenant_id).order_by(
                    version_table.c.version)).mappings().all()
        return [
            StoredConfigVersion(
                tenant_id=tenant_id,
                version=int(row["version"]),
                tenant=self.codec.decode(row["config_snapshot"], row["encrypted_secrets"]),
                created_by=row["created_by"],
                reason=row["reason"],
                created_at=row["created_at"],
                rolled_back=bool(row["rolled_back"]),
                rolled_back_to=row["rolled_back_to"],
            ) for row in rows
        ]

    def mark_outbox_published(self, event_id: str) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                update(outbox_table).where(outbox_table.c.event_id == event_id).values(status="published",
                                                                                       published_at=self._utcnow()))

    def pending_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._engine.connect() as connection:
            rows = connection.execute(
                select(outbox_table).where(outbox_table.c.status == "pending").order_by(
                    outbox_table.c.created_at).limit(limit)).mappings().all()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self._engine.dispose()
