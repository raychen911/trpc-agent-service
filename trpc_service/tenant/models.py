"""Domain models for the multi-tenant Agent platform."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlsplit

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python 3.10 compatibility
    from enum import Enum

    class StrEnum(str, Enum):
        pass


from typing import Any


def utc_now() -> datetime:
    return datetime.now(UTC)


def _public_config_value(value: Any) -> Any:
    """Return a public-safe view of tenant configuration values."""

    from trpc_service.security.secrets import redact_secret_data, redact_secret_text

    def sensitive_key(key: str, item: object) -> bool:
        key_text = key.lower()
        if key_text.endswith("_ref"):
            return True
        if any(marker in key_text for marker in ("token", "secret", "password", "api_key", "authorization")):
            return True
        if key_text in {"webhook_url", "url"}:
            return any(part in str(item).lower() for part in ("token=", "key=", "secret="))
        return False

    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if item and sensitive_key(str(key), item):
                result[str(key)] = "[configured]"
            else:
                result[str(key)] = _public_config_value(item)
        return result
    if isinstance(value, list):
        return [_public_config_value(item) for item in value]
    if isinstance(value, tuple):
        return [_public_config_value(item) for item in value]
    if isinstance(value, str):
        return redact_secret_text(value)
    return redact_secret_data(value)


class TenantStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"
    DRAFT = "draft"


class IsolationMode(StrEnum):
    SHARED_TABLE = "shared_table"
    DEDICATED_SCHEMA = "dedicated_schema"
    DEDICATED_DATABASE = "dedicated_database"


@dataclass(slots=True)
class ModelConfig:
    provider: str
    model: str
    temperature: float = 0.2
    timeout_ms: int = 60_000
    base_url: str = ""
    api_key_env: str = "OPENAI_API_KEY"
    api_key_ref: str = ""
    wire_api: str = "responses"
    max_output_tokens: int = 1_024
    cost_per_1k_tokens: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ModelConfig:
        return cls(
            provider=str(data["provider"]),
            model=str(data["model"]),
            temperature=float(data.get("temperature", 0.2)),
            timeout_ms=int(data.get("timeout_ms", 60_000)),
            base_url=str(data.get("base_url", "")),
            api_key_env=str(data.get("api_key_env", "OPENAI_API_KEY")),
            api_key_ref=str(data.get("api_key_ref", "")),
            wire_api=str(data.get("wire_api", "responses")),
            max_output_tokens=int(data.get("max_output_tokens", 1_024)),
            cost_per_1k_tokens=float(data.get("cost_per_1k_tokens", 0.0)),
        )


@dataclass(slots=True)
class ToolPolicy:
    allowlist: list[str] = field(default_factory=list)
    denylist: list[str] = field(default_factory=list)
    approval_rules: list[str] = field(default_factory=list)
    risk_levels: dict[str, str] = field(default_factory=dict)
    require_confirmation_for_risk: list[str] = field(default_factory=lambda: ["high", "critical"])
    max_calls_per_request: int = 16
    max_side_effect_calls_per_request: int = 4

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ToolPolicy:
        data = data or {}
        return cls(
            allowlist=list(data.get("allowlist", [])),
            denylist=list(data.get("denylist", [])),
            approval_rules=list(data.get("approval_rules", [])),
            risk_levels={str(key): str(value).lower() for key, value in dict(data.get("risk_levels", {})).items()},
            require_confirmation_for_risk=[
                str(value).lower()
                for value in data.get("require_confirmation_for_risk", ["high", "critical"])
            ],
            max_calls_per_request=int(data.get("max_calls_per_request", 16)),
            max_side_effect_calls_per_request=int(data.get("max_side_effect_calls_per_request", 4)),
        )


@dataclass(slots=True)
class QuotaPolicy:
    qps_limit: int = 20
    daily_token_limit: int = 1_000_000
    daily_cost_limit: float = 100.0

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> QuotaPolicy:
        data = data or {}
        return cls(
            qps_limit=int(data.get("qps_limit", 20)),
            daily_token_limit=int(data.get("daily_token_limit", 1_000_000)),
            daily_cost_limit=float(data.get("daily_cost_limit", 100.0)),
        )


@dataclass(slots=True)
class AuditPolicy:
    retention_days: int = 180
    redact_rules: list[str] = field(default_factory=lambda: ["token", "authorization", "phone", "email"])

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> AuditPolicy:
        data = data or {}
        return cls(
            retention_days=int(data.get("retention_days", 180)),
            redact_rules=list(data.get("redact_rules", ["token", "authorization", "phone", "email"])),
        )


@dataclass(slots=True)
class GrayReleasePolicy:
    enabled: bool = False
    candidate_version: int | None = None
    percent: int = 0
    session_overrides: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> GrayReleasePolicy:
        data = data or {}
        candidate = data.get("candidate_version")
        return cls(
            enabled=bool(data.get("enabled", False)),
            candidate_version=int(candidate) if candidate is not None else None,
            percent=int(data.get("percent", 0)),
            session_overrides={str(key): int(value) for key, value in dict(data.get("session_overrides", {})).items()},
        )


@dataclass(slots=True)
class StorageProfile:
    session_backend: str = "memory"
    memory_backend: str = "memory"
    summary_backend: str = "memory"
    knowledge_backend: str = "vector"
    artifact_backend: str = "object"
    audit_backend: str = "memory"
    redis_url: str = ""
    sql_dsn: str = ""
    redis_url_ref: str = ""
    sql_dsn_ref: str = ""
    knowledge_collection: str = "default"
    external_memory_url: str = ""
    external_memory_token_ref: str = ""
    vector_url: str = ""
    vector_token_ref: str = ""
    vector_provider: str = "qdrant"
    vector_dimension: int = 64
    embedding_url: str = ""
    embedding_model: str = ""
    embedding_token_ref: str = ""
    object_endpoint: str = ""
    object_bucket: str = ""
    object_region: str = ""
    object_access_key_ref: str = ""
    object_secret_key_ref: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> StorageProfile:
        data = data or {}
        return cls(
            session_backend=str(data.get("session_backend", "memory")).lower(),
            memory_backend=str(data.get("memory_backend", "memory")).lower(),
            summary_backend=str(data.get("summary_backend", data.get("session_backend", "memory"))).lower(),
            knowledge_backend=str(data.get("knowledge_backend", "vector")).lower(),
            artifact_backend=str(data.get("artifact_backend", "object")).lower(),
            audit_backend=str(data.get("audit_backend", data.get("memory_backend", "memory"))).lower(),
            redis_url=str(data.get("redis_url", "")),
            sql_dsn=str(data.get("sql_dsn", "")),
            redis_url_ref=str(data.get("redis_url_ref", "")),
            sql_dsn_ref=str(data.get("sql_dsn_ref", "")),
            knowledge_collection=str(data.get("knowledge_collection", "default")),
            external_memory_url=str(data.get("external_memory_url", "")),
            external_memory_token_ref=str(data.get("external_memory_token_ref", "")),
            vector_url=str(data.get("vector_url", "")),
            vector_token_ref=str(data.get("vector_token_ref", "")),
            vector_provider=str(data.get("vector_provider", "qdrant")).lower(),
            vector_dimension=int(data.get("vector_dimension", 64)),
            embedding_url=str(data.get("embedding_url", "")),
            embedding_model=str(data.get("embedding_model", "")),
            embedding_token_ref=str(data.get("embedding_token_ref", "")),
            object_endpoint=str(data.get("object_endpoint", "")),
            object_bucket=str(data.get("object_bucket", "")),
            object_region=str(data.get("object_region", "")),
            object_access_key_ref=str(data.get("object_access_key_ref", "")),
            object_secret_key_ref=str(data.get("object_secret_key_ref", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        values = {
            "session_backend": self.session_backend,
            "memory_backend": self.memory_backend,
            "summary_backend": self.summary_backend,
            "knowledge_backend": self.knowledge_backend,
            "artifact_backend": self.artifact_backend,
            "audit_backend": self.audit_backend,
            # Keep public endpoints, but never persist credentials embedded in
            # a connection URL. Passwords belong in *_ref fields.
            "redis_url": _safe_connection_value(self.redis_url),
            "sql_dsn": _safe_connection_value(self.sql_dsn),
            "redis_url_ref": self.redis_url_ref,
            "sql_dsn_ref": self.sql_dsn_ref,
            "knowledge_collection": self.knowledge_collection,
            "external_memory_url": self.external_memory_url,
            "external_memory_token_ref": self.external_memory_token_ref,
            "vector_url": self.vector_url,
            "vector_token_ref": self.vector_token_ref,
            "vector_provider": self.vector_provider,
            "vector_dimension": self.vector_dimension,
            "embedding_url": self.embedding_url,
            "embedding_model": self.embedding_model,
            "embedding_token_ref": self.embedding_token_ref,
            "object_endpoint": self.object_endpoint,
            "object_bucket": self.object_bucket,
            "object_region": self.object_region,
            "object_access_key_ref": self.object_access_key_ref,
            "object_secret_key_ref": self.object_secret_key_ref,
        }
        return {key: value for key, value in values.items() if value != ""}

    def to_public_dict(self) -> dict[str, Any]:
        """Return backend selection without exposing connection credentials."""
        data = self.to_dict()
        for key in ("redis_url", "sql_dsn", "redis_url_ref", "sql_dsn_ref"):
            if data.get(key):
                data[key] = "[configured]"
        return data


def _safe_connection_value(value: str) -> str:
    """Return a connection endpoint only when it has no embedded secret."""
    if not value:
        return ""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return ""
    if parsed.username or parsed.password:
        return ""
    secret_keys = {"password", "passwd", "token", "secret", "api_key"}
    if any(key.lower() in secret_keys for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
        return ""
    return value


@dataclass(slots=True)
class AgentApp:
    agent_app_id: str
    agent_name: str
    model_config: ModelConfig
    prompt: str = "You are a helpful assistant."
    prompt_ref: str | None = None
    tool_policy: ToolPolicy = field(default_factory=ToolPolicy)
    status: TenantStatus = TenantStatus.ACTIVE
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentApp:
        return cls(
            agent_app_id=str(data["agent_app_id"]),
            agent_name=str(data.get("agent_name", data["agent_app_id"])),
            prompt=str(data.get("prompt", "You are a helpful assistant.")),
            prompt_ref=data.get("prompt_ref"),
            model_config=ModelConfig.from_dict(data["model_config"]),
            tool_policy=ToolPolicy.from_dict(data.get("tool_policy")),
            status=TenantStatus(data.get("status", TenantStatus.ACTIVE)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(slots=True)
class ChannelBinding:
    tenant_id: str
    binding_id: str
    channel: str
    account_id: str
    agent_app_id: str
    token_ref: str | None = None
    secret_ref: str | None = None
    enabled: bool = True
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def allowed_user_ids(self) -> set[str]:
        return {str(value) for value in self.config.get("allowed_user_ids", [])}

    def resolve_user_id(self, external_user_id: str) -> str:
        """Resolve a provider identity to the tenant's internal user identity."""
        mapping = self.config.get("identity_mapping") or self.config.get("user_identity_map") or {}
        if not isinstance(mapping, dict):
            return external_user_id
        external_to_internal = mapping.get("external_to_internal", mapping)
        if not isinstance(external_to_internal, dict):
            return external_user_id
        resolved = external_to_internal.get(external_user_id)
        return str(resolved) if resolved not in (None, "") else external_user_id

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChannelBinding:
        return cls(
            tenant_id=str(data["tenant_id"]),
            binding_id=str(data["binding_id"]),
            channel=str(data["channel"]).lower(),
            account_id=str(data["account_id"]),
            agent_app_id=str(data["agent_app_id"]),
            token_ref=data.get("token_ref"),
            secret_ref=data.get("secret_ref"),
            enabled=bool(data.get("enabled", True)),
            config=dict(data.get("config", {})),
        )


@dataclass(slots=True)
class TenantConfig:
    tenant_id: str
    status: TenantStatus = TenantStatus.ACTIVE
    config_version: int = 1
    isolation_mode: IsolationMode = IsolationMode.SHARED_TABLE
    apps: list[AgentApp] = field(default_factory=list)
    channel_bindings: list[ChannelBinding] = field(default_factory=list)
    storage_profile: StorageProfile = field(default_factory=StorageProfile)
    quota_policy: QuotaPolicy = field(default_factory=QuotaPolicy)
    audit_policy: AuditPolicy = field(default_factory=AuditPolicy)
    gray_release: GrayReleasePolicy = field(default_factory=GrayReleasePolicy)
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TenantConfig:
        tenant_id = str(data["tenant_id"])
        created_at = data.get("created_at", utc_now())
        updated_at = data.get("updated_at", utc_now())
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if isinstance(updated_at, str):
            updated_at = datetime.fromisoformat(updated_at)
        apps = [AgentApp.from_dict(app) for app in data.get("apps", [])]
        bindings = []
        for binding in data.get("channel_bindings", []):
            binding = dict(binding)
            binding.setdefault("tenant_id", tenant_id)
            binding.setdefault(
                "binding_id", f"{binding.get('channel', 'channel')}:{binding.get('account_id', 'default')}"
            )
            bindings.append(ChannelBinding.from_dict(binding))
        return cls(
            tenant_id=tenant_id,
            status=TenantStatus(data.get("status", TenantStatus.ACTIVE)),
            config_version=int(data.get("config_version", 1)),
            isolation_mode=IsolationMode(data.get("isolation_mode", IsolationMode.SHARED_TABLE)),
            apps=apps,
            channel_bindings=bindings,
            storage_profile=StorageProfile.from_dict(data.get("storage_profile")),
            quota_policy=QuotaPolicy.from_dict(data.get("quota_policy")),
            audit_policy=AuditPolicy.from_dict(data.get("audit_policy")),
            gray_release=GrayReleasePolicy.from_dict(data.get("gray_release")),
            created_at=created_at,
            updated_at=updated_at,
        )

    def app(self, agent_app_id: str) -> AgentApp:
        for app in self.apps:
            if app.agent_app_id == agent_app_id:
                return app
        raise KeyError(f"agent app not found: {agent_app_id}")

    def channel_binding(self, channel: str, account_id: str) -> ChannelBinding:
        channel = channel.lower()
        for binding in self.channel_bindings:
            if binding.channel == channel and binding.account_id == account_id and binding.enabled:
                return binding
        raise KeyError(f"channel binding not found: {channel}/{account_id}")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        data["isolation_mode"] = self.isolation_mode.value
        data["storage_profile"] = self.storage_profile.to_dict()
        data["created_at"] = self.created_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        return data

    def to_public_dict(self) -> dict[str, Any]:
        data = self.to_dict()
        data["storage_profile"] = self.storage_profile.to_public_dict()
        for app in data.get("apps", []):
            app["model_config"] = _public_config_value(app.get("model_config", {}))
            app["metadata"] = _public_config_value(app.get("metadata", {}))
        for binding in data.get("channel_bindings", []):
            if binding.get("token_ref"):
                binding["token_ref"] = "[configured]"
            if binding.get("secret_ref"):
                binding["secret_ref"] = "[configured]"
            binding["config"] = _public_config_value(binding.get("config", {}))
        return data

    def immutable_snapshot(self):
        """Return a strict frozen representation for publication and audit."""
        from trpc_service.tenant.immutable import ImmutableTenantConfig

        return ImmutableTenantConfig.from_config(self)


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str
    agent_app_id: str
    config_version: int
    trace_id: str
    session_id: str | None = None
    channel: str | None = None
    user_id: str | None = None
    traceparent: str | None = None


@dataclass(slots=True)
class UserInput:
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RunRequest:
    tenant_context: TenantContext
    user_input: UserInput
    idempotency_key: str


@dataclass(slots=True)
class AgentEvent:
    event_type: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


def default_demo_config() -> TenantConfig:
    import os

    app = AgentApp(
        agent_app_id="app_support",
        agent_name="support-agent",
        model_config=ModelConfig(
            provider="openai-compatible",
            model=os.getenv("CPA_MODEL", ""),
            timeout_ms=int(os.getenv("CPA_TIMEOUT_MS", "60000")),
            base_url=os.getenv("CPA_BASE_URL", ""),
            api_key_env=os.getenv("CPA_API_KEY_ENV", "OPENAI_API_KEY"),
            api_key_ref=os.getenv("CPA_API_KEY_REF", ""),
            wire_api=os.getenv("CPA_WIRE_API", "responses"),
            max_output_tokens=int(os.getenv("CPA_MAX_OUTPUT_TOKENS", "1024")),
            cost_per_1k_tokens=float(os.getenv("MODEL_COST_PER_1K_TOKENS", "0")),
        ),
        tool_policy=ToolPolicy(
            allowlist=["search_knowledge"],
            approval_rules=["send_external_message"],
        ),
    )
    default_channels = ["feishu", "telegram"]
    if os.getenv("ENABLE_LEGACY_WECOM", "0").lower() in {"1", "true", "yes", "on"}:
        default_channels.append("wecom")
    bindings = [
        ChannelBinding(
            tenant_id="tenant_demo",
            binding_id=f"{channel}:corp_account_1",
            channel=channel,
            account_id="corp_account_1",
            agent_app_id="app_support",
            token_ref=f"secret://tenant_demo/{channel}/token",
            secret_ref=f"secret://tenant_demo/{channel}/secret",
            config=(
                {
                    "app_id": os.getenv("FEISHU_APP_ID", ""),
                    "app_secret_ref": os.getenv("FEISHU_APP_SECRET_REF", ""),
                    "encrypt_key_ref": os.getenv("FEISHU_ENCRYPT_KEY_REF", ""),
                }
                if channel == "feishu"
                else {}
            ),
        )
        for channel in default_channels
    ]
    bindings.append(
        ChannelBinding(
            tenant_id="tenant_demo",
            binding_id="web:web_demo",
            channel="web",
            account_id="web_demo",
            agent_app_id="app_support",
        )
    )
    ai_bot_account_id = os.getenv("WECOM_AI_BOT_ACCOUNT_ID", "").strip()
    if ai_bot_account_id:
        ai_bot_secret_ref = os.getenv(
            "WECOM_AI_BOT_SECRET_REF",
            "secret://tenant_demo/wecom_ai_bot/bot-secret",
        )
        bindings.append(
            ChannelBinding(
                tenant_id="tenant_demo",
                binding_id=f"wecom_ai_bot:{ai_bot_account_id}",
                channel="wecom_ai_bot",
                account_id=ai_bot_account_id,
                agent_app_id="app_support",
                secret_ref=ai_bot_secret_ref,
                config={"bot_secret_ref": ai_bot_secret_ref},
            )
        )
    return TenantConfig(
        tenant_id="tenant_demo",
        apps=[app],
        channel_bindings=bindings,
        storage_profile=StorageProfile(
            session_backend=os.getenv("DEFAULT_SESSION_BACKEND", "memory"),
            memory_backend=os.getenv("DEFAULT_MEMORY_BACKEND", "memory"),
            summary_backend=os.getenv("DEFAULT_SUMMARY_BACKEND", os.getenv("DEFAULT_SESSION_BACKEND", "memory")),
            knowledge_backend=os.getenv("DEFAULT_KNOWLEDGE_BACKEND", "vector"),
            artifact_backend=os.getenv("DEFAULT_ARTIFACT_BACKEND", "object"),
            audit_backend=os.getenv(
                "DEFAULT_AUDIT_BACKEND",
                os.getenv("DEFAULT_MEMORY_BACKEND", "memory"),
            ),
            redis_url_ref="env://REDIS_URL",
            sql_dsn_ref="env://POSTGRES_DSN",
        ),
    )
