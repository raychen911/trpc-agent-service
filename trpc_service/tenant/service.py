"""Tenant configuration service and validation rules."""

from __future__ import annotations

import hashlib
import os
from urllib.parse import parse_qsl, urlsplit

from trpc_service.tenant.models import ChannelBinding, TenantConfig, TenantStatus
from trpc_service.tenant.repository import TenantRepository, TenantRepositoryConflict
from trpc_service.security.secrets import SecretManager
from trpc_service.security.identifiers import validate_identifier


class TenantValidationError(ValueError):
    pass


class TenantConfigConflict(TenantValidationError):
    """The caller attempted to update a stale tenant configuration."""


class TenantService:
    def __init__(self, repository: TenantRepository, secrets: SecretManager | None = None) -> None:
        self.repository = repository
        self.secrets = secrets or SecretManager()

    def validate(self, config: TenantConfig) -> None:
        if not config.tenant_id:
            raise TenantValidationError("tenant_id is required")
        try:
            validate_identifier(config.tenant_id, "tenant_id")
        except ValueError as exc:
            raise TenantValidationError(str(exc)) from exc
        if not config.apps:
            raise TenantValidationError("at least one agent app is required")
        app_ids = {app.agent_app_id for app in config.apps}
        if len(app_ids) != len(config.apps):
            raise TenantValidationError("agent_app_id must be unique per tenant")
        binding_keys: set[tuple[str, str]] = set()
        for binding in config.channel_bindings:
            binding.channel = binding.channel.lower()
            binding_key = (binding.channel, binding.account_id)
            if binding_key in binding_keys:
                raise TenantValidationError(f"channel binding already exists: {binding.channel}/{binding.account_id}")
            binding_keys.add(binding_key)
            try:
                validate_identifier(binding.account_id, "channel account_id")
                validate_identifier(binding.binding_id, "channel binding_id")
            except ValueError as exc:
                raise TenantValidationError(str(exc)) from exc
            if binding.tenant_id != config.tenant_id:
                raise TenantValidationError("channel binding tenant_id mismatch")
            if binding.agent_app_id not in app_ids:
                raise TenantValidationError(f"channel binding targets missing app: {binding.agent_app_id}")
            self._reject_plaintext_secrets(binding.config)
            self._validate_channel_binding_config(binding)
            for secret_field in self._secret_references(binding):
                if secret_field and not secret_field.startswith("secret://"):
                    raise TenantValidationError("channel secrets must be stored as secret:// references")
                if secret_field and os.getenv("VALIDATE_SECRETS", "0") == "1":
                    self.secrets.validate(secret_field)
        all_active = getattr(self.repository, "all_active", lambda: [])()
        for other in all_active:
            if other.tenant_id == config.tenant_id:
                continue
            other_keys = {
                (binding.channel.lower(), binding.account_id) for binding in other.channel_bindings if binding.enabled
            }
            conflict = binding_keys.intersection(other_keys)
            if conflict:
                channel, account_id = sorted(conflict)[0]
                raise TenantValidationError(
                    f"channel binding is already owned by another tenant: " f"{channel}/{account_id}"
                )
        for app in config.apps:
            if not app.model_config.provider or not app.model_config.model:
                raise TenantValidationError("model provider and model are required")
        if config.quota_policy.qps_limit <= 0:
            raise TenantValidationError("qps_limit must be positive")
        for field, value in (
            ("storage_profile.redis_url", config.storage_profile.redis_url),
            ("storage_profile.sql_dsn", config.storage_profile.sql_dsn),
        ):
            if value and self._contains_connection_secret(value):
                raise TenantValidationError(
                    f"plaintext database credentials are not allowed in {field}; use *_url_ref/*_dsn_ref"
                )
        gray = config.gray_release
        if gray.percent < 0 or gray.percent > 100:
            raise TenantValidationError("gray_release.percent must be between 0 and 100")
        if gray.enabled and gray.candidate_version is None:
            raise TenantValidationError("gray_release.candidate_version is required when enabled")
        if gray.candidate_version is not None and gray.candidate_version <= 0:
            raise TenantValidationError("gray_release.candidate_version must be positive")

    @staticmethod
    def _strict_channel_config_enabled() -> bool:
        return os.getenv("STRICT_CHANNEL_CONFIG", "0") == "1"

    @staticmethod
    def _contains_connection_secret(value: str) -> bool:
        try:
            parsed = urlsplit(value)
        except ValueError:
            return True
        if parsed.username or parsed.password:
            return True
        secret_keys = {"password", "passwd", "token", "secret", "api_key"}
        return any(key.lower() in secret_keys for key, _ in parse_qsl(parsed.query, keep_blank_values=True))

    @staticmethod
    def _secret_references(binding: ChannelBinding) -> list[str | None]:
        references: list[str | None] = [binding.token_ref, binding.secret_ref]
        for key, value in binding.config.items():
            if key.endswith("_ref") and value is not None:
                references.append(str(value))
        return references

    def _reject_plaintext_secrets(self, value: object, path: str = "config") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                key_text = str(key).lower()
                next_path = f"{path}.{key}"
                if key_text.endswith("_ref"):
                    continue
                if item and any(
                    marker in key_text for marker in ("token", "secret", "password", "api_key", "authorization")
                ):
                    raise TenantValidationError(f"plaintext secret is not allowed in {next_path}; use secret:// *_ref")
                if key_text in {"webhook_url", "url"} and any(
                    marker in str(item).lower() for marker in ("token=", "key=", "secret=")
                ):
                    raise TenantValidationError(
                        f"secret-bearing URL is not allowed in {next_path}; use secret:// *_ref"
                    )
                self._reject_plaintext_secrets(item, next_path)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                self._reject_plaintext_secrets(item, f"{path}[{index}]")

    def _validate_channel_binding_config(self, binding: ChannelBinding) -> None:
        if not self._strict_channel_config_enabled() or not binding.enabled:
            return
        config = binding.config
        channel = binding.channel.lower()
        if channel == "telegram":
            self._require(binding.token_ref, "telegram.token_ref")
            self._require(binding.secret_ref, "telegram.secret_ref")
        elif channel == "wecom":
            self._require(config.get("corp_id") or config.get("corpid"), "wecom.config.corp_id")
            self._require(config.get("agent_id"), "wecom.config.agent_id")
            self._require(config.get("corp_secret_ref"), "wecom.config.corp_secret_ref")
            self._require(binding.token_ref, "wecom.token_ref")
            self._require(config.get("aes_key_ref"), "wecom.config.aes_key_ref")
        elif channel == "wechat_official_account":
            self._require(config.get("app_id") or config.get("appid"), "wechat_official_account.config.app_id")
            self._require(config.get("app_secret_ref"), "wechat_official_account.config.app_secret_ref")
            self._require(binding.token_ref, "wechat_official_account.token_ref")
            self._require(config.get("aes_key_ref"), "wechat_official_account.config.aes_key_ref")
        elif channel == "wechat_customer_service":
            self._require(
                config.get("access_token_ref") or binding.token_ref,
                "wechat_customer_service.access_token_ref or token_ref",
            )

    @staticmethod
    def _require(value: object, field: str) -> None:
        if value is None or value == "":
            raise TenantValidationError(f"strict channel config missing required field: {field}")

    def create_tenant(self, config: TenantConfig) -> TenantConfig:
        self.validate(config)
        return self.repository.create(config)

    def get_tenant(self, tenant_id: str, version: int | None = None) -> TenantConfig:
        return self.repository.get(tenant_id, version=version)

    def create_config_version(
        self,
        tenant_id: str,
        config: TenantConfig,
        expected_version: int | None = None,
    ) -> TenantConfig:
        if tenant_id != config.tenant_id:
            raise TenantValidationError("path tenant_id and body tenant_id differ")
        self._check_expected_version(tenant_id, expected_version)
        self.validate(config)
        return self._save_version(config, expected_version)

    def check_expected_version(self, tenant_id: str, expected_version: int | None) -> None:
        self._check_expected_version(tenant_id, expected_version)

    def publish(self, tenant_id: str, version: int, expected_version: int | None = None) -> TenantConfig:
        config = self.repository.get(tenant_id, version=version)
        if config.status != TenantStatus.ACTIVE:
            raise TenantValidationError("only active tenant configs can be published")
        try:
            return self.repository.publish(tenant_id, version, expected_version=expected_version)
        except TenantRepositoryConflict as exc:
            raise TenantConfigConflict(str(exc)) from exc

    def rollback(self, tenant_id: str, version: int, expected_version: int | None = None) -> TenantConfig:
        try:
            return self.repository.rollback(tenant_id, version, expected_version=expected_version)
        except TenantRepositoryConflict as exc:
            raise TenantConfigConflict(str(exc)) from exc

    def configure_gray_release(
        self,
        tenant_id: str,
        candidate_version: int | None,
        percent: int,
        session_overrides: dict[str, int] | None = None,
        enabled: bool = True,
        expected_version: int | None = None,
    ) -> TenantConfig:
        if percent < 0 or percent > 100:
            raise TenantValidationError("gray release percent must be between 0 and 100")
        current = self.repository.get(tenant_id)
        self._check_expected_version(tenant_id, expected_version, current=current)
        if enabled:
            if candidate_version is None:
                raise TenantValidationError("candidate_version is required when gray release is enabled")
            self.repository.get(tenant_id, version=candidate_version)
        current.gray_release.enabled = enabled
        current.gray_release.candidate_version = candidate_version if enabled else None
        current.gray_release.percent = percent if enabled else 0
        current.gray_release.session_overrides = dict(session_overrides or {})
        self.validate(current)
        return self._save_and_publish(current, expected_version)

    def resolve_runtime_config(self, tenant_id: str, route_key: str) -> TenantConfig:
        active = self.repository.get(tenant_id)
        gray = active.gray_release
        override_version = gray.session_overrides.get(route_key)
        if override_version is not None:
            return self.repository.get(tenant_id, version=override_version)
        if not gray.enabled or gray.candidate_version is None or gray.percent <= 0:
            return active
        if gray.percent >= 100 or self._bucket(tenant_id, route_key) < gray.percent:
            return self.repository.get(tenant_id, version=gray.candidate_version)
        return active

    @staticmethod
    def _bucket(tenant_id: str, route_key: str) -> int:
        digest = hashlib.sha256(f"{tenant_id}:{route_key}".encode("utf-8")).hexdigest()
        return int(digest[:8], 16) % 100

    def add_channel_binding(
        self,
        tenant_id: str,
        binding: ChannelBinding,
        expected_version: int | None = None,
    ) -> TenantConfig:
        current = self.repository.get(tenant_id)
        self._check_expected_version(tenant_id, expected_version, current=current)
        if binding.tenant_id != tenant_id:
            raise TenantValidationError("binding tenant_id mismatch")
        if any(
            item.channel == binding.channel and item.account_id == binding.account_id
            for item in current.channel_bindings
        ):
            raise TenantValidationError(f"channel binding already exists: {binding.channel}/{binding.account_id}")
        current.channel_bindings.append(binding)
        self.validate(current)
        return self._save_and_publish(current, expected_version)

    def resolve_binding(self, channel: str, account_id: str) -> ChannelBinding:
        return self.repository.find_binding(channel, account_id)

    def _check_expected_version(
        self,
        tenant_id: str,
        expected_version: int | None,
        current: TenantConfig | None = None,
    ) -> None:
        if expected_version is None:
            return
        current = current or self.repository.get(tenant_id)
        if current.config_version != expected_version:
            raise TenantConfigConflict(
                f"tenant config changed: expected v{expected_version}, current v{current.config_version}"
            )

    def _save_version(self, config: TenantConfig, expected_version: int | None) -> TenantConfig:
        try:
            return self.repository.save_version(config, expected_version=expected_version)
        except TenantRepositoryConflict as exc:
            raise TenantConfigConflict(str(exc)) from exc

    def _save_and_publish(self, config: TenantConfig, expected_version: int | None) -> TenantConfig:
        try:
            return self.repository.save_and_publish(config, expected_version=expected_version)
        except TenantRepositoryConflict as exc:
            raise TenantConfigConflict(str(exc)) from exc
