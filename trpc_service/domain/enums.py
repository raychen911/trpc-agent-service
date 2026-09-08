from enum import Enum


class StringEnum(str, Enum):
    pass


class TenantStatus(StringEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DISABLED = "disabled"


class AppStatus(StringEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    DISABLED = "disabled"


class ConfigStatus(StringEnum):
    DRAFT = "draft"
    PUBLISHED = "published"


class PermissionEffect(StringEnum):
    ALLOW = "allow"
    DENY = "deny"


class ChannelType(StringEnum):
    WECOM = "wecom"
    TELEGRAM = "telegram"
    WECHAT_OFFICIAL = "wechat_official"
    HTTP = "http"


class BackendKind(StringEnum):
    SESSION = "session"
    MEMORY = "memory"
    SUMMARY = "summary"
    KNOWLEDGE = "knowledge"
    ARTIFACT = "artifact"
    AUDIT = "audit"
