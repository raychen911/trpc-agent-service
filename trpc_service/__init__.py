# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Multi-tenant service layer built on tRPC-Agent-Python.

This package adds tenant modelling, tenant-scoped storage, governance filters,
audit logging and observability helpers on top of the core framework without
modifying any existing module. Tenant isolation is achieved by namespacing the
framework's existing ``app_name`` scope with the tenant id.
"""

from .agent._queue import StreamQueue
from .agent._queue import TaskMessage
from ._utils import scope_key
from ._utils import to_agent_name
from .metrics._observability import TENANT_ID_ATTRIBUTE
from .metrics._observability import attach_tenant_to_span
from .metrics._observability import callback_span
from .metrics._observability import configure_telemetry
from .metrics._observability import current_trace_id
from .metrics._observability import extracted_trace_context
from .metrics._observability import inject_trace_headers
from .metrics._observability import tenant_attributes
from .metrics._observability import storage_span
from .metrics import EnterpriseMetrics
from .metrics import get_enterprise_metrics
from .agent._fallback_model import FallbackLLMModel
from .log import AuditLogEntry
from .log import AuditLogger
from .log import SqlAuditSink
from .log import RedactingLogFilter
from .log import SecretMasker
from .log import install_redacting_log_filter
from .log import safe_error_message
from .web.admin import create_admin_router
from .channels import CHAT_GROUP
from .channels import CHAT_PRIVATE
from .channels import ChannelAdapter
from .channels import InboundMessage
from .channels import OutboundMessage
from .channels import SendResult
from .channels import DingTalkAdapter
from .channels import FeishuAdapter
from .channels import QQAdapter
from .channels import WecomAdapter
from .channels import WechatCustomerServiceAdapter
from .channels import generate_session_id
from .web.gateway import ChannelRegistry
from .web.gateway import create_gateway_app
from .tool import BudgetExceededError
from .tool import BudgetTracker
from .tool import RedisBudgetTracker
from .tool import ConfirmationManager
from .tool import ChannelUserAuthorizationFilter
from .tool import RedisConfirmationManager
from .tool import ModelBudgetFilter
from .tool import ModelPricing
from .tool import SensitiveDataRedactor
from .tool import TenantResolver
from .tool import ToolAllowlistFilter
from .tool import ToolConfirmationRequired
from .tool import ToolOutputRedactionFilter
from .tool import ToolCallLimitFilter
from .tool import build_governance_filters
from .tool import apply_tenant_governance
from .workspace import TenantMemoryService
from .workspace import TenantObjectStore
from .workspace import TenantBackendMigrationAdapter
from .workspace import TenantDataMigrator
from .workspace import TenantSessionService
from .workspace import TenantStorageRouter
from .workspace import TenantVectorStore
from .workspace import VectorMatch
from .workspace import VectorRecord
from .workspace import VectorStoreABC
from .workspace import ObjectInfo
from .workspace import ObjectStoreABC
from .workspace import QdrantVectorStore
from .workspace import S3CompatibleObjectStore
from .workspace import DualWriteBackend
from .workspace import LocalMigrationBackend
from .workspace import MigrationBackend
from .workspace import MigrationReport
from .workspace import StorageMigrator
from .workspace import StorageRecord
from .tenant import BaseChannelConfig
from .tenant import ChannelConfig
from .tenant import DingTalkChannelConfig
from .tenant import FeishuChannelConfig
from .tenant import ObjectBackendConfig
from .tenant import QQChannelConfig
from .tenant import Tenant
from .tenant import TenantConfigManager
from .tenant import build_tenant_config_manager
from .tenant import TenantStatus
from .tenant import ToolPermissions
from .tenant import VectorBackendConfig
from .tenant import WeComChannelConfig
from .tenant import WechatCustomerServiceChannelConfig
from .tenant import load_tenants
from .agent import StreamWorker
from .agent import TenantWorker
from .agent import LocalSessionLockManager
from .agent import RedisSessionLockManager
from .agent import LocalTaskResultStore
from .agent import RedisTaskResultStore

__all__ = [
    "TENANT_ID_ATTRIBUTE",
    "attach_tenant_to_span",
    "callback_span",
    "configure_telemetry",
    "current_trace_id",
    "extracted_trace_context",
    "inject_trace_headers",
    "scope_key",
    "tenant_attributes",
    "storage_span",
    "to_agent_name",
    "EnterpriseMetrics",
    "get_enterprise_metrics",
    "FallbackLLMModel",
    "AuditLogEntry",
    "AuditLogger",
    "SqlAuditSink",
    "RedactingLogFilter",
    "SecretMasker",
    "install_redacting_log_filter",
    "safe_error_message",
    "create_admin_router",
    "CHAT_GROUP",
    "CHAT_PRIVATE",
    "ChannelAdapter",
    "ChannelRegistry",
    "InboundMessage",
    "OutboundMessage",
    "SendResult",
    "StreamQueue",
    "TaskMessage",
    "DingTalkAdapter",
    "FeishuAdapter",
    "QQAdapter",
    "WecomAdapter",
    "WechatCustomerServiceAdapter",
    "create_gateway_app",
    "generate_session_id",
    "BudgetExceededError",
    "BudgetTracker",
    "BaseChannelConfig",
    "ChannelConfig",
    "RedisBudgetTracker",
    "ConfirmationManager",
    "ChannelUserAuthorizationFilter",
    "RedisConfirmationManager",
    "ModelBudgetFilter",
    "ModelPricing",
    "SensitiveDataRedactor",
    "TenantResolver",
    "ToolAllowlistFilter",
    "ToolConfirmationRequired",
    "ToolOutputRedactionFilter",
    "ToolCallLimitFilter",
    "build_governance_filters",
    "apply_tenant_governance",
    "TenantMemoryService",
    "TenantObjectStore",
    "TenantBackendMigrationAdapter",
    "TenantDataMigrator",
    "TenantSessionService",
    "TenantStorageRouter",
    "TenantVectorStore",
    "VectorMatch",
    "VectorRecord",
    "VectorStoreABC",
    "ObjectInfo",
    "ObjectStoreABC",
    "QdrantVectorStore",
    "S3CompatibleObjectStore",
    "DualWriteBackend",
    "LocalMigrationBackend",
    "MigrationBackend",
    "MigrationReport",
    "StorageMigrator",
    "StorageRecord",
    "DingTalkChannelConfig",
    "FeishuChannelConfig",
    "ObjectBackendConfig",
    "QQChannelConfig",
    "Tenant",
    "TenantConfigManager",
    "build_tenant_config_manager",
    "TenantStatus",
    "StreamWorker",
    "TenantWorker",
    "LocalSessionLockManager",
    "RedisSessionLockManager",
    "LocalTaskResultStore",
    "RedisTaskResultStore",
    "ToolPermissions",
    "VectorBackendConfig",
    "WeComChannelConfig",
    "WechatCustomerServiceChannelConfig",
    "load_tenants",
]
