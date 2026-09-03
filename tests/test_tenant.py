"""
Unit tests for Tenant Manager and Tenant Context.
"""

import pytest
from trpc_service.config.models import TenantConfig, AgentAppConfig, ChannelBindingConfig, ChannelType
from trpc_service.tenant.manager import TenantManager, TenantContext


def test_tenant_registration():
    mgr = TenantManager()
    tenant = TenantConfig(tenant_id="dept_hr", name="HR Department")
    registered = mgr.register_tenant(tenant)

    assert registered.tenant_id == "dept_hr"
    assert mgr.get_tenant("dept_hr") is not None
    assert len(mgr.list_tenants()) == 1


def test_agent_app_registration():
    mgr = TenantManager()
    tenant = TenantConfig(tenant_id="dept_it", name="IT Dept")
    mgr.register_tenant(tenant)

    app = AgentAppConfig(
        app_id="app_it_helper",
        tenant_id="dept_it",
        name="IT Helper",
        allowed_tools=["calculator", "knowledge_search"],
    )
    mgr.register_app(app)

    retrieved = mgr.get_app("app_it_helper")
    assert retrieved is not None
    assert retrieved.allowed_tools == ["calculator", "knowledge_search"]
    assert len(mgr.get_tenant_apps("dept_it")) == 1


def test_quota_tracking():
    mgr = TenantManager()
    tenant = TenantConfig(tenant_id="dept_sales", name="Sales Dept")
    tenant.quota_policy.daily_token_budget = 1000
    mgr.register_tenant(tenant)

    assert mgr.check_quota("dept_sales") is True
    mgr.record_token_usage("dept_sales", 600)
    assert mgr.check_quota("dept_sales") is True
    mgr.record_token_usage("dept_sales", 500)
    assert mgr.check_quota("dept_sales") is False


def test_tenant_context():
    tokens = TenantContext.set_context("tenant_abc", "trace_123", "user_456")
    assert TenantContext.get_tenant_id() == "tenant_abc"
    assert TenantContext.get_trace_id() == "trace_123"
    assert TenantContext.get_user_id() == "user_456"

    TenantContext.reset_context(tokens)
    assert TenantContext.get_tenant_id() is None
