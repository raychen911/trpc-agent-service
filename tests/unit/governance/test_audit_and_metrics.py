from uuid import uuid4
from trpc_service.audit.models import AuditRecord, AuditDecision
from trpc_service.channels.contracts import Channel
from trpc_service.metrics.shared import SharedMetricsRecorder
from trpc_service.audit.models import TenantScope
from datetime import datetime, timezone

def test_audit_has_governance_summary_without_raw_subject():
    r=AuditRecord(audit_id=uuid4(),trace_id=uuid4(),tenant_id='tenant-a',channel=Channel.FEISHU,binding_id_digest='sha256:'+'a'*64,user_id='sha256:'+'b'*64,decision=AuditDecision.AUTHORIZED,latency_ms=0,created_at=datetime.now(timezone.utc),policy_version=1,principal_digest='c'*64,governance_decision='allow')
    assert r.policy_version == 1 and 'raw' not in r.model_dump()

def test_shared_metrics_labels_are_bounded():
    m=SharedMetricsRecorder(node_id='node-a'); m.observe_trace(TenantScope(tenant_id='tenant-a'),backend='redis',outcome='allow',first_trace=None,owner_trace=None,execution_trace=None,generation=1); assert 'tenant-a' not in str(m.events)
