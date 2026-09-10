from trpc_service.observability.operational import OperationalEvent

def test_operational_event_allowlist_excludes_business_content():
    # Phase-8 hardening: digests are sha256-referenced, never raw values.
    event=OperationalEvent(component="data",operation="read",error_type="audit_unavailable",retryable=True,trace_digest="sha256:"+"a"*16)
    assert event.to_dict().keys() == {"component","operation","error_type","retryable","trace_digest"}
