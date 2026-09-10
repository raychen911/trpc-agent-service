from trpc_service.governance.content import inspect
def test_content_boundaries_redact_phone_and_token():
    x=inspect('call 13800138000 token=abc123',rules={'phone':'redact','token':'redact'}); assert '13800138000' not in x.safe_text and 'abc123' not in x.safe_text
