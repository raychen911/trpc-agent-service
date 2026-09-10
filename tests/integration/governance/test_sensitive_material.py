from trpc_service.governance.content import inspect
def test_sensitive_material_never_in_finding():
    x=inspect('secret=abc',rules={'token':'redact'}); assert 'abc' not in str(x.findings)
