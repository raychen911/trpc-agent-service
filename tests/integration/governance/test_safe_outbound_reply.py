from trpc_service.governance.content import inspect, InspectionAction
def test_unsafe_outbound_is_rejected():
    assert inspect('token=abc',rules={'token':'reject'}).action == InspectionAction.REJECT
