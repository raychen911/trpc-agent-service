"""Recheck current IM grants immediately before execution, independent of cached configuration."""

from trpc_service.persistence.models import IMMembershipRow, TenantRow, ChannelBindingRow


def require_im_execution(database, route, expected_epoch):
    with database.sessions() as session:
        require_im_execution_in_session(session, route, expected_epoch)


def require_im_execution_in_session(session, route, expected_epoch):
    tenant = session.get(TenantRow, route.tenant_id)
    binding = session.get(ChannelBindingRow, (route.tenant_id, route.channel_binding_id))
    member = session.get(IMMembershipRow, (route.tenant_id, route.channel_binding_id, route.actor_id))
    if (tenant is None or tenant.status != "active" or binding is None or not binding.enabled
            or binding.agent_app_id != route.agent_app_id or member is None or not member.active
            or member.authz_epoch != expected_epoch or "chat" not in member.grants_json):
        raise PermissionError("IM execution authorization is no longer valid")
