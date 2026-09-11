"""Per-application protected clients for the existing offline migration workflow."""

from .protected_session import ProtectedSessionService


class ProtectedMigrationStore:

    def __init__(self, kind, url, allowed_apps):
        self.kind, self.url = kind, url
        self.allowed_apps, self.services = frozenset(allowed_apps), {}

    async def service(self, app_name):
        if app_name not in self.allowed_apps:
            raise PermissionError("migration application is outside its tenant")
        if app_name not in self.services:
            native = getattr(ProtectedSessionService, self.kind)(self.url, app_name)
            try:
                await native.initialize()
            except BaseException:
                await native.close()
                raise
            self.services[app_name] = native
        return self.services[app_name]

    async def get_session(self, **identity):
        return await (await self.service(identity["app_name"])).get_session(**identity)

    async def close(self):
        for service in self.services.values():
            await service.close()
