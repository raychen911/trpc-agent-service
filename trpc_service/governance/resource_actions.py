"""Approved immutable file versions, scoped by the persisted IM invocation."""

from sqlalchemy import select
from trpc_agent_sdk.abc import ArtifactId
from trpc_agent_sdk.types import Part

from trpc_service.persistence.models import PendingActionRow
from trpc_service.management import ConfigurationStore
from trpc_service.storage.runtime_resources import build_resources
from trpc_service.telemetry.runtime import operation
from .action_worker import ActionDefinition
from .actions import ActionService


def prepare_write(arguments):
    if set(arguments) != {"filename", "text"}:
        raise ValueError("file write requires filename and text")
    filename, text = arguments["filename"], arguments["text"]
    if (not isinstance(filename, str) or not 1 <= len(filename) <= 255
            or any(value in filename for value in ("/", "\\", "..")) or not isinstance(text, str)
            or len(text.encode()) > 100_000):
        raise ValueError("invalid bounded text file")
    return dict(arguments), {
        "operation": "append_immutable_version"
    }, {
        "operation": "保存文件新版本",
        "target": filename,
        "impact": "保留已有版本，写入仅限当前会话"
    }


def resource_catalog(database):
    store = ConfigurationStore(database)
    catalog = {}
    for tenant in store.active():
        tenant_id = tenant.tenant_id

        async def check(arguments, preconditions):
            return prepare_write(arguments)[1] == preconditions

        async def execute(arguments, preconditions, action_id, tenant_id=tenant_id):
            if prepare_write(arguments)[1] != preconditions:
                raise PermissionError("file operation differs from approved precondition")
            with database.sessions() as session:
                action = session.scalar(
                    select(PendingActionRow).where(PendingActionRow.tenant_id == tenant_id,
                                                   PendingActionRow.action_id == action_id))
                if action is None or action.status != "executing":
                    raise PermissionError("file write requires an executing approval")
                parent = ActionService.parent(session, tenant_id, action.execution_id)
                route = dict(parent.payload_json["route"])
            config = store.published(tenant_id, route["config_version"])
            resources = await build_resources(store, config, database)
            try:
                identity = ArtifactId(app_name=tenant_id + ":" + route["agent_app_id"],
                                      user_id=route["internal_user_id"],
                                      session_id=route["session_id"],
                                      filename=arguments["filename"])
                with operation("object.upload"):
                    version = await resources.artifact.save_artifact(artifact_id=identity,
                                                                     artifact=Part.from_text(text=arguments["text"]),
                                                                     metadata={"action_id": action_id})
                return {"filename": arguments["filename"], "version": version}
            finally:
                await resources.close()

        definition = ActionDefinition("write_artifact", "v1", prepare_write, check, execute)
        catalog[(tenant_id, definition.name, definition.revision)] = definition
    return catalog
