import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.config import Settings
from trpc_service.container import ApplicationContainer, build_application_container
from trpc_service.web import create_app
from trpc_service.workspace import LocalWorkspaceProvider, WorkspaceProvider


class _InjectedRunner:

    def __init__(self) -> None:
        self.closed = False

    async def run(self, context, tools):  # type: ignore[no-untyped-def]
        del context, tools
        raise AssertionError("the compatibility Runner must not execute while building the app")

    async def close(self) -> None:
        self.closed = True


def test_default_application_container_owns_extension_registries() -> None:
    # Unit tests opt out of the developer's local .env so this assertion keeps
    # covering the code-level, all-in-memory fallback configuration.
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    container = build_application_container(
        settings=Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:"),
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )

    assert isinstance(container, ApplicationContainer)
    assert container.channels.supported_types == ("feishu", "wecom")
    assert container.storage_backends.names == ("inmemory", )
    assert container.agent_pipeline is not None
    assert container.approvals is not None
    assert isinstance(container.workspace, WorkspaceProvider)
    assert isinstance(container.workspace, LocalWorkspaceProvider)


def test_application_factory_keeps_an_explicit_container() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    settings = Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:")
    container = build_application_container(
        settings=settings,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )
    app = create_app(settings, engine, container)

    assert app.state.container is container
    assert app.state.session_factory is container.session_factory


@pytest.mark.anyio
async def test_composition_root_keeps_an_explicit_agent_runner() -> None:
    """Runner replacement remains at the composition root, outside the Web API."""

    runner = _InjectedRunner()
    settings = Settings(
        _env_file=None,
        database_url="sqlite+aiosqlite:///:memory:",
        auto_create_schema=True,
        worker_concurrency=0,
        delivery_concurrency=0,
    )
    engine = create_async_engine(settings.resolved_database_url)
    container = build_application_container(
        settings=settings,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        agent_runner_factory=lambda _settings, _mcp, _skills: runner,
    )
    app = create_app(settings, engine, container)

    assert app.state.container.agent_runner is runner
    async with app.router.lifespan_context(app):
        pass
    assert runner.closed is True


def test_composition_rejects_missing_required_dependencies() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    settings = Settings(_env_file=None)

    with pytest.raises(TypeError, match="agent_runner_factory"):
        build_application_container(
            settings=settings,
            session_factory=sessions,
            agent_runner_factory=None,  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="session_factory"):
        build_application_container(
            settings=settings,
            session_factory=None,  # type: ignore[arg-type]
        )


@pytest.mark.anyio
async def test_container_close_releases_later_resources_after_an_earlier_failure() -> None:
    """A failed node shutdown must not leak the shared HTTP client."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    container = build_application_container(
        settings=Settings(
            _env_file=None,
            database_url="sqlite+aiosqlite:///:memory:",
            worker_concurrency=0,
            delivery_concurrency=0,
        ),
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )

    with pytest.raises(SQLAlchemyError, match="runtime_node"):
        await container.close()

    assert container.http_client.is_closed is True
    await container.storage_composition.close()
    await engine.dispose()


def test_channel_runtime_composes_wecom_transport_without_agent_slots() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    container = build_application_container(
        settings=Settings(
            _env_file=None,
            database_url="sqlite+aiosqlite:///:memory:",
            runtime_role="channel",
            worker_concurrency=0,
        ),
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )

    assert container.agent_workers is None
    assert container.delivery_workers is not None
    assert container.wecom_supervisor is not None
    assert container.feishu_supervisor is not None
    assert container.channels.supported_types == ("feishu", "wecom")
