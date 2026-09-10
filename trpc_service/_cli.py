"""命令行入口：python -m trpc_service._cli <command>"""
import typer

app = typer.Typer(help="trpc-agent-service 命令行工具", no_args_is_help=True)


@app.command()
def version():
    """显示版本号"""
    from trpc_service.version import __version__

    typer.echo(f"trpc agent service version: {__version__}")


@app.command()
def tenants():
    """列出全部租户（读 YAML 引导配置，不依赖服务进程）"""
    from trpc_service.config.loader import load_config

    for cfg in load_config().values():
        typer.echo(
            f"{cfg.tenant_id}\t{cfg.name}\t{cfg.app.app_name}\t{cfg.storage.session_backend}"
        )


@app.command()
def serve():
    """启动服务（inline 模式，等价 start.sh）"""
    import uvicorn

    from trpc_service.config.settings import ServerConfig

    s = ServerConfig()
    uvicorn.run("trpc_service.web.app:app", host=s.host, port=s.port)


@app.command()
def worker():
    """启动队列消费 Worker（需 QUEUE_MODE=redis 与 REDIS_URL）"""
    from trpc_service.worker import main

    main()


@app.command()
def migrate():
    """执行 Alembic 迁移到最新版本（平台表建表/变更）"""
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    ini = Path(__file__).resolve().parent.parent / "alembic.ini"
    cfg = Config(str(ini))
    command.upgrade(cfg, "head")
    typer.echo("migration done: upgrade head")


if __name__ == "__main__":
    app()
