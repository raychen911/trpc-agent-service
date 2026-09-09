# ===================================================================
# _cli - 命令行入口
# ===================================================================
# 说明: python -m trpc_service._cli <command>（题目骨架 _cli.py）
#   命令:
#     gateway          启动 Agent Gateway（webhook + Web UI 自测）
#     admin            启动 Admin API（租户管理 / 审计查询）
#     example-config   生成示例配置文件
#     demo-tenant      输出内置 demo 租户配置（供 Admin 创建）
# 规范: 服务启动前先 setup_logging；端口取自 PlatformSettings。
#       CLI 只做参数解析与服务生命周期；装配逻辑在 bootstrap.py
#       （可脱离 CLI 单测，此前内嵌于 274 行 _serve_gateway 且零覆盖）。
# ===================================================================

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, Optional

from .bootstrap import build_gateway, feishu_sdk_env_channel, wecom_bot_env_channel
from .config.loader import ConfigError, load_settings, save_example_config
from .log.logger import setup_logging_from_settings


def _load_settings_or_exit(path: Optional[str]):
    """加载配置，失败时打印错误并退出。"""
    try:
        return load_settings(path)
    except ConfigError as exc:
        print(f"[配置错误] {exc}", file=sys.stderr)
        sys.exit(1)


async def _serve_gateway(config_path: Optional[str],
                         storage_backend: str = "inmemory",
                         runner_kind: str = "mock",
                         wecom_bot: bool = False,
                         feishu_sdk: bool = False) -> None:
    import uvicorn

    settings = _load_settings_or_exit(config_path)
    setup_logging_from_settings(settings)

    # 装配委托 bootstrap（存储后端 / Runner / 租户注册表 / 广播器 / 限流器 /
    # 预算器 / FastAPI 组装均在其内，可独立于 CLI 单测）
    bundle = await build_gateway(settings, storage_backend, runner_kind)
    app = bundle.app
    registry = bundle.registry
    storage = bundle.storage
    storage_manager = bundle.storage_manager
    broadcaster = bundle.broadcaster
    channel_factory = bundle.channel_factory

    cfg = settings.gateway
    print(f"[Gateway] 启动 http://{cfg.host}:{cfg.port}（Web UI: / , webhook: /webhook/...）")
    config = uvicorn.Config(app, host=cfg.host, port=cfg.port, log_level=settings.log_level.lower())
    server = uvicorn.Server(config)

    # 统一停止事件（广播订阅 / 长连接通道共用）
    stop_event = asyncio.Event()
    subscriber: Optional[asyncio.Task] = None
    bot_task: Optional[asyncio.Task] = None
    feishu_task: Optional[asyncio.Task] = None

    # Redis 后端时订阅配置失效通知（Admin 更新/回滚 → 本节点下一请求回源加载）
    if broadcaster is not None:
        subscriber = asyncio.create_task(
            broadcaster.subscribe_loop(registry,
                                       stop_event,
                                       storage_manager=storage_manager,
                                       channel_factory=channel_factory))
        print("[Gateway] 已订阅租户配置变更通知（Redis pub/sub）")

    # 企微智能机器人·长连接（PRD 3.3/3.5 第二接入形态，支持群聊 @）:
    # 与 webhook 共用 process_event 治理链；同一 bot 只允许单副本开启。
    from .runtime.pipeline import process_event
    from .tenant.models import ImChannelConfig

    if wecom_bot:
        bot_cfg = wecom_bot_env_channel()
        if bot_cfg is None:
            print("[Gateway] --wecom-bot 已开启但未配置 WECOM_BOT_ID / WECOM_BOT_SECRET，跳过")
        else:
            from .channels.wecom_bot import WecomBotConnector

            async def bot_processor(event):
                return await process_event(event,
                                           chain=app.state.chain,
                                           ctx=app.state.chain_ctx,
                                           runtime=app.state.runtime)

            connector = WecomBotConnector(tenant_id="demo", config=ImChannelConfig(**bot_cfg), processor=bot_processor)
            bot_task = asyncio.create_task(connector.run(stop_event))
            print("[Gateway] 企微智能机器人长连接启动（demo 租户；同一 bot 请勿多副本开启 --wecom-bot）")

    # 飞书官方 SDK·长连接（feishu_sdk，PRD 3.3 第二实现形态）:
    # 与手写 webhook feishu 代码并存；同一应用事件订阅投递方式（长连接 vs webhook）
    # 二选一，请勿与 webhook 版同时在线。同一应用只允许单副本开启。
    if feishu_sdk:
        fs_cfg = feishu_sdk_env_channel()
        if fs_cfg is None:
            print("[Gateway] --feishu-sdk 已开启但未配置 FEISHU_APP_ID / FEISHU_APP_SECRET，跳过")
        else:
            from .channels.feishu_sdk import FeishuSdkConnector

            async def feishu_processor(event):
                return await process_event(event,
                                           chain=app.state.chain,
                                           ctx=app.state.chain_ctx,
                                           runtime=app.state.runtime)

            connector = FeishuSdkConnector(tenant_id="demo",
                                           config=ImChannelConfig(**fs_cfg),
                                           processor=feishu_processor)
            feishu_task = asyncio.create_task(connector.run(stop_event))
            print("[Gateway] 飞书 SDK 长连接启动（demo 租户；同一应用请勿与 webhook 投递同时在线 --feishu-sdk）")

    try:
        await server.serve()
    finally:
        stop_event.set()
        if subscriber is not None:
            await subscriber
        if bot_task is not None:
            await bot_task
        if feishu_task is not None:
            await feishu_task
        # 排空 Runtime 后台任务（LLM 摘要 / 预算落库），避免进程退出丢写入
        drain = getattr(app.state.runtime, "drain_background_tasks", None)
        if drain is not None:
            await drain()
        # 关闭按租户懒建的 Storage 缓存（连接池释放）
        if storage_manager is not None:
            await storage_manager.close()
        close_storage = getattr(storage, "close", None)
        if close_storage is not None:
            await close_storage()


async def _serve_admin(config_path: Optional[str]) -> None:
    import uvicorn

    settings = _load_settings_or_exit(config_path)
    setup_logging_from_settings(settings)

    from .storage.factory import StorageFactory
    from .storage.sql_store import SqlTenantStore, create_sql_engine
    from .tenant.models import DataBackendConfig
    from .tenant.registry import TenantRegistry
    from .web.admin import build_admin_app

    # 租户 + 审计持久化到 SQL（PRD 2.5 数据模型；sqlite 本地自测，生产接 mysql/pg）
    engine = await create_sql_engine(settings.storage.sql.dsn, settings.storage.sql.echo)
    tenant_store = SqlTenantStore(engine)
    factory = StorageFactory(sql_engine=engine)
    storage = await factory.create("admin", DataBackendConfig(session="inmemory", memory="inmemory", audit="sql"))
    registry = TenantRegistry()
    api_key = settings.admin.api_key.get_secret_value() if settings.admin.api_key else None
    if not api_key:
        # 密钥注入口径与平台一致: 环境变量 TENEURIS_ADMIN_API_KEY（.env / CNB 注入 /
        # 手动 export 均可）或 yaml admin.api_key。缺失时拒绝静默裸奔，显式告警。
        print("[Admin] 警告: 未配置 admin api_key，Admin API 处于无鉴权状态"
              "（生产环境请设置 TENEURIS_ADMIN_API_KEY 或在 yaml 中配置 admin.api_key）")
    # 跨节点失效广播（PRD 5.2）: Redis 不可用时仅本进程 Registry 生效
    broadcaster = None
    try:
        import redis.asyncio as aioredis

        redis_client = aioredis.Redis.from_url(settings.storage.redis.dsn)
        await redis_client.ping()
        from .tenant.broadcaster import ConfigBroadcaster

        broadcaster = ConfigBroadcaster(redis_client)
    except Exception as exc:  # noqa: BLE001 - 广播不可用降级单进程
        print(f"[Admin] 警告: Redis 不可用，租户热更新仅本进程生效（{exc}）")
    app = build_admin_app(storage=storage,
                          registry=registry,
                          api_key=api_key,
                          tenant_store=tenant_store,
                          broadcaster=broadcaster)
    cfg = settings.admin
    print(f"[Admin] 启动 http://{cfg.host}:{cfg.port}（租户管理 / 审计查询）")
    server = uvicorn.Server(uvicorn.Config(app, host=cfg.host, port=cfg.port, log_level=settings.log_level.lower()))
    await server.serve()


def _example_config(path: str) -> None:
    save_example_config(path)
    print(f"[OK] 示例配置已生成: {path}")


def _demo_tenant() -> None:
    demo = {
        "tenant_id": "demo",
        "name": "演示租户",
        "status": "active",
        "app": {
            "agent_type": "llm",
            "system_prompt": "你是 Teneuris 演示助手。"
        },
        "model": {
            "provider": "mock",
            "model_name": "mock"
        },
        "tools": {
            "allowlist": ["echo", "get_time", "calculator", "web_search", "knowledge_search"],
            "dangerous_tools": ["delete_file"]
        },
        "im": [{
            "channel_type": "web",
            "webhook_path": ""
        }],
        "backends": {
            "session": "inmemory",
            "memory": "inmemory",
            "summary": "inmemory",
            "audit": "inmemory"
        },
        "rate_limit_per_min": 60,
    }
    print(json.dumps(demo, ensure_ascii=False, indent=2))


async def _migrate_summaries(config_path: Optional[str], tenant_id: str, source: str, target: str) -> None:
    """Summary 数据域跨后端迁移（PRD 2.3-D 可执行版）: copy + verify。

    用法: migrate-summaries --tenant demo --source inmemory --target sql
    后端: redis / sql(sqlite, 配置 DSN) / inmemory。迁移为回填+校验，
    双写/切读由 Admin 租户 backends 热切完成（StorageManager 已支持）。
    """
    settings = _load_settings_or_exit(config_path)
    setup_logging_from_settings(settings)
    from .storage.migration import copy_summaries, verify_summaries
    from .storage.sql_store import SqlSummaryStore, create_sql_engine

    async def _build_store(kind: str) -> Any:
        if kind == "inmemory":
            from .storage.inmemory import InMemorySummaryStore

            return InMemorySummaryStore()
        if kind == "sql":
            engine = await create_sql_engine(settings.storage.sql.dsn, settings.storage.sql.echo)
            return SqlSummaryStore(engine)
        if kind == "redis":
            import redis.asyncio as aioredis

            from .storage.redis_store import RedisSummaryStore

            client = aioredis.Redis.from_url(settings.storage.redis.dsn)
            return RedisSummaryStore(client)
        raise ValueError(f"不支持的迁移后端: {kind}（可选 redis/sql/inmemory）")

    src = await _build_store(source)
    dst = await _build_store(target)
    try:
        copied = await copy_summaries(src, dst, tenant_id)
        result = await verify_summaries(src, dst, tenant_id)
    finally:
        for store in (src, dst):
            close = getattr(store, "close", None)
            if close is not None:
                await close()
    print(f"[migration] Summary 迁移完成: copied={copied} "
          f"source={result['source']} target={result['target']} "
          f"matched={result['matched']} mismatched={len(result['mismatched'])}")
    if result["mismatched"]:
        print(f"[migration] ⚠️ 差异条目: {result['mismatched'][:10]}")
        print("   重跑本命令可收敛（copy 幂等）；仍不一致请检查两端数据")
        raise SystemExit(2)
    print("[migration] ✅ 迁移校验一致，可在 Admin 中把该租户 summary 后端切换到目标后端")


def _chunk_text(text: str, chunk_size: int) -> list[str]:
    """固定窗口切块（PRD 3.5.4：500 字窗口；生产接 embedding 前处理管线）。"""
    text = text.strip()
    if not text:
        return []
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]


async def _knowledge_add(config_path: Optional[str], tenant_id: str, doc_id: Optional[str], file_path: Optional[str],
                         text: Optional[str], backend: str, chunk_size: int) -> None:
    """知识库文档录入（PRD 2.1 Knowledge 域数据入口）。

    用法: knowledge-add --tenant demo --doc-id tenant-faq --file data/knowledge/tenant-faq.txt
    后端须与租户 backends.knowledge 一致（redis 多节点共享 / inmemory 单机）。
    """
    if not doc_id:
        print("[knowledge] ❌ 需要 --doc-id（文档唯一标识）", file=sys.stderr)
        sys.exit(1)
    if bool(file_path) == bool(text):
        print("[knowledge] ❌ --file 与 --text 二选一", file=sys.stderr)
        sys.exit(1)
    if file_path:
        try:
            with open(file_path, encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            print(f"[knowledge] ❌ 文件读取失败: {exc}", file=sys.stderr)
            sys.exit(1)
    chunks = _chunk_text(text or "", chunk_size)
    if not chunks:
        print("[knowledge] ❌ 文档内容为空", file=sys.stderr)
        sys.exit(1)

    settings = _load_settings_or_exit(config_path)
    setup_logging_from_settings(settings)
    store: Any = None
    client: Any = None
    if backend == "redis":
        import redis.asyncio as aioredis

        from .storage.knowledge_redis import RedisKnowledgeStore

        client = aioredis.Redis.from_url(settings.storage.redis.dsn)
        try:
            await client.ping()
        except Exception as exc:  # noqa: BLE001 - 依赖不可用给出明确指引
            print(f"[knowledge] ❌ Redis 不可达（{settings.storage.redis.dsn}）: {exc}", file=sys.stderr)
            sys.exit(1)
        store = RedisKnowledgeStore(client)
    else:
        from .storage.knowledge_inmemory import InMemoryKnowledgeStore

        store = InMemoryKnowledgeStore()

    hit = False
    try:
        payload = [{
            "id": f"{doc_id}:{i:03d}",
            "content": seg,
            "metadata": {
                "source": file_path or "inline"
            }
        } for i, seg in enumerate(chunks)]
        await store.add_document(tenant_id, doc_id, chunks=payload)
        # 自检: 用首块内容的关键词检索应命中该文档
        probe = await store.search(tenant_id, chunks[0][:32], top_k=3)
        hit = any(h.get("doc_id") == doc_id for h in probe)
    finally:
        if client is not None:
            await client.aclose()

    print(f"[knowledge] 文档已录入: tenant={tenant_id} doc={doc_id} chunks={len(chunks)} backend={backend}")
    if hit:
        print(f"[knowledge] ✅ 检索自检命中（tenant backends.knowledge={backend} 时网关立即可用）")
    else:
        print("[knowledge] ⚠️ 检索自检未命中，请检查租户 backends.knowledge 与 --backend 是否一致")
        sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(prog="trpc_service", description="Teneuris 多租户 AI Agent 平台")
    parser.add_argument(
        "command", choices=["gateway", "admin", "example-config", "demo-tenant", "migrate-summaries", "knowledge-add"])
    parser.add_argument("--config", "-c", default=None, help="配置文件路径（yaml）")
    parser.add_argument("--path", default="config/teneuris.yaml", help="example-config 输出路径")
    parser.add_argument("--tenant", default="demo", help="migrate-summaries / knowledge-add 目标租户")
    parser.add_argument("--source", choices=["redis", "sql", "inmemory"], help="migrate-summaries 源后端")
    parser.add_argument("--target", choices=["redis", "sql", "inmemory"], help="migrate-summaries 目标后端")
    parser.add_argument("--doc-id", help="knowledge-add 文档唯一标识（同 id 重复录入为整体替换）")
    parser.add_argument("--file", help="knowledge-add 文档文件路径（UTF-8 文本）")
    parser.add_argument("--text", help="knowledge-add 内联文档内容（与 --file 二选一）")
    parser.add_argument("--backend",
                        choices=["redis", "inmemory"],
                        default="redis",
                        help="knowledge-add 录入后端（须与租户 backends.knowledge 一致）")
    parser.add_argument("--chunk-size", type=int, default=500, help="knowledge-add 切块窗口（字符，默认 500）")
    parser.add_argument("--storage",
                        default="redis",
                        choices=["inmemory", "redis"],
                        help="gateway 存储后端（默认 redis 生产；本地无 Redis 开发用 --storage inmemory）")
    parser.add_argument("--runner",
                        default="framework",
                        choices=["mock", "framework"],
                        help="gateway 的 Agent Runner（默认 framework 真实 LLM；本地无 key 开发用 --runner mock）")
    parser.add_argument("--wecom-bot", action="store_true", help="启动企微智能机器人长连接（第二接入形态，支持群聊 @；需 WECOM_BOT_ID/SECRET）")
    parser.add_argument("--feishu-sdk",
                        action="store_true",
                        help="启动飞书官方 SDK 长连接通道（feishu_sdk；需 FEISHU_APP_ID/SECRET；同一应用与 webhook 投递二选一）")
    args = parser.parse_args()

    if args.command == "gateway":
        if args.feishu_sdk:
            # lark-oapi 的 ws/client 在「模块导入时」绑定全局事件循环
            # （asyncio.get_event_loop()，ws/client.py:32）。若在 asyncio.run 之后才
            # import（uvicorn loop 运行中），会绑到正在运行的 loop，后台线程
            # run_until_complete 报 "This event loop is already running"（09-02 实测）。
            # 故 --feishu-sdk 时须在 asyncio.run 之前预导入，绑定一个空闲 loop。
            try:
                import lark_oapi.channel  # noqa: F401
                import lark_oapi.ws.client  # noqa: F401
            except ImportError:  # pragma: no cover - 依赖缺失由 connector 再提示
                pass
        asyncio.run(_serve_gateway(args.config, args.storage, args.runner, args.wecom_bot, args.feishu_sdk))
    elif args.command == "admin":
        asyncio.run(_serve_admin(args.config))
    elif args.command == "example-config":
        _example_config(args.path)
    elif args.command == "demo-tenant":
        _demo_tenant()
    elif args.command == "migrate-summaries":
        if not args.source or not args.target:
            parser.error("migrate-summaries 需要 --source 与 --target（redis/sql/inmemory）")
        asyncio.run(_migrate_summaries(args.config, args.tenant, args.source, args.target))
    elif args.command == "knowledge-add":
        asyncio.run(
            _knowledge_add(args.config, args.tenant, args.doc_id, args.file, args.text, args.backend, args.chunk_size))


if __name__ == "__main__":
    main()
