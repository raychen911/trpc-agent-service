# ===================================================================
# bootstrap - 网关装配层（从 _cli.py 抽出，可脱离 CLI 单测）
# ===================================================================
# 说明: CLI 的职责是「参数解析 + 调用装配」；此前装配逻辑（存储后端选择 /
#   Runner 构建 / 租户注册表 / 广播器 / 限流器 / 预算器 / FastAPI 组装）与
#   argparse 混在同一个 274 行函数里且零测试覆盖——装配错误只能靠真实启动
#   暴露。本模块把「组装出可运行的 Gateway」收敛为纯装配入口
#   build_gateway()，CLI 与未来其他入口（脚本 / 测试）共用。
# 规范: 本模块不做参数解析、不起 uvicorn；打印仅限启动诊断信息。
# ===================================================================

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Optional

from .config.settings import PlatformSettings

DEMO_MODEL_BY_RUNNER: dict[str, dict] = {
    "mock": {
        "provider": "mock",
        "model_name": "mock"
    },
    "framework": {
        "provider": "deepseek",
        "model_name": "deepseek-chat",
        # 单价（USD / 每百万 token，按 deepseek-chat 官网价配置，可经 Admin 调整）:
        # 预算成本闭环演示用（PRD 4.2/6-10）；价格仅影响成本统计，不影响调用。
        "input_price_per_1m_usd": 0.27,
        "output_price_per_1m_usd": 1.10,
    },
}
"""演示租户的模型配置，随 --runner 切换。

framework 模式不写死 api_key —— 由 model_factory 从环境变量
DEEPSEEK_API_KEY 注入（PRD 4.5 密钥不落库）。
"""


def demo_tenant_dict(runner_kind: str, demo_backends: dict, im_channels: Optional[list[dict]] = None) -> dict:
    """内置 demo 租户配置（load_tenant 兜底与首启自动播种共用同一份）。

    Args:
        runner_kind: mock / framework，决定内置模型配置
        demo_backends: 随启动存储模式生成的数据后端字典
        im_channels: 可选 IM 通道列表；缺省仅 web 通道 + env 注入通道
    """
    channels = im_channels if im_channels is not None else im_channels_from_env()
    return {
        "tenant_id": "demo",
        "name": "演示租户",
        "status": "active",
        "app": {
            "agent_type": "llm",
            "system_prompt": "你是 Teneuris 演示助手。"
        },
        "model": DEMO_MODEL_BY_RUNNER[runner_kind],
        "tools": {
            "allowlist": ["echo", "get_time", "calculator", "web_search", "knowledge_search"],
            "dangerous_tools": ["delete_file"]
        },
        "im": channels,
        "backends": demo_backends,
        "rate_limit_per_min": 60,
    }


async def ensure_demo_tenant(tenant_store: Any, runner_kind: str, demo_backends: dict) -> bool:
    """首启自动播种: 租户存储为空时写入内置 demo 租户（get-or-create 幂等）。

    动机: Admin 与 Gateway 的租户配置统一回源共享 SQL 存储（PRD 5.2），
    空库时 Gateway 回退内置 demo 而 Admin PUT 404「租户不存在」——两侧
    配置源分裂。网关首启即播种，Admin 可直接对 demo 热更新/回滚。
    语义:
    - 仅空库播种: 非空库不补种（尊重 Admin 删除 demo 的运维意图）
    - 多节点并发首启: 两节点同时 list 为空并 create，后到者撞唯一键
      IntegrityError，捕获后放弃即可（对方已播种同款配置）
    - 返回是否实际播种（测试断言用）
    """
    if tenant_store is None:
        return False
    try:
        existing = await tenant_store.list()
    except Exception:  # noqa: BLE001 - 存储异常时回退内置 demo 兜底路径
        return False
    if existing:
        return False
    from .tenant.models import tenant_from_dict

    config = tenant_from_dict(demo_tenant_dict(runner_kind, demo_backends))
    try:
        await tenant_store.create(config)
    except Exception:  # noqa: BLE001 - 并发首启唯一键冲突 = 对方已播种
        return False
    print("[Gateway] 首次启动: demo 租户已自动播种到租户存储（Admin 可直接热更新）")
    return True


async def reconcile_demo_model(tenant_store: Any, runner_kind: str) -> bool:
    """demo 租户模型与启动 Runner 对齐（mock 种子 + framework 启动的修复路径）。

    场景: demo 租户曾在 mock runner 下播种（model.provider=mock），运维切到
    framework runner 重启后，租户回源读到 mock 模型——build_llm_model 无对应
    环境变量注入路径，请求时才崩溃。本修复在启动时把 demo 的 mock 模型
    对齐为 framework 档（DEMO_MODEL_BY_RUNNER）。
    边界: 仅当 provider 仍为 mock（平台种子形态）才动——运营者经 Admin
    自定义过模型的租户（deepseek/openai/...）一律不触碰。
    返回是否实际修复（测试断言用）。
    """
    if tenant_store is None or runner_kind != "framework":
        return False
    try:
        config = await tenant_store.get("demo")
    except Exception:  # noqa: BLE001 - 存储异常时交由运行期显式报错
        return False
    if config is None or config.model.provider != "mock":
        return False
    from .tenant.models import ModelConfig

    config.model = ModelConfig(**DEMO_MODEL_BY_RUNNER["framework"])
    try:
        await tenant_store.update("demo", config)
    except Exception:  # noqa: BLE001 - 更新失败交由运行期显式报错
        return False
    print("[Gateway] demo 租户模型已对齐 framework Runner（原为 mock 种子配置）")
    return True


def feishu_sdk_env_channel() -> Optional[dict]:
    """飞书官方 SDK 长连接通道（feishu_sdk）env 配置；凭证缺失返回 None。

    复用 FEISHU_APP_ID / FEISHU_APP_SECRET；verification_token / encrypt_key
    仅 webhook 形态使用，长连接不需要。注意: 同一飞书应用的事件订阅投递
    方式（长连接 vs webhook）二选一，不可双活（PRD 3.3）。
    """
    import os

    app_id = os.environ.get("FEISHU_APP_ID", "")
    secret = os.environ.get("FEISHU_APP_SECRET", "")
    if not app_id or not secret:
        return None
    return {
        "channel_type": "feishu_sdk",
        "webhook_path": "",
        "app_id": app_id,
        "secret_ref": secret,
        # 兜底发送目标（open_id）；SDK 版回复以入站 chat_id 寻址，此项为预留
        "default_target": os.environ.get("FEISHU_DEMO_USER_OPEN_ID", ""),
    }


def wecom_bot_env_channel() -> Optional[dict]:
    """企微智能机器人（长连接）env 通道配置；凭证缺失返回 None。

    bot_id/secret 来源: 企微工作台 -> 智能机器人 -> API 模式凭证。
    注意: 同一 bot 的长连接只允许单实例在线（PRD 3.3 第二接入形态）。
    """
    import os

    bot_id = os.environ.get("WECOM_BOT_ID", "")
    secret = os.environ.get("WECOM_BOT_SECRET", "")
    if not bot_id or not secret:
        return None
    return {
        "channel_type": "wecom_bot",
        "webhook_path": "",
        "app_id": bot_id,
        "secret_ref": secret,
    }


def im_channels_from_env() -> list[dict]:
    """从环境变量读取已真连通道配置（.env / 密钥仓库注入）。

    企微: WECOM_CORP_ID + WECOM_AGENT_ID + WECOM_SECRET + WECOM_TOKEN
       + WECOM_AES_KEY；飞书: FEISHU_APP_ID + FEISHU_APP_SECRET。
    密钥不落库、不入日志（PRD 4.5），仅注入演示租户的 im 配置。
    """
    channels: list[dict] = []
    import os

    if os.environ.get("WECOM_CORP_ID") and os.environ.get("WECOM_SECRET"):
        channels.append({
            "channel_type": "wechat_work",
            "webhook_path": "/webhook/wechat_work/demo__wecom",
            "app_id": os.environ["WECOM_CORP_ID"],
            "agent_id": os.environ.get("WECOM_AGENT_ID", ""),
            "token_ref": os.environ.get("WECOM_TOKEN", ""),
            "secret_ref": os.environ["WECOM_SECRET"],
            "aes_key_ref": os.environ.get("WECOM_AES_KEY", ""),
        })
    if os.environ.get("FEISHU_APP_ID") and os.environ.get("FEISHU_APP_SECRET"):
        channels.append({
            "channel_type": "feishu",
            "webhook_path": "/webhook/feishu/demo__feishu",
            "app_id": os.environ["FEISHU_APP_ID"],
            "token_ref": os.environ.get("FEISHU_VERIFICATION_TOKEN", ""),
            "secret_ref": os.environ["FEISHU_APP_SECRET"],
            "aes_key_ref": os.environ.get("FEISHU_ENCRYPT_KEY", ""),
            # 默认发送目标（1-on-1 收件人）；老师换测试账号只改 .env 的 FEISHU_DEMO_USER_OPEN_ID
            "default_target": os.environ.get("FEISHU_DEMO_USER_OPEN_ID", ""),
        })
    bot_channel = wecom_bot_env_channel()
    if bot_channel is not None:
        channels.append(bot_channel)
    feishu_sdk_channel = feishu_sdk_env_channel()
    if feishu_sdk_channel is not None:
        channels.append(feishu_sdk_channel)
    return channels


@dataclass
class GatewayBundle:
    """build_gateway 的装配产物（CLI 据此起服务与收尾清理）。"""

    app: Any
    """FastAPI 应用（app.state.chain / chain_ctx / runtime 供长连接复用）。"""
    registry: Any
    storage: Any
    storage_manager: Optional[Any]
    channel_factory: Any
    broadcaster: Optional[Any]
    runner_kind: str


async def build_gateway(settings: PlatformSettings, storage_backend: str, runner_kind: str) -> GatewayBundle:
    """按配置装配出可运行的 Gateway（存储 / Runner / 注册表 / FastAPI）。

    Args:
        settings: 平台配置（_cli._load_settings_or_exit 产物）
        storage_backend: "redis"（生产）或 "inmemory"（单机开发）
        runner_kind: "framework"（真实 LLM）或 "mock"（本地回声）

    Returns:
        GatewayBundle；调用方负责 server.serve() 与 finally 清理。
    """
    from .channels.factory import ChannelFactory
    from .metrics.metrics import get_metrics
    from .runtime.runtime import Runtime
    from .runtime.runner import MockAgentRunner
    from .tenant.registry import TenantRegistry
    from .web.app import build_gateway_app

    # 按 storage_backend 选择后端:
    #   inmemory: 单机开发，开箱即用（多节点禁用）
    #   redis:    Session/幂等/锁/Memory 走真实 Redis；租户/审计/Summary 走 SQL
    #             （PRD 2.5 数据模型分域：tenant/audit/summary → SQL 表）
    factory = None
    storage_manager = None
    redis_client = None
    sql_engine = None
    if storage_backend == "redis":
        import redis.asyncio as aioredis

        from .storage import StorageFactory
        from .storage.manager import StorageManager
        from .storage.sql_store import create_sql_engine
        from .tenant.models import DataBackendConfig

        redis_client = aioredis.Redis.from_url(settings.storage.redis.dsn)
        try:
            await redis_client.ping()  # 启动即探活，连接失败快速失败
        except Exception as exc:  # noqa: BLE001 - 依赖不可用给出明确指引
            print(f"[Gateway] ❌ Redis 不可达（{settings.storage.redis.dsn}）: {exc}", file=sys.stderr)
            print("   生产部署请先启动 Redis；本地无 Redis 开发可用 --storage inmemory（明确降级）", file=sys.stderr)
            sys.exit(1)
        # SQL 引擎与 Redis 同建：租户配置回源 / 审计 / Summary 持久化
        # （PRD 2.2/2.5；sqlite 本地自测，生产 DSN 换 mysql/pg）
        sql_engine = await create_sql_engine(settings.storage.sql.dsn, settings.storage.sql.echo)
        factory = StorageFactory(redis=redis_client, sql_engine=sql_engine)
        # 共享基础设施兜底（幂等/锁/审计走此实例；ctx.storage）
        platform_cfg = DataBackendConfig(session="redis", memory="redis", summary="sql", audit="sql")
        storage = await factory.create("__platform__", platform_cfg)
        # 按租户懒建（PRD 2.1: 各租户 data_backend_config 真实生效）
        storage_manager = StorageManager(factory)
        # knowledge=redis: 多节点共享知识库（InMemory 各进程独立实例，
        # 多节点下互不可见——联调 2026-09-06 实测根因，PRD 2.1 Knowledge 域）
        demo_backends = {
            "session": "redis",
            "memory": "redis",
            "summary": "sql",
            "audit": "sql",
            "knowledge": "redis",
        }
        print(f"[Gateway] 存储后端: Redis({settings.storage.redis.dsn}) + SQL({settings.storage.sql.dsn})")
    else:
        from .storage.inmemory import InMemoryStorage

        storage = InMemoryStorage()
        demo_backends = {"session": "inmemory", "memory": "inmemory", "summary": "inmemory", "audit": "inmemory"}
        print("[Gateway] 存储后端: InMemory（单机开发，多节点请用 --storage redis）")

    summarizer = None
    if runner_kind == "framework":
        from .runtime.runner import FrameworkAgentRunner

        # 启动预检: 缺 api_key 立即退出，不等到首请求才报错，
        # 也绝不静默回落到 mock（阶段二 Spec 用例 6）
        from .agent.model_factory import build_llm_model
        from .agent.summarizer import LlmSummarizer
        from .tenant.models import ModelConfig

        try:
            build_llm_model(ModelConfig(**DEMO_MODEL_BY_RUNNER["framework"]))
        except ValueError as exc:
            print(f"[配置错误] {exc}", file=sys.stderr)
            print("提示: export DEEPSEEK_API_KEY=sk-xxx，或改用 --runner mock", file=sys.stderr)
            sys.exit(1)
        runner = FrameworkAgentRunner(storage=storage, storage_manager=storage_manager)
        # LLM 异步摘要与 Agent 同源复用模型工厂（PRD 2.2），失败回落确定性摘要
        summarizer = LlmSummarizer(build_llm_model)
        print("[Gateway] Runner: framework（真实 LLM）")
    else:
        runner = MockAgentRunner()
        print("[Gateway] Runner: mock（本地回声，不调 LLM）")

    registry = TenantRegistry(load_fn=None)

    # 租户配置存储（PRD 5.2 热更新/回滚的回源）: SQL 优先，不可用则 demo 兜底
    tenant_store = None
    try:
        from .storage.sql_store import SqlTenantStore, create_sql_engine

        tenant_sql_engine = await create_sql_engine(settings.storage.sql.dsn, settings.storage.sql.echo)
        tenant_store = SqlTenantStore(tenant_sql_engine)
    except Exception as exc:  # noqa: BLE001 - 租户存储不可用回退内置 demo
        print(f"[Gateway] 警告: 租户 SQL 存储不可用，回退内置 demo 配置（{exc}）")

    async def load_tenant(tenant_id: str) -> Optional[dict]:
        # 存储优先: Admin 热更新/回滚后经广播失效本地缓存，此处回源读取新配置
        if tenant_store is not None:
            config = await tenant_store.get(tenant_id)
            if config is not None:
                return config.model_dump()
        if tenant_id == "demo":
            im_channels = [{"channel_type": "web", "webhook_path": ""}]
            return demo_tenant_dict(runner_kind, demo_backends, im_channels + im_channels_from_env())
        return None

    registry._load_fn = load_tenant

    # 首启自动播种: 空库时把内置 demo 租户落共享存储，消除「Gateway 兜底
    # demo / Admin PUT 404」的配置源分裂窗口（PRD 5.2 热更新回源基础）
    await ensure_demo_tenant(tenant_store, runner_kind, demo_backends)
    await reconcile_demo_model(tenant_store, runner_kind)

    # Redis 可用时建广播器（跨节点配置失效 + 预算累加后失效），
    # 供 BudgetTracker 发布与 CLI 侧 subscribe_loop 订阅共用同一实例。
    broadcaster = None
    if storage_backend == "redis":
        from .tenant.broadcaster import ConfigBroadcaster

        broadcaster = ConfigBroadcaster(redis_client)

    # 租户限流（PRD 4.1）: Redis 可用时多节点共享固定窗口额度（否则各节点
    # 进程内令牌桶会把限额放大 N 倍）。
    limiter = None
    if storage_backend == "redis":
        from .filters.rate_limiter import RedisFixedWindowLimiter

        limiter = RedisFixedWindowLimiter(redis_client)

    # 成本结算（PRD 4.2/6-10）: SQL 原子累加 used_budget_usd + 缓存/广播失效。
    # 注: 首启已自动播种 demo（见 ensure_demo_tenant），demo 亦有持久化行；
    # 仅租户存储不可用时回退内置 demo（无行），预算对回退场景不生效。
    # mock runner 无 token 不产生成本。
    budget_tracker = None
    if tenant_store is not None:
        from .tenant.budget import BudgetTracker

        budget_tracker = BudgetTracker(tenant_store=tenant_store, registry=registry, broadcaster=broadcaster)

    # 就绪探针（/readyz 真实查依赖连通；InMemory 单机开发为空）
    readiness_checks: dict[str, Any] = {}
    if storage_backend == "redis":

        async def _sql_probe() -> None:
            from sqlalchemy import text

            async with sql_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

        readiness_checks = {
            "redis": redis_client.ping,
            "sql": _sql_probe,
        }

    channel_factory = ChannelFactory()
    app = build_gateway_app(
        registry=registry,
        storage=storage,
        runtime=Runtime(registry=registry,
                        storage=storage,
                        metrics=get_metrics(),
                        runner=runner,
                        summarizer=summarizer,
                        budget_tracker=budget_tracker,
                        storage_manager=storage_manager),
        channel_factory=channel_factory,
        storage_manager=storage_manager,
        readiness_checks=readiness_checks,
        limiter=limiter,
    )
    return GatewayBundle(app=app,
                         registry=registry,
                         storage=storage,
                         storage_manager=storage_manager,
                         channel_factory=channel_factory,
                         broadcaster=broadcaster,
                         runner_kind=runner_kind)
