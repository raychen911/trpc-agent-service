# ===================================================================
# web.admin - Admin API（租户管理 / 审计查询 / 配置下发）
# ===================================================================
# 说明: 独立部署、内网访问（PRD 0.2），职责:
#   - 租户 CRUD（创建 / 查询 / 更新 / 停用）
#   - 审计日志查询（按 tenant_id + 过滤条件，PRD 4.4）
#   - 配置热更新（写入 registry 缓存 + 存储，PRD 5.2 灰度/回滚）
# 规范: 所有租户数据行级隔离；Admin 接口可选 api_key 鉴权。
# ===================================================================

from __future__ import annotations

import hmac
from typing import Any, Optional
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..log.logger import get_logger
from ..storage.base import Storage
from ..tenant.models import TenantConfig, tenant_from_dict
from ..tenant.registry import TenantRegistry

log = get_logger("web.admin")

MAX_KEEP_VERSIONS = 5
"""每租户保留的历史版本数（回滚用；生产建议持久化版本表，见 stage4 设计）。"""

# 前端模板拆分自本文件（此前以字符串常量内嵌 292 行，
# 视图与接口层耦合且无法 lint/高亮）；见 templates/ 目录。
ADMIN_UI_HTML = (Path(__file__).parent / "templates" / "admin_ui.html").read_text(encoding="utf-8")


class TenantRepository:
    """租户配置的存储读写（生产走 SQL，未配置时回退 InMemory + Registry 缓存）。

    密钥脱敏: 内部以 TenantConfig 对象存储，输出时 model_dump 排除
    api_key_ref / token_ref 等密钥字段（PRD 4.5，禁止明文回显）。
    热更新: 更新/回滚后写 Registry 缓存，并经 ConfigBroadcaster 广播失效
    通知，各 Gateway 节点下一请求回源加载新配置（PRD 5.2，无需重启）。
    """

    # model_dump 排除的密钥字段（顶层 + 嵌套）
    _SECRET_EXCLUDE = {
        "model": {"api_key_ref"},
        "im": {
            "__all__": {"token_ref", "secret_ref", "aes_key_ref"}
        },
    }

    def __init__(
        self,
        storage: Storage,
        registry: TenantRegistry,
        tenant_store: Any = None,
        broadcaster: Any = None,
    ) -> None:
        """Args:
            storage: 审计等存储后端
            registry: 租户配置 LRU 缓存
            tenant_store: SqlTenantStore（SQL 持久化）；None 时回退进程内 dict
            broadcaster: ConfigBroadcaster（跨节点失效通知）；None 时仅本进程生效
        """
        self._storage = storage
        self._registry = registry
        self._tenant_store = tenant_store
        self._broadcaster = broadcaster
        self._tenants: dict[str, TenantConfig] = {}
        self._history: dict[str, list[TenantConfig]] = {}
        """租户配置历史版本（后进先出回滚；仅 Admin 进程内存，重启即失）。"""

    async def _audit(self, tenant_id: str, action: str, operator: Optional[str] = None) -> None:
        """写操作审计留痕（decision=admin_*，payload 含 operator）。

        operator 由调用方逐请求传入（X-Admin-Operator 头）——不用实例字段
        承载请求级状态：TenantRepository 是单例，FastAPI 并发请求在 await
        处交错时实例字段会互相覆盖，导致审计操作人张冠李戴（审查 09-04）。
        失败仅告警不抛错——审计不能因存储故障阻断租户写操作本身。
        """
        try:
            await self._storage.audit.write_log(
                tenant_id, {
                    "channel": "admin",
                    "agent_name": "admin",
                    "decision": action,
                    "error_type": None,
                    "cost": 0.0,
                    "payload": {
                        "operator": operator,
                        "action": action,
                    },
                })
        except Exception as exc:  # noqa: BLE001 - 审计失败不影响主操作
            log.warning("admin audit failed", extra={"tenant_id": tenant_id, "action": action, "error": str(exc)})

    @classmethod
    def _dump(cls, config: TenantConfig) -> dict[str, Any]:
        """输出脱敏后的租户配置（密钥字段排除）。"""
        return config.model_dump(exclude=cls._SECRET_EXCLUDE)

    @classmethod
    def _dump_full(cls, config: TenantConfig) -> dict[str, Any]:
        """非脱敏 dump：仅用作 update 合并基线（不对外输出）。

        审查 09-04 缺陷修复：此前 update 以脱敏 _dump 为合并基线，密钥字段
        先被剔除再合并 → 任何局部更新都会把内存路径租户的密钥静默清空。
        注意：SqlTenantStore 本就不落密钥（PRD 4.5），store 路径重载后密钥
        为空属既定设计；本修复保证「旧配置 + 传入变更」的合并语义完整。
        """
        return config.model_dump()

    @staticmethod
    def _warn_budget_without_price(config: TenantConfig) -> None:
        """服务端守卫（问题 1）：设置了月度预算但单价全为 0 时打 WARNING。

        预算 > 0 意味着要按成本计费，而 model 侧单价为 0 会导致
        BudgetFilter 永远算不出花费（used_budget_usd 恒为 0），
        预算形同虚设。此处只告警不拦截，避免阻断合法的"先建租户后补价"流程。
        """
        if config.monthly_budget_usd <= 0:
            return
        if config.model.input_price_per_1m_usd > 0 or config.model.output_price_per_1m_usd > 0:
            return
        log.warning(
            "budget set but model unit price is 0: cost accounting will be disabled",
            extra={
                "tenant_id": config.tenant_id,
                "monthly_budget_usd": config.monthly_budget_usd,
                "input_price_per_1m_usd": config.model.input_price_per_1m_usd,
                "output_price_per_1m_usd": config.model.output_price_per_1m_usd,
            },
        )

    async def list(self) -> list[dict[str, Any]]:
        if self._tenant_store is not None:
            return [self._dump(c) for c in await self._tenant_store.list()]
        return [self._dump(config) for config in self._tenants.values()]

    async def get(self, tenant_id: str) -> Optional[dict[str, Any]]:
        if self._tenant_store is not None:
            config = await self._tenant_store.get(tenant_id)
            return self._dump(config) if config is not None else None
        config = self._tenants.get(tenant_id)
        if config is None:
            config = await self._registry.get(tenant_id)
        return self._dump(config) if config is not None else None

    async def create(self, data: dict[str, Any], operator: Optional[str] = None) -> TenantConfig:
        config = tenant_from_dict(data)
        self._warn_budget_without_price(config)
        if self._tenant_store is not None:
            if await self._tenant_store.get(config.tenant_id) is not None:
                raise ValueError(f"租户已存在: {config.tenant_id}")
            await self._tenant_store.create(config)
        else:
            if config.tenant_id in self._tenants:
                raise ValueError(f"租户已存在: {config.tenant_id}")
            self._tenants[config.tenant_id] = config
        self._registry.put(config.tenant_id, config)
        await self._audit(config.tenant_id, "admin_create", operator=operator)
        return config

    async def update(self, tenant_id: str, data: dict[str, Any], operator: Optional[str] = None) -> TenantConfig:
        if self._tenant_store is not None:
            existing = await self._tenant_store.get(tenant_id)
        else:
            existing = self._tenants.get(tenant_id)
        if existing is None:
            raise KeyError(f"租户不存在: {tenant_id}")
        # 以现有对象为基底合并更新字段：必须用非脱敏 dump（_dump_full）——
        # 脱敏 dump 已剔除密钥字段，作基线会把密钥静默清空（审查 09-04）
        merged = self._dump_full(existing)
        merged.update(data)
        merged["tenant_id"] = tenant_id
        config = tenant_from_dict(merged)
        self._warn_budget_without_price(config)
        # 快照当前版本供回滚（PRD 5.2）：有共享 tenant_store 时落 SQL 历史
        # （多节点一致、重启不丢）；否则维持进程内环形快照（内存 demo 模式）
        if hasattr(self._tenant_store, "push_history"):
            await self._tenant_store.push_history(tenant_id, existing, keep=MAX_KEEP_VERSIONS)
        else:
            hist = self._history.setdefault(tenant_id, [])
            hist.append(existing)
            del hist[:-MAX_KEEP_VERSIONS]
        await self._save(tenant_id, config, action="admin_update", operator=operator)
        return config

    async def rollback(self, tenant_id: str, operator: Optional[str] = None) -> TenantConfig:
        """回滚到上一版本（PRD 5.2）：弹出最近历史版本并写为当前配置。"""
        if hasattr(self._tenant_store, "pop_latest_history"):
            prev = await self._tenant_store.pop_latest_history(tenant_id)
            if prev is None:
                raise KeyError(f"无可回滚版本: {tenant_id}")
        else:
            hist = self._history.get(tenant_id) or []
            if not hist:
                raise KeyError(f"无可回滚版本: {tenant_id}")
            prev = hist.pop()
        await self._save(tenant_id, prev, action="admin_rollback", operator=operator)
        return prev

    async def _save(self,
                    tenant_id: str,
                    config: TenantConfig,
                    action: str = "admin_update",
                    operator: Optional[str] = None) -> None:
        """写共享存储 + 本进程 Registry + 跨节点失效广播 + 操作审计。"""
        if self._tenant_store is not None:
            await self._tenant_store.update(tenant_id, config)
        else:
            self._tenants[tenant_id] = config
        self._registry.put(tenant_id, config)  # 本进程热更新（PRD 5.2）
        if self._broadcaster is not None:
            await self._broadcaster.publish_invalidated(tenant_id)
        await self._audit(tenant_id, action, operator=operator)

    async def delete(self, tenant_id: str, operator: Optional[str] = None) -> None:
        if self._tenant_store is not None:
            existing = await self._tenant_store.get(tenant_id)
        else:
            existing = self._tenants.get(tenant_id)
        if existing is None:
            raise KeyError(f"租户不存在: {tenant_id}")
        if self._tenant_store is not None:
            await self._tenant_store.delete(tenant_id)
        else:
            self._tenants.pop(tenant_id)
        self._registry.invalidate(tenant_id)
        await self._audit(tenant_id, "admin_delete", operator=operator)


def build_admin_app(
    storage: Storage,
    registry: TenantRegistry,
    api_key: Optional[str] = None,
    tenant_store: Any = None,
    broadcaster: Any = None,
) -> FastAPI:
    """构建 Admin API 应用。

    Args:
        storage: 存储后端（审计查询）
        registry: 租户注册表
        api_key: 可选访问密钥（Header X-Admin-Key）
        tenant_store: SqlTenantStore（SQL 持久化）；None 时回退进程内 dict
        broadcaster: ConfigBroadcaster（跨节点失效通知）；None 时仅本进程生效
    """
    repo = TenantRepository(storage, registry, tenant_store=tenant_store, broadcaster=broadcaster)
    app = FastAPI(title="Teneuris Admin API", version="0.1.0")

    async def _check_key(x_admin_key: str = Header(default="")) -> None:
        # 常数时间比较，防时序侧信道推测 admin key（审查 09-04）
        if api_key and not hmac.compare_digest(x_admin_key, api_key):
            raise HTTPException(status_code=401, detail="invalid admin key")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        """Admin 控制台页面（TDesign CDN 单文件，鉴权仍走 X-Admin-Key）。"""
        return ADMIN_UI_HTML

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    # ---------------- 租户管理 ----------------

    @app.get("/tenants")
    async def list_tenants(x_admin_key: str = Header(default="")) -> JSONResponse:
        await _check_key(x_admin_key)
        return JSONResponse({"tenants": await repo.list()})

    @app.post("/tenants")
    async def create_tenant(
        request: Request, x_admin_key: str = Header(default=""), x_admin_operator: str = Header(default="")
    ) -> JSONResponse:
        await _check_key(x_admin_key)
        try:
            data = await request.json()
        except Exception:  # noqa: BLE001 - 畸形 JSON 回 400 而非未处理 500（审查 09-04）
            return JSONResponse({"error": "请求体需为合法 JSON"}, status_code=400)
        try:
            config = await repo.create(data, operator=x_admin_operator or None)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        log.info("tenant created", extra={"tenant_id": config.tenant_id})
        return JSONResponse({"tenant_id": config.tenant_id, "status": "created"}, status_code=201)

    @app.get("/tenants/{tenant_id}")
    async def get_tenant(tenant_id: str, x_admin_key: str = Header(default="")) -> JSONResponse:
        await _check_key(x_admin_key)
        try:
            data = await repo.get(tenant_id)
        except KeyError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        if data is None:
            return JSONResponse({"error": f"租户不存在: {tenant_id}"}, status_code=404)
        return JSONResponse(data)

    @app.put("/tenants/{tenant_id}")
    async def update_tenant(
        tenant_id: str,
        request: Request,
        x_admin_key: str = Header(default=""),
        x_admin_operator: str = Header(default="")
    ) -> JSONResponse:
        await _check_key(x_admin_key)
        try:
            data = await request.json()
        except Exception:  # noqa: BLE001 - 畸形 JSON 回 400 而非未处理 500（审查 09-04）
            return JSONResponse({"error": "请求体需为合法 JSON"}, status_code=400)
        try:
            await repo.update(tenant_id, data, operator=x_admin_operator or None)
        except (KeyError, ValueError) as exc:
            return JSONResponse({"error": str(exc)}, status_code=404 if isinstance(exc, KeyError) else 400)
        log.info("tenant updated", extra={"tenant_id": tenant_id})
        return JSONResponse({"tenant_id": tenant_id, "status": "updated"})

    @app.post("/tenants/{tenant_id}/rollback")
    async def rollback_tenant(
        tenant_id: str, x_admin_key: str = Header(default=""),
        x_admin_operator: str = Header(default="")) -> JSONResponse:
        """回滚到上一版本（PRD 5.2 配置回滚）。"""
        await _check_key(x_admin_key)
        try:
            await repo.rollback(tenant_id, operator=x_admin_operator or None)
        except KeyError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        log.info("tenant rolled back", extra={"tenant_id": tenant_id})
        return JSONResponse({"tenant_id": tenant_id, "status": "rolled_back"})

    @app.delete("/tenants/{tenant_id}")
    async def delete_tenant(
        tenant_id: str, x_admin_key: str = Header(default=""),
        x_admin_operator: str = Header(default="")) -> JSONResponse:
        await _check_key(x_admin_key)
        try:
            await repo.delete(tenant_id, operator=x_admin_operator or None)
        except KeyError as exc:
            return JSONResponse({"error": str(exc)}, status_code=404)
        return JSONResponse({"tenant_id": tenant_id, "status": "deleted"})

    # ---------------- 审计查询 ----------------

    @app.get("/audit/{tenant_id}")
    async def query_audit(
            tenant_id: str,
            request: Request,
            x_admin_key: str = Header(default=""),
    ) -> JSONResponse:
        """审计日志查询（PRD 4.4），支持 filter 参数（JSON）。"""
        await _check_key(x_admin_key)
        filters: dict[str, Any] = {}
        filter_raw = request.query_params.get("filter")
        if filter_raw:
            import json

            try:
                filters = json.loads(filter_raw)
            except json.JSONDecodeError:
                return JSONResponse({"error": "filter 需为 JSON"}, status_code=400)
        logs = await storage.audit.query_logs(tenant_id, filters, limit=100)
        return JSONResponse({"tenant_id": tenant_id, "logs": logs})

    return app
