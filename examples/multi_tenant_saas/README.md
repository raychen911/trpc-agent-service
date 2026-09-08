# 3 租户 SaaS 客服中台示例

基于 `trpc_service` 的完整多租户演示：3 个租户（电商/物流/金融）各自拥有
独立的模型、工具权限、IM 通道、数据后端与预算配置，演示租户隔离、无状态路由与治理。

## 目录

```
multi_tenant_saas/
├── agent.py          # 客服 Agent 工厂（有 API key 用 LlmAgent，否则用 mock）
├── tools.py          # 演示工具：query_order / query_logistics / query_balance / cancel_order
├── tenants.yaml      # 3 个租户的完整配置
├── app.py            # 组装 gateway + worker
├── run_gateway.py    # uvicorn 启动入口
├── simulate.py       # 离线模拟（无需 IM/LLM 凭证）
└── README.md
```

## 快速体验（离线，无需任何凭证）

```bash
cd examples/multi_tenant_saas
python simulate.py
```

`simulate.py` 用 mock Agent 直接驱动 worker，把同一个用户消息发给三个租户，展示：

- **租户隔离**：同一 `user_id` 在三个租户下得到不同的会话和回复；
- **指令隔离**：每个租户的客服角色指令不同（售前/物流/金融）；
- **session 路由确定性**：`sha256(tenant:channel:user)` 稳定且跨租户互不相同。

## 接入真实 LLM（可选）

```bash
export TRPC_SERVICE_MODEL_API_KEY=sk-xxx
cd examples/multi_tenant_saas
python run_gateway.py   # 或 uvicorn run_gateway:app --port 8080
```

此时 `create_agent` 会用租户各自的 `model` 配置构建 `LlmAgent`（含工具白名单过滤）。

## 接入真实 IM

在 `tenants.yaml` 中填好各租户的 IM 密钥。企业层只开放企业微信、微信客服、
钉钉和飞书四种适配器，IM 平台回调统一指向：

```
POST http://<host>:8080/webhook/{tenant_id}/{channel}
```

- 企业微信：`/webhook/tenant_ecom/wecom`
- 钉钉：`/webhook/tenant_logi/dingtalk`
- 飞书：`/webhook/tenant_fin/feishu`
- 微信客服：`/webhook/tenant_fin/wechat_kf`

## 验证隔离的 curl 示例

```bash
# 同一用户消息发往不同租户（mock 模式下回复内容体现各租户指令）
curl -X POST http://localhost:8080/webhook/tenant_ecom/wecom
curl -X POST http://localhost:8080/webhook/tenant_logi/dingtalk
```

> 真实 IM 回调需要合法的验签；离线演示请使用 `simulate.py`。
