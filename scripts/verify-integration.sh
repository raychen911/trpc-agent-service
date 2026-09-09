#!/bin/sh
# ===================================================================
# scripts/verify-integration.sh - Redis 后端端到端联调（两网关节点 + Admin）
# ===================================================================
# 前置: 本机 redis-server 可用（start.sh 会自动拉起）；mock runner 离线运行。
# 场景:
#   A 多节点共享 session: 三轮轮换 gw1/gw2/gw1，Redis 中 events 累计、version 递增
#   B 幂等去重: 同 msg_id 重投必须拒绝
#   C 知识库跨进程共享: CLI 录入（redis）→ 独立进程检索命中
#   D 配置热更新跨进程生效: Admin 改租户名 → gw 无重启即用新名上报指标 → 回滚恢复
#   E 审计查询: /audit/{tenant} 有记录且字段齐全
#   F Redis 故障恢复: 重启 Redis → 网关自愈
# 用法: sh scripts/verify-integration.sh
# ===================================================================
set -e
cd "$(dirname "$0")/.."

GW1=http://127.0.0.1:8000
GW2=http://127.0.0.1:8001
ADMIN=http://127.0.0.1:8002
SID="itg-$(date +%s)"
TS=$(date +%s)
FAIL=""

note() { echo; echo "=== $1 ==="; }
check() { if [ "$1" = "ok" ]; then echo "  ✅ $2"; else FAIL="$FAIL|$2"; echo "  ❌ $2"; fi; }

cleanup() {
    for pid in $(cat data/logs/itg-*.pid 2>/dev/null); do kill "$pid" 2>/dev/null || true; done
    rm -f data/logs/itg-*.pid /tmp/gw-itg.yaml
}
trap cleanup EXIT

# ------------------------------------------------------------------
# 启动: redis + gw1(8000) + gw2(8001) + admin(8002)
# ------------------------------------------------------------------
note "启动联调环境（redis + 双网关 + admin，runner=mock）"
redis-cli ping >/dev/null 2>&1 || redis-server --daemonize yes --dir data
python -m trpc_service._cli gateway --config config/teneuris.yaml --storage redis --runner mock \
    > data/logs/itg-gw1.log 2>&1 & echo $! > data/logs/itg-gw1.pid
sed 's/port: 8000/port: 8001/' config/teneuris.yaml > /tmp/gw-itg.yaml
python -m trpc_service._cli gateway --config /tmp/gw-itg.yaml --storage redis --runner mock \
    > data/logs/itg-gw2.log 2>&1 & echo $! > data/logs/itg-gw2.pid
python -m trpc_service._cli admin --config config/teneuris.yaml \
    > data/logs/itg-admin.log 2>&1 & echo $! > data/logs/itg-admin.pid

for i in $(seq 1 40); do
    sleep 0.5
    curl -sf -m 2 $GW1/healthz >/dev/null 2>&1 && curl -sf -m 2 $GW2/healthz >/dev/null 2>&1 \
        && curl -sf -m 2 $ADMIN/healthz >/dev/null 2>&1 && break
done
curl -sf -m 2 $GW1/healthz >/dev/null || { echo "❌ gw1 未就绪"; exit 1; }
curl -sf -m 2 $GW2/healthz >/dev/null || { echo "❌ gw2 未就绪"; exit 1; }
echo "  gw1/gw2/admin 均就绪"

# demo 租户由网关首启自动播种（bootstrap.ensure_demo_tenant，空库才播种）。
# 此处仅归一化 name/backends，兼容旧库残留（上次联调若在回滚前中断）:
curl -sf -m 10 -X PUT $ADMIN/tenants/demo -H 'Content-Type: application/json' \
    -d '{"name":"演示租户","backends":{"session":"redis","memory":"redis","summary":"sql","audit":"sql","knowledge":"redis"}}' > /dev/null

# ------------------------------------------------------------------
note "场景 A: 多节点共享 session（8000 → 8001 → 8000）"
for round in 1 2 3; do
    if [ $((round % 2)) -eq 1 ]; then GW=$GW1; else GW=$GW2; fi
    R=$(curl -sf -m 10 -X POST $GW/chat -H 'Content-Type: application/json' \
        -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID\",\"content\":\"第${round}轮\",\"msg_id\":\"$SID-$round\"}")
    echo "$R" | grep -q '"response_type":"text"' || { echo "  第${round}轮失败: $R"; exit 1; }
    echo "  第${round}轮 → $(echo $GW | grep -o ':[0-9]*$') ok"
done
STATE=$(redis-cli --raw HGET "session:demo:$SID" state)
VERSION=$(redis-cli --raw HGET "session:demo:$SID" version)
HIST_COUNT=$(echo "$STATE" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('history', [])))")
echo "  Redis 中 history=$HIST_COUNT 条, version=$VERSION"
[ "$HIST_COUNT" -ge 3 ] && [ "$VERSION" -ge 3 ] && check ok "跨节点会话连续累计（无 sticky，共享 Redis）" || check fail "跨节点会话累计 (history=$HIST_COUNT version=$VERSION)"

# ------------------------------------------------------------------
note "场景 B: 同 msg_id 重复投递幂等"
curl -sf -m 10 -X POST $GW1/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID\",\"content\":\"重复投递\",\"msg_id\":\"$SID-dup\"}" >/dev/null
DUP=$(curl -s -m 10 -X POST $GW2/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID\",\"content\":\"重复投递\",\"msg_id\":\"$SID-dup\"}")
echo "$DUP" | grep -q 'duplicate message' && check ok "跨节点 msg_id 去重生效" || check fail "幂等去重失效: $DUP"

# ------------------------------------------------------------------
note "场景 C: 知识库跨进程共享（CLI 写 → 独立进程读）"
python -m trpc_service._cli knowledge-add --tenant demo --backend redis \
    --doc-id "itg-$TS" --text "联调验证文档 itg$TS 唯一关键词 zebraqx7" > /dev/null
HIT=$(python3 - <<PYEOF
import asyncio, redis.asyncio as aioredis
from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

async def main():
    client = aioredis.Redis.from_url("redis://localhost:6379/0")
    store = RedisKnowledgeStore(client)
    hits = await store.search("demo", "zebraqx7", top_k=5)
    await client.aclose()
    return len(hits)

print(asyncio.run(main()))
PYEOF
)
[ "$HIT" -ge 1 ] && check ok "CLI 录入后独立进程立即可检索" || check fail "知识库共享失败 (hits=$HIT)"

# ------------------------------------------------------------------
note "场景 D: 配置热更新跨进程生效（Admin → 广播 → gw 指标换名）"
NAME_NEW="热更新验证$TS"
curl -sf -m 10 -X PUT $ADMIN/tenants/demo -H 'Content-Type: application/json' \
    -d "{\"name\":\"$NAME_NEW\"}" > /dev/null
curl -sf -m 10 -X POST $GW1/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID-hu\",\"content\":\"热更新后首聊\",\"msg_id\":\"$SID-hu\"}" > /dev/null
sleep 1
curl -s -m 5 $GW1/metrics | grep -q "agent_name=\"$NAME_NEW\"" \
    && check ok "gw 无重启即按新配置上报（pub/sub 失效广播生效）" || check fail "热更新未生效（gw 指标仍是旧名）"

curl -sf -m 10 -X POST $ADMIN/tenants/demo/rollback > /dev/null
curl -sf -m 10 -X POST $GW1/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID-rb\",\"content\":\"回滚后首聊\",\"msg_id\":\"$SID-rb\"}" > /dev/null
sleep 1
curl -s -m 5 $GW1/metrics | grep -q "agent_name=\"演示租户\"" \
    && check ok "一键回滚后恢复原名" || check fail "回滚未生效"

# ------------------------------------------------------------------
note "场景 E: 审计查询字段完整性"
AUDIT_FILE=/tmp/itg-audit-$TS.json
curl -s -m 10 $ADMIN/audit/demo > "$AUDIT_FILE"
python3 - "$AUDIT_FILE" <<'PYEOF' && check ok "审计记录含 tenant_id/session_id/decision/trace_id 等字段" || check fail "审计字段缺失"
import json, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
rows = data if isinstance(data, list) else data.get("logs") or []
assert rows, "审计为空"
required = {"tenant_id", "session_id", "decision", "trace_id", "user_id", "channel"}
missing = required - set(rows[0].keys())
assert not missing, f"缺字段: {missing}"
PYEOF
rm -f "$AUDIT_FILE"

# ------------------------------------------------------------------
note "场景 F: Redis 重启故障恢复"
redis-cli shutdown nosave 2>/dev/null || true
sleep 1
ERR=$(curl -s -m 10 -X POST $GW1/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID-fr\",\"content\":\"故障期间\",\"msg_id\":\"$SID-fr\"}")
echo "  故障期间响应: $(echo "$ERR" | head -c 120)"
redis-server --daemonize yes --dir data
sleep 2
curl -sf -m 5 $GW1/readyz > /dev/null || sleep 3
RECOVER=$(curl -s -m 10 -X POST $GW1/chat -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"demo\",\"user_id\":\"u-itg\",\"session_id\":\"$SID-fr2\",\"content\":\"恢复后首聊\",\"msg_id\":\"$SID-fr2\"}")
echo "$RECOVER" | grep -q '"response_type":"text"' && check ok "Redis 重启后网关自愈" || check fail "恢复失败: $RECOVER"

# ------------------------------------------------------------------
echo
if [ -z "$FAIL" ]; then
    echo "=== 联调全部通过 ==="
else
    echo "=== 联调存在失败项:$FAIL"
    exit 1
fi
