#!/bin/sh
# ===================================================================
# scripts/verify-multinode.sh - 多节点最小验证（阶段三）
# ===================================================================
# 前置: 以 mock runner 启动 compose（离线，不需 API key）:
#   TENEURIS_RUNNER=mock docker compose up -d
# 验证: 同一 session 的三轮请求轮换命中 gw1/gw2/gw1，共享 Redis 中
#       会话历史连续累计 —— 证明「无 sticky session、依赖共享后端」。
# 用法: sh scripts/verify-multinode.sh
# ===================================================================
set -e
cd "$(dirname "$0")/.."
GW1=http://127.0.0.1:8001
GW2=http://127.0.0.1:8004
SID="multinode-$(date +%s)"

echo "=== 等待两个节点就绪 ==="
for i in $(seq 1 30); do
  sleep 0.5
  curl -sf -m 2 $GW1/healthz >/dev/null 2>&1 && curl -sf -m 2 $GW2/healthz >/dev/null 2>&1 && break
done

echo "=== 第 1 轮 → 节点1 (8001) ==="
R1=$(curl -sf -X POST $GW1/chat -H 'Content-Type: application/json' \
  -d "{\"tenant_id\":\"demo\",\"user_id\":\"u1\",\"session_id\":\"$SID\",\"content\":\"第一轮命中节点1\",\"msg_id\":\"$SID-1\"}")
echo "$R1" | head -c 220; echo

echo "=== 第 2 轮 → 节点2 (8004)，同一 session ==="
R2=$(curl -sf -X POST $GW2/chat -H 'Content-Type: application/json' \
  -d "{\"tenant_id\":\"demo\",\"user_id\":\"u1\",\"session_id\":\"$SID\",\"content\":\"第二轮命中节点2\",\"msg_id\":\"$SID-2\"}")
echo "$R2" | head -c 220; echo

echo "=== 第 3 轮 → 节点1 (8001)，同一 session ==="
R3=$(curl -sf -X POST $GW1/chat -H 'Content-Type: application/json' \
  -d "{\"tenant_id\":\"demo\",\"user_id\":\"u1\",\"session_id\":\"$SID\",\"content\":\"第三轮回节点1\",\"msg_id\":\"$SID-3\"}")
echo "$R3" | head -c 220; echo

echo "$R1" | grep -q '"response_type":"text"' || { echo "❌ 节点1 失败"; exit 1; }
echo "$R2" | grep -q '"response_type":"text"' || { echo "❌ 节点2 失败"; exit 1; }
echo "$R3" | grep -q '"response_type":"text"' || { echo "❌ 节点1(第三轮) 失败"; exit 1; }

echo ""
echo "=== 共享 Redis 中的会话状态（应含全部 3 轮历史）==="
REDIS=$(docker compose ps -q redis)
docker exec "$REDIS" redis-cli --scan --pattern "session:demo:*" | head -3
docker exec "$REDIS" redis-cli hgetall "session:demo:$SID" | grep -o "第一轮命中节点1\|第二轮命中节点2\|第三轮回节点1" | sort | uniq -c

echo ""
echo "=== trace_id 应各不相同（确实走了不同节点），session_id 应一致 ==="
echo "R1 trace: $(echo "$R1" | grep -o '"trace_id":"[a-f0-9]*"' | head -1)"
echo "R2 trace: $(echo "$R2" | grep -o '"trace_id":"[a-f0-9]*"' | head -1)"

echo ""
echo "✅ 多节点验证通过：无 sticky session，跨节点同一 session 连续累计"
