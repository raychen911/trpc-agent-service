#!/usr/bin/env sh
# ===================================================================
# scripts/restore-wecom.sh - 环境重启后一键恢复企微联调
# ===================================================================
# 背景: 沙箱重启后以下配置会变，需重新准备:
#   1. redis-server       (未自动拉起则手动启动)
#   2. Gateway 服务       (8000 端口)
#   3. cloudflared 隧道   (公网 HTTPS URL，每次重启都变)
#   4. 出口公网 IP        (可信 IP 白名单可能失效)
# 不变: .env 里的 corp_id / agent_id / secret / token / aes_key
#   （企微后台静态配置，永久有效）
#
# 用法: sh scripts/restore-wecom.sh [--storage redis|inmemory]
# 输出: 隧道 URL + 当前出口 IP + 需要去企微后台更新的配置项清单
# ===================================================================
set -e
cd "$(dirname "$0")/.."

STORAGE="${1#--storage=}"
STORAGE="${STORAGE:-inmemory}"
CF_BIN="${CF_BIN:-/tmp/cloudflared}"
GATEWAY_PORT=8000

step() { echo ""; echo "=== [restore] $1 ==="; }

# ---------- 1. redis（可选） ----------
if [ "$STORAGE" = "redis" ]; then
  step "1/5 redis-server"
  if ! redis-cli ping >/dev/null 2>&1; then
    echo "启动 redis-server..."
    mkdir -p data && redis-server --daemonize yes --dir data
    sleep 1
  fi
  redis-cli ping
fi

# ---------- 2. Gateway ----------
step "2/5 Agent Gateway (:$GATEWAY_PORT)"
if curl -sf -m 2 "http://127.0.0.1:$GATEWAY_PORT/healthz" >/dev/null 2>&1; then
  echo "Gateway 已在运行，跳过"
else
  echo "启动 Gateway..."
  mkdir -p data/logs
  nohup python -m trpc_service._cli gateway \
    --config config/teneuris.yaml \
    --storage "$STORAGE" \
    --runner mock > data/logs/gateway.log 2>&1 &
  sleep 4
  curl -sf -m 2 "http://127.0.0.1:$GATEWAY_PORT/healthz" || { echo "❌ Gateway 启动失败，看 data/logs/gateway.log"; exit 1; }
fi

# ---------- 3. cloudflared 隧道 ----------
step "3/5 cloudflared 公网隧道"
if [ ! -x "$CF_BIN" ]; then
  echo "下载 cloudflared..."
  arch=$(uname -m)
  case "$arch" in
    x86_64) url="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64" ;;
    aarch64|arm64) url="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64" ;;
    *) echo "❌ 不支持的架构: $arch"; exit 1 ;;
  esac
  curl -fsSL -m 60 -o "$CF_BIN" "$url" && chmod +x "$CF_BIN"
fi
if pgrep -f "cloudflared tunnel" >/dev/null 2>&1; then
  echo "已有隧道在运行，跳过"
else
  nohup "$CF_BIN" tunnel --url "http://127.0.0.1:$GATEWAY_PORT" --no-autoupdate > /tmp/cloudflared.log 2>&1 &
  sleep 8
fi
TUNNEL_URL=$(grep -oE "https://[a-z0-9-]+\.trycloudflare\.com" /tmp/cloudflared.log | head -1)
if [ -z "$TUNNEL_URL" ]; then
  echo "❌ 未拿到隧道 URL，看 /tmp/cloudflared.log"; exit 1
fi
echo "隧道 URL: $TUNNEL_URL"
echo "回调 URL: $TUNNEL_URL/webhook/wechat_work/demo__wecom"

# ---------- 4. 出口 IP ----------
step "4/5 当前出口公网 IP"
EGRESS_IP=$(timeout 8 python -c "
import urllib.request
try:
    r = urllib.request.urlopen('https://ifconfig.me/ip', timeout=6)
    print(r.read().decode().strip())
except Exception:
    pass
" 2>/dev/null)
echo "出口 IP: ${EGRESS_IP:-<探测失败>}"

# ---------- 5. 自检: gettoken + 真发 ----------
step "5/5 凭证自检（gettoken + 真发）"
if [ -f .env ]; then
  set +e
  timeout 20 python scripts/verify-wecom-send.py \
    "${WECOM_TO_USER:-NiuChenXun}" "Teneuris 重启恢复自检 $(date +%H:%M:%S)" 2>&1 | tail -4
  set -e
fi

# ---------- 恢复清单 ----------
echo ""
echo "================================================================"
echo " ✅ 服务已恢复。若自检报错，按下面清单去企微后台更新："
echo "================================================================"
echo " 1) 可信 IP 白名单（若 60020 报错）:"
echo "    应用详情 -> 企业可信IP -> 改为: ${EGRESS_IP:-?}"
echo " 2) 接收消息服务器 URL（若需回调接收）:"
echo "    应用详情 -> 接收消息 -> 改为: $TUNNEL_URL/webhook/wechat_work/demo__wecom"
echo " 3) Token / EncodingAESKey 不变，除非你在后台重置过"
echo " 4) .env 凭证（corp_id/agent_id/secret）不变，永久有效"
echo "================================================================"
