# ===================================================================
# CodeBuddy Code
# ===================================================================
# 安装方式:
#   npm install -g @tencent-ai/codebuddy-code (需要 Node.js 18.20+)
# 首次使用: 终端输入 codebuddy，选择 "Log in via Chinese Site"
#           自动打开浏览器，通过腾讯云国内站 (copilot.tencent.com) 登录
# ===================================================================
# 一键运行:
#   bash /workspace/scripts/init-codebuddy.sh
#       安装 CLI + 取消 CNB 平台注入的自动登录 + 进入登录引导
#       默认不使用 CNB 自动登录，全程无需确认
# ===================================================================

set -eo pipefail
echo "=== CodeBuddy Code 安装与初始化 ==="

# 取消 CNB 平台注入的自动登录（默认视为确认，无需交互）
cancel_cnb_inject() {
    # 1. 注释 /etc/profile 中的注入行（容器级生效，影响该容器内所有终端）
    PROFILE=/etc/profile
    if [ -w "$PROFILE" ]; then
        if grep -q '^export ACC_PRODUCT_CONFIG_V2=' "$PROFILE"; then
            cp "$PROFILE" "$PROFILE.bak"
            sed -i \
                -e 's/^export ACC_PRODUCT_CONFIG_V2=/#export ACC_PRODUCT_CONFIG_V2=/' \
                -e 's/^export CNB_TOKEN=/#export CNB_TOKEN=/' \
                -e 's/^export CNB_TOKEN_USER_NAME=/#export CNB_TOKEN_USER_NAME=/' \
                "$PROFILE"
            echo "    ✓ 已注释 $PROFILE 中的注入行（备份: $PROFILE.bak）"
        else
            echo "    ✓ $PROFILE 未发现注入行（可能已注释）"
        fi
    else
        echo "    ⚠️  $PROFILE 不可写，无法注释注入行"
        echo "       请以 root 运行: sudo bash $(basename "$0")"
    fi

    # 2. 清除注入的环境变量（影响当前 shell 及后续启动的 codebuddy）
    for VAR in ACC_PRODUCT_CONFIG_V2 ACC_PRODUCT_CONFIG_V3 \
               CNB_TOKEN CNB_TOKEN_USER_NAME \
               CODEBUDDY_AUTH_TOKEN CODEBUDDY_SERVICE_PROXY_URL \
               CODEBUDDY_INTERNET_ENVIRONMENT; do
        unset "$VAR" 2>/dev/null || true
    done
    echo "    ✓ 已清除注入环境变量"

    # 3. 清除 local_storage 中缓存的 CNB 认证配置
    #    部分缓存是 gzip+base64 压缩的（如 entry_*.info），需先解码再匹配
    LS_DIR="$HOME/.codebuddy/local_storage"
    if [ -d "$LS_DIR" ]; then
        # 备份后清除与 CNB/认证相关的缓存条目（避免误删其他非认证缓存）
        BK_DIR="$LS_DIR/backup-$(date +%Y%m%d-%H%M%S)"
        mkdir -p "$BK_DIR"
        MATCHED=0
        for f in "$LS_DIR"/entry_*.info; do
            [ -e "$f" ] || continue
            IS_CNB=$(python3 -c '
import json, base64, gzip, sys
def looks_cnb(path):
    try:
        s = open(path, encoding="utf-8", errors="ignore").read().strip()
        try:
            d = json.loads(s)
            if isinstance(d, str):  # gzip+base64 压缩内容
                s = gzip.decompress(base64.b64decode(d)).decode("utf-8", errors="ignore")
        except Exception:
            pass
        return any(k in s for k in ("api.cnb.cool", "custom-token", "@cnb"))
    except Exception:
        return False
print("1" if looks_cnb(sys.argv[1]) else "0")
' "$f" 2>/dev/null || echo "0")
            if [ "$IS_CNB" = "1" ]; then
                cp "$f" "$BK_DIR/" 2>/dev/null || true
                rm -f "$f"
                MATCHED=1
                echo "    ✓ 清除认证缓存: $(basename "$f")"
            fi
        done
        if [ "$MATCHED" = "0" ]; then
            echo "    ℹ️  未发现 CNB 认证缓存（可能已被清除）"
        else
            echo "    ℹ️  缓存已备份至: $BK_DIR"
        fi
    fi
}

# 输出登录方式选择提示并启动 codebuddy
launch_codebuddy() {
    echo ""
    echo "============================================"
    echo "  ✅ CNB 自动登录已取消"
    echo ""
    echo "  启动 codebuddy 会出现登录方式选择:"
    echo "     Log in via Chinese Site   ← 个人 codebuddy.cn 账号（推荐）"
    echo "     Log in via International Site"
    echo "     Log in via Enterprise Domain"
    echo "============================================"
    echo ""
    if [ -t 0 ]; then
        echo "启动 codebuddy 并进入登录流程..."
        codebuddy
    else
        echo "非交互终端，请手动执行: codebuddy"
    fi
}

# 仅取消 CNB 自动登录（可选子命令，供环境重启后单独执行）
if [ "${1:-}" = "cancel-inject" ]; then
    echo "🧹 正在取消 CNB 平台注入的自动登录..."
    cancel_cnb_inject
    if ! command -v codebuddy >/dev/null 2>&1; then
        echo ""
        echo "❌ 未安装 CodeBuddy Code CLI，请先运行: bash $(basename "$0")"
        exit 1
    fi
    launch_codebuddy
    exit 0
fi

# 检查 Node.js 是否可用且版本满足 18.20+
if ! command -v node >/dev/null 2>&1; then
    echo ""
    echo "❌ 未检测到 Node.js，请先安装 Node.js 18.20+"
    echo "   安装参考: https://nodejs.org/"
    exit 1
fi
NODE_MAJOR=$(node -e "console.log(process.versions.node.split('.')[0])" 2>/dev/null || echo "0")
if [ "$NODE_MAJOR" -lt 18 ]; then
    echo ""
    echo "❌ Node.js 版本过低 (当前 $(node --version))，需要 18.20+"
    exit 1
fi

echo "📦 正在安装 CodeBuddy Code CLI..."
echo "    源: npm install -g @tencent-ai/codebuddy-code"
echo ""
npm install -g @tencent-ai/codebuddy-code
echo ""
echo "✅ CodeBuddy Code CLI 安装完成"

# 默认不使用 CNB 自动登录：安装完成后自动取消注入（无需确认）
echo ""
echo "🧹 默认不使用 CNB 自动登录，正在取消平台注入..."
cancel_cnb_inject

launch_codebuddy
