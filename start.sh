#!/usr/bin/env bash
# PR #7 sanitized demonstration launcher. Application source must remain unchanged.
# Usage: bash start.sh [up|check|status|demo|verify|connections|ui|stop|rebuild]
# UI: bash start.sh ui [demo|traces|metrics|storage|all]
# Set TRPC_DEMO_OPEN_UI=0 to start services without opening browser tabs.
# Runtime configuration lives outside the clone; Docker volumes retain data.
set +vx  # Never echo plaintext credentials through shell tracing.
set -Eeuo pipefail

# ==================== EDIT ONLY THIS CONFIGURATION BLOCK ====================
# 在下面等号右侧直接填写明文值，不要加引号，不要写行尾注释。
# 每个值写一行；美元符号、引号、反斜杠等字符均按原样读取，不执行命令。
# REPLACE_ 开头的 7 项必须填写；租户、应用和提示词可保留默认值。
# 此模板不含真实凭据；填好后就是私用文件，不要提交 Git 或投屏展示。
# PostgreSQL / Redis / MinIO 等内部密码由脚本随机生成，无需另带 .env。
IFS= read -r -d '' DEMO_CONFIG <<'TRPC_DEMO_CONFIG' || true
MODEL_PROVIDER=openai
MODEL_BASE_URL=REPLACE_WITH_HTTPS_MODEL_BASE_URL
MODEL_NAME=REPLACE_WITH_MODEL_NAME
MODEL_API_KEY=REPLACE_WITH_MODEL_API_KEY
APP_ID=assistant

TENANT_A_ID=demo-tenant-a
TENANT_A_NAME=演示租户 A
BOT_A_ID=REPLACE_WITH_BOT_A_ID
BOT_A_SECRET=REPLACE_WITH_BOT_A_SECRET
BINDING_A_ID=demo-wecom-a
INSTRUCTIONS_A=你是演示租户 A 的助手。请用中文简洁回答，准确使用当前会话中的信息。

TENANT_B_ID=demo-tenant-b
TENANT_B_NAME=演示租户 B
BOT_B_ID=REPLACE_WITH_BOT_B_ID
BOT_B_SECRET=REPLACE_WITH_BOT_B_SECRET
BINDING_B_ID=demo-wecom-b
INSTRUCTIONS_B=你是演示租户 B 的助手。请用中文简洁回答，准确使用当前会话中的信息。
TRPC_DEMO_CONFIG
readonly DEMO_CONFIG
# ==================== END OF EDITABLE CONFIGURATION =========================

validate_plain_config() {
    local line key value failed=0
    local -A seen=()
    while IFS= read -r line; do
        line="${line%$'\r'}"
        [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
        if [[ "$line" != *=* ]]; then
            echo 'ERROR: invalid configuration line; use KEY=value (values hidden).' >&2
            return 1
        fi
        key="${line%%=*}"
        value="${line#*=}"
        case "$key" in
            MODEL_PROVIDER|MODEL_BASE_URL|MODEL_NAME|MODEL_API_KEY|APP_ID|TENANT_A_ID|TENANT_A_NAME|BOT_A_ID|BOT_A_SECRET|BINDING_A_ID|INSTRUCTIONS_A|TENANT_B_ID|TENANT_B_NAME|BOT_B_ID|BOT_B_SECRET|BINDING_B_ID|INSTRUCTIONS_B) ;;
            *) echo 'ERROR: unknown configuration field (values hidden).' >&2; return 1;;
        esac
        if [[ -n "${seen[$key]+set}" ]]; then
            printf 'ERROR: duplicate configuration field: %s\n' "$key" >&2
            return 1
        fi
        seen["$key"]=1
        if [[ -z "$value" || "$value" == REPLACE_* ]]; then
            printf 'ERROR: fill %s in the configuration block at the top of start.sh.\n' "$key" >&2
            failed=1
        fi
    done <<< "$DEMO_CONFIG"
    for key in MODEL_PROVIDER MODEL_BASE_URL MODEL_NAME MODEL_API_KEY APP_ID TENANT_A_ID TENANT_A_NAME BOT_A_ID BOT_A_SECRET BINDING_A_ID INSTRUCTIONS_A TENANT_B_ID TENANT_B_NAME BOT_B_ID BOT_B_SECRET BINDING_B_ID INSTRUCTIONS_B; do
        if [[ -z "${seen[$key]+set}" ]]; then
            printf 'ERROR: missing configuration field: %s\n' "$key" >&2
            failed=1
        fi
    done
    return "$failed"
}

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'
readonly SUBMISSION=9d0e14a3a28e8501dea698fed99448e1a358e7f9
readonly IMAGE=trpc-pr7-demo:9d0e14a
readonly TRACE_UI_URL='http://127.0.0.1:26686/search?service=trpc-agent-service-worker&lookback=1h&limit=20'
readonly METRICS_UI_URL='http://127.0.0.1:29090/targets'
readonly STORAGE_UI_URL='http://127.0.0.1:29001'
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ACTION="${1:-up}"
if [ "$ACTION" != ui ]; then validate_plain_config; fi
cd "$ROOT"

native_path() {
    if command -v cygpath >/dev/null 2>&1; then cygpath -am "$1"; else printf '%s\n' "$1"; fi
}
verify_source() {
    # Launcher-only commits may follow the submitted application commit.
    # All application files must still match that immutable baseline below.
    git merge-base --is-ancestor "$SUBMISSION" HEAD || {
        echo 'ERROR: checkout does not contain the submitted application commit; clone the full branch history.' >&2
        exit 1
    }
    git diff --quiet "$SUBMISSION" -- . ':!start.sh' || {
        echo 'ERROR: application source differs from the submitted commit.' >&2; exit 1;
    }
    git diff --cached --quiet HEAD -- start.sh || { echo "ERROR: private start.sh must not be staged or committed." >&2; exit 1; }
    local extra
    extra="$(git ls-files --others --directory --no-empty-directory)"
    test -z "$extra" || { printf 'ERROR: unexpected files in clone:\n%s\n' "$extra" >&2; exit 1; }
}
verify_source
# This command only opens existing pages; it does not restart services or read
# model/Bot credentials. Browser arguments are selected from fixed local URLs.
open_url() {
    local url="$1" attempt
    if command -v curl >/dev/null 2>&1; then
        for attempt in $(seq 1 10); do
            if curl --noproxy '*' --fail --silent --max-time 2 "$url" >/dev/null; then break; fi
            if [ "$attempt" = 10 ]; then
                printf 'Page is not ready: %s\nRun bash start.sh to start the services.\n' "$url" >&2
                return 1
            fi
            sleep 1
        done
    fi
    printf 'Opening: %s\n' "$url"
    case "$(uname -s)" in
        MINGW*|MSYS*|CYGWIN*)
            TRPC_DEMO_OPEN_URL="$url" powershell.exe -NoLogo -NoProfile -NonInteractive -WindowStyle Hidden \
                -Command '$ErrorActionPreference="Stop"; Start-Process -FilePath $env:TRPC_DEMO_OPEN_URL' ;;
        Darwin*) open "$url" ;;
        *)
            if command -v xdg-open >/dev/null 2>&1; then
                xdg-open "$url" >/dev/null 2>&1 &
            else
                printf 'No desktop browser launcher found. Open this URL manually: %s\n' "$url"
            fi ;;
    esac
}
open_ui() {
    local target="${1:-demo}" failed=0
    case "$target" in
        demo|all|traces|metrics|storage) ;;
        *) echo 'Usage: bash start.sh ui [demo|traces|metrics|storage|all]' >&2; return 2;;
    esac
    if [ "$target" = demo ] || [ "$target" = all ] || [ "$target" = metrics ]; then
        open_url "$METRICS_UI_URL" || failed=1
    fi
    if [ "$target" = all ] || [ "$target" = storage ]; then
        open_url "$STORAGE_UI_URL" || failed=1
    fi
    if [ "$target" = demo ] || [ "$target" = all ] || [ "$target" = traces ]; then
        # Open traces last so the Agent execution view is the foreground tab.
        open_url "$TRACE_UI_URL" || failed=1
    fi
    return "$failed"
}
if [ "$ACTION" = ui ]; then
    open_ui "${2:-demo}"
    exit $?
fi
for dependency in git docker openssl; do
    command -v "$dependency" >/dev/null || { echo "Missing dependency: $dependency" >&2; exit 1; }
done
if ! docker info >/dev/null 2>&1; then
    docker desktop start --timeout 120
fi
# A checkout gets its own generated settings and Compose volumes.
CHECKOUT_KEY="$(printf '%s' "$(native_path "$ROOT")" | openssl dgst -sha256 | awk '{print substr($NF,1,12)}')"
readonly PROJECT="trpc-pr7-live-$CHECKOUT_KEY"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/$PROJECT"
umask 077
mkdir -p "$STATE"
STATE="$(cd "$STATE" && pwd)"
case "$STATE/" in "$ROOT/"*) echo 'Runtime directory must be outside the source clone.' >&2; exit 1;; esac
chmod 700 "$STATE"
case "$ACTION" in
    up|rebuild|check|status|demo|verify|connections|stop) ;;
    *) echo 'Usage: bash start.sh [up|check|status|demo|verify|connections|ui|stop|rebuild]' >&2; exit 2;;
esac
# First start in EACH new checkout rebuilds from git archive, using Docker's
# dependency cache when available. No local .venv, helper source or old image
# application files are copied into the build context.
if [ "$ACTION" = up ] || [ "$ACTION" = rebuild ]; then
    if [ "$ACTION" = rebuild ] || [ ! -f "$STATE/build.complete" ] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        echo 'Building the submitted Git tree with its unchanged Dockerfile and uv.lock...'
        git archive --format=tar "$SUBMISSION" | docker build \
            --build-arg "TRPC_SOURCE_FINGERPRINT=$SUBMISSION" --tag "$IMAGE" -
        test "$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$SUBMISSION"
        printf '%s\n' "$SUBMISSION" > "$STATE/build.complete"
    fi
elif ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo 'Run bash start.sh first to build the frozen image.' >&2; exit 1
fi
# Pass literal configuration via stdin, never via process arguments or env.
CONFIG_SHA="$(printf '%s' "$DEMO_CONFIG" | openssl dgst -sha256 | awk '{print $NF}')"
printf '%s' "$DEMO_CONFIG" | docker run --rm --network none -i \
    --user "$(id -u):$(id -g)" --entrypoint python \
    --mount "type=bind,source=$(native_path "$STATE"),target=/state" "$IMAGE" -c '
import json,pathlib,re,sys
from urllib.parse import urlsplit
from trpc_service.tenant.models import ModelPolicy
try:
    fields={}
    for line in sys.stdin.buffer.read().decode("utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key,separator,value=line.partition("=")
        if not separator or key in fields:
            raise ValueError("invalid configuration format")
        fields[key]=value
    required="MODEL_PROVIDER MODEL_BASE_URL MODEL_NAME MODEL_API_KEY APP_ID TENANT_A_ID TENANT_A_NAME BOT_A_ID BOT_A_SECRET BINDING_A_ID INSTRUCTIONS_A TENANT_B_ID TENANT_B_NAME BOT_B_ID BOT_B_SECRET BINDING_B_ID INSTRUCTIONS_B".split()
    if set(fields)!=set(required) or any(not fields[k].strip() or fields[k].startswith("REPLACE_") for k in required):
        raise ValueError("configuration is incomplete")
    for key in ("APP_ID","TENANT_A_ID","TENANT_B_ID","BINDING_A_ID","BINDING_B_ID"):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",fields[key]):
            raise ValueError("invalid application, tenant or binding identifier")
    for left,right in (("TENANT_A_ID","TENANT_B_ID"),("BOT_A_ID","BOT_B_ID"),("BINDING_A_ID","BINDING_B_ID")):
        if fields[left]==fields[right]:
            raise ValueError("the two tenants, bots and bindings must be distinct")
    model={"provider":fields["MODEL_PROVIDER"],"model":fields["MODEL_NAME"],"base_url":fields["MODEL_BASE_URL"]}
    u=urlsplit(model["base_url"])
    if u.scheme!="https" or not u.hostname or u.username or u.password or u.query or u.fragment:
        raise ValueError("model endpoint must be a plain HTTPS API URL")
    ModelPolicy(**model)
    config={"app_id":fields["APP_ID"],"model":model,"bots":[{
        "tenant_id":fields["TENANT_"+suffix+"_ID"],
        "name":fields["TENANT_"+suffix+"_NAME"],
        "bot_id":fields["BOT_"+suffix+"_ID"],
        "binding_id":fields["BINDING_"+suffix+"_ID"],
        "instructions":fields["INSTRUCTIONS_"+suffix]
    } for suffix in ("A","B")]}
    files={"model.secret":fields["MODEL_API_KEY"],"wecom_a.secret":fields["BOT_A_SECRET"],"wecom_b.secret":fields["BOT_B_SECRET"]}
    files["wecom.json"]=json.dumps(config,ensure_ascii=False)
    for name,value in files.items():
        p=pathlib.Path("/state")/name
        if not p.exists() or p.read_text(encoding="utf-8")!=value:
            p.write_text(value,encoding="utf-8")
        p.chmod(0o644)
except Exception:
    print("ERROR: invalid plaintext configuration or runtime directory permissions (values hidden).",file=sys.stderr)
    raise SystemExit(1)
'
echo 'Plaintext API/Bot configuration injected locally; values hidden.'
if [ ! -f "$STATE/settings.env" ]; then
    {
        for key in POSTGRES_PASSWORD MIGRATION_DATABASE_PASSWORD RUNTIME_DATABASE_PASSWORD \
            WORKER_DATABASE_PASSWORD METRICS_DATABASE_PASSWORD REDIS_PASSWORD \
            MINIO_ROOT_PASSWORD SESSION_HMAC_KEY EMERGENCY_QUEUE_KEY DEVELOPMENT_TOKEN; do
            if [ "$key" = EMERGENCY_QUEUE_KEY ]; then
                printf '%s=%s\n' "$key" "$(openssl rand -base64 32)"
            else
                printf '%s=%s\n' "$key" "$(openssl rand -hex 32)"
            fi
        done
        cat <<'ENV'
POSTGRES_DB=trpc_service
POSTGRES_USER=trpc
TRPC_RUNTIME_USER=trpc_runtime
TRPC_WORKER_USER=trpc_worker
MINIO_ROOT_USER=trpc-minio
MINIO_BUCKET=trpc-artifacts
MODEL_API_KEY=offline-demo-unused
TRPC_SERVICE_IMAGE=trpc-pr7-demo:9d0e14a
TRPC_SERVICE_ENVIRONMENT=development
TRPC_SERVICE_CAPTURE_CONTENT=false
TRPC_SERVICE_DATABASE_POOL_MIN_SIZE=1
TRPC_SERVICE_DATABASE_POOL_MAX_SIZE=5
TRPC_SERVICE_WORKER_CONCURRENCY=2
TRPC_SERVICE_WECOM_ENABLED=false
GATEWAY_PORT=28080
ADMIN_PORT=28081
POSTGRES_PORT=25432
REDIS_PORT=26379
MINIO_PORT=29000
MINIO_CONSOLE_PORT=29001
PROMETHEUS_PORT=29090
JAEGER_UI_PORT=26686
ENV
    } > "$STATE/settings.env"
fi
# Compose normally lets the caller environment override --env-file. Clear only
# keys generated by THIS launcher so old shell settings cannot change this run.
while IFS="=" read -r setting_name setting_value; do
    unset "$setting_name"
done < "$STATE/settings.env"
unset setting_name setting_value
# Only deployment settings are overridden; commands executing the application
# and its database migrations are the commands in the submitted Compose file.
cat > "$STATE/compose.override.yaml" <<'YAML'
services:
  otel-collector:
    ports: !reset []
    healthcheck:
      disable: true
  jaeger:
    ports: !override
      - "127.0.0.1:26686:16686"
    healthcheck:
      disable: true
YAML
COMPOSE=(docker compose --project-name "$PROJECT" --project-directory "$(native_path "$ROOT")"
    --env-file "$(native_path "$STATE/settings.env")"
    -f "$(native_path "$ROOT/docker-compose.yml")" -f "$(native_path "$STATE/compose.override.yaml")")
ONLINE=false
if [ -f "$STATE/wecom.json" ]; then
    ONLINE=true
    for secret_file in wecom_a.secret wecom_b.secret model.secret; do
        test -s "$STATE/$secret_file" || { echo "Missing local secret file: $secret_file" >&2; exit 1; }
    done
    MODEL_HOSTS="$(docker run --rm --entrypoint python --mount "type=bind,source=$(native_path "$STATE/wecom.json"),target=/demo.json,readonly" "$IMAGE" -c 'import json;from urllib.parse import urlsplit;print(json.dumps([urlsplit(json.load(open("/demo.json"))["model"]["base_url"]).hostname]))')"
    cat > "$STATE/compose.online.yaml" <<YAML
services:
  worker:
    labels:
      io.trpc.demo.configuration: "$CONFIG_SHA"
    environment:
      TRPC_SERVICE_MODEL_ENDPOINT_HOSTS: '$MODEL_HOSTS'
    secrets:
      - wecom_bot_secret_a
      - wecom_bot_secret_b
    volumes:
      - "$(native_path "$STATE/wecom.json"):/run/demo/wecom.json:ro"
  wecom-connector:
    labels:
      io.trpc.demo.configuration: "$CONFIG_SHA"
    environment:
      TRPC_SERVICE_WECOM_ENABLED: "true"
    secrets:
      - wecom_bot_secret_a
      - wecom_bot_secret_b
secrets:
  model_api_key: !override
    file: "$(native_path "$STATE/model.secret")"
  wecom_bot_secret_a:
    file: "$(native_path "$STATE/wecom_a.secret")"
  wecom_bot_secret_b:
    file: "$(native_path "$STATE/wecom_b.secret")"
YAML
    COMPOSE+=(-f "$(native_path "$STATE/compose.online.yaml")")
fi
compose() { "${COMPOSE[@]}" "$@"; }
SERVICES=(postgres redis minio minio-init migrate gateway admin worker outbox-dispatcher
    post-turn-projector session-recovery artifact-gc otel-collector jaeger prometheus)
if "$ONLINE"; then SERVICES+=(wecom-connector); fi
show_urls() {
    printf '\nApplication baseline commit: %s\n' "$SUBMISSION"
    printf 'Launcher checkout commit: %s\n' "$(git rev-parse HEAD)"
    printf 'Source verification: PASS (only start.sh may differ)\n'
    printf 'Gateway health: http://127.0.0.1:28080/health/ready\n'
    printf 'Admin health:   http://127.0.0.1:28081/health/ready\n'
    printf '\nDemonstration pages (automatically opened after startup):\n'
    printf 'Agent traces / Jaeger: %s\n' "$TRACE_UI_URL"
    printf 'Service metrics / Prometheus: %s\n' "$METRICS_UI_URL"
    printf 'File storage / MinIO (optional, login required): %s\n' "$STORAGE_UI_URL"
    printf 'Reopen demo pages: bash start.sh ui\n'
    printf 'Open file storage: bash start.sh ui storage\n'
    printf 'This submitted version provides Agent administration through an API, without a management web UI.\n'
    printf 'Generated runtime config: %s (created from this script; do not share)\n' "$STATE"
    printf 'Isolated Compose project: %s\n' "$PROJECT"
    printf 'Connection-only check before the first message: bash start.sh connections\n'
    if "$ONLINE"; then
        printf '\nTwo-WeCom-bot mode: configured real model; after real messages, check receipts with bash start.sh verify\n'
    else
        printf '\nLocal preflight: offline Agent; no external model/IM calls.\n'
        printf 'This launcher requires the two-bot configuration at the top of start.sh.\n'
    fi
    printf 'Stop without deleting data: bash start.sh stop\n'
}
case "$ACTION" in
    stop) compose stop; verify_source; exit 0;;
    check) compose config --quiet; verify_source; echo 'PASS: source and deployment configuration are valid.'; exit 0;;
    status) compose ps; verify_source; show_urls; exit 0;;
    up|rebuild|demo|verify|connections) ;;
    *) echo 'Usage: bash start.sh [up|check|status|demo|verify|connections|ui|stop|rebuild]' >&2; exit 2;;
esac
if [ "$ACTION" = up ] || [ "$ACTION" = rebuild ]; then
    test "$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')" = "$SUBMISSION"
    # Preserve the same bytes if an early launcher wrote the key as hex. The
    # submitted decoder accepts base64 or 32 raw bytes, not a hex string.
    docker run --rm -i --entrypoint python "$IMAGE" -c '
import base64,sys
lines=sys.stdin.read().splitlines()
for i,line in enumerate(lines):
    if line.startswith("EMERGENCY_QUEUE_KEY="):
        value=line.split("=",1)[1]
        if len(value)==64 and all(c in "0123456789abcdef" for c in value):
            value=base64.b64encode(bytes.fromhex(value)).decode()
            lines[i]="EMERGENCY_QUEUE_KEY="+value
        assert len(base64.b64decode(value))==32, "Emergency queue key must decode to 32 bytes"
print("\n".join(lines))
' < "$STATE/settings.env" > "$STATE/settings.checked.env"
    mv "$STATE/settings.checked.env" "$STATE/settings.env"
    compose config --quiet
    # Only older PR7 demo projects carrying the SAME submitted image revision
    # are stopped, to release the fixed ports and avoid two clients for a Bot.
    # Data volumes are retained. Other application projects are not targeted.
    while IFS= read -r previous_project; do
        case "$previous_project" in
            trpc-pr7-demo|trpc-pr7-live-*)
                if [ "$previous_project" != "$PROJECT" ]; then
                    mapfile -t previous_ids < <(docker ps --filter "label=com.docker.compose.project=$previous_project" --format '{{.ID}}')
                    if [ "${#previous_ids[@]}" -gt 0 ]; then
                        printf 'Stopping previous PR7 demo project %s (data retained).\n' "$previous_project"
                        docker stop "${previous_ids[@]}" >/dev/null
                    fi
                fi ;;
        esac
    done < <(docker ps --filter "label=org.opencontainers.image.revision=$SUBMISSION" --format '{{.Label "com.docker.compose.project"}}' | sort -u)
    compose up --detach --no-build --scale worker=2 "${SERVICES[@]}"
    echo 'Waiting for the original health/readiness checks...'
    for attempt in $(seq 1 90); do
        if compose exec -T gateway python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/ready',timeout=3); urllib.request.urlopen('http://admin:8081/health/ready',timeout=3)" >/dev/null 2>&1 \
            && compose exec -T worker python -m trpc_service.probe --role worker >/dev/null 2>&1; then break; fi
        if [ "$attempt" = 90 ]; then echo 'Readiness failed; inspect: bash start.sh status' >&2; exit 1; fi
        sleep 2
    done
    compose exec -T gateway trpc-service doctor
fi
if "$ONLINE" && { [ "$ACTION" = up ] || [ "$ACTION" = rebuild ]; }; then
    echo 'Registering two separate tenants through the submitted Admin API...'
    compose exec -T worker python - <<'PY'
import hashlib,json,pathlib,httpx
settings=json.loads(pathlib.Path('/run/demo/wecom.json').read_text(encoding='utf-8'))
app_id=settings['app_id']
token=pathlib.Path('/run/secrets/development_token').read_text().strip()
client=httpx.Client(base_url='http://admin:8081',headers={'Authorization':'Bearer '+token},timeout=15)
def write(method,path,body,tenant,create=False):
    key=hashlib.sha256(json.dumps([method,path,body],sort_keys=True).encode()).hexdigest()
    headers={'Idempotency-Key':'demo-'+key}
    if not create:
        current=client.get('/v1/tenants/'+tenant); current.raise_for_status()
        headers['If-Match']=current.headers['etag']
    r=client.request(method,path,json=body,headers=headers)
    if r.status_code>=400: raise RuntimeError(f'Admin operation failed: {method} {path} HTTP {r.status_code}')
    return r.json()
for i,bot in enumerate(settings['bots']):
    tenant=bot['tenant_id']; root='/v1/tenants/'+tenant
    existing=client.get(root)
    if existing.status_code==404:
        write('POST','/v1/tenants',{'tenant_id':tenant,'display_name':bot['name']},tenant,True)
    else: existing.raise_for_status()
    model={**settings['model'],'api_key_ref':{'uri':'file:///run/secrets/model_api_key'},'timeout_seconds':120}
    result=write('POST',root+'/config-revisions',{'app_id':app_id,'config':{
        'model':model,'storage':{'profile_id':'default'},'instructions':bot['instructions'],
        'budget':{'max_tokens_per_turn':2048}}},tenant)
    version=result['version']
    write('POST',root+f'/config-revisions/{version}:activate',{'app_id':app_id,'percentage':100},tenant)
    write('PUT',root+'/channel-bindings/'+bot['binding_id'],{
        'app_id':app_id,'channel':'wecom_ai_bot','account_id':bot['bot_id'],'enabled':True,
        'secret_refs':{'bot_secret':{'uri':f'file:///run/secrets/wecom_bot_secret_{"ab"[i]}'}}},tenant)
    print(f'{tenant}: model configuration {version} active; binding registered; awaiting provider authentication.')
client.close()
PY
fi
if [ "$ACTION" = demo ] || { ! "$ONLINE" && [ "$ACTION" = up ]; }; then
    echo 'Local preflight: two tenants, signed HTTP callbacks, duplicate-message handling, real Workers.'
    echo 'The model is the submitted offline Agent; outbound stays queued (no IM provider is called).'
    compose exec -T worker python - <<'PY'
import asyncio,base64,hashlib,json,pathlib,time,uuid
import asyncpg,httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
from trpc_service._cli import _worker_database_dsn
from trpc_service.config.settings import ServiceSettings
from trpc_service.config.secrets import LocalSecretProvider
token=pathlib.Path('/run/secrets/development_token').read_text().strip()
key=pathlib.Path('/run/secrets/session_hmac_key').read_text().strip()
client=httpx.Client(base_url='http://admin:8081',headers={'Authorization':'Bearer '+token},timeout=15)
def write(method,path,body,tenant,create=False):
    digest=hashlib.sha256(json.dumps([method,path,body],sort_keys=True).encode()).hexdigest()
    headers={'Idempotency-Key':'preflight-'+digest}
    if not create:
        r=client.get('/v1/tenants/'+tenant); r.raise_for_status(); headers['If-Match']=r.headers['etag']
    r=client.request(method,path,json=body,headers=headers)
    if r.status_code>=400: raise RuntimeError(f'Admin preflight failed: {method} {path} HTTP {r.status_code}')
    return r.json()
run=uuid.uuid4().hex
message='demo-'+run
targets=[]
for suffix in 'ab':
    tenant='preflight-tenant-'+suffix; binding='preflight-feishu-'+suffix; account='cli_preflight_'+suffix
    root='/v1/tenants/'+tenant
    r=client.get(root)
    if r.status_code==404:
        write('POST','/v1/tenants',{'tenant_id':tenant,'display_name':'Offline preflight '+suffix},tenant,True)
    else: r.raise_for_status()
    result=write('POST',root+'/config-revisions',{'app_id':'preflight','config':{
        'model':{'provider':'offline','model':'deterministic'},'storage':{'profile_id':'default'},
        'instructions':'Local synthetic preflight; no external model or IM calls.'}},tenant)
    write('POST',root+f'/config-revisions/{result["version"]}:activate',{'app_id':'preflight','percentage':100},tenant)
    write('PUT',root+'/channel-bindings/'+binding,{'app_id':'preflight','channel':'feishu',
        'account_id':account,'enabled':True,'secret_refs':{name:{'uri':'file:///run/secrets/session_hmac_key'}
        for name in ('app_secret','verification_token','encrypt_key')}},tenant)
    now=str(int(time.time()*1000))
    payload={'schema':'2.0','header':{'event_id':'event-'+run,'event_type':'im.message.receive_v1',
        'create_time':now,'token':key,'app_id':account},'event':{
        'sender':{'sender_id':{'open_id':'ou_'+run},'sender_type':'user'},
        'message':{'message_id':message,'create_time':now,'chat_id':'oc_'+run,'chat_type':'p2p',
        'message_type':'text','content':json.dumps({'text':'Local PR7 runtime preflight'})}}}
    raw=json.dumps(payload,separators=(',',':')).encode()
    pad=padding.PKCS7(128).padder(); padded=pad.update(raw)+pad.finalize()
    iv=uuid.uuid4().bytes; cipher=Cipher(algorithms.AES(hashlib.sha256(key.encode()).digest()),modes.CBC(iv)).encryptor()
    body=json.dumps({'encrypt':base64.b64encode(iv+cipher.update(padded)+cipher.finalize()).decode()},separators=(',',':')).encode()
    timestamp=str(int(time.time())); nonce=uuid.uuid4().hex
    signature=hashlib.sha256(timestamp.encode()+nonce.encode()+key.encode()+body).hexdigest()
    headers={'content-type':'application/json','X-Lark-Request-Timestamp':timestamp,
        'X-Lark-Request-Nonce':nonce,'X-Lark-Signature':signature}
    url='http://gateway:8080/v1/channels/feishu/'+binding+'/callback'
    with httpx.Client(timeout=15) as gateway:
        rejected=gateway.post(url,content=body,headers={**headers,'X-Lark-Signature':'0'*64})
        assert rejected.status_code==403, 'Invalid signatures must be rejected'
        for _ in range(3):
            response=gateway.post(url,content=body,headers=headers)
            assert response.status_code==200, f'Callback HTTP {response.status_code}'
    targets.append(tenant)
client.close()
async def inspect():
    connection=await asyncpg.connect(_worker_database_dsn(ServiceSettings(),LocalSecretProvider()))
    sessions=[]
    try:
        for tenant in targets:
            for attempt in range(60):
                inbound=await connection.fetch('SELECT inbound_id,session_id,status FROM inbound_messages WHERE tenant_id=$1 AND external_message_id=$2',tenant,message)
                if inbound and inbound[0]['status']=='committed': break
                await asyncio.sleep(1)
            assert len(inbound)==1 and inbound[0]['status']=='committed', f'{tenant}: expected exactly one committed inbound'
            turns=await connection.fetchval("SELECT count(*) FROM session_turns WHERE tenant_id=$1 AND inbound_id=$2 AND status='committed'",tenant,inbound[0]['inbound_id'])
            replies=await connection.fetch('SELECT status,payload_json FROM outbound_messages WHERE tenant_id=$1 AND session_id=$2',tenant,inbound[0]['session_id'])
            assert turns==1 and len(replies)==1, f'{tenant}: duplicate turn or missing outbound'
            reply=json.loads(replies[0]['payload_json'])
            sessions.append(inbound[0]['session_id'])
            print(json.dumps({'tenant':tenant,'callbacks':3,'inbound':len(inbound),'committed_turns':turns,
                'outbound':len(replies),'delivery_status':replies[0]['status'],'reply':reply.get('text'),
                'invalid_signature_http':403},ensure_ascii=False))
        assert sessions[0]!=sessions[1], 'Tenant sessions must be distinct'
        print('PASS: two tenants isolated; duplicate callbacks commit once; the original Agent execution completed.')
    finally: await connection.close()
asyncio.run(inspect())
PY
fi
if [ "$ACTION" = verify ] || [ "$ACTION" = connections ]; then
    compose exec -T worker python - "$ACTION" <<'PY'
import asyncio,json,pathlib,sys
from trpc_service._cli import _worker_database_dsn
from trpc_service.config.settings import ServiceSettings
from trpc_service.config.secrets import LocalSecretProvider
import asyncpg
async def main():
    settings=ServiceSettings(); secrets=LocalSecretProvider()
    connection=await asyncpg.connect(_worker_database_dsn(settings,secrets))
    ready=True
    try:
        demo_config=json.loads(pathlib.Path('/run/demo/wecom.json').read_text(encoding='utf-8'))
        for bot in demo_config['bots']:
            tenant=bot['tenant_id']
            state=await connection.fetchrow('SELECT phase,epoch,authenticated_at,updated_at FROM wecom_connection_state WHERE tenant_id=$1 ORDER BY updated_at DESC LIMIT 1',tenant)
            counts=await connection.fetchrow("SELECT (SELECT count(*) FROM inbound_messages WHERE tenant_id=$1) AS inbound,(SELECT count(*) FROM session_turns WHERE tenant_id=$1 AND status='committed') AS committed_turns,(SELECT count(*) FROM outbound_messages WHERE tenant_id=$1 AND status='delivered' AND provider_message_id IS NOT NULL) AS delivered_with_receipt",tenant)
            print(json.dumps({'tenant':tenant,'connection':dict(state) if state else None,**dict(counts)},default=str))
            ready=ready and state is not None and state['phase']=='authenticated'
            if sys.argv[1]=='verify': ready=ready and counts['delivered_with_receipt']>0
    finally: await connection.close()
    if not ready:
        print('NOT READY: check both authentications; verify additionally requires each bot to receive a real message and return a provider-confirmed reply.')
        raise SystemExit(1)
    print('PASS: both WeCom bindings are authenticated.' if sys.argv[1]=='connections' else 'PASS: both WeCom bindings are authenticated and both have delivered replies with provider receipts.')
asyncio.run(main())
PY
fi
verify_source
compose ps
show_urls
if { [ "$ACTION" = up ] || [ "$ACTION" = rebuild ]; } && [ "${TRPC_DEMO_OPEN_UI:-1}" != 0 ]; then
    if ! open_ui; then
        echo 'Services started, but a browser page could not be opened. Use the URLs above.' >&2
    fi
fi
