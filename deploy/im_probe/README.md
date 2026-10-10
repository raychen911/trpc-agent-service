# 独立 Feishu/WeCom 在线验收探针

`server.py` 是 `scripts/im_online_gate.py` 所需的独立签名端点。它只负责
验证本次请求是否属于已部署候选、调用一个外部 provider runner、验证 runner
返回的供应商证据，并用 Ed25519 私钥签名响应。它不属于 Gateway、Channel
Dispatcher 或 WeCom Connector 进程，因此不会把被测服务自己的状态当作独立证据。

探针不实现或伪造平台行为。没有 `TRPC_IM_PROBE_RUNNER`、runner 非零退出、输出
不是严格 JSON、缺少完整 provider evidence，或者任一 observation 不满足
`im_online_gate.py` 的校验契约时，端点仍可返回签名响应，但 case 全部是
`not_run`，生产门禁不会通过。

## 组件与信任边界

| 组件 | 运行位置 | 能看到的内容 | 不能替代的证据 |
| --- | --- | --- | --- |
| `server.py` | 独立探针主机 | 候选绑定、账号指纹、driver 的脱敏结果 | 供应商 callback/WSS/ACK |
| `provider_runner.py` | checkout 外的固定可执行文件 | 当前通道 profile、账号 ID、Secret 文件路径 | provider observation |
| `control_broker.py` | host-only Unix socket | 固定 profile、固定 action executable/SHA-256/argv | action 执行结果 |
| `feishu_callback_observer.py` | loopback mirror + Unix socket | 独立验签/解密后的域分离哈希 | OpenAPI 发送 ACK |
| `feishu_openapi_witness.py` | loopback HTTPS 代理 + Unix socket | 状态码、平台码、Retry-After、请求 ID/请求体哈希 | callback 入站 |
| `feishu_provider_driver.py` | checkout 外的固定可执行文件 | broker 结果及两条 witness 的哈希 receipt | operator action |
| `wecom_provider_driver.py` | checkout 外的固定可执行文件 | broker 结果及 Admin 的 epoch/lifecycle 哈希快照 | 第二条 WSS 或平台重放 |

broker 的 action executable 是部署/租户特定的受审控制器：它负责按固定 action 对测试账号、ACK 集群
和现有 Connector 执行真实操作，并返回严格 JSON。本仓库故意不提供一份持有 Kubernetes 管理员、
数据库、OIDC 或供应商管理员凭据的通用脚本。缺少任一 action executable、配置未固定其字节 SHA-256，
或文件被替换但未同步更新受审摘要时，broker `--check` 以及每次 action 调用都会 fail closed；
用静态 JSON、录制结果或 driver 自报字段替代 action 会使真实在线门禁失去独立性。

## 凭据安装边界

正式应用凭据由 Kubernetes Secret/外部 Secret 管理器提供给 production cluster；独立验收探针不挂载
应用 Pod 的 Secret。部署探针时，应由受控 Secret 管理器以 root 将同一测试账号的凭据原子安装为
`/etc/trpc-im-probe/secrets` 下的同名**普通文件**；目录及其任一父目录、文件本身都不能是
symlink。目录使用 `root:trpcagent 0750`，每个凭据文件和签名私钥使用
`root:trpcagent 0640`，写入临时文件并完成 owner/mode 校验后再原子 rename。不要把其他 Secret 目录
symlink 到探针目录，也不要让 `trpcagent` 对目录或文件拥有写权限；否则 host 预检和探针会
fail closed。`im-probe.env` 与 `feishu-observer.env` 只填写新路径，不复制 secret 值。

账号 ID 通过 `TRPC_IM_PROBE_FEISHU_APP_ID` 和
`TRPC_IM_PROBE_WECOM_BOT_ID` 配置，并且必须与实际 binding/account 一致。
探针只读取 secret 计算指纹；runner 通过环境变量拿到文件路径，不会从探针
响应中得到 secret 内容。

## 安装边界

1. 将 probe/runner/driver/broker/observer/witness 固定版本安装到
   `/opt/trpc-im-probe/current` 和 `/usr/local/libexec`；不要直接执行被测应用的可变 symlink。将
   `im-probe.env.example` 复制为 host-only 的 `/etc/trpc-im-probe/im-probe.env`，填入当前候选镜像 digest、
   固定探针身份指纹、真实账号 ID、secret 路径和经过审查的 runner 绝对路径。
   runner 应安装在应用 checkout 之外（模板使用
   `/usr/local/libexec/trpc-im-provider-runner`），由独立验收代码所有者审查。
   仓库中的 `provider_runner.py`、`feishu_provider_driver.py` 和 `wecom_provider_driver.py` 是可安装的
   fail-closed 编排器；复制到上述路径后设为 `root:root 0755`，且路径任一层都不能是 symlink。
   在独立探针主机的最终安装路径对 runner 和两个 driver 分别执行 `sha256sum`，把结果写入
   `TRPC_IM_PROBE_RUNNER_SHA256`、`TRPC_IM_PROBE_FEISHU_DRIVER_SHA256` 和
   `TRPC_IM_PROBE_WECOM_DRIVER_SHA256`。探针和 runner 会在每次执行前重新 open/fstat/hash；文件被替换
   或 owner/mode/父目录链不再受 root 控制时必须保持 `not_ready`/`not_run`。IM probe 部署在与被测
   production cluster 隔离的验收主机；Gateway、Channel Dispatcher 和 Feishu/WeCom Connector 仍全部
   位于 production cluster，集群内不运行持有供应商管理权限的 provider action。
2. 把 `feishu-control-profile.example.json`、`wecom-control-profile.example.json`、
   `feishu-control-action.example.json`、`wecom-control-action.example.json` 和
   `control-broker.example.json` 渲染到 `/etc/trpc-im-probe`。两个 action 模板中的 `<...>` 都是必须替换的
   占位符；token 只填写由 Secret 管理器安装的绝对文件路径，不能把 token 值写入 JSON。先固定
   tenant/binding/account、URL、observer/witness socket 和 8 个 action，再对最终文件字节计算 SHA-256；
   同一值必须同时写入 broker config、
   `TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256` 或
   `TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256`。不要对模板或格式化前的文件计算摘要。
   五份渲染 JSON、两个 env 文件和 token 文件使用 `root:trpcagent 0640`，目录和 `secrets/` 使用
   `root:trpcagent 0750`。
   对每个最终安装的 broker action executable 字节计算 SHA-256，并把摘要写入相应 action 的必填
   `sha256` 字段；不要对本地模板、软链接目标名或安装前的其他副本计算摘要。action executable 使用
   `root:root 0755`，不得 group/other-writable。每次受审控制器更新都必须先更新摘要再执行 `--check`；
   未同步的替换必须保持 `not_ready`/`not_run`。
3. 生成一份独立的 32 字节 Ed25519 seed，以 Base64 单行形式保存到
   `TRPC_IM_PROBE_SIGNING_KEY_FILE`，权限设为 root:trpcagent `0640`；把对应公钥
   和 key ID 写入 `deploy/im-probe-trust.json`。私钥绝不能进入 Git、报告、日志或
   runner stdout。
4. 以 root 安装 `trpc-im-control-broker.service`、`trpc-im-feishu-callback-observer.service`、
   `trpc-im-feishu-openapi-witness.service` 和 `trpc-im-probe.service`。callback observer 使用独立的
   `feishu-observer.env`，不能加载含另一通道路径的 probe env。执行 `systemctl daemon-reload`、
   `systemctl enable --now`，确认四个单元 active，再确认探针的 `/health/ready` 为 200。探针进程
   只监听 loopback；公网 HTTPS 必须由单独的 nginx vhost 终止并做出口 allowlist。
   安装 unit 前先创建固定系统账号 `trpcimbroker`；probe/runner 使用既有 `trpcagent`，broker、callback
   observer 和 OpenAPI witness 使用 `trpcimbroker`，两者的主组统一为共享组 `trpcagent`。必须确认
   `id -u trpcimbroker` 与 `id -u trpcagent` 不同；不要使用 `DynamicUser`，也不要复用应用 UID。
   `trpc-im-control-broker.service` 创建并以 `trpcimbroker:trpcagent 0750` 持有
   `/run/trpc-im-probe`，两个 witness unit 使用同一 owner/group，probe unit 不声明或接管该
   `RuntimeDirectory`。broker 以 `trpcimbroker:trpcagent 0660` 创建 `control.sock`；把 broker 的数字 UID
   和其有效共享组 GID 写入
   `TRPC_IM_PROBE_BROKER_UID/GID`。runner 只能通过共享组连接，不能拥有或替换 runtime 目录/socket。
   两个 driver 都在实际 broker 请求所用的同一个已连接 socket 上、发送前用 Linux
   `SO_PEERCRED` 核对 broker UID/GID；独立的预检连接不能替代此检查。不要让 broker 与 runner 复用
   账号，也不要使用不稳定的动态 UID/GID。
5. 按 `nginx-server.conf.example` 配置独立 HTTPS 主机、证书和 release-gate
   出口 IP/CIDR allowlist。探针 URL 的 base 必须与 trust 文件和
   `TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST` 完全一致；门禁会自行追加 `/probe`。
   callback vhost 必须异步 mirror 到 8751；`/feishu-openapi/` 只允许 Channel Dispatcher 的固定出口
   CIDR并转发到 8752。不要把这两个 loopback 端口直接暴露到公网。
6. 仅在本次在线验收窗口，把 Channel Dispatcher 设置为
   `TRPC_SERVICE_ONLINE_TESTS_ENABLED=true` 和
   `TRPC_SERVICE_FEISHU_SEND_API_ROOT=https://<probe-host>/feishu-openapi` 后滚动更新。其他角色和正常运行
   始终保留 `https://open.feishu.cn`；验收结束立即恢复默认值并再次等待 rollout。

### action 配置的原子安装、权限与检查顺序

更新 action executable、profile 或 action 配置时，必须先停止 broker，避免它在多文件替换中看到混合
版本。所有 `.new` 文件必须直接暂存在目标文件的同一文件系统中；不要先覆盖最终路径，也不要通过
symlink 交换版本。推荐顺序如下：

1. 执行 `systemctl stop trpc-im-control-broker.service`，再用
   `install -d -o root -g trpcagent -m 0750` 创建 `/etc/trpc-im-probe` 和 `secrets/`。
2. 将两个 action executable 安装到 `/usr/local/libexec/.trpc-im-*-control-action.new`，设为
   `root:root 0755`；将 token、profile、action JSON 和 broker JSON 渲染到
   `/etc/trpc-im-probe/.<name>.new`，设为 `root:trpcagent 0640`。真实 token 由 Secret 管理器直接写入
   暂存文件，不能出现在 shell 参数、环境变量、模板或日志中。
3. 对**暂存后的**两个 executable 和两个 profile 执行 `sha256sum`。把 executable 摘要写入暂存的
   broker `sha256` 字段，把 profile 摘要写入对应 `control_profile_sha256`；此后不得再格式化或改写这些
   已取摘要的文件。
4. 用 `lstat`/`stat` 确认目标父目录和每个暂存文件都不是 symlink，owner/mode 与上文一致，且
   broker config、profile、action executable 和 token 都由 root（uid 0）所有，executable 不可
   group/other-writable。逐级检查每条受信路径的父目录直到 `/`：任何一级都不可 group/other-writable；
   非 root 所有的目录还不可 owner-writable。保持 broker 停止，先用同文件系统的 `mv -T` 原子替换
   token、executable、profile 和 action JSON，最后原子替换 `control-broker.json`。
5. 在**最终路径**重新执行 `sha256sum`，确认结果逐字匹配最终 broker 配置；再以服务账号运行配置检查：

   ```bash
   sudo -u trpcimbroker /usr/local/libexec/trpc-im-feishu-control-action \
     --config /etc/trpc-im-probe/feishu-control-action.json --check
   sudo -u trpcimbroker env \
     TRPC_IM_CONTROL_BROKER_CONFIG_FILE=/etc/trpc-im-probe/control-broker.json \
     /opt/trpc-im-probe/current/.venv/bin/python \
     /opt/trpc-im-probe/current/deploy/im_probe/control_broker.py --check
   ```

   任一 hash、权限或 `--check` 不通过都必须保持 broker 停止。全部通过后才执行
   `systemctl start trpc-im-control-broker.service` 和 `systemctl is-active trpc-im-control-broker.service`；
   不要用重启成功代替上述最终路径检查。

runner 必须执行真实的外部动作并在 stdout 输出一个严格 JSON 对象：

```json
{"artifact_attestation":{"schema_version":1,"runner_sha256":"...","runner_contract_version":1,"driver_sha256":"...","driver_contract_version":1},"provider_evidence":{"source":"...","independent_paths":["...","..."],"run_nonce":"...","account_fingerprint":"...","observations":{}}}
```

每个 case 的 action 必须按固定顺序调用 Admin：先以 `channel + run_id + run_nonce` 请求
`POST /v1/tenants/{tenant}/bindings/{binding}/im-acceptance/runs`，确认服务端返回的
`run_id_sha256` 和 `run_binding_sha256` 与本轮请求一致；随后才允许触发真实供应商动作。平台事件进入
业务数据库后，再把同一个 `run_id + run_nonce` 和 `provider_event_hash` 交给
`im-acceptance/event-evidence`。注册记录由数据库时间限定为 30–900 秒且只能绑定一个事件；旧事件、
错误 nonce、重复绑定、跨 run 复用或先于注册到达的事件都必须 fail-closed。迁移 `0021` 和 `0022`
必须在包含这些 action 的新 Pod 启动前完成，不能先滚动应用再补表。

它必须证明飞书 callback + OpenAPI send ack、企微 WebSocket event + send ack，
以及 round trip、idempotency、media、reconnect、rate-limit/Retry-After、credential
rotation、至少 60 秒 prolonged outage 和 response-drop ambiguous 共 8 个 case。
其中测试用户/群、媒体对象、限流触发、旧/新凭证切换、长断线窗口和人工 replay
批准仍需要外部平台账号/租户协调；本探针只提供安全的执行与签名边界，不把这些
缺失动作写成 pass。

| case | 飞书独立证据 | 企业微信独立证据 |
| --- | --- | --- |
| `round_trip` | callback receipt + OpenAPI ACK receipt | WSS event hash + send ACK |
| `idempotency` | 平台真实重投同一 event/message，observer 计数至少 2 | 同一已持久化 event 的 service replay，不同 processing ID |
| `media` | callback media locator 哈希 + 下载产物哈希 | WSS media event 哈希 + 下载产物哈希 |
| `reconnect` | Gateway EndpointSlice replacement 稳定 + 新 callback/ACK | 正常交接 disconnected→released→acquired→authenticated，或硬故障 takeover→authenticated；新 epoch 递增 |
| `rate_limit_retry_after` | witness 的平台限流/Retry-After 与后续成功 ACK | WSS send ACK 的平台限流/退避与后续成功 ACK |
| `credential_rotation` | 旧凭据拒绝、新凭据 callback/ACK | 旧 Secret 失效、新 epoch 认证及 ACK |
| `prolonged_outage` | 单 endpoint 故障至少 60 秒且冗余 endpoint 持续交付 | 单 Connector 故障至少 60 秒且热备接管 |
| `ambiguous` | witness 在真实 ACK 后丢下游响应，自动重放为 0 | send ACK 结果未知，自动重放为 0，人工 replay 标识 |

企业微信全部 WSS 同时断线时没有官方 replay cursor/resume/history pull。断线期间未进入本服务的消息
只能记录为 provider delivery gap（`not_run` 或 `fail`），不得用恢复后的新消息冒充旧消息恢复。
企业微信锁证据必须与 Admin 中的真实生命周期一致：正常交接设置
`old_lock_owner_released=true`，硬故障接管设置为 `false`，且不得为硬故障合成 `released` 事件；
两条路径都要求新 owner 已获取、epoch 至少递增到 2，并继续绑定真实 provider event、reply 和 ACK。

`provider_runner.py` 不直接实现供应商操作，而是把同一严格请求交给当前通道的
driver。driver 的 stdout 必须恰好是
`{"schema_version":1,"observations":{...八个 case...}}`；runner 会再次校验
时间、nonce、平台限流码、重连锁接管、60 秒断线和 ambiguous 无自动重放等语义，
再生成 provider evidence。driver 进程只得到当前通道账号 ID 和 secret 文件路径，
不会得到另一通道配置或应用 `PYTHONPATH`。因此 driver 的 shebang 应使用绝对解释器
路径（例如 `/usr/bin/python3`），不能依赖继承的 `PATH`。

本地只做配置/签名闭环检查：

```powershell
$env:TRPC_IM_PROBE_SIGNING_KEY_FILE = "<test-key-file>"
$env:TRPC_IM_PROBE_KEY_ID = "test-key"
$env:TRPC_IM_PROBE_RELEASE_CONTEXT_FILE = "<root-owned-release-context.json>"
$env:TRPC_IM_PROBE_IDENTITY_SHA256 = "<fixed-64-hex>"
... # 其余账号、secret 文件和 runner 变量见 im-probe.env.example
.\.venv\Scripts\python.exe deploy\im_probe\server.py --check
```

release context 必须是 root-owned、不可 group/other-writable 的非 symlink 普通文件，且只包含
`schema_version=1`、`release_id`、`nonce_sha256`、`source_fingerprint` 和不可变 `image_digest`。
probe 启动时会对当前部署目录执行与候选锁相同的 source fingerprint；仅复制新 context 到旧代码目录
会 fail closed，不能把旧探针服务伪装成新候选。

先用同一个 host-only 配置文件运行完整预检；它不会访问网络、执行 driver 或生成
生产 IM 通过证据：

```powershell
.\.venv\Scripts\python.exe scripts\im_probe_preflight.py `
  --mode local `
  --env-file deploy\im_probe\im-probe.env `
  --candidate-lock runs\multitenant\candidate-lock.json `
  --trust-file deploy\im-probe-trust.json
```

复制到 Linux 探针主机后再运行一次 `--mode host`，只有真实路径、权限、候选镜像、
release binding、URL allowlist、Ed25519 公私钥和两条 driver 全部一致时，预检的
`readiness` 才能是 `pass`；其 `production_gate` 永远保持 `not_run`。配置模板把探针
与 release-gate 的非凭据开关放在同一文件中，真实 provider secret 值仍由 Secret
管理器在执行在线门禁时注入，不能写入该文件。

项目“功能完成”可以用当前候选在真实 Feishu/WeCom 上的基础双向收发闭环：每个通道都必须有唯一
入站、唯一出站和供应商回执。该基础证据不属于 `im-online.json` 的生产 8-case，不能将
`online_im` 或 `production_gate` 写成 `pass`；生产发布和 release manifest 仍要求两个通道的完整
8-case。破坏性生产灾备默认必须真实通过；唯一例外是发布者显式使用 `--allow-functional-dr`，并由当前
候选的功能灾备 `pass` 授权其诚实保持 `not_run`。该选项不能豁免 `online_im=not_run` 或任何失败门禁。

production cluster 承载真实 IM 回调域名对应的 Gateway、Channel Dispatcher、Feishu/WeCom Connector，
以及 Kubernetes、迁移、故障、HPA 和性能验收。probe/control broker/runner/driver 位于独立验收主机，
只生成外部供应商证据，禁止承担性能压测或应用流量。两侧证据只有在 release context、实际测得 source
fingerprint 和不可变 image digest 与 candidate lock 完全一致时才能合并。可以在候选冻结前先做双向
收发 smoke，但最终签名 8-case 必须在候选锁和镜像 digest 冻结后重跑，再与同一集群的运行报告共同
生成 release manifest。

真正生产验收仍必须由下面这一条（同一候选、同一 release binding）命令发起：

```bash
TRPC_IM_ONLINE_TESTS_ENABLED=true \
TRPC_IM_ONLINE_PROBE_URL=https://probe.example.invalid \
TRPC_IM_ONLINE_PROBE_URL_ALLOWLIST=https://probe.example.invalid \
TRPC_IM_ONLINE_IMAGE_DIGEST=sha256:<candidate-image-digest> \
TRPC_IM_ONLINE_PROBE_IDENTITY_SHA256=<fixed-64-hex> \
TRPC_IM_ONLINE_FEISHU_CONTROL_PROFILE_SHA256=<feishu-profile-sha256> \
TRPC_IM_ONLINE_WECOM_CONTROL_PROFILE_SHA256=<wecom-profile-sha256> \
TRPC_RELEASE_ID=<release-id> TRPC_RELEASE_NONCE=<high-entropy-release-nonce> \
FEISHU_APP_ID=cli_<real-id> FEISHU_APP_SECRET=<secret-from-secret-manager> \
FEISHU_VERIFICATION_TOKEN=<secret-from-secret-manager> FEISHU_ENCRYPT_KEY=<secret-from-secret-manager> \
WECOM_BOT_ID=<real-id> WECOM_BOT_SECRET=<secret-from-secret-manager> \
python scripts/im_online_gate.py --timeout-seconds 240 --require-production \
  --output runs/multitenant/im-online.json
```

其中 `<secret-from-secret-manager>` 只能由不落盘的 Secret 注入机制替换，不能把
真实值写入命令历史、代码或报告。该命令必须在已安装并 ready 的独立探针上运行，
并在同一 release ID、release nonce 和候选镜像 digest 下运行。`server.py` 自己
生成的签名响应不能替代 provider-originated evidence。
