# Registry candidate release

`scripts/candidate_session.py` orchestrates the two immutable image references needed by
the production Kubernetes gate: an initial candidate and a distinct rollout
candidate. It invokes the registry publisher exactly once, writes that first
result only as a private receipt, creates one release-specific private context,
then rebinds the already-published immutable digests to the context nonce. The
formal binding and lock are staged and installed as a pair only after their
release ID, nonce hash, source fingerprint, and both image digests agree. The
raw nonce is never written to a public artifact or printed.

The command uses a single-platform `linux/amd64` build so the digest observed
by the registry and Kubernetes refers to the same manifest. It does not apply
Kubernetes resources.

```powershell
$repository = "ghcr.io/<owner>/trpc-agent-service"

.venv\Scripts\python.exe -m scripts.candidate_session publish `
  --repository $repository `
  --output runs\multitenant\registry-image-binding.json `
  --lock-output runs\multitenant\candidate-lock.json `
  --private-directory runs\multitenant\.ack-runtime-private `
  --public-directory runs\multitenant
```

The Docker credential helper supplies authentication. The command only prints
the release ID, source fingerprint, immutable references, artifact paths, and
binding digest; do not
replace it with `docker login --password` or echo credential variables.

After the command succeeds, load the binding report and pass the values to the
production gates in the same PowerShell session:

```powershell
$binding = Get-Content runs\multitenant\candidate-lock.json -Raw | ConvertFrom-Json
$env:TRPC_REAL_IMAGE_DIGEST = [string]$binding.image_digest
$env:TRPC_MIGRATION_IMAGE_DIGEST = [string]$binding.image_digest
$env:TRPC_K8S_RUNTIME_IMAGE = [string]$binding.images.initial.reference
$env:TRPC_K8S_RUNTIME_UPGRADE_IMAGE = [string]$binding.images.upgrade.reference
```

`TRPC_K8S_RUNTIME_IMAGE` and `TRPC_K8S_RUNTIME_UPGRADE_IMAGE` must remain the
full `repository@sha256:...` values. The release reports, manifest, and
Kubernetes runtime evidence must then be produced with the same release ID,
release nonce, source fingerprint, and initial image digest. A local Docker
image ID or a tag-only reference is not a registry digest and cannot satisfy
the production release gate.

发布命令在两次 push 后重新计算 checkout 指纹，并生成 `candidate-lock.json`。后续 runtime gate 的
`release.image_binding` 必须指向这个 lock，而不是重新读取可被替换的 tag；lock 同时固定原始 binding
内容哈希、release ID/nonce 哈希、源码指纹和两条 `repository@sha256:...` 引用。可在每个生产演练前
再次执行以下只读校验：

```powershell
.venv\Scripts\python.exe scripts\candidate_lock.py verify
```

The repository's production Kustomize overlay keeps a placeholder registry and
digest on purpose. Replace it only in the reviewed deployment input or pass
the digest-pinned references through the runtime gate; never commit credentials
or an unreviewed mutable tag.
