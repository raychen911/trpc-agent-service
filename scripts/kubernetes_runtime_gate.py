"""Static and non-destructive runtime gates for a Kubernetes candidate."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_DEPLOYMENTS = {
    "gateway",
    "worker",
    "compensation-worker",
    "outbound-worker",
    "durable-outbox-worker",
    "session-ready-worker",
    "mailbox-maintenance",
}
REQUIRED_PDBS = {"gateway", "worker", "durable-outbox-worker"}


def _check(name: str, ok: bool, detail: str) -> dict[str, object]:
    return {"name": name, "ok": bool(ok), "detail": detail}


def validate_manifest(
    manifest_path: str | Path | None = None,
    kustomization_path: str | Path | None = None,
) -> dict[str, object]:
    """Validate production invariants without requiring a live cluster."""

    manifest_file = Path(manifest_path or ROOT / "deployment" / "kubernetes" / "platform.yaml")
    kustomization_file = Path(
        kustomization_path or ROOT / "deployment" / "kubernetes" / "kustomization.yaml"
    )
    checks: list[dict[str, object]] = []
    try:
        import yaml
        documents = [item for item in yaml.safe_load_all(manifest_file.read_text(encoding="utf-8")) if item]
        kustomization = yaml.safe_load(kustomization_file.read_text(encoding="utf-8"))
    except ImportError as exc:
        return {
            "status": "fail",
            "manifest": str(manifest_file),
            "checks": [_check("parse manifests", False, f"PyYAML is required: {exc}")],
        }
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return {
            "status": "fail",
            "manifest": str(manifest_file),
            "checks": [_check("parse manifests", False, str(exc)[:1000])],
        }

    resources: dict[tuple[str, str], dict] = {}
    duplicate_resources: list[str] = []
    for document in documents:
        kind = str(document.get("kind", ""))
        name = str(document.get("metadata", {}).get("name", ""))
        key = (kind, name)
        if key in resources:
            duplicate_resources.append(f"{kind}/{name}")
        resources[key] = document
    checks.append(
        _check(
            "unique resource identities",
            not duplicate_resources,
            "duplicate resources: " + ", ".join(duplicate_resources)
            if duplicate_resources
            else "all kind/name pairs are unique",
        )
    )

    namespace = resources.get(("Namespace", "trpc-agent"))
    checks.append(_check("production namespace", namespace is not None, "Namespace/trpc-agent is present"))

    deployments = {
        name: document
        for (kind, name), document in resources.items()
        if kind == "Deployment"
    }
    missing_deployments = sorted(REQUIRED_DEPLOYMENTS - set(deployments))
    checks.append(
        _check(
            "required application deployments",
            not missing_deployments,
            "missing: " + ", ".join(missing_deployments)
            if missing_deployments
            else "gateway and all worker roles are present",
        )
    )

    insecure_containers: list[str] = []
    unpinned_images: list[str] = []
    missing_gateway_probes: list[str] = []
    for name, deployment in deployments.items():
        pod_spec = deployment.get("spec", {}).get("template", {}).get("spec", {})
        for container in pod_spec.get("containers", []):
            container_name = f"{name}/{container.get('name', '<unnamed>')}"
            image = str(container.get("image", ""))
            if not image or image.endswith(":latest"):
                unpinned_images.append(container_name)
            security = container.get("securityContext", {})
            if not (
                security.get("allowPrivilegeEscalation") is False
                and security.get("readOnlyRootFilesystem") is True
                and security.get("runAsNonRoot") is True
                and "ALL" in security.get("capabilities", {}).get("drop", [])
            ):
                insecure_containers.append(container_name)
        if name == "gateway":
            container = (pod_spec.get("containers") or [{}])[0]
            if not container.get("readinessProbe") or not container.get("livenessProbe"):
                missing_gateway_probes.append("gateway")
    checks.append(
        _check(
            "container security contexts",
            not insecure_containers,
            "insecure containers: " + ", ".join(insecure_containers)
            if insecure_containers
            else "all production containers are non-root, read-only, and drop capabilities",
        )
    )
    checks.append(
        _check(
            "container image contract",
            not unpinned_images,
            "missing image or latest tag: " + ", ".join(unpinned_images)
            if unpinned_images
            else "no container uses the latest tag; release overlay controls the final image",
        )
    )
    checks.append(
        _check(
            "gateway health probes",
            not missing_gateway_probes,
            "gateway readiness and liveness probes are present",
        )
    )

    required_kinds = {
        ("ExternalSecret", "trpc-agent-secrets"),
        ("ExternalSecret", "trpc-agent-migration-secrets"),
        ("HorizontalPodAutoscaler", "worker"),
        *{("PodDisruptionBudget", name) for name in REQUIRED_PDBS},
        ("NetworkPolicy", "gateway-egress"),
        ("NetworkPolicy", "worker-egress"),
    }
    missing_resources = sorted(
        f"{kind}/{name}" for kind, name in required_kinds if (kind, name) not in resources
    )
    checks.append(
        _check(
            "availability and secret controls",
            not missing_resources,
            "missing: " + ", ".join(missing_resources)
            if missing_resources
            else "ExternalSecrets, HPA, PDB, and egress policy resources are present",
        )
    )

    ingress = resources.get(("Ingress", "gateway"), {})
    tls = ingress.get("spec", {}).get("tls", [])
    checks.append(
        _check(
            "TLS ingress",
            bool(tls) and all(item.get("secretName") for item in tls),
            "gateway ingress declares a TLS secret" if tls else "gateway ingress has no TLS block",
        )
    )

    image_overrides = (kustomization or {}).get("images", []) if isinstance(kustomization, dict) else []
    override_ok = any(
        item.get("name") == "ghcr.io/xyxhhhhh/trpc-agent-service"
        and item.get("newName")
        and item.get("newTag")
        and str(item.get("newTag")).lower() != "latest"
        for item in image_overrides
        if isinstance(item, dict)
    )
    checks.append(
        _check(
            "release image override",
            override_ok,
            "Kustomize requires an explicit release image replacement"
            if override_ok
            else "missing explicit application image replacement",
        )
    )
    return {
        "status": "pass" if all(item["ok"] for item in checks) else "fail",
        "manifest": str(manifest_file),
        "kustomization": str(kustomization_file),
        "checks": checks,
    }


def _run(command: list[str], timeout: int = 120) -> dict[str, object]:
    try:
        result = subprocess.run(command, text=True, capture_output=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"ok": False, "command": command, "error": str(exc)}
    return {
        "ok": result.returncode == 0,
        "command": command,
        "output": (result.stdout + result.stderr)[-3000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--static", action="store_true", help="validate repository manifests without a cluster")
    parser.add_argument("--manifest", default=str(ROOT / "deployment" / "kubernetes" / "platform.yaml"))
    parser.add_argument(
        "--kustomization",
        default=str(ROOT / "deployment" / "kubernetes" / "kustomization.yaml"),
    )
    args = parser.parse_args()
    if args.static:
        report = validate_manifest(args.manifest, args.kustomization)
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "pass" else 1

    namespace = os.getenv("KUBE_NAMESPACE", "trpc-agent")
    deployments = ["gateway", "worker", "compensation-worker", "outbound-worker", "durable-outbox-worker"]
    checks = [
        _run(["kubectl", "get", "namespace", namespace]),
        _run(["kubectl", "get", "hpa", "worker", "-n", namespace]),
        _run(["kubectl", "get", "pdb", "-n", namespace]),
    ]
    checks.extend(
        _run(["kubectl", "rollout", "status", f"deployment/{name}", "-n", namespace], timeout=300)
        for name in deployments
    )
    if os.getenv("KUBE_NODE_EVICTION_ACCEPTANCE", "0").lower() in {"1", "true", "yes", "on"}:
        checks.append(
            _run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    namespace,
                    "-l",
                    "app=trpc-agent-worker",
                    "-o",
                    "wide",
                ]
            )
        )
    report = {"status": "pass" if all(item["ok"] for item in checks) else "fail", "checks": checks}
    print(json.dumps(report, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
