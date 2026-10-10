from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PRODUCTION = ROOT / "deploy" / "kustomize" / "overlays" / "production"


def test_production_wecom_replicas_require_distinct_nodes() -> None:
    kustomization = yaml.safe_load((PRODUCTION / "kustomization.yaml").read_text("utf-8"))
    assert {entry["path"] for entry in kustomization["patches"]} >= {
        "replicas-patch.yaml",
        "wecom-ha-patch.yaml",
    }

    patch = yaml.safe_load((PRODUCTION / "wecom-ha-patch.yaml").read_text("utf-8"))
    assert patch["metadata"]["name"] == "trpc-wecom-connector"
    pod_spec = patch["spec"]["template"]["spec"]
    constraints = pod_spec["topologySpreadConstraints"]
    assert any(
        constraint["topologyKey"] == "kubernetes.io/hostname"
        and constraint["whenUnsatisfiable"] == "DoNotSchedule"
        for constraint in constraints
    )
    required = pod_spec["affinity"]["podAntiAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"
    ]
    assert any(rule["topologyKey"] == "kubernetes.io/hostname" for rule in required)
