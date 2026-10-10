from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
POLICY_ROOT = ROOT / "deploy" / "kustomize" / "im-external-egress"
OVERLAYS = {
    "production": ROOT / "deploy" / "kustomize" / "overlays" / "production",
    "performance": ROOT / "deploy" / "kustomize" / "overlays" / "performance",
}
PRODUCTION_README = OVERLAYS["production"] / "README.md"


def _policy() -> dict:
    return yaml.safe_load((POLICY_ROOT / "network-policy.yaml").read_text("utf-8"))


def _render(overlay: Path) -> list[dict]:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required for Kustomize render verification")
    completed = subprocess.run(  # noqa: S603 - resolved kubectl and fixed local overlay
        [kubectl, "kustomize", str(overlay)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    return [
        document for document in yaml.safe_load_all(completed.stdout) if isinstance(document, dict)
    ]


def test_im_external_egress_is_only_two_workloads_on_tcp_443() -> None:
    policy = _policy()
    spec = policy["spec"]

    assert policy["metadata"]["name"] == "trpc-allow-im-provider-https-egress"
    assert spec["podSelector"] == {
        "matchExpressions": [
            {
                "key": "app.kubernetes.io/name",
                "operator": "In",
                "values": ["trpc-channel-dispatcher", "trpc-wecom-connector"],
            }
        ]
    }
    assert spec["policyTypes"] == ["Egress"]
    assert spec["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "0.0.0.0/0"}}],
            "ports": [{"port": 443, "protocol": "TCP"}],
        }
    ]
    assert "ingress" not in spec


def test_im_external_egress_portable_fallback_is_documented() -> None:
    policy_source = (POLICY_ROOT / "network-policy.yaml").read_text("utf-8")
    readme = PRODUCTION_README.read_text("utf-8")

    for source in (policy_source, readme):
        normalized = " ".join(source.split())
        assert "Standard Kubernetes NetworkPolicy" in normalized
        assert "FQDN" in normalized
        assert "0.0.0.0/0" in normalized
    assert "trpc-channel-dispatcher" in readme
    assert "trpc-wecom-connector" in readme


@pytest.mark.parametrize("overlay_name", sorted(OVERLAYS))
def test_im_external_egress_is_referenced_and_rendered(overlay_name: str) -> None:
    overlay = OVERLAYS[overlay_name]
    kustomization = yaml.safe_load((overlay / "kustomization.yaml").read_text("utf-8"))

    assert "../../im-external-egress" in kustomization["resources"]
    rendered = _render(overlay)
    matches = [
        item
        for item in rendered
        if item.get("kind") == "NetworkPolicy"
        and item.get("metadata", {}).get("name") == "trpc-allow-im-provider-https-egress"
    ]
    assert len(matches) == 1
    assert matches[0]["metadata"]["namespace"] == "trpc-service"
    assert matches[0]["spec"] == _policy()["spec"]
