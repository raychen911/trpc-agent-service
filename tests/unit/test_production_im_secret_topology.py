from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
PRODUCTION = ROOT / "deploy" / "kustomize" / "overlays" / "production"


def test_production_overlay_mounts_im_secrets_only_on_required_roles() -> None:
    kustomization = yaml.safe_load((PRODUCTION / "kustomization.yaml").read_text("utf-8"))
    assert {patch["path"] for patch in kustomization["patches"]} >= {"im-secret-mounts-patch.yaml"}

    documents = list(
        yaml.safe_load_all((PRODUCTION / "im-secret-mounts-patch.yaml").read_text("utf-8"))
    )
    assert {document["metadata"]["name"] for document in documents} == {
        "trpc-gateway",
        "trpc-worker",
        "trpc-channel-dispatcher",
        "trpc-wecom-connector",
    }
    for document in documents:
        pod = document["spec"]["template"]["spec"]
        assert pod["volumes"] == [
            {
                "name": "im-secrets",
                "secret": {"secretName": "trpc-im-secrets", "defaultMode": 0o440},
            }
        ]
        assert pod["containers"][0]["volumeMounts"] == [
            {"name": "im-secrets", "mountPath": "/run/secrets/im", "readOnly": True}
        ]


def test_im_secret_example_has_exact_provider_keys() -> None:
    documents = list(
        yaml.safe_load_all(
            (ROOT / "deploy" / "kustomize" / "base" / "secrets.example.yaml").read_text("utf-8")
        )
    )
    secret = next(item for item in documents if item["metadata"]["name"] == "trpc-im-secrets")
    assert set(secret["stringData"]) == {
        "feishu_app_secret",
        "feishu_verification_token",
        "feishu_encrypt_key",
        "wecom_bot_secret",
    }
