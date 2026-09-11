"""Idempotently register operator profiles and tenant configuration snapshots."""

import argparse
import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("TRPC_MANAGEMENT_URL", "http://localhost:8080"))
    parser.add_argument("--profiles-file", default=os.environ.get("TRPC_BOOTSTRAP_PROFILES"))
    parser.add_argument("--tenants-file", default=os.environ.get("TRPC_BOOTSTRAP_TENANTS"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    mode = os.environ.get("TRPC_RUNTIME_MODE", "simulation")
    profiles_file = Path(args.profiles_file) if args.profiles_file else root / (
        "protected-profiles.example.json" if mode == "protected" else "management-profiles.json")
    tenants_file = Path(args.tenants_file) if args.tenants_file else root / (
        "protected-tenants.example.json" if mode == "protected" else "simulation-tenants.json")
    if mode == "protected" and (not args.profiles_file or not args.tenants_file):
        raise SystemExit("protected bootstrap requires --profiles-file and --tenants-file")
    token = os.environ.get("TRPC_ADMIN_TOKEN")
    if not token or len(token) < 32:
        raise SystemExit("TRPC_ADMIN_TOKEN must contain at least 32 characters")

    def request(method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = Request(args.url.rstrip("/") + "/admin" + path, data=data, method=method,
                          headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=30) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            try:
                detail = json.load(error)
            except Exception:
                detail = {}
            return error.code, detail

    def profile_key(value):
        fields = ("profile_id", "profile_revision", "kind", "provider", "endpoint", "capabilities", "options")
        return {key: value.get(key) for key in fields}

    profiles = json.loads(profiles_file.read_text(encoding="utf-8"))
    profile_status, existing_profiles = request("GET", "/backend-profiles")
    if profile_status != 200:
        raise SystemExit(f"cannot list backend profiles (HTTP {profile_status})")
    for profile in profiles:
        status, _ = request("POST", "/backend-profiles", profile)
        if status == 201:
            continue
        existing = next((item for item in existing_profiles
                         if item.get("profile_id") == profile.get("profile_id")
                         and item.get("profile_revision") == profile.get("profile_revision")), None)
        if status == 409 and existing and profile_key(existing) == profile_key(profile):
            continue
        raise SystemExit(f"profile {profile.get('profile_id')} registration failed (HTTP {status})")

    tenants = json.loads(tenants_file.read_text(encoding="utf-8"))
    for config in tenants:
        path = f"/tenants/{config['tenant_id']}/configs"
        body = {"config": config, "profile_revisions": {role: 1 for role in config["data_backends"]}}
        status, _ = request("POST", path, body)
        if status not in (201, 409):
            raise SystemExit(f"tenant {config['tenant_id']} draft failed (HTTP {status})")
        if status == 409:
            status_existing, existing = request("GET", f"{path}/{config['config_version']}")
            if status_existing != 200 or existing.get("config") != config:
                raise SystemExit(f"tenant {config['tenant_id']} version drift detected; refusing overwrite")
        publish_status, _ = request("POST", f"{path}/{config['config_version']}/publish",
                                    {"expected_active_version": 0})
        if publish_status == 409:
            versions_status, versions = request("GET", path)
            active = next((item for item in versions if item.get("config_version") == config["config_version"]), None)
            if versions_status != 200 or not active or active.get("status") != "published":
                raise SystemExit(f"tenant {config['tenant_id']} publish failed (HTTP 409)")
        elif publish_status != 200:
            raise SystemExit(f"tenant {config['tenant_id']} publish failed (HTTP {publish_status})")
    print(f"Bootstrap complete: {len(profiles)} profiles, {len(tenants)} tenants")


if __name__ == "__main__":
    main()
