"""Reject the unrelated local fork when testing or starting a service runtime."""

import json
import subprocess
from importlib.metadata import distribution
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname


OFFICIAL_URL = "https://github.com/trpc-group/trpc-agent-python.git"
OFFICIAL_COMMIT = "f05797d9f9dff2461922b5985aeccc1b636b7c8d"


def verify_official_sdk():
    source = json.loads(distribution("trpc-agent-py").read_text("direct_url.json") or "{}")
    if source.get("url") == OFFICIAL_URL and source.get("vcs_info", {}).get("commit_id") == OFFICIAL_COMMIT:
        return
    # A wheel built locally from this exact clean upstream checkout is also
    # verifiable. An arbitrary editable directory or same version is not.
    parsed = urlparse(source.get("url", ""))
    expected = Path(__file__).resolve().parents[1] / ".upstream-sdk"
    if parsed.scheme == "file" and Path(url2pathname(parsed.path)).resolve() == expected:
        def git(*args):
            return subprocess.check_output(["git", "-C", str(expected), *args], text=True).strip()

        if (git("remote", "get-url", "origin") == OFFICIAL_URL and git("rev-parse", "HEAD") == OFFICIAL_COMMIT
                and not git("status", "--porcelain")):
            return
    raise RuntimeError("SDK must be installed from the pinned official repository; activate the project environment")
