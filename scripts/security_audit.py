"""Audit the locked development and production dependency graph."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path


def executable(name: str) -> str:
    resolved = shutil.which(name)
    if resolved is None:
        raise RuntimeError(f"required executable is unavailable: {name}")
    return resolved


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    with tempfile.NamedTemporaryFile(suffix="-requirements.txt", delete=False) as handle:
        requirements = Path(handle.name)
    try:
        subprocess.run(  # noqa: S603 - fixed executable and argument vector
            [
                executable("uv"),
                "export",
                "--locked",
                "--all-extras",
                "--no-emit-project",
                "--quiet",
                "--output-file",
                str(requirements),
            ],
            cwd=root,
            check=True,
        )
        subprocess.run(  # noqa: S603 - fixed executable and argument vector
            [
                executable("uvx"),
                "pip-audit",
                "--requirement",
                str(requirements),
                # A fully hashed ``uv export --all-extras`` is the complete
                # locked graph. Disable pip's second resolver/index pass, which
                # is nondeterministic and can hang on restricted CI networks.
                "--disable-pip",
                "--require-hashes",
                "--progress-spinner",
                "off",
                "--timeout",
                "10",
            ],
            cwd=root,
            check=True,
        )
    finally:
        requirements.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
