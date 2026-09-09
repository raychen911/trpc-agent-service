# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

from pathlib import Path
import tomllib


def test_build_tool_and_im_static_assets_are_packaged_from_the_python_package():
    root = Path(__file__).resolve().parents[1]
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    assert "build" in config["project"]["optional-dependencies"]["dev"]
    assert config["tool"]["hatch"]["build"]["targets"]["wheel"]["packages"] == ["trpc_service"]
    assert (root / "data" / "README.md").is_file()
    assert (root / "trpc_service" / "workspace" / "__init__.py").is_file()
    static = root / "trpc_service" / "web" / "static"
    assert {path.name for path in static.iterdir()} >= {"im.html", "im.css", "im.js"}
