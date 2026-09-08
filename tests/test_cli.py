from argparse import ArgumentParser

import pytest

from trpc_service import _cli


def test_build_parser() -> None:
    parser = _cli.build_parser()

    assert isinstance(parser, ArgumentParser)
    args = parser.parse_args(["serve", "--host", "127.0.0.1", "--port", "9000"])
    assert args.command == "serve"
    assert args.host == "127.0.0.1"
    assert args.port == 9000


def test_main_starts_uvicorn(monkeypatch: pytest.MonkeyPatch) -> None:
    received: dict[str, object] = {}

    def fake_run(app: str, **kwargs: object) -> None:
        received["app"] = app
        received.update(kwargs)

    monkeypatch.setattr(_cli.uvicorn, "run", fake_run)
    _cli.main(["serve", "--host", "127.0.0.1", "--port", "9001", "--reload"])

    assert received == {
        "app": "trpc_service.web.app:app",
        "host": "127.0.0.1",
        "port": 9001,
        "reload": True,
    }


def test_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        _cli.main(["--version"])

    assert error.value.code == 0
    assert capsys.readouterr().out.strip() == "0.6.0"
