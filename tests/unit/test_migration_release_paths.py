from __future__ import annotations

import ast
import inspect
import textwrap
from types import ModuleType

import pytest

import scripts.migrate_data as migrate_data
import scripts.migration_full_acceptance as migration_full_acceptance


def _run_function(module: ModuleType) -> ast.AsyncFunctionDef:
    function = ast.parse(textwrap.dedent(inspect.getsource(module._run))).body[0]
    assert isinstance(function, ast.AsyncFunctionDef)
    return function


def _method_calls(node: ast.AST, method: str) -> list[ast.Call]:
    return [
        call
        for call in ast.walk(node)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == method
    ]


@pytest.mark.parametrize(
    ("module", "expected_guard_releases"),
    ((migrate_data, 2), (migration_full_acceptance, 1)),
)
def test_terminal_paths_release_only_through_guard(
    module: ModuleType, expected_guard_releases: int
) -> None:
    function = _run_function(module)
    target_barrier_releases = _method_calls(function, "release_write_barrier")
    guard_releases = [
        call
        for call in _method_calls(function, "release")
        if isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "guard"
    ]

    assert not target_barrier_releases
    assert len(guard_releases) == expected_guard_releases


@pytest.mark.parametrize("module", (migrate_data, migration_full_acceptance))
def test_failure_finally_path_keeps_barrier_owned_by_guard(module: ModuleType) -> None:
    function = _run_function(module)
    finally_bodies = [
        try_node.finalbody
        for try_node in ast.walk(function)
        if isinstance(try_node, ast.Try) and try_node.finalbody
    ]
    finally_calls = [
        call
        for body in finally_bodies
        for call in _method_calls(ast.Module(body=body, type_ignores=[]), "release")
    ]

    assert not finally_calls
    assert "retained after unsuccessful run" in inspect.getsource(module._run)


def test_production_pass_keeps_explicit_empty_rejection_reasons(tmp_path) -> None:
    report = migrate_data._report(
        tmp_path / "migration.json",
        gate="pass",
        rejection_reasons=[],
        production_gate="pass",
        production_rejection_reasons=[],
    )

    assert report["production_rejection_reasons"] == []
