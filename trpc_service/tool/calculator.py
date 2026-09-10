"""A bounded arithmetic tool for Agent tool-call demonstrations."""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable

Number = int | float
BinaryOperator = Callable[[Number, Number], Number]

_BINARY_OPERATORS: dict[type[ast.operator], BinaryOperator] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPERATORS: dict[type[ast.unaryop], Callable[[Number], Number]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}
_MAX_ABS_VALUE = 1_000_000_000_000


def calculator(expression: str) -> Number:
    """Evaluate a basic arithmetic expression and return its numeric result."""
    if not expression or len(expression) > 200:
        raise ValueError("expression must contain between 1 and 200 characters")
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError("invalid arithmetic expression") from exc
    return _evaluate(tree.body)


def _evaluate(node: ast.AST) -> Number:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return _bounded(node.value)
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPERATORS:
        return _bounded(_UNARY_OPERATORS[type(node.op)](_evaluate(node.operand)))
    if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
        left = _evaluate(node.left)
        right = _evaluate(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 12:
            raise ValueError("exponent is too large")
        try:
            return _bounded(_BINARY_OPERATORS[type(node.op)](left, right))
        except ZeroDivisionError as exc:
            raise ValueError("division by zero") from exc
    raise ValueError("only numeric arithmetic operators are allowed")


def _bounded(value: Number) -> Number:
    if isinstance(value, complex) or abs(value) > _MAX_ABS_VALUE:
        raise ValueError("result is outside the allowed range")
    return value


__all__ = ["calculator"]
