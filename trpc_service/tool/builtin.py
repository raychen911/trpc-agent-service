"""Small deterministic Tools used to verify the governed Agent Tool path."""

import ast
from collections.abc import Callable, Mapping
from decimal import Decimal, DecimalException
import operator

from trpc_service.agent.contracts import (
    AgentExecutionContext,
    AgentToolCall,
    AgentToolResult,
)
from trpc_service.agent.ports import AgentToolInvoker

_BinaryOperator = Callable[[Decimal, Decimal], Decimal]
_BINARY_OPERATORS: Mapping[type[ast.operator], _BinaryOperator] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}
_MAX_EXPRESSION_LENGTH = 200
_MAX_AST_NODES = 64
_MAX_ABSOLUTE_RESULT = Decimal("1e100")


def _calculate(expression: str) -> str:
    """Evaluate bounded arithmetic without executing names, calls or attributes."""

    if not isinstance(expression, str) or not expression.strip():
        raise ValueError("calculation expression must be a non-empty string")
    if len(expression) > _MAX_EXPRESSION_LENGTH:
        raise ValueError("calculation expression is too long")
    try:
        parsed = ast.parse(expression, mode="eval")
    except SyntaxError as error:
        raise ValueError("unsupported calculation expression") from error
    if sum(1 for _ in ast.walk(parsed)) > _MAX_AST_NODES:
        raise ValueError("calculation expression is too complex")

    def evaluate(node: ast.AST) -> Decimal:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ValueError("unsupported calculation expression")
            return Decimal(str(node.value))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = evaluate(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.BinOp):
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Pow):
                if right != right.to_integral_value() or abs(right) > 12:
                    raise ValueError("calculation exponent must be an integer from -12 to 12")
                value = left**int(right)
            else:
                operation = _BINARY_OPERATORS.get(type(node.op))
                if operation is None:
                    raise ValueError("unsupported calculation expression")
                value = operation(left, right)
            if not value.is_finite() or abs(value) > _MAX_ABSOLUTE_RESULT:
                raise ValueError("calculation result is outside the supported range")
            return value
        raise ValueError("unsupported calculation expression")

    try:
        result = evaluate(parsed)
    except (ArithmeticError, DecimalException) as error:
        raise ValueError("calculation could not be completed") from error
    # A numeric literal does not pass through a BinOp node, so enforce the
    # result bound once more at the expression boundary.
    if not result.is_finite() or abs(result) > _MAX_ABSOLUTE_RESULT:
        raise ValueError("calculation result is outside the supported range")
    rendered = format(result.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"", "-0"} else rendered


class BuiltinToolInvoker(AgentToolInvoker):
    """Route approved local Tool calls without adding network dependencies."""

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        """Execute a registered deterministic Tool inside the supplied scope."""

        del context
        if call.name != "calculate":
            raise PermissionError(f"tool is not registered for this Agent: {call.name}")
        expression = call.arguments.get("expression")
        if not isinstance(expression, str):
            raise ValueError("calculate requires a string expression")
        return AgentToolResult(call_id=call.call_id, content=_calculate(expression))
