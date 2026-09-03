"""
Base Tool definition and standard built-in tools for Agent Worker execution.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
import math
import logging

logger = logging.getLogger(__name__)


class BaseTool(ABC):
    """Abstract base class for Agent execution tools."""

    name: str
    description: str
    is_dangerous: bool = False

    @abstractmethod
    async def execute(self, **kwargs) -> Any:
        """Execute tool logic and return serializable output."""
        pass


class CalculatorTool(BaseTool):
    name = "calculator"
    description = "Evaluates basic mathematical expressions, e.g. expression='2 * (3 + 5)'."
    is_dangerous = False

    async def execute(self, expression: str = "", **kwargs) -> Dict[str, Any]:
        try:
            # Safe eval with restricted globals/locals
            allowed_names = {"math": math, "abs": abs, "round": round, "pow": pow}
            res = eval(expression, {"__builtins__": {}}, allowed_names)
            return {"result": res, "status": "success"}
        except Exception as e:
            return {"error": f"Calculation failed: {str(e)}", "status": "error"}


class KnowledgeSearchTool(BaseTool):
    name = "knowledge_search"
    description = "Searches internal company knowledge base and documentation."
    is_dangerous = False

    async def execute(self, query: str = "", **kwargs) -> Dict[str, Any]:
        # Simulated knowledge base retrieval
        sample_docs = [
            f"Result 1 for '{query}': tRPC-Agent-Python is an enterprise-grade agent orchestration framework.",
            f"Result 2 for '{query}': Supports multi-tenant isolation, Filter chain governance, and stateless worker clusters.",
        ]
        return {"query": query, "results": sample_docs, "status": "success"}


class DatabaseQueryTool(BaseTool):
    name = "database_query"
    description = "Executes read-only SQL query against permitted database tables."
    is_dangerous = False

    async def execute(self, sql: str = "", **kwargs) -> Dict[str, Any]:
        if any(keyword in sql.upper() for keyword in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]):
            return {"error": "Write operations are strictly prohibited.", "status": "permission_denied"}
        return {
            "sql": sql,
            "rows": [{"id": 1, "department": "R&D", "headcount": 42}, {"id": 2, "department": "Marketing", "headcount": 18}],
            "status": "success",
        }


class DangerousTransferTool(BaseTool):
    name = "fund_transfer"
    description = "Transfers financial funds between corporate accounts. Requires secondary authorization."
    is_dangerous = True

    async def execute(self, amount: float = 0.0, target_account: str = "", **kwargs) -> Dict[str, Any]:
        return {
            "action": "transfer",
            "amount": amount,
            "target": target_account,
            "status": "executed",
            "message": f"Successfully transferred ${amount} to {target_account}.",
        }


class ToolRegistry:
    """Registry maintaining available tools."""

    def __init__(self):
        self._tools: Dict[str, BaseTool] = {}
        self.register(CalculatorTool())
        self.register(KnowledgeSearchTool())
        self.register(DatabaseQueryTool())
        self.register(DangerousTransferTool())

    def register(self, tool: BaseTool):
        self._tools[tool.name] = tool

    def get_tool(self, name: str) -> Optional[BaseTool]:
        return self._tools.get(name)

    def list_tools(self) -> List[BaseTool]:
        return list(self._tools.values())


tool_registry = ToolRegistry()
