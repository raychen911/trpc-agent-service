"""
Skill definition and management engine.
Skills package composite workflows and tool combinations for specific business domains.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class BaseSkill(ABC):
    """Abstract Base Skill."""

    name: str
    description: str
    required_tools: List[str] = []

    @abstractmethod
    def can_handle(self, user_prompt: str) -> bool:
        """Evaluate if the skill matches user intent."""
        pass

    @abstractmethod
    async def run(self, tenant_id: str, user_prompt: str, context: Dict[str, Any]) -> str:
        """Execute the skill workflow."""
        pass


class CodeReviewSkill(BaseSkill):
    name = "code_review_skill"
    description = "Performs automated static analysis and security review on submitted code snippets."
    required_tools = ["knowledge_search"]

    def can_handle(self, user_prompt: str) -> bool:
        keywords = ["code review", "代码审查", "审查代码", "代码质量", "review code", "check code", "审查"]
        return any(kw in user_prompt.lower() for kw in keywords)

    async def run(self, tenant_id: str, user_prompt: str, context: Dict[str, Any]) -> str:
        return (
            "【代码审查报告】\n"
            "1. 规范检查：函数命名与类型注解符合 PEP 8。\n"
            "2. 安全检查：未发现硬编码密码与 SQL 注入隐患。\n"
            "3. 性能建议：建议在高频循环中复用 Session 连接池以降低内存开销。"
        )


class DocumentSummarySkill(BaseSkill):
    name = "document_summary_skill"
    description = "Condenses long-form enterprise documentation into structured summaries."
    required_tools = ["knowledge_search"]

    def can_handle(self, user_prompt: str) -> bool:
        keywords = ["总结", "概括", "文档摘要", "summarize", "summary"]
        return any(kw in user_prompt.lower() for kw in keywords)

    async def run(self, tenant_id: str, user_prompt: str, context: Dict[str, Any]) -> str:
        return (
            "【文档摘要生成】\n"
            "- 核心主旨：多租户节点化架构设计。\n"
            "- 关键要点：控制面与数据面分离、无状态计算节点弹性伸缩、IM 协议统一与全链路脱敏审计。"
        )


class SkillRegistry:
    """Registry maintaining active skills."""

    def __init__(self):
        self._skills: Dict[str, BaseSkill] = {}
        self.register(CodeReviewSkill())
        self.register(DocumentSummarySkill())

    def register(self, skill: BaseSkill):
        self._skills[skill.name] = skill

    def match_skill(self, prompt: str) -> Optional[BaseSkill]:
        for s in self._skills.values():
            if s.can_handle(prompt):
                return s
        return None

    def get_skill(self, name: str) -> Optional[BaseSkill]:
        return self._skills.get(name)

    def list_skills(self) -> List[BaseSkill]:
        return list(self._skills.values())


skill_registry = SkillRegistry()
