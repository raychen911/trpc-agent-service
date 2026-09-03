"""
Skill package initialization.
"""

from trpc_service.skill.base import (
    BaseSkill,
    CodeReviewSkill,
    DocumentSummarySkill,
    SkillRegistry,
    skill_registry,
)

__all__ = [
    "BaseSkill",
    "CodeReviewSkill",
    "DocumentSummarySkill",
    "SkillRegistry",
    "skill_registry",
]
