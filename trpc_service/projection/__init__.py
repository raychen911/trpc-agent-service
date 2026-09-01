"""Durable post-turn Summary and Memory projection pipeline."""

from .algorithms import (
    EncryptedProjectionTextReader,
    ExplicitInstructionMemoryExtractor,
    ExtractiveWindowSummary,
    ProjectionText,
)
from .contracts import MemoryAlgorithm, MemoryCandidate, ProjectionPort, SummaryAlgorithm
from .worker import (
    ProjectionLeaseLostError,
    ProjectionOutcome,
    ProjectionOutputError,
    ProjectionRunResult,
    ProjectionWorker,
)

__all__ = [
    "EncryptedProjectionTextReader",
    "ExplicitInstructionMemoryExtractor",
    "ExtractiveWindowSummary",
    "MemoryAlgorithm",
    "MemoryCandidate",
    "ProjectionLeaseLostError",
    "ProjectionOutcome",
    "ProjectionOutputError",
    "ProjectionPort",
    "ProjectionRunResult",
    "ProjectionText",
    "ProjectionWorker",
    "SummaryAlgorithm",
]
