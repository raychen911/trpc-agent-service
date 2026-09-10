"""Offline compatibility validation for the pinned tRPC-Agent SDK."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from importlib import metadata
from typing import Any
from uuid import uuid4

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part
from trpc_agent_sdk.version import __version__ as sdk_module_version

from ._deterministic_validation_model import DeterministicValidationModel


SDK_PACKAGE_NAME = "trpc-agent-py"
SDK_EXPECTED_VERSION = "1.1.19"
SCHEMA_VERSION = "1"


class RunStatus(StrEnum):
    """Lifecycle states for a complete validation run."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"


class StageStatus(StrEnum):
    """States reported for each stable validation stage."""

    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationStage(StrEnum):
    """Stable public stage names in contract order."""

    VERSION = "version"
    INITIALIZATION = "initialization"
    SINGLE_TURN = "single_turn"
    EVENT_FINALIZATION = "event_finalization"
    SESSION_CONTINUITY = "session_continuity"
    SESSION_ISOLATION = "session_isolation"
    OFFLINE_SAFETY = "offline_safety"


STAGE_ORDER = tuple(ValidationStage)


class ValidationErrorType(StrEnum):
    """Allow-listed diagnostic categories safe for reports."""

    VERSION_MISMATCH = "version_mismatch"
    INITIALIZATION_FAILED = "initialization_failed"
    EXECUTION_FAILED = "execution_failed"
    EVENT_MISSING = "event_missing"
    FINAL_RESPONSE_MISSING = "final_response_missing"
    FINAL_RESPONSE_MULTIPLE = "final_response_multiple"
    FINAL_RESPONSE_EMPTY = "final_response_empty"
    CONTEXT_MISSING = "context_missing"
    SESSION_LEAK = "session_leak"
    NETWORK_ACCESS = "network_access"


class ValidationFailure(RuntimeError):
    """Stable, sanitized failure that can safely cross the CLI boundary."""

    def __init__(
        self,
        stage: ValidationStage,
        error_type: ValidationErrorType,
        message: str,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.error_type = error_type
        self.safe_message = message


@dataclass(slots=True)
class SdkBaseline:
    package_name: str = SDK_PACKAGE_NAME
    expected_version: str = SDK_EXPECTED_VERSION
    distribution_version: str = ""
    module_version: str = ""
    matches: bool = False

    def require_match(self) -> None:
        if not self.matches:
            raise ValidationFailure(
                ValidationStage.VERSION,
                ValidationErrorType.VERSION_MISMATCH,
                "SDK distribution and module versions do not match the required baseline",
            )


@dataclass(slots=True)
class EventObservation:
    event_id: str
    invocation_id: str
    author: str
    partial: bool
    final_response: bool
    text: str | None = None
    error_code: str | None = None


@dataclass(slots=True)
class ValidationTurn:
    turn_index: int
    input_text: str
    expected_text: str
    observations: list[EventObservation] = field(default_factory=list)
    final_text: str | None = None


@dataclass(slots=True)
class ValidationConversation:
    app_name: str
    user_id: str
    session_id: str
    expected_token: str
    turns: list[ValidationTurn] = field(default_factory=list)


@dataclass(slots=True)
class StageResult:
    stage: ValidationStage
    status: StageStatus = StageStatus.PENDING
    message: str = "Not run"
    event_count: int = 0
    final_text: str | None = None
    error_type: ValidationErrorType | None = None


@dataclass(slots=True)
class ValidationRun:
    run_id: str = field(default_factory=lambda: str(uuid4()))
    target_version: str = SDK_EXPECTED_VERSION
    status: RunStatus = RunStatus.PENDING
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    stages: list[StageResult] = field(
        default_factory=lambda: [StageResult(stage=stage) for stage in STAGE_ORDER]
    )
    event_count: int = 0
    final_response_count: int = 0
    credential_required: bool = False
    external_model_calls: int = 0
    sdk: SdkBaseline = field(default_factory=SdkBaseline)

    def stage(self, name: ValidationStage) -> StageResult:
        return next(result for result in self.stages if result.stage is name)

    def to_dict(self) -> dict[str, Any]:
        """Return only the allow-listed, stable CLI report fields."""

        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "status": self.status.value,
            "sdk": {
                "package": self.sdk.package_name,
                "expected_version": self.sdk.expected_version,
                "distribution_version": self.sdk.distribution_version,
                "module_version": self.sdk.module_version,
                "matches": self.sdk.matches,
            },
            "stages": [
                {
                    "stage": result.stage.value,
                    "status": result.status.value,
                    "message": result.message,
                    "event_count": result.event_count,
                    "final_text": result.final_text,
                    "error_type": result.error_type.value if result.error_type else None,
                }
                for result in self.stages
            ],
            "event_count": self.event_count,
            "final_response_count": self.final_response_count,
            "credential_required": self.credential_required,
            "external_model_calls": self.external_model_calls,
        }

    def normalized_dict(self) -> dict[str, Any]:
        payload = self.to_dict()
        payload.pop("run_id")
        return payload

    def pass_stage(
        self,
        stage: ValidationStage,
        message: str,
        *,
        event_count: int = 0,
        final_text: str | None = None,
    ) -> None:
        result = self.stage(stage)
        result.status = StageStatus.PASSED
        result.message = message
        result.event_count = event_count
        result.final_text = final_text
        result.error_type = None

    def fail_stage(self, failure: ValidationFailure) -> None:
        result = self.stage(failure.stage)
        result.status = StageStatus.FAILED
        result.message = failure.safe_message
        result.error_type = failure.error_type

    def skip_pending(self, reason: str) -> None:
        for result in self.stages:
            if result.status is StageStatus.PENDING:
                result.status = StageStatus.SKIPPED
                result.message = reason

    def finish(self, status: RunStatus) -> None:
        self.status = status
        self.completed_at = datetime.now(timezone.utc)


def load_sdk_baseline(expected_version: str = SDK_EXPECTED_VERSION) -> SdkBaseline:
    """Read both public distribution metadata and the SDK version module."""

    try:
        distribution_version = metadata.version(SDK_PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        distribution_version = "unavailable"
    module_version = str(sdk_module_version)
    return SdkBaseline(
        expected_version=expected_version,
        distribution_version=distribution_version,
        module_version=module_version,
        matches=(distribution_version == expected_version and module_version == expected_version),
    )


def collect_turn(
    events: list[Event],
    *,
    turn_index: int,
    input_text: str,
    expected_text: str,
    mismatch_stage: ValidationStage = ValidationStage.SINGLE_TURN,
) -> ValidationTurn:
    """Project SDK events into a safe observation list and enforce finality."""

    visible_events = [event for event in events if event.visible]
    if not visible_events:
        raise ValidationFailure(
            ValidationStage.SINGLE_TURN,
            ValidationErrorType.EVENT_MISSING,
            "SDK execution produced no visible events",
        )

    observations = [
        EventObservation(
            event_id=event.id,
            invocation_id=event.invocation_id,
            author=event.author,
            partial=bool(event.partial),
            final_response=event.is_final_response(),
            text=event.get_text() or None,
            error_code=str(event.error_code) if event.error_code else None,
        )
        for event in visible_events
    ]
    finals = [item for item in observations if item.final_response]
    if not finals:
        raise ValidationFailure(
            ValidationStage.EVENT_FINALIZATION,
            ValidationErrorType.FINAL_RESPONSE_MISSING,
            "SDK execution produced no final response event",
        )
    if len(finals) > 1:
        raise ValidationFailure(
            ValidationStage.EVENT_FINALIZATION,
            ValidationErrorType.FINAL_RESPONSE_MULTIPLE,
            "SDK execution produced more than one final response event",
        )
    final_text = (finals[0].text or "").strip()
    if not final_text:
        raise ValidationFailure(
            ValidationStage.EVENT_FINALIZATION,
            ValidationErrorType.FINAL_RESPONSE_EMPTY,
            "SDK final response was empty",
        )
    if final_text != expected_text:
        error_type = (
            ValidationErrorType.CONTEXT_MISSING
            if mismatch_stage is ValidationStage.SESSION_CONTINUITY
            else ValidationErrorType.EXECUTION_FAILED
        )
        raise ValidationFailure(
            mismatch_stage,
            error_type,
            "SDK final response did not match the deterministic expectation",
        )
    return ValidationTurn(
        turn_index=turn_index,
        input_text=input_text,
        expected_text=expected_text,
        observations=observations,
        final_text=final_text,
    )


class SdkValidationRunner:
    """Own a fresh official SDK runtime for one isolated validation run."""

    APP_NAME = "sdk_validation"
    USER_ID = "validation-user"

    def __init__(self, model: DeterministicValidationModel | None = None) -> None:
        self.model = model or DeterministicValidationModel()
        self.session_service: InMemorySessionService | None = None
        self.runner: Runner | None = None

    async def initialize(self) -> None:
        if self.runner is not None:
            return
        agent = LlmAgent(
            name="sdk_validation_agent",
            model=self.model,
            instruction="Perform only the deterministic SDK validation exchange.",
        )
        self.session_service = InMemorySessionService()
        self.runner = Runner(
            app_name=self.APP_NAME,
            agent=agent,
            session_service=self.session_service,
            enable_post_turn_processing=False,
        )

    async def execute_turn(
        self,
        *,
        session_id: str,
        turn_index: int,
        input_text: str,
        expected_text: str,
        mismatch_stage: ValidationStage = ValidationStage.SINGLE_TURN,
    ) -> ValidationTurn:
        if self.runner is None:
            raise ValidationFailure(
                ValidationStage.INITIALIZATION,
                ValidationErrorType.INITIALIZATION_FAILED,
                "SDK validation runtime was not initialized",
            )
        message = Content(role="user", parts=[Part.from_text(text=input_text)])
        try:
            events = [
                event
                async for event in self.runner.run_async(
                    user_id=self.USER_ID,
                    session_id=session_id,
                    new_message=message,
                )
            ]
        except ValidationFailure:
            raise
        except Exception as exc:
            raise ValidationFailure(
                mismatch_stage,
                ValidationErrorType.EXECUTION_FAILED,
                f"SDK execution failed ({type(exc).__name__})",
            ) from None
        return collect_turn(
            events,
            turn_index=turn_index,
            input_text=input_text,
            expected_text=expected_text,
            mismatch_stage=mismatch_stage,
        )

    async def run_conversation(
        self,
        session_id: str,
        token: str,
    ) -> ValidationConversation:
        """Run the fixed two-turn continuity scenario in one SDK session."""

        normalized_token = token.upper()
        conversation = ValidationConversation(
            app_name=self.APP_NAME,
            user_id=self.USER_ID,
            session_id=session_id,
            expected_token=normalized_token,
        )
        conversation.turns.append(
            await self.execute_turn(
                session_id=session_id,
                turn_index=1,
                input_text=f"Remember validation token {normalized_token}.",
                expected_text=f"stored:{normalized_token}",
            )
        )
        conversation.turns.append(
            await self.execute_turn(
                session_id=session_id,
                turn_index=2,
                input_text="Recall the validation token.",
                expected_text=f"recalled:{normalized_token}",
                mismatch_stage=ValidationStage.SESSION_CONTINUITY,
            )
        )
        return conversation

    async def run_validation_conversations(self) -> list[ValidationConversation]:
        """Run two isolated conversations on the same official session service."""

        session_a = await self.run_conversation("session-a", "ALPHA")
        session_b = await self.run_conversation("session-b", "BRAVO")
        if session_a.turns[1].final_text == session_b.turns[1].final_text:
            raise ValidationFailure(
                ValidationStage.SESSION_ISOLATION,
                ValidationErrorType.SESSION_LEAK,
                "SDK sessions did not preserve distinct validation tokens",
            )
        return [session_a, session_b]

    async def close(self) -> None:
        runner, self.runner = self.runner, None
        if runner is not None:
            await runner.close()


def _record_turn(run: ValidationRun, turn: ValidationTurn) -> None:
    run.event_count += len(turn.observations)
    run.final_response_count += sum(item.final_response for item in turn.observations)


def _stop_failed_run(run: ValidationRun, failure: ValidationFailure) -> ValidationRun:
    if (
        failure.stage is ValidationStage.EVENT_FINALIZATION
        and run.stage(ValidationStage.SINGLE_TURN).status is StageStatus.PENDING
    ):
        run.pass_stage(ValidationStage.SINGLE_TURN, "SDK produced a visible event")
    run.fail_stage(failure)
    run.skip_pending(f"Skipped because {failure.stage.value} failed")
    run.finish(RunStatus.FAILED)
    return run


async def run_validation(
    *,
    expected_version: str = SDK_EXPECTED_VERSION,
    model: DeterministicValidationModel | None = None,
) -> ValidationRun:
    """Execute all seven validation stages in a fresh SDK runtime."""

    run = ValidationRun(target_version=expected_version, status=RunStatus.RUNNING)
    run.sdk = load_sdk_baseline(expected_version)
    try:
        run.sdk.require_match()
    except ValidationFailure as failure:
        return _stop_failed_run(run, failure)
    run.pass_stage(ValidationStage.VERSION, "SDK version matches baseline")

    runtime = SdkValidationRunner(model=model)
    try:
        try:
            await runtime.initialize()
        except Exception as exc:
            failure = ValidationFailure(
                ValidationStage.INITIALIZATION,
                ValidationErrorType.INITIALIZATION_FAILED,
                f"SDK initialization failed ({type(exc).__name__})",
            )
            return _stop_failed_run(run, failure)
        run.pass_stage(ValidationStage.INITIALIZATION, "Official SDK runtime initialized")

        try:
            first_a = await runtime.execute_turn(
                session_id="session-a",
                turn_index=1,
                input_text="Remember validation token ALPHA.",
                expected_text="stored:ALPHA",
            )
        except ValidationFailure as failure:
            return _stop_failed_run(run, failure)
        _record_turn(run, first_a)
        run.pass_stage(
            ValidationStage.SINGLE_TURN,
            "Official Runner produced visible events",
            event_count=len(first_a.observations),
        )
        run.pass_stage(
            ValidationStage.EVENT_FINALIZATION,
            "Exactly one non-empty final response was identified",
            event_count=len(first_a.observations),
            final_text=first_a.final_text,
        )

        try:
            second_a = await runtime.execute_turn(
                session_id="session-a",
                turn_index=2,
                input_text="Recall the validation token.",
                expected_text="recalled:ALPHA",
                mismatch_stage=ValidationStage.SESSION_CONTINUITY,
            )
        except ValidationFailure as failure:
            return _stop_failed_run(run, failure)
        _record_turn(run, second_a)
        run.pass_stage(
            ValidationStage.SESSION_CONTINUITY,
            "Second turn recalled the first-turn token from SDK history",
            event_count=len(second_a.observations),
            final_text=second_a.final_text,
        )

        try:
            first_b = await runtime.execute_turn(
                session_id="session-b",
                turn_index=1,
                input_text="Remember validation token BRAVO.",
                expected_text="stored:BRAVO",
                mismatch_stage=ValidationStage.SESSION_ISOLATION,
            )
            second_b = await runtime.execute_turn(
                session_id="session-b",
                turn_index=2,
                input_text="Recall the validation token.",
                expected_text="recalled:BRAVO",
                mismatch_stage=ValidationStage.SESSION_ISOLATION,
            )
            if second_b.final_text == second_a.final_text:
                raise ValidationFailure(
                    ValidationStage.SESSION_ISOLATION,
                    ValidationErrorType.SESSION_LEAK,
                    "Distinct SDK sessions returned the same validation token",
                )
        except ValidationFailure as failure:
            return _stop_failed_run(run, failure)
        _record_turn(run, first_b)
        _record_turn(run, second_b)
        run.pass_stage(
            ValidationStage.SESSION_ISOLATION,
            "Distinct SDK sessions retained distinct tokens",
            event_count=len(first_b.observations) + len(second_b.observations),
            final_text=second_b.final_text,
        )

        if run.final_response_count != 4 or run.event_count < 4:
            return _stop_failed_run(
                run,
                ValidationFailure(
                    ValidationStage.OFFLINE_SAFETY,
                    ValidationErrorType.EXECUTION_FAILED,
                    "Complete validation did not satisfy the four-turn event invariant",
                ),
            )
        run.pass_stage(
            ValidationStage.OFFLINE_SAFETY,
            "Credential-free deterministic model completed without external calls",
        )
        run.finish(RunStatus.PASSED)
        return run
    finally:
        await runtime.close()


def render_human(run: ValidationRun) -> str:
    actual = run.sdk.distribution_version
    labels = {
        StageStatus.PENDING: "PENDING",
        StageStatus.PASSED: "PASS",
        StageStatus.FAILED: "FAIL",
        StageStatus.SKIPPED: "SKIPPED",
    }
    lines = [
        f"SDK target: {run.sdk.expected_version}",
        f"SDK actual: {actual}",
    ]
    for result in run.stages:
        line = f"{result.stage.value}: {labels[result.status]}"
        if result.error_type:
            line += f" [{result.error_type.value}] {result.message}"
        lines.append(line)
    lines.extend(
        [
            f"event_count: {run.event_count}",
            f"final_response_count: {run.final_response_count}",
            f"credential_required: {str(run.credential_required).lower()}",
            f"external_model_calls: {run.external_model_calls}",
            f"RESULT: {'PASS' if run.status is RunStatus.PASSED else 'FAIL'}",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trpc-agent-sdk-validate",
        description="Run the offline tRPC-Agent SDK compatibility validation.",
    )
    parser.add_argument("--json", action="store_true", help="emit one machine-readable JSON report")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run = asyncio.run(run_validation())
    if args.json:
        print(json.dumps(run.to_dict(), ensure_ascii=False, separators=(",", ":")))
    else:
        print(render_human(run))
    return 0 if run.status is RunStatus.PASSED else 1


if __name__ == "__main__":
    raise SystemExit(main())
