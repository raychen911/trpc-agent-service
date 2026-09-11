"""Operator action views; acting identity is server configured, never request data."""

import os
from typing import Literal

from .admin_auth import current_actor

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from trpc_service.channels.persistence import ContextCipher
from trpc_service.persistence.models import PendingActionRow, ActionJobRow, ActionNoticeRow
from .actions import ActionService, ActionConflict


class ActionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    decision: Literal["approve", "reject", "cancel"]
    nonce: str = Field(pattern=r"^[A-Za-z0-9_-]{22}$", repr=False)
    expected_revision: int = Field(ge=1)


def action_router(database, actor):
    # Mounted only under the existing authenticated operator router. Browser
    # sessions/OIDC will supply a verified principal in the future UI integration.
    router = APIRouter()

    @router.get("/tenants/{tenant_id}/actions")
    def actions(tenant_id: str,
                status: str | None = None,
                limit: int = Query(50, ge=1, le=200),
                offset: int = Query(0, ge=0)):
        with database.sessions() as session:
            query = select(PendingActionRow).where(PendingActionRow.tenant_id == tenant_id)
            if status:
                query = query.where(PendingActionRow.status == status)
            return [
                ActionService.view(row) for row in session.scalars(
                    query.order_by(PendingActionRow.created_at.desc(), PendingActionRow.action_id).limit(limit).offset(
                        offset))
            ]

    @router.get("/tenants/{tenant_id}/actions/{action_id}")
    def action(tenant_id: str, action_id: str):
        with database.sessions() as session:
            row = session.get(PendingActionRow, (tenant_id, action_id))
            if row is None:
                raise HTTPException(404, "action not found")
            job = session.get(ActionJobRow, (tenant_id, action_id))
            return {
                **ActionService.view(row), "job_status":
                job.status if job else None,
                "notices": [{
                    "notice_id": notice.notice_id,
                    "kind": notice.kind,
                    "status": notice.status,
                    "attempt": notice.attempt
                } for notice in session.scalars(
                    select(ActionNoticeRow).where(ActionNoticeRow.tenant_id == tenant_id,
                                                  ActionNoticeRow.action_id == action_id).order_by(
                                                      ActionNoticeRow.created_at, ActionNoticeRow.notice_id))]
            }

    @router.post("/tenants/{tenant_id}/actions/{action_id}/decision")
    def decide(tenant_id: str, action_id: str, body: ActionDecision):
        keys = os.environ.get("TRPC_IM_CONTEXT_KEYS")
        if not keys:
            raise HTTPException(503, "action encryption keys are not configured")
        try:
            cipher = ContextCipher([key.encode() for key in keys.split(",")])
            return ActionService(database, cipher).decide(tenant_id, action_id, current_actor(actor), body.decision,
                                                          body.nonce, body.expected_revision)
        except ActionConflict:
            raise HTTPException(409, "action revision or state conflict") from None
        except PermissionError:
            raise HTTPException(403, "action approval is not authorized") from None
        except LookupError:
            raise HTTPException(404, "action not found") from None
        except ValueError:
            raise HTTPException(422, "invalid action decision or encryption configuration") from None

    return router
