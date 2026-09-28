"""Attach uniquely governed model profiles to legacy Agent applications.

Revision ID: 20260908_0024
Revises: 20260908_0023
Create Date: 2026-09-08
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260908_0024"
down_revision: str | None = "20260908_0023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MIGRATION_ACTOR = "migration:20260908_0024"


def upgrade() -> None:
    """Release corrected snapshots where a tenant has one active profile."""

    op.execute(f"""
        WITH unique_profile AS (
            SELECT tenant_id, min(model_profile_id::text)::uuid AS model_profile_id
            FROM model_profile
            WHERE status = 'active'
            GROUP BY tenant_id
            HAVING count(*) = 1
        ), target AS (
            SELECT
                agent.tenant_id,
                agent.agent_app_id,
                profile.model_profile_id,
                COALESCE(max(version.version), 0) + 1 AS next_version
            FROM agent_app AS agent
            JOIN unique_profile AS profile USING (tenant_id)
            LEFT JOIN agent_config_version AS version
                ON version.tenant_id = agent.tenant_id
                AND version.agent_app_id = agent.agent_app_id
            WHERE agent.model_profile_id IS NULL
            GROUP BY agent.tenant_id, agent.agent_app_id, profile.model_profile_id
        )
        INSERT INTO agent_config_version (
            tenant_id, agent_app_id, version, status, snapshot, created_by, reason
        )
        SELECT
            agent.tenant_id,
            agent.agent_app_id,
            target.next_version,
            'released',
            jsonb_build_object(
                'model_profile_id', target.model_profile_id::text,
                'application_config', agent.application_config,
                'model_settings', agent.model_config,
                'tool_permissions', agent.tool_permissions,
                'knowledge_config', agent.knowledge_config,
                'backend_config', agent.backend_config
            ),
            '{_MIGRATION_ACTOR}',
            'attached unique active platform Model Profile'
        FROM agent_app AS agent
        JOIN target
            ON target.tenant_id = agent.tenant_id
            AND target.agent_app_id = agent.agent_app_id
        """)
    op.execute(f"""
        WITH migrated AS (
            SELECT tenant_id, agent_app_id, version,
                   (snapshot->>'model_profile_id')::uuid AS model_profile_id
            FROM agent_config_version
            WHERE created_by = '{_MIGRATION_ACTOR}'
        )
        UPDATE agent_app AS agent
        SET model_profile_id = migrated.model_profile_id,
            stable_config_version = migrated.version,
            canary_config_version = NULL,
            canary_percent = 0
        FROM migrated
        WHERE agent.tenant_id = migrated.tenant_id
          AND agent.agent_app_id = migrated.agent_app_id
        """)


def downgrade() -> None:
    """Detach only Agents whose stable pointer still targets this backfill."""

    op.execute(f"""
        WITH migrated AS (
            SELECT tenant_id, agent_app_id, version
            FROM agent_config_version
            WHERE created_by = '{_MIGRATION_ACTOR}'
        ), previous AS (
            SELECT migrated.tenant_id, migrated.agent_app_id,
                   max(version.version) AS version
            FROM migrated
            JOIN agent_config_version AS version
              ON version.tenant_id = migrated.tenant_id
             AND version.agent_app_id = migrated.agent_app_id
             AND version.version < migrated.version
            GROUP BY migrated.tenant_id, migrated.agent_app_id
        )
        UPDATE agent_app AS agent
        SET model_profile_id = NULL,
            stable_config_version = previous.version,
            canary_config_version = NULL,
            canary_percent = 0
        FROM migrated
        JOIN previous USING (tenant_id, agent_app_id)
        WHERE agent.tenant_id = migrated.tenant_id
          AND agent.agent_app_id = migrated.agent_app_id
          AND agent.stable_config_version = migrated.version
        """)
    op.execute(f"DELETE FROM agent_config_version WHERE created_by = '{_MIGRATION_ACTOR}'")
