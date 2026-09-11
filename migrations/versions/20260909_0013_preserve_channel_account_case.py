"""preserve case-sensitive external IM account identifiers."""
from typing import Sequence, Union

from alembic import op

revision: str = "0013_channel_account_case"
down_revision: Union[str, None] = "0012_tenant_config_rollouts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLES = (
    ("channel_bindings", "channel_bindings_account_normalized"),
    ("channel_binding_versions", "channel_binding_versions_account_normalized"),
)
_CASE_SENSITIVE_CHECK = "external_account_id = btrim(external_account_id) AND external_account_id <> ''"
_LOWERCASE_CHECK = "external_account_id = lower(btrim(external_account_id)) AND external_account_id <> ''"


def upgrade() -> None:
    for table, constraint in _TABLES:
        op.drop_constraint(constraint, table, type_="check")
        op.create_check_constraint(constraint, table, _CASE_SENSITIVE_CHECK)


def downgrade() -> None:
    for table, constraint in _TABLES:
        op.drop_constraint(constraint, table, type_="check")
    op.execute("DO $$ BEGIN "
               "IF EXISTS (SELECT 1 FROM channel_bindings "
               "WHERE external_account_id <> lower(btrim(external_account_id))) "
               "OR EXISTS (SELECT 1 FROM channel_binding_versions "
               "WHERE external_account_id <> lower(btrim(external_account_id))) THEN "
               "RAISE EXCEPTION 'cannot downgrade while case-sensitive channel account identifiers exist'; "
               "END IF; END $$")
    for table, constraint in _TABLES:
        op.create_check_constraint(constraint, table, _LOWERCASE_CHECK)
