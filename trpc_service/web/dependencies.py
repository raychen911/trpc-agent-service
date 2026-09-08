from collections.abc import Iterator

from fastapi import Request
from sqlalchemy.orm import Session


def get_db_session(request: Request) -> Iterator[Session]:
    tenant_id = request.path_params.get("tenant_id")
    with request.app.state.database.session_factory() as session:
        if tenant_id:
            session.info["tenant_id"] = str(tenant_id)
        yield session
