"""平台数据库连接的显式持有者（依赖注入用）。"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.tenant.storage.tables import Base


def platform_db_url() -> Optional[str]:
    """平台表使用的连接串（与框架共用同一 MySQL，但连接池独立）。"""
    return os.getenv("SQL_URL") or None


class Database:
    """平台表连接池持有者：一个实例 = 一个 Engine = 一个连接池。"""

    def __init__(self, url: str, **engine_kwargs):
        self.url = url
        # pool_pre_ping：借出连接前先 ping，MySQL 8h 空闲断连自动重建——生产必备
        self.engine: Engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            **engine_kwargs,
        )
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def create_all(self) -> None:
        """按模型建全部缺失表。"""
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        """会话上下文：用完即还连接。expire_on_commit=False 避免 commit 后读属性触发惰性刷新。"""
        s = self._session_factory()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    def dispose(self) -> None:
        self.engine.dispose()
