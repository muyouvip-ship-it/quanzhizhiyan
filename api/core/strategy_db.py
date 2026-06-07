from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy import inspection
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from api.core.env import load_project_env

load_project_env()

_POSTGRES_PREFIXES = ("postgresql://", "postgresql+", "postgres://")


def _database_url() -> str | None:
    database_url = os.getenv("STRATEGY_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not database_url:
        return None
    if not database_url.startswith(_POSTGRES_PREFIXES):
        raise RuntimeError("Strategy database URL must point to PostgreSQL.")
    return database_url


_strategy_engine: Engine | None = None
_strategy_session_local = None


def _require_strategy_engine() -> Engine:
    global _strategy_engine, _strategy_session_local
    if _strategy_engine is not None:
        return _strategy_engine
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL or STRATEGY_DATABASE_URL is required. PostgreSQL is the only supported database.")
    _strategy_engine = create_engine(database_url, echo=False)
    _strategy_session_local = sessionmaker(autocommit=False, autoflush=False, bind=_strategy_engine)
    return _strategy_engine


class _LazyStrategyEngineProxy:
    def _engine(self) -> Engine:
        return _require_strategy_engine()

    def __getattr__(self, item):
        return getattr(self._engine(), item)

    @property
    def url(self):
        return self._engine().url

    def __repr__(self) -> str:
        try:
            return repr(self._engine())
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                "Strategy engine not yet resolved: %s", exc
            )
            return "<LazyStrategyEngineProxy unresolved>"


@inspection._inspects(_LazyStrategyEngineProxy)
def _inspect_lazy_strategy_engine(target: _LazyStrategyEngineProxy):
    return inspect(target._engine())


class _LazyStrategySessionLocal:
    def __call__(self, *args, **kwargs):
        return _strategy_session_local_factory()(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(_strategy_session_local_factory(), item)


def _strategy_session_local_factory():
    _require_strategy_engine()
    assert _strategy_session_local is not None
    return _strategy_session_local


strategy_engine = _LazyStrategyEngineProxy()
StrategySessionLocal = _LazyStrategySessionLocal()
_strategy_schema_ready = False


def ensure_strategy_schema_ready() -> None:
    global _strategy_schema_ready
    if _strategy_schema_ready:
        return
    from api.models.strategy_models import Base

    Base.metadata.create_all(strategy_engine)
    _strategy_schema_ready = True


def get_strategy_db() -> Iterator[Session]:
    ensure_strategy_schema_ready()
    db = StrategySessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_strategy_db_ctx() -> Iterator[Session]:
    ensure_strategy_schema_ready()
    db = StrategySessionLocal()
    try:
        yield db
    finally:
        db.close()
