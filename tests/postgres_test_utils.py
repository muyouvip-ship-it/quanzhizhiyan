from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker


_POSTGRES_PREFIXES = ("postgresql://", "postgresql+", "postgres://")


def require_postgres_database_url() -> str:
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL-backed tests.", allow_module_level=True)
    os.environ["DATABASE_URL"] = test_database_url
    database_url = test_database_url

    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL-backed tests.", allow_module_level=True)
    if not database_url.startswith(_POSTGRES_PREFIXES):
        pytest.skip("PostgreSQL TEST_DATABASE_URL is required for database tests.", allow_module_level=True)
    if _looks_like_production_database(database_url):
        pytest.fail("TEST_DATABASE_URL must not point at the production trading_agents database.")
    return database_url


def _looks_like_production_database(database_url: str) -> bool:
    try:
        database_name = make_url(database_url).database or ""
    except Exception:
        return False
    return database_name in {"trading_agents", "trading_agents_prod"}


def database_url_for_schema(database_url: str, schema: str) -> str:
    url = make_url(database_url)
    query = dict(url.query)
    query["options"] = f"-csearch_path={schema}"
    return url.update_query_dict(query).render_as_string(hide_password=False)


@contextmanager
def isolated_postgres_engine(*, schema_prefix: str = "ta_test") -> Iterator[tuple[Engine, str, str]]:
    database_url = require_postgres_database_url()
    schema = f"{schema_prefix}_{uuid4().hex}"
    schema_ident = f'"{schema}"'

    admin_engine = create_engine(database_url, pool_pre_ping=True)
    try:
        with admin_engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA {schema_ident}"))
    finally:
        admin_engine.dispose()

    schema_url = database_url_for_schema(database_url, schema)
    engine = create_engine(schema_url, pool_pre_ping=True)
    try:
        yield engine, schema_url, schema
    finally:
        engine.dispose()
        cleanup_engine = create_engine(database_url, pool_pre_ping=True)
        try:
            with cleanup_engine.begin() as conn:
                conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_ident} CASCADE"))
        finally:
            cleanup_engine.dispose()


@contextmanager
def isolated_postgres_session(base, *, schema_prefix: str = "ta_test") -> Iterator[Session]:
    with isolated_postgres_engine(schema_prefix=schema_prefix) as (engine, _schema_url, _schema):
        base.metadata.create_all(bind=engine)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()


require_postgres_database_url()
