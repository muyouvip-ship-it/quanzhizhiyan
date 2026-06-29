from __future__ import annotations

import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy.engine import make_url


def pytest_configure() -> None:
    env_path = find_dotenv(usecwd=True)
    if env_path:
        load_dotenv(env_path, override=False)

    test_database_url = os.getenv("TEST_DATABASE_URL")
    if test_database_url:
        os.environ["DATABASE_URL"] = test_database_url
        os.environ.setdefault("STRATEGY_DATABASE_URL", test_database_url)
    else:
        os.environ.pop("DATABASE_URL", None)
        os.environ.pop("STRATEGY_DATABASE_URL", None)

    if test_database_url and _looks_like_production_database(test_database_url):
        raise RuntimeError("TEST_DATABASE_URL must not point at the production trading_agents database.")

    os.environ.setdefault("TA_APP_SECRET_KEY", "tradingagents-test-secret")
    os.environ["RATE_LIMIT_ENABLED"] = "0"
    os.environ.setdefault("ENABLE_NEWS_EYE_WORKER", "0")
    os.environ.setdefault("ENABLE_DAILY_REVIEW_WORKER", "0")


def _looks_like_production_database(database_url: str) -> bool:
    try:
        database_name = make_url(database_url).database or ""
    except Exception:
        return False
    return database_name in {"trading_agents", "trading_agents_prod"}
