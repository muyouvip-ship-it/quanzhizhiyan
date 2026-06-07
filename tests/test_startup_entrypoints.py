from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import types


def test_database_modules_import_without_database_url() -> None:
    env = os.environ.copy()
    env["TA_DISABLE_DOTENV"] = "1"
    env.pop("DATABASE_URL", None)
    env.pop("STRATEGY_DATABASE_URL", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import api.database, api.core.strategy_db, api.main; print('ok')",
        ],
        cwd=os.getcwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_init_db_still_requires_database_url_without_env(monkeypatch) -> None:
    env = os.environ.copy()
    env["TA_DISABLE_DOTENV"] = "1"
    env.pop("DATABASE_URL", None)

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from api.database import init_db\n"
                "try:\n"
                "    init_db()\n"
                "except RuntimeError as exc:\n"
                "    print(str(exc))\n"
                "else:\n"
                "    raise SystemExit('init_db should require DATABASE_URL')\n"
            ),
        ],
        cwd=os.getcwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "DATABASE_URL is required" in result.stdout


def test_cli_defaults_to_api_runner(monkeypatch) -> None:
    called: list[str] = []
    fake_api_main = types.ModuleType("api.main")
    fake_api_main.run = lambda: called.append("api")

    monkeypatch.setitem(sys.modules, "api.main", fake_api_main)
    monkeypatch.setattr(sys, "argv", ["tradingagents"])

    from cli.main import app

    app()

    assert called == ["api"]


def _run_lifespan_worker_scenario(monkeypatch, *, qmt_market: str, qmt_minute: str | None) -> list[str]:
    from api import lifespan as lifespan_module

    calls: list[str] = []

    class _Store:
        def clear(self) -> None:
            calls.append("store.clear")

    async def _start_market() -> None:
        calls.append("start:qmt_market")

    async def _stop_market() -> None:
        calls.append("stop:qmt_market")

    async def _start_minute() -> None:
        calls.append("start:qmt_minute")

    async def _stop_minute() -> None:
        calls.append("stop:qmt_minute")

    monkeypatch.setenv("TA_APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("CLEAR_JOB_STORE_ON_STARTUP", "0")
    monkeypatch.setenv("ENABLE_QMT_SYNC_WORKER", "0")
    monkeypatch.setenv("ENABLE_QMT_MARKET_SYNC_WORKER", qmt_market)
    monkeypatch.setenv("ENABLE_BACKTEST_AUTO_UPDATE_WORKER", "0")
    monkeypatch.setenv("ENABLE_NEWS_EYE_WORKER", "0")
    monkeypatch.setenv("ENABLE_REALTIME_MONITOR_WORKER", "0")
    monkeypatch.setenv("ENABLE_DAILY_REVIEW_WORKER", "0")
    if qmt_minute is None:
        monkeypatch.delenv("ENABLE_QMT_MINUTE_SUBSCRIPTION_WORKER", raising=False)
    else:
        monkeypatch.setenv("ENABLE_QMT_MINUTE_SUBSCRIPTION_WORKER", qmt_minute)

    monkeypatch.setattr(lifespan_module, "init_db", lambda: calls.append("init_db"))
    monkeypatch.setattr(lifespan_module.Base.metadata, "create_all", lambda engine: calls.append("strategy_db"))
    monkeypatch.setattr(lifespan_module, "get_job_store", lambda: _Store())
    monkeypatch.setattr(
        "tradingagents.dataflows.trade_calendar._load_cn_trade_dates",
        lambda: calls.append("calendar"),
    )
    monkeypatch.setattr("api.core.stock_map.load_cn_stock_map", lambda: calls.append("stock_map"))
    monkeypatch.setattr("api.core.stock_map.refresh_cn_stock_map_if_stale", lambda: calls.append("stock_map_refresh"))
    monkeypatch.setattr(lifespan_module.qmt_market_sync_service, "start_background_worker", _start_market)
    monkeypatch.setattr(lifespan_module.qmt_market_sync_service, "stop_background_worker", _stop_market)
    monkeypatch.setattr(lifespan_module.qmt_minute_subscription_service, "start_background_worker", _start_minute)
    monkeypatch.setattr(lifespan_module.qmt_minute_subscription_service, "stop_background_worker", _stop_minute)

    async def scenario() -> None:
        async with lifespan_module.lifespan(object()):
            calls.append("inside")
            await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(scenario())
    return calls


def test_lifespan_starts_qmt_minute_subscription_with_market_sync_default(monkeypatch) -> None:
    calls = _run_lifespan_worker_scenario(monkeypatch, qmt_market="1", qmt_minute=None)

    assert calls.index("start:qmt_market") < calls.index("start:qmt_minute")
    assert calls.index("start:qmt_minute") < calls.index("inside")
    assert calls.index("stop:qmt_minute") < calls.index("stop:qmt_market")


def test_lifespan_can_disable_qmt_minute_subscription_explicitly(monkeypatch) -> None:
    calls = _run_lifespan_worker_scenario(monkeypatch, qmt_market="1", qmt_minute="0")

    assert "start:qmt_market" in calls
    assert "stop:qmt_market" in calls
    assert "start:qmt_minute" not in calls
    assert "stop:qmt_minute" not in calls
