from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from api.services import catalyst_selection_service, qmt_market_sync_service


CN_TZ = ZoneInfo("Asia/Shanghai")


def test_should_run_eod_sync_at_1535_on_trading_day(monkeypatch):
    moment = datetime(2026, 4, 28, 15, 35, tzinfo=CN_TZ)
    monkeypatch.setattr(qmt_market_sync_service, "_is_trading_day", lambda local_now: True)

    assert qmt_market_sync_service._should_run_eod_sync(moment, None) is True


def test_should_run_eod_sync_only_once_per_day(monkeypatch):
    moment = datetime(2026, 4, 28, 15, 40, tzinfo=CN_TZ)
    monkeypatch.setattr(qmt_market_sync_service, "_is_trading_day", lambda local_now: True)

    assert qmt_market_sync_service._should_run_eod_sync(moment, date(2026, 4, 28)) is False


def test_should_run_repair_sync_at_1830_on_trading_day(monkeypatch):
    moment = datetime(2026, 4, 28, 18, 30, tzinfo=CN_TZ)
    monkeypatch.setattr(qmt_market_sync_service, "_is_trading_day", lambda local_now: True)

    assert qmt_market_sync_service._should_run_repair_sync(moment, None) is True


def test_extract_stock_codes_skips_indices():
    codes = qmt_market_sync_service._extract_stock_codes(
        ["000001.SZ", "000300.SH", "399001.SZ", "600000.SH", "430001.BJ", "899050.BJ"]
    )

    assert codes == ["000001", "600000", "430001"]


def test_run_stock_daily_sync_prefers_auto_update(monkeypatch):
    monkeypatch.setattr(qmt_market_sync_service, "_trigger_stock_daily_auto_updates", lambda: [101, 102])
    monkeypatch.setattr(
        qmt_market_sync_service,
        "_run_targeted_stock_daily_sync",
        lambda trade_day, stock_codes: {"success": True, "mode": "targeted_daily_sync", "records": 99},
    )

    payload = qmt_market_sync_service._run_stock_daily_sync(
        datetime(2026, 4, 28, 15, 35, tzinfo=CN_TZ),
        ["000001.SZ", "000300.SH"],
    )

    assert payload["success"] is True
    assert payload["mode"] == "backtest_auto_update"
    assert payload["task_ids"] == [101, 102]


def test_capture_intraday_for_target_uses_user_db_context(monkeypatch):
    fake_db = object()
    captured = {}

    class FakeSessionLocal:
        def __enter__(self):
            return fake_db

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_capture(symbols, *, trade_date, period, account_key, db, user_id):
        captured.update(
            {
                "symbols": symbols,
                "trade_date": trade_date,
                "period": period,
                "account_key": account_key,
                "db": db,
                "user_id": user_id,
            }
        )
        return {"success": True, "rows": 1}

    monkeypatch.setattr(qmt_market_sync_service, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(qmt_market_sync_service, "capture_intraday_symbols", fake_capture)

    target = qmt_market_sync_service._MarketSyncTarget(
        user_id="user-1",
        account_key="paper_sim",
        symbols=["300520.SZ"],
    )
    result = qmt_market_sync_service._capture_intraday_for_target(target, trade_date="2026-05-08")

    assert result == {"success": True, "rows": 1}
    assert captured["symbols"] == ["300520.SZ"]
    assert captured["trade_date"] == "2026-05-08"
    assert captured["period"] == "1m"
    assert captured["account_key"] == "paper_sim"
    assert captured["db"] is fake_db
    assert captured["user_id"] == "user-1"


def test_capture_intraday_for_target_returns_failure_payload_on_exception(monkeypatch):
    fake_db = object()

    class FakeSessionLocal:
        def __enter__(self):
            return fake_db

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_capture(*args, **kwargs):
        raise RuntimeError("bridge timeout")

    monkeypatch.setattr(qmt_market_sync_service, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(qmt_market_sync_service, "capture_intraday_symbols", fake_capture)

    target = qmt_market_sync_service._MarketSyncTarget(
        user_id="user-1",
        account_key="paper_sim",
        symbols=["300520.SZ"],
    )

    result = qmt_market_sync_service._capture_intraday_for_target(target, trade_date="2026-05-08")

    assert result["success"] is False
    assert result["rows"] == 0
    assert result["missing_symbols"] == ["300520.SZ"]
    assert "QMT盘中分钟线采集异常" in result["message"]


@pytest.mark.parametrize(
    ("runner_name", "expected_trigger"),
    [
        ("_run_eod_sync", "qmt-market-sync:eod"),
        ("_run_repair_sync", "qmt-market-sync:repair"),
    ],
)
def test_market_daily_sync_triggers_event_driven_selection_refresh(monkeypatch, runner_name, expected_trigger):
    fake_db = object()

    class FakeSessionLocal:
        def __enter__(self):
            return fake_db

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(qmt_market_sync_service, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(
        qmt_market_sync_service,
        "capture_intraday_symbols",
        lambda *args, **kwargs: {"success": True, "rows": 1},
    )
    monkeypatch.setattr(
        qmt_market_sync_service,
        "sync_major_index_daily",
        lambda *args, **kwargs: {"success": True, "rows": 1},
    )
    monkeypatch.setattr(
        qmt_market_sync_service,
        "_refresh_daily_kline_cache_from_db",
        lambda *args, **kwargs: {"updated": True, "records": 1},
    )
    monkeypatch.setattr(
        qmt_market_sync_service,
        "build_market_integrity_report",
        lambda *args, **kwargs: {"tables": {"stock_daily_kline": {}}},
    )
    monkeypatch.setattr(
        qmt_market_sync_service,
        "_load_latest_stock_daily_trade_date",
        lambda db: date(2026, 4, 28),
    )

    captured: dict[str, object] = {}

    def fake_refresh_event_driven_selection(session, *, trigger, windows, limit, user_id=None, trade_date=None):
        captured.update(
            {
                "db": session,
                "trigger": trigger,
                "windows": tuple(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
            }
        )
        return {
            "trigger": trigger,
            "generated": [{"window": "premarket", "item_count": 1}],
            "errors": [],
            "skipped": False,
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh_event_driven_selection)

    target = qmt_market_sync_service._MarketSyncTarget(
        user_id="user-1",
        account_key="paper_sim",
        symbols=["300520.SZ"],
    )
    runner = getattr(qmt_market_sync_service, runner_name)
    runner(
        datetime(2026, 4, 28, 15, 35, tzinfo=CN_TZ),
        target,
        stock_daily_result={"success": True, "mode": "targeted_daily_sync", "records": 1},
    )

    assert captured["db"] is fake_db
    assert captured["trigger"] == expected_trigger
    assert captured["windows"] == ("premarket", "24h")
    assert captured["limit"] == 10
    assert captured["user_id"] == "user-1"


def test_market_intraday_capture_refreshes_24h_selection_after_rows(monkeypatch):
    qmt_market_sync_service._LAST_INTRADAY_SELECTION_REFRESH_AT = {}
    monkeypatch.setenv("AI_QUANT_INTRADAY_CAPTURE_REFRESH_SELECTION", "1")
    monkeypatch.setenv("AI_QUANT_INTRADAY_SELECTION_REFRESH_INTERVAL_SECONDS", "55")
    calls: list[dict[str, object]] = []

    def fake_refresh_event_driven_selection(*args, **kwargs):
        raise AssertionError("intraday capture should schedule selection refresh asynchronously")

    def fake_schedule(*, trigger, windows, limit, user_id=None, trade_date=None, reason=None, context=None):
        calls.append(
            {
                "trigger": trigger,
                "windows": tuple(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
                "reason": reason,
                "context": context,
            }
        )
        return {
            "trigger": trigger,
            "windows": list(windows),
            "generated": [],
            "errors": [],
            "skipped": False,
            "status": "scheduled",
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh_event_driven_selection)
    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule)
    now = datetime(2026, 6, 2, 10, 0, tzinfo=CN_TZ)

    refreshed = qmt_market_sync_service._refresh_event_driven_selection_after_intraday_capture(
        trigger="qmt-market-sync:intraday",
        user_id="user-1",
        local_now=now,
        capture_result={"success": True, "rows": 128},
    )
    debounced = qmt_market_sync_service._refresh_event_driven_selection_after_intraday_capture(
        trigger="qmt-market-sync:intraday",
        user_id="user-1",
        local_now=now + timedelta(seconds=10),
        capture_result={"success": True, "rows": 88},
    )

    assert refreshed["status"] == "scheduled"
    assert refreshed["scheduled"] is True
    assert refreshed["generated_count"] == 0
    assert refreshed["capture_success"] is True
    assert refreshed["capture_rows"] == 128
    assert debounced == {"status": "skipped", "reason": "debounced"}
    assert calls == [
        {
            "trigger": "qmt-market-sync:intraday",
            "windows": ("24h",),
            "limit": 10,
            "user_id": "user-1",
            "trade_date": None,
            "reason": "intraday_capture",
            "context": {
                "capture_success": True,
                "capture_rows": 128,
                "source": "qmt_market_sync",
            },
        }
    ]


def test_market_intraday_capture_refreshes_selection_even_when_qmt_returns_no_rows(monkeypatch):
    qmt_market_sync_service._LAST_INTRADAY_SELECTION_REFRESH_AT = {}
    monkeypatch.setenv("AI_QUANT_INTRADAY_CAPTURE_REFRESH_SELECTION", "1")
    calls: list[dict[str, object]] = []

    def fake_refresh_event_driven_selection(*args, **kwargs):
        raise AssertionError("intraday capture should schedule selection refresh asynchronously")

    def fake_schedule(*, trigger, windows, limit, user_id=None, trade_date=None, reason=None, context=None):
        calls.append(
            {
                "trigger": trigger,
                "windows": tuple(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
                "reason": reason,
                "context": context,
            }
        )
        return {
            "trigger": trigger,
            "windows": list(windows),
            "generated": [],
            "errors": [],
            "skipped": False,
            "status": "scheduled",
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh_event_driven_selection)
    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule)

    refreshed = qmt_market_sync_service._refresh_event_driven_selection_after_intraday_capture(
        trigger="qmt-market-sync:intraday",
        user_id="user-1",
        local_now=datetime(2026, 6, 2, 10, 5, tzinfo=CN_TZ),
        capture_result={"success": False, "rows": 0, "message": "no intraday bars"},
    )

    assert refreshed["status"] == "fallback_scheduled"
    assert refreshed["reason"] == "qmt_no_success_rows"
    assert refreshed["capture_success"] is False
    assert refreshed["capture_rows"] == 0
    assert refreshed["generated_count"] == 0
    assert calls == [
        {
            "trigger": "qmt-market-sync:intraday",
            "windows": ("24h",),
            "limit": 10,
            "user_id": "user-1",
            "trade_date": None,
            "reason": "qmt_no_success_rows",
            "context": {
                "capture_success": False,
                "capture_rows": 0,
                "source": "qmt_market_sync",
            },
        }
    ]
