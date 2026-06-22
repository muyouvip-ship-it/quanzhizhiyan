import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from api.main import _resolve_scheduled_trade_date, _run_job
from api.schemas.analysis import AnalyzeRequest


CN_TZ = ZoneInfo("Asia/Shanghai")


def test_run_job_delegates_to_background_analysis(monkeypatch):
    captured = {}

    def fake_resolve_selected_analysts(requested, user_id):
        captured["requested"] = requested
        captured["user_id"] = user_id
        return ["market", "news"]

    async def fake_run_background_analysis_job(**kwargs):
        captured["job_kwargs"] = kwargs

    monkeypatch.setattr(
        "api.routes.chat._resolve_selected_analysts",
        fake_resolve_selected_analysts,
    )
    monkeypatch.setattr(
        "api.routes.chat._run_background_analysis_job",
        fake_run_background_analysis_job,
    )

    request = AnalyzeRequest(
        symbol="300750.SZ",
        trade_date="2026-04-30",
        query="定时分析 300750.SZ",
        selected_analysts=["macro"],
    )

    asyncio.run(_run_job("job-123", request, False, True, "user-1", "scheduled"))

    assert captured["requested"] == ["macro"]
    assert captured["user_id"] == "user-1"
    assert captured["job_kwargs"] == {
        "job_id": "job-123",
        "symbol": "300750.SZ",
        "trade_date": "2026-04-30",
        "query": "定时分析 300750.SZ",
        "user_id": "user-1",
        "selected_analysts": ["market", "news"],
    }


def test_background_analysis_does_not_silently_use_lightweight_fallback(monkeypatch):
    from api.core.progress_tracker import AgentProgressTracker
    from api.routes.chat import _run_analysis_with_fallback

    async def fail_deep_pipeline(**kwargs):
        raise RuntimeError("missing api key")

    monkeypatch.setattr("api.routes.chat._run_deep_analysis_pipeline", fail_deep_pipeline)

    tracker = AgentProgressTracker(emit_events=False)
    try:
        asyncio.run(
            _run_analysis_with_fallback(
                symbol="300750.SZ",
                trade_date="2026-04-30",
                query="定时分析 300750.SZ",
                user_id="user-1",
                selected_analysts=["market", "fundamentals"],
                tracker=tracker,
                job_id="job-no-fallback",
                allow_lightweight_fallback=False,
            )
        )
    except RuntimeError as exc:
        assert "missing api key" in str(exc)
    else:
        raise AssertionError("background analysis should fail instead of creating a lightweight report")


def test_stream_analysis_can_still_use_lightweight_fallback(monkeypatch):
    from api.core.progress_tracker import AgentProgressTracker
    from api.routes.chat import _run_analysis_with_fallback

    async def fail_deep_pipeline(**kwargs):
        raise RuntimeError("temporary llm failure")

    monkeypatch.setattr("api.routes.chat._run_deep_analysis_pipeline", fail_deep_pipeline)

    result_payload, risk_items, key_metrics, decision = asyncio.run(
        _run_analysis_with_fallback(
            symbol="300750.SZ",
            trade_date="2026-04-30",
            query="分析 300750.SZ",
            user_id="user-1",
            selected_analysts=["market", "fundamentals"],
            tracker=AgentProgressTracker(emit_events=False),
            job_id="job-fallback",
            allow_lightweight_fallback=True,
        )
    )

    assert result_payload["symbol"] == "300750.SZ"
    assert decision in {"BUY", "HOLD", "WATCH"}
    assert risk_items
    assert key_metrics


def test_resolve_scheduled_trade_date_uses_previous_day_before_open(monkeypatch):
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.is_cn_trading_day", lambda date: True)
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.previous_cn_trading_day", lambda date: "2026-05-06")
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.now_cn", lambda: datetime(2026, 5, 7, 1, 30, tzinfo=CN_TZ))

    assert _resolve_scheduled_trade_date("2026-05-07") == "2026-05-06"


def test_resolve_scheduled_trade_date_keeps_today_after_close(monkeypatch):
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.is_cn_trading_day", lambda date: True)
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.previous_cn_trading_day", lambda date: "2026-05-06")
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.now_cn", lambda: datetime(2026, 5, 7, 20, 30, tzinfo=CN_TZ))

    assert _resolve_scheduled_trade_date("2026-05-07") == "2026-05-07"


def test_resolve_scheduled_trade_date_uses_previous_for_non_trading_day(monkeypatch):
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.is_cn_trading_day", lambda date: False)
    monkeypatch.setattr("tradingagents.dataflows.trade_calendar.previous_cn_trading_day", lambda date: "2026-05-06")

    assert _resolve_scheduled_trade_date("2026-05-09") == "2026-05-06"
