from __future__ import annotations

from datetime import date, timedelta

from api.services.daily_review_service import (
    _build_narrative_markdown,
    _build_rule_based_review,
    _derive_market_sentiment_metrics,
    _merge_review_payload,
)
from api.services.daily_review_technical_diagnostics import compute_stock_technical_diagnostic


def _daily_rows(days: int = 90) -> list[dict]:
    start = date(2026, 1, 1)
    rows: list[dict] = []
    previous_close = 10.0
    for index in range(days):
        close = round(10 + index * 0.1, 2)
        rows.append(
            {
                "symbol": "600000.SH",
                "trade_date": start + timedelta(days=index),
                "open": round(close - 0.08, 2),
                "high": round(close + 0.2, 2),
                "low": round(close - 0.25, 2),
                "close": close,
                "volume": 1000 + index * 10,
                "amount": close * (1000 + index * 10),
                "pre_close": previous_close,
            }
        )
        previous_close = close
    return rows


def _minute_rows() -> list[dict]:
    rows: list[dict] = []
    for index in range(120):
        close = round(18 + index * 0.01, 2)
        rows.append(
            {
                "symbol": "600000.SH",
                "trade_time": f"2026-05-06 09:{30 + index % 30:02d}:00" if index < 30 else f"2026-05-06 {10 + index // 60:02d}:{index % 60:02d}:00",
                "open": close - 0.01,
                "high": close + 0.02,
                "low": close - 0.03,
                "close": close,
                "volume": 100,
                "amount": close * 100,
            }
        )
    return rows


def test_stock_diagnostic_computes_daily_indicators_and_t0_plan() -> None:
    diagnostic = compute_stock_technical_diagnostic(
        symbol="600000.SH",
        name="浦发银行",
        daily_rows=_daily_rows(),
        minute_rows=_minute_rows(),
        daily_source="test:daily",
        minute_source="test:minute",
    )

    assert diagnostic["symbol"] == "600000.SH"
    assert diagnostic["daily_macd"]["dif"] is not None
    assert diagnostic["bollinger"]["upper"] > diagnostic["bollinger"]["middle"] > diagnostic["bollinger"]["lower"]
    assert diagnostic["bollinger"]["bandwidth"] is not None
    assert diagnostic["volume_price"]["volume_ratio"] is not None
    assert diagnostic["t0_plan"]["pressure_zone"]["label"]
    assert diagnostic["t0_plan"]["support_zone"]["label"]
    assert diagnostic["minute_macd_60m"] is not None
    assert diagnostic["data_quality"]["daily_rows"] == 90


def test_missing_minute_rows_degrades_without_blocking() -> None:
    diagnostic = compute_stock_technical_diagnostic(
        symbol="600000.SH",
        name="浦发银行",
        daily_rows=_daily_rows(),
        minute_rows=[],
    )

    assert diagnostic["daily_macd"] is not None
    assert diagnostic["minute_macd_60m"] is None
    assert "minute_kline" in diagnostic["data_quality"]["missing_fields"]


def test_missing_daily_rows_does_not_fabricate_indicators() -> None:
    diagnostic = compute_stock_technical_diagnostic(
        symbol="600000.SH",
        name="浦发银行",
        daily_rows=[],
        minute_rows=[],
    )

    assert diagnostic["daily_macd"] is None
    assert diagnostic["bollinger"] is None
    assert diagnostic["t0_plan"]["pressure_zone"]["label"] == "需盘中确认"
    assert "daily_kline" in diagnostic["data_quality"]["missing_fields"]


def test_merge_review_payload_preserves_new_dual_track_fields() -> None:
    rule_based = {
        "market_summary": {"headline": "规则摘要", "bullets": ["a"]},
        "portfolio_summary": {"headline": "持仓摘要", "bullets": []},
        "current_main_themes": [],
        "current_key_stocks": [],
        "next_main_themes": [],
        "next_candidate_stocks": [],
        "risk_watchpoints": [],
        "narrative_markdown": "# 规则长文",
        "portfolio_technical_diagnostics": [{"symbol": "600000.SH"}],
    }
    llm_payload = {"narrative_markdown": "# LLM 长文", "market_summary": {"headline": "LLM 摘要", "bullets": ["b"]}}

    merged = _merge_review_payload(rule_based, llm_payload)

    assert merged["market_summary"]["headline"] == "LLM 摘要"
    assert merged["narrative_markdown"] == "# 规则长文"
    assert merged["portfolio_technical_diagnostics"] == [{"symbol": "600000.SH"}]


def test_merge_review_payload_accepts_strong_llm_narrative() -> None:
    strong_markdown = "\n".join(
        [
            "# 2026-05-25 每日收盘深度量化复盘",
            "## 1. 大盘大局观与多空资金博弈 (Market Matrix)",
            "指数失真、资金虹吸、个股失血和主线情绪反复确认。" * 20,
            "## 2. 核心 Battlefield：绝对主线与板块逻辑 (Sectors)",
            "主线抽血明显，流动性外溢仍需观察。" * 20,
            "## 3. Wolf's Quant 持仓个股硬核量化诊断 (Portfolio T+0 Strategy)",
            "持仓逐股拆解，并给出支撑压力和开盘观察。" * 20,
            "## 4. 调仓风控提示与知行合一 (Risk & Action)",
            "主线强则做T，分歧破位则先降仓位风险。" * 20,
        ]
    )
    rule_based = {
        "market_summary": {"headline": "规则摘要", "bullets": []},
        "portfolio_summary": {"headline": "持仓摘要", "bullets": []},
        "current_main_themes": [],
        "current_key_stocks": [],
        "next_main_themes": [],
        "next_candidate_stocks": [],
        "risk_watchpoints": [],
        "narrative_markdown": "# 规则长文",
        "portfolio_technical_diagnostics": [],
    }

    merged = _merge_review_payload(rule_based, {"narrative_markdown": strong_markdown})

    assert merged["narrative_markdown"] == strong_markdown


def test_market_sentiment_metrics_use_previous_limit_up_pool() -> None:
    previous_rows = [
        {"symbol": "600000.SH", "close": 11.0, "high": 11.0, "pre_close": 10.0, "amount": 1000},
        {"symbol": "600001.SH", "close": 22.2, "high": 22.2, "pre_close": 20.0, "amount": 1200},
    ]
    current_rows = [
        {"symbol": "600000.SH", "close": 11.0, "high": 11.1, "pre_close": 10.0, "amount": 1300},
        {"symbol": "600001.SH", "close": 20.5, "high": 22.2, "pre_close": 20.0, "amount": 1400},
    ]

    metrics = _derive_market_sentiment_metrics(current_rows, previous_rows, source="test:sentiment")

    assert metrics["limit_up_promotion_base"] == 2
    assert metrics["limit_up_promotion_count"] == 1
    assert metrics["limit_up_promotion_rate"] == 50.0
    assert metrics["failed_limit_up_count"] == 1
    assert metrics["failed_limit_up_rate"] == 50.0
    assert metrics["sentiment_missing_fields"] == []


def test_rule_based_review_injects_sentiment_pressure_language() -> None:
    market = {
        "indices": [
            {"symbol": "000001.SH", "name": "上证指数", "change_pct": 0.8},
            {"symbol": "000688.SH", "name": "科创50", "change_pct": 4.8},
        ],
        "sector_gainers": [{"sector_name": "半导体", "change_pct": 7.2}],
        "sector_losers": [{"sector_name": "银行", "change_pct": -1.1}],
        "sector_inflows": [],
        "sector_outflows": [],
        "top_gainers": [],
        "top_losers": [],
        "market_stats": {
            "index_turnover_amount": 3_200_000_000_000,
            "up_count": 3869,
            "down_count": 1509,
            "limit_up_count": 119,
            "limit_down_count": 5,
            "limit_up_promotion_rate": 42.0,
            "limit_up_promotion_count": 21,
            "limit_up_promotion_base": 50,
            "failed_limit_up_rate": 18.0,
            "failed_limit_up_count": 9,
            "limit_up_touch_count": 50,
            "sentiment_source": "test:sentiment",
        },
    }
    payload = _build_rule_based_review(
        "2026-05-25",
        market,
        {
            "holdings": [],
            "watchlist": [],
            "today_reports": [],
            "latest_report_map": {},
            "holdings_quotes": {},
            "portfolio_technical_diagnostics": [],
        },
        [],
    )

    assert any("连板晋级率" in item for item in payload["market_summary"]["bullets"])
    assert any("炸板率" in item for item in payload["market_summary"]["bullets"])
    assert "情绪" in payload["market_summary"]["headline"]


def test_narrative_markdown_mentions_sentiment_pressure() -> None:
    narrative = _build_narrative_markdown(
        trade_date="2026-05-25",
        market={
            "indices": [{"name": "科创50", "change_pct": 4.8}],
            "sector_gainers": [{"sector_name": "半导体", "change_pct": 7.2}],
            "sector_losers": [{"sector_name": "银行", "change_pct": -1.1}],
            "top_gainers": [{"name": "A", "change_pct": 9.9}],
            "top_losers": [{"name": "B", "change_pct": -9.9}],
            "market_stats": {
                "index_turnover_amount": 3_200_000_000_000,
                "up_count": 3869,
                "down_count": 1509,
                "limit_up_count": 119,
                "limit_down_count": 5,
                "limit_up_promotion_rate": 42.0,
                "limit_up_promotion_count": 21,
                "limit_up_promotion_base": 50,
                "failed_limit_up_rate": 18.0,
                "failed_limit_up_count": 9,
                "limit_up_touch_count": 50,
                "sentiment_source": "test:sentiment",
            },
        },
        payload={
            "market_summary": {"headline": "测试盘面摘要", "bullets": []},
            "portfolio_summary": {"headline": "测试持仓摘要", "bullets": []},
            "current_main_themes": [{"theme": "半导体", "summary": "主线测试", "strength": "主线级"}],
            "risk_watchpoints": [{"title": "测试风险", "detail": "测试风险描述", "level": "medium"}],
        },
        diagnostics=[],
    )

    assert "短线情绪压强" in narrative
    assert "连板晋级率" in narrative
    assert "炸板率" in narrative
