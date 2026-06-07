from __future__ import annotations

import json
from datetime import date, timedelta

import pandas as pd

from api.services import daily_review_service
from api.services.daily_review_service import (
    _apply_known_market_close_snapshot,
    _build_narrative_markdown,
    _build_rule_based_review,
    _derive_market_sentiment_metrics,
    _index_turnover_amount,
    _load_market_breadth,
    _locked_values_for_narrative,
    _merge_review_payload,
)
from api.services.daily_review_market_behavior import interpret_market_behavior
from api.services.daily_review_technical_diagnostics import compute_stock_technical_diagnostic, _normalize_daily_source_frame


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


def test_daily_review_market_breadth_requires_exact_requested_date(monkeypatch) -> None:
    class _ScalarResult:
        def scalar(self):
            return None

    class _FakeDB:
        statements: list[str]

        def __init__(self) -> None:
            self.statements = []

        def execute(self, statement, params=None):
            self.statements.append(str(statement))
            return _ScalarResult()

    fake_db = _FakeDB()
    monkeypatch.setattr(daily_review_service, "preferred_daily_kline_table", lambda: "stock_daily_kline")

    payload = _load_market_breadth(fake_db, "2026-06-05")

    assert payload["trade_date"] == "2026-06-05"
    assert payload["stock_count"] == 0
    assert "daily_kline" in payload["missing_fields"]
    assert any("trade_date = :trade_date" in statement for statement in fake_db.statements)
    assert not any("trade_date <= :trade_date" in statement for statement in fake_db.statements)


def test_daily_review_index_turnover_rejects_stale_index_dates() -> None:
    stale_indices = [
        {"symbol": "000001.SH", "amount": 1_000_000.0, "trade_time": "2026-04-29"},
        {"symbol": "399001.SZ", "amount": 2_000_000.0, "trade_time": "2026-04-28"},
    ]
    same_day_indices = [
        {"symbol": "000001.SH", "amount": 1_000_000.0, "trade_time": "2026-06-05"},
        {"symbol": "399001.SZ", "amount": 2_000_000.0, "trade_time": "2026-06-05 15:00:00"},
    ]

    assert _index_turnover_amount(stale_indices, trade_date="2026-06-05") is None
    assert _index_turnover_amount(same_day_indices, trade_date="2026-06-05") == 3_000_000.0


def test_daily_review_applies_known_close_snapshot_when_local_market_is_incomplete() -> None:
    market = {
        "indices": [
            {"symbol": "000001.SH", "name": "上证指数", "price": 4107.51, "change_pct": 0.71, "trade_time": "2026-04-29"},
            {"symbol": "399001.SZ", "name": "深证成指", "price": None, "change_pct": None, "trade_time": None},
            {"symbol": "399006.SZ", "name": "创业板指", "price": None, "change_pct": None, "trade_time": None},
        ],
        "market_stats": {
            "trade_date": "2026-06-05",
            "stock_count": 5197,
            "total_amount": 3_068_810_360_200.0,
            "up_count": 2982,
            "down_count": 2091,
            "source": "postgresql:stock_daily_kline",
        },
    }

    corrected = _apply_known_market_close_snapshot("2026-06-05", market)

    stats = corrected["market_stats"]
    assert stats["fallback_applied"] is True
    assert stats["source"] == "verified_close_snapshot:sfccn+cnfin+eastmoney"
    assert stats["total_amount"] == 3_100_600_000_000.0
    assert stats["amount_change"] == 321_600_000_000.0
    assert stats["up_count"] == 3277
    assert stats["down_count"] == 2113
    assert stats["local_market_stats"]["up_count"] == 2982

    index_map = {item["symbol"]: item for item in corrected["indices"]}
    assert index_map["000001.SH"]["price"] == 4027.74
    assert index_map["000001.SH"]["change_pct"] == -0.74
    assert index_map["399001.SZ"]["price"] == 15314.70
    assert index_map["399006.SZ"]["change_pct"] == -3.20
    assert corrected["market_data_quality"]["close_snapshot_fallback"]["applied"] is True


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


def test_merge_review_payload_rejects_llm_narrative_that_changes_locked_values() -> None:
    strong_markdown = "\n".join(
        [
            "# 2026-05-25 每日收盘深度量化复盘",
            "## 1. 大盘大局观与多空资金博弈 (Market Matrix)",
            "指数失真、资金虹吸、个股失血和主线情绪反复确认。两市成交 3.20 万亿元，上涨 2181 家，下跌 3218 家。" * 12,
            "## 2. 核心 Battlefield：绝对主线与板块逻辑 (Sectors)",
            "主线抽血明显，流动性外溢仍需观察。" * 20,
            "## 3. Wolf's Quant 持仓个股硬核量化诊断 (Portfolio T+0 Strategy)",
            "持仓逐股拆解，压力 10.2-10.4，支撑 9.8-10.0。" * 20,
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
        "narrative_markdown": "# 规则长文\n压力 10.20-10.40，支撑 9.80-10.00，两市成交 3.21 万亿元，上涨 2181 家，下跌 3218 家。",
        "portfolio_technical_diagnostics": [
            {
                "symbol": "600000.SH",
                "t0_plan": {
                    "pressure_zone": {"label": "10.20-10.40"},
                    "support_zone": {"label": "9.80-10.00"},
                },
            }
        ],
        "market_behavior_labels": {
            "locked_values": {
                "total_amount_label": "3.21 万亿元",
                "up_count": 2181,
                "down_count": 3218,
                "limit_up_promotion_rate_label": "18.18%",
                "failed_limit_up_rate_label": "35.00%",
            }
        },
    }

    assert "10.20-10.40" in _locked_values_for_narrative(rule_based)
    merged = _merge_review_payload(rule_based, {"narrative_markdown": strong_markdown})

    assert merged["narrative_markdown"] == rule_based["narrative_markdown"]


def test_merge_review_payload_accepts_strong_llm_narrative_with_locked_values() -> None:
    locked_tail = "两市成交 3.21 万亿元，上涨 2181 家，下跌 3218 家，连板晋级率 18.18%，炸板率 35.00%，压力 10.20-10.40，支撑 9.80-10.00。"
    strong_markdown = "\n".join(
        [
            "# 2026-05-25 每日收盘深度量化复盘",
            "## 1. 大盘大局观与多空资金博弈 (Market Matrix)",
            ("指数失真、资金虹吸、个股失血和主线情绪反复确认。" + locked_tail) * 12,
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
        "portfolio_technical_diagnostics": [
            {
                "symbol": "600000.SH",
                "t0_plan": {
                    "pressure_zone": {"label": "10.20-10.40"},
                    "support_zone": {"label": "9.80-10.00"},
                },
            }
        ],
        "market_behavior_labels": {
            "locked_values": {
                "total_amount_label": "3.21 万亿元",
                "up_count": 2181,
                "down_count": 3218,
                "limit_up_promotion_rate_label": "18.18%",
                "failed_limit_up_rate_label": "35.00%",
            }
        },
    }

    merged = _merge_review_payload(rule_based, {"narrative_markdown": strong_markdown})

    assert merged["narrative_markdown"] == strong_markdown


def test_llm_enhance_review_uses_complete_runtime_config(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
            "quick_think_llm": "deepseek-v4-flash",
            "deep_think_llm": "deepseek-v4-pro",
            "api_key": "volcengine-key",
        }

    class FakeLLM:
        def invoke(self, messages):
            captured["messages"] = messages
            return type("Result", (), {"content": json.dumps({"market_summary": {"headline": "ok"}})})()

    class FakeClient:
        def get_llm(self):
            return FakeLLM()

    def fake_create_llm_client(provider, model, base_url=None, **kwargs):
        captured.update({"provider": provider, "model": model, "base_url": base_url, "kwargs": kwargs})
        return FakeClient()

    monkeypatch.setattr("api.core.runtime_config.build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(daily_review_service, "create_llm_client", fake_create_llm_client)

    parsed, runtime = daily_review_service._llm_enhance_review(
        db=None,
        user_id="user-1",
        trade_date="2026-06-02",
        rule_based={},
        market={},
        user_context={},
        news_items=[],
    )

    assert parsed == {"market_summary": {"headline": "ok"}}
    assert runtime["provider"] == "openai"
    assert runtime["model"] == "deepseek-v4-pro"
    assert captured["provider"] == "openai"
    assert captured["model"] == "deepseek-v4-pro"
    assert captured["base_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert captured["kwargs"]["api_key"] == "volcengine-key"
    assert captured["kwargs"]["timeout"] == 60.0


def test_llm_enhance_review_does_not_patch_only_user_key(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
            "quick_think_llm": "deepseek-v4-flash",
            "deep_think_llm": "",
            "api_key": "",
        }

    class FakeLLM:
        def invoke(self, messages):
            return type("Result", (), {"content": json.dumps({"market_summary": {"headline": "no-key"}})})()

    class FakeClient:
        def get_llm(self):
            return FakeLLM()

    def fake_create_llm_client(provider, model, base_url=None, **kwargs):
        captured.update({"provider": provider, "model": model, "base_url": base_url, "kwargs": kwargs})
        return FakeClient()

    monkeypatch.setattr("api.core.runtime_config.build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(daily_review_service, "create_llm_client", fake_create_llm_client)
    monkeypatch.setattr(
        daily_review_service.auth_service,
        "get_user_llm_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not read key outside runtime config")),
    )

    parsed, runtime = daily_review_service._llm_enhance_review(
        db=None,
        user_id="user-1",
        trade_date="2026-06-02",
        rule_based={},
        market={},
        user_context={},
        news_items=[],
    )

    assert parsed == {"market_summary": {"headline": "no-key"}}
    assert runtime["model"] == "deepseek-v4-flash"
    assert "api_key" not in captured["kwargs"]


def test_llm_enhance_review_rejects_mixed_account_runtime(monkeypatch) -> None:
    captured = {"called": False}

    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "env-quick",
            "deep_think_llm": "env-deep",
            "api_key": "volcengine-key-only",
            "_api_key_source": "user_config",
            "_llm_provider_source": "TA_LLM_PROVIDER",
            "_backend_url_source": "TA_BASE_URL",
            "_quick_think_llm_source": "TA_LLM_QUICK",
            "_deep_think_llm_source": "TA_LLM_DEEP",
        }

    def fake_create_llm_client(*args, **kwargs):
        captured["called"] = True
        raise AssertionError("mixed account runtime must not invoke LLM")

    monkeypatch.setattr("api.core.runtime_config.build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(daily_review_service, "create_llm_client", fake_create_llm_client)

    parsed, runtime = daily_review_service._llm_enhance_review(
        db=None,
        user_id="user-1",
        trade_date="2026-06-02",
        rule_based={},
        market={},
        user_context={},
        news_items=[],
    )

    assert parsed is None
    assert captured["called"] is False
    assert runtime["enabled"] is False
    assert runtime["error"] == "mixed_runtime_rejected"
    assert runtime["runtime_package_source"] == "mixed_runtime"
    assert runtime["mixed_account_runtime"] is True
    assert runtime["api_key_source"] == "user_config"


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
    market["market_behavior_labels"] = interpret_market_behavior(market)
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
    assert payload["market_behavior_labels"]["liquidity_state"]["label"] == "流动性极度充沛"
    assert "情绪" in payload["market_summary"]["headline"]


def test_narrative_markdown_mentions_sentiment_pressure() -> None:
    market = {
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
    }
    market["market_behavior_labels"] = interpret_market_behavior(market)
    narrative = _build_narrative_markdown(
        trade_date="2026-05-25",
        market=market,
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
    assert "流动性极度充沛" in narrative


def test_normalize_daily_source_frame_accepts_parquet_date_column() -> None:
    frame = _normalize_daily_source_frame(
        pd.DataFrame(
            [
                {
                    "symbol": "600000.SH",
                    "date": "2026-05-25",
                    "open": "10.0",
                    "high": "10.5",
                    "low": "9.8",
                    "close": "10.2",
                    "volume": "1000",
                    "amount": "10200",
                }
            ]
        ),
        date_column="date",
    )

    assert frame.iloc[0]["normalized_symbol"] == "600000.SH"
    assert str(frame.iloc[0]["trade_date"])[:10] == "2026-05-25"
    assert frame.iloc[0]["close"] == 10.2


def test_load_recent_daily_frame_falls_back_for_symbols_missing_from_parquet(monkeypatch) -> None:
    from api.services import daily_review_technical_diagnostics as module

    parquet = pd.DataFrame(
        [
            {
                "symbol": "600000.SH",
                "date": "2026-05-24",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000,
                "amount": 10200,
                "pre_close": 10.0,
            }
        ]
    )
    db_rows_by_params = {
        "missing": [
            {
                "symbol": "000001.SZ",
                "trade_date": "2026-05-24",
                "open": 12.0,
                "high": 12.5,
                "low": 11.8,
                "close": 12.2,
                "volume": 2000,
                "amount": 24400,
                "pre_close": 12.0,
            }
        ],
        "tail": [
            {
                "symbol": "600000.SH",
                "trade_date": "2026-05-25",
                "open": 10.2,
                "high": 10.7,
                "low": 10.1,
                "close": 10.6,
                "volume": 1300,
                "amount": 13780,
                "pre_close": 10.2,
            }
        ],
    }

    class Result:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class FakeDb:
        def execute(self, _statement, params):
            symbols = set(params["symbols"])
            if "000001.SZ" in symbols:
                return Result(db_rows_by_params["missing"])
            return Result(db_rows_by_params["tail"])

    monkeypatch.setattr(module, "load_daily_kline_slice_from_parquet", lambda **_kwargs: parquet)

    frame, source = module._load_recent_daily_frame(
        FakeDb(),
        symbols=["600000.SH", "000001.SZ"],
        trade_date="2026-05-25",
    )

    assert set(frame["normalized_symbol"]) == {"600000.SH", "000001.SZ"}
    assert "parquet:daily_kline" in source
    assert "postgresql_missing" in source
    assert "postgresql_tail" in source
