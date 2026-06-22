from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

from api.services import selection_center_service as svc


def test_selection_task_summary_omits_candidate_payload():
    created_at = datetime(2026, 6, 15, 22, 10, 8)
    row = {
        "id": "task-1",
        "user_id": "user-1",
        "name": "首日波段",
        "mode": "strategy",
        "status": "completed",
        "progress": 100,
        "universe": "全A",
        "rule": "首日波段 / 买点规则 1",
        "filters_json": ["非 ST"],
        "config_json": {"mode": "strategy"},
        "candidate_count": 915,
        "error_message": None,
        "created_at": created_at,
        "started_at": created_at,
        "completed_at": created_at,
        "updated_at": created_at,
    }

    summary = svc._selection_task_summary(row)

    assert summary["candidate_count"] == 915
    assert summary["candidates"] == []
    assert summary["created_at"] == "2026-06-15T22:10:08"


def test_latest_rows_query_uses_distinct_on_for_indexed_lookup():
    sql = svc._latest_rows_for_symbols_sql()

    assert "DISTINCT ON (symbol)" in sql.text
    assert "ORDER BY symbol, trade_date DESC" in sql.text


def test_candidate_metric_enrichment_refreshes_since_selected_change_when_display_fields_complete(monkeypatch):
    task = {
        "id": "task-ready",
        "candidates": [
            {
                "symbol": "002426.SZ",
                "name": "胜利精密",
                "metrics": {
                    "close": 3.86,
                    "change_pct": 9.97,
                    "float_market_cap_yi": 131.34,
                    "total_market_cap_yi": 131.34,
                    "board": "主板",
                    "selected_at": "2026-06-15",
                    "current_close": 3.86,
                    "since_selected_change_pct": 0.0,
                },
            }
        ],
    }

    monkeypatch.setattr(
        svc,
        "_load_latest_rows_for_symbols",
        lambda db, symbols: [
            {
                "symbol": "002426.SZ",
                "trade_date": "2026-06-16",
                "close": Decimal("4.25"),
                "pre_close": Decimal("3.86"),
                "float_market_cap": Decimal("14460000000"),
                "total_market_cap": Decimal("14460000000"),
                "sw_industry_l1": "电子",
                "sw_industry_l2": None,
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_load_rows_for_symbol_dates",
        lambda db, symbol_dates: [
            {
                "symbol": "002426.SZ",
                "trade_date": datetime(2026, 6, 15).date(),
                "close": Decimal("3.86"),
                "pre_close": Decimal("3.51"),
                "amount": Decimal("1719000000"),
                "float_market_cap": Decimal("13134000000"),
                "total_market_cap": Decimal("13134000000"),
                "sw_industry_l1": "电子",
                "sw_industry_l2": None,
            }
        ],
    )

    svc._enrich_task_candidate_market_metrics(task)

    metrics = task["candidates"][0]["metrics"]
    assert metrics["change_pct"] == 9.97
    assert metrics["current_trade_date"] == "2026-06-16"
    assert metrics["current_close"] == 4.25
    assert metrics["current_change_pct"] == 10.1
    assert metrics["since_selected_change_pct"] == 10.1


def test_strategy_selection_requires_signal_match(monkeypatch):
    config = {
        "mode": "strategy",
        "include_boards": ["主板"],
        "strategy_id": "strategy-1",
        "strategy_name": "首日波段交易策略",
        "signal_id": "strategy-1:买点:dsl-1",
        "signal_name": "买点规则 1",
        "signal_side": "买点",
        "filter_config": {
            "exclude_st": False,
            "exclude_suspended": False,
            "amount_enabled": False,
            "market_cap_enabled": False,
            "event_heat_enabled": False,
        },
    }
    strategy_payload = {
        "current_version": {
            "dsl": {
                "entry": {"logic": "all", "conditions": [{"type": "cross_above"}]},
                "exit": {"logic": "any", "conditions": []},
            }
        }
    }
    compiled = SimpleNamespace(
        status="passed",
        errors=[],
        entry_rules=[
            {
                "type": "cross_above",
                "rule_key": "cross_above",
                "params": {"left": "first_day_band", "right": "first_day_band_b1"},
            }
        ],
        exit_rules=[],
    )
    latest_rows = [
        {
            "symbol": "000001.SZ",
            "trade_date": "2026-06-09",
            "close": 10.0,
            "pre_close": 9.5,
            "amount": 100_000_000,
            "total_market_cap": 10_000_000_000,
            "ma20": 9.8,
            "amount_ma20": 80_000_000,
        },
        {
            "symbol": "000002.SZ",
            "trade_date": "2026-06-09",
            "close": 8.0,
            "pre_close": 8.1,
            "amount": 90_000_000,
            "total_market_cap": 9_000_000_000,
            "ma20": 8.2,
            "amount_ma20": 90_000_000,
        },
    ]
    feature_frame = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "date": "2026-06-09",
                "first_day_band": 25.0,
                "first_day_band_b1": 20.0,
                "first_day_band_cross": 1.0,
                "first_day_band_dead_cross": 0.0,
                "factor_score": 1.0,
            },
            {
                "symbol": "000002.SZ",
                "date": "2026-06-09",
                "first_day_band": 15.0,
                "first_day_band_b1": 20.0,
                "first_day_band_cross": 0.0,
                "first_day_band_dead_cross": 0.0,
                "factor_score": 0.0,
            },
        ]
    )

    monkeypatch.setattr(svc, "compile_strategy_dsl", lambda dsl: compiled)
    monkeypatch.setattr(svc, "compute_daily_features", lambda frame, compiled_strategy: (feature_frame, "unit"))
    monkeypatch.setattr(svc, "_load_recent_strategy_rows", lambda db, **kwargs: latest_rows)
    monkeypatch.setattr(svc, "_load_latest_market_rows", lambda db, **kwargs: latest_rows)
    monkeypatch.setattr(svc, "get_reverse_stock_map", lambda: {"000001.SZ": "平安银行", "000002.SZ": "万科A"})

    candidates = svc._generate_candidates(None, config, "首日波段 / 买点规则 1", strategy_payload=strategy_payload)

    assert [item["symbol"] for item in candidates] == ["000001.SZ"]
    assert candidates[0]["metrics"]["first_day_band_cross"] == 1.0
    assert "命中买点规则 1" in candidates[0]["reason"]


def test_selected_signal_rules_accepts_legacy_buy_sell_ids():
    rules = [
        {"rule_key": "cross_above", "params": {"left": "a", "right": "b"}},
        {"rule_key": "close_above_indicator", "params": {"indicator": "ma20"}},
    ]

    selected, single = svc._selected_signal_rules(rules, {"signal_id": "strategy-1:买点:buy-2"})
    assert single is True
    assert selected == [rules[1]]

    selected, single = svc._selected_signal_rules(rules, {"signal_id": "strategy-1:卖点:sell-1"})
    assert single is True
    assert selected == [rules[0]]


def test_enrich_candidate_recalculates_selected_day_change_pct(monkeypatch):
    task = {
        "id": "task-1",
        "candidates": [
            {
                "symbol": "600301.SH",
                "name": "华锡有色",
                "metrics": {
                    "trade_date": "2026-06-12",
                    "selected_at": "2026-06-12",
                    "close": 63.0,
                    "change_pct": 14.44,
                    "current_close": 63.0,
                },
            }
        ],
    }

    monkeypatch.setattr(
        svc,
        "_load_latest_rows_for_symbols",
        lambda db, symbols: [
            {
                "symbol": "600301.SH",
                "trade_date": "2026-06-12",
                "close": Decimal("63.00"),
                "pre_close": Decimal("58.54"),
                "float_market_cap": Decimal("17346000000"),
                "total_market_cap": Decimal("39852000000"),
                "sw_industry_l1": "有色金属",
                "sw_industry_l2": None,
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_load_rows_for_symbol_dates",
        lambda db, symbol_dates: [
            {
                "symbol": "600301.SH",
                "trade_date": datetime(2026, 6, 12).date(),
                "close": Decimal("63.00"),
                "pre_close": Decimal("58.54"),
                "amount": Decimal("1767541888"),
                "float_market_cap": Decimal("17346000000"),
                "total_market_cap": Decimal("39852000000"),
                "sw_industry_l1": "有色金属",
                "sw_industry_l2": None,
            }
        ],
    )

    svc._enrich_task_candidate_market_metrics(task)

    metrics = task["candidates"][0]["metrics"]
    assert metrics["change_pct"] == 7.62
    assert metrics["current_change_pct"] == 7.62
    assert metrics["float_market_cap_yi"] == 173.46
    assert metrics["total_market_cap_yi"] == 398.52


def test_enrich_candidate_assigns_recommendation_rank_from_selected_day_features(monkeypatch):
    task = {
        "id": "task-rank",
        "candidates": [
            {
                "symbol": "000001.SZ",
                "name": "强势候选",
                "score": 80,
                "metrics": {
                    "trade_date": "2026-06-12",
                    "selected_at": "2026-06-12",
                    "close": 10.0,
                    "change_pct": 8.0,
                },
            },
            {
                "symbol": "000002.SZ",
                "name": "弱势候选",
                "score": 80,
                "metrics": {
                    "trade_date": "2026-06-12",
                    "selected_at": "2026-06-12",
                    "close": 10.0,
                    "change_pct": -2.0,
                },
            },
        ],
    }

    monkeypatch.setattr(
        svc,
        "_load_latest_rows_for_symbols",
        lambda db, symbols: [
            {
                "symbol": "000001.SZ",
                "trade_date": "2026-06-15",
                "close": Decimal("11.00"),
                "pre_close": Decimal("10.50"),
                "float_market_cap": Decimal("8000000000"),
                "total_market_cap": Decimal("9000000000"),
                "sw_industry_l1": "电子",
                "sw_industry_l2": None,
            },
            {
                "symbol": "000002.SZ",
                "trade_date": "2026-06-15",
                "close": Decimal("9.00"),
                "pre_close": Decimal("9.50"),
                "float_market_cap": Decimal("900000000"),
                "total_market_cap": Decimal("1000000000"),
                "sw_industry_l1": "地产",
                "sw_industry_l2": None,
            },
        ],
    )
    monkeypatch.setattr(
        svc,
        "_load_rows_for_symbol_dates",
        lambda db, symbol_dates: [
            {
                "symbol": "000001.SZ",
                "trade_date": datetime(2026, 6, 12).date(),
                "close": Decimal("10.00"),
                "pre_close": Decimal("9.26"),
                "amount": Decimal("180000000"),
                "amount_ma20": Decimal("120000000"),
                "turnover_rate": Decimal("5.5"),
                "float_market_cap": Decimal("8000000000"),
                "total_market_cap": Decimal("9000000000"),
                "ma20": Decimal("9.60"),
                "ma60": Decimal("8.80"),
                "high60": Decimal("10.80"),
                "low60": Decimal("7.50"),
                "close_lag3": Decimal("9.70"),
                "close_lag5": Decimal("9.40"),
                "sw_industry_l1": "电子",
                "sw_industry_l2": None,
            },
            {
                "symbol": "000002.SZ",
                "trade_date": datetime(2026, 6, 12).date(),
                "close": Decimal("10.00"),
                "pre_close": Decimal("10.20"),
                "amount": Decimal("50000000"),
                "amount_ma20": Decimal("180000000"),
                "turnover_rate": Decimal("0.8"),
                "float_market_cap": Decimal("900000000"),
                "total_market_cap": Decimal("1000000000"),
                "ma20": Decimal("12.00"),
                "ma60": Decimal("13.00"),
                "high60": Decimal("18.00"),
                "low60": Decimal("9.50"),
                "close_lag3": Decimal("11.00"),
                "close_lag5": Decimal("12.00"),
                "sw_industry_l1": "地产",
                "sw_industry_l2": None,
            },
        ],
    )

    svc._enrich_task_candidate_market_metrics(task)

    by_symbol = {item["symbol"]: item["metrics"] for item in task["candidates"]}
    assert by_symbol["000001.SZ"]["recommendation_rank"] == 1
    assert by_symbol["000002.SZ"]["recommendation_rank"] == 2
    assert by_symbol["000001.SZ"]["recommendation_score"] > by_symbol["000002.SZ"]["recommendation_score"]
    assert "中期趋势确认" in by_symbol["000001.SZ"]["recommendation_reasons"]
    assert by_symbol["000001.SZ"]["selected_amount_ratio20"] == 1.5


def test_recommendation_prefers_continuation_setup_over_overheated_breakout():
    candidates = [
        {
            "symbol": "601958.SH",
            "name": "启动延续",
            "score": 80,
            "metrics": {
                "change_pct": 10.0,
                "selected_amount_ratio20": 0.95,
                "selected_close_to_ma20_pct": 2.0,
                "selected_close_to_ma60_pct": 8.0,
                "selected_position_60d": 0.58,
                "selected_ret3_pct": 9.0,
                "float_market_cap_yi": 80.0,
            },
        },
        {
            "symbol": "688010.SH",
            "name": "过热追高",
            "score": 80,
            "metrics": {
                "change_pct": 19.9,
                "selected_amount_ratio20": 2.4,
                "selected_close_to_ma20_pct": 18.0,
                "selected_close_to_ma60_pct": 35.0,
                "selected_position_60d": 0.92,
                "selected_ret3_pct": 26.0,
                "float_market_cap_yi": 70.0,
            },
        },
    ]

    svc._assign_candidate_recommendations(candidates)
    by_symbol = {item["symbol"]: item["metrics"] for item in candidates}

    assert by_symbol["601958.SH"]["recommendation_rank"] == 1
    assert by_symbol["688010.SH"]["recommendation_rank"] == 2
    assert by_symbol["601958.SH"]["recommendation_score"] > by_symbol["688010.SH"]["recommendation_score"]
    assert "启动延续形态" in by_symbol["601958.SH"]["recommendation_reasons"]


def test_recommendation_tiebreaker_prefers_better_continuation_shape_not_symbol_order():
    candidates = [
        {
            "symbol": "000001.SZ",
            "name": "同分但形态一般",
            "score": 80,
            "metrics": {
                "change_pct": 7.2,
                "selected_amount_ratio20": 1.9,
                "selected_close_to_ma20_pct": 3.9,
                "selected_close_to_ma60_pct": 2.0,
                "selected_position_60d": 0.36,
                "selected_ret3_pct": 3.2,
                "float_market_cap_yi": 80.0,
            },
        },
        {
            "symbol": "999999.SH",
            "name": "同分但形态更好",
            "score": 80,
            "metrics": {
                "change_pct": 10.0,
                "selected_amount_ratio20": 1.05,
                "selected_close_to_ma20_pct": 1.5,
                "selected_close_to_ma60_pct": 10.0,
                "selected_position_60d": 0.55,
                "selected_ret3_pct": 9.0,
                "float_market_cap_yi": 80.0,
            },
        },
    ]

    svc._assign_candidate_recommendations(candidates)
    by_symbol = {item["symbol"]: item["metrics"] for item in candidates}

    assert by_symbol["999999.SH"]["recommendation_rank"] == 1
    assert by_symbol["000001.SZ"]["recommendation_rank"] == 2
    assert by_symbol["999999.SH"]["recommendation_sort_score"] > by_symbol["000001.SZ"]["recommendation_sort_score"]


def test_selection_target_trade_date_cutoff(monkeypatch):
    monkeypatch.setattr(svc, "_is_cn_trade_date", lambda value: value.weekday() < 5)

    early = datetime(2026, 6, 12, 0, 41, tzinfo=ZoneInfo("Asia/Shanghai"))
    after_close = datetime(2026, 6, 12, 22, 23, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert svc._resolve_selection_target_trade_date(early).isoformat() == "2026-06-11"
    assert svc._resolve_selection_target_trade_date(after_close).isoformat() == "2026-06-12"


def test_selection_requires_target_trade_date_coverage(monkeypatch):
    class FakeDb:
        def execute(self, sql, params=None):
            class Result:
                def fetchone(self):
                    return SimpleNamespace(symbol_count=1)

            return Result()

    monkeypatch.setattr(svc, "MIN_DAILY_SELECTION_SYMBOLS", 3000)

    try:
        svc._load_latest_market_rows(FakeDb(), target_trade_date=datetime(2026, 6, 12).date())
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected missing target trade date coverage to fail")

    assert "2026-06-12" in message
    assert "日K数据未同步完成" in message
