import json
from pathlib import Path
from copy import deepcopy

import pandas as pd
from fastapi.testclient import TestClient

from api.app import app
from api.routes import strategy_platform
from api.routes.strategy_platform import _default_dsl, _first_day_band_dsl
from api.services.a_share_market_rules import get_a_share_market_rule, round_to_tick
from api.services.minute_data_service import evaluate_intraday_confirmation, get_minute_cache_root, load_aggregated_minute_bars
from api.services.strategy_dsl_compiler import compile_strategy_dsl
from api.services.strategy_platform_engine import (
    run_strategy_backtest,
    _calculate_metrics,
    _dedupe_symbol_listing_dates,
    _exit_reason_from_compiled_rules,
    _simulate_portfolio,
)


def test_compile_strategy_dsl_returns_execution_ir():
    compiled = compile_strategy_dsl(_default_dsl("portfolio").model_dump())
    assert compiled.status == "passed"
    assert compiled.selection_plan["market"] == "A_SHARE"
    assert "1d" in compiled.timeframes_required
    assert "1w" in compiled.timeframes_required
    assert "30m" in compiled.timeframes_required
    assert compiled.minute_requirements["enabled"] is True
    assert compiled.backend_resolution["compute"] in {"polars", "pandas_fallback"}


def test_compile_strategy_dsl_rejects_unknown_schema_fields():
    dsl = _default_dsl("portfolio").model_dump()
    dsl["factor_model"]["unknown_field"] = True
    compiled = compile_strategy_dsl(dsl)
    assert compiled.status == "failed"
    assert any("DSL schema 校验失败" in error for error in compiled.errors)


def test_compile_strategy_dsl_returns_pending_confirmation_for_unknown_factor():
    dsl = _default_dsl("portfolio").model_dump()
    dsl["factor_model"]["factors"][0]["name"] = "custom_alpha_x"
    compiled = compile_strategy_dsl(dsl)

    assert compiled.status == "passed"
    assert compiled.pending_confirmations
    assert compiled.pending_confirmations[0]["kind"] == "unknown_factor"


def test_compile_strategy_dsl_blocks_future_function_fields():
    dsl = _default_dsl("portfolio").model_dump()
    dsl["entry"]["conditions"][0]["indicator"] = "future_return_5d"
    compiled = compile_strategy_dsl(dsl)

    assert compiled.status == "failed"
    assert compiled.future_function_risks
    assert any("疑似未来函数" in error for error in compiled.errors)


def test_first_day_band_exit_waits_for_dead_cross():
    compiled = compile_strategy_dsl(_first_day_band_dsl().model_dump())

    holding_row = pd.Series(
        {
            "close": 10.97,
            "ma20": 11.0,
            "first_day_band": 21.234857,
            "first_day_band_b1": 19.913118,
            "first_day_band_dead_cross": 0.0,
        }
    )
    dead_cross_row = holding_row.copy()
    dead_cross_row["first_day_band"] = 22.074818
    dead_cross_row["first_day_band_b1"] = 22.701644
    dead_cross_row["first_day_band_dead_cross"] = 1.0

    assert _exit_reason_from_compiled_rules(holding_row, compiled) is None
    assert _exit_reason_from_compiled_rules(dead_cross_row, compiled) == "first_day_band_dead_cross"


def test_listing_date_dedup_keeps_earliest_normalized_symbol_date():
    frame = pd.DataFrame(
        [
            {"symbol": "000001", "symbol_code": "000001", "listing_date": "1991-01-03"},
            {"symbol": "000001.SZ", "symbol_code": "000001", "listing_date": "2024-09-04"},
            {"symbol": "000026.SZ", "symbol_code": "000026", "listing_date": "2024-09-04"},
            {"symbol": "000026", "symbol_code": "000026", "listing_date": "1993-06-03"},
        ]
    )

    deduped = _dedupe_symbol_listing_dates(frame)
    by_symbol = deduped.set_index("symbol")

    assert len(deduped) == 2
    assert by_symbol.loc["000001.SZ", "listing_date"] == pd.Timestamp("1991-01-03")
    assert by_symbol.loc["000026.SZ", "listing_date"] == pd.Timestamp("1993-06-03")


def test_llm_draft_exposes_structured_output_schema_and_compile_report():
    client = TestClient(app)
    schema_response = client.get("/v1/strategies/dsl-schema")
    assert schema_response.status_code == 200
    schema_payload = schema_response.json()
    assert schema_payload["structured_outputs"] is True
    assert "json_schema" in schema_payload

    draft_response = client.post("/v1/strategies/llm-draft", json={"prompt": "创建算力板块业绩暴增选股策略"})
    assert draft_response.status_code == 200
    draft_payload = draft_response.json()
    assert draft_payload["compile_report"]["status"] == "passed"
    assert draft_payload["structured_output_schema"]["title"] == "StrategyDslSchema"
    assert draft_payload["llm_runtime"]["shared_with_settings"] is True
    assert draft_payload["llm_runtime"]["source"] in {"server_default", "user_settings"}
    assert "api_key" not in draft_payload["llm_runtime"]


def test_llm_draft_uses_complete_user_runtime_config(monkeypatch):
    captured = {}

    def fake_build_runtime_config(overrides, user_id=None, db=None):
        del overrides, db
        assert user_id
        return {
            "llm_provider": "openai",
            "backend_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
            "quick_think_llm": "deepseek-v4-flash",
            "deep_think_llm": "deepseek-v4-pro",
            "api_key": "volcengine-key",
            "_api_key_source": "user_config",
            "_llm_provider_source": "user_config",
            "_backend_url_source": "user_config",
            "_quick_think_llm_source": "user_config",
            "_deep_think_llm_source": "user_config",
            "_llm_runtime_force_skipped": "complete_user_config",
        }

    class FakeLLM:
        def invoke(self, messages):
            captured["messages"] = messages
            dsl = _default_dsl("selection").model_dump()
            dsl["universe"]["include_concepts"] = ["半导体", "先进封装"]
            return type(
                "Result",
                (),
                {
                    "content": json.dumps(
                        {
                            "name": "半导体先进封装选股策略",
                            "strategy_type": "selection",
                            "intent_summary": "筛选先进封装产业链里资金与业绩共振的标的。",
                            "pending_confirmations": [],
                            "data_dependencies": ["stock_daily_kline.close", "stock_daily_kline.net_profit_ttm"],
                            "risk_notes": ["样本外验证后再进入纸交易。"],
                            "dsl": dsl,
                            "explanation": "由远程 LLM 根据用户提示生成。",
                        },
                        ensure_ascii=False,
                    )
                },
            )()

    class FakeClient:
        def get_llm(self):
            return FakeLLM()

    def fake_create_llm_client(provider, model, base_url=None, **kwargs):
        captured.update({"provider": provider, "model": model, "base_url": base_url, "kwargs": kwargs})
        return FakeClient()

    monkeypatch.setattr(strategy_platform, "build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(strategy_platform, "create_llm_client", fake_create_llm_client)

    client = TestClient(app)
    response = client.post(
        "/v1/strategies/llm-draft",
        json={"prompt": "创建一个半导体先进封装选股策略", "strategy_type": "selection"},
        headers={"Authorization": "Bearer dev-test-token-001"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert captured["provider"] == "openai"
    assert captured["model"] == "deepseek-v4-pro"
    assert captured["base_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert captured["kwargs"]["api_key"] == "volcengine-key"
    assert payload["name"] == "半导体先进封装选股策略"
    assert payload["compile_report"]["status"] == "passed"
    assert payload["llm_runtime"]["used"] is True
    assert payload["llm_runtime"]["source"] == "user_settings"
    assert payload["llm_runtime"]["api_key_source"] == "user_config"
    assert payload["llm_runtime"]["base_url_source"] == "user_config"
    assert payload["llm_runtime"]["model_source"] == "user_config"
    assert payload["llm_runtime"]["runtime_package_source"] == "user_config"
    assert payload["llm_runtime"]["mixed_account_runtime"] is False
    assert "api_key" not in payload["llm_runtime"]


def test_llm_draft_rejects_mixed_account_runtime(monkeypatch):
    captured = {"called": False}

    def fake_build_runtime_config(overrides, user_id=None, db=None):
        del overrides, db
        assert user_id
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

    monkeypatch.setattr(strategy_platform, "build_runtime_config", fake_build_runtime_config)
    monkeypatch.setattr(strategy_platform, "create_llm_client", fake_create_llm_client)

    client = TestClient(app)
    response = client.post(
        "/v1/strategies/llm-draft",
        json={"prompt": "创建一个AI选股策略", "strategy_type": "selection"},
        headers={"Authorization": "Bearer dev-test-token-001"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert captured["called"] is False
    assert payload["llm_runtime"]["ready"] is False
    assert payload["llm_runtime"]["status"] == "mixed_runtime_rejected"
    assert payload["llm_runtime"]["runtime_package_source"] == "mixed_runtime"
    assert payload["llm_runtime"]["mixed_account_runtime"] is True
    assert payload["llm_runtime"]["api_key_source"] == "user_config"
    assert "api_key" not in payload["llm_runtime"]


def test_llm_draft_respects_requested_selection_strategy_type():
    client = TestClient(app)
    response = client.post(
        "/v1/strategies/llm-draft",
        json={
            "prompt": "创建一个选股策略：算力板块、市值100亿到200亿、业绩高增长，只做选股不做交易",
            "strategy_type": "selection",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["strategy_type"] == "selection"
    assert payload["dsl"]["strategy_type"] == "selection"
    assert payload["dsl"]["entry"]["conditions"] == []
    assert payload["dsl"]["exit"]["conditions"] == []


def test_factor_registry_routes_expose_builtin_metadata():
    client = TestClient(app)
    list_response = client.get("/v1/factors")
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["total"] >= 8
    names = {item["name"] for item in payload["items"]}
    assert "money_flow_strength_20d" in names

    detail_response = client.get("/v1/factors/money_flow_strength_20d")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["display_name"] == "20日资金强度"
    assert "amount" in detail["required_fields"]
    assert "polars" in detail["backend_support"]


def test_strategy_clone_and_version_routes():
    client = TestClient(app)
    strategy = client.get("/v1/strategies").json()["strategies"][0]
    strategy_id = strategy["id"]

    clone_response = client.post(f"/v1/strategies/{strategy_id}/clone", json={"name": "克隆策略测试"})
    assert clone_response.status_code == 200
    cloned = clone_response.json()
    assert cloned["name"] == "克隆策略测试"
    assert cloned["id"] != strategy_id

    versions_response = client.get(f"/v1/strategies/{strategy_id}/versions")
    assert versions_response.status_code == 200
    assert versions_response.json()["versions"]

    dsl = strategy["current_version"]["dsl"]
    dsl["factor_model"]["select"]["min_score"] = 0.7
    version_response = client.post(
        f"/v1/strategies/{strategy_id}/versions",
        json={"dsl": dsl, "change_summary": "提高选股阈值", "activate": True},
    )
    assert version_response.status_code == 200
    updated = version_response.json()
    assert updated["version"] >= 2
    assert updated["current_version"]["change_summary"] == "提高选股阈值"

    activate_response = client.post(f"/v1/strategies/{strategy_id}/activate", json={"status": "active"})
    assert activate_response.status_code == 200
    assert activate_response.json()["status"] == "active"


def test_minute_aggregation_and_confirmation_work_with_fallback():
    aggregated = load_aggregated_minute_bars(
        symbols=["300750.SZ"],
        trade_date="2024-10-08",
        timeframe="30m",
    )
    assert aggregated.timeframe == "30m"
    assert aggregated.items
    first = aggregated.items[0]
    assert {"symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"} <= set(first.keys())

    confirmation = evaluate_intraday_confirmation(
        symbols=["300750.SZ"],
        trade_date="2024-10-08",
        timeframe="30m",
    )
    assert confirmation.items
    assert "confirmed" in confirmation.items[0]


def test_minute_cache_root_and_parquet_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("MINUTE_CACHE_ROOT", str(tmp_path / "minute_cache"))
    aggregated = load_aggregated_minute_bars(
        symbols=["300750.SZ"],
        trade_date="2024-10-08",
        timeframe="15m",
    )

    assert get_minute_cache_root() == tmp_path / "minute_cache"
    assert aggregated.cache_path
    assert Path(aggregated.cache_path).exists()
    if aggregated.parquet_cache_path:
        assert Path(aggregated.parquet_cache_path).exists()


def test_backtest_endpoint_returns_minute_engine_diagnostics():
    client = TestClient(app)
    strategy_id = client.get("/v1/strategies").json()["strategies"][0]["id"]
    response = client.post(
        "/v1/backtests",
        json={
            "strategy_id": strategy_id,
            "symbols": ["300750.SZ", "300520.SZ"],
            "start_date": "2024-09-01",
            "end_date": "2024-12-31",
            "initial_capital": 1_000_000,
            "frequency": "daily_minute",
            "benchmark": "沪深300",
            "use_minute_confirm": True,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert "engine_mode" in payload["result"]["summary"]
    assert "watchlist_days" in payload["result"]["summary"]
    assert "confirm_hit_rate" in payload["result"]["diagnostics"]
    assert "universe_filter" in payload["result"]["diagnostics"]
    assert "order_count" in payload["result"]["diagnostics"]
    assert "risk_event_count" in payload["result"]["diagnostics"]
    run_id = payload["id"]
    watchlists = client.get(f"/v1/backtests/{run_id}/watchlists").json()["items"]
    confirmations = client.get(f"/v1/backtests/{run_id}/minute-confirmations").json()["items"]
    orders = client.get(f"/v1/backtests/{run_id}/orders").json()["items"]
    metrics_response = client.get(f"/v1/backtests/{run_id}/metrics")
    paged_watchlists = client.get(f"/v1/backtests/{run_id}/watchlists?limit=1&sort_by=rank&sort_order=asc").json()
    assert watchlists
    assert confirmations
    assert orders
    assert metrics_response.status_code == 200
    assert metrics_response.json()["metrics"]["final_capital"] > 0
    assert paged_watchlists["total"] >= 1
    assert len(paged_watchlists["items"]) == 1


def test_portfolio_watchlist_keeps_full_candidate_pool_beyond_max_positions():
    symbols = ["300901.SZ", "300902.SZ", "300903.SZ", "300904.SZ"]
    rows = []
    for date_index, date_value in enumerate(pd.to_datetime(["2024-01-02", "2024-01-03"])):
        for symbol_index, symbol in enumerate(symbols):
            close = 20.0 + symbol_index + date_index * 0.2
            rows.append(
                {
                    "symbol": symbol,
                    "date": date_value,
                    "open": close - 0.1,
                    "high": close + 0.3,
                    "low": close - 0.4,
                    "close": close,
                    "pre_close": close - 0.2,
                    "volume": 1_000_000,
                    "amount": close * 1_000_000,
                    "turnover_rate": 2.0,
                    "ma5": close - 0.2,
                    "ma20": close - 1.0,
                    "momentum_20d": 0.12,
                    "weekly_trend_pass": True,
                    "factor_score": 1.0 - symbol_index * 0.05,
                }
            )
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["universe"]["filters"] = []
    dsl["universe"]["min_listing_days"] = 0
    dsl["entry"]["conditions"] = []
    dsl["factor_model"]["select"] = {"top_n": len(symbols), "min_score": 0.0}
    dsl["risk"].update(
        {
            "max_positions": 1,
            "take_profit_pct": 2.0,
            "trailing_stop_pct": 0.8,
            "max_drawdown_pct": 1.0,
            "max_daily_loss_pct": 1.0,
        }
    )

    portfolio = _simulate_portfolio(
        pd.DataFrame(rows),
        compiled=compile_strategy_dsl(dsl),
        initial_capital=1_000_000,
        frequency="daily",
        use_minute_confirm=False,
    )

    first_date = min(item["date"] for item in portfolio["watchlists"])
    first_day_watchlists = [item for item in portfolio["watchlists"] if item["date"] == first_date]
    buy_orders = [item for item in portfolio["orders"] if item["side"] == "buy"]

    assert len(first_day_watchlists) == len(symbols)
    assert max(item["rank"] for item in first_day_watchlists) == len(symbols)
    assert len(buy_orders) == 1


def _accounting_rows(closes: list[float], *, symbol: str = "300999.SZ") -> list[dict]:
    rows = []
    for date_index, close in enumerate(closes):
        date_value = pd.Timestamp("2024-01-02") + pd.Timedelta(days=date_index)
        rows.append(
            {
                "symbol": symbol,
                "date": date_value,
                "open": close,
                "high": close * 1.02,
                "low": close * 0.98,
                "close": close,
                "pre_close": closes[date_index - 1] if date_index else close,
                "volume": 1_000_000,
                "amount": close * 1_000_000,
                "turnover_rate": 2.0,
                "ma5": close * 0.95,
                "ma20": close * 0.9,
                "momentum_20d": 0.2,
                "momentum_60d": 0.2,
                "volatility_20d": 0.2,
                "rsi_14": 60,
                "atr_14": close * 0.02,
                "money_flow_strength_20d": 0.2,
                "weekly_trend_pass": True,
                "factor_score": 1.0,
            }
        )
    return rows


def _accounting_dsl() -> dict:
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["universe"]["filters"] = []
    dsl["universe"]["min_listing_days"] = 0
    dsl["entry"]["conditions"] = []
    dsl["exit"]["conditions"] = []
    dsl["factor_model"]["select"] = {"top_n": 1, "min_score": 0.0}
    dsl["position"].update(
        {
            "method": "equal_weight",
            "max_single_position_pct": 1.0,
            "max_position_pct": 1.0,
            "cash_reserve_pct": 0.0,
            "initial_position_pct": 1.0,
        }
    )
    dsl["risk"].update(
        {
            "max_positions": 1,
            "stop_loss_pct": 1.0,
            "take_profit_pct": 5.0,
            "trailing_stop_pct": 1.0,
            "max_drawdown_pct": 0.01,
            "max_daily_loss_pct": 0.01,
        }
    )
    dsl["execution"].update(
        {
            "commission_rate": 0.001,
            "min_commission": 5,
            "stamp_duty_rate": 0.001,
            "slippage_model": {"type": "bps", "value": 10},
        }
    )
    return dsl


def test_backtest_accounting_uses_nav_and_forces_final_liquidation():
    portfolio = _simulate_portfolio(
        pd.DataFrame(_accounting_rows([10.0, 11.0, 12.0])),
        compiled=compile_strategy_dsl(_accounting_dsl()),
        initial_capital=10_000,
        frequency="daily",
        use_minute_confirm=False,
    )
    metrics = _calculate_metrics(portfolio["equity"], portfolio["trades"], 10_000)
    buy_trade = next(trade for trade in portfolio["trades"] if trade["direction"] == "buy")

    assert all(float(item["available_cash"]) >= 0 for item in portfolio["equity"])
    assert buy_trade["quantity"] == 900
    assert buy_trade["cash_cost"] <= 10_000
    assert portfolio["equity"][-1]["position_value"] == 0.0
    assert portfolio["equity"][-1]["positions_value"] == 0.0
    assert portfolio["equity"][-1]["total_equity"] == portfolio["equity"][-1]["available_cash"]
    assert portfolio["equity"][-1]["nav"] == round(portfolio["equity"][-1]["total_equity"] / 10_000, 6)
    assert metrics["total_return"] == round(portfolio["equity"][-1]["nav"] - 1, 6)
    assert metrics["final_nav"] == portfolio["equity"][-1]["nav"]
    assert any(trade.get("virtual_liquidation") for trade in portfolio["trades"])
    assert any(order.get("virtual_liquidation") for order in portfolio["orders"])
    assert len([event for event in portfolio["accounting_events"] if event["type"] == "virtual_liquidation"]) == 1


def test_backtest_does_not_halt_on_drawdown_or_daily_loss_limits():
    portfolio = _simulate_portfolio(
        pd.DataFrame(_accounting_rows([10.0, 8.0, 7.5, 7.0, 6.5])),
        compiled=compile_strategy_dsl(_accounting_dsl()),
        initial_capital=10_000,
        frequency="daily",
        use_minute_confirm=False,
    )

    watchlist_dates = {item["date"][:10] for item in portfolio["watchlists"]}
    assert "2024-01-06" in watchlist_dates
    assert portfolio["risk_events"] == []
    assert any(event["type"] == "virtual_liquidation" for event in portfolio["accounting_events"])


def test_backtest_endpoint_supports_walk_forward():
    client = TestClient(app)
    strategy_id = client.get("/v1/strategies").json()["strategies"][0]["id"]
    response = client.post(
        "/v1/backtests",
        json={
            "strategy_id": strategy_id,
            "symbols": ["300750.SZ", "300520.SZ", "601136.SH"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "initial_capital": 1_000_000,
            "frequency": "daily",
            "benchmark": "沪深300",
            "use_minute_confirm": False,
            "walk_forward": {
                "enabled": True,
                "train_days": 40,
                "test_days": 20,
                "step_days": 20,
            },
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["result"]["summary"]["walk_forward_enabled"] is True
    assert payload["result"]["diagnostics"]["walk_forward"]["enabled"] is True
    assert payload["result"]["diagnostics"]["walk_forward"]["window_count"] >= 1


def test_a_share_market_rules_are_board_aware():
    main_rule = get_a_share_market_rule("600000.SH")
    chinext_rule = get_a_share_market_rule("300750.SZ")
    star_rule = get_a_share_market_rule("688001.SH")
    bse_rule = get_a_share_market_rule("830000.BJ")
    st_rule = get_a_share_market_rule("600000.SH", is_st=True)

    assert main_rule.daily_limit_pct == 0.10
    assert chinext_rule.daily_limit_pct == 0.20
    assert star_rule.daily_limit_pct == 0.20
    assert bse_rule.daily_limit_pct == 0.30
    assert st_rule.daily_limit_pct == 0.05
    assert round_to_tick(10.006) == 10.01


def test_position_models_generate_distinct_allocations():
    base_dsl = _default_dsl("portfolio").model_dump()
    base_dsl["universe"]["min_listing_days"] = 1
    base_dsl["universe"]["filters"] = []
    base_dsl["factor_model"]["select"] = {"top_n": 3, "min_score": 0.3}
    base_dsl["risk"].update(
        {
            "max_positions": 3,
            "take_profit_pct": 1.5,
            "trailing_stop_pct": 0.8,
            "max_drawdown_pct": 1.0,
            "max_daily_loss_pct": 1.0,
        }
    )
    base_dsl["position"].update(
        {
            "max_single_position_pct": 0.35,
            "max_position_pct": 1.0,
            "cash_reserve_pct": 0.0,
            "initial_position_pct": 0.18,
            "risk_per_trade_pct": 0.01,
            "target_volatility_pct": 0.12,
        }
    )
    symbols = ["300750.SZ", "300520.SZ", "601136.SH"]
    results = {}
    for method in ("equal_weight", "factor_weight", "volatility_target", "risk_budget"):
        dsl = deepcopy(base_dsl)
        dsl["position"]["method"] = method
        result = run_strategy_backtest(
            run_id=f"unit_position_model_{method}",
            strategy_name=f"{method}测试",
            dsl=dsl,
            symbols=symbols,
            start_date="2024-01-01",
            end_date="2024-06-30",
            initial_capital=1_000_000,
            frequency="daily",
            benchmark="沪深300",
            use_minute_confirm=False,
        )
        results[method] = result
        buy_orders = [item for item in result.orders if item["side"] == "buy" and not item.get("is_pyramid_add")]
        assert buy_orders
        assert all(item.get("allocation_method") for item in buy_orders)

    def allocations_by_day(result):
        buy_orders = [item for item in result.orders if item["side"] == "buy" and not item.get("is_pyramid_add")]
        mapping = {}
        for item in buy_orders:
            mapping.setdefault(item["signal_date"], []).append(round(float(item.get("allocation_cash") or 0.0), 2))
        return mapping

    def allocations_by_event(result):
        buy_orders = [item for item in result.orders if item["side"] == "buy" and not item.get("is_pyramid_add")]
        return {
            (item["signal_date"], item["symbol"]): round(float(item.get("allocation_cash") or 0.0), 2)
            for item in buy_orders
        }

    equal_allocations = allocations_by_day(results["equal_weight"])
    factor_allocations = allocations_by_day(results["factor_weight"])
    vol_allocations = allocations_by_day(results["volatility_target"])
    risk_allocations = allocations_by_day(results["risk_budget"])
    equal_events = allocations_by_event(results["equal_weight"])
    factor_events = allocations_by_event(results["factor_weight"])
    vol_events = allocations_by_event(results["volatility_target"])
    risk_events = allocations_by_event(results["risk_budget"])
    first_equal_event = equal_events[min(equal_events)]

    assert first_equal_event > 0
    shared_factor_events = set(equal_events) & set(factor_events)
    shared_vol_events = set(equal_events) & set(vol_events)
    shared_risk_events = set(equal_events) & set(risk_events)
    assert shared_factor_events
    assert shared_vol_events
    assert shared_risk_events
    model_differences = [
        any(factor_events[event] != equal_events[event] for event in shared_factor_events),
        any(vol_events[event] != equal_events[event] for event in shared_vol_events),
        any(risk_events[event] != equal_events[event] for event in shared_risk_events),
    ]
    assert any(model_differences)
    assert risk_allocations


def test_pyramid_add_orders_are_generated():
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["min_listing_days"] = 1
    dsl["universe"]["filters"] = []
    dsl["factor_model"]["select"] = {"top_n": 1, "min_score": 0.2}
    dsl["position"].update(
        {
            "method": "risk_budget",
            "initial_position_pct": 0.1,
            "max_single_position_pct": 0.4,
            "max_position_pct": 1.0,
            "cash_reserve_pct": 0.0,
            "risk_per_trade_pct": 0.01,
            "pyramid_enabled": True,
            "pyramid_max_adds": 2,
            "pyramid_trigger_pct": 0.015,
            "pyramid_scale_pct": 0.5,
        }
    )
    dsl["risk"].update(
        {
            "max_positions": 1,
            "stop_loss_pct": 0.3,
            "take_profit_pct": 2.0,
            "trailing_stop_pct": 0.8,
            "max_drawdown_pct": 1.0,
            "max_daily_loss_pct": 1.0,
        }
    )
    result = run_strategy_backtest(
        run_id="unit_pyramid_add",
        strategy_name="金字塔加仓测试",
        dsl=dsl,
        symbols=["300750.SZ"],
        start_date="2024-01-01",
        end_date="2024-07-31",
        initial_capital=1_000_000,
        frequency="daily",
        benchmark="沪深300",
        use_minute_confirm=False,
    )
    pyramid_orders = [item for item in result.orders if item["side"] == "buy" and item.get("is_pyramid_add")]
    pyramid_trades = [item for item in result.trades if item["direction"] == "buy" and item.get("is_pyramid_add")]
    assert pyramid_orders
    assert pyramid_trades
    assert all(item["reason"] == "pyramid_add" for item in pyramid_orders)


def test_universe_filters_and_order_artifact_are_written():
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["min_listing_days"] = 1
    dsl["universe"]["filters"] = [
        {"field": "float_market_cap", "op": "between", "value": [10_000_000_000, 14_000_000_000], "unit": "CNY"}
    ]
    dsl["factor_model"]["select"] = {"top_n": 5, "min_score": 0.5}
    result = run_strategy_backtest(
        run_id="unit_universe_orders",
        strategy_name="股票池过滤订单测试",
        dsl=dsl,
        symbols=["300750.SZ", "300520.SZ", "601136.SH"],
        start_date="2024-01-01",
        end_date="2024-06-30",
        initial_capital=1_000_000,
        frequency="daily",
        benchmark="沪深300",
        use_minute_confirm=False,
    )

    assert result.diagnostics["universe_filter"]["applied_filters"]
    assert result.diagnostics["universe_filter"]["concept_filter"]["status"] in {"metadata_missing", "applied", "not_requested", "no_match_fallback"}
    assert result.orders
    assert result.diagnostics["order_count"] == len(result.orders)
    assert "orders" in {path.name.removesuffix(".json") for path in Path(result.artifact_root).glob("*.json")}


def test_evolution_detail_and_paper_account_create_routes():
    client = TestClient(app)
    strategy_id = client.get("/v1/strategies").json()["strategies"][0]["id"]
    experiment_response = client.post("/v1/evolution/experiments", json={"strategy_id": strategy_id})
    assert experiment_response.status_code == 200
    experiment_id = experiment_response.json()["id"]
    detail_response = client.get(f"/v1/evolution/experiments/{experiment_id}")
    assert detail_response.status_code == 200
    assert detail_response.json()["id"] == experiment_id
    candidates_response = client.get(f"/v1/evolution/experiments/{experiment_id}/candidates")
    assert candidates_response.status_code == 200
    assert candidates_response.json()["candidates"]

    account_response = client.post(
        "/v1/paper/accounts",
        json={"id": "paper-route-test", "name": "纸交易测试账户", "initial_capital": 500000},
    )
    assert account_response.status_code in {200, 409}
    if account_response.status_code == 200:
        assert account_response.json()["cash"] == 500000


def test_backtest_cancel_and_compare_routes():
    client = TestClient(app)
    strategy_id = client.get("/v1/strategies").json()["strategies"][0]["id"]

    payload = {
        "strategy_id": strategy_id,
        "symbols": ["300750.SZ", "300520.SZ"],
        "start_date": "2024-09-01",
        "end_date": "2024-12-31",
        "initial_capital": 1_000_000,
        "frequency": "daily_minute",
        "benchmark": "沪深300",
        "use_minute_confirm": True,
    }
    first = client.post("/v1/backtests", json=payload)
    second = client.post("/v1/backtests", json={**payload, "symbols": ["300750.SZ"]})

    assert first.status_code == 200
    assert second.status_code == 200

    first_run_id = first.json()["id"]
    second_run_id = second.json()["id"]

    cancel_response = client.post(f"/v1/backtests/{first_run_id}/cancel")
    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == "completed"

    compare_response = client.post("/v1/backtests/compare", json={"run_ids": [first_run_id, second_run_id]})
    assert compare_response.status_code == 200
    compare_payload = compare_response.json()
    assert compare_payload["run_ids"] == [first_run_id, second_run_id]
    assert len(compare_payload["runs"]) == 2
    assert "total_return" in compare_payload["summary"]
    assert compare_payload["runs"][0]["diagnostics"]["engine_mode"] in {"true_engine", "fallback"}
