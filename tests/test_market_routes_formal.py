from datetime import date, datetime, timedelta
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import market
from api.routes.market import router, search_stocks


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def test_market_quote_endpoint_uses_formal_qmt_service(monkeypatch):
    monkeypatch.setattr(
        "api.routes.market.fetch_realtime_quotes",
        lambda symbols: {
            "000001.SH": {
                "symbol": "000001.SH",
                "price": 3123.45,
                "change_pct": 0.56,
                "quote_time": "2026-04-28 10:31:00",
                "source": "qmt_realtime",
            }
        },
    )

    client = _client()
    response = client.get("/v1/market/quote", params={"symbol": "000001.SH"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "000001.SH"
    assert payload["quote"]["price"] == 3123.45
    assert payload["source"] == "qmt_realtime"


def test_market_intraday_endpoint_returns_qmt_intraday_payload(monkeypatch):
    monkeypatch.setattr(
        "api.routes.market.fetch_intraday_bars",
        lambda symbol, trade_date, period, include_latest_quote, account_key=None, persist=True: {
            "symbol": symbol,
            "trade_date": trade_date,
            "period": period,
            "items": [
                {
                    "symbol": symbol,
                    "trade_time": "2026-04-28 09:31:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "volume": 1234,
                    "amount": 12345.6,
                }
            ],
            "latest_quote": {
                "symbol": symbol,
                "price": 10.1,
                "quote_time": "2026-04-28 09:31:30",
            },
            "source": "qmt_intraday+postgresql_cache",
        },
    )

    client = _client()
    response = client.get(
        "/v1/market/intraday",
        params={"symbol": "000001.SZ", "trade_date": "2026-04-28", "period": "1m", "include_latest_quote": "true"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "000001.SZ"
    assert payload["source"] == "qmt_intraday+postgresql_cache"
    assert payload["items"][0]["trade_time"] == "2026-04-28 09:31:00"


def test_aggregate_intraday_bars_uses_trading_sequence_boundaries():
    items = []
    current = datetime(2026, 4, 28, 9, 31)
    while current <= datetime(2026, 4, 28, 11, 30):
        index = len(items) + 1
        items.append({
            "symbol": "600519.SH",
            "trade_time": current.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(index),
            "high": float(index) + 0.2,
            "low": float(index) - 0.2,
            "close": float(index) + 0.1,
            "volume": 1,
            "amount": 10,
        })
        current += timedelta(minutes=1)
    current = datetime(2026, 4, 28, 13, 1)
    while current <= datetime(2026, 4, 28, 15, 0):
        index = len(items) + 1
        items.append({
            "symbol": "600519.SH",
            "trade_time": current.strftime("%Y-%m-%d %H:%M:%S"),
            "open": float(index),
            "high": float(index) + 0.2,
            "low": float(index) - 0.2,
            "close": float(index) + 0.1,
            "volume": 1,
            "amount": 10,
        })
        current += timedelta(minutes=1)

    bars = market._aggregate_intraday_bars(items, "15m")

    assert len(bars) == 16
    assert bars[0]["trade_time"] == "2026-04-28T09:45:00"
    assert bars[7]["trade_time"] == "2026-04-28T11:30:00"
    assert bars[8]["trade_time"] == "2026-04-28T13:15:00"
    assert bars[-1]["trade_time"] == "2026-04-28T15:00:00"
    assert bars[0]["open"] == 1.0
    assert bars[0]["close"] == 15.1
    assert bars[0]["volume"] == 15.0


def test_stock_search_uses_authenticated_user_for_quote_lookup(monkeypatch):
    seen = {}

    monkeypatch.setattr("api.routes.market.get_reverse_stock_map", lambda: {"600000.SH": "浦发银行"})
    monkeypatch.setattr("api.routes.market.search_cn_stock_by_name", lambda query: None)
    monkeypatch.setattr("api.routes.market._load_latest_stock_changes", lambda db, symbols: {})

    def fake_load_quote_map(symbols, *, timeout_seconds=None, db=None, user_id=None):
        seen["symbols"] = symbols
        seen["user_id"] = user_id
        return {"600000.SH": {"price": 8.88, "change_pct": 1.23}}

    monkeypatch.setattr("api.routes.market._load_quote_map", fake_load_quote_map)

    payload = search_stocks(q="浦发", db=object(), current_user=SimpleNamespace(id="user-123"))

    assert seen == {"symbols": ["600000.SH"], "user_id": "user-123"}
    assert payload["results"][0]["symbol"] == "600000.SH"
    assert payload["results"][0]["current_price"] == 8.88


def test_market_overview_exposes_market_stats_and_behavior_labels(monkeypatch):
    monkeypatch.setattr(market, "_load_quote_map", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        market,
        "_load_latest_index_item",
        lambda db, code: {
            "price": 3200.0,
            "pre_close": 3180.0,
            "change": 20.0,
            "change_pct": 0.6289,
            "trade_date": "2026-05-26",
            "amount": 1_200_000_000_000 if code == "000001" else 1_000_000_000_000,
            "source": "postgresql:index_daily_kline",
        },
    )
    monkeypatch.setattr(
        market,
        "_load_stock_rankings",
        lambda db, limit: (
            [{"symbol": "600584.SH", "name": "长电科技", "change_pct": 5.0, "source": "postgresql:stock_daily_kline"}],
            [{"symbol": "601398.SH", "name": "工商银行", "change_pct": -1.2, "source": "postgresql:stock_daily_kline"}],
        ),
    )
    monkeypatch.setattr(
        market,
        "_load_sector_rankings",
        lambda db, limit: (
            [{"sector_name": "电子", "change_pct": 4.8, "member_count": 30, "amount": 320_000_000_000, "source": "industry_aggregate:stock_daily_kline"}],
            [{"sector_name": "银行", "change_pct": -1.4, "member_count": 20, "amount": 90_000_000_000, "source": "industry_aggregate:stock_daily_kline"}],
        ),
    )
    monkeypatch.setattr(market, "_load_sector_fund_flow", lambda limit: ([{"sector_name": "电子", "net_inflow": 6_000_000_000}], []))
    monkeypatch.setattr(
        market,
        "_load_market_stats",
        lambda db: {
            "trade_date": "2026-05-26",
            "total_amount": 2_200_000_000_000,
            "amount_change": 100_000_000_000,
            "up_count": 3100,
            "down_count": 1800,
            "flat_count": 100,
            "limit_up_count": 90,
            "limit_down_count": 12,
            "limit_up_promotion_rate": 32.5,
            "failed_limit_up_rate": 18.0,
            "source": "postgresql:stock_daily_kline",
        },
    )

    payload = market.get_market_overview(limit=20, db=object(), current_user=SimpleNamespace(id="user-123"))

    assert payload["market_stats"]["index_turnover_amount"] == 2_200_000_000_000
    assert payload["market_stats"]["up_count"] == 3100
    assert payload["market_behavior_labels"]["locked_values"]["up_count"] == 3100
    assert payload["market_behavior_labels"]["breadth_state"]["label"] in {"赚钱效应温和扩散", "全市场右侧多头普涨修复"}


def test_load_market_stats_derives_width_amount_and_sentiment(monkeypatch):
    rows_by_date = {
        date(2026, 5, 26): [
            {"symbol": "600001.SH", "close": 10.98, "high": 10.98, "pre_close": 10.0, "amount": 100.0},
            {"symbol": "600002.SH", "close": 11.2, "high": 11.4, "pre_close": 10.8, "amount": 200.0},
            {"symbol": "600003.SH", "close": 9.5, "high": 10.2, "pre_close": 10.0, "amount": 300.0},
        ],
        date(2026, 5, 25): [
            {"symbol": "600001.SH", "close": 10.98, "high": 10.98, "pre_close": 10.0, "amount": 80.0},
            {"symbol": "600004.SH", "close": 8.8, "high": 8.8, "pre_close": 8.0, "amount": 120.0},
        ],
    }
    monkeypatch.setattr(market, "_preferred_market_latest_daily_table", lambda db: "stock_daily_kline")
    monkeypatch.setattr(market, "_has_table", lambda db, table_name: True)
    monkeypatch.setattr(market, "_load_latest_daily_trade_date", lambda db, table_name, trade_date=None: date(2026, 5, 26))
    monkeypatch.setattr(market, "_load_previous_daily_trade_date", lambda db, table_name, target_date: date(2026, 5, 25))
    monkeypatch.setattr(market, "_load_market_stat_rows", lambda db, table_name, trade_date: rows_by_date[trade_date])

    payload = market._load_market_stats(object())

    assert payload["total_amount"] == 600.0
    assert payload["previous_total_amount"] == 200.0
    assert payload["amount_change"] == 400.0
    assert payload["up_count"] == 2
    assert payload["down_count"] == 1
    assert payload["limit_up_count"] == 1
    assert payload["limit_up_promotion_base"] == 2
    assert payload["limit_up_promotion_count"] == 1
