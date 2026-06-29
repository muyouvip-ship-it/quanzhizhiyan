from __future__ import annotations

from datetime import date, datetime, timedelta

from sqlalchemy import text

from api.database import Base, WatchlistItemDB
from api.models.strategy_models import Base as StrategyBase
from api.models.strategy_models import SelectionCenterTaskDB, StrategyDB, StrategyStatus, StrategyType
from api.services.strategy_platform_repository import PLATFORM_META_KEY
from api.services import stock_pool_service
from tests.postgres_test_utils import isolated_postgres_session


def test_default_group_imports_legacy_watchlist_and_dedupes_items():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        db.add(WatchlistItemDB(id="legacy-1", user_id="user-1", symbol="600000.SH", sort_order=0))
        db.commit()

        default_group = stock_pool_service.ensure_default_group(db, "user-1")
        duplicate = stock_pool_service.add_group_item(db, "user-1", default_group["id"], "600000.SH", name="浦发银行")
        added = stock_pool_service.add_group_item(db, "user-1", default_group["id"], "000001.SZ", name="平安银行")
        items = stock_pool_service.list_group_items(db, "user-1", default_group["id"])["items"]

        assert default_group["name"] == "我的自选"
        assert duplicate["status"] == "duplicate"
        assert added["status"] == "added"
        assert [item["symbol"] for item in items] == ["600000.SH", "000001.SZ"]


def test_selection_task_is_readonly_virtual_group_and_can_be_copied():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        StrategyBase.metadata.create_all(bind=db.get_bind())
        strategy_task = SelectionCenterTaskDB(
            id="task-1",
            user_id="user-1",
            name="首日波段",
            mode="strategy",
            status="completed",
            progress=100,
            universe="主板、创业板",
            rule="首日波段交易策略 / 买点规则 1 / 日K",
            filters_json=["非 ST"],
            config_json={},
            candidates_json=[
                {"symbol": "600000.SH", "name": "浦发银行", "source": "strategy", "metrics": {"sw_industry_l1": "银行"}},
                {"symbol": "000001.SZ", "name": "平安银行", "source": "strategy", "metrics": {"sw_industry_l1": "银行"}},
            ],
            created_at=datetime(2026, 6, 12, 0, 41, 53),
            completed_at=datetime(2026, 6, 12, 0, 46, 49),
        )
        db.add(strategy_task)
        db.commit()

        groups = stock_pool_service.list_groups(db, db, "user-1")["groups"]
        virtual = next(group for group in groups if group["id"] == "selection:task-1")
        virtual_items = stock_pool_service.list_group_items(db, "user-1", "selection:task-1", strategy_db=db)["items"]
        copied = stock_pool_service.copy_selection_task_to_group(db, db, "user-1", "task-1")
        copied_items = stock_pool_service.list_group_items(db, "user-1", copied["group"]["id"])["items"]

        assert virtual["readonly"] is True
        assert virtual["candidate_count"] == 2
        assert [item["symbol"] for item in virtual_items] == ["600000.SH", "000001.SZ"]
        assert copied["added"] == 2
        assert "首日波段" in copied["group"]["name"]
        assert copied["group"]["name"].endswith("2只")
        assert [item["symbol"] for item in copied_items] == ["600000.SH", "000001.SZ"]


def test_stock_pool_items_include_daily_enrichment_fields():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        db.execute(
            text(
                """
                CREATE TABLE stock_daily_kline (
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    amount DOUBLE PRECISION,
                    turnover_rate DOUBLE PRECISION,
                    pre_close DOUBLE PRECISION,
                    float_market_cap DOUBLE PRECISION,
                    total_market_cap DOUBLE PRECISION,
                    net_profit_ttm DOUBLE PRECISION,
                    sw_industry_l1 VARCHAR(128),
                    sw_industry_l2 VARCHAR(128),
                    sw_industry_l3 VARCHAR(128)
                )
                """
            )
        )
        db.execute(
            text(
                """
                INSERT INTO stock_daily_kline
                    (symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
                     float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3)
                VALUES
                    ('000001.SZ', '2026-06-22', 10.52, 10.67, 10.42, 10.65, 126530000, 1335000000, 0.65, 10.52,
                     206000000000, 207000000000, 45600000000, '银行', '股份制银行Ⅱ', '股份制银行Ⅲ')
                """
            )
        )
        db.commit()

        default_group = stock_pool_service.ensure_default_group(db, "user-1")
        stock_pool_service.add_group_item(db, "user-1", default_group["id"], "000001.SZ", name="平安银行")
        market_item = stock_pool_service.list_group_items(db, "user-1", "market:all", page_size=1)["items"][0]
        watchlist_item = stock_pool_service.list_group_items(db, "user-1", default_group["id"])["items"][0]

        for item in (market_item, watchlist_item):
            assert item["open"] == 10.52
            assert item["high"] == 10.67
            assert item["low"] == 10.42
            assert item["pre_close"] == 10.52
            assert item["volume"] == 126530000
            assert item["amount"] == 1335000000
            assert item["turnover_rate"] == 0.65
            assert item["float_market_cap"] == 206000000000
            assert item["total_market_cap"] == 207000000000
            assert item["net_profit_ttm"] == 45600000000
            assert item["industry_l2"] == "股份制银行Ⅱ"
            assert item["industry_l3"] == "股份制银行Ⅲ"


def test_stock_pool_items_can_sort_by_daily_fields_before_pagination():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        StrategyBase.metadata.create_all(bind=db.get_bind())
        db.execute(text("DROP TABLE IF EXISTS stock_daily_kline"))
        db.execute(
            text(
                """
                CREATE TABLE stock_daily_kline (
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    amount DOUBLE PRECISION,
                    pre_close DOUBLE PRECISION,
                    float_market_cap DOUBLE PRECISION,
                    total_market_cap DOUBLE PRECISION,
                    net_profit_ttm DOUBLE PRECISION,
                    sw_industry_l1 VARCHAR(128),
                    sw_industry_l2 VARCHAR(128),
                    sw_industry_l3 VARCHAR(128)
                )
                """
            )
        )
        db.execute(
            text(
                """
                INSERT INTO stock_daily_kline
                    (symbol, trade_date, open, high, low, close, volume, amount, pre_close,
                     float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3)
                VALUES
                    ('000001.SZ', '2026-06-22', 10, 11, 9, 10.5, 1000, 1000000, 10,
                     1000000000, 2000000000, 100000000, '银行', '银行Ⅱ', '银行Ⅲ'),
                    ('600000.SH', '2026-06-22', 20, 22, 19, 21.0, 2000, 5000000, 20,
                     5000000000, 6000000000, 200000000, '银行', '银行Ⅱ', '银行Ⅲ')
                """
            )
        )
        db.add(
            SelectionCenterTaskDB(
                id="sort-task",
                user_id="user-1",
                name="首日波段",
                mode="strategy",
                status="completed",
                progress=100,
                universe="主板",
                rule="首日波段交易策略 / 买点规则 1 / 日K",
                filters_json=[],
                config_json={},
                candidates_json=[
                    {"symbol": "000001.SZ", "name": "平安银行", "metrics": {}},
                    {"symbol": "600000.SH", "name": "浦发银行", "metrics": {}},
                ],
                created_at=datetime(2026, 6, 22, 9, 30),
                completed_at=datetime(2026, 6, 22, 9, 31),
            )
        )
        db.commit()

        market_items = stock_pool_service.list_group_items(
            db,
            "user-1",
            "market:all",
            page=1,
            page_size=1,
            sort_by="amount",
            sort_direction="desc",
        )["items"]
        selection_items = stock_pool_service.list_group_items(
            db,
            "user-1",
            "selection:sort-task",
            strategy_db=db,
            page=1,
            page_size=1,
            sort_by="amount",
            sort_direction="desc",
        )["items"]

        assert market_items[0]["symbol"] == "600000.SH"
        assert selection_items[0]["symbol"] == "600000.SH"


def test_strategy_preview_returns_buy_and_sell_markers_from_daily_kline():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        db.execute(
            text(
                """
                CREATE TABLE stock_daily_kline (
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    amount DOUBLE PRECISION,
                    pre_close DOUBLE PRECISION,
                    sw_industry_l1 VARCHAR(128)
                )
                """
            )
        )
        start = date(2026, 1, 1)
        closes = [10, 9, 8, 7, 8, 9, 10, 11, 10, 9, 8]
        rows = []
        previous = None
        for index, close in enumerate(closes):
            trade_date = start + timedelta(days=index)
            rows.append(
                {
                    "symbol": "000001.SZ",
                    "trade_date": trade_date,
                    "open": close,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1000000,
                    "amount": close * 1000000,
                    "pre_close": previous,
                    "sw_industry_l1": "银行",
                }
            )
            previous = close
        db.execute(
            text(
                """
                INSERT INTO stock_daily_kline
                    (symbol, trade_date, open, high, low, close, volume, amount, pre_close, sw_industry_l1)
                VALUES
                    (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :pre_close, :sw_industry_l1)
                """
            ),
            rows,
        )
        db.commit()

        preview = stock_pool_service.preview_strategy_markers(
            db,
            symbol="000001.SZ",
            strategy_id="ma-preview",
            period="daily",
            start_date="2026-01-01",
            end_date="2026-01-11",
        )

        assert preview["symbol"] == "000001.SZ"
        assert {marker["side"] for marker in preview["markers"]} == {"buy", "sell"}
        assert all(marker["price"] for marker in preview["markers"])
        assert all("MA5" in marker["reason"] for marker in preview["markers"])


def test_strategy_preview_uses_selected_strategy_dsl_when_strategy_db_is_provided():
    with isolated_postgres_session(Base, schema_prefix="ta_stock_pool") as db:
        StrategyBase.metadata.create_all(bind=db.get_bind())
        dsl = {
            "schema_version": "1.0",
            "strategy_type": "trading",
            "universe": {},
            "factor_model": {
                "factors": [{"name": "momentum_20d", "weight": 1.0, "direction": "higher_better", "transform": "rank_pct"}],
                "select": {"top_n": 20, "min_score": 0.6},
            },
            "entry": {"logic": "all", "conditions": [{"type": "cross_above", "left": "close", "right": "ma5"}]},
            "exit": {"logic": "any", "conditions": [{"type": "cross_below", "left": "close", "right": "ma5"}]},
            "position": {},
            "risk": {},
            "execution": {"minute_loading": {"forbid_full_market_preload": True}},
        }
        db.add(
            StrategyDB(
                id="strategy-dsl-preview",
                name="DSL预览策略",
                strategy_type=StrategyType.TRADING,
                status=StrategyStatus.ACTIVE,
                is_active=True,
                parameters={
                    PLATFORM_META_KEY: {
                        "status": "active",
                        "source": "manual",
                        "current_version": {"id": "v1", "version": 1, "dsl": dsl},
                    }
                },
                created_at=datetime(2026, 1, 1, 9, 30),
                updated_at=datetime(2026, 1, 1, 9, 30),
            )
        )
        start = date(2026, 1, 1)
        closes = [10, 9, 8, 7, 8, 9, 10, 11, 10, 9, 8]
        rows = []
        previous = None
        for index, close in enumerate(closes):
            rows.append(
                {
                    "symbol": "000001.SZ",
                    "trade_date": start + timedelta(days=index),
                    "open": close,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1000000,
                    "amount": close * 1000000,
                    "pre_close": previous,
                    "turnover_rate": 1.2,
                    "net_profit_ttm": 1000,
                }
            )
            previous = close
        db.execute(
            text(
                """
                INSERT INTO stock_daily_kline
                    (symbol, trade_date, open, high, low, close, volume, amount, pre_close, turnover_rate, net_profit_ttm)
                VALUES
                    (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :pre_close, :turnover_rate, :net_profit_ttm)
                """
            ),
            rows,
        )
        db.commit()

        preview = stock_pool_service.preview_strategy_markers(
            db,
            strategy_db=db,
            symbol="000001.SZ",
            strategy_id="strategy-dsl-preview",
            period="daily",
            start_date="2026-01-01",
            end_date="2026-01-11",
        )

        assert preview["source"].startswith("strategy_dsl:")
        assert {marker["side"] for marker in preview["markers"]} == {"buy", "sell"}
        assert all("DSL预览策略" in marker["reason"] for marker in preview["markers"])
