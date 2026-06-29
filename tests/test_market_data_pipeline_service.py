from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import text

from api import database as database_module
from tests.postgres_test_utils import isolated_postgres_engine
from api.services.market_data_pipeline_service import (
    ingest_raw_daily_rows,
    ingest_raw_minute_rows,
    preferred_daily_kline_table,
    preferred_minute_kline_table,
    publish_minute_trade_date,
    reconcile_daily_trade_dates,
)


@pytest.fixture()
def isolated_market_data_db(monkeypatch):
    import api.backtest_data_api as backtest_data_api
    import api.services.market_data_pipeline_service as pipeline

    with isolated_postgres_engine(schema_prefix="ta_market_data_pipeline") as (test_engine, _database_url, _schema):
        test_session_local = database_module.sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
        monkeypatch.setattr(database_module, "engine", test_engine)
        monkeypatch.setattr(database_module, "SessionLocal", test_session_local)
        monkeypatch.setattr(pipeline, "engine", test_engine)
        monkeypatch.setattr(pipeline, "SessionLocal", test_session_local)
        monkeypatch.setattr(backtest_data_api, "SessionLocal", test_session_local, raising=False)
        database_module._init_db_completed_for = None
        database_module.init_db(force=True)
        yield test_engine, test_session_local
    database_module._init_db_completed_for = None


def test_daily_reconcile_prefers_akshare_and_records_warning(isolated_market_data_db) -> None:
    test_engine, _ = isolated_market_data_db
    trade_day = date(2026, 5, 6)

    ingest_raw_daily_rows(
        source="akshare",
        rows=[{
            "symbol": "600000",
            "trade_date": trade_day,
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "volume": 1000,
            "amount": 10200,
        }],
    )
    ingest_raw_daily_rows(
        source="baostock",
        rows=[{
            "symbol": "600000",
            "trade_date": trade_day,
            "open": 10.0,
            "high": 10.6,
            "low": 9.8,
            "close": 10.4,
            "volume": 1400,
            "amount": 14560,
        }],
    )

    result = reconcile_daily_trade_dates(trade_dates=[trade_day], symbols=["600000"])
    assert result["success"] is True
    assert result["warning_count"] == 1

    with test_engine.begin() as conn:
        final_row = conn.execute(
            text(f"SELECT close FROM {preferred_daily_kline_table()} WHERE symbol = :symbol AND trade_date = :trade_date"),
            {"symbol": "600000.SH", "trade_date": trade_day},
        ).mappings().one()
        published_row = conn.execute(
            text("SELECT source, publish_status, close FROM pub_stock_daily_kline WHERE symbol = :symbol AND trade_date = :trade_date"),
            {"symbol": "600000.SH", "trade_date": trade_day},
        ).mappings().one()
        recon = conn.execute(
            text(
                """
                SELECT publish_status, chosen_source
                FROM daily_kline_reconciliation_items
                WHERE symbol = :symbol AND trade_date = :trade_date
                """
            ),
            {"symbol": "600000.SH", "trade_date": trade_day},
        ).mappings().one()

    assert preferred_daily_kline_table() == "stock_daily_kline"
    assert float(final_row["close"]) == 10.2
    assert published_row["source"] == "akshare"
    assert published_row["publish_status"] == "published_with_warning"
    assert float(published_row["close"]) == 10.2
    assert recon["chosen_source"] == "akshare"


def test_minute_publish_uses_qmt_then_fills_with_akshare(isolated_market_data_db) -> None:
    test_engine, _ = isolated_market_data_db
    trade_day = date(2026, 5, 6)
    qmt_rows = [
        {
            "symbol": "000001",
            "trade_time": datetime(2026, 5, 6, 9, 31),
            "open": 10.0,
            "high": 10.1,
            "low": 9.9,
            "close": 10.0,
            "volume": 100,
            "amount": 1000,
        }
    ]
    akshare_rows = [
        {
            "symbol": "000001",
            "trade_time": datetime(2026, 5, 6, 9, 32),
            "open": 10.0,
            "high": 10.2,
            "low": 9.95,
            "close": 10.1,
            "volume": 120,
            "amount": 1212,
        }
    ]

    ingest_raw_minute_rows(source="qmt", rows=qmt_rows)
    ingest_raw_minute_rows(source="akshare", rows=akshare_rows)

    result = publish_minute_trade_date(trade_date=trade_day, symbols=["000001"], minimum_coverage_ratio=0.0)
    assert result["success"] is True
    assert result["warning_count"] == 1

    with test_engine.begin() as conn:
        final_rows = conn.execute(
            text(
                f"""
                SELECT trade_time, close
                FROM {preferred_minute_kline_table()}
                WHERE symbol = :symbol AND DATE(trade_time) = :trade_date
                ORDER BY trade_time
                """
            ),
            {"symbol": "000001.SZ", "trade_date": trade_day},
        ).mappings().all()
        published_rows = conn.execute(
            text(
                """
                SELECT trade_time, primary_source, source_mix, publish_status
                FROM pub_stock_minute_kline
                WHERE symbol = :symbol AND trade_date = :trade_date
                ORDER BY trade_time
                """
            ),
            {"symbol": "000001.SZ", "trade_date": trade_day},
        ).mappings().all()
        recon = conn.execute(
            text(
                """
                SELECT actual_bars, chosen_source, source_summary
                FROM minute_kline_reconciliation_items
                WHERE symbol = :symbol AND trade_date = :trade_date
                """
            ),
            {"symbol": "000001.SZ", "trade_date": trade_day},
        ).mappings().one()

    assert preferred_minute_kline_table() == "stock_minute_kline"
    assert len(final_rows) == 2
    assert [float(item["close"]) for item in final_rows] == [10.0, 10.1]
    assert len(published_rows) == 2
    assert published_rows[0]["primary_source"] == "qmt"
    assert published_rows[0]["publish_status"] == "published_with_warning"
    assert "qmt" in str(published_rows[0]["source_mix"])
    assert "akshare" in str(published_rows[0]["source_mix"])
    assert int(recon["actual_bars"]) == 2
    assert recon["chosen_source"] == "qmt"


def test_backtest_stats_reports_final_minute_table_without_view_estimate(isolated_market_data_db) -> None:
    test_engine, test_session_local = isolated_market_data_db
    trade_time = datetime(2026, 5, 6, 9, 31)

    with test_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stock_minute_kline
                (symbol, trade_time, open, high, low, close, volume, amount)
                VALUES (:symbol, :trade_time, 10, 10.1, 9.9, 10, 100, 1000)
                """
            ),
            {"symbol": "000001.SZ", "trade_time": trade_time},
        )
        conn.execute(text("ANALYZE stock_minute_kline"))

    from api.backtest_data_api import _build_backtest_table_stat
    with test_session_local() as db:
        stat = _build_backtest_table_stat(
            db,
            data_type="minute_kline",
            table_name="stock_minute_kline",
            date_column="trade_time",
        )

    assert stat is not None
    assert stat.total_records == 1
    assert stat.date_range_start == trade_time.date()
    assert stat.date_range_end == trade_time.date()
    assert stat.coverage_source == "postgresql_fast"


def test_market_data_status_summarizes_final_minute_table(isolated_market_data_db) -> None:
    test_engine, _ = isolated_market_data_db
    trade_time = datetime(2026, 5, 6, 9, 31)

    with test_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stock_minute_kline
                (symbol, trade_time, open, high, low, close, volume, amount)
                VALUES (:symbol, :trade_time, 10, 10.1, 9.9, 10, 100, 1000)
                """
            ),
            {"symbol": "000001.SZ", "trade_time": trade_time},
        )

    from api.services.market_data_pipeline_service import get_market_data_publish_status

    payload = get_market_data_publish_status(trade_date=trade_time.date(), symbols=["000001"], limit=10)

    assert payload["tables"]["minute"] == "stock_minute_kline"
    assert payload["minute"]["summary"]["counts"] == {"final": 1}
    assert payload["minute"]["items"][0]["publish_status"] == "final"
    assert payload["minute"]["items"][0]["bars"] == 1
