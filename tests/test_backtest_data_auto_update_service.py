from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from api.services.backtest_data_auto_update_service import (
    _effective_subscription_data_source,
    _order_subscription_data_types,
    recover_stale_running_tasks,
)
from tests.postgres_test_utils import isolated_postgres_engine


def test_daily_kline_subscription_uses_tdx_when_config_source_is_unsupported() -> None:
    assert _effective_subscription_data_source("daily_kline", "tencent") == "tencent"
    assert _effective_subscription_data_source("daily_kline", "qmt") == "tdx"
    assert _effective_subscription_data_source("daily_kline", None) == "tdx"


def test_minute_kline_subscription_keeps_qmt_source() -> None:
    assert _effective_subscription_data_source("minute_kline", "qmt") == "qmt"


def test_subscription_data_types_prioritize_index_tasks_before_full_market_minute() -> None:
    assert _order_subscription_data_types(
        ["daily_kline", "minute_kline", "index_data", "index_minute_kline"]
    ) == ["daily_kline", "index_data", "index_minute_kline", "minute_kline"]


def test_recover_stale_running_tasks_does_not_fail_pending_queue_items() -> None:
    with isolated_postgres_engine(schema_prefix="ta_backtest_auto") as (engine, _schema_url, _schema):
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE backtest_data_tasks (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    task_type VARCHAR(50) NOT NULL,
                    data_source VARCHAR(100),
                    symbols TEXT[],
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    error_message TEXT,
                    subscription_config_id INTEGER,
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                    completed_at TIMESTAMP WITHOUT TIME ZONE
                )
            """))
            conn.execute(text("""
                CREATE TABLE backtest_data_watermarks (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    config_id INTEGER NOT NULL,
                    data_type VARCHAR(50) NOT NULL,
                    data_source VARCHAR(100),
                    scope_key VARCHAR(255) NOT NULL,
                    last_status VARCHAR(20),
                    last_error TEXT,
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                )
            """))
            old_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=180)
            conn.execute(text("""
                INSERT INTO backtest_data_tasks
                (user_id, task_type, data_source, status, subscription_config_id, updated_at)
                VALUES
                ('user-1', 'index_data', 'tdx', 'pending', 1, :old_time),
                ('user-1', 'minute_kline', 'tdx', 'running', 1, :old_time)
            """), {"old_time": old_time})
            conn.execute(text("""
                INSERT INTO backtest_data_watermarks
                (user_id, config_id, data_type, data_source, scope_key, last_status)
                VALUES
                ('user-1', 1, 'index_data', 'tdx', 'all', 'running'),
                ('user-1', 1, 'minute_kline', 'tdx', 'all', 'running')
            """))

        with engine.begin() as conn:
            recovered = recover_stale_running_tasks(conn, stale_minutes=120)

        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT task_type, status
                FROM backtest_data_tasks
                ORDER BY id
            """)).fetchall()
            watermarks = conn.execute(text("""
                SELECT data_type, last_status
                FROM backtest_data_watermarks
                ORDER BY id
            """)).fetchall()

    assert recovered == 1
    assert [(row.task_type, row.status) for row in rows] == [
        ("index_data", "pending"),
        ("minute_kline", "failed"),
    ]
    assert [(row.data_type, row.last_status) for row in watermarks] == [
        ("index_data", "running"),
        ("minute_kline", "failed"),
    ]
