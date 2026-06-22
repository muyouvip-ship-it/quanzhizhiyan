"""Database configuration and session management."""

import logging
import os
from datetime import datetime, timezone
from typing import Generator

from sqlalchemy import Boolean, create_engine, Column, Date, String, DateTime, Text, Integer, Float, JSON, UniqueConstraint, event, inspect, text
from sqlalchemy import inspection
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session

from api.core.env import load_project_env
from api.core.utils import env_flag as _env_flag

load_project_env()

_POSTGRES_PREFIXES = ("postgresql://", "postgresql+", "postgres://")


def _database_url() -> str | None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None
    if not database_url.startswith(_POSTGRES_PREFIXES):
        raise RuntimeError("DATABASE_URL must point to PostgreSQL.")
    return database_url


_engine: Engine | None = None
_session_local = None


def _require_engine() -> Engine:
    global _engine, _session_local
    if _engine is not None:
        return _engine
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required. This project now supports PostgreSQL only.")
    _engine = create_engine(database_url, echo=False)
    _session_local = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _engine


class _LazySessionLocal:
    def __call__(self, *args, **kwargs):
        return _session_local_factory()(*args, **kwargs)

    def __getattr__(self, item):
        return getattr(_session_local_factory(), item)


def _session_local_factory():
    _require_engine()
    assert _session_local is not None
    return _session_local


class _LazyEngineProxy:
    def _engine(self) -> Engine:
        return _require_engine()

    def __getattr__(self, item):
        return getattr(self._engine(), item)

    @property
    def url(self):
        return self._engine().url

    def __repr__(self) -> str:
        try:
            return repr(self._engine())
        except Exception:
            return "<LazyEngineProxy unresolved>"


@inspection._inspects(_LazyEngineProxy)
def _inspect_lazy_engine(target: _LazyEngineProxy):
    return inspect(target._engine())


engine = _LazyEngineProxy()
SessionLocal = _LazySessionLocal()

# Base class for models
Base = declarative_base()
logger = logging.getLogger(__name__)
_init_db_completed_for: str | None = None



def get_db() -> Generator[Session, None, None]:
    """Get database session (for FastAPI Depends)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class get_db_ctx:
    """Context manager for manual DB session usage.

    Usage:
        with get_db_ctx() as db:
            db.query(...)
    """

    def __init__(self) -> None:
        self.db: Session | None = None

    def __enter__(self) -> Session:
        self.db = SessionLocal()
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.db is not None:
            if exc_type is not None:
                self.db.rollback()
            self.db.close()


def init_db(*, force: bool = False) -> None:
    """Initialize database tables."""
    global _init_db_completed_for
    if _env_flag("TA_SKIP_STARTUP_DDL", "0") and not force:
        logger.info("Database schema initialization skipped by TA_SKIP_STARTUP_DDL.")
        return
    init_signature = str(engine.url)
    if not force and _init_db_completed_for == init_signature:
        return
    Base.metadata.create_all(bind=engine)
    _ensure_report_schema()
    _ensure_user_schema()
    _ensure_daily_review_schema()
    _ensure_market_data_schema()
    _ensure_market_data_pipeline_schema()
    _ensure_backtest_data_schema()
    _init_db_completed_for = init_signature


def _ensure_market_data_schema() -> None:
    """Ensure production market data tables and indexes exist."""
    try:
        with engine.begin() as conn:
            inspector = inspect(engine)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stock_daily_kline (
                    id BIGSERIAL PRIMARY KEY,
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
                    sw_industry_l3 VARCHAR(128),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stock_minute_kline (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    trade_time TIMESTAMP NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    amount DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS index_daily_kline (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    amount DOUBLE PRECISION,
                    source VARCHAR(32) DEFAULT 'qmt',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS index_minute_kline (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    trade_time TIMESTAMP NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    amount DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_daily_kline_symbol_date ON stock_daily_kline(symbol, trade_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stock_daily_kline_trade_date ON stock_daily_kline(trade_date)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_index_daily_kline_symbol_date ON index_daily_kline(symbol, trade_date)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_index_minute_kline_symbol_time ON index_minute_kline(symbol, trade_time)"))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS stock_minute_kline_symbol_trade_time_key ON stock_minute_kline(symbol, trade_time)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_minute_symbol ON stock_minute_kline(symbol)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_minute_time ON stock_minute_kline(trade_time)"))
            current_inspector = inspect(conn)
            if current_inspector.has_table("stock_daily_kline"):
                stock_daily_columns = {column["name"] for column in current_inspector.get_columns("stock_daily_kline")}
                for column_name, ddl in (
                    ("turnover_rate", "ALTER TABLE stock_daily_kline ADD COLUMN turnover_rate DOUBLE PRECISION"),
                    ("pre_close", "ALTER TABLE stock_daily_kline ADD COLUMN pre_close DOUBLE PRECISION"),
                    ("float_market_cap", "ALTER TABLE stock_daily_kline ADD COLUMN float_market_cap DOUBLE PRECISION"),
                    ("total_market_cap", "ALTER TABLE stock_daily_kline ADD COLUMN total_market_cap DOUBLE PRECISION"),
                    ("net_profit_ttm", "ALTER TABLE stock_daily_kline ADD COLUMN net_profit_ttm DOUBLE PRECISION"),
                    ("cash_flow_ttm", "ALTER TABLE stock_daily_kline ADD COLUMN cash_flow_ttm DOUBLE PRECISION"),
                    ("net_assets", "ALTER TABLE stock_daily_kline ADD COLUMN net_assets DOUBLE PRECISION"),
                    ("total_assets", "ALTER TABLE stock_daily_kline ADD COLUMN total_assets DOUBLE PRECISION"),
                    ("total_liabilities", "ALTER TABLE stock_daily_kline ADD COLUMN total_liabilities DOUBLE PRECISION"),
                    ("net_profit_quarter", "ALTER TABLE stock_daily_kline ADD COLUMN net_profit_quarter DOUBLE PRECISION"),
                    ("medium_buy", "ALTER TABLE stock_daily_kline ADD COLUMN medium_buy DOUBLE PRECISION"),
                    ("medium_sell", "ALTER TABLE stock_daily_kline ADD COLUMN medium_sell DOUBLE PRECISION"),
                    ("large_buy", "ALTER TABLE stock_daily_kline ADD COLUMN large_buy DOUBLE PRECISION"),
                    ("large_sell", "ALTER TABLE stock_daily_kline ADD COLUMN large_sell DOUBLE PRECISION"),
                    ("retail_buy", "ALTER TABLE stock_daily_kline ADD COLUMN retail_buy DOUBLE PRECISION"),
                    ("retail_sell", "ALTER TABLE stock_daily_kline ADD COLUMN retail_sell DOUBLE PRECISION"),
                    ("institution_buy", "ALTER TABLE stock_daily_kline ADD COLUMN institution_buy DOUBLE PRECISION"),
                    ("institution_sell", "ALTER TABLE stock_daily_kline ADD COLUMN institution_sell DOUBLE PRECISION"),
                    ("is_hs300", "ALTER TABLE stock_daily_kline ADD COLUMN is_hs300 BOOLEAN DEFAULT FALSE"),
                    ("is_sz50", "ALTER TABLE stock_daily_kline ADD COLUMN is_sz50 BOOLEAN DEFAULT FALSE"),
                    ("is_zz500", "ALTER TABLE stock_daily_kline ADD COLUMN is_zz500 BOOLEAN DEFAULT FALSE"),
                    ("is_zz1000", "ALTER TABLE stock_daily_kline ADD COLUMN is_zz1000 BOOLEAN DEFAULT FALSE"),
                    ("is_zz2000", "ALTER TABLE stock_daily_kline ADD COLUMN is_zz2000 BOOLEAN DEFAULT FALSE"),
                    ("is_cyb", "ALTER TABLE stock_daily_kline ADD COLUMN is_cyb BOOLEAN DEFAULT FALSE"),
                    ("sw_industry_l1", "ALTER TABLE stock_daily_kline ADD COLUMN sw_industry_l1 VARCHAR(128)"),
                    ("sw_industry_l2", "ALTER TABLE stock_daily_kline ADD COLUMN sw_industry_l2 VARCHAR(128)"),
                    ("sw_industry_l3", "ALTER TABLE stock_daily_kline ADD COLUMN sw_industry_l3 VARCHAR(128)"),
                    ("close_0935", "ALTER TABLE stock_daily_kline ADD COLUMN close_0935 DOUBLE PRECISION"),
                    ("close_0945", "ALTER TABLE stock_daily_kline ADD COLUMN close_0945 DOUBLE PRECISION"),
                    ("close_0955", "ALTER TABLE stock_daily_kline ADD COLUMN close_0955 DOUBLE PRECISION"),
                    ("created_at", "ALTER TABLE stock_daily_kline ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"),
                    ("updated_at", "ALTER TABLE stock_daily_kline ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"),
                ):
                    if column_name not in stock_daily_columns:
                        conn.execute(text(ddl))
            if current_inspector.has_table("index_daily_kline"):
                index_daily_columns = {column["name"] for column in current_inspector.get_columns("index_daily_kline")}
                if "source" not in index_daily_columns:
                    conn.execute(text("ALTER TABLE index_daily_kline ADD COLUMN source VARCHAR(32) DEFAULT 'qmt'"))
                else:
                    conn.execute(text("ALTER TABLE index_daily_kline ALTER COLUMN source TYPE VARCHAR(32)"))
                if "created_at" not in index_daily_columns:
                    conn.execute(text("ALTER TABLE index_daily_kline ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"))
                if "updated_at" not in index_daily_columns:
                    conn.execute(text("ALTER TABLE index_daily_kline ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"))
            if current_inspector.has_table("index_minute_kline"):
                index_minute_columns = {column["name"] for column in current_inspector.get_columns("index_minute_kline")}
                if "created_at" not in index_minute_columns:
                    conn.execute(text("ALTER TABLE index_minute_kline ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"))
                if "updated_at" not in index_minute_columns:
                    conn.execute(text("ALTER TABLE index_minute_kline ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"))
            if current_inspector.has_table("stock_minute_kline"):
                stock_minute_columns = {column["name"] for column in current_inspector.get_columns("stock_minute_kline")}
                if "created_at" not in stock_minute_columns:
                    conn.execute(text("ALTER TABLE stock_minute_kline ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"))
                if "updated_at" not in stock_minute_columns:
                    conn.execute(text("ALTER TABLE stock_minute_kline ADD COLUMN updated_at TIMESTAMP DEFAULT NOW()"))
    except Exception as e:
        logger.error("Failed to ensure market data schema: %s", e)


def _ensure_market_data_pipeline_schema() -> None:
    """Ensure raw/normalized/published/reconciliation market data tables exist."""
    try:
        with engine.begin() as conn:
            daily_raw_tables = [
                "raw_stock_daily_kline_postgresql",
                "raw_stock_daily_kline_quantclass",
                "raw_stock_daily_kline_tdx",
                "raw_stock_daily_kline_akshare",
                "raw_stock_daily_kline_baostock",
                "raw_stock_daily_kline_efinance",
            ]
            minute_raw_tables = [
                "raw_stock_minute_kline_postgresql",
                "raw_stock_minute_kline_qmt",
                "raw_stock_minute_kline_tdx",
                "raw_stock_minute_kline_akshare",
            ]
            timestamp_default = "CURRENT_TIMESTAMP"
            float_type = "DOUBLE PRECISION"
            bigint_type = "BIGINT"
            text_json_type = "TEXT"
            id_type = "BIGSERIAL PRIMARY KEY"

            for table_name in daily_raw_tables:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id {id_type},
                        symbol VARCHAR(20) NOT NULL,
                        trade_date DATE NOT NULL,
                        open {float_type},
                        high {float_type},
                        low {float_type},
                        close {float_type},
                        volume {float_type},
                        amount {float_type},
                        turnover_rate {float_type},
                        pre_close {float_type},
                        float_market_cap {float_type},
                        total_market_cap {float_type},
                        net_profit_ttm {float_type},
                        sw_industry_l1 VARCHAR(128),
                        sw_industry_l2 VARCHAR(128),
                        sw_industry_l3 VARCHAR(128),
                        batch_id VARCHAR(64),
                        fetched_at TIMESTAMP DEFAULT {timestamp_default},
                        created_at TIMESTAMP DEFAULT {timestamp_default},
                        updated_at TIMESTAMP DEFAULT {timestamp_default}
                    )
                """))
                conn.execute(text(f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_symbol_date ON {table_name}(symbol, trade_date)"))

            for table_name in minute_raw_tables:
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id {id_type},
                        symbol VARCHAR(20) NOT NULL,
                        trade_time TIMESTAMP NOT NULL,
                        trade_date DATE NOT NULL,
                        open {float_type},
                        high {float_type},
                        low {float_type},
                        close {float_type},
                        volume {float_type},
                        amount {float_type},
                        batch_id VARCHAR(64),
                        fetched_at TIMESTAMP DEFAULT {timestamp_default},
                        created_at TIMESTAMP DEFAULT {timestamp_default},
                        updated_at TIMESTAMP DEFAULT {timestamp_default}
                    )
                """))
                conn.execute(text(f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{table_name}_symbol_time ON {table_name}(symbol, trade_time)"))
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)"))
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date_symbol ON {table_name}(trade_date, symbol)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS norm_stock_daily_kline (
                    id {id_type},
                    source VARCHAR(32) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open {float_type},
                    high {float_type},
                    low {float_type},
                    close {float_type},
                    volume {float_type},
                    amount {float_type},
                    turnover_rate {float_type},
                    pre_close {float_type},
                    float_market_cap {float_type},
                    total_market_cap {float_type},
                    net_profit_ttm {float_type},
                    sw_industry_l1 VARCHAR(128),
                    sw_industry_l2 VARCHAR(128),
                    sw_industry_l3 VARCHAR(128),
                    batch_id VARCHAR(64),
                    fetched_at TIMESTAMP DEFAULT {timestamp_default},
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_norm_stock_daily_source_symbol_date ON norm_stock_daily_kline(source, symbol, trade_date)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS norm_stock_minute_kline (
                    id {id_type},
                    source VARCHAR(32) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    trade_time TIMESTAMP NOT NULL,
                    trade_date DATE NOT NULL,
                    open {float_type},
                    high {float_type},
                    low {float_type},
                    close {float_type},
                    volume {float_type},
                    amount {float_type},
                    batch_id VARCHAR(64),
                    fetched_at TIMESTAMP DEFAULT {timestamp_default},
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_norm_stock_minute_source_symbol_time ON norm_stock_minute_kline(source, symbol, trade_time)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_norm_stock_minute_trade_date ON norm_stock_minute_kline(trade_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_norm_stock_minute_trade_date_symbol ON norm_stock_minute_kline(trade_date, symbol)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS pub_stock_daily_kline (
                    id {id_type},
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    open {float_type},
                    high {float_type},
                    low {float_type},
                    close {float_type},
                    volume {float_type},
                    amount {float_type},
                    turnover_rate {float_type},
                    pre_close {float_type},
                    float_market_cap {float_type},
                    total_market_cap {float_type},
                    net_profit_ttm {float_type},
                    sw_industry_l1 VARCHAR(128),
                    sw_industry_l2 VARCHAR(128),
                    sw_industry_l3 VARCHAR(128),
                    source VARCHAR(32),
                    source_summary {text_json_type},
                    quality_status VARCHAR(32),
                    publish_status VARCHAR(32),
                    freshness_status VARCHAR(32),
                    coverage_ratio {float_type},
                    validation_sources TEXT,
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_pub_stock_daily_symbol_date ON pub_stock_daily_kline(symbol, trade_date)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS pub_stock_minute_kline (
                    id {id_type},
                    symbol VARCHAR(20) NOT NULL,
                    trade_time TIMESTAMP NOT NULL,
                    trade_date DATE NOT NULL,
                    open {float_type},
                    high {float_type},
                    low {float_type},
                    close {float_type},
                    volume {float_type},
                    amount {float_type},
                    primary_source VARCHAR(32),
                    source_mix TEXT,
                    quality_status VARCHAR(32),
                    publish_status VARCHAR(32),
                    freshness_status VARCHAR(32),
                    coverage_ratio {float_type},
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_pub_stock_minute_symbol_time ON pub_stock_minute_kline(symbol, trade_time)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pub_stock_minute_trade_date ON pub_stock_minute_kline(trade_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pub_stock_minute_trade_date_symbol ON pub_stock_minute_kline(trade_date, symbol)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS daily_kline_reconciliation_runs (
                    id {id_type},
                    run_id VARCHAR(64) NOT NULL,
                    trade_date DATE NOT NULL,
                    published_count INTEGER DEFAULT 0,
                    warning_count INTEGER DEFAULT 0,
                    missing_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_kline_reconciliation_run_id ON daily_kline_reconciliation_runs(run_id)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS daily_kline_reconciliation_items (
                    id {id_type},
                    run_id VARCHAR(64) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    chosen_source VARCHAR(32),
                    publish_status VARCHAR(32),
                    quality_status VARCHAR(32),
                    coverage_ratio {float_type},
                    issues {text_json_type},
                    source_summary {text_json_type},
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_kline_reconciliation_item_key ON daily_kline_reconciliation_items(run_id, symbol, trade_date)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS minute_kline_reconciliation_runs (
                    id {id_type},
                    run_id VARCHAR(64) NOT NULL,
                    trade_date DATE NOT NULL,
                    published_count INTEGER DEFAULT 0,
                    warning_count INTEGER DEFAULT 0,
                    missing_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_minute_kline_reconciliation_run_id ON minute_kline_reconciliation_runs(run_id)"))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS minute_kline_reconciliation_items (
                    id {id_type},
                    run_id VARCHAR(64) NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    trade_date DATE NOT NULL,
                    chosen_source VARCHAR(32),
                    publish_status VARCHAR(32),
                    quality_status VARCHAR(32),
                    coverage_ratio {float_type},
                    expected_bars INTEGER DEFAULT 0,
                    actual_bars INTEGER DEFAULT 0,
                    missing_times {text_json_type},
                    issues {text_json_type},
                    source_summary {text_json_type},
                    created_at TIMESTAMP DEFAULT {timestamp_default},
                    updated_at TIMESTAMP DEFAULT {timestamp_default}
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_minute_kline_reconciliation_item_key ON minute_kline_reconciliation_items(run_id, symbol, trade_date)"))

            inspector = inspect(conn)
            daily_enrichment_columns = (
                ("float_market_cap", f"ALTER TABLE {{table_name}} ADD COLUMN float_market_cap {float_type}"),
                ("total_market_cap", f"ALTER TABLE {{table_name}} ADD COLUMN total_market_cap {float_type}"),
                ("net_profit_ttm", f"ALTER TABLE {{table_name}} ADD COLUMN net_profit_ttm {float_type}"),
                ("sw_industry_l1", "ALTER TABLE {table_name} ADD COLUMN sw_industry_l1 VARCHAR(128)"),
                ("sw_industry_l2", "ALTER TABLE {table_name} ADD COLUMN sw_industry_l2 VARCHAR(128)"),
                ("sw_industry_l3", "ALTER TABLE {table_name} ADD COLUMN sw_industry_l3 VARCHAR(128)"),
            )
            for table_name in [*daily_raw_tables, "norm_stock_daily_kline"]:
                if inspector.has_table(table_name):
                    existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
                    for column_name, ddl_template in daily_enrichment_columns:
                        if column_name not in existing_columns:
                            conn.execute(text(ddl_template.format(table_name=table_name)))
            if inspector.has_table("pub_stock_daily_kline"):
                pub_daily_columns = {column["name"] for column in inspector.get_columns("pub_stock_daily_kline")}
                for column_name, ddl in (
                    ("float_market_cap", f"ALTER TABLE pub_stock_daily_kline ADD COLUMN float_market_cap {float_type}"),
                    ("total_market_cap", f"ALTER TABLE pub_stock_daily_kline ADD COLUMN total_market_cap {float_type}"),
                    ("net_profit_ttm", f"ALTER TABLE pub_stock_daily_kline ADD COLUMN net_profit_ttm {float_type}"),
                    ("sw_industry_l1", "ALTER TABLE pub_stock_daily_kline ADD COLUMN sw_industry_l1 VARCHAR(128)"),
                    ("sw_industry_l2", "ALTER TABLE pub_stock_daily_kline ADD COLUMN sw_industry_l2 VARCHAR(128)"),
                    ("sw_industry_l3", "ALTER TABLE pub_stock_daily_kline ADD COLUMN sw_industry_l3 VARCHAR(128)"),
                ):
                    if column_name not in pub_daily_columns:
                        conn.execute(text(ddl))
            _ensure_market_data_increment_views(conn)
    except Exception as e:
        logger.error("Failed to ensure market data pipeline schema: %s", e)


def _ensure_market_data_increment_views(conn) -> None:
    """Expose legacy K-line tables plus incremental published rows without mutating legacy data."""
    conn.execute(text("DROP VIEW IF EXISTS market_stock_daily_kline"))
    conn.execute(text("DROP VIEW IF EXISTS market_stock_minute_kline"))
    false_expr = "'false'"
    pub_daily_symbol_key = "split_part(p.symbol, '.', 1)"
    legacy_daily_symbol_key = "split_part(s.symbol, '.', 1)"
    pub_minute_symbol_key = pub_daily_symbol_key
    legacy_minute_symbol_key = legacy_daily_symbol_key

    conn.execute(text(f"""
        CREATE VIEW market_stock_daily_kline AS
        SELECT
            -p.id AS id,
            p.symbol,
            p.trade_date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.volume,
            p.amount,
            p.turnover_rate,
            p.created_at,
            p.updated_at,
            p.pre_close,
            p.float_market_cap,
            p.total_market_cap,
            p.net_profit_ttm,
            CAST(NULL AS DOUBLE PRECISION) AS cash_flow_ttm,
            CAST(NULL AS DOUBLE PRECISION) AS net_assets,
            CAST(NULL AS DOUBLE PRECISION) AS total_assets,
            CAST(NULL AS DOUBLE PRECISION) AS total_liabilities,
            CAST(NULL AS DOUBLE PRECISION) AS net_profit_quarter,
            CAST(NULL AS DOUBLE PRECISION) AS medium_buy,
            CAST(NULL AS DOUBLE PRECISION) AS medium_sell,
            CAST(NULL AS DOUBLE PRECISION) AS large_buy,
            CAST(NULL AS DOUBLE PRECISION) AS large_sell,
            CAST(NULL AS DOUBLE PRECISION) AS retail_buy,
            CAST(NULL AS DOUBLE PRECISION) AS retail_sell,
            CAST(NULL AS DOUBLE PRECISION) AS institution_buy,
            CAST(NULL AS DOUBLE PRECISION) AS institution_sell,
            CAST({false_expr} AS VARCHAR) AS is_hs300,
            CAST({false_expr} AS VARCHAR) AS is_sz50,
            CAST({false_expr} AS VARCHAR) AS is_zz500,
            CAST({false_expr} AS VARCHAR) AS is_zz1000,
            CAST({false_expr} AS VARCHAR) AS is_zz2000,
            CAST({false_expr} AS VARCHAR) AS is_cyb,
            p.sw_industry_l1,
            p.sw_industry_l2,
            p.sw_industry_l3,
            CAST(NULL AS DOUBLE PRECISION) AS close_0935,
            CAST(NULL AS DOUBLE PRECISION) AS close_0945,
            CAST(NULL AS DOUBLE PRECISION) AS close_0955,
            p.source,
            p.source_summary,
            p.quality_status,
            p.publish_status,
            p.freshness_status,
            p.coverage_ratio,
            p.validation_sources
        FROM pub_stock_daily_kline p
        UNION ALL
        SELECT
            s.id,
            s.symbol,
            s.trade_date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume,
            s.amount,
            s.turnover_rate,
            s.created_at,
            s.updated_at,
            s.pre_close,
            s.float_market_cap,
            s.total_market_cap,
            s.net_profit_ttm,
            s.cash_flow_ttm,
            s.net_assets,
            s.total_assets,
            s.total_liabilities,
            s.net_profit_quarter,
            s.medium_buy,
            s.medium_sell,
            s.large_buy,
            s.large_sell,
            s.retail_buy,
            s.retail_sell,
            s.institution_buy,
            s.institution_sell,
            CAST(s.is_hs300 AS VARCHAR) AS is_hs300,
            CAST(s.is_sz50 AS VARCHAR) AS is_sz50,
            CAST(s.is_zz500 AS VARCHAR) AS is_zz500,
            CAST(s.is_zz1000 AS VARCHAR) AS is_zz1000,
            CAST(s.is_zz2000 AS VARCHAR) AS is_zz2000,
            CAST(s.is_cyb AS VARCHAR) AS is_cyb,
            s.sw_industry_l1,
            s.sw_industry_l2,
            s.sw_industry_l3,
            s.close_0935,
            s.close_0945,
            s.close_0955,
            'postgresql' AS source,
            '{{"source_mix":["postgresql"],"legacy_table":"stock_daily_kline"}}' AS source_summary,
            'legacy' AS quality_status,
            'legacy' AS publish_status,
            'historical' AS freshness_status,
            1.0 AS coverage_ratio,
            'postgresql' AS validation_sources
        FROM stock_daily_kline s
        WHERE NOT EXISTS (
            SELECT 1
            FROM pub_stock_daily_kline p
            WHERE {pub_daily_symbol_key} = {legacy_daily_symbol_key}
              AND p.trade_date = s.trade_date
        )
    """))

    conn.execute(text(f"""
        CREATE VIEW market_stock_minute_kline AS
        SELECT
            -p.id AS id,
            p.symbol,
            p.trade_time,
            p.trade_date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.volume,
            p.amount,
            p.created_at,
            p.updated_at,
            p.primary_source,
            p.source_mix,
            p.quality_status,
            p.publish_status,
            p.freshness_status,
            p.coverage_ratio
        FROM pub_stock_minute_kline p
        UNION ALL
        SELECT
            s.id,
            s.symbol,
            s.trade_time,
            DATE(s.trade_time) AS trade_date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume,
            s.amount,
            s.created_at,
            s.updated_at,
            'postgresql' AS primary_source,
            'postgresql' AS source_mix,
            'legacy' AS quality_status,
            'legacy' AS publish_status,
            'historical' AS freshness_status,
            1.0 AS coverage_ratio
        FROM stock_minute_kline s
        WHERE NOT EXISTS (
            SELECT 1
            FROM pub_stock_minute_kline p
            WHERE {pub_minute_symbol_key} = {legacy_minute_symbol_key}
              AND p.trade_time = s.trade_time
        )
    """))


def _ensure_report_schema() -> None:
    """Add lightweight report columns for existing deployments without migrations."""
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'reports' AND table_schema = 'public'
            """))
            columns = {row[0] for row in result}
            
            if "direction" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN direction VARCHAR(50)"))
            if "status" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN status VARCHAR(20) DEFAULT 'completed'"))
            if "error" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN error TEXT"))
            if "analyst_traces" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN analyst_traces JSON"))
            if "macro_report" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN macro_report TEXT"))
            if "smart_money_report" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN smart_money_report TEXT"))
            if "game_theory_report" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN game_theory_report TEXT"))
            if "volume_price_report" not in columns:
                conn.execute(text("ALTER TABLE reports ADD COLUMN volume_price_report TEXT"))
    except Exception as e:
        logger.error("Failed to ensure report schema: %s", e)


def _ensure_user_schema() -> None:
    """Add user columns for existing deployments without migrations."""
    try:
        with engine.begin() as conn:
            user_result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'users' AND table_schema = 'public'
            """))
            columns = {row[0] for row in user_result}

            llm_result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'user_llm_configs' AND table_schema = 'public'
            """))
            llm_columns = {row[0] for row in llm_result}
            
            if "last_login_ip" not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN last_login_ip VARCHAR(45)"))
            if "email_report_enabled" not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN email_report_enabled BOOLEAN NOT NULL DEFAULT 1"))
            if "wecom_report_enabled" not in columns:
                conn.execute(text("ALTER TABLE users ADD COLUMN wecom_report_enabled BOOLEAN NOT NULL DEFAULT 1"))
            if "wecom_webhook_encrypted" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN wecom_webhook_encrypted TEXT"))
            if "default_analysts" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN default_analysts TEXT"))
            if "qmt_paper_account_config" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN qmt_paper_account_config TEXT"))
            if "qmt_live_account_config" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN qmt_live_account_config TEXT"))
            if "news_llm_provider" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN news_llm_provider VARCHAR(50)"))
            if "news_backend_url" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN news_backend_url VARCHAR(500)"))
            if "news_analysis_llm" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN news_analysis_llm VARCHAR(255)"))
            if "news_api_key_encrypted" not in llm_columns:
                conn.execute(text("ALTER TABLE user_llm_configs ADD COLUMN news_api_key_encrypted TEXT"))
    except Exception as e:
        logger.error("Failed to ensure user schema: %s", e)

    _migrate_tokens_to_hashed()
    _migrate_api_keys_reencrypt()


def _ensure_daily_review_schema() -> None:
    """Ensure daily review tables exist and contain required columns."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS daily_reviews (
                    id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(64) NOT NULL,
                    trade_date VARCHAR(10) NOT NULL,
                    status VARCHAR(20) DEFAULT 'completed',
                    market_summary JSON,
                    portfolio_summary JSON,
                    current_main_themes JSON,
                    current_key_stocks JSON,
                    next_main_themes JSON,
                    next_candidate_stocks JSON,
                    risk_watchpoints JSON,
                    narrative_markdown TEXT,
                    portfolio_technical_diagnostics JSON,
                    raw_result_data JSON,
                    push_status VARCHAR(20),
                    push_error TEXT,
                    last_pushed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_reviews_user_date ON daily_reviews(user_id, trade_date)"))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_daily_review_configs (
                    user_id VARCHAR(36) PRIMARY KEY,
                    enabled BOOLEAN NOT NULL DEFAULT FALSE,
                    trigger_time VARCHAR(5) NOT NULL DEFAULT '21:10',
                    push_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    last_run_date VARCHAR(10),
                    last_run_status VARCHAR(20),
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))

            current_inspector = inspect(conn)
            if current_inspector.has_table("daily_reviews"):
                columns = {column["name"] for column in current_inspector.get_columns("daily_reviews")}
                for column_name, ddl in (
                    ("status", "ALTER TABLE daily_reviews ADD COLUMN status VARCHAR(20) DEFAULT 'completed'"),
                    ("market_summary", "ALTER TABLE daily_reviews ADD COLUMN market_summary JSON"),
                    ("portfolio_summary", "ALTER TABLE daily_reviews ADD COLUMN portfolio_summary JSON"),
                    ("current_main_themes", "ALTER TABLE daily_reviews ADD COLUMN current_main_themes JSON"),
                    ("current_key_stocks", "ALTER TABLE daily_reviews ADD COLUMN current_key_stocks JSON"),
                    ("next_main_themes", "ALTER TABLE daily_reviews ADD COLUMN next_main_themes JSON"),
                    ("next_candidate_stocks", "ALTER TABLE daily_reviews ADD COLUMN next_candidate_stocks JSON"),
                    ("risk_watchpoints", "ALTER TABLE daily_reviews ADD COLUMN risk_watchpoints JSON"),
                    ("narrative_markdown", "ALTER TABLE daily_reviews ADD COLUMN narrative_markdown TEXT"),
                    ("portfolio_technical_diagnostics", "ALTER TABLE daily_reviews ADD COLUMN portfolio_technical_diagnostics JSON"),
                    ("raw_result_data", "ALTER TABLE daily_reviews ADD COLUMN raw_result_data JSON"),
                    ("push_status", "ALTER TABLE daily_reviews ADD COLUMN push_status VARCHAR(20)"),
                    ("push_error", "ALTER TABLE daily_reviews ADD COLUMN push_error TEXT"),
                    ("last_pushed_at", "ALTER TABLE daily_reviews ADD COLUMN last_pushed_at TIMESTAMP"),
                ):
                    if column_name not in columns:
                        conn.execute(text(ddl))
            if current_inspector.has_table("user_daily_review_configs"):
                columns = {column["name"] for column in current_inspector.get_columns("user_daily_review_configs")}
                for column_name, ddl in (
                    ("enabled", "ALTER TABLE user_daily_review_configs ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT FALSE"),
                    ("trigger_time", "ALTER TABLE user_daily_review_configs ADD COLUMN trigger_time VARCHAR(5) NOT NULL DEFAULT '21:10'"),
                    ("push_enabled", "ALTER TABLE user_daily_review_configs ADD COLUMN push_enabled BOOLEAN NOT NULL DEFAULT TRUE"),
                    ("last_run_date", "ALTER TABLE user_daily_review_configs ADD COLUMN last_run_date VARCHAR(10)"),
                    ("last_run_status", "ALTER TABLE user_daily_review_configs ADD COLUMN last_run_status VARCHAR(20)"),
                    ("last_error", "ALTER TABLE user_daily_review_configs ADD COLUMN last_error TEXT"),
                ):
                    if column_name not in columns:
                        conn.execute(text(ddl))
    except Exception as e:
        logger.error("Failed to ensure daily review schema: %s", e)


def _ensure_backtest_data_schema() -> None:
    """Ensure backtest subscription/config/task tables exist and contain required columns."""
    try:
        with engine.begin() as conn:
            create_tasks = """
                CREATE TABLE IF NOT EXISTS backtest_data_tasks (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    task_type VARCHAR(50) NOT NULL,
                    data_source VARCHAR(100),
                    date_range_start DATE NOT NULL,
                    date_range_end DATE NOT NULL,
                    symbols TEXT[],
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    progress INTEGER DEFAULT 0,
                    total_records INTEGER DEFAULT 0,
                    downloaded_records INTEGER DEFAULT 0,
                    error_message TEXT,
                    subscription_config_id INTEGER,
                    trigger_mode VARCHAR(20) DEFAULT 'manual',
                    task_scope VARCHAR(50) DEFAULT 'primary',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    completed_at TIMESTAMP
                )
            """
            create_configs = """
                CREATE TABLE IF NOT EXISTS backtest_data_configs (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    config_name VARCHAR(100) NOT NULL,
                    enabled_data_types TEXT[] NOT NULL DEFAULT '{}',
                    default_date_range_days INTEGER DEFAULT 365,
                    default_symbols TEXT[] DEFAULT '{}',
                    data_source_preference VARCHAR(100) DEFAULT 'tdx',
                    auto_download BOOLEAN DEFAULT FALSE,
                    update_frequency VARCHAR(20),
                    schedule_time VARCHAR(8) DEFAULT '15:05',
                    timezone VARCHAR(64) DEFAULT 'Asia/Shanghai',
                    only_trading_day BOOLEAN DEFAULT TRUE,
                    last_run_at TIMESTAMP,
                    last_success_at TIMESTAMP,
                    last_updated_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_user_config_name UNIQUE (user_id, config_name)
                )
            """
            create_stats = """
                CREATE TABLE IF NOT EXISTS backtest_data_stats (
                    id SERIAL PRIMARY KEY,
                    data_type VARCHAR(50) NOT NULL,
                    symbol VARCHAR(20),
                    date_range_start DATE,
                    date_range_end DATE,
                    total_records BIGINT DEFAULT 0,
                    last_updated_date DATE,
                    data_quality_score INTEGER DEFAULT 100,
                    missing_dates DATE[],
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_data_stats UNIQUE (data_type, symbol)
                )
            """
            create_watermarks = """
                CREATE TABLE IF NOT EXISTS backtest_data_watermarks (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    config_id INTEGER,
                    data_type VARCHAR(50) NOT NULL,
                    data_source VARCHAR(100),
                    scope_key TEXT NOT NULL,
                    last_data_date DATE,
                    last_run_started_at TIMESTAMP,
                    last_success_at TIMESTAMP,
                    last_status VARCHAR(20),
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT uq_backtest_watermark UNIQUE (user_id, config_id, data_type, data_source, scope_key)
                )
            """

            conn.execute(text(create_tasks))
            conn.execute(text(create_configs))
            conn.execute(text(create_stats))
            conn.execute(text(create_watermarks))

            task_columns = {
                row[0] for row in conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'backtest_data_tasks' AND table_schema = 'public'
                """))
            }
            config_columns = {
                row[0] for row in conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'backtest_data_configs' AND table_schema = 'public'
                """))
            }
            watermark_columns = {
                row[0] for row in conn.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'backtest_data_watermarks' AND table_schema = 'public'
                """))
            }

            if "subscription_config_id" not in task_columns:
                conn.execute(text("ALTER TABLE backtest_data_tasks ADD COLUMN subscription_config_id INTEGER"))
            if "trigger_mode" not in task_columns:
                conn.execute(text("ALTER TABLE backtest_data_tasks ADD COLUMN trigger_mode VARCHAR(20) DEFAULT 'manual'"))
            if "task_scope" not in task_columns:
                conn.execute(text("ALTER TABLE backtest_data_tasks ADD COLUMN task_scope VARCHAR(50) DEFAULT 'primary'"))

            if "schedule_time" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN schedule_time VARCHAR(8) DEFAULT '15:05'"))
            if "timezone" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN timezone VARCHAR(64) DEFAULT 'Asia/Shanghai'"))
            if "only_trading_day" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN only_trading_day BOOLEAN DEFAULT TRUE"))
            if "daily_kline_policy" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN daily_kline_policy TEXT"))
            if "minute_kline_policy" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN minute_kline_policy TEXT"))
            if "last_run_at" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN last_run_at TIMESTAMP"))
            if "last_success_at" not in config_columns:
                conn.execute(text("ALTER TABLE backtest_data_configs ADD COLUMN last_success_at TIMESTAMP"))

            if "scope_key" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN scope_key TEXT DEFAULT 'all'"))
            if "last_data_date" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN last_data_date DATE"))
            if "last_run_started_at" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN last_run_started_at TIMESTAMP"))
            if "last_success_at" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN last_success_at TIMESTAMP"))
            if "last_status" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN last_status VARCHAR(20)"))
            if "last_error" not in watermark_columns:
                conn.execute(text("ALTER TABLE backtest_data_watermarks ADD COLUMN last_error TEXT"))

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_backtest_tasks_user_id ON backtest_data_tasks(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_backtest_tasks_status ON backtest_data_tasks(status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_backtest_configs_user_id ON backtest_data_configs(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_backtest_watermarks_lookup ON backtest_data_watermarks(user_id, config_id, data_type)"))
    except Exception as e:
        logger.error("Failed to ensure backtest data schema: %s", e)


def _migrate_tokens_to_hashed() -> None:
    """Migrate plaintext API tokens to HMAC-SHA256 hashed storage."""
    import hashlib, hmac
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'user_tokens' AND table_schema = 'public'
            """))
            token_cols = {row[0] for row in result}
            
            if "token_hint" not in token_cols:
                conn.execute(text("ALTER TABLE user_tokens ADD COLUMN token_hint VARCHAR(8)"))

            # Detect un-migrated rows: plaintext tokens start with "ta-sk-"
            rows = conn.execute(text("SELECT id, token FROM user_tokens WHERE token LIKE 'ta-sk-%'")).fetchall()
            if not rows:
                return
            from api.services.auth_service import _secret_key
            key = _secret_key().encode("utf-8")
            for row_id, plaintext in rows:
                token_hash = hmac.new(key, plaintext.encode("utf-8"), hashlib.sha256).hexdigest()
                hint = plaintext[-4:]
                conn.execute(
                    text("UPDATE user_tokens SET token = :hash, token_hint = :hint WHERE id = :id"),
                    {"hash": token_hash, "hint": hint, "id": row_id},
                )
            logger.info("[security] Migrated %s API tokens from plaintext to hashed storage.", len(rows))
    except Exception as e:
        logger.error("Token hash migration failed: %s", e)


def _migrate_api_keys_reencrypt() -> None:
    """Re-encrypt user secrets when TA_APP_SECRET_KEY changes.

    On startup, if a custom secret is configured, tries to decrypt each secret
    with the current secret. If that fails, tries the default secret (old data).
    If the default key works, re-encrypts with the current key and writes back.
    """
    from api.services.auth_service import (
        is_custom_secret_configured, decrypt_secret,
        decrypt_secret_with_fallback, encrypt_secret,
    )
    if not is_custom_secret_configured():
        return
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT user_id, api_key_encrypted, wecom_webhook_encrypted
                    FROM user_llm_configs
                    WHERE api_key_encrypted IS NOT NULL OR wecom_webhook_encrypted IS NOT NULL
                    """
                )
            ).fetchall()
            if not rows:
                return
            # Quick check: if the first row decrypts fine, likely all are OK already.
            _, first_api_key, first_wecom_webhook = rows[0]
            first_secret = first_api_key or first_wecom_webhook
            if first_secret and decrypt_secret(first_secret) is not None and len(rows) < 50:
                # Small dataset, still verify all — but for large sets, skip if first is OK
                pass
            migrated = 0
            for user_id, encrypted_api_key, encrypted_wecom_webhook in rows:
                for column_name, encrypted_value in (
                    ("api_key_encrypted", encrypted_api_key),
                    ("wecom_webhook_encrypted", encrypted_wecom_webhook),
                ):
                    if not encrypted_value:
                        continue
                    if decrypt_secret(encrypted_value) is not None:
                        continue
                    plaintext = decrypt_secret_with_fallback(encrypted_value)
                    if plaintext is None:
                        logger.warning(
                            "[security] Cannot decrypt %s for user %s with any known key. Skipping.",
                            column_name,
                            user_id,
                        )
                        continue
                    new_encrypted = encrypt_secret(plaintext)
                    if column_name == "api_key_encrypted":
                        conn.execute(
                            text("UPDATE user_llm_configs SET api_key_encrypted = :enc WHERE user_id = :uid"),
                            {"enc": new_encrypted, "uid": user_id},
                        )
                    elif column_name == "wecom_webhook_encrypted":
                        conn.execute(
                            text("UPDATE user_llm_configs SET wecom_webhook_encrypted = :enc WHERE user_id = :uid"),
                            {"enc": new_encrypted, "uid": user_id},
                        )
                    migrated += 1
            if migrated:
                logger.info("[security] Re-encrypted %s user secret(s) with new TA_APP_SECRET_KEY.", migrated)
    except Exception as e:
        logger.error("User secret re-encryption migration failed: %s", e)


# Report Model
class ReportDB(Base):
    """Report database model."""
    
    __tablename__ = "reports"
    
    id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(64), index=True, nullable=True)  # For future multi-user support
    symbol = Column(String(20), index=True, nullable=False)
    trade_date = Column(String(10), nullable=False)
    
    # Task lifecycle info
    status = Column(String(20), default="completed", index=True)  # pending, running, completed, failed
    error = Column(Text, nullable=True)
    
    # Decision info
    decision = Column(String(50), nullable=True)  # BUY, SELL, HOLD, etc.
    direction = Column(String(50), nullable=True)  # 看多、偏多、中性、偏空、看空
    confidence = Column(Integer, nullable=True)  # 0-100
    target_price = Column(Float, nullable=True)
    stop_loss_price = Column(Float, nullable=True)
    
    # Full analysis results stored as JSON
    result_data = Column(JSON, nullable=True)

    # LLM-extracted structured data
    risk_items = Column(JSON, nullable=True)   # [{"name": "...", "level": "high|medium|low", "description": "..."}]
    key_metrics = Column(JSON, nullable=True)  # [{"name": "...", "value": "...", "status": "good|neutral|bad"}]
    analyst_traces = Column(JSON, nullable=True) # [{"agent": "...", "verdict": "...", "key_finding": "..."}]

    # Individual reports (for quick access)
    market_report = Column(Text, nullable=True)
    sentiment_report = Column(Text, nullable=True)
    news_report = Column(Text, nullable=True)
    fundamentals_report = Column(Text, nullable=True)
    macro_report = Column(Text, nullable=True)
    smart_money_report = Column(Text, nullable=True)
    volume_price_report = Column(Text, nullable=True)
    game_theory_report = Column(Text, nullable=True)
    investment_plan = Column(Text, nullable=True)
    trader_investment_plan = Column(Text, nullable=True)
    final_trade_decision = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "symbol": self.symbol,
            "trade_date": self.trade_date,
            "decision": self.decision,
            "direction": self.direction,
            "confidence": self.confidence,
            "target_price": self.target_price,
            "stop_loss_price": self.stop_loss_price,
            "result_data": self.result_data,
            "risk_items": self.risk_items,
            "key_metrics": self.key_metrics,
            "analyst_traces": self.analyst_traces,
            "market_report": self.market_report,
            "sentiment_report": self.sentiment_report,
            "news_report": self.news_report,
            "fundamentals_report": self.fundamentals_report,
            "macro_report": self.macro_report,
            "smart_money_report": self.smart_money_report,
            "volume_price_report": self.volume_price_report,
            "game_theory_report": self.game_theory_report,
            "investment_plan": self.investment_plan,
            "trader_investment_plan": self.trader_investment_plan,
            "final_trade_decision": self.final_trade_decision,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class UserDB(Base):
    __tablename__ = "users"

    id = Column(String(36), primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    last_login_at = Column(DateTime, nullable=True)
    last_login_ip = Column(String(45), nullable=True)
    email_report_enabled = Column(Boolean, default=True, nullable=False, server_default=text("true"))
    wecom_report_enabled = Column(Boolean, default=True, nullable=False, server_default=text("true"))


class EmailVerificationCodeDB(Base):
    __tablename__ = "email_verification_codes"

    id = Column(String(36), primary_key=True, index=True)
    email = Column(String(255), index=True, nullable=False)
    code_hash = Column(String(255), nullable=False)
    purpose = Column(String(50), default="login", nullable=False)
    expires_at = Column(DateTime, nullable=False)
    consumed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class UserLLMConfigDB(Base):
    __tablename__ = "user_llm_configs"

    user_id = Column(String(36), primary_key=True, index=True)
    llm_provider = Column(String(50), nullable=True)
    backend_url = Column(String(500), nullable=True)
    quick_think_llm = Column(String(255), nullable=True)
    deep_think_llm = Column(String(255), nullable=True)
    max_debate_rounds = Column(Integer, nullable=True)
    max_risk_discuss_rounds = Column(Integer, nullable=True)
    api_key_encrypted = Column(Text, nullable=True)
    wecom_webhook_encrypted = Column(Text, nullable=True)
    default_analysts = Column(Text, nullable=True)  # JSON list, e.g. '["market","social",...]'
    qmt_paper_account_config = Column(Text, nullable=True)
    qmt_live_account_config = Column(Text, nullable=True)
    news_llm_provider = Column(String(50), nullable=True)
    news_backend_url = Column(String(500), nullable=True)
    news_analysis_llm = Column(String(255), nullable=True)
    news_api_key_encrypted = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class UserTokenDB(Base):
    __tablename__ = "user_tokens"

    id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(36), index=True, nullable=False)
    name = Column(String(50), nullable=False)
    token = Column(String(128), unique=True, index=True, nullable=False)
    token_hint = Column(String(8), nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    last_used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class VersionStatsDB(Base):
    __tablename__ = "version_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(50), nullable=True)
    nonce = Column(String(64), nullable=True)
    remote_ip = Column(String(45), nullable=True, index=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class WatchlistItemDB(Base):
    """User watchlist items."""
    __tablename__ = "watchlist_items"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    symbol = Column(String(20), nullable=False)
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (UniqueConstraint('user_id', 'symbol', name='uq_watchlist_user_symbol'),)


class ScheduledAnalysisDB(Base):
    """Scheduled daily analysis tasks."""
    __tablename__ = "scheduled_analyses"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    symbol = Column(String(20), nullable=False)
    horizon = Column(String(10), default="short")
    trigger_time = Column(String(5), default="20:00")
    is_active = Column(Boolean, default=True)
    last_run_date = Column(String(10), nullable=True)
    last_run_status = Column(String(10), nullable=True)
    last_report_id = Column(String(36), nullable=True)
    consecutive_failures = Column(Integer, default=0)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (UniqueConstraint('user_id', 'symbol', name='uq_scheduled_user_symbol'),)


class DailyReviewDB(Base):
    __tablename__ = "daily_reviews"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    trade_date = Column(String(10), nullable=False, index=True)
    status = Column(String(20), default="completed", nullable=False)
    market_summary = Column(JSON, nullable=True)
    portfolio_summary = Column(JSON, nullable=True)
    current_main_themes = Column(JSON, nullable=True)
    current_key_stocks = Column(JSON, nullable=True)
    next_main_themes = Column(JSON, nullable=True)
    next_candidate_stocks = Column(JSON, nullable=True)
    risk_watchpoints = Column(JSON, nullable=True)
    narrative_markdown = Column(Text, nullable=True)
    portfolio_technical_diagnostics = Column(JSON, nullable=True)
    raw_result_data = Column(JSON, nullable=True)
    push_status = Column(String(20), nullable=True)
    push_error = Column(Text, nullable=True)
    last_pushed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (UniqueConstraint("user_id", "trade_date", name="uq_daily_reviews_user_date"),)


class UserDailyReviewConfigDB(Base):
    __tablename__ = "user_daily_review_configs"

    user_id = Column(String(36), primary_key=True, index=True)
    enabled = Column(Boolean, default=False, nullable=False, server_default=text("false"))
    trigger_time = Column(String(5), default="21:10", nullable=False)
    push_enabled = Column(Boolean, default=True, nullable=False, server_default=text("true"))
    last_run_date = Column(String(10), nullable=True)
    last_run_status = Column(String(20), nullable=True)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class SponsorDB(Base):
    """Sponsor records managed by admin project."""
    __tablename__ = "sponsors"

    id = Column(String(36), primary_key=True, index=True)
    sponsor_type = Column(String(20), nullable=False, index=True)  # money | token
    name = Column(String(100), nullable=False)
    github = Column(String(100), nullable=True)
    avatar = Column(String(500), nullable=True)
    email = Column(String(255), nullable=True)
    provider = Column(String(100), nullable=True)       # token sponsor: provider name
    amount = Column(Float, nullable=True)                # admin-only, NOT exposed in public API
    date = Column(String(10), nullable=False)
    sort_order = Column(Integer, default=0)
    is_visible = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class FeedbackDB(Base):
    """User feedback / message board."""
    __tablename__ = "feedbacks"

    id = Column(String(36), primary_key=True, index=True)
    user_id = Column(String(64), index=True, nullable=False)
    user_email = Column(String(255), nullable=False)
    subject = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    admin_reply = Column(Text, nullable=True)
    replied_at = Column(DateTime, nullable=True)
    is_read = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class ImportedPortfolioPositionDB(Base):
    """Imported current holdings snapshot plus recent trade points for a symbol."""

    __tablename__ = "imported_portfolio_positions"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    source = Column(String(32), default="manual", nullable=False)
    symbol = Column(String(20), nullable=False)
    security_name = Column(String(80), nullable=True)
    current_position = Column(Float, nullable=True)
    available_position = Column(Float, nullable=True)
    average_cost = Column(Float, nullable=True)
    market_value = Column(Float, nullable=True)
    current_position_pct = Column(Float, nullable=True)
    trade_points_json = Column(JSON, nullable=True)
    trade_points_count = Column(Integer, default=0, nullable=False)
    latest_trade_at = Column(String(32), nullable=True)
    latest_trade_action = Column(String(16), nullable=True)
    last_imported_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'source', 'symbol', name='uq_imported_portfolio_user_source_symbol'),
    )


class VirtualPositionStateDB(Base):
    """Track virtual broker positions across syncs for holding-day estimation."""

    __tablename__ = "virtual_position_states"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    broker = Column(String(32), nullable=False, default="qmt")
    account_id = Column(String(64), nullable=False)
    symbol = Column(String(20), nullable=False)
    first_seen_at = Column(DateTime, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)
    last_quantity = Column(Float, nullable=True)
    last_price = Column(Float, nullable=True)
    last_market_value = Column(Float, nullable=True)
    last_payload_json = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'broker', 'account_id', 'symbol', name='uq_virtual_position_state'),
    )


class QmtSyncProfileDB(Base):
    """User-level QMT auto sync profile."""

    __tablename__ = "qmt_sync_profiles"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    account_key = Column(String(64), nullable=False)
    is_active = Column(Boolean, default=False, nullable=False)
    sync_interval_seconds = Column(Integer, default=30, nullable=False)
    sync_tracking_board = Column(Boolean, default=True, nullable=False)
    alert_on_disconnect = Column(Boolean, default=True, nullable=False)
    last_synced_at = Column(DateTime, nullable=True)
    last_status = Column(String(32), nullable=True)
    last_error = Column(Text, nullable=True)
    consecutive_failures = Column(Integer, default=0, nullable=False)
    last_alerted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'account_key', name='uq_qmt_sync_profile_user_account'),
    )


class QmtAccountSnapshotDB(Base):
    """Persist latest warehouse snapshot for each user/account to survive disconnects."""

    __tablename__ = "qmt_account_snapshots"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    account_key = Column(String(64), nullable=False)
    role = Column(String(32), nullable=True)
    account_id = Column(String(64), nullable=True)
    connection_json = Column(JSON, nullable=True)
    account_json = Column(JSON, nullable=True)
    positions_json = Column(JSON, nullable=True)
    orders_json = Column(JSON, nullable=True)
    trades_json = Column(JSON, nullable=True)
    summary_json = Column(JSON, nullable=True)
    fetched_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'account_key', name='uq_qmt_account_snapshot_user_account'),
    )


class QmtAccountEquitySnapshotDB(Base):
    """Persist daily latest QMT account equity snapshots for return statistics."""

    __tablename__ = "qmt_account_equity_snapshots"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    account_key = Column(String(64), index=True, nullable=False)
    role = Column(String(32), index=True, nullable=True)
    account_id = Column(String(64), nullable=True)
    snapshot_date = Column(Date, index=True, nullable=False)
    total_asset = Column(Float, nullable=False, default=0.0)
    market_value = Column(Float, nullable=False, default=0.0)
    available_cash = Column(Float, nullable=False, default=0.0)
    total_pnl = Column(Float, nullable=False, default=0.0)
    total_pnl_pct = Column(Float, nullable=True)
    today_pnl = Column(Float, nullable=False, default=0.0)
    summary_json = Column(JSON, nullable=True)
    fetched_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'account_key', 'snapshot_date', name='uq_qmt_equity_snapshot_user_account_date'),
    )


class QmtAccountTradeHistoryDB(Base):
    """Persist deduplicated QMT account trades for historical security lists."""

    __tablename__ = "qmt_account_trade_history"

    id = Column(String(36), primary_key=True)
    user_id = Column(String(64), index=True, nullable=False)
    account_key = Column(String(64), index=True, nullable=False)
    role = Column(String(32), index=True, nullable=True)
    account_id = Column(String(64), nullable=True)
    trade_uid = Column(String(96), nullable=False)
    trade_id = Column(String(128), nullable=True)
    order_id = Column(String(128), nullable=True)
    symbol = Column(String(32), index=True, nullable=False)
    name = Column(String(128), nullable=True)
    side = Column(String(32), nullable=True)
    price = Column(Float, nullable=True)
    quantity = Column(Float, nullable=True)
    amount = Column(Float, nullable=True)
    cost_price = Column(Float, nullable=True)
    cost_basis = Column(Float, nullable=True)
    realized_pnl = Column(Float, nullable=True)
    realized_pnl_pct = Column(Float, nullable=True)
    pnl_status = Column(String(32), nullable=True)
    trade_time = Column(DateTime, nullable=True)
    trade_date = Column(Date, index=True, nullable=True)
    raw_json = Column(JSON, nullable=True)
    fetched_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('user_id', 'account_key', 'trade_uid', name='uq_qmt_trade_history_user_account_uid'),
    )
