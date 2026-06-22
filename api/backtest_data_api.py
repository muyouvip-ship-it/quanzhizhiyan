from api.core.utils import run_async
"""
回测数据配置和管理API
"""

from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy.orm import Session
from sqlalchemy import bindparam, text
import calendar as month_calendar
from datetime import date, datetime, timedelta
from typing import Any, List, Optional
import asyncio
import json
import logging
import os
import threading
import pandas as pd

from api.database import get_db, get_db_ctx, UserDB
from api.deps import require_api_user as get_current_user
from api.core.settings import settings
from api.data_downloader import DataDownloader
from api.quantclass_downloader import QuantClassDownloader
from api.quantclass_importer import import_stock_daily_from_quantclass
from api.data_quality_manager import DataQualityManager
from api.data_source_monitor import get_data_source_monitor
from api.services.daily_kline_parquet_store import get_daily_kline_parquet_stats, write_daily_kline_parquet_cache
from api.services.market_data_pipeline_service import DAILY_RAW_TABLES, preferred_daily_kline_table
from api.services.qmt_market_data_service import sync_index_daily_history, sync_index_minute_history
from tradingagents.dataflows.trade_calendar import is_cn_trading_day
from .backtest_data_models import (
    BacktestDataTaskCreate, BacktestDataTask,
    BacktestDataConfigCreate, BacktestDataConfig,
    BacktestDataSubscriptionStatus,
    BacktestDataStats, BatchDataDownloadRequest,
    BacktestDataTaskListResponse, BacktestDataConfigListResponse,
    BacktestDataStatsListResponse
)
from api.services import backtest_data_auto_update_service

router = APIRouter(prefix="/v1/backtest-data", tags=["backtest-data"])

_TABLE_STATS_MAPPING = {
    # 设置页统计必须走最终物理表，不能扫 market_* 兼容视图。
    "daily_kline": ("stock_daily_kline", "trade_date"),
    "index_data": ("index_daily_kline", "trade_date"),
    "minute_kline": ("stock_minute_kline", "trade_time"),
    "index_minute_kline": ("index_minute_kline", "trade_time"),
}
_ALLOWED_TABLE_NAMES = frozenset({
    *(name for name, _ in _TABLE_STATS_MAPPING.values()),
    *DAILY_RAW_TABLES.values(),
    "norm_stock_daily_kline",
    "pub_stock_daily_kline",
})


def _validate_table_name(table_name: str) -> str:
    """Whitelist table names to prevent SQL injection via dynamic identifiers."""
    if table_name not in _ALLOWED_TABLE_NAMES:
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name

_DAILY_KLINE_CALENDAR_TABLES = ("stock_daily_kline",)
_FAST_STATS_EXACT_ROW_THRESHOLD = 2_000_000
_FULL_MARKET_MINUTE_MIN_SYMBOLS = max(int(os.getenv("BACKTEST_FULL_MARKET_MINUTE_MIN_SYMBOLS", "3000") or 3000), 1)
_INDEX_MIN_SYMBOLS = max(int(os.getenv("BACKTEST_INDEX_MIN_SYMBOLS", "8") or 8), 1)
_MINUTE_MIN_BARS = max(int(os.getenv("BACKTEST_MINUTE_COMPLETE_MIN_BARS", "200") or 200), 1)
DEFAULT_MARKET_CLOSE_SYNC_TYPES = ["daily_kline", "index_data", "index_minute_kline", "minute_kline"]
DEFAULT_MARKET_CLOSE_DATA_SOURCE = "tdx"
DEFAULT_MARKET_CLOSE_SCHEDULE_TIME = "15:05"
DEFAULT_AKSHARE_BATCH_SIZE = max(int(os.getenv("AKSHARE_BATCH_SIZE", "20") or 20), 1)
DEFAULT_AKSHARE_BATCH_SLEEP_SECONDS = max(float(os.getenv("AKSHARE_BATCH_SLEEP_SECONDS", "0.5") or 0.5), 0.0)


def _normalize_config_payload(payload: dict) -> dict:
    raw = dict(payload or {})
    config_name = str(raw.get("config_name") or "default").strip() or "default"
    enabled_data_types = raw.get("enabled_data_types")
    if not enabled_data_types:
        enabled_data_types = raw.get("data_types") or []
    enabled_data_types = [str(item).strip() for item in enabled_data_types if str(item).strip()]
    auto_download = bool(raw.get("auto_download")) if "auto_download" in raw else bool(raw.get("auto_update", False))
    if not enabled_data_types and auto_download:
        enabled_data_types = list(DEFAULT_MARKET_CLOSE_SYNC_TYPES)
    default_symbols = raw.get("default_symbols")
    if default_symbols is None:
        default_symbols = raw.get("symbols") or []
    default_symbols = [str(item).strip().upper() for item in default_symbols if str(item).strip()]

    date_range_start = raw.get("date_range_start")
    date_range_end = raw.get("date_range_end")
    default_date_range_days = raw.get("default_date_range_days")
    if default_date_range_days in (None, "", 0):
        try:
            if date_range_start and date_range_end:
                start = date.fromisoformat(str(date_range_start))
                end = date.fromisoformat(str(date_range_end))
                default_date_range_days = max((end - start).days + 1, 1)
            else:
                default_date_range_days = 365
        except Exception:
            default_date_range_days = 365

    data_source_preference = str(
        raw.get("data_source_preference")
        or raw.get("data_source")
        or DEFAULT_MARKET_CLOSE_DATA_SOURCE
    ).strip() or DEFAULT_MARKET_CLOSE_DATA_SOURCE
    # Subscription UI no longer exposes Tencent as a selectable daily source.
    # Keep Tencent available for explicit one-off download tasks, but normalize
    # stale saved subscription configs back to the market-close TDX path.
    if "daily_kline" in enabled_data_types and data_source_preference.lower() == "tencent":
        data_source_preference = DEFAULT_MARKET_CLOSE_DATA_SOURCE
    update_frequency = raw.get("update_frequency")
    if not update_frequency and auto_download:
        update_frequency = "daily"
    schedule_time = str(raw.get("schedule_time") or DEFAULT_MARKET_CLOSE_SCHEDULE_TIME).strip() or DEFAULT_MARKET_CLOSE_SCHEDULE_TIME
    timezone_value = str(raw.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
    only_trading_day = bool(raw.get("only_trading_day", True))
    daily_kline_policy = raw.get("daily_kline_policy")
    minute_kline_policy = raw.get("minute_kline_policy")
    return {
        "config_name": config_name,
        "enabled_data_types": enabled_data_types,
        "default_date_range_days": max(int(default_date_range_days or 365), 1),
        "default_symbols": default_symbols,
        "data_source_preference": data_source_preference,
        "auto_download": auto_download,
        "update_frequency": str(update_frequency).strip() if update_frequency else None,
        "schedule_time": schedule_time,
        "timezone": timezone_value,
        "only_trading_day": only_trading_day,
        "daily_kline_policy": daily_kline_policy,
        "minute_kline_policy": minute_kline_policy,
    }


def _row_to_backtest_config(row) -> BacktestDataConfig:
    return BacktestDataConfig(
        id=row.id,
        user_id=row.user_id,
        config_name=row.config_name,
        enabled_data_types=row.enabled_data_types or [],
        default_date_range_days=row.default_date_range_days,
        default_symbols=row.default_symbols or [],
        data_source_preference=row.data_source_preference,
        auto_download=row.auto_download,
        update_frequency=row.update_frequency,
        schedule_time=getattr(row, "schedule_time", None),
        timezone=getattr(row, "timezone", None),
        only_trading_day=bool(getattr(row, "only_trading_day", True)),
        daily_kline_policy=_parse_json_config(getattr(row, "daily_kline_policy", None)),
        minute_kline_policy=_parse_json_config(getattr(row, "minute_kline_policy", None)),
        last_run_at=getattr(row, "last_run_at", None),
        last_success_at=getattr(row, "last_success_at", None),
        last_updated_at=row.last_updated_at,
        subscription_status=None,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _parse_json_config(value: object) -> dict | None:
    if value in (None, "", {}):
        return None
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_written_records(message: str | None, fallback: int = 0) -> int:
    text_value = str(message or "")
    marker = "写入 "
    if marker not in text_value:
        return fallback
    tail = text_value.rsplit(marker, 1)[-1]
    digits: list[str] = []
    for char in tail:
        if char.isdigit():
            digits.append(char)
        elif digits:
            break
    if not digits:
        return fallback
    try:
        return int("".join(digits))
    except ValueError:
        return fallback


def _build_backtest_table_stat(db: Session, *, data_type: str, table_name: str, date_column: str) -> BacktestDataStats | None:
    _validate_table_name(table_name)
    if not _relation_exists(db, table_name):
        return None

    if data_type == "daily_kline":
        return _build_daily_kline_stat(db, data_type=data_type, table_name=table_name, date_column=date_column)

    if data_type in {"minute_kline", "index_minute_kline"}:
        return _build_large_table_stat(db, data_type=data_type, table_name=table_name, date_column=date_column)

    normalized_symbol_expr = _normalized_symbol_sql("symbol") if data_type == "daily_kline" else "symbol"
    row = db.execute(text(f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT {normalized_symbol_expr}) AS symbol_count,
            COUNT(DISTINCT DATE({date_column})) AS trading_days,
            MIN(DATE({date_column})) AS date_range_start,
            MAX(DATE({date_column})) AS date_range_end
        FROM {table_name}
    """)).fetchone()
    db_stats = None if row is None else {
        "total_records": int(row.total_records or 0),
        "symbol_count": int(row.symbol_count or 0),
        "trading_days": int(row.trading_days or 0),
        "date_range_start": row.date_range_start,
        "date_range_end": row.date_range_end,
    }

    effective_stats = db_stats
    if effective_stats is None or int(effective_stats.get("total_records") or 0) <= 0:
        return None

    issues_score = 100

    last_table_updated_at = None
    if _table_has_column(db, table_name, "updated_at"):
        last_table_updated_at = db.execute(text(f"SELECT MAX(updated_at) FROM {table_name}")).scalar()
    elif _table_has_column(db, table_name, "created_at"):
        last_table_updated_at = db.execute(text(f"SELECT MAX(created_at) FROM {table_name}")).scalar()
    effective_last_updated_at = last_table_updated_at

    return BacktestDataStats(
        data_type=data_type,
        symbol=None,
        date_range_start=effective_stats.get("date_range_start"),
        date_range_end=effective_stats.get("date_range_end"),
        total_records=int(effective_stats.get("total_records") or 0),
        symbol_count=int(effective_stats.get("symbol_count") or 0),
        trading_days=int(effective_stats.get("trading_days") or 0),
        last_updated_date=effective_stats.get("date_range_end"),
        last_table_updated_at=effective_last_updated_at,
        coverage_source="postgresql",
        db_date_range_start=db_stats.get("date_range_start") if db_stats else None,
        db_date_range_end=db_stats.get("date_range_end") if db_stats else None,
        cache_date_range_start=None,
        cache_date_range_end=None,
        cache_last_updated_at=None,
        data_quality_score=max(int(issues_score), 0),
        missing_dates=[],
        created_at=effective_last_updated_at or datetime.utcnow(),
        updated_at=effective_last_updated_at or datetime.utcnow(),
    )


def _build_daily_kline_stat(db: Session, *, data_type: str, table_name: str, date_column: str) -> BacktestDataStats | None:
    cache_stats = get_daily_kline_parquet_stats()
    db_stats = _collect_large_table_stats(db, data_type=data_type, table_name=table_name, date_column=date_column)
    cache_end = _coerce_date(cache_stats.get("date_range_end")) if cache_stats else None
    db_end = _coerce_date(db_stats.get("date_range_end")) if db_stats else None

    if cache_stats and int(cache_stats.get("total_records") or 0) > 0 and (db_end is None or (cache_end and cache_end >= db_end)):
        cache_last_updated_at = cache_stats.get("last_table_updated_at")
        now = cache_last_updated_at or datetime.utcnow()
        return BacktestDataStats(
            data_type=data_type,
            symbol=None,
            date_range_start=cache_stats.get("date_range_start"),
            date_range_end=cache_stats.get("date_range_end"),
            total_records=int(cache_stats.get("total_records") or 0),
            symbol_count=int(cache_stats.get("symbol_count") or 0),
            trading_days=int(cache_stats.get("trading_days") or 0),
            last_updated_date=cache_stats.get("date_range_end"),
            last_table_updated_at=cache_last_updated_at,
            coverage_source="parquet_cache",
            db_date_range_start=db_stats.get("date_range_start") if db_stats else None,
            db_date_range_end=db_stats.get("date_range_end") if db_stats else None,
            cache_date_range_start=cache_stats.get("date_range_start"),
            cache_date_range_end=cache_stats.get("date_range_end"),
            cache_last_updated_at=cache_last_updated_at,
            data_quality_score=95,
            missing_dates=[],
            created_at=now,
            updated_at=now,
        )

    if db_stats is None or int(db_stats.get("total_records") or 0) <= 0:
        return None

    now = db_stats.get("last_table_updated_at") or datetime.utcnow()
    return BacktestDataStats(
        data_type=data_type,
        symbol=None,
        date_range_start=db_stats.get("date_range_start"),
        date_range_end=db_stats.get("date_range_end"),
        total_records=int(db_stats.get("total_records") or 0),
        symbol_count=int(db_stats.get("symbol_count") or 0) if db_stats.get("symbol_count") is not None else None,
        trading_days=int(db_stats.get("trading_days") or 0) if db_stats.get("trading_days") is not None else None,
        last_updated_date=db_stats.get("date_range_end"),
        last_table_updated_at=db_stats.get("last_table_updated_at"),
        coverage_source=db_stats.get("coverage_source") or "postgresql_fast",
        db_date_range_start=db_stats.get("date_range_start"),
        db_date_range_end=db_stats.get("date_range_end"),
        cache_date_range_start=cache_stats.get("date_range_start") if cache_stats else None,
        cache_date_range_end=cache_stats.get("date_range_end") if cache_stats else None,
        cache_last_updated_at=cache_stats.get("last_table_updated_at") if cache_stats else None,
        data_quality_score=95,
        missing_dates=[],
        created_at=now,
        updated_at=now,
    )


def _build_large_table_stat(db: Session, *, data_type: str, table_name: str, date_column: str) -> BacktestDataStats | None:
    stats = _collect_large_table_stats(db, data_type=data_type, table_name=table_name, date_column=date_column)
    if not stats or int(stats.get("total_records") or 0) <= 0:
        return None

    min_date = stats.get("date_range_start")
    max_date = stats.get("date_range_end")
    last_table_updated_at = stats.get("last_table_updated_at")
    now = last_table_updated_at or datetime.utcnow()
    quality_score = 80
    if data_type in {"minute_kline", "index_minute_kline"}:
        coverage = _latest_minute_coverage_snapshot(
            db,
            table_name=table_name,
            target_date=max_date,
            expected_min_symbols=_INDEX_MIN_SYMBOLS if data_type == "index_minute_kline" else _FULL_MARKET_MINUTE_MIN_SYMBOLS,
        )
        if coverage.get("complete"):
            quality_score = 95
            if stats.get("symbol_count") is None:
                stats["symbol_count"] = coverage.get("qualified_symbol_count")
        elif int(coverage.get("qualified_symbol_count") or 0) > 0:
            quality_score = 80
        else:
            quality_score = 60
        if stats.get("trading_days") is None:
            stats["trading_days"] = _estimate_cn_trading_days(min_date, max_date)

    return BacktestDataStats(
        data_type=data_type,
        symbol=None,
        date_range_start=min_date,
        date_range_end=max_date,
        total_records=int(stats.get("total_records") or 0),
        symbol_count=stats.get("symbol_count"),
        trading_days=stats.get("trading_days"),
        last_updated_date=max_date,
        last_table_updated_at=last_table_updated_at,
        coverage_source=stats.get("coverage_source") or "postgresql_fast",
        db_date_range_start=min_date,
        db_date_range_end=max_date,
        cache_date_range_start=None,
        cache_date_range_end=None,
        cache_last_updated_at=None,
        data_quality_score=quality_score,
        missing_dates=[],
        created_at=now,
        updated_at=now,
    )


def _collect_daily_kline_stats(db: Session, *, table_name: str, date_column: str) -> dict | None:
    base_stats = _collect_large_table_stats(
        db,
        data_type="daily_kline",
        table_name=table_name,
        date_column=date_column,
    )
    if not base_stats:
        return None
    return {
        **base_stats,
        "coverage_source": "postgresql_final_fast",
    }


def _collect_large_table_stats(db: Session, *, data_type: str, table_name: str, date_column: str) -> dict | None:
    if not _relation_exists(db, table_name):
        return None

    cached = _load_cached_data_stat(db, data_type=data_type)
    estimated_records = _estimate_table_rows(db, table_name)
    primary_key_records = _fast_primary_key_row_estimate(db, table_name)
    date_range = _fast_min_max_date_by_order(db, table_name=table_name, date_column=date_column)
    min_date = date_range[0] if date_range else None
    max_date = date_range[1] if date_range else None
    if cached:
        min_date = _min_date(min_date, cached.get("date_range_start"))
        max_date = _max_date(max_date, cached.get("date_range_end"))

    total_records = _choose_fast_total_records(
        cached_total=int(cached.get("total_records") or 0) if cached else 0,
        estimated_records=int(estimated_records or 0),
        primary_key_records=int(primary_key_records or 0),
        cached_end=_coerce_date(cached.get("date_range_end")) if cached else None,
        db_end=max_date,
        prefer_estimated=data_type in {"minute_kline", "index_minute_kline"},
    )

    if total_records <= 0 and (min_date is None or max_date is None):
        return None

    last_table_updated_at = _fast_latest_timestamp_by_primary_key(db, table_name)
    cached_updated_at = cached.get("updated_at") if cached else None
    last_table_updated_at = _max_datetime(last_table_updated_at, cached_updated_at)

    symbol_count = None
    trading_days = None
    if total_records and total_records <= _FAST_STATS_EXACT_ROW_THRESHOLD:
        light_stats = _aggregate_table_stats(db, table_name=table_name, date_column=date_column)
        if light_stats:
            total_records = int(light_stats.get("total_records") or 0)
            min_date = _min_date(min_date, light_stats.get("date_range_start"))
            max_date = _max_date(max_date, light_stats.get("date_range_end"))
            symbol_count = light_stats.get("symbol_count")
            trading_days = light_stats.get("trading_days")
            last_table_updated_at = _max_datetime(last_table_updated_at, light_stats.get("last_table_updated_at"))

    return {
        "total_records": int(total_records or 0),
        "symbol_count": symbol_count,
        "trading_days": trading_days,
        "date_range_start": min_date,
        "date_range_end": max_date,
        "last_table_updated_at": last_table_updated_at,
        "coverage_source": "postgresql_fast",
    }


def _choose_fast_total_records(
    *,
    cached_total: int,
    estimated_records: int,
    primary_key_records: int,
    cached_end: date | None,
    db_end: date | None,
    prefer_estimated: bool = False,
) -> int:
    if prefer_estimated and estimated_records > 0:
        return int(max(estimated_records, cached_total or 0))
    if cached_total > 0 and cached_end and db_end and cached_end >= db_end:
        return int(cached_total)
    if estimated_records >= max(cached_total // 2, 1_000_000):
        return int(estimated_records)
    if cached_total > 0 and cached_end and db_end and cached_end >= db_end - timedelta(days=7):
        return int(cached_total)
    if cached_total > 0 and primary_key_records <= 0:
        return int(cached_total)
    if primary_key_records > 0:
        return int(primary_key_records)
    return max(int(cached_total or 0), int(estimated_records or 0))


def _latest_minute_coverage_snapshot(
    db: Session,
    *,
    table_name: str,
    target_date: date | None,
    expected_min_symbols: int,
) -> dict[str, Any]:
    _validate_table_name(table_name)
    if target_date is None or not _relation_exists(db, table_name):
        return {"complete": False, "qualified_symbol_count": 0, "expected_min": expected_min_symbols}
    start_time = datetime.combine(target_date, datetime.min.time())
    end_time = start_time + timedelta(days=1)
    row = db.execute(text(f"""
        SELECT COUNT(*) AS qualified_symbol_count
        FROM (
            SELECT symbol, COUNT(*) AS bar_count
            FROM {table_name}
            WHERE trade_time >= :start_time
              AND trade_time < :end_time
            GROUP BY symbol
            HAVING COUNT(*) >= :min_bars
        ) t
    """), {
        "start_time": start_time,
        "end_time": end_time,
        "min_bars": _MINUTE_MIN_BARS,
    }).fetchone()
    qualified = int(row.qualified_symbol_count or 0) if row else 0
    expected = max(int(expected_min_symbols or 1), 1)
    return {
        "complete": qualified >= expected,
        "qualified_symbol_count": qualified,
        "expected_min": expected,
        "min_bars": _MINUTE_MIN_BARS,
    }


def _estimate_cn_trading_days(start: date | None, end: date | None) -> int | None:
    if start is None or end is None or start > end:
        return None
    current = start
    total = 0
    while current <= end:
        try:
            if is_cn_trading_day(current.isoformat()):
                total += 1
        except Exception:
            if current.weekday() < 5:
                total += 1
        current += timedelta(days=1)
    return total


def _load_cached_data_stat(db: Session, *, data_type: str) -> dict | None:
    if not _relation_exists(db, "backtest_data_stats"):
        return None
    row = db.execute(text("""
        SELECT total_records, date_range_start, date_range_end, last_updated_date, updated_at
        FROM backtest_data_stats
        WHERE data_type = :data_type
          AND (symbol IS NULL OR symbol = '')
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
    """), {"data_type": data_type}).mappings().first()
    return dict(row) if row else None


def _aggregate_table_stats(db: Session, *, table_name: str, date_column: str) -> dict | None:
    _validate_table_name(table_name)
    if not _relation_exists(db, table_name):
        return None
    row = db.execute(text(f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT symbol) AS symbol_count,
            COUNT(DISTINCT DATE({date_column})) AS trading_days,
            MIN(DATE({date_column})) AS date_range_start,
            MAX(DATE({date_column})) AS date_range_end
        FROM {table_name}
    """)).fetchone()
    if row is None or int(row.total_records or 0) <= 0:
        return None
    return {
        "total_records": int(row.total_records or 0),
        "symbol_count": int(row.symbol_count or 0),
        "trading_days": int(row.trading_days or 0),
        "date_range_start": row.date_range_start,
        "date_range_end": row.date_range_end,
        "last_table_updated_at": _latest_table_timestamp(db, table_name),
    }


def _table_has_column(db: Session, table_name: str, column_name: str) -> bool:
    return bool(db.execute(text("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :table_name
          AND column_name = :column_name
    """), {
        "table_name": table_name,
        "column_name": column_name,
    }).scalar())


def _estimate_table_rows(db: Session, table_name: str) -> int:
    if not _relation_exists(db, table_name):
        return 0
    return int(db.execute(text("""
        SELECT GREATEST(
            COALESCE((
                SELECT n_live_tup::bigint
                FROM pg_stat_user_tables
                WHERE schemaname = ANY(current_schemas(false))
                  AND relname = :table_name
            ), 0),
            COALESCE((
                SELECT reltuples::bigint
                FROM pg_class
                WHERE oid = to_regclass(:table_name)
            ), 0),
            0
        )
    """), {
        "table_name": table_name,
    }).scalar() or 0)


def _fast_primary_key_row_estimate(db: Session, table_name: str) -> int:
    if not _relation_exists(db, table_name) or not _table_has_column(db, table_name, "id"):
        return 0
    value = db.execute(text(f"""
        SELECT id
        FROM {table_name}
        ORDER BY id DESC
        LIMIT 1
    """)).scalar()
    return int(value or 0)


def _estimate_is_small(row_estimate: int | None, *, threshold: int = 5_000_000) -> bool:
    return int(row_estimate or 0) <= threshold


def _relation_exists(db: Session, table_name: str) -> bool:
    return bool(db.execute(text("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = :table_name
    """), {"table_name": table_name}).scalar())


def _count_table_rows(db: Session, table_name: str) -> int:
    if not _relation_exists(db, table_name):
        return 0
    return int(db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() or 0)


def _fast_min_max_date(db: Session, table_name: str, date_column: str) -> tuple[date | None, date | None] | None:
    if not _relation_exists(db, table_name):
        return None
    row = db.execute(text(f"""
        SELECT MIN({date_column}) AS min_value, MAX({date_column}) AS max_value
        FROM {table_name}
    """)).fetchone()
    if row is None:
        return None
    return _coerce_date(row.min_value), _coerce_date(row.max_value)


def _fast_min_max_date_by_order(db: Session, *, table_name: str, date_column: str) -> tuple[date | None, date | None] | None:
    if not _relation_exists(db, table_name):
        return None
    min_value = db.execute(text(f"""
        SELECT {date_column}
        FROM {table_name}
        WHERE {date_column} IS NOT NULL
        ORDER BY {date_column} ASC
        LIMIT 1
    """)).scalar()
    max_value = db.execute(text(f"""
        SELECT {date_column}
        FROM {table_name}
        WHERE {date_column} IS NOT NULL
        ORDER BY {date_column} DESC
        LIMIT 1
    """)).scalar()
    return _coerce_date(min_value), _coerce_date(max_value)


def _latest_table_timestamp(db: Session, table_name: str) -> datetime | None:
    if _table_has_column(db, table_name, "updated_at"):
        return db.execute(text(f"SELECT MAX(updated_at) FROM {table_name}")).scalar()
    if _table_has_column(db, table_name, "created_at"):
        return db.execute(text(f"SELECT MAX(created_at) FROM {table_name}")).scalar()
    return None


def _fast_latest_timestamp_by_primary_key(db: Session, table_name: str) -> datetime | None:
    if not _relation_exists(db, table_name) or not _table_has_column(db, table_name, "id"):
        return None
    if _table_has_column(db, table_name, "updated_at"):
        column = "updated_at"
    elif _table_has_column(db, table_name, "created_at"):
        column = "created_at"
    else:
        return None
    return db.execute(text(f"""
        SELECT {column}
        FROM {table_name}
        WHERE {column} IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
    """)).scalar()


def _count_distinct_dates_after(db: Session, *, table_name: str, date_column: str, after_date: date) -> int:
    if not _relation_exists(db, table_name):
        return 0
    return int(db.execute(text(f"""
        SELECT COUNT(DISTINCT DATE({date_column}))
        FROM {table_name}
        WHERE DATE({date_column}) > :after_date
    """), {"after_date": after_date}).scalar() or 0)


def _coerce_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _min_date(*values) -> date | None:
    dates = [_coerce_date(value) for value in values if _coerce_date(value) is not None]
    return min(dates) if dates else None


def _max_date(*values) -> date | None:
    dates = [_coerce_date(value) for value in values if _coerce_date(value) is not None]
    return max(dates) if dates else None


def _max_datetime(*values) -> datetime | None:
    datetimes = [value for value in values if isinstance(value, datetime)]
    return max(datetimes) if datetimes else None


def _daily_kline_calendar_source_tables(db: Session) -> list[str]:
    return [table_name for table_name in _DAILY_KLINE_CALENDAR_TABLES if _relation_exists(db, table_name)]


def _daily_kline_calendar_min_max(db: Session) -> tuple[date | None, date | None, list[str]]:
    source_tables = _daily_kline_calendar_source_tables(db)
    min_values: list[date] = []
    max_values: list[date] = []
    for table_name in source_tables:
        date_range = _fast_min_max_date(db, table_name, "trade_date")
        if not date_range:
            continue
        min_value, max_value = date_range
        if min_value:
            min_values.append(min_value)
        if max_value:
            max_values.append(max_value)
    if min_values or max_values:
        return (min(min_values) if min_values else None, max(max_values) if max_values else None, source_tables)

    fallback_table = preferred_daily_kline_table()
    if fallback_table not in source_tables and _relation_exists(db, fallback_table):
        date_range = _fast_min_max_date(db, fallback_table, "trade_date")
        if date_range:
            return date_range[0], date_range[1], [fallback_table]
    return None, None, source_tables


def _daily_kline_calendar_is_rest_day(value: date) -> bool:
    try:
        return not is_cn_trading_day(value.isoformat())
    except Exception:
        return value.weekday() >= 5


def _daily_kline_calendar_rows(
    db: Session,
    *,
    start_date: date,
    end_date: date,
    source_tables: list[str],
) -> list[Any]:
    physical_tables = [table_name for table_name in source_tables if table_name in _DAILY_KLINE_CALENDAR_TABLES]
    if physical_tables:
        branches: list[str] = []
        for index, table_name in enumerate(physical_tables):
            normalized_symbol_expr = _normalized_symbol_sql("symbol")
            branches.append(f"""
                SELECT trade_date, symbol_key
                FROM (
                    SELECT
                        trade_date::date AS trade_date,
                        {normalized_symbol_expr} AS symbol_key
                    FROM {table_name}
                    WHERE trade_date >= :start_date
                      AND trade_date < :end_date
                      AND symbol IS NOT NULL
                ) daily_calendar_{index}
                WHERE symbol_key <> ''
            """)
        return db.execute(text(f"""
            WITH daily_symbols AS (
                {" UNION ".join(branches)}
            )
            SELECT
                trade_date,
                COUNT(*) AS row_count,
                COUNT(*) AS symbol_count
            FROM daily_symbols
            GROUP BY trade_date
            ORDER BY trade_date
        """), {
            "start_date": start_date,
            "end_date": end_date,
        }).fetchall()

    table_name = preferred_daily_kline_table()
    normalized_symbol_expr = _normalized_symbol_sql("symbol")
    return db.execute(text(f"""
        SELECT
            trade_date::date AS trade_date,
            COUNT(*) AS row_count,
            COUNT(DISTINCT {normalized_symbol_expr}) AS symbol_count
        FROM {table_name}
        WHERE trade_date >= :start_date
          AND trade_date < :end_date
        GROUP BY trade_date::date
        ORDER BY trade_date::date
    """), {
        "start_date": start_date,
        "end_date": end_date,
    }).fetchall()


def _load_daily_kline_frame_for_cache(
    db: Session,
    *,
    start_date: date,
    end_date: date,
    symbols: Optional[list[str]] = None,
) -> pd.DataFrame | None:
    table_name = preferred_daily_kline_table()
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }
    if symbols:
        query = text("""
            SELECT symbol, trade_date AS date, open, high, low, close, volume, amount,
                   turnover_rate, pre_close, float_market_cap, total_market_cap, net_profit_ttm
            FROM {table_name}
            WHERE trade_date >= :start_date
              AND trade_date <= :end_date
              AND symbol IN :symbols
            ORDER BY trade_date, symbol
        """.format(table_name=table_name)).bindparams(bindparam("symbols", expanding=True))
        params["symbols"] = symbols
    else:
        query = text("""
            SELECT symbol, trade_date AS date, open, high, low, close, volume, amount,
                   turnover_rate, pre_close, float_market_cap, total_market_cap, net_profit_ttm
            FROM {table_name}
            WHERE trade_date >= :start_date
              AND trade_date <= :end_date
            ORDER BY trade_date, symbol
        """.format(table_name=table_name))
    rows = db.execute(query, params).mappings().all()
    if not rows:
        return None
    return pd.DataFrame(rows)


def _refresh_daily_kline_cache_from_db(
    db: Session,
    *,
    start_date: date | None,
    end_date: date | None,
    symbols: Optional[list[str]] = None,
) -> dict:
    if start_date is None or end_date is None or start_date > end_date:
        return {"updated": False, "written_paths": None, "records": 0}
    frame = _load_daily_kline_frame_for_cache(db, start_date=start_date, end_date=end_date, symbols=symbols)
    if frame is None or frame.empty:
        return {"updated": False, "written_paths": None, "records": 0}
    written_paths = write_daily_kline_parquet_cache(frame)
    return {
        "updated": bool(written_paths),
        "written_paths": written_paths,
        "records": int(len(frame)),
        "date_range_start": start_date,
        "date_range_end": end_date,
    }


def _get_actual_table_coverage(db: Session, *, task_type: str) -> dict | None:
    mapping = _TABLE_STATS_MAPPING.get(task_type)
    if not mapping:
        return None
    table_name, date_column = mapping
    if not _relation_exists(db, table_name):
        return None
    if task_type == "daily_kline":
        stats = _collect_daily_kline_stats(db, table_name=table_name, date_column=date_column)
        if not stats:
            return None
        return {
            "total_records": int(stats.get("total_records") or 0),
            "date_range_start": stats.get("date_range_start"),
            "date_range_end": stats.get("date_range_end"),
        }
    if task_type in {"minute_kline", "index_minute_kline"}:
        date_range = _fast_min_max_date(db, table_name, date_column)
        if date_range is None:
            return None
        return {
            "total_records": _estimate_table_rows(db, table_name),
            "date_range_start": date_range[0],
            "date_range_end": date_range[1],
        }
    row = db.execute(text(f"""
        SELECT
            COUNT(*) AS total_records,
            MIN(DATE({date_column})) AS date_range_start,
            MAX(DATE({date_column})) AS date_range_end
        FROM {table_name}
    """)).fetchone()
    if row is None:
        return None
    return {
        "total_records": int(row.total_records or 0),
        "date_range_start": row.date_range_start,
        "date_range_end": row.date_range_end,
    }


def _parse_optional_date(value) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _normalized_symbol_sql(column_name: str = "symbol") -> str:
    return (
        f"regexp_replace("
        f"regexp_replace(upper(trim({column_name})), '^(SH|SZ|BJ)', ''), "
        f"'\\.(SH|SZ|BJ)$', ''"
        f")"
    )

# 数据源兼容性映射
DATA_SOURCE_COMPATIBILITY = {
    'daily_kline': ['tdx', 'quantclass', 'tencent', 'akshare', 'baostock'],
    'minute_kline': ['tdx', 'qmt', 'akshare'],
    'index_data': ['tdx', 'qmt', 'akshare', 'quantclass', 'baostock', 'tushare', 'eastmoney'],
    'index_minute_kline': ['tdx', 'qmt', 'akshare'],
    'chip_data': ['quantclass'],  # 只有量化课堂支持
    'financial_data': ['quantclass'],  # 只有量化课堂支持
    'research_reports': ['eastmoney']  # 只有东方财富支持
}

# ========== 数据下载任务API ==========

@router.post("/tasks", response_model=BacktestDataTask)
def create_backtest_data_task(
    task: BacktestDataTaskCreate,
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """创建回测数据下载任务"""
    try:
        # 插入任务记录
        query = text("""
            INSERT INTO backtest_data_tasks 
            (user_id, task_type, data_source, date_range_start, date_range_end, symbols, status, task_scope)
            VALUES (:user_id, :task_type, :data_source, :date_range_start, :date_range_end, :symbols, 'pending', 'primary')
            RETURNING id
        """)
        
        result = db.execute(query, {
            "user_id": current_user.id,
            "task_type": task.task_type,
            "data_source": task.data_source or DEFAULT_MARKET_CLOSE_DATA_SOURCE,
            "date_range_start": task.date_range_start,
            "date_range_end": task.date_range_end,
            "symbols": task.symbols or []
        })
        task_id = result.fetchone()[0]
        db.commit()
        
        # 获取创建的任务
        task_query = text("""
            SELECT * FROM backtest_data_tasks WHERE id = :task_id
        """)
        task_result = db.execute(task_query, {"task_id": task_id})
        row = task_result.fetchone()
        
        return BacktestDataTask(
            id=row.id,
            user_id=row.user_id,
            task_type=row.task_type,
            data_source=row.data_source,
            date_range_start=row.date_range_start,
            date_range_end=row.date_range_end,
            symbols=row.symbols or [],
            status=row.status,
            progress=row.progress or 0,
            total_records=row.total_records or 0,
            downloaded_records=row.downloaded_records or 0,
            error_message=row.error_message,
            created_at=row.created_at,
            updated_at=row.updated_at,
            completed_at=row.completed_at
        )
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"创建任务失败: {str(e)}")


@router.get("/tasks", response_model=BacktestDataTaskListResponse)
def list_backtest_data_tasks(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    """获取回测数据下载任务列表"""
    try:
        # 获取任务总数
        count_query = text("""
            SELECT COUNT(*) FROM backtest_data_tasks WHERE user_id = :user_id
        """)
        count_result = db.execute(count_query, {"user_id": current_user.id})
        total = count_result.fetchone()[0]
        
        # 获取任务列表
        tasks_query = text("""
            SELECT * FROM backtest_data_tasks 
            WHERE user_id = :user_id 
            ORDER BY created_at DESC 
            LIMIT :limit OFFSET :skip
        """)
        tasks_result = db.execute(tasks_query, {
            "user_id": current_user.id,
            "limit": limit,
            "skip": skip
        })
        
        tasks = []
        for row in tasks_result:
            tasks.append(BacktestDataTask(
                id=row.id,
                user_id=row.user_id,
                task_type=row.task_type,
                data_source=row.data_source,
                date_range_start=row.date_range_start,
                date_range_end=row.date_range_end,
                symbols=row.symbols or [],
                status=row.status,
                progress=row.progress or 0,
                total_records=row.total_records or 0,
                downloaded_records=row.downloaded_records or 0,
                error_message=row.error_message,
                created_at=row.created_at,
                updated_at=row.updated_at,
                completed_at=row.completed_at
            ))
        
        return BacktestDataTaskListResponse(tasks=tasks, total=total)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务列表失败: {str(e)}")


@router.get("/tasks/{task_id}", response_model=BacktestDataTask)
def get_backtest_data_task(
    task_id: int,
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取单个回测数据下载任务"""
    try:
        query = text("""
            SELECT * FROM backtest_data_tasks 
            WHERE id = :task_id AND user_id = :user_id
        """)
        result = db.execute(query, {"task_id": task_id, "user_id": current_user.id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return BacktestDataTask(
            id=row.id,
            user_id=row.user_id,
            task_type=row.task_type,
            data_source=row.data_source,
            date_range_start=row.date_range_start,
            date_range_end=row.date_range_end,
            symbols=row.symbols or [],
            status=row.status,
            progress=row.progress or 0,
            total_records=row.total_records or 0,
            downloaded_records=row.downloaded_records or 0,
            error_message=row.error_message,
            created_at=row.created_at,
            updated_at=row.updated_at,
            completed_at=row.completed_at
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取任务失败: {str(e)}")


# ========== 数据配置API ==========

@router.post("/configs", response_model=BacktestDataConfig)
def create_backtest_data_config(
    payload: dict = Body(...),
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """创建或更新回测数据配置"""
    try:
        normalized = _normalize_config_payload(payload)
        try:
            BacktestDataConfigCreate(**normalized)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"配置参数无效: {exc}") from exc

        existing_query = text("""
            SELECT * FROM backtest_data_configs
            WHERE user_id = :user_id AND config_name = :config_name
        """)
        existing = db.execute(existing_query, {
            "user_id": current_user.id,
            "config_name": normalized["config_name"],
        }).fetchone()

        if existing:
            db.execute(text("""
                UPDATE backtest_data_configs
                SET enabled_data_types = :enabled_data_types,
                    default_date_range_days = :default_date_range_days,
                    default_symbols = :default_symbols,
                    data_source_preference = :data_source_preference,
                    auto_download = :auto_download,
                    update_frequency = :update_frequency,
                    schedule_time = :schedule_time,
                    timezone = :timezone,
                    only_trading_day = :only_trading_day,
                    daily_kline_policy = :daily_kline_policy,
                    minute_kline_policy = :minute_kline_policy,
                    updated_at = NOW()
                WHERE id = :config_id
            """), {
                "config_id": existing.id,
                **normalized,
                "daily_kline_policy": json.dumps(normalized.get("daily_kline_policy")) if normalized.get("daily_kline_policy") is not None else None,
                "minute_kline_policy": json.dumps(normalized.get("minute_kline_policy")) if normalized.get("minute_kline_policy") is not None else None,
            })
            config_id = existing.id
        else:
            result = db.execute(text("""
                INSERT INTO backtest_data_configs
                (user_id, config_name, enabled_data_types, default_date_range_days,
                 default_symbols, data_source_preference, auto_download, update_frequency,
                 schedule_time, timezone, only_trading_day, daily_kline_policy, minute_kline_policy)
                VALUES (:user_id, :config_name, :enabled_data_types, :default_date_range_days,
                        :default_symbols, :data_source_preference, :auto_download, :update_frequency,
                        :schedule_time, :timezone, :only_trading_day, :daily_kline_policy, :minute_kline_policy)
                RETURNING id
            """), {
                "user_id": current_user.id,
                **normalized,
                "daily_kline_policy": json.dumps(normalized.get("daily_kline_policy")) if normalized.get("daily_kline_policy") is not None else None,
                "minute_kline_policy": json.dumps(normalized.get("minute_kline_policy")) if normalized.get("minute_kline_policy") is not None else None,
            })
            config_id = result.fetchone()[0]
        db.commit()

        config_query = text("""
            SELECT * FROM backtest_data_configs WHERE id = :config_id
        """)
        config_result = db.execute(config_query, {"config_id": config_id})
        row = config_result.fetchone()
        config = _row_to_backtest_config(row)
        config.subscription_status = backtest_data_auto_update_service.get_config_status(int(row.id), user_id=str(current_user.id))
        return config
    except Exception as e:
        db.rollback()
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"创建配置失败: {str(e)}")


@router.get("/configs", response_model=BacktestDataConfigListResponse)
def list_backtest_data_configs(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取回测数据配置列表"""
    try:
        # 获取配置列表
        query = text("""
            SELECT * FROM backtest_data_configs 
            WHERE user_id = :user_id 
            ORDER BY created_at DESC
        """)
        result = db.execute(query, {"user_id": current_user.id})
        
        configs = []
        for row in result:
            config = _row_to_backtest_config(row)
            try:
                config.subscription_status = backtest_data_auto_update_service.get_config_status(int(row.id), user_id=str(current_user.id))
            except Exception:
                config.subscription_status = None
            configs.append(config)
        
        return BacktestDataConfigListResponse(configs=configs, total=len(configs))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取配置列表失败: {str(e)}")


@router.get("/configs/{config_id}/subscription-status", response_model=BacktestDataSubscriptionStatus)
def get_backtest_data_subscription_status(
    config_id: int,
    current_user: UserDB = Depends(get_current_user),
):
    """获取订阅配置执行状态、水位与下次执行时间"""
    try:
        payload = backtest_data_auto_update_service.get_config_status(config_id, user_id=str(current_user.id))
        return BacktestDataSubscriptionStatus(**payload)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"获取订阅状态失败: {exc}") from exc


@router.post("/configs/{config_id}/run")
def run_backtest_data_subscription_now(
    config_id: int,
    current_user: UserDB = Depends(get_current_user),
):
    """立即按当前订阅配置执行一次增量下载"""
    try:
        status = backtest_data_auto_update_service.get_config_status(config_id, user_id=str(current_user.id))
        del status
        task_ids = backtest_data_auto_update_service.trigger_config_now(config_id)
        return {
            "message": "订阅执行已触发" if task_ids else "当前没有可执行的增量任务",
            "config_id": config_id,
            "task_ids": task_ids,
            "created_count": len(task_ids),
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"立即执行订阅失败: {exc}") from exc


# ========== 数据统计API ==========

@router.get("/stats", response_model=BacktestDataStatsListResponse)
def get_backtest_data_stats(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取回测数据统计"""
    try:
        stats: list[BacktestDataStats] = []
        for data_type, (table_name, date_column) in _TABLE_STATS_MAPPING.items():
            item = _build_backtest_table_stat(db, data_type=data_type, table_name=table_name, date_column=date_column)
            if item is not None:
                stats.append(item)
        return BacktestDataStatsListResponse(stats=stats, total=len(stats))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据统计失败: {str(e)}")


@router.get("/daily-kline/coverage-calendar")
def get_daily_kline_coverage_calendar(
    year: Optional[int] = None,
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """返回股票日K线按年的月度覆盖视图。"""
    try:
        del current_user
        min_date, max_date, source_tables = _daily_kline_calendar_min_max(db)
        if min_date is None or max_date is None:
            raise HTTPException(status_code=404, detail="数据库 stock_daily_kline 暂无可展示数据")

        min_year = int(min_date.year)
        max_year = int(max_date.year)
        selected_year = int(year or max_year)
        if selected_year < min_year or selected_year > max_year:
            raise HTTPException(status_code=400, detail=f"年份超出范围：{min_year} - {max_year}")

        start_date = date(selected_year, 1, 1)
        end_date = date(selected_year + 1, 1, 1)
        rows = _daily_kline_calendar_rows(
            db,
            start_date=start_date,
            end_date=end_date,
            source_tables=source_tables,
        )

        coverage_by_date = {
            row.trade_date: {
                "row_count": int(row.row_count or 0),
                "symbol_count": int(row.symbol_count or 0),
            }
            for row in rows
        }

        months: list[dict[str, object]] = []
        total_days_with_data = 0
        for month in range(1, 13):
            _, days_in_month = month_calendar.monthrange(selected_year, month)
            month_days: list[dict[str, object]] = []
            days_with_data = 0
            for day in range(1, days_in_month + 1):
                current_date = date(selected_year, month, day)
                payload = coverage_by_date.get(current_date)
                has_data = payload is not None and int(payload.get("symbol_count") or 0) > 0
                is_rest_day = _daily_kline_calendar_is_rest_day(current_date)
                if has_data:
                    days_with_data += 1
                month_days.append({
                    "date": current_date.isoformat(),
                    "day": day,
                    "weekday": int(current_date.weekday()),
                    "has_data": has_data,
                    "is_trading_day": not is_rest_day,
                    "is_rest_day": is_rest_day,
                    "symbol_count": int(payload.get("symbol_count") or 0) if payload else 0,
                    "row_count": int(payload.get("row_count") or 0) if payload else 0,
                })
            total_days_with_data += days_with_data
            months.append({
                "month": month,
                "days": month_days,
                "days_in_month": days_in_month,
                "days_with_data": days_with_data,
            })

        return {
            "data_type": "daily_kline",
            "year": selected_year,
            "min_year": min_year,
            "max_year": max_year,
            "available_years": list(range(min_year, max_year + 1)),
            "total_days_with_data": total_days_with_data,
            "source_tables": source_tables,
            "months": months,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"获取日K覆盖日历失败: {exc}") from exc


@router.post("/daily-kline/cache-sync")
def sync_daily_kline_parquet_cache(
    payload: Optional[dict] = Body(None),
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """将 PostgreSQL 的 stock_daily_kline 同步到日线 Parquet 回测缓存。"""
    try:
        payload = payload or {}
        force_full = bool(payload.get("force_full", False))
        start_date = _parse_optional_date(payload.get("start_date"))
        end_date = _parse_optional_date(payload.get("end_date"))

        db_coverage = _get_actual_table_coverage(db, task_type="daily_kline")
        if not db_coverage or int(db_coverage.get("total_records") or 0) <= 0:
            raise HTTPException(status_code=404, detail="数据库 stock_daily_kline 暂无可同步数据")

        cache_before = get_daily_kline_parquet_stats()
        db_start = db_coverage.get("date_range_start")
        db_end = db_coverage.get("date_range_end")
        if db_start is None or db_end is None:
            raise HTTPException(status_code=404, detail="数据库 stock_daily_kline 缺少有效日期区间")

        if start_date is None or end_date is None:
            if force_full or cache_before is None or not cache_before.get("date_range_end"):
                start_date = db_start
                end_date = db_end
            else:
                cache_end = cache_before.get("date_range_end")
                if cache_end >= db_end:
                    return {
                        "success": True,
                        "message": "日线 Parquet 缓存已与数据库一致，无需同步",
                        "synced": False,
                        "db_coverage": db_coverage,
                        "cache_before": cache_before,
                        "cache_after": cache_before,
                    }
                start_date = cache_end + timedelta(days=1)
                end_date = db_end

        if start_date > end_date:
            raise HTTPException(status_code=400, detail="同步开始日期不能晚于结束日期")

        result = _refresh_daily_kline_cache_from_db(
            db,
            start_date=start_date,
            end_date=end_date,
        )
        cache_after = get_daily_kline_parquet_stats()
        quality_manager = DataQualityManager()
        quality_result = quality_manager.validate_database_integrity(db, "stock_daily_kline", "daily_kline")
        return {
            "success": True,
            "message": "日线 Parquet 缓存同步完成" if result.get("updated") else "未发现可写入的日线数据",
            "synced": bool(result.get("updated")),
            "sync_range": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "result": result,
            "db_coverage": db_coverage,
            "cache_before": cache_before,
            "cache_after": cache_after,
            "quality": {
                "valid": quality_result.get("valid"),
                "issues": quality_result.get("issues", []),
                "stats": quality_result.get("stats", {}),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步日线 Parquet 缓存失败: {str(e)}")


@router.get("/daily-kline/governance-summary")
def get_daily_kline_governance_summary(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """返回股票日 K 多源治理摘要，用于设置页解释最终表与过程层口径。"""
    try:
        del current_user
        preferred_table = preferred_daily_kline_table()
        final_table = _daily_governance_table_stats(db, preferred_table)
        published = _daily_governance_table_stats(db, "pub_stock_daily_kline")
        norm = _daily_governance_table_stats(db, "norm_stock_daily_kline")
        final_summary = _serialize_governance_stats({
            **final_table,
            "table_name": preferred_table,
            "layer": "final_table",
            "description": "最终业务表，吸收治理链路发布后的日 K 数据",
        }) if final_table.get("exists") else _serialize_governance_stats(
            {
                "table_name": preferred_table,
                "layer": "final_table",
                "description": "最终业务表，吸收治理链路发布后的日 K 数据",
                "exists": _relation_exists(db, preferred_table),
                "total_records": 0,
            }
        )

        raw_layers = []
        for source, table_name in DAILY_RAW_TABLES.items():
            payload = _daily_governance_table_stats(db, table_name)
            payload["source"] = source
            payload["layer"] = "raw"
            raw_layers.append(payload)

        latest_runs = _daily_reconciliation_runs(db)
        latest_run_id = str(latest_runs[0]["run_id"]) if latest_runs else None
        return {
            "success": True,
            "updated_at": datetime.utcnow().isoformat(),
            "preferred_table": preferred_table,
            "read_policy": "业务侧统一读取 stock_daily_kline 最终业务表；raw/norm/pub/对账表仅用于采集、标准化、审计和质量追踪。",
            "unified": final_summary,
            "legacy": final_table,
            "published": published,
            "norm": norm,
            "raw_layers": raw_layers,
            "source_summary": _daily_publish_source_summary(db),
            "latest_reconciliation_runs": latest_runs,
            "latest_reconciliation_item_summary": _daily_reconciliation_item_summary(db, latest_run_id),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"获取日K多源治理摘要失败: {exc}") from exc


def _serialize_governance_stats(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    for key in ["date_range_start", "date_range_end", "last_table_updated_at", "updated_at"]:
        value = result.get(key)
        if hasattr(value, "isoformat"):
            result[key] = value.isoformat()
    result["exists"] = bool(result.get("exists", True))
    result["total_records"] = int(result.get("total_records") or 0)
    result["symbol_count"] = int(result.get("symbol_count") or 0)
    result["trading_days"] = int(result.get("trading_days") or 0)
    return result


def _daily_governance_table_stats(db: Session, table_name: str) -> dict[str, Any]:
    _validate_table_name(table_name)
    exists = _relation_exists(db, table_name)
    if not exists:
        return {
            "table_name": table_name,
            "exists": False,
            "total_records": 0,
            "symbol_count": 0,
            "trading_days": 0,
            "date_range_start": None,
            "date_range_end": None,
            "latest_date_row_count": 0,
        }
    if table_name == preferred_daily_kline_table():
        stats = _collect_daily_kline_stats(db, table_name=table_name, date_column="trade_date") or {}
    else:
        stats = _aggregate_table_stats(db, table_name=table_name, date_column="trade_date") or {}
    latest_date = stats.get("date_range_end")
    payload = _serialize_governance_stats({
        "table_name": table_name,
        "exists": True,
        **stats,
        **_latest_daily_status_fields(db, table_name=table_name, latest_date=latest_date),
    })
    payload["latest_date_row_count"] = _count_rows_on_date(
        db,
        table_name=table_name,
        date_column="trade_date",
        trade_date=latest_date,
    ) if latest_date else 0
    return payload


def _latest_daily_status_fields(db: Session, *, table_name: str, latest_date: date | None) -> dict[str, Any]:
    if latest_date is None:
        return {}
    columns = {
        row.column_name
        for row in db.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
              AND column_name IN ('source', 'quality_status', 'publish_status')
        """), {"table_name": table_name}).fetchall()
    }
    if not {"source", "quality_status", "publish_status"}.issubset(columns):
        return {}
    row = db.execute(text(f"""
        SELECT
            source,
            quality_status,
            publish_status,
            COUNT(*) AS row_count
        FROM {table_name}
        WHERE trade_date = :latest_date
        GROUP BY source, quality_status, publish_status
        ORDER BY row_count DESC
        LIMIT 1
    """), {"latest_date": latest_date}).mappings().first()
    if not row:
        return {}
    return {
        "source": str(row.get("source") or ""),
        "quality_status": str(row.get("quality_status") or ""),
        "publish_status": str(row.get("publish_status") or ""),
    }


def _count_rows_on_date(db: Session, *, table_name: str, date_column: str, trade_date: date | None) -> int:
    if trade_date is None or not _relation_exists(db, table_name):
        return 0
    return int(db.execute(text(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE {date_column} = :trade_date
    """), {"trade_date": trade_date}).scalar() or 0)


def _daily_publish_source_summary(db: Session) -> list[dict[str, Any]]:
    if not _relation_exists(db, "pub_stock_daily_kline"):
        return []
    rows = db.execute(text("""
        WITH latest AS (
            SELECT MAX(trade_date) AS latest_date
            FROM pub_stock_daily_kline
        )
        SELECT
            source,
            quality_status,
            publish_status,
            COUNT(*) AS total_records,
            MIN(trade_date) AS date_range_start,
            MAX(trade_date) AS date_range_end,
            COUNT(*) FILTER (WHERE trade_date = (SELECT latest_date FROM latest)) AS latest_date_row_count,
            MAX(updated_at) AS updated_at
        FROM pub_stock_daily_kline
        GROUP BY source, quality_status, publish_status
        ORDER BY date_range_end DESC, total_records DESC
    """)).mappings().all()
    return [
        _serialize_governance_stats({
            "source": str(row.get("source") or ""),
            "quality_status": str(row.get("quality_status") or ""),
            "publish_status": str(row.get("publish_status") or ""),
            "total_records": int(row.get("total_records") or 0),
            "date_range_start": row.get("date_range_start"),
            "date_range_end": row.get("date_range_end"),
            "latest_date_row_count": int(row.get("latest_date_row_count") or 0),
            "updated_at": row.get("updated_at"),
        })
        for row in rows
    ]


def _daily_reconciliation_runs(db: Session) -> list[dict[str, Any]]:
    if not _relation_exists(db, "daily_kline_reconciliation_runs"):
        return []
    rows = db.execute(text("""
        SELECT run_id, trade_date, published_count, warning_count, missing_count, created_at, updated_at
        FROM daily_kline_reconciliation_runs
        ORDER BY trade_date DESC, created_at DESC, id DESC
        LIMIT 5
    """)).mappings().all()
    return [
        {
            "run_id": str(row.get("run_id") or ""),
            "trade_date": row.get("trade_date").isoformat() if hasattr(row.get("trade_date"), "isoformat") else row.get("trade_date"),
            "published_count": int(row.get("published_count") or 0),
            "warning_count": int(row.get("warning_count") or 0),
            "missing_count": int(row.get("missing_count") or 0),
            "created_at": row.get("created_at").isoformat() if hasattr(row.get("created_at"), "isoformat") else row.get("created_at"),
            "updated_at": row.get("updated_at").isoformat() if hasattr(row.get("updated_at"), "isoformat") else row.get("updated_at"),
        }
        for row in rows
    ]


def _daily_reconciliation_item_summary(db: Session, run_id: str | None) -> list[dict[str, Any]]:
    if not run_id or not _relation_exists(db, "daily_kline_reconciliation_items"):
        return []
    rows = db.execute(text("""
        SELECT
            chosen_source,
            publish_status,
            quality_status,
            COUNT(*) AS item_count,
            AVG(coverage_ratio) AS avg_coverage_ratio,
            COUNT(*) FILTER (
                WHERE BTRIM(COALESCE(issues, '')) NOT IN ('', '[]', '{}', 'null')
            ) AS issue_count,
            COUNT(*) FILTER (WHERE COALESCE(issues, '') ILIKE '%source_conflict%') AS conflict_count,
            COUNT(*) FILTER (WHERE publish_status = 'published_with_warning') AS warning_count,
            COUNT(*) FILTER (WHERE publish_status = 'missing_or_conflicted') AS missing_count
        FROM daily_kline_reconciliation_items
        WHERE run_id = :run_id
        GROUP BY chosen_source, publish_status, quality_status
        ORDER BY item_count DESC
    """), {"run_id": run_id}).mappings().all()
    return [
        {
            "chosen_source": str(row.get("chosen_source") or ""),
            "publish_status": str(row.get("publish_status") or ""),
            "quality_status": str(row.get("quality_status") or ""),
            "item_count": int(row.get("item_count") or 0),
            "avg_coverage_ratio": float(row.get("avg_coverage_ratio") or 0),
            "issue_count": int(row.get("issue_count") or 0),
            "conflict_count": int(row.get("conflict_count") or 0),
            "warning_count": int(row.get("warning_count") or 0),
            "missing_count": int(row.get("missing_count") or 0),
        }
        for row in rows
    ]


# ========== 批量下载API ==========

@router.post("/batch-download")
def batch_download_data(
    request: BatchDataDownloadRequest,
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """批量下载数据 - 自动取消同类型的旧任务"""
    try:
        task_ids = []
        
        # 为每种数据类型创建下载任务
        for data_type in request.data_types:
            # 取消同类型的pending/running任务
            cancel_query = text("""
                UPDATE backtest_data_tasks 
                SET status = 'cancelled', error_message = '被新任务取代', updated_at = NOW()
                WHERE user_id = :user_id 
                  AND task_type = :task_type 
                  AND status IN ('pending', 'running')
            """)
            db.execute(cancel_query, {
                "user_id": current_user.id,
                "task_type": data_type
            })
            
            # 创建新任务
            query = text("""
                INSERT INTO backtest_data_tasks 
                (user_id, task_type, data_source, date_range_start, date_range_end, symbols, status, task_scope)
                VALUES (:user_id, :task_type, :data_source, :date_range_start, :date_range_end, :symbols, 'pending', 'primary')
                RETURNING id
            """)
            
            result = db.execute(query, {
                "user_id": current_user.id,
                "task_type": data_type,
                "data_source": request.data_source,
                "date_range_start": request.date_range_start,
                "date_range_end": request.date_range_end,
                "symbols": request.symbols or []
            })
            task_id = result.fetchone()[0]
            task_ids.append(task_id)
        
        db.commit()
        
        thread = threading.Thread(
            target=_run_process_batch_download,
            args=(task_ids, current_user.id),
            daemon=True,
            name=f"backtest-batch-download-{','.join(map(str, task_ids))}",
        )
        thread.start()
        
        return {
            "message": "批量下载任务已创建",
            "task_ids": task_ids,
            "total_tasks": len(task_ids)
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"创建批量下载任务失败: {str(e)}")


# ========== 数据源配置API ==========

@router.get("/data-sources")
def list_data_sources(
    db: Session = Depends(get_db)
):
    """获取数据源配置列表"""
    try:
        query = text("""
            SELECT * FROM data_source_configs 
            WHERE is_active = TRUE 
            ORDER BY priority DESC, source_name
        """)
        result = db.execute(query)
        
        sources = []
        for row in result:
            sources.append({
                "id": row.id,
                "source_name": row.source_name,
                "source_type": row.source_type,
                "description": row.description,
                "rate_limit_per_minute": row.rate_limit_per_minute,
                "priority": row.priority,
                "requires_api_key": bool(row.api_key)
            })
        
        return {"sources": sources, "total": len(sources)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据源列表失败: {str(e)}")


# ========== 辅助函数 ==========

async def _process_batch_download(task_ids: List[int], user_id: str):
    """处理批量下载任务 - 使用AKShare真实数据"""
    from api.database import get_db_ctx
    import logging
    
    logger = logging.getLogger(__name__)
    
    for task_id in task_ids:
        try:
            # 获取任务信息
            with get_db_ctx() as db:
                task_query = text("SELECT * FROM backtest_data_tasks WHERE id = :task_id")
                task_result = db.execute(task_query, {"task_id": task_id})
                task = task_result.fetchone()
                
                if not task:
                    logger.error(f"任务 {task_id} 不存在")
                    continue
                
                # ✅ 验证数据源兼容性
                compatible_sources = DATA_SOURCE_COMPATIBILITY.get(task.task_type, [])
                if task.data_source not in compatible_sources:
                    logger.warning(f"数据类型 {task.task_type} 不支持数据源 {task.data_source}")
                    # 自动切换到兼容的数据源
                    if compatible_sources:
                        old_source = task.data_source
                        new_source = compatible_sources[0]
                        logger.info(f"已自动切换数据源: {old_source} -> {new_source}")
                        # 更新数据库
                        db.execute(text("UPDATE backtest_data_tasks SET data_source = :source WHERE id = :task_id"), 
                                   {"source": new_source, "task_id": task_id})
                        db.commit()
                        # 重新查询任务以获取更新后的数据源
                        task_result = db.execute(task_query, {"task_id": task_id})
                        task = task_result.fetchone()
                
                # 更新任务状态为运行中
                db.execute(text("""
                    UPDATE backtest_data_tasks
                    SET status = 'running',
                        error_message = '任务已启动，等待下载器执行',
                        updated_at = NOW()
                    WHERE id = :task_id
                """), {"task_id": task_id})
                db.commit()
                
                logger.info(f"开始处理任务 {task_id}: {task.task_type} (数据源: {task.data_source})")
            
            # 创建下载器
            with get_db_ctx() as db:
                downloader = DataDownloader(db)
                
                total_records = 0
                success_count = 0
                error_count = 0
                specific_error_message = None
                
                # 根据数据类型下载
                if task.task_type == 'daily_kline':
                    # 检查数据源
                    if task.data_source == 'tdx':
                        logger.info("使用通达信/TDX数据源同步股票日K线")

                        def tdx_daily_progress_callback(progress: int, message: str):
                            written_records = _extract_written_records(message, total_records)
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        downloaded_records = :records,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": max(0, min(int(progress), 100)),
                                    "records": written_records,
                                    "error_message": message[:500] if message else None,
                                })
                                db_update.commit()

                        try:
                            from api.services.tdx_market_data_service import sync_stock_daily_history as sync_tdx_stock_daily_history

                            result = sync_tdx_stock_daily_history(
                                start_date=task.date_range_start,
                                end_date=task.date_range_end,
                                symbols=task.symbols or [],
                                progress_callback=tdx_daily_progress_callback,
                            )
                        except Exception as exc:
                            result = {"success": False, "error": str(exc), "rows": 0}

                        if result.get("success"):
                            total_records = int(result.get("rows") or 0)
                            success_count = int(result.get("success_symbols") or 0) or len([value for value in (result.get("symbol_rows") or {}).values() if value]) or 1
                            cache_refresh = _refresh_daily_kline_cache_from_db(
                                db,
                                start_date=task.date_range_start,
                                end_date=task.date_range_end,
                                symbols=task.symbols or None,
                            )
                            logger.info(
                                "TDX日K同步完成: rows=%s symbols=%s cache_updated=%s",
                                total_records,
                                success_count,
                                cache_refresh.get("updated"),
                            )
                        else:
                            error_count = 1
                            specific_error_message = result.get("error") or "TDX股票日K同步未写入记录"
                            logger.error("TDX股票日K同步失败: %s", specific_error_message)

                    elif task.data_source == 'quantclass':
                        # 使用量化课堂数据源
                        logger.info("使用量化课堂数据源下载股票日K线")
                        
                        try:
                            # 量化课堂配置
                            quantclass_api_key = os.getenv("QUANTCLASS_API_KEY", "2HUTNZYOSRA8X5Z7TY2VZGKNTX5UN28B")
                            quantclass_hid = os.getenv("QUANTCLASS_HID", "1ad9e296ad8d3816b9bce5cba86b1ff6")
                            
                            # 创建下载器
                            qc_downloader = QuantClassDownloader(quantclass_api_key, quantclass_hid)
                            
                            # Daily enrichment tasks are date-scoped: download the exact
                            # target day so a newer QuantClass package cannot advance the
                            # watermark for the wrong date.
                            task_scope = str(getattr(task, "task_scope", None) or "primary")
                            download_date_time = (
                                task.date_range_end.isoformat()
                                if task_scope == "daily_enrichment" and task.date_range_end
                                else None
                            )
                            download_result = qc_downloader.download_product(
                                'stock-trading-data-pro',
                                date_time=download_date_time,
                            )
                            
                            if download_result['success']:
                                # 导入数据库
                                import_result = import_stock_daily_from_quantclass(db, download_result['data_path'])
                                
                                if import_result['success']:
                                    imported_max_date = import_result.get('max_trade_date')
                                    requested_end_date = task.date_range_end
                                    if imported_max_date and requested_end_date and imported_max_date < requested_end_date:
                                        error_count = 1
                                        stale_message = (
                                            f"量化课堂返回的数据最新日期为 {imported_max_date}，"
                                            f"未覆盖请求结束日期 {requested_end_date}"
                                        )
                                        specific_error_message = stale_message
                                        logger.error(stale_message)
                                        with get_db_ctx() as db_update:
                                            db_update.execute(text("""
                                                UPDATE backtest_data_tasks
                                                SET error_message = :error_message,
                                                    updated_at = NOW()
                                                WHERE id = :task_id
                                            """), {"task_id": task_id, "error_message": stale_message})
                                            db_update.commit()
                                    else:
                                        total_records = import_result['records_imported']
                                        success_count = import_result['stocks_count']
                                        cache_refresh = _refresh_daily_kline_cache_from_db(
                                            db,
                                            start_date=import_result.get('min_trade_date'),
                                            end_date=import_result.get('max_trade_date'),
                                        )
                                        logger.info(
                                            "量化课堂日K缓存刷新: updated=%s records=%s range=%s~%s",
                                            cache_refresh.get("updated"),
                                            cache_refresh.get("records"),
                                            cache_refresh.get("date_range_start"),
                                            cache_refresh.get("date_range_end"),
                                        )
                                        logger.info(f"量化课堂导入成功: {total_records}条记录, {success_count}只股票")
                                else:
                                    error_count = 1
                                    logger.error(f"量化课堂导入失败: {import_result.get('error')}")
                            else:
                                error_count = 1
                                logger.error(f"量化课堂下载失败: {download_result.get('error')}")
                        except Exception as e:
                            error_count = 1
                            logger.error(f"量化课堂处理异常: {e}")
                        
                        # ✅ 量化课堂下载完成，跳过AKShare下载流程
                        pass
                    
                    else:
                        # 使用AKShare数据源
                        logger.info("使用%s数据源下载股票日K线", task.data_source or "akshare")
                        
                        # 获取股票列表
                        symbols = downloader.get_all_stock_symbols()
                        if task.symbols:
                            # 如果指定了股票代码，只下载指定的
                            symbols = [s for s in symbols if s in task.symbols]
                        if not symbols:
                            error_count = 1
                            specific_error_message = "未获取到可下载的股票列表，任务未执行；请稍后重试或改用指定股票范围。"
                            logger.error(specific_error_message)
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {"task_id": task_id, "error_message": specific_error_message})
                                db_update.commit()
                        else:
                        
                            total_stocks = len(symbols)
                            logger.info(f"准备下载 {total_stocks} 只股票的日K线数据 (使用{task.data_source or 'akshare'})")
                    
                            batch_size = DEFAULT_AKSHARE_BATCH_SIZE
                            for i in range(0, len(symbols), batch_size):
                                batch = symbols[i:i+batch_size]
                            
                                # 并行下载这一批股票
                                download_tasks = [
                                    downloader.download_daily_kline(
                                        symbol, task.date_range_start, task.date_range_end, source=task.data_source or "akshare"
                                    )
                                    for symbol in batch
                                ]
                                results = await asyncio.gather(*download_tasks, return_exceptions=True)
                            
                                # 统计结果
                                for symbol, result in zip(batch, results):
                                    if isinstance(result, Exception):
                                        logger.error(f"股票 {symbol} 下载异常: {result}")
                                        error_count += 1
                                    elif result['success']:
                                        total_records += result['records']
                                        success_count += 1
                                    else:
                                        logger.error(f"股票 {symbol} 下载失败: {result.get('error', '未知错误')}")
                                        error_count += 1
                            
                                # 更新进度
                                progress = int((i + len(batch)) / max(len(symbols), 1) * 100)
                                with get_db_ctx() as db_update:
                                    db_update.execute(text("""
                                        UPDATE backtest_data_tasks 
                                        SET progress = :progress, 
                                            downloaded_records = :records,
                                            updated_at = NOW()
                                        WHERE id = :task_id
                                    """), {
                                        "task_id": task_id,
                                        "progress": min(progress, 100),
                                        "records": total_records
                                    })
                                    db_update.commit()
                            
                                if DEFAULT_AKSHARE_BATCH_SLEEP_SECONDS > 0:
                                    await asyncio.sleep(DEFAULT_AKSHARE_BATCH_SLEEP_SECONDS)

                            if success_count > 0:
                                cache_refresh = _refresh_daily_kline_cache_from_db(
                                    db,
                                    start_date=task.date_range_start,
                                    end_date=task.date_range_end,
                                    symbols=task.symbols or None,
                                )
                                logger.info(
                                    "%s日K缓存刷新: updated=%s records=%s range=%s~%s",
                                    task.data_source or "akshare",
                                    cache_refresh.get("updated"),
                                    cache_refresh.get("records"),
                                    cache_refresh.get("date_range_start"),
                                    cache_refresh.get("date_range_end"),
                                )
                            if total_records <= 0 and error_count == 0:
                                error_count = 1
                                specific_error_message = (
                                    f"{task.data_source or 'akshare'} 未返回 {task.date_range_start} ~ "
                                    f"{task.date_range_end} 的日K数据，任务未写入新记录。"
                                )
                elif task.task_type == 'index_data':
                    # 检查数据源
                    if task.data_source == 'quantclass':
                        # 使用量化课堂数据源
                        logger.info("使用量化课堂数据源下载指数数据")
                        
                        try:
                            # 量化课堂配置
                            quantclass_api_key = os.getenv("QUANTCLASS_API_KEY", "2HUTNZYOSRA8X5Z7TY2VZGKNTX5UN28B")
                            quantclass_hid = os.getenv("QUANTCLASS_HID", "1ad9e296ad8d3816b9bce5cba86b1ff6")
                            
                            # 创建下载器
                            qc_downloader = QuantClassDownloader(quantclass_api_key, quantclass_hid)
                            
                            # 下载指数数据
                            download_result = qc_downloader.download_product('stock-main-index-data')
                            
                            if download_result['success']:
                                # 导入数据库
                                from api.generic_importer import import_generic_data
                                import_result = import_generic_data(db, download_result['data_path'], 'index_daily')
                                
                                if import_result['success']:
                                    total_records = import_result['records_imported']
                                    success_count = 1
                                    logger.info(f"量化课堂指数数据导入成功: {total_records}条记录")
                                else:
                                    error_count = 1
                                    logger.error(f"量化课堂指数数据导入失败: {import_result.get('error')}")
                            else:
                                error_count = 1
                                logger.error(f"量化课堂指数数据下载失败: {download_result.get('error')}")
                        except Exception as e:
                            error_count = 1
                            logger.error(f"量化课堂指数数据下载异常: {e}")
                    
                    else:
                        requested_source = str(task.data_source or "akshare").strip().lower() or "akshare"
                        sync_source = requested_source if requested_source in {"tdx", "qmt", "akshare"} else "akshare"
                        index_symbols = task.symbols or []
                        logger.info(
                            "准备同步 %s 个市场页指数日K数据 (source=%s, table=index_daily_kline)",
                            len(index_symbols) or 8,
                            sync_source,
                        )

                        def index_daily_progress_callback(progress: int, message: str):
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": max(0, min(int(progress), 100)),
                                    "error_message": message[:500] if message else None,
                                })
                                db_update.commit()

                        try:
                            if sync_source == "tdx":
                                from api.services.tdx_market_data_service import sync_index_daily_history as sync_tdx_index_daily_history

                                result = sync_tdx_index_daily_history(
                                    start_date=task.date_range_start.isoformat(),
                                    end_date=task.date_range_end.isoformat(),
                                    symbols=index_symbols,
                                    progress_callback=index_daily_progress_callback,
                                )
                            else:
                                result = sync_index_daily_history(
                                    start_date=task.date_range_start.isoformat(),
                                    end_date=task.date_range_end.isoformat(),
                                    symbols=index_symbols,
                                    data_source=sync_source,
                                    db=db,
                                    progress_callback=index_daily_progress_callback,
                                )
                        except Exception as exc:
                            result = {"success": False, "error": str(exc), "rows": 0}

                        if result.get('success'):
                            total_records += int(result.get('rows') or 0)
                            success_count += len([value for value in (result.get('symbol_rows') or {}).values() if value]) or len(result.get('symbols') or []) or 1
                        else:
                            error_count += 1
                            specific_error_message = result.get('error') or "指数日K同步未写入记录"
                            logger.error(f"指数日K下载失败: {specific_error_message}")
                
                elif task.task_type == 'minute_kline':
                    if task.data_source == 'tdx':
                        symbols = task.symbols or []
                        total_stocks = len(symbols)
                        scope_text = f"{total_stocks} 只股票" if total_stocks > 0 else "全市场股票"
                        logger.info(f"准备下载 {scope_text} 的1分钟K线数据 (使用TDX)")

                        def tdx_minute_progress_callback(progress: int, message: str):
                            written_records = _extract_written_records(message, total_records)
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        downloaded_records = :records,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": max(0, min(int(progress), 100)),
                                    "records": written_records,
                                    "error_message": message[:500] if message else None,
                                })
                                db_update.commit()

                        try:
                            from api.services.tdx_market_data_service import sync_stock_minute_history as sync_tdx_stock_minute_history

                            result = sync_tdx_stock_minute_history(
                                start_date=task.date_range_start,
                                end_date=task.date_range_end,
                                symbols=task.symbols or [],
                                progress_callback=tdx_minute_progress_callback,
                            )
                        except Exception as exc:
                            result = {"success": False, "error": str(exc), "rows": 0}

                        if result.get('success'):
                            total_records += int(result.get('rows') or 0)
                            success_count += int(result.get("success_symbols") or 0) or len([value for value in (result.get("symbol_rows") or {}).values() if value]) or 1
                        else:
                            error_count += 1
                            specific_error_message = result.get('error') or "TDX 1分钟K线同步未写入记录"
                            logger.error(f"TDX 1分钟K线下载失败: {specific_error_message}")

                        with get_db_ctx() as db_update:
                            db_update.execute(text("""
                                UPDATE backtest_data_tasks
                                SET progress = :progress,
                                    downloaded_records = :records,
                                    error_message = :error_message,
                                    updated_at = NOW()
                                WHERE id = :task_id
                            """), {
                                "task_id": task_id,
                                "progress": 100 if result.get('success') else 0,
                                "records": total_records,
                                "error_message": (
                                    f"TDX 分钟线同步完成，区间记录约 {total_records} 条"
                                    if result.get('success')
                                    else result.get('error')
                                )
                            })
                            db_update.commit()

                    elif task.data_source == 'qmt':
                        symbols = task.symbols or []
                        total_stocks = len(symbols)
                        scope_text = f"{total_stocks} 只股票" if total_stocks > 0 else "全市场股票"
                        logger.info(f"准备下载 {scope_text} 的1分钟K线数据 (使用QMT)")
                        async def qmt_progress_callback(progress: int, message: str):
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": max(0, min(int(progress), 100)),
                                    "error_message": message[:500] if message else None,
                                })
                                db_update.commit()

                        await qmt_progress_callback(
                            5,
                            f"QMT 连接检查通过，准备启动历史分钟线同步脚本（专用通道：{settings.qmt_minute_history_account_key or 'live_real'}）",
                        )

                        result = await downloader.download_minute_kline_from_qmt(
                            start_date=task.date_range_start,
                            end_date=task.date_range_end,
                            symbols=task.symbols or [],
                            progress_callback=qmt_progress_callback,
                        )
                        if result.get('success'):
                            total_records += int(result.get('records') or 0)
                            success_count += total_stocks if total_stocks > 0 else 1
                        else:
                            error_count += 1
                            logger.error(f"QMT 1分钟K线下载失败: {result.get('error', '未知错误')}")

                        with get_db_ctx() as db_update:
                            db_update.execute(text("""
                                UPDATE backtest_data_tasks
                                SET progress = :progress,
                                    downloaded_records = :records,
                                    error_message = :error_message,
                                    updated_at = NOW()
                                WHERE id = :task_id
                            """), {
                                "task_id": task_id,
                                "progress": 100 if result.get('success') else 0,
                                "records": total_records,
                                "error_message": (
                                    f"QMT 分钟线同步完成，区间记录约 {total_records} 条；通道：{result.get('account_key') or 'paper_sim'}；bridge：{result.get('bridge') or '8710'}"
                                    if result.get('success')
                                    else result.get('error')
                                )
                            })
                            db_update.commit()
                    else:
                        symbols = downloader.get_all_stock_symbols()
                        if task.symbols:
                            symbols = [s for s in symbols if s in task.symbols]
                        total_stocks = len(symbols)
                        logger.info(f"准备下载 {total_stocks} 只股票的1分钟K线数据 (使用AKShare)")

                        batch_size = DEFAULT_AKSHARE_BATCH_SIZE
                        for i in range(0, len(symbols), batch_size):
                            batch = symbols[i:i+batch_size]

                            download_tasks = [
                                downloader.download_minute_kline(
                                    symbol, task.date_range_start, task.date_range_end, source=task.data_source or "akshare"
                                )
                                for symbol in batch
                            ]
                            results = await asyncio.gather(*download_tasks, return_exceptions=True)

                            for symbol, result in zip(batch, results):
                                if isinstance(result, Exception):
                                    logger.error(f"股票 {symbol} 1分钟K线下载异常: {result}")
                                    error_count += 1
                                elif result['success']:
                                    total_records += result['records']
                                    success_count += 1
                                else:
                                    logger.error(f"股票 {symbol} 1分钟K线下载失败: {result.get('error', '未知错误')}")
                                    error_count += 1

                            progress = int((i + len(batch)) / max(len(symbols), 1) * 100)
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        downloaded_records = :records,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": min(progress, 100),
                                    "records": total_records,
                                    "error_message": f"AKShare 批次 {i // batch_size + 1}/{max((len(symbols) + batch_size - 1) // batch_size, 1)}，已处理 {min(i + len(batch), len(symbols))}/{len(symbols)} 只股票"
                                })
                                db_update.commit()

                            if DEFAULT_AKSHARE_BATCH_SLEEP_SECONDS > 0:
                                await asyncio.sleep(DEFAULT_AKSHARE_BATCH_SLEEP_SECONDS)

                elif task.task_type == 'index_minute_kline':
                    if task.data_source not in {'tdx', 'qmt', 'akshare'}:
                        error_count += 1
                        logger.error("指数1分钟K线当前仅支持TDX、QMT或AKShare数据源")
                    else:
                        index_symbols = task.symbols or []
                        logger.info(f"准备下载 {len(index_symbols) or 8} 个指数的1分钟K线数据 (使用{task.data_source})")

                        def index_progress_callback(progress: int, message: str):
                            with get_db_ctx() as db_update:
                                db_update.execute(text("""
                                    UPDATE backtest_data_tasks
                                    SET progress = :progress,
                                        error_message = :error_message,
                                        updated_at = NOW()
                                    WHERE id = :task_id
                                """), {
                                    "task_id": task_id,
                                    "progress": max(0, min(int(progress), 100)),
                                    "error_message": message[:500] if message else None,
                                })
                                db_update.commit()

                        index_progress_callback(
                            5,
                            (
                                "TDX 指数分钟线历史同步已启动"
                                if task.data_source == "tdx"
                                else f"QMT 指数分钟线历史同步已启动，专用通道：{settings.qmt_minute_history_account_key or 'live_real'}"
                                if task.data_source == "qmt"
                                else "AKShare 指数分钟线历史同步已启动（仅最近 5 个交易日可用）"
                            ),
                        )
                        try:
                            if task.data_source == "tdx":
                                from api.services.tdx_market_data_service import sync_index_minute_history as sync_tdx_index_minute_history

                                result = sync_tdx_index_minute_history(
                                    start_date=task.date_range_start.isoformat(),
                                    end_date=task.date_range_end.isoformat(),
                                    symbols=index_symbols,
                                    progress_callback=index_progress_callback,
                                )
                            else:
                                result = sync_index_minute_history(
                                    start_date=task.date_range_start.isoformat(),
                                    end_date=task.date_range_end.isoformat(),
                                    symbols=index_symbols,
                                    account_key=None,
                                    data_source=task.data_source,
                                    progress_callback=index_progress_callback,
                                )
                        except Exception as exc:
                            result = {"success": False, "error": str(exc), "rows": 0}

                        if result.get('success'):
                            total_records += int(result.get('rows') or 0)
                            success_count += len(result.get('symbols') or []) or 1
                        else:
                            error_count += 1
                            specific_error_message = result.get('error') or "指数1分钟K线同步未写入记录"
                            logger.error(f"指数1分钟K线下载失败: {specific_error_message}")

                        with get_db_ctx() as db_update:
                            db_update.execute(text("""
                                UPDATE backtest_data_tasks
                                SET progress = :progress,
                                    downloaded_records = :records,
                                    error_message = :error_message,
                                    updated_at = NOW()
                                WHERE id = :task_id
                            """), {
                                "task_id": task_id,
                                "progress": 100 if result.get('success') else 0,
                                "records": total_records,
                                "error_message": (
                                    f"{task.data_source.upper()} 指数分钟线同步完成，区间记录约 {total_records} 条；缺失指数: {','.join(result.get('missing_symbols') or []) or '无'}"
                                    if result.get('success')
                                    else result.get('error')
                                )
                            })
                            db_update.commit()
                
                elif task.task_type == 'chip_data':
                    # 筹码数据
                    if task.data_source == 'quantclass':
                        logger.info("使用量化课堂数据源下载筹码数据")
                        
                        try:
                            quantclass_api_key = os.getenv("QUANTCLASS_API_KEY", "2HUTNZYOSRA8X5Z7TY2VZGKNTX5UN28B")
                            quantclass_hid = os.getenv("QUANTCLASS_HID", "1ad9e296ad8d3816b9bce5cba86b1ff6")
                            
                            qc_downloader = QuantClassDownloader(quantclass_api_key, quantclass_hid)
                            download_result = qc_downloader.download_product('stock-chip-distribution')
                            
                            if download_result['success']:
                                from api.generic_importer import import_generic_data
                                import_result = import_generic_data(db, download_result['data_path'], 'chip_data')
                                
                                if import_result['success']:
                                    total_records = import_result['records_imported']
                                    success_count = 1
                                    logger.info(f"筹码数据导入成功: {total_records}条记录")
                                else:
                                    error_count = 1
                                    logger.error(f"筹码数据导入失败: {import_result.get('error')}")
                            else:
                                error_count = 1
                                logger.error(f"筹码数据下载失败: {download_result.get('error')}")
                        except Exception as e:
                            error_count = 1
                            logger.error(f"筹码数据下载异常: {e}")
                    else:
                        logger.warning("AKShare不支持筹码数据下载")
                        error_count = 1
                
                elif task.task_type == 'financial_data':
                    # 财务数据
                    if task.data_source == 'quantclass':
                        logger.info("使用量化课堂数据源下载财务数据")
                        
                        try:
                            quantclass_api_key = os.getenv("QUANTCLASS_API_KEY", "2HUTNZYOSRA8X5Z7TY2VZGKNTX5UN28B")
                            quantclass_hid = os.getenv("QUANTCLASS_HID", "1ad9e296ad8d3816b9bce5cba86b1ff6")
                            
                            qc_downloader = QuantClassDownloader(quantclass_api_key, quantclass_hid)
                            download_result = qc_downloader.download_product('stock-fin-pre-data-sina')
                            
                            if download_result['success']:
                                from api.generic_importer import import_generic_data
                                import_result = import_generic_data(db, download_result['data_path'], 'financial_data')
                                
                                if import_result['success']:
                                    total_records = import_result['records_imported']
                                    success_count = 1
                                    logger.info(f"财务数据导入成功: {total_records}条记录")
                                else:
                                    error_count = 1
                                    logger.error(f"财务数据导入失败: {import_result.get('error')}")
                            else:
                                error_count = 1
                                logger.error(f"财务数据下载失败: {download_result.get('error')}")
                        except Exception as e:
                            error_count = 1
                            logger.error(f"财务数据下载异常: {e}")
                    else:
                        logger.warning("AKShare财务数据下载功能待实现")
                        error_count = 1
                
                elif task.task_type == 'research_reports':
                    # 研报数据
                    logger.warning("研报数据下载功能待实现")
                    error_count = 1
                
                else:
                    logger.warning(f"未知的数据类型: {task.task_type}")
                    error_count = 1
                
                final_status = 'completed'
                clear_error_message = True
                final_error_message = None
                if error_count > 0 and success_count == 0:
                    final_status = 'failed'
                    clear_error_message = False
                    final_error_message = specific_error_message or f"任务执行失败，成功 {success_count}，失败 {error_count}"
                elif error_count > 0:
                    final_status = 'completed'
                    clear_error_message = False
                    final_error_message = specific_error_message or f"任务部分成功，成功 {success_count}，失败 {error_count}"

                # 更新任务状态
                with get_db_ctx() as db:
                    actual_coverage = _get_actual_table_coverage(db, task_type=task.task_type)
                    actual_last_data_date = actual_coverage.get("date_range_end") if actual_coverage else None
                    actual_start_date = actual_coverage.get("date_range_start") if actual_coverage else None
                    actual_total_records = actual_coverage.get("total_records") if actual_coverage else total_records

                    db.execute(text("""
                        UPDATE backtest_data_tasks 
                        SET status = :final_status, 
                            progress = 100, 
                            total_records = :total_records,
                            downloaded_records = :total_records,
                            error_message = CASE
                                WHEN :clear_error_message THEN NULL
                                ELSE :final_error_message
                            END,
                            completed_at = NOW(),
                            updated_at = NOW()
                        WHERE id = :task_id
                    """), {
                        "task_id": task_id,
                        "total_records": total_records,
                        "final_status": final_status,
                        "clear_error_message": clear_error_message,
                        "final_error_message": final_error_message,
                    })

                    subscription_config_id = getattr(task, "subscription_config_id", None)
                    if subscription_config_id:
                        task_scope = str(getattr(task, "task_scope", None) or "primary")
                        scope_key = "all"
                        task_symbols = list(task.symbols or [])
                        if task_symbols:
                            scope_key = "symbols:" + ",".join(sorted({str(item).strip().upper() for item in task_symbols if str(item).strip()})[:200])
                        watermark_data_type = task.task_type
                        if task_scope == "daily_enrichment" and task.task_type == "daily_kline":
                            watermark_data_type = "daily_kline_enrichment"

                        watermark_existing = db.execute(text("""
                            SELECT id
                            FROM backtest_data_watermarks
                            WHERE user_id = :user_id
                              AND config_id = :config_id
                              AND data_type = :data_type
                              AND COALESCE(data_source, '') = :data_source
                              AND scope_key = :scope_key
                            LIMIT 1
                        """), {
                            "user_id": str(task.user_id),
                            "config_id": int(subscription_config_id),
                            "data_type": watermark_data_type,
                            "data_source": str(task.data_source or ""),
                            "scope_key": scope_key,
                        }).fetchone()
                        watermark_last_data_date = actual_last_data_date
                        if task_scope == "daily_enrichment" and task.task_type == "daily_kline":
                            watermark_last_data_date = task.date_range_end

                        watermark_payload = {
                            "user_id": str(task.user_id),
                            "config_id": int(subscription_config_id),
                            "data_type": watermark_data_type,
                            "data_source": str(task.data_source or ""),
                            "scope_key": scope_key,
                            "last_run_started_at": datetime.utcnow(),
                            "last_data_date": watermark_last_data_date if final_status == "completed" else None,
                            "last_success_at": datetime.utcnow() if final_status == "completed" else None,
                            "last_status": final_status,
                            "last_error": final_error_message if final_status != "completed" else None,
                        }
                        if watermark_existing:
                            db.execute(text("""
                                UPDATE backtest_data_watermarks
                                SET last_run_started_at = :last_run_started_at,
                                    last_data_date = COALESCE(:last_data_date, last_data_date),
                                    last_success_at = COALESCE(:last_success_at, last_success_at),
                                    last_status = :last_status,
                                    last_error = :last_error,
                                    updated_at = NOW()
                                WHERE id = :id
                            """), {**watermark_payload, "id": watermark_existing.id})
                        else:
                            db.execute(text("""
                                INSERT INTO backtest_data_watermarks
                                (user_id, config_id, data_type, data_source, scope_key, last_run_started_at, last_data_date, last_success_at, last_status, last_error, created_at, updated_at)
                                VALUES (:user_id, :config_id, :data_type, :data_source, :scope_key, :last_run_started_at, :last_data_date, :last_success_at, :last_status, :last_error, NOW(), NOW())
                            """), watermark_payload)

                        if final_status == "completed":
                            db.execute(text("""
                                UPDATE backtest_data_configs
                                SET last_success_at = NOW(),
                                    last_updated_at = NOW(),
                                    updated_at = NOW()
                                WHERE id = :config_id
                            """), {"config_id": int(subscription_config_id)})
                    
                    # 更新数据统计（先删除旧记录，再插入新记录，避免PostgreSQL NULL值问题）
                    db.execute(text("""
                        DELETE FROM backtest_data_stats 
                        WHERE data_type = :data_type AND (symbol = :symbol OR (symbol IS NULL AND :symbol IS NULL))
                    """), {
                        "data_type": task.task_type,
                        "symbol": None
                    })
                    
                    db.execute(text("""
                        INSERT INTO backtest_data_stats 
                        (data_type, symbol, total_records, date_range_start, date_range_end, data_quality_score, last_updated_date)
                        VALUES (:data_type, NULL, :total_records, :date_range_start, :date_range_end, 95, :last_updated_date)
                    """), {
                        "data_type": task.task_type,
                        "total_records": actual_total_records,
                        "date_range_start": actual_start_date,
                        "date_range_end": actual_last_data_date,
                        "last_updated_date": actual_last_data_date,
                    })
                    db.commit()
                    
                    # 数据质量检查（质量优先原则）
                    try:
                        quality_manager = DataQualityManager()
                        
                        # 确定表名
                        table_name = 'stock_daily_kline'
                        if task.task_type == 'index_data':
                            table_name = 'index_daily_kline'
                        elif task.task_type == 'minute_kline':
                            table_name = 'stock_minute_kline'
                        elif task.task_type == 'index_minute_kline':
                            table_name = 'index_minute_kline'
                        
                        # 执行质量检查
                        quality_result = quality_manager.validate_database_integrity(
                            db, table_name, task.task_type
                        )
                        
                        if quality_result['valid']:
                            logger.info(f"✅ 数据质量检查通过: {quality_result['stats']}")
                        else:
                            logger.warning(f"⚠️ 数据质量问题: {quality_result['issues']}")
                    except Exception as e:
                        logger.error(f"数据质量检查异常: {e}")
                    
                    # 记录数据源使用统计
                    try:
                        monitor = get_data_source_monitor()
                        monitor.record_download(
                            source=task.data_source or 'quantclass',
                            data_type=task.task_type,
                            records=total_records,
                            success=(error_count == 0)
                        )
                        logger.info(f"✅ 使用统计已记录: {task.data_source} - {task.task_type}")
                    except Exception as e:
                        logger.error(f"使用统计记录异常: {e}")
                    
                    logger.info(f"任务 {task_id} 完成: 成功 {success_count}, 失败 {error_count}, 总记录 {total_records}")
                
        except Exception as e:
            logger.error(f"任务 {task_id} 执行失败: {e}")
            # 更新任务状态为失败
            with get_db_ctx() as db:
                db.execute(text("""
                    UPDATE backtest_data_tasks 
                    SET status = 'failed', 
                        error_message = :error,
                        updated_at = NOW()
                    WHERE id = :task_id
                """), {"task_id": task_id, "error": str(e)})
                db.commit()


def _run_process_batch_download(task_ids: List[int], user_id: str) -> None:
    """Run the async batch processor off the main event loop."""
    run_async(_process_batch_download(task_ids, user_id))




# ========== 数据下载状态检查 ==========

@router.get("/download-status")
def get_download_status(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取数据下载状态概览"""
    try:
        # 统计各种状态的任务数量
        status_query = text("""
            SELECT status, COUNT(*) as count 
            FROM backtest_data_tasks 
            WHERE user_id = :user_id 
            GROUP BY status
        """)
        status_result = db.execute(status_query, {"user_id": current_user.id})
        
        status_counts = {}
        for row in status_result:
            status_counts[row.status] = row.count
        
        # 获取最近的任务
        recent_query = text("""
            SELECT * FROM backtest_data_tasks 
            WHERE user_id = :user_id 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        recent_result = db.execute(recent_query, {"user_id": current_user.id})
        
        recent_tasks = []
        for row in recent_result:
            recent_tasks.append({
                "id": row.id,
                "task_type": row.task_type,
                "status": row.status,
                "progress": row.progress or 0,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "completed_at": row.completed_at.isoformat() if row.completed_at else None
            })
        
        return {
            "status_counts": status_counts,
            "recent_tasks": recent_tasks,
            "total_tasks": sum(status_counts.values())
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取下载状态失败: {str(e)}")


# ========== 数据质量检查API ==========

@router.get("/quality-check/{table_name}")
def check_data_quality(
    table_name: str,
    data_type: str = "daily_kline",
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    检查数据质量
    
    Args:
        table_name: 表名（如 stock_daily_kline）
        data_type: 数据类型（daily_kline, index_data等）
    """
    try:
        quality_manager = DataQualityManager()
        
        # 执行数据库完整性检查
        db_result = quality_manager.validate_database_integrity(db, table_name, data_type)
        
        return {
            "success": True,
            "table_name": table_name,
            "data_type": data_type,
            "valid": db_result['valid'],
            "issues": db_result['issues'],
            "stats": db_result['stats']
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据质量检查失败: {str(e)}")


@router.get("/quality-report")
def generate_quality_report(
    current_user: UserDB = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    生成完整的数据质量报告
    
    包括：
    - 股票日K线数据质量
    - 指数数据质量
    - 数据库完整性
    """
    try:
        quality_manager = DataQualityManager()
        
        report = {
            "generated_at": datetime.now().isoformat(),
            "tables": {}
        }
        
        # 检查股票日K线
        daily_kline_result = quality_manager.validate_database_integrity(
            db, "stock_daily_kline", "daily_kline"
        )
        report["tables"]["stock_daily_kline"] = {
            "valid": daily_kline_result['valid'],
            "issues": daily_kline_result['issues'],
            "stats": daily_kline_result['stats']
        }
        
        # 检查指数数据（如果表存在）
        try:
            index_result = quality_manager.validate_database_integrity(
                db, "index_daily_kline", "index_data"
            )
            report["tables"]["index_daily_kline"] = {
                "valid": index_result['valid'],
                "issues": index_result['issues'],
                "stats": index_result['stats']
            }
        except:
            report["tables"]["index_daily_kline"] = {
                "valid": False,
                "issues": ["表不存在"],
                "stats": {}
            }

        try:
            index_minute_result = quality_manager.validate_database_integrity(
                db, "index_minute_kline", "index_minute_kline"
            )
            report["tables"]["index_minute_kline"] = {
                "valid": index_minute_result['valid'],
                "issues": index_minute_result['issues'],
                "stats": index_minute_result['stats']
            }
        except Exception:
            report["tables"]["index_minute_kline"] = {
                "valid": False,
                "issues": ["表不存在"],
                "stats": {}
            }

        return {
            "success": True,
            "report": report
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成质量报告失败: {str(e)}")


# ========== 数据源使用统计API ==========

@router.get("/source-usage")
def get_data_source_usage(
    current_user: UserDB = Depends(get_current_user)
):
    """
    获取数据源使用统计
    
    包括：
    - 量化课堂使用次数和剩余次数
    - AKShare使用统计
    - 每日下载记录
    """
    try:
        monitor = get_data_source_monitor()
        
        return {
            "success": True,
            "sources": monitor.get_all_sources_status(),
            "daily_report": monitor.get_daily_report()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取使用统计失败: {str(e)}")


@router.get("/source-status/{source}")
def get_source_status(
    source: str,
    current_user: UserDB = Depends(get_current_user)
):
    """
    获取指定数据源状态
    
    Args:
        source: 数据源名称（quantclass, akshare）
    """
    try:
        monitor = get_data_source_monitor()
        status = monitor.get_usage_status(source)
        
        if 'error' in status:
            raise HTTPException(status_code=400, detail=status['error'])
        
        return {
            "success": True,
            "status": status
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据源状态失败: {str(e)}")
