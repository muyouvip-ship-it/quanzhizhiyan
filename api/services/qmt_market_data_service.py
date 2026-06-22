from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

import requests
from sqlalchemy import bindparam, inspect, text
from sqlalchemy.orm import Session

from api.core.settings import settings
from api.database import SessionLocal, engine
from api.core.utils import safe_float as _safe_float
from api.services.market_data_pipeline_service import (
    ingest_raw_minute_rows,
    preferred_daily_kline_table,
    preferred_minute_kline_table,
    publish_minute_trade_date_batched,
    publish_minute_trade_date,
)
from api.services import qmt_virtual_account_service
from tradingagents.dataflows.trade_calendar import CN_TZ, _load_cn_trade_dates


logger = logging.getLogger(__name__)

MAJOR_INDEX_PRESETS = [
    {"symbol": "000001.SH", "code": "000001", "name": "上证指数"},
    {"symbol": "399001.SZ", "code": "399001", "name": "深证成指"},
    {"symbol": "399006.SZ", "code": "399006", "name": "创业板指"},
    {"symbol": "000300.SH", "code": "000300", "name": "沪深300"},
    {"symbol": "000905.SH", "code": "000905", "name": "中证500"},
    {"symbol": "000852.SH", "code": "000852", "name": "中证1000"},
    {"symbol": "000688.SH", "code": "000688", "name": "科创50"},
    {"symbol": "899050.BJ", "code": "899050", "name": "北证50"},
]

_INDEX_CODE_TO_SYMBOL = {item["code"]: item["symbol"] for item in MAJOR_INDEX_PRESETS}
_INDEX_SYMBOLS = {item["symbol"] for item in MAJOR_INDEX_PRESETS}
_INDEX_NAMES = {item["symbol"]: item["name"] for item in MAJOR_INDEX_PRESETS}


def get_index_presets() -> list[dict[str, str]]:
    return [dict(item) for item in MAJOR_INDEX_PRESETS]


def normalize_market_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    if "." in symbol:
        return symbol
    if len(symbol) == 6 and symbol.isdigit():
        if symbol.startswith(("4", "8")):
            return f"{symbol}.BJ"
        if symbol.startswith(("5", "6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"
    return symbol


def is_index_symbol(value: Any) -> bool:
    return bool(_normalize_index_symbol(value))


def fetch_realtime_quotes(
    symbols: list[str],
    *,
    account_key: str | None = None,
    timeout_seconds: float | None = None,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return {}
    config = _resolve_market_config(account_key, db=db, user_id=user_id)
    try:
        if config.bridge_base_url:
            return _fetch_quotes_via_bridge(config, normalized, timeout_seconds=timeout_seconds)
        return _fetch_quotes_via_local_xt(normalized)
    except Exception as exc:
        logger.warning("[qmt-market] quote fetch failed symbols=%s error=%s", len(normalized), exc)
        return {}


def fetch_intraday_bars(
    symbol: str,
    *,
    trade_date: str,
    period: str = "1m",
    include_latest_quote: bool = False,
    account_key: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
    persist: bool = True,
    quote_timeout_seconds: float | None = None,
    force_refresh: bool = False,
    allow_cache: bool = True,
) -> dict[str, Any]:
    normalized = normalize_market_symbol(symbol)
    if not normalized:
        raise ValueError("symbol is required")
    table_name = _minute_table_name(normalized)
    cached_rows = [] if force_refresh or not allow_cache else _load_intraday_rows_from_db(table_name, normalized, trade_date)
    source_rows = []
    if force_refresh or not cached_rows:
        source_rows = _fetch_intraday_rows_safe(
            [normalized],
            trade_date=trade_date,
            period=period,
            account_key=account_key,
            db=db,
            user_id=user_id,
        )
    if source_rows and persist:
        _upsert_intraday_rows(table_name, source_rows)
        if allow_cache and not force_refresh:
            cached_rows = _load_intraday_rows_from_db(table_name, normalized, trade_date)
    items = source_rows if force_refresh or not allow_cache else (cached_rows or source_rows)
    if cached_rows and source_rows and not force_refresh and allow_cache:
        source_label = "qmt_intraday+published_cache"
    elif cached_rows:
        source_label = "postgresql_cache"
    elif source_rows:
        source_label = "qmt_intraday"
    else:
        source_label = "empty"
    latest_quote = (
        fetch_realtime_quotes(
            [normalized],
            account_key=account_key,
            timeout_seconds=quote_timeout_seconds,
            db=db,
            user_id=user_id,
        ).get(normalized)
        if include_latest_quote
        else None
    )
    return {
        "symbol": normalized,
        "trade_date": trade_date,
        "period": period,
        "items": items,
        "latest_quote": latest_quote,
        "source": source_label,
    }


def capture_intraday_symbols(
    symbols: list[str],
    *,
    trade_date: str,
    period: str = "1m",
    account_key: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
    timeout_seconds: float | None = None,
    retry_missing: bool = True,
) -> dict[str, Any]:
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return {"success": False, "rows": 0, "symbols": [], "message": "empty symbols"}
    payload = _fetch_intraday_payload_safe(
        normalized,
        trade_date=trade_date,
        period=period,
        account_key=account_key,
        db=db,
        user_id=user_id,
        timeout_seconds=timeout_seconds,
    )
    rows = _normalize_intraday_payload(payload)
    symbol_errors = dict(payload.get("symbol_errors") or {})
    captured_symbols = {
        normalize_market_symbol(item.get("symbol"))
        for item in rows
        if normalize_market_symbol(item.get("symbol"))
    }
    missing_symbols = [symbol for symbol in normalized if symbol not in captured_symbols]

    if retry_missing and missing_symbols:
        # QMT batch minute queries occasionally miss individual symbols without failing the
        # whole request. Retry those names one by one so live monitoring doesn't silently
        # monitor only a subset of holdings.
        retry_rows: list[dict[str, Any]] = []
        for symbol in missing_symbols:
            retry_payload = _fetch_intraday_payload_safe(
                [symbol],
                trade_date=trade_date,
                period=period,
                account_key=account_key,
                db=db,
                user_id=user_id,
                timeout_seconds=timeout_seconds,
            )
            retry_items = _normalize_intraday_payload(retry_payload)
            if retry_items:
                retry_rows.extend(retry_items)
                captured_symbols.add(symbol)
            retry_errors = retry_payload.get("symbol_errors") or {}
            if isinstance(retry_errors, dict) and symbol in retry_errors:
                symbol_errors[symbol] = retry_errors[symbol]

        if retry_rows:
            rows = (
                sorted(
                    {
                        (
                            normalize_market_symbol(item.get("symbol")),
                            str(item.get("trade_time")),
                        ): item
                        for item in [*rows, *retry_rows]
                    }.values(),
                    key=lambda item: (str(item.get("symbol") or ""), str(item.get("trade_time") or "")),
                )
            )

    captured_symbols = {
        normalize_market_symbol(item.get("symbol"))
        for item in rows
        if normalize_market_symbol(item.get("symbol"))
    }
    missing_symbols = [symbol for symbol in normalized if symbol not in captured_symbols]
    symbol_rows: dict[str, int] = {symbol: 0 for symbol in normalized}
    symbol_latest_trade_times: dict[str, str] = {}
    for item in rows:
        symbol = normalize_market_symbol(item.get("symbol"))
        if symbol:
            symbol_rows[symbol] = symbol_rows.get(symbol, 0) + 1
            trade_time = str(item.get("trade_time") or "")
            if trade_time and trade_time > symbol_latest_trade_times.get(symbol, ""):
                symbol_latest_trade_times[symbol] = trade_time

    if not rows:
        return {
            "success": False,
            "rows": 0,
            "symbols": normalized,
            "captured_symbols": [],
            "missing_symbols": normalized,
            "symbol_rows": symbol_rows,
            "symbol_latest_trade_times": symbol_latest_trade_times,
            "symbol_errors": symbol_errors,
            "trade_date": trade_date,
            "period": period,
            "message": "no intraday bars",
            "source": "qmt_intraday",
        }
    stock_rows = [item for item in rows if not is_index_symbol(item.get("symbol"))]
    index_rows = [item for item in rows if is_index_symbol(item.get("symbol"))]
    if stock_rows:
        _upsert_intraday_rows("stock_minute_kline", stock_rows)
    if index_rows:
        _upsert_intraday_rows("index_minute_kline", index_rows)
    return {
        "success": True,
        "rows": len(rows),
        "symbols": normalized,
        "captured_symbols": sorted(captured_symbols),
        "missing_symbols": missing_symbols,
        "symbol_rows": symbol_rows,
        "symbol_latest_trade_times": symbol_latest_trade_times,
        "symbol_errors": symbol_errors,
        "partial": bool(missing_symbols),
        "trade_date": trade_date,
        "period": period,
        "message": "intraday bars captured" if not missing_symbols else f"intraday bars partially captured; missing={len(missing_symbols)}",
        "source": "qmt_intraday",
    }


def fetch_daily_bars(
    symbol: str,
    *,
    start_date: str,
    end_date: str,
    account_key: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    normalized = normalize_market_symbol(symbol)
    if not normalized:
        raise ValueError("symbol is required")
    if not is_index_symbol(normalized):
        raise ValueError("daily bar sync currently supports index symbols only")
    rows = _fetch_daily_rows_safe(
        [normalized],
        start_date=start_date,
        end_date=end_date,
        account_key=account_key,
        db=db,
        user_id=user_id,
    )
    if rows and persist:
        _upsert_index_daily_rows(rows)
    items = _load_daily_rows_from_db(normalized, start_date, end_date) or rows
    return {
        "symbol": normalized,
        "start_date": start_date,
        "end_date": end_date,
        "items": items,
        "source": "qmt_daily+postgresql_cache" if items else "empty",
    }


def sync_major_index_daily(
    *,
    start_date: str,
    end_date: str,
    symbols: list[str] | None = None,
    account_key: str | None = None,
    data_source: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    return sync_index_daily_history(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        account_key=account_key,
        data_source=data_source,
        db=db,
        user_id=user_id,
        progress_callback=progress_callback,
    )


def sync_index_daily_history(
    *,
    start_date: str,
    end_date: str,
    symbols: list[str] | None = None,
    account_key: str | None = None,
    data_source: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    normalized_symbols = _normalize_index_symbols(symbols)
    if not normalized_symbols:
        normalized_symbols = [item["symbol"] for item in MAJOR_INDEX_PRESETS]

    start = _parse_trade_date(start_date)
    end = _parse_trade_date(end_date)
    if start is None or end is None or start > end:
        raise ValueError("start_date / end_date is invalid")

    normalized_source = str(data_source or "qmt").strip().lower() or "qmt"
    if normalized_source == "akshare":
        return _sync_index_daily_history_via_akshare(
            start=start,
            end=end,
            symbols=normalized_symbols,
            progress_callback=progress_callback,
        )

    if callable(progress_callback):
        progress_callback(5, f"正在同步指数日K {start.isoformat()} ~ {end.isoformat()}")

    rows = _fetch_daily_rows_safe(
        normalized_symbols,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        account_key=account_key,
        db=db,
        user_id=user_id,
    )
    inserted = _upsert_index_daily_rows(rows) if rows else 0
    present_symbols = {normalize_market_symbol(item.get("symbol")) for item in rows if item.get("symbol")}
    missing_symbols = [symbol for symbol in normalized_symbols if symbol not in present_symbols]

    if callable(progress_callback):
        progress_callback(100 if inserted else 0, f"指数日K同步完成，写入 {inserted} 条")

    return {
        "success": bool(inserted),
        "rows": inserted,
        "symbols": normalized_symbols,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "missing_symbols": missing_symbols,
        "source": "qmt_daily",
    }


def sync_index_minute_history(
    *,
    start_date: str,
    end_date: str,
    symbols: list[str] | None = None,
    account_key: str | None = None,
    data_source: str | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    resolved_account_key = str(
        account_key
        or os.getenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY")
        or os.getenv("QMT_HISTORY_ACCOUNT_KEY")
        or getattr(settings, "qmt_minute_history_account_key", None)
        or getattr(settings, "qmt_history_account_key", None)
        or "paper_sim"
    ).strip() or "paper_sim"
    normalized_symbols = _normalize_index_symbols(symbols)
    if not normalized_symbols:
        normalized_symbols = [item["symbol"] for item in MAJOR_INDEX_PRESETS]

    start = _parse_trade_date(start_date)
    end = _parse_trade_date(end_date)
    if start is None or end is None or start > end:
        raise ValueError("start_date / end_date is invalid")

    trade_dates = _resolve_trade_dates(start, end)
    if not trade_dates:
        return {
            "success": True,
            "rows": 0,
            "symbols": normalized_symbols,
            "trade_dates": [],
            "missing_symbols": normalized_symbols,
            "source": "qmt_intraday",
        }

    total_days = len(trade_dates)
    total_rows = 0
    day_rows: dict[str, int] = {}
    missing_symbols: set[str] = set()
    normalized_source = str(data_source or "qmt").strip().lower() or "qmt"

    if normalized_source == "akshare":
        return _sync_index_minute_history_via_akshare(
            start=start,
            end=end,
            symbols=normalized_symbols,
            progress_callback=progress_callback,
        )

    for index, trade_day in enumerate(trade_dates, start=1):
        if callable(progress_callback):
            progress = min(95, max(1, int((index - 1) / max(total_days, 1) * 100)))
            progress_callback(
                progress,
                f"正在同步指数分钟线 {trade_day.isoformat()} ({index}/{total_days})",
            )

        payload = _fetch_intraday_payload_safe(
            normalized_symbols,
            trade_date=trade_day.isoformat(),
            period="1m",
            account_key=resolved_account_key,
        )
        if _payload_all_symbols_minute_unsupported(payload, normalized_symbols):
            if normalized_source == "qmt":
                error_message = (
                    "当前 Windows QMT 客户端不支持指数分钟历史接口（function not realize / ErrorID 300000）。"
                    "请升级 QMT 客户端/投研版，或改用非 QMT 的指数分钟历史补数方案。"
                )
                if callable(progress_callback):
                    progress_callback(0, error_message)
                return {
                    "success": False,
                    "rows": total_rows,
                    "symbols": normalized_symbols,
                    "trade_dates": [item.isoformat() for item in trade_dates[:index]],
                    "day_rows": day_rows,
                    "missing_symbols": normalized_symbols,
                    "source": "qmt_intraday",
                    "error": error_message,
                    "symbol_errors": payload.get("symbol_errors") or {},
                }
            return _sync_index_minute_history_via_akshare(
                start=start,
                end=end,
                symbols=normalized_symbols,
                progress_callback=progress_callback,
                upstream_error=payload.get("symbol_errors") or {},
            )
        rows = _normalize_intraday_payload(payload)
        index_rows = [item for item in rows if is_index_symbol(item.get("symbol"))]
        inserted = _upsert_intraday_rows("index_minute_kline", index_rows) if index_rows else 0
        total_rows += inserted
        day_rows[trade_day.isoformat()] = inserted
        present_symbols = {normalize_market_symbol(item.get("symbol")) for item in index_rows if item.get("symbol")}
        missing_symbols.update(symbol for symbol in normalized_symbols if symbol not in present_symbols)

    if callable(progress_callback):
        progress_callback(100, f"指数分钟线同步完成，共处理 {total_days} 个交易日")

    return {
        "success": True,
        "rows": total_rows,
        "symbols": normalized_symbols,
        "trade_dates": [item.isoformat() for item in trade_dates],
        "day_rows": day_rows,
        "missing_symbols": sorted(missing_symbols),
        "source": "qmt_intraday",
    }


def build_market_integrity_report(
    db: Session,
    *,
    target_date: str | None = None,
) -> dict[str, Any]:
    report_date = _parse_trade_date(target_date) if target_date else datetime.now(CN_TZ).date()
    daily_table = preferred_daily_kline_table()
    minute_table = preferred_minute_kline_table()
    tables = {
        daily_table: _table_integrity_snapshot(db, daily_table, "trade_date", expected_symbols=None, target_date=report_date),
        minute_table: _table_integrity_snapshot(db, minute_table, "trade_time", expected_symbols=None, target_date=report_date),
        "index_daily_kline": _table_integrity_snapshot(
            db,
            "index_daily_kline",
            "trade_date",
            expected_symbols=[item["symbol"] for item in MAJOR_INDEX_PRESETS],
            target_date=report_date,
        ),
        "index_minute_kline": _table_integrity_snapshot(
            db,
            "index_minute_kline",
            "trade_time",
            expected_symbols=[item["symbol"] for item in MAJOR_INDEX_PRESETS],
            target_date=report_date,
        ),
    }
    return {
        "generated_at": datetime.now(CN_TZ).isoformat(),
        "target_date": report_date.isoformat(),
        "tables": tables,
    }


def _resolve_market_config(
    account_key: str | None,
    *,
    db: Session | None = None,
    user_id: str | None = None,
) -> qmt_virtual_account_service.QmtRuntimeConfig:
    preferred_key = resolve_market_account_key(db=db, user_id=user_id, preferred_account_key=account_key)
    return qmt_virtual_account_service._resolve_runtime_config(preferred_key, db=db, user_id=user_id)


def resolve_market_account_key(
    *,
    db: Session | None = None,
    user_id: str | None = None,
    preferred_account_key: str | None = None,
) -> str | None:
    preferred_key = str(preferred_account_key or settings.qmt_history_account_key or "").strip()
    configs = qmt_virtual_account_service._load_runtime_configs(db=db, user_id=user_id)

    def _usable(config: qmt_virtual_account_service.QmtRuntimeConfig) -> bool:
        return bool(config.enabled and (config.bridge_base_url or config.userdata_path))

    if preferred_key:
        preferred = next((config for config in configs if config.key == preferred_key), None)
        if preferred is not None and _usable(preferred):
            return preferred.key

    for role in ("paper", "live"):
        for config in configs:
            if config.role == role and _usable(config):
                return config.key

    for config in configs:
        if config.enabled:
            return config.key

    return preferred_key or (configs[0].key if configs else None)


def _sync_index_daily_history_via_akshare(
    *,
    start: date,
    end: date,
    symbols: list[str],
    progress_callback: Any | None,
) -> dict[str, Any]:
    try:
        import akshare as ak
    except Exception as exc:
        return {
            "success": False,
            "rows": 0,
            "symbols": symbols,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "missing_symbols": symbols,
            "symbol_rows": {},
            "source": "akshare_index_daily",
            "error": f"AKShare 不可用：{exc}",
        }

    total_symbols = len(symbols)
    total_rows = 0
    symbol_rows: dict[str, int] = {}
    missing_symbols: list[str] = []
    symbol_errors: dict[str, str] = {}

    for index, symbol in enumerate(symbols, start=1):
        if callable(progress_callback):
            progress = min(95, max(1, int((index - 1) / max(total_symbols, 1) * 100)))
            progress_callback(progress, f"正在通过 AKShare 同步指数日K {symbol} ({index}/{total_symbols})")
        try:
            rows = _fetch_index_daily_rows_via_akshare_symbol(ak, symbol, start=start, end=end)
            inserted = _upsert_index_daily_rows(rows) if rows else 0
        except Exception as exc:
            inserted = 0
            rows = []
            symbol_errors[symbol] = str(exc)[:300]
            logger.warning("[qmt-market] akshare index daily sync failed symbol=%s error=%s", symbol, exc)

        symbol_rows[symbol] = inserted
        total_rows += inserted
        if not rows:
            missing_symbols.append(symbol)

    if callable(progress_callback):
        progress_callback(100 if total_rows else 0, f"AKShare 指数日K同步完成，共写入 {total_rows} 条")

    return {
        "success": total_rows > 0,
        "rows": total_rows,
        "symbols": symbols,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "missing_symbols": missing_symbols,
        "symbol_rows": symbol_rows,
        "symbol_errors": symbol_errors,
        "source": "akshare_index_daily",
    }


def _fetch_index_daily_rows_via_akshare_symbol(
    ak: Any,
    symbol: str,
    *,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    normalized = normalize_market_symbol(symbol)
    code = normalized.split(".", 1)[0] if "." in normalized else normalized
    start_text = start.strftime("%Y%m%d")
    end_text = end.strftime("%Y%m%d")

    try:
        frame = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start_text, end_date=end_text)
        rows = _normalize_akshare_index_daily_frame(
            frame,
            symbol=normalized,
            start=start,
            end=end,
            source="akshare_em",
        )
        if rows:
            return rows
    except Exception as exc:
        logger.info("[qmt-market] akshare index_zh_a_hist fallback symbol=%s error=%s", normalized, exc)

    frame = ak.stock_zh_index_daily(symbol=_sina_index_symbol(normalized))
    return _normalize_akshare_index_daily_frame(
        frame,
        symbol=normalized,
        start=start,
        end=end,
        source="akshare_sina",
    )


def _normalize_akshare_index_daily_frame(
    frame: Any,
    *,
    symbol: str,
    start: date,
    end: date,
    source: str,
) -> list[dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    rows: list[dict[str, Any]] = []
    for raw_row in frame.to_dict("records"):
        trade_day = _parse_trade_date(_row_first_value(raw_row, "日期", "date", "交易日期", "trade_date"))
        if trade_day is None or trade_day < start or trade_day > end:
            continue
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_day,
                "open": _safe_float(_row_first_value(raw_row, "开盘", "open")),
                "high": _safe_float(_row_first_value(raw_row, "最高", "high")),
                "low": _safe_float(_row_first_value(raw_row, "最低", "low")),
                "close": _safe_float(_row_first_value(raw_row, "收盘", "close")),
                "volume": _safe_float(_row_first_value(raw_row, "成交量", "volume")),
                "amount": _safe_float(_row_first_value(raw_row, "成交额", "amount")),
                "source": source,
            }
        )
    rows.sort(key=lambda item: (item["symbol"], item["trade_date"]))
    return rows


def _row_first_value(row: dict[str, Any], *columns: str) -> Any:
    for column in columns:
        if column not in row:
            continue
        value = row.get(column)
        if value is None:
            continue
        text_value = str(value).strip()
        if not text_value or text_value.lower() in {"nan", "nat", "none"}:
            continue
        return value
    return None


def _sina_index_symbol(symbol: str) -> str:
    normalized = normalize_market_symbol(symbol)
    code = normalized.split(".", 1)[0] if "." in normalized else normalized
    if normalized.endswith(".SH") or code.startswith(("0", "5", "6", "9")):
        return f"sh{code}"
    if normalized.endswith(".BJ") or code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


def _sync_index_minute_history_via_akshare(
    *,
    start: date,
    end: date,
    symbols: list[str],
    progress_callback: Any | None,
    upstream_error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    supported_dates = _recent_trade_dates(limit=5)
    if not supported_dates:
        return {
            "success": False,
            "rows": 0,
            "symbols": symbols,
            "missing_symbols": symbols,
            "source": "akshare_index_minute",
            "error": "无法确定 AKShare 指数分钟线可用交易日窗口。",
            "upstream_error": upstream_error or {},
        }
    supported_start = supported_dates[0]
    supported_end = supported_dates[-1]
    if start < supported_start or end > supported_end:
        error_message = (
            f"AKShare 指数 1 分钟数据当前仅能正式补最近 5 个交易日："
            f"{supported_start.isoformat()} ~ {supported_end.isoformat()}。"
            f"请求区间为 {start.isoformat()} ~ {end.isoformat()}，超出可补范围。"
        )
        if callable(progress_callback):
            progress_callback(0, error_message)
        return {
            "success": False,
            "rows": 0,
            "symbols": symbols,
            "missing_symbols": symbols,
            "source": "akshare_index_minute",
            "error": error_message,
            "supported_range": {
                "start_date": supported_start.isoformat(),
                "end_date": supported_end.isoformat(),
            },
            "upstream_error": upstream_error or {},
        }

    import akshare as ak

    total_rows = 0
    day_rows: dict[str, int] = {}
    missing_symbols: set[str] = set()
    payload_rows: list[dict[str, Any]] = []
    total_symbols = len(symbols)
    start_ts = f"{start.isoformat()} 09:30:00"
    end_ts = f"{end.isoformat()} 15:00:00"

    for index, symbol in enumerate(symbols, start=1):
        if callable(progress_callback):
            progress = min(95, max(1, int((index - 1) / max(total_symbols, 1) * 100)))
            progress_callback(progress, f"正在通过 AKShare 同步指数分钟线 {symbol} ({index}/{total_symbols})")
        code = symbol.split(".", 1)[0]
        try:
            frame = ak.index_zh_a_hist_min_em(
                symbol=code,
                period="1",
                start_date=start_ts,
                end_date=end_ts,
            )
        except Exception as exc:
            missing_symbols.add(symbol)
            logger.warning("[qmt-market] akshare index minute fetch failed symbol=%s error=%s", symbol, exc)
            continue
        rows = _normalize_akshare_index_minute_frame(symbol, frame)
        if not rows:
            missing_symbols.add(symbol)
            continue
        payload_rows.extend(rows)
        total_rows += len(rows)
        for item in rows:
            day_key = str(item["trade_time"])[:10]
            day_rows[day_key] = day_rows.get(day_key, 0) + 1

    if payload_rows:
        _upsert_intraday_rows("index_minute_kline", payload_rows)
    if callable(progress_callback):
        progress_callback(100, f"AKShare 指数分钟线同步完成，共写入 {total_rows} 条")
    return {
        "success": total_rows > 0,
        "rows": total_rows,
        "symbols": symbols,
        "trade_dates": sorted(day_rows.keys()),
        "day_rows": day_rows,
        "missing_symbols": sorted(missing_symbols),
        "source": "akshare_index_minute",
        "supported_range": {
            "start_date": supported_start.isoformat(),
            "end_date": supported_end.isoformat(),
        },
        "upstream_error": upstream_error or {},
    }


def _fetch_intraday_payload_safe(
    symbols: list[str],
    *,
    trade_date: str,
    period: str,
    account_key: str | None,
    db: Session | None = None,
    user_id: str | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    config = _resolve_market_config(account_key, db=db, user_id=user_id)
    try:
        if config.bridge_base_url:
            payload = _bridge_post(
                config,
                "/market/minute-bars",
                {"symbols": symbols, "trade_date": trade_date, "period": period},
                timeout_seconds=timeout_seconds,
            )
            return payload if isinstance(payload, dict) else {"items": []}
        rows = _fetch_intraday_rows_via_local_xt(symbols, trade_date=trade_date, period=period)
        return {"items": rows, "rows": len(rows), "symbol_errors": {}}
    except Exception as exc:
        logger.warning("[qmt-market] intraday payload fetch failed symbols=%s trade_date=%s error=%s", len(symbols), trade_date, exc)
        return {"items": [], "rows": 0, "symbol_errors": {"__request__": {"message": str(exc), "unsupported": False}}}


def _payload_all_symbols_minute_unsupported(payload: dict[str, Any], symbols: list[str]) -> bool:
    errors = payload.get("symbol_errors")
    if not isinstance(errors, dict) or not errors:
        return False
    normalized = _normalize_symbols(symbols)
    if not normalized:
        return False
    matched = 0
    for symbol in normalized:
        item = errors.get(symbol)
        if not isinstance(item, dict) or not bool(item.get("unsupported")):
            return False
        matched += 1
    return matched == len(normalized)


def _recent_trade_dates(*, limit: int) -> list[date]:
    calendar_dates, _ = _load_cn_trade_dates()
    today = datetime.now(CN_TZ).date()
    if calendar_dates:
        recent = [item for item in calendar_dates if item <= today]
        return recent[-limit:] if len(recent) >= limit else recent
    result: list[date] = []
    cursor = today
    while len(result) < limit:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor -= timedelta(days=1)
    result.reverse()
    return result


def _normalize_akshare_index_minute_frame(symbol: str, frame: Any) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except Exception:
        pd = None
    if frame is None or pd is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    data = frame.copy()
    rename_map = {
        "时间": "trade_time",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
    }
    data = data.rename(columns=rename_map)
    required = ["trade_time", "open", "high", "low", "close", "volume", "amount"]
    if any(column not in data.columns for column in required):
        return []
    rows: list[dict[str, Any]] = []
    for item in data.to_dict("records"):
        trade_time = _normalize_timestamp(item.get("trade_time"))
        if not trade_time:
            continue
        rows.append(
            {
                "symbol": symbol,
                "trade_time": trade_time,
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
                "volume": int(float(item.get("volume") or 0)),
                "amount": _safe_float(item.get("amount")) or 0.0,
            }
        )
    rows.sort(key=lambda item: item["trade_time"])
    return rows


def _normalize_index_symbols(symbols: list[str] | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in symbols or []:
        symbol = _normalize_index_symbol(item)
        if symbol and symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


def _normalize_index_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if raw in _INDEX_SYMBOLS:
        return raw
    if "." in raw:
        normalized = normalize_market_symbol(raw)
        return normalized if normalized in _INDEX_SYMBOLS else ""
    if raw.startswith(("SH", "SZ", "BJ")) and len(raw) >= 8:
        prefix = raw[:2]
        code = raw[2:]
        candidate = _INDEX_CODE_TO_SYMBOL.get(code)
        return candidate if candidate and candidate.endswith(f".{prefix}") else ""
    return _INDEX_CODE_TO_SYMBOL.get(raw, "")


def _resolve_trade_dates(start: date, end: date) -> list[date]:
    calendar_dates, _ = _load_cn_trade_dates()
    if calendar_dates:
        return [item for item in calendar_dates if start <= item <= end]
    result: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            result.append(cursor)
        cursor += timedelta(days=1)
    return result


def _fetch_quotes_via_bridge(
    config: qmt_virtual_account_service.QmtRuntimeConfig,
    symbols: list[str],
    *,
    timeout_seconds: float | None = None,
) -> dict[str, dict[str, Any]]:
    try:
        payload = _bridge_post(config, "/market/quotes", {"symbols": symbols}, timeout_seconds=timeout_seconds)
        return _normalize_quote_payload(payload)
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        if response is not None and response.status_code == 404:
            logger.info("[qmt-market] bridge /market/quotes missing, fallback to minute-bars latest-close")
            return _derive_quotes_from_minute_bars(config, symbols)
        raise


def _fetch_quotes_via_local_xt(symbols: list[str]) -> dict[str, dict[str, Any]]:
    from xtquant import xtdata

    payload = xtdata.get_full_tick(symbols) or {}
    result: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        normalized = normalize_market_symbol(symbol)
        item = payload.get(normalized) if isinstance(payload, dict) else None
        quote = _normalize_quote_item(normalized, item or {}, source="qmt_local_xt")
        if quote.get("price") is not None:
            result[normalized] = quote
    return result


def _fetch_intraday_rows_safe(
    symbols: list[str],
    *,
    trade_date: str,
    period: str,
    account_key: str | None,
    db: Session | None = None,
    user_id: str | None = None,
) -> list[dict[str, Any]]:
    payload = _fetch_intraday_payload_safe(
        symbols,
        trade_date=trade_date,
        period=period,
        account_key=account_key,
        db=db,
        user_id=user_id,
    )
    return _normalize_intraday_payload(payload)


def _fetch_intraday_rows_via_local_xt(symbols: list[str], *, trade_date: str, period: str) -> list[dict[str, Any]]:
    from xtquant import xtdata

    trade_day = str(trade_date or "").replace("-", "").strip()
    start_time = f"{trade_day}000000"
    end_time = f"{trade_day}235959"
    rows: list[dict[str, Any]] = []
    reader = getattr(xtdata, "get_market_data_ex", None) or getattr(xtdata, "get_market_data", None)
    downloader = getattr(xtdata, "download_history_data2", None) or getattr(xtdata, "download_history_data", None)
    if reader is None or downloader is None:
        return []
    for symbol in symbols:
        normalized = normalize_market_symbol(symbol)
        try:
            try:
                downloader(normalized, period, start_time=start_time, end_time=end_time)
            except TypeError:
                downloader(normalized, period, start_time, end_time)
            try:
                raw = reader(
                    field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                    stock_list=[normalized],
                    period=period,
                    start_time=start_time,
                    end_time=end_time,
                    count=-1,
                    dividend_type="none",
                    fill_data=False,
                )
            except TypeError:
                raw = reader(["time", "open", "high", "low", "close", "volume", "amount"], [normalized], period, start_time, end_time, -1, "none", False)
        except Exception:
            continue
        rows.extend(_normalize_intraday_payload({"items": _extract_history_items(normalized, raw)}))
    return rows


def _fetch_daily_rows_safe(
    symbols: list[str],
    *,
    start_date: str,
    end_date: str,
    account_key: str | None,
    db: Session | None = None,
    user_id: str | None = None,
) -> list[dict[str, Any]]:
    config = _resolve_market_config(account_key, db=db, user_id=user_id)
    try:
        if config.bridge_base_url:
            payload = _bridge_post(
                config,
                "/market/daily-bars",
                {"symbols": symbols, "start_date": start_date, "end_date": end_date},
            )
            return _normalize_daily_payload(payload)
        return _fetch_daily_rows_via_local_xt(symbols, start_date=start_date, end_date=end_date)
    except Exception as exc:
        logger.warning("[qmt-market] daily fetch failed symbols=%s range=%s..%s error=%s", len(symbols), start_date, end_date, exc)
        return []


def _fetch_daily_rows_via_local_xt(symbols: list[str], *, start_date: str, end_date: str) -> list[dict[str, Any]]:
    from xtquant import xtdata

    start_time = str(start_date or "").replace("-", "").strip() + "000000"
    end_time = str(end_date or "").replace("-", "").strip() + "235959"
    rows: list[dict[str, Any]] = []
    reader = getattr(xtdata, "get_market_data_ex", None) or getattr(xtdata, "get_market_data", None)
    downloader = getattr(xtdata, "download_history_data2", None) or getattr(xtdata, "download_history_data", None)
    if reader is None or downloader is None:
        return rows
    for symbol in symbols:
        normalized = normalize_market_symbol(symbol)
        try:
            try:
                downloader(normalized, "1d", start_time=start_time, end_time=end_time)
            except TypeError:
                downloader(normalized, "1d", start_time, end_time)
            try:
                raw = reader(
                    field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                    stock_list=[normalized],
                    period="1d",
                    start_time=start_time,
                    end_time=end_time,
                    count=-1,
                    dividend_type="none",
                    fill_data=False,
                )
            except TypeError:
                raw = reader(["time", "open", "high", "low", "close", "volume", "amount"], [normalized], "1d", start_time, end_time, -1, "none", False)
        except Exception:
            continue
        rows.extend(_normalize_daily_payload({"items": _extract_history_items(normalized, raw)}))
    return rows


def _bridge_post(
    config: qmt_virtual_account_service.QmtRuntimeConfig,
    path: str,
    body: dict[str, Any],
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").rstrip("/")
    if not base_url:
        raise RuntimeError("bridge_base_url is empty")
    headers = {"Content-Type": "application/json"}
    if config.bridge_token:
        headers["Authorization"] = f"Bearer {config.bridge_token}"
    response = requests.post(
        f"{base_url}{path}",
        json=body,
        headers=headers,
        timeout=_normalize_timeout_seconds(timeout_seconds),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected bridge payload: {type(payload).__name__}")
    return payload


def _normalize_timeout_seconds(timeout_seconds: float | None, *, default: float = 30.0) -> float:
    try:
        value = float(timeout_seconds) if timeout_seconds is not None else float(default)
    except Exception:
        value = float(default)
    return max(value, 1.0)


def _normalize_quote_payload(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = normalize_market_symbol(item.get("symbol"))
        if not symbol:
            continue
        quote = _normalize_quote_item(symbol, item, source=str(item.get("source") or "qmt_bridge"))
        if quote.get("price") is not None:
            result[symbol] = quote
            result[symbol.split(".", 1)[0]] = quote
    return result


def _normalize_quote_item(symbol: str, item: dict[str, Any], *, source: str) -> dict[str, Any]:
    price = _safe_float(
        item.get("price")
        or item.get("lastPrice")
        or item.get("last_price")
        or item.get("last")
        or item.get("close")
    )
    previous_close = _safe_float(
        item.get("previous_close")
        or item.get("lastClose")
        or item.get("last_close")
        or item.get("preClose")
        or item.get("prevClose")
    )
    change = _safe_float(item.get("change"))
    if change is None and price is not None and previous_close not in (None, 0):
        change = round(price - float(previous_close), 4)
    change_pct = _safe_float(item.get("change_pct") or item.get("pct_chg") or item.get("changePercent"))
    if change_pct is None and change is not None and previous_close not in (None, 0):
        change_pct = round(change / float(previous_close) * 100, 4)
    return {
        "symbol": symbol,
        "name": _INDEX_NAMES.get(symbol),
        "price": price,
        "open": _safe_float(item.get("open")),
        "high": _safe_float(item.get("high")),
        "low": _safe_float(item.get("low")),
        "previous_close": previous_close,
        "change": change,
        "change_pct": change_pct,
        "volume": _safe_float(item.get("volume")),
        "amount": _safe_float(item.get("amount") or item.get("turnover")),
        "quote_time": _normalize_timestamp(item.get("quote_time") or item.get("time") or item.get("timetag")),
        "source": source,
    }


def _normalize_intraday_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = normalize_market_symbol(item.get("symbol"))
        trade_time = _normalize_timestamp(item.get("trade_time") or item.get("time"))
        if not symbol or not trade_time:
            continue
        rows.append(
            {
                "symbol": symbol,
                "trade_time": trade_time,
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
                "volume": int(float(item.get("volume") or 0)),
                "amount": _safe_float(item.get("amount")) or 0.0,
            }
        )
    rows.sort(key=lambda item: (item["symbol"], item["trade_time"]))
    return rows


def _derive_quotes_from_minute_bars(
    config: qmt_virtual_account_service.QmtRuntimeConfig,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    trade_date = datetime.now(CN_TZ).date().isoformat()
    payload = _bridge_post(
        config,
        "/market/minute-bars",
        {"symbols": symbols, "trade_date": trade_date, "period": "1m"},
    )
    rows = _normalize_intraday_payload(payload)
    latest_by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest_by_symbol[row["symbol"]] = row
    result: dict[str, dict[str, Any]] = {}
    for symbol, row in latest_by_symbol.items():
        quote = {
            "symbol": symbol,
            "name": _INDEX_NAMES.get(symbol),
            "price": _safe_float(row.get("close")),
            "open": _safe_float(row.get("open")),
            "high": _safe_float(row.get("high")),
            "low": _safe_float(row.get("low")),
            "previous_close": None,
            "change": None,
            "change_pct": None,
            "volume": _safe_float(row.get("volume")),
            "amount": _safe_float(row.get("amount")),
            "quote_time": str(row.get("trade_time") or ""),
            "source": "qmt_bridge:min1_fallback",
        }
        result[symbol] = quote
        result[symbol.split(".", 1)[0]] = quote
    return result


def _normalize_daily_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        symbol = normalize_market_symbol(item.get("symbol"))
        trade_date = _normalize_trade_date(item.get("trade_date") or item.get("trade_time") or item.get("time"))
        if not symbol or not trade_date:
            continue
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
                "volume": _safe_float(item.get("volume")),
                "amount": _safe_float(item.get("amount")),
                "source": str(item.get("source") or "qmt_bridge"),
            }
        )
    rows.sort(key=lambda item: (item["symbol"], item["trade_date"]))
    return rows


def _extract_history_items(symbol: str, payload: Any) -> list[dict[str, Any]]:
    try:
        import pandas as pd
    except Exception:
        pd = None

    frame = None
    if pd is not None and isinstance(payload, pd.DataFrame):
        frame = payload
    elif isinstance(payload, dict):
        candidates = [symbol, symbol.lower(), symbol.split(".", 1)[0], symbol.split(".", 1)[0].lower()]
        for key in candidates:
            value = payload.get(key)
            if pd is not None and isinstance(value, pd.DataFrame):
                frame = value
                break
            if isinstance(value, dict):
                frame = pd.DataFrame(value) if pd is not None else None
                break
            if isinstance(value, list):
                frame = pd.DataFrame(value) if pd is not None else None
                break
        if frame is None and any(name in payload for name in ("time", "open", "high", "low", "close")) and pd is not None:
            frame = pd.DataFrame(payload)
    elif isinstance(payload, list) and pd is not None:
        frame = pd.DataFrame(payload)
    if frame is None or pd is None or frame.empty:
        return []
    data = frame.copy()
    if "time" not in data.columns and "trade_time" in data.columns:
        data["time"] = data["trade_time"]
    data = data.rename(columns={"Time": "time", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume", "Amount": "amount"})
    items: list[dict[str, Any]] = []
    for row in data.to_dict("records"):
        items.append({"symbol": symbol, **row})
    return items


def _minute_table_name(symbol: str) -> str:
    return "index_minute_kline" if is_index_symbol(symbol) else preferred_minute_kline_table()


def _load_intraday_rows_from_db(table_name: str, symbol: str, trade_date: str) -> list[dict[str, Any]]:
    if not _has_table(table_name):
        return []
    symbols = _symbol_query_variants(symbol)
    with SessionLocal() as db:
        rows = db.execute(
            text(
                f"""
                SELECT symbol, trade_time, open, high, low, close, volume, amount
                FROM {table_name}
                WHERE symbol IN :symbols
                  AND DATE(trade_time) = :trade_date
                ORDER BY trade_time ASC
                """
            ).bindparams(bindparam("symbols", expanding=True)),
            {"symbols": symbols, "trade_date": trade_date},
        ).mappings().all()
    deduped: dict[Any, dict[str, Any]] = {}
    for row in rows:
        trade_time = _normalize_timestamp(row["trade_time"])
        record = {
            "symbol": normalize_market_symbol(row["symbol"]),
            "trade_time": trade_time,
            "open": _safe_float(row["open"]),
            "high": _safe_float(row["high"]),
            "low": _safe_float(row["low"]),
            "close": _safe_float(row["close"]),
            "volume": int(row["volume"] or 0),
            "amount": _safe_float(row["amount"]) or 0.0,
        }
        previous = deduped.get(trade_time)
        if previous is None or record["symbol"] == normalize_market_symbol(symbol):
            deduped[trade_time] = record
    return [
        deduped[key]
        for key in sorted(deduped)
    ]


def _symbol_query_variants(symbol: str) -> list[str]:
    normalized = normalize_market_symbol(symbol)
    variants = {normalized}
    if "." in normalized:
        variants.add(normalized.split(".", 1)[0])
    return sorted(item for item in variants if item)


def _load_daily_rows_from_db(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    if not _has_table("index_daily_kline"):
        return []
    with SessionLocal() as db:
        rows = db.execute(
            text(
                """
                SELECT symbol, trade_date, open, high, low, close, volume, amount, source
                FROM index_daily_kline
                WHERE symbol = :symbol
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                ORDER BY trade_date ASC
                """
            ),
            {"symbol": symbol, "start_date": start_date, "end_date": end_date},
        ).mappings().all()
    return [
        {
            "symbol": str(row["symbol"]),
            "trade_date": row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"]),
            "open": _safe_float(row["open"]),
            "high": _safe_float(row["high"]),
            "low": _safe_float(row["low"]),
            "close": _safe_float(row["close"]),
            "volume": _safe_float(row["volume"]),
            "amount": _safe_float(row["amount"]),
            "source": str(row.get("source") or "postgresql"),
        }
        for row in rows
    ]


def _upsert_intraday_rows(table_name: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    columns = _load_table_columns(table_name)
    has_created_at = "created_at" in columns
    has_updated_at = "updated_at" in columns
    payload: list[dict[str, Any]] = []
    now = datetime.now(CN_TZ).replace(tzinfo=None)
    for item in rows:
        symbol = normalize_market_symbol(item.get("symbol"))
        if table_name == "stock_minute_kline" and is_index_symbol(symbol):
            logger.warning("[qmt-market] skip index row for stock_minute_kline symbol=%s", symbol)
            continue
        if table_name == "index_minute_kline" and not is_index_symbol(symbol):
            logger.warning("[qmt-market] skip stock row for index_minute_kline symbol=%s", symbol)
            continue
        row = {
            "symbol": symbol,
            "trade_time": _parse_datetime(item.get("trade_time")),
            "open": _safe_float(item.get("open")),
            "high": _safe_float(item.get("high")),
            "low": _safe_float(item.get("low")),
            "close": _safe_float(item.get("close")),
            "volume": int(float(item.get("volume") or 0)),
            "amount": _safe_float(item.get("amount")) or 0.0,
        }
        if not row["symbol"] or row["trade_time"] is None:
            continue
        if has_created_at:
            row["created_at"] = now
        if has_updated_at:
            row["updated_at"] = now
        payload.append(row)
    if not payload:
        return 0
    insert_columns = ["symbol", "trade_time", "open", "high", "low", "close", "volume", "amount"]
    if has_created_at:
        insert_columns.append("created_at")
    if has_updated_at:
        insert_columns.append("updated_at")
    update_sql = [
        "open = EXCLUDED.open",
        "high = EXCLUDED.high",
        "low = EXCLUDED.low",
        "close = EXCLUDED.close",
        "volume = EXCLUDED.volume",
        "amount = EXCLUDED.amount",
    ]
    if has_updated_at:
        update_sql.append("updated_at = EXCLUDED.updated_at")
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {table_name} ({", ".join(insert_columns)})
                VALUES ({", ".join(f":{column}" for column in insert_columns)})
                ON CONFLICT (symbol, trade_time) DO UPDATE SET
                    {", ".join(update_sql)}
                """
            ),
            payload,
        )
    if table_name == "stock_minute_kline":
        try:
            result = ingest_raw_minute_rows(source="qmt", rows=payload)
            for trade_day in result.get("trade_dates") or []:
                affected_symbols = sorted({str(item["symbol"]) for item in payload if item.get("symbol")})
                publish_minute_trade_date_batched(
                    trade_date=trade_day,
                    symbols=affected_symbols or None,
                    minimum_coverage_ratio=0.0,
                )
        except Exception as exc:
            logger.warning("[qmt-market] raw/published minute pipeline update failed rows=%s error=%s", len(payload), exc)
    return len(payload)


def _upsert_index_daily_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    columns = _load_table_columns("index_daily_kline")
    has_created_at = "created_at" in columns
    has_updated_at = "updated_at" in columns
    has_source = "source" in columns
    payload: list[dict[str, Any]] = []
    now = datetime.now(CN_TZ).replace(tzinfo=None)
    for item in rows:
        trade_date = _normalize_trade_date(item.get("trade_date"))
        symbol = normalize_market_symbol(item.get("symbol"))
        if not trade_date or not symbol:
            continue
        row = {
            "symbol": symbol,
            "trade_date": trade_date,
            "open": _safe_float(item.get("open")),
            "high": _safe_float(item.get("high")),
            "low": _safe_float(item.get("low")),
            "close": _safe_float(item.get("close")),
            "volume": _safe_float(item.get("volume")),
            "amount": _safe_float(item.get("amount")),
        }
        if has_source:
            row["source"] = str(item.get("source") or "qmt_bridge")[:20]
        if has_created_at:
            row["created_at"] = now
        if has_updated_at:
            row["updated_at"] = now
        payload.append(row)
    if not payload:
        return 0
    insert_columns = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
    if has_source:
        insert_columns.append("source")
    if has_created_at:
        insert_columns.append("created_at")
    if has_updated_at:
        insert_columns.append("updated_at")
    update_sql = [
        "open = EXCLUDED.open",
        "high = EXCLUDED.high",
        "low = EXCLUDED.low",
        "close = EXCLUDED.close",
        "volume = EXCLUDED.volume",
        "amount = EXCLUDED.amount",
    ]
    if has_source:
        update_sql.append("source = EXCLUDED.source")
    if has_updated_at:
        update_sql.append("updated_at = EXCLUDED.updated_at")
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO index_daily_kline ({", ".join(insert_columns)})
                VALUES ({", ".join(f":{column}" for column in insert_columns)})
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    {", ".join(update_sql)}
                """
            ),
            payload,
        )
    return len(payload)


def _table_integrity_snapshot(
    db: Session,
    table_name: str,
    date_column: str,
    *,
    expected_symbols: list[str] | None,
    target_date: date,
) -> dict[str, Any]:
    if not _has_table_with_db(db, table_name):
        return {
            "table": table_name,
            "available": False,
            "coverage_start": None,
            "coverage_end": None,
            "symbol_count": 0,
            "trading_day_count": 0,
            "missing_trading_days": [],
            "missing_symbols": expected_symbols or [],
            "last_sync_time": None,
        }
    date_expr = f"DATE({date_column})" if date_column == "trade_time" else date_column
    sync_expr = _last_sync_expr(db, table_name)
    row = db.execute(
        text(
            f"""
            SELECT
                MIN({date_expr}) AS coverage_start,
                MAX({date_expr}) AS coverage_end,
                COUNT(DISTINCT symbol) AS symbol_count,
                COUNT(DISTINCT {date_expr}) AS trading_day_count,
                MAX({sync_expr}) AS last_sync_time
            FROM {table_name}
            """
        )
    ).mappings().first()
    coverage_start = row["coverage_start"]
    coverage_end = row["coverage_end"]
    present_symbols = {
        str(item[0]).upper()
        for item in db.execute(text(f"SELECT DISTINCT symbol FROM {table_name}")).fetchall()
        if item and item[0]
    }
    missing_symbols = []
    if expected_symbols:
        missing_symbols = [symbol for symbol in expected_symbols if symbol.upper() not in present_symbols]
    missing_days = _compute_missing_trading_days(db, table_name, date_expr, coverage_start, coverage_end, target_date)
    return {
        "table": table_name,
        "available": True,
        "coverage_start": coverage_start.isoformat() if hasattr(coverage_start, "isoformat") else coverage_start,
        "coverage_end": coverage_end.isoformat() if hasattr(coverage_end, "isoformat") else coverage_end,
        "symbol_count": int(row["symbol_count"] or 0),
        "trading_day_count": int(row["trading_day_count"] or 0),
        "missing_trading_days": missing_days,
        "missing_symbols": missing_symbols,
        "last_sync_time": _normalize_timestamp(row["last_sync_time"]),
    }


def _compute_missing_trading_days(
    db: Session,
    table_name: str,
    date_expr: str,
    coverage_start: Any,
    coverage_end: Any,
    target_date: date,
) -> list[str]:
    if coverage_start is None:
        return []
    start = coverage_start if isinstance(coverage_start, date) else _parse_trade_date(str(coverage_start))
    end = coverage_end if isinstance(coverage_end, date) else _parse_trade_date(str(coverage_end or target_date.isoformat()))
    if start is None or end is None:
        return []
    end = min(end, target_date)
    calendar_dates, _ = _load_cn_trade_dates()
    if calendar_dates:
        expected = [item for item in calendar_dates if start <= item <= end]
    else:
        expected = []
        cursor = start
        while cursor <= end:
            if cursor.weekday() < 5:
                expected.append(cursor)
            cursor += timedelta(days=1)
    existing = {
        item[0] for item in db.execute(text(f"SELECT DISTINCT {date_expr} AS trade_day FROM {table_name}")).fetchall() if item and item[0]
    }
    missing = [item.isoformat() for item in expected if item not in existing]
    return missing[:200]


def _last_sync_expr(db: Session, table_name: str) -> str:
    columns = {column["name"] for column in inspect(db.bind).get_columns(table_name)}
    if "updated_at" in columns:
        return "updated_at"
    if "created_at" in columns:
        return "created_at"
    return "CURRENT_TIMESTAMP"


def _load_table_columns(table_name: str) -> set[str]:
    try:
        return {str(column["name"]) for column in inspect(engine).get_columns(table_name)}
    except Exception:
        return set()


def _has_table(table_name: str) -> bool:
    try:
        return inspect(engine).has_table(table_name)
    except Exception:
        return False


def _has_table_with_db(db: Session, table_name: str) -> bool:
    try:
        return inspect(db.bind).has_table(table_name)
    except Exception:
        return False


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in symbols:
        symbol = normalize_market_symbol(item)
        if symbol and symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


def _normalize_timestamp(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_trade_date(value: Any) -> str | None:
    parsed = _parse_trade_date(value)
    return parsed.isoformat() if parsed else None


def _parse_trade_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = _parse_datetime(value)
    if parsed is not None:
        return parsed.date()
    raw = str(value or "").strip()
    if len(raw) == 10:
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, (int, float)):
        number = int(value)
        digits = len(str(abs(number)))
        if digits >= 18:
            return datetime.fromtimestamp(number / 1_000_000_000)
        if digits >= 16:
            return datetime.fromtimestamp(number / 1_000_000)
        if digits >= 13:
            return datetime.fromtimestamp(number / 1000)
        if digits >= 10:
            return datetime.fromtimestamp(number)
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if len(raw) == 8 and raw.isdigit():
        try:
            return datetime.strptime(raw, "%Y%m%d")
        except ValueError:
            return None
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None
