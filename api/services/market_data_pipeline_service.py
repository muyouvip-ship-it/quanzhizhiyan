from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy import bindparam, inspect, text

from api.database import SessionLocal, engine
from api.core.utils import safe_float as _safe_float


logger = logging.getLogger(__name__)

DAILY_RAW_TABLES = {
    "postgresql": "raw_stock_daily_kline_postgresql",
    "quantclass": "raw_stock_daily_kline_quantclass",
    "akshare": "raw_stock_daily_kline_akshare",
    "baostock": "raw_stock_daily_kline_baostock",
    "efinance": "raw_stock_daily_kline_efinance",
}
MINUTE_RAW_TABLES = {
    "postgresql": "raw_stock_minute_kline_postgresql",
    "qmt": "raw_stock_minute_kline_qmt",
    "tdx": "raw_stock_minute_kline_tdx",
    "akshare": "raw_stock_minute_kline_akshare",
}
DAILY_SOURCE_PRIORITY = {"quantclass": 110, "akshare": 100, "baostock": 80, "postgresql": 70, "efinance": 60}
MINUTE_SOURCE_PRIORITY = {"qmt": 100, "tdx": 90, "postgresql": 70, "akshare": 50}
MINUTE_EXPECTED_BARS = 240
MINUTE_PUBLISH_MIN_RATIO = 0.98
FINAL_DAILY_KLINE_TABLE = "stock_daily_kline"
FINAL_MINUTE_KLINE_TABLE = "stock_minute_kline"


@dataclass
class PublicationSummary:
    published_count: int
    warning_count: int
    missing_count: int


def preferred_daily_kline_table() -> str:
    if _has_table(FINAL_DAILY_KLINE_TABLE):
        return FINAL_DAILY_KLINE_TABLE
    if _has_table("market_stock_daily_kline"):
        return "market_stock_daily_kline"
    if _table_has_rows("pub_stock_daily_kline"):
        return "pub_stock_daily_kline"
    return FINAL_DAILY_KLINE_TABLE


def preferred_minute_kline_table() -> str:
    if _has_table(FINAL_MINUTE_KLINE_TABLE):
        return FINAL_MINUTE_KLINE_TABLE
    if _has_table("market_stock_minute_kline"):
        return "market_stock_minute_kline"
    if _published_minute_reads_enabled() and _table_has_rows("pub_stock_minute_kline"):
        return "pub_stock_minute_kline"
    return FINAL_MINUTE_KLINE_TABLE


def published_daily_kline_table() -> str:
    return "pub_stock_daily_kline" if _has_table("pub_stock_daily_kline") else preferred_daily_kline_table()


def published_minute_kline_table() -> str:
    return "pub_stock_minute_kline" if _has_table("pub_stock_minute_kline") else preferred_minute_kline_table()


def ingest_raw_daily_rows(
    *,
    source: str,
    rows: Iterable[dict[str, Any]],
    batch_id: str | None = None,
) -> dict[str, Any]:
    normalized_source = str(source or "").strip().lower()
    table_name = DAILY_RAW_TABLES.get(normalized_source)
    if not table_name:
        return {"success": False, "rows": 0, "error": f"unsupported daily raw source: {source}"}
    payload, trade_dates = _normalize_daily_rows(rows, source=normalized_source, batch_id=batch_id or uuid4().hex)
    if not payload:
        return {"success": False, "rows": 0, "error": "empty daily rows"}

    with engine.begin() as conn:
        _upsert_daily_rows_to_table(conn, table_name, payload)
        _upsert_daily_rows_to_norm(conn, payload)

    return {
        "success": True,
        "rows": len(payload),
        "batch_id": payload[0]["batch_id"],
        "trade_dates": sorted(trade_dates),
    }


def reconcile_daily_trade_dates(
    *,
    trade_dates: Iterable[date | str],
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    normalized_dates = sorted({_normalize_trade_date(item) for item in trade_dates if _normalize_trade_date(item)})
    if not normalized_dates:
        return {"success": False, "published_count": 0, "warning_count": 0, "missing_count": 0, "error": "empty trade_dates"}

    published_count = 0
    warning_count = 0
    missing_count = 0

    with engine.begin() as conn:
        for trade_day in normalized_dates:
            run_id = uuid4().hex
            candidates = _load_daily_candidates(conn, trade_day=trade_day, symbols=symbols)
            grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
            for row in candidates:
                grouped[(str(row["symbol"]), trade_day)].append(row)

            published_rows: list[dict[str, Any]] = []
            recon_payload: list[dict[str, Any]] = []
            for (symbol, _), items in grouped.items():
                selected, publish_status, quality_status, issues, source_summary = _select_daily_candidate(items)
                if selected is None:
                    missing_count += 1
                    recon_payload.append(
                        _build_daily_recon_item(
                            run_id=run_id,
                            symbol=symbol,
                            trade_date=trade_day,
                            chosen_source=None,
                            publish_status="missing_or_conflicted",
                            quality_status="missing",
                            issues=issues,
                            source_summary=source_summary,
                            coverage_ratio=0.0,
                        )
                    )
                    continue

                if publish_status == "published_with_warning":
                    warning_count += 1
                else:
                    published_count += 1
                published_rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": trade_day,
                        "open": selected.get("open"),
                        "high": selected.get("high"),
                        "low": selected.get("low"),
                        "close": selected.get("close"),
                        "volume": selected.get("volume"),
                        "amount": selected.get("amount"),
                        "turnover_rate": selected.get("turnover_rate"),
                        "pre_close": selected.get("pre_close"),
                        "float_market_cap": selected.get("float_market_cap"),
                        "total_market_cap": selected.get("total_market_cap"),
                        "net_profit_ttm": selected.get("net_profit_ttm"),
                        "sw_industry_l1": selected.get("sw_industry_l1"),
                        "sw_industry_l2": selected.get("sw_industry_l2"),
                        "sw_industry_l3": selected.get("sw_industry_l3"),
                        "source": selected.get("source"),
                        "source_summary": _json_dumps(source_summary),
                        "quality_status": quality_status,
                        "publish_status": publish_status,
                        "freshness_status": "validated",
                        "coverage_ratio": 1.0,
                        "validation_sources": ",".join(sorted({item["source"] for item in items})),
                    }
                )
                recon_payload.append(
                    _build_daily_recon_item(
                        run_id=run_id,
                        symbol=symbol,
                        trade_date=trade_day,
                        chosen_source=str(selected.get("source") or ""),
                        publish_status=publish_status,
                        quality_status=quality_status,
                        issues=issues,
                        source_summary=source_summary,
                        coverage_ratio=1.0,
                    )
                )

            if published_rows:
                _upsert_published_daily_rows(conn, published_rows)
                _mirror_published_daily_rows_to_legacy(conn, published_rows)
            _upsert_daily_reconciliation_run(
                conn,
                run_id=run_id,
                trade_date=trade_day,
                published_count=len(published_rows),
                warning_count=sum(1 for item in recon_payload if item["publish_status"] == "published_with_warning"),
                missing_count=sum(1 for item in recon_payload if item["publish_status"] == "missing_or_conflicted"),
            )
            if recon_payload:
                _upsert_daily_reconciliation_items(conn, recon_payload)

    return {
        "success": True,
        "published_count": published_count,
        "warning_count": warning_count,
        "missing_count": missing_count,
    }


def ingest_raw_minute_rows(
    *,
    source: str,
    rows: Iterable[dict[str, Any]],
    batch_id: str | None = None,
) -> dict[str, Any]:
    normalized_source = str(source or "").strip().lower()
    table_name = MINUTE_RAW_TABLES.get(normalized_source)
    if not table_name:
        return {"success": False, "rows": 0, "error": f"unsupported minute raw source: {source}"}
    payload, trade_dates = _normalize_minute_rows(rows, source=normalized_source, batch_id=batch_id or uuid4().hex)
    if not payload:
        return {"success": False, "rows": 0, "error": "empty minute rows"}

    with engine.begin() as conn:
        _upsert_minute_rows_to_table(conn, table_name, payload)
        _upsert_minute_rows_to_norm(conn, payload)

    return {
        "success": True,
        "rows": len(payload),
        "batch_id": payload[0]["batch_id"],
        "trade_dates": sorted(trade_dates),
    }


def publish_minute_trade_date(
    *,
    trade_date: date | str,
    symbols: list[str] | None = None,
    minimum_coverage_ratio: float = MINUTE_PUBLISH_MIN_RATIO,
) -> dict[str, Any]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    if normalized_trade_date is None:
        return {"success": False, "published_count": 0, "warning_count": 0, "missing_count": 0, "error": "invalid trade_date"}

    published_count = 0
    warning_count = 0
    missing_count = 0
    with engine.begin() as conn:
        run_id = uuid4().hex
        rows = _load_minute_candidates(conn, trade_date=normalized_trade_date, symbols=symbols)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row["symbol"])].append(row)

        published_rows: list[dict[str, Any]] = []
        recon_payload: list[dict[str, Any]] = []
        for symbol, items in grouped.items():
            final_rows, publish_status, quality_status, source_mix, missing_times, coverage_ratio = _compose_minute_symbol_rows(
                symbol=symbol,
                trade_date=normalized_trade_date,
                items=items,
                minimum_coverage_ratio=minimum_coverage_ratio,
            )
            actual_bars = len(final_rows)
            if actual_bars == 0:
                missing_count += 1
            elif publish_status == "published_with_warning":
                warning_count += 1
            else:
                published_count += 1

            recon_payload.append(
                {
                    "run_id": run_id,
                    "symbol": symbol,
                    "trade_date": normalized_trade_date,
                    "chosen_source": source_mix[0] if source_mix else "",
                    "publish_status": publish_status,
                    "quality_status": quality_status,
                    "coverage_ratio": coverage_ratio,
                    "expected_bars": MINUTE_EXPECTED_BARS,
                    "actual_bars": actual_bars,
                    "missing_times": _json_dumps(missing_times),
                    "issues": _json_dumps(_minute_quality_issues(publish_status, quality_status, coverage_ratio, missing_times)),
                    "source_summary": _json_dumps({"source_mix": source_mix}),
                }
            )
            if not final_rows:
                continue
            for row in final_rows:
                row["primary_source"] = source_mix[0] if source_mix else ""
                row["source_mix"] = ",".join(source_mix)
                row["quality_status"] = quality_status
                row["publish_status"] = publish_status
                row["freshness_status"] = "validated" if coverage_ratio >= minimum_coverage_ratio else "raw_ingested"
                row["coverage_ratio"] = coverage_ratio
            published_rows.extend(final_rows)

        if published_rows:
            _upsert_published_minute_rows(conn, published_rows)
            _mirror_published_minute_rows_to_legacy(conn, published_rows)
        _upsert_minute_reconciliation_run(
            conn,
            run_id=run_id,
            trade_date=normalized_trade_date,
            published_count=published_count,
            warning_count=warning_count,
            missing_count=missing_count,
        )
        if recon_payload:
            _upsert_minute_reconciliation_items(conn, recon_payload)

    return {
        "success": True,
        "published_count": published_count,
        "warning_count": warning_count,
        "missing_count": missing_count,
    }


def sync_legacy_minute_to_raw(
    *,
    source: str,
    trade_date: date | str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    table_name = MINUTE_RAW_TABLES.get(str(source or "").strip().lower())
    if normalized_trade_date is None or table_name is None or not _has_table("stock_minute_kline"):
        return {"success": False, "rows": 0}

    query_symbols = _normalize_symbols(symbols or [])
    with SessionLocal() as db:
        if query_symbols:
            statement = text(
                """
                SELECT symbol, trade_time, open, high, low, close, volume, amount
                FROM stock_minute_kline
                WHERE DATE(trade_time) = :trade_date
                  AND symbol IN :symbols
                ORDER BY symbol, trade_time
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = db.execute(statement, {"trade_date": normalized_trade_date, "symbols": query_symbols}).mappings().all()
        else:
            rows = db.execute(
                text(
                    """
                    SELECT symbol, trade_time, open, high, low, close, volume, amount
                    FROM stock_minute_kline
                    WHERE DATE(trade_time) = :trade_date
                    ORDER BY symbol, trade_time
                    """
                ),
                {"trade_date": normalized_trade_date},
            ).mappings().all()
    result = ingest_raw_minute_rows(source=source, rows=[dict(row) for row in rows])
    if result.get("success"):
        publish_minute_trade_date(trade_date=normalized_trade_date, symbols=query_symbols or None, minimum_coverage_ratio=0.0)
    return result


def get_market_data_publish_status(
    *,
    trade_date: date | str | None = None,
    symbols: list[str] | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    normalized_trade_date = _normalize_trade_date(trade_date) if trade_date is not None else None
    normalized_symbols = _normalize_symbols(symbols or [])
    daily_table = preferred_daily_kline_table()
    minute_table = preferred_minute_kline_table()

    with SessionLocal() as db:
        resolved_trade_date = normalized_trade_date
        if resolved_trade_date is None and _has_table(daily_table):
            resolved_trade_date = _load_max_trade_date(db, table_name=daily_table)
        daily_summary = _load_daily_publish_summary(db, table_name=daily_table, trade_date=resolved_trade_date, symbols=normalized_symbols, limit=limit)
        minute_summary = _load_minute_publish_summary(db, table_name=minute_table, trade_date=resolved_trade_date, symbols=normalized_symbols, limit=limit)
        return {
            "trade_date": resolved_trade_date.isoformat() if hasattr(resolved_trade_date, "isoformat") else None,
            "daily": daily_summary,
            "minute": minute_summary,
            "tables": {
                "daily": daily_table,
                "minute": minute_table,
            },
            "published_tables": {
                "daily": published_daily_kline_table(),
                "minute": published_minute_kline_table(),
            },
        }


def _normalize_daily_rows(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
    batch_id: str,
) -> tuple[list[dict[str, Any]], set[date]]:
    payload: list[dict[str, Any]] = []
    trade_dates: set[date] = set()
    fetched_at = datetime.now().replace(microsecond=0)
    for item in rows:
        symbol = _normalize_symbol(item.get("symbol"))
        trade_date = _normalize_trade_date(item.get("trade_date") or item.get("date"))
        if not symbol or trade_date is None:
            continue
        trade_dates.add(trade_date)
        payload.append(
            {
                "source": source,
                "symbol": symbol,
                "trade_date": trade_date,
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
                "volume": _safe_float(item.get("volume")),
                "amount": _safe_float(item.get("amount")),
                "turnover_rate": _safe_float(item.get("turnover_rate") or item.get("turnover")),
                "pre_close": _safe_float(item.get("pre_close")),
                "float_market_cap": _safe_float(item.get("float_market_cap")),
                "total_market_cap": _safe_float(item.get("total_market_cap")),
                "net_profit_ttm": _safe_float(item.get("net_profit_ttm")),
                "sw_industry_l1": _safe_text(item.get("sw_industry_l1")),
                "sw_industry_l2": _safe_text(item.get("sw_industry_l2")),
                "sw_industry_l3": _safe_text(item.get("sw_industry_l3")),
                "batch_id": batch_id,
                "fetched_at": fetched_at,
            }
        )
    return payload, trade_dates


def _normalize_minute_rows(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
    batch_id: str,
) -> tuple[list[dict[str, Any]], set[date]]:
    payload: list[dict[str, Any]] = []
    trade_dates: set[date] = set()
    fetched_at = datetime.now().replace(microsecond=0)
    for item in rows:
        symbol = _normalize_symbol(item.get("symbol"))
        trade_time = _normalize_datetime(item.get("trade_time") or item.get("time"))
        if not symbol or trade_time is None:
            continue
        trade_date = trade_time.date()
        trade_dates.add(trade_date)
        payload.append(
            {
                "source": source,
                "symbol": symbol,
                "trade_time": trade_time,
                "trade_date": trade_date,
                "open": _safe_float(item.get("open")),
                "high": _safe_float(item.get("high")),
                "low": _safe_float(item.get("low")),
                "close": _safe_float(item.get("close")),
                "volume": _safe_float(item.get("volume")),
                "amount": _safe_float(item.get("amount")),
                "batch_id": batch_id,
                "fetched_at": fetched_at,
            }
        )
    return payload, trade_dates


def _load_daily_candidates(conn: Any, *, trade_day: date, symbols: list[str] | None) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    normalized_symbols = _normalize_symbols(symbols or [])
    for source, table_name in DAILY_RAW_TABLES.items():
        if not _has_table(table_name):
            continue
        if normalized_symbols:
            statement = text(
                f"""
                SELECT :source AS source, symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
                       float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3
                FROM {table_name}
                WHERE trade_date = :trade_date
                  AND symbol IN :symbols
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = conn.execute(statement, {"source": source, "trade_date": trade_day, "symbols": normalized_symbols}).mappings().all()
        else:
            rows = conn.execute(
                text(
                    f"""
                    SELECT :source AS source, symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
                           float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3
                    FROM {table_name}
                    WHERE trade_date = :trade_date
                    """
                ),
                {"source": source, "trade_date": trade_day},
            ).mappings().all()
        all_rows.extend(dict(row) for row in rows)
    return all_rows


def _load_minute_candidates(conn: Any, *, trade_date: date, symbols: list[str] | None) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    normalized_symbols = _normalize_symbols(symbols or [])
    for source, table_name in MINUTE_RAW_TABLES.items():
        if not _has_table(table_name):
            continue
        if normalized_symbols:
            statement = text(
                f"""
                SELECT :source AS source, symbol, trade_time, trade_date, open, high, low, close, volume, amount
                FROM {table_name}
                WHERE trade_date = :trade_date
                  AND symbol IN :symbols
                ORDER BY symbol, trade_time
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = conn.execute(statement, {"source": source, "trade_date": trade_date, "symbols": normalized_symbols}).mappings().all()
        else:
            rows = conn.execute(
                text(
                    f"""
                    SELECT :source AS source, symbol, trade_time, trade_date, open, high, low, close, volume, amount
                    FROM {table_name}
                    WHERE trade_date = :trade_date
                    ORDER BY symbol, trade_time
                    """
                ),
                {"source": source, "trade_date": trade_date},
            ).mappings().all()
        all_rows.extend(dict(row) for row in rows)
    return all_rows


def _select_daily_candidate(items: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str, str, list[str], dict[str, Any]]:
    issues: list[str] = []
    valid_items = [item for item in items if _daily_row_is_valid(item, issues=None)]
    if not valid_items:
        issues.append("no_valid_source")
        return None, "missing_or_conflicted", "invalid", issues, {"sources": [str(item.get("source") or "") for item in items]}

    valid_items.sort(key=lambda item: DAILY_SOURCE_PRIORITY.get(str(item.get("source") or ""), 0), reverse=True)
    selected = valid_items[0]
    source_summary = {
        "sources": [str(item.get("source") or "") for item in items],
        "selected_source": str(selected.get("source") or ""),
        "comparisons": {},
    }
    warning = False
    selected_source = str(selected.get("source") or "")
    selected_signature = _daily_numeric_signature(selected)
    for item in valid_items[1:]:
        source = str(item.get("source") or "")
        diffs = _daily_diff_summary(selected, item)
        source_summary["comparisons"][source] = diffs
        if diffs["price_diff_max"] > 0.011 or diffs["volume_ratio_diff"] > 0.15 or diffs["amount_ratio_diff"] > 0.15:
            warning = True
            issues.append(f"source_conflict:{selected_source}:{source}")
    publish_status = "published_with_warning" if warning else "published"
    quality_status = "warning" if warning else "validated"
    return selected, publish_status, quality_status, issues, source_summary


def _compose_minute_symbol_rows(
    *,
    symbol: str,
    trade_date: date,
    items: list[dict[str, Any]],
    minimum_coverage_ratio: float,
) -> tuple[list[dict[str, Any]], str, str, list[str], list[str], float]:
    per_source: dict[str, dict[datetime, dict[str, Any]]] = defaultdict(dict)
    for item in items:
        source = str(item.get("source") or "")
        trade_time = _normalize_datetime(item.get("trade_time"))
        if trade_time is None:
            continue
        per_source[source][trade_time] = {
            "symbol": symbol,
            "trade_time": trade_time,
            "trade_date": trade_date,
            "open": _safe_float(item.get("open")),
            "high": _safe_float(item.get("high")),
            "low": _safe_float(item.get("low")),
            "close": _safe_float(item.get("close")),
            "volume": _safe_float(item.get("volume")),
            "amount": _safe_float(item.get("amount")),
        }
    if not per_source:
        return [], "missing_or_conflicted", "missing", [], [], 0.0

    source_order = sorted(per_source.keys(), key=lambda item: MINUTE_SOURCE_PRIORITY.get(item, 0), reverse=True)
    published: dict[datetime, dict[str, Any]] = {}
    source_mix: list[str] = []
    for source in source_order:
        rows = per_source[source]
        if rows and source not in source_mix:
            source_mix.append(source)
        for trade_time, row in rows.items():
            published.setdefault(trade_time, row)

    sorted_rows = [published[key] for key in sorted(published)]
    actual_bars = len(sorted_rows)
    coverage_ratio = round(actual_bars / MINUTE_EXPECTED_BARS, 4) if MINUTE_EXPECTED_BARS else 0.0
    missing_times = [
        item.strftime("%H:%M")
        for item in _expected_market_minute_slots(trade_date)
        if item not in published
    ]
    if actual_bars == 0:
        return [], "missing_or_conflicted", "missing", source_mix, missing_times, 0.0
    if coverage_ratio >= 1.0:
        return sorted_rows, "published", "validated", source_mix, missing_times, coverage_ratio
    if coverage_ratio >= minimum_coverage_ratio:
        return sorted_rows, "published_with_warning", "warning", source_mix, missing_times, coverage_ratio
    return sorted_rows, "raw_ingested", "partial", source_mix, missing_times, coverage_ratio


def _daily_row_is_valid(row: dict[str, Any], issues: list[str] | None = None) -> bool:
    open_price = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    close = _safe_float(row.get("close"))
    volume = _safe_float(row.get("volume"))
    amount = _safe_float(row.get("amount"))
    valid = True
    if high is not None and max(value for value in [open_price, close, low, high] if value is not None) > high:
        valid = False
        if issues is not None:
            issues.append("high_below_ohlc")
    if low is not None and min(value for value in [open_price, close, low, high] if value is not None) < low:
        valid = False
        if issues is not None:
            issues.append("low_above_ohlc")
    if volume is not None and volume < 0:
        valid = False
        if issues is not None:
            issues.append("negative_volume")
    if amount is not None and amount < 0:
        valid = False
        if issues is not None:
            issues.append("negative_amount")
    return valid


def _daily_numeric_signature(row: dict[str, Any]) -> tuple[float | None, ...]:
    return (
        _safe_float(row.get("open")),
        _safe_float(row.get("high")),
        _safe_float(row.get("low")),
        _safe_float(row.get("close")),
        _safe_float(row.get("volume")),
        _safe_float(row.get("amount")),
    )


def _daily_diff_summary(reference: dict[str, Any], compare: dict[str, Any]) -> dict[str, float]:
    price_fields = ["open", "high", "low", "close"]
    price_diff_max = max(abs((_safe_float(reference.get(field)) or 0.0) - (_safe_float(compare.get(field)) or 0.0)) for field in price_fields)
    ref_volume = _safe_float(reference.get("volume")) or 0.0
    cmp_volume = _safe_float(compare.get("volume")) or 0.0
    ref_amount = _safe_float(reference.get("amount")) or 0.0
    cmp_amount = _safe_float(compare.get("amount")) or 0.0
    return {
        "price_diff_max": round(price_diff_max, 6),
        "volume_ratio_diff": round(abs(ref_volume - cmp_volume) / max(abs(ref_volume), 1.0), 6),
        "amount_ratio_diff": round(abs(ref_amount - cmp_amount) / max(abs(ref_amount), 1.0), 6),
    }


def _build_daily_recon_item(
    *,
    run_id: str,
    symbol: str,
    trade_date: date,
    chosen_source: str | None,
    publish_status: str,
    quality_status: str,
    issues: list[str],
    source_summary: dict[str, Any],
    coverage_ratio: float,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "symbol": symbol,
        "trade_date": trade_date,
        "chosen_source": str(chosen_source or ""),
        "publish_status": publish_status,
        "quality_status": quality_status,
        "coverage_ratio": coverage_ratio,
        "issues": _json_dumps(issues),
        "source_summary": _json_dumps(source_summary),
    }


def _minute_quality_issues(
    publish_status: str,
    quality_status: str,
    coverage_ratio: float,
    missing_times: list[str],
) -> list[str]:
    issues: list[str] = []
    if publish_status == "raw_ingested":
        issues.append("coverage_below_publish_threshold")
    if quality_status == "partial":
        issues.append("partial_minute_coverage")
    if coverage_ratio < 1.0 and missing_times:
        issues.append(f"missing_bars:{len(missing_times)}")
    return issues


def _upsert_daily_rows_to_table(conn: Any, table_name: str, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            f"""
            INSERT INTO {table_name}
            (symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
             float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3,
             batch_id, fetched_at, created_at, updated_at)
            VALUES
            (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :turnover_rate, :pre_close,
             :float_market_cap, :total_market_cap, :net_profit_ttm, :sw_industry_l1, :sw_industry_l2, :sw_industry_l3,
             :batch_id, :fetched_at, :fetched_at, :fetched_at)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                turnover_rate = EXCLUDED.turnover_rate,
                pre_close = EXCLUDED.pre_close,
                float_market_cap = EXCLUDED.float_market_cap,
                total_market_cap = EXCLUDED.total_market_cap,
                net_profit_ttm = EXCLUDED.net_profit_ttm,
                sw_industry_l1 = EXCLUDED.sw_industry_l1,
                sw_industry_l2 = EXCLUDED.sw_industry_l2,
                sw_industry_l3 = EXCLUDED.sw_industry_l3,
                batch_id = EXCLUDED.batch_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            """
        ),
        rows,
    )


def _upsert_daily_rows_to_norm(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO norm_stock_daily_kline
            (source, symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
             float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3,
             batch_id, fetched_at, created_at, updated_at)
            VALUES
            (:source, :symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :turnover_rate, :pre_close,
             :float_market_cap, :total_market_cap, :net_profit_ttm, :sw_industry_l1, :sw_industry_l2, :sw_industry_l3,
             :batch_id, :fetched_at, :fetched_at, :fetched_at)
            ON CONFLICT (source, symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                turnover_rate = EXCLUDED.turnover_rate,
                pre_close = EXCLUDED.pre_close,
                float_market_cap = EXCLUDED.float_market_cap,
                total_market_cap = EXCLUDED.total_market_cap,
                net_profit_ttm = EXCLUDED.net_profit_ttm,
                sw_industry_l1 = EXCLUDED.sw_industry_l1,
                sw_industry_l2 = EXCLUDED.sw_industry_l2,
                sw_industry_l3 = EXCLUDED.sw_industry_l3,
                batch_id = EXCLUDED.batch_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            """
        ),
        rows,
    )


def _upsert_published_daily_rows(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO pub_stock_daily_kline
            (symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
             float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3,
             source, source_summary, quality_status, publish_status, freshness_status, coverage_ratio, validation_sources, created_at, updated_at)
            VALUES
            (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :turnover_rate, :pre_close,
             :float_market_cap, :total_market_cap, :net_profit_ttm, :sw_industry_l1, :sw_industry_l2, :sw_industry_l3,
             :source, :source_summary, :quality_status, :publish_status, :freshness_status, :coverage_ratio, :validation_sources, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                turnover_rate = EXCLUDED.turnover_rate,
                pre_close = EXCLUDED.pre_close,
                float_market_cap = EXCLUDED.float_market_cap,
                total_market_cap = EXCLUDED.total_market_cap,
                net_profit_ttm = EXCLUDED.net_profit_ttm,
                sw_industry_l1 = EXCLUDED.sw_industry_l1,
                sw_industry_l2 = EXCLUDED.sw_industry_l2,
                sw_industry_l3 = EXCLUDED.sw_industry_l3,
                source = EXCLUDED.source,
                source_summary = EXCLUDED.source_summary,
                quality_status = EXCLUDED.quality_status,
                publish_status = EXCLUDED.publish_status,
                freshness_status = EXCLUDED.freshness_status,
                coverage_ratio = EXCLUDED.coverage_ratio,
                validation_sources = EXCLUDED.validation_sources,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _mirror_published_daily_rows_to_legacy(conn: Any, rows: list[dict[str, Any]]) -> None:
    if not _has_table("stock_daily_kline"):
        return
    conn.execute(
        text(
            """
            INSERT INTO stock_daily_kline
            (symbol, trade_date, open, high, low, close, volume, amount, turnover_rate, pre_close,
             float_market_cap, total_market_cap, net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3,
             created_at, updated_at)
            VALUES
            (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :turnover_rate, :pre_close,
             :float_market_cap, :total_market_cap, :net_profit_ttm, :sw_industry_l1, :sw_industry_l2, :sw_industry_l3,
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                turnover_rate = EXCLUDED.turnover_rate,
                pre_close = EXCLUDED.pre_close,
                float_market_cap = EXCLUDED.float_market_cap,
                total_market_cap = EXCLUDED.total_market_cap,
                net_profit_ttm = EXCLUDED.net_profit_ttm,
                sw_industry_l1 = EXCLUDED.sw_industry_l1,
                sw_industry_l2 = EXCLUDED.sw_industry_l2,
                sw_industry_l3 = EXCLUDED.sw_industry_l3,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _upsert_minute_rows_to_table(conn: Any, table_name: str, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            f"""
            INSERT INTO {table_name}
            (symbol, trade_time, trade_date, open, high, low, close, volume, amount, batch_id, fetched_at, created_at, updated_at)
            VALUES
            (:symbol, :trade_time, :trade_date, :open, :high, :low, :close, :volume, :amount, :batch_id, :fetched_at, :fetched_at, :fetched_at)
            ON CONFLICT (symbol, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                batch_id = EXCLUDED.batch_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            """
        ),
        rows,
    )


def _upsert_minute_rows_to_norm(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO norm_stock_minute_kline
            (source, symbol, trade_time, trade_date, open, high, low, close, volume, amount, batch_id, fetched_at, created_at, updated_at)
            VALUES
            (:source, :symbol, :trade_time, :trade_date, :open, :high, :low, :close, :volume, :amount, :batch_id, :fetched_at, :fetched_at, :fetched_at)
            ON CONFLICT (source, symbol, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                batch_id = EXCLUDED.batch_id,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            """
        ),
        rows,
    )


def _upsert_published_minute_rows(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO pub_stock_minute_kline
            (symbol, trade_time, trade_date, open, high, low, close, volume, amount, primary_source, source_mix,
             quality_status, publish_status, freshness_status, coverage_ratio, created_at, updated_at)
            VALUES
            (:symbol, :trade_time, :trade_date, :open, :high, :low, :close, :volume, :amount, :primary_source, :source_mix,
             :quality_status, :publish_status, :freshness_status, :coverage_ratio, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                primary_source = EXCLUDED.primary_source,
                source_mix = EXCLUDED.source_mix,
                quality_status = EXCLUDED.quality_status,
                publish_status = EXCLUDED.publish_status,
                freshness_status = EXCLUDED.freshness_status,
                coverage_ratio = EXCLUDED.coverage_ratio,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _mirror_published_minute_rows_to_legacy(conn: Any, rows: list[dict[str, Any]]) -> None:
    if not _has_table("stock_minute_kline"):
        return
    conn.execute(
        text(
            """
            INSERT INTO stock_minute_kline
            (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            (:symbol, :trade_time, :open, :high, :low, :close, :volume, :amount, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _upsert_daily_reconciliation_run(conn: Any, *, run_id: str, trade_date: date, published_count: int, warning_count: int, missing_count: int) -> None:
    conn.execute(
        text(
            """
            INSERT INTO daily_kline_reconciliation_runs
            (run_id, trade_date, published_count, warning_count, missing_count, created_at, updated_at)
            VALUES (:run_id, :trade_date, :published_count, :warning_count, :missing_count, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (run_id) DO UPDATE SET
                published_count = EXCLUDED.published_count,
                warning_count = EXCLUDED.warning_count,
                missing_count = EXCLUDED.missing_count,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "run_id": run_id,
            "trade_date": trade_date,
            "published_count": published_count,
            "warning_count": warning_count,
            "missing_count": missing_count,
        },
    )


def _upsert_daily_reconciliation_items(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO daily_kline_reconciliation_items
            (run_id, symbol, trade_date, chosen_source, publish_status, quality_status, coverage_ratio, issues, source_summary, created_at, updated_at)
            VALUES (:run_id, :symbol, :trade_date, :chosen_source, :publish_status, :quality_status, :coverage_ratio, :issues, :source_summary, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (run_id, symbol, trade_date) DO UPDATE SET
                chosen_source = EXCLUDED.chosen_source,
                publish_status = EXCLUDED.publish_status,
                quality_status = EXCLUDED.quality_status,
                coverage_ratio = EXCLUDED.coverage_ratio,
                issues = EXCLUDED.issues,
                source_summary = EXCLUDED.source_summary,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _upsert_minute_reconciliation_run(conn: Any, *, run_id: str, trade_date: date, published_count: int, warning_count: int, missing_count: int) -> None:
    conn.execute(
        text(
            """
            INSERT INTO minute_kline_reconciliation_runs
            (run_id, trade_date, published_count, warning_count, missing_count, created_at, updated_at)
            VALUES (:run_id, :trade_date, :published_count, :warning_count, :missing_count, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (run_id) DO UPDATE SET
                published_count = EXCLUDED.published_count,
                warning_count = EXCLUDED.warning_count,
                missing_count = EXCLUDED.missing_count,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {
            "run_id": run_id,
            "trade_date": trade_date,
            "published_count": published_count,
            "warning_count": warning_count,
            "missing_count": missing_count,
        },
    )


def _upsert_minute_reconciliation_items(conn: Any, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        text(
            """
            INSERT INTO minute_kline_reconciliation_items
            (run_id, symbol, trade_date, chosen_source, publish_status, quality_status, coverage_ratio, expected_bars, actual_bars,
             missing_times, issues, source_summary, created_at, updated_at)
            VALUES (:run_id, :symbol, :trade_date, :chosen_source, :publish_status, :quality_status, :coverage_ratio, :expected_bars, :actual_bars,
                    :missing_times, :issues, :source_summary, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (run_id, symbol, trade_date) DO UPDATE SET
                chosen_source = EXCLUDED.chosen_source,
                publish_status = EXCLUDED.publish_status,
                quality_status = EXCLUDED.quality_status,
                coverage_ratio = EXCLUDED.coverage_ratio,
                expected_bars = EXCLUDED.expected_bars,
                actual_bars = EXCLUDED.actual_bars,
                missing_times = EXCLUDED.missing_times,
                issues = EXCLUDED.issues,
                source_summary = EXCLUDED.source_summary,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        rows,
    )


def _expected_market_minute_slots(trade_date: date) -> list[datetime]:
    slots: list[datetime] = []
    cursor = datetime.combine(trade_date, time(9, 31))
    morning_end = datetime.combine(trade_date, time(11, 30))
    while cursor <= morning_end:
        slots.append(cursor)
        cursor += timedelta(minutes=1)
    cursor = datetime.combine(trade_date, time(13, 1))
    afternoon_end = datetime.combine(trade_date, time(15, 0))
    while cursor <= afternoon_end:
        slots.append(cursor)
        cursor += timedelta(minutes=1)
    return slots


def _load_daily_publish_summary(
    db: Any,
    *,
    table_name: str,
    trade_date: date | None,
    symbols: list[str],
    limit: int,
) -> dict[str, Any]:
    if trade_date is None or not _has_table(table_name):
        return {"summary": {}, "items": []}
    columns = _relation_columns(table_name)
    if "publish_status" not in columns:
        return _load_final_daily_summary(db, table_name=table_name, trade_date=trade_date, symbols=symbols, limit=limit)
    params: dict[str, Any] = {"trade_date": trade_date, "limit": max(int(limit or 200), 1)}
    filters = ["trade_date = :trade_date"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    where_clause = " AND ".join(filters)
    binded = text(
        f"""
        SELECT symbol, source, publish_status, quality_status, freshness_status, coverage_ratio, updated_at
        FROM {table_name}
        WHERE {where_clause}
        ORDER BY symbol ASC
        LIMIT :limit
        """
    )
    if symbols:
        binded = binded.bindparams(bindparam("symbols", expanding=True))
    items = [dict(row) for row in db.execute(binded, params).mappings().all()]
    summary = _load_status_counts(db, table_name=table_name, trade_date=trade_date, symbols=symbols)
    return {
        "summary": summary,
        "items": [
            {
                "symbol": str(item["symbol"]),
                "source": str(item.get("source") or ""),
                "publish_status": str(item.get("publish_status") or ""),
                "quality_status": str(item.get("quality_status") or ""),
                "freshness_status": str(item.get("freshness_status") or ""),
                "coverage_ratio": _safe_float(item.get("coverage_ratio")),
                "updated_at": item.get("updated_at").isoformat() if hasattr(item.get("updated_at"), "isoformat") else item.get("updated_at"),
            }
            for item in items
        ],
    }


def _load_minute_publish_summary(
    db: Any,
    *,
    table_name: str,
    trade_date: date | None,
    symbols: list[str],
    limit: int,
) -> dict[str, Any]:
    if trade_date is None or not _has_table(table_name):
        return {"summary": {}, "items": []}
    if table_name == "market_stock_minute_kline" and not symbols:
        return _load_market_minute_overview(db, trade_date=trade_date, limit=limit)
    columns = _relation_columns(table_name)
    if "publish_status" not in columns:
        return _load_final_minute_summary(db, table_name=table_name, trade_date=trade_date, symbols=symbols, limit=limit)
    params: dict[str, Any] = {"trade_date": trade_date, "limit": max(int(limit or 200), 1)}
    filters = ["trade_date = :trade_date"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    where_clause = " AND ".join(filters)
    binded = text(
        f"""
        SELECT symbol,
               primary_source,
               source_mix,
               publish_status,
               quality_status,
               freshness_status,
               MAX(coverage_ratio) AS coverage_ratio,
               MAX(updated_at) AS updated_at,
               COUNT(*) AS bars
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY symbol, primary_source, source_mix, publish_status, quality_status, freshness_status
        ORDER BY symbol ASC
        LIMIT :limit
        """
    )
    if symbols:
        binded = binded.bindparams(bindparam("symbols", expanding=True))
    items = [dict(row) for row in db.execute(binded, params).mappings().all()]
    summary = _load_status_counts(db, table_name=table_name, trade_date=trade_date, symbols=symbols)
    return {
        "summary": summary,
        "items": [
            {
                "symbol": str(item["symbol"]),
                "primary_source": str(item.get("primary_source") or ""),
                "source_mix": str(item.get("source_mix") or ""),
                "publish_status": str(item.get("publish_status") or ""),
                "quality_status": str(item.get("quality_status") or ""),
                "freshness_status": str(item.get("freshness_status") or ""),
                "coverage_ratio": _safe_float(item.get("coverage_ratio")),
                "bars": int(item.get("bars") or 0),
                "updated_at": item.get("updated_at").isoformat() if hasattr(item.get("updated_at"), "isoformat") else item.get("updated_at"),
            }
            for item in items
        ],
    }


def _load_final_daily_summary(
    db: Any,
    *,
    table_name: str,
    trade_date: date,
    symbols: list[str],
    limit: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {"trade_date": trade_date, "limit": max(int(limit or 200), 1)}
    filters = ["trade_date = :trade_date"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    statement = text(
        f"""
        SELECT symbol, updated_at
        FROM {table_name}
        WHERE {" AND ".join(filters)}
        ORDER BY symbol ASC
        LIMIT :limit
        """
    )
    if symbols:
        statement = statement.bindparams(bindparam("symbols", expanding=True))
    rows = [dict(row) for row in db.execute(statement, params).mappings().all()]
    summary = _load_final_status_counts(db, table_name=table_name, trade_date=trade_date, symbols=symbols)
    return {
        "summary": summary,
        "items": [
            {
                "symbol": str(row["symbol"]),
                "source": "final_table",
                "publish_status": "final",
                "quality_status": "validated",
                "freshness_status": "business_source",
                "coverage_ratio": 1.0,
                "updated_at": row.get("updated_at").isoformat() if hasattr(row.get("updated_at"), "isoformat") else row.get("updated_at"),
            }
            for row in rows
        ],
    }


def _load_final_minute_summary(
    db: Any,
    *,
    table_name: str,
    trade_date: date,
    symbols: list[str],
    limit: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "start_dt": datetime.combine(trade_date, time.min),
        "end_dt": datetime.combine(trade_date + timedelta(days=1), time.min),
        "limit": max(int(limit or 200), 1),
    }
    filters = ["trade_time >= :start_dt", "trade_time < :end_dt"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    statement = text(
        f"""
        SELECT symbol,
               COUNT(*) AS bars,
               MAX(updated_at) AS updated_at
        FROM {table_name}
        WHERE {" AND ".join(filters)}
        GROUP BY symbol
        ORDER BY symbol ASC
        LIMIT :limit
        """
    )
    if symbols:
        statement = statement.bindparams(bindparam("symbols", expanding=True))
    rows = [dict(row) for row in db.execute(statement, params).mappings().all()]
    summary = _load_final_status_counts(db, table_name=table_name, trade_date=trade_date, symbols=symbols)
    return {
        "summary": summary,
        "items": [
            {
                "symbol": str(row["symbol"]),
                "primary_source": "final_table",
                "source_mix": "final_table",
                "publish_status": "final",
                "quality_status": "validated",
                "freshness_status": "business_source",
                "coverage_ratio": round((int(row["bars"] or 0) / MINUTE_EXPECTED_BARS), 4) if MINUTE_EXPECTED_BARS else None,
                "bars": int(row["bars"] or 0),
                "updated_at": row.get("updated_at").isoformat() if hasattr(row.get("updated_at"), "isoformat") else row.get("updated_at"),
            }
            for row in rows
        ],
    }


def _load_market_minute_overview(db: Any, *, trade_date: date, limit: int) -> dict[str, Any]:
    sample_limit = max(int(limit or 200), 1)
    pub_items: list[dict[str, Any]] = []
    pub_summary = {"counts": {}, "last_updated_at": None, "symbol_count": 0}
    if _table_has_rows("pub_stock_minute_kline"):
        pub_items = _load_minute_publish_summary(
            db,
            table_name="pub_stock_minute_kline",
            trade_date=trade_date,
            symbols=[],
            limit=sample_limit,
        ).get("items", [])
        pub_summary = _load_status_counts(db, table_name="pub_stock_minute_kline", trade_date=trade_date, symbols=[])

    legacy_items: list[dict[str, Any]] = []
    legacy_symbol_count = 0
    if _has_table("stock_daily_kline") and _has_table("stock_minute_kline"):
        legacy_symbol_count = int(
            db.execute(
                text("SELECT COUNT(DISTINCT symbol) FROM stock_daily_kline WHERE trade_date = :trade_date"),
                {"trade_date": trade_date},
            ).scalar()
            or 0
        )
        sample_symbols = [
            str(row[0])
            for row in db.execute(
                text(
                    """
                    SELECT DISTINCT symbol
                    FROM stock_daily_kline
                    WHERE trade_date = :trade_date
                    ORDER BY symbol
                    LIMIT :limit
                    """
                ),
                {"trade_date": trade_date, "limit": sample_limit},
            ).fetchall()
        ]
        if sample_symbols:
            start_dt = datetime.combine(trade_date, time.min)
            end_dt = datetime.combine(trade_date + timedelta(days=1), time.min)
            statement = text(
                """
                SELECT symbol,
                       COUNT(*) AS bars,
                       MAX(updated_at) AS updated_at
                FROM stock_minute_kline
                WHERE symbol IN :symbols
                  AND trade_time >= :start_dt
                  AND trade_time < :end_dt
                GROUP BY symbol
                ORDER BY symbol ASC
                LIMIT :limit
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = db.execute(
                statement,
                {"symbols": sample_symbols, "start_dt": start_dt, "end_dt": end_dt, "limit": sample_limit},
            ).mappings().all()
            legacy_items = [
                {
                    "symbol": str(row["symbol"]),
                    "primary_source": "postgresql",
                    "source_mix": "postgresql",
                    "publish_status": "legacy",
                    "quality_status": "legacy",
                    "freshness_status": "historical",
                    "coverage_ratio": round((int(row["bars"] or 0) / MINUTE_EXPECTED_BARS), 4) if MINUTE_EXPECTED_BARS else None,
                    "bars": int(row["bars"] or 0),
                    "updated_at": row.get("updated_at").isoformat() if hasattr(row.get("updated_at"), "isoformat") else row.get("updated_at"),
                }
                for row in rows
            ]

    items = [*pub_items, *legacy_items][:sample_limit]
    counts = dict(pub_summary.get("counts") or {})
    if legacy_symbol_count:
        counts["legacy"] = legacy_symbol_count
    return {
        "summary": {
            "counts": counts,
            "last_updated_at": pub_summary.get("last_updated_at"),
            "symbol_count": int(pub_summary.get("symbol_count") or 0) + legacy_symbol_count,
            "sampled": True,
            "sample_note": "分钟线旧表体量较大，未无条件全表扫描；未指定 symbols 时只抽样展示。",
        },
        "items": items,
    }


def _load_status_counts(db: Any, *, table_name: str, trade_date: date, symbols: list[str]) -> dict[str, Any]:
    if "publish_status" not in _relation_columns(table_name):
        return _load_final_status_counts(db, table_name=table_name, trade_date=trade_date, symbols=symbols)
    params: dict[str, Any] = {"trade_date": trade_date}
    filters = ["trade_date = :trade_date"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    where_clause = " AND ".join(filters)
    binded = text(
        f"""
        SELECT publish_status, COUNT(DISTINCT symbol) AS symbol_count, MAX(updated_at) AS last_updated_at
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY publish_status
        """
    )
    if symbols:
        binded = binded.bindparams(bindparam("symbols", expanding=True))
    rows = db.execute(binded, params).mappings().all()
    counts = {str(row["publish_status"] or ""): int(row["symbol_count"] or 0) for row in rows}
    last_updated = max(
        [row["last_updated_at"] for row in rows if row.get("last_updated_at") is not None],
        default=None,
    )
    return {
        "counts": counts,
        "last_updated_at": last_updated.isoformat() if hasattr(last_updated, "isoformat") else None,
        "symbol_count": sum(counts.values()),
    }


def _load_final_status_counts(db: Any, *, table_name: str, trade_date: date, symbols: list[str]) -> dict[str, Any]:
    columns = _relation_columns(table_name)
    params: dict[str, Any] = {}
    if "trade_date" in columns:
        params["trade_date"] = trade_date
        filters = ["trade_date = :trade_date"]
    else:
        params["start_dt"] = datetime.combine(trade_date, time.min)
        params["end_dt"] = datetime.combine(trade_date + timedelta(days=1), time.min)
        filters = ["trade_time >= :start_dt", "trade_time < :end_dt"]
    if symbols:
        params["symbols"] = symbols
        filters.append("symbol IN :symbols")
    statement = text(
        f"""
        SELECT COUNT(DISTINCT symbol) AS symbol_count, MAX(updated_at) AS last_updated_at
        FROM {table_name}
        WHERE {" AND ".join(filters)}
        """
    )
    if symbols:
        statement = statement.bindparams(bindparam("symbols", expanding=True))
    row = db.execute(statement, params).mappings().first()
    symbol_count = int((row or {}).get("symbol_count") or 0)
    last_updated = (row or {}).get("last_updated_at")
    return {
        "counts": {"final": symbol_count} if symbol_count else {},
        "last_updated_at": last_updated.isoformat() if hasattr(last_updated, "isoformat") else None,
        "symbol_count": symbol_count,
    }


def _load_max_trade_date(db: Any, *, table_name: str) -> date | None:
    if not _has_table(table_name):
        return None
    if table_name == "market_stock_daily_kline":
        values = []
        if _has_table("pub_stock_daily_kline"):
            values.append(db.execute(text("SELECT MAX(trade_date) FROM pub_stock_daily_kline")).scalar())
        if _has_table("stock_daily_kline"):
            values.append(db.execute(text("SELECT MAX(trade_date) FROM stock_daily_kline")).scalar())
        return max([value for value in values if value is not None], default=None)
    if table_name == "market_stock_minute_kline":
        values = []
        if _has_table("pub_stock_minute_kline"):
            values.append(db.execute(text("SELECT MAX(trade_date) FROM pub_stock_minute_kline")).scalar())
        if _has_table("stock_minute_kline"):
            latest_time = db.execute(text("SELECT MAX(trade_time) FROM stock_minute_kline")).scalar()
            values.append(latest_time.date() if hasattr(latest_time, "date") else None)
        return max([value for value in values if value is not None], default=None)
    columns = _relation_columns(table_name)
    if "trade_date" in columns:
        return db.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).scalar()
    if "trade_time" in columns:
        return db.execute(text(f"SELECT MAX(DATE(trade_time)) FROM {table_name}")).scalar()
    return None


def _normalize_trade_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(text_value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _normalize_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    text_value = str(value).strip()
    if not text_value:
        return None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
        try:
            return datetime.strptime(text_value, pattern)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    for prefix in ("SH", "SZ", "BJ"):
        if symbol.startswith(prefix) and len(symbol) == 8 and symbol[2:].isdigit():
            return f"{symbol[2:]}.{prefix}"
    if "." in symbol:
        return symbol
    if len(symbol) == 6 and symbol.isdigit():
        if symbol.startswith(("4", "8")) or symbol.startswith("92"):
            return f"{symbol}.BJ"
        if symbol.startswith(("5", "6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"
    return symbol


def _normalize_symbols(values: list[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        normalized = _normalize_symbol(value)
        if normalized:
            result.append(normalized)
            if "." in normalized:
                result.append(normalized.split(".", 1)[0])
    return sorted(set(result))




def _safe_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _published_minute_reads_enabled() -> bool:
    return os.getenv("MARKET_DATA_READ_PUBLISHED_MINUTE", "0").strip().lower() in {"1", "true", "yes", "on"}


def _relation_columns(table_name: str) -> set[str]:
    try:
        return {column["name"] for column in inspect(engine).get_columns(table_name)}
    except Exception:
        return set()


def _has_table(table_name: str) -> bool:
    try:
        return inspect(engine).has_table(table_name)
    except Exception:
        return False


def _table_has_rows(table_name: str) -> bool:
    if not _has_table(table_name):
        return False
    try:
        with engine.connect() as conn:
            return bool(conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1")).first())
    except Exception:
        return False
