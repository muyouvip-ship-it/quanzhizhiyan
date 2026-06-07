from __future__ import annotations

import concurrent.futures
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from api.core.stock_map import get_reverse_stock_map
from api.core.stock_utils import normalize_symbol, search_cn_stock_by_name
from api.database import get_db
from api.deps import optional_web_user, require_api_user
from api.services.qmt_market_data_service import (
    build_market_integrity_report,
    fetch_daily_bars,
    fetch_intraday_bars,
    fetch_realtime_quotes,
    get_index_presets,
)
from api.services.data_source_governance import build_market_overview_governance
from api.services.daily_review_market_behavior import interpret_market_behavior
from api.services.market_data_pipeline_service import preferred_daily_kline_table, preferred_minute_kline_table

router = APIRouter(prefix="/v1/market", tags=["Market"])

INDEX_PRESETS = get_index_presets()
FAST_QUOTE_TIMEOUT_SECONDS = 2.5
FAST_INTRADAY_QUOTE_TIMEOUT_SECONDS = 2.0
SECTOR_FUND_FLOW_WAIT_SECONDS = 1.5
SECTOR_FUND_FLOW_TTL_SECONDS = 300
SECTOR_FUND_FLOW_STALE_SECONDS = 1800
_SECTOR_FUND_FLOW_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="market-fund-flow")
_SECTOR_FUND_FLOW_LOCK = threading.Lock()
_SECTOR_FUND_FLOW_FUTURE: concurrent.futures.Future[list[dict[str, Any]]] | None = None
_SECTOR_FUND_FLOW_STARTED_AT = 0.0
_SECTOR_FUND_FLOW_CACHE: dict[str, Any] = {"items": [], "updated_at": 0.0}


@router.get("/stock-search")
def search_stocks(
    q: str = Query("", min_length=1, max_length=20),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
):
    q = q.strip()
    if not q:
        return {"results": []}

    code_to_name = get_reverse_stock_map()
    results = []
    q_upper = q.upper()

    for code, name in code_to_name.items():
        if q in name or q_upper in code.upper() or q in code:
            results.append({"symbol": code, "name": name, "source": "cache"})

    if not results:
        found = search_cn_stock_by_name(q)
        if found:
            results.append({"symbol": found, "name": code_to_name.get(found, q), "source": "akshare"})

    quote_map = _load_quote_map([item["symbol"] for item in results[:20]], db=db, user_id=str(current_user.id))
    latest_map = _load_latest_stock_changes(db, [item["symbol"] for item in results[:20]])
    for item in results:
        quote = quote_map.get(item["symbol"]) or quote_map.get(item["symbol"].split(".", 1)[0]) or {}
        latest = latest_map.get(item["symbol"]) or {}
        price = _to_float(quote.get("price")) or latest.get("price")
        change_pct = _to_float(quote.get("change_pct")) or latest.get("change_pct")
        item.update(
            {
                "market": item["symbol"].split(".", 1)[-1] if "." in item["symbol"] else "",
                "exchange": item["symbol"].split(".", 1)[-1] if "." in item["symbol"] else "",
                "current_price": price,
                "change_pct": change_pct,
            }
        )

    return {"results": results[:20]}


@router.get("/kline")
def get_kline(
    symbol: str,
    start_date: str,
    end_date: str,
    db: Session = Depends(get_db),
    current_user=Depends(optional_web_user),
):
    normalized = normalize_symbol(symbol)
    user_id = str(current_user.id) if current_user is not None else None
    code = normalized.split(".", 1)[0]
    index_codes = {item["code"] for item in INDEX_PRESETS}
    is_index = normalized in {item["symbol"] for item in INDEX_PRESETS} or code in index_codes
    rows = _load_kline_rows(db, code, start_date, end_date, prefer_index=is_index)
    if is_index and not rows:
        try:
            fetch_daily_bars(normalized, start_date=start_date, end_date=end_date, db=db, user_id=user_id)
            rows = _load_kline_rows(db, code, start_date, end_date, prefer_index=True)
        except Exception:
            rows = rows or []

    candles = []
    previous_close = None
    for row in rows:
        open_price = _to_float(row["open"])
        high = _to_float(row["high"])
        low = _to_float(row["low"])
        close = _to_float(row["close"])
        if open_price is None or high is None or low is None or close is None:
            continue
        pre_close = _to_float(row["pre_close"]) or previous_close
        change = round(close - pre_close, 4) if pre_close else None
        change_percent = round(change / pre_close * 100, 4) if change is not None and pre_close else None
        candles.append(
            {
                "date": row["trade_date"].isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": _to_float(row["volume"]),
                "amount": _to_float(row["amount"]),
                "change": change,
                "change_percent": change_percent,
                "turnover_rate": _to_float(row["turnover_rate"]),
            }
        )
        previous_close = close

    _append_live_candle(candles, normalized, start_date, end_date, db=db, user_id=user_id)
    return {
        "symbol": normalized,
        "start_date": start_date,
        "end_date": end_date,
        "candles": candles,
        "source": "qmt_realtime+postgresql_daily" if candles else "empty",
    }


@router.get("/intraday")
def get_intraday(
    symbol: str,
    trade_date: str,
    period: str = Query("1m", pattern="^(1m|5m|15m|30m|60m)$"),
    include_latest_quote: bool = Query(True),
    lookback_sessions: int = Query(1, ge=1, le=60),
    db: Session = Depends(get_db),
    current_user=Depends(optional_web_user),
):
    normalized = normalize_symbol(symbol)
    user_id = str(current_user.id) if current_user is not None else None
    if lookback_sessions > 1:
        return _load_intraday_history_payload(
            normalized,
            trade_date=trade_date,
            period=period,
            include_latest_quote=include_latest_quote,
            lookback_sessions=lookback_sessions,
            db=db,
            user_id=user_id,
        )
    return _load_intraday_payload_with_fallback(
        normalized,
        trade_date=trade_date,
        period=period,
        include_latest_quote=include_latest_quote,
        db=db,
        user_id=user_id,
    )


@router.get("/quote")
def get_market_quote(
    symbol: str,
    db: Session = Depends(get_db),
    current_user=Depends(optional_web_user),
):
    normalized = normalize_symbol(symbol)
    user_id = str(current_user.id) if current_user is not None else None
    quote = _fetch_realtime_quotes_compat([normalized], timeout_seconds=FAST_QUOTE_TIMEOUT_SECONDS, db=db, user_id=user_id).get(normalized)
    if not quote:
        raise HTTPException(status_code=404, detail=f"QMT quote unavailable for {normalized}")
    return {
        "symbol": normalized,
        "quote": quote,
        "source": "qmt_realtime",
    }


@router.get("/overview")
def get_market_overview(
    limit: int = Query(20, ge=5, le=100),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> Dict[str, Any]:
    user_id = str(current_user.id)
    index_symbols = [item["symbol"] for item in INDEX_PRESETS]
    quote_map = _load_quote_map(index_symbols, timeout_seconds=FAST_QUOTE_TIMEOUT_SECONDS, db=db, user_id=user_id)
    indices = []
    for item in INDEX_PRESETS:
        latest = _load_latest_index_item(db, item["code"])
        quote = quote_map.get(item["symbol"]) or quote_map.get(item["code"]) or {}
        merged = _merge_market_item(
            symbol=item["symbol"],
            name=item["name"],
            latest=latest,
            quote=quote,
            source="qmt_realtime" if quote else (latest.get("source") or "postgresql:index_daily_kline"),
        )
        indices.append(merged)

    top_gainers, top_losers = _load_stock_rankings(db, limit=limit)
    sector_gainers, sector_losers = _load_sector_rankings(db, limit=limit)
    sector_fund_inflows, sector_fund_outflows = _load_sector_fund_flow(limit=limit)
    market_stats = _load_market_stats(db)
    index_turnover = _index_turnover_amount(indices)
    if index_turnover is not None:
        market_stats["index_turnover_amount"] = round(index_turnover, 2)
    market_behavior_labels = interpret_market_behavior(
        {
            "indices": indices,
            "sector_gainers": sector_gainers,
            "sector_losers": sector_losers,
            "sector_inflows": sector_fund_inflows,
            "sector_outflows": sector_fund_outflows,
            "market_stats": market_stats,
        }
    )
    payload = {
        "indices": indices,
        "top_gainers": top_gainers,
        "top_losers": top_losers,
        "sector_gainers": sector_gainers,
        "sector_losers": sector_losers,
        "sector_fund_inflows": sector_fund_inflows,
        "sector_fund_outflows": sector_fund_outflows,
        "market_stats": market_stats,
        "market_behavior_labels": market_behavior_labels,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": "qmt_realtime+postgresql_fallback",
        "fallback": not bool(quote_map),
    }
    payload["data_governance"] = build_market_overview_governance(payload)
    return payload


@router.get("/integrity-report")
def get_market_integrity_report(
    target_date: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
):
    del current_user
    return build_market_integrity_report(db, target_date=target_date)


@router.get("/kline/chanlun")
def get_chanlun_overlay(
    symbol: str,
    start_date: str,
    end_date: str,
    period: str = Query("daily", pattern="^(daily|1m|5m|15m|30m|60m)$"),
    lookback_sessions: int = Query(1, ge=1, le=60),
    db: Session = Depends(get_db),
    current_user=Depends(optional_web_user),
):
    normalized = normalize_symbol(symbol)
    user_id = str(current_user.id) if current_user is not None else None
    if period != "daily":
        payload = (
            _load_intraday_history_payload(
                normalized,
                trade_date=end_date,
                period=period,
                include_latest_quote=False,
                lookback_sessions=lookback_sessions,
                db=db,
                user_id=user_id,
            )
            if lookback_sessions > 1
            else _load_intraday_payload_with_fallback(
                normalized,
                trade_date=end_date,
                period=period,
                include_latest_quote=False,
                db=db,
                user_id=user_id,
            )
        )
        candles = []
        for item in payload.get("items") or []:
            open_price = _to_float(item.get("open"))
            high = _to_float(item.get("high"))
            low = _to_float(item.get("low"))
            close = _to_float(item.get("close"))
            trade_time = str(item.get("trade_time") or "")
            if not trade_time or open_price is None or high is None or low is None or close is None:
                continue
            candles.append(
                {
                    "date": trade_time,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "pre_close": None,
                }
            )
        overlay = _calculate_chanlun_overlay(candles)
        overlay.update(
            {
                "symbol": normalized,
                "start_date": payload.get("start_trade_date") or payload.get("trade_date") or start_date,
                "end_date": payload.get("end_trade_date") or payload.get("trade_date") or end_date,
                "requested_trade_date": payload.get("requested_trade_date"),
                "period": period,
                "source": payload.get("source") or "intraday",
                "message": None if len(candles) >= 10 else "分时K线数量不足，缠论指标仅显示可确认部分。",
            }
        )
        return overlay

    code = normalized.split(".", 1)[0]
    index_codes = {item["code"] for item in INDEX_PRESETS}
    is_index = normalized in {item["symbol"] for item in INDEX_PRESETS} or code in index_codes
    rows = _load_kline_rows(db, code, start_date, end_date, prefer_index=is_index)
    candles = []
    previous_close = None
    for row in rows:
        open_price = _to_float(row["open"])
        high = _to_float(row["high"])
        low = _to_float(row["low"])
        close = _to_float(row["close"])
        if open_price is None or high is None or low is None or close is None:
            continue
        pre_close = _to_float(row.get("pre_close")) or previous_close
        candles.append(
            {
                "date": row["trade_date"].isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "pre_close": pre_close,
            }
        )
        previous_close = close
    overlay = _calculate_chanlun_overlay(candles)
    overlay.update(
        {
            "symbol": normalized,
            "start_date": start_date,
            "end_date": end_date,
            "period": "daily",
            "source": "postgresql_daily",
            "message": None if len(candles) >= 10 else "K线数量不足，缠论指标仅显示可确认部分。",
        }
    )
    return overlay


@router.get("/hot-stocks")
def get_hot_stocks(source: str = "em", limit: int = 30) -> Dict:
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    return {"source": source, "limit": limit, "items": [], "fallback": True}


def _load_kline_rows(db: Session, code: str, start_date: str, end_date: str, *, prefer_index: bool = False):
    table_candidates = ["index_daily_kline", "index_daily_data"] if prefer_index else [preferred_daily_kline_table(), "index_daily_kline", "index_daily_data"]
    symbol_candidates = [code]
    if not prefer_index and len(code) == 6 and code.isdigit():
        if code.startswith(("4", "8")) or code.startswith("92"):
            symbol_candidates.append(f"{code}.BJ")
        elif code.startswith(("5", "6", "9")):
            symbol_candidates.append(f"{code}.SH")
        else:
            symbol_candidates.append(f"{code}.SZ")
    if prefer_index:
        symbol_candidates = [code, f"sh{code}", f"sz{code}", f"{code}.SH", f"{code}.SZ"]
    placeholders = ", ".join(f":symbol_{index}" for index, _ in enumerate(symbol_candidates))
    params = {
        "start_date": start_date,
        "end_date": end_date,
        **{f"symbol_{index}": value for index, value in enumerate(symbol_candidates)},
    }
    for table_name in table_candidates:
        if not _has_table(db, table_name):
            continue
        pre_close_expr = "pre_close" if _has_column(db, table_name, "pre_close") else "NULL AS pre_close"
        turnover_expr = "turnover_rate" if _has_column(db, table_name, "turnover_rate") else "NULL AS turnover_rate"
        try:
            return db.execute(
                text(
                    f"""
                    SELECT trade_date, open, high, low, close, volume, amount, {turnover_expr}, {pre_close_expr}
                    FROM {table_name}
                    WHERE symbol IN ({placeholders}) AND trade_date >= :start_date AND trade_date <= :end_date
                    ORDER BY trade_date ASC
                    """
                ),
                params,
            ).mappings().all()
        except Exception:
            continue
    return []


def _has_table(db: Session, table_name: str) -> bool:
    try:
        return inspect(db.bind).has_table(table_name)
    except Exception:
        return False


def _has_column(db: Session, table_name: str, column_name: str) -> bool:
    try:
        return column_name in {column["name"] for column in inspect(db.bind).get_columns(table_name)}
    except Exception:
        return False


def _fetch_realtime_quotes_compat(
    symbols: list[str],
    *,
    timeout_seconds: float | None = None,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    kwargs: dict[str, Any] = {}
    if timeout_seconds is not None:
        kwargs["timeout_seconds"] = timeout_seconds
    if db is not None:
        kwargs["db"] = db
    if user_id is not None:
        kwargs["user_id"] = user_id
    try:
        parsed = fetch_realtime_quotes(symbols, **kwargs)
    except TypeError:
        kwargs.pop("db", None)
        kwargs.pop("user_id", None)
        try:
            parsed = fetch_realtime_quotes(symbols, **kwargs)
        except TypeError:
            parsed = fetch_realtime_quotes(symbols)
    return parsed if isinstance(parsed, dict) else {}

def _aggregate_intraday_bars(items: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    """Aggregate 1-minute bars into higher-period bars (5m/15m/30m/60m).

    Groups 1m bars by trading-minute sequence and computes OHLCV for each bucket.
    """
    if period == "1m" or not items:
        return items

    minutes = {"5m": 5, "15m": 15, "30m": 30, "60m": 60}.get(period)
    if not minutes:
        return items

    parsed_items: list[tuple[str, datetime, dict[str, Any]]] = []
    for bar in items:
        trade_time = str(bar.get("trade_time") or "")
        if not trade_time:
            continue
        try:
            parsed_time = datetime.fromisoformat(trade_time.replace(" ", "T").replace("Z", "+00:00"))
        except ValueError:
            continue
        parsed_items.append((parsed_time.date().isoformat(), parsed_time, bar))

    parsed_items.sort(key=lambda item: (item[0], item[1]))
    aggregated: list[dict[str, Any]] = []
    bucket: list[tuple[str, datetime, dict[str, Any]]] = []
    current_day: str | None = None

    def flush_bucket() -> None:
        nonlocal bucket
        if not bucket:
            return
        valid_bars = [
            item
            for item in bucket
            if _to_float(item[2].get("open")) is not None
            and _to_float(item[2].get("high")) is not None
            and _to_float(item[2].get("low")) is not None
            and _to_float(item[2].get("close")) is not None
        ]
        if not valid_bars:
            bucket = []
            return
        _, end_time, end_bar = valid_bars[-1]
        first_bar = valid_bars[0][2]
        high_values = [_to_float(item[2].get("high")) for item in valid_bars]
        low_values = [_to_float(item[2].get("low")) for item in valid_bars]
        aggregated.append(
            {
                "symbol": end_bar.get("symbol") or first_bar.get("symbol"),
                "trade_time": end_time.isoformat(timespec="seconds"),
                "open": _to_float(first_bar.get("open")),
                "high": max(value for value in high_values if value is not None),
                "low": min(value for value in low_values if value is not None),
                "close": _to_float(end_bar.get("close")),
                "volume": sum(float(item[2].get("volume") or 0) for item in valid_bars),
                "amount": sum(float(item[2].get("amount") or 0) for item in valid_bars),
            }
        )
        bucket = []

    for trade_day, parsed_time, bar in parsed_items:
        if current_day is not None and trade_day != current_day:
            flush_bucket()
        current_day = trade_day
        bucket.append((trade_day, parsed_time, bar))
        if len(bucket) >= minutes:
            flush_bucket()
    flush_bucket()

    return aggregated


def _load_intraday_payload_with_fallback(
    symbol: str,
    *,
    trade_date: str,
    period: str,
    include_latest_quote: bool,
    db: Session,
    user_id: str | None,
) -> dict[str, Any]:
    fetch_period = "1m"
    payload = _fetch_intraday_bars_compat(
        symbol,
        trade_date=trade_date,
        period=fetch_period,
        include_latest_quote=include_latest_quote,
        account_key=None,
        persist=True,
        quote_timeout_seconds=FAST_INTRADAY_QUOTE_TIMEOUT_SECONDS,
        db=db,
        user_id=user_id,
    )
    if period != "1m" and payload.get("items"):
        payload["items"] = _aggregate_intraday_bars(payload["items"], period)
        payload["period"] = period
        payload["source"] = (payload.get("source") or "") + f"_aggregated_to_{period}"
        return payload
    if payload.get("items"):
        payload["period"] = period
        return payload

    fallback_date = _latest_available_intraday_trade_date(db, symbol, trade_date)
    if fallback_date and fallback_date != trade_date:
        fallback_payload = _fetch_intraday_bars_compat(
            symbol,
            trade_date=fallback_date,
            period=fetch_period,
            include_latest_quote=include_latest_quote,
            account_key=None,
            persist=True,
            quote_timeout_seconds=FAST_INTRADAY_QUOTE_TIMEOUT_SECONDS,
            db=db,
            user_id=user_id,
        )
        if fallback_payload.get("items"):
            fallback_payload["requested_trade_date"] = trade_date
            fallback_payload["trade_date"] = fallback_date
            fallback_payload["source"] = (fallback_payload.get("source") or "") + ":latest_available_fallback"
            if period != "1m":
                fallback_payload["items"] = _aggregate_intraday_bars(fallback_payload["items"], period)
                fallback_payload["period"] = period
                fallback_payload["source"] = (fallback_payload.get("source") or "") + f"_aggregated_to_{period}"
            else:
                fallback_payload["period"] = period
            return fallback_payload

    payload["period"] = period
    return payload


def _load_intraday_history_payload(
    symbol: str,
    *,
    trade_date: str,
    period: str,
    include_latest_quote: bool,
    lookback_sessions: int,
    db: Session,
    user_id: str | None,
) -> dict[str, Any]:
    table_name, trade_dates, items = _load_intraday_history_rows_from_db(
        db,
        symbol,
        requested_trade_date=trade_date,
        lookback_sessions=lookback_sessions,
    )
    if not items:
        payload = _load_intraday_payload_with_fallback(
            symbol,
            trade_date=trade_date,
            period=period,
            include_latest_quote=include_latest_quote,
            db=db,
            user_id=user_id,
        )
        payload["lookback_sessions"] = lookback_sessions
        return payload

    normalized = normalize_symbol(symbol)
    aggregated_items = _aggregate_intraday_bars(items, period) if period != "1m" else items
    latest_trade_date = trade_dates[-1] if trade_dates else trade_date
    latest_quote = (
        _fetch_realtime_quotes_compat(
            [normalized],
            timeout_seconds=FAST_INTRADAY_QUOTE_TIMEOUT_SECONDS,
            db=db,
            user_id=user_id,
        ).get(normalized)
        if include_latest_quote
        else None
    )
    return {
        "symbol": normalized,
        "trade_date": latest_trade_date,
        "requested_trade_date": trade_date if latest_trade_date != trade_date else None,
        "start_trade_date": trade_dates[0] if trade_dates else latest_trade_date,
        "end_trade_date": latest_trade_date,
        "period": period,
        "lookback_sessions": lookback_sessions,
        "loaded_sessions": len(trade_dates),
        "items": aggregated_items,
        "latest_quote": latest_quote,
        "source": f"postgresql_cache:{table_name}:history_{len(trade_dates)}sessions"
        + (f"_aggregated_to_{period}" if period != "1m" else ""),
    }


def _load_intraday_history_rows_from_db(
    db: Session,
    symbol: str,
    *,
    requested_trade_date: str,
    lookback_sessions: int,
) -> tuple[str | None, list[str], list[dict[str, Any]]]:
    normalized = normalize_symbol(symbol)
    code = normalized.split(".", 1)[0]
    index_codes = {item["code"] for item in INDEX_PRESETS}
    index_symbols = {item["symbol"] for item in INDEX_PRESETS}
    is_index = normalized in index_symbols or code in index_codes
    table_candidates = ["index_minute_kline"] if is_index else [preferred_minute_kline_table(), "stock_minute_kline", "pub_stock_minute_kline"]
    symbol_candidates = _intraday_symbol_candidates(normalized, is_index=is_index)
    symbol_placeholders = ", ".join(f":symbol_{index}" for index, _ in enumerate(symbol_candidates))
    symbol_params = {f"symbol_{index}": value for index, value in enumerate(symbol_candidates)}
    session_limit = max(1, min(int(lookback_sessions or 1), 60))

    for table_name in dict.fromkeys(table_candidates):
        if not _has_table(db, table_name):
            continue
        try:
            date_rows = db.execute(
                text(
                    f"""
                    SELECT DATE(trade_time) AS trade_day
                    FROM {table_name}
                    WHERE symbol IN ({symbol_placeholders})
                      AND DATE(trade_time) <= :requested_trade_date
                    GROUP BY DATE(trade_time)
                    ORDER BY trade_day DESC
                    LIMIT :session_limit
                    """
                ),
                {
                    **symbol_params,
                    "requested_trade_date": requested_trade_date,
                    "session_limit": session_limit,
                },
            ).all()
        except Exception:
            continue
        trade_dates = sorted(
            row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])[:10]
            for row in date_rows
            if row and row[0]
        )
        if not trade_dates:
            continue

        date_placeholders = ", ".join(f":trade_date_{index}" for index, _ in enumerate(trade_dates))
        date_params = {f"trade_date_{index}": value for index, value in enumerate(trade_dates)}
        try:
            rows = db.execute(
                text(
                    f"""
                    SELECT symbol, trade_time, open, high, low, close, volume, amount
                    FROM {table_name}
                    WHERE symbol IN ({symbol_placeholders})
                      AND DATE(trade_time) IN ({date_placeholders})
                    ORDER BY trade_time ASC
                    """
                ),
                {**symbol_params, **date_params},
            ).mappings().all()
        except Exception:
            continue

        deduped: dict[str, dict[str, Any]] = {}
        for row in rows:
            trade_time_raw = row.get("trade_time")
            trade_time = trade_time_raw.isoformat(timespec="seconds") if hasattr(trade_time_raw, "isoformat") else str(trade_time_raw)
            record = {
                "symbol": normalized,
                "trade_time": trade_time,
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
                "volume": _to_float(row.get("volume")) or 0.0,
                "amount": _to_float(row.get("amount")) or 0.0,
            }
            previous = deduped.get(trade_time)
            if previous is None or str(row.get("symbol") or "").upper() == normalized:
                deduped[trade_time] = record
        items = [deduped[key] for key in sorted(deduped)]
        if items:
            return table_name, trade_dates, items
    return None, [], []


def _latest_available_intraday_trade_date(db: Session, symbol: str, requested_trade_date: str) -> str | None:
    normalized = normalize_symbol(symbol)
    code = normalized.split(".", 1)[0]
    index_codes = {item["code"] for item in INDEX_PRESETS}
    index_symbols = {item["symbol"] for item in INDEX_PRESETS}
    is_index = normalized in index_symbols or code in index_codes
    table_candidates = ["index_minute_kline"] if is_index else [preferred_minute_kline_table(), "stock_minute_kline", "pub_stock_minute_kline"]
    symbol_candidates = _intraday_symbol_candidates(normalized, is_index=is_index)
    placeholders = ", ".join(f":symbol_{index}" for index, _ in enumerate(symbol_candidates))
    params = {
        "requested_trade_date": requested_trade_date,
        **{f"symbol_{index}": value for index, value in enumerate(symbol_candidates)},
    }

    for table_name in dict.fromkeys(table_candidates):
        if not _has_table(db, table_name):
            continue
        try:
            row = db.execute(
                text(
                    f"""
                    SELECT DATE(trade_time) AS trade_day
                    FROM {table_name}
                    WHERE symbol IN ({placeholders})
                      AND DATE(trade_time) <= :requested_trade_date
                    GROUP BY DATE(trade_time)
                    ORDER BY trade_day DESC
                    LIMIT 1
                    """
                ),
                params,
            ).first()
        except Exception:
            continue
        if row and row[0]:
            return row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0])[:10]
    return None


def _intraday_symbol_candidates(symbol: str, *, is_index: bool) -> list[str]:
    normalized = normalize_symbol(symbol)
    code = normalized.split(".", 1)[0]
    candidates = {normalized, code}
    if is_index and code:
        candidates.update({f"sh{code}", f"sz{code}", f"{code}.SH", f"{code}.SZ"})
    return sorted(item for item in candidates if item)



def _fetch_intraday_bars_compat(
    symbol: str,
    *,
    trade_date: str,
    period: str,
    include_latest_quote: bool,
    account_key: str | None,
    persist: bool,
    quote_timeout_seconds: float | None = None,
    db: Session | None = None,
    user_id: str | None = None,
):
    kwargs = {
        "trade_date": trade_date,
        "period": period,
        "include_latest_quote": include_latest_quote,
        "account_key": account_key,
        "persist": persist,
    }
    if quote_timeout_seconds is not None:
        kwargs["quote_timeout_seconds"] = quote_timeout_seconds
    if db is not None:
        kwargs["db"] = db
    if user_id is not None:
        kwargs["user_id"] = user_id
    try:
        return fetch_intraday_bars(symbol, **kwargs)
    except TypeError:
        kwargs.pop("quote_timeout_seconds", None)
        try:
            return fetch_intraday_bars(symbol, **kwargs)
        except TypeError:
            kwargs.pop("db", None)
            kwargs.pop("user_id", None)
            return fetch_intraday_bars(symbol, **kwargs)


def _load_quote_map(
    symbols: list[str],
    *,
    timeout_seconds: float | None = None,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    normalized = [normalize_symbol(symbol) for symbol in symbols]
    try:
        parsed = _fetch_realtime_quotes_compat(normalized, timeout_seconds=timeout_seconds, db=db, user_id=user_id)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, value in parsed.items():
        if isinstance(value, dict):
            result[str(key).upper()] = value
            result[str(key).split(".", 1)[0].upper()] = value
    return result


def _load_latest_stock_changes(db: Session, symbols: list[str]) -> dict[str, dict[str, float | None]]:
    daily_table = _preferred_market_latest_daily_table(db)
    if not symbols or not _has_table(db, daily_table):
        return {}
    codes = sorted({variant for symbol in symbols for variant in {normalize_symbol(symbol), normalize_symbol(symbol).split(".", 1)[0]} if variant})
    target_date = _load_latest_daily_trade_date(db, daily_table)
    if not target_date:
        return {}
    placeholders = ", ".join(f":symbol_{index}" for index, _ in enumerate(codes))
    params = {
        "target_date": target_date,
        **{f"symbol_{index}": value for index, value in enumerate(codes)},
    }
    try:
        rows = db.execute(
            text(
                f"""
                SELECT symbol, close, pre_close
                FROM {daily_table}
                WHERE trade_date = :target_date AND symbol IN ({placeholders})
                """
            ),
            params,
        ).mappings().all()
    except Exception:
        return {}
    code_to_name = get_reverse_stock_map()
    result = {}
    for row in rows:
        code = str(row["symbol"])
        symbol = _code_to_symbol(code)
        close = _to_float(row["close"])
        pre_close = _to_float(row["pre_close"])
        change_pct = round((close - pre_close) / pre_close * 100, 4) if close is not None and pre_close else None
        result[symbol] = {"price": close, "change_pct": change_pct, "name": code_to_name.get(symbol)}
    return result


def _load_latest_index_item(db: Session, code: str, trade_date: str | None = None) -> dict[str, Any]:
    symbol_candidates = [code, f"sh{code}", f"sz{code}", f"{code}.SH", f"{code}.SZ"]
    placeholders = ", ".join(f":symbol_{index}" for index, _ in enumerate(symbol_candidates))
    params = {f"symbol_{index}": value for index, value in enumerate(symbol_candidates)}
    date_clause = "AND trade_date <= :trade_date" if trade_date else ""
    if trade_date:
        params["trade_date"] = trade_date
    for table_name in ("index_daily_kline", "index_daily_data"):
        if not _has_table(db, table_name):
            continue
        try:
            rows = db.execute(
                text(
                    f"""
                    SELECT trade_date, open, high, low, close, volume, amount
                    FROM {table_name}
                    WHERE symbol IN ({placeholders})
                      {date_clause}
                    ORDER BY trade_date DESC
                    LIMIT 2
                    """
                ),
                params,
            ).mappings().all()
            if rows:
                latest = rows[0]
                previous = rows[1] if len(rows) > 1 else None
                close = _to_float(latest["close"])
                pre_close = _to_float(previous["close"]) if previous else None
                return {
                    "price": close,
                    "pre_close": pre_close,
                    "change": round(close - pre_close, 4) if close is not None and pre_close else None,
                    "change_pct": round((close - pre_close) / pre_close * 100, 4) if close is not None and pre_close else None,
                    "trade_date": latest["trade_date"].isoformat(),
                    "volume": _to_float(latest["volume"]),
                    "amount": _to_float(latest["amount"]),
                    "source": f"postgresql:{table_name}",
                }
        except Exception:
            continue
    return {}


def _merge_market_item(symbol: str, name: str, latest: dict[str, Any], quote: dict[str, Any], source: str) -> dict[str, Any]:
    price = _to_float(quote.get("price")) or latest.get("price")
    pre_close = _to_float(quote.get("previous_close")) or latest.get("pre_close")
    change = _to_float(quote.get("change")) or latest.get("change")
    change_pct = _to_float(quote.get("change_pct")) or latest.get("change_pct")
    if change is None and price is not None and pre_close:
        change = round(price - pre_close, 4)
    if change_pct is None and change is not None and pre_close:
        change_pct = round(change / pre_close * 100, 4)
    return {
        "symbol": symbol,
        "name": name,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "volume": _to_float(quote.get("volume")) or latest.get("volume"),
        "amount": _to_float(quote.get("amount")) or latest.get("amount"),
        "trade_time": quote.get("quote_time") or latest.get("trade_date"),
        "source": source,
    }


def _load_market_stats(db: Session, trade_date: str | None = None) -> dict[str, Any]:
    daily_table = _preferred_market_latest_daily_table(db)
    if not _has_table(db, daily_table):
        return {}
    target_date = _load_latest_daily_trade_date(db, daily_table, trade_date=trade_date)
    if not target_date:
        return {}
    previous_date = _load_previous_daily_trade_date(db, daily_table, target_date)
    try:
        rows = _load_market_stat_rows(db, daily_table, target_date)
        previous_rows = _load_market_stat_rows(db, daily_table, previous_date) if previous_date else []
    except Exception:
        return {}

    total_amount = 0.0
    previous_amount = sum(float(row.get("amount") or 0.0) for row in previous_rows)
    up_count = 0
    down_count = 0
    flat_count = 0
    limit_up_count = 0
    limit_down_count = 0
    for row in rows:
        close = _to_float(row.get("close"))
        pre_close = _to_float(row.get("pre_close"))
        if close is None or pre_close is None or pre_close <= 0:
            continue
        total_amount += float(row.get("amount") or 0.0)
        change_ratio = (close - pre_close) / pre_close
        if change_ratio > 0:
            up_count += 1
        elif change_ratio < 0:
            down_count += 1
        else:
            flat_count += 1
        if change_ratio >= _limit_up_threshold(row.get("symbol")):
            limit_up_count += 1
        if change_ratio <= _limit_down_threshold(row.get("symbol")):
            limit_down_count += 1

    previous_amount_value = float(previous_amount) if previous_rows else None
    payload = {
        "trade_date": target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date),
        "previous_trade_date": previous_date.isoformat() if hasattr(previous_date, "isoformat") else (str(previous_date) if previous_date else None),
        "stock_count": len(rows),
        "total_amount": round(total_amount, 2),
        "previous_total_amount": round(previous_amount_value, 2) if previous_amount_value is not None else None,
        "amount_change": round(total_amount - previous_amount_value, 2) if previous_amount_value is not None else None,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "limit_up_count": limit_up_count,
        "limit_down_count": limit_down_count,
        "source": f"postgresql:{daily_table}",
    }
    payload.update(
        _derive_market_sentiment_metrics(
            rows,
            previous_rows,
            source=f"postgresql:{daily_table}:daily_ohlc_estimate",
        )
    )
    return payload


def _load_market_stat_rows(db: Session, daily_table: str, trade_date: Any) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in db.execute(
            text(
                f"""
                SELECT symbol, close, high, pre_close, amount
                FROM {daily_table}
                WHERE trade_date = :trade_date
                  AND close IS NOT NULL
                  AND pre_close IS NOT NULL
                  AND pre_close > 0
                """
            ),
            {"trade_date": trade_date},
        ).mappings().all()
    ]


def _load_previous_daily_trade_date(db: Session, table_name: str, target_date: Any):
    try:
        return db.execute(
            text(f"SELECT MAX(trade_date) FROM {table_name} WHERE trade_date < :target_date"),
            {"target_date": target_date},
        ).scalar()
    except Exception:
        return None


def _index_turnover_amount(indices: list[dict[str, Any]]) -> float | None:
    by_symbol = {str(item.get("symbol") or "").upper(): item for item in indices}
    sh_amount = _to_float((by_symbol.get("000001.SH") or {}).get("amount"))
    sz_amount = _to_float((by_symbol.get("399001.SZ") or {}).get("amount"))
    if sh_amount is None or sz_amount is None:
        return None
    return sh_amount + sz_amount


def _limit_up_threshold(symbol: Any) -> float:
    code = str(symbol or "").upper().split(".", 1)[0]
    if code.startswith(("300", "301", "688", "689")):
        return 0.198
    if code.startswith(("4", "8", "9")):
        return 0.298
    return 0.098


def _limit_down_threshold(symbol: Any) -> float:
    code = str(symbol or "").upper().split(".", 1)[0]
    if code.startswith(("300", "301", "688", "689")):
        return -0.198
    if code.startswith(("4", "8", "9")):
        return -0.298
    return -0.098


def _row_price_change_ratio(row: Any, price_field: str) -> float | None:
    price = _to_float(row.get(price_field))
    pre_close = _to_float(row.get("pre_close"))
    if price is None or pre_close is None or pre_close <= 0:
        return None
    return (price - pre_close) / pre_close


def _row_is_limit_up_close(row: Any) -> bool:
    change_ratio = _row_price_change_ratio(row, "close")
    return change_ratio is not None and change_ratio >= _limit_up_threshold(row.get("symbol"))


def _row_is_limit_up_touch(row: Any) -> bool:
    change_ratio = _row_price_change_ratio(row, "high")
    return change_ratio is not None and change_ratio >= _limit_up_threshold(row.get("symbol"))


def _market_symbol_key(symbol: Any) -> str:
    return str(symbol or "").strip().upper()


def _derive_market_sentiment_metrics(
    rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
    *,
    source: str,
) -> dict[str, Any]:
    current_limit_up_symbols: set[str] = set()
    limit_up_touch_count = 0
    failed_limit_up_count = 0
    rows_with_high = 0

    for row in rows:
        symbol = _market_symbol_key(row.get("symbol"))
        if _row_is_limit_up_close(row):
            current_limit_up_symbols.add(symbol)
        if row.get("high") is not None:
            rows_with_high += 1
        if _row_is_limit_up_touch(row):
            limit_up_touch_count += 1
            if not _row_is_limit_up_close(row):
                failed_limit_up_count += 1

    previous_limit_up_symbols = {
        _market_symbol_key(row.get("symbol"))
        for row in previous_rows
        if _row_is_limit_up_close(row)
    }
    promotion_base = len(previous_limit_up_symbols)
    promotion_count = len(previous_limit_up_symbols & current_limit_up_symbols) if promotion_base else None
    promotion_rate = promotion_count / promotion_base * 100 if promotion_base and promotion_count is not None else None
    failed_rate = failed_limit_up_count / limit_up_touch_count * 100 if limit_up_touch_count else (0.0 if rows_with_high else None)

    missing_fields: list[str] = []
    if not rows_with_high:
        missing_fields.append("daily_high")
    if not previous_rows:
        missing_fields.append("previous_session_limit_up_pool")
    elif promotion_base == 0:
        missing_fields.append("previous_session_limit_up_count_zero")

    return {
        "limit_up_touch_count": limit_up_touch_count if rows_with_high else None,
        "failed_limit_up_count": failed_limit_up_count if rows_with_high else None,
        "failed_limit_up_rate": round(failed_rate, 2) if failed_rate is not None else None,
        "limit_up_promotion_base": promotion_base if previous_rows else None,
        "limit_up_promotion_count": promotion_count,
        "limit_up_promotion_rate": round(promotion_rate, 2) if promotion_rate is not None else None,
        "sentiment_source": source,
        "sentiment_missing_fields": missing_fields,
    }


def _load_stock_rankings(db: Session, *, limit: int, trade_date: str | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_table = _preferred_market_latest_daily_table(db)
    if not _has_table(db, daily_table):
        return [], []
    code_to_name = get_reverse_stock_map()
    target_date = _load_latest_daily_trade_date(db, daily_table, trade_date=trade_date)
    if not target_date:
        return [], []
    params = {"target_date": target_date, "limit": int(limit)}
    try:
        gainer_rows = _query_stock_ranking_rows(db, daily_table, params, direction="DESC")
        loser_rows = _query_stock_ranking_rows(db, daily_table, params, direction="ASC")
    except Exception:
        return [], []
    def build_items(rows: list[Any]) -> list[dict[str, Any]]:
        items = []
        for row in rows:
            code = str(row["symbol"])
            symbol = _code_to_symbol(code)
            close = _to_float(row["close"])
            pre_close = _to_float(row["pre_close"])
            if close is None or not pre_close:
                continue
            change = round(close - pre_close, 4)
            change_pct = round(change / pre_close * 100, 4)
            items.append(
                {
                    "symbol": symbol,
                    "name": code_to_name.get(symbol, code),
                    "price": close,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": _to_float(row["volume"]),
                    "amount": _to_float(row["amount"]),
                    "trade_time": row["trade_date"].isoformat(),
                    "source": f"postgresql:{daily_table}",
                }
            )
        return items

    return build_items(gainer_rows), build_items(loser_rows)


def _query_stock_ranking_rows(db: Session, daily_table: str, params: dict[str, Any], *, direction: str):
    order_direction = "ASC" if direction.upper() == "ASC" else "DESC"
    return db.execute(
        text(
            f"""
            SELECT symbol, close, pre_close, volume, amount, trade_date
            FROM {daily_table}
            WHERE trade_date = :target_date
              AND close IS NOT NULL AND pre_close IS NOT NULL AND pre_close > 0
            ORDER BY ((close - pre_close) / NULLIF(pre_close, 0) * 100) {order_direction}
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()


def _preferred_market_latest_daily_table(db: Session) -> str:
    """Pick a fast physical table for latest-day market rankings.

    The incremental compatibility view is useful for historical reads, but it can
    force expensive anti-join scans over the legacy 10M+ row table when the market
    page only needs the latest trading day.
    """
    for table_name in ("stock_daily_kline", "pub_stock_daily_kline"):
        if not _has_table(db, table_name):
            continue
        if _load_latest_daily_trade_date(db, table_name):
            return table_name
    return preferred_daily_kline_table()


def _load_latest_daily_trade_date(db: Session, table_name: str, *, trade_date: str | None = None):
    if not _has_table(db, table_name):
        return None
    date_clause = "WHERE trade_date <= :trade_date" if trade_date else ""
    params = {"trade_date": trade_date} if trade_date else {}
    try:
        return db.execute(
            text(
                f"""
                SELECT trade_date
                FROM {table_name}
                {date_clause}
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ),
            params,
        ).scalar()
    except Exception:
        return None


def _load_sector_rankings(db: Session, *, limit: int, trade_date: str | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily_table = _preferred_market_latest_daily_table(db)
    if not _has_table(db, daily_table) or not _has_column(db, daily_table, "sw_industry_l1"):
        return [], []
    target_date = _load_latest_daily_trade_date(db, daily_table, trade_date=trade_date)
    if not target_date:
        return [], []
    params = {"target_date": target_date, "limit": int(limit)}
    try:
        gainers = _query_sector_ranking_rows(db, daily_table, params, direction="DESC")
        losers = _query_sector_ranking_rows(db, daily_table, params, direction="ASC")
    except Exception:
        return [], []
    return gainers, losers


def _query_sector_ranking_rows(db: Session, daily_table: str, params: dict[str, Any], *, direction: str) -> list[dict[str, Any]]:
    order_direction = "ASC" if direction.upper() == "ASC" else "DESC"
    rows = db.execute(
        text(
            f"""
            SELECT sw_industry_l1 AS sector_name,
                   AVG((close - pre_close) / NULLIF(pre_close, 0) * 100) AS change_pct,
                   COUNT(*) AS member_count,
                   SUM(amount) AS amount
            FROM {daily_table}
            WHERE trade_date = :target_date
              AND sw_industry_l1 IS NOT NULL AND sw_industry_l1 <> ''
              AND close IS NOT NULL AND pre_close IS NOT NULL AND pre_close > 0
            GROUP BY sw_industry_l1
            HAVING COUNT(*) >= 2
            ORDER BY AVG((close - pre_close) / NULLIF(pre_close, 0) * 100) {order_direction}
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return [
        {
            "sector_name": str(row["sector_name"]),
            "change_pct": _to_float(row["change_pct"]),
            "member_count": int(row["member_count"] or 0),
            "amount": _to_float(row["amount"]),
            "source": f"industry_aggregate:{daily_table}",
        }
        for row in rows
        if row["sector_name"] is not None
    ]


def _load_sector_fund_flow(*, limit: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = _load_sector_fund_flow_rows_fast()
    if not rows:
        return [], []
    inflows = sorted(rows, key=lambda item: item.get("net_inflow") or 0, reverse=True)[:limit]
    outflows = sorted(rows, key=lambda item: item.get("net_inflow") or 0)[:limit]
    return inflows, outflows


def _load_sector_fund_flow_rows_fast() -> list[dict[str, Any]]:
    cached_rows = _get_sector_fund_flow_cache(max_age_seconds=SECTOR_FUND_FLOW_TTL_SECONDS)
    if cached_rows:
        return cached_rows

    future, started_at = _ensure_sector_fund_flow_future()
    if future.done():
        return _finish_sector_fund_flow_future(future)
    if time.monotonic() - started_at > SECTOR_FUND_FLOW_WAIT_SECONDS:
        return _get_sector_fund_flow_cache(max_age_seconds=SECTOR_FUND_FLOW_STALE_SECONDS)
    try:
        rows = future.result(timeout=SECTOR_FUND_FLOW_WAIT_SECONDS)
        return _finish_sector_fund_flow_future(future, rows=rows)
    except concurrent.futures.TimeoutError:
        return _get_sector_fund_flow_cache(max_age_seconds=SECTOR_FUND_FLOW_STALE_SECONDS)
    except Exception:
        return _finish_sector_fund_flow_future(future)


def _ensure_sector_fund_flow_future() -> tuple[concurrent.futures.Future[list[dict[str, Any]]], float]:
    global _SECTOR_FUND_FLOW_FUTURE, _SECTOR_FUND_FLOW_STARTED_AT
    with _SECTOR_FUND_FLOW_LOCK:
        if _SECTOR_FUND_FLOW_FUTURE is None:
            _SECTOR_FUND_FLOW_FUTURE = _SECTOR_FUND_FLOW_EXECUTOR.submit(_fetch_sector_fund_flow_rows)
            _SECTOR_FUND_FLOW_STARTED_AT = time.monotonic()
        return _SECTOR_FUND_FLOW_FUTURE, _SECTOR_FUND_FLOW_STARTED_AT


def _get_sector_fund_flow_cache(*, max_age_seconds: int) -> list[dict[str, Any]]:
    with _SECTOR_FUND_FLOW_LOCK:
        updated_at = float(_SECTOR_FUND_FLOW_CACHE.get("updated_at") or 0)
        if not updated_at or time.monotonic() - updated_at > max_age_seconds:
            return []
        rows = _SECTOR_FUND_FLOW_CACHE.get("items") or []
        return [dict(item) for item in rows if isinstance(item, dict)]


def _finish_sector_fund_flow_future(
    future: concurrent.futures.Future[list[dict[str, Any]]],
    *,
    rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    global _SECTOR_FUND_FLOW_FUTURE
    if rows is None:
        try:
            rows = future.result()
        except Exception:
            rows = []
    with _SECTOR_FUND_FLOW_LOCK:
        if _SECTOR_FUND_FLOW_FUTURE is future:
            _SECTOR_FUND_FLOW_FUTURE = None
        if rows:
            _SECTOR_FUND_FLOW_CACHE["items"] = [dict(item) for item in rows]
            _SECTOR_FUND_FLOW_CACHE["updated_at"] = time.monotonic()
            return [dict(item) for item in rows]
    return _get_sector_fund_flow_cache(max_age_seconds=SECTOR_FUND_FLOW_STALE_SECONDS)


def _fetch_sector_fund_flow_rows() -> list[dict[str, Any]]:
    try:
        import akshare as ak
    except Exception:
        return []

    for loader in (_fetch_sector_fund_flow_rows_em, _fetch_sector_fund_flow_rows_ths):
        try:
            rows = loader(ak)
            if rows:
                return rows
        except Exception:
            continue
    return []


def _fetch_sector_fund_flow_rows_em(ak: Any) -> list[dict[str, Any]]:
    if hasattr(ak, "stock_sector_fund_flow_rank"):
        frame = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        source = "akshare:stock_sector_fund_flow_rank"
    elif hasattr(ak, "stock_board_industry_fund_flow_em"):
        frame = ak.stock_board_industry_fund_flow_em(symbol="今日")
        source = "akshare:stock_board_industry_fund_flow_em"
    else:
        return []
    if frame is None or frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        sector_name = str(row.get("名称") or row.get("行业") or "").strip()
        if not sector_name:
            continue
        net_inflow = _to_float(
            row.get("今日主力净流入-净额")
            or row.get("主力净流入-净额")
            or row.get("今日主力净流入")
            or row.get("主力净流入")
        )
        change_pct = _to_float(row.get("今日涨跌幅") or row.get("涨跌幅"))
        rows.append(
            {
                "sector_name": sector_name,
                "change_pct": change_pct,
                "net_inflow": net_inflow,
                "source": source,
            }
        )
    return rows


def _fetch_sector_fund_flow_rows_ths(ak: Any) -> list[dict[str, Any]]:
    if not hasattr(ak, "stock_fund_flow_industry"):
        return []
    frame = ak.stock_fund_flow_industry(symbol="即时")
    if frame is None or frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        sector_name = str(row.get("行业") or row.get("名称") or "").strip()
        if not sector_name:
            continue
        net_inflow = _to_float(row.get("净额"))
        # 同花顺该接口以“亿元”为展示单位，统一转为元，便于前端按亿/万格式化。
        if net_inflow is not None and abs(net_inflow) < 1_000_000:
            net_inflow *= 100_000_000
        rows.append(
            {
                "sector_name": sector_name,
                "change_pct": _to_float(row.get("行业-涨跌幅") or row.get("涨跌幅")),
                "net_inflow": net_inflow,
                "member_count": int(row.get("公司家数") or 0),
                "source": "akshare:stock_fund_flow_industry",
            }
        )
    return rows


def _code_to_symbol(code: str) -> str:
    raw = str(code or "").strip().upper()
    if "." in raw:
        base, suffix = raw.split(".", 1)
        if base.isdigit() and suffix in {"SH", "SZ", "SS", "BJ"}:
            return f"{base}.{'SH' if suffix == 'SS' else suffix}"
    code = raw.split(".", 1)[0]
    if code.startswith(("4", "8")) or code.startswith("92"):
        return f"{code}.BJ"
    if code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _merge_chanlun_kbars(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """K线包含处理：将存在包含关系的相邻K线合并。

    缠论中，相邻两根K线如果一根完全包含另一根（高更高、低更低），
    需要按趋势方向合并为一根新K线：
    - 上升趋势中：取高高（两K线高点取高，低点取高）
    - 下降趋势中：取低低（两K线高点取低，低点取低）
    """
    if len(candles) < 2:
        return list(candles)

    merged: list[dict[str, Any]] = [dict(candles[0])]
    # Determine initial direction from first two candles
    direction = "up" if candles[1]["high"] > candles[0]["high"] else "down"

    for i in range(1, len(candles)):
        current = dict(candles[i])
        prev = merged[-1]

        # Check inclusion: current completely contains prev or vice versa
        cur_contains_prev = current["high"] >= prev["high"] and current["low"] <= prev["low"]
        prev_contains_cur = prev["high"] >= current["high"] and prev["low"] <= current["low"]

        if cur_contains_prev or prev_contains_cur:
            # Inclusion detected – merge based on direction
            if direction == "up":
                merged[-1] = {
                    "date": current["date"],
                    "open": prev["open"],
                    "high": max(prev["high"], current["high"]),
                    "low": max(prev["low"], current["low"]),
                    "close": current["close"],
                    "volume": (prev.get("volume", 0) or 0) + (current.get("volume", 0) or 0),
                    "amount": (prev.get("amount", 0) or 0) + (current.get("amount", 0) or 0),
                }
            else:
                merged[-1] = {
                    "date": current["date"],
                    "open": prev["open"],
                    "high": min(prev["high"], current["high"]),
                    "low": min(prev["low"], current["low"]),
                    "close": current["close"],
                    "volume": (prev.get("volume", 0) or 0) + (current.get("volume", 0) or 0),
                    "amount": (prev.get("amount", 0) or 0) + (current.get("amount", 0) or 0),
                }
        else:
            # No inclusion – update direction
            if current["high"] > prev["high"]:
                direction = "up"
            elif current["low"] < prev["low"]:
                direction = "down"
            merged.append(current)

    return merged


def _detect_fractals(kbars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """在合并后的K线序列上检测顶底分型。

    顶分型：中间K线高点最高，低点最高（三根K线）
    底分型：中间K线低点最低，高点最低（三根K线）
    """
    fractals: list[dict[str, Any]] = []
    for i in range(1, len(kbars) - 1):
        left, mid, right = kbars[i - 1], kbars[i], kbars[i + 1]
        # Top fractal: mid high is highest, mid low is highest
        if mid["high"] > left["high"] and mid["high"] > right["high"] and mid["low"] > left["low"] and mid["low"] > right["low"]:
            fractals.append({"date": mid["date"], "type": "top", "price": mid["high"], "index": i})
        # Bottom fractal: mid low is lowest, mid high is lowest
        if mid["low"] < left["low"] and mid["low"] < right["low"] and mid["high"] < left["high"] and mid["high"] < right["high"]:
            fractals.append({"date": mid["date"], "type": "bottom", "price": mid["low"], "index": i})
    return fractals


def _normalize_fractals(fractals: list[dict[str, Any]], kbars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """标准化分型序列：去重、确保顶底交替、间隔足够。"""
    if not fractals:
        return []

    fractals.sort(key=lambda f: f["index"])
    result: list[dict[str, Any]] = [fractals[0]]

    for f in fractals[1:]:
        last = result[-1]
        # Same type: keep the more extreme one
        if f["type"] == last["type"]:
            if (f["type"] == "top" and f["price"] >= last["price"]) or (
                f["type"] == "bottom" and f["price"] <= last["price"]
            ):
                result[-1] = f
            continue
        # Must have at least 3 merged K-bars between alternating fractals
        if f["index"] - last["index"] < 3:
            continue
        result.append(f)

    return result


def _build_strokes(fractals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从分型序列构建笔。相邻的底-顶构成向上笔，顶-底构成向下笔。"""
    strokes: list[dict[str, Any]] = []
    for i in range(0, len(fractals) - 1, 1):
        start, end = fractals[i], fractals[i + 1]
        if start["type"] == end["type"]:
            continue
        direction = "up" if start["type"] == "bottom" else "down"
        strokes.append({
            "start_date": start["date"],
            "end_date": end["date"],
            "start_price": start["price"],
            "end_price": end["price"],
            "direction": direction,
        })
    return strokes


def _build_segments(strokes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从笔序列构建线段。

    线段由至少3笔构成，方向由第一笔决定。
    线段结束条件：出现反向笔且该反向笔之后又出现同向笔。
    """
    if len(strokes) < 3:
        return []

    segments: list[dict[str, Any]] = []
    seg_start = 0

    for i in range(2, len(strokes)):
        # Check if strokes [seg_start:i+1] form a valid segment
        part = strokes[seg_start:i + 1]
        if len(part) < 3:
            continue

        first_dir = part[0]["direction"]
        # A segment is valid when we have at least one opposite-direction stroke
        # and the segment direction is determined by the first stroke
        has_opposite = any(s["direction"] != first_dir for s in part)

        if has_opposite:
            # Check if the last stroke direction confirms segment end
            # Segment ends when we return to the original direction
            last_dir = part[-1]["direction"]
            if last_dir == first_dir and len(part) >= 3:
                segments.append({
                    "start_date": part[0]["start_date"],
                    "end_date": part[-1]["end_date"],
                    "start_price": part[0]["start_price"],
                    "end_price": part[-1]["end_price"],
                    "direction": first_dir,
                })
                seg_start = i

    return segments


def _build_zhongshu(strokes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从笔序列构建中枢。中枢由连续3笔的重叠区间构成。"""
    zhongshu: list[dict[str, Any]] = []
    for i in range(0, max(len(strokes) - 2, 0)):
        part = strokes[i:i + 3]
        ranges = [
            (min(s["start_price"], s["end_price"]), max(s["start_price"], s["end_price"]))
            for s in part
        ]
        low = max(r[0] for r in ranges)
        high = min(r[1] for r in ranges)
        if low < high:
            zhongshu.append({
                "start_date": part[0]["start_date"],
                "end_date": part[-1]["end_date"],
                "low": round(low, 4),
                "high": round(high, 4),
                "mid": round((low + high) / 2, 4),
            })
    return zhongshu


def _calculate_chanlun_overlay(candles: list[dict[str, Any]]) -> dict[str, Any]:
    """计算缠论叠加指标：分型、笔、线段、中枢、买卖点。

    严格按缠论步骤：
    1. K线包含处理（合并包含关系的相邻K线）
    2. 在合并K线上检测顶底分型
    3. 标准化分型序列
    4. 构建笔
    5. 构建线段
    6. 构建中枢
    7. 推导买卖点
    """
    if len(candles) < 3:
        return {
            "fractals": [],
            "bi": [],
            "segments": [],
            "zhongshu": [],
            "buy_sell_points": [],
            "pending_bi": [],
            "pending_fractals": [],
        }

    # Step 1: K-line inclusion processing
    kbars = _merge_chanlun_kbars(candles)

    # Step 2: Detect fractals on merged K-bars
    raw_fractals = _detect_fractals(kbars)

    # Step 3: Normalize fractal sequence
    fractals = _normalize_fractals(raw_fractals, kbars)

    # Step 4: Build strokes (笔)
    strokes = _build_strokes(fractals)

    # Step 5: Build segments (线段)
    segments = _build_segments(strokes)

    # Step 6: Build zhongshu (中枢)
    zhongshu = _build_zhongshu(strokes)

    # Step 7: Derive buy/sell points
    buy_sell_points = _derive_chanlun_points(fractals, zhongshu)

    # Step 8: Add pending/unconfirmed strokes and fractals for recent K-lines
    pending_strokes: list[dict[str, Any]] = []
    pending_fractals: list[dict[str, Any]] = []

    if kbars:
        last_kbar = kbars[-1]
        last_idx = len(kbars) - 1

        # Detect tentative fractals on the last 2 K-lines (missing right neighbor)
        for offset in (0, 1):
            idx = last_idx - offset
            if idx < 2:
                continue
            left, mid = kbars[idx - 1], kbars[idx]
            # Tentative top: mid high > left high
            if mid["high"] > left["high"]:
                pending_fractals.append({
                    "date": mid["date"], "type": "top", "price": mid["high"],
                    "index": idx, "confirmed": False,
                })
            # Tentative bottom: mid low < left low
            if mid["low"] < left["low"]:
                pending_fractals.append({
                    "date": mid["date"], "type": "bottom", "price": mid["low"],
                    "index": idx, "confirmed": False,
                })

        # Build pending stroke from last confirmed fractal to latest price
        if fractals:
            last_fractal = fractals[-1]
            if last_fractal["index"] < len(kbars) - 2:
                if last_fractal["type"] == "bottom":
                    pending_strokes.append({
                        "start_date": last_fractal["date"],
                        "end_date": last_kbar["date"],
                        "start_price": last_fractal["price"],
                        "end_price": last_kbar["close"],
                        "direction": "up",
                        "confirmed": False,
                    })
                else:
                    pending_strokes.append({
                        "start_date": last_fractal["date"],
                        "end_date": last_kbar["date"],
                        "start_price": last_fractal["price"],
                        "end_price": last_kbar["close"],
                        "direction": "down",
                        "confirmed": False,
                    })

    return {
        "fractals": fractals,
        "bi": strokes,
        "segments": segments,
        "zhongshu": zhongshu,
        "buy_sell_points": buy_sell_points,
        "pending_bi": pending_strokes,
        "pending_fractals": pending_fractals,
    }


def _derive_chanlun_points(fractals: list[dict[str, Any]], zhongshu: list[dict[str, Any]]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    latest_zone = zhongshu[-1] if zhongshu else None
    previous_bottom = None
    previous_top = None
    for fractal in fractals:
        if fractal["type"] == "bottom":
            point_type = "1_buy"
            reason = "底分型确认，疑似一类买点"
            if previous_bottom and fractal["price"] > previous_bottom["price"]:
                point_type = "2_buy"
                reason = "回调底高于前低，疑似二类买点"
            if latest_zone and fractal["price"] > latest_zone["high"]:
                point_type = "3_buy"
                reason = "中枢上方回踩不破，疑似三类买点"
            previous_bottom = fractal
            points.append({"date": fractal["date"], "price": fractal["price"], "type": point_type, "side": "buy", "reason": reason})
        else:
            point_type = "1_sell"
            reason = "顶分型确认，疑似一类卖点"
            if previous_top and fractal["price"] < previous_top["price"]:
                point_type = "2_sell"
                reason = "反弹顶低于前高，疑似二类卖点"
            if latest_zone and fractal["price"] < latest_zone["low"]:
                point_type = "3_sell"
                reason = "中枢下方反抽不回，疑似三类卖点"
            previous_top = fractal
            points.append({"date": fractal["date"], "price": fractal["price"], "type": point_type, "side": "sell", "reason": reason})
    return points


def _append_live_candle(
    candles: list[dict],
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    db: Session | None = None,
    user_id: str | None = None,
) -> None:
    quote = _fetch_realtime_quotes_compat([symbol], timeout_seconds=FAST_QUOTE_TIMEOUT_SECONDS, db=db, user_id=user_id).get(symbol) or {}
    quote_time = str(quote.get("quote_time") or "")
    quote_date = quote_time[:10]
    if not quote_date or quote_date < start_date or quote_date > end_date:
        return
    if candles and candles[-1].get("date") == quote_date:
        return

    price = _to_float(quote.get("price"))
    open_price = _to_float(quote.get("open")) or price
    high = _to_float(quote.get("high")) or price
    low = _to_float(quote.get("low")) or price
    previous_close = _to_float(quote.get("previous_close"))
    if price is None or open_price is None or high is None or low is None:
        return
    change = _to_float(quote.get("change"))
    change_percent = _to_float(quote.get("change_pct"))
    if change is None and previous_close:
        change = round(price - previous_close, 4)
    if change_percent is None and change is not None and previous_close:
        change_percent = round(change / previous_close * 100, 4)
    candles.append(
        {
            "date": quote_date,
            "open": open_price,
            "high": high,
            "low": low,
            "close": price,
            "volume": _to_float(quote.get("volume")),
            "amount": _to_float(quote.get("amount")),
            "change": change,
            "change_percent": change_percent,
            "turnover_rate": None,
        }
    )


def _to_float(value):
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except Exception:
        return None
