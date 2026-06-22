from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any, Callable, Iterable
from uuid import uuid4

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from api.core.utils import safe_float as _safe_float
from api.database import SessionLocal, engine
from api.services.market_data_pipeline_service import (
    ingest_raw_daily_rows,
    ingest_raw_minute_rows,
    publish_minute_trade_date_batched,
    publish_minute_trade_date,
    reconcile_daily_trade_dates,
)
from api.services.qmt_market_data_service import get_index_presets, normalize_market_symbol
from tradingagents.dataflows.trade_calendar import CN_TZ, is_cn_trading_day


logger = logging.getLogger(__name__)

TDX_SERVERS = [
    ("180.153.18.170", 7709),
    ("180.153.18.171", 7709),
    ("202.108.253.130", 7709),
    ("202.108.253.131", 7709),
    ("119.147.212.81", 7709),
]
KLINE_PAGE_SIZE = 800
DEFAULT_MINUTE_PAGES = max(int(os.getenv("TDX_MINUTE_KLINE_PAGES", "5") or 5), 1)
DEFAULT_DAILY_INGEST_BATCH_SYMBOLS = max(int(os.getenv("TDX_DAILY_INGEST_BATCH_SYMBOLS", "250") or 250), 1)
DEFAULT_DAILY_FETCH_WORKERS = max(int(os.getenv("TDX_DAILY_FETCH_WORKERS", "3") or 3), 1)
DEFAULT_MINUTE_INGEST_BATCH_ROWS = max(int(os.getenv("TDX_MINUTE_INGEST_BATCH_ROWS", "60000") or 60000), 1)
DEFAULT_MINUTE_FETCH_WORKERS = max(int(os.getenv("TDX_MINUTE_FETCH_WORKERS", "3") or 3), 1)
DEFAULT_MINUTE_SKIP_MIN_BARS_PER_DAY = max(int(os.getenv("TDX_MINUTE_SKIP_MIN_BARS_PER_DAY", "200") or 200), 1)


def describe_tdx_capabilities() -> dict[str, Any]:
    return {
        "market_kline": {
            "daily_kline": "get_security_bars 支持股票日K，原始字段只有 open/high/low/close/vol/amount/datetime；pre_close/涨跌幅需要用上一根日K派生。",
            "minute_kline": "get_security_bars 支持近期股票1分钟K，单次每页800根；历史很久的分钟线不保证全量。",
            "index_data": "支持主要指数日K。",
            "index_minute_kline": "支持主要指数近期1分钟K。",
            "realtime_quote": "get_security_quotes 支持实时盘口/价格/last_close，但只适合当前交易日快照，不适合作为历史日K字段源。",
        },
        "extra_data": {
            "finance": "get_finance_info 可取基础财务快照和股本字段（liutongguben/zongguben），可用于按收盘价派生流通/总市值；不是完整财报三表。",
            "xdxr": "get_xdxr_info 可取除权除息。",
            "f10": "get_company_info_category/content 可取F10/公司资料文本，结构化程度低。",
            "block": "板块/分类能力依赖通达信本地block文件或F10文本；行情服务器不直接返回申万行业中文名，不适合作为服务端稳定源。",
            "chip": "标准pytdx行情接口没有可靠筹码分布接口。",
            "research_reports": "标准pytdx行情接口没有稳定研报API。",
        },
    }


def sync_stock_daily_history(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: list[str] | None = None,
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start is None or end is None or start > end:
        raise ValueError("start_date / end_date is invalid")

    universe = resolve_stock_universe(symbols)
    if not universe:
        return {"success": False, "rows": 0, "symbols": [], "error": "TDX未获取到股票池"}

    requested_symbols = [item["symbol"] for item in universe]
    completed_symbols = _load_completed_daily_symbols(start=start, end=end, symbols=requested_symbols)
    skipped_symbols = 0
    if completed_symbols:
        original_count = len(universe)
        universe = [item for item in universe if item["symbol"] not in completed_symbols]
        skipped_symbols = original_count - len(universe)

    if not universe:
        expected_dates = _expected_daily_trade_dates(start, end)
        published_count = 0
        if expected_dates:
            reconcile_result = reconcile_daily_trade_dates(trade_dates=expected_dates, symbols=requested_symbols if symbols else None)
            published_count = int(reconcile_result.get("published_count") or 0)
        _progress(progress_callback, 100, f"TDX 股票日K已是最新，跳过 {skipped_symbols} 只已入库股票，发布 {published_count} 条")
        return {
            "success": True,
            "rows": 0,
            "published_count": published_count,
            "symbols": requested_symbols,
            "symbol_rows": {},
            "success_symbols": 0,
            "error_symbols": 0,
            "skipped_symbols": skipped_symbols,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "source": "tdx",
        }

    skip_text = f"，跳过已入库 {skipped_symbols} 只" if skipped_symbols else ""
    _progress(progress_callback, 3, f"TDX 股票日K同步启动，待同步 {len(universe)} 只{skip_text}")
    total_rows = 0
    success_symbols = 0
    error_symbols = 0
    trade_dates: set[date] = set()
    symbol_rows: dict[str, int] = {}
    pending_rows: list[dict[str, Any]] = []
    pending_symbols: set[str] = set()
    fetch_workers = min(DEFAULT_DAILY_FETCH_WORKERS, len(universe))
    thread_state = threading.local()
    api_lock = threading.Lock()
    created_apis: list[Any] = []

    def flush_pending() -> None:
        nonlocal total_rows
        if not pending_rows:
            return
        ingest_result = ingest_raw_daily_rows(source="tdx", rows=pending_rows, batch_id=uuid4().hex)
        if not ingest_result.get("success"):
            raise RuntimeError(str(ingest_result.get("error") or "TDX日K批量写入失败"))
        row_count = int(ingest_result.get("rows") or 0)
        total_rows += row_count
        trade_dates.update(_as_date(value) for value in ingest_result.get("trade_dates") or [] if _as_date(value) is not None)
        pending_rows.clear()
        pending_symbols.clear()

    def worker_api() -> Any:
        api = getattr(thread_state, "api", None)
        if api is None:
            api = _connect_tdx()
            thread_state.api = api
            with api_lock:
                created_apis.append(api)
        return api

    def fetch_item(item: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        api = worker_api()
        rows = _fetch_security_bars(
            api,
            category=_tdx_params().KLINE_TYPE_DAILY,
            market=item["market"],
            code=item["code"],
            start=start,
            end=end,
        )
        return item, rows

    def handle_rows(item: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        nonlocal success_symbols, error_symbols
        if rows:
            payload = [_bar_to_daily_row(bar, item["symbol"]) for bar in rows]
            pending_rows.extend(payload)
            pending_symbols.add(item["symbol"])
            success_symbols += 1
            symbol_rows[item["symbol"]] = len(payload)
            if len(pending_symbols) >= DEFAULT_DAILY_INGEST_BATCH_SYMBOLS:
                flush_pending()
        else:
            error_symbols += 1

    try:
        if fetch_workers <= 1:
            api = worker_api()
            for index, item in enumerate(universe, start=1):
                rows = _fetch_security_bars(
                    api,
                    category=_tdx_params().KLINE_TYPE_DAILY,
                    market=item["market"],
                    code=item["code"],
                    start=start,
                    end=end,
                )
                handle_rows(item, rows)
                effective_done = skipped_symbols + index
                if index % 25 == 0 or index == len(universe):
                    _progress(
                        progress_callback,
                        _bounded_progress(effective_done, len(requested_symbols)),
                        f"TDX 股票日K {effective_done}/{len(requested_symbols)}，写入 {total_rows + len(pending_rows)} 条",
                    )
        else:
            _progress(progress_callback, 3, f"TDX 股票日K并发拉取启动，并发 {fetch_workers}，待同步 {len(universe)} 只")
            with ThreadPoolExecutor(max_workers=fetch_workers, thread_name_prefix="tdx-daily") as executor:
                futures = {executor.submit(fetch_item, item): item for item in universe}
                for index, future in enumerate(as_completed(futures), start=1):
                    item = futures[future]
                    try:
                        _, rows = future.result()
                    except Exception as exc:
                        error_symbols += 1
                        logger.warning("TDX 股票日K拉取失败 symbol=%s error=%s", item.get("symbol"), exc)
                        rows = []
                    else:
                        handle_rows(item, rows)
                    effective_done = skipped_symbols + index
                    if index % 25 == 0 or index == len(universe):
                        _progress(
                            progress_callback,
                            _bounded_progress(effective_done, len(requested_symbols)),
                            f"TDX 股票日K {effective_done}/{len(requested_symbols)}，写入 {total_rows + len(pending_rows)} 条",
                        )
        flush_pending()
    finally:
        for api in created_apis:
            _disconnect_tdx(api)

    published_count = 0
    if trade_dates:
        reconcile_result = reconcile_daily_trade_dates(trade_dates=trade_dates, symbols=requested_symbols if symbols else None)
        published_count = int(reconcile_result.get("published_count") or 0)

    return {
        "success": total_rows > 0,
        "rows": total_rows,
        "published_count": published_count,
        "symbols": requested_symbols,
        "symbol_rows": symbol_rows,
        "success_symbols": success_symbols,
        "error_symbols": error_symbols,
        "skipped_symbols": skipped_symbols,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source": "tdx",
    }


def sync_stock_minute_history(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: list[str] | None = None,
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start is None or end is None or start > end:
        raise ValueError("start_date / end_date is invalid")

    universe = resolve_stock_universe(symbols)
    if not universe:
        return {"success": False, "rows": 0, "symbols": [], "error": "TDX未获取到股票池"}

    requested_symbols = [item["symbol"] for item in universe]
    completed_symbols = _load_completed_minute_symbols(start=start, end=end, symbols=requested_symbols)
    skipped_symbols = 0
    if completed_symbols:
        original_count = len(universe)
        universe = [item for item in universe if item["symbol"] not in completed_symbols]
        skipped_symbols = original_count - len(universe)

    if not universe:
        trade_dates = _expected_daily_trade_dates(start, end)
        published_count = 0
        for trade_day in trade_dates:
            publish_result = publish_minute_trade_date_batched(
                trade_date=trade_day,
                symbols=requested_symbols,
                minimum_coverage_ratio=0.0,
            )
            published_count += int(publish_result.get("published_count") or 0)
        _progress(progress_callback, 100, f"TDX 股票1分钟K已是最新，跳过 {skipped_symbols} 只已入库股票，发布 {published_count} 只")
        return {
            "success": True,
            "rows": 0,
            "published_count": published_count,
            "symbols": requested_symbols,
            "symbol_rows": {},
            "success_symbols": 0,
            "error_symbols": 0,
            "skipped_symbols": skipped_symbols,
            "trade_dates": [item.isoformat() for item in trade_dates],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "source": "tdx",
        }

    skip_text = f"，跳过已入库 {skipped_symbols} 只" if skipped_symbols else ""
    _progress(progress_callback, 3, f"TDX 股票1分钟K同步启动，待同步 {len(universe)} 只{skip_text}")
    total_rows = 0
    success_symbols = 0
    error_symbols = 0
    trade_dates: set[date] = set()
    symbol_rows: dict[str, int] = {}
    pending_rows: list[dict[str, Any]] = []
    fetch_workers = min(DEFAULT_MINUTE_FETCH_WORKERS, len(universe))
    thread_state = threading.local()
    api_lock = threading.Lock()
    created_apis: list[Any] = []

    def flush_pending() -> None:
        nonlocal total_rows
        if not pending_rows:
            return
        ingest_result = ingest_raw_minute_rows(source="tdx", rows=pending_rows, batch_id=uuid4().hex)
        if not ingest_result.get("success"):
            raise RuntimeError(str(ingest_result.get("error") or "TDX分钟线批量写入失败"))
        total_rows += int(ingest_result.get("rows") or 0)
        trade_dates.update(_as_date(value) for value in ingest_result.get("trade_dates") or [] if _as_date(value) is not None)
        pending_rows.clear()

    def worker_api() -> Any:
        api = getattr(thread_state, "api", None)
        if api is None:
            api = _connect_tdx()
            thread_state.api = api
            with api_lock:
                created_apis.append(api)
        return api

    def fetch_item(item: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        api = worker_api()
        rows = _fetch_security_bars(
            api,
            category=_tdx_params().KLINE_TYPE_1MIN,
            market=item["market"],
            code=item["code"],
            start=start,
            end=end,
            max_pages=DEFAULT_MINUTE_PAGES,
        )
        return item, rows

    def handle_rows(item: dict[str, Any], rows: list[dict[str, Any]]) -> None:
        nonlocal success_symbols, error_symbols
        if rows:
            payload = [_bar_to_minute_row(bar, item["symbol"]) for bar in rows]
            pending_rows.extend(payload)
            success_symbols += 1
            symbol_rows[item["symbol"]] = len(payload)
            if len(pending_rows) >= DEFAULT_MINUTE_INGEST_BATCH_ROWS:
                flush_pending()
        else:
            error_symbols += 1

    try:
        if fetch_workers <= 1:
            api = worker_api()
            for index, item in enumerate(universe, start=1):
                _, rows = fetch_item(item)
                handle_rows(item, rows)
                effective_done = skipped_symbols + index
                if index % 10 == 0 or index == len(universe):
                    _progress(
                        progress_callback,
                        _bounded_progress(effective_done, len(requested_symbols)),
                        f"TDX 股票1分钟K {effective_done}/{len(requested_symbols)}，写入 {total_rows + len(pending_rows)} 条",
                    )
        else:
            _progress(progress_callback, 3, f"TDX 股票1分钟K并发拉取启动，并发 {fetch_workers}，待同步 {len(universe)} 只")
            with ThreadPoolExecutor(max_workers=fetch_workers, thread_name_prefix="tdx-minute") as executor:
                futures = {executor.submit(fetch_item, item): item for item in universe}
                for index, future in enumerate(as_completed(futures), start=1):
                    item = futures[future]
                    try:
                        _, rows = future.result()
                    except Exception as exc:
                        error_symbols += 1
                        logger.warning("TDX 股票1分钟K拉取失败 symbol=%s error=%s", item.get("symbol"), exc)
                        rows = []
                    else:
                        handle_rows(item, rows)
                    effective_done = skipped_symbols + index
                    if index % 10 == 0 or index == len(universe):
                        _progress(
                            progress_callback,
                            _bounded_progress(effective_done, len(requested_symbols)),
                            f"TDX 股票1分钟K {effective_done}/{len(requested_symbols)}，写入 {total_rows + len(pending_rows)} 条",
                        )
        flush_pending()
    finally:
        for api in created_apis:
            _disconnect_tdx(api)

    published_count = 0
    for trade_day in sorted(trade_dates):
        publish_result = publish_minute_trade_date_batched(
            trade_date=trade_day,
            symbols=requested_symbols,
            minimum_coverage_ratio=0.0,
        )
        published_count += int(publish_result.get("published_count") or 0)

    return {
        "success": total_rows > 0,
        "rows": total_rows,
        "published_count": published_count,
        "symbols": requested_symbols,
        "symbol_rows": symbol_rows,
        "success_symbols": success_symbols,
        "error_symbols": error_symbols,
        "skipped_symbols": skipped_symbols,
        "trade_dates": [item.isoformat() for item in sorted(trade_dates)],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source": "tdx",
    }


def sync_index_daily_history(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: list[str] | None = None,
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    return _sync_index_history(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        category=_tdx_params().KLINE_TYPE_DAILY,
        table_name="index_daily_kline",
        progress_label="TDX 指数日K",
        progress_callback=progress_callback,
    )


def sync_index_minute_history(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: list[str] | None = None,
    progress_callback: Callable[[int, str], None] | None = None,
) -> dict[str, Any]:
    return _sync_index_history(
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        category=_tdx_params().KLINE_TYPE_1MIN,
        table_name="index_minute_kline",
        progress_label="TDX 指数1分钟K",
        progress_callback=progress_callback,
        max_pages=DEFAULT_MINUTE_PAGES,
    )


def resolve_stock_universe(symbols: list[str] | None = None) -> list[dict[str, Any]]:
    requested = {normalize_market_symbol(item) for item in (symbols or []) if normalize_market_symbol(item)}
    if requested:
        return [_stock_item(symbol) for symbol in sorted(requested) if _stock_item(symbol)]

    rows: list[dict[str, Any]] = []
    api = _connect_tdx()
    try:
        for market in (0, 1, 2):
            count = int(api.get_security_count(market) or 0)
            for start in range(0, count, 1000):
                for item in api.get_security_list(market, start) or []:
                    code = str(item.get("code") or "").strip()
                    name = str(item.get("name") or "").strip()
                    symbol = _normalize_tdx_stock_symbol(market, code)
                    if not symbol or not _is_a_share_code(code):
                        continue
                    rows.append({"symbol": symbol, "code": code, "market": market, "name": name})
    finally:
        _disconnect_tdx(api)

    if rows:
        deduped = {item["symbol"]: item for item in rows}
        return [deduped[key] for key in sorted(deduped)]
    return _fallback_stock_universe_from_db()


def _sync_index_history(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: list[str] | None,
    category: int,
    table_name: str,
    progress_label: str,
    progress_callback: Callable[[int, str], None] | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start is None or end is None or start > end:
        raise ValueError("start_date / end_date is invalid")

    index_items = _resolve_index_items(symbols)
    if not index_items:
        return {"success": False, "rows": 0, "symbols": [], "error": "未获取到指数列表"}

    requested_symbols = [item["symbol"] for item in index_items]
    min_bars_per_day = DEFAULT_MINUTE_SKIP_MIN_BARS_PER_DAY if table_name == "index_minute_kline" else 1
    completed_symbols = _load_completed_index_symbols(
        table_name=table_name,
        start=start,
        end=end,
        symbols=requested_symbols,
        min_bars_per_day=min_bars_per_day,
    )
    skipped_symbols = 0
    if completed_symbols:
        original_count = len(index_items)
        index_items = [item for item in index_items if item["symbol"] not in completed_symbols]
        skipped_symbols = original_count - len(index_items)

    if not index_items:
        _progress(progress_callback, 100, f"{progress_label} 已是最新，跳过 {skipped_symbols} 个已入库指数")
        return {
            "success": True,
            "rows": 0,
            "symbols": requested_symbols,
            "symbol_rows": {},
            "missing_symbols": [],
            "skipped_symbols": skipped_symbols,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "source": "tdx",
        }

    skip_text = f"，跳过已入库 {skipped_symbols} 个" if skipped_symbols else ""
    _progress(progress_callback, 5, f"{progress_label} 同步启动，待同步指数数 {len(index_items)}{skip_text}")
    total_rows = 0
    symbol_rows: dict[str, int] = {}
    missing_symbols: list[str] = []
    pending_rows: list[dict[str, Any]] = []
    api = _connect_tdx()
    try:
        for index, item in enumerate(index_items, start=1):
            bars = _fetch_index_bars(
                api,
                category=category,
                market=item["market"],
                code=item["code"],
                start=start,
                end=end,
                max_pages=max_pages,
            )
            payload = [_bar_to_index_row(bar, item["symbol"], table_name=table_name) for bar in bars]
            if payload:
                pending_rows.extend(payload)
                total_rows += len(payload)
                symbol_rows[item["symbol"]] = len(payload)
            else:
                missing_symbols.append(item["symbol"])
            _progress(
                progress_callback,
                _bounded_progress(skipped_symbols + index, len(requested_symbols)),
                f"{progress_label} {skipped_symbols + index}/{len(requested_symbols)}，写入 {total_rows} 条",
            )
    finally:
        _disconnect_tdx(api)

    inserted = _upsert_index_rows(table_name, pending_rows) if pending_rows else 0

    return {
        "success": inserted > 0,
        "rows": inserted,
        "symbols": requested_symbols,
        "symbol_rows": symbol_rows,
        "missing_symbols": missing_symbols,
        "skipped_symbols": skipped_symbols,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source": "tdx",
    }


def _fetch_security_bars(
    api: Any,
    *,
    category: int,
    market: int,
    code: str,
    start: date,
    end: date,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pages = max_pages or 400
    daily_category = _is_daily_category(category)
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
    for page in range(pages):
        data = api.get_security_bars(category, market, code, page * KLINE_PAGE_SIZE, KLINE_PAGE_SIZE)
        if not data:
            break
        for bar in data:
            trade_at = _bar_datetime(bar)
            if trade_at is None:
                continue
            if start_dt <= trade_at < end_dt:
                rows.append(dict(bar))
        oldest_dt = _bar_datetime(data[0])
        newest_dt = _bar_datetime(data[-1])
        oldest_day = oldest_dt.date() if oldest_dt is not None else _bar_date(data[0])
        newest_day = newest_dt.date() if newest_dt is not None else _bar_date(data[-1])
        if newest_day is not None and newest_day < start:
            break
        if daily_category and oldest_day is not None and oldest_day <= start:
            break
        if not daily_category and oldest_dt is not None and oldest_dt <= start_dt:
            break
        if len(data) < KLINE_PAGE_SIZE:
            break
    rows.sort(key=lambda item: _bar_datetime(item) or datetime.min)
    return rows


def _fetch_index_bars(
    api: Any,
    *,
    category: int,
    market: int,
    code: str,
    start: date,
    end: date,
    max_pages: int | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pages = max_pages or 400
    daily_category = _is_daily_category(category)
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
    for page in range(pages):
        data = api.get_index_bars(category, market, code, page * KLINE_PAGE_SIZE, KLINE_PAGE_SIZE)
        if not data:
            break
        for bar in data:
            trade_at = _bar_datetime(bar)
            if trade_at is None:
                continue
            if start_dt <= trade_at < end_dt:
                rows.append(dict(bar))
        oldest_dt = _bar_datetime(data[0])
        newest_dt = _bar_datetime(data[-1])
        oldest_day = oldest_dt.date() if oldest_dt is not None else _bar_date(data[0])
        newest_day = newest_dt.date() if newest_dt is not None else _bar_date(data[-1])
        if newest_day is not None and newest_day < start:
            break
        if daily_category and oldest_day is not None and oldest_day <= start:
            break
        if not daily_category and oldest_dt is not None and oldest_dt <= start_dt:
            break
        if len(data) < KLINE_PAGE_SIZE:
            break
    rows.sort(key=lambda item: _bar_datetime(item) or datetime.min)
    return rows


def _upsert_index_rows(table_name: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    now = datetime.now(CN_TZ).replace(tzinfo=None)
    for row in rows:
        row["created_at"] = now
        row["updated_at"] = now
    if table_name == "index_daily_kline":
        statement = text(
            """
            INSERT INTO index_daily_kline
            (symbol, trade_date, open, high, low, close, volume, amount, source, created_at, updated_at)
            VALUES
            (:symbol, :trade_date, :open, :high, :low, :close, :volume, :amount, :source, :created_at, :updated_at)
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                source = EXCLUDED.source,
                updated_at = EXCLUDED.updated_at
            """
        )
    elif table_name == "index_minute_kline":
        statement = text(
            """
            INSERT INTO index_minute_kline
            (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            (:symbol, :trade_time, :open, :high, :low, :close, :volume, :amount, :created_at, :updated_at)
            ON CONFLICT (symbol, trade_time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                updated_at = EXCLUDED.updated_at
            """
        )
    else:
        raise ValueError(f"unsupported index table: {table_name}")
    with engine.begin() as conn:
        conn.execute(statement, rows)
    return len(rows)


def _connect_tdx() -> Any:
    try:
        from pytdx.hq import TdxHq_API
    except Exception as exc:
        raise RuntimeError("当前运行环境未安装 pytdx，无法使用通达信数据源") from exc

    last_error: Exception | None = None
    servers = _configured_servers()
    for host, port in servers:
        api = TdxHq_API(heartbeat=True, auto_retry=True)
        try:
            if api.connect(host, int(port)):
                return api
        except Exception as exc:
            last_error = exc
        try:
            api.disconnect()
        except Exception:
            pass
    raise RuntimeError(f"TDX服务器连接失败: {last_error or servers}")


def _disconnect_tdx(api: Any) -> None:
    try:
        api.disconnect()
    except Exception:
        pass


def _configured_servers() -> list[tuple[str, int]]:
    raw = os.getenv("TDX_HQ_SERVERS", "").strip()
    if not raw:
        return TDX_SERVERS
    result: list[tuple[str, int]] = []
    for item in raw.split(","):
        host, _, port_text = item.strip().partition(":")
        if not host:
            continue
        try:
            result.append((host, int(port_text or 7709)))
        except ValueError:
            result.append((host, 7709))
    return result or TDX_SERVERS


def _tdx_params() -> Any:
    from pytdx.params import TDXParams

    return TDXParams


def _stock_item(symbol: str) -> dict[str, Any] | None:
    normalized = normalize_market_symbol(symbol)
    if not normalized or "." not in normalized:
        return None
    code, suffix = normalized.split(".", 1)
    if not _is_a_share_code(code):
        return None
    market = 1 if suffix == "SH" else 2 if suffix == "BJ" else 0
    return {"symbol": normalized, "code": code, "market": market, "name": ""}


def _fallback_stock_universe_from_db() -> list[dict[str, Any]]:
    with SessionLocal() as db:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT symbol
                FROM stock_daily_kline
                WHERE symbol IS NOT NULL AND symbol <> ''
                ORDER BY symbol
                """
            )
        ).fetchall()
    return [item for item in (_stock_item(row[0]) for row in rows) if item]


def _load_completed_daily_symbols(*, start: date, end: date, symbols: list[str]) -> set[str]:
    if not symbols:
        return set()
    expected_dates = _expected_daily_trade_dates(start, end)
    if not expected_dates:
        return set()
    try:
        with SessionLocal() as db:
            statement = text(
                """
                SELECT symbol, COUNT(DISTINCT trade_date) AS date_count
                FROM raw_stock_daily_kline_tdx
                WHERE trade_date BETWEEN :start_date AND :end_date
                  AND symbol IN :symbols
                GROUP BY symbol
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = db.execute(
                statement,
                {"start_date": start, "end_date": end, "symbols": symbols},
            ).fetchall()
    except SQLAlchemyError as exc:
        logger.info("TDX raw daily table unavailable while checking resume state: %s", exc)
        return set()
    required_count = len(expected_dates)
    return {str(row.symbol) for row in rows if int(row.date_count or 0) >= required_count}


def _load_completed_minute_symbols(*, start: date, end: date, symbols: list[str]) -> set[str]:
    if not symbols:
        return set()
    expected_dates = _expected_daily_trade_dates(start, end)
    if not expected_dates:
        return set()
    try:
        with SessionLocal() as db:
            statement = text(
                """
                SELECT symbol,
                       COUNT(DISTINCT trade_date) AS date_count,
                       MIN(day_bars) AS min_day_bars
                FROM (
                    SELECT symbol, trade_date, COUNT(*) AS day_bars
                    FROM raw_stock_minute_kline_tdx
                    WHERE trade_date BETWEEN :start_date AND :end_date
                      AND symbol IN :symbols
                    GROUP BY symbol, trade_date
                ) t
                GROUP BY symbol
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = db.execute(
                statement,
                {"start_date": start, "end_date": end, "symbols": symbols},
            ).fetchall()
    except SQLAlchemyError as exc:
        logger.info("TDX raw minute table unavailable while checking resume state: %s", exc)
        return set()
    required_count = len(expected_dates)
    return {
        str(row.symbol)
        for row in rows
        if int(row.date_count or 0) >= required_count and int(row.min_day_bars or 0) >= DEFAULT_MINUTE_SKIP_MIN_BARS_PER_DAY
    }


def _load_completed_index_symbols(
    *,
    table_name: str,
    start: date,
    end: date,
    symbols: list[str],
    min_bars_per_day: int = 1,
) -> set[str]:
    if not symbols:
        return set()
    expected_dates = _expected_daily_trade_dates(start, end)
    if not expected_dates:
        return set()
    if table_name == "index_daily_kline":
        date_expr = "trade_date"
        source_table = "index_daily_kline"
    elif table_name == "index_minute_kline":
        date_expr = "DATE(trade_time)"
        source_table = "index_minute_kline"
    else:
        return set()
    try:
        with SessionLocal() as db:
            statement = text(
                f"""
                SELECT symbol,
                       COUNT(DISTINCT trade_date) AS date_count,
                       MIN(day_bars) AS min_day_bars
                FROM (
                    SELECT symbol, {date_expr} AS trade_date, COUNT(*) AS day_bars
                    FROM {source_table}
                    WHERE {date_expr} BETWEEN :start_date AND :end_date
                      AND symbol IN :symbols
                    GROUP BY symbol, {date_expr}
                ) t
                GROUP BY symbol
                """
            ).bindparams(bindparam("symbols", expanding=True))
            rows = db.execute(
                statement,
                {"start_date": start, "end_date": end, "symbols": symbols},
            ).fetchall()
    except SQLAlchemyError as exc:
        logger.info("TDX index table unavailable while checking resume state table=%s error=%s", table_name, exc)
        return set()
    required_count = len(expected_dates)
    return {
        str(row.symbol)
        for row in rows
        if int(row.date_count or 0) >= required_count and int(row.min_day_bars or 0) >= min_bars_per_day
    }


def _normalize_tdx_stock_symbol(market: int, code: str) -> str:
    if not code or len(code) != 6 or not code.isdigit():
        return ""
    suffix = "SH" if market == 1 else "BJ" if market == 2 else "SZ"
    return f"{code}.{suffix}"


def _is_a_share_code(code: str) -> bool:
    if not code or len(code) != 6 or not code.isdigit():
        return False
    return code.startswith(("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689", "430", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "920"))


def _resolve_index_items(symbols: list[str] | None) -> list[dict[str, Any]]:
    presets = get_index_presets()
    by_symbol = {normalize_market_symbol(item["symbol"]): item for item in presets}
    requested = [normalize_market_symbol(item) for item in (symbols or []) if normalize_market_symbol(item)]
    selected = [by_symbol[item] for item in requested if item in by_symbol] if requested else presets
    result: list[dict[str, Any]] = []
    for item in selected:
        symbol = normalize_market_symbol(item.get("symbol"))
        code = str(item.get("code") or symbol.split(".", 1)[0]).strip()
        market = _index_market(symbol)
        result.append({"symbol": symbol, "code": code, "market": market, "name": item.get("name") or ""})
    return result


def _index_market(symbol: str) -> int:
    normalized = normalize_market_symbol(symbol)
    if normalized.endswith(".SH"):
        return 1
    if normalized.endswith(".BJ"):
        return 2
    return 0


def _bar_to_daily_row(bar: dict[str, Any], symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "trade_date": _bar_date(bar),
        "open": _safe_float(bar.get("open")),
        "high": _safe_float(bar.get("high")),
        "low": _safe_float(bar.get("low")),
        "close": _safe_float(bar.get("close")),
        "volume": _safe_float(bar.get("vol") or bar.get("volume")),
        "amount": _safe_float(bar.get("amount")),
    }


def _bar_to_minute_row(bar: dict[str, Any], symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "trade_time": _bar_datetime(bar),
        "open": _safe_float(bar.get("open")),
        "high": _safe_float(bar.get("high")),
        "low": _safe_float(bar.get("low")),
        "close": _safe_float(bar.get("close")),
        "volume": int(float(bar.get("vol") or bar.get("volume") or 0)),
        "amount": _safe_float(bar.get("amount")) or 0.0,
    }


def _bar_to_index_row(bar: dict[str, Any], symbol: str, *, table_name: str) -> dict[str, Any]:
    if table_name == "index_daily_kline":
        return {**_bar_to_daily_row(bar, symbol), "source": "tdx"}
    return _bar_to_minute_row(bar, symbol)


def _bar_date(bar: dict[str, Any]) -> date | None:
    try:
        return date(int(bar["year"]), int(bar["month"]), int(bar["day"]))
    except Exception:
        return _parse_date(bar.get("datetime"))


def _bar_datetime(bar: dict[str, Any]) -> datetime | None:
    try:
        return datetime(
            int(bar["year"]),
            int(bar["month"]),
            int(bar["day"]),
            int(bar.get("hour") or 0),
            int(bar.get("minute") or 0),
        )
    except Exception:
        value = str(bar.get("datetime") or "").strip()
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None


def _parse_date(value: Any) -> date | None:
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


def _as_date(value: Any) -> date | None:
    return _parse_date(value)


def _expected_daily_trade_dates(start: date, end: date) -> list[date]:
    result: list[date] = []
    current = start
    while current <= end:
        try:
            is_trading = bool(is_cn_trading_day(current.isoformat()))
        except Exception:
            is_trading = current.weekday() < 5
        if is_trading:
            result.append(current)
        current += timedelta(days=1)
    return result


def _is_daily_category(category: int) -> bool:
    try:
        return int(category) == int(_tdx_params().KLINE_TYPE_DAILY)
    except Exception:
        return False


def _bounded_progress(index: int, total: int) -> int:
    if total <= 0:
        return 100
    return max(5, min(99, int(index / total * 98)))


def _progress(callback: Callable[[int, str], None] | None, progress: int, message: str) -> None:
    if callable(callback):
        callback(max(0, min(int(progress), 100)), message)


def count_cn_trading_days(start: date, end: date) -> int:
    current = start
    count = 0
    while current <= end:
        try:
            is_trading = bool(is_cn_trading_day(current.isoformat()))
        except Exception:
            is_trading = current.weekday() < 5
        if is_trading:
            count += 1
        current = date.fromordinal(current.toordinal() + 1)
    return count
