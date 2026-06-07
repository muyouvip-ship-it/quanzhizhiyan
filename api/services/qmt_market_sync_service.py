from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timezone, timedelta

from sqlalchemy import text

from api.backtest_data_api import _refresh_daily_kline_cache_from_db
from api.database import SessionLocal
from api.core.utils import env_flag as _env_flag, run_async
from api.services.qmt_market_data_service import (
    build_market_integrity_report,
    capture_intraday_symbols,
    get_index_presets,
    is_index_symbol,
    resolve_market_account_key,
    sync_major_index_daily,
)
from api.services.market_data_pipeline_service import preferred_daily_kline_table
from api.services.qmt_minute_subscription_service import _resolve_capture_symbols
from tradingagents.dataflows.trade_calendar import CN_TZ, is_cn_trading_day


logger = logging.getLogger(__name__)
_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_POLL_SECONDS = 60
_LAST_EOD_SYNC_DATE: date | None = None
_LAST_REPAIR_SYNC_DATE: date | None = None
_LAST_INTRADAY_SELECTION_REFRESH_AT: dict[str, datetime] = {}


@dataclass(frozen=True)
class _MarketSyncTarget:
    user_id: str
    account_key: str
    symbols: list[str]


async def start_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _TASK and not _TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _TASK = asyncio.create_task(_run_loop(), name="qmt-market-sync")


async def stop_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _TASK is not None:
        try:
            await _TASK
        except Exception:
            logger.exception("[qmt-market-sync] stop worker failed")
    _TASK = None
    _STOP_EVENT = None


async def _run_loop() -> None:
    logger.info("[qmt-market-sync] background worker started")
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.to_thread(_scan_and_run_once)
        except Exception:
            logger.exception("[qmt-market-sync] loop iteration failed")
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    logger.info("[qmt-market-sync] background worker stopped")


def _scan_and_run_once() -> None:
    global _LAST_EOD_SYNC_DATE, _LAST_REPAIR_SYNC_DATE
    local_now = datetime.now(timezone.utc).astimezone(CN_TZ)
    if not _is_trading_day(local_now):
        return

    trade_date = local_now.date().isoformat()
    targets = _load_market_sync_targets()
    if not targets:
        return
    all_symbols = _merge_symbols([target.symbols for target in targets])
    run_eod = _should_run_eod_sync(local_now, _LAST_EOD_SYNC_DATE)
    run_repair = _should_run_repair_sync(local_now, _LAST_REPAIR_SYNC_DATE)

    if _is_trading_session(local_now):
        for target in targets:
            capture_result = _capture_intraday_for_target(target, trade_date=trade_date)
            selection_refresh = _refresh_event_driven_selection_after_intraday_capture(
                trigger="qmt-market-sync:intraday",
                user_id=target.user_id,
                local_now=local_now,
                capture_result=capture_result,
            )
            logger.info(
                "[qmt-market-sync] intraday capture user=%s account=%s rows=%s symbols=%s success=%s selection_refresh=%s",
                target.user_id,
                target.account_key,
                capture_result.get("rows", 0),
                len(target.symbols),
                capture_result.get("success"),
                selection_refresh.get("status"),
            )

    if run_eod:
        stock_daily_result = _run_stock_daily_sync(local_now, all_symbols)
        for target in targets:
            _run_eod_sync(local_now, target, stock_daily_result=stock_daily_result)
        _LAST_EOD_SYNC_DATE = local_now.date()

    if run_repair:
        stock_daily_result = _run_stock_daily_sync(local_now, all_symbols)
        for target in targets:
            _run_repair_sync(local_now, target, stock_daily_result=stock_daily_result)
        _LAST_REPAIR_SYNC_DATE = local_now.date()


def _capture_intraday_for_target(target: _MarketSyncTarget, *, trade_date: str) -> dict[str, object]:
    try:
        with SessionLocal() as db:
            return capture_intraday_symbols(
                target.symbols,
                trade_date=trade_date,
                period="1m",
                account_key=target.account_key,
                db=db,
                user_id=target.user_id,
            )
    except Exception as exc:
        logger.warning(
            "[qmt-market-sync] intraday capture failed user=%s account=%s symbols=%s trade_date=%s error=%s",
            target.user_id,
            target.account_key,
            len(target.symbols),
            trade_date,
            exc,
        )
        return {
            "success": False,
            "rows": 0,
            "symbols": list(target.symbols),
            "captured_symbols": [],
            "missing_symbols": list(target.symbols),
            "message": f"QMT盘中分钟线采集异常：{exc}"[:240],
            "source": "qmt_intraday",
            "error": str(exc)[:240],
        }


def _run_eod_sync(local_now: datetime, target: _MarketSyncTarget, *, stock_daily_result: dict[str, object] | None = None) -> None:
    trade_date = local_now.date().isoformat()
    with SessionLocal() as db:
        intraday_result = capture_intraday_symbols(
            target.symbols,
            trade_date=trade_date,
            period="1m",
            account_key=target.account_key,
            db=db,
            user_id=target.user_id,
        )
        index_daily_result = sync_major_index_daily(
            start_date=trade_date,
            end_date=trade_date,
            account_key=target.account_key,
            db=db,
            user_id=target.user_id,
        )
        latest_trade_date = _load_latest_stock_daily_trade_date(db)
        cache_result = _refresh_daily_kline_cache_from_db(
            db,
            start_date=latest_trade_date,
            end_date=latest_trade_date,
        ) if latest_trade_date else {"updated": False, "records": 0}
        integrity = build_market_integrity_report(db, target_date=trade_date)
    stock_daily_result = stock_daily_result or _run_stock_daily_sync(local_now, target.symbols)
    selection_refresh = _refresh_event_driven_selection_after_market_sync(
        trigger="qmt-market-sync:eod",
        user_id=target.user_id,
    )
    logger.info(
        "[qmt-market-sync] eod sync user=%s account=%s trade_date=%s intraday_rows=%s stock_daily_mode=%s stock_daily_records=%s index_daily_rows=%s cache_updated=%s integrity_tables=%s selection_generated=%s selection_errors=%s",
        target.user_id,
        target.account_key,
        trade_date,
        intraday_result.get("rows", 0),
        stock_daily_result.get("mode"),
        stock_daily_result.get("records", 0),
        index_daily_result.get("rows", 0),
        cache_result.get("updated", False),
        ",".join(sorted((integrity.get("tables") or {}).keys())),
        len(selection_refresh.get("generated") or []),
        len(selection_refresh.get("errors") or []),
    )


def _run_repair_sync(local_now: datetime, target: _MarketSyncTarget, *, stock_daily_result: dict[str, object] | None = None) -> None:
    trade_date = local_now.date().isoformat()
    with SessionLocal() as db:
        index_daily_result = sync_major_index_daily(
            start_date=trade_date,
            end_date=trade_date,
            account_key=target.account_key,
            db=db,
            user_id=target.user_id,
        )
        latest_trade_date = _load_latest_stock_daily_trade_date(db)
        cache_result = _refresh_daily_kline_cache_from_db(
            db,
            start_date=latest_trade_date,
            end_date=latest_trade_date,
        ) if latest_trade_date else {"updated": False, "records": 0}
        integrity = build_market_integrity_report(db, target_date=trade_date)
    stock_daily_result = stock_daily_result or _run_stock_daily_sync(local_now, target.symbols)
    selection_refresh = _refresh_event_driven_selection_after_market_sync(
        trigger="qmt-market-sync:repair",
        user_id=target.user_id,
    )
    logger.info(
        "[qmt-market-sync] repair sync user=%s account=%s trade_date=%s stock_daily_mode=%s stock_daily_records=%s index_daily_rows=%s cache_updated=%s integrity_tables=%s selection_generated=%s selection_errors=%s",
        target.user_id,
        target.account_key,
        trade_date,
        stock_daily_result.get("mode"),
        stock_daily_result.get("records", 0),
        index_daily_result.get("rows", 0),
        cache_result.get("updated", False),
        ",".join(sorted((integrity.get("tables") or {}).keys())),
        len(selection_refresh.get("generated") or []),
        len(selection_refresh.get("errors") or []),
    )


def _refresh_event_driven_selection_after_intraday_capture(
    *,
    trigger: str,
    user_id: str,
    local_now: datetime,
    capture_result: dict[str, object],
) -> dict[str, object]:
    if not _env_flag("AI_QUANT_INTRADAY_CAPTURE_REFRESH_SELECTION", "1"):
        return {"status": "skipped", "reason": "disabled"}

    interval_seconds = _env_int("AI_QUANT_INTRADAY_SELECTION_REFRESH_INTERVAL_SECONDS", 55)
    last = _LAST_INTRADAY_SELECTION_REFRESH_AT.get(user_id)
    if last is not None and (local_now - last) < timedelta(seconds=interval_seconds):
        return {"status": "skipped", "reason": "debounced"}

    capture_rows = int(capture_result.get("rows") or 0)
    capture_success = bool(capture_result.get("success"))
    try:
        from api.services import catalyst_selection_service

        payload = catalyst_selection_service.schedule_event_driven_selection_refresh(
            trigger=trigger,
            windows=("24h",),
            limit=10,
            user_id=user_id,
            reason="intraday_capture" if capture_success and capture_rows > 0 else "qmt_no_success_rows",
            context={
                "capture_success": capture_success,
                "capture_rows": capture_rows,
                "source": "qmt_market_sync",
            },
        )
    except Exception as exc:
        logger.exception("[qmt-market-sync] intraday event-driven selection schedule failed trigger=%s user=%s", trigger, user_id)
        return {
            "status": "failed",
            "reason": str(exc)[:240],
            "generated_count": 0,
            "error_count": 1,
            "skipped": False,
            "capture_success": capture_success,
            "capture_rows": capture_rows,
        }

    _LAST_INTRADAY_SELECTION_REFRESH_AT[user_id] = local_now
    scheduled_status = str(payload.get("status") or "scheduled")
    if capture_success and capture_rows > 0:
        status = scheduled_status
        reason = None
    else:
        status = "fallback_running" if scheduled_status == "running" else "fallback_scheduled"
        reason = "qmt_no_success_rows"
    return {
        "status": status,
        "reason": reason,
        "deduped": bool(payload.get("deduped")),
        "scheduled": scheduled_status == "scheduled",
        "generated_count": len(payload.get("generated") or []),
        "error_count": len(payload.get("errors") or []),
        "skipped": bool(payload.get("skipped")),
        "capture_success": capture_success,
        "capture_rows": capture_rows,
        "window_count": len(payload.get("windows") or []),
    }


def _refresh_event_driven_selection_after_market_sync(
    *,
    trigger: str,
    user_id: str,
    windows: tuple[str, ...] = ("premarket", "24h"),
) -> dict[str, object]:
    try:
        from api.services import catalyst_selection_service

        with SessionLocal() as db:
            return catalyst_selection_service.refresh_event_driven_selection(
                db,
                trigger=trigger,
                windows=windows,
                limit=10,
                user_id=user_id,
            )
    except Exception as exc:
        logger.exception("[qmt-market-sync] event-driven selection refresh failed trigger=%s user=%s", trigger, user_id)
        return {
            "trigger": trigger,
            "generated": [],
            "errors": [{"window": "premarket", "error": str(exc)}],
            "skipped": False,
        }


def _run_stock_daily_sync(local_now: datetime, symbols: list[str]) -> dict[str, object]:
    task_ids = _trigger_stock_daily_auto_updates()
    if task_ids:
        return {
            "success": True,
            "mode": "backtest_auto_update",
            "task_ids": task_ids,
            "records": 0,
        }

    stock_codes = _extract_stock_codes(symbols)
    if not stock_codes:
        return {
            "success": False,
            "mode": "skipped_no_stock_symbols",
            "task_ids": [],
            "records": 0,
        }

    return _run_targeted_stock_daily_sync(local_now.date(), stock_codes)


def _trigger_stock_daily_auto_updates() -> list[int]:
    from api.services import backtest_data_auto_update_service

    task_ids: list[int] = []
    with SessionLocal() as db:
        rows = db.execute(text("""
            SELECT id, enabled_data_types
            FROM backtest_data_configs
            WHERE auto_download = TRUE
            ORDER BY updated_at DESC, id DESC
        """)).fetchall()
    for row in rows:
        enabled = {str(item).strip() for item in (row.enabled_data_types or []) if str(item).strip()}
        if "daily_kline" not in enabled:
            continue
        try:
            task_ids.extend(backtest_data_auto_update_service.trigger_config_now(int(row.id)))
        except Exception:
            logger.exception("[qmt-market-sync] trigger stock daily auto update failed config_id=%s", row.id)
    return task_ids


def _run_targeted_stock_daily_sync(trade_day: date, stock_codes: list[str]) -> dict[str, object]:
    from api.data_downloader import DataDownloader

    success_symbols = 0
    error_symbols = 0
    total_records = 0
    samples: list[str] = []
    with SessionLocal() as db:
        downloader = DataDownloader(db)
        for code in stock_codes[:200]:
            try:
                result = run_async(downloader.download_daily_kline(code, trade_day, trade_day, force=True))
            except Exception as exc:
                logger.warning("[qmt-market-sync] targeted stock daily sync failed symbol=%s error=%s", code, exc)
                error_symbols += 1
                continue
            if result.get("success"):
                success_symbols += 1
                total_records += int(result.get("records") or 0)
                if len(samples) < 10:
                    samples.append(code)
            else:
                error_symbols += 1
    return {
        "success": success_symbols > 0,
        "mode": "targeted_daily_sync",
        "records": total_records,
        "success_symbols": success_symbols,
        "error_symbols": error_symbols,
        "sample_symbols": samples,
    }


def _extract_stock_codes(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    codes: list[str] = []
    for symbol in symbols:
        normalized = str(symbol or "").strip().upper()
        if not normalized or is_index_symbol(normalized):
            continue
        code = normalized.split(".", 1)[0]
        if len(code) == 6 and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes


def _load_latest_stock_daily_trade_date(db) -> date | None:
    table_name = preferred_daily_kline_table()
    return db.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).scalar()


def _load_market_sync_targets(preferred_account_key: str | None = None) -> list[_MarketSyncTarget]:
    target_symbols: dict[tuple[str, str], list[str]] = {}
    seen_by_target: dict[tuple[str, str], set[str]] = {}

    def _add_many(key: tuple[str, str], values: list[str]) -> None:
        symbols = target_symbols.setdefault(key, [])
        seen = seen_by_target.setdefault(key, set())
        for item in values:
            symbol = str(item or "").strip().upper()
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)

    with SessionLocal() as db:
        rows = db.execute(text("""
            SELECT *
            FROM backtest_data_configs
            WHERE auto_download = TRUE
              AND data_source_preference = 'qmt'
            ORDER BY updated_at DESC, id DESC
        """)).fetchall()
        for row in rows:
            enabled = {str(item).strip() for item in (row.enabled_data_types or []) if str(item).strip()}
            if not ({"daily_kline", "minute_kline"} & enabled):
                continue
            user_id = str(row.user_id)
            account_key = str(
                resolve_market_account_key(
                    db=db,
                    user_id=user_id,
                    preferred_account_key=preferred_account_key,
                )
                or "paper_sim"
            ).strip() or "paper_sim"
            key = (user_id, account_key)
            _add_many(key, [item["symbol"] for item in get_index_presets()])
            if "minute_kline" in enabled:
                resolved = _resolve_capture_symbols(db, user_id, row.default_symbols or [], account_key=account_key)
                _add_many(key, resolved)
            else:
                _add_many(key, [str(item) for item in (row.default_symbols or []) if str(item).strip()])

    return [
        _MarketSyncTarget(user_id=user_id, account_key=account_key, symbols=symbols[:400])
        for (user_id, account_key), symbols in target_symbols.items()
        if symbols
    ]


def _merge_symbols(groups: list[list[str]]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for group in groups:
        for item in group:
            symbol = str(item or "").strip().upper()
            if symbol and symbol not in seen:
                seen.add(symbol)
                result.append(symbol)
    return result


def _load_active_market_symbols(account_key: str) -> list[str]:
    return _merge_symbols([target.symbols for target in _load_market_sync_targets(preferred_account_key=account_key)])[:400]


def _is_trading_day(local_now: datetime) -> bool:
    try:
        return is_cn_trading_day(local_now.date().isoformat())
    except Exception:
        return local_now.weekday() < 5


def _is_trading_session(local_now: datetime) -> bool:
    current = local_now.time()
    return dtime(9, 30) <= current <= dtime(11, 30) or dtime(13, 0) <= current <= dtime(15, 0)



def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, str(default))))
    except Exception:
        return default


def _should_run_eod_sync(local_now: datetime, last_sync_date: date | None) -> bool:
    if last_sync_date == local_now.date():
        return False
    if not _is_trading_day(local_now):
        return False
    return local_now.time() >= dtime(15, 35)


def _should_run_repair_sync(local_now: datetime, last_sync_date: date | None) -> bool:
    if last_sync_date == local_now.date():
        return False
    if not _is_trading_day(local_now):
        return False
    return local_now.time() >= dtime(18, 30)
