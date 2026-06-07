from __future__ import annotations

import asyncio
import logging
import os
import threading
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import text

from api.database import SessionLocal
from api.services.market_data_pipeline_service import preferred_daily_kline_table, preferred_minute_kline_table
from tradingagents.dataflows.trade_calendar import is_cn_trading_day
from api.core.utils import run_async, env_flag as _env_flag


logger = logging.getLogger(__name__)
_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_POLL_SECONDS = 60
_STALE_TASK_MINUTES = max(int(os.getenv("BACKTEST_DATA_TASK_STALE_MINUTES", "120") or 120), 30)



def is_worker_enabled() -> bool:
    return _env_flag("ENABLE_BACKTEST_AUTO_UPDATE_WORKER", "0")


def is_worker_running() -> bool:
    return bool(_TASK is not None and not _TASK.done())


async def start_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _TASK and not _TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _TASK = asyncio.create_task(_run_loop(), name="backtest-data-auto-update")


async def stop_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _TASK is not None:
        try:
            await _TASK
        except Exception:
            logger.exception("[backtest-auto-update] stop worker failed")
    _TASK = None
    _STOP_EVENT = None


async def _run_loop() -> None:
    logger.info("[backtest-auto-update] background worker started")
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.to_thread(_scan_and_run_once)
        except Exception:
            logger.exception("[backtest-auto-update] loop iteration failed")
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    logger.info("[backtest-auto-update] background worker stopped")


def _scan_and_run_once() -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        recovered = recover_stale_running_tasks(db, now=now)
        if recovered:
            db.commit()
            logger.warning("[backtest-auto-update] recovered stale running tasks count=%s", recovered)
        rows = db.execute(text("""
            SELECT *
            FROM backtest_data_configs
            WHERE auto_download = TRUE
            ORDER BY updated_at DESC, id DESC
        """)).fetchall()
        for row in rows:
            if not _should_run(row, now):
                continue
            _run_single_config(row.id, now)


def trigger_config_now(config_id: int) -> list[int]:
    now = datetime.now(timezone.utc)
    return _run_single_config(config_id, now, force=True)


def get_config_status(config_id: int, *, user_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    with SessionLocal() as db:
        row = db.execute(text("""
            SELECT *
            FROM backtest_data_configs
            WHERE id = :config_id
        """), {"config_id": config_id}).fetchone()
        if row is None:
            raise ValueError("订阅配置不存在")
        if user_id and str(row.user_id) != str(user_id):
            raise ValueError("无权访问该订阅配置")
        return _build_status_payload(db, row, now)


def _should_run(row, now: datetime) -> bool:
    timezone_name = str(getattr(row, "timezone", None) or "Asia/Shanghai").strip() or "Asia/Shanghai"
    local_now = now.astimezone(_safe_zoneinfo(timezone_name))
    schedule_time = _parse_schedule_time(str(getattr(row, "schedule_time", None) or "18:30"))
    if schedule_time is None:
        schedule_time = (18, 30)
    if (local_now.hour, local_now.minute) < schedule_time:
        return False

    local_today = local_now.date()
    if bool(getattr(row, "only_trading_day", True)):
        try:
            if not is_cn_trading_day(local_today.isoformat()):
                return False
        except Exception:
            logger.warning("[backtest-auto-update] trading day check failed config=%s", getattr(row, "id", None))

    frequency = str(row.update_frequency or "daily").strip().lower()
    last_run = _ensure_utc(getattr(row, "last_run_at", None))
    if last_run is None:
        return True
    local_last_run = last_run.astimezone(_safe_zoneinfo(timezone_name))
    if frequency == "weekly":
        return local_last_run.date() <= (local_today - timedelta(days=7))
    if frequency == "monthly":
        return (local_last_run.year, local_last_run.month) != (local_now.year, local_now.month)
    if local_last_run.date() < local_today:
        return True
    if local_last_run.date() > local_today:
        return False

    last_run_time = (local_last_run.hour, local_last_run.minute)
    current_time = (local_now.hour, local_now.minute)
    if last_run_time < schedule_time <= current_time:
        return True

    last_success = _ensure_utc(getattr(row, "last_success_at", None))
    retry_minutes = max(int(os.getenv("BACKTEST_AUTO_UPDATE_RETRY_MINUTES", "30") or 30), 5)
    if (last_success is None or last_success < last_run) and (local_now - local_last_run) >= timedelta(minutes=retry_minutes):
        return True
    return False


def _run_single_config(config_id: int, now: datetime, *, force: bool = False) -> list[int]:
    with SessionLocal() as db:
        row = db.execute(text("""
            SELECT *
            FROM backtest_data_configs
            WHERE id = :config_id
        """), {"config_id": config_id}).fetchone()
        if row is None:
            return []
        recovered = recover_stale_running_tasks(db, now=now, user_id=str(row.user_id), config_id=int(row.id))
        if recovered:
            db.commit()
            logger.warning("[backtest-auto-update] recovered stale tasks before config run config=%s count=%s", config_id, recovered)
        data_types = [str(item).strip() for item in (row.enabled_data_types or []) if str(item).strip()]
        if not data_types:
            stats_rows = db.execute(text("""
                SELECT DISTINCT data_type
                FROM backtest_data_stats
                WHERE last_updated_date IS NOT NULL
            """)).fetchall()
            data_types = [str(item[0]).strip() for item in stats_rows if str(item[0]).strip()]
        if not data_types:
            logger.info("[backtest-auto-update] skip config=%s no data types", config_id)
            return []

        task_ids: list[int] = []
        blocked_by_running = False
        target_data_date = _resolve_target_data_date_for_config(row, now)
        for data_type in data_types:
            scope_key = _scope_key(row.default_symbols or [])
            data_source = _effective_subscription_data_source(data_type, row.data_source_preference)
            date_range = _resolve_incremental_date_range(
                db,
                config_id=int(row.id),
                user_id=str(row.user_id),
                data_type=data_type,
                data_source=data_source,
                scope_key=scope_key,
                default_days=row.default_date_range_days or 365,
                target_date=target_data_date,
            )
            if date_range is None:
                continue
            start_date, end_date = date_range
            running = db.execute(text("""
                SELECT id
                FROM backtest_data_tasks
                WHERE user_id = :user_id
                  AND task_type = :task_type
                  AND COALESCE(subscription_config_id, 0) = :config_id
                  AND status IN ('pending', 'running')
                LIMIT 1
            """), {"user_id": row.user_id, "task_type": data_type, "config_id": int(row.id)}).fetchone()
            if running is not None:
                blocked_by_running = True
                continue
            _touch_watermark(
                db,
                user_id=str(row.user_id),
                config_id=int(row.id),
                data_type=data_type,
                data_source=data_source,
                scope_key=scope_key,
                last_run_started_at=now,
                last_status="running",
                last_error=None,
            )
            result = db.execute(text("""
                INSERT INTO backtest_data_tasks
                (user_id, task_type, data_source, date_range_start, date_range_end, symbols, status, error_message, subscription_config_id, trigger_mode)
                VALUES (:user_id, :task_type, :data_source, :date_range_start, :date_range_end, :symbols, 'pending', :error_message, :subscription_config_id, :trigger_mode)
                RETURNING id
            """), {
                "user_id": row.user_id,
                "task_type": data_type,
                "data_source": data_source,
                "date_range_start": start_date,
                "date_range_end": end_date,
                "symbols": row.default_symbols or [],
                "error_message": "自动更新任务已创建",
                "subscription_config_id": int(row.id),
                "trigger_mode": "manual" if force else "scheduled",
            })
            task_ids.append(int(result.fetchone()[0]))

        if not task_ids:
            if blocked_by_running:
                logger.info("[backtest-auto-update] skip config=%s because tasks are already running", config_id)
                return []
            db.execute(text("""
                UPDATE backtest_data_configs
                SET last_updated_at = :last_updated_at,
                    last_run_at = :last_run_at,
                    updated_at = NOW()
                WHERE id = :config_id
            """), {"config_id": config_id, "last_updated_at": now, "last_run_at": now})
            db.commit()
            return []

        db.execute(text("""
            UPDATE backtest_data_configs
            SET last_updated_at = :last_updated_at,
                last_run_at = :last_run_at,
                updated_at = NOW()
            WHERE id = :config_id
        """), {"config_id": config_id, "last_updated_at": now, "last_run_at": now})
        db.commit()

    thread = threading.Thread(
        target=_launch_download_worker,
        args=(task_ids, str(row.user_id)),
        daemon=True,
        name=f"backtest-auto-update-{config_id}",
    )
    thread.start()
    logger.info("[backtest-auto-update] %s config=%s tasks=%s", "manual-triggered" if force else "scheduled", config_id, task_ids)
    return task_ids


def _launch_download_worker(task_ids: list[int], user_id: str) -> None:
    from api.backtest_data_api import _process_batch_download

    try:
        run_async(_process_batch_download(task_ids, user_id))
    except Exception:
        logger.exception("[backtest-auto-update] background download failed user=%s tasks=%s", user_id, task_ids)


def _resolve_incremental_date_range(
    db,
    *,
    config_id: int,
    user_id: str,
    data_type: str,
    data_source: str,
    scope_key: str,
    default_days: int,
    target_date: date | None = None,
) -> tuple[date, date] | None:
    target_date = target_date or _resolve_target_data_date(datetime.now().date())
    actual_data_end = _resolve_actual_data_end(db, data_type=data_type, symbols=None if scope_key == "all" else _symbols_from_scope_key(scope_key))
    watermark = db.execute(text("""
        SELECT *
        FROM backtest_data_watermarks
        WHERE user_id = :user_id
          AND config_id = :config_id
          AND data_type = :data_type
          AND COALESCE(data_source, '') = :data_source
          AND scope_key = :scope_key
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
    """), {
        "user_id": user_id,
        "config_id": config_id,
        "data_type": data_type,
        "data_source": data_source,
        "scope_key": scope_key,
    }).fetchone()
    if actual_data_end is not None:
        if actual_data_end >= target_date:
            return None
        return actual_data_end + timedelta(days=1), target_date

    if watermark is not None and getattr(watermark, "last_data_date", None):
        last_data_date = watermark.last_data_date
        if last_data_date >= target_date:
            return None
        return last_data_date + timedelta(days=1), target_date

    stat = db.execute(text("""
        SELECT *
        FROM backtest_data_stats
        WHERE data_type = :data_type AND symbol IS NULL
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
    """), {"data_type": data_type}).fetchone()
    if stat is None:
        start = target_date - timedelta(days=max(int(default_days or 365) - 1, 0))
        return start, target_date

    existing_end = stat.date_range_end or stat.last_updated_date
    last_updated_date = stat.last_updated_date
    if data_type in {"minute_kline", "index_minute_kline"}:
        if last_updated_date == target_date and existing_end == target_date:
            return None
        if existing_end and existing_end < target_date:
            return existing_end + timedelta(days=1), target_date
        return target_date, target_date

    if last_updated_date == target_date and existing_end and existing_end >= target_date:
        return None
    if existing_end and existing_end < target_date:
        return existing_end + timedelta(days=1), target_date
    return target_date, target_date


def _resolve_target_data_date(today: date) -> date:
    current = today
    while not _is_likely_cn_trading_day(current):
        current -= timedelta(days=1)
    return current


def _resolve_target_data_date_for_config(row, now: datetime) -> date:
    timezone_name = str(getattr(row, "timezone", None) or "Asia/Shanghai").strip() or "Asia/Shanghai"
    local_now = now.astimezone(_safe_zoneinfo(timezone_name))
    schedule_time = _parse_schedule_time(str(getattr(row, "schedule_time", None) or "18:30")) or (18, 30)
    target = local_now.date()
    if (local_now.hour, local_now.minute) < schedule_time:
        target -= timedelta(days=1)
    return _resolve_target_data_date(target)


def _is_likely_cn_trading_day(value: date) -> bool:
    try:
        return bool(is_cn_trading_day(value.isoformat()))
    except Exception:
        return value.weekday() < 5


def _resolve_actual_data_end(db, *, data_type: str, symbols: list[str] | None) -> date | None:
    table_mapping = {
        "daily_kline": (preferred_daily_kline_table(), "trade_date"),
        "index_data": ("index_daily_data", "trade_date"),
        "minute_kline": (preferred_minute_kline_table(), "trade_time"),
        "index_minute_kline": ("index_minute_kline", "trade_time"),
    }
    table_info = table_mapping.get(data_type)
    if table_info is None:
        return None
    table_name, date_column = table_info
    if data_type == "daily_kline" and table_name == "market_stock_daily_kline":
        return _resolve_market_daily_actual_end(db, symbols=symbols)
    date_expr = date_column if date_column == "trade_date" else f"DATE({date_column})"
    if symbols:
        row = db.execute(text(f"""
            SELECT MAX({date_expr}) AS max_date
            FROM {table_name}
            WHERE symbol = ANY(:symbols)
        """), {"symbols": symbols}).fetchone()
    else:
        row = db.execute(text(f"""
            SELECT MAX({date_expr}) AS max_date
            FROM {table_name}
        """)).fetchone()
    return row.max_date if row and getattr(row, "max_date", None) else None


def _resolve_market_daily_actual_end(db, *, symbols: list[str] | None) -> date | None:
    values: list[date] = []
    for table_name in ("stock_daily_kline", "pub_stock_daily_kline"):
        if not _relation_exists(db, table_name):
            continue
        if symbols:
            row = db.execute(text(f"""
                SELECT MAX(trade_date) AS max_date
                FROM {table_name}
                WHERE symbol = ANY(:symbols)
            """), {"symbols": symbols}).fetchone()
        else:
            row = db.execute(text(f"""
                SELECT MAX(trade_date) AS max_date
                FROM {table_name}
            """)).fetchone()
        value = row.max_date if row and getattr(row, "max_date", None) else None
        if value is not None:
            values.append(value)
    return max(values) if values else None


def _relation_exists(db, table_name: str) -> bool:
    return bool(db.execute(text("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = :table_name
    """), {"table_name": table_name}).scalar())


def _symbols_from_scope_key(scope_key: str) -> list[str]:
    if not scope_key or scope_key == "all":
        return []
    if not scope_key.startswith("symbols:"):
        return []
    raw = scope_key.split(":", 1)[1]
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _scope_key(symbols: list[str]) -> str:
    normalized = sorted({str(item).strip().upper() for item in (symbols or []) if str(item).strip()})
    if not normalized:
        return "all"
    return "symbols:" + ",".join(normalized[:200])


def _effective_subscription_data_source(data_type: str, preferred: str | None) -> str:
    # Daily K-line subscriptions are backed by QuantClass. Keep alternate
    # daily sources available only for explicit one-off download tasks.
    if str(data_type or "").strip() == "daily_kline":
        return "quantclass"
    return str(preferred or "akshare").strip() or "akshare"


def _build_status_payload(db, row, now: datetime) -> dict:
    timezone_name = str(getattr(row, "timezone", None) or "Asia/Shanghai").strip() or "Asia/Shanghai"
    next_run_at = _compute_next_run_at(row, now)
    config_enabled = bool(getattr(row, "auto_download", False))
    worker_enabled = is_worker_enabled()
    worker_running = is_worker_running()
    if not config_enabled:
        effective_status = "disabled"
        status_message = "订阅规则未启用，后台不会自动创建增量任务。"
    elif worker_enabled and worker_running:
        effective_status = "active"
        status_message = "订阅规则已启用，后台自动更新 worker 正在运行。"
    elif worker_enabled:
        effective_status = "config_only"
        status_message = "订阅规则已启用，但当前进程内后台自动更新 worker 未运行；请检查后端启动日志。"
    else:
        effective_status = "config_only"
        status_message = "订阅规则已启用，但 ENABLE_BACKTEST_AUTO_UPDATE_WORKER 未开启，后台不会自动执行。"
    latest_task = db.execute(text("""
        SELECT *
        FROM backtest_data_tasks
        WHERE user_id = :user_id
          AND COALESCE(subscription_config_id, 0) = :config_id
          AND COALESCE(data_source, '') = :data_source
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """), {
        "user_id": row.user_id,
        "config_id": int(row.id),
        "data_source": _effective_subscription_data_source(
            "daily_kline" if "daily_kline" in (getattr(row, "enabled_data_types", None) or []) else "",
            getattr(row, "data_source_preference", None),
        ),
    }).fetchone()
    running_task_count = db.execute(text("""
        SELECT COUNT(*)
        FROM backtest_data_tasks
        WHERE user_id = :user_id
          AND COALESCE(subscription_config_id, 0) = :config_id
          AND status IN ('pending', 'running')
    """), {"user_id": row.user_id, "config_id": int(row.id)}).scalar() or 0
    watermark_rows = db.execute(text("""
        SELECT *
        FROM backtest_data_watermarks
        WHERE user_id = :user_id
          AND config_id = :config_id
        ORDER BY updated_at DESC, id DESC
        LIMIT 20
    """), {"user_id": row.user_id, "config_id": int(row.id)}).fetchall()

    watermarks: list[dict] = []
    latest_watermark_date: date | None = None
    intraday_capture: dict | None = None
    for item in watermark_rows:
        payload = {
            "data_type": item.data_type,
            "data_source": item.data_source,
            "scope_key": item.scope_key,
            "last_data_date": item.last_data_date.isoformat() if getattr(item, "last_data_date", None) else None,
            "last_run_started_at": item.last_run_started_at.isoformat() if getattr(item, "last_run_started_at", None) else None,
            "last_success_at": item.last_success_at.isoformat() if getattr(item, "last_success_at", None) else None,
            "last_status": item.last_status,
            "last_error": item.last_error,
            "updated_at": item.updated_at.isoformat() if getattr(item, "updated_at", None) else None,
        }
        if str(item.data_type) == "minute_kline_intraday":
            intraday_capture = payload
            continue
        watermarks.append(payload)
        if getattr(item, "last_data_date", None) and (latest_watermark_date is None or item.last_data_date > latest_watermark_date):
            latest_watermark_date = item.last_data_date

    return {
        "config_id": int(row.id),
        "auto_download": config_enabled,
        "config_enabled": config_enabled,
        "worker_enabled": worker_enabled,
        "worker_running": worker_running,
        "effective_status": effective_status,
        "status_message": status_message,
        "timezone": timezone_name,
        "next_run_at": next_run_at.isoformat() if next_run_at else None,
        "now": now.isoformat(),
        "running_task_count": int(running_task_count),
        "latest_task": _task_row_to_payload(latest_task) if latest_task else None,
        "watermarks": watermarks,
        "latest_watermark_date": latest_watermark_date.isoformat() if latest_watermark_date else None,
        "intraday_capture": intraday_capture,
    }


def _compute_next_run_at(row, now: datetime) -> datetime | None:
    if not bool(getattr(row, "auto_download", False)):
        return None
    timezone_name = str(getattr(row, "timezone", None) or "Asia/Shanghai").strip() or "Asia/Shanghai"
    local_zone = _safe_zoneinfo(timezone_name)
    local_now = now.astimezone(local_zone)
    schedule_time = _parse_schedule_time(str(getattr(row, "schedule_time", None) or "18:30")) or (18, 30)
    frequency = str(getattr(row, "update_frequency", None) or "daily").strip().lower() or "daily"
    candidate = local_now.replace(hour=schedule_time[0], minute=schedule_time[1], second=0, microsecond=0)
    last_run = _ensure_utc(getattr(row, "last_run_at", None))
    last_run_local_date = last_run.astimezone(local_zone).date() if last_run else None

    if frequency == "weekly":
        if candidate <= local_now or last_run_local_date == local_now.date():
            candidate = candidate + timedelta(days=7)
    elif frequency == "monthly":
        if candidate <= local_now or (last_run and last_run.astimezone(local_zone).year == local_now.year and last_run.astimezone(local_zone).month == local_now.month):
            year = candidate.year + (1 if candidate.month == 12 else 0)
            month = 1 if candidate.month == 12 else candidate.month + 1
            candidate = candidate.replace(year=year, month=month, day=1)
    else:
        if candidate <= local_now or last_run_local_date == local_now.date():
            candidate = candidate + timedelta(days=1)

    if bool(getattr(row, "only_trading_day", True)):
        while True:
            try:
                if is_cn_trading_day(candidate.date().isoformat()):
                    break
            except Exception:
                break
            candidate = candidate + timedelta(days=1)
            candidate = candidate.replace(hour=schedule_time[0], minute=schedule_time[1], second=0, microsecond=0)
    return candidate.astimezone(timezone.utc)


def _task_row_to_payload(row) -> dict:
    return {
        "id": int(row.id),
        "task_type": row.task_type,
        "data_source": row.data_source,
        "status": row.status,
        "progress": int(getattr(row, "progress", 0) or 0),
        "total_records": int(getattr(row, "total_records", 0) or 0),
        "downloaded_records": int(getattr(row, "downloaded_records", 0) or 0),
        "trigger_mode": getattr(row, "trigger_mode", None),
        "date_range_start": row.date_range_start.isoformat() if getattr(row, "date_range_start", None) else None,
        "date_range_end": row.date_range_end.isoformat() if getattr(row, "date_range_end", None) else None,
        "created_at": row.created_at.isoformat() if getattr(row, "created_at", None) else None,
        "updated_at": row.updated_at.isoformat() if getattr(row, "updated_at", None) else None,
        "completed_at": row.completed_at.isoformat() if getattr(row, "completed_at", None) else None,
        "error_message": row.error_message,
    }


def recover_stale_running_tasks(
    db,
    *,
    now: datetime | None = None,
    user_id: str | None = None,
    config_id: int | None = None,
    stale_minutes: int | None = None,
) -> int:
    del now
    effective_stale_minutes = int(stale_minutes if stale_minutes is not None else _STALE_TASK_MINUTES)
    params: dict[str, object] = {
        "stale_minutes": effective_stale_minutes,
        "error_message": f"任务超过 {effective_stale_minutes} 分钟未更新，已自动标记失败；可重新触发订阅。",
    }
    filters = ["status IN ('pending', 'running')", "updated_at < (NOW() - (:stale_minutes * INTERVAL '1 minute'))"]
    if user_id is not None:
        filters.append("user_id = :user_id")
        params["user_id"] = str(user_id)
    if config_id is not None:
        filters.append("COALESCE(subscription_config_id, 0) = :config_id")
        params["config_id"] = int(config_id)

    rows = db.execute(text(f"""
        SELECT id, user_id, task_type, data_source, symbols, subscription_config_id
        FROM backtest_data_tasks
        WHERE {" AND ".join(filters)}
        ORDER BY updated_at ASC, id ASC
        LIMIT 200
    """), params).fetchall()
    if not rows:
        return 0

    task_ids = [int(row.id) for row in rows]
    db.execute(text("""
        UPDATE backtest_data_tasks
        SET status = 'failed',
            error_message = :error_message,
            completed_at = NOW(),
            updated_at = NOW()
        WHERE id = ANY(:task_ids)
    """), {**params, "task_ids": task_ids})

    for row in rows:
        if not getattr(row, "subscription_config_id", None):
            continue
        task_symbols = list(row.symbols or [])
        scope_key = "all"
        if task_symbols:
            scope_key = "symbols:" + ",".join(sorted({str(item).strip().upper() for item in task_symbols if str(item).strip()})[:200])
        watermark = db.execute(text("""
            SELECT id
            FROM backtest_data_watermarks
            WHERE user_id = :user_id
              AND config_id = :config_id
              AND data_type = :data_type
              AND COALESCE(data_source, '') = :data_source
              AND scope_key = :scope_key
            LIMIT 1
        """), {
            "user_id": str(row.user_id),
            "config_id": int(row.subscription_config_id),
            "data_type": str(row.task_type or ""),
            "data_source": str(row.data_source or ""),
            "scope_key": scope_key,
        }).fetchone()
        if watermark:
            db.execute(text("""
                UPDATE backtest_data_watermarks
                SET last_status = 'failed',
                    last_error = :error_message,
                    updated_at = NOW()
                WHERE id = :id
            """), {
                "id": int(watermark.id),
                "error_message": params["error_message"],
            })
    return len(rows)


def _touch_watermark(
    db,
    *,
    user_id: str,
    config_id: int,
    data_type: str,
    data_source: str,
    scope_key: str,
    last_run_started_at: datetime | None = None,
    last_data_date: date | None = None,
    last_success_at: datetime | None = None,
    last_status: str | None = None,
    last_error: str | None = None,
) -> None:
    existing = db.execute(text("""
        SELECT id
        FROM backtest_data_watermarks
        WHERE user_id = :user_id
          AND config_id = :config_id
          AND data_type = :data_type
          AND COALESCE(data_source, '') = :data_source
          AND scope_key = :scope_key
        LIMIT 1
    """), {
        "user_id": user_id,
        "config_id": config_id,
        "data_type": data_type,
        "data_source": data_source,
        "scope_key": scope_key,
    }).fetchone()
    payload = {
        "user_id": user_id,
        "config_id": config_id,
        "data_type": data_type,
        "data_source": data_source,
        "scope_key": scope_key,
        "last_run_started_at": last_run_started_at,
        "last_data_date": last_data_date,
        "last_success_at": last_success_at,
        "last_status": last_status,
        "last_error": last_error,
    }
    if existing is None:
        db.execute(text("""
            INSERT INTO backtest_data_watermarks
            (user_id, config_id, data_type, data_source, scope_key, last_run_started_at, last_data_date, last_success_at, last_status, last_error, created_at, updated_at)
            VALUES (:user_id, :config_id, :data_type, :data_source, :scope_key, :last_run_started_at, :last_data_date, :last_success_at, :last_status, :last_error, NOW(), NOW())
        """), payload)
    else:
        db.execute(text("""
            UPDATE backtest_data_watermarks
            SET last_run_started_at = COALESCE(:last_run_started_at, last_run_started_at),
                last_data_date = COALESCE(:last_data_date, last_data_date),
                last_success_at = COALESCE(:last_success_at, last_success_at),
                last_status = COALESCE(:last_status, last_status),
                last_error = :last_error,
                updated_at = NOW()
            WHERE id = :id
        """), {**payload, "id": existing.id})


def _parse_schedule_time(value: str) -> tuple[int, int] | None:
    text_value = str(value or "").strip()
    if not text_value or ":" not in text_value:
        return None
    try:
        hour_text, minute_text = text_value.split(":", 1)
        hour = max(0, min(int(hour_text), 23))
        minute = max(0, min(int(minute_text), 59))
        return hour, minute
    except Exception:
        return None


def _safe_zoneinfo(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("Asia/Shanghai")


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
