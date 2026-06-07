from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from sqlalchemy import and_, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.core.strategy_db import get_strategy_db_ctx
from api.database import get_db_ctx
from api.models.strategy_models import (
    RealtimeApprovalDB,
    RealtimeEventDB,
    RealtimeMonitorDB,
    RealtimeSignalExecutionDB,
)
from api.services.data_source_governance import (
    build_realtime_monitor_governance,
    build_realtime_positions_governance,
)
from api.services.qmt_market_data_service import fetch_intraday_bars
from api.services import qmt_virtual_account_service, watchlist_service
from api.services.minute_data_service import evaluate_first_day_band_signals, evaluate_intraday_confirmation
from api.services.qmt_realtime_minute_capture_service import capture_today_minute_bars
from api.services.strategy_dsl_compiler import compile_strategy_dsl
from api.services.strategy_platform_repository import get_platform_strategy
from tradingagents.dataflows.trade_calendar import is_cn_trading_day


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REALTIME_LOG_PATH = PROJECT_ROOT / "realtime_monitor.runtime.log"

_WORKER_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_POLL_SECONDS = 5
_EVENT_SUBSCRIBERS: dict[tuple[str, str], list[tuple[asyncio.AbstractEventLoop, asyncio.Queue[dict[str, Any]]]]] = {}
_EVENT_SUBSCRIBERS_LOCK = threading.Lock()
_SIGNAL_PROCESSED_EVENT_TYPES = {
    "signal_generated",
    "signal_blocked",
    "order_rejected",
    "approval_created",
    "order_intent",
    "order_submitted",
    "order_error",
}
_FUSE_EVENT_COOLDOWN_SECONDS = 300
_REPETITIVE_STATUS_EVENT_COOLDOWN_SECONDS = 300


def subscribe_event_queue(user_id: str, monitor_id: str) -> asyncio.Queue[dict[str, Any]]:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
    key = (str(user_id), str(monitor_id))
    with _EVENT_SUBSCRIBERS_LOCK:
        _EVENT_SUBSCRIBERS.setdefault(key, []).append((loop, queue))
    return queue


def unsubscribe_event_queue(user_id: str, monitor_id: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
    key = (str(user_id), str(monitor_id))
    with _EVENT_SUBSCRIBERS_LOCK:
        subscribers = [
            item for item in _EVENT_SUBSCRIBERS.get(key, [])
            if item[1] is not queue
        ]
        if subscribers:
            _EVENT_SUBSCRIBERS[key] = subscribers
        else:
            _EVENT_SUBSCRIBERS.pop(key, None)


def _publish_event_to_subscribers(event: RealtimeEventDB) -> None:
    payload = event.to_dict()
    key = (str(event.user_id), str(event.monitor_id))
    with _EVENT_SUBSCRIBERS_LOCK:
        subscribers = list(_EVENT_SUBSCRIBERS.get(key, []))
    for loop, queue in subscribers:
        if loop.is_closed():
            continue
        loop.call_soon_threadsafe(_queue_event_drop_oldest, queue, payload)


def _queue_event_drop_oldest(queue: asyncio.Queue[dict[str, Any]], payload: dict[str, Any]) -> None:
    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    queue.put_nowait(payload)


def create_monitor(strategy_db: Session, main_db: Session, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    strategy = _require_strategy(strategy_db, str(payload.get("strategy_id") or ""))
    compiled = _compile_strategy_payload(strategy)
    if compiled.status != "passed":
        raise ValueError("策略 DSL 编译未通过，不能创建实时监控实例：" + "；".join(compiled.errors))

    account_key = str(payload.get("account_key") or "paper_sim").strip() or "paper_sim"
    account_role = _account_role(account_key, db=main_db, user_id=user_id)
    live_trading_enabled = bool(payload.get("live_trading_enabled", False))
    execution_mode = str(payload.get("execution_mode") or "auto").strip() or "auto"
    if account_role == "live" and live_trading_enabled and not bool(payload.get("live_confirmed")):
        raise ValueError("实盘自动交易必须显式确认 live_confirmed=true")

    pool_config = dict(payload.get("monitor_pool") or {})
    pool_config.setdefault("mode", "strategy_positions_watchlist")
    pool_config["resolved_symbols"] = _resolve_monitor_symbols(main_db, user_id, account_key, strategy, pool_config)

    monitor = RealtimeMonitorDB(
        id=uuid4().hex,
        user_id=user_id,
        name=str(payload.get("name") or f"实时监控-{strategy['name']}").strip(),
        account_key=account_key,
        account_role=account_role,
        strategy_id=strategy["id"],
        strategy_version_id=payload.get("strategy_version_id") or strategy.get("current_version_id"),
        status="ready",
        execution_mode=execution_mode,
        auto_trade_enabled=execution_mode == "auto",
        live_trading_enabled=live_trading_enabled,
        quote_source="qmt",
        monitor_pool_json=pool_config,
        config_json=_default_config(payload.get("config") or {}),
        risk_config_json=_default_risk_config(strategy, payload.get("risk_config") or {}),
        state_json={
            "compiled_status": compiled.status,
            "timeframes_required": compiled.timeframes_required,
            "minute_requirements": compiled.minute_requirements,
            "latest_cycle": None,
            "stats": {"signals": 0, "orders": 0, "rejections": 0, "approvals": 0},
        },
        created_at=_now_dt(),
        updated_at=_now_dt(),
    )
    strategy_db.add(monitor)
    strategy_db.commit()
    strategy_db.refresh(monitor)
    _append_event(strategy_db, monitor, "monitor_created", payload={"monitor": monitor.to_dict()})
    strategy_db.commit()
    return _monitor_payload(monitor)


def list_monitors(db: Session, user_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(RealtimeMonitorDB)
        .filter(RealtimeMonitorDB.user_id == user_id)
        .order_by(RealtimeMonitorDB.updated_at.desc(), RealtimeMonitorDB.created_at.desc())
        .all()
    )
    return [_monitor_payload(row) for row in rows]


def get_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    return _monitor_payload(_require_monitor(db, user_id, monitor_id))


def delete_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    payload = _monitor_payload(monitor)
    db.query(RealtimeApprovalDB).filter(
        RealtimeApprovalDB.monitor_id == monitor.id,
        RealtimeApprovalDB.user_id == user_id,
    ).delete(synchronize_session=False)
    db.query(RealtimeEventDB).filter(
        RealtimeEventDB.monitor_id == monitor.id,
        RealtimeEventDB.user_id == user_id,
    ).delete(synchronize_session=False)
    db.delete(monitor)
    db.commit()
    return {
        "message": "实时监控实例已删除",
        "monitor": payload,
    }


def start_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    strategy = _require_strategy(db, monitor.strategy_id)
    compiled = _compile_strategy_payload(strategy)
    if compiled.status != "passed":
        monitor.status = "error"
        monitor.fused_reason = "策略 DSL 编译未通过"
        monitor.updated_at = _now_dt()
        db.add(monitor)
        _append_event(db, monitor, "monitor_error", error_payload={"errors": compiled.errors})
        db.commit()
        raise ValueError("策略 DSL 编译未通过，不能启动实时监控：" + "；".join(compiled.errors))

    if monitor.account_role == "live" and monitor.auto_trade_enabled and not monitor.live_trading_enabled:
        _append_event(db, monitor, "live_readonly_guard", payload={"message": "实盘未进入白名单，启动为只读监控"})
        monitor.auto_trade_enabled = False
        monitor.execution_mode = "monitor_only"

    monitor.status = "running"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "monitor_started", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def pause_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    monitor.status = "paused"
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "monitor_paused", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def stop_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    monitor.status = "halted"
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "monitor_stopped", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def resume_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    if monitor.status not in {"paused", "halted", "ready"}:
        raise ValueError("只有 ready/paused/halted 状态可以恢复运行")
    monitor.status = "running"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "monitor_resumed", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def fuse_reset_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    if monitor.status != "fused":
        raise ValueError("当前实例未处于熔断状态")
    monitor.status = "paused"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "fuse_reset", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def run_monitor_once(strategy_db: Session, main_db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(strategy_db, user_id, monitor_id)
    if monitor.status not in {"ready", "paused", "running"}:
        raise ValueError("只有 ready/paused/running 状态可以立即执行一轮监控")
    if monitor.status == "fused":
        raise ValueError("当前实例已熔断，请先解除熔断后再执行")
    _append_event(strategy_db, monitor, "manual_cycle_requested", payload={"source": "manual_run_once"})
    strategy_db.commit()
    _run_monitor_cycle(monitor_id, force=True, trigger_source="manual")
    strategy_db.expire_all()
    refreshed = _require_monitor(strategy_db, user_id, monitor_id)
    return {
        "monitor": _monitor_payload(refreshed),
        "events": list_events(strategy_db, user_id, monitor_id, limit=30),
    }


def list_events(
    db: Session,
    user_id: str,
    monitor_id: str,
    *,
    limit: int = 200,
    after_id: str | None = None,
    since_started: bool = False,
) -> list[dict[str, Any]]:
    _require_monitor(db, user_id, monitor_id)
    max_limit = max(min(limit, 50000), 1)
    query = db.query(RealtimeEventDB).filter(
        RealtimeEventDB.monitor_id == monitor_id,
        RealtimeEventDB.user_id == user_id,
    )
    runtime_start_at = _latest_runtime_start_at(db, user_id, monitor_id) if since_started else None
    if runtime_start_at is not None:
        query = query.filter(RealtimeEventDB.created_at >= runtime_start_at)
    if after_id:
        cursor = db.query(RealtimeEventDB).filter(
            RealtimeEventDB.id == after_id,
            RealtimeEventDB.monitor_id == monitor_id,
            RealtimeEventDB.user_id == user_id,
        ).first()
        if cursor and cursor.created_at:
            rows = (
                query.filter(
                    or_(
                        RealtimeEventDB.created_at > cursor.created_at,
                        and_(RealtimeEventDB.created_at == cursor.created_at, RealtimeEventDB.id > cursor.id),
                    )
                )
                .order_by(RealtimeEventDB.created_at.asc(), RealtimeEventDB.id.asc())
                .limit(max_limit)
                .all()
            )
            return [row.to_dict() for row in rows]
    rows = query.order_by(RealtimeEventDB.created_at.desc(), RealtimeEventDB.id.desc()).limit(max_limit).all()
    return [row.to_dict() for row in reversed(rows)]


def _latest_runtime_start_at(db: Session, user_id: str, monitor_id: str) -> datetime | None:
    row = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.monitor_id == monitor_id,
            RealtimeEventDB.user_id == user_id,
            RealtimeEventDB.event_type.in_(["monitor_started", "monitor_resumed"]),
        )
        .order_by(RealtimeEventDB.created_at.desc())
        .first()
    )
    return row.created_at if row and row.created_at else None


def list_orders(db: Session, user_id: str, monitor_id: str) -> list[dict[str, Any]]:
    _require_monitor(db, user_id, monitor_id)
    order_event_types = [
        "order_intent",
        "order_submitted",
        "order_snapshot_refreshed",
        "order_status_changed",
        "order_cancel_requested",
        "order_cancelled",
        "order_cancel_error",
        "order_replace_requested",
        "order_rejected",
        "order_error",
    ]
    rows = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.user_id == user_id,
            RealtimeEventDB.monitor_id == monitor_id,
            RealtimeEventDB.event_type.in_(order_event_types),
        )
        .order_by(RealtimeEventDB.created_at.desc())
        .limit(200)
        .all()
    )
    return [row.to_dict() for row in rows]


def list_trades(db: Session, user_id: str, monitor_id: str) -> list[dict[str, Any]]:
    _require_monitor(db, user_id, monitor_id)
    rows = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.user_id == user_id,
            RealtimeEventDB.monitor_id == monitor_id,
            RealtimeEventDB.event_type.in_(["trade_confirmed", "position_changed", "order_submitted"]),
        )
        .order_by(RealtimeEventDB.created_at.desc())
        .limit(200)
        .all()
    )
    return [row.to_dict() for row in rows]


def get_positions(db: Session, main_db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    overview = qmt_virtual_account_service.get_qmt_virtual_account_overview(
        main_db,
        user_id,
        account_key=monitor.account_key,
        prefer_cache=True,
        allow_cache_fallback=True,
    )
    payload = {
        "monitor_id": monitor.id,
        "account_key": monitor.account_key,
        "positions": overview.get("positions") or [],
        "account": overview.get("account"),
        "connection": overview.get("connection"),
        "fetched_at": overview.get("fetched_at"),
        "data_source": overview.get("data_source"),
        "is_stale": bool(overview.get("is_stale", False)),
    }
    payload["data_governance"] = build_realtime_positions_governance(payload)
    return payload


def list_approvals(db: Session, user_id: str, status: str | None = None) -> list[dict[str, Any]]:
    query = db.query(RealtimeApprovalDB).filter(RealtimeApprovalDB.user_id == user_id)
    if status:
        query = query.filter(RealtimeApprovalDB.status == status)
    rows = query.order_by(RealtimeApprovalDB.created_at.desc()).limit(200).all()
    return [row.to_dict() for row in rows]


def approve_task(db: Session, main_db: Session, user_id: str, approval_id: str, decision: dict[str, Any] | None = None) -> dict[str, Any]:
    approval = _require_approval(db, user_id, approval_id)
    monitor = _require_monitor(db, user_id, approval.monitor_id)
    if approval.status != "pending":
        raise ValueError("该确认任务已处理")
    intent = dict(approval.order_intent_json or {})
    result = _execute_order_intent(db, main_db, monitor, intent, reason="manual_approval")
    if intent.get("signal_key"):
        _update_signal_execution(
            db,
            monitor,
            {
                "signal_key": intent.get("signal_key"),
                "symbol": intent.get("symbol"),
                "side": intent.get("side"),
                "timeframe": intent.get("signal_timeframe"),
                "bar_end": intent.get("signal_bar_end"),
                "reason": intent.get("order_remark"),
                "source": "manual_approval",
            },
            "approval_executed" if result.get("success") else "approval_order_error",
            order_intent=intent,
            broker_result=result if result.get("success") else None,
            error_message=None if result.get("success") else str(result.get("error") or "approval_order_error"),
        )
    approval.status = "executed" if result.get("success") else "approved"
    approval.decision_json = dict(decision or {}) | {"broker_result": result}
    approval.decided_at = _now_dt()
    approval.updated_at = _now_dt()
    db.add(approval)
    db.commit()
    db.refresh(approval)
    _append_event(db, monitor, "approval_executed", symbol=approval.symbol, order_payload=intent, broker_result=result)
    db.commit()
    return approval.to_dict()


def reject_task(db: Session, user_id: str, approval_id: str, decision: dict[str, Any] | None = None) -> dict[str, Any]:
    approval = _require_approval(db, user_id, approval_id)
    monitor = _require_monitor(db, user_id, approval.monitor_id)
    if approval.status != "pending":
        raise ValueError("该确认任务已处理")
    approval.status = "rejected"
    approval.decision_json = dict(decision or {})
    approval.decided_at = _now_dt()
    approval.updated_at = _now_dt()
    db.add(approval)
    db.commit()
    db.refresh(approval)
    _append_event(db, monitor, "approval_rejected", symbol=approval.symbol, payload={"approval_id": approval.id})
    db.commit()
    return approval.to_dict()


async def start_background_worker() -> None:
    global _WORKER_TASK, _STOP_EVENT
    if _WORKER_TASK and not _WORKER_TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _WORKER_TASK = asyncio.create_task(_worker_loop(), name="realtime-monitor-worker")


async def stop_background_worker() -> None:
    global _WORKER_TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _WORKER_TASK is not None:
        try:
            await _WORKER_TASK
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[realtime-monitor] stop worker failed")
    _WORKER_TASK = None
    _STOP_EVENT = None


async def _worker_loop() -> None:
    _runtime_log("实时监控后台 worker 已启动")
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.to_thread(_scan_and_run_once)
        except Exception:
            logger.exception("[realtime-monitor] scan loop failed")
            _runtime_log("实时监控后台扫描异常", level="ERROR")
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    _runtime_log("实时监控后台 worker 已停止")


def _scan_and_run_once() -> None:
    with get_strategy_db_ctx() as db:
        rows = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.status == "running").all()
        due_ids = [row.id for row in rows if _monitor_due(row)]
    for monitor_id in due_ids:
        try:
            _run_monitor_cycle(monitor_id)
        except Exception as exc:
            logger.exception("[realtime-monitor] run cycle failed monitor=%s", monitor_id)
            with get_strategy_db_ctx() as db:
                monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).first()
                if monitor:
                    _fuse_monitor(db, monitor, f"实时监控循环异常：{exc}")


def _run_monitor_cycle(monitor_id: str, *, force: bool = False, trigger_source: str = "worker") -> None:
    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        monitor = strategy_db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).first()
        if monitor is None:
            return
        if force:
            if monitor.status not in {"ready", "paused", "running"}:
                return
        elif monitor.status != "running":
            return
        cycle_id = uuid4().hex
        now = _now_dt()
        strategy = _require_strategy(strategy_db, monitor.strategy_id)
        compiled = _compile_strategy_payload(strategy)
        if compiled.status != "passed":
            _fuse_monitor(strategy_db, monitor, "策略 DSL 编译失败")
            return

        monitor.last_heartbeat_at = now
        monitor.updated_at = now
        if not _is_monitor_trading_window(now) and not bool((monitor.config_json or {}).get("allow_outside_session")):
            if _mark_repetitive_status_event_allowed(
                monitor,
                "cycle_skipped",
                "outside_trading_session",
                now,
                force_emit=trigger_source == "manual",
            ):
                _append_event(
                    strategy_db,
                    monitor,
                    "cycle_skipped",
                    payload={
                        "cycle_id": cycle_id,
                        "reason": "outside_trading_session",
                        "trigger_source": trigger_source,
                        "session": "09:30-11:30,13:00-15:00",
                    },
                    correlation_id=cycle_id,
                )
            _update_state_stats(monitor, latest_cycle=cycle_id)
            strategy_db.add(monitor)
            strategy_db.commit()
            return

        overview = _get_realtime_qmt_overview(main_db, monitor.user_id, monitor.account_key)
        if not _is_realtime_qmt_overview_live(overview):
            _fuse_monitor(strategy_db, monitor, _realtime_qmt_overview_error(overview))
            return

        pool = dict(monitor.monitor_pool_json or {})
        symbols = _resolve_monitor_symbols(main_db, monitor.user_id, monitor.account_key, strategy, pool, overview=overview)
        pool["resolved_symbols"] = symbols
        monitor.monitor_pool_json = pool
        if not symbols:
            if _mark_repetitive_status_event_allowed(
                monitor,
                "cycle_skipped",
                "empty_universe",
                now,
                force_emit=trigger_source == "manual",
            ):
                _append_event(
                    strategy_db,
                    monitor,
                    "cycle_skipped",
                    payload={"reason": "empty_universe", "trigger_source": trigger_source},
                    correlation_id=cycle_id,
                )
            _update_state_stats(monitor, latest_cycle=cycle_id)
            strategy_db.add(monitor)
            strategy_db.commit()
            return
        _append_event(
            strategy_db,
            monitor,
            "cycle_started",
            payload={"cycle_id": cycle_id, "symbol_count": len(symbols), "trigger_source": trigger_source},
            correlation_id=cycle_id,
        )
        _refresh_execution_state(strategy_db, main_db, monitor, overview, correlation_id=cycle_id)
        quotes = qmt_virtual_account_service._fetch_live_quotes(
            symbols,
            account_key=monitor.account_key,
            timeout_seconds=2.5,
            db=main_db,
            user_id=monitor.user_id,
        )
        if not quotes:
            _fuse_monitor(strategy_db, monitor, "QMT/实时行情不可用，已立即熔断")
            return
        quote_sample = {symbol: quotes.get(symbol) for symbol in symbols[:10]}
        _append_event(strategy_db, monitor, "market_snapshot", payload={"cycle_id": cycle_id, "quotes": quote_sample}, correlation_id=cycle_id)

        local_now = now.astimezone().replace(tzinfo=None)
        minute_capture = capture_today_minute_bars(
            account_key=monitor.account_key,
            symbols=symbols,
            trade_date=local_now.date().isoformat(),
            db=main_db,
            user_id=monitor.user_id,
        )
        _append_event(
            strategy_db,
            monitor,
            "minute_capture",
            payload={
                "cycle_id": cycle_id,
                "success": bool(minute_capture.get("success")),
                "rows": int(minute_capture.get("rows") or 0),
                "trade_date": minute_capture.get("trade_date"),
                "source": minute_capture.get("source"),
                "message": minute_capture.get("message"),
                "captured_symbols": minute_capture.get("captured_symbols") or [],
                "missing_symbols": minute_capture.get("missing_symbols") or [],
                "symbol_rows": minute_capture.get("symbol_rows") or {},
                "symbol_latest_trade_times": minute_capture.get("symbol_latest_trade_times") or {},
                "symbol_errors": minute_capture.get("symbol_errors") or {},
                "partial": bool(minute_capture.get("partial")),
            },
            correlation_id=cycle_id,
        )

        minute_features = _build_minute_features(
            monitor,
            symbols,
            minute_capture=minute_capture,
            current_time=local_now,
            main_db=main_db,
            user_id=monitor.user_id,
        )
        _append_event(
            strategy_db,
            monitor,
            "minute_features",
            payload={
                "cycle_id": cycle_id,
                "source": minute_features.get("source"),
                "timeframe": minute_features.get("timeframe"),
                "items": minute_features.get("items", [])[:20],
                "qmt_required": bool(minute_features.get("qmt_required")),
                "missing_symbols": minute_features.get("missing_symbols") or [],
                "stale_symbols": minute_features.get("stale_symbols") or [],
                "incomplete_symbols": minute_features.get("incomplete_symbols") or [],
                "capture_missing_symbols": minute_features.get("capture_missing_symbols") or [],
                "capture_stale_symbols": minute_features.get("capture_stale_symbols") or [],
                "capture_latest_trade_times": minute_features.get("capture_latest_trade_times") or {},
                "latest_closed_bar_end": minute_features.get("latest_closed_bar_end"),
            },
            correlation_id=cycle_id,
        )

        bar_clock_key = _signal_bar_clock_key(monitor, minute_features)
        if not force and _should_skip_signal_evaluation_for_bar(monitor, bar_clock_key):
            if _mark_signal_clock_skip_seen(monitor, bar_clock_key):
                _append_event(
                    strategy_db,
                    monitor,
                    "signal_evaluation_skipped",
                    payload={
                        "cycle_id": cycle_id,
                        "trigger_source": trigger_source,
                        "reason": "same_bar_already_evaluated",
                        "bar_clock_key": bar_clock_key,
                        "timeframe": minute_features.get("timeframe"),
                        "latest_closed_bar_end": minute_features.get("latest_closed_bar_end"),
                    },
                    correlation_id=cycle_id,
                )
            _update_state_stats(monitor, latest_cycle=cycle_id)
            strategy_db.add(monitor)
            strategy_db.commit()
            return
        _mark_signal_bar_evaluated(monitor, bar_clock_key, minute_features)

        max_signals = max(int((monitor.config_json or {}).get("max_signals_per_cycle") or 3), 1)
        raw_signals = _generate_signals(monitor, strategy, overview, quotes, minute_features)
        signals, suppressed_signals = _filter_actionable_signals(strategy_db, monitor, raw_signals, limit=max_signals)
        if not signals:
            if suppressed_signals:
                if _mark_signal_suppression_seen(monitor, suppressed_signals):
                    _append_event(
                        strategy_db,
                        monitor,
                        "signal_deduplicated",
                        payload={
                            "cycle_id": cycle_id,
                            "trigger_source": trigger_source,
                            "suppressed_count": len(suppressed_signals),
                            "reason": "same_signal_already_processed_in_bar",
                            "signal_keys": [signal.get("signal_key") for signal in suppressed_signals[:10]],
                            "bar_ends": sorted({str(signal.get("bar_end") or "") for signal in suppressed_signals if signal.get("bar_end")}),
                        },
                        correlation_id=cycle_id,
                    )
            else:
                _append_event(
                    strategy_db,
                    monitor,
                    "no_signal",
                    payload={"cycle_id": cycle_id, "trigger_source": trigger_source},
                    correlation_id=cycle_id,
                )
            _update_state_stats(monitor, latest_cycle=cycle_id)
            strategy_db.add(monitor)
            strategy_db.commit()
            return

        for signal in signals:
            signal_key = str(signal.get("signal_key") or _signal_request_id(monitor, signal))
            signal["signal_key"] = signal_key
            _append_event(
                strategy_db,
                monitor,
                "signal_generated",
                symbol=signal["symbol"],
                signal_payload=signal,
                request_id=signal_key,
                correlation_id=cycle_id,
            )
            _update_signal_execution(strategy_db, monitor, signal, "generated")
            intent = _build_order_intent(monitor, overview, signal)
            risk = _risk_check(strategy_db, monitor, intent, signal)
            if not risk["passed"]:
                if _should_block_unsellable_signal(monitor, intent, signal, risk):
                    _append_event(
                        strategy_db,
                        monitor,
                        "signal_blocked",
                        symbol=signal["symbol"],
                        signal_payload=signal,
                        risk_payload={
                            **risk,
                            "reason": "available_position_below_lot_size",
                        },
                        order_payload=intent,
                        payload={
                            "message": "卖出信号命中，但当前可卖仓位不足一手，已跳过下单",
                            "available_position": intent.get("available_position"),
                            "current_position": intent.get("current_position"),
                            "lot_size": _monitor_lot_size(monitor),
                        },
                        correlation_id=cycle_id,
                        request_id=signal_key,
                    )
                    _update_signal_execution(
                        strategy_db,
                        monitor,
                        signal,
                        "blocked",
                        order_intent=intent,
                        error_message="available_position_below_lot_size",
                    )
                    continue
                _append_event(
                    strategy_db,
                    monitor,
                    "order_rejected",
                    symbol=signal["symbol"],
                    signal_payload=signal,
                    risk_payload=risk,
                    order_payload=intent,
                    request_id=signal_key,
                    correlation_id=cycle_id,
                )
                _update_signal_execution(strategy_db, monitor, signal, "risk_rejected", order_intent=intent, error_message=str(risk.get("reason") or "risk_rejected"))
                _bump_stat(monitor, "rejections")
                continue

            if _needs_manual_approval(strategy_db, monitor, intent, signal):
                approval = _create_approval(strategy_db, monitor, intent, "同票多策略冲突，暂停自动执行")
                _append_event(
                    strategy_db,
                    monitor,
                    "approval_created",
                    symbol=signal["symbol"],
                    signal_payload=signal,
                    risk_payload=risk,
                    order_payload=intent,
                    payload={"approval_id": approval.id, "reason": approval.reason},
                    request_id=signal_key,
                    correlation_id=cycle_id,
                )
                _update_signal_execution(strategy_db, monitor, signal, "approval_pending", order_intent=intent)
                _bump_stat(monitor, "approvals")
                continue

            _append_event(strategy_db, monitor, "order_intent", symbol=signal["symbol"], signal_payload=signal, risk_payload=risk, order_payload=intent, request_id=signal_key, correlation_id=cycle_id)
            _update_signal_execution(strategy_db, monitor, signal, "intent_created", order_intent=intent)
            broker_result = _execute_order_intent(strategy_db, main_db, monitor, intent, reason="auto_monitor")
            _append_event(
                strategy_db,
                monitor,
                "order_submitted" if broker_result.get("success") else "order_error",
                symbol=signal["symbol"],
                signal_payload=signal,
                risk_payload=risk,
                order_payload=intent,
                broker_result=broker_result if broker_result.get("success") else {},
                error_payload={} if broker_result.get("success") else broker_result,
                request_id=signal_key,
                correlation_id=cycle_id,
            )
            _update_signal_execution(
                strategy_db,
                monitor,
                signal,
                "submitted" if broker_result.get("success") else "order_error",
                order_intent=intent,
                broker_result=broker_result if broker_result.get("success") else None,
                error_message=None if broker_result.get("success") else str(broker_result.get("error") or "order_error"),
            )
            if broker_result.get("success"):
                _bump_stat(monitor, "orders")
                _register_pending_order(monitor, broker_result, intent)
                _append_broker_followup_events(strategy_db, monitor, signal["symbol"], broker_result, correlation_id=cycle_id)

        _bump_stat(monitor, "signals", len(signals))
        _update_state_stats(monitor, latest_cycle=cycle_id)
        strategy_db.add(monitor)
        strategy_db.commit()
        _capture_catalyst_feedback_after_cycle(strategy_db, main_db, monitor)


def _capture_catalyst_feedback_after_cycle(strategy_db: Session, main_db: Session, monitor: RealtimeMonitorDB) -> None:
    pool = monitor.monitor_pool_json if isinstance(monitor.monitor_pool_json, dict) else {}
    if str(pool.get("source") or "").strip() != "catalyst-selection":
        return
    try:
        from api.services import catalyst_selection_service

        catalyst_selection_service.capture_realtime_monitor_feedback(
            strategy_db,
            main_db,
            monitor_id=monitor.id,
            limit=100,
            refresh_profiles=True,
        )
    except Exception:
        logger.exception("[realtime-monitor] catalyst feedback capture failed monitor=%s", monitor.id)


def _monitor_due(monitor: RealtimeMonitorDB) -> bool:
    interval = int((monitor.config_json or {}).get("poll_interval_seconds") or 20)
    if monitor.last_heartbeat_at is None:
        return True
    return (_now_dt() - _ensure_utc(monitor.last_heartbeat_at)) >= timedelta(seconds=max(interval, 5))


def _signal_bar_clock_key(monitor: RealtimeMonitorDB, minute_features: dict[str, Any]) -> str | None:
    latest_closed_bar_end = str(minute_features.get("latest_closed_bar_end") or "").strip()
    if not latest_closed_bar_end:
        return None
    config = dict(monitor.config_json or {})
    signal_mode = str(config.get("signal_mode") or minute_features.get("signal_mode") or "intraday_confirmation").strip().lower()
    timeframe = str(config.get("signal_timeframe") or minute_features.get("timeframe") or "30m").strip().lower()
    return f"{signal_mode}:{timeframe}:{latest_closed_bar_end}"


def _should_skip_signal_evaluation_for_bar(monitor: RealtimeMonitorDB, bar_clock_key: str | None) -> bool:
    if not bar_clock_key or _has_intrabar_risk_rules(monitor):
        return False
    state = dict(monitor.state_json or {})
    clock = dict(state.get("signal_clock") or {})
    return clock.get("last_evaluated_bar_key") == bar_clock_key


def _mark_signal_bar_evaluated(
    monitor: RealtimeMonitorDB,
    bar_clock_key: str | None,
    minute_features: dict[str, Any],
) -> None:
    if not bar_clock_key:
        return
    state = dict(monitor.state_json or {})
    clock = dict(state.get("signal_clock") or {})
    clock["last_evaluated_bar_key"] = bar_clock_key
    clock["last_evaluated_at"] = _now_dt().isoformat()
    clock["timeframe"] = minute_features.get("timeframe")
    clock["latest_closed_bar_end"] = minute_features.get("latest_closed_bar_end")
    state["signal_clock"] = clock
    monitor.state_json = state


def _mark_signal_clock_skip_seen(monitor: RealtimeMonitorDB, bar_clock_key: str | None) -> bool:
    if not bar_clock_key:
        return False
    state = dict(monitor.state_json or {})
    clock = dict(state.get("signal_clock") or {})
    if clock.get("last_skip_bar_key") == bar_clock_key:
        return False
    clock["last_skip_bar_key"] = bar_clock_key
    clock["last_skip_at"] = _now_dt().isoformat()
    state["signal_clock"] = clock
    monitor.state_json = state
    return True


def _has_intrabar_risk_rules(monitor: RealtimeMonitorDB) -> bool:
    risk = dict(monitor.risk_config_json or {})
    return float(risk.get("stop_loss_pct") or 0.0) > 0


def _compile_strategy_payload(strategy: dict[str, Any]):
    version = strategy.get("current_version") or {}
    dsl = version.get("dsl") or {}
    return compile_strategy_dsl(dsl)


def _get_realtime_qmt_overview(main_db: Session, user_id: str, account_key: str) -> dict[str, Any]:
    return qmt_virtual_account_service.get_qmt_virtual_account_overview(
        main_db,
        user_id,
        account_key=account_key,
        allow_cache_fallback=False,
    )


def _is_realtime_qmt_overview_live(overview: dict[str, Any]) -> bool:
    connection = overview.get("connection") if isinstance(overview.get("connection"), dict) else {}
    return (
        str(overview.get("data_source") or "").strip().lower() == "live"
        and bool(connection.get("connected")) is True
        and bool(overview.get("is_stale", False)) is False
    )


def _realtime_qmt_overview_error(overview: dict[str, Any]) -> str:
    connection = overview.get("connection") if isinstance(overview.get("connection"), dict) else {}
    message = str(connection.get("message") or overview.get("message") or "").strip()
    if message:
        return f"QMT 账户实时快照不可用，实时监控禁止使用缓存：{message}"
    source = str(overview.get("data_source") or "unknown")
    return f"QMT 账户实时快照不可用，实时监控禁止使用缓存（source={source}）"


def _resolve_monitor_symbols(
    main_db: Session,
    user_id: str,
    account_key: str,
    strategy: dict[str, Any],
    pool: dict[str, Any],
    *,
    overview: dict[str, Any] | None = None,
) -> list[str]:
    mode = str(pool.get("mode") or "strategy_positions_watchlist").strip().lower()
    symbols: set[str] = set()
    if mode not in {"qmt_positions_only", "positions_only"}:
        for raw in pool.get("symbols") or pool.get("manual_symbols") or []:
            normalized = _normalize_symbol(raw)
            if normalized:
                symbols.add(normalized)
        if mode not in {"manual_only"}:
            dsl = (strategy.get("current_version") or {}).get("dsl") or {}
            universe = dsl.get("universe") or {}
            for raw in universe.get("symbols") or []:
                normalized = _normalize_symbol(raw)
                if normalized:
                    symbols.add(normalized)
    try:
        active_overview = overview or _get_realtime_qmt_overview(main_db, user_id, account_key)
        if _is_realtime_qmt_overview_live(active_overview):
            for position in active_overview.get("positions") or []:
                normalized = _normalize_symbol(position.get("symbol"))
                if normalized:
                    symbols.add(normalized)
        else:
            logger.warning("[realtime-monitor] skip qmt positions because strict live overview is unavailable: %s", _realtime_qmt_overview_error(active_overview))
    except Exception as exc:
        logger.warning("[realtime-monitor] resolve qmt positions failed: %s", exc)
    if mode not in {"qmt_positions_only", "positions_only", "manual_only"}:
        try:
            for item in watchlist_service.list_watchlist(main_db, user_id):
                normalized = _normalize_symbol(item.get("symbol"))
                if normalized:
                    symbols.add(normalized)
        except Exception as exc:
            logger.warning("[realtime-monitor] resolve watchlist failed: %s", exc)
    return sorted(symbols)


def _build_minute_features(
    monitor: RealtimeMonitorDB,
    symbols: list[str],
    *,
    minute_capture: dict[str, Any] | None = None,
    current_time: datetime | None = None,
    main_db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    raw_now = current_time or datetime.now().astimezone()
    now_local = raw_now.astimezone().replace(tzinfo=None) if raw_now.tzinfo else raw_now.replace(tzinfo=None)
    trade_date = now_local.date().isoformat()
    config = dict(monitor.config_json or {})
    signal_mode = str(config.get("signal_mode") or "intraday_confirmation").strip().lower()
    timeframe = str(config.get("signal_timeframe") or "30m").strip().lower() or "30m"
    all_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)})
    qmt_symbols, capture_missing_symbols, capture_stale_symbols, capture_latest_trade_times = _qmt_fresh_captured_symbols(
        all_symbols,
        minute_capture,
        trade_date=trade_date,
        timeframe=timeframe,
        current_time=now_local,
    )
    signal_symbols = sorted(qmt_symbols)
    if minute_capture is not None and not signal_symbols:
        latest_closed_bar_end = _latest_closed_bar_end(now_local, timeframe)
        return {
            "timeframe": timeframe,
            "source": str(minute_capture.get("source") or "qmt_intraday"),
            "items": [],
            "missing_symbols": all_symbols,
            "stale_symbols": capture_stale_symbols,
            "incomplete_symbols": [],
            "capture_missing_symbols": capture_missing_symbols or all_symbols,
            "capture_stale_symbols": capture_stale_symbols,
            "capture_latest_trade_times": capture_latest_trade_times,
            "latest_closed_bar_end": latest_closed_bar_end.isoformat(),
            "qmt_required": True,
            "signal_mode": signal_mode,
            "error": "本轮 QMT 未返回足够新的分钟线，已跳过信号计算",
        }
    try:
        if signal_mode == "first_day_band":
            result = evaluate_first_day_band_signals(symbols=signal_symbols, trade_date=trade_date, timeframe=timeframe)
            items, missing_symbols, stale_symbols, incomplete_symbols = _fresh_minute_items_for_trade_date(
                result.items,
                required_symbols=signal_symbols,
                trade_date=trade_date,
                timeframe=timeframe,
                current_time=now_local,
                require_latest_bar=True,
            )
            if missing_symbols:
                supplemented = _supplement_first_day_band_result(
                    account_key=monitor.account_key,
                    symbols=missing_symbols,
                    trade_date=trade_date,
                    timeframe=timeframe,
                    force_refresh=True,
                    db=main_db,
                    user_id=user_id or getattr(monitor, "user_id", None),
                )
                if supplemented is not None:
                    supplement_items, supplement_missing, supplement_stale, supplement_incomplete = _fresh_minute_items_for_trade_date(
                        supplemented.items,
                        required_symbols=missing_symbols,
                        trade_date=trade_date,
                        timeframe=timeframe,
                        current_time=now_local,
                        require_latest_bar=True,
                    )
                    by_symbol = {str(item.get("symbol")): item for item in items}
                    by_symbol.update({str(item.get("symbol")): item for item in supplement_items})
                    items = list(by_symbol.values())
                    missing_symbols = sorted(set(supplement_missing) | (set(missing_symbols) - set(by_symbol)))
                    stale_symbols = sorted((set(stale_symbols) | set(supplement_stale)) - set(by_symbol))
                    incomplete_symbols = sorted((set(incomplete_symbols) | set(supplement_incomplete)) - set(by_symbol))
                    source = f"{result.source}+{supplemented.source}"
                else:
                    source = result.source
            else:
                source = result.source
            seen = {str(item.get("symbol")) for item in items if item.get("symbol")}
            latest_closed_bar_end = _latest_closed_bar_end(now_local, timeframe)
            return {
                "timeframe": result.timeframe,
                "source": _realtime_minute_source(source, minute_capture),
                "items": items,
                "missing_symbols": sorted(set(all_symbols) - seen),
                "stale_symbols": sorted(set(stale_symbols) | set(capture_stale_symbols)),
                "incomplete_symbols": incomplete_symbols,
                "capture_missing_symbols": capture_missing_symbols,
                "capture_stale_symbols": capture_stale_symbols,
                "capture_latest_trade_times": capture_latest_trade_times,
                "latest_closed_bar_end": latest_closed_bar_end.isoformat(),
                "qmt_required": True,
                "signal_mode": signal_mode,
            }
        result = evaluate_intraday_confirmation(
            symbols=signal_symbols,
            trade_date=trade_date,
            timeframe=timeframe,
            allow_cache=False,
            allow_synthetic=False,
        )
        items, missing_symbols, stale_symbols, incomplete_symbols = _fresh_minute_items_for_trade_date(
            result.items,
            required_symbols=signal_symbols,
            trade_date=trade_date,
            timeframe=timeframe,
            current_time=now_local,
            require_latest_bar=True,
        )
        seen = {str(item.get("symbol")) for item in items if item.get("symbol")}
        latest_closed_bar_end = _latest_closed_bar_end(now_local, timeframe)
        return {
            "timeframe": result.timeframe,
            "source": _realtime_minute_source(result.source, minute_capture),
            "items": items,
            "missing_symbols": sorted(set(all_symbols) - seen),
            "stale_symbols": sorted(set(stale_symbols) | set(capture_stale_symbols)),
            "incomplete_symbols": incomplete_symbols,
            "capture_missing_symbols": capture_missing_symbols,
            "capture_stale_symbols": capture_stale_symbols,
            "capture_latest_trade_times": capture_latest_trade_times,
            "latest_closed_bar_end": latest_closed_bar_end.isoformat(),
            "qmt_required": True,
            "signal_mode": signal_mode,
        }
    except Exception as exc:
        return {
            "timeframe": timeframe,
            "source": "unavailable",
            "items": [],
            "missing_symbols": all_symbols,
            "stale_symbols": [],
            "incomplete_symbols": [],
            "capture_missing_symbols": capture_missing_symbols,
            "capture_stale_symbols": capture_stale_symbols,
            "capture_latest_trade_times": capture_latest_trade_times,
            "latest_closed_bar_end": _latest_closed_bar_end(now_local, timeframe).isoformat(),
            "error": str(exc),
            "signal_mode": signal_mode,
            "qmt_required": True,
        }


def _qmt_captured_symbols(
    symbols: list[str],
    minute_capture: dict[str, Any] | None,
    *,
    trade_date: str,
) -> tuple[set[str], list[str]]:
    normalized = {_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)}
    if minute_capture is None:
        return set(normalized), []
    captured_trade_date = str(minute_capture.get("trade_date") or "").strip()
    if captured_trade_date and captured_trade_date != trade_date:
        return set(), sorted(normalized)
    symbol_rows = minute_capture.get("symbol_rows") if isinstance(minute_capture.get("symbol_rows"), dict) else {}
    captured_raw = minute_capture.get("captured_symbols") if isinstance(minute_capture.get("captured_symbols"), list) else []
    missing_raw = minute_capture.get("missing_symbols") if isinstance(minute_capture.get("missing_symbols"), list) else []
    captured = {_normalize_symbol(symbol) for symbol in captured_raw if _normalize_symbol(symbol)}
    if not captured and symbol_rows:
        captured = {
            _normalize_symbol(symbol)
            for symbol, rows in symbol_rows.items()
            if _normalize_symbol(symbol) and int(rows or 0) > 0
        }
    if not captured and not symbol_rows and not missing_raw and bool(minute_capture.get("success")) and int(minute_capture.get("rows") or 0) > 0:
        captured = set(normalized)
    captured = {symbol for symbol in captured if symbol in normalized}
    missing = {
        _normalize_symbol(symbol)
        for symbol in missing_raw
        if _normalize_symbol(symbol) in normalized
    }
    missing |= normalized - captured
    return captured, sorted(missing)


def _qmt_fresh_captured_symbols(
    symbols: list[str],
    minute_capture: dict[str, Any] | None,
    *,
    trade_date: str,
    timeframe: str,
    current_time: datetime,
) -> tuple[set[str], list[str], list[str], dict[str, str]]:
    captured, missing = _qmt_captured_symbols(symbols, minute_capture, trade_date=trade_date)
    if minute_capture is None or not captured:
        return captured, missing, [], {}

    latest_closed_bar_end = _latest_closed_bar_end(current_time, timeframe)
    latest_raw = minute_capture.get("symbol_latest_trade_times")
    latest_by_symbol = latest_raw if isinstance(latest_raw, dict) else {}
    fresh: set[str] = set()
    stale: set[str] = set()
    latest_trade_times: dict[str, str] = {}
    for symbol in captured:
        latest_text = str(latest_by_symbol.get(symbol) or "").strip()
        if latest_text:
            latest_trade_times[symbol] = latest_text
        latest_dt = _parse_local_datetime(latest_text)
        if latest_dt is None or latest_dt.date().isoformat() != trade_date:
            stale.add(symbol)
            continue
        if latest_dt < latest_closed_bar_end:
            stale.add(symbol)
            continue
        fresh.add(symbol)
    missing_symbols = sorted(set(missing) | (captured - fresh))
    return fresh, missing_symbols, sorted(stale), latest_trade_times


def _realtime_minute_source(source: str, minute_capture: dict[str, Any] | None) -> str:
    source_text = str(source or "empty")
    if minute_capture is None:
        return source_text
    capture_source = str((minute_capture or {}).get("source") or "qmt_intraday").strip()
    if not capture_source or capture_source in source_text:
        return source_text
    return f"{capture_source}+{source_text}"


def _fresh_minute_items_for_trade_date(
    items: list[dict[str, Any]],
    *,
    required_symbols: list[str],
    trade_date: str,
    timeframe: str,
    current_time: datetime | None = None,
    require_latest_bar: bool = False,
) -> tuple[list[dict[str, Any]], list[str], list[str], list[str]]:
    required = {_normalize_symbol(symbol) for symbol in required_symbols if _normalize_symbol(symbol)}
    latest_closed_bar_end = _latest_closed_bar_end(current_time or datetime.now(), timeframe)
    fresh_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    stale_symbols: set[str] = set()
    incomplete_symbols: set[str] = set()
    for raw_item in items or []:
        if not isinstance(raw_item, dict):
            continue
        symbol = _normalize_symbol(raw_item.get("symbol"))
        if not symbol or symbol not in required:
            continue
        bar_end = str(raw_item.get("bar_end") or "")
        if bar_end[:10] != trade_date:
            stale_symbols.add(symbol)
            continue
        bar_end_dt = _parse_local_datetime(bar_end)
        if bar_end_dt is None:
            stale_symbols.add(symbol)
            continue
        if bar_end_dt > latest_closed_bar_end:
            incomplete_symbols.add(symbol)
            continue
        if require_latest_bar and bar_end_dt < latest_closed_bar_end:
            stale_symbols.add(symbol)
            continue
        item = dict(raw_item)
        item["symbol"] = symbol
        fresh_items.append(item)
        seen.add(symbol)
    missing_symbols = sorted(required - seen)
    return fresh_items, missing_symbols, sorted(stale_symbols - seen), sorted(incomplete_symbols - seen)


def _latest_closed_bar_end(current_time: datetime, timeframe: str) -> datetime:
    current = current_time.replace(tzinfo=None)
    rule = _timeframe_to_pandas_rule(timeframe)
    return pd.Timestamp(current).floor(rule).to_pydatetime()


def _timeframe_to_pandas_rule(timeframe: str) -> str:
    mapping = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min"}
    return mapping.get(str(timeframe or "").strip().lower(), "30min")


def _parse_local_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _minute_result_covers_trade_date(items: list[dict[str, Any]], trade_date: str) -> bool:
    for item in items or []:
        bar_end = str(item.get("bar_end") or "")
        if bar_end[:10] == trade_date:
            return True
    return False


def _filter_actionable_signals(
    db: Session,
    monitor: RealtimeMonitorDB,
    signals: list[dict[str, Any]],
    *,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not signals:
        return [], []
    normalized_signals: list[dict[str, Any]] = []
    request_ids: list[str] = []
    for raw_signal in signals:
        signal = dict(raw_signal)
        signal_key = _signal_request_id(monitor, signal)
        signal["signal_key"] = signal_key
        signal["signal_identity"] = _signal_identity_payload(monitor, signal)
        normalized_signals.append(signal)
        request_ids.append(signal_key)

    event_rows = (
        db.query(RealtimeEventDB.request_id)
        .filter(
            RealtimeEventDB.monitor_id == monitor.id,
            RealtimeEventDB.request_id.in_(request_ids),
            RealtimeEventDB.event_type.in_(_SIGNAL_PROCESSED_EVENT_TYPES),
        )
        .all()
    )
    ledger_rows = (
        db.query(RealtimeSignalExecutionDB.signal_key)
        .filter(
            RealtimeSignalExecutionDB.monitor_id == monitor.id,
            RealtimeSignalExecutionDB.signal_key.in_(request_ids),
        )
        .all()
    )
    processed = {str(row[0]) for row in event_rows if row and row[0]}
    processed |= {str(row[0]) for row in ledger_rows if row and row[0]}
    actionable: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    seen_in_batch: set[str] = set()
    max_actionable = max(int(limit), 1) if limit is not None else None
    for signal in normalized_signals:
        signal_key = str(signal.get("signal_key") or "")
        if signal_key in processed or signal_key in seen_in_batch:
            suppressed.append(signal)
            continue
        seen_in_batch.add(signal_key)
        if max_actionable is not None and len(actionable) >= max_actionable:
            continue
        if not _reserve_signal_execution(db, monitor, signal):
            suppressed.append(signal)
            continue
        actionable.append(signal)
    return actionable, suppressed


def _reserve_signal_execution(db: Session, monitor: RealtimeMonitorDB, signal: dict[str, Any]) -> bool:
    signal_key = str(signal.get("signal_key") or "")
    if not signal_key:
        return False
    identity = dict(signal.get("signal_identity") or _signal_identity_payload(monitor, signal))
    row = RealtimeSignalExecutionDB(
        id=uuid4().hex,
        monitor_id=monitor.id,
        user_id=monitor.user_id,
        account_key=monitor.account_key,
        strategy_id=monitor.strategy_id,
        strategy_version_id=monitor.strategy_version_id,
        symbol=str(identity.get("symbol") or _normalize_symbol(signal.get("symbol"))),
        side=str(identity.get("side") or signal.get("side") or ""),
        timeframe=str(identity.get("timeframe") or signal.get("timeframe") or ""),
        bar_end=str(identity.get("bar_end") or signal.get("bar_end") or ""),
        signal_key=signal_key,
        status="reserved",
        signal_identity_json=_json_safe(identity),
        signal_payload_json=_json_safe(signal),
        first_seen_at=_now_dt(),
        updated_at=_now_dt(),
    )
    try:
        with db.begin_nested():
            db.add(row)
            db.flush()
        return True
    except IntegrityError:
        return False


def _update_signal_execution(
    db: Session,
    monitor: RealtimeMonitorDB,
    signal: dict[str, Any],
    status: str,
    *,
    order_intent: dict[str, Any] | None = None,
    broker_result: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    signal_key = str(signal.get("signal_key") or "")
    if not signal_key:
        return
    row = (
        db.query(RealtimeSignalExecutionDB)
        .filter(
            RealtimeSignalExecutionDB.monitor_id == monitor.id,
            RealtimeSignalExecutionDB.signal_key == signal_key,
        )
        .first()
    )
    if row is None:
        identity = _signal_identity_payload(monitor, signal)
        row = RealtimeSignalExecutionDB(
            id=uuid4().hex,
            monitor_id=monitor.id,
            user_id=monitor.user_id,
            account_key=monitor.account_key,
            strategy_id=monitor.strategy_id,
            strategy_version_id=monitor.strategy_version_id,
            symbol=str(identity.get("symbol") or _normalize_symbol(signal.get("symbol"))),
            side=str(identity.get("side") or signal.get("side") or ""),
            timeframe=str(identity.get("timeframe") or signal.get("timeframe") or ""),
            bar_end=str(identity.get("bar_end") or signal.get("bar_end") or ""),
            signal_key=signal_key,
            first_seen_at=_now_dt(),
        )
    row.status = status
    row.signal_payload_json = _json_safe(signal)
    if not row.signal_identity_json:
        row.signal_identity_json = _json_safe(_signal_identity_payload(monitor, signal))
    if order_intent is not None:
        row.order_intent_json = _json_safe(order_intent)
    if broker_result is not None:
        row.broker_result_json = _json_safe(broker_result)
    row.error_message = error_message
    row.updated_at = _now_dt()
    db.add(row)


def _signal_request_id(monitor: RealtimeMonitorDB, signal: dict[str, Any]) -> str:
    identity = _signal_identity_payload(monitor, signal)
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sig_" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _signal_identity_payload(monitor: RealtimeMonitorDB, signal: dict[str, Any]) -> dict[str, Any]:
    timeframe = str(signal.get("timeframe") or signal.get("signal_timeframe") or "tick").strip().lower() or "tick"
    bar_end = str(signal.get("bar_end") or signal.get("bar_time") or "").strip()
    if not bar_end:
        if str(signal.get("source") or "").strip().lower() == "risk_stop_loss":
            bar_end = _now_dt().astimezone().date().isoformat()
        else:
            bar_end = "unknown_bar"
    return {
        "monitor_id": monitor.id,
        "strategy_id": monitor.strategy_id,
        "strategy_version_id": monitor.strategy_version_id or "",
        "account_key": monitor.account_key,
        "symbol": _normalize_symbol(signal.get("symbol")),
        "side": str(signal.get("side") or "").strip().lower(),
        "source": str(signal.get("source") or "").strip().lower(),
        "reason": str(signal.get("reason") or "").strip().lower(),
        "timeframe": timeframe,
        "bar_end": bar_end,
    }


def _mark_signal_suppression_seen(monitor: RealtimeMonitorDB, suppressed_signals: list[dict[str, Any]]) -> bool:
    keys = sorted({str(signal.get("signal_key") or "") for signal in suppressed_signals if signal.get("signal_key")})
    if not keys:
        return False
    state = dict(monitor.state_json or {})
    fence = dict(state.get("signal_fence") or {})
    suppression_key = "|".join(keys)
    if fence.get("last_suppression_key") == suppression_key:
        return False
    fence["last_suppression_key"] = suppression_key
    fence["last_suppression_at"] = _now_dt().isoformat()
    fence["last_suppressed_count"] = len(suppressed_signals)
    state["signal_fence"] = fence
    monitor.state_json = state
    return True


def _supplement_first_day_band_result(
    *,
    account_key: str,
    symbols: list[str],
    trade_date: str,
    timeframe: str,
    force_refresh: bool = False,
    db: Session | None = None,
    user_id: str | None = None,
):
    live_records: list[dict[str, Any]] = []
    for symbol in symbols:
        payload = fetch_intraday_bars(
            symbol,
            trade_date=trade_date,
            period="1m",
            include_latest_quote=False,
            account_key=account_key,
            db=db,
            user_id=user_id,
            persist=True,
            force_refresh=force_refresh,
            allow_cache=not force_refresh,
        )
        items = payload.get("items") or []
        if isinstance(items, list):
            live_records.extend([dict(item) for item in items if isinstance(item, dict)])
    if not live_records:
        return None
    supplement_frame = pd.DataFrame(live_records)
    if supplement_frame.empty:
        return None
    return evaluate_first_day_band_signals(
        symbols=symbols,
        trade_date=trade_date,
        timeframe=timeframe,
        supplement_frame=supplement_frame,
        supplement_source="qmt_bridge_live",
    )


def _generate_signals(
    monitor: RealtimeMonitorDB,
    strategy: dict[str, Any],
    overview: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    minute_features: dict[str, Any],
) -> list[dict[str, Any]]:
    config = monitor.config_json or {}
    signal_mode = str(config.get("signal_mode") or "intraday_confirmation").strip().lower()
    signal_timeframe = str(config.get("signal_timeframe") or minute_features.get("timeframe") or "30m").strip().lower()
    positions = {item.get("symbol"): item for item in (overview.get("positions") or []) if item.get("symbol")}
    signals: list[dict[str, Any]] = []
    risk = dict(monitor.risk_config_json or {})
    stop_loss_pct = float(risk.get("stop_loss_pct") or 0.0)
    current_bar_end = str(minute_features.get("latest_closed_bar_end") or "")
    for symbol, position in positions.items():
        quote = quotes.get(symbol) or {}
        price = _to_float(quote.get("price"), position.get("current_price"))
        avg_cost = _to_float(position.get("average_cost"))
        if stop_loss_pct > 0 and price and avg_cost and price <= avg_cost * (1 - stop_loss_pct):
            signals.append(
                {
                    "symbol": symbol,
                    "side": "sell",
                    "price": price,
                    "reason": f"stop_loss_{stop_loss_pct:.2%}",
                    "target_position_pct": 0.0,
                    "strategy_id": strategy["id"],
                    "source": "risk_stop_loss",
                    "timeframe": "risk",
                    "bar_end": _now_dt().astimezone().date().isoformat(),
                }
            )
    if signal_mode == "first_day_band":
        for item in minute_features.get("items") or []:
            symbol = item.get("symbol")
            action = str(item.get("signal") or "hold").lower()
            if not symbol or action not in {"buy", "sell"}:
                continue
            quote = quotes.get(symbol) or {}
            price = _to_float(quote.get("price"), item.get("close"), quote.get("close"))
            if not price:
                continue
            if action == "buy":
                signals.append(
                    {
                        "symbol": symbol,
                        "side": "buy",
                        "price": price,
                        "reason": f"first_day_band_{signal_timeframe}_golden_cross",
                        "target_position_pct": _target_position_pct(strategy),
                        "strategy_id": strategy["id"],
                        "source": "first_day_band_realtime",
                        "timeframe": signal_timeframe,
                        "bar_end": item.get("bar_end") or current_bar_end,
                        "bar_start": item.get("bar_start"),
                    }
                )
            if action == "sell":
                signals.append(
                    {
                        "symbol": symbol,
                        "side": "sell",
                        "price": price,
                        "reason": f"first_day_band_{signal_timeframe}_death_cross",
                        "target_position_pct": 0.0,
                        "strategy_id": strategy["id"],
                        "source": "first_day_band_realtime",
                        "timeframe": signal_timeframe,
                        "bar_end": item.get("bar_end") or current_bar_end,
                        "bar_start": item.get("bar_start"),
                    }
                )
    else:
        confirmed_items = [
            item
            for item in minute_features.get("items") or []
            if isinstance(item, dict) and item.get("confirmed") is True and item.get("symbol")
        ]
        for item in confirmed_items:
            symbol = item.get("symbol")
            quote = quotes.get(symbol) or {}
            price = _to_float(quote.get("price"), quote.get("close"))
            if not price:
                continue
            signals.append(
                {
                    "symbol": symbol,
                    "side": "buy",
                    "price": price,
                    "reason": "multi_timeframe_confirmed",
                    "target_position_pct": _target_position_pct(strategy),
                    "strategy_id": strategy["id"],
                    "source": "dsl_realtime_ir",
                    "timeframe": signal_timeframe,
                    "bar_end": item.get("bar_end") or current_bar_end,
                    "bar_start": item.get("bar_start"),
                }
            )
    return signals


def _build_order_intent(monitor: RealtimeMonitorDB, overview: dict[str, Any], signal: dict[str, Any]) -> dict[str, Any]:
    account = overview.get("account") or {}
    positions = {item.get("symbol"): item for item in (overview.get("positions") or []) if item.get("symbol")}
    side = signal["side"]
    symbol = signal["symbol"]
    price = float(signal.get("price") or 0)
    lot_size = _monitor_lot_size(monitor)
    reentry_anchor_quantity = None
    position = positions.get(symbol) or {}
    available_position = float(position.get("available_position") or 0.0)
    current_position = float(position.get("current_position") or 0.0)
    if side == "sell":
        quantity = int(available_position // lot_size) * lot_size
    else:
        total_asset = float(account.get("total_asset") or account.get("available_cash") or 0.0)
        available_cash = float(account.get("available_cash") or account.get("cash") or total_asset)
        reentry_anchor_quantity = _resolve_reentry_buy_quantity(
            monitor,
            overview,
            symbol=symbol,
            price=price,
            lot_size=lot_size,
        )
        if reentry_anchor_quantity is not None:
            quantity = reentry_anchor_quantity
        else:
            target_pct = float(signal.get("target_position_pct") or 0.02)
            target_cash = min(total_asset * target_pct, available_cash)
            quantity = int((target_cash / max(price, 0.01)) // lot_size) * lot_size
    intent = {
        "account_key": monitor.account_key,
        "symbol": symbol,
        "side": side,
        "quantity": max(quantity, 0),
        "price": None,
        "reference_price": price,
        "price_type": (monitor.config_json or {}).get("price_type") or "opponent",
        "strategy_name": f"RealtimeMonitor-{monitor.id[:8]}",
        "order_remark": signal.get("reason") or "realtime_monitor",
        "target_position_pct": signal.get("target_position_pct"),
        "available_position": round(available_position, 2),
        "current_position": round(current_position, 2),
        "signal_key": signal.get("signal_key"),
        "signal_timeframe": signal.get("timeframe"),
        "signal_bar_end": signal.get("bar_end"),
    }
    if reentry_anchor_quantity is not None:
        intent["reentry_anchor_quantity"] = reentry_anchor_quantity
    return intent


def _risk_check(db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any], signal: dict[str, Any]) -> dict[str, Any]:
    if monitor.account_role == "live" and not monitor.live_trading_enabled:
        return {"passed": False, "reason": "live_readonly_not_whitelisted"}
    if monitor.status == "fused":
        return {"passed": False, "reason": "monitor_fused"}
    if not _is_monitor_trading_window(_now_dt()) and not bool((monitor.config_json or {}).get("allow_outside_session")):
        return {"passed": False, "reason": "outside_continuous_auction_session"}
    if int(intent.get("quantity") or 0) < _monitor_lot_size(monitor):
        return {"passed": False, "reason": "quantity_below_lot_size"}
    return {"passed": True, "reason": "passed", "signal_source": signal.get("source")}


def _needs_manual_approval(db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any], signal: dict[str, Any]) -> bool:
    recent_cutoff = _now_dt() - timedelta(minutes=5)
    rows = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.account_key == monitor.account_key,
            RealtimeEventDB.symbol == intent["symbol"],
            RealtimeEventDB.event_type == "signal_generated",
            RealtimeEventDB.created_at >= recent_cutoff,
            RealtimeEventDB.monitor_id != monitor.id,
        )
        .all()
    )
    for row in rows:
        other_side = (row.signal_payload or {}).get("side")
        if other_side and other_side != intent["side"]:
            return True
    return bool(
        db.query(RealtimeApprovalDB)
        .filter(
            RealtimeApprovalDB.account_key == monitor.account_key,
            RealtimeApprovalDB.symbol == intent["symbol"],
            RealtimeApprovalDB.status == "pending",
        )
        .first()
    )


def _create_approval(db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any], reason: str) -> RealtimeApprovalDB:
    approval = RealtimeApprovalDB(
        id=uuid4().hex,
        monitor_id=monitor.id,
        user_id=monitor.user_id,
        account_key=monitor.account_key,
        strategy_id=monitor.strategy_id,
        symbol=intent.get("symbol"),
        side=intent.get("side"),
        status="pending",
        reason=reason,
        order_intent_json=dict(intent),
        created_at=_now_dt(),
        updated_at=_now_dt(),
    )
    db.add(approval)
    return approval


def _execute_order_intent(db: Session, main_db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any], *, reason: str) -> dict[str, Any]:
    if monitor.account_role == "live" and not monitor.live_trading_enabled:
        return {"success": False, "error": "live_readonly_not_whitelisted"}
    if not monitor.auto_trade_enabled and reason != "manual_approval":
        return {"success": False, "error": "auto_trade_disabled"}
    try:
        result = qmt_virtual_account_service.submit_qmt_order(
            main_db,
            monitor.user_id,
            account_key=monitor.account_key,
            symbol=intent["symbol"],
            side=intent["side"],
            quantity=int(intent["quantity"]),
            price=intent.get("price"),
            price_type=str(intent.get("price_type") or "opponent"),
            strategy_name=str(intent.get("strategy_name") or "RealtimeMonitor"),
            order_remark=str(intent.get("order_remark") or reason),
            overview_allow_cache_fallback=False,
        )
        return _normalize_broker_result(result)
    except Exception as exc:
        _fuse_monitor(db, monitor, f"QMT 交易接口异常：{exc}")
        return {"success": False, "error": str(exc)}


def _fuse_monitor(db: Session, monitor: RealtimeMonitorDB, reason: str) -> None:
    now = _now_dt()
    should_emit_event = _mark_fuse_event_allowed(monitor, reason, now)
    monitor.status = "fused"
    monitor.fused_reason = reason
    monitor.updated_at = now
    db.add(monitor)
    if should_emit_event:
        _append_event(db, monitor, "monitor_fused", error_payload={"reason": reason})
    db.commit()
    _runtime_log(f"监控实例熔断 monitor={monitor.id} reason={reason}", level="ERROR")


def _mark_fuse_event_allowed(monitor: RealtimeMonitorDB, reason: str, now: datetime) -> bool:
    state = dict(monitor.state_json or {})
    guard = dict(state.get("fuse_guard") or {})
    last_reason = str(guard.get("last_reason") or "")
    last_event_at = _parse_datetime_value(guard.get("last_event_at"))
    if (
        monitor.status == "fused"
        and last_reason == str(reason)
        and last_event_at is not None
        and (_ensure_utc(now) - _ensure_utc(last_event_at)).total_seconds() < _FUSE_EVENT_COOLDOWN_SECONDS
    ):
        guard["suppressed_count"] = int(guard.get("suppressed_count") or 0) + 1
        guard["last_suppressed_at"] = now.isoformat()
        state["fuse_guard"] = guard
        monitor.state_json = state
        return False
    guard["last_reason"] = str(reason)
    guard["last_event_at"] = now.isoformat()
    guard["suppressed_count"] = 0
    state["fuse_guard"] = guard
    monitor.state_json = state
    return True


def _clear_fuse_guard(monitor: RealtimeMonitorDB) -> None:
    state = dict(monitor.state_json or {})
    if "fuse_guard" in state:
        state.pop("fuse_guard", None)
        monitor.state_json = state


def _mark_repetitive_status_event_allowed(
    monitor: RealtimeMonitorDB,
    event_type: str,
    reason: str,
    now: datetime,
    *,
    force_emit: bool = False,
) -> bool:
    if force_emit:
        return True
    state = dict(monitor.state_json or {})
    guard = dict(state.get("event_guard") or {})
    guard_key = f"{event_type}:{reason}"
    item = dict(guard.get(guard_key) or {})
    last_event_at = _parse_datetime_value(item.get("last_event_at"))
    if (
        last_event_at is not None
        and (_ensure_utc(now) - _ensure_utc(last_event_at)).total_seconds() < _REPETITIVE_STATUS_EVENT_COOLDOWN_SECONDS
    ):
        item["suppressed_count"] = int(item.get("suppressed_count") or 0) + 1
        item["last_suppressed_at"] = now.isoformat()
        guard[guard_key] = item
        state["event_guard"] = guard
        monitor.state_json = state
        return False
    item["last_event_at"] = now.isoformat()
    item["suppressed_count"] = 0
    guard[guard_key] = item
    state["event_guard"] = guard
    monitor.state_json = state
    return True


def _parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _append_event(
    db: Session,
    monitor: RealtimeMonitorDB,
    event_type: str,
    *,
    symbol: str | None = None,
    payload: dict[str, Any] | None = None,
    signal_payload: dict[str, Any] | None = None,
    risk_payload: dict[str, Any] | None = None,
    order_payload: dict[str, Any] | None = None,
    broker_result: dict[str, Any] | None = None,
    error_payload: dict[str, Any] | None = None,
    request_id: str | None = None,
    correlation_id: str | None = None,
) -> RealtimeEventDB:
    event = RealtimeEventDB(
        id=uuid4().hex,
        monitor_id=monitor.id,
        user_id=monitor.user_id,
        event_type=event_type,
        account_key=monitor.account_key,
        strategy_id=monitor.strategy_id,
        strategy_version_id=monitor.strategy_version_id,
        symbol=symbol,
        trade_time=_now_dt(),
        payload=_json_safe(payload or {}),
        signal_payload=_json_safe(signal_payload or {}),
        risk_payload=_json_safe(risk_payload or {}),
        order_payload=_json_safe(order_payload or {}),
        broker_result=_json_safe(broker_result or {}),
        error_payload=_json_safe(error_payload or {}),
        request_id=request_id or uuid4().hex,
        correlation_id=correlation_id,
        created_at=_now_dt(),
    )
    db.add(event)
    db.flush()
    _publish_event_to_subscribers(event)
    _runtime_log(
        f"event={event_type} monitor={monitor.id} account={monitor.account_key} strategy={monitor.strategy_id} symbol={symbol or '-'}"
    )
    return event


def _append_broker_followup_events(
    db: Session,
    monitor: RealtimeMonitorDB,
    symbol: str,
    broker_result: dict[str, Any],
    *,
    correlation_id: str | None = None,
) -> None:
    latest_order = broker_result.get("latest_order")
    if isinstance(latest_order, dict) and latest_order:
        _append_event(
            db,
            monitor,
            "order_snapshot_refreshed",
            symbol=symbol,
            order_payload=latest_order,
            broker_result={"order_id": broker_result.get("order_id")},
            correlation_id=correlation_id,
        )
    latest_trade = broker_result.get("latest_trade")
    if isinstance(latest_trade, dict) and latest_trade:
        _append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol=symbol,
            broker_result=latest_trade,
            correlation_id=correlation_id,
        )


def _refresh_execution_state(
    db: Session,
    main_db: Session,
    monitor: RealtimeMonitorDB,
    overview: dict[str, Any],
    *,
    correlation_id: str | None = None,
) -> None:
    tracker = _get_execution_tracker(monitor)
    current_orders = _current_order_map(overview)
    current_trades = _current_trade_map(overview)
    current_positions = _current_position_map(overview)
    if not tracker.get("initialized"):
        tracker["initialized"] = True
        tracker["last_orders"] = _json_safe(current_orders)
        tracker["seen_trade_ids"] = sorted(current_trades.keys())[-300:]
        tracker["last_positions"] = _json_safe(current_positions)
        _append_event(
            db,
            monitor,
            "execution_tracker_initialized",
            payload={
                "orders": len(current_orders),
                "trades": len(current_trades),
                "positions": len(current_positions),
            },
            correlation_id=correlation_id,
        )
    else:
        _emit_order_status_updates(db, monitor, tracker, current_orders, correlation_id=correlation_id)
        _emit_trade_updates(db, monitor, tracker, current_trades, correlation_id=correlation_id)
        _emit_position_updates(db, monitor, tracker, current_positions, correlation_id=correlation_id)
    _handle_pending_orders(db, main_db, monitor, tracker, current_orders, current_trades, correlation_id=correlation_id)
    _set_execution_tracker(monitor, tracker)


def _current_order_map(overview: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in overview.get("orders") or []:
        if not isinstance(item, dict):
            continue
        order_id = str(item.get("order_id") or item.get("entrust_no") or "").strip()
        if not order_id:
            continue
        rows[order_id] = _json_safe(item)
    return rows


def _current_trade_map(overview: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in overview.get("trades") or []:
        if not isinstance(item, dict):
            continue
        trade_id = _trade_identity(item)
        if not trade_id:
            continue
        rows[trade_id] = _json_safe(item)
    return rows


def _current_position_map(overview: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in overview.get("positions") or []:
        if not isinstance(item, dict):
            continue
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol:
            continue
        rows[symbol] = _json_safe(
            {
                "symbol": symbol,
                "name": item.get("name"),
                "current_position": item.get("current_position"),
                "available_position": item.get("available_position"),
                "market_value": item.get("market_value"),
                "average_cost": item.get("average_cost"),
            }
        )
    return rows


def _emit_order_status_updates(
    db: Session,
    monitor: RealtimeMonitorDB,
    tracker: dict[str, Any],
    current_orders: dict[str, dict[str, Any]],
    *,
    correlation_id: str | None = None,
) -> None:
    last_orders = dict(tracker.get("last_orders") or {})
    for order_id, item in current_orders.items():
        previous = last_orders.get(order_id)
        if previous != item:
            _append_event(
                db,
                monitor,
                "order_status_changed" if previous else "order_snapshot_refreshed",
                symbol=item.get("symbol"),
                payload={
                    "order_id": order_id,
                    "previous_status": (previous or {}).get("status"),
                    "current_status": item.get("status"),
                    "can_cancel": item.get("can_cancel"),
                    "filled_quantity": item.get("filled_quantity"),
                },
                order_payload=item,
                correlation_id=correlation_id,
            )
    tracker["last_orders"] = _json_safe(current_orders)


def _emit_trade_updates(
    db: Session,
    monitor: RealtimeMonitorDB,
    tracker: dict[str, Any],
    current_trades: dict[str, dict[str, Any]],
    *,
    correlation_id: str | None = None,
) -> None:
    seen_ids = set(str(item) for item in (tracker.get("seen_trade_ids") or []))
    for trade_id, item in current_trades.items():
        if trade_id in seen_ids:
            continue
        _append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol=item.get("symbol"),
            payload={"trade_id": trade_id, "order_id": item.get("order_id")},
            broker_result=item,
            correlation_id=correlation_id,
        )
        seen_ids.add(trade_id)
        _complete_pending_order(tracker, item.get("order_id"))
    tracker["seen_trade_ids"] = sorted(seen_ids)[-500:]


def _emit_position_updates(
    db: Session,
    monitor: RealtimeMonitorDB,
    tracker: dict[str, Any],
    current_positions: dict[str, dict[str, Any]],
    *,
    correlation_id: str | None = None,
) -> None:
    last_positions = dict(tracker.get("last_positions") or {})
    changed_symbols = sorted(set(last_positions) | set(current_positions))
    for symbol in changed_symbols:
        previous = last_positions.get(symbol)
        current = current_positions.get(symbol)
        if previous == current:
            continue
        _sync_reentry_anchor_with_position_change(monitor, symbol, previous, current)
        _append_event(
            db,
            monitor,
            "position_changed",
            symbol=symbol,
            payload={"previous": previous, "current": current},
            correlation_id=correlation_id,
        )
    tracker["last_positions"] = _json_safe(current_positions)


def _handle_pending_orders(
    db: Session,
    main_db: Session,
    monitor: RealtimeMonitorDB,
    tracker: dict[str, Any],
    current_orders: dict[str, dict[str, Any]],
    current_trades: dict[str, dict[str, Any]],
    *,
    correlation_id: str | None = None,
) -> None:
    if monitor.account_role != "paper" or not monitor.auto_trade_enabled:
        return
    config = dict(monitor.config_json or {})
    if not bool(config.get("auto_cancel_replace_enabled", True)):
        return
    cancel_after_seconds = int(config.get("cancel_after_seconds") or 20)
    max_replace_attempts = int(config.get("max_replace_attempts") or 1)
    lot_size = int(config.get("lot_size") or 100)
    pending_orders = dict(tracker.get("pending_orders") or {})
    if not pending_orders:
        tracker["pending_orders"] = pending_orders
        return
    now = _now_dt()
    for order_id, entry in list(pending_orders.items()):
        current_order = current_orders.get(order_id)
        if _has_trade_for_order(current_trades, order_id):
            pending_orders.pop(order_id, None)
            continue
        if current_order is None:
            if _seconds_since(entry.get("submitted_at"), now) > cancel_after_seconds * 3:
                pending_orders.pop(order_id, None)
            continue
        if _is_terminal_order(current_order):
            pending_orders.pop(order_id, None)
            continue
        entry["last_status"] = current_order.get("status")
        entry["last_seen_at"] = now.isoformat()
        if _seconds_since(entry.get("submitted_at"), now) < cancel_after_seconds:
            continue
        if not bool(current_order.get("can_cancel")):
            continue
        replace_attempts = int(entry.get("replace_attempts") or 0)
        _append_event(
            db,
            monitor,
            "order_cancel_requested",
            symbol=current_order.get("symbol"),
            payload={"order_id": order_id, "age_seconds": _seconds_since(entry.get("submitted_at"), now), "replace_attempts": replace_attempts},
            order_payload=current_order,
            correlation_id=correlation_id,
        )
        cancel_payload = qmt_virtual_account_service.cancel_qmt_order(
            main_db,
            monitor.user_id,
            account_key=monitor.account_key,
            order_id=order_id,
        )
        cancel_result = _normalize_cancel_result(cancel_payload)
        _append_event(
            db,
            monitor,
            "order_cancelled" if cancel_result.get("success") else "order_cancel_error",
            symbol=current_order.get("symbol"),
            payload={"order_id": order_id, "replace_attempts": replace_attempts},
            order_payload=current_order,
            broker_result=cancel_result if cancel_result.get("success") else {},
            error_payload={} if cancel_result.get("success") else cancel_result,
            correlation_id=correlation_id,
        )
        pending_orders.pop(order_id, None)
        if not cancel_result.get("success") or replace_attempts >= max_replace_attempts:
            continue
        original_intent = dict(entry.get("order_intent") or {})
        replace_quantity = _remaining_quantity(current_order, original_intent, lot_size)
        if replace_quantity < lot_size:
            continue
        original_intent["quantity"] = replace_quantity
        original_intent["replace_attempts"] = replace_attempts + 1
        original_intent["order_remark"] = f"{original_intent.get('order_remark') or 'realtime_monitor'}|replace_{replace_attempts + 1}"
        _append_event(
            db,
            monitor,
            "order_replace_requested",
            symbol=original_intent.get("symbol"),
            payload={"parent_order_id": order_id, "replace_attempts": replace_attempts + 1},
            order_payload=original_intent,
            correlation_id=correlation_id,
        )
        replace_result = _execute_order_intent(db, main_db, monitor, original_intent, reason="auto_replace")
        _append_event(
            db,
            monitor,
            "order_submitted" if replace_result.get("success") else "order_error",
            symbol=original_intent.get("symbol"),
            payload={"parent_order_id": order_id, "replace_attempts": replace_attempts + 1},
            order_payload=original_intent,
            broker_result=replace_result if replace_result.get("success") else {},
            error_payload={} if replace_result.get("success") else replace_result,
            correlation_id=correlation_id,
        )
        if replace_result.get("success"):
            _bump_stat(monitor, "orders")
            tracker["pending_orders"] = _json_safe(pending_orders)
            _set_execution_tracker(monitor, tracker)
            _register_pending_order(monitor, replace_result, original_intent)
            tracker.update(_get_execution_tracker(monitor))
            pending_orders = dict(tracker.get("pending_orders") or {})
            _append_broker_followup_events(db, monitor, str(original_intent.get("symbol") or ""), replace_result, correlation_id=correlation_id)
    tracker["pending_orders"] = _json_safe(pending_orders)


def _register_pending_order(monitor: RealtimeMonitorDB, broker_result: dict[str, Any], intent: dict[str, Any]) -> None:
    order_id = str(broker_result.get("order_id") or "").strip()
    if not order_id:
        return
    tracker = _get_execution_tracker(monitor)
    latest_trade = broker_result.get("latest_trade")
    if isinstance(latest_trade, dict) and latest_trade:
        trade_id = _trade_identity(latest_trade)
        seen_ids = set(str(item) for item in (tracker.get("seen_trade_ids") or []))
        if trade_id:
            seen_ids.add(trade_id)
            tracker["seen_trade_ids"] = sorted(seen_ids)[-500:]
        _complete_pending_order(tracker, order_id)
        _set_execution_tracker(monitor, tracker)
        return
    pending_orders = dict(tracker.get("pending_orders") or {})
    pending_orders[order_id] = _json_safe(
        {
            "order_id": order_id,
            "symbol": intent.get("symbol"),
            "side": intent.get("side"),
            "quantity": intent.get("quantity"),
            "submitted_at": _now_dt().isoformat(),
            "replace_attempts": int(intent.get("replace_attempts") or 0),
            "order_intent": dict(intent),
            "last_status": (broker_result.get("latest_order") or {}).get("status"),
            "last_seen_at": _now_dt().isoformat(),
        }
    )
    tracker["pending_orders"] = pending_orders
    last_orders = dict(tracker.get("last_orders") or {})
    latest_order = broker_result.get("latest_order")
    if isinstance(latest_order, dict) and latest_order:
        last_orders[order_id] = _json_safe(latest_order)
        tracker["last_orders"] = last_orders
    _set_execution_tracker(monitor, tracker)


def _complete_pending_order(tracker: dict[str, Any], order_id: Any) -> None:
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        return
    pending_orders = dict(tracker.get("pending_orders") or {})
    if normalized_order_id in pending_orders:
        pending_orders.pop(normalized_order_id, None)
        tracker["pending_orders"] = pending_orders


def _get_execution_tracker(monitor: RealtimeMonitorDB) -> dict[str, Any]:
    state = dict(monitor.state_json or {})
    tracker = dict(state.get("execution_tracker") or {})
    tracker.setdefault("initialized", False)
    tracker.setdefault("pending_orders", {})
    tracker.setdefault("last_orders", {})
    tracker.setdefault("seen_trade_ids", [])
    tracker.setdefault("last_positions", {})
    return tracker


def _set_execution_tracker(monitor: RealtimeMonitorDB, tracker: dict[str, Any]) -> None:
    state = dict(monitor.state_json or {})
    state["execution_tracker"] = _json_safe(tracker)
    state["execution_tracker_summary"] = {
        "pending_orders": len((tracker.get("pending_orders") or {})),
        "tracked_orders": len((tracker.get("last_orders") or {})),
        "tracked_trades": len((tracker.get("seen_trade_ids") or [])),
        "tracked_positions": len((tracker.get("last_positions") or {})),
    }
    monitor.state_json = _json_safe(state)


def _resolve_reentry_buy_quantity(
    monitor: RealtimeMonitorDB,
    overview: dict[str, Any],
    *,
    symbol: str,
    price: float,
    lot_size: int,
) -> int | None:
    anchor = _get_reentry_anchor(monitor, symbol)
    if not anchor:
        return None
    positions = {item.get("symbol"): item for item in (overview.get("positions") or []) if item.get("symbol")}
    if symbol in positions:
        return None
    available_cash = float((overview.get("account") or {}).get("available_cash") or (overview.get("account") or {}).get("cash") or 0.0)
    target_quantity = int(anchor.get("quantity") or 0)
    if target_quantity <= 0:
        return None
    if lot_size > 0:
        target_quantity = int(target_quantity // lot_size) * lot_size
    affordable_quantity = int((available_cash / max(price, 0.01)) // max(lot_size, 1)) * max(lot_size, 1)
    quantity = min(target_quantity, affordable_quantity) if affordable_quantity > 0 else 0
    return quantity if quantity > 0 else None


def _sync_reentry_anchor_with_position_change(
    monitor: RealtimeMonitorDB,
    symbol: str,
    previous: dict[str, Any] | None,
    current: dict[str, Any] | None,
) -> None:
    target_symbol = _single_symbol_reentry_target(monitor)
    if target_symbol != symbol:
        return
    previous_position = int(float((previous or {}).get("current_position") or 0))
    current_position = int(float((current or {}).get("current_position") or 0))
    if previous_position > 0 and current_position <= 0:
        _set_reentry_anchor(
            monitor,
            symbol,
            quantity=previous_position,
            previous_position=previous_position,
            current_position=current_position,
            source="position_changed_exit",
        )
        return
    if current_position > 0:
        _clear_reentry_anchor(monitor, symbol, reason="position_restored")


def _single_symbol_reentry_target(monitor: RealtimeMonitorDB) -> str | None:
    config = dict(monitor.config_json or {})
    if str(config.get("signal_mode") or "").strip().lower() != "first_day_band":
        return None
    pool = dict(monitor.monitor_pool_json or {})
    if str(pool.get("mode") or "").strip().lower() != "manual_only":
        return None
    candidates = pool.get("manual_symbols") or pool.get("symbols") or pool.get("resolved_symbols") or []
    normalized = []
    seen: set[str] = set()
    for item in candidates:
        normalized_symbol = _normalize_symbol(item)
        if normalized_symbol and normalized_symbol not in seen:
            seen.add(normalized_symbol)
            normalized.append(normalized_symbol)
    if len(normalized) != 1:
        return None
    return normalized[0]


def _get_reentry_anchor(monitor: RealtimeMonitorDB, symbol: str) -> dict[str, Any] | None:
    if _single_symbol_reentry_target(monitor) != symbol:
        return None
    state = dict(monitor.state_json or {})
    anchors = dict(state.get("reentry_anchors") or {})
    anchor = anchors.get(symbol)
    return dict(anchor) if isinstance(anchor, dict) else None


def _set_reentry_anchor(
    monitor: RealtimeMonitorDB,
    symbol: str,
    *,
    quantity: int,
    previous_position: int,
    current_position: int,
    source: str,
) -> None:
    if quantity <= 0:
        return
    state = dict(monitor.state_json or {})
    anchors = dict(state.get("reentry_anchors") or {})
    anchors[symbol] = {
        "symbol": symbol,
        "quantity": int(quantity),
        "previous_position": int(previous_position),
        "current_position": int(current_position),
        "source": source,
        "captured_at": _now_dt().isoformat(),
    }
    state["reentry_anchors"] = _json_safe(anchors)
    monitor.state_json = _json_safe(state)


def _clear_reentry_anchor(monitor: RealtimeMonitorDB, symbol: str, *, reason: str) -> None:
    state = dict(monitor.state_json or {})
    anchors = dict(state.get("reentry_anchors") or {})
    if symbol not in anchors:
        return
    anchors.pop(symbol, None)
    state["reentry_anchors"] = _json_safe(anchors)
    state["reentry_anchor_last_cleared"] = {
        "symbol": symbol,
        "reason": reason,
        "cleared_at": _now_dt().isoformat(),
    }
    monitor.state_json = _json_safe(state)


def _monitor_payload(monitor: RealtimeMonitorDB) -> dict[str, Any]:
    payload = monitor.to_dict()
    pool = dict(payload.get("monitor_pool") or {})
    manual_symbols = [
        _normalize_symbol(item)
        for item in (pool.get("manual_symbols") or pool.get("symbols") or [])
        if _normalize_symbol(item)
    ]
    resolved_symbols = [
        _normalize_symbol(item)
        for item in (pool.get("resolved_symbols") or [])
        if _normalize_symbol(item)
    ]
    payload["manual_symbols"] = manual_symbols
    payload["resolved_symbols"] = resolved_symbols
    payload["manual_symbol_count"] = len(manual_symbols)
    payload["resolved_symbol_count"] = len(resolved_symbols)
    payload["display_symbols"] = resolved_symbols or manual_symbols
    payload["display_symbol_count"] = len(payload["display_symbols"])
    payload["circuit_breaker"] = {
        "active": monitor.status == "fused",
        "reason": monitor.fused_reason,
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
    }
    payload["data_governance"] = build_realtime_monitor_governance(payload)
    return payload


def _normalize_broker_result(result: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(result or {})
    nested = payload.get("order_result")
    nested_result = nested if isinstance(nested, dict) else {}
    success = bool(payload.get("success"))
    if not success:
        success = bool(nested_result.get("success"))
    order_id = (
        payload.get("order_id")
        or nested_result.get("order_id")
        or nested_result.get("entrust_no")
    )
    overview = payload.get("overview") if isinstance(payload.get("overview"), dict) else {}
    latest_order = _find_matching_broker_item(overview.get("orders"), order_id=order_id)
    latest_trade = _find_matching_broker_item(overview.get("trades"), order_id=order_id)
    bridge = nested_result.get("bridge") if isinstance(nested_result.get("bridge"), dict) else payload.get("bridge")
    error = payload.get("error") or nested_result.get("error")
    if not success and not error and nested_result:
        error = nested_result.get("raw") or nested_result
    return {
        "success": success,
        "order_id": str(order_id) if order_id not in (None, "") else None,
        "request_id": payload.get("request_id"),
        "account_key": payload.get("account_key"),
        "bridge": _json_safe(bridge or {}),
        "order_result": _json_safe(nested_result),
        "overview": _json_safe(overview),
        "latest_order": _json_safe(latest_order or {}),
        "latest_trade": _json_safe(latest_trade or {}),
        "error": _json_safe(error),
        "raw": _json_safe(payload),
    }


def _normalize_cancel_result(result: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(result or {})
    nested = payload.get("cancel_result")
    nested_result = nested if isinstance(nested, dict) else {}
    success = bool(payload.get("success"))
    if not success:
        success = bool(nested_result.get("success"))
    order_id = payload.get("order_id") or nested_result.get("order_id")
    overview = payload.get("overview") if isinstance(payload.get("overview"), dict) else {}
    latest_order = _find_matching_broker_item(overview.get("orders"), order_id=order_id)
    latest_trade = _find_matching_broker_item(overview.get("trades"), order_id=order_id)
    error = payload.get("error") or nested_result.get("error")
    return {
        "success": success,
        "order_id": str(order_id) if order_id not in (None, "") else None,
        "request_id": payload.get("request_id"),
        "account_key": payload.get("account_key"),
        "cancel_result": _json_safe(nested_result),
        "overview": _json_safe(overview),
        "latest_order": _json_safe(latest_order or {}),
        "latest_trade": _json_safe(latest_trade or {}),
        "error": _json_safe(error),
        "raw": _json_safe(payload),
    }


def _find_matching_broker_item(items: Any, *, order_id: Any) -> dict[str, Any] | None:
    if not isinstance(items, list):
        return None
    normalized_order_id = str(order_id or "").strip()
    for item in items:
        if not isinstance(item, dict):
            continue
        item_order_id = str(item.get("order_id") or item.get("entrust_no") or "").strip()
        if normalized_order_id and item_order_id == normalized_order_id:
            return item
    if not normalized_order_id and items:
        first = items[0]
        return first if isinstance(first, dict) else None
    return None


def _trade_identity(item: dict[str, Any]) -> str | None:
    trade_id = str(item.get("trade_id") or item.get("business_no") or "").strip()
    if trade_id:
        return trade_id
    parts = [
        str(item.get("order_id") or "").strip(),
        str(item.get("symbol") or "").strip(),
        str(item.get("trade_time") or "").strip(),
        str(item.get("quantity") or "").strip(),
        str(item.get("price") or "").strip(),
    ]
    if any(parts):
        return "|".join(parts)
    return None


def _has_trade_for_order(current_trades: dict[str, dict[str, Any]], order_id: str) -> bool:
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        return False
    return any(str(item.get("order_id") or "").strip() == normalized_order_id for item in current_trades.values())


def _is_terminal_order(item: dict[str, Any]) -> bool:
    status_text = str(item.get("status") or "").strip().lower()
    if any(keyword in status_text for keyword in ("filled", "cancel", "rejected", "invalid", "done", "success_all")):
        return True
    return bool(item.get("can_cancel")) is False and float(item.get("filled_quantity") or 0) >= float(item.get("quantity") or 0)


def _seconds_since(value: Any, now: datetime) -> float:
    dt = _parse_datetime(value)
    if dt is None:
        return 0.0
    return max((now - dt).total_seconds(), 0.0)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    return _ensure_utc(parsed)


def _remaining_quantity(current_order: dict[str, Any], original_intent: dict[str, Any], lot_size: int) -> int:
    total_quantity = int(float(current_order.get("quantity") or original_intent.get("quantity") or 0))
    filled_quantity = int(float(current_order.get("filled_quantity") or 0))
    remaining = max(total_quantity - filled_quantity, 0)
    if lot_size <= 0:
        return remaining
    return int(remaining // lot_size) * lot_size


def _require_monitor(db: Session, user_id: str, monitor_id: str) -> RealtimeMonitorDB:
    row = (
        db.query(RealtimeMonitorDB)
        .filter(RealtimeMonitorDB.id == monitor_id, RealtimeMonitorDB.user_id == user_id)
        .first()
    )
    if row is None:
        raise KeyError("实时监控实例不存在")
    return row


def _require_approval(db: Session, user_id: str, approval_id: str) -> RealtimeApprovalDB:
    row = db.query(RealtimeApprovalDB).filter(RealtimeApprovalDB.id == approval_id, RealtimeApprovalDB.user_id == user_id).first()
    if row is None:
        raise KeyError("确认任务不存在")
    return row


def _require_strategy(db: Session, strategy_id: str) -> dict[str, Any]:
    strategy = get_platform_strategy(db, strategy_id)
    if strategy is None:
        raise KeyError("策略不存在")
    return strategy


def _default_config(raw: dict[str, Any]) -> dict[str, Any]:
    config = {
        "poll_interval_seconds": 20,
        "price_type": "opponent",
        "lot_size": 100,
        "max_signals_per_cycle": 3,
        "signal_mode": "intraday_confirmation",
        "signal_timeframe": "30m",
        "allow_outside_session": False,
        "auto_cancel_replace_enabled": True,
        "cancel_after_seconds": 20,
        "max_replace_attempts": 1,
    }
    config.update(raw or {})
    return config


def _default_risk_config(strategy: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    dsl = (strategy.get("current_version") or {}).get("dsl") or {}
    risk = dict(dsl.get("risk") or {})
    risk.update(raw or {})
    risk.setdefault("max_daily_orders", 20)
    return risk


def _target_position_pct(strategy: dict[str, Any]) -> float:
    dsl = (strategy.get("current_version") or {}).get("dsl") or {}
    position = dsl.get("position") or {}
    return float(position.get("initial_position_pct") or position.get("max_single_position_pct") or 0.02)


def _account_role(account_key: str, *, db: Session | None = None, user_id: str | None = None) -> str:
    for account in qmt_virtual_account_service._runtime_configs(db=db, user_id=user_id):
        if account.key == account_key:
            return str(account.role or "paper").lower()
    return "paper" if "paper" in account_key else "live" if "live" in account_key else "paper"


def _is_trading_day(value: datetime) -> bool:
    local_date = value.astimezone().date()
    try:
        return is_cn_trading_day(local_date.isoformat())
    except Exception:
        return local_date.weekday() < 5


def _is_trading_session(value: datetime) -> bool:
    local = value.astimezone().time()
    return dtime(9, 30) <= local <= dtime(11, 30) or dtime(13, 0) <= local <= dtime(15, 0)


def _is_monitor_trading_window(value: datetime) -> bool:
    return _is_trading_day(value) and _is_trading_session(value)


def _already_ordered_today(db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any]) -> bool:
    start = _day_start()
    return (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.monitor_id == monitor.id,
            RealtimeEventDB.event_type == "order_submitted",
            RealtimeEventDB.symbol == intent["symbol"],
            RealtimeEventDB.created_at >= start,
        )
        .first()
        is not None
    )


def _today_order_count(db: Session, monitor: RealtimeMonitorDB) -> int:
    return (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.monitor_id == monitor.id,
            RealtimeEventDB.event_type == "order_submitted",
            RealtimeEventDB.created_at >= _day_start(),
        )
        .count()
    )


def _bump_stat(monitor: RealtimeMonitorDB, key: str, amount: int = 1) -> None:
    state = dict(monitor.state_json or {})
    stats = dict(state.get("stats") or {})
    stats[key] = int(stats.get(key) or 0) + amount
    state["stats"] = stats
    monitor.state_json = state


def _update_state_stats(monitor: RealtimeMonitorDB, *, latest_cycle: str) -> None:
    state = dict(monitor.state_json or {})
    state["latest_cycle"] = latest_cycle
    state["last_updated_at"] = _now_dt().isoformat()
    monitor.state_json = state


def _monitor_lot_size(monitor: RealtimeMonitorDB) -> int:
    return max(int((monitor.config_json or {}).get("lot_size") or 100), 1)


def _should_block_unsellable_signal(
    monitor: RealtimeMonitorDB,
    intent: dict[str, Any],
    signal: dict[str, Any],
    risk: dict[str, Any],
) -> bool:
    return (
        str(signal.get("side") or "").lower() == "sell"
        and str(risk.get("reason") or "") == "quantity_below_lot_size"
        and int(intent.get("quantity") or 0) < _monitor_lot_size(monitor)
        and float(intent.get("available_position") or 0.0) < _monitor_lot_size(monitor)
    )


def _runtime_log(message: str, *, level: str = "INFO") -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {level}: {message}"
    try:
        with REALTIME_LOG_PATH.open("a", encoding="utf-8") as file:
            file.write(line + "\n")
    except Exception:
        logger.debug("[realtime-monitor] write runtime log failed", exc_info=True)
    if level == "ERROR":
        logger.error("[realtime-monitor] %s", message)
    else:
        logger.info("[realtime-monitor] %s", message)


def _normalize_symbol(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    if "." in text:
        return text
    if len(text) == 6:
        if text.startswith("6"):
            return f"{text}.SH"
        if text.startswith(("0", "3")):
            return f"{text}.SZ"
        if text.startswith(("4", "8")):
            return f"{text}.BJ"
    return text


def _to_float(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_safe(item())
        except Exception:
            pass
    return str(value)


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        return value.replace(tzinfo=local_tz).astimezone(timezone.utc)
    return value.astimezone(timezone.utc)


def _day_start() -> datetime:
    now = _now_dt()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)
