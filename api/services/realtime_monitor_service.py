from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import threading
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any
from uuid import uuid4

import pandas as pd
from sqlalchemy import and_, bindparam, or_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.core.stock_map import get_reverse_stock_map
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
from api.services.strategy_compute_backend import compute_daily_features
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
    "signal_notified",
    "signal_blocked",
    "order_rejected",
    "order_intent",
    "order_submitted",
    "order_error",
}
_FUSE_EVENT_COOLDOWN_SECONDS = 300
_REPETITIVE_STATUS_EVENT_COOLDOWN_SECONDS = 300
_QMT_AUTO_RESUME_CHECK_SECONDS = 30
_QMT_AUTO_RESUME_MAX_CANDIDATES = 10
_CN_TZ = ZoneInfo("Asia/Shanghai")
_QMT_INTERRUPT_EVENT_TYPES = ("monitor_fused", "monitor_interrupted")
_QMT_LIFECYCLE_EVENT_TYPES = (
    "monitor_paused",
    "monitor_stopped",
    "monitor_started",
    "monitor_resumed",
    "monitor_auto_resumed",
    "fuse_reset",
    "monitor_fused",
    "monitor_interrupted",
)
_SIGNAL_RULE_INDEX_RE = re.compile(r"dsl-(\d+)$")
_ACTIVITY_EVENT_TYPES = frozenset(
    {
        "signal_generated",
        "signal_notified",
        "signal_blocked",
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
        "trade_confirmed",
        "position_changed",
    }
)


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


def _validate_live_auto_trading_gate(
    *,
    account_role: str,
    execution_mode: str,
    live_trading_enabled: bool,
    live_confirmed: bool,
) -> None:
    if str(account_role or "").strip().lower() != "live":
        return
    if live_trading_enabled and not live_confirmed:
        raise ValueError("实盘自动交易必须显式确认 live_confirmed=true")
    if str(execution_mode or "").strip().lower() == "auto" and not (live_trading_enabled and live_confirmed):
        raise ValueError("实盘自动交易必须显式确认 live_trading_enabled=true 且 live_confirmed=true")


def create_monitor(strategy_db: Session, main_db: Session, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    strategy = _require_strategy(strategy_db, str(payload.get("strategy_id") or ""))
    compiled = _compile_strategy_payload(strategy)
    if compiled.status != "passed":
        raise ValueError("策略 DSL 编译未通过，不能创建实时监控实例：" + "；".join(compiled.errors))

    account_key = str(payload.get("account_key") or "paper_sim").strip() or "paper_sim"
    account_role = _account_role(account_key, db=main_db, user_id=user_id)
    live_trading_enabled = bool(payload.get("live_trading_enabled", False))
    execution_mode = str(payload.get("execution_mode") or "auto").strip() or "auto"
    _validate_live_auto_trading_gate(
        account_role=account_role,
        execution_mode=execution_mode,
        live_trading_enabled=live_trading_enabled,
        live_confirmed=bool(payload.get("live_confirmed")),
    )

    pool_config = dict(payload.get("monitor_pool") or {})
    pool_config.setdefault("mode", "strategy_positions_watchlist")
    pool_config["resolved_symbols"] = _resolve_monitor_symbols(main_db, user_id, account_key, strategy, pool_config)
    config = _default_config(payload.get("config") or {})

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
        config_json=config,
        risk_config_json=_default_risk_config(strategy, payload.get("risk_config") or {}),
        state_json={
            "compiled_status": compiled.status,
            "timeframes_required": _config_timeframes(config, compiled.timeframes_required),
            "minute_requirements": compiled.minute_requirements,
            "latest_cycle": None,
            "stats": {"signals": 0, "orders": 0, "rejections": 0},
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


def update_monitor(
    strategy_db: Session,
    main_db: Session,
    user_id: str,
    monitor_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    monitor = _require_monitor(strategy_db, user_id, monitor_id)
    if monitor.status == "running":
        raise ValueError("运行中实例请先暂停，再编辑监控配置")

    next_strategy_id = str(payload.get("strategy_id") or monitor.strategy_id or "").strip()
    strategy = _require_strategy(strategy_db, next_strategy_id)
    compiled = _compile_strategy_payload(strategy)
    if compiled.status != "passed":
        raise ValueError("策略 DSL 编译未通过，不能更新实时监控实例：" + "；".join(compiled.errors))

    next_account_key = str(payload.get("account_key") or monitor.account_key or "paper_sim").strip() or "paper_sim"
    next_account_role = _account_role(next_account_key, db=main_db, user_id=user_id)
    next_execution_mode = str(payload.get("execution_mode") or monitor.execution_mode or "auto").strip() or "auto"
    next_live_trading_enabled = bool(
        payload["live_trading_enabled"]
        if "live_trading_enabled" in payload
        else monitor.live_trading_enabled
    )
    _validate_live_auto_trading_gate(
        account_role=next_account_role,
        execution_mode=next_execution_mode,
        live_trading_enabled=next_live_trading_enabled,
        live_confirmed=bool(payload.get("live_confirmed")),
    )

    if "monitor_pool" in payload:
        pool_config = dict(payload.get("monitor_pool") or {})
    else:
        pool_config = dict(monitor.monitor_pool_json or {})
    pool_config.setdefault("mode", "strategy_positions_watchlist")
    pool_config["resolved_symbols"] = _resolve_monitor_symbols(main_db, user_id, next_account_key, strategy, pool_config)

    config = _default_config(payload.get("config") if "config" in payload else dict(monitor.config_json or {}))
    risk_config = _default_risk_config(
        strategy,
        payload.get("risk_config") if "risk_config" in payload else dict(monitor.risk_config_json or {}),
    )
    state = dict(monitor.state_json or {})
    state["compiled_status"] = compiled.status
    state["timeframes_required"] = _config_timeframes(config, compiled.timeframes_required)
    state["minute_requirements"] = compiled.minute_requirements
    stats = dict(state.get("stats") or {})
    for key in ("signals", "orders", "rejections"):
        stats.setdefault(key, 0)
    stats.pop("approvals", None)
    state["stats"] = stats
    state["last_updated_at"] = _now_dt().isoformat()

    if "name" in payload:
        monitor.name = str(payload.get("name") or f"实时监控-{strategy['name']}").strip()
    monitor.account_key = next_account_key
    monitor.account_role = next_account_role
    monitor.strategy_id = strategy["id"]
    monitor.strategy_version_id = payload.get("strategy_version_id") or strategy.get("current_version_id")
    monitor.execution_mode = next_execution_mode
    monitor.auto_trade_enabled = next_execution_mode == "auto"
    monitor.live_trading_enabled = next_live_trading_enabled
    monitor.monitor_pool_json = pool_config
    monitor.config_json = config
    monitor.risk_config_json = risk_config
    monitor.state_json = state
    monitor.updated_at = _now_dt()

    _append_event(strategy_db, monitor, "monitor_updated", payload={"monitor": monitor.to_dict()})
    strategy_db.commit()
    strategy_db.refresh(monitor)
    return _monitor_payload(monitor)


def list_monitors(db: Session, user_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(RealtimeMonitorDB)
        .filter(RealtimeMonitorDB.user_id == user_id)
        .order_by(RealtimeMonitorDB.updated_at.desc(), RealtimeMonitorDB.created_at.desc())
        .all()
    )
    return [_monitor_payload(row) for row in rows if not _is_hidden_system_monitor(row)]


def get_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    return _monitor_payload(_require_monitor(db, user_id, monitor_id))


def delete_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    payload = _monitor_payload(monitor)
    _delete_monitor_records(db, [monitor.id], user_id=user_id)
    db.commit()
    return {
        "message": "实时监控实例已删除",
        "monitor": payload,
    }


def delete_monitor_records(db: Session, monitor_ids: list[str], *, user_id: str | None = None) -> dict[str, int]:
    return _delete_monitor_records(db, monitor_ids, user_id=user_id)


def _delete_monitor_records(db: Session, monitor_ids: list[str], *, user_id: str | None = None) -> dict[str, int]:
    ids = [str(monitor_id) for monitor_id in monitor_ids if str(monitor_id or "").strip()]
    if not ids:
        return {"monitors": 0, "events": 0, "legacy_approvals": 0, "signal_executions": 0}

    monitor_filter = [RealtimeMonitorDB.id.in_(ids)]
    event_filter = [RealtimeEventDB.monitor_id.in_(ids)]
    approval_filter = [RealtimeApprovalDB.monitor_id.in_(ids)]
    signal_filter = [RealtimeSignalExecutionDB.monitor_id.in_(ids)]
    if user_id is not None:
        monitor_filter.append(RealtimeMonitorDB.user_id == user_id)
        event_filter.append(RealtimeEventDB.user_id == user_id)
        approval_filter.append(RealtimeApprovalDB.user_id == user_id)
        signal_filter.append(RealtimeSignalExecutionDB.user_id == user_id)

    approval_count = db.query(RealtimeApprovalDB).filter(*approval_filter).delete(synchronize_session=False)
    signal_count = db.query(RealtimeSignalExecutionDB).filter(*signal_filter).delete(synchronize_session=False)
    event_count = db.query(RealtimeEventDB).filter(*event_filter).delete(synchronize_session=False)
    monitor_count = db.query(RealtimeMonitorDB).filter(*monitor_filter).delete(synchronize_session=False)
    return {
        "monitors": monitor_count,
        "events": event_count,
        "legacy_approvals": approval_count,
        "signal_executions": signal_count,
    }


def _is_hidden_system_monitor(monitor: RealtimeMonitorDB) -> bool:
    config = monitor.config_json if isinstance(monitor.config_json, dict) else {}
    pool = monitor.monitor_pool_json if isinstance(monitor.monitor_pool_json, dict) else {}
    return (
        str(config.get("source") or "").strip() == "catalyst-selection"
        or str(pool.get("source") or "").strip() == "catalyst-selection"
        or str(monitor.name or "").strip().startswith("AI监控池 ")
    )


def start_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    archive_obsolete_pending_approvals(db, user_id=user_id, monitor_id=monitor_id)
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
    _clear_qmt_auto_resume_state(monitor)
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
    _clear_qmt_auto_resume_state(monitor)
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
    _clear_qmt_auto_resume_state(monitor)
    monitor.updated_at = _now_dt()
    db.add(monitor)
    db.commit()
    db.refresh(monitor)
    _append_event(db, monitor, "monitor_stopped", payload={"status": monitor.status})
    db.commit()
    return _monitor_payload(monitor)


def resume_monitor(db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    if monitor.status not in {"paused", "halted", "ready", "fused"}:
        raise ValueError("只有 ready/paused/halted/fused 状态可以恢复运行")
    archive_obsolete_pending_approvals(db, user_id=user_id, monitor_id=monitor_id)
    monitor.status = "running"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    _clear_qmt_auto_resume_state(monitor)
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
    previous_reason = _monitor_interrupt_reason(monitor)
    monitor.status = "paused"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    if _is_qmt_interrupt_reason(previous_reason):
        _mark_qmt_auto_resume_pending(monitor, previous_reason, _now_dt(), source="fuse_reset")
    else:
        _clear_qmt_auto_resume_state(monitor)
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
    archive_obsolete_pending_approvals(strategy_db, user_id=user_id, monitor_id=monitor_id)
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
    activity_only: bool = False,
) -> list[dict[str, Any]]:
    _require_monitor(db, user_id, monitor_id)
    max_limit = max(min(limit, 50000), 1)
    query = db.query(RealtimeEventDB).filter(
        RealtimeEventDB.monitor_id == monitor_id,
        RealtimeEventDB.user_id == user_id,
    )
    if activity_only:
        query = query.filter(RealtimeEventDB.event_type.in_(sorted(_ACTIVITY_EVENT_TYPES)))
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
            cursor_query = query.filter(
                or_(
                    RealtimeEventDB.created_at > cursor.created_at,
                    and_(RealtimeEventDB.created_at == cursor.created_at, RealtimeEventDB.id > cursor.id),
                )
            )
            rows = (
                cursor_query
                .order_by(RealtimeEventDB.created_at.asc(), RealtimeEventDB.id.asc())
                .limit(max_limit)
                .all()
            )
            if activity_only:
                return [row.to_dict() for row in _collect_activity_event_rows(cursor_query, max_limit, descending=False)]
            return [row.to_dict() for row in rows]
    if activity_only:
        rows = _collect_activity_event_rows(query, max_limit, descending=True)
        return [row.to_dict() for row in reversed(rows)]
    rows = query.order_by(RealtimeEventDB.created_at.desc(), RealtimeEventDB.id.desc()).limit(max_limit).all()
    return [row.to_dict() for row in reversed(rows)]


def _collect_activity_event_rows(query: Any, limit: int, *, descending: bool) -> list[RealtimeEventDB]:
    order_by = (
        (RealtimeEventDB.created_at.desc(), RealtimeEventDB.id.desc())
        if descending
        else (RealtimeEventDB.created_at.asc(), RealtimeEventDB.id.asc())
    )
    rows: list[RealtimeEventDB] = []
    offset = 0
    batch_size = min(max(limit * 2, 200), 1000)
    while len(rows) < limit:
        batch = query.order_by(*order_by).offset(offset).limit(batch_size).all()
        if not batch:
            break
        rows.extend(row for row in batch if _is_meaningful_activity_event_row(row))
        offset += len(batch)
        if len(batch) < batch_size:
            break
    return rows[:limit]


def _is_meaningful_activity_event_row(row: RealtimeEventDB) -> bool:
    if row.event_type not in _ACTIVITY_EVENT_TYPES:
        return False
    if row.event_type != "position_changed":
        return True
    payload = row.payload if isinstance(row.payload, dict) else {}
    previous = payload.get("previous") if isinstance(payload.get("previous"), dict) else None
    current = payload.get("current") if isinstance(payload.get("current"), dict) else None
    return _position_quantity_delta(previous, current) != 0


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


def get_performance(db: Session, main_db: Session, user_id: str, monitor_id: str) -> dict[str, Any]:
    monitor = _require_monitor(db, user_id, monitor_id)
    overview = qmt_virtual_account_service.get_qmt_virtual_account_overview(
        main_db,
        user_id,
        account_key=monitor.account_key,
        prefer_cache=True,
        allow_cache_fallback=True,
    )
    state = dict(monitor.state_json or {})
    baseline = state.get("performance_baseline") if isinstance(state.get("performance_baseline"), dict) else None
    if not baseline:
        baseline = _build_performance_baseline(overview)
        state["performance_baseline"] = baseline
        monitor.state_json = state
        monitor.updated_at = _now_dt()
        db.add(monitor)
        db.commit()
        db.refresh(monitor)
    return _build_performance_payload(db, monitor, overview, baseline)


def _build_performance_baseline(overview: dict[str, Any]) -> dict[str, Any]:
    account = overview.get("account") if isinstance(overview.get("account"), dict) else {}
    positions = overview.get("positions") if isinstance(overview.get("positions"), list) else []
    normalized_positions = [_performance_position_snapshot(item) for item in positions if isinstance(item, dict)]
    start_market_value = sum(_to_float(item.get("market_value")) or 0.0 for item in normalized_positions)
    start_total_asset = _to_float(account.get("total_asset"))
    if start_total_asset is None:
        start_cash = _to_float(account.get("available_cash"), account.get("cash")) or 0.0
        start_total_asset = start_cash + start_market_value
    else:
        start_cash = start_total_asset - start_market_value
    return _json_safe(
        {
            "captured_at": _now_dt().isoformat(),
            "account": {
                "total_asset": round(start_total_asset, 2),
                "available_cash": round(start_cash, 2),
                "market_value": round(start_market_value, 2),
            },
            "positions": normalized_positions,
            "start_total_asset": round(start_total_asset, 2),
            "start_cash": round(start_cash, 2),
            "start_market_value": round(start_market_value, 2),
        }
    )


def _performance_position_snapshot(position: dict[str, Any]) -> dict[str, Any]:
    symbol = _normalize_symbol(position.get("symbol")) or str(position.get("symbol") or "").strip().upper()
    quantity = _to_float(position.get("current_position"), position.get("quantity"), position.get("volume")) or 0.0
    price = _position_price(position)
    market_value = _to_float(position.get("market_value"))
    if market_value is None and price is not None:
        market_value = quantity * price
    return _json_safe(
        {
            "symbol": symbol,
            "name": position.get("name"),
            "current_position": quantity,
            "current_price": price,
            "market_value": round(market_value or 0.0, 2),
        }
    )


def _build_performance_payload(
    db: Session,
    monitor: RealtimeMonitorDB,
    overview: dict[str, Any],
    baseline: dict[str, Any],
) -> dict[str, Any]:
    account = overview.get("account") if isinstance(overview.get("account"), dict) else {}
    current_positions = [
        item for item in (overview.get("positions") if isinstance(overview.get("positions"), list) else [])
        if isinstance(item, dict)
    ]
    current_by_symbol = {
        _normalize_symbol(item.get("symbol")): item
        for item in current_positions
        if _normalize_symbol(item.get("symbol"))
    }
    baseline_positions = [
        item for item in (baseline.get("positions") if isinstance(baseline.get("positions"), list) else [])
        if isinstance(item, dict)
    ]
    baseline_by_symbol = {
        _normalize_symbol(item.get("symbol")): item
        for item in baseline_positions
        if _normalize_symbol(item.get("symbol"))
    }
    performance_trade_date = _performance_trade_date(overview)
    trade_cashflows = _performance_trade_cashflows(
        db,
        monitor,
        trade_date=performance_trade_date,
        current_positions=current_by_symbol,
    )
    start_total_asset = _to_float(baseline.get("start_total_asset"), (baseline.get("account") or {}).get("total_asset")) or 0.0
    start_cash = _to_float(baseline.get("start_cash"), (baseline.get("account") or {}).get("available_cash")) or 0.0
    strategy_total_asset = _to_float(account.get("total_asset"))
    if strategy_total_asset is None:
        strategy_total_asset = (_to_float(account.get("available_cash"), account.get("cash")) or 0.0) + sum(
            _to_float(item.get("market_value")) or 0.0 for item in current_positions
        )
    all_symbols = sorted({symbol for symbol in baseline_by_symbol if symbol} | {symbol for symbol in current_by_symbol if symbol})
    symbol_rows: list[dict[str, Any]] = []
    hold_market_value = 0.0
    strategy_today_pnl_total = 0.0
    hold_today_pnl_total = 0.0
    baseline_today_market_value_total = 0.0
    yesterday_cash = strategy_total_asset - sum(_to_float(item.get("market_value")) or 0.0 for item in current_positions)
    for symbol in all_symbols:
        baseline_position = baseline_by_symbol.get(symbol) or {}
        current_position = current_by_symbol.get(symbol) or {}
        previous_close = _to_float(current_position.get("previous_close"), baseline_position.get("current_price"))
        baseline_quantity = _today_baseline_quantity(current_position, baseline_position)
        baseline_price = previous_close if previous_close is not None else _to_float(baseline_position.get("current_price")) or 0.0
        baseline_market_value = baseline_quantity * baseline_price
        current_quantity = _to_float(current_position.get("current_position"), current_position.get("quantity")) or 0.0
        current_price = _position_price(current_position) or _to_float(baseline_position.get("current_price")) or 0.0
        current_market_value = _to_float(current_position.get("market_value"))
        if current_market_value is None:
            current_market_value = current_quantity * current_price
        hold_value = baseline_quantity * current_price
        hold_pnl = hold_value - baseline_market_value
        cashflow = trade_cashflows.get(symbol) or {}
        trade_buy_amount = _to_float(cashflow.get("buy_amount")) or 0.0
        trade_sell_amount = _to_float(cashflow.get("sell_amount")) or 0.0
        realized_pnl = _to_float(cashflow.get("realized_pnl")) or 0.0
        strategy_pnl = (current_market_value or 0.0) + trade_sell_amount - trade_buy_amount - baseline_market_value
        excess_pnl = strategy_pnl - hold_pnl
        hold_market_value += hold_value
        strategy_today_pnl_total += strategy_pnl
        hold_today_pnl_total += hold_pnl
        baseline_today_market_value_total += baseline_market_value
        yesterday_cash -= baseline_market_value
        symbol_rows.append(
            {
                "symbol": symbol,
                "name": current_position.get("name") or baseline_position.get("name") or symbol,
                "baseline_quantity": round(baseline_quantity, 2),
                "strategy_quantity": round(current_quantity, 2),
                "baseline_price": round(baseline_price, 4),
                "current_price": round(current_price, 4),
                "baseline_market_value": round(baseline_market_value, 2),
                "hold_market_value": round(hold_value, 2),
                "strategy_market_value": round(current_market_value or 0.0, 2),
                "strategy_pnl": round(strategy_pnl, 2),
                "hold_pnl": round(hold_pnl, 2),
                "excess_pnl": round(excess_pnl, 2),
                "trade_buy_amount": round(trade_buy_amount, 2),
                "trade_sell_amount": round(trade_sell_amount, 2),
                "realized_pnl": round(realized_pnl, 2),
                "trades": cashflow.get("trades") or [],
                "strategy_position_pnl": round(strategy_pnl, 2),
                "position_delta": round(current_quantity - baseline_quantity, 2),
            }
        )
    hold_total_asset = max(yesterday_cash, 0.0) + hold_market_value
    strategy_pnl = strategy_today_pnl_total
    hold_pnl = hold_today_pnl_total
    excess_pnl = strategy_pnl - hold_pnl
    today_base = baseline_today_market_value_total or start_total_asset
    return _json_safe(
        {
            "monitor_id": monitor.id,
            "account_key": monitor.account_key,
            "currency": "CNY",
            "baseline_captured_at": baseline.get("captured_at"),
            "trade_date": performance_trade_date,
            "performance_mode": "today_strategy_vs_hold",
            "calculated_at": _now_dt().isoformat(),
            "fetched_at": overview.get("fetched_at"),
            "data_source": overview.get("data_source"),
            "is_stale": bool(overview.get("is_stale", False)),
            "start_total_asset": round(today_base, 2),
            "start_cash": round(max(yesterday_cash, 0.0), 2),
            "strategy": {
                "total_asset": round(strategy_total_asset, 2),
                "pnl": round(strategy_pnl, 2),
                "return_pct": _safe_pct(strategy_pnl, today_base),
                "available_cash": round(_to_float(account.get("available_cash"), account.get("cash")) or 0.0, 2),
                "market_value": round(sum(_to_float(item.get("market_value")) or 0.0 for item in current_positions), 2),
            },
            "hold_baseline": {
                "total_asset": round(hold_total_asset, 2),
                "pnl": round(hold_pnl, 2),
                "return_pct": _safe_pct(hold_pnl, today_base),
                "cash": round(max(yesterday_cash, 0.0), 2),
                "market_value": round(hold_market_value, 2),
            },
            "excess": {
                "pnl": round(excess_pnl, 2),
                "return_pct": _safe_pct(excess_pnl, today_base),
            },
            "symbols": symbol_rows,
        }
    )


def _performance_trade_cashflows(
    db: Session,
    monitor: RealtimeMonitorDB,
    *,
    trade_date: str,
    current_positions: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, float]]:
    query = db.query(RealtimeEventDB).filter(
        RealtimeEventDB.monitor_id == monitor.id,
        RealtimeEventDB.user_id == monitor.user_id,
        RealtimeEventDB.event_type == "trade_confirmed",
    )
    rows = query.order_by(RealtimeEventDB.created_at.asc(), RealtimeEventDB.id.asc()).all()
    cashflows: dict[str, dict[str, Any]] = {}
    seen_trade_ids: set[str] = set()
    parsed_rows: list[tuple[RealtimeEventDB, dict[str, Any], datetime | None, bool]] = []
    has_exact_trade_date = False
    for row in rows:
        trade = row.broker_result if isinstance(row.broker_result, dict) else {}
        broker_trade_dt = _broker_trade_datetime(row)
        trade_dt = broker_trade_dt or _event_trade_datetime(row)
        matches_trade_date = _cn_date_text(trade_dt) == trade_date
        if matches_trade_date:
            has_exact_trade_date = True
        parsed_rows.append((row, trade, trade_dt, broker_trade_dt is not None))

    for row, trade, trade_dt, has_broker_trade_time in parsed_rows:
        if _cn_date_text(trade_dt) != trade_date:
            if has_exact_trade_date or has_broker_trade_time:
                continue
        symbol = _normalize_symbol(row.symbol or trade.get("symbol"))
        if not symbol:
            continue
        current_position = (current_positions or {}).get(symbol) or {}
        trade_id = _trade_identity(trade) or row.id
        if trade_id in seen_trade_ids:
            continue
        seen_trade_ids.add(trade_id)
        side = _normalize_event_side(trade.get("side") or trade.get("direction") or (row.order_payload or {}).get("side"))
        amount = _trade_amount(trade)
        if amount <= 0:
            continue
        quantity = _trade_quantity(trade)
        price = _trade_price(trade)
        reference_cost = _trade_reference_cost(trade)
        if reference_cost is None:
            reference_cost = _to_float(current_position.get("average_cost"), current_position.get("cost_price"), current_position.get("open_price"))
        current_price = _to_float(current_position.get("current_price"), trade.get("current_price"), trade.get("last_price"))
        realized_pnl = 0.0
        excess_pnl = 0.0
        if side == "sell":
            if reference_cost is not None:
                realized_pnl = (price - reference_cost) * quantity
            if current_price is not None:
                excess_pnl = (price - current_price) * quantity
        elif side == "buy":
            if current_price is not None:
                realized_pnl = (current_price - price) * quantity
                excess_pnl = realized_pnl
        bucket = cashflows.setdefault(symbol, {"buy_amount": 0.0, "sell_amount": 0.0, "realized_pnl": 0.0, "trades": []})
        if side == "sell":
            bucket["sell_amount"] += amount
        elif side == "buy":
            bucket["buy_amount"] += amount
        bucket["realized_pnl"] += realized_pnl
        bucket["trades"].append(
            {
                "event_id": row.id,
                "trade_id": trade_id,
                "trade_time": trade_dt.isoformat() if trade_dt else None,
                "side": side,
                "quantity": round(quantity, 2),
                "price": round(price, 4),
                "amount": round(amount, 2),
                "reference_cost": round(reference_cost, 4) if reference_cost is not None else None,
                "current_price": round(current_price, 4) if current_price is not None else None,
                "realized_pnl": round(realized_pnl, 2),
                "excess_pnl": round(excess_pnl, 2),
            }
        )
    return {
        symbol: {
            "buy_amount": round(float(item.get("buy_amount") or 0.0), 2),
            "sell_amount": round(float(item.get("sell_amount") or 0.0), 2),
            "realized_pnl": round(float(item.get("realized_pnl") or 0.0), 2),
            "trades": item.get("trades") or [],
        }
        for symbol, item in cashflows.items()
    }


def _trade_amount(trade: dict[str, Any]) -> float:
    explicit = _to_float(
        trade.get("amount"),
        trade.get("trade_amount"),
        trade.get("business_amount"),
        trade.get("deal_amount"),
        trade.get("filled_amount"),
    )
    if explicit is not None:
        return abs(explicit)
    quantity = _trade_quantity(trade)
    price = _to_float(
        trade.get("price"),
        trade.get("trade_price"),
        trade.get("deal_price"),
        trade.get("business_price"),
    ) or 0.0
    return abs(quantity * price)


def _trade_price(trade: dict[str, Any]) -> float:
    return _to_float(
        trade.get("price"),
        trade.get("trade_price"),
        trade.get("deal_price"),
        trade.get("business_price"),
        trade.get("traded_price"),
    ) or 0.0


def _trade_reference_cost(trade: dict[str, Any]) -> float | None:
    return _to_float(
        trade.get("reference_cost"),
        trade.get("cost_price"),
        trade.get("average_cost"),
        trade.get("avg_price"),
        trade.get("position_cost"),
    )


def _performance_trade_date(overview: dict[str, Any]) -> str:
    fetched_at = _parse_datetime(overview.get("fetched_at"))
    if fetched_at is not None:
        return _cn_date_text(fetched_at)
    return _now_dt().astimezone(_CN_TZ).date().isoformat()


def _today_baseline_quantity(current_position: dict[str, Any], fallback_position: dict[str, Any]) -> float:
    yesterday_quantity = _to_float(
        current_position.get("yesterday_position"),
        current_position.get("yesterday_volume"),
        (current_position.get("raw") or {}).get("yesterday_volume") if isinstance(current_position.get("raw"), dict) else None,
        (current_position.get("raw") or {}).get("m_nYesterdayVolume") if isinstance(current_position.get("raw"), dict) else None,
    )
    if yesterday_quantity is not None:
        return yesterday_quantity
    return _to_float(fallback_position.get("current_position"), fallback_position.get("quantity")) or 0.0


def _event_trade_datetime(row: RealtimeEventDB) -> datetime | None:
    trade = row.broker_result if isinstance(row.broker_result, dict) else {}
    parsed = _parse_datetime(
        trade.get("trade_time")
        or trade.get("traded_time")
        or trade.get("business_time")
        or row.trade_time
        or row.created_at
    )
    return parsed


def _broker_trade_datetime(row: RealtimeEventDB) -> datetime | None:
    trade = row.broker_result if isinstance(row.broker_result, dict) else {}
    return _parse_datetime(
        trade.get("trade_time")
        or trade.get("traded_time")
        or trade.get("business_time")
    )


def _cn_date_text(value: datetime | date | None) -> str:
    if value is None:
        return _now_dt().astimezone(_CN_TZ).date().isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_CN_TZ)
        return value.astimezone(_CN_TZ).date().isoformat()
    return value.isoformat()


def _attach_trade_performance_to_events(events: list[RealtimeEventDB], overview: dict[str, Any]) -> None:
    positions = overview.get("positions") if isinstance(overview.get("positions"), list) else []
    by_symbol = {
        _normalize_symbol(item.get("symbol")): item
        for item in positions
        if isinstance(item, dict) and _normalize_symbol(item.get("symbol"))
    }
    for row in events:
        if row.event_type != "trade_confirmed":
            continue
        trade = row.broker_result if isinstance(row.broker_result, dict) else {}
        symbol = _normalize_symbol(row.symbol or trade.get("symbol"))
        position = by_symbol.get(symbol) or {}
        price = _trade_price(trade)
        quantity = _trade_quantity(trade)
        side = _normalize_event_side(trade.get("side") or trade.get("direction") or (row.order_payload or {}).get("side"))
        reference_cost = _trade_reference_cost(trade)
        if reference_cost is None:
            reference_cost = _to_float(position.get("average_cost"), position.get("cost_price"), position.get("open_price"))
        current_price = _to_float(position.get("current_price"), trade.get("current_price"), trade.get("last_price"))
        realized_pnl = 0.0
        excess_pnl = 0.0
        if side == "sell":
            if reference_cost is not None:
                realized_pnl = (price - reference_cost) * quantity
            if current_price is not None:
                excess_pnl = (price - current_price) * quantity
        elif side == "buy":
            if current_price is not None:
                realized_pnl = (current_price - price) * quantity
                excess_pnl = realized_pnl
        payload = dict(row.payload or {})
        payload["performance"] = _json_safe(
            {
                "trade_pnl": round(realized_pnl, 2),
                "realized_pnl": round(realized_pnl, 2),
                "excess_pnl": round(excess_pnl, 2),
                "reference_cost": round(reference_cost, 4) if reference_cost is not None else None,
                "current_price": round(current_price, 4) if current_price is not None else None,
                "basis": "sell_cost_and_hold_opportunity" if side == "sell" else "buy_mark_to_market",
            }
        )
        row.payload = payload



def _position_price(position: dict[str, Any]) -> float | None:
    quantity = _to_float(position.get("current_position"), position.get("quantity"), position.get("volume")) or 0.0
    market_value = _to_float(position.get("market_value"))
    explicit = _to_float(position.get("current_price"), position.get("price"), position.get("last_price"), position.get("close"))
    if explicit is not None:
        return explicit
    if quantity > 0 and market_value is not None:
        return market_value / quantity
    return None


def _safe_pct(amount: float, base: float) -> float:
    if not base:
        return 0.0
    return round(amount / base, 6)


def archive_obsolete_pending_approvals(
    db: Session,
    *,
    user_id: str | None = None,
    monitor_id: str | None = None,
    account_key: str | None = None,
    symbol: str | None = None,
) -> int:
    query = db.query(RealtimeApprovalDB).filter(RealtimeApprovalDB.status == "pending")
    if user_id:
        query = query.filter(RealtimeApprovalDB.user_id == user_id)
    if monitor_id:
        query = query.filter(RealtimeApprovalDB.monitor_id == monitor_id)
    if account_key:
        query = query.filter(RealtimeApprovalDB.account_key == account_key)
    if symbol:
        query = query.filter(RealtimeApprovalDB.symbol == _normalize_symbol(symbol))
    now = _now_dt()
    count = 0
    for approval in query.all():
        decision = dict(approval.decision_json or {})
        decision.update(
            {
                "reason": "manual_approval_flow_removed",
                "archived_by": "realtime_monitor_service",
                "archived_at": now.isoformat(),
            }
        )
        approval.status = "rejected"
        approval.decision_json = _json_safe(decision)
        approval.decided_at = now
        approval.updated_at = now
        db.add(approval)
        count += 1
    return count


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
    _auto_resume_qmt_recovered_monitors()
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


def _auto_resume_qmt_recovered_monitors() -> None:
    now = _now_dt()
    with get_strategy_db_ctx() as strategy_db, get_db_ctx() as main_db:
        rows = (
            strategy_db.query(RealtimeMonitorDB)
            .filter(RealtimeMonitorDB.status.in_(["paused", "fused"]))
            .order_by(RealtimeMonitorDB.updated_at.desc(), RealtimeMonitorDB.created_at.desc())
            .limit(100)
            .all()
        )
        candidate_ids: list[str] = []
        for monitor in rows:
            if _should_try_qmt_auto_resume(strategy_db, monitor, now):
                candidate_ids.append(monitor.id)
                if len(candidate_ids) >= _QMT_AUTO_RESUME_MAX_CANDIDATES:
                    break
        strategy_db.commit()

        for monitor_id in candidate_ids:
            monitor = strategy_db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.id == monitor_id).first()
            if monitor is None:
                continue
            try:
                _try_auto_resume_qmt_monitor(strategy_db, main_db, monitor, _now_dt())
                strategy_db.commit()
            except Exception as exc:
                strategy_db.rollback()
                logger.exception("[realtime-monitor] auto resume failed monitor=%s", monitor_id)
                _runtime_log(f"QMT 自动恢复检查异常 monitor={monitor_id} error={exc}", level="ERROR")


def _try_auto_resume_qmt_monitor(
    strategy_db: Session,
    main_db: Session,
    monitor: RealtimeMonitorDB,
    now: datetime | None = None,
) -> bool:
    current_now = now or _now_dt()
    if not _should_try_qmt_auto_resume(strategy_db, monitor, current_now):
        return False
    recovered, message, overview = _probe_qmt_realtime_snapshot(main_db, monitor)
    if not recovered:
        _record_qmt_auto_resume_check(monitor, current_now, success=False, message=message)
        strategy_db.add(monitor)
        return False

    previous_status = str(monitor.status or "")
    reason = _qmt_auto_resume_reason(monitor) or "QMT 通讯恢复"
    monitor.status = "running"
    monitor.fused_reason = None
    _clear_fuse_guard(monitor)
    _record_qmt_auto_resume_check(monitor, current_now, success=True, message=message, clear_pending=True)
    monitor.updated_at = current_now
    strategy_db.add(monitor)
    _append_event(
        strategy_db,
        monitor,
        "monitor_auto_resumed",
        payload={
            "status": monitor.status,
            "previous_status": previous_status,
            "reason": reason,
            "message": message,
            "data_source": overview.get("data_source"),
            "fetched_at": overview.get("fetched_at"),
            "connection": _qmt_connection_resume_payload(overview),
        },
    )
    _runtime_log(f"QMT 通讯恢复，实时监控已自动恢复 monitor={monitor.id}")
    return True


def _should_try_qmt_auto_resume(db: Session, monitor: RealtimeMonitorDB, now: datetime) -> bool:
    if str(monitor.status or "") not in {"paused", "fused"}:
        return False
    state = dict(monitor.state_json or {})
    resume_state = dict(state.get("qmt_auto_resume") or {})
    if bool(resume_state.get("enabled")) and _is_qmt_interrupt_reason(resume_state.get("reason")):
        return _qmt_auto_resume_check_due(resume_state, now)

    reason = _monitor_interrupt_reason(monitor)
    if monitor.status == "fused" and _is_qmt_interrupt_reason(reason):
        _mark_qmt_auto_resume_pending(monitor, reason, now, source="fused_status")
        return True

    legacy_reason = _legacy_qmt_auto_resume_reason(db, monitor)
    if legacy_reason:
        _mark_qmt_auto_resume_pending(monitor, legacy_reason, now, source="legacy_events")
        return True
    return False


def _probe_qmt_realtime_snapshot(main_db: Session, monitor: RealtimeMonitorDB) -> tuple[bool, str, dict[str, Any]]:
    try:
        overview = qmt_virtual_account_service.get_qmt_virtual_account_overview(
            main_db,
            monitor.user_id,
            account_key=monitor.account_key,
            allow_cache_fallback=False,
        )
    except Exception as exc:
        return False, str(exc) or exc.__class__.__name__, {}

    connection = dict(overview.get("connection") or {})
    connected = bool(connection.get("connected"))
    is_stale = bool(overview.get("is_stale", True))
    data_source = str(overview.get("data_source") or "").strip()
    if connected and not is_stale and data_source not in {"", "cache", "cache_recent", "empty"}:
        return True, str(connection.get("message") or "QMT 实时快照已恢复"), overview
    message = str(connection.get("message") or "").strip()
    if not message:
        message = f"QMT 实时快照未恢复：connected={connected} stale={is_stale} data_source={data_source or '-'}"
    return False, message, overview


def _legacy_qmt_auto_resume_reason(db: Session, monitor: RealtimeMonitorDB) -> str | None:
    latest_lifecycle = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.monitor_id == monitor.id,
            RealtimeEventDB.event_type.in_(_QMT_LIFECYCLE_EVENT_TYPES),
        )
        .order_by(RealtimeEventDB.created_at.desc(), RealtimeEventDB.id.desc())
        .first()
    )
    if latest_lifecycle and latest_lifecycle.event_type in {"monitor_paused", "monitor_stopped"}:
        return None

    qmt_event = _latest_qmt_interrupt_event(db, monitor)
    if qmt_event is None:
        return None
    if monitor.status == "fused":
        return _event_reason(qmt_event)
    if latest_lifecycle and latest_lifecycle.event_type == "fuse_reset":
        latest_created = _ensure_utc(latest_lifecycle.created_at)
        qmt_created = _ensure_utc(qmt_event.created_at)
        if latest_created >= qmt_created:
            return _event_reason(qmt_event)
    return None


def _latest_qmt_interrupt_event(db: Session, monitor: RealtimeMonitorDB) -> RealtimeEventDB | None:
    rows = (
        db.query(RealtimeEventDB)
        .filter(
            RealtimeEventDB.monitor_id == monitor.id,
            RealtimeEventDB.event_type.in_(_QMT_INTERRUPT_EVENT_TYPES),
        )
        .order_by(RealtimeEventDB.created_at.desc(), RealtimeEventDB.id.desc())
        .limit(20)
        .all()
    )
    for row in rows:
        if _is_qmt_interrupt_reason(_event_reason(row)):
            return row
    return None


def _monitor_interrupt_reason(monitor: RealtimeMonitorDB) -> str:
    state = dict(monitor.state_json or {})
    last_interrupt = dict(state.get("last_interrupt") or {})
    return str(monitor.fused_reason or last_interrupt.get("reason") or "").strip()


def _event_reason(event: RealtimeEventDB | None) -> str:
    if event is None:
        return ""
    for payload in (event.error_payload, event.payload, event.risk_payload):
        if isinstance(payload, dict):
            reason = payload.get("reason") or payload.get("message") or payload.get("error")
            if reason:
                return str(reason)
    return ""


def _is_qmt_interrupt_reason(reason: Any) -> bool:
    text = str(reason or "").strip().lower()
    if not text:
        return False
    return any(
        keyword in text
        for keyword in (
            "qmt",
            "bridge",
            "xtquant",
            "xttrader",
            "账户实时快照",
            "交易接口异常",
            "快照不可用",
            "实时快照",
        )
    )


def _qmt_auto_resume_state(monitor: RealtimeMonitorDB) -> dict[str, Any]:
    state = dict(monitor.state_json or {})
    return dict(state.get("qmt_auto_resume") or {})


def _qmt_auto_resume_reason(monitor: RealtimeMonitorDB) -> str:
    return str(_qmt_auto_resume_state(monitor).get("reason") or _monitor_interrupt_reason(monitor) or "").strip()


def _qmt_auto_resume_check_due(resume_state: dict[str, Any], now: datetime) -> bool:
    last_checked_at = _parse_datetime_value(resume_state.get("last_checked_at"))
    if last_checked_at is None:
        return True
    elapsed = (_ensure_utc(now) - _ensure_utc(last_checked_at)).total_seconds()
    return elapsed >= _QMT_AUTO_RESUME_CHECK_SECONDS


def _mark_qmt_auto_resume_pending(monitor: RealtimeMonitorDB, reason: Any, now: datetime, *, source: str) -> None:
    state = dict(monitor.state_json or {})
    existing = dict(state.get("qmt_auto_resume") or {})
    state["qmt_auto_resume"] = {
        **existing,
        "enabled": True,
        "reason": str(reason or "QMT 通讯中断"),
        "source": source,
        "marked_at": existing.get("marked_at") or now.isoformat(),
        "attempts": int(existing.get("attempts") or 0),
    }
    monitor.state_json = _json_safe(state)


def _record_qmt_auto_resume_check(
    monitor: RealtimeMonitorDB,
    now: datetime,
    *,
    success: bool,
    message: str,
    clear_pending: bool = False,
) -> None:
    state = dict(monitor.state_json or {})
    resume_state = dict(state.get("qmt_auto_resume") or {})
    attempts = int(resume_state.get("attempts") or 0) + 1
    if clear_pending:
        state.pop("qmt_auto_resume", None)
        state["last_qmt_auto_resume"] = {
            "success": success,
            "message": str(message or ""),
            "attempts": attempts,
            "at": now.isoformat(),
        }
    else:
        resume_state.update(
            {
                "enabled": True,
                "attempts": attempts,
                "last_checked_at": now.isoformat(),
                "last_success": success,
                "last_message": str(message or ""),
            }
        )
        state["qmt_auto_resume"] = resume_state
    monitor.state_json = _json_safe(state)


def _clear_qmt_auto_resume_state(monitor: RealtimeMonitorDB) -> None:
    state = dict(monitor.state_json or {})
    if "qmt_auto_resume" not in state:
        return
    state.pop("qmt_auto_resume", None)
    monitor.state_json = _json_safe(state)


def _qmt_connection_resume_payload(overview: dict[str, Any]) -> dict[str, Any]:
    connection = dict(overview.get("connection") or {})
    return {
        "account_key": connection.get("account_key"),
        "role": connection.get("role"),
        "connected": bool(connection.get("connected")),
        "bridge_base_url": connection.get("bridge_base_url"),
        "account_id": connection.get("account_id"),
        "message": connection.get("message"),
    }


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

        archive_obsolete_pending_approvals(
            strategy_db,
            user_id=monitor.user_id,
            account_key=monitor.account_key,
        )
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
            _fuse_monitor(strategy_db, monitor, "QMT/实时行情不可用，已跳过本轮，等待自动恢复")
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

        routes = _configured_signal_routes(monitor)
        if routes:
            raw_signals, route_clock_key = _generate_signal_route_signals(
                strategy_db,
                main_db,
                monitor,
                default_strategy=strategy,
                overview=overview,
                quotes=quotes,
                symbols=symbols,
                minute_capture=minute_capture,
                current_time=local_now,
                cycle_id=cycle_id,
            )
            bar_clock_key = route_clock_key
            signal_clock_meta = {
                "timeframe": "multi-route",
                "latest_closed_bar_end": route_clock_key,
                "route_bar_clock_key": route_clock_key,
            }
        else:
            minute_features = _build_minute_features(
                monitor,
                symbols,
                minute_capture=minute_capture,
                current_time=local_now,
                main_db=main_db,
                user_id=monitor.user_id,
            )
            _append_minute_features_event(strategy_db, monitor, minute_features, cycle_id=cycle_id)
            bar_clock_key = _signal_bar_clock_key(monitor, minute_features)
            raw_signals = _generate_signals(monitor, strategy, overview, quotes, minute_features)
            signal_clock_meta = minute_features

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
                        "timeframe": signal_clock_meta.get("timeframe"),
                        "latest_closed_bar_end": signal_clock_meta.get("latest_closed_bar_end"),
                    },
                    correlation_id=cycle_id,
                )
            _update_state_stats(monitor, latest_cycle=cycle_id)
            strategy_db.add(monitor)
            strategy_db.commit()
            return
        _mark_signal_bar_evaluated(monitor, bar_clock_key, signal_clock_meta)

        max_signals = max(int((monitor.config_json or {}).get("max_signals_per_cycle") or 3), 1)
        raw_signals = _resolve_signal_route_conflicts(raw_signals)
        signals, suppressed_signals = _filter_actionable_signals(strategy_db, monitor, raw_signals, limit=max_signals)
        if not signals:
            if suppressed_signals:
                _mark_signal_suppression_seen(monitor, suppressed_signals)
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

            if str(signal.get("execution_action") or "").strip().lower() == "notify_only":
                _append_event(
                    strategy_db,
                    monitor,
                    "signal_notified",
                    symbol=signal["symbol"],
                    signal_payload=signal,
                    payload={
                        "message": "路线配置为只提醒，已记录信号，不自动下单",
                        "route_id": signal.get("route_id"),
                        "signal_id": signal.get("signal_id"),
                    },
                    request_id=signal_key,
                    correlation_id=cycle_id,
                )
                _update_signal_execution(strategy_db, monitor, signal, "notified")
                continue

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
                _register_pending_order(monitor, broker_result, intent, signal_payload=signal, risk_payload=risk)
                _append_broker_followup_events(
                    strategy_db,
                    monitor,
                    signal["symbol"],
                    broker_result,
                    signal_payload=signal,
                    risk_payload=risk,
                    order_payload=intent,
                    correlation_id=cycle_id,
                )

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
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_now = current_time or datetime.now().astimezone()
    now_local = raw_now.astimezone().replace(tzinfo=None) if raw_now.tzinfo else raw_now.replace(tzinfo=None)
    trade_date = now_local.date().isoformat()
    config = dict(monitor.config_json or {})
    config.update(runtime_config or {})
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


def _append_minute_features_event(
    db: Session,
    monitor: RealtimeMonitorDB,
    minute_features: dict[str, Any],
    *,
    cycle_id: str,
    route_ids: list[str] | None = None,
) -> None:
    _append_event(
        db,
        monitor,
        "minute_features",
        payload={
            "cycle_id": cycle_id,
            "route_ids": route_ids or [],
            "source": minute_features.get("source"),
            "timeframe": minute_features.get("timeframe"),
            "signal_mode": minute_features.get("signal_mode"),
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


def _configured_signal_routes(monitor: RealtimeMonitorDB) -> list[dict[str, Any]]:
    config = monitor.config_json if isinstance(monitor.config_json, dict) else {}
    routes = config.get("signal_routes")
    if not isinstance(routes, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, raw_route in enumerate(routes):
        if not isinstance(raw_route, dict):
            continue
        route = dict(raw_route)
        if route.get("enabled") is False:
            continue
        route_id = str(route.get("id") or f"route_{index + 1}").strip() or f"route_{index + 1}"
        side = _route_side(route)
        timeframe = _normalize_route_timeframe(route.get("timeframe") or route.get("period") or "30m")
        route["id"] = route_id
        route["side"] = side
        route["timeframe"] = timeframe
        route["action"] = _normalize_route_action(route.get("action"), side, timeframe)
        route["require_approval"] = False
        route["priority"] = int(float(route.get("priority") or _default_route_priority(side, timeframe)))
        normalized.append(route)
    return normalized


def _generate_signal_route_signals(
    strategy_db: Session,
    main_db: Session,
    monitor: RealtimeMonitorDB,
    *,
    default_strategy: dict[str, Any],
    overview: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    symbols: list[str],
    minute_capture: dict[str, Any] | None,
    current_time: datetime,
    cycle_id: str,
) -> tuple[list[dict[str, Any]], str | None]:
    routes = _configured_signal_routes(monitor)
    if not routes:
        return [], None

    strategy_cache: dict[str, tuple[dict[str, Any], Any]] = {}
    minute_feature_cache: dict[tuple[str, str], dict[str, Any]] = {}
    daily_feature_cache: dict[str, tuple[pd.DataFrame, str]] = {}
    signals: list[dict[str, Any]] = _risk_stop_loss_signals(monitor, default_strategy, overview, quotes)
    clock_parts: list[str] = []

    for route in routes:
        try:
            route_strategy, compiled = _route_strategy(strategy_db, route, default_strategy, strategy_cache)
            side = _route_side(route)
            timeframe = _normalize_route_timeframe(route.get("timeframe"))
            rules = _selected_route_rules(compiled, route, side)
            if not rules:
                _append_event(
                    strategy_db,
                    monitor,
                    "signal_evaluation_skipped",
                    payload={
                        "cycle_id": cycle_id,
                        "route_id": route.get("id"),
                        "reason": "route_has_no_compiled_rule",
                        "side": side,
                        "timeframe": timeframe,
                    },
                    correlation_id=cycle_id,
                )
                continue

            if _is_daily_timeframe(timeframe):
                route_signals, daily_clock = _generate_daily_route_signals(
                    main_db,
                    monitor,
                    route,
                    route_strategy,
                    compiled,
                    overview,
                    quotes,
                    symbols,
                    current_time=current_time,
                    daily_feature_cache=daily_feature_cache,
                )
                if daily_clock:
                    clock_parts.append(daily_clock)
                signals.extend(route_signals)
                continue

            signal_mode = _route_signal_mode(route, rules)
            cache_key = (timeframe, signal_mode)
            minute_features = minute_feature_cache.get(cache_key)
            if minute_features is None:
                minute_features = _build_minute_features(
                    monitor,
                    symbols,
                    minute_capture=minute_capture,
                    current_time=current_time,
                    main_db=main_db,
                    user_id=monitor.user_id,
                    runtime_config={"signal_timeframe": timeframe, "signal_mode": signal_mode},
                )
                minute_feature_cache[cache_key] = minute_features
                related_route_ids = [
                    str(item.get("id"))
                    for item in routes
                    if _normalize_route_timeframe(item.get("timeframe")) == timeframe
                    and str(item.get("enabled", True)).lower() != "false"
                ]
                _append_minute_features_event(strategy_db, monitor, minute_features, cycle_id=cycle_id, route_ids=related_route_ids)
            clock_key = _signal_bar_clock_key_for_config(monitor, minute_features, signal_mode=signal_mode, timeframe=timeframe)
            if clock_key:
                clock_parts.append(f"{route.get('id')}:{clock_key}")
            signals.extend(
                _generate_minute_route_signals(
                    monitor,
                    route,
                    route_strategy,
                    quotes,
                    minute_features,
                    rules=rules,
                    signal_mode=signal_mode,
                )
            )
        except Exception as exc:
            logger.exception("[realtime-monitor] route evaluation failed monitor=%s route=%s", monitor.id, route.get("id"))
            _append_event(
                strategy_db,
                monitor,
                "signal_evaluation_skipped",
                payload={
                    "cycle_id": cycle_id,
                    "route_id": route.get("id"),
                    "reason": "route_evaluation_error",
                    "error": str(exc),
                },
                correlation_id=cycle_id,
            )

    return signals, "|".join(sorted(set(clock_parts))) if clock_parts else None


def _route_strategy(
    strategy_db: Session,
    route: dict[str, Any],
    default_strategy: dict[str, Any],
    cache: dict[str, tuple[dict[str, Any], Any]],
) -> tuple[dict[str, Any], Any]:
    strategy_id = str(route.get("strategy_id") or default_strategy.get("id") or "").strip()
    if not strategy_id:
        strategy_id = str(default_strategy.get("id") or "")
    if strategy_id in cache:
        return cache[strategy_id]
    strategy = default_strategy if strategy_id == str(default_strategy.get("id") or "") else _require_strategy(strategy_db, strategy_id)
    compiled = _compile_strategy_payload(strategy)
    if compiled.status != "passed":
        errors = "；".join(compiled.errors[:3]) or "未知编译错误"
        raise ValueError(f"路线策略编译失败：{errors}")
    cache[strategy_id] = (strategy, compiled)
    return strategy, compiled


def _selected_route_rules(compiled: Any, route: dict[str, Any], side: str | None = None) -> list[dict[str, Any]]:
    route_side = side or _route_side(route)
    rules = list(compiled.entry_rules if route_side == "buy" else compiled.exit_rules)
    if not rules:
        return []
    signal_id = str(route.get("signal_id") or "")
    match = _SIGNAL_RULE_INDEX_RE.search(signal_id)
    if not match:
        return rules
    index = int(match.group(1)) - 1
    if index < 0 or index >= len(rules):
        raise ValueError(f"选择的买卖点不存在：{route.get('signal_name') or signal_id}")
    return [rules[index]]


def _generate_minute_route_signals(
    monitor: RealtimeMonitorDB,
    route: dict[str, Any],
    strategy: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    minute_features: dict[str, Any],
    *,
    rules: list[dict[str, Any]],
    signal_mode: str,
) -> list[dict[str, Any]]:
    side = _route_side(route)
    timeframe = _normalize_route_timeframe(route.get("timeframe") or minute_features.get("timeframe"))
    current_bar_end = str(minute_features.get("latest_closed_bar_end") or "")
    signals: list[dict[str, Any]] = []
    for item in minute_features.get("items") or []:
        if not isinstance(item, dict):
            continue
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol:
            continue
        if not _minute_item_matches_route(item, side, signal_mode, rules):
            continue
        quote = quotes.get(symbol) or {}
        price = _to_float(quote.get("price"), item.get("close"), quote.get("close"))
        if not price:
            continue
        signals.append(
            _route_signal_payload(
                monitor,
                route,
                strategy,
                symbol=symbol,
                side=side,
                price=price,
                source=f"{signal_mode}_route",
                reason=_route_signal_reason(route, signal_mode, side),
                timeframe=timeframe,
                bar_end=item.get("bar_end") or current_bar_end,
                bar_start=item.get("bar_start"),
                extra={
                    "minute_source": minute_features.get("source"),
                    "signal_condition": route.get("signal_condition"),
                },
            )
        )
    return signals


def _generate_daily_route_signals(
    main_db: Session,
    monitor: RealtimeMonitorDB,
    route: dict[str, Any],
    strategy: dict[str, Any],
    compiled: Any,
    overview: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
    symbols: list[str],
    *,
    current_time: datetime,
    daily_feature_cache: dict[str, tuple[pd.DataFrame, str]],
) -> tuple[list[dict[str, Any]], str | None]:
    side = _route_side(route)
    rules = _selected_route_rules(compiled, route, side)
    if not rules:
        return [], None
    strategy_id = str(strategy.get("id") or "")
    cached = daily_feature_cache.get(strategy_id)
    if cached is None:
        rows = _load_recent_daily_rows_for_symbols(main_db, symbols, current_time=current_time)
        if not rows:
            daily_feature_cache[strategy_id] = (pd.DataFrame(), "empty")
        else:
            frame = pd.DataFrame.from_records(rows)
            _coerce_daily_feature_frame(frame)
            features, backend = compute_daily_features(frame, compiled)
            daily_feature_cache[strategy_id] = (features, backend)
    features, backend = daily_feature_cache.get(strategy_id, (pd.DataFrame(), "empty"))
    if features.empty:
        return [], None
    features = features.copy()
    features["symbol"] = features["symbol"].astype(str).str.upper()
    features["date"] = pd.to_datetime(features["date"])
    logic = _route_rules_logic(strategy, side, single_rule=len(rules) == 1)
    match_mask = _evaluate_daily_route_rules(features, rules, side=side, logic=logic)
    latest = (
        features.assign(_route_signal_match=match_mask.fillna(False))
        .sort_values(["symbol", "date"])
        .groupby("symbol", as_index=False, group_keys=False)
        .tail(1)
    )
    matched = latest[latest["_route_signal_match"]]
    signals: list[dict[str, Any]] = []
    positions = {item.get("symbol"): item for item in (overview.get("positions") or []) if item.get("symbol")}
    for _, row in matched.iterrows():
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        if side == "sell" and symbol not in positions:
            continue
        quote = quotes.get(symbol) or {}
        price = _to_float(quote.get("price"), row.get("close"), quote.get("close"))
        if not price:
            continue
        signals.append(
            _route_signal_payload(
                monitor,
                route,
                strategy,
                symbol=symbol,
                side=side,
                price=price,
                source="daily_dsl_route",
                reason=_route_signal_reason(route, "daily_dsl", side),
                timeframe="1d",
                bar_end=pd.Timestamp(row.get("date")).date().isoformat(),
                extra={
                    "daily_backend": backend,
                    "factor_score": _to_float(row.get("factor_score")),
                    "signal_condition": route.get("signal_condition"),
                },
            )
        )
    latest_date = None
    if not latest.empty:
        latest_date = pd.Timestamp(latest["date"].max()).date().isoformat()
    return signals, f"{route.get('id')}:daily_dsl:1d:{latest_date or 'unknown'}"


def _risk_stop_loss_signals(
    monitor: RealtimeMonitorDB,
    strategy: dict[str, Any],
    overview: dict[str, Any],
    quotes: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    positions = {item.get("symbol"): item for item in (overview.get("positions") or []) if item.get("symbol")}
    signals: list[dict[str, Any]] = []
    risk = dict(monitor.risk_config_json or {})
    stop_loss_pct = float(risk.get("stop_loss_pct") or 0.0)
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
                    "priority": 10_000,
                    "execution_action": "clear_position",
                }
            )
    return signals


def _minute_item_matches_route(
    item: dict[str, Any],
    side: str,
    signal_mode: str,
    rules: list[dict[str, Any]],
) -> bool:
    if signal_mode == "first_day_band":
        item_signal = str(item.get("signal") or "").lower()
        if side == "buy":
            return item_signal == "buy" or bool(item.get("cross_above"))
        return item_signal == "sell" or bool(item.get("cross_below"))
    if side == "buy":
        return bool(item.get("confirmed")) is True
    if any(_is_first_day_band_rule(rule) for rule in rules):
        return bool(item.get("cross_below")) or str(item.get("signal") or "").lower() == "sell"
    return False


def _route_signal_payload(
    monitor: RealtimeMonitorDB,
    route: dict[str, Any],
    strategy: dict[str, Any],
    *,
    symbol: str,
    side: str,
    price: float,
    source: str,
    reason: str,
    timeframe: str,
    bar_end: Any,
    bar_start: Any = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    action = _normalize_route_action(route.get("action"), side, timeframe)
    position_pct = _route_position_pct(route)
    trade_amount = _route_trade_amount(route)
    share_quantity = _route_share_quantity(route)
    payload = {
        "symbol": symbol,
        "side": side,
        "price": price,
        "reason": reason,
        "target_position_pct": _target_position_pct(strategy) if side == "buy" else 0.0,
        "buy_cash_pct": position_pct if side == "buy" and action == "buy_or_add" else None,
        "sell_position_pct": position_pct if side == "sell" and action == "reduce_position" else None,
        "trade_amount": trade_amount if side == "buy" and action == "buy_amount" else None,
        "share_quantity": share_quantity if action in {"buy_quantity", "sell_quantity"} else None,
        "strategy_id": str(route.get("strategy_id") or strategy.get("id") or monitor.strategy_id),
        "strategy_version_id": route.get("strategy_version_id") or strategy.get("current_version_id") or monitor.strategy_version_id,
        "source": source,
        "timeframe": timeframe,
        "bar_end": str(bar_end or ""),
        "bar_start": bar_start,
        "route_id": route.get("id"),
        "signal_id": route.get("signal_id"),
        "signal_name": route.get("signal_name") or route.get("side_label"),
        "execution_action": action,
        "require_approval": False,
        "priority": int(float(route.get("priority") or _default_route_priority(side, timeframe))),
    }
    if side == "buy" and action == "buy_or_add" and position_pct is not None:
        payload["target_position_pct"] = position_pct
        payload["buy_cash_pct"] = position_pct
    if action == "clear_position":
        payload["target_position_pct"] = 0.0
        payload["sell_position_pct"] = 1.0
    payload.update(extra or {})
    return _json_safe(payload)


def _resolve_signal_route_conflicts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not signals:
        return []
    grouped: dict[str, list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []
    for signal in signals:
        symbol = _normalize_symbol(signal.get("symbol"))
        if not symbol:
            continue
        if str(signal.get("source") or "") == "risk_stop_loss":
            passthrough.append(signal)
            continue
        grouped.setdefault(symbol, []).append(signal)

    resolved = list(passthrough)
    for symbol_signals in grouped.values():
        if len(symbol_signals) == 1:
            resolved.extend(symbol_signals)
            continue
        resolved.append(max(symbol_signals, key=_signal_conflict_score))
    return sorted(resolved, key=lambda item: (-_signal_conflict_score(item), str(item.get("symbol") or "")))


def _signal_conflict_score(signal: dict[str, Any]) -> int:
    action = str(signal.get("execution_action") or "")
    side = str(signal.get("side") or "")
    timeframe = str(signal.get("timeframe") or "")
    score = int(float(signal.get("priority") or 0))
    if action == "clear_position":
        score += 10_000
    elif side == "sell":
        score += 5_000
    elif side == "buy":
        score += 1_000
    if timeframe in {"1d", "daily"}:
        score += 500
    return score


def _load_recent_daily_rows_for_symbols(
    db: Session,
    symbols: list[str],
    *,
    current_time: datetime,
) -> list[dict[str, Any]]:
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)})
    if not normalized_symbols:
        return []
    start_date = (current_time.date() - timedelta(days=420)).isoformat()
    statement = (
        text(
            """
            SELECT
                symbol,
                trade_date AS date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                turnover_rate,
                pre_close,
                float_market_cap,
                total_market_cap,
                NULL AS net_profit_ttm
            FROM stock_daily_kline
            WHERE symbol IN :symbols
              AND trade_date >= :start_date
            ORDER BY symbol, trade_date
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )
    try:
        return [
            dict(row)
            for row in db.execute(statement, {"symbols": normalized_symbols, "start_date": start_date}).mappings().all()
        ]
    except Exception as exc:
        logger.warning("[realtime-monitor] load daily rows failed: %s", exc)
        return []


def _coerce_daily_feature_frame(frame: pd.DataFrame) -> None:
    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover_rate",
        "pre_close",
        "float_market_cap",
        "total_market_cap",
        "net_profit_ttm",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")


def _evaluate_daily_route_rules(
    frame: pd.DataFrame,
    rules: list[dict[str, Any]],
    *,
    side: str,
    logic: str,
) -> pd.Series:
    masks = [_evaluate_daily_route_rule(frame, rule, side=side) for rule in rules]
    if not masks:
        return pd.Series(False, index=frame.index)
    result = masks[0]
    for mask in masks[1:]:
        result = (result | mask) if logic == "any" else (result & mask)
    return result.fillna(False)


def _evaluate_daily_route_rule(frame: pd.DataFrame, rule: dict[str, Any], *, side: str) -> pd.Series:
    rule_key = str(rule.get("rule_key") or "")
    params = rule.get("params") or {}
    if rule_key == "close_above_indicator":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or params.get("indicator") or params.get("field") or "ma20")
        op = str(params.get("op") or "above")
        return _numeric_route_column(frame, left) < _numeric_route_column(frame, right) if op == "below" else _numeric_route_column(frame, left) > _numeric_route_column(frame, right)
    if rule_key == "alligator_proxy":
        return _numeric_route_column(frame, "ma5") >= _numeric_route_column(frame, "ma20")
    if rule_key == "cross_above":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or "ma5")
        if left == "first_day_band" and right == "first_day_band_b1" and "first_day_band_cross" in frame.columns:
            return _numeric_route_column(frame, "first_day_band_cross") > 0
        left_values = _numeric_route_column(frame, left)
        right_values = _numeric_route_column(frame, right)
        return (left_values > right_values) & (_previous_route_value(frame, left_values) <= _previous_route_value(frame, right_values))
    if rule_key == "close_below_indicator":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or params.get("indicator") or "ma20")
        if left == "first_day_band" and right == "first_day_band_b1" and "first_day_band_dead_cross" in frame.columns:
            return _numeric_route_column(frame, "first_day_band_dead_cross") > 0
        return _numeric_route_column(frame, left) < _numeric_route_column(frame, right)
    if rule_key == "factor_rank_drop":
        rank_below = float(params.get("rank_below") or 0.5)
        return _numeric_route_column(frame, "factor_score") < rank_below
    if side == "sell" and rule_key == "atr_trailing_stop":
        return pd.Series(False, index=frame.index)
    return pd.Series(False, index=frame.index)


def _numeric_route_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(float("nan"), index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _previous_route_value(frame: pd.DataFrame, values: pd.Series) -> pd.Series:
    return values.groupby(frame["symbol"]).shift(1)


def _route_rules_logic(strategy: dict[str, Any], side: str, *, single_rule: bool) -> str:
    if single_rule:
        return "all"
    dsl = (strategy.get("current_version") or {}).get("dsl") or {}
    branch = dsl.get("entry" if side == "buy" else "exit") or {}
    return str(branch.get("logic") or ("all" if side == "buy" else "any")).lower()


def _route_signal_mode(route: dict[str, Any], rules: list[dict[str, Any]]) -> str:
    explicit = str(route.get("signal_mode") or "").strip().lower()
    if explicit:
        return explicit
    if any(_is_first_day_band_rule(rule) for rule in rules):
        return "first_day_band"
    if any(str(rule.get("rule_key") or "") == "lazy_minute_confirm" for rule in rules):
        return "intraday_confirmation"
    condition = f"{route.get('signal_condition') or ''} {route.get('signal_name') or ''}".lower()
    if "first_day_band" in condition or "首日" in condition:
        return "first_day_band"
    return "intraday_confirmation"


def _is_first_day_band_rule(rule: dict[str, Any]) -> bool:
    params = rule.get("params") or {}
    values = " ".join(str(params.get(key) or "") for key in ("left", "right", "field", "indicator"))
    return "first_day_band" in values


def _route_signal_reason(route: dict[str, Any], signal_mode: str, side: str) -> str:
    signal_name = str(route.get("signal_name") or ("买点" if side == "buy" else "卖点")).strip()
    timeframe = _normalize_route_timeframe(route.get("timeframe"))
    return f"{signal_mode}_{timeframe}_{signal_name}"


def _route_side(route: dict[str, Any]) -> str:
    text = f"{route.get('side') or ''} {route.get('side_label') or ''} {route.get('signal_name') or ''}".lower()
    if "sell" in text or "卖" in text or "exit" in text:
        return "sell"
    return "buy"


def _normalize_route_timeframe(value: Any) -> str:
    text_value = str(value or "30m").strip()
    mapping = {
        "5分钟": "5m",
        "15分钟": "15m",
        "30分钟": "30m",
        "60分钟": "60m",
        "日K": "1d",
        "daily": "1d",
        "day": "1d",
        "1D": "1d",
    }
    return mapping.get(text_value, text_value.lower() or "30m")


def _is_daily_timeframe(timeframe: str) -> bool:
    return _normalize_route_timeframe(timeframe) in {"1d", "daily", "day"}


def _default_route_action(side: str, timeframe: str) -> str:
    if side == "buy":
        return "buy_or_add"
    return "clear_position" if _is_daily_timeframe(timeframe) else "reduce_position"


def _normalize_route_action(value: Any, side: str, timeframe: str) -> str:
    action = str(value or "").strip().lower()
    allowed_by_side = {
        "buy": {"buy_or_add", "buy_amount", "buy_quantity", "notify_only"},
        "sell": {"reduce_position", "sell_quantity", "clear_position", "notify_only"},
    }
    if action in allowed_by_side.get(side, set()):
        return action
    return _default_route_action(side, timeframe)


def _default_route_priority(side: str, timeframe: str) -> int:
    if side == "sell" and _is_daily_timeframe(timeframe):
        return 100
    return 60 if side == "sell" else 20


def _route_position_pct(route: dict[str, Any]) -> float | None:
    raw = route.get("position_pct")
    if raw in (None, ""):
        return None
    value = _to_float(raw)
    if value is None or value <= 0:
        return None
    return value / 100 if value > 1 else value


def _route_trade_amount(route: dict[str, Any]) -> float | None:
    value = _to_float(route.get("trade_amount"), route.get("buy_amount"), route.get("amount"))
    if value is None or value <= 0:
        return None
    return value


def _route_share_quantity(route: dict[str, Any]) -> int | None:
    value = _to_float(route.get("share_quantity"), route.get("quantity"), route.get("shares"))
    if value is None or value <= 0:
        return None
    return int(value)


def _round_lot_quantity(value: float, lot_size: int) -> int:
    return int(max(value, 0) // lot_size) * lot_size


def _signal_bar_clock_key_for_config(
    monitor: RealtimeMonitorDB,
    minute_features: dict[str, Any],
    *,
    signal_mode: str,
    timeframe: str,
) -> str | None:
    latest_closed_bar_end = str(minute_features.get("latest_closed_bar_end") or "").strip()
    if not latest_closed_bar_end:
        return None
    return f"{signal_mode}:{timeframe}:{latest_closed_bar_end}"


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
        strategy_id=str(identity.get("strategy_id") or monitor.strategy_id or ""),
        strategy_version_id=str(identity.get("strategy_version_id") or monitor.strategy_version_id or ""),
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
            strategy_id=str(identity.get("strategy_id") or monitor.strategy_id or ""),
            strategy_version_id=str(identity.get("strategy_version_id") or monitor.strategy_version_id or ""),
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
    strategy_id = str(signal.get("strategy_id") or monitor.strategy_id or "").strip()
    strategy_version_id = str(signal.get("strategy_version_id") or monitor.strategy_version_id or "").strip()
    return {
        "monitor_id": monitor.id,
        "strategy_id": strategy_id,
        "strategy_version_id": strategy_version_id,
        "account_key": monitor.account_key,
        "symbol": _normalize_symbol(signal.get("symbol")),
        "side": str(signal.get("side") or "").strip().lower(),
        "source": str(signal.get("source") or "").strip().lower(),
        "reason": str(signal.get("reason") or "").strip().lower(),
        "timeframe": timeframe,
        "bar_end": bar_end,
        "route_id": str(signal.get("route_id") or "").strip(),
        "signal_id": str(signal.get("signal_id") or "").strip(),
        "execution_action": str(signal.get("execution_action") or "").strip().lower(),
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
    execution_action = str(signal.get("execution_action") or "").strip().lower()
    lot_size = _monitor_lot_size(monitor)
    reentry_anchor_quantity = None
    position = positions.get(symbol) or {}
    available_position = float(position.get("available_position") or 0.0)
    current_position = float(position.get("current_position") or 0.0)
    if side == "sell":
        fixed_quantity = _to_float(signal.get("share_quantity"), signal.get("quantity")) if execution_action == "sell_quantity" else None
        if fixed_quantity is not None and fixed_quantity > 0:
            quantity_basis = min(float(fixed_quantity), available_position)
        else:
            sell_pct = _to_float(signal.get("sell_position_pct"))
            if sell_pct is not None and sell_pct > 1:
                sell_pct = sell_pct / 100
            if execution_action == "reduce_position" and sell_pct is not None:
                sell_pct = max(min(sell_pct, 1.0), 0.0)
                quantity_basis = available_position * sell_pct
            else:
                quantity_basis = available_position
        quantity = _round_lot_quantity(quantity_basis, lot_size)
    else:
        total_asset = float(account.get("total_asset") or account.get("available_cash") or 0.0)
        available_cash = float(account.get("available_cash") or account.get("cash") or total_asset)
        cash_buffer_pct = _monitor_buy_cash_buffer_pct(monitor)
        price_buffer_pct = _monitor_buy_price_buffer_pct(monitor)
        effective_price = max(price * (1 + price_buffer_pct), 0.01)
        fixed_quantity = _to_float(signal.get("share_quantity"), signal.get("quantity")) if execution_action == "buy_quantity" else None
        fixed_amount = _to_float(signal.get("trade_amount"), signal.get("buy_amount")) if execution_action == "buy_amount" else None
        if fixed_quantity is not None and fixed_quantity > 0:
            affordable_quantity = _round_lot_quantity(available_cash / effective_price, lot_size)
            quantity = min(_round_lot_quantity(fixed_quantity, lot_size), affordable_quantity)
            sizing = {
                "mode": "fixed_share_quantity",
                "requested_quantity": int(fixed_quantity),
                "affordable_quantity": affordable_quantity,
                "available_cash": round(available_cash, 2),
                "reference_price": round(price, 4),
                "effective_price": round(effective_price, 4),
                "price_buffer_pct": price_buffer_pct,
            }
        elif fixed_amount is not None and fixed_amount > 0:
            cash_budget = min(max(fixed_amount, 0.0), available_cash)
            effective_cash = max(cash_budget * (1 - cash_buffer_pct), 0.0)
            quantity = _round_lot_quantity(effective_cash / effective_price, lot_size)
            sizing = {
                "mode": "fixed_trade_amount",
                "trade_amount": round(fixed_amount, 2),
                "available_cash": round(available_cash, 2),
                "effective_cash": round(effective_cash, 2),
                "reference_price": round(price, 4),
                "effective_price": round(effective_price, 4),
                "cash_buffer_pct": cash_buffer_pct,
                "price_buffer_pct": price_buffer_pct,
            }
        else:
            reentry_anchor_quantity = _resolve_reentry_buy_quantity(
                monitor,
                overview,
                symbol=symbol,
                price=price,
                lot_size=lot_size,
            )
            if reentry_anchor_quantity is not None:
                quantity = reentry_anchor_quantity
                sizing = {
                    "mode": "reentry_anchor",
                    "available_cash": round(available_cash, 2),
                    "reference_price": round(price, 4),
                }
            else:
                buy_cash_pct = _normalize_pct(signal.get("buy_cash_pct"), signal.get("target_position_pct"), default=0.02)
                cash_budget = max(available_cash, 0.0) * buy_cash_pct
                max_position_pct = _monitor_max_single_position_pct(monitor)
                current_market_value = _to_float(
                    position.get("market_value"),
                    float(position.get("current_position") or 0.0) * price if price > 0 else None,
                ) or 0.0
                max_position_cash = max(total_asset * max_position_pct - current_market_value, 0.0) if max_position_pct > 0 else cash_budget
                effective_cash = max(min(cash_budget, max_position_cash, available_cash) * (1 - cash_buffer_pct), 0.0)
                quantity = _round_lot_quantity(effective_cash / effective_price, lot_size)
                sizing = {
                    "mode": "available_cash_pct",
                    "buy_cash_pct": buy_cash_pct,
                    "max_single_position_pct": max_position_pct,
                    "available_cash": round(available_cash, 2),
                    "cash_budget": round(cash_budget, 2),
                    "max_position_cash": round(max_position_cash, 2),
                    "effective_cash": round(effective_cash, 2),
                    "reference_price": round(price, 4),
                    "effective_price": round(effective_price, 4),
                    "cash_buffer_pct": cash_buffer_pct,
                    "price_buffer_pct": price_buffer_pct,
                    "current_market_value": round(current_market_value, 2),
                }
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
        "signal_route_id": signal.get("route_id"),
        "signal_id": signal.get("signal_id"),
        "signal_name": signal.get("signal_name"),
        "signal_source": signal.get("source"),
        "signal_reason": signal.get("reason"),
        "execution_action": execution_action or None,
        "strategy_id": signal.get("strategy_id") or monitor.strategy_id,
        "strategy_version_id": signal.get("strategy_version_id") or monitor.strategy_version_id,
        "sell_position_pct": signal.get("sell_position_pct"),
        "trade_amount": signal.get("trade_amount"),
        "share_quantity": signal.get("share_quantity"),
    }
    if side == "buy":
        intent["sizing"] = _json_safe(sizing)
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


def _execute_order_intent(db: Session, main_db: Session, monitor: RealtimeMonitorDB, intent: dict[str, Any], *, reason: str) -> dict[str, Any]:
    if monitor.account_role == "live" and not monitor.live_trading_enabled:
        return {"success": False, "error": "live_readonly_not_whitelisted"}
    if not monitor.auto_trade_enabled:
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
        return {
            "success": False,
            "error": f"QMT 交易接口异常：{exc}",
            "recoverable": True,
            "interrupted": False,
        }


def _fuse_monitor(db: Session, monitor: RealtimeMonitorDB, reason: str) -> None:
    now = _now_dt()
    should_emit_event = _mark_fuse_event_allowed(monitor, reason, now)
    state = dict(monitor.state_json or {})
    state["last_interrupt"] = {"reason": str(reason), "at": now.isoformat()}
    monitor.state_json = state
    if monitor.status == "fused":
        monitor.status = "running"
    monitor.fused_reason = None
    monitor.updated_at = now
    db.add(monitor)
    if should_emit_event:
        _append_event(
            db,
            monitor,
            "monitor_interrupted",
            payload={"status": monitor.status, "auto_recover": True},
            error_payload={"reason": reason},
        )
    db.commit()
    _runtime_log(f"监控实例本轮中断，保持自动恢复 monitor={monitor.id} reason={reason}", level="ERROR")


def _mark_fuse_event_allowed(monitor: RealtimeMonitorDB, reason: str, now: datetime) -> bool:
    state = dict(monitor.state_json or {})
    guard = dict(state.get("fuse_guard") or {})
    last_reason = str(guard.get("last_reason") or "")
    last_event_at = _parse_datetime_value(guard.get("last_event_at"))
    if (
        last_reason == str(reason)
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
    signal_payload: dict[str, Any] | None = None,
    risk_payload: dict[str, Any] | None = None,
    order_payload: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> None:
    latest_order = broker_result.get("latest_order")
    if isinstance(latest_order, dict) and latest_order:
        _append_event(
            db,
            monitor,
            "order_snapshot_refreshed",
            symbol=symbol,
            signal_payload=signal_payload,
            risk_payload=risk_payload,
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
            signal_payload=signal_payload,
            risk_payload=risk_payload,
            order_payload=order_payload,
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


def _signal_payload_from_order_intent(intent: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(intent, dict) or not intent:
        return {}
    symbol = _normalize_symbol(intent.get("symbol"))
    side = str(intent.get("side") or "").strip().lower()
    if not symbol or not side:
        return {}
    sizing = intent.get("sizing") if isinstance(intent.get("sizing"), dict) else {}
    payload = {
        "symbol": symbol,
        "side": side,
        "price": intent.get("reference_price") or intent.get("price"),
        "reason": intent.get("signal_reason") or intent.get("order_remark"),
        "signal_key": intent.get("signal_key"),
        "timeframe": intent.get("signal_timeframe"),
        "bar_end": intent.get("signal_bar_end"),
        "route_id": intent.get("signal_route_id"),
        "signal_id": intent.get("signal_id"),
        "signal_name": intent.get("signal_name") or intent.get("order_remark"),
        "execution_action": intent.get("execution_action"),
        "strategy_id": intent.get("strategy_id"),
        "strategy_version_id": intent.get("strategy_version_id"),
        "target_position_pct": intent.get("target_position_pct"),
        "buy_cash_pct": sizing.get("buy_cash_pct") if side == "buy" else None,
        "sell_position_pct": intent.get("sell_position_pct") if side == "sell" else None,
        "trade_amount": intent.get("trade_amount"),
        "share_quantity": intent.get("share_quantity"),
        "source": intent.get("signal_source") or "order_intent",
    }
    return _json_safe({key: value for key, value in payload.items() if value not in (None, "")})


def _event_context_from_pending_entry(entry: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(entry, dict) or not entry:
        return {}
    order_payload = entry.get("order_intent") if isinstance(entry.get("order_intent"), dict) else {}
    if not order_payload and isinstance(entry.get("order_payload"), dict):
        order_payload = entry.get("order_payload") or {}
    signal_payload = entry.get("signal_payload") if isinstance(entry.get("signal_payload"), dict) else {}
    if not signal_payload:
        signal_payload = _signal_payload_from_order_intent(order_payload)
    risk_payload = entry.get("risk_payload") if isinstance(entry.get("risk_payload"), dict) else {}
    return {
        "signal_payload": _json_safe(signal_payload or {}),
        "risk_payload": _json_safe(risk_payload or {}),
        "order_payload": _json_safe(order_payload or {}),
    }


def _pending_order_event_context(tracker: dict[str, Any], order_id: Any) -> dict[str, dict[str, Any]]:
    normalized_order_id = str(order_id or "").strip()
    if not normalized_order_id:
        return {}
    pending_orders = dict(tracker.get("pending_orders") or {})
    return _event_context_from_pending_entry(pending_orders.get(normalized_order_id))


def _normalize_event_side(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if "sell" in raw or "卖" in raw:
        return "sell"
    if "buy" in raw or "买" in raw:
        return "buy"
    return raw


def _context_side(context: dict[str, dict[str, Any]]) -> str:
    signal = context.get("signal_payload") or {}
    order = context.get("order_payload") or {}
    return _normalize_event_side(signal.get("side") or order.get("side"))


def _context_symbol(context: dict[str, dict[str, Any]]) -> str | None:
    signal = context.get("signal_payload") or {}
    order = context.get("order_payload") or {}
    return _normalize_symbol(signal.get("symbol") or order.get("symbol"))


def _context_quantity(context: dict[str, dict[str, Any]]) -> float:
    order = context.get("order_payload") or {}
    return _to_float(order.get("quantity")) or 0.0


def _trade_quantity(item: dict[str, Any]) -> float:
    return _to_float(
        item.get("quantity"),
        item.get("traded_quantity"),
        item.get("trade_quantity"),
        item.get("deal_quantity"),
        item.get("business_volume"),
        item.get("filled_quantity"),
        item.get("business_amount"),
    ) or 0.0


def _remember_recent_execution_context(
    tracker: dict[str, Any],
    trade: dict[str, Any],
    context: dict[str, dict[str, Any]],
) -> None:
    if not context:
        return
    symbol = _normalize_symbol(trade.get("symbol")) or _context_symbol(context)
    if not symbol:
        return
    side = _context_side(context) or _normalize_event_side(trade.get("side"))
    quantity = _trade_quantity(trade) or _context_quantity(context)
    recorded_at = _now_dt()
    recent = [item for item in (tracker.get("recent_execution_contexts") or []) if isinstance(item, dict)]
    cutoff = recorded_at - timedelta(minutes=30)
    compacted = []
    for item in recent:
        recorded = _parse_datetime(item.get("recorded_at"))
        if recorded is not None and recorded < cutoff:
            continue
        compacted.append(item)
    compacted.append(
        _json_safe(
            {
                "order_id": str(trade.get("order_id") or trade.get("entrust_no") or "").strip(),
                "trade_id": _trade_identity(trade),
                "symbol": symbol,
                "side": side,
                "quantity": quantity,
                "recorded_at": recorded_at.isoformat(),
                **context,
            }
        )
    )
    tracker["recent_execution_contexts"] = compacted[-200:]


def _position_quantity_delta(previous: dict[str, Any] | None, current: dict[str, Any] | None) -> float:
    previous_quantity = _to_float((previous or {}).get("current_position"), (previous or {}).get("quantity"), (previous or {}).get("position")) or 0.0
    current_quantity = _to_float((current or {}).get("current_position"), (current or {}).get("quantity"), (current or {}).get("position")) or 0.0
    return current_quantity - previous_quantity


def _position_change_event_context(
    tracker: dict[str, Any],
    symbol: str,
    previous: dict[str, Any] | None,
    current: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    normalized_symbol = _normalize_symbol(symbol)
    delta = _position_quantity_delta(previous, current)
    if not normalized_symbol or delta == 0:
        return {}
    expected_side = "buy" if delta > 0 else "sell"
    quantity = abs(delta)
    recent = [item for item in (tracker.get("recent_execution_contexts") or []) if isinstance(item, dict)]
    recent.sort(key=lambda item: _parse_datetime(item.get("recorded_at")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    for item in recent:
        if _normalize_symbol(item.get("symbol")) != normalized_symbol:
            continue
        item_side = _normalize_event_side(item.get("side"))
        if item_side and item_side != expected_side:
            continue
        item_quantity = _to_float(item.get("quantity")) or 0.0
        if item_quantity > 0 and abs(item_quantity - quantity) > 1:
            continue
        return _event_context_from_pending_entry(item)

    for entry in (tracker.get("pending_orders") or {}).values():
        context = _event_context_from_pending_entry(entry if isinstance(entry, dict) else {})
        if not context:
            continue
        if _context_symbol(context) != normalized_symbol:
            continue
        item_side = _context_side(context)
        if item_side and item_side != expected_side:
            continue
        item_quantity = _context_quantity(context)
        if item_quantity > 0 and abs(item_quantity - quantity) > 1:
            continue
        return context
    return {}


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
            context = _pending_order_event_context(tracker, order_id)
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
                signal_payload=context.get("signal_payload"),
                risk_payload=context.get("risk_payload"),
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
        context = _pending_order_event_context(tracker, item.get("order_id"))
        _append_event(
            db,
            monitor,
            "trade_confirmed",
            symbol=item.get("symbol"),
            payload={"trade_id": trade_id, "order_id": item.get("order_id")},
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
            order_payload=context.get("order_payload"),
            broker_result=item,
            correlation_id=correlation_id,
        )
        _remember_recent_execution_context(tracker, item, context)
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
        context = _position_change_event_context(tracker, symbol, previous, current)
        _append_event(
            db,
            monitor,
            "position_changed",
            symbol=symbol,
            payload={"previous": previous, "current": current},
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
            order_payload=context.get("order_payload"),
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
        context = _event_context_from_pending_entry(entry if isinstance(entry, dict) else {})
        _append_event(
            db,
            monitor,
            "order_cancel_requested",
            symbol=current_order.get("symbol"),
            payload={"order_id": order_id, "age_seconds": _seconds_since(entry.get("submitted_at"), now), "replace_attempts": replace_attempts},
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
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
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
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
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
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
            signal_payload=context.get("signal_payload"),
            risk_payload=context.get("risk_payload"),
            order_payload=original_intent,
            broker_result=replace_result if replace_result.get("success") else {},
            error_payload={} if replace_result.get("success") else replace_result,
            correlation_id=correlation_id,
        )
        if replace_result.get("success"):
            _bump_stat(monitor, "orders")
            tracker["pending_orders"] = _json_safe(pending_orders)
            _set_execution_tracker(monitor, tracker)
            _register_pending_order(
                monitor,
                replace_result,
                original_intent,
                signal_payload=context.get("signal_payload"),
                risk_payload=context.get("risk_payload"),
            )
            tracker.update(_get_execution_tracker(monitor))
            pending_orders = dict(tracker.get("pending_orders") or {})
            _append_broker_followup_events(
                db,
                monitor,
                str(original_intent.get("symbol") or ""),
                replace_result,
                signal_payload=context.get("signal_payload"),
                risk_payload=context.get("risk_payload"),
                order_payload=original_intent,
                correlation_id=correlation_id,
            )
    tracker["pending_orders"] = _json_safe(pending_orders)


def _register_pending_order(
    monitor: RealtimeMonitorDB,
    broker_result: dict[str, Any],
    intent: dict[str, Any],
    *,
    signal_payload: dict[str, Any] | None = None,
    risk_payload: dict[str, Any] | None = None,
) -> None:
    order_id = str(broker_result.get("order_id") or "").strip()
    if not order_id:
        return
    tracker = _get_execution_tracker(monitor)
    context = {
        "signal_payload": _json_safe(dict(signal_payload or {}) or _signal_payload_from_order_intent(intent)),
        "risk_payload": _json_safe(dict(risk_payload or {})),
        "order_payload": _json_safe(dict(intent)),
    }
    latest_trade = broker_result.get("latest_trade")
    if isinstance(latest_trade, dict) and latest_trade:
        _remember_recent_execution_context(tracker, latest_trade, context)
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
            "signal_payload": context["signal_payload"],
            "risk_payload": context["risk_payload"],
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
    tracker.setdefault("recent_execution_contexts", [])
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
    state = dict(payload.get("state") or {})
    stats = dict(state.get("stats") or {})
    stats.pop("approvals", None)
    if stats:
        state["stats"] = stats
    elif "stats" in state:
        state.pop("stats", None)
    payload["state"] = state
    pool = dict(payload.get("monitor_pool") or {})
    manual_symbols = _dedupe_normalized_symbols(pool.get("manual_symbols") or pool.get("symbols") or [])
    resolved_symbols = _dedupe_normalized_symbols(pool.get("resolved_symbols") or [])
    display_symbols = _displayable_symbols(resolved_symbols or manual_symbols)
    payload["manual_symbols"] = manual_symbols
    payload["resolved_symbols"] = resolved_symbols
    payload["manual_symbol_count"] = len(manual_symbols)
    payload["resolved_symbol_count"] = len(resolved_symbols)
    payload["display_symbols"] = display_symbols
    payload["display_symbol_count"] = len(payload["display_symbols"])
    payload["display_symbol_items"] = _display_symbol_items(display_symbols)
    payload["circuit_breaker"] = {
        "active": monitor.status == "fused",
        "reason": monitor.fused_reason,
        "last_heartbeat_at": payload.get("last_heartbeat_at"),
    }
    payload["data_governance"] = build_realtime_monitor_governance(payload)
    return payload


def _dedupe_normalized_symbols(values: Any) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        symbol = _normalize_symbol(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _is_displayable_stock_symbol(symbol: str) -> bool:
    return bool(re.match(r"^\d{6}\.(SH|SZ|BJ)$", str(symbol or "").strip().upper()))


def _displayable_symbols(symbols: list[str]) -> list[str]:
    return [symbol for symbol in _dedupe_normalized_symbols(symbols) if _is_displayable_stock_symbol(symbol)]


def _display_symbol_items(symbols: list[str]) -> list[dict[str, Any]]:
    try:
        name_map = get_reverse_stock_map()
    except Exception:
        logger.debug("[realtime-monitor] stock name map unavailable", exc_info=True)
        name_map = {}
    items: list[dict[str, Any]] = []
    for symbol in symbols:
        code = symbol.split(".")[0]
        name = str(name_map.get(symbol) or name_map.get(code) or "").strip()
        items.append(
            {
                "symbol": symbol,
                "name": name,
                "recognized": bool(name),
            }
        )
    return items


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


def _require_strategy(db: Session, strategy_id: str) -> dict[str, Any]:
    strategy = get_platform_strategy(db, strategy_id)
    if strategy is None:
        raise KeyError("策略不存在")
    return strategy


def _config_timeframes(config: dict[str, Any], fallback: list[str] | tuple[str, ...] | None = None) -> list[str]:
    routes = config.get("signal_routes") if isinstance(config, dict) else None
    timeframes: list[str] = []
    if isinstance(routes, list):
        for route in routes:
            if not isinstance(route, dict):
                continue
            timeframe = str(route.get("timeframe") or "").strip()
            if timeframe and timeframe not in timeframes:
                timeframes.append(timeframe)
    if timeframes:
        return timeframes
    return list(fallback or [])


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
    routes = config.get("signal_routes")
    if isinstance(routes, list):
        normalized_routes: list[dict[str, Any]] = []
        for index, raw_route in enumerate(routes):
            if not isinstance(raw_route, dict):
                continue
            route = dict(raw_route)
            side = _route_side(route)
            timeframe = _normalize_route_timeframe(route.get("timeframe") or route.get("period") or "30m")
            route["id"] = str(route.get("id") or f"route_{index + 1}").strip() or f"route_{index + 1}"
            route["side"] = side
            route["timeframe"] = timeframe
            route["action"] = _normalize_route_action(route.get("action"), side, timeframe)
            route["require_approval"] = False
            normalized_routes.append(route)
        config["signal_routes"] = normalized_routes
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
    stats.pop("approvals", None)
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


def _normalize_pct(*values: Any, default: float = 0.0, allow_zero: bool = False) -> float:
    for value in values:
        number = _to_float(value)
        if number is None or number < 0 or (number == 0 and not allow_zero):
            continue
        normalized = number / 100 if number > 1 else number
        return max(min(normalized, 1.0), 0.0)
    return max(min(default, 1.0), 0.0)


def _monitor_max_single_position_pct(monitor: RealtimeMonitorDB) -> float:
    return _normalize_pct((monitor.risk_config_json or {}).get("max_single_position_pct"), default=1.0)


def _monitor_buy_cash_buffer_pct(monitor: RealtimeMonitorDB) -> float:
    return _normalize_pct((monitor.config_json or {}).get("buy_cash_buffer_pct"), default=0.02, allow_zero=True)


def _monitor_buy_price_buffer_pct(monitor: RealtimeMonitorDB) -> float:
    return _normalize_pct((monitor.config_json or {}).get("buy_price_buffer_pct"), default=0.01, allow_zero=True)


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
