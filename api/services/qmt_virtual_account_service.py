from __future__ import annotations

import copy
import asyncio
import hashlib
import ipaddress
import calendar
import logging
import math
import os
import socket
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import httpx
import requests
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from api.core.stock_map import get_reverse_stock_map_cached_only
from api.core.settings import settings
from api.database import ImportedPortfolioPositionDB, QmtAccountEquitySnapshotDB, QmtAccountSnapshotDB, QmtAccountTradeHistoryDB, QmtSyncProfileDB, VirtualPositionStateDB
from api.services import auth_service, portfolio_import_service
from api.services.data_source_governance import build_virtual_warehouse_governance
from api.core.utils import run_async


logger = logging.getLogger(__name__)
SOURCE_NAME = "qmt_virtual"
CN_TZ = timezone(timedelta(hours=8))
_BULK_SELL_TASKS: dict[str, dict[str, Any]] = {}
_BULK_SELL_TASKS_LOCK = threading.RLock()
_BULK_SELL_TASK_RETENTION_SECONDS = 60 * 60 * 6
_QMT_SNAPSHOT_TIMEOUT_SECONDS = max(float(os.getenv("QMT_SNAPSHOT_TIMEOUT_SECONDS", "6")), 1.0)
_QMT_RECENT_PAYLOAD_TTL_SECONDS = max(float(os.getenv("QMT_RECENT_PAYLOAD_TTL_SECONDS", "5")), 1.0)
_QMT_FAILURE_COOLDOWN_SECONDS = max(float(os.getenv("QMT_FAILURE_COOLDOWN_SECONDS", "120")), 1.0)
_QMT_RECENT_PAYLOADS: dict[str, dict[str, Any]] = {}
_QMT_RECENT_FAILURES: dict[str, dict[str, Any]] = {}
_QMT_FETCH_LOCKS: dict[str, threading.Lock] = {}
_QMT_BACKGROUND_REFRESH_STATE: dict[str, dict[str, Any]] = {}
_QMT_FETCH_STATE_LOCK = threading.RLock()
_QMT_EQUITY_SCHEMA_READY_FOR: set[str] = set()


@dataclass(frozen=True)
class QmtRuntimeConfig:
    key: str
    enabled: bool
    host: str
    port: int
    account_id: str
    account_type: str
    account_name: str
    userdata_path: str
    role: str
    bridge_base_url: str
    bridge_token: str
    refresh_interval_seconds: int


def create_qmt_bulk_sell_task(
    db: Session,
    user_id: str,
    *,
    account_key: str | None,
    strategy_name: str | None = None,
) -> dict[str, Any]:
    config = _resolve_runtime_config(account_key, db=db, user_id=user_id)
    request_id = uuid4().hex
    if str(config.role or "").strip().lower() == "live":
        _audit_qmt_action("bulk_sell.reject", config, request_id, status="live_bulk_sell_disabled")
        raise RuntimeError("实盘账户不支持一键卖出全部持仓。请逐笔核对后手动提交卖出委托。")
    _ensure_qmt_trading_allowed(config, request_id=request_id, action="bulk_sell")
    overview = get_qmt_virtual_account_overview(db, user_id, account_key=config.key)
    positions = overview.get("positions") or []
    sellable_positions = [
        {
            "symbol": str(item.get("symbol") or "").strip().upper(),
            "name": str(item.get("name") or "").strip(),
            "quantity": _normalize_order_quantity(item.get("available_position") or item.get("current_position")),
        }
        for item in positions
        if _normalize_order_quantity(item.get("available_position") or item.get("current_position")) > 0
    ]
    if not sellable_positions:
        raise RuntimeError("当前没有可卖出的持仓。")

    task_id = uuid4().hex
    snapshot = {
        "id": task_id,
        "task_type": "qmt_bulk_sell",
        "user_id": user_id,
        "account_key": config.key,
        "account_id": config.account_id,
        "account_name": config.account_name,
        "status": "pending",
        "strategy_name": str(strategy_name or "量化之神").strip() or "量化之神",
        "total": len(sellable_positions),
        "processed": 0,
        "success_count": 0,
        "failure_count": 0,
        "current_symbol": None,
        "current_name": None,
        "recent_failures": [],
        "items": [],
        "version": 0,
        "created_at": _iso_now(),
        "updated_at": _iso_now(),
        "completed_at": None,
        "request_id": request_id,
    }
    with _BULK_SELL_TASKS_LOCK:
        _cleanup_expired_bulk_sell_tasks()
        existing = _find_active_bulk_sell_task_locked(user_id, config.key)
        if existing is not None:
            raise RuntimeError(
                f"当前账户已有一键卖出任务运行中：{existing.get('id')}，"
                "请等待任务完成后再发起新的清仓。"
            )
        _BULK_SELL_TASKS[task_id] = snapshot

    worker = threading.Thread(
        target=_run_bulk_sell_task_worker,
        args=(task_id, user_id, config.key, snapshot["strategy_name"], sellable_positions),
        daemon=True,
        name=f"qmt-bulk-sell-{task_id[:8]}",
    )
    worker.start()
    return get_qmt_bulk_sell_task(user_id, task_id)


def get_qmt_bulk_sell_task(user_id: str, task_id: str) -> dict[str, Any]:
    with _BULK_SELL_TASKS_LOCK:
        task = _BULK_SELL_TASKS.get(task_id)
        if task is None or task.get("user_id") != user_id:
            raise RuntimeError("清仓任务不存在")
        return copy.deepcopy(_public_bulk_sell_task(task))


def get_qmt_virtual_account_overview(
    db: Session,
    user_id: str,
    *,
    account_key: str | None = None,
    preferred_role: str | None = None,
    prefer_cache: bool = False,
    sync_to_imports: bool = False,
    allow_cache_fallback: bool = True,
) -> dict[str, Any]:
    configs = _load_runtime_configs(db=db, user_id=user_id)
    sync_profile_map = _load_qmt_sync_profile_map(db, user_id)
    account_summaries: list[dict[str, Any]] = []
    active_payload: dict[str, Any] | None = None
    active_key = _resolve_active_key(configs, account_key, preferred_role=preferred_role)

    for config in configs:
        is_active = config.key == active_key
        if is_active:
            payload = _load_account_payload(
                db,
                user_id,
                config,
                prefer_cache=prefer_cache and is_active,
                sync_to_imports=sync_to_imports and is_active,
                allow_cache_fallback=allow_cache_fallback,
            )
        else:
            payload = _load_cached_payload(db, user_id, config) or _load_empty_payload(config)
        payload = _annotate_qmt_connection_health(payload, sync_profile_map.get(config.key))
        account_summaries.append({
            "account_key": config.key,
            "role": config.role,
            "connection": payload["connection"],
            "account": payload["account"],
            "summary": payload["summary"],
            "refresh_interval_seconds": payload["refresh_interval_seconds"],
            "last_synced_at": payload.get("last_synced_at"),
            "data_source": payload.get("data_source"),
            "is_stale": bool(payload.get("is_stale", False)),
            "sync_profile": payload.get("sync_profile"),
        })
        if is_active:
            active_payload = payload

    if active_payload is None:
        fallback_config = _pick_active_config(configs, active_key)
        active_payload = _annotate_qmt_connection_health(
            _load_empty_payload(fallback_config),
            sync_profile_map.get(fallback_config.key),
        )

    active_account_key = active_payload["connection"].get("account_key")
    response_payload = {
        **active_payload,
        "active_account_key": active_account_key,
        "accounts": account_summaries,
        "background_refresh": _get_background_refresh_status(
            _qmt_fetch_cache_key(user_id, active_account_key) if active_account_key else None,
        ),
    }
    response_payload["data_governance"] = build_virtual_warehouse_governance(response_payload)
    return response_payload


def get_qmt_return_stats(
    db: Session,
    user_id: str,
    *,
    account_key: str | None = None,
    preferred_role: str | None = None,
) -> dict[str, Any]:
    _ensure_qmt_equity_snapshot_schema(db)
    configs = _load_runtime_configs(db=db, user_id=user_id)
    active_key = _resolve_active_key(configs, account_key, preferred_role=preferred_role)
    config = _pick_active_config(configs, active_key)
    _ensure_equity_snapshot_from_latest_cache(db, user_id, config)
    current = _latest_equity_snapshot(db, user_id, config.key)
    fetched_at = current.fetched_at if current else None
    current_date = current.snapshot_date if current else datetime.now(CN_TZ).date()

    periods = {
        "day": _build_return_period(
            db,
            user_id,
            config.key,
            current,
            key="day",
            label="日收益",
            period_start=current_date,
            allow_today_pnl_fallback=True,
        ),
        "month": _build_return_period(
            db,
            user_id,
            config.key,
            current,
            key="month",
            label="月收益",
            period_start=current_date.replace(day=1),
        ),
        "year": _build_return_period(
            db,
            user_id,
            config.key,
            current,
            key="year",
            label="年收益",
            period_start=current_date.replace(month=1, day=1),
        ),
    }
    return {
        "account_key": config.key,
        "role": config.role,
        "account_id": config.account_id,
        "currency": "CNY",
        "display_mode_default": "amount",
        "periods": periods,
        "calendar": _build_return_calendar(db, user_id, config.key, current_date=current_date),
        "traded_securities": _build_traded_security_summaries(db, user_id, config.key),
        "updated_at": fetched_at.isoformat() if fetched_at else None,
        "snapshot_date": current_date.isoformat(),
    }


def trigger_qmt_background_refresh(
    db: Session,
    user_id: str,
    *,
    account_key: str | None = None,
    preferred_role: str | None = None,
) -> dict[str, Any]:
    configs = _load_runtime_configs(db=db, user_id=user_id)
    active_key = _resolve_active_key(configs, account_key, preferred_role=preferred_role)
    scheduled = _schedule_qmt_background_refresh(user_id, active_key)
    cache_key = _qmt_fetch_cache_key(user_id, active_key)
    return {
        "message": "QMT 后台刷新已启动" if scheduled else "QMT 后台刷新已在进行中",
        "scheduled": scheduled,
        "account_key": active_key,
        "background_refresh": _get_background_refresh_status(cache_key),
    }


def sync_qmt_virtual_positions(db: Session, user_id: str, account_key: str | None = None) -> dict[str, Any]:
    overview = get_qmt_virtual_account_overview(db, user_id, account_key=account_key, sync_to_imports=False)
    summary = overview.get("summary") or {}
    return {
        "message": "QMT 仓位与跟踪看板已隔离，当前版本不再执行同步写入",
        "source": None,
        "summary": {
            "positions": summary.get("position_count", 0),
            "market_value": summary.get("market_value", 0.0),
            "total_asset": summary.get("total_asset", 0.0),
        },
        "overview": overview,
    }


def _load_qmt_sync_profile_map(db: Session, user_id: str) -> dict[str, dict[str, Any]]:
    rows = (
        db.query(QmtSyncProfileDB)
        .filter(QmtSyncProfileDB.user_id == user_id)
        .all()
    )
    return {str(row.account_key): _qmt_sync_profile_to_dict(row) for row in rows}


def _qmt_sync_profile_to_dict(row: QmtSyncProfileDB) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "account_key": row.account_key,
        "is_active": bool(row.is_active),
        "sync_interval_seconds": int(row.sync_interval_seconds or 30),
        "sync_tracking_board": bool(row.sync_tracking_board),
        "alert_on_disconnect": bool(row.alert_on_disconnect),
        "last_synced_at": row.last_synced_at.isoformat() if row.last_synced_at else None,
        "last_status": row.last_status,
        "last_error": row.last_error,
        "consecutive_failures": int(row.consecutive_failures or 0),
        "last_alerted_at": row.last_alerted_at.isoformat() if row.last_alerted_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _annotate_qmt_connection_health(payload: dict[str, Any], sync_profile: dict[str, Any] | None) -> dict[str, Any]:
    annotated = dict(payload)
    connection = dict(annotated.get("connection") or {})
    profile = dict(sync_profile or {}) if sync_profile else None
    annotated["sync_profile"] = profile

    is_stale = bool(annotated.get("is_stale"))
    direct_connected = bool(connection.get("connected")) and not is_stale
    profile_is_fresh = _is_qmt_sync_profile_fresh(profile)
    snapshot_available = bool(
        annotated.get("last_synced_at")
        or annotated.get("account")
        or annotated.get("positions")
        or annotated.get("orders")
        or annotated.get("trades")
    )

    profile_success_at = str((profile or {}).get("last_synced_at") or "").strip() or None
    if direct_connected:
        health_status = "live"
        health_label = "实时直连"
        health_message = connection.get("message") or "本次 QMT 快照查询成功。"
        effective_connected = True
    elif profile_is_fresh:
        health_status = "background_live"
        health_label = "后台在线"
        health_message = f"后台同步最近成功于 {profile_success_at}，页面当前展示快照数据。"
        effective_connected = True
    elif snapshot_available:
        health_status = "snapshot_available"
        health_label = "快照可用"
        health_message = connection.get("message") or "页面当前展示最近一次成功同步的 QMT 快照，等待下一次后台刷新。"
        effective_connected = False
    else:
        health_status = "disconnected"
        health_label = "未连接"
        health_message = connection.get("message") or "当前没有可用的 QMT 连接或本地快照。"
        effective_connected = False

    connection["health_status"] = health_status
    connection["health_label"] = health_label
    connection["health_message"] = health_message
    connection["effective_connected"] = effective_connected
    if profile_success_at:
        connection["last_background_success_at"] = profile_success_at
    if profile and profile.get("last_status"):
        connection["background_status"] = profile.get("last_status")
    annotated["connection"] = connection
    return annotated


def _is_qmt_sync_profile_fresh(profile: dict[str, Any] | None) -> bool:
    if not profile:
        return False
    if not bool(profile.get("is_active")):
        return False
    if str(profile.get("last_status") or "").lower() != "success":
        return False
    last_synced_at = _parse_qmt_sync_profile_datetime(profile.get("last_synced_at"))
    if last_synced_at is None:
        return False
    interval = max(int(profile.get("sync_interval_seconds") or 30), 10)
    freshness_seconds = max(interval * 3, 120)
    return (datetime.now(timezone.utc) - last_synced_at) <= timedelta(seconds=freshness_seconds)


def _parse_qmt_sync_profile_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=CN_TZ).astimezone(timezone.utc)
    return parsed.astimezone(timezone.utc)


def list_qmt_orders(db: Session, user_id: str, *, account_key: str | None = None) -> dict[str, Any]:
    overview = get_qmt_virtual_account_overview(db, user_id, account_key=account_key)
    return {
        "active_account_key": overview.get("active_account_key"),
        "items": overview.get("orders") or [],
        "connection": overview.get("connection") or {},
        "fetched_at": overview.get("fetched_at"),
    }


def list_qmt_trades(db: Session, user_id: str, *, account_key: str | None = None) -> dict[str, Any]:
    overview = get_qmt_virtual_account_overview(db, user_id, account_key=account_key)
    return {
        "active_account_key": overview.get("active_account_key"),
        "items": overview.get("trades") or [],
        "connection": overview.get("connection") or {},
        "fetched_at": overview.get("fetched_at"),
    }


def submit_qmt_order(
    db: Session,
    user_id: str,
    *,
    account_key: str | None,
    symbol: str,
    side: str,
    quantity: int,
    price: float | None,
    price_type: str,
    strategy_name: str | None = None,
    order_remark: str | None = None,
    include_overview: bool = True,
    overview_allow_cache_fallback: bool = True,
) -> dict[str, Any]:
    config = _resolve_runtime_config(account_key, db=db, user_id=user_id)
    request_id = uuid4().hex
    _audit_qmt_action("submit_order.request", config, request_id, status="received", symbol=symbol, side=side, quantity=quantity)
    if not config.enabled:
        _audit_qmt_action("submit_order.reject", config, request_id, status="disabled")
        raise RuntimeError("当前 QMT 账户未启用")
    _ensure_qmt_trading_allowed(config, request_id=request_id, action="submit_order")
    if quantity <= 0:
        _audit_qmt_action("submit_order.reject", config, request_id, status="invalid_quantity", quantity=quantity)
        raise RuntimeError("委托数量必须大于 0")
    if str(price_type or "limit").strip().lower() == "limit" and price in (None, 0):
        _audit_qmt_action("submit_order.reject", config, request_id, status="invalid_limit_price", price=price)
        raise RuntimeError("限价委托必须填写价格")

    try:
        result = _submit_qmt_order(
            config,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            price_type=price_type,
            strategy_name=strategy_name,
            order_remark=order_remark,
        )
        _audit_qmt_action("submit_order.success", config, request_id, status="success", order_id=result.get("order_id"))
    except requests.exceptions.Timeout as exc:
        message = (
            "QMT 委托提交超时：bridge 在 20 秒内未返回结果。"
            "请刷新委托/成交确认是否已被 QMT 接收；如未出现委托，说明本次未成功提交。"
        )
        _audit_qmt_action("submit_order.error", config, request_id, status="timeout", error=str(exc))
        raise RuntimeError(message) from exc
    except requests.exceptions.HTTPError as exc:
        message = _bridge_http_error_message("QMT 委托提交失败", exc)
        _audit_qmt_action("submit_order.error", config, request_id, status="bridge_http_error", error=message)
        raise RuntimeError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"QMT 委托提交失败：bridge 通信异常（{exc}）"
        _audit_qmt_action("submit_order.error", config, request_id, status="request_error", error=str(exc))
        raise RuntimeError(message) from exc
    except Exception as exc:
        _audit_qmt_action("submit_order.error", config, request_id, status="error", error=str(exc))
        raise RuntimeError(f"QMT 委托提交失败：{exc}") from exc
    overview = (
        get_qmt_virtual_account_overview(
            db,
            user_id,
            account_key=config.key,
            allow_cache_fallback=overview_allow_cache_fallback,
        )
        if include_overview
        else None
    )
    return {
        "message": "QMT 委托已提交",
        "account_key": config.key,
        "request_id": request_id,
        "order_result": result,
        "overview": overview,
    }


def cancel_qmt_order(
    db: Session,
    user_id: str,
    *,
    account_key: str | None,
    order_id: str,
) -> dict[str, Any]:
    config = _resolve_runtime_config(account_key, db=db, user_id=user_id)
    request_id = uuid4().hex
    _audit_qmt_action("cancel_order.request", config, request_id, status="received", order_id=order_id)
    if not config.enabled:
        _audit_qmt_action("cancel_order.reject", config, request_id, status="disabled")
        raise RuntimeError("当前 QMT 账户未启用")
    _ensure_qmt_trading_allowed(config, request_id=request_id, action="cancel_order")
    if not str(order_id or "").strip():
        _audit_qmt_action("cancel_order.reject", config, request_id, status="missing_order_id")
        raise RuntimeError("缺少 order_id")

    try:
        result = _cancel_qmt_order(config, order_id=order_id)
        _audit_qmt_action("cancel_order.success", config, request_id, status="success", order_id=result.get("order_id"))
    except requests.exceptions.Timeout as exc:
        message = (
            "QMT 撤单请求超时：bridge 在 20 秒内未返回结果。"
            "请刷新委托/成交确认是否已被 QMT 接收。"
        )
        _audit_qmt_action("cancel_order.error", config, request_id, status="timeout", order_id=order_id, error=str(exc))
        raise RuntimeError(message) from exc
    except requests.exceptions.HTTPError as exc:
        message = _bridge_http_error_message("QMT 撤单失败", exc)
        _audit_qmt_action("cancel_order.error", config, request_id, status="bridge_http_error", order_id=order_id, error=message)
        raise RuntimeError(message) from exc
    except requests.exceptions.RequestException as exc:
        message = f"QMT 撤单失败：bridge 通信异常（{exc}）"
        _audit_qmt_action("cancel_order.error", config, request_id, status="request_error", order_id=order_id, error=str(exc))
        raise RuntimeError(message) from exc
    except Exception as exc:
        _audit_qmt_action("cancel_order.error", config, request_id, status="error", order_id=order_id, error=str(exc))
        raise RuntimeError(f"QMT 撤单失败：{exc}") from exc
    overview = get_qmt_virtual_account_overview(db, user_id, account_key=config.key)
    return {
        "message": "QMT 撤单请求已提交",
        "account_key": config.key,
        "request_id": request_id,
        "cancel_result": result,
        "overview": overview,
    }


def _run_bulk_sell_task_worker(
    task_id: str,
    user_id: str,
    account_key: str,
    strategy_name: str,
    sellable_positions: list[dict[str, Any]],
) -> None:
    try:
        _update_bulk_sell_task(
            task_id,
            status="running",
            updated_at=_iso_now(),
        )
        from api.database import get_db_ctx

        with get_db_ctx() as db:
            for index, item in enumerate(sellable_positions, start=1):
                symbol = str(item.get("symbol") or "").strip().upper()
                name = str(item.get("name") or "").strip()
                quantity = int(item.get("quantity") or 0)
                if not symbol or quantity <= 0:
                    _append_bulk_sell_item(
                        task_id,
                        {
                            "symbol": symbol,
                            "name": name,
                            "quantity": quantity,
                            "status": "skipped",
                            "message": "无有效可卖数量",
                        },
                    )
                    _update_bulk_sell_task(
                        task_id,
                        processed=index,
                        current_symbol=symbol or None,
                        current_name=name or None,
                        updated_at=_iso_now(),
                    )
                    continue

                _update_bulk_sell_task(
                    task_id,
                    processed=index - 1,
                    current_symbol=symbol,
                    current_name=name or None,
                    updated_at=_iso_now(),
                )
                try:
                    response = submit_qmt_order(
                        db,
                        user_id,
                        account_key=account_key,
                        symbol=symbol,
                        side="sell",
                        quantity=quantity,
                        price=None,
                        price_type="latest",
                        strategy_name=strategy_name,
                        order_remark=f"一键清仓 {name or symbol} {symbol}",
                        include_overview=False,
                    )
                    order_result = dict(response.get("order_result") or {})
                    _append_bulk_sell_item(
                        task_id,
                        {
                            "symbol": symbol,
                            "name": name,
                            "quantity": quantity,
                            "status": "success",
                            "order_id": order_result.get("order_id"),
                            "message": "委托已提交",
                        },
                    )
                    _increment_bulk_sell_counter(task_id, "success_count")
                except Exception as exc:
                    message = str(exc)
                    _append_bulk_sell_item(
                        task_id,
                        {
                            "symbol": symbol,
                            "name": name,
                            "quantity": quantity,
                            "status": "failed",
                            "message": message,
                        },
                    )
                    _increment_bulk_sell_counter(task_id, "failure_count")
                    _push_bulk_sell_failure(task_id, f"{symbol}: {message}")
                finally:
                    _update_bulk_sell_task(
                        task_id,
                        processed=index,
                        current_symbol=symbol,
                        current_name=name or None,
                        updated_at=_iso_now(),
                    )

            final_overview = get_qmt_virtual_account_overview(db, user_id, account_key=account_key)
            final_state = get_qmt_bulk_sell_task(user_id, task_id)
            final_status = "completed_with_errors" if int(final_state.get("failure_count") or 0) > 0 else "completed"
            _update_bulk_sell_task(
                task_id,
                status=final_status,
                current_symbol=None,
                current_name=None,
                completed_at=_iso_now(),
                updated_at=_iso_now(),
                overview=final_overview,
            )
    except Exception as exc:
        logger.exception("[qmt-bulk-sell] task failed id=%s", task_id)
        _update_bulk_sell_task(
            task_id,
            status="failed",
            current_symbol=None,
            current_name=None,
            completed_at=_iso_now(),
            updated_at=_iso_now(),
        )
        _push_bulk_sell_failure(task_id, str(exc))


def _public_bulk_sell_task(task: dict[str, Any]) -> dict[str, Any]:
    payload = dict(task)
    payload.pop("user_id", None)
    payload["recent_failures"] = list(payload.get("recent_failures") or [])
    payload["items"] = list(payload.get("items") or [])
    return payload


def _find_active_bulk_sell_task_locked(user_id: str, account_key: str) -> dict[str, Any] | None:
    for task in _BULK_SELL_TASKS.values():
        if task.get("user_id") != user_id or task.get("account_key") != account_key:
            continue
        if str(task.get("status") or "") in {"pending", "running"}:
            return task
    return None


def _update_bulk_sell_task(task_id: str, **updates: Any) -> None:
    with _BULK_SELL_TASKS_LOCK:
        task = _BULK_SELL_TASKS.get(task_id)
        if task is None:
            return
        task.update(updates)
        task["updated_at"] = updates.get("updated_at") or _iso_now()
        task["version"] = int(task.get("version") or 0) + 1


def _append_bulk_sell_item(task_id: str, item: dict[str, Any]) -> None:
    with _BULK_SELL_TASKS_LOCK:
        task = _BULK_SELL_TASKS.get(task_id)
        if task is None:
            return
        items = list(task.get("items") or [])
        items.append(item)
        task["items"] = items[-500:]
        task["updated_at"] = _iso_now()
        task["version"] = int(task.get("version") or 0) + 1


def _increment_bulk_sell_counter(task_id: str, field: str) -> None:
    with _BULK_SELL_TASKS_LOCK:
        task = _BULK_SELL_TASKS.get(task_id)
        if task is None:
            return
        task[field] = int(task.get(field) or 0) + 1
        task["updated_at"] = _iso_now()
        task["version"] = int(task.get("version") or 0) + 1


def _push_bulk_sell_failure(task_id: str, message: str) -> None:
    with _BULK_SELL_TASKS_LOCK:
        task = _BULK_SELL_TASKS.get(task_id)
        if task is None:
            return
        failures = list(task.get("recent_failures") or [])
        failures.append(message)
        task["recent_failures"] = failures[-10:]
        task["updated_at"] = _iso_now()
        task["version"] = int(task.get("version") or 0) + 1


def _cleanup_expired_bulk_sell_tasks() -> None:
    now = time.time()
    for task_id, task in list(_BULK_SELL_TASKS.items()):
        completed_at = _parse_iso_datetime(task.get("completed_at"))
        if completed_at is None:
            continue
        age_seconds = now - completed_at.timestamp()
        if age_seconds > _BULK_SELL_TASK_RETENTION_SECONDS:
            _BULK_SELL_TASKS.pop(task_id, None)


def _ensure_qmt_trading_allowed(config: QmtRuntimeConfig, *, request_id: str, action: str) -> None:
    if not str(config.account_id or "").strip():
        _audit_qmt_action(action + ".reject", config, request_id, status="missing_account_id")
        raise RuntimeError("缺少 QMT account_id，无法提交交易指令。")
    if config.bridge_base_url:
        try:
            health = _fetch_qmt_bridge_health(config, timeout=2.5)
        except Exception as exc:
            message = _compact_qmt_snapshot_error(exc, config)
            _audit_qmt_action(action + ".reject", config, request_id, status="bridge_health_failed", error=message)
            raise RuntimeError(f"QMT 交易通道预检失败：{message}") from exc
        _validate_qmt_bridge_metadata(config, health, request_id=request_id, action=action, source="health")
        if not _truthy(health.get("trading_allowed")):
            _audit_qmt_action(action + ".reject", config, request_id, status="bridge_readonly")
            role_label = "实盘" if str(config.role or "").strip().lower() == "live" else "模拟"
            raise RuntimeError(
                f"QMT {role_label} bridge 当前为只读状态，交易接口不可用。"
                "请在 Windows bridge 启动环境确认 QMT_BRIDGE_ALLOW_TRADING=1，并重启对应 bridge。"
            )
    _audit_qmt_action(action + ".allow", config, request_id, status="trading_allowed")


def _audit_qmt_action(action: str, config: QmtRuntimeConfig, request_id: str, **fields: Any) -> None:
    logger.info(
        "[qmt-audit] action=%s request_id=%s account_key=%s account_id=%s role=%s bridge_url=%s %s",
        action,
        request_id,
        config.key,
        config.account_id,
        config.role,
        config.bridge_base_url,
        " ".join(f"{key}={value}" for key, value in fields.items() if value is not None),
    )


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on", "allow", "allowed"}


def _qmt_bridge_headers(config: QmtRuntimeConfig) -> dict[str, str]:
    headers: dict[str, str] = {}
    if config.bridge_token:
        headers["Authorization"] = f"Bearer {config.bridge_token}"
    return headers


def _fetch_qmt_bridge_health(config: QmtRuntimeConfig, *, timeout: float = 2.0) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("未配置 bridge_base_url")
    response = requests.get(f"{base_url}/health", headers=_qmt_bridge_headers(config), timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _bridge_metadata_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    bridge = payload.get("bridge")
    if isinstance(bridge, dict):
        return dict(bridge)
    keys = ("role", "account_key", "account_id", "account_type", "trading_allowed")
    metadata = {key: payload.get(key) for key in keys if payload.get(key) not in (None, "")}
    if bridge not in (None, ""):
        metadata["raw_bridge"] = str(bridge)
    return metadata


def _validate_qmt_bridge_metadata(
    config: QmtRuntimeConfig,
    payload: dict[str, Any],
    *,
    request_id: str | None = None,
    action: str = "bridge",
    source: str = "bridge",
) -> None:
    bridge = _bridge_metadata_payload(payload)
    bridge_role = str(bridge.get("role") or "").strip().lower()
    expected_role = str(config.role or "").strip().lower()
    if bridge_role and expected_role and bridge_role != expected_role:
        if request_id:
            _audit_qmt_action(action + ".reject", config, request_id, status="bridge_role_mismatch", bridge_role=bridge_role)
        raise RuntimeError(f"QMT bridge 角色不匹配：配置为 {expected_role}，{source} 返回 {bridge_role}。")

    bridge_account_key = str(bridge.get("account_key") or "").strip()
    if bridge_account_key and bridge_account_key != config.key:
        if request_id:
            _audit_qmt_action(action + ".reject", config, request_id, status="bridge_account_key_mismatch", bridge_account_key=bridge_account_key)
        raise RuntimeError(f"QMT bridge account_key 不匹配：配置为 {config.key}，{source} 返回 {bridge_account_key}。")

    bridge_account_id = str(bridge.get("account_id") or "").strip()
    if bridge_account_id and config.account_id and bridge_account_id != config.account_id:
        if request_id:
            _audit_qmt_action(action + ".reject", config, request_id, status="bridge_account_id_mismatch", bridge_account_id=bridge_account_id)
        raise RuntimeError(f"QMT bridge account_id 不匹配：配置为 {config.account_id}，{source} 返回 {bridge_account_id}。")


def _validate_qmt_snapshot_identity(config: QmtRuntimeConfig, payload: dict[str, Any]) -> None:
    _validate_qmt_bridge_metadata(config, _bridge_metadata_payload(payload), source="snapshot")
    asset = dict(payload.get("asset") or {})
    asset_account_id = str(
        asset.get("account_id")
        or asset.get("m_strAccountID")
        or asset.get("m_strAccountId")
        or ""
    ).strip()
    if asset_account_id and config.account_id and asset_account_id != config.account_id:
        raise RuntimeError(f"QMT 资产快照账号不匹配：配置为 {config.account_id}，快照返回 {asset_account_id}。")


def _bridge_http_error_message(prefix: str, exc: requests.exceptions.RequestException) -> str:
    response = getattr(exc, "response", None)
    if response is None:
        return f"{prefix}：bridge 通信异常（{exc}）"
    detail = ""
    try:
        payload = response.json()
        detail = str(payload.get("detail") or payload.get("message") or payload.get("error") or "").strip()
    except Exception:
        detail = str(getattr(response, "text", "") or "").strip()
    status_code = getattr(response, "status_code", "")
    suffix = f"HTTP {status_code}"
    if detail:
        suffix = f"{suffix}，{detail}"
    return f"{prefix}：bridge 返回 {suffix}"


def diagnose_qmt_accounts(db: Session | None = None, user_id: str | None = None, account_key: str | None = None, run_connect_test: bool = False) -> dict[str, Any]:
    configs = _load_runtime_configs(db=db, user_id=user_id)
    active_key = _resolve_active_key(configs, account_key)
    items = [_diagnose_single_account(config, run_connect_test=run_connect_test) for config in configs]
    return {
        "active_account_key": active_key,
        "run_connect_test": run_connect_test,
        "items": items,
        "summary": {
            "total": len(items),
            "enabled": sum(1 for item in items if item["enabled"]),
            "ready": sum(1 for item in items if item["ready"]),
            "connected": sum(1 for item in items if item.get("connect_test", {}).get("connected") is True),
        },
        "checked_at": _iso_now(),
    }


def _runtime_configs(*, db: Session | None = None, user_id: str | None = None) -> list[QmtRuntimeConfig]:
    configs: list[QmtRuntimeConfig] = []
    raw_accounts = (
        auth_service.get_user_qmt_account_configs(db, user_id).values()
        if db is not None and user_id
        else auth_service.default_qmt_account_configs().values()
    )
    for raw in raw_accounts:
        configs.append(
            QmtRuntimeConfig(
                key=str(raw.get("key") or "qmt_default").strip() or "qmt_default",
                enabled=bool(raw.get("enabled", False)),
                host=str(raw.get("host") or settings.qmt_host),
                port=int(raw.get("port") or settings.qmt_port or 58610),
                account_id=str(raw.get("account_id") or "").strip(),
                account_type=str(raw.get("account_type") or settings.qmt_account_type or "STOCK").strip() or "STOCK",
                account_name=str(raw.get("account_name") or "QMT 账户").strip() or "QMT 账户",
                userdata_path=str(raw.get("userdata_path") or "").strip(),
                role=str(raw.get("role") or "paper").strip() or "paper",
                bridge_base_url=str(raw.get("bridge_base_url") or "").strip(),
                bridge_token=str(raw.get("bridge_token") or settings.qmt_bridge_token or "").strip(),
                refresh_interval_seconds=max(int(raw.get("refresh_interval_seconds") or settings.qmt_refresh_interval_seconds or 10), 5),
            )
        )
    return configs or [
        QmtRuntimeConfig(
            key="paper_sim",
            enabled=False,
            host=settings.qmt_host,
            port=settings.qmt_port,
            account_id="",
            account_type=settings.qmt_account_type or "STOCK",
            account_name="QMT 模拟账户",
            userdata_path="",
            role="paper",
            bridge_base_url="",
            bridge_token=str(settings.qmt_bridge_token or "").strip(),
            refresh_interval_seconds=max(int(settings.qmt_refresh_interval_seconds or 10), 5),
        )
    ]


def _load_runtime_configs(*, db: Session | None = None, user_id: str | None = None) -> list[QmtRuntimeConfig]:
    try:
        return _runtime_configs(db=db, user_id=user_id)
    except TypeError:
        # 测试里会用不接收 kwargs 的 monkeypatch 替换 _runtime_configs，这里做兼容。
        return _runtime_configs()


def _resolve_runtime_config(account_key: str | None, *, db: Session | None = None, user_id: str | None = None) -> QmtRuntimeConfig:
    configs = _load_runtime_configs(db=db, user_id=user_id)
    active_key = _resolve_active_key(configs, account_key)
    return _pick_active_config(configs, active_key)


def _pick_active_config(configs: list[QmtRuntimeConfig], account_key: str | None) -> QmtRuntimeConfig:
    for config in configs:
        if config.key == account_key:
            return config
    return configs[0]


def _resolve_active_key(configs: list[QmtRuntimeConfig], account_key: str | None, *, preferred_role: str | None = None) -> str:
    if account_key:
        return account_key
    normalized_role = str(preferred_role or "").strip().lower()
    default_key = (settings.qmt_default_account_key or "").strip()
    if default_key:
        default_config = next((config for config in configs if config.key == default_key), None)
        if default_config and (not normalized_role or default_config.role == normalized_role):
            return default_key
    if normalized_role:
        for config in configs:
            if config.enabled and config.role == normalized_role:
                return config.key
        for config in configs:
            if config.role == normalized_role:
                return config.key
    for config in configs:
        if config.enabled:
            return config.key
    return configs[0].key


def _load_account_payload(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    *,
    prefer_cache: bool = False,
    sync_to_imports: bool = False,
    allow_cache_fallback: bool = True,
) -> dict[str, Any]:
    connection = {
        "account_key": config.key,
        "role": config.role,
        "enabled": config.enabled,
        "provider": "xtquant",
        "host": config.host,
        "port": config.port,
        "account_id": config.account_id,
        "account_type": config.account_type,
        "account_name": config.account_name,
        "userdata_path": config.userdata_path,
        "bridge_base_url": config.bridge_base_url,
        "connected": False,
        "message": "",
    }
    empty = _load_empty_payload(config, connection=connection)
    if not config.enabled:
        connection["message"] = "当前账户未启用，请在设置页的 QMT 账户配置中打开 enabled。"
        return empty
    if not config.account_id:
        connection["message"] = "缺少 account_id，无法查询账户资产。"
        return empty
    if not config.userdata_path and not config.bridge_base_url:
        host_reachable, reachability_message = _probe_tcp_port(config.host, config.port)
        if host_reachable:
            connection["message"] = "已探测到 QMT 端口可达，但未配置 bridge_base_url / userdata_path，暂无法读取资产与持仓。"
        else:
            connection["message"] = f"缺少 bridge_base_url / userdata_path，且端口探测未通过：{reachability_message}"
        return empty

    cache_key = _qmt_fetch_cache_key(user_id, config.key)
    recent_failure = _get_recent_fetch_failure(cache_key)
    if recent_failure and not sync_to_imports and allow_cache_fallback:
        cached = _load_cached_payload(db, user_id, config, connection_override={**connection, "message": recent_failure})
        if cached is not None:
            return cached
        connection["message"] = recent_failure
        return empty

    if prefer_cache and not sync_to_imports and allow_cache_fallback:
        recent_live_payload = _get_recent_live_payload(cache_key, ttl_seconds=_recent_payload_ttl_seconds(config))
        if recent_live_payload is not None and recent_failure is None:
            recent_payload = copy.deepcopy(recent_live_payload)
            recent_payload["fetched_at"] = _iso_now()
            recent_payload["data_source"] = "cache_recent"
            recent_payload["is_stale"] = False
            _schedule_qmt_background_refresh(user_id, config.key)
            return recent_payload
        cached_message = "页面已优先展示本地快照，后台正在刷新 QMT 数据"
        if recent_failure:
            cached_message = f"{recent_failure}；当前先展示本地快照"
        cached = _load_cached_payload(db, user_id, config, connection_override={**connection, "message": cached_message})
        if recent_failure is None:
            _schedule_qmt_background_refresh(user_id, config.key)
        if cached is not None:
            return cached
        connection["message"] = (
            f"{recent_failure}，暂无可用本地快照" if recent_failure else "正在后台刷新 QMT 数据，页面先展示本地空状态"
        )
        return empty

    fetch_lock = _get_qmt_fetch_lock(cache_key)
    acquired = fetch_lock.acquire(blocking=False)
    if not acquired:
        if sync_to_imports:
            fetch_lock.acquire()
            acquired = True
        elif not allow_cache_fallback:
            acquired = fetch_lock.acquire(timeout=3.0)
            if not acquired:
                connection["message"] = "QMT 快照刷新中，实时监控禁止使用缓存，已跳过本轮快照"
                return empty
        else:
            cached = _load_cached_payload(
                db,
                user_id,
                config,
                connection_override={**connection, "message": "QMT 快照刷新中，已先返回最近缓存"},
            )
            if cached is not None:
                return cached
            connection["message"] = "QMT 快照刷新中，请稍后重试"
            return empty

    try:
        if acquired and not sync_to_imports and allow_cache_fallback:
            recent_failure = _get_recent_fetch_failure(cache_key)
            if recent_failure:
                cached = _load_cached_payload(db, user_id, config, connection_override={**connection, "message": recent_failure})
                if cached is not None:
                    return cached
                connection["message"] = recent_failure
                return empty
        snapshot = _query_qmt_snapshot(config)
    except ImportError as exc:
        connection["message"] = f"xtquant 未安装：{exc}"
        _remember_fetch_failure(cache_key, connection["message"])
        if not allow_cache_fallback:
            return empty
        cached = _load_cached_payload(db, user_id, config, connection_override=connection)
        return cached or empty
    except Exception as exc:
        connection["message"] = _compact_qmt_snapshot_error(exc, config)
        if config.bridge_base_url:
            logger.warning("[qmt] fetch overview failed for %s: %s", config.key, connection["message"])
        else:
            logger.exception("[qmt] fetch overview failed for %s", config.key)
        _remember_fetch_failure(cache_key, connection["message"])
        if not allow_cache_fallback:
            return empty
        cached = _load_cached_payload(db, user_id, config, connection_override=connection)
        return cached or empty
    finally:
        if acquired:
            fetch_lock.release()

    return _materialize_qmt_snapshot_payload(
        db,
        user_id,
        config,
        snapshot,
        sync_to_imports=sync_to_imports,
    )


def _load_empty_payload(
    config: QmtRuntimeConfig,
    *,
    connection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_connection = connection or {
        "account_key": config.key,
        "role": config.role,
        "enabled": config.enabled,
        "provider": "xtquant",
        "host": config.host,
        "port": config.port,
        "account_id": config.account_id,
        "account_type": config.account_type,
        "account_name": config.account_name,
        "userdata_path": config.userdata_path,
        "bridge_base_url": config.bridge_base_url,
        "connected": False,
        "message": "",
    }
    return {
        "connection": active_connection,
        "account": None,
        "positions": [],
        "orders": [],
        "trades": [],
        "summary": {
            "total_asset": 0.0,
            "total_pnl": 0.0,
            "today_pnl": 0.0,
            "market_value": 0.0,
            "available_cash": 0.0,
            "position_count": 0,
        },
        "refresh_interval_seconds": config.refresh_interval_seconds,
        "fetched_at": _iso_now(),
        "last_synced_at": None,
        "data_source": "empty",
        "is_stale": True,
    }


def _persist_account_snapshot(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    payload: dict[str, Any],
) -> None:
    fetched_at = _parse_iso_datetime(payload.get("last_synced_at") or payload.get("fetched_at"))
    row = (
        db.query(QmtAccountSnapshotDB)
        .filter(
            QmtAccountSnapshotDB.user_id == user_id,
            QmtAccountSnapshotDB.account_key == config.key,
        )
        .first()
    )
    if row is None:
        row = QmtAccountSnapshotDB(
            id=uuid4().hex,
            user_id=user_id,
            account_key=config.key,
        )
        db.add(row)
    row.role = config.role
    row.account_id = config.account_id
    row.connection_json = dict(payload.get("connection") or {})
    row.account_json = dict(payload.get("account") or {}) if payload.get("account") else None
    row.positions_json = list(payload.get("positions") or [])
    row.orders_json = list(payload.get("orders") or [])
    row.trades_json = list(payload.get("trades") or [])
    row.summary_json = dict(payload.get("summary") or {})
    row.fetched_at = fetched_at
    _persist_account_equity_snapshot(db, user_id, config, payload, fetched_at=fetched_at)
    _persist_qmt_trade_history(db, user_id, config, payload, fetched_at=fetched_at)
    db.commit()


def _persist_account_equity_snapshot(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    payload: dict[str, Any],
    *,
    fetched_at: datetime | None,
) -> None:
    _ensure_qmt_equity_snapshot_schema(db)
    account = dict(payload.get("account") or {})
    summary = dict(payload.get("summary") or {})
    if not account and not summary:
        return
    snapshot_date = _qmt_snapshot_cn_date(fetched_at)
    row = (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == config.key,
            QmtAccountEquitySnapshotDB.snapshot_date == snapshot_date,
        )
        .first()
    )
    if row is None:
        row = QmtAccountEquitySnapshotDB(
            id=uuid4().hex,
            user_id=user_id,
            account_key=config.key,
            snapshot_date=snapshot_date,
        )
        db.add(row)
    row.role = str(account.get("role") or config.role or "").strip() or None
    row.account_id = str(account.get("account_id") or config.account_id or "").strip() or None
    row.total_asset = round(float(_to_float(account.get("total_asset"), summary.get("total_asset"), 0.0) or 0.0), 2)
    row.market_value = round(float(_to_float(account.get("market_value"), summary.get("market_value"), 0.0) or 0.0), 2)
    row.available_cash = round(float(_to_float(account.get("available_cash"), summary.get("available_cash"), 0.0) or 0.0), 2)
    row.total_pnl = round(float(_to_float(account.get("total_pnl"), summary.get("total_pnl"), 0.0) or 0.0), 2)
    row.total_pnl_pct = _to_float(account.get("total_pnl_pct"), summary.get("total_pnl_pct"))
    row.today_pnl = round(float(_to_float(account.get("today_pnl"), summary.get("today_pnl"), 0.0) or 0.0), 2)
    row.summary_json = summary
    row.fetched_at = fetched_at


def _ensure_equity_snapshot_from_latest_cache(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
) -> None:
    _ensure_qmt_equity_snapshot_schema(db)
    cached = (
        db.query(QmtAccountSnapshotDB)
        .filter(
            QmtAccountSnapshotDB.user_id == user_id,
            QmtAccountSnapshotDB.account_key == config.key,
        )
        .first()
    )
    if cached is None or not (cached.account_json or cached.summary_json):
        return
    payload = {
        "account": dict(cached.account_json or {}),
        "summary": dict(cached.summary_json or {}),
        "trades": list(cached.trades_json or []),
    }
    _persist_account_equity_snapshot(db, user_id, config, payload, fetched_at=cached.fetched_at)
    _persist_qmt_trade_history(db, user_id, config, payload, fetched_at=cached.fetched_at)
    db.commit()


def _ensure_qmt_equity_snapshot_schema(db: Session) -> None:
    bind = db.get_bind()
    bind_key = str(bind.url)
    if bind_key in _QMT_EQUITY_SCHEMA_READY_FOR:
        return
    QmtAccountEquitySnapshotDB.__table__.create(bind=bind, checkfirst=True)
    QmtAccountTradeHistoryDB.__table__.create(bind=bind, checkfirst=True)
    inspector = inspect(bind)
    columns = {column["name"] for column in inspector.get_columns(QmtAccountTradeHistoryDB.__tablename__)}
    for column_name, ddl in (
        ("cost_price", "ALTER TABLE qmt_account_trade_history ADD COLUMN cost_price DOUBLE PRECISION"),
        ("cost_basis", "ALTER TABLE qmt_account_trade_history ADD COLUMN cost_basis DOUBLE PRECISION"),
        ("realized_pnl", "ALTER TABLE qmt_account_trade_history ADD COLUMN realized_pnl DOUBLE PRECISION"),
        ("realized_pnl_pct", "ALTER TABLE qmt_account_trade_history ADD COLUMN realized_pnl_pct DOUBLE PRECISION"),
        ("pnl_status", "ALTER TABLE qmt_account_trade_history ADD COLUMN pnl_status VARCHAR(32)"),
    ):
        if column_name not in columns:
            db.execute(text(ddl))
    _QMT_EQUITY_SCHEMA_READY_FOR.add(bind_key)


def _persist_qmt_trade_history(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    payload: dict[str, Any],
    *,
    fetched_at: datetime | None,
) -> None:
    _ensure_qmt_equity_snapshot_schema(db)
    trades = list(payload.get("trades") or [])
    if not trades:
        return
    positions_by_symbol = {
        str(item.get("symbol") or "").strip().upper(): item
        for item in list(payload.get("positions") or [])
        if isinstance(item, dict) and item.get("symbol")
    }
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        symbol = str(trade.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        trade_uid = _qmt_trade_uid(config, trade)
        trade_time = _parse_iso_datetime(trade.get("trade_time"))
        trade_date = _qmt_snapshot_cn_date(trade_time or fetched_at)
        row = (
            db.query(QmtAccountTradeHistoryDB)
            .filter(
                QmtAccountTradeHistoryDB.user_id == user_id,
                QmtAccountTradeHistoryDB.account_key == config.key,
                QmtAccountTradeHistoryDB.trade_uid == trade_uid,
            )
            .first()
        )
        if row is None:
            row = QmtAccountTradeHistoryDB(
                id=uuid4().hex,
                user_id=user_id,
                account_key=config.key,
                trade_uid=trade_uid,
            )
            db.add(row)
        row.role = config.role
        row.account_id = config.account_id
        row.trade_id = str(trade.get("trade_id") or "").strip() or None
        row.order_id = str(trade.get("order_id") or "").strip() or None
        row.symbol = symbol
        row.name = str(trade.get("name") or "").strip() or None
        row.side = str(trade.get("side") or "").strip().lower() or None
        row.price = _to_float(trade.get("price"))
        row.quantity = _to_float(trade.get("quantity"))
        row.amount = _to_float(trade.get("amount"))
        pnl_context = _calculate_trade_realized_pnl(
            db,
            user_id,
            config,
            trade,
            positions_by_symbol=positions_by_symbol,
        )
        row.cost_price = pnl_context.get("cost_price")
        row.cost_basis = pnl_context.get("cost_basis")
        row.realized_pnl = pnl_context.get("realized_pnl")
        row.realized_pnl_pct = pnl_context.get("realized_pnl_pct")
        row.pnl_status = pnl_context.get("pnl_status")
        row.trade_time = trade_time
        row.trade_date = trade_date
        row.raw_json = dict(trade.get("raw") or trade)
        row.fetched_at = fetched_at


def _qmt_trade_uid(config: QmtRuntimeConfig, trade: dict[str, Any]) -> str:
    trade_id = str(trade.get("trade_id") or "").strip()
    if trade_id:
        return f"id:{trade_id}"[:96]
    fingerprint = "|".join(
        str(value or "").strip()
        for value in (
            config.key,
            trade.get("order_id"),
            trade.get("symbol"),
            trade.get("side"),
            trade.get("trade_time"),
            trade.get("price"),
            trade.get("quantity"),
            trade.get("amount"),
        )
    )
    return f"hash:{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()[:48]}"


def _calculate_trade_realized_pnl(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    trade: dict[str, Any],
    *,
    positions_by_symbol: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    side = str(trade.get("side") or "").strip().lower()
    quantity = _to_float(trade.get("quantity"))
    price = _to_float(trade.get("price"))
    amount = _to_float(trade.get("amount"))
    if amount is None and price is not None and quantity is not None:
        amount = round(float(price) * float(quantity), 2)
    if side == "buy":
        return {
            "cost_price": price,
            "cost_basis": amount,
            "realized_pnl": 0.0,
            "realized_pnl_pct": 0.0,
            "pnl_status": "buy_open",
        }
    if side != "sell" or quantity in (None, 0) or amount is None:
        return {
            "cost_price": None,
            "cost_basis": None,
            "realized_pnl": None,
            "realized_pnl_pct": None,
            "pnl_status": "unsupported",
        }

    cost_price = _resolve_trade_cost_price(db, user_id, config, trade, positions_by_symbol=positions_by_symbol)
    if cost_price is None or cost_price <= 0:
        return {
            "cost_price": None,
            "cost_basis": None,
            "realized_pnl": None,
            "realized_pnl_pct": None,
            "pnl_status": "cost_missing",
        }
    cost_basis = round(float(cost_price) * float(quantity), 2)
    realized_pnl = round(float(amount) - cost_basis, 2)
    realized_pnl_pct = round((realized_pnl / cost_basis) * 100, 2) if cost_basis > 0 else None
    return {
        "cost_price": round(float(cost_price), 4),
        "cost_basis": cost_basis,
        "realized_pnl": realized_pnl,
        "realized_pnl_pct": realized_pnl_pct,
        "pnl_status": "estimated",
    }


def _resolve_trade_cost_price(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    trade: dict[str, Any],
    *,
    positions_by_symbol: dict[str, dict[str, Any]],
) -> float | None:
    symbol = str(trade.get("symbol") or "").strip().upper()
    position = positions_by_symbol.get(symbol) or {}
    cost_price = _to_float(position.get("average_cost"), position.get("cost_price"), position.get("costPrice"))
    if cost_price is not None and cost_price > 0:
        return cost_price

    state = (
        db.query(VirtualPositionStateDB)
        .filter(
            VirtualPositionStateDB.user_id == user_id,
            VirtualPositionStateDB.broker == "qmt",
            VirtualPositionStateDB.account_id == config.account_id,
            VirtualPositionStateDB.symbol == symbol,
        )
        .first()
    )
    state_payload = dict(state.last_payload_json or {}) if state and isinstance(state.last_payload_json, dict) else {}
    cost_price = _to_float(
        state_payload.get("average_cost"),
        state_payload.get("cost_price"),
        state_payload.get("costPrice"),
        state_payload.get("cost_price_avg"),
        state_payload.get("open_price"),
    )
    if cost_price is not None and cost_price > 0:
        return cost_price

    buy_rows = (
        db.query(QmtAccountTradeHistoryDB)
        .filter(
            QmtAccountTradeHistoryDB.user_id == user_id,
            QmtAccountTradeHistoryDB.account_key == config.key,
            QmtAccountTradeHistoryDB.symbol == symbol,
            QmtAccountTradeHistoryDB.side == "buy",
        )
        .all()
    )
    buy_quantity = sum(float(row.quantity or 0.0) for row in buy_rows)
    buy_amount = sum(float(row.amount or 0.0) for row in buy_rows)
    if buy_quantity > 0 and buy_amount > 0:
        return round(buy_amount / buy_quantity, 4)
    return None


def _load_cached_payload(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    *,
    connection_override: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    row = (
        db.query(QmtAccountSnapshotDB)
        .filter(
            QmtAccountSnapshotDB.user_id == user_id,
            QmtAccountSnapshotDB.account_key == config.key,
        )
        .first()
    )
    if row is None:
        return None
    connection = dict(row.connection_json or {})
    cached_connected = bool(connection.get("connected"))
    if connection_override:
        override = dict(connection_override)
        override_connected = override.pop("connected", None)
        connection.update(override)
        if override_connected is True:
            cached_connected = True
        elif override_connected is False:
            cached_connected = cached_connected or False
    connection["connected"] = cached_connected
    base_message = str(connection_override.get("message") if connection_override else "").strip()
    if row.fetched_at:
        cached_label = row.fetched_at.astimezone(timezone.utc).isoformat()
        prefix = base_message or ("页面展示最近一次成功同步的 QMT 快照" if cached_connected else "QMT 当前不可用")
        connection["message"] = f"{prefix}，已回退到最近快照（{cached_label}）"
    else:
        prefix = base_message or ("页面展示最近一次成功同步的 QMT 快照" if cached_connected else "QMT 当前不可用")
        connection["message"] = f"{prefix}，已回退到本地缓存"
    return {
        "connection": connection,
        "account": dict(row.account_json or {}) if row.account_json else None,
        "positions": list(row.positions_json or []),
        "orders": list(row.orders_json or []),
        "trades": list(row.trades_json or []),
        "summary": dict(row.summary_json or {}),
        "refresh_interval_seconds": config.refresh_interval_seconds,
        "fetched_at": _iso_now(),
        "last_synced_at": row.fetched_at.isoformat() if row.fetched_at else None,
        "data_source": "cache",
        "is_stale": True,
    }


def _qmt_fetch_cache_key(user_id: str, account_key: str) -> str:
    return f"{user_id}:{account_key}"


def _recent_payload_ttl_seconds(config: QmtRuntimeConfig) -> float:
    return max(_QMT_RECENT_PAYLOAD_TTL_SECONDS, float(config.refresh_interval_seconds) + 2.0)


def _get_qmt_fetch_lock(cache_key: str) -> threading.Lock:
    with _QMT_FETCH_STATE_LOCK:
        lock = _QMT_FETCH_LOCKS.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _QMT_FETCH_LOCKS[cache_key] = lock
        return lock


def _schedule_qmt_background_refresh(user_id: str, account_key: str) -> bool:
    cache_key = _qmt_fetch_cache_key(user_id, account_key)
    with _QMT_FETCH_STATE_LOCK:
        current = _QMT_BACKGROUND_REFRESH_STATE.get(cache_key) or {}
        worker = current.get("thread")
        if worker is not None and getattr(worker, "is_alive", lambda: False)():
            return False
        thread = threading.Thread(
            target=_run_qmt_background_refresh,
            args=(user_id, account_key),
            daemon=True,
            name=f"qmt-bg-refresh-{account_key[:12]}",
        )
        _QMT_BACKGROUND_REFRESH_STATE[cache_key] = {
            "thread": thread,
            "started_at": time.time(),
            "finished_at": current.get("finished_at"),
            "last_error": None,
        }
    thread.start()
    return True


def _run_qmt_background_refresh(user_id: str, account_key: str) -> None:
    cache_key = _qmt_fetch_cache_key(user_id, account_key)
    config: QmtRuntimeConfig | None = None
    try:
        from api.database import get_db_ctx

        with get_db_ctx() as db:
            config = _resolve_runtime_config(account_key, db=db, user_id=user_id)
            recent_failure = _get_recent_fetch_failure(cache_key)
            if recent_failure:
                logger.info("[qmt] background refresh skipped for %s due to recent failure: %s", cache_key, recent_failure)
                with _QMT_FETCH_STATE_LOCK:
                    state = _QMT_BACKGROUND_REFRESH_STATE.get(cache_key) or {}
                    state["last_error"] = recent_failure
                    _QMT_BACKGROUND_REFRESH_STATE[cache_key] = state
                return
            if config.bridge_base_url:
                snapshot = run_async(_query_qmt_snapshot_via_bridge_async(config))
                _materialize_qmt_snapshot_payload(db, user_id, config, snapshot)
            else:
                _load_account_payload(db, user_id, config, prefer_cache=False, sync_to_imports=False)
        with _QMT_FETCH_STATE_LOCK:
            state = _QMT_BACKGROUND_REFRESH_STATE.get(cache_key) or {}
            state["last_error"] = None
            state["last_success_at"] = time.time()
            _QMT_BACKGROUND_REFRESH_STATE[cache_key] = state
    except Exception as exc:
        message = _compact_qmt_snapshot_error(exc, config)
        _remember_fetch_failure(cache_key, message)
        logger.warning("[qmt] background refresh failed for %s: %s", cache_key, message)
        with _QMT_FETCH_STATE_LOCK:
            state = _QMT_BACKGROUND_REFRESH_STATE.get(cache_key) or {}
            state["last_error"] = message
            _QMT_BACKGROUND_REFRESH_STATE[cache_key] = state
    finally:
        with _QMT_FETCH_STATE_LOCK:
            state = _QMT_BACKGROUND_REFRESH_STATE.get(cache_key)
            if state is not None:
                state["finished_at"] = time.time()
                state.pop("thread", None)


def _get_background_refresh_status(cache_key: str | None) -> dict[str, Any] | None:
    if not cache_key:
        return None
    with _QMT_FETCH_STATE_LOCK:
        state = dict(_QMT_BACKGROUND_REFRESH_STATE.get(cache_key) or {})
    return {
        "active": bool(state.get("thread") and getattr(state.get("thread"), "is_alive", lambda: False)()),
        "started_at": _epoch_to_iso(state.get("started_at")),
        "finished_at": _epoch_to_iso(state.get("finished_at")),
        "last_success_at": _epoch_to_iso(state.get("last_success_at")),
        "last_error": state.get("last_error"),
    }


def _epoch_to_iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    except Exception:
        return None


def _materialize_qmt_snapshot_payload(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    snapshot: dict[str, Any],
    *,
    sync_to_imports: bool = False,
) -> dict[str, Any]:
    security_name_map = _security_name_map_from_cache()
    positions = _build_position_items(db, user_id, config, snapshot.get("positions") or [], security_name_map)
    quote_map = _fetch_live_quotes([item["symbol"] for item in positions], account_key=config.key, db=db, user_id=user_id)
    positions = _apply_quote_metrics(
        positions,
        quote_map,
        prefer_position_market_value=str(config.role or "").strip().lower() == "live",
    )
    _sync_position_state(db, user_id, config.account_id, positions)
    if sync_to_imports:
        _sync_qmt_positions_to_imports(db, user_id, config.key, positions)
    account_payload = _build_account_payload(config, snapshot, positions)
    payload = {
        "connection": {
            "account_key": config.key,
            "role": config.role,
            "enabled": config.enabled,
            "provider": "xtquant",
            "host": config.host,
            "port": config.port,
            "account_id": config.account_id,
            "account_type": config.account_type,
            "account_name": config.account_name,
            "userdata_path": config.userdata_path,
            "bridge_base_url": config.bridge_base_url,
            "connected": True,
            "message": f"已连接 QMT {('模拟' if config.role == 'paper' else '实盘')}账户",
        },
        "account": account_payload,
        "positions": positions,
        "orders": _build_order_items(snapshot.get("orders") or [], security_name_map),
        "trades": _build_trade_items(snapshot.get("trades") or [], security_name_map),
        "summary": {
            "total_asset": account_payload["total_asset"],
            "total_pnl": account_payload["total_pnl"],
            "today_pnl": account_payload["today_pnl"],
            "market_value": account_payload["market_value"],
            "available_cash": account_payload["available_cash"],
            "position_count": len(positions),
        },
        "refresh_interval_seconds": config.refresh_interval_seconds,
        "fetched_at": _iso_now(),
        "last_synced_at": _iso_now(),
        "data_source": "live",
        "is_stale": False,
    }
    _persist_account_snapshot(db, user_id, config, payload)
    _remember_live_payload(_qmt_fetch_cache_key(user_id, config.key), payload)
    _clear_recent_fetch_failure(_qmt_fetch_cache_key(user_id, config.key))
    return payload


def _remember_live_payload(cache_key: str, payload: dict[str, Any]) -> None:
    with _QMT_FETCH_STATE_LOCK:
        _QMT_RECENT_PAYLOADS[cache_key] = {
            "stored_at": time.time(),
            "payload": copy.deepcopy(payload),
        }


def _get_recent_live_payload(cache_key: str, *, ttl_seconds: float) -> dict[str, Any] | None:
    now = time.time()
    with _QMT_FETCH_STATE_LOCK:
        entry = _QMT_RECENT_PAYLOADS.get(cache_key)
        if entry is None:
            return None
        stored_at = float(entry.get("stored_at") or 0)
        if now - stored_at > max(ttl_seconds, 0):
            _QMT_RECENT_PAYLOADS.pop(cache_key, None)
            return None
        return copy.deepcopy(entry.get("payload") or {})


def _remember_fetch_failure(cache_key: str, message: str) -> None:
    with _QMT_FETCH_STATE_LOCK:
        _QMT_RECENT_FAILURES[cache_key] = {
            "failed_at": time.time(),
            "message": str(message or "QMT 连接失败"),
        }


def _get_recent_fetch_failure(cache_key: str) -> str | None:
    now = time.time()
    with _QMT_FETCH_STATE_LOCK:
        entry = _QMT_RECENT_FAILURES.get(cache_key)
        if entry is None:
            return None
        failed_at = float(entry.get("failed_at") or 0)
        if now - failed_at > _QMT_FAILURE_COOLDOWN_SECONDS:
            _QMT_RECENT_FAILURES.pop(cache_key, None)
            return None
        return str(entry.get("message") or "QMT 连接失败")


def _clear_recent_fetch_failure(cache_key: str) -> None:
    with _QMT_FETCH_STATE_LOCK:
        _QMT_RECENT_FAILURES.pop(cache_key, None)


def _compact_qmt_snapshot_error(exc: Exception, config: QmtRuntimeConfig | None = None) -> str:
    message = str(exc) or exc.__class__.__name__
    lowered = message.lower()
    base_url = str(getattr(config, "bridge_base_url", "") or "").strip().rstrip("/")
    if base_url and _bridge_url_points_to_local_machine(base_url) and (
        "connection" in lowered
        or "max retries" in lowered
        or "failed to establish" in lowered
        or "timeout" in lowered
        or "timed out" in lowered
    ):
        return f"QMT bridge地址疑似配成当前后端本机地址：{base_url}；请改为 Windows bridge 的实际 IP。"
    if base_url and ("timeout" in lowered or "timed out" in lowered):
        return f"QMT bridge连接超时：{base_url}"
    if base_url and ("connection" in lowered or "max retries" in lowered or "failed to establish" in lowered):
        return f"QMT bridge不可达：{base_url}"
    return f"QMT 连接失败：{message}"[:240]


def _diagnose_single_account(config: QmtRuntimeConfig, *, run_connect_test: bool) -> dict[str, Any]:
    userdata_path_exists = bool(config.userdata_path) and os.path.exists(config.userdata_path)
    xtquant_installed, xtquant_message = _check_xtquant_available()
    tcp_reachable, tcp_message = _probe_tcp_port(config.host, config.port)
    bridge_reachable, bridge_message, bridge_health = _probe_bridge_health(config)
    bridge_role = str(bridge_health.get("role") or "").strip().lower()
    bridge_account_key = str(bridge_health.get("account_key") or "").strip()
    bridge_trading_allowed = _truthy(bridge_health.get("trading_allowed")) if bridge_health else False
    bridge_role_matches = bool(not bridge_role or bridge_role == str(config.role or "").strip().lower())
    bridge_account_key_matches = bool(not bridge_account_key or bridge_account_key == config.key)
    qmt_host_is_local = _host_points_to_local_machine(config.host)
    bridge_host = _bridge_url_host(config.bridge_base_url)
    bridge_host_is_local = _host_points_to_local_machine(bridge_host)
    checks = {
        "enabled": config.enabled,
        "account_id_configured": bool(config.account_id),
        "userdata_path_configured": bool(config.userdata_path),
        "userdata_path_exists": userdata_path_exists,
        "xtquant_installed": xtquant_installed,
        "tcp_port_reachable": tcp_reachable,
        "bridge_configured": bool(config.bridge_base_url),
        "bridge_reachable": bridge_reachable,
        "bridge_trading_allowed": bridge_trading_allowed,
        "bridge_role_matches": bridge_role_matches,
        "bridge_account_key_matches": bridge_account_key_matches,
        "qmt_host_is_local_machine": qmt_host_is_local,
        "bridge_host_is_local_machine": bridge_host_is_local,
    }
    warnings: list[str] = []
    if config.enabled and not config.account_id:
        warnings.append("缺少 account_id")
    if config.enabled and not config.userdata_path and not config.bridge_base_url:
        warnings.append("缺少 bridge_base_url / userdata_path")
    if config.enabled and config.userdata_path and not userdata_path_exists:
        warnings.append("userdata_path 不存在或当前运行环境无法访问")
    if config.enabled and not xtquant_installed and not config.bridge_base_url:
        warnings.append("xtquant 未安装")
    if config.enabled and not tcp_reachable:
        warnings.append("QMT 端口不可达")
    if config.enabled and config.bridge_base_url and not bridge_reachable:
        warnings.append("QMT bridge 不可达")
    if config.enabled and config.bridge_base_url and bridge_reachable and not bridge_role_matches:
        warnings.append(f"QMT bridge 角色不匹配：配置 {config.role}，实际 {bridge_role or 'unknown'}")
    if config.enabled and config.bridge_base_url and bridge_reachable and not bridge_account_key_matches:
        warnings.append(f"QMT bridge account_key 不匹配：配置 {config.key}，实际 {bridge_account_key}")
    if config.enabled and config.bridge_base_url and bridge_reachable and not bridge_trading_allowed:
        warnings.append("QMT bridge 当前只读，交易接口不可用")
    if config.enabled and qmt_host_is_local and not tcp_reachable:
        warnings.append(f"QMT host {config.host} 是当前后端机器本机地址，请改成 Windows QMT/bridge 的实际 IP")
    if config.enabled and config.bridge_base_url and bridge_host_is_local and not bridge_reachable:
        warnings.append(f"bridge_base_url 指向当前后端机器本机地址（{bridge_host}），请改成 Windows bridge 的实际 IP")

    connect_test = {
        "attempted": False,
        "connected": False,
        "message": "未执行连接测试",
    }
    can_connect = (
        config.enabled
        and checks["account_id_configured"]
        and (
            bool(config.bridge_base_url and bridge_reachable)
            or bool(config.userdata_path and userdata_path_exists and xtquant_installed)
        )
    )
    if run_connect_test and can_connect:
        connect_test = _run_connect_diagnostic(config)

    ready = (
        config.enabled
        and checks["account_id_configured"]
        and (
            bool(config.bridge_base_url and bridge_reachable)
            or bool(config.userdata_path and userdata_path_exists and xtquant_installed)
        )
    )
    return {
        "account_key": config.key,
        "role": config.role,
        "enabled": config.enabled,
        "account_id": config.account_id,
        "account_name": config.account_name,
        "host": config.host,
        "port": config.port,
        "userdata_path": config.userdata_path,
        "bridge_base_url": config.bridge_base_url,
        "ready": ready,
        "checks": checks,
        "warnings": warnings,
        "xtquant_message": xtquant_message,
        "tcp_probe": {
            "reachable": tcp_reachable,
            "message": tcp_message,
        },
        "bridge_probe": {
            "configured": bool(config.bridge_base_url),
            "reachable": bridge_reachable,
            "message": bridge_message,
            "health": bridge_health,
        },
        "trading_probe": {
            "configured": bool(config.bridge_base_url),
            "allowed": bridge_trading_allowed if config.bridge_base_url else True,
            "role": bridge_role or None,
            "account_key": bridge_account_key or None,
            "role_matches": bridge_role_matches,
            "account_key_matches": bridge_account_key_matches,
            "message": (
                "交易通道可用"
                if (not config.bridge_base_url or (bridge_reachable and bridge_trading_allowed and bridge_role_matches and bridge_account_key_matches))
                else "交易通道不可用，请检查 Windows bridge 角色、account_key 与 QMT_BRIDGE_ALLOW_TRADING"
            ),
        },
        "connect_test": connect_test,
    }


def _check_xtquant_available() -> tuple[bool, str]:
    try:
        import xtquant  # type: ignore

        version = getattr(xtquant, "__version__", None)
        return True, f"xtquant 已安装{f'，版本 {version}' if version else ''}"
    except Exception as exc:
        return False, f"xtquant 不可用：{exc}"


def _run_connect_diagnostic(config: QmtRuntimeConfig) -> dict[str, Any]:
    try:
        _query_qmt_snapshot(config)
        return {
            "attempted": True,
            "connected": True,
            "message": "连接成功，可读取账户资产与持仓",
        }
    except Exception as exc:
        logger.warning("[qmt] diagnostic connect failed for %s: %s", config.key, exc)
        return {
            "attempted": True,
            "connected": False,
            "message": f"连接失败：{exc}",
        }


def _probe_tcp_port(host: str, port: int, timeout: float = 1.5) -> tuple[bool, str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, int(port)))
        return True, f"{host}:{port} 可达"
    except Exception as exc:
        return False, str(exc)
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _bridge_url_host(base_url: str) -> str:
    text = str(base_url or "").strip()
    if not text:
        return ""
    try:
        return str(urlparse(text).hostname or "").strip()
    except Exception:
        return ""


def _bridge_url_points_to_local_machine(base_url: str) -> bool:
    return _host_points_to_local_machine(_bridge_url_host(base_url))


def _host_points_to_local_machine(host: str) -> bool:
    text = str(host or "").strip()
    if not text:
        return False
    try:
        infos = socket.getaddrinfo(text, None, socket.AF_INET, socket.SOCK_STREAM)
        addresses = [str(item[4][0]) for item in infos if item and item[4]]
    except Exception:
        addresses = [text]
    for address in dict.fromkeys(addresses):
        if _ipv4_address_belongs_to_local_machine(address):
            return True
    return False


def _ipv4_address_belongs_to_local_machine(address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(str(address or "").strip())
    except Exception:
        return False
    if parsed.version != 4:
        return False
    if parsed.is_loopback or parsed.is_unspecified:
        return True
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((str(parsed), 0))
        return True
    except OSError:
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _probe_bridge_health(config: QmtRuntimeConfig, timeout: float = 2.0) -> tuple[bool, str, dict[str, Any]]:
    base_url = str(config.bridge_base_url or "").strip().rstrip("/")
    if not base_url:
        return False, "未配置 bridge_base_url", {}
    try:
        payload = _fetch_qmt_bridge_health(config, timeout=timeout)
        role = str(payload.get("role") or "").strip() or "unknown"
        trading_allowed = _truthy(payload.get("trading_allowed"))
        account_key = str(payload.get("account_key") or "").strip()
        account_label = f" account_key={account_key}" if account_key else ""
        return True, f"{base_url}/health 可达 role={role} trading_allowed={trading_allowed}{account_label}", payload
    except Exception as exc:
        return False, str(exc), {}


def _probe_bridge(config: QmtRuntimeConfig, timeout: float = 2.0) -> tuple[bool, str]:
    reachable, message, _payload = _probe_bridge_health(config, timeout=timeout)
    return reachable, message


def _query_qmt_snapshot(config: QmtRuntimeConfig) -> dict[str, Any]:
    if config.bridge_base_url:
        return _query_qmt_snapshot_via_bridge(config)
    return _query_qmt_snapshot_via_local_xttrader(config)


def _submit_qmt_order(
    config: QmtRuntimeConfig,
    *,
    symbol: str,
    side: str,
    quantity: int,
    price: float | None,
    price_type: str,
    strategy_name: str | None,
    order_remark: str | None,
) -> dict[str, Any]:
    if config.bridge_base_url:
        return _submit_qmt_order_via_bridge(
            config,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            price_type=price_type,
            strategy_name=strategy_name,
            order_remark=order_remark,
        )
    return _submit_qmt_order_via_local_xttrader(
        config,
        symbol=symbol,
        side=side,
        quantity=quantity,
        price=price,
        price_type=price_type,
        strategy_name=strategy_name,
        order_remark=order_remark,
    )


def _cancel_qmt_order(config: QmtRuntimeConfig, *, order_id: str) -> dict[str, Any]:
    if config.bridge_base_url:
        return _cancel_qmt_order_via_bridge(config, order_id=order_id)
    return _cancel_qmt_order_via_local_xttrader(config, order_id=order_id)


def _query_qmt_snapshot_via_bridge(config: QmtRuntimeConfig) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").rstrip("/")
    if not base_url:
        raise RuntimeError("bridge_base_url 为空")
    response = requests.get(
        f"{base_url}/snapshot",
        params={"account_id": config.account_id, "account_type": config.account_type, "account_key": config.key},
        headers=_qmt_bridge_headers(config),
        timeout=_QMT_SNAPSHOT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    payload = payload if isinstance(payload, dict) else {"raw": payload}
    bridge = _bridge_metadata_payload(payload)
    _validate_qmt_snapshot_identity(config, payload)
    return {
        "fund": payload.get("fund") or {},
        "positions": payload.get("positions") or [],
        "asset": payload.get("asset") or {},
        "orders": payload.get("orders") or [],
        "trades": payload.get("trades") or [],
        "bridge": bridge,
    }


async def _query_qmt_snapshot_via_bridge_async(config: QmtRuntimeConfig) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").rstrip("/")
    if not base_url:
        raise RuntimeError("bridge_base_url 为空")
    async with httpx.AsyncClient(timeout=_QMT_SNAPSHOT_TIMEOUT_SECONDS) as client:
        response = await client.get(
            f"{base_url}/snapshot",
            params={"account_id": config.account_id, "account_type": config.account_type, "account_key": config.key},
            headers=_qmt_bridge_headers(config),
        )
        response.raise_for_status()
        payload = response.json()
    payload = payload if isinstance(payload, dict) else {"raw": payload}
    bridge = _bridge_metadata_payload(payload)
    _validate_qmt_snapshot_identity(config, payload)
    return {
        "fund": payload.get("fund") or {},
        "positions": payload.get("positions") or [],
        "asset": payload.get("asset") or {},
        "orders": payload.get("orders") or [],
        "trades": payload.get("trades") or [],
        "bridge": bridge,
    }


def _submit_qmt_order_via_bridge(
    config: QmtRuntimeConfig,
    *,
    symbol: str,
    side: str,
    quantity: int,
    price: float | None,
    price_type: str,
    strategy_name: str | None,
    order_remark: str | None,
) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").rstrip("/")
    if not base_url:
        raise RuntimeError("bridge_base_url 为空")
    response = requests.post(
        f"{base_url}/orders",
        json={
            "account_id": config.account_id,
            "account_type": config.account_type,
            "account_key": config.key,
            "symbol": str(symbol or "").strip().upper(),
            "side": side,
            "quantity": int(quantity),
            "price": float(price) if price is not None else None,
            "price_type": price_type,
            "strategy_name": strategy_name,
            "order_remark": order_remark,
        },
        headers=_qmt_bridge_headers(config),
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    payload = payload if isinstance(payload, dict) else {"raw": payload}
    bridge = _bridge_metadata_payload(payload)
    _validate_qmt_bridge_metadata(config, bridge, source="order_response")
    return {
        "success": bool(payload.get("success", True)),
        "order_id": str(payload.get("order_id") or ""),
        "result": payload.get("result"),
        "request": payload.get("request") or {},
        "bridge": bridge,
        "raw": payload,
    }


def _cancel_qmt_order_via_bridge(config: QmtRuntimeConfig, *, order_id: str) -> dict[str, Any]:
    base_url = str(config.bridge_base_url or "").rstrip("/")
    if not base_url:
        raise RuntimeError("bridge_base_url 为空")
    response = requests.post(
        f"{base_url}/orders/{order_id}/cancel",
        params={"account_id": config.account_id, "account_type": config.account_type, "account_key": config.key},
        headers=_qmt_bridge_headers(config),
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    payload = payload if isinstance(payload, dict) else {"raw": payload}
    bridge = _bridge_metadata_payload(payload)
    _validate_qmt_bridge_metadata(config, bridge, source="cancel_response")
    return {
        "success": bool(payload.get("success", True)),
        "order_id": str(payload.get("order_id") or order_id),
        "result": payload.get("result"),
        "bridge": bridge,
        "raw": payload,
    }


def _query_qmt_snapshot_via_local_xttrader(config: QmtRuntimeConfig) -> dict[str, Any]:
    from xtquant.xttrader import XtQuantTrader
    from xtquant.xttype import StockAccount

    session_id = int(time.time() * 1000) % 100000000
    trader = XtQuantTrader(config.userdata_path, session_id)
    account = StockAccount(config.account_id, config.account_type)
    start = getattr(trader, "start", None)
    if callable(start):
        start()
    connect_result = getattr(trader, "connect")()
    if connect_result not in (0, None):
        raise RuntimeError(f"connect 返回异常：{connect_result}")
    subscribe = getattr(trader, "subscribe", None)
    if callable(subscribe):
        subscribe(account)

    fund = None
    positions: list[Any] | None = None
    asset = None
    orders: list[Any] | None = None
    trades: list[Any] | None = None
    try:
        query_com_fund = getattr(trader, "query_com_fund", None)
        if callable(query_com_fund):
            fund = query_com_fund(account)
        query_com_position = getattr(trader, "query_com_position", None)
        if callable(query_com_position):
            positions = query_com_position(account)
        asset = trader.query_stock_asset(account)
        if positions in (None, []):
            positions = trader.query_stock_positions(account)
        query_stock_orders = getattr(trader, "query_stock_orders", None)
        if callable(query_stock_orders):
            orders = query_stock_orders(account)
        query_stock_trades = getattr(trader, "query_stock_trades", None)
        if callable(query_stock_trades):
            trades = query_stock_trades(account)
    finally:
        stop = getattr(trader, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                logger.debug("[qmt] trader.stop failed", exc_info=True)
    return {"fund": fund, "positions": positions or [], "asset": asset, "orders": orders or [], "trades": trades or []}


def _submit_qmt_order_via_local_xttrader(
    config: QmtRuntimeConfig,
    *,
    symbol: str,
    side: str,
    quantity: int,
    price: float | None,
    price_type: str,
    strategy_name: str | None,
    order_remark: str | None,
) -> dict[str, Any]:
    from xtquant import xtconstant
    from xtquant.xttrader import XtQuantTrader
    from xtquant.xttype import StockAccount

    symbol_value = str(symbol or "").strip().upper()
    side_key = str(side or "").strip().lower()
    if side_key in {"buy", "long_buy", "b"}:
        order_type = getattr(xtconstant, "STOCK_BUY", 23)
    elif side_key in {"sell", "long_sell", "s"}:
        order_type = getattr(xtconstant, "STOCK_SELL", 24)
    else:
        raise RuntimeError(f"不支持的 side: {side}")

    price_key = str(price_type or "limit").strip().lower()
    exchange = symbol_value.split(".")[-1] if "." in symbol_value else ""
    price_mode_map = {
        "limit": getattr(xtconstant, "FIX_PRICE", 11),
        "latest": getattr(xtconstant, "LATEST_PRICE", getattr(xtconstant, "FIX_PRICE", 11)),
        "opponent": getattr(xtconstant, "MARKET_PEER_PRICE_FIRST", getattr(xtconstant, "FIX_PRICE", 11)),
        "self_best": getattr(xtconstant, "MARKET_MINE_PRICE_FIRST", getattr(xtconstant, "FIX_PRICE", 11)),
        "best5_cancel": getattr(
            xtconstant,
            "MARKET_SH_CONVERT_5_CANCEL" if exchange == "SH" else "MARKET_SZ_CONVERT_5_CANCEL",
            getattr(xtconstant, "FIX_PRICE", 11),
        ),
    }
    if price_key not in price_mode_map:
        raise RuntimeError(f"不支持的 price_type: {price_type}")

    session_id = int(time.time() * 1000) % 100000000
    trader = XtQuantTrader(config.userdata_path, session_id)
    account = StockAccount(config.account_id, config.account_type)
    start = getattr(trader, "start", None)
    if callable(start):
        start()
    connect_result = getattr(trader, "connect")()
    if connect_result not in (0, None):
        raise RuntimeError(f"connect 返回异常：{connect_result}")
    subscribe = getattr(trader, "subscribe", None)
    if callable(subscribe):
        subscribe(account)
    try:
        order_stock = getattr(trader, "order_stock", None)
        if not callable(order_stock):
            raise RuntimeError("xttrader.order_stock 不可用")
        result = order_stock(
            account,
            symbol_value,
            order_type,
            int(quantity),
            price_mode_map[price_key],
            float(price or 0.0),
            str(strategy_name or "量化之神"),
            str(order_remark or ""),
        )
    finally:
        stop = getattr(trader, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                logger.debug("[qmt] trader.stop failed", exc_info=True)
    return {
        "success": True,
        "order_id": str(result),
        "result": result,
        "request": {
            "symbol": symbol_value,
            "side": side,
            "quantity": int(quantity),
            "price": price,
            "price_type": price_type,
            "strategy_name": strategy_name,
            "order_remark": order_remark,
        },
    }


def _cancel_qmt_order_via_local_xttrader(config: QmtRuntimeConfig, *, order_id: str) -> dict[str, Any]:
    from xtquant.xttrader import XtQuantTrader
    from xtquant.xttype import StockAccount

    session_id = int(time.time() * 1000) % 100000000
    trader = XtQuantTrader(config.userdata_path, session_id)
    account = StockAccount(config.account_id, config.account_type)
    start = getattr(trader, "start", None)
    if callable(start):
        start()
    connect_result = getattr(trader, "connect")()
    if connect_result not in (0, None):
        raise RuntimeError(f"connect 返回异常：{connect_result}")
    subscribe = getattr(trader, "subscribe", None)
    if callable(subscribe):
        subscribe(account)
    try:
        cancel_order_stock = getattr(trader, "cancel_order_stock", None)
        if not callable(cancel_order_stock):
            raise RuntimeError("xttrader.cancel_order_stock 不可用")
        cancel_arg: Any = int(order_id) if str(order_id).isdigit() else order_id
        result = cancel_order_stock(account, cancel_arg)
    finally:
        stop = getattr(trader, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception:
                logger.debug("[qmt] trader.stop failed", exc_info=True)
    return {
        "success": True,
        "order_id": str(order_id),
        "result": result,
    }


def _build_position_items(
    db: Session,
    user_id: str,
    config: QmtRuntimeConfig,
    raw_positions: list[Any],
    security_name_map: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    tracked_states = {
        row.symbol: row
        for row in db.query(VirtualPositionStateDB).filter(
            VirtualPositionStateDB.user_id == user_id,
            VirtualPositionStateDB.broker == "qmt",
            VirtualPositionStateDB.account_id == config.account_id,
        ).all()
    }
    items: list[dict[str, Any]] = []
    for raw in raw_positions:
        try:
            payload = raw if isinstance(raw, dict) else _object_to_dict(raw)
            symbol = _normalize_symbol(payload.get("stockCode") or payload.get("stock_code"))
            quantity = _to_float(payload.get("totalAmt"), payload.get("volume"))
            if not symbol or quantity in (None, 0):
                continue
            available = _to_float(payload.get("enableAmount"), payload.get("can_use_volume"))
            current_price = _to_float(payload.get("lastPrice"), payload.get("last_price"), payload.get("price"))
            avg_price = _to_float(
                payload.get("costPrice"),
                payload.get("avg_price"),
                payload.get("open_price"),
                payload.get("m_dAvgPrice"),
                payload.get("m_dOpenPrice"),
            )
            market_value = _to_float(payload.get("marketValue"), payload.get("market_value"), payload.get("m_dMarketValue"))
            price_source = "qmt_position_price" if current_price is not None else None
            if current_price is None and market_value is not None and quantity not in (None, 0):
                current_price = round(float(market_value) / float(quantity), 4)
                price_source = "qmt_position_market_value"
            if market_value is None and current_price is not None and quantity is not None:
                market_value = round(current_price * quantity, 2)
            total_pnl = _to_float(payload.get("income"), payload.get("position_profit"), payload.get("floating_pnl"))
            if total_pnl is None and current_price is not None and avg_price is not None:
                total_pnl = round((current_price - avg_price) * quantity, 2)
            total_pnl_pct = None
            if current_price is not None and avg_price not in (None, 0):
                total_pnl_pct = round(((current_price - avg_price) / avg_price) * 100, 2)
            state = tracked_states.get(symbol)
            first_seen_at = state.first_seen_at if state and (state.last_quantity or 0) > 0 else datetime.now(timezone.utc)
            holding_days = max((datetime.now(timezone.utc).date() - first_seen_at.date()).days + 1, 1)
            break_even_rise_pct = 0.0
            if current_price not in (None, 0) and avg_price and current_price < avg_price:
                break_even_rise_pct = round(((avg_price / current_price) - 1) * 100, 2)
            yesterday_position = _to_float(payload.get("yesterday_volume"), payload.get("m_nYesterdayVolume"))
            items.append(
                {
                    "symbol": symbol,
                    "name": _resolve_security_name(payload, symbol, security_name_map),
                    "account_id": config.account_id,
                    "current_position": round(float(quantity), 2),
                    "available_position": round(float(available or 0.0), 2),
                    "yesterday_position": round(float(yesterday_position), 2) if yesterday_position is not None else None,
                    "average_cost": round(float(avg_price or 0.0), 4),
                    "current_price": round(float(current_price or 0.0), 4) if current_price is not None else None,
                    "price_source": price_source,
                    "market_value": round(float(market_value or 0.0), 2),
                    "total_pnl": round(float(total_pnl or 0.0), 2),
                    "total_pnl_pct": total_pnl_pct,
                    "today_pnl": None,
                    "today_pnl_pct": None,
                    "holding_days": holding_days,
                    "break_even_rise_pct": break_even_rise_pct,
                    "position_pct": None,
                    "raw": payload,
                }
            )
        except Exception:
            logger.exception("[qmt] build position item failed for account=%s raw=%s", config.key, raw)
            continue
    total_market_value = sum(float(item["market_value"] or 0.0) for item in items)
    if total_market_value > 0:
        for item in items:
            item["position_pct"] = round((float(item["market_value"] or 0.0) / total_market_value) * 100, 2)
    items.sort(key=lambda item: float(item.get("market_value") or 0.0), reverse=True)
    return items


def _build_order_items(raw_orders: list[Any], security_name_map: dict[str, str] | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw in raw_orders:
        payload = raw if isinstance(raw, dict) else _object_to_dict(raw)
        symbol = _normalize_symbol(_first_present(payload, "stockCode", "stock_code", "symbol", "m_strStockCode"))
        if not symbol:
            continue
        order_time = _normalize_qmt_time(_first_present(payload, "orderTime", "insert_time", "order_time", "created_at", "m_nOrderTime"))
        quantity = _to_float(
            payload.get("orderVolume"),
            payload.get("order_volume"),
            payload.get("volume"),
            payload.get("orderQty"),
            payload.get("m_nOrderVolume"),
        )
        filled_quantity = _to_float(
            payload.get("tradedVolume"),
            payload.get("traded_volume"),
            payload.get("business_amount"),
            payload.get("filled_quantity"),
            payload.get("m_nTradedVolume"),
        )
        price = _to_float(payload.get("orderPrice"), payload.get("price"), payload.get("m_dPrice"))
        amount = _to_float(payload.get("orderAmount"), payload.get("amount"))
        if amount is None and price is not None and quantity is not None:
            amount = round(float(price) * float(quantity), 2)
        status = _normalize_order_status(payload, quantity=quantity, filled_quantity=filled_quantity)
        items.append(
            {
                "order_id": str(_first_present(payload, "orderId", "order_id", "entrust_no", "m_nOrderID") or ""),
                "symbol": symbol,
                "name": _resolve_security_name(payload, symbol, security_name_map),
                "side": _normalize_side(_first_present(payload, "orderType", "order_type", "side", "entrust_bs", "m_nOrderType")),
                "status": status,
                "price": price,
                "quantity": quantity,
                "filled_quantity": filled_quantity,
                "amount": amount,
                "order_time": order_time,
                "can_cancel": _is_order_cancelable(payload, status=status, quantity=quantity, filled_quantity=filled_quantity),
                "raw": payload,
            }
        )
    items.sort(key=lambda item: str(item.get("order_time") or ""), reverse=True)
    return items[:50]


def _build_trade_items(raw_trades: list[Any], security_name_map: dict[str, str] | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw in raw_trades:
        payload = raw if isinstance(raw, dict) else _object_to_dict(raw)
        symbol = _normalize_symbol(_first_present(payload, "stockCode", "stock_code", "symbol", "m_strStockCode"))
        if not symbol:
            continue
        trade_time = _normalize_qmt_time(_first_present(payload, "tradedTime", "traded_time", "business_time", "executed_at", "m_nTradedTime"))
        quantity = _to_float(payload.get("tradedVolume"), payload.get("traded_volume"), payload.get("volume"), payload.get("business_amount"), payload.get("m_nTradedVolume"))
        price = _to_float(payload.get("tradedPrice"), payload.get("traded_price"), payload.get("price"), payload.get("business_price"), payload.get("m_dTradedPrice"))
        amount = _to_float(payload.get("tradedAmount"), payload.get("traded_amount"), payload.get("amount"), payload.get("business_balance"), payload.get("m_dTradedAmount"))
        items.append(
            {
                "trade_id": str(_first_present(payload, "tradedId", "traded_id", "traded_id1", "trade_id", "business_no", "m_strTradedID", "m_strTradedIDNew") or ""),
                "order_id": str(_first_present(payload, "orderId", "order_id", "entrust_no", "m_nOrderID") or ""),
                "symbol": symbol,
                "name": _resolve_security_name(payload, symbol, security_name_map),
                "side": _normalize_side(_first_present(payload, "orderType", "order_type", "side", "entrust_bs", "m_nOrderType")),
                "price": price,
                "quantity": quantity,
                "amount": amount if amount is not None else round(float(quantity or 0.0) * float(price or 0.0), 2) if quantity is not None and price is not None else None,
                "trade_time": trade_time,
                "raw": payload,
            }
        )
    items.sort(key=lambda item: str(item.get("trade_time") or ""), reverse=True)
    return items[:50]


def _is_order_cancelable(
    payload: dict[str, Any],
    *,
    status: str | None = None,
    quantity: float | None = None,
    filled_quantity: float | None = None,
) -> bool:
    order_id = str(_first_present(payload, "orderId", "order_id", "entrust_no", "m_nOrderID") or "").strip()
    if not order_id:
        return False
    if quantity is not None and filled_quantity is not None and quantity > 0 and filled_quantity >= quantity:
        return False
    status_text = str(status or _first_present(payload, "orderStatus", "status", "status_name", "order_status", "m_nOrderStatus") or "").strip().lower()
    if not status_text:
        return True
    terminal_keywords = ("filled", "cancel", "rejected", "invalid", "expired", "done", "success_all", "废", "撤", "成")
    return not any(keyword in status_text for keyword in terminal_keywords)


def _apply_quote_metrics(
    items: list[dict[str, Any]],
    quote_map: dict[str, dict[str, Any]],
    *,
    prefer_position_market_value: bool = False,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for item in items:
        try:
            quote = quote_map.get(item["symbol"]) or {}
            resolved_name = _resolve_security_name(quote, item["symbol"], _security_name_map_from_cache())
            quote_price = _to_float(quote.get("price"))
            position_price = _to_float(item.get("current_price"))
            current_price = (
                _to_float(position_price, quote_price)
                if prefer_position_market_value
                else _to_float(quote_price, position_price)
            )
            previous_close = _to_float(quote.get("previous_close"))
            change_price = _to_float(quote_price, current_price)
            price_change = _to_float(quote.get("change"))
            if price_change is None and change_price is not None and previous_close not in (None, 0):
                price_change = round(change_price - previous_close, 4)
            pnl_quantity = _to_float(item.get("yesterday_position"), item.get("current_position"), 0.0)
            today_pnl = round(float(price_change or 0.0) * float(pnl_quantity or 0.0), 2) if price_change is not None else None
            today_pnl_pct = _to_float(quote.get("change_pct"))
            if today_pnl_pct is None and price_change is not None and previous_close not in (None, 0):
                today_pnl_pct = round((float(price_change) / float(previous_close)) * 100, 4)
            total_pnl = item.get("total_pnl")
            total_pnl_pct = _to_float(item.get("total_pnl_pct"))
            avg_price = _to_float(item.get("average_cost"))
            if current_price is not None and avg_price not in (None, 0):
                total_pnl = round((current_price - avg_price) * float(item.get("current_position") or 0.0), 2)
                total_pnl_pct = round(((current_price - avg_price) / avg_price) * 100, 2)
                item["break_even_rise_pct"] = round(max((avg_price / current_price) - 1, 0) * 100, 2) if current_price > 0 else None
            market_value = item.get("market_value")
            if current_price is not None and (not prefer_position_market_value or market_value in (None, 0)):
                market_value = round(current_price * float(item.get("current_position") or 0.0), 2)
            enriched.append(
                {
                    **item,
                    "name": resolved_name if resolved_name and not _looks_like_symbol(resolved_name) else item.get("name"),
                    "current_price": round(float(current_price), 4) if current_price is not None else item.get("current_price"),
                    "price_source": item.get("price_source") if prefer_position_market_value and item.get("price_source") else quote.get("source"),
                    "market_value": market_value,
                    "total_pnl": total_pnl,
                    "total_pnl_pct": total_pnl_pct,
                    "today_pnl": today_pnl,
                    "today_pnl_pct": today_pnl_pct,
                    "previous_close": previous_close,
                    "quote_time": quote.get("quote_time"),
                    "quote_source": quote.get("source"),
                }
            )
        except Exception:
            logger.exception("[qmt] apply quote metrics failed for symbol=%s", item.get("symbol"))
            enriched.append({**item, "quote_source": item.get("quote_source") or "fallback"})
    return enriched


def _build_account_payload(
    config: QmtRuntimeConfig,
    snapshot: dict[str, Any],
    positions: list[dict[str, Any]],
) -> dict[str, Any]:
    fund = snapshot.get("fund") or {}
    asset = snapshot.get("asset")
    asset_payload = _object_to_dict(asset) if asset is not None else {}
    total_asset = _to_float(fund.get("assetBalance"), asset_payload.get("total_asset"))
    market_value = _to_float(fund.get("marketValue"), asset_payload.get("market_value"))
    available_cash = _to_float(fund.get("enableBalance"), asset_payload.get("cash"))
    if market_value is None:
        market_value = round(sum(float(item.get("market_value") or 0.0) for item in positions), 2)
    if available_cash is None:
        available_cash = 0.0
    if total_asset is None:
        total_asset = round(float(available_cash) + float(market_value or 0.0), 2)
    total_pnl = round(sum(float(item.get("total_pnl") or 0.0) for item in positions), 2)
    today_pnl = round(sum(float(item.get("today_pnl") or 0.0) for item in positions if item.get("today_pnl") is not None), 2)
    total_cost = sum(float(item.get("average_cost") or 0.0) * float(item.get("current_position") or 0.0) for item in positions)
    total_pnl_pct = round((total_pnl / total_cost) * 100, 2) if total_cost > 0 else 0.0
    return {
        "account_key": config.key,
        "role": config.role,
        "broker": "QMT",
        "mode": "极简模式 / Python 策略端",
        "account_name": config.account_name,
        "account_id": config.account_id,
        "security_account_name": config.account_name,
        "total_asset": round(float(total_asset or 0.0), 2),
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "today_pnl": today_pnl,
        "market_value": round(float(market_value or 0.0), 2),
        "available_cash": round(float(available_cash or 0.0), 2),
        "position_count": len(positions),
    }


def _sync_position_state(
    db: Session,
    user_id: str,
    account_id: str,
    positions: list[dict[str, Any]],
) -> None:
    now = datetime.now(timezone.utc)
    rows = db.query(VirtualPositionStateDB).filter(
        VirtualPositionStateDB.user_id == user_id,
        VirtualPositionStateDB.broker == "qmt",
        VirtualPositionStateDB.account_id == account_id,
    ).all()
    state_map = {row.symbol: row for row in rows}
    active_symbols = {item["symbol"] for item in positions}

    for item in positions:
        row = state_map.get(item["symbol"])
        if row is None:
            row = VirtualPositionStateDB(
                id=uuid4().hex,
                user_id=user_id,
                broker="qmt",
                account_id=account_id,
                symbol=item["symbol"],
                first_seen_at=now,
                created_at=now,
            )
            db.add(row)
        elif not row.first_seen_at or (row.last_quantity or 0) <= 0:
            row.first_seen_at = now
        row.last_seen_at = now
        row.last_quantity = float(item.get("current_position") or 0.0)
        row.last_price = _to_float(item.get("current_price"))
        row.last_market_value = _to_float(item.get("market_value"))
        row.last_payload_json = dict(item)

    for row in rows:
        if row.symbol in active_symbols:
            continue
        row.last_seen_at = now
        row.last_quantity = 0.0

    db.commit()


def _sync_qmt_positions_to_imports(db: Session, user_id: str, account_key: str, positions: list[dict[str, Any]]) -> None:
    config = _resolve_runtime_config(account_key, db=db, user_id=user_id)
    source = _source_name(account_key, config.role)
    payload = [
        {
            "symbol": item["symbol"],
            "name": item.get("name"),
            "current_position": item.get("current_position"),
            "available_position": item.get("available_position"),
            "average_cost": item.get("average_cost"),
            "market_value": item.get("market_value"),
            "current_position_pct": item.get("position_pct"),
        }
        for item in positions
    ]
    if not payload:
        db.query(ImportedPortfolioPositionDB).filter(
            ImportedPortfolioPositionDB.user_id == user_id,
            ImportedPortfolioPositionDB.source == source,
        ).delete()
        db.commit()
        return
    portfolio_import_service.sync_positions(
        db=db,
        user_id=user_id,
        positions=payload,
        source=source,
        auto_apply_scheduled=True,
    )


def _source_name(account_key: str, role: str = "paper") -> str:
    key = (account_key or "qmt_default").strip() or "qmt_default"
    prefix = "qmt_live" if str(role or "").strip().lower() == "live" else SOURCE_NAME
    return f"{prefix}:{key}"


def _fetch_live_quotes(
    symbols: list[str],
    *,
    account_key: str | None = None,
    timeout_seconds: float | None = None,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    try:
        from api.services.qmt_market_data_service import fetch_realtime_quotes

        return fetch_realtime_quotes(
            symbols,
            account_key=account_key or settings.qmt_history_account_key,
            timeout_seconds=timeout_seconds,
            db=db,
            user_id=user_id,
        )
    except Exception as exc:
        logger.warning("[qmt] realtime quote fetch failed: %s", exc)
        return {}


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


def _security_name_map_from_cache() -> dict[str, str]:
    try:
        return get_reverse_stock_map_cached_only()
    except Exception:
        logger.debug("[qmt] get stock name cache failed", exc_info=True)
        return {}


def _resolve_security_name(
    payload: dict[str, Any],
    symbol: str,
    security_name_map: dict[str, str] | None = None,
) -> str:
    for key in (
        "stockName",
        "stock_name",
        "security_name",
        "name",
        "instrument_name",
        "InstrumentName",
        "m_strStockName",
        "m_strInstrumentName",
    ):
        value = str(payload.get(key) or "").strip()
        if value and not _looks_like_symbol(value):
            return value
    name_map = security_name_map or {}
    normalized_symbol = _normalize_symbol(symbol) or symbol
    code = normalized_symbol.split(".", 1)[0]
    return (
        name_map.get(normalized_symbol)
        or name_map.get(code)
        or name_map.get(str(symbol or "").strip().upper())
        or normalized_symbol
    )


def _looks_like_symbol(value: str) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    if text.isdigit() and len(text) == 6:
        return True
    if len(text) == 9 and text[:6].isdigit() and text[6:] in (".SH", ".SZ", ".BJ"):
        return True
    return False


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _normalize_side(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"23", "buy", "b", "买入"}:
        return "buy"
    if text in {"24", "sell", "s", "卖出"}:
        return "sell"
    return text or "unknown"


def _normalize_order_status(
    payload: dict[str, Any],
    *,
    quantity: float | None = None,
    filled_quantity: float | None = None,
) -> str:
    status_value = _first_present(payload, "orderStatus", "status", "status_name", "order_status", "m_nOrderStatus")
    status_text = str(status_value or "").strip().lower()
    if quantity is not None and filled_quantity is not None and quantity > 0:
        if filled_quantity >= quantity:
            return "filled"
        if filled_quantity > 0:
            return "partially_filled"
    if not status_text:
        return "unknown"
    if status_text in {"filled", "submitted", "cancelled", "rejected", "partially_filled"}:
        return status_text
    numeric_status = _safe_int(status_text)
    if numeric_status is not None:
        mapping = {
            48: "pending",
            49: "pending",
            50: "submitted",
            51: "submitted",
            52: "partially_filled",
            53: "partially_filled",
            54: "cancelled",
            55: "cancelled",
            56: "filled",
            57: "rejected",
        }
        return mapping.get(numeric_status, str(numeric_status))
    if any(keyword in status_text for keyword in ("filled", "已成", "全成", "success_all")):
        return "filled"
    if any(keyword in status_text for keyword in ("partial", "部成")):
        return "partially_filled"
    if any(keyword in status_text for keyword in ("cancel", "已撤", "撤单")):
        return "cancelled"
    if any(keyword in status_text for keyword in ("reject", "废", "invalid")):
        return "rejected"
    return status_text


def _normalize_qmt_time(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(CN_TZ).isoformat() if value.tzinfo else value.replace(tzinfo=CN_TZ).isoformat()
    text = str(value).strip()
    if not text:
        return None
    digits = text.replace(".", "", 1)
    if digits.isdigit():
        try:
            number = float(text)
            if number > 1_000_000_000_000:
                return datetime.fromtimestamp(number / 1000, tz=CN_TZ).isoformat()
            if number > 1_000_000_000:
                return datetime.fromtimestamp(number, tz=CN_TZ).isoformat()
        except Exception:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=CN_TZ).isoformat()
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(CN_TZ).isoformat() if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ).isoformat()
    except Exception:
        return text


def _safe_int(value: Any) -> int | None:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _object_to_dict(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return dict(obj)
    data: dict[str, Any] = {}
    for key in dir(obj):
        if key.startswith("_"):
            continue
        try:
            value = getattr(obj, key)
        except Exception:
            continue
        if callable(value):
            continue
        if isinstance(value, (str, int, float, bool, dict, list)) or value is None:
            data[key] = value
    return data


def _to_float(*values: Any) -> float | None:
    for value in values:
        if value is None or value == "":
            continue
        try:
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                continue
            return number
        except Exception:
            continue
    return None


def _normalize_order_quantity(value: Any) -> int:
    try:
        quantity = max(float(value or 0), 0.0)
    except Exception:
        return 0
    return int(quantity // 100 * 100)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _qmt_snapshot_cn_date(value: datetime | None) -> date:
    if value is None:
        return datetime.now(CN_TZ).date()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(CN_TZ).date()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _latest_equity_snapshot(
    db: Session,
    user_id: str,
    account_key: str,
) -> QmtAccountEquitySnapshotDB | None:
    return (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == account_key,
        )
        .order_by(
            QmtAccountEquitySnapshotDB.snapshot_date.desc(),
            QmtAccountEquitySnapshotDB.fetched_at.desc(),
            QmtAccountEquitySnapshotDB.updated_at.desc(),
        )
        .first()
    )


def _previous_equity_snapshot(
    db: Session,
    user_id: str,
    account_key: str,
    snapshot_date: date,
) -> QmtAccountEquitySnapshotDB | None:
    return (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == account_key,
            QmtAccountEquitySnapshotDB.snapshot_date < snapshot_date,
        )
        .order_by(
            QmtAccountEquitySnapshotDB.snapshot_date.desc(),
            QmtAccountEquitySnapshotDB.fetched_at.desc(),
        )
        .first()
    )


def _period_baseline_equity_snapshot(
    db: Session,
    user_id: str,
    account_key: str,
    *,
    period_start: date,
    current_date: date,
) -> tuple[QmtAccountEquitySnapshotDB | None, bool]:
    previous = (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == account_key,
            QmtAccountEquitySnapshotDB.snapshot_date < period_start,
        )
        .order_by(
            QmtAccountEquitySnapshotDB.snapshot_date.desc(),
            QmtAccountEquitySnapshotDB.fetched_at.desc(),
        )
        .first()
    )
    if previous is not None:
        return previous, True
    earliest = (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == account_key,
            QmtAccountEquitySnapshotDB.snapshot_date >= period_start,
            QmtAccountEquitySnapshotDB.snapshot_date <= current_date,
        )
        .order_by(
            QmtAccountEquitySnapshotDB.snapshot_date.asc(),
            QmtAccountEquitySnapshotDB.fetched_at.asc(),
        )
        .first()
    )
    return earliest, False


def _build_return_period(
    db: Session,
    user_id: str,
    account_key: str,
    current: QmtAccountEquitySnapshotDB | None,
    *,
    key: str,
    label: str,
    period_start: date,
    allow_today_pnl_fallback: bool = False,
) -> dict[str, Any]:
    if current is None:
        return {
            "key": key,
            "label": label,
            "amount": None,
            "rate": None,
            "baseline_asset": None,
            "current_asset": None,
            "start_date": period_start.isoformat(),
            "end_date": period_start.isoformat(),
            "coverage": "empty",
            "coverage_label": "暂无资产快照",
        }

    current_asset = round(float(current.total_asset or 0.0), 2)
    baseline: QmtAccountEquitySnapshotDB | None
    coverage = "full"
    if key == "day":
        baseline = _previous_equity_snapshot(db, user_id, account_key, current.snapshot_date)
        if baseline is None and allow_today_pnl_fallback:
            amount = round(float(current.today_pnl or 0.0), 2)
            baseline_asset = round(current_asset - amount, 2)
            rate = _calculate_return_rate(amount, baseline_asset)
            return {
                "key": key,
                "label": label,
                "amount": amount,
                "rate": rate,
                "baseline_asset": baseline_asset if baseline_asset > 0 else None,
                "current_asset": current_asset,
                "start_date": current.snapshot_date.isoformat(),
                "end_date": current.snapshot_date.isoformat(),
                "coverage": "fallback",
                "coverage_label": "暂无上一快照，已按当日盈亏估算",
            }
    else:
        baseline, has_period_anchor = _period_baseline_equity_snapshot(
            db,
            user_id,
            account_key,
            period_start=period_start,
            current_date=current.snapshot_date,
        )
        coverage = "full" if has_period_anchor else "partial"

    if baseline is None:
        return {
            "key": key,
            "label": label,
            "amount": None,
            "rate": None,
            "baseline_asset": None,
            "current_asset": current_asset,
            "start_date": period_start.isoformat(),
            "end_date": current.snapshot_date.isoformat(),
            "coverage": "empty",
            "coverage_label": "数据沉淀中",
        }

    baseline_asset = round(float(baseline.total_asset or 0.0), 2)
    amount = round(current_asset - baseline_asset, 2)
    return {
        "key": key,
        "label": label,
        "amount": amount,
        "rate": _calculate_return_rate(amount, baseline_asset),
        "baseline_asset": baseline_asset,
        "current_asset": current_asset,
        "start_date": baseline.snapshot_date.isoformat(),
        "end_date": current.snapshot_date.isoformat(),
        "coverage": coverage,
        "coverage_label": "完整统计" if coverage == "full" else "数据沉淀中",
    }


def _calculate_return_rate(amount: float | None, baseline_asset: float | None) -> float | None:
    if amount is None or baseline_asset is None or baseline_asset <= 0:
        return None
    return round((float(amount) / float(baseline_asset)) * 100, 2)


def _build_return_calendar(
    db: Session,
    user_id: str,
    account_key: str,
    *,
    current_date: date,
) -> dict[str, Any]:
    month_start = current_date.replace(day=1)
    month_end = current_date.replace(day=calendar.monthrange(current_date.year, current_date.month)[1])
    rows = (
        db.query(QmtAccountEquitySnapshotDB)
        .filter(
            QmtAccountEquitySnapshotDB.user_id == user_id,
            QmtAccountEquitySnapshotDB.account_key == account_key,
            QmtAccountEquitySnapshotDB.snapshot_date >= month_start,
            QmtAccountEquitySnapshotDB.snapshot_date <= month_end,
        )
        .order_by(QmtAccountEquitySnapshotDB.snapshot_date.asc())
        .all()
    )
    previous = _previous_equity_snapshot(db, user_id, account_key, month_start)
    rows_by_date = {row.snapshot_date: row for row in rows}
    daily_values: dict[date, dict[str, Any]] = {}
    rolling_previous = previous
    max_abs_amount = 0.0
    for row in rows:
        current_asset = round(float(row.total_asset or 0.0), 2)
        if rolling_previous is not None:
            baseline_asset = round(float(rolling_previous.total_asset or 0.0), 2)
            amount = round(current_asset - baseline_asset, 2)
            coverage = "full"
            coverage_label = "完整统计"
        else:
            amount = round(float(row.today_pnl or 0.0), 2)
            baseline_asset = round(current_asset - amount, 2)
            coverage = "fallback"
            coverage_label = "按当日盈亏估算"
        rate = _calculate_return_rate(amount, baseline_asset)
        max_abs_amount = max(max_abs_amount, abs(amount))
        daily_values[row.snapshot_date] = {
            "date": row.snapshot_date.isoformat(),
            "amount": amount,
            "rate": rate,
            "baseline_asset": baseline_asset if baseline_asset > 0 else None,
            "current_asset": current_asset,
            "coverage": coverage,
            "coverage_label": coverage_label,
            "has_snapshot": True,
            "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
        }
        rolling_previous = row

    days: list[dict[str, Any]] = []
    cursor = month_start
    while cursor <= month_end:
        item = daily_values.get(cursor)
        if item is None:
            item = {
                "date": cursor.isoformat(),
                "amount": None,
                "rate": None,
                "baseline_asset": None,
                "current_asset": None,
                "coverage": "empty",
                "coverage_label": "暂无快照",
                "has_snapshot": False,
                "fetched_at": None,
            }
        amount = item.get("amount")
        intensity = 0.0
        if amount is not None and max_abs_amount > 0:
            intensity = round(min(abs(float(amount)) / max_abs_amount, 1.0), 4)
        item.update(
            {
                "day": cursor.day,
                "weekday": cursor.weekday(),
                "intensity": intensity,
                "tone": "gain" if (amount or 0) > 0 else "loss" if (amount or 0) < 0 else "flat" if item.get("has_snapshot") else "empty",
            }
        )
        days.append(item)
        cursor += timedelta(days=1)

    return {
        "year": current_date.year,
        "month": current_date.month,
        "month_label": f"{current_date.year}年{current_date.month:02d}月",
        "start_date": month_start.isoformat(),
        "end_date": month_end.isoformat(),
        "max_abs_amount": round(max_abs_amount, 2),
        "days": days,
    }


def _build_traded_security_summaries(
    db: Session,
    user_id: str,
    account_key: str,
) -> list[dict[str, Any]]:
    rows = (
        db.query(QmtAccountTradeHistoryDB)
        .filter(
            QmtAccountTradeHistoryDB.user_id == user_id,
            QmtAccountTradeHistoryDB.account_key == account_key,
        )
        .order_by(QmtAccountTradeHistoryDB.trade_time.desc().nullslast(), QmtAccountTradeHistoryDB.fetched_at.desc().nullslast())
        .limit(2000)
        .all()
    )
    summary_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.symbol or "").strip().upper()
        if not symbol:
            continue
        item = summary_map.setdefault(
            symbol,
            {
                "symbol": symbol,
                "name": row.name or symbol,
                "trade_count": 0,
                "buy_quantity": 0.0,
                "sell_quantity": 0.0,
                "buy_amount": 0.0,
                "sell_amount": 0.0,
                "net_quantity": 0.0,
                "net_cashflow": 0.0,
                "realized_cost_basis": 0.0,
                "realized_pnl": 0.0,
                "realized_pnl_pct": None,
                "pnl_status": "empty",
                "latest_side": None,
                "latest_price": None,
                "latest_trade_time": None,
                "first_trade_time": None,
            },
        )
        side = str(row.side or "").strip().lower()
        quantity = float(row.quantity or 0.0)
        amount = float(row.amount or 0.0)
        item["trade_count"] += 1
        if side == "buy":
            item["buy_quantity"] += quantity
            item["buy_amount"] += amount
            item["net_quantity"] += quantity
            item["net_cashflow"] -= amount
        elif side == "sell":
            item["sell_quantity"] += quantity
            item["sell_amount"] += amount
            item["net_quantity"] -= quantity
            item["net_cashflow"] += amount
            if row.realized_pnl is not None:
                item["realized_pnl"] += float(row.realized_pnl or 0.0)
                item["realized_cost_basis"] += float(row.cost_basis or 0.0)
                item["pnl_status"] = "estimated"
            elif item["pnl_status"] != "estimated":
                item["pnl_status"] = str(row.pnl_status or "cost_missing")
        trade_time_text = row.trade_time.isoformat() if row.trade_time else None
        if item["latest_trade_time"] is None:
            item["latest_side"] = side or None
            item["latest_price"] = row.price
            item["latest_trade_time"] = trade_time_text
        item["first_trade_time"] = trade_time_text or item["first_trade_time"]
        if row.name:
            item["name"] = row.name

    items = list(summary_map.values())
    for item in items:
        if item["realized_cost_basis"] > 0:
            item["realized_pnl_pct"] = round((float(item["realized_pnl"]) / float(item["realized_cost_basis"])) * 100, 2)
        for key in ("buy_quantity", "sell_quantity", "buy_amount", "sell_amount", "net_quantity", "net_cashflow", "realized_cost_basis", "realized_pnl"):
            item[key] = round(float(item[key] or 0.0), 2)
    items.sort(key=lambda item: str(item.get("latest_trade_time") or ""), reverse=True)
    return items
