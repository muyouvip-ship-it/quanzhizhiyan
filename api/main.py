from __future__ import annotations

import asyncio
import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from tradingagents.default_config import DEFAULT_CONFIG as _BASE_DEFAULT_CONFIG

from api.app import app
from api.core.scheduler import scheduled_analysis_slot
from api.core.runtime_config import build_runtime_config
from api.core.stock_map import (
    _cn_stock_map as _core_cn_stock_map,
    _cn_stock_reverse_map as _core_cn_stock_reverse_map,
    load_cn_stock_map as _core_load_cn_stock_map,
)
from api.core.stock_utils import (
    normalize_symbol,
    resolve_watchlist_identifier,
    search_cn_stock_by_name,
    split_watchlist_batch_text,
)
from api.database import UserDB, get_db_ctx
from api.job_store import get_job_store
from api.schemas.analysis import AnalyzeRequest
from api.schemas.config import UserRuntimeConfigResponse, UserRuntimeConfigUpdateRequest
from api.services import portfolio_import_service
from api.services.config_service import (
    build_pending_runtime_config,
    config_response_for_user,
    invoke_runtime_warmup,
    mask_wecom_webhook,
    probe_runtime_config,
    run_config_warmup,
    should_probe_runtime_config,
    should_trigger_config_warmup,
    warmup_model_names,
)

DEFAULT_CONFIG = deepcopy(_BASE_DEFAULT_CONFIG)
_cn_stock_map = _core_cn_stock_map
_cn_stock_reverse_map = _core_cn_stock_reverse_map
logger = logging.getLogger(__name__)


def run() -> None:
    import uvicorn

    host = os.getenv("API_HOST", "127.0.0.1")
    port = int(os.getenv("API_PORT", "8500"))
    reload_enabled = os.getenv("API_RELOAD", "0").strip().lower() in {"1", "true", "yes", "on"}
    uvicorn.run("api.app:app", host=host, port=port, reload=reload_enabled)


def _build_runtime_config(*args, **kwargs):
    overrides = args[0] if args else kwargs.get("overrides", {})
    user_id = kwargs.get("user_id")
    db = kwargs.get("db")
    return build_runtime_config(overrides or {}, user_id=user_id, db=db)


def _build_pending_runtime_config(updates: UserRuntimeConfigUpdateRequest, user_id: str, db) -> Dict[str, Any]:
    return build_pending_runtime_config(updates, user_id, db)


def _warmup_model_names(config: Dict[str, Any]) -> List[str]:
    return warmup_model_names(config)


def _should_probe_runtime_config(
    before_cfg: UserRuntimeConfigResponse,
    pending_cfg: Dict[str, Any],
    updates: UserRuntimeConfigUpdateRequest,
) -> bool:
    return should_probe_runtime_config(before_cfg, pending_cfg, updates)


def _should_trigger_config_warmup(
    before_cfg: UserRuntimeConfigResponse,
    after_cfg: UserRuntimeConfigResponse,
    updates: UserRuntimeConfigUpdateRequest,
) -> bool:
    return should_trigger_config_warmup(before_cfg, after_cfg, updates)


def _probe_runtime_config(config: Dict[str, Any]) -> Dict[str, str]:
    return probe_runtime_config(config)


def _invoke_runtime_warmup(config: Dict[str, Any], prompt: str, user_id: str, timeout: float = 20.0) -> List[Dict[str, Any]]:
    return invoke_runtime_warmup(config, prompt, user_id, timeout=timeout)


def _run_config_warmup(config: Dict[str, Any], user_id: str) -> None:
    return run_config_warmup(config, user_id)


def _mask_wecom_webhook(webhook_url: Optional[str]) -> Optional[str]:
    return mask_wecom_webhook(webhook_url)


def _config_response_for_user(user: Optional[UserDB], db) -> UserRuntimeConfigResponse:
    return config_response_for_user(user, db)


def _load_cn_stock_map():
    global _cn_stock_map, _cn_stock_reverse_map
    _cn_stock_map = dict(_core_load_cn_stock_map())
    _cn_stock_reverse_map = {code: name for name, code in _cn_stock_map.items()}
    return dict(_cn_stock_map)


def _get_reverse_stock_map():
    global _cn_stock_reverse_map
    if _cn_stock_map is None:
        _load_cn_stock_map()
    if _cn_stock_map is not None:
        _cn_stock_reverse_map = {code: name for name, code in _cn_stock_map.items()}
    return dict(_cn_stock_reverse_map or {})


def _get_reverse_stock_map_cached_only():
    global _cn_stock_reverse_map
    if _cn_stock_map is None:
        return {}
    _cn_stock_reverse_map = {code: name for name, code in _cn_stock_map.items()}
    return dict(_cn_stock_reverse_map or {})


def _set_job(*args, **kwargs):
    return get_job_store().set_job(*args, **kwargs)


def _get_job(job_id: str):
    job = get_job_store().get_job(job_id)
    return job or None


def _emit_job_event(job_id: str, event: str, data: Dict[str, Any]):
    return get_job_store().emit_event(job_id, event, data)


def _create_tracked_task(coro):
    return asyncio.create_task(coro)


def cn_today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _resolve_scheduled_trade_date(trade_date: str) -> str:
    try:
        from tradingagents.dataflows.trade_calendar import cn_market_phase, is_cn_trading_day, now_cn, previous_cn_trading_day

        if not is_cn_trading_day(trade_date):
            return previous_cn_trading_day(trade_date)

        local_now = now_cn()
        if trade_date == local_now.date().strftime("%Y-%m-%d") and cn_market_phase(local_now) in {"pre_open", "in_session", "lunch_break"}:
            return previous_cn_trading_day(trade_date)
        return trade_date
    except Exception:
        return trade_date


def _extract_chat_text(messages: List[dict]) -> str:
    return "\n".join(str(message.get("content") or "").strip() for message in messages if str(message.get("content") or "").strip())


def _ai_extract_symbol_and_date(
    text: str,
    config: Dict[str, Any],
) -> tuple[Optional[str], Optional[str], List[str], List[str], List[str], Dict[str, Any]]:
    del config
    text = text.strip()
    if not text:
        return None, None, ["short"], [], [], {}
    symbol = search_cn_stock_by_name(text) or normalize_symbol(text)
    reverse_map = _get_reverse_stock_map()
    if reverse_map and symbol not in reverse_map:
        return None, None, ["short"], [], [], {}
    return symbol, cn_today_str(), ["short"], [], [], {}


def _require_job_owner(job_id: str, current_user: UserDB) -> Dict[str, Any]:
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    owner_id = job.get("user_id")
    if owner_id and owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="job not found")
    return job


def _build_manual_imported_user_context(db, user_id: str, symbol: str) -> Dict[str, Any]:
    return portfolio_import_service.build_scheduled_user_context(db, user_id, symbol)


def _build_imported_user_context(db, user_id: str, symbol: str) -> Dict[str, Any]:
    return _build_manual_imported_user_context(db, user_id, symbol)


def _attach_stock_names(items: List[dict], code_to_name: Dict[str, str]) -> List[dict]:
    for item in items:
        symbol = str(item.get("symbol") or "").upper()
        item["name"] = code_to_name.get(symbol, symbol)
    return items


def _annotate_scheduled_with_imported_context(items: List[dict], db, user_id: str) -> List[dict]:
    imported_map = {row["symbol"]: row for row in portfolio_import_service.list_imported_positions(db, user_id)}
    for item in items:
        imported = imported_map.get(item["symbol"])
        item["has_imported_context"] = imported is not None
        item["imported_current_position"] = imported.get("current_position") if imported else None
        item["imported_average_cost"] = imported.get("average_cost") if imported else None
        item["imported_trade_points_count"] = imported.get("trade_points_count") if imported else 0
    return items


def _extract_scheduled_update_kwargs(body: dict) -> dict:
    kwargs = {}
    if "is_active" in body:
        kwargs["is_active"] = bool(body["is_active"])
    if "horizon" in body:
        kwargs["horizon"] = body["horizon"]
    if "trigger_time" in body:
        kwargs["trigger_time"] = body["trigger_time"]
    return kwargs


def _build_scheduled_analyze_request(
    db,
    user_id: str,
    symbol: str,
    horizon: str,
    trade_date: str,
    scheduled_user_context: Optional[Dict[str, Any]] = None,
) -> AnalyzeRequest:
    del db, user_id, horizon
    scheduled_user_context = scheduled_user_context or {}
    return AnalyzeRequest(
        symbol=symbol,
        trade_date=trade_date,
        query=f"定时分析 {symbol}",
        objective=scheduled_user_context.get("objective"),
        current_position=scheduled_user_context.get("current_position"),
        current_position_pct=scheduled_user_context.get("current_position_pct"),
        average_cost=scheduled_user_context.get("average_cost"),
        user_notes=scheduled_user_context.get("user_notes"),
    )


async def _run_job(job_id: str, request: AnalyzeRequest, *args, **kwargs) -> None:
    from api.routes.chat import _resolve_selected_analysts, _run_background_analysis_job

    user_id = kwargs.get("user_id")
    if user_id is None and len(args) >= 3:
        user_id = args[2]
    if not user_id:
        raise ValueError("scheduled analysis requires user_id")

    selected_analysts = _resolve_selected_analysts(
        getattr(request, "selected_analysts", None),
        user_id,
    )
    await _run_background_analysis_job(
        job_id=job_id,
        symbol=request.symbol,
        trade_date=request.trade_date or cn_today_str(),
        query=request.query or f"定时分析 {request.symbol}",
        user_id=user_id,
        selected_analysts=selected_analysts,
    )


async def _run_scheduled_analysis_once(
    task: dict,
    requested_trade_date: str,
    job_id: str,
    *,
    mark_schedule_run: bool,
) -> None:
    from api.services import report_service, scheduled_service

    task_id = task["id"]
    user_id = task["user_id"]
    symbol = task["symbol"]
    horizon = task.get("horizon") or "short"
    actual_trade_date = _resolve_scheduled_trade_date(requested_trade_date)
    try:
        async with scheduled_analysis_slot(job_id, symbol):
            with get_db_ctx() as db:
                scheduled_user_context = task.get("manual_user_context") or _build_manual_imported_user_context(db, user_id, symbol)
                req = _build_scheduled_analyze_request(
                    db=db,
                    user_id=user_id,
                    symbol=symbol,
                    horizon=horizon,
                    trade_date=actual_trade_date,
                    scheduled_user_context=scheduled_user_context,
                )
            await _run_job(job_id, req, False, True, user_id, "scheduled" if mark_schedule_run else "scheduled_manual")
        job_state = _get_job(job_id) or {}
        if job_state.get("status") == "failed":
            raise RuntimeError(job_state.get("error") or f"scheduled analysis job {job_id} failed")
        with get_db_ctx() as db:
            if mark_schedule_run:
                scheduled_service.mark_run_success(db, task_id, requested_trade_date, job_id)
            else:
                scheduled_service.record_manual_test_result(db, task_id, "success", report_id=job_id)
    except Exception as exc:
        logger.exception("Scheduled analysis failed task=%s symbol=%s job=%s: %s", task_id, symbol, job_id, exc)
        with get_db_ctx() as db:
            report_service.update_report_partial(db, job_id, status="failed", error=f"智能分析失败：{exc}")
            if mark_schedule_run:
                scheduled_service.mark_run_failed(db, task_id, requested_trade_date)
            else:
                scheduled_service.record_manual_test_result(db, task_id, "failed")


async def _run_scheduled_job(task: dict, trade_date: str):
    job_id = datetime.now().strftime("%Y%m%d%H%M%S%f")[-24:]
    await _run_scheduled_analysis_once(task, trade_date, job_id, mark_schedule_run=True)
