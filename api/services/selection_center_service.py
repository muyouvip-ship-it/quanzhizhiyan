from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import logging
import os
import re
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from api.core.strategy_db import get_strategy_db_ctx
from api.core.stock_map import get_reverse_stock_map
from api.database import get_db_ctx
from api.models.strategy_models import SelectionCenterTaskDB
from api.services.market_data_pipeline_service import preferred_daily_kline_table
from api.services.minute_data_service import _aggregate_minute_frame, _compute_first_day_band, _try_load_minute_frame_range
from api.services.strategy_compute_backend import compute_daily_features
from api.services.strategy_dsl_compiler import CompiledStrategy, compile_strategy_dsl
from api.services.strategy_platform_repository import get_platform_strategy
from tradingagents.dataflows.trade_calendar import is_cn_trading_day


logger = logging.getLogger(__name__)
BOARD_OPTIONS = ("主板", "创业板", "科创板", "北交所")
CN_TZ = ZoneInfo("Asia/Shanghai")
DAILY_SELECTION_READY_TIME = time(15, 5)
MIN_DAILY_SELECTION_SYMBOLS = max(int(os.getenv("SELECTION_CENTER_MIN_DAILY_SYMBOLS", "3000") or 3000), 1)
MODE_LABELS = {
    "strategy": "策略选股",
    "catalyst": "催化选股",
    "hybrid": "混合选股",
}
_SIGNAL_RULE_INDEX_RE = re.compile(r"(?:dsl|buy|sell)-(\d+)$")


@dataclass
class StrategySignalContext:
    side: str
    signal_name: str
    symbols: set[str]
    feature_rows: dict[str, dict[str, Any]]
    backend: str


def list_tasks(
    strategy_db: Session,
    user_id: str,
    *,
    mode: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters = ["user_id = :user_id"]
    params: dict[str, Any] = {"user_id": user_id, "limit": int(limit)}
    if mode and mode != "history":
        filters.append("mode = :mode")
        params["mode"] = mode
    rows = strategy_db.execute(
        text(
            f"""
            SELECT
                id,
                user_id,
                name,
                mode,
                status,
                progress,
                universe,
                rule,
                filters_json,
                config_json,
                jsonb_array_length(COALESCE(candidates_json::jsonb, '[]'::jsonb)) AS candidate_count,
                error_message,
                created_at,
                started_at,
                completed_at,
                updated_at
            FROM selection_center_tasks
            WHERE {" AND ".join(filters)}
            ORDER BY created_at DESC, id DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()
    return [_selection_task_summary(row) for row in rows]


def get_task(strategy_db: Session, user_id: str, task_id: str) -> dict[str, Any] | None:
    row = (
        strategy_db.query(SelectionCenterTaskDB)
        .filter(SelectionCenterTaskDB.user_id == user_id, SelectionCenterTaskDB.id == task_id)
        .first()
    )
    if row is None:
        return None
    task = row.to_dict()
    _enrich_task_candidate_market_metrics(task)
    task["candidate_count"] = len(task.get("candidates") or [])
    return task


def get_task_confirmation_filters(
    strategy_db: Session,
    user_id: str,
    task_id: str,
    *,
    timeframe: str = "30m",
) -> dict[str, Any] | None:
    task = get_task(strategy_db, user_id, task_id)
    if task is None:
        return None
    candidates = list(task.get("candidates") or [])
    symbol_dates = _candidate_confirmation_symbol_dates(candidates, fallback_date=_parse_trade_date(task.get("completed_at") or task.get("created_at")))
    symbols = sorted({symbol for symbol, _ in symbol_dates})
    if not symbols or not symbol_dates:
        return {
            "task_id": task_id,
            "timeframe": _normalize_confirmation_timeframe(timeframe),
            "total": len(candidates),
            "items": [],
            "criteria": _selection_confirmation_criteria(),
        }

    min_date = min(trade_date for _, trade_date in symbol_dates)
    max_date = max(trade_date for _, trade_date in symbol_dates)
    normalized_timeframe = _normalize_confirmation_timeframe(timeframe)
    daily_rows: list[dict[str, Any]] = []
    minute_frame: pd.DataFrame | None = None
    try:
        with get_db_ctx() as db:
            daily_start = min_date - timedelta(days=180) if normalized_timeframe == "1d" else min_date
            daily_rows = _load_confirmation_daily_rows(db, symbols, daily_start, max_date + timedelta(days=14))
        if normalized_timeframe != "1d":
            minute_start = (min_date - timedelta(days=14)).isoformat()
            minute_end = (max_date + timedelta(days=3)).isoformat()
            minute_frame = _try_load_minute_frame_range(symbols, start_date=minute_start, end_date=minute_end)
    except Exception:
        logger.exception("Failed to load selection confirmation data task=%s", task_id)

    breakout_by_symbol = _evaluate_next_day_breakout(candidates, daily_rows)
    no_reverse_by_symbol = (
        _evaluate_daily_no_immediate_dead_cross(candidates, daily_rows)
        if normalized_timeframe == "1d"
        else _evaluate_intraday_no_immediate_dead_cross(candidates, minute_frame, normalized_timeframe)
    )
    no_reverse_missing_reason = "缺少日K，无法判断下一根日线是否反叉" if normalized_timeframe == "1d" else "缺少分钟K线，无法判断金叉后一根K线"
    items = []
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").upper()
        if not symbol:
            continue
        items.append(
            {
                "symbol": symbol,
                "name": candidate.get("name") or "",
                "selected_date": _candidate_selected_date(candidate).isoformat() if _candidate_selected_date(candidate) else None,
                "checks": {
                    "no_immediate_dead_cross": no_reverse_by_symbol.get(symbol) or _confirmation_status("missing", no_reverse_missing_reason),
                    "break_previous_high": breakout_by_symbol.get(symbol) or _confirmation_status("missing", "缺少日K，无法判断次日是否突破前高"),
                },
            }
        )

    return {
        "task_id": task_id,
        "timeframe": normalized_timeframe,
        "total": len(items),
        "criteria": _selection_confirmation_criteria(),
        "items": items,
    }


def create_task(
    strategy_db: Session,
    user_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    config = _normalize_config(payload)
    return _selection_task_summary_from_model(_create_task_row(strategy_db, user_id, config))


def rerun_task(
    strategy_db: Session,
    user_id: str,
    task_id: str,
) -> dict[str, Any] | None:
    current = (
        strategy_db.query(SelectionCenterTaskDB)
        .filter(SelectionCenterTaskDB.user_id == user_id, SelectionCenterTaskDB.id == task_id)
        .first()
    )
    if current is None:
        return None
    config = deepcopy(current.config_json or {})
    config["name"] = current.name
    return _selection_task_summary_from_model(_create_task_row(strategy_db, user_id, _normalize_config(config)))


def _iso_datetime(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else None


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    return []


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _selection_task_summary(row: Any) -> dict[str, Any]:
    getter = row.get if hasattr(row, "get") else lambda key, default=None: getattr(row, key, default)
    config = _json_dict(getter("config_json"))
    return {
        "id": getter("id"),
        "user_id": getter("user_id"),
        "name": getter("name") or "",
        "mode": getter("mode") or "strategy",
        "status": getter("status") or "running",
        "progress": float(getter("progress") or 0.0),
        "universe": getter("universe") or "",
        "rule": getter("rule") or "",
        "filters": _summary_filter_labels(config, getter("filters_json")),
        "config": config,
        "candidate_count": int(getter("candidate_count") or 0),
        "candidates": [],
        "error_message": getter("error_message"),
        "created_at": _iso_datetime(getter("created_at")),
        "started_at": _iso_datetime(getter("started_at")),
        "completed_at": _iso_datetime(getter("completed_at")),
        "updated_at": _iso_datetime(getter("updated_at")),
    }


def _selection_task_summary_from_model(row: SelectionCenterTaskDB) -> dict[str, Any]:
    return _selection_task_summary(
        {
            "id": row.id,
            "user_id": row.user_id,
            "name": row.name,
            "mode": row.mode,
            "status": row.status,
            "progress": row.progress,
            "universe": row.universe,
            "rule": row.rule,
            "filters_json": row.filters_json,
            "config_json": row.config_json,
            "candidate_count": len(row.candidates_json or []),
            "error_message": row.error_message,
            "created_at": row.created_at,
            "started_at": row.started_at,
            "completed_at": row.completed_at,
            "updated_at": row.updated_at,
        }
    )


def _summary_filter_labels(config: dict[str, Any], stored_filters: Any) -> list[str]:
    filter_config = config.get("filter_config") if isinstance(config.get("filter_config"), dict) else None
    if filter_config:
        return _build_filter_labels(filter_config)
    return _json_list(stored_filters)


def execute_task(task_id: str) -> None:
    """Run a selection-center task after the API response has returned."""
    try:
        with get_strategy_db_ctx() as strategy_db:
            row = strategy_db.query(SelectionCenterTaskDB).filter(SelectionCenterTaskDB.id == task_id).first()
            if row is None or row.status != "running":
                return
            row.started_at = row.started_at or datetime.now()
            row.progress = 24.0
            row.updated_at = datetime.now()
            row.error_message = None
            strategy_db.add(row)
            strategy_db.commit()
            config = deepcopy(row.config_json or {})
            rule_label = row.rule or _build_rule_label(config)
            strategy_payload = _load_strategy_payload(strategy_db, config)

        def update_progress(progress: float) -> None:
            _update_task_progress(task_id, progress)

        with get_db_ctx() as market_db:
            candidates = _generate_candidates(
                market_db,
                config,
                rule_label,
                strategy_payload=strategy_payload,
                progress_callback=update_progress,
            )

        with get_strategy_db_ctx() as strategy_db:
            row = strategy_db.query(SelectionCenterTaskDB).filter(SelectionCenterTaskDB.id == task_id).first()
            if row is None:
                return
            row.candidates_json = candidates
            row.status = "completed"
            row.progress = 100.0
            row.completed_at = datetime.now()
            row.updated_at = row.completed_at
            row.error_message = None
            strategy_db.add(row)
            strategy_db.commit()
    except Exception as exc:
        logger.exception("Selection center task %s failed", task_id)
        _mark_task_failed(task_id, f"选股执行失败：{exc}")


def _create_task_row(strategy_db: Session, user_id: str, config: dict[str, Any]) -> SelectionCenterTaskDB:
    local_now = datetime.now(CN_TZ)
    now = local_now.replace(tzinfo=None)
    config["target_trade_date"] = _resolve_selection_target_trade_date(local_now).isoformat()
    row = SelectionCenterTaskDB(
        id=uuid4().hex,
        user_id=user_id,
        name=config["name"],
        mode=config["mode"],
        status="running",
        progress=12.0,
        universe=_build_universe_label(config),
        rule=_build_rule_label(config),
        filters_json=_build_filter_labels(config.get("filter_config") or {}),
        config_json=config,
        candidates_json=[],
        created_at=now,
        started_at=now,
        updated_at=now,
    )
    strategy_db.add(row)
    strategy_db.commit()
    strategy_db.refresh(row)
    return row


def _update_task_progress(task_id: str, progress: float) -> None:
    with get_strategy_db_ctx() as strategy_db:
        row = strategy_db.query(SelectionCenterTaskDB).filter(SelectionCenterTaskDB.id == task_id).first()
        if row is None or row.status != "running":
            return
        row.progress = max(float(row.progress or 0.0), min(float(progress), 99.0))
        row.updated_at = datetime.now()
        strategy_db.add(row)
        strategy_db.commit()


def _mark_task_failed(task_id: str, message: str) -> None:
    with get_strategy_db_ctx() as strategy_db:
        row = strategy_db.query(SelectionCenterTaskDB).filter(SelectionCenterTaskDB.id == task_id).first()
        if row is None:
            return
        row.status = "failed"
        row.progress = 100.0
        row.completed_at = datetime.now()
        row.updated_at = row.completed_at
        row.error_message = message
        row.candidates_json = []
        strategy_db.add(row)
        strategy_db.commit()


def _load_strategy_payload(strategy_db: Session, config: dict[str, Any]) -> dict[str, Any] | None:
    mode = str(config.get("mode") or "strategy")
    if mode not in {"strategy", "hybrid"}:
        return None

    strategy_id = str(config.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("策略选股需要选择一个策略资产")
    strategy = get_platform_strategy(strategy_db, strategy_id)
    if strategy is None:
        raise ValueError(f"策略资产不存在或不可用：{strategy_id}")
    return strategy


def delete_task(strategy_db: Session, user_id: str, task_id: str) -> bool:
    row = (
        strategy_db.query(SelectionCenterTaskDB)
        .filter(SelectionCenterTaskDB.user_id == user_id, SelectionCenterTaskDB.id == task_id)
        .first()
    )
    if row is None:
        return False
    strategy_db.delete(row)
    strategy_db.commit()
    return True


def _normalize_config(payload: dict[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("mode") or "strategy").strip()
    if mode not in MODE_LABELS:
        raise ValueError("选股类型必须是 strategy/catalyst/hybrid")

    include_boards = [str(item) for item in (payload.get("include_boards") or BOARD_OPTIONS) if str(item) in BOARD_OPTIONS]
    if not include_boards:
        raise ValueError("至少需要选择一个板块范围")

    return {
        "name": str(payload.get("name") or MODE_LABELS[mode]).strip()[:200] or MODE_LABELS[mode],
        "mode": mode,
        "include_boards": include_boards,
        "strategy_id": str(payload.get("strategy_id") or ""),
        "strategy_name": str(payload.get("strategy_name") or "").strip(),
        "signal_id": str(payload.get("signal_id") or ""),
        "signal_name": str(payload.get("signal_name") or "").strip(),
        "signal_side": str(payload.get("signal_side") or "").strip(),
        "period": str(payload.get("period") or "日K").strip(),
        "catalyst_rule": str(payload.get("catalyst_rule") or "事件热度").strip(),
        "filter_config": dict(payload.get("filter_config") or {}),
    }


def _build_universe_label(config: dict[str, Any]) -> str:
    boards = [item for item in (config.get("include_boards") or []) if item in BOARD_OPTIONS]
    if len(boards) == len(BOARD_OPTIONS):
        return "全A"
    return "、".join(boards) if boards else "未选择板块"


def _build_rule_label(config: dict[str, Any]) -> str:
    mode = config.get("mode")
    catalyst_rule = str(config.get("catalyst_rule") or "事件热度")
    if mode == "catalyst":
        return catalyst_rule

    strategy_name = str(config.get("strategy_name") or config.get("strategy_id") or "未选择策略")
    signal_name = str(config.get("signal_name") or config.get("signal_id") or "未选择买卖点")
    period = str(config.get("period") or "日K")
    strategy_rule = f"{strategy_name} / {signal_name} / {period}"
    return f"{strategy_rule} + {catalyst_rule}" if mode == "hybrid" else strategy_rule


def _build_filter_labels(filter_config: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    if bool(filter_config.get("exclude_st", True)):
        labels.append("非 ST")
    if bool(filter_config.get("exclude_suspended", True)):
        labels.append("排除停牌")
    min_amount = _num(filter_config.get("min_amount"))
    if bool(filter_config.get("amount_enabled")) and min_amount is not None:
        labels.append(f"成交额 >= {_format_number(min_amount)} 亿")
    if bool(filter_config.get("market_cap_enabled")):
        min_cap = _num(filter_config.get("min_market_cap"))
        max_cap = _num(filter_config.get("max_market_cap"))
        if min_cap is not None or max_cap is not None:
            labels.append(f"市值 {_format_number(min_cap or 0)}-{_format_number(max_cap) if max_cap is not None else '不限'} 亿")
    if bool(filter_config.get("trend_up")):
        ma = int(filter_config.get("trend_ma") or 20)
        labels.append(f"站上MA{ma}")
    if bool(filter_config.get("volume_up")):
        labels.append("量能放大")
    min_heat = _num(filter_config.get("min_event_heat"))
    if bool(filter_config.get("event_heat_enabled")) and min_heat is not None:
        labels.append(f"事件热度 >= {_format_number(min_heat)}")
    return labels


def _trend_ma_period(value: Any) -> int:
    try:
        period = int(float(value or 20))
    except (TypeError, ValueError):
        period = 20
    return period if period > 0 else 20


def _passes_trend_ma(row: dict[str, Any], close: float | None, period: int) -> bool:
    ma_value = _num(row.get(f"ma{period}"))
    window_count = _num(row.get(f"ma{period}_window_count"))
    return close is not None and ma_value is not None and window_count is not None and window_count >= period and close >= ma_value


def _generate_candidates(
    market_db: Session,
    config: dict[str, Any],
    rule_label: str,
    *,
    strategy_payload: dict[str, Any] | None = None,
    progress_callback: Callable[[float], None] | None = None,
) -> list[dict[str, Any]]:
    target_trade_date = _parse_trade_date(config.get("target_trade_date")) or _resolve_selection_target_trade_date()
    rows = _load_latest_market_rows(
        market_db,
        target_trade_date=target_trade_date,
    )
    if progress_callback:
        progress_callback(32.0)
    strategy_signal = _resolve_strategy_signal(
        market_db,
        config,
        strategy_payload=strategy_payload,
        target_trade_date=target_trade_date,
        progress_callback=progress_callback,
    )
    reverse_map = get_reverse_stock_map()
    selected_boards = set(config.get("include_boards") or BOARD_OPTIONS)
    filter_config = config.get("filter_config") or {}
    mode = str(config.get("mode") or "strategy")
    candidates: list[dict[str, Any]] = []
    total_rows = len(rows)
    progress_step = max(total_rows // 8, 1) if total_rows else 1

    for index, row in enumerate(rows, start=1):
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        board = _classify_board(symbol)
        if board not in selected_boards:
            continue
        if strategy_signal is not None and symbol not in strategy_signal.symbols:
            continue

        name = reverse_map.get(symbol) or symbol
        if bool(filter_config.get("exclude_st", True)) and _is_st_name(name):
            continue

        close = _num(row.get("close"))
        pre_close = _num(row.get("pre_close"))
        amount = _num(row.get("amount"))
        if bool(filter_config.get("exclude_suspended", True)) and (close is None or close <= 0 or amount is None or amount <= 0):
            continue

        min_amount = _num(filter_config.get("min_amount"))
        if bool(filter_config.get("amount_enabled")) and min_amount is not None:
            if amount is None or amount < min_amount * 100_000_000:
                continue

        market_cap = _market_cap_yi(row)
        min_cap = _num(filter_config.get("min_market_cap"))
        max_cap = _num(filter_config.get("max_market_cap"))
        if bool(filter_config.get("market_cap_enabled")):
            if min_cap is not None and (market_cap is None or market_cap < min_cap):
                continue
            if max_cap is not None and (market_cap is None or market_cap > max_cap):
                continue

        ma20 = _num(row.get("ma20"))
        amount_ma20 = _num(row.get("amount_ma20"))
        if bool(filter_config.get("trend_up")):
            trend_ma = _trend_ma_period(filter_config.get("trend_ma"))
            if not _passes_trend_ma(row, close, trend_ma):
                continue
        if bool(filter_config.get("volume_up")) and amount is not None and amount_ma20 is not None and amount < amount_ma20 * 1.1:
            continue

        event_heat = _event_heat(row)
        min_heat = _num(filter_config.get("min_event_heat"))
        if bool(filter_config.get("event_heat_enabled")) and min_heat is not None and event_heat < min_heat:
            continue

        score = _score_candidate(row, mode, event_heat)
        signal_feature_row = strategy_signal.feature_rows.get(symbol) if strategy_signal is not None else None
        factor_score = _num((signal_feature_row or {}).get("factor_score"))
        if factor_score is not None:
            score = max(score, int(max(0, min(99, round(factor_score * 100)))))
        source = MODE_LABELS.get(mode, "选股中心")
        tags = _candidate_tags(config, row, board, event_heat)
        metrics = {
            "trade_date": str(row.get("trade_date") or ""),
            "selected_at": str(row.get("trade_date") or ""),
            "close": close,
            "change_pct": _change_pct(close, pre_close),
            "amount_yi": _round_or_none((amount or 0) / 100_000_000, 2) if amount is not None else None,
            "float_market_cap_yi": _round_or_none(_market_cap_value_yi(row, "float_market_cap"), 2),
            "total_market_cap_yi": _round_or_none(_market_cap_value_yi(row, "total_market_cap"), 2),
            "market_cap_yi": _round_or_none(market_cap, 2),
            "board": board,
            "industry": str(row.get("sw_industry_l1") or row.get("sw_industry_l2") or "").strip(),
            "event_heat": event_heat,
            "current_close": close,
            "since_selected_change_pct": 0.0,
        }
        _apply_selected_day_factor_metrics(metrics, row, close)
        if signal_feature_row is not None:
            metrics.update(_strategy_signal_metrics(signal_feature_row, strategy_signal))
        candidates.append(
            {
                "symbol": symbol,
                "name": name,
                "score": score,
                "source": source,
                "rule": rule_label,
                "reason": _candidate_reason(config, row, board, event_heat, strategy_signal=strategy_signal),
                "tags": tags,
                "metrics": metrics,
            }
        )
        if progress_callback and (index % progress_step == 0 or index == total_rows):
            progress_callback(32.0 + (index / max(total_rows, 1)) * 58.0)

    candidates.sort(key=lambda item: (-float(item.get("score") or 0), str(item.get("symbol") or "")))
    _assign_candidate_recommendations(candidates)
    if progress_callback:
        progress_callback(94.0)
    return candidates


def _resolve_strategy_signal(
    market_db: Session,
    config: dict[str, Any],
    *,
    strategy_payload: dict[str, Any] | None,
    target_trade_date: date,
    progress_callback: Callable[[float], None] | None = None,
) -> StrategySignalContext | None:
    mode = str(config.get("mode") or "strategy")
    if mode not in {"strategy", "hybrid"}:
        return None
    if strategy_payload is None:
        raise ValueError("策略选股需要可用的策略资产")

    dsl = _strategy_dsl(strategy_payload)
    compiled = compile_strategy_dsl(dsl)
    if compiled.status != "passed":
        errors = "；".join(compiled.errors[:3]) or "未知编译错误"
        raise ValueError(f"策略编译失败：{errors}")

    side = _signal_side(config)
    branch = dsl.get("entry" if side == "buy" else "exit") or {}
    rules = compiled.entry_rules if side == "buy" else compiled.exit_rules
    selected_rules, selected_single = _selected_signal_rules(rules, config)
    if not selected_rules:
        side_label = "买点" if side == "buy" else "卖点"
        raise ValueError(f"策略没有可执行的{side_label}规则")

    if progress_callback:
        progress_callback(36.0)
    rows = _load_recent_strategy_rows(market_db, target_trade_date=target_trade_date)
    if not rows:
        return StrategySignalContext(side=side, signal_name=_signal_name(config, side), symbols=set(), feature_rows={}, backend="none")

    frame = pd.DataFrame.from_records(rows)
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    selected_boards = set(config.get("include_boards") or BOARD_OPTIONS)
    frame["board"] = frame["symbol"].map(_classify_board)
    frame = frame[frame["board"].isin(selected_boards)].drop(columns=["board"])
    if frame.empty:
        return StrategySignalContext(side=side, signal_name=_signal_name(config, side), symbols=set(), feature_rows={}, backend="none")
    _coerce_strategy_frame_numbers(frame)

    if progress_callback:
        progress_callback(48.0)
    features, backend = compute_daily_features(frame, compiled)
    features["symbol"] = features["symbol"].astype(str).str.upper()
    features["date"] = pd.to_datetime(features["date"])
    logic = "all" if selected_single else str(branch.get("logic") or ("all" if side == "buy" else "any"))
    match_mask = _evaluate_strategy_rules(features, selected_rules, side=side, logic=logic)

    latest = (
        features.assign(_signal_match=match_mask.fillna(False))
        .sort_values(["symbol", "date"])
        .groupby("symbol", as_index=False, group_keys=False)
        .tail(1)
    )
    matched = latest[latest["_signal_match"]]
    symbols = set(matched["symbol"].astype(str))
    feature_rows = {
        str(row["symbol"]): row.drop(labels=["_signal_match"], errors="ignore").to_dict()
        for _, row in matched.iterrows()
    }
    if progress_callback:
        progress_callback(58.0)
    return StrategySignalContext(
        side=side,
        signal_name=_signal_name(config, side),
        symbols=symbols,
        feature_rows=feature_rows,
        backend=backend,
    )


def _strategy_dsl(strategy_payload: dict[str, Any]) -> dict[str, Any]:
    current_version = strategy_payload.get("current_version") or {}
    dsl = current_version.get("dsl") or strategy_payload.get("dsl")
    if not isinstance(dsl, dict) or not dsl:
        raise ValueError("策略当前版本没有可执行 DSL")
    return deepcopy(dsl)


def _signal_side(config: dict[str, Any]) -> str:
    raw = f"{config.get('signal_side') or ''} {config.get('signal_id') or ''} {config.get('signal_name') or ''}"
    if "卖" in raw or "exit" in raw.lower() or "sell" in raw.lower():
        return "sell"
    return "buy"


def _signal_name(config: dict[str, Any], side: str) -> str:
    fallback = "买点规则" if side == "buy" else "卖点规则"
    return str(config.get("signal_name") or fallback).strip() or fallback


def _selected_signal_rules(rules: list[dict[str, Any]], config: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    signal_id = str(config.get("signal_id") or "")
    match = _SIGNAL_RULE_INDEX_RE.search(signal_id)
    if not match:
        return list(rules), False
    index = int(match.group(1)) - 1
    if index < 0 or index >= len(rules):
        raise ValueError(f"选择的买卖点不存在：{config.get('signal_name') or signal_id}")
    return [rules[index]], True


def _evaluate_strategy_rules(
    frame: pd.DataFrame,
    rules: list[dict[str, Any]],
    *,
    side: str,
    logic: str,
) -> pd.Series:
    masks = [_evaluate_strategy_rule(frame, rule, side=side) for rule in rules]
    if not masks:
        return pd.Series(False, index=frame.index)
    result = masks[0]
    for mask in masks[1:]:
        result = (result | mask) if logic == "any" else (result & mask)
    return result.fillna(False)


def _evaluate_strategy_rule(frame: pd.DataFrame, rule: dict[str, Any], *, side: str) -> pd.Series:
    rule_key = str(rule.get("rule_key") or "")
    params = rule.get("params") or {}

    if side == "buy" and rule_key == "close_above_indicator":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or params.get("indicator") or params.get("field") or "ma20")
        op = str(params.get("op") or "above")
        if str(rule.get("timeframe") or "1d") == "1w" and right == "ma20" and "weekly_trend_pass" in frame.columns:
            weekly = frame["weekly_trend_pass"].fillna(False).astype(bool)
            return ~weekly if op == "below" else weekly
        return _numeric_column(frame, left) < _numeric_column(frame, right) if op == "below" else _numeric_column(frame, left) > _numeric_column(frame, right)

    if side == "buy" and rule_key == "alligator_proxy":
        return _numeric_column(frame, "ma5") >= _numeric_column(frame, "ma20")

    if side == "buy" and rule_key == "cross_above":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or "ma5")
        if left == "first_day_band" and right == "first_day_band_b1" and "first_day_band_cross" in frame.columns:
            return _numeric_column(frame, "first_day_band_cross") > 0
        left_values = _numeric_column(frame, left)
        right_values = _numeric_column(frame, right)
        return (left_values > right_values) & (_previous_by_symbol(frame, left_values) <= _previous_by_symbol(frame, right_values))

    if side == "buy" and rule_key == "lazy_minute_confirm":
        raise ValueError("选股中心暂不支持分钟确认买点，请选择日线规则或先接入分钟线选股执行器")

    if side == "sell" and rule_key == "close_below_indicator":
        left = str(params.get("left") or "close")
        right = str(params.get("right") or params.get("indicator") or "ma20")
        if left == "first_day_band" and right == "first_day_band_b1" and "first_day_band_dead_cross" in frame.columns:
            return _numeric_column(frame, "first_day_band_dead_cross") > 0
        return _numeric_column(frame, left) < _numeric_column(frame, right)

    if side == "sell" and rule_key == "factor_rank_drop":
        rank_below = float(params.get("rank_below") or 0.5)
        return _numeric_column(frame, "factor_score") < rank_below

    if side == "sell" and rule_key == "atr_trailing_stop":
        raise ValueError("ATR 跟踪止损需要持仓入场价和最高价状态，不能作为无持仓选股条件直接执行")

    raise ValueError(f"选股中心暂不支持该策略规则：{rule.get('type') or rule_key}")


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise ValueError(f"策略字段缺失：{column}")
    return pd.to_numeric(frame[column], errors="coerce")


def _previous_by_symbol(frame: pd.DataFrame, values: pd.Series) -> pd.Series:
    return values.groupby(frame["symbol"]).shift(1)


def _strategy_signal_metrics(row: dict[str, Any], signal: StrategySignalContext) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "strategy_signal": signal.signal_name,
        "strategy_side": "buy" if signal.side == "buy" else "sell",
        "strategy_backend": signal.backend,
        "factor_score": _round_or_none(_num(row.get("factor_score")), 4),
    }
    for key in ("first_day_band", "first_day_band_b1", "first_day_band_cross", "first_day_band_dead_cross"):
        value = _num(row.get(key))
        if value is not None:
            metrics[key] = _round_or_none(value, 4)
    return metrics


def _coerce_strategy_frame_numbers(frame: pd.DataFrame) -> None:
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
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(float)


def _load_recent_strategy_rows(db: Session, *, target_trade_date: date) -> list[dict[str, Any]]:
    sql = text(
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
            sw_industry_l1,
            sw_industry_l2,
            NULL::DOUBLE PRECISION AS net_profit_ttm
        FROM stock_daily_kline
        WHERE trade_date >= CAST(:target_trade_date AS DATE) - INTERVAL '260 days'
          AND trade_date <= CAST(:target_trade_date AS DATE)
        ORDER BY symbol, trade_date
        """
    )
    return [dict(row) for row in db.execute(sql, {"target_trade_date": target_trade_date}).mappings().all()]


def _load_latest_market_rows(db: Session, *, target_trade_date: date) -> list[dict[str, Any]]:
    coverage_count = _count_daily_symbols(db, target_trade_date)
    if coverage_count < MIN_DAILY_SELECTION_SYMBOLS:
        raise ValueError(
            f"选股目标日 {target_trade_date.isoformat()} 的日K数据未同步完成"
            f"（当前 {coverage_count} 只，至少需要 {MIN_DAILY_SELECTION_SYMBOLS} 只）。"
            "请先在设置-回测数据同步当日股票日K，再重新创建选股任务。"
        )

    sql = text(
        """
        WITH recent AS (
            SELECT
                symbol,
                trade_date,
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
                sw_industry_l1,
                sw_industry_l2,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS ma15,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS ma15_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS ma30,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS ma30_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60_window_count,
                AVG(amount) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_ma20,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date DESC
                ) AS rn
            FROM stock_daily_kline
            WHERE trade_date >= CAST(:target_trade_date AS DATE) - INTERVAL '260 days'
              AND trade_date <= CAST(:target_trade_date AS DATE)
        ),
        latest AS (
            SELECT *
            FROM recent
            WHERE rn = 1
              AND trade_date = CAST(:target_trade_date AS DATE)
        )
        SELECT
            latest.symbol,
            latest.trade_date,
            latest.open,
            latest.high,
            latest.low,
            latest.close,
            latest.volume,
            latest.amount,
            latest.turnover_rate,
            latest.pre_close,
            latest.float_market_cap,
            latest.total_market_cap,
            latest.sw_industry_l1,
            latest.sw_industry_l2,
            latest.ma5,
            latest.ma5_window_count,
            latest.ma10,
            latest.ma10_window_count,
            latest.ma15,
            latest.ma15_window_count,
            latest.ma20,
            latest.ma20_window_count,
            latest.ma30,
            latest.ma30_window_count,
            latest.ma60,
            latest.ma60_window_count,
            latest.amount_ma20,
            latest.rn
        FROM latest
        ORDER BY COALESCE(latest.amount, 0) DESC
        """
    )
    return [dict(row) for row in db.execute(sql, {"target_trade_date": target_trade_date}).mappings().all()]


def _count_daily_symbols(db: Session, target_trade_date: date) -> int:
    row = db.execute(
        text(
            """
            SELECT COUNT(DISTINCT symbol) AS symbol_count
            FROM stock_daily_kline
            WHERE trade_date = :target_trade_date
            """
        ),
        {"target_trade_date": target_trade_date},
    ).fetchone()
    return int(row.symbol_count or 0) if row else 0


def _resolve_selection_target_trade_date(now: datetime | None = None) -> date:
    local_now = now or datetime.now(CN_TZ)
    if local_now.tzinfo is None:
        local_now = local_now.replace(tzinfo=CN_TZ)
    else:
        local_now = local_now.astimezone(CN_TZ)

    target = local_now.date()
    if not _is_cn_trade_date(target) or local_now.time() < DAILY_SELECTION_READY_TIME:
        target = target - timedelta(days=1)
    while not _is_cn_trade_date(target):
        target = target - timedelta(days=1)
    return target


def _is_cn_trade_date(value: date) -> bool:
    try:
        return bool(is_cn_trading_day(value.isoformat()))
    except Exception:
        return value.weekday() < 5


def _parse_trade_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _classify_board(symbol: str) -> str:
    code = symbol.split(".", 1)[0]
    suffix = symbol.rsplit(".", 1)[-1] if "." in symbol else ""
    if suffix == "BJ" or code.startswith(("4", "8", "920")):
        return "北交所"
    if code.startswith(("688", "689")):
        return "科创板"
    if code.startswith(("300", "301")):
        return "创业板"
    return "主板"


def _is_st_name(name: str) -> bool:
    normalized = str(name or "").upper().replace(" ", "")
    return "ST" in normalized or normalized.startswith("退市")


def _enrich_task_candidate_market_metrics(task: dict[str, Any]) -> None:
    candidates = list(task.get("candidates") or [])
    symbols = sorted({str(item.get("symbol") or "").upper() for item in candidates if item.get("symbol")})
    if not symbols:
        return

    try:
        with get_db_ctx() as db:
            latest_rows = _load_latest_rows_for_symbols(db, symbols)
            selected_rows = _load_rows_for_symbol_dates(db, _candidate_symbol_dates(candidates))
    except Exception:
        logger.exception("Failed to enrich selection task candidate market metrics task=%s", task.get("id"))
        return

    latest_by_symbol = {str(row.get("symbol") or "").upper(): row for row in latest_rows}
    selected_by_symbol_date = {
        (str(row.get("symbol") or "").upper(), str(row.get("trade_date") or "")[:10]): row
        for row in selected_rows
    }
    for item in candidates:
        symbol = str(item.get("symbol") or "").upper()
        row = latest_by_symbol.get(symbol)
        if not row:
            continue
        metrics = dict(item.get("metrics") or {})
        selected_date = str(metrics.get("trade_date") or metrics.get("selected_at") or "")[:10]
        selected_row = selected_by_symbol_date.get((symbol, selected_date)) if selected_date else None
        selected_close = _num((selected_row or {}).get("close")) if selected_row else _num(metrics.get("close"))
        current_close = _num(row.get("close"))
        metrics.setdefault("selected_at", metrics.get("trade_date") or str(row.get("trade_date") or ""))
        if selected_row is not None:
            metrics["trade_date"] = str(selected_row.get("trade_date") or metrics.get("trade_date") or "")
            metrics["close"] = selected_close
            metrics["amount_yi"] = _round_or_none((_num(selected_row.get("amount")) or 0) / 100_000_000, 2) if _num(selected_row.get("amount")) is not None else metrics.get("amount_yi")
            metrics["change_pct"] = _round_or_none(_change_pct(selected_close, _num(selected_row.get("pre_close"))), 2)
            _apply_selected_day_factor_metrics(metrics, selected_row, selected_close)
        metrics["current_trade_date"] = str(row.get("trade_date") or "")
        metrics["current_close"] = current_close
        metrics["current_change_pct"] = _round_or_none(_change_pct(current_close, _num(row.get("pre_close"))), 2)
        metrics["float_market_cap_yi"] = _round_or_none(_market_cap_value_yi(row, "float_market_cap"), 2)
        metrics["total_market_cap_yi"] = _round_or_none(_market_cap_value_yi(row, "total_market_cap"), 2)
        metrics["board"] = metrics.get("board") or _classify_board(symbol)
        metrics["industry"] = metrics.get("industry") or str(row.get("sw_industry_l1") or row.get("sw_industry_l2") or "").strip()
        if _num(metrics.get("change_pct")) is None:
            metrics["change_pct"] = metrics["current_change_pct"]
        if selected_close is not None and selected_close != 0 and current_close is not None:
            metrics["since_selected_change_pct"] = _round_or_none((current_close / selected_close - 1) * 100, 2)
        item["metrics"] = metrics
    _assign_candidate_recommendations(candidates)
    task["candidates"] = candidates


def _candidate_display_metrics_complete(candidates: list[dict[str, Any]]) -> bool:
    if not candidates:
        return True
    required_keys = (
        "close",
        "change_pct",
        "float_market_cap_yi",
        "total_market_cap_yi",
        "board",
        "selected_at",
        "current_close",
        "since_selected_change_pct",
    )
    for item in candidates:
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        for key in required_keys:
            value = metrics.get(key)
            if value is None or value == "":
                return False
    return True


def _candidate_symbol_dates(candidates: list[dict[str, Any]]) -> list[tuple[str, date]]:
    pairs: set[tuple[str, date]] = set()
    for item in candidates:
        symbol = str(item.get("symbol") or "").upper()
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        trade_date = _parse_trade_date((metrics or {}).get("trade_date") or (metrics or {}).get("selected_at"))
        if symbol and trade_date:
            pairs.add((symbol, trade_date))
    return sorted(pairs)


def _apply_selected_day_factor_metrics(metrics: dict[str, Any], row: dict[str, Any], selected_close: float | None) -> None:
    amount = _num(row.get("amount"))
    amount_ma20 = _num(row.get("amount_ma20"))
    if amount is not None:
        metrics["selected_amount_yi"] = _round_or_none(amount / 100_000_000, 2)
    if amount is not None and amount_ma20 is not None and amount_ma20 > 0:
        metrics["selected_amount_ratio20"] = _round_or_none(amount / amount_ma20, 2)

    for key in ("ma5", "ma10", "ma15", "ma20", "ma30", "ma60", "turnover_rate"):
        value = _num(row.get(key))
        if value is not None:
            metrics[f"selected_{key}"] = _round_or_none(value, 4 if key.startswith("ma") else 2)
        if key.startswith("ma"):
            count = _num(row.get(f"{key}_window_count"))
            if count is not None:
                metrics[f"selected_{key}_window_count"] = int(count)

    for key in ("close_lag3", "close_lag5"):
        value = _num(row.get(key))
        if value is not None:
            metrics[f"selected_{key}"] = _round_or_none(value, 4)

    if selected_close is None:
        return

    ma20 = _num(row.get("ma20"))
    ma60 = _num(row.get("ma60"))
    high60 = _num(row.get("high60"))
    low60 = _num(row.get("low60"))
    close_lag3 = _num(row.get("close_lag3"))
    close_lag5 = _num(row.get("close_lag5"))
    if ma20:
        metrics["selected_close_to_ma20_pct"] = _round_or_none((selected_close / ma20 - 1) * 100, 2)
    if ma60:
        metrics["selected_close_to_ma60_pct"] = _round_or_none((selected_close / ma60 - 1) * 100, 2)
    if high60 is not None and low60 is not None and high60 > low60:
        metrics["selected_position_60d"] = _round_or_none((selected_close - low60) / (high60 - low60), 4)
    if close_lag3:
        metrics["selected_ret3_pct"] = _round_or_none((selected_close / close_lag3 - 1) * 100, 2)
    if close_lag5:
        metrics["selected_ret5_pct"] = _round_or_none((selected_close / close_lag5 - 1) * 100, 2)


def _assign_candidate_recommendations(candidates: list[dict[str, Any]]) -> None:
    scored: list[tuple[float, float, str, dict[str, Any]]] = []
    for item in candidates:
        metrics = dict(item.get("metrics") or {})
        recommendation = _candidate_recommendation(metrics, item)
        metrics["recommendation_score"] = recommendation["score"]
        metrics["recommendation_sort_score"] = recommendation["sort_score"]
        metrics["recommendation_reasons"] = recommendation["reasons"]
        item["metrics"] = metrics
        scored.append((recommendation["sort_score"], float(item.get("score") or 0), str(item.get("symbol") or ""), item))

    scored.sort(key=lambda entry: (-entry[0], -entry[1], entry[2]))
    for rank, (_, _, _, item) in enumerate(scored, start=1):
        metrics = dict(item.get("metrics") or {})
        metrics["recommendation_rank"] = rank
        item["metrics"] = metrics


def _candidate_recommendation(metrics: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    change = _num(metrics.get("change_pct"))
    amount_ratio = _num(metrics.get("selected_amount_ratio20"))
    close_to_ma20 = _num(metrics.get("selected_close_to_ma20_pct"))
    close_to_ma60 = _num(metrics.get("selected_close_to_ma60_pct"))
    position_60d = _num(metrics.get("selected_position_60d"))
    ret3 = _num(metrics.get("selected_ret3_pct"))
    float_cap = _num(metrics.get("float_market_cap_yi"))
    turnover = _num(metrics.get("selected_turnover_rate"))
    score = 35.0
    reasons: list[str] = []

    if change is not None:
        if 7 <= change <= 11.5:
            score += 18
            reasons.append("入选涨幅有效")
        elif 3 <= change < 7:
            score += 10
            reasons.append("温和启动")
        elif 11.5 < change <= 15:
            score += 9
            reasons.append("入选日偏强")
        elif change > 15:
            score += 4
            reasons.append("当日涨幅过大")
        elif change >= 0:
            score += 5
            reasons.append("入选日收红")
        else:
            score -= 7
            reasons.append("入选日偏弱")

    if close_to_ma60 is not None:
        if close_to_ma60 >= 0:
            score += 14
            reasons.append("中期趋势确认")
        else:
            score -= 7
            reasons.append("仍在60日线下")

    if close_to_ma20 is not None:
        if 0 <= close_to_ma20 <= 4:
            score += 14
            reasons.append("贴近20日线启动")
        elif 4 < close_to_ma20 <= 10:
            score += 8
            reasons.append("站上20日线")
        elif close_to_ma20 > 10:
            score += 2
            reasons.append("站上20日线")
        else:
            score -= 5
        if close_to_ma20 > 12:
            score -= 8
            reasons.append("短线偏离偏高")

    if amount_ratio is not None:
        if 0.75 <= amount_ratio < 1.2:
            score += 8
            reasons.append("量能未过热")
        elif 1.2 <= amount_ratio <= 2.0:
            score += 7
            reasons.append("量能温和放大")
        elif 2.0 < amount_ratio <= 3.5:
            score -= 2
            reasons.append("量能明显放大")
        elif amount_ratio > 3.5:
            score -= 8
            reasons.append("放量过猛")
        elif amount_ratio < 0.75:
            score -= 2
            reasons.append("量能不足")

    if float_cap is not None:
        if 30 <= float_cap <= 300:
            score += 6
            reasons.append("流通市值适中")
        elif float_cap < 20:
            score -= 6
            reasons.append("流动性偏小")

    if ret3 is not None:
        if 3 <= ret3 <= 13:
            score += 12
            reasons.append("启动延续形态")
        elif 0 <= ret3 < 3:
            score += 5
            reasons.append("短线转强")
        elif 13 < ret3 <= 20:
            score += 3
            reasons.append("短线强势延续")
        elif ret3 > 20:
            score -= 6
            reasons.append("三日涨幅过热")
    if position_60d is not None:
        if 0.35 <= position_60d <= 0.75:
            score += 6
            reasons.append("区间位置健康")
        elif 0.75 < position_60d <= 0.9:
            score += 3
            reasons.append("60日区间强势")
        elif position_60d > 0.9:
            score -= 4
            reasons.append("区间位置偏高")
        elif position_60d <= 0.25:
            score -= 3
            reasons.append("仍处低位修复")
    if turnover is not None and turnover < 1:
        score -= 3
        reasons.append("换手偏低")

    base_score = _num(item.get("score"))
    if base_score is not None:
        score += max(-3.0, min(4.0, (base_score - 70.0) / 6.0))

    if not reasons:
        reasons.append("基础条件通过")
    deduped_reasons = _prioritize_recommendation_reasons(list(dict.fromkeys(reasons)))
    return {
        "score": _round_or_none(max(0, min(99, 55 + score * 0.35)), 1),
        "sort_score": _round_or_none(score, 4),
        "reasons": deduped_reasons[:5],
    }


def _prioritize_recommendation_reasons(reasons: list[str]) -> list[str]:
    priority = {
        "启动延续形态": 0,
        "贴近20日线启动": 1,
        "入选涨幅有效": 2,
        "量能未过热": 3,
        "量能温和放大": 4,
        "区间位置健康": 5,
        "中期趋势确认": 6,
        "流通市值适中": 7,
    }
    return [reason for _, reason in sorted(enumerate(reasons), key=lambda item: (priority.get(item[1], 50), item[0]))]


def _selection_confirmation_criteria() -> list[dict[str, str]]:
    return [
        {
            "key": "no_immediate_dead_cross",
            "name": "下一根K线不立刻死叉",
            "description": "按确认周期找到入选日最后一次首日波段金叉，下一根K线不能立刻死叉。分钟周期偏当日确认，日线偏次日跟踪。",
        },
        {
            "key": "break_previous_high",
            "name": "次日突破入选日前高",
            "description": "入选后第一个交易日最高价必须突破入选日最高价；下一交易日日K未入库前显示待确认。",
        },
    ]


def _normalize_confirmation_timeframe(value: Any) -> str:
    text_value = str(value or "30m").strip().lower()
    cn_map = {
        "1分钟": "1m",
        "5分钟": "5m",
        "15分钟": "15m",
        "30分钟": "30m",
        "60分钟": "60m",
        "日线": "1d",
        "日k": "1d",
        "日K": "1d",
    }
    text_value = cn_map.get(str(value or "").strip(), text_value)
    return text_value if text_value in {"1m", "5m", "15m", "30m", "60m", "1d"} else "30m"


def _candidate_selected_date(candidate: dict[str, Any]) -> date | None:
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    return _parse_trade_date((metrics or {}).get("trade_date") or (metrics or {}).get("selected_at"))


def _candidate_confirmation_symbol_dates(candidates: list[dict[str, Any]], fallback_date: date | None = None) -> list[tuple[str, date]]:
    pairs: set[tuple[str, date]] = set()
    for item in candidates:
        symbol = str(item.get("symbol") or "").upper()
        selected_date = _candidate_selected_date(item) or fallback_date
        if symbol and selected_date:
            pairs.add((symbol, selected_date))
    return sorted(pairs)


def _confirmation_status(status: str, reason: str, **extra: Any) -> dict[str, Any]:
    payload = {"status": status, "reason": reason}
    payload.update({key: value for key, value in extra.items() if value is not None})
    return payload


def _safe_iso_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def _evaluate_next_day_breakout(candidates: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        trade_date = _parse_trade_date(row.get("trade_date"))
        if not symbol or trade_date is None:
            continue
        normalized = dict(row)
        normalized["trade_date"] = trade_date
        rows_by_symbol.setdefault(symbol, []).append(normalized)
    for symbol_rows in rows_by_symbol.values():
        symbol_rows.sort(key=lambda item: item["trade_date"])

    result: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").upper()
        selected_date = _candidate_selected_date(candidate)
        symbol_rows = rows_by_symbol.get(symbol) or []
        selected_row = next((row for row in symbol_rows if row["trade_date"] == selected_date), None)
        if not symbol:
            continue
        if selected_date is None:
            result[symbol] = _confirmation_status("missing", "缺少入选日期，无法判断次日突破")
            continue
        if selected_row is None:
            result[symbol] = _confirmation_status("missing", "缺少入选日日K，无法判断前高", selected_date=selected_date.isoformat())
            continue
        selected_high = _num(selected_row.get("high"))
        if selected_high is None:
            result[symbol] = _confirmation_status("missing", "入选日日K缺少最高价", selected_date=selected_date.isoformat())
            continue
        next_row = next((row for row in symbol_rows if row["trade_date"] > selected_date), None)
        if next_row is None:
            result[symbol] = _confirmation_status(
                "pending",
                "下一交易日日K尚未入库，等待确认",
                selected_date=selected_date.isoformat(),
                selected_high=_round_or_none(selected_high, 4),
            )
            continue
        next_high = _num(next_row.get("high"))
        if next_high is None:
            result[symbol] = _confirmation_status(
                "missing",
                "下一交易日日K缺少最高价",
                selected_date=selected_date.isoformat(),
                selected_high=_round_or_none(selected_high, 4),
                next_trade_date=next_row["trade_date"].isoformat(),
            )
            continue
        passed = next_high > selected_high
        result[symbol] = _confirmation_status(
            "pass" if passed else "fail",
            "次日高点已突破入选日前高" if passed else "次日高点未突破入选日前高",
            selected_date=selected_date.isoformat(),
            selected_high=_round_or_none(selected_high, 4),
            next_trade_date=next_row["trade_date"].isoformat(),
            next_high=_round_or_none(next_high, 4),
        )
    return result


def _evaluate_intraday_no_immediate_dead_cross(
    candidates: list[dict[str, Any]],
    minute_frame: pd.DataFrame | None,
    timeframe: str,
) -> dict[str, dict[str, Any]]:
    symbols = sorted({str(item.get("symbol") or "").upper() for item in candidates if item.get("symbol")})
    if minute_frame is None or minute_frame.empty:
        return {symbol: _confirmation_status("missing", "缺少分钟K线，无法判断金叉后一根K线") for symbol in symbols}

    selected_dates = {str(item.get("symbol") or "").upper(): _candidate_selected_date(item) for item in candidates}
    try:
        aggregated = _aggregate_minute_frame(minute_frame, timeframe).sort_values(["symbol", "bar_end"]).reset_index(drop=True)
    except Exception:
        logger.exception("Failed to aggregate minute frame for selection confirmation timeframe=%s", timeframe)
        return {symbol: _confirmation_status("missing", "分钟K线聚合失败，无法判断金叉后一根K线") for symbol in symbols}

    result: dict[str, dict[str, Any]] = {}
    for symbol, group in aggregated.groupby("symbol", sort=False):
        normalized_symbol = str(symbol or "").upper()
        selected_date = selected_dates.get(normalized_symbol)
        if selected_date is None:
            result[normalized_symbol] = _confirmation_status("missing", "缺少入选日期，无法判断金叉后一根K线")
            continue
        try:
            computed = _compute_first_day_band(group.copy())
        except Exception:
            logger.exception("Failed to compute first-day-band confirmation symbol=%s timeframe=%s", normalized_symbol, timeframe)
            result[normalized_symbol] = _confirmation_status("missing", "该股票分钟K线计算失败，无法判断金叉后一根K线")
            continue
        if computed.empty:
            result[normalized_symbol] = _confirmation_status("missing", "分钟K线不足，无法计算首日波段")
            continue
        computed = computed.sort_values("bar_end").reset_index(drop=True)
        computed["bar_end"] = pd.to_datetime(computed["bar_end"])
        band = pd.to_numeric(computed["first_day_band"], errors="coerce")
        b1 = pd.to_numeric(computed["first_day_band_b1"], errors="coerce")
        prev_band = band.shift(1)
        prev_b1 = b1.shift(1)
        computed["cross_above"] = (band > b1) & ((prev_band <= prev_b1) | prev_band.isna() | prev_b1.isna())
        computed["cross_below"] = (band < b1) & (prev_band >= prev_b1)
        selected_hits = computed[
            (computed["bar_end"].dt.date == selected_date)
            & computed["cross_above"].fillna(False)
        ]
        if selected_hits.empty:
            result[normalized_symbol] = _confirmation_status(
                "missing",
                "入选日未找到对应周期首日波段金叉",
                selected_date=selected_date.isoformat(),
                timeframe=timeframe,
            )
            continue
        hit_index = int(selected_hits.index[-1])
        hit_row = computed.iloc[hit_index]
        next_index = hit_index + 1
        if next_index >= len(computed):
            result[normalized_symbol] = _confirmation_status(
                "pending",
                "金叉后一根K线尚未出现，等待确认",
                selected_date=selected_date.isoformat(),
                timeframe=timeframe,
                signal_bar_end=_safe_iso_value(hit_row.get("bar_end")),
            )
            continue
        next_row = computed.iloc[next_index]
        failed = bool(next_row.get("cross_below"))
        result[normalized_symbol] = _confirmation_status(
            "fail" if failed else "pass",
            "金叉后一根K线立刻死叉" if failed else "金叉后一根K线未立刻死叉",
            selected_date=selected_date.isoformat(),
            timeframe=timeframe,
            signal_bar_end=_safe_iso_value(hit_row.get("bar_end")),
            next_bar_end=_safe_iso_value(next_row.get("bar_end")),
        )

    for symbol in symbols:
        result.setdefault(symbol, _confirmation_status("missing", "缺少该股票分钟K线，无法判断金叉后一根K线"))
    return result


def _evaluate_daily_no_immediate_dead_cross(candidates: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    symbols = sorted({str(item.get("symbol") or "").upper() for item in candidates if item.get("symbol")})
    if not rows:
        return {symbol: _confirmation_status("missing", "缺少日K，无法判断下一根日线是否反叉") for symbol in symbols}

    frame = pd.DataFrame(rows)
    if frame.empty or "symbol" not in frame.columns or "trade_date" not in frame.columns:
        return {symbol: _confirmation_status("missing", "缺少日K，无法判断下一根日线是否反叉") for symbol in symbols}
    frame = frame.copy()
    frame["symbol"] = frame["symbol"].map(lambda value: str(value or "").upper())
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "trade_date"])
    frame["bar_end"] = frame["trade_date"]
    frame["bar_start"] = frame["trade_date"]
    selected_dates = {str(item.get("symbol") or "").upper(): _candidate_selected_date(item) for item in candidates}

    result: dict[str, dict[str, Any]] = {}
    for symbol, group in frame.groupby("symbol", sort=False):
        normalized_symbol = str(symbol or "").upper()
        if normalized_symbol not in symbols:
            continue
        selected_date = selected_dates.get(normalized_symbol)
        if selected_date is None:
            result[normalized_symbol] = _confirmation_status("missing", "缺少入选日期，无法判断下一根日线")
            continue
        try:
            computed = _compute_first_day_band(group.sort_values("bar_end").reset_index(drop=True))
        except Exception:
            logger.exception("Failed to compute daily first-day-band confirmation symbol=%s", normalized_symbol)
            result[normalized_symbol] = _confirmation_status("missing", "该股票日K计算失败，无法判断下一根日线")
            continue
        if computed.empty:
            result[normalized_symbol] = _confirmation_status("missing", "日K不足，无法计算首日波段")
            continue
        computed = computed.sort_values("bar_end").reset_index(drop=True)
        computed["bar_end"] = pd.to_datetime(computed["bar_end"])
        band = pd.to_numeric(computed["first_day_band"], errors="coerce")
        b1 = pd.to_numeric(computed["first_day_band_b1"], errors="coerce")
        prev_band = band.shift(1)
        prev_b1 = b1.shift(1)
        computed["cross_above"] = (band > b1) & ((prev_band <= prev_b1) | prev_band.isna() | prev_b1.isna())
        computed["cross_below"] = (band < b1) & (prev_band >= prev_b1)
        selected_hits = computed[
            (computed["bar_end"].dt.date == selected_date)
            & computed["cross_above"].fillna(False)
        ]
        if selected_hits.empty:
            result[normalized_symbol] = _confirmation_status(
                "missing",
                "入选日未找到日线首日波段金叉",
                selected_date=selected_date.isoformat(),
                timeframe="1d",
            )
            continue
        hit_index = int(selected_hits.index[-1])
        hit_row = computed.iloc[hit_index]
        next_index = hit_index + 1
        if next_index >= len(computed):
            result[normalized_symbol] = _confirmation_status(
                "pending",
                "下一交易日日K尚未入库，等待确认日线是否反叉",
                selected_date=selected_date.isoformat(),
                timeframe="1d",
                signal_bar_end=_safe_iso_value(hit_row.get("bar_end")),
            )
            continue
        next_row = computed.iloc[next_index]
        failed = bool(next_row.get("cross_below"))
        result[normalized_symbol] = _confirmation_status(
            "fail" if failed else "pass",
            "下一根日线立刻死叉" if failed else "下一根日线未立刻死叉",
            selected_date=selected_date.isoformat(),
            timeframe="1d",
            signal_bar_end=_safe_iso_value(hit_row.get("bar_end")),
            next_bar_end=_safe_iso_value(next_row.get("bar_end")),
        )

    for symbol in symbols:
        result.setdefault(symbol, _confirmation_status("missing", "缺少该股票日K，无法判断下一根日线"))
    return result


def _load_confirmation_daily_rows(db: Session, symbols: list[str], start_date: date, end_date: date) -> list[dict[str, Any]]:
    if not symbols:
        return []
    table_name = preferred_daily_kline_table()
    sql = text(
        f"""
        SELECT symbol, trade_date, open, high, low, close, pre_close, volume, amount
        FROM {table_name}
        WHERE symbol IN :symbols
          AND trade_date >= :start_date
          AND trade_date <= :end_date
        ORDER BY symbol, trade_date
        """
    ).bindparams(bindparam("symbols", expanding=True))
    return [
        dict(row)
        for row in db.execute(sql, {"symbols": symbols, "start_date": start_date, "end_date": end_date}).mappings().all()
    ]


def _load_rows_for_symbol_dates(db: Session, symbol_dates: list[tuple[str, date]]) -> list[dict[str, Any]]:
    if not symbol_dates:
        return []
    symbols = sorted({symbol for symbol, _ in symbol_dates})
    dates = sorted({trade_date for _, trade_date in symbol_dates})
    sql = text(
        """
        WITH recent AS (
            SELECT
                symbol,
                trade_date,
                close,
                pre_close,
                amount,
                turnover_rate,
                float_market_cap,
                total_market_cap,
                sw_industry_l1,
                sw_industry_l2,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS ma15,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
                ) AS ma15_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS ma30,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS ma30_window_count,
                AVG(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60,
                COUNT(close) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60_window_count,
                AVG(amount) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS amount_ma20,
                MAX(high) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS high60,
                MIN(low) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS low60,
                LAG(close, 3) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                ) AS close_lag3,
                LAG(close, 5) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                ) AS close_lag5
            FROM stock_daily_kline
            WHERE symbol IN :symbols
              AND trade_date >= CAST(:min_trade_date AS DATE) - INTERVAL '260 days'
              AND trade_date <= CAST(:max_trade_date AS DATE)
        )
        SELECT
            symbol,
            trade_date,
            close,
            pre_close,
            amount,
            amount_ma20,
            turnover_rate,
            float_market_cap,
            total_market_cap,
            ma5,
            ma5_window_count,
            ma10,
            ma10_window_count,
            ma15,
            ma15_window_count,
            ma20,
            ma20_window_count,
            ma30,
            ma30_window_count,
            ma60,
            ma60_window_count,
            high60,
            low60,
            close_lag3,
            close_lag5,
            sw_industry_l1,
            sw_industry_l2
        FROM recent
        WHERE symbol IN :symbols
          AND trade_date IN :trade_dates
        """
    ).bindparams(bindparam("symbols", expanding=True), bindparam("trade_dates", expanding=True))
    needed = {(symbol, trade_date.isoformat()) for symbol, trade_date in symbol_dates}
    rows = [
        dict(row)
        for row in db.execute(
            sql,
            {
                "symbols": symbols,
                "trade_dates": dates,
                "min_trade_date": dates[0],
                "max_trade_date": dates[-1],
            },
        ).mappings().all()
    ]
    return [
        row for row in rows
        if (str(row.get("symbol") or "").upper(), str(row.get("trade_date") or "")[:10]) in needed
    ]


def _load_latest_rows_for_symbols(db: Session, symbols: list[str]) -> list[dict[str, Any]]:
    sql = _latest_rows_for_symbols_sql()
    return [dict(row) for row in db.execute(sql, {"symbols": symbols}).mappings().all()]


def _latest_rows_for_symbols_sql():
    return text(
        """
        SELECT DISTINCT ON (symbol)
            symbol,
            trade_date,
            close,
            pre_close,
            float_market_cap,
            total_market_cap,
            sw_industry_l1,
            sw_industry_l2
        FROM stock_daily_kline
        WHERE symbol IN :symbols
        ORDER BY symbol, trade_date DESC
        """
    ).bindparams(bindparam("symbols", expanding=True))


def _market_cap_value_yi(row: dict[str, Any], key: str) -> float | None:
    value = _num(row.get(key))
    if value is None:
        return None
    return value / 100_000_000 if abs(value) > 1_000_000 else value


def _market_cap_yi(row: dict[str, Any]) -> float | None:
    value = _num(row.get("total_market_cap")) or _num(row.get("float_market_cap"))
    if value is None:
        return None
    return value / 100_000_000 if abs(value) > 1_000_000 else value


def _event_heat(row: dict[str, Any]) -> int:
    amount = _num(row.get("amount")) or 0.0
    amount_ma20 = _num(row.get("amount_ma20")) or 0.0
    close = _num(row.get("close"))
    pre_close = _num(row.get("pre_close"))
    turnover = _num(row.get("turnover_rate")) or 0.0
    amount_ratio = amount / amount_ma20 if amount_ma20 > 0 else 1.0
    change = abs(_change_pct(close, pre_close) or 0.0)
    heat = 45 + min(amount_ratio, 3.0) * 12 + min(change, 10.0) * 1.3 + min(turnover, 20.0) * 0.45
    return int(max(0, min(99, round(heat))))


def _score_candidate(row: dict[str, Any], mode: str, event_heat: int) -> int:
    close = _num(row.get("close"))
    pre_close = _num(row.get("pre_close"))
    amount = _num(row.get("amount")) or 0.0
    amount_ma20 = _num(row.get("amount_ma20")) or 0.0
    ma20 = _num(row.get("ma20"))
    turnover = _num(row.get("turnover_rate")) or 0.0
    change = _change_pct(close, pre_close) or 0.0
    amount_ratio = amount / amount_ma20 if amount_ma20 > 0 else 1.0
    score = 58.0
    score += max(-14.0, min(18.0, change * 1.6))
    score += min(amount / 100_000_000, 20.0) * 0.45
    score += min(max(amount_ratio - 1.0, 0.0), 2.0) * 8.0
    score += min(turnover, 20.0) * 0.35
    if close is not None and ma20 is not None and close >= ma20:
        score += 8.0
    if mode in {"catalyst", "hybrid"}:
        score = score * 0.78 + event_heat * 0.22
    return int(max(0, min(99, round(score))))


def _candidate_tags(config: dict[str, Any], row: dict[str, Any], board: str, event_heat: int) -> list[str]:
    tags = [board]
    signal_side = str(config.get("signal_side") or "").strip()
    if signal_side:
        tags.append(signal_side)
    industry = str(row.get("sw_industry_l1") or row.get("sw_industry_l2") or "").strip()
    if industry:
        tags.append(industry[:8])
    close = _num(row.get("close"))
    ma20 = _num(row.get("ma20"))
    amount = _num(row.get("amount")) or 0.0
    amount_ma20 = _num(row.get("amount_ma20")) or 0.0
    if close is not None and ma20 is not None and close >= ma20:
        tags.append("趋势")
    if amount_ma20 > 0 and amount >= amount_ma20 * 1.1:
        tags.append("放量")
    if event_heat >= 75:
        tags.append("高热度")
    return tags[:5]


def _candidate_reason(
    config: dict[str, Any],
    row: dict[str, Any],
    board: str,
    event_heat: int,
    *,
    strategy_signal: StrategySignalContext | None = None,
) -> str:
    close = _num(row.get("close"))
    pre_close = _num(row.get("pre_close"))
    amount = _num(row.get("amount"))
    ma20 = _num(row.get("ma20"))
    change = _change_pct(close, pre_close)
    parts = [f"{board}范围内"]
    if change is not None:
        parts.append(f"最新涨跌幅 {change:+.2f}%")
    if amount is not None:
        parts.append(f"成交额 {amount / 100_000_000:.2f} 亿")
    if close is not None and ma20 is not None:
        parts.append("站上20日均线" if close >= ma20 else "贴近20日均线")
    if strategy_signal is not None:
        parts.append(f"命中{strategy_signal.signal_name}")
    if config.get("mode") in {"catalyst", "hybrid"}:
        parts.append(f"事件热度 {event_heat}")
    return "，".join(parts)


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _change_pct(close: float | None, pre_close: float | None) -> float | None:
    if close is None or pre_close is None or pre_close == 0:
        return None
    return (close / pre_close - 1) * 100


def _round_or_none(value: float | None, digits: int) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")
