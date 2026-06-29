from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pandas as pd
from sqlalchemy import func, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.core.stock_map import get_reverse_stock_map
from api.core.stock_utils import normalize_symbol
from api.database import StockPoolGroupDB, StockPoolItemDB, WatchlistItemDB
from api.models.strategy_models import SelectionCenterTaskDB
from api.services.selection_center_service import _evaluate_strategy_rules
from api.services.strategy_compute_backend import compute_daily_features
from api.services.strategy_dsl_compiler import compile_strategy_dsl
from api.services.market_data_pipeline_service import preferred_daily_kline_table
from api.services.strategy_platform_repository import get_platform_strategy


MARKET_GROUP_ID = "market:all"
SELECTION_GROUP_PREFIX = "selection:"
DEFAULT_WATCHLIST_NAME = "我的自选"
DAILY_ITEM_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "volume",
    "amount",
    "turnover_rate",
    "float_market_cap",
    "total_market_cap",
    "net_profit_ttm",
    "sw_industry_l1",
    "sw_industry_l2",
    "sw_industry_l3",
)
ITEM_SORT_FIELDS = {
    "stock",
    "symbol",
    "name",
    "price",
    "change_pct",
    "pre_close",
    "open",
    "high",
    "low",
    "volume",
    "amount",
    "float_market_cap",
    "total_market_cap",
    "net_profit_ttm",
    "sector",
    "industry",
    "trade_date",
    "date",
    "joined_at",
}


def list_groups(db: Session, strategy_db: Session | None, user_id: str, *, selection_limit: int = 50) -> dict[str, Any]:
    default_group = ensure_default_group(db, user_id)
    persisted = (
        db.query(StockPoolGroupDB)
        .filter(StockPoolGroupDB.user_id == user_id)
        .order_by(StockPoolGroupDB.sort_order, StockPoolGroupDB.created_at)
        .all()
    )
    groups = [
        {
            "id": MARKET_GROUP_ID,
            "name": "全市场",
            "group_type": "market",
            "readonly": True,
            "is_default": False,
            "sort_order": -100,
            "item_count": _market_item_count(db),
        }
    ]
    groups.extend(_group_to_dict(db, row) for row in persisted)
    if not any(group["id"] == default_group["id"] for group in groups):
        groups.insert(1, default_group)
    groups.extend(_selection_task_groups(strategy_db, user_id, limit=selection_limit) if strategy_db is not None else [])
    return {"groups": groups, "total": len(groups)}


def ensure_default_group(db: Session, user_id: str) -> dict[str, Any]:
    group = (
        db.query(StockPoolGroupDB)
        .filter(StockPoolGroupDB.user_id == user_id, StockPoolGroupDB.is_default.is_(True))
        .order_by(StockPoolGroupDB.created_at)
        .first()
    )
    if group is None:
        group = StockPoolGroupDB(
            id=uuid4().hex,
            user_id=user_id,
            name=DEFAULT_WATCHLIST_NAME,
            group_type="watchlist",
            is_default=True,
            sort_order=0,
        )
        db.add(group)
        db.commit()
        db.refresh(group)
    _import_legacy_watchlist(db, user_id, group.id)
    return _group_to_dict(db, group)


def create_group(db: Session, user_id: str, name: str) -> dict[str, Any]:
    clean_name = _clean_group_name(name)
    max_sort = (
        db.query(func.max(StockPoolGroupDB.sort_order))
        .filter(StockPoolGroupDB.user_id == user_id)
        .scalar()
    )
    group = StockPoolGroupDB(
        id=uuid4().hex,
        user_id=user_id,
        name=clean_name,
        group_type="custom",
        is_default=False,
        sort_order=int(max_sort or 0) + 10,
    )
    db.add(group)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError(f"分组已存在：{clean_name}") from exc
    db.refresh(group)
    return _group_to_dict(db, group)


def update_group(db: Session, user_id: str, group_id: str, *, name: str | None = None, sort_order: int | None = None) -> dict[str, Any]:
    group = _get_persisted_group(db, user_id, group_id)
    if group is None:
        raise KeyError("分组不存在")
    if name is not None:
        group.name = _clean_group_name(name)
    if sort_order is not None:
        group.sort_order = int(sort_order)
    group.updated_at = _now()
    db.add(group)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ValueError(f"分组已存在：{group.name}") from exc
    db.refresh(group)
    return _group_to_dict(db, group)


def delete_group(db: Session, user_id: str, group_id: str) -> bool:
    group = _get_persisted_group(db, user_id, group_id)
    if group is None:
        return False
    if group.is_default:
        raise ValueError("默认自选分组不能删除")
    db.query(StockPoolItemDB).filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id).delete()
    db.delete(group)
    db.commit()
    return True


def list_group_items(
    db: Session,
    user_id: str,
    group_id: str,
    *,
    strategy_db: Session | None = None,
    page: int = 1,
    page_size: int = 80,
    q: str | None = None,
    sector: str | None = None,
    sort_by: str | None = None,
    sort_direction: str | None = None,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    page_size = min(max(int(page_size or 80), 1), 300)
    sort_by = _normalize_sort_by(sort_by)
    sort_direction = _normalize_sort_direction(sort_direction)
    if group_id == MARKET_GROUP_ID:
        return _list_market_items(db, page=page, page_size=page_size, q=q, sector=sector, sort_by=sort_by, sort_direction=sort_direction)
    if group_id.startswith(SELECTION_GROUP_PREFIX):
        if strategy_db is None:
            raise ValueError("读取选股结果分组需要策略数据库")
        task_id = group_id[len(SELECTION_GROUP_PREFIX):]
        return _list_selection_task_items(
            db,
            strategy_db,
            user_id,
            task_id,
            page=page,
            page_size=page_size,
            q=q,
            sector=sector,
            sort_by=sort_by,
            sort_direction=sort_direction,
        )

    group = _get_persisted_group(db, user_id, group_id)
    if group is None:
        raise KeyError("分组不存在")
    query = (
        db.query(StockPoolItemDB)
        .filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id)
        .order_by(StockPoolItemDB.sort_order, StockPoolItemDB.created_at)
    )
    if q:
        query = query.filter((StockPoolItemDB.symbol.ilike(f"%{q}%")) | (StockPoolItemDB.name.ilike(f"%{q}%")))
    rows = query.all() if sort_by or sector else query.offset((page - 1) * page_size).limit(page_size).all()
    latest = _latest_daily_map(db, [row.symbol for row in rows])
    items = [_item_to_dict(row, latest.get(row.symbol)) for row in rows]
    if sector:
        items = [item for item in items if item.get("sector") == sector]
    if sort_by:
        items = _sort_items(items, sort_by, sort_direction)
    total = len(items) if sort_by or sector else query.count()
    if sort_by or sector:
        start = (page - 1) * page_size
        items = items[start:start + page_size]
    return {"group": _group_to_dict(db, group), "items": items, "total": total, "page": page, "page_size": page_size}


def add_group_item(db: Session, user_id: str, group_id: str, symbol: str, *, name: str | None = None, source: str = "manual") -> dict[str, Any]:
    group = _get_persisted_group(db, user_id, group_id)
    if group is None:
        raise KeyError("分组不存在")
    normalized = normalize_symbol(symbol)
    code_to_name = get_reverse_stock_map()
    display_name = (name or code_to_name.get(normalized) or normalized).strip()
    existing = (
        db.query(StockPoolItemDB)
        .filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id, StockPoolItemDB.symbol == normalized)
        .first()
    )
    if existing is not None:
        return {"status": "duplicate", "message": "已在该分组中", "item": _item_to_dict(existing, _latest_daily_map(db, [existing.symbol]).get(existing.symbol))}
    max_sort = (
        db.query(func.max(StockPoolItemDB.sort_order))
        .filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id)
        .scalar()
    )
    item = StockPoolItemDB(
        id=uuid4().hex,
        user_id=user_id,
        group_id=group_id,
        symbol=normalized,
        name=display_name,
        source=source,
        sort_order=int(max_sort or 0) + 10,
    )
    db.add(item)
    _sync_legacy_watchlist_for_default_group(db, user_id, group, normalized)
    db.commit()
    db.refresh(item)
    return {"status": "added", "message": "已加入股票池", "item": _item_to_dict(item, _latest_daily_map(db, [item.symbol]).get(item.symbol))}


def delete_group_item(db: Session, user_id: str, group_id: str, item_id: str) -> bool:
    group = _get_persisted_group(db, user_id, group_id)
    if group is None:
        return False
    item = (
        db.query(StockPoolItemDB)
        .filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id, StockPoolItemDB.id == item_id)
        .first()
    )
    if item is None:
        return False
    db.delete(item)
    db.commit()
    return True


def copy_selection_task_to_group(db: Session, strategy_db: Session, user_id: str, task_id: str, *, name: str | None = None) -> dict[str, Any]:
    task = _get_selection_task(strategy_db, user_id, task_id)
    if task is None:
        raise KeyError("选股任务不存在")
    candidates = list(task.candidates_json or [])
    if not candidates:
        raise ValueError("该选股任务没有可复制的股票")
    group_name = _unique_group_name(db, user_id, name or _selection_group_name(task))
    group = create_group(db, user_id, group_name)
    added = 0
    duplicates = 0
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").strip()
        if not symbol:
            continue
        result = add_group_item(
            db,
            user_id,
            group["id"],
            symbol,
            name=str(candidate.get("name") or ""),
            source="selection",
        )
        if result["status"] == "added":
            added += 1
        elif result["status"] == "duplicate":
            duplicates += 1
    return {"group": _group_to_dict(db, _get_persisted_group(db, user_id, group["id"])), "added": added, "duplicates": duplicates}


def preview_strategy_markers(
    db: Session,
    *,
    strategy_db: Session | None = None,
    symbol: str,
    strategy_id: str,
    period: str = "daily",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_symbol(symbol)
    end_date = end_date or datetime.now().date().isoformat()
    start_date = start_date or (datetime.fromisoformat(end_date).date() - timedelta(days=180)).isoformat()
    rows = _load_daily_rows(db, normalized, start_date, end_date)
    if strategy_db is not None:
        strategy_payload = get_platform_strategy(strategy_db, strategy_id)
        if strategy_payload is not None:
            return _preview_strategy_dsl(
                rows,
                symbol=normalized,
                strategy_id=strategy_id,
                strategy_payload=strategy_payload,
                period=period,
                start_date=start_date,
                end_date=end_date,
            )
    markers: list[dict[str, Any]] = []
    closes: list[float] = []
    previous_close: float | None = None
    previous_ma5: float | None = None
    for row in rows:
        close = _to_float(row.get("close"))
        if close is None:
            continue
        closes.append(close)
        if len(closes) < 5:
            continue
        ma5 = sum(closes[-5:]) / 5
        trade_date = row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"])
        if previous_close is not None and previous_ma5 is not None:
            if previous_close <= previous_ma5 and close > ma5:
                markers.append(_preview_marker(trade_date, "buy", close, f"MA5 上穿确认 · {strategy_id}"))
            elif previous_close >= previous_ma5 and close < ma5:
                markers.append(_preview_marker(trade_date, "sell", close, f"MA5 下穿确认 · {strategy_id}"))
        previous_close = close
        previous_ma5 = ma5
    return {
        "symbol": normalized,
        "strategy_id": strategy_id,
        "period": period,
        "start_date": start_date,
        "end_date": end_date,
        "markers": markers[-120:],
        "source": "postgresql_daily_ma5_preview",
        "message": None if rows else "暂无可用于策略预览的日K数据",
    }


def _preview_strategy_dsl(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    strategy_id: str,
    strategy_payload: dict[str, Any],
    period: str,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    if not rows:
        return {
            "symbol": symbol,
            "strategy_id": strategy_id,
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "markers": [],
            "source": "strategy_dsl:empty",
            "message": "暂无可用于策略预览的日K数据",
        }
    dsl = _strategy_payload_dsl(strategy_payload)
    compiled = compile_strategy_dsl(dsl)
    if compiled.status != "passed":
        errors = "；".join(compiled.errors[:3]) or "未知编译错误"
        raise ValueError(f"策略编译失败：{errors}")

    frame = pd.DataFrame.from_records([
        {
            "symbol": symbol,
            "date": row.get("trade_date"),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "turnover_rate": row.get("turnover_rate"),
            "pre_close": row.get("pre_close"),
            "float_market_cap": row.get("float_market_cap"),
            "total_market_cap": row.get("total_market_cap"),
            "net_profit_ttm": row.get("net_profit_ttm"),
        }
        for row in rows
    ])
    _coerce_strategy_frame_numbers(frame)
    features, backend = compute_daily_features(frame, compiled)
    features["symbol"] = features["symbol"].astype(str).str.upper()
    features["date"] = pd.to_datetime(features["date"])
    markers: list[dict[str, Any]] = []
    warnings: list[str] = []
    strategy_name = str(strategy_payload.get("name") or strategy_id)

    entry_rules = list(compiled.entry_rules or [])
    if entry_rules:
        try:
            logic = str((dsl.get("entry") or {}).get("logic") or "all")
            buy_mask = _evaluate_strategy_rules(features, entry_rules, side="buy", logic=logic)
            markers.extend(_markers_from_strategy_mask(features, buy_mask, side="buy", strategy_name=strategy_name))
        except Exception as exc:
            warnings.append(f"买点规则暂不可预览：{exc}")
    else:
        warnings.append("策略没有配置买点规则")

    exit_rules = list(compiled.exit_rules or [])
    if exit_rules:
        try:
            logic = str((dsl.get("exit") or {}).get("logic") or "any")
            sell_mask = _evaluate_strategy_rules(features, exit_rules, side="sell", logic=logic)
            markers.extend(_markers_from_strategy_mask(features, sell_mask, side="sell", strategy_name=strategy_name))
        except Exception as exc:
            warnings.append(f"卖点规则暂不可预览：{exc}")

    markers.sort(key=lambda item: (item.get("date") or "", item.get("side") or ""))
    return {
        "symbol": symbol,
        "strategy_id": strategy_id,
        "period": period,
        "start_date": start_date,
        "end_date": end_date,
        "markers": markers[-160:],
        "source": f"strategy_dsl:{backend}",
        "message": "；".join(warnings) if warnings else None,
    }


def _selection_task_groups(strategy_db: Session, user_id: str, *, limit: int) -> list[dict[str, Any]]:
    rows = (
        strategy_db.query(SelectionCenterTaskDB)
        .filter(SelectionCenterTaskDB.user_id == user_id, SelectionCenterTaskDB.status == "completed")
        .order_by(SelectionCenterTaskDB.created_at.desc(), SelectionCenterTaskDB.id.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": f"{SELECTION_GROUP_PREFIX}{row.id}",
            "name": _selection_group_name(row),
            "group_type": "selection",
            "readonly": True,
            "is_default": False,
            "sort_order": 10000 + index,
            "item_count": len(row.candidates_json or []),
            "candidate_count": len(row.candidates_json or []),
            "source_task_id": row.id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        for index, row in enumerate(rows)
    ]


def _list_selection_task_items(
    db: Session,
    strategy_db: Session,
    user_id: str,
    task_id: str,
    *,
    page: int,
    page_size: int,
    q: str | None,
    sector: str | None,
    sort_by: str | None,
    sort_direction: str,
) -> dict[str, Any]:
    task = _get_selection_task(strategy_db, user_id, task_id)
    if task is None:
        raise KeyError("选股任务不存在")
    candidates = list(task.candidates_json or [])
    if q:
        query = q.strip().lower()
        candidates = [
            item for item in candidates
            if query in str(item.get("symbol") or "").lower() or query in str(item.get("name") or "").lower()
        ]
    latest = _latest_daily_map(db, [str(item.get("symbol") or "") for item in candidates])
    items = [_selection_candidate_to_item(item, latest.get(normalize_symbol(str(item.get("symbol") or "")))) for item in candidates]
    if sector:
        items = [item for item in items if item.get("sector") == sector]
    if sort_by:
        items = _sort_items(items, sort_by, sort_direction)
    total = len(items)
    start = (page - 1) * page_size
    return {
        "group": {
            "id": f"{SELECTION_GROUP_PREFIX}{task.id}",
            "name": _selection_group_name(task),
            "group_type": "selection",
            "readonly": True,
            "item_count": len(task.candidates_json or []),
            "source_task_id": task.id,
        },
        "items": items[start:start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def _list_market_items(
    db: Session,
    *,
    page: int,
    page_size: int,
    q: str | None,
    sector: str | None,
    sort_by: str | None,
    sort_direction: str,
) -> dict[str, Any]:
    table_name = _preferred_existing_daily_table(db)
    if not table_name:
        return {"group": _market_group(0), "items": [], "total": 0, "page": page, "page_size": page_size}
    target_date = _latest_trade_date(db, table_name)
    if not target_date:
        return {"group": _market_group(0), "items": [], "total": 0, "page": page, "page_size": page_size}
    columns = _table_columns(db, table_name)
    filters = ["trade_date = :target_date", "close IS NOT NULL"]
    params: dict[str, Any] = {"target_date": target_date, "limit": page_size, "offset": (page - 1) * page_size}
    if q:
        filters.append("symbol ILIKE :q")
        params["q"] = f"%{q.strip()}%"
    if sector and "sw_industry_l1" in columns:
        filters.append("sw_industry_l1 = :sector")
        params["sector"] = sector
    where_clause = " AND ".join(filters)
    order_clause = _market_order_clause(columns, sort_by, sort_direction)
    total = int(db.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"), params).scalar() or 0)
    rows = db.execute(
        text(
            f"""
            SELECT {", ".join(_daily_item_select_parts(columns))}
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY {order_clause}
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()
    code_to_name = get_reverse_stock_map()
    items = []
    for row in rows:
        symbol = _code_to_symbol(str(row["symbol"]))
        latest = _daily_row_to_item_fields(row, code_to_name)
        items.append({
            "id": symbol,
            "symbol": symbol,
            "name": code_to_name.get(symbol, symbol),
            **latest,
            "source": "market",
            "joined_at": latest.get("trade_date"),
        })
    return {"group": _market_group(total), "items": items, "total": total, "page": page, "page_size": page_size}


def _group_to_dict(db: Session, group: StockPoolGroupDB | dict[str, Any] | None) -> dict[str, Any]:
    if group is None:
        return {}
    if isinstance(group, dict):
        return group
    item_count = db.query(StockPoolItemDB).filter(StockPoolItemDB.user_id == group.user_id, StockPoolItemDB.group_id == group.id).count()
    return {
        "id": group.id,
        "name": group.name,
        "group_type": group.group_type,
        "readonly": False,
        "is_default": bool(group.is_default),
        "sort_order": int(group.sort_order or 0),
        "item_count": int(item_count),
        "created_at": group.created_at.isoformat() if group.created_at else None,
        "updated_at": group.updated_at.isoformat() if group.updated_at else None,
    }


def _item_to_dict(item: StockPoolItemDB, latest: dict[str, Any] | None = None) -> dict[str, Any]:
    latest = latest or {}
    return {
        "id": item.id,
        "group_id": item.group_id,
        "symbol": item.symbol,
        "name": item.name or latest.get("name") or item.symbol,
        **_latest_item_payload(latest),
        "source": item.source,
        "joined_at": item.created_at.isoformat() if item.created_at else None,
        "trade_date": latest.get("trade_date"),
        "readonly": False,
    }


def _selection_candidate_to_item(candidate: dict[str, Any], latest: dict[str, Any] | None) -> dict[str, Any]:
    latest = latest or {}
    symbol = normalize_symbol(str(candidate.get("symbol") or ""))
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    payload = _latest_item_payload(latest)
    payload["price"] = _to_float(metrics.get("price") or metrics.get("current_price") or metrics.get("current_close")) or payload.get("price")
    payload["change_pct"] = _to_float(metrics.get("current_change_pct") or metrics.get("change_pct") or metrics.get("入选涨幅比例")) or payload.get("change_pct")
    payload["sector"] = metrics.get("sw_industry_l1") or metrics.get("sector") or metrics.get("industry") or payload.get("sector") or ""
    payload["industry_l1"] = payload.get("industry_l1") or payload["sector"]
    payload["float_market_cap"] = payload.get("float_market_cap") or _yi_to_value(metrics.get("float_market_cap_yi"))
    payload["total_market_cap"] = payload.get("total_market_cap") or _yi_to_value(metrics.get("total_market_cap_yi") or metrics.get("market_cap_yi"))
    return {
        "id": symbol,
        "symbol": symbol,
        "name": candidate.get("name") or latest.get("name") or symbol,
        **payload,
        "source": "selection",
        "joined_at": candidate.get("selected_at") or candidate.get("date") or "",
        "readonly": True,
    }


def _latest_daily_map(db: Session, symbols: list[str]) -> dict[str, dict[str, Any]]:
    normalized = [normalize_symbol(symbol) for symbol in symbols if str(symbol or "").strip()]
    if not normalized:
        return {}
    table_name = _preferred_existing_daily_table(db)
    if not table_name:
        return {}
    target_date = _latest_trade_date(db, table_name)
    if not target_date:
        return {}
    columns = _table_columns(db, table_name)
    candidates = sorted({variant for symbol in normalized for variant in _symbol_variants(symbol)})
    placeholders = ", ".join(f":symbol_{index}" for index in range(len(candidates)))
    params = {"target_date": target_date, **{f"symbol_{index}": value for index, value in enumerate(candidates)}}
    rows = db.execute(
        text(
            f"""
            SELECT {", ".join(_daily_item_select_parts(columns))}
            FROM {table_name}
            WHERE trade_date = :target_date AND symbol IN ({placeholders})
            """
        ),
        params,
    ).mappings().all()
    code_to_name = get_reverse_stock_map()
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = _code_to_symbol(str(row["symbol"]))
        result[symbol] = _daily_row_to_item_fields(row, code_to_name)
    return result


def _load_daily_rows(db: Session, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    table_name = _preferred_existing_daily_table(db)
    if not table_name:
        return []
    columns = _table_columns(db, table_name)
    select_parts = [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume" if "volume" in columns else "NULL AS volume",
        "amount" if "amount" in columns else "NULL AS amount",
        "turnover_rate" if "turnover_rate" in columns else "NULL AS turnover_rate",
        "pre_close" if "pre_close" in columns else "NULL AS pre_close",
        "float_market_cap" if "float_market_cap" in columns else "NULL AS float_market_cap",
        "total_market_cap" if "total_market_cap" in columns else "NULL AS total_market_cap",
        "net_profit_ttm" if "net_profit_ttm" in columns else "NULL AS net_profit_ttm",
    ]
    variants = sorted(_symbol_variants(symbol))
    placeholders = ", ".join(f":symbol_{index}" for index in range(len(variants)))
    params = {
        "start_date": start_date,
        "end_date": end_date,
        **{f"symbol_{index}": value for index, value in enumerate(variants)},
    }
    return list(
        db.execute(
            text(
                f"""
                SELECT {", ".join(select_parts)}
                FROM {table_name}
                WHERE symbol IN ({placeholders}) AND trade_date >= :start_date AND trade_date <= :end_date
                ORDER BY trade_date ASC
                """
            ),
            params,
        ).mappings().all()
    )


def _strategy_payload_dsl(strategy_payload: dict[str, Any]) -> dict[str, Any]:
    current_version = strategy_payload.get("current_version") or {}
    dsl = current_version.get("dsl") or strategy_payload.get("dsl")
    if not isinstance(dsl, dict) or not dsl:
        raise ValueError("策略当前版本没有可执行 DSL")
    return deepcopy(dsl)


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


def _markers_from_strategy_mask(
    features: pd.DataFrame,
    mask: pd.Series,
    *,
    side: str,
    strategy_name: str,
) -> list[dict[str, Any]]:
    matched = features[mask.fillna(False)].sort_values("date")
    label = "买点" if side == "buy" else "卖点"
    return [
        _preview_marker(
            row["date"].date().isoformat() if hasattr(row["date"], "date") else str(row["date"])[:10],
            side,
            float(row["close"]),
            f"{strategy_name} · {label}",
        )
        for _, row in matched.iterrows()
        if _to_float(row.get("close")) is not None
    ]


def _import_legacy_watchlist(db: Session, user_id: str, group_id: str) -> None:
    legacy_rows = (
        db.query(WatchlistItemDB)
        .filter(WatchlistItemDB.user_id == user_id)
        .order_by(WatchlistItemDB.sort_order, WatchlistItemDB.created_at)
        .all()
    )
    if not legacy_rows:
        return
    existing_symbols = {
        row.symbol
        for row in db.query(StockPoolItemDB.symbol)
        .filter(StockPoolItemDB.user_id == user_id, StockPoolItemDB.group_id == group_id)
        .all()
    }
    code_to_name = get_reverse_stock_map()
    changed = False
    for index, legacy in enumerate(legacy_rows):
        symbol = normalize_symbol(legacy.symbol)
        if symbol in existing_symbols:
            continue
        db.add(
            StockPoolItemDB(
                id=uuid4().hex,
                user_id=user_id,
                group_id=group_id,
                symbol=symbol,
                name=code_to_name.get(symbol, symbol),
                source="legacy_watchlist",
                sort_order=int(legacy.sort_order or index),
            )
        )
        changed = True
    if changed:
        db.commit()


def _sync_legacy_watchlist_for_default_group(db: Session, user_id: str, group: StockPoolGroupDB, symbol: str) -> None:
    if not group.is_default:
        return
    existing = db.query(WatchlistItemDB).filter(WatchlistItemDB.user_id == user_id, WatchlistItemDB.symbol == symbol).first()
    if existing is None:
        db.add(WatchlistItemDB(id=uuid4().hex, user_id=user_id, symbol=symbol))


def _get_persisted_group(db: Session, user_id: str, group_id: str) -> StockPoolGroupDB | None:
    return (
        db.query(StockPoolGroupDB)
        .filter(StockPoolGroupDB.user_id == user_id, StockPoolGroupDB.id == group_id)
        .first()
    )


def _get_selection_task(strategy_db: Session, user_id: str, task_id: str) -> SelectionCenterTaskDB | None:
    return (
        strategy_db.query(SelectionCenterTaskDB)
        .filter(SelectionCenterTaskDB.user_id == user_id, SelectionCenterTaskDB.id == task_id)
        .first()
    )


def _clean_group_name(name: str) -> str:
    clean = str(name or "").strip()
    if not clean:
        raise ValueError("分组名称不能为空")
    return clean[:120]


def _unique_group_name(db: Session, user_id: str, base_name: str) -> str:
    base = _clean_group_name(base_name)
    exists = {
        row.name
        for row in db.query(StockPoolGroupDB.name)
        .filter(StockPoolGroupDB.user_id == user_id)
        .all()
    }
    if base not in exists:
        return base
    for index in range(2, 100):
        candidate = f"{base} ({index})"
        if candidate not in exists:
            return candidate
    return f"{base} ({uuid4().hex[:6]})"


def _selection_group_name(task: SelectionCenterTaskDB) -> str:
    created = task.created_at.strftime("%Y-%m-%d") if task.created_at else ""
    count = len(task.candidates_json or [])
    prefix = f"{created} " if created else ""
    return f"{prefix}{task.name} {count}只"


def _preferred_existing_daily_table(db: Session) -> str | None:
    candidates = [preferred_daily_kline_table(), "stock_daily_kline", "pub_stock_daily_kline", "market_stock_daily_kline"]
    for table_name in dict.fromkeys(candidates):
        if _has_table(db, table_name):
            return table_name
    return None


def _market_item_count(db: Session) -> int:
    table_name = _preferred_existing_daily_table(db)
    if not table_name:
        return 0
    target_date = _latest_trade_date(db, table_name)
    if not target_date:
        return 0
    return int(db.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE trade_date = :target_date"), {"target_date": target_date}).scalar() or 0)


def _market_group(total: int) -> dict[str, Any]:
    return {
        "id": MARKET_GROUP_ID,
        "name": "全市场",
        "group_type": "market",
        "readonly": True,
        "is_default": False,
        "sort_order": -100,
        "item_count": total,
    }


def _latest_trade_date(db: Session, table_name: str):
    try:
        return db.execute(text(f"SELECT MAX(trade_date) FROM {table_name}")).scalar()
    except Exception:
        return None


def _table_columns(db: Session, table_name: str) -> set[str]:
    try:
        return {column["name"] for column in inspect(db.bind).get_columns(table_name)}
    except Exception:
        return set()


def _normalize_sort_by(value: str | None) -> str | None:
    key = str(value or "").strip()
    return key if key in ITEM_SORT_FIELDS else None


def _normalize_sort_direction(value: str | None) -> str:
    return "desc" if str(value or "").lower() == "desc" else "asc"


def _market_order_clause(columns: set[str], sort_by: str | None, sort_direction: str) -> str:
    direction = "DESC" if sort_direction == "desc" else "ASC"
    expression = "symbol"
    if sort_by in {"stock", "symbol", "name"}:
        expression = "symbol"
    elif sort_by == "price":
        expression = "close"
    elif sort_by == "change_pct" and {"close", "pre_close"}.issubset(columns):
        expression = "CASE WHEN pre_close IS NOT NULL AND pre_close <> 0 THEN (close - pre_close) / pre_close END"
    elif sort_by in {"sector", "industry"} and "sw_industry_l1" in columns:
        expression = "sw_industry_l1"
    elif sort_by in {"trade_date", "date", "joined_at"}:
        expression = "trade_date"
    elif sort_by and sort_by in columns:
        expression = sort_by
    return f"{expression} {direction} NULLS LAST, symbol ASC"


def _sort_items(items: list[dict[str, Any]], sort_by: str, sort_direction: str) -> list[dict[str, Any]]:
    if not sort_by:
        return items

    def value_for(item: dict[str, Any]) -> Any:
        if sort_by in {"stock", "name"}:
            return (str(item.get("name") or "").lower(), str(item.get("symbol") or ""))
        if sort_by == "symbol":
            return str(item.get("symbol") or "")
        if sort_by in {"sector", "industry"}:
            return (
                str(item.get("sector") or "").lower(),
                str(item.get("industry_l2") or "").lower(),
                str(item.get("industry_l3") or "").lower(),
            )
        if sort_by in {"trade_date", "date", "joined_at"}:
            return str(item.get("trade_date") or item.get("joined_at") or "")
        return _to_float(item.get(sort_by))

    valued: list[tuple[Any, dict[str, Any]]] = []
    empty: list[dict[str, Any]] = []
    for item in items:
        value = value_for(item)
        if value is None or value == "" or value == ("", "", ""):
            empty.append(item)
        else:
            valued.append((value, item))
    valued.sort(key=lambda pair: pair[0], reverse=sort_direction == "desc")
    return [item for _, item in valued] + empty


def _daily_item_select_parts(columns: set[str]) -> list[str]:
    return [
        "symbol",
        "trade_date",
        *[
            field if field in columns else f"NULL AS {field}"
            for field in DAILY_ITEM_FIELDS
        ],
    ]


def _daily_row_to_item_fields(row: dict[str, Any], code_to_name: dict[str, str]) -> dict[str, Any]:
    symbol = _code_to_symbol(str(row.get("symbol") or ""))
    close = _to_float(row.get("close"))
    pre_close = _to_float(row.get("pre_close"))
    trade_date = row.get("trade_date")
    industry_l1 = row.get("sw_industry_l1") or ""
    return {
        "name": code_to_name.get(symbol),
        "price": close,
        "change_pct": _change_pct(close, pre_close),
        "open": _to_float(row.get("open")),
        "high": _to_float(row.get("high")),
        "low": _to_float(row.get("low")),
        "pre_close": pre_close,
        "volume": _to_float(row.get("volume")),
        "amount": _to_float(row.get("amount")),
        "turnover_rate": _to_float(row.get("turnover_rate")),
        "float_market_cap": _to_float(row.get("float_market_cap")),
        "total_market_cap": _to_float(row.get("total_market_cap")),
        "net_profit_ttm": _to_float(row.get("net_profit_ttm")),
        "sector": industry_l1,
        "industry_l1": industry_l1,
        "industry_l2": row.get("sw_industry_l2") or "",
        "industry_l3": row.get("sw_industry_l3") or "",
        "trade_date": trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date or ""),
    }


def _latest_item_payload(latest: dict[str, Any]) -> dict[str, Any]:
    return {
        "price": latest.get("price"),
        "change_pct": latest.get("change_pct"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "pre_close": latest.get("pre_close"),
        "volume": latest.get("volume"),
        "amount": latest.get("amount"),
        "turnover_rate": latest.get("turnover_rate"),
        "float_market_cap": latest.get("float_market_cap"),
        "total_market_cap": latest.get("total_market_cap"),
        "net_profit_ttm": latest.get("net_profit_ttm"),
        "sector": latest.get("sector") or "",
        "industry_l1": latest.get("industry_l1") or latest.get("sector") or "",
        "industry_l2": latest.get("industry_l2") or "",
        "industry_l3": latest.get("industry_l3") or "",
    }


def _has_table(db: Session, table_name: str) -> bool:
    try:
        return inspect(db.bind).has_table(table_name)
    except Exception:
        return False


def _symbol_variants(symbol: str) -> set[str]:
    normalized = normalize_symbol(symbol)
    code = normalized.split(".", 1)[0]
    return {normalized, code}


def _code_to_symbol(code: str) -> str:
    value = str(code or "").upper()
    if "." in value:
        return normalize_symbol(value)
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 6:
        return normalize_symbol(digits)
    return value


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def _yi_to_value(value: Any) -> float | None:
    number = _to_float(value)
    if number is None:
        return None
    return number * 100_000_000


def _change_pct(close: float | None, pre_close: float | None) -> float | None:
    if close is None or not pre_close:
        return None
    return round((close - pre_close) / pre_close * 100, 4)


def _preview_marker(date: str, side: str, price: float, reason: str) -> dict[str, Any]:
    return {
        "date": date,
        "side": side,
        "price": round(price, 4),
        "reason": reason,
        "text": "买点" if side == "buy" else "卖点",
        "color": "#ef4444" if side == "buy" else "#10b981",
    }


def _now() -> datetime:
    return datetime.now(timezone.utc)
