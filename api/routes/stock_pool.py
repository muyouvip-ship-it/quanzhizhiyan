from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.core.strategy_db import get_strategy_db
from api.database import get_db
from api.deps import require_api_user
from api.services import stock_pool_service


router = APIRouter(prefix="/v1/stock-pool", tags=["Stock Pool"])


class StockPoolGroupCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)


class StockPoolGroupUpdateRequest(BaseModel):
    name: str | None = Field(default=None, max_length=120)
    sort_order: int | None = None


class StockPoolItemCreateRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    name: str | None = Field(default=None, max_length=120)
    source: str = Field(default="manual", max_length=40)


class StrategyPreviewRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    strategy_id: str = Field(..., min_length=1, max_length=80)
    period: str = "daily"
    start_date: str | None = None
    end_date: str | None = None


def _as_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc).strip("'"))
    return HTTPException(status_code=500, detail=str(exc))


@router.get("/groups")
def list_stock_pool_groups(
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    return stock_pool_service.list_groups(db, strategy_db, str(current_user.id))


@router.post("/groups")
def create_stock_pool_group(
    body: StockPoolGroupCreateRequest,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return stock_pool_service.create_group(db, str(current_user.id), body.name)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.patch("/groups/{group_id}")
def update_stock_pool_group(
    group_id: str,
    body: StockPoolGroupUpdateRequest,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return stock_pool_service.update_group(
            db,
            str(current_user.id),
            group_id,
            name=body.name,
            sort_order=body.sort_order,
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.delete("/groups/{group_id}")
def delete_stock_pool_group(
    group_id: str,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    try:
        deleted = stock_pool_service.delete_group(db, str(current_user.id), group_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="分组不存在")
    return {"message": "Stock pool group deleted"}


@router.get("/groups/{group_id}/items")
def list_stock_pool_items(
    group_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=80, ge=1, le=300),
    q: str | None = Query(default=None, max_length=40),
    sector: str | None = Query(default=None, max_length=80),
    sort_by: str | None = Query(default=None, max_length=40),
    sort_direction: str | None = Query(default=None, pattern="^(asc|desc)$"),
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    try:
        return stock_pool_service.list_group_items(
            db,
            str(current_user.id),
            group_id,
            strategy_db=strategy_db,
            page=page,
            page_size=page_size,
            q=q,
            sector=sector,
            sort_by=sort_by,
            sort_direction=sort_direction,
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/groups/{group_id}/items")
def add_stock_pool_item(
    group_id: str,
    body: StockPoolItemCreateRequest,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return stock_pool_service.add_group_item(
            db,
            str(current_user.id),
            group_id,
            body.symbol,
            name=body.name,
            source=body.source,
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.delete("/groups/{group_id}/items/{item_id}")
def delete_stock_pool_item(
    group_id: str,
    item_id: str,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
) -> dict[str, str]:
    deleted = stock_pool_service.delete_group_item(db, str(current_user.id), group_id, item_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="股票不在该分组中")
    return {"message": "Stock pool item deleted"}


@router.post("/from-selection-task/{task_id}")
def copy_selection_task_to_stock_pool(
    task_id: str,
    body: dict[str, Any] | None = Body(default=None),
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    try:
        return stock_pool_service.copy_selection_task_to_group(
            db,
            strategy_db,
            str(current_user.id),
            task_id,
            name=str((body or {}).get("name") or "").strip() or None,
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/strategy-preview")
def preview_stock_pool_strategy(
    body: StrategyPreviewRequest,
    current_user=Depends(require_api_user),
    db: Session = Depends(get_db),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    del current_user
    try:
        return stock_pool_service.preview_strategy_markers(
            db,
            strategy_db=strategy_db,
            symbol=body.symbol,
            strategy_id=body.strategy_id,
            period=body.period,
            start_date=body.start_date,
            end_date=body.end_date,
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc
