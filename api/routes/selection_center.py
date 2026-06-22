from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.core.strategy_db import get_strategy_db
from api.deps import require_api_user
from api.services import selection_center_service


router = APIRouter(prefix="/v1/selection-center", tags=["Selection Center"])


class SelectionFilterConfig(BaseModel):
    exclude_st: bool = True
    exclude_suspended: bool = True
    trend_up: bool = False
    trend_ma: int = 20
    volume_up: bool = False
    amount_enabled: bool = False
    min_amount: str | float | None = None
    market_cap_enabled: bool = False
    min_market_cap: str | float | None = None
    max_market_cap: str | float | None = None
    event_heat_enabled: bool = False
    min_event_heat: str | float | None = None


class SelectionTaskCreateRequest(BaseModel):
    name: str = Field(default="", max_length=200)
    mode: Literal["strategy", "catalyst", "hybrid"] = "strategy"
    include_boards: list[str] = Field(default_factory=list)
    strategy_id: str | None = None
    strategy_name: str | None = None
    signal_id: str | None = None
    signal_name: str | None = None
    signal_side: str | None = None
    period: str = "日K"
    catalyst_rule: str | None = None
    filter_config: SelectionFilterConfig = Field(default_factory=SelectionFilterConfig)


def _as_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@router.get("/tasks")
def list_selection_center_tasks(
    mode: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=300),
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    items = selection_center_service.list_tasks(strategy_db, current_user.id, mode=mode, limit=limit)
    return {"items": items, "total": len(items)}


@router.post("/tasks")
def create_selection_center_task(
    body: SelectionTaskCreateRequest,
    background_tasks: BackgroundTasks,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    try:
        task = selection_center_service.create_task(strategy_db, current_user.id, body.model_dump())
        background_tasks.add_task(selection_center_service.execute_task, task["id"])
        return task
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/tasks/{task_id}")
def get_selection_center_task(
    task_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    task = selection_center_service.get_task(strategy_db, current_user.id, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="选股任务不存在")
    return task


@router.post("/tasks/{task_id}/rerun")
def rerun_selection_center_task(
    task_id: str,
    background_tasks: BackgroundTasks,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, Any]:
    try:
        task = selection_center_service.rerun_task(strategy_db, current_user.id, task_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc
    if task is None:
        raise HTTPException(status_code=404, detail="选股任务不存在")
    background_tasks.add_task(selection_center_service.execute_task, task["id"])
    return task


@router.delete("/tasks/{task_id}")
def delete_selection_center_task(
    task_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
) -> dict[str, str]:
    deleted = selection_center_service.delete_task(strategy_db, current_user.id, task_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="选股任务不存在")
    return {"message": "Selection task deleted"}
