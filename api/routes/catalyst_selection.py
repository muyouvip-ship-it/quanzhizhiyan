from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from api.database import get_db
from api.deps import require_api_user
from api.schemas.catalyst_selection import (
    CatalystClosedLoopAuditResponse,
    CatalystEventRefreshRunResponse,
    CatalystLearningReplayResponse,
    CatalystMonitorPoolResponse,
    CatalystSelectionBackfillRequest,
    CatalystSelectionHistoryResponse,
    CatalystSelectionOpportunityEventResponse,
    CatalystSelectionRankResponse,
)
from api.services import catalyst_selection_service


router = APIRouter(prefix="/v1/catalyst-selection", tags=["Catalyst Selection"])


@router.get("", response_model=CatalystSelectionRankResponse)
def list_catalyst_selections(
    trade_date: str | None = Query(default=None, min_length=10, max_length=10),
    window: str = Query(default="premarket"),
    limit: int = Query(default=10, ge=1, le=30),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    try:
        return catalyst_selection_service.list_or_generate_selections(
            db,
            trade_date=trade_date,
            window=window,
            limit=limit,
            force=force,
            user_id=current_user.id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"生成催化选股失败：{exc}") from exc


@router.post("/generate", response_model=CatalystSelectionRankResponse)
def generate_catalyst_selections(
    payload: CatalystSelectionBackfillRequest,
    window: str = Query(default="premarket"),
    limit: int = Query(default=10, ge=1, le=30),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    try:
        return catalyst_selection_service.list_or_generate_selections(
            db,
            trade_date=payload.trade_date,
            window=window,
            limit=limit,
            force=True if payload.force is None else payload.force,
            user_id=current_user.id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"生成催化选股失败：{exc}") from exc


@router.get("/history", response_model=CatalystSelectionHistoryResponse)
def list_catalyst_selection_history(
    limit: int = Query(default=30, ge=1, le=120),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return catalyst_selection_service.list_history(db, limit=limit)


@router.get("/events", response_model=CatalystSelectionOpportunityEventResponse, include_in_schema=False)
@router.get("/opportunity-events", response_model=CatalystSelectionOpportunityEventResponse)
def list_catalyst_opportunity_events(
    trade_date: str | None = Query(default=None, min_length=10, max_length=10),
    window: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    event_level: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return catalyst_selection_service.list_opportunity_events(
        db,
        trade_date=trade_date,
        window=window,
        symbol=symbol,
        event_level=event_level,
        limit=limit,
    )


@router.get("/monitor-pool", response_model=CatalystMonitorPoolResponse)
def get_catalyst_monitor_pool(
    trade_date: str | None = Query(default=None, min_length=10, max_length=10),
    window: str = Query(default="24h"),
    limit: int = Query(default=10, ge=1, le=30),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    try:
        return catalyst_selection_service.build_monitor_pool(
            db,
            trade_date=trade_date,
            window=window,
            limit=limit,
            force=force,
            user_id=current_user.id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"生成催化监控池失败：{exc}") from exc


@router.get("/closed-loop/audits", response_model=CatalystClosedLoopAuditResponse, include_in_schema=False)
@router.get("/closed-loop-audits", response_model=CatalystClosedLoopAuditResponse)
def list_catalyst_closed_loop_audits(
    trade_date: str | None = Query(default=None, min_length=10, max_length=10),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return catalyst_selection_service.list_closed_loop_audits(
        db,
        trade_date=trade_date,
        limit=limit,
    )


@router.get("/event-refresh-runs", response_model=CatalystEventRefreshRunResponse)
def list_catalyst_event_refresh_runs(
    limit: int = Query(default=20, ge=1, le=100),
    status: str | None = Query(default=None),
    trigger: str | None = Query(default=None),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    return catalyst_selection_service.list_event_refresh_runs(
        db,
        user_id=current_user.id,
        limit=limit,
        status=status,
        trigger=trigger,
    )


@router.get("/learning-replay", response_model=CatalystLearningReplayResponse)
def get_catalyst_learning_replay(
    trade_date: str | None = Query(default=None, min_length=10, max_length=10),
    limit: int = Query(default=20, ge=1, le=80),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return catalyst_selection_service.get_learning_replay(
        db,
        trade_date=trade_date,
        limit=limit,
    )


@router.post("/settle")
def settle_catalyst_selection(
    payload: CatalystSelectionBackfillRequest,
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    try:
        return catalyst_selection_service.settle_selection(
            db,
            trade_date=payload.trade_date,
            force=payload.force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"结算催化选股失败：{exc}") from exc
