from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from api.database import get_db
from api.deps import require_api_user
from api.schemas.news_eye import (
    NewsEyeAnalyzeRequest,
    NewsEyeAnalyzeResponse,
    NewsEyeListResponse,
    NewsThemePerformanceResponse,
    NewsThemeRankingResponse,
    NewsThemeSnapshotResponse,
)
from api.services import news_eye_service, news_theme_service

router = APIRouter(prefix="/v1/news-eye", tags=["News Eye"])


@router.get("/items", response_model=NewsEyeListResponse)
def list_news_items(
    limit: int = Query(120, ge=1, le=500),
    offset: int = Query(0, ge=0, le=5000),
    source: str | None = Query(None),
    sentiment: str | None = Query(None),
    symbol: str | None = Query(None),
    sector: str | None = Query(None),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return news_eye_service.list_news_items(
        db,
        limit=limit,
        offset=offset,
        source=source,
        sentiment=sentiment,
        symbol=symbol,
        sector=sector,
    )


@router.get("/themes", response_model=NewsThemeRankingResponse)
def list_news_themes(
    window: str = Query("premarket"),
    limit: int = Query(20, ge=1, le=50),
    include_evidence: bool = Query(True),
    allow_async_llm: bool = Query(True),
    force_sync_llm: bool = Query(False),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    return news_theme_service.list_theme_rankings(
        db,
        window=window,
        limit=limit,
        include_evidence=include_evidence,
        user_id=current_user.id,
        allow_async_llm=allow_async_llm,
        force_sync_llm=force_sync_llm,
    )


@router.get("/theme-snapshots", response_model=NewsThemeSnapshotResponse)
def list_news_theme_snapshots(
    date: str = Query(..., min_length=10, max_length=10),
    window: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return news_theme_service.list_theme_snapshots(
        db,
        snapshot_date=date,
        window=window,
        limit=limit,
    )


@router.get("/theme-performance", response_model=NewsThemePerformanceResponse)
def get_news_theme_performance(
    snapshot_date: str = Query(..., min_length=10, max_length=10),
    horizon: str = Query("3d"),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    del current_user
    return news_theme_service.get_theme_performance(
        db,
        snapshot_date=snapshot_date,
        horizon=horizon,
    )


@router.post("/refresh")
def refresh_news_items(
    limit: int = Query(160, ge=10, le=500),
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    symbols = news_eye_service.load_user_focus_symbols(db, current_user.id)
    return news_eye_service.refresh_news_cache(
        db,
        limit=limit,
        symbols=symbols,
        trigger="manual",
        user_id=current_user.id,
        async_event_driven_selection=True,
    )


@router.post("/analyze", response_model=NewsEyeAnalyzeResponse)
def analyze_news_item(
    payload: NewsEyeAnalyzeRequest,
    db: Session = Depends(get_db),
    current_user=Depends(require_api_user),
) -> dict[str, Any]:
    return news_eye_service.analyze_news_item(
        db,
        user_id=current_user.id,
        payload=payload.model_dump(),
    )
