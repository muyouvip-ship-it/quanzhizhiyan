from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class NewsEyeAnalyzeRequest(BaseModel):
    content: str
    source: str = ""
    published_at: str | None = None
    sentiment: str | None = None
    positive_sectors: list[str] = Field(default_factory=list)
    negative_sectors: list[str] = Field(default_factory=list)
    positive_symbols: list[dict[str, str]] = Field(default_factory=list)
    negative_symbols: list[dict[str, str]] = Field(default_factory=list)
    related_symbols: list[dict[str, str]] = Field(default_factory=list)


class NewsEyeAnalyzeResponse(BaseModel):
    provider: str
    model: str
    summary: str
    sentiment: str
    sentiment_reason: str
    positive_sectors: list[str] = Field(default_factory=list)
    negative_sectors: list[str] = Field(default_factory=list)
    positive_symbols: list[str] = Field(default_factory=list)
    negative_symbols: list[str] = Field(default_factory=list)
    trading_takeaway: str
    generated_at: str
    raw: str | None = None


class NewsEyeHistoryMeta(BaseModel):
    offset: int
    limit: int
    returned: int
    has_more: bool
    earliest_published_at: str | None = None
    latest_published_at: str | None = None
    total_available: int = 0


class NewsEyeListResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    updated_at: str
    source: str = "cache:market_news_items"
    fallback: bool = False
    data_governance: dict[str, Any] = Field(default_factory=dict)
    background: dict[str, Any] = Field(default_factory=dict)
    history: NewsEyeHistoryMeta


class NewsThemeRankingResponse(BaseModel):
    window: str
    items: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: str
    source: str
    message: str
    data_governance: dict[str, Any] = Field(default_factory=dict)


class NewsThemeSnapshotResponse(BaseModel):
    snapshot_date: str
    items: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: str


class NewsThemePerformanceResponse(BaseModel):
    snapshot_date: str
    horizon: str
    items: list[dict[str, Any]] = Field(default_factory=list)
    updated_at: str
