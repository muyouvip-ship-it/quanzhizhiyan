from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class DailyReviewConfigResponse(BaseModel):
    enabled: bool = False
    trigger_time: str = "21:10"
    push_enabled: bool = True
    last_run_date: Optional[str] = None
    last_run_status: Optional[str] = None
    last_error: Optional[str] = None


class DailyReviewConfigUpdateRequest(BaseModel):
    enabled: Optional[bool] = None
    trigger_time: Optional[str] = None
    push_enabled: Optional[bool] = None


class DailyReviewGenerateRequest(BaseModel):
    trade_date: Optional[str] = None
    push_after_generate: Optional[bool] = None


class DailyReviewHistoryItem(BaseModel):
    id: str
    trade_date: str
    status: str
    headline: str = ""
    push_status: Optional[str] = None
    updated_at: Optional[str] = None
    created_at: Optional[str] = None


class DailyReviewHistoryResponse(BaseModel):
    items: list[DailyReviewHistoryItem] = Field(default_factory=list)


class DailyReviewResponse(BaseModel):
    id: str
    user_id: str
    trade_date: str
    status: str
    market_summary: dict[str, Any] = Field(default_factory=dict)
    portfolio_summary: dict[str, Any] = Field(default_factory=dict)
    current_main_themes: list[dict[str, Any]] = Field(default_factory=list)
    current_key_stocks: list[dict[str, Any]] = Field(default_factory=list)
    next_main_themes: list[dict[str, Any]] = Field(default_factory=list)
    next_candidate_stocks: list[dict[str, Any]] = Field(default_factory=list)
    risk_watchpoints: list[dict[str, Any]] = Field(default_factory=list)
    narrative_markdown: Optional[str] = None
    portfolio_technical_diagnostics: list[dict[str, Any]] = Field(default_factory=list)
    raw_result_data: dict[str, Any] = Field(default_factory=dict)
    push_status: Optional[str] = None
    push_error: Optional[str] = None
    last_pushed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
