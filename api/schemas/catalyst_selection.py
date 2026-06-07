from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CatalystSelectionThemeMatch(BaseModel):
    theme: str
    score: float
    catalyst: str | None = None
    summary: str | None = None
    source_tier: str | None = None
    evidence_count: int = 0
    relation_score: float = 0.0
    mainline_alignment_score: float = 0.0
    mainline_alignment_reasons: list[str] = Field(default_factory=list)
    event_semantic: dict[str, Any] = Field(default_factory=dict)
    semantic_source: str | None = None


class CatalystSelectionItem(BaseModel):
    rank: int
    symbol: str
    name: str
    industry: str | None = None
    sector: str | None = None
    concepts: list[str] = Field(default_factory=list)
    score: float
    pre_execution_score: float | None = None
    execution_gate_adjustment: dict[str, Any] = Field(default_factory=dict)
    catalyst_score: float
    theme_score: float
    relation_score: float
    market_confirm_score: float
    event_intelligence_score: float = 0.0
    momentum_score: float
    fundamental_score: float
    continuity_score: float
    adaptive_feedback_score: float = 50.0
    risk_penalty: float
    risk_flags: list[str] = Field(default_factory=list)
    reason_parts: list[str] = Field(default_factory=list)
    theme_matches: list[CatalystSelectionThemeMatch] = Field(default_factory=list)
    signal_flags: list[str] = Field(default_factory=list)
    risk_control: dict[str, Any] = Field(default_factory=dict)
    closed_loop_trace: dict[str, Any] = Field(default_factory=dict)
    market_background: str
    market_behavior_labels: dict[str, Any] = Field(default_factory=dict)
    metric_snapshot: dict[str, Any] = Field(default_factory=dict)
    settlement: dict[str, Any] | None = None


class CatalystSelectionRankResponse(BaseModel):
    trade_date: str
    window: str
    updated_at: str
    source: str
    message: str
    items: list[CatalystSelectionItem] = Field(default_factory=list)
    market_background: str
    market_behavior_labels: dict[str, Any] = Field(default_factory=dict)
    data_governance: dict[str, Any] = Field(default_factory=dict)


class CatalystSelectionSettlementItem(BaseModel):
    trade_date: str
    settlement_date: str
    symbol: str
    name: str
    rank: int
    entry_price: float | None = None
    close_price: float | None = None
    next_open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    change_pct: float | None = None
    max_up_pct: float | None = None
    max_down_pct: float | None = None
    hit_score: float | None = None
    outcome: str
    protected: bool
    settlement_notes: list[str] = Field(default_factory=list)


class CatalystSelectionHistoryItem(BaseModel):
    trade_date: str
    item_count: int
    top_symbol: str | None = None
    top_name: str | None = None
    average_change_pct: float | None = None
    hit_rate: float | None = None
    protected_count: int = 0
    data_source: str | None = None
    updated_at: str


class CatalystSelectionHistoryResponse(BaseModel):
    items: list[CatalystSelectionHistoryItem] = Field(default_factory=list)
    updated_at: str


class CatalystSelectionOpportunityEvent(BaseModel):
    event_id: str
    run_id: str
    trade_date: str
    window: str
    symbol: str
    name: str
    rank: int
    score: float
    previous_rank: int | None = None
    previous_score: float | None = None
    rank_delta: int | None = None
    score_delta: float | None = None
    event_level: str
    event_types: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    risk_action: str | None = None
    risk_level: str | None = None
    trace: dict[str, Any] = Field(default_factory=dict)
    created_at: str


class CatalystSelectionOpportunityEventResponse(BaseModel):
    items: list[CatalystSelectionOpportunityEvent] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    updated_at: str


class CatalystMonitorPoolResponse(BaseModel):
    trade_date: str
    window: str
    updated_at: str
    source: str
    suggested_execution_mode: str = "monitor_only"
    monitor_pool: dict[str, Any] = Field(default_factory=dict)
    risk_config: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)


class CatalystClosedLoopAuditItem(BaseModel):
    audit_id: str
    trade_date: str | None = None
    trigger: str
    status: str
    requirement_summary: dict[str, Any] = Field(default_factory=dict)
    requirement_checks: list[dict[str, Any]] = Field(default_factory=list)
    end_to_end_evidence: dict[str, Any] = Field(default_factory=dict)
    requested_window_count: int = 0
    generated_window_count: int = 0
    failed_window_count: int = 0
    total_selected_count: int = 0
    opportunity_event_count: int = 0
    risk_action_counts: dict[str, int] = Field(default_factory=dict)
    risk_level_counts: dict[str, int] = Field(default_factory=dict)
    feedback: dict[str, Any] = Field(default_factory=dict)
    monitor_activation: dict[str, Any] = Field(default_factory=dict)
    llm_ready_window_count: int = 0
    settlement: dict[str, Any] = Field(default_factory=dict)
    generated: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    skip_reason: str | None = None
    created_at: str
    updated_at: str


class CatalystClosedLoopAuditResponse(BaseModel):
    items: list[CatalystClosedLoopAuditItem] = Field(default_factory=list)
    updated_at: str


class CatalystEventRefreshRunItem(BaseModel):
    refresh_key: str
    trigger: str
    user_id: str | None = None
    trade_date: str | None = None
    windows: list[str] = Field(default_factory=list)
    limit: int = 0
    reason: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    status: str
    deduped: bool = False
    generated: list[dict[str, Any]] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    skipped: bool = False
    skip_reason: str | None = None
    audit_id: str | None = None
    duration_ms: int | None = None
    scheduled_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    updated_at: str


class CatalystEventRefreshRunResponse(BaseModel):
    items: list[CatalystEventRefreshRunItem] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    updated_at: str


class CatalystLearningReplayResponse(BaseModel):
    trade_date: str | None = None
    status: str
    source: str
    score_version: str
    feedback_model_version: str
    realtime_feedback_model_version: str
    audit_id: str | None = None
    audit_created_at: str | None = None
    candidate_impact_count: int = 0
    active_impact_count: int = 0
    unique_symbol_count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)
    profile_scope_counts: dict[str, int] = Field(default_factory=dict)
    score_changed_count: int = 0
    rank_changed_count: int = 0
    risk_changed_count: int = 0
    gate_applied_count: int = 0
    action_changed_count: int = 0
    improved_rank_count: int = 0
    reduced_rank_count: int = 0
    average_score_delta: float | None = None
    max_abs_score_delta: float | None = None
    average_rank_delta: float | None = None
    average_max_position_delta_pct: float | None = None
    windows: list[dict[str, Any]] = Field(default_factory=list)
    items: list[dict[str, Any]] = Field(default_factory=list)
    settlement_feedback_replay: dict[str, Any] = Field(default_factory=dict)
    realtime_feedback_replay: dict[str, Any] = Field(default_factory=dict)
    evidence: list[str] = Field(default_factory=list)
    gaps: list[str] = Field(default_factory=list)
    updated_at: str


class CatalystSelectionBackfillRequest(BaseModel):
    trade_date: str
    force: bool = False
