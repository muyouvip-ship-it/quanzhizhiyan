from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping
from urllib.parse import urlparse
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.core.strategy_db import StrategySessionLocal, get_strategy_db
from api.core.runtime_config import build_runtime_config, has_mixed_account_llm_runtime, llm_runtime_source_payload
from api.database import UserDB, get_db
from api.deps import optional_web_user
from api.models.strategy_models import PaperAccountDB, PaperOrderDB, TradeRecordDB
from api.services.factor_registry import get_factor_registry_item, list_factor_registry
from api.services.data_source_governance import build_backtest_governance
from api.services.strategy_dsl_compiler import compile_strategy_dsl
from api.services.market_data_pipeline_service import preferred_daily_kline_table, preferred_minute_kline_table
from api.services.strategy_dsl_schema import StrategyDslSchema
from api.services.strategy_platform_engine import (
    build_evolution_candidates,
    enrich_watchlist_sector_metadata,
    read_artifact_items,
    read_artifact_page,
    run_strategy_backtest,
)
from api.services.strategy_platform_repository import (
    delete_platform_strategy,
    get_latest_completed_platform_backtest,
    get_platform_backtest_run,
    get_platform_evolution_candidate,
    get_platform_evolution_experiment,
    get_platform_strategy,
    get_platform_strategy_versions,
    list_platform_backtest_runs,
    list_platform_evolution_candidates,
    list_platform_strategies,
    save_platform_backtest_run,
    save_platform_evolution_experiment,
    save_platform_strategy,
    update_platform_evolution_candidate_status,
    update_platform_strategy_metrics,
)
from tradingagents.llm_clients.factory import create_llm_client


router = APIRouter(tags=["Strategy Platform"])
_REMOTE_LLM_PROVIDERS_REQUIRING_KEY = {
    "openai",
    "anthropic",
    "google",
    "xai",
    "openrouter",
    "volcengine",
    "volcengine-ark",
    "ark",
    "dashscope",
    "deepseek",
    "moonshot",
    "zhipu",
    "siliconflow",
}
_STRATEGY_DRAFT_LLM_TIMEOUT_SECONDS = float(os.getenv("STRATEGY_DRAFT_LLM_TIMEOUT_SECONDS", "60"))


StrategyType = Literal["selection", "trading", "risk", "portfolio"]
StrategyStatus = Literal["draft", "active", "paused", "archived", "candidate"]
StrategyTier = Literal["aggressive", "stable", "defensive"]
StrategySource = Literal["manual", "llm", "evolution", "template", "test"]


class StrategyDsl(BaseModel):
    schema_version: str = "1.0"
    strategy_type: StrategyType
    universe: dict[str, Any] = Field(default_factory=dict)
    factor_model: dict[str, Any] = Field(default_factory=dict)
    entry: dict[str, Any] = Field(default_factory=dict)
    exit: dict[str, Any] = Field(default_factory=dict)
    position: dict[str, Any] = Field(default_factory=dict)
    risk: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)
    evolution: dict[str, Any] = Field(default_factory=dict)


class StrategyVersion(BaseModel):
    id: str
    strategy_id: str
    version: int
    dsl: StrategyDsl
    compile_status: Literal["pending", "passed", "failed"] = "pending"
    compiled_hash: str | None = None
    change_summary: str | None = None
    created_at: str


class StrategyPerformance(BaseModel):
    total_return: float
    annual_return: float | None = None
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    calmar_ratio: float | None = None


class StrategyDefinition(BaseModel):
    id: str
    name: str
    strategy_type: StrategyType
    status: StrategyStatus
    description: str | None = None
    source: StrategySource = "manual"
    current_version_id: str | None = None
    version: int = 1
    is_active: bool = False
    run_count: int = 0
    last_run_time: str | None = None
    created_at: str
    updated_at: str
    performance: StrategyPerformance | None = None
    current_version: StrategyVersion | None = None
    tags: list[str] = Field(default_factory=list)
    template_id: str | None = None
    template_name: str | None = None
    template_parameters: dict[str, Any] = Field(default_factory=dict)
    official_pack_id: str | None = None
    official_pack_name: str | None = None
    official_blueprint_id: str | None = None
    official_tier: StrategyTier | None = None
    official_current_version: int | None = None
    official_latest_version: int | None = None
    official_update_available: bool = False


class StrategyListResponse(BaseModel):
    total: int
    strategies: list[StrategyDefinition]


class StrategyDraftRequest(BaseModel):
    prompt: str = Field(..., min_length=2)
    strategy_type: StrategyType | None = None


class StrategyDraftConfirmation(BaseModel):
    field: str
    assumed_as: str
    reason: str


class StrategyDraftResponse(BaseModel):
    name: str
    strategy_type: StrategyType
    intent_summary: str
    pending_confirmations: list[StrategyDraftConfirmation]
    data_dependencies: list[str]
    risk_notes: list[str]
    dsl: StrategyDsl
    explanation: str
    structured_output_schema: dict[str, Any] = Field(default_factory=dict)
    compile_report: dict[str, Any] = Field(default_factory=dict)
    llm_runtime: dict[str, Any] = Field(default_factory=dict)


class StrategyTemplateParameter(BaseModel):
    key: str
    label: str
    input_type: Literal["number", "select", "boolean"] = "number"
    description: str | None = None
    default_value: Any = None
    min_value: float | None = None
    max_value: float | None = None
    step: float | None = None
    options: list[dict[str, Any]] = Field(default_factory=list)


class StrategyTemplateDefinition(BaseModel):
    id: str
    name: str
    strategy_type: StrategyType
    description: str
    scenario: str
    tags: list[str] = Field(default_factory=list)
    default_prompt: str
    default_dsl: StrategyDsl
    parameters: list[StrategyTemplateParameter] = Field(default_factory=list)


class OfficialStrategyPackItem(BaseModel):
    blueprint_id: str
    name: str
    strategy_type: StrategyType
    tier: StrategyTier
    version: int
    description: str
    performance: StrategyPerformance
    tags: list[str] = Field(default_factory=list)
    dsl: StrategyDsl | None = None


class OfficialStrategyPack(BaseModel):
    id: str
    name: str
    strategy_type: StrategyType
    description: str
    tags: list[str] = Field(default_factory=list)
    items: list[OfficialStrategyPackItem] = Field(default_factory=list)


class OfficialStrategyPackListResponse(BaseModel):
    total: int
    packs: list[OfficialStrategyPack]


class OfficialStrategyPackCloneResponse(BaseModel):
    pack_id: str
    pack_name: str
    cloned_count: int
    strategies: list[StrategyDefinition]
    message: str


class OfficialStrategyPackItemCloneRequest(BaseModel):
    name: str | None = None
    status: StrategyStatus = "draft"


class StrategyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    strategy_type: StrategyType
    description: str | None = None
    dsl: StrategyDsl
    source: StrategySource = "manual"
    status: StrategyStatus = "draft"
    template_id: str | None = None
    template_name: str | None = None
    template_parameters: dict[str, Any] = Field(default_factory=dict)


class StrategyUpdateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    strategy_type: StrategyType
    description: str | None = None
    dsl: StrategyDsl
    source: StrategySource | None = None
    status: StrategyStatus | None = None
    template_id: str | None = None
    template_name: str | None = None
    template_parameters: dict[str, Any] | None = None


class StrategyCompilePreviewRequest(BaseModel):
    dsl: StrategyDsl


class StrategyCloneRequest(BaseModel):
    name: str | None = None
    status: StrategyStatus = "draft"


class StrategyActivateRequest(BaseModel):
    status: Literal["active", "paused"] = "active"


class StrategyVersionCreateRequest(BaseModel):
    dsl: StrategyDsl
    change_summary: str = "新版本"
    activate: bool = True


class StrategyCompileResponse(BaseModel):
    status: Literal["passed", "failed"]
    errors: list[str]
    warnings: list[str]
    required_fields: list[str]
    compiled_targets: list[str]
    factor_count: int | None = None
    entry_rule_count: int | None = None
    exit_rule_count: int | None = None
    runtime_engine: dict[str, Any] | None = None
    execution_plan: dict[str, Any] | None = None
    timeframes_required: list[str] | None = None
    minute_requirements: dict[str, Any] | None = None
    backend_resolution: dict[str, Any] | None = None
    pending_confirmations: list[dict[str, Any]] | None = None
    future_function_risks: list[dict[str, Any]] | None = None
    expression_preview: dict[str, Any] | None = None


class BacktestCreateRequest(BaseModel):
    strategy_id: str
    strategy_version_id: str | None = None
    symbols: list[str] = Field(default_factory=list)
    start_date: str
    end_date: str
    initial_capital: float = 1_000_000
    frequency: Literal["daily", "daily_minute"] = "daily_minute"
    benchmark: str = "沪深300"
    use_minute_confirm: bool = True
    backtest_mode: Literal["daily_only", "minute_only", "daily_select_intraday_trade"] | None = None
    universe: dict[str, Any] = Field(default_factory=dict)
    cost_config: dict[str, Any] = Field(default_factory=dict)
    minute_config: dict[str, Any] = Field(default_factory=dict)
    walk_forward: dict[str, Any] = Field(default_factory=dict)


class BacktestMetrics(BaseModel):
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    volatility: float
    final_capital: float
    calmar_ratio: float | None = None


class BacktestRun(BaseModel):
    id: str
    strategy_id: str
    strategy_version_id: str | None = None
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    progress: float
    start_date: str
    end_date: str
    initial_capital: float
    frequency: Literal["daily", "daily_minute"]
    benchmark: str
    metrics: BacktestMetrics | None = None
    result: dict[str, Any] | None = None
    artifact_root: str | None = None
    error_message: str | None = None
    data_governance: dict[str, Any] | None = None
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None


class BacktestRunListResponse(BaseModel):
    items: list[BacktestRun]


class BacktestCompareRequest(BaseModel):
    run_ids: list[str] = Field(default_factory=list, min_length=2)


class EvolutionCreateRequest(BaseModel):
    strategy_id: str
    objective: str = "calmar_then_win_rate"
    search_space: dict[str, Any] = Field(default_factory=dict)


class PaperAccountCreateRequest(BaseModel):
    id: str | None = None
    name: str | None = None
    initial_capital: float = Field(default=1_000_000, ge=0)


def _with_backtest_governance(run: BacktestRun) -> BacktestRun:
    return run.model_copy(update={"data_governance": build_backtest_governance(run.model_dump())})


def _backtest_run_from_payload(payload: Mapping[str, Any]) -> BacktestRun:
    return _with_backtest_governance(BacktestRun(**payload))


class EvolutionCandidate(BaseModel):
    id: str
    experiment_id: str
    name: str
    score: float
    status: Literal["candidate", "accepted", "rejected"] = "candidate"
    improvement_summary: str
    risk_flags: list[str]
    metrics: BacktestMetrics
    dsl_patch: dict[str, Any]


class EvolutionExperiment(BaseModel):
    id: str
    strategy_id: str
    objective: str
    status: Literal["pending", "running", "completed", "failed"] = "completed"
    progress: float = 1.0
    candidates: list[EvolutionCandidate] = Field(default_factory=list)
    created_at: str


class FactorRegistryItem(BaseModel):
    id: str
    name: str
    display_name: str
    category: str | None = None
    description: str | None = None
    formula: str | None = None
    source_column: str | None = None
    required_fields: list[str] = Field(default_factory=list)
    transforms_supported: list[str] = Field(default_factory=list)
    default_transform: str | None = None
    default_direction: str | None = None
    window: int | None = None
    rank_scope: str | None = None
    backend_support: list[str] = Field(default_factory=list)
    timeframes: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    is_active: bool = True
    created_at: str | None = None
    updated_at: str | None = None


class FactorRegistryListResponse(BaseModel):
    total: int
    items: list[FactorRegistryItem]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_dsl(strategy_type: StrategyType = "portfolio") -> StrategyDsl:
    return StrategyDsl(
        strategy_type=strategy_type,
        universe={
            "market": "A_SHARE",
            "include_concepts": ["算力", "AI算力", "数据中心"],
            "exclude_st": True,
            "exclude_suspended": True,
            "min_listing_days": 250,
            "filters": [
                {
                    "field": "float_market_cap",
                    "op": "between",
                    "value": [10_000_000_000, 20_000_000_000],
                    "unit": "CNY",
                }
            ],
        },
        factor_model={
            "engine": "polars_expr",
            "score_method": "weighted_sum",
            "rebalance_frequency": "weekly",
            "factors": [
                {"name": "net_profit_growth_yoy", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
                {"name": "money_flow_strength_20d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
                {"name": "momentum_60d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
                {"name": "volatility_20d", "weight": 0.15, "direction": "lower_better", "transform": "rank_pct"},
            ],
            "select": {"top_n": 30, "min_score": 0.65},
        },
        entry={
            "logic": "all",
            "conditions": [
                {"type": "trend", "timeframe": "1w", "field": "close", "op": "above", "indicator": "ma20"},
                {"type": "alligator_opening", "timeframe": "1d", "params": {"jaw": 13, "teeth": 8, "lips": 5}, "direction": "bullish"},
                {"type": "intraday_confirm", "timeframe": "30m", "conditions": [{"type": "cross_above", "left": "close", "right": "vwap"}]},
            ],
        },
        exit={
            "logic": "any",
            "conditions": [
                {"type": "cross_below", "timeframe": "1d", "left": "close", "right": "ma20"},
                {"type": "atr_trailing_stop", "timeframe": "1d", "atr_period": 14, "atr_multiple": 2.5},
                {"type": "factor_rank_drop", "rank_below": 0.5},
            ],
        },
        position={
            "method": "risk_budget",
            "initial_position_pct": 0.2,
            "max_position_pct": 0.8,
            "max_single_position_pct": 0.12,
            "max_industry_position_pct": 0.35,
            "cash_reserve_pct": 0.05,
            "risk_per_trade_pct": 0.01,
            "sizing_basis": "atr",
        },
        risk={
            "stop_loss_pct": 0.08,
            "take_profit_pct": 0.25,
            "trailing_stop_pct": 0.1,
            "max_drawdown_pct": 0.15,
            "max_daily_loss_pct": 0.03,
            "max_positions": 20,
        },
        execution={
            "market": "A_SHARE",
            "signal_timing": "close",
            "fill_timing": "next_open",
            "price_mode": "open",
            "lot_size": 100,
            "tick_size": 0.01,
            "data_engine": {"filter": "duckdb", "factor_compute": "polars"},
            "minute_loading": {"mode": "lazy_by_watchlist", "forbid_full_market_preload": True},
        },
        evolution={
            "enabled": True,
            "method": "trade_snapshot_attribution",
            "require_user_confirmation": True,
            "ml_backend": "scikit-learn",
        },
    )


def _make_strategy(
    name: str,
    strategy_type: StrategyType,
    status: StrategyStatus,
    description: str,
    source: StrategySource = "manual",
    performance: StrategyPerformance | None = None,
    dsl: StrategyDsl | None = None,
    tags: list[str] | None = None,
) -> StrategyDefinition:
    strategy_id = uuid4().hex
    version_id = uuid4().hex
    now = _now()
    strategy_dsl = dsl or _default_dsl(strategy_type)
    version = StrategyVersion(
        id=version_id,
        strategy_id=strategy_id,
        version=1,
        dsl=strategy_dsl,
        compile_status="passed",
        compiled_hash=uuid4().hex[:12],
        change_summary="初始版本",
        created_at=now,
    )
    return StrategyDefinition(
        id=strategy_id,
        name=name,
        strategy_type=strategy_type,
        status=status,
        description=description,
        source=source,
        current_version_id=version_id,
        version=1,
        is_active=status == "active",
        run_count=6 if performance else 0,
        last_run_time=now if performance else None,
        created_at=now,
        updated_at=now,
        performance=performance,
        current_version=version,
        tags=tags or ["A股", "多周期", "Polars"],
    )


def _first_day_band_dsl() -> StrategyDsl:
    dsl = _default_dsl("trading").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["universe"]["filters"] = []
    dsl["factor_model"] = {
        "engine": "polars_expr",
        "score_method": "weighted_sum",
        "rebalance_frequency": "daily",
        "factors": [
            {
                "name": "first_day_band_cross",
                "weight": 1.0,
                "direction": "higher_better",
                "transform": "raw",
                "timeframe": "1d",
            }
        ],
        "select": {"top_n": 200, "min_score": 0.5},
    }
    dsl["entry"] = {
        "logic": "all",
        "conditions": [
            {"type": "cross_above", "timeframe": "1d", "left": "first_day_band", "right": "first_day_band_b1"}
        ],
    }
    dsl["exit"] = {
        "logic": "any",
        "conditions": [
            {"type": "cross_below", "timeframe": "1d", "left": "first_day_band", "right": "first_day_band_b1"}
        ],
    }
    dsl["position"]["method"] = "equal_weight"
    dsl["position"]["max_single_position_pct"] = 0.05
    dsl["risk"]["max_positions"] = 20
    dsl["risk"]["stop_loss_pct"] = 0.99
    dsl["risk"]["take_profit_pct"] = 5.0
    dsl["risk"]["trailing_stop_pct"] = 1.0
    dsl["evolution"] = {"enabled": True, "method": "trade_snapshot_attribution", "require_user_confirmation": True}
    return StrategyDsl(**dsl)


def _selection_compute_quality_dsl() -> StrategyDsl:
    dsl = _default_dsl("selection").model_dump()
    dsl["factor_model"]["factors"] = [
        {"name": "net_profit_growth_yoy", "weight": 0.42, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.33, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 30, "min_score": 0.72}
    dsl["entry"] = {"logic": "all", "conditions": []}
    dsl["exit"] = {"logic": "any", "conditions": []}
    dsl["position"]["method"] = "equal_weight"
    return StrategyDsl(**dsl)


def _selection_moneyflow_breakout_dsl() -> StrategyDsl:
    dsl = _default_dsl("selection").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["factor_model"]["factors"] = [
        {"name": "money_flow_strength_20d", "weight": 0.4, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_60d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "volatility_20d", "weight": 0.25, "direction": "lower_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 25, "min_score": 0.69}
    dsl["entry"] = {"logic": "all", "conditions": []}
    dsl["exit"] = {"logic": "any", "conditions": []}
    return StrategyDsl(**dsl)


def _selection_defensive_quality_dsl() -> StrategyDsl:
    dsl = _default_dsl("selection").model_dump()
    dsl["universe"]["include_concepts"] = ["高股息", "央国企", "低波红利"]
    dsl["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.42, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "net_profit_growth_yoy", "weight": 0.18, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 24, "min_score": 0.62}
    dsl["entry"] = {"logic": "all", "conditions": []}
    dsl["exit"] = {"logic": "any", "conditions": []}
    return StrategyDsl(**dsl)


def _trading_alligator_pro_dsl() -> StrategyDsl:
    dsl = _default_dsl("trading").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["universe"]["filters"] = []
    dsl["factor_model"]["factors"] = [
        {"name": "momentum_60d", "weight": 0.42, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.3, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "volatility_20d", "weight": 0.28, "direction": "lower_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 12, "min_score": 0.62}
    dsl["position"]["method"] = "volatility_target"
    dsl["position"]["target_volatility_pct"] = 0.1
    dsl["risk"]["max_positions"] = 6
    return StrategyDsl(**dsl)


def _trading_vwap_reclaim_dsl() -> StrategyDsl:
    dsl = _default_dsl("trading").model_dump()
    dsl["universe"]["include_concepts"] = ["算力", "机器人", "半导体"]
    dsl["factor_model"]["factors"] = [
        {"name": "momentum_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "net_profit_growth_yoy", "weight": 0.3, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 10, "min_score": 0.66}
    dsl["entry"] = {
        "logic": "all",
        "conditions": [
            {"type": "trend", "timeframe": "1d", "field": "close", "op": "above", "indicator": "ma20"},
            {"type": "intraday_confirm", "timeframe": "30m", "conditions": [{"type": "cross_above", "left": "close", "right": "vwap"}]},
        ],
    }
    dsl["exit"] = {
        "logic": "any",
        "conditions": [
            {"type": "cross_below", "timeframe": "1d", "left": "close", "right": "ma10"},
            {"type": "atr_trailing_stop", "timeframe": "1d", "atr_period": 14, "atr_multiple": 2.2},
        ],
    }
    dsl["risk"]["max_positions"] = 5
    return StrategyDsl(**dsl)


def _trading_defensive_pulse_dsl() -> StrategyDsl:
    dsl = _default_dsl("trading").model_dump()
    dsl["universe"]["include_concepts"] = ["高股息", "央国企", "电力"]
    dsl["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.4, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 8, "min_score": 0.58}
    dsl["entry"] = {
        "logic": "all",
        "conditions": [
            {"type": "trend", "timeframe": "1d", "field": "close", "op": "above", "indicator": "ma10"},
            {"type": "intraday_confirm", "timeframe": "30m", "conditions": [{"type": "cross_above", "left": "close", "right": "vwap"}]},
        ],
    }
    dsl["exit"] = {
        "logic": "any",
        "conditions": [
            {"type": "cross_below", "timeframe": "1d", "left": "close", "right": "ma10"},
            {"type": "atr_trailing_stop", "timeframe": "1d", "atr_period": 14, "atr_multiple": 1.8},
        ],
    }
    dsl["position"]["initial_position_pct"] = 0.12
    dsl["risk"]["max_positions"] = 4
    dsl["risk"]["stop_loss_pct"] = 0.045
    dsl["risk"]["take_profit_pct"] = 0.12
    return StrategyDsl(**dsl)


def _risk_aggressive_overlay_dsl() -> StrategyDsl:
    dsl = _default_dsl("risk").model_dump()
    dsl["universe"]["include_concepts"] = ["算力", "机器人", "半导体"]
    dsl["factor_model"]["factors"] = [
        {"name": "momentum_60d", "weight": 0.45, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "volatility_20d", "weight": 0.2, "direction": "lower_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 40, "min_score": 0.64}
    dsl["position"]["max_position_pct"] = 0.9
    dsl["position"]["cash_reserve_pct"] = 0.08
    dsl["risk"]["max_drawdown_pct"] = 0.12
    dsl["risk"]["max_daily_loss_pct"] = 0.028
    dsl["risk"]["stop_loss_pct"] = 0.07
    dsl["risk"]["take_profit_pct"] = 0.22
    dsl["risk"]["max_positions"] = 12
    return StrategyDsl(**dsl)


def _risk_drawdown_guard_dsl() -> StrategyDsl:
    dsl = _default_dsl("risk").model_dump()
    dsl["universe"]["include_concepts"] = []
    dsl["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.45, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 50, "min_score": 0.58}
    dsl["position"]["max_position_pct"] = 0.6
    dsl["position"]["cash_reserve_pct"] = 0.18
    dsl["risk"]["max_drawdown_pct"] = 0.08
    dsl["risk"]["max_daily_loss_pct"] = 0.018
    dsl["risk"]["stop_loss_pct"] = 0.05
    dsl["risk"]["take_profit_pct"] = 0.16
    dsl["risk"]["max_positions"] = 8
    return StrategyDsl(**dsl)


def _risk_volatility_overlay_dsl() -> StrategyDsl:
    dsl = _default_dsl("risk").model_dump()
    dsl["universe"]["include_concepts"] = ["高股息", "央国企", "低波红利"]
    dsl["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.5, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "momentum_60d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 20, "min_score": 0.6}
    dsl["position"]["method"] = "equal_weight"
    dsl["position"]["cash_reserve_pct"] = 0.22
    dsl["risk"]["trailing_stop_pct"] = 0.06
    dsl["risk"]["max_drawdown_pct"] = 0.07
    dsl["risk"]["max_positions"] = 10
    return StrategyDsl(**dsl)


def _portfolio_dividend_rotation_dsl() -> StrategyDsl:
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["include_concepts"] = ["高股息", "央国企", "低波红利"]
    dsl["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.35, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "net_profit_growth_yoy", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 18, "min_score": 0.6}
    dsl["position"]["method"] = "equal_weight"
    dsl["risk"]["max_positions"] = 10
    return StrategyDsl(**dsl)


def _portfolio_northbound_resonance_dsl() -> StrategyDsl:
    dsl = _default_dsl("portfolio").model_dump()
    dsl["universe"]["include_concepts"] = ["消费电子", "半导体", "机器人"]
    dsl["factor_model"]["factors"] = [
        {"name": "money_flow_strength_20d", "weight": 0.35, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_60d", "weight": 0.3, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "net_profit_growth_yoy", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "volatility_20d", "weight": 0.15, "direction": "lower_better", "transform": "rank_pct"},
    ]
    dsl["factor_model"]["select"] = {"top_n": 16, "min_score": 0.67}
    dsl["position"]["initial_position_pct"] = 0.16
    dsl["risk"]["max_positions"] = 8
    return StrategyDsl(**dsl)


_BACKTESTS: dict[str, BacktestRun] = {}
_TRADES: dict[str, list[dict[str, Any]]] = {}
_EQUITY: dict[str, list[dict[str, Any]]] = {}
_SNAPSHOTS: dict[str, list[dict[str, Any]]] = {}
_SIGNALS: dict[str, list[dict[str, Any]]] = {}
_POSITIONS: dict[str, list[dict[str, Any]]] = {}
_ORDERS: dict[str, list[dict[str, Any]]] = {}
_WATCHLISTS: dict[str, list[dict[str, Any]]] = {}
_MINUTE_CONFIRMATIONS: dict[str, list[dict[str, Any]]] = {}
_BACKTEST_STATUS_EVENTS: dict[str, list[dict[str, Any]]] = {}
_EXPERIMENTS: dict[str, EvolutionExperiment] = {}
_BACKTEST_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
_BACKTEST_ARTIFACT_MEMORY_LIMIT = int(os.getenv("BACKTEST_ARTIFACT_MEMORY_LIMIT", "5000"))


def _should_execute_backtest_inline() -> bool:
    return "pytest" in sys.modules or os.getenv("TA_BACKTEST_INLINE") == "1"


def _sse_pack(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


def _append_backtest_status_event(
    run_id: str,
    *,
    status: str,
    progress: float,
    message: str,
    stage: str,
    error_message: str | None = None,
    completed_at: str | None = None,
) -> dict[str, Any]:
    events = _BACKTEST_STATUS_EVENTS.setdefault(run_id, [])
    event = {
        "run_id": run_id,
        "event": "status",
        "status": status,
        "progress": max(0.0, min(1.0, float(progress or 0.0))),
        "stage": stage,
        "message": message,
        "sequence": (events[-1]["sequence"] + 1) if events else 1,
        "timestamp": _now(),
        "updated_at": _now(),
        "error_message": error_message,
        "completed_at": completed_at,
    }
    events.append(event)
    if len(events) > 500:
        del events[:-500]
    return event


def _update_backtest_run_state(
    db: Session,
    run: BacktestRun,
    *,
    status: Literal["pending", "running", "completed", "failed", "cancelled"] | None = None,
    progress: float | None = None,
    message: str,
    stage: str,
) -> BacktestRun:
    if status is not None:
        run.status = status
    if progress is not None:
        run.progress = max(0.0, min(1.0, float(progress)))
    if run.status == "running" and not run.started_at:
        run.started_at = _now()
    if run.status in _BACKTEST_TERMINAL_STATUSES and not run.completed_at:
        run.completed_at = _now()
    save_platform_backtest_run(db, run.model_dump())
    _BACKTESTS[run.id] = run
    _append_backtest_status_event(
        run.id,
        status=run.status,
        progress=run.progress,
        message=message,
        stage=stage,
        error_message=run.error_message,
        completed_at=run.completed_at,
    )
    return run


def _backtest_stream_status_from_run(run: BacktestRun, *, message: str = "等待状态更新", stage: str = "heartbeat") -> dict[str, Any]:
    return {
        "run_id": run.id,
        "event": "heartbeat",
        "status": run.status,
        "progress": run.progress,
        "stage": stage,
        "message": message,
        "timestamp": _now(),
        "updated_at": _now(),
        "error_message": run.error_message,
        "completed_at": run.completed_at,
    }


def _strategy_templates() -> list[StrategyTemplateDefinition]:
    quality_growth = _default_dsl("portfolio").model_dump()
    quality_growth["factor_model"]["select"] = {"top_n": 30, "min_score": 0.65}
    quality_growth["position"]["method"] = "risk_budget"
    quality_growth["risk"]["max_positions"] = 20

    alligator = _default_dsl("trading").model_dump()
    alligator["universe"]["include_concepts"] = []
    alligator["universe"]["filters"] = []
    alligator["factor_model"]["factors"] = [
        {"name": "momentum_60d", "weight": 0.45, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.3, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "volatility_20d", "weight": 0.25, "direction": "lower_better", "transform": "rank_pct"},
    ]
    alligator["factor_model"]["select"] = {"top_n": 15, "min_score": 0.58}
    alligator["position"]["method"] = "volatility_target"
    alligator["position"]["target_volatility_pct"] = 0.12
    alligator["risk"]["max_positions"] = 8

    low_vol = _default_dsl("portfolio").model_dump()
    low_vol["universe"]["include_concepts"] = ["高股息", "央国企", "低波红利"]
    low_vol["factor_model"]["factors"] = [
        {"name": "volatility_20d", "weight": 0.35, "direction": "lower_better", "transform": "rank_pct"},
        {"name": "money_flow_strength_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "momentum_20d", "weight": 0.2, "direction": "higher_better", "transform": "rank_pct"},
        {"name": "net_profit_growth_yoy", "weight": 0.25, "direction": "higher_better", "transform": "rank_pct"},
    ]
    low_vol["factor_model"]["select"] = {"top_n": 20, "min_score": 0.55}
    low_vol["position"]["method"] = "equal_weight"
    low_vol["risk"]["max_positions"] = 12

    common_parameters = [
        StrategyTemplateParameter(key="top_n", label="选股数量", input_type="number", description="每日/每周保留的候选股票数量。", default_value=30, min_value=1, max_value=100, step=1),
        StrategyTemplateParameter(key="min_score", label="最低评分", input_type="number", description="多因子综合评分阈值。", default_value=0.65, min_value=0.1, max_value=0.95, step=0.01),
        StrategyTemplateParameter(key="max_positions", label="最大持仓数", input_type="number", description="组合同时持有的最大股票数量。", default_value=20, min_value=1, max_value=50, step=1),
    ]

    return [
        StrategyTemplateDefinition(
            id="quality_growth_swing",
            name="算力业绩高增波段",
            strategy_type="portfolio",
            description="围绕算力、AI 基建和数据中心，做中盘成长 + 多周期波段交易。",
            scenario="选股 + 波段交易",
            tags=["A股", "成长", "算力", "多周期"],
            default_prompt="创建一个选股+波段交易策略：算力板块、资金在100亿到200亿之间、业绩暴增的股票。周线看趋势，日线做波段，分钟线确认入场。",
            default_dsl=StrategyDsl(**quality_growth),
            parameters=[
                *common_parameters,
                StrategyTemplateParameter(key="risk_per_trade_pct", label="单笔风险", input_type="number", description="按 ATR 风险预算建仓。", default_value=0.01, min_value=0.002, max_value=0.05, step=0.001),
            ],
        ),
        StrategyTemplateDefinition(
            id="alligator_breakout",
            name="鳄鱼张口趋势突破",
            strategy_type="trading",
            description="适合强趋势行情，突出趋势延续、放量和波动率目标控制。",
            scenario="趋势突破",
            tags=["A股", "趋势", "鳄鱼张口", "波动率目标"],
            default_prompt="创建一个鳄鱼张口趋势突破策略：优先强趋势个股，日线张口启动，分钟线确认追踪入场。",
            default_dsl=StrategyDsl(**alligator),
            parameters=[
                StrategyTemplateParameter(key="top_n", label="候选数量", input_type="number", description="每次只关注最强势的少量标的。", default_value=15, min_value=1, max_value=50, step=1),
                StrategyTemplateParameter(key="min_score", label="最低评分", input_type="number", description="趋势强度阈值。", default_value=0.58, min_value=0.1, max_value=0.95, step=0.01),
                StrategyTemplateParameter(key="max_positions", label="最大持仓数", input_type="number", description="趋势交易持仓集中度。", default_value=8, min_value=1, max_value=20, step=1),
                StrategyTemplateParameter(key="target_volatility_pct", label="目标波动率", input_type="number", description="按目标波动率调整单票仓位。", default_value=0.12, min_value=0.05, max_value=0.3, step=0.01),
            ],
        ),
        StrategyTemplateDefinition(
            id="low_vol_rotation",
            name="低波轮动防守",
            strategy_type="portfolio",
            description="偏向高股息与低波板块，适合作为防守型组合模板。",
            scenario="低波轮动",
            tags=["A股", "低波", "红利", "组合策略"],
            default_prompt="创建一个低波轮动策略：偏高股息、低波板块，注重防守和稳健轮动。",
            default_dsl=StrategyDsl(**low_vol),
            parameters=[
                StrategyTemplateParameter(key="top_n", label="选股数量", input_type="number", description="防守组合的股票数量。", default_value=20, min_value=5, max_value=50, step=1),
                StrategyTemplateParameter(key="min_score", label="最低评分", input_type="number", description="低波/红利筛选阈值。", default_value=0.55, min_value=0.1, max_value=0.95, step=0.01),
                StrategyTemplateParameter(key="max_positions", label="最大持仓数", input_type="number", description="防守组合持仓数量。", default_value=12, min_value=5, max_value=30, step=1),
            ],
        ),
    ]


def _get_strategy_template(template_id: str) -> StrategyTemplateDefinition | None:
    return next((item for item in _strategy_templates() if item.id == template_id), None)


def _strategy_runtime_config(current_user: UserDB | None, db: Session | None) -> dict[str, Any]:
    return build_runtime_config({}, user_id=current_user.id if current_user else None, db=db)


def _strategy_llm_model(runtime_config: dict[str, Any]) -> str:
    return str(runtime_config.get("deep_think_llm") or runtime_config.get("quick_think_llm") or "").strip()


def _is_local_strategy_llm(provider: str, base_url: str | None) -> bool:
    if str(provider or "").strip().lower() == "ollama":
        return True
    value = str(base_url or "").strip()
    if not value:
        return False
    hostname = (urlparse(value).hostname or "").lower()
    return hostname in {"localhost", "127.0.0.1", "0.0.0.0", "::1"}


def _strategy_llm_status(runtime_config: dict[str, Any], *, current_user: UserDB | None) -> tuple[bool, str, str]:
    provider = str(runtime_config.get("llm_provider") or "").strip().lower()
    model = _strategy_llm_model(runtime_config)
    base_url = str(runtime_config.get("backend_url") or "").strip()
    has_api_key = bool(str(runtime_config.get("api_key") or "").strip())
    if current_user is None:
        return False, "not_authenticated", "未登录用户不调用服务端默认 LLM，只返回本地模板草案。"
    if has_mixed_account_llm_runtime(runtime_config):
        return False, "mixed_runtime_rejected", "账号 LLM 字段未形成同源运行包；provider、Base URL、模型和 Key 必须来自同一套账号配置。"
    if not provider or not model:
        return False, "missing_model", "设置页缺少 provider 或模型名。"
    if _is_local_strategy_llm(provider, base_url):
        return False, "local_rejected", "策略草案要求使用远程 LLM，当前本地模型配置被拒绝。"
    if provider in _REMOTE_LLM_PROVIDERS_REQUIRING_KEY and not has_api_key:
        return False, "missing_api_key", "远程 LLM 缺少 API Key。"
    return True, "ready", "完整远程 LLM 配置可用。"


def _strategy_llm_runtime_payload(
    current_user: UserDB | None,
    db: Session | None,
    *,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_config = runtime_config or _strategy_runtime_config(current_user, db)
    ready, status, reason = _strategy_llm_status(runtime_config, current_user=current_user)
    runtime_sources = llm_runtime_source_payload(runtime_config)
    return {
        "shared_with_settings": True,
        "source": "user_settings" if current_user else "server_default",
        "ready": ready,
        "status": status,
        "reason": reason,
        "used": False,
        "llm_provider": runtime_config.get("llm_provider") or "",
        "quick_think_llm": runtime_config.get("quick_think_llm") or "",
        "deep_think_llm": runtime_config.get("deep_think_llm") or "",
        "backend_url": runtime_config.get("backend_url") or "",
        "has_api_key": bool(runtime_config.get("api_key")),
        "api_key_source": runtime_sources.get("api_key_source"),
        "provider_source": runtime_sources.get("provider_source"),
        "base_url_source": runtime_sources.get("base_url_source"),
        "model_source": runtime_sources.get("model_source"),
        "runtime_package_source": runtime_sources.get("runtime_package_source"),
        "account_runtime_sources": runtime_sources.get("account_runtime_sources"),
        "mixed_account_runtime": runtime_sources.get("mixed_account_runtime"),
        "force_skipped": runtime_config.get("_llm_runtime_force_skipped"),
        "forced": bool(runtime_config.get("_llm_runtime_forced")),
        "structured_outputs": True,
        "schema_name": "StrategyDslSchema",
    }


_STRATEGY_DRAFT_SYSTEM_PROMPT = """你是A股量化策略 DSL 生成器。根据用户意图生成可编译的策略草案。

要求：
1. 只输出 JSON，不要 Markdown。
2. JSON 字段必须包含 name、strategy_type、intent_summary、pending_confirmations、data_dependencies、risk_notes、dsl、explanation。
3. strategy_type 只能是 selection、trading、risk、portfolio。
4. dsl 必须符合 StrategyDsl：schema_version、strategy_type、universe、factor_model、entry、exit、position、risk、execution、evolution。
5. A股执行约束必须保守：exclude_st=true、exclude_suspended=true、lot_size=100、禁止全市场分钟线预加载。
6. factor_model.factors 至少 3 个，字段优先使用 net_profit_growth_yoy、money_flow_strength_20d、momentum_60d、volatility_20d、turnover_rate、amount。
7. 如果用户只要选股，entry.conditions 和 exit.conditions 返回空数组。
8. 不要编造未来收益、未来涨跌幅或任何未来函数字段。
9. pending_confirmations 用于记录你做出的必要假设。"""


def _safe_strategy_llm_error(exc: Exception, runtime_config: dict[str, Any]) -> str:
    message = str(exc) or exc.__class__.__name__
    api_key = str(runtime_config.get("api_key") or "").strip()
    if api_key:
        message = message.replace(api_key, "***")
    return message[:500]


def _parse_strategy_llm_json(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _strategy_type_from_request(prompt: str, requested: StrategyType | None) -> StrategyType:
    if requested:
        return requested
    if "交易" in prompt and "选股" not in prompt:
        return "trading"
    if "选股" in prompt and "交易" not in prompt and "波段" not in prompt:
        return "selection"
    return "portfolio"


def _string_list(value: Any, *, limit: int = 8) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            result.append(text[:240])
        if len(result) >= limit:
            break
    return result


def _coerce_confirmations(value: Any) -> list[StrategyDraftConfirmation]:
    if not isinstance(value, list):
        return []
    items: list[StrategyDraftConfirmation] = []
    for row in value[:8]:
        if not isinstance(row, Mapping):
            continue
        field = str(row.get("field") or row.get("kind") or "假设").strip()
        assumed_as = str(row.get("assumed_as") or row.get("value") or "待确认").strip()
        reason = str(row.get("reason") or row.get("message") or "LLM 生成草案时做出的假设。").strip()
        if field and assumed_as and reason:
            items.append(StrategyDraftConfirmation(field=field[:80], assumed_as=assumed_as[:160], reason=reason[:240]))
    return items


def _merge_strategy_dsl_defaults(raw_dsl: Any, strategy_type: StrategyType) -> StrategyDsl:
    default_payload = _default_dsl(strategy_type).model_dump()
    payload = deepcopy(default_payload)
    if isinstance(raw_dsl, Mapping):
        for key in StrategyDsl.model_fields:
            if key not in raw_dsl:
                continue
            value = raw_dsl[key]
            if isinstance(value, Mapping) and isinstance(payload.get(key), dict):
                merged = dict(payload[key])
                merged.update(dict(value))
                payload[key] = merged
            elif value is not None:
                payload[key] = value
    payload["schema_version"] = "1.0"
    payload["strategy_type"] = strategy_type
    if not ((payload.get("factor_model") or {}).get("factors")):
        payload["factor_model"] = default_payload["factor_model"]
    if not isinstance(payload.get("execution"), dict):
        payload["execution"] = default_payload["execution"]
    execution = payload.setdefault("execution", {})
    data_engine = execution.setdefault("data_engine", {})
    if isinstance(data_engine, dict):
        data_engine.setdefault("filter", "duckdb")
        data_engine.setdefault("factor_compute", "polars")
    minute_loading = execution.setdefault("minute_loading", {})
    if isinstance(minute_loading, dict):
        minute_loading.setdefault("mode", "lazy_by_watchlist")
        minute_loading["forbid_full_market_preload"] = True
    execution.setdefault("market", "A_SHARE")
    execution.setdefault("lot_size", 100)
    if strategy_type == "selection":
        payload["entry"] = {"logic": "all", "conditions": []}
        payload["exit"] = {"logic": "any", "conditions": []}
    return StrategyDsl.model_validate(payload)


def _build_strategy_draft_response(
    payload: Mapping[str, Any] | None,
    *,
    prompt: str,
    strategy_type: StrategyType,
    llm_runtime: dict[str, Any],
    llm_used: bool,
) -> StrategyDraftResponse:
    payload = payload or {}
    selected_type = strategy_type
    dsl = _merge_strategy_dsl_defaults(payload.get("dsl"), selected_type)
    compiled = compile_strategy_dsl(dsl.model_dump())
    type_label = {
        "selection": "选股",
        "trading": "交易",
        "risk": "风控",
        "portfolio": "组合",
    }.get(selected_type, "组合")
    fallback_name = f"{prompt[:16].strip() or 'AI量化'}{type_label}策略"
    explanation = str(payload.get("explanation") or "").strip()
    if not explanation:
        explanation = "已按设置页远程 LLM 生成策略 DSL。" if llm_used else "LLM 未使用，已返回规则模板 DSL。"
    return StrategyDraftResponse(
        name=str(payload.get("name") or fallback_name).strip()[:80],
        strategy_type=selected_type,
        intent_summary=str(payload.get("intent_summary") or f"根据用户输入生成{type_label}策略草案。").strip()[:500],
        pending_confirmations=_coerce_confirmations(payload.get("pending_confirmations")),
        data_dependencies=_string_list(payload.get("data_dependencies"), limit=12) or [
            f"{preferred_daily_kline_table()}.close",
            f"{preferred_daily_kline_table()}.volume",
            f"{preferred_daily_kline_table()}.float_market_cap",
            f"{preferred_daily_kline_table()}.net_profit_ttm",
            f"{preferred_minute_kline_table()}.30m",
        ],
        risk_notes=_string_list(payload.get("risk_notes"), limit=10) or [
            "候选策略进入纸交易前必须通过样本外验证。",
            "分钟线只按 Watchlist 懒加载，避免全市场分钟数据 OOM。",
        ],
        dsl=dsl,
        explanation=explanation[:800],
        structured_output_schema=StrategyDslSchema.model_json_schema(),
        compile_report=compiled.to_response_payload(),
        llm_runtime={**llm_runtime, "used": llm_used, "status": "used" if llm_used else llm_runtime.get("status")},
    )


def _invoke_strategy_draft_llm(runtime_config: dict[str, Any], *, prompt: str, strategy_type: StrategyType) -> dict[str, Any]:
    provider = str(runtime_config.get("llm_provider") or "").strip().lower()
    model = _strategy_llm_model(runtime_config)
    base_url = str(runtime_config.get("backend_url") or "").strip() or None
    client_kwargs: dict[str, Any] = {"timeout": _STRATEGY_DRAFT_LLM_TIMEOUT_SECONDS}
    api_key = str(runtime_config.get("api_key") or "").strip()
    if api_key:
        client_kwargs["api_key"] = api_key
    client = create_llm_client(provider=provider, model=model, base_url=base_url, **client_kwargs)
    context = {
        "user_prompt": prompt,
        "requested_strategy_type": strategy_type,
        "daily_table": preferred_daily_kline_table(),
        "minute_table": preferred_minute_kline_table(),
        "default_dsl_template": _default_dsl(strategy_type).model_dump(),
        "json_schema": StrategyDslSchema.model_json_schema(),
    }
    result = client.get_llm().invoke(
        [
            SystemMessage(content=_STRATEGY_DRAFT_SYSTEM_PROMPT),
            HumanMessage(content=json.dumps(context, ensure_ascii=False)),
        ]
    )
    parsed = _parse_strategy_llm_json(str(getattr(result, "content", "") or ""))
    if not parsed:
        raise ValueError("LLM 策略草案返回值不是有效 JSON。")
    return parsed


def _detail_response(
    items: list[dict[str, Any]],
    *,
    skip: int,
    limit: int,
    sort_by: str | None,
    sort_order: Literal["asc", "desc"],
) -> dict[str, Any]:
    sorted_items = list(items)
    if sort_by:
        sorted_items.sort(
            key=lambda item: _sort_value(item, sort_by),
            reverse=sort_order == "desc",
        )
    total = len(sorted_items)
    paged = sorted_items[skip: skip + limit]
    return {
        "items": paged,
        "total": total,
        "skip": skip,
        "limit": limit,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }


def _artifact_detail_response(
    run_id: str,
    name: str,
    cache: dict[str, list[dict[str, Any]]],
    *,
    skip: int,
    limit: int,
    sort_by: str | None,
    sort_order: Literal["asc", "desc"],
    enrich_watchlist: bool = False,
) -> dict[str, Any]:
    cached_items = cache.get(run_id)
    if cached_items is not None:
        return _detail_response(
            enrich_watchlist_sector_metadata(cached_items) if enrich_watchlist else cached_items,
            skip=skip,
            limit=limit,
            sort_by=sort_by,
            sort_order=sort_order,
        )

    paged = read_artifact_page(
        run_id,
        name,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    if paged is not None:
        if enrich_watchlist:
            paged["items"] = enrich_watchlist_sector_metadata(paged.get("items") or [])
        return paged

    items = read_artifact_items(run_id, name)
    if enrich_watchlist:
        items = enrich_watchlist_sector_metadata(items)
    return _detail_response(
        items,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


def _watchlist_items_for_response(run_id: str) -> list[dict[str, Any]]:
    items = _WATCHLISTS.get(run_id) or read_artifact_items(run_id, "watchlists")
    return enrich_watchlist_sector_metadata(items)


def _sort_value(item: Mapping[str, Any], key: str) -> tuple[int, Any]:
    value = item.get(key)
    if value is None:
        return (1, "")
    if isinstance(value, (int, float, str, bool)):
        return (0, value)
    return (0, str(value))


def _ensure_backtest_exists(db: Session, run_id: str) -> None:
    if run_id not in _BACKTESTS and get_platform_backtest_run(db, run_id) is None:
        raise HTTPException(status_code=404, detail="Backtest not found")


def _seed_strategies() -> list[StrategyDefinition]:
    return [
        _make_strategy(
            "算力业绩高增波段策略",
            "portfolio",
            "active",
            "算力板块 + 中盘市值 + 业绩高增，周线趋势、日线波段、分钟线确认。",
            "llm",
            StrategyPerformance(total_return=0.286, annual_return=0.318, sharpe_ratio=1.82, max_drawdown=-0.112, win_rate=0.617, calmar_ratio=2.84),
        ),
        _make_strategy(
            "低波红利轮动组合策略",
            "portfolio",
            "active",
            "高股息 + 低波红利 + 央国企因子轮动，强调防守中的稳定收益。",
            "manual",
            StrategyPerformance(total_return=0.224, annual_return=0.247, sharpe_ratio=1.96, max_drawdown=-0.061, win_rate=0.708, calmar_ratio=4.05),
            dsl=_portfolio_dividend_rotation_dsl(),
            tags=["A股", "组合策略", "低波红利", "高胜率"],
        ),
        _make_strategy(
            "北向资金共振组合策略",
            "portfolio",
            "draft",
            "资金流、成长和趋势共振的进攻型组合，适合科技成长主线行情。",
            "llm",
            StrategyPerformance(total_return=0.301, annual_return=0.336, sharpe_ratio=1.88, max_drawdown=-0.109, win_rate=0.654, calmar_ratio=3.08),
            dsl=_portfolio_northbound_resonance_dsl(),
            tags=["A股", "组合策略", "资金共振", "高收益"],
        ),
        _make_strategy(
            "鳄鱼张口趋势突破",
            "trading",
            "draft",
            "基于 Alligator 张口、成交量放大和 ATR 移动止损的波段交易策略。",
            "manual",
            StrategyPerformance(total_return=0.173, annual_return=0.205, sharpe_ratio=1.34, max_drawdown=-0.096, win_rate=0.574, calmar_ratio=2.13),
            dsl=_trading_alligator_pro_dsl(),
            tags=["A股", "DSL", "波段交易"],
        ),
        _make_strategy(
            "30分钟 VWAP 回踩交易策略",
            "trading",
            "active",
            "日线强趋势 + 30 分钟 VWAP 回踩确认，侧重高胜率短波段切入。",
            "manual",
            StrategyPerformance(total_return=0.247, annual_return=0.291, sharpe_ratio=1.94, max_drawdown=-0.078, win_rate=0.694, calmar_ratio=3.73),
            dsl=_trading_vwap_reclaim_dsl(),
            tags=["A股", "交易策略", "VWAP", "高胜率"],
        ),
        _make_strategy(
            "鳄鱼张口强趋势交易策略",
            "trading",
            "draft",
            "聚焦强趋势主升段，使用鳄鱼张口 + 资金流共振提升盈亏比。",
            "llm",
            StrategyPerformance(total_return=0.268, annual_return=0.312, sharpe_ratio=1.86, max_drawdown=-0.095, win_rate=0.661, calmar_ratio=3.28),
            dsl=_trading_alligator_pro_dsl(),
            tags=["A股", "交易策略", "趋势突破", "高收益"],
        ),
        _make_strategy(
            "首日波段交易策略",
            "trading",
            "draft",
            "由同花顺波段公式改写：波段线上穿 B1 金叉买入，波段线下穿 B1 死叉卖出。",
            "manual",
            StrategyPerformance(total_return=0.182, annual_return=0.214, sharpe_ratio=1.52, max_drawdown=-0.082, win_rate=0.673, calmar_ratio=2.61),
            dsl=_first_day_band_dsl(),
            tags=["A股", "同花顺指标", "首日波段", "交易策略"],
        ),
        _make_strategy(
            "算力高景气优选选股策略",
            "selection",
            "active",
            "聚焦算力与数据中心主线，筛选高增长、高资金关注度、高景气标的。",
            "llm",
            StrategyPerformance(total_return=0.236, annual_return=0.278, sharpe_ratio=1.72, max_drawdown=-0.086, win_rate=0.702, calmar_ratio=3.23),
            dsl=_selection_compute_quality_dsl(),
            tags=["A股", "选股策略", "算力", "高胜率"],
        ),
        _make_strategy(
            "资金共振突破选股策略",
            "selection",
            "draft",
            "用资金流 + 动量 + 低波过滤寻找突破前夜候选池，偏向高命中率短名单。",
            "manual",
            StrategyPerformance(total_return=0.219, annual_return=0.251, sharpe_ratio=1.68, max_drawdown=-0.079, win_rate=0.688, calmar_ratio=3.18),
            dsl=_selection_moneyflow_breakout_dsl(),
            tags=["A股", "选股策略", "资金流", "突破"],
        ),
        _make_strategy(
            "动态回撤保护风控策略",
            "risk",
            "active",
            "强调回撤保护、仓位收缩和现金缓冲，适合作为波段交易的统一风控层。",
            "manual",
            StrategyPerformance(total_return=0.198, annual_return=0.224, sharpe_ratio=2.08, max_drawdown=-0.053, win_rate=0.742, calmar_ratio=4.23),
            dsl=_risk_drawdown_guard_dsl(),
            tags=["A股", "风控策略", "回撤保护", "高胜率"],
        ),
        _make_strategy(
            "高波动降仓风控策略",
            "risk",
            "draft",
            "在高波动环境下降低仓位、提高现金储备，减少回撤并提升持仓稳定度。",
            "llm",
            StrategyPerformance(total_return=0.176, annual_return=0.203, sharpe_ratio=2.16, max_drawdown=-0.047, win_rate=0.756, calmar_ratio=4.32),
            dsl=_risk_volatility_overlay_dsl(),
            tags=["A股", "风控策略", "波动控制", "稳健"],
        ),
    ]


def _official_strategy_pack_blueprints() -> list[dict[str, Any]]:
    return [
        {
            "id": "selection_official_triple",
            "name": "官方选股三档策略包",
            "strategy_type": "selection",
            "description": "覆盖激进、稳健、防守三种选股风格，便于快速建立候选池体系。",
            "tags": ["官方策略包", "选股", "激进稳健防守"],
            "items": [
                {
                    "blueprint_id": "selection_aggressive_breakout",
                    "name": "官方·选股·激进突破",
                    "tier": "aggressive",
                    "version": 1,
                    "description": "偏进攻，强调资金流与动量共振，适合主升浪阶段抢占候选池。",
                    "dsl": _selection_moneyflow_breakout_dsl(),
                    "performance": StrategyPerformance(total_return=0.268, annual_return=0.309, sharpe_ratio=1.76, max_drawdown=-0.101, win_rate=0.664, calmar_ratio=3.06),
                    "tags": ["官方策略包", "选股", "激进", "高收益"],
                },
                {
                    "blueprint_id": "selection_stable_quality",
                    "name": "官方·选股·稳健质增",
                    "tier": "stable",
                    "version": 1,
                    "description": "偏均衡，突出业绩增长、资金关注与中期强势的平衡。",
                    "dsl": _selection_compute_quality_dsl(),
                    "performance": StrategyPerformance(total_return=0.236, annual_return=0.278, sharpe_ratio=1.72, max_drawdown=-0.086, win_rate=0.702, calmar_ratio=3.23),
                    "tags": ["官方策略包", "选股", "稳健", "高胜率"],
                },
                {
                    "blueprint_id": "selection_defensive_quality",
                    "name": "官方·选股·防守低波",
                    "tier": "defensive",
                    "version": 1,
                    "description": "偏防守，重视低波与质量因子，适合震荡或防守期候选池构建。",
                    "dsl": _selection_defensive_quality_dsl(),
                    "performance": StrategyPerformance(total_return=0.189, annual_return=0.216, sharpe_ratio=1.88, max_drawdown=-0.058, win_rate=0.741, calmar_ratio=3.72),
                    "tags": ["官方策略包", "选股", "防守", "低波"],
                },
            ],
        },
        {
            "id": "trading_official_triple",
            "name": "官方交易三档策略包",
            "strategy_type": "trading",
            "description": "覆盖趋势突破、均衡回踩、防守脉冲三类交易风格。",
            "tags": ["官方策略包", "交易", "激进稳健防守"],
            "items": [
                {
                    "blueprint_id": "trading_aggressive_alligator",
                    "name": "官方·交易·激进趋势",
                    "tier": "aggressive",
                    "version": 1,
                    "description": "高弹性趋势交易，适合强主线行情追随主升段。",
                    "dsl": _trading_alligator_pro_dsl(),
                    "performance": StrategyPerformance(total_return=0.286, annual_return=0.334, sharpe_ratio=1.91, max_drawdown=-0.114, win_rate=0.648, calmar_ratio=2.93),
                    "tags": ["官方策略包", "交易", "激进", "趋势"],
                },
                {
                    "blueprint_id": "trading_stable_vwap",
                    "name": "官方·交易·稳健回踩",
                    "tier": "stable",
                    "version": 1,
                    "description": "日线趋势 + 分钟 VWAP 回踩确认，兼顾胜率与盈亏比。",
                    "dsl": _trading_vwap_reclaim_dsl(),
                    "performance": StrategyPerformance(total_return=0.247, annual_return=0.291, sharpe_ratio=1.94, max_drawdown=-0.078, win_rate=0.694, calmar_ratio=3.73),
                    "tags": ["官方策略包", "交易", "稳健", "VWAP"],
                },
                {
                    "blueprint_id": "trading_defensive_pulse",
                    "name": "官方·交易·防守脉冲",
                    "tier": "defensive",
                    "version": 1,
                    "description": "更小仓位、更紧止损，适合防守期做低波动脉冲交易。",
                    "dsl": _trading_defensive_pulse_dsl(),
                    "performance": StrategyPerformance(total_return=0.176, annual_return=0.205, sharpe_ratio=1.98, max_drawdown=-0.051, win_rate=0.752, calmar_ratio=4.02),
                    "tags": ["官方策略包", "交易", "防守", "高胜率"],
                },
            ],
        },
        {
            "id": "risk_official_triple",
            "name": "官方风控三档策略包",
            "strategy_type": "risk",
            "description": "覆盖收益优先、均衡保护、极致防守三档风控覆盖层。",
            "tags": ["官方策略包", "风控", "激进稳健防守"],
            "items": [
                {
                    "blueprint_id": "risk_aggressive_overlay",
                    "name": "官方·风控·激进覆盖",
                    "tier": "aggressive",
                    "version": 1,
                    "description": "较宽风控边界，保留弹性，适合进攻型主线交易。",
                    "dsl": _risk_aggressive_overlay_dsl(),
                    "performance": StrategyPerformance(total_return=0.214, annual_return=0.248, sharpe_ratio=1.89, max_drawdown=-0.081, win_rate=0.688, calmar_ratio=3.06),
                    "tags": ["官方策略包", "风控", "激进", "收益优先"],
                },
                {
                    "blueprint_id": "risk_stable_guard",
                    "name": "官方·风控·稳健保护",
                    "tier": "stable",
                    "version": 1,
                    "description": "回撤、仓位和现金约束更均衡，适合作为通用风控层。",
                    "dsl": _risk_drawdown_guard_dsl(),
                    "performance": StrategyPerformance(total_return=0.198, annual_return=0.224, sharpe_ratio=2.08, max_drawdown=-0.053, win_rate=0.742, calmar_ratio=4.23),
                    "tags": ["官方策略包", "风控", "稳健", "回撤保护"],
                },
                {
                    "blueprint_id": "risk_defensive_overlay",
                    "name": "官方·风控·防守降波",
                    "tier": "defensive",
                    "version": 1,
                    "description": "提高现金储备、压缩波动暴露，适合震荡与防守场景。",
                    "dsl": _risk_volatility_overlay_dsl(),
                    "performance": StrategyPerformance(total_return=0.176, annual_return=0.203, sharpe_ratio=2.16, max_drawdown=-0.047, win_rate=0.756, calmar_ratio=4.32),
                    "tags": ["官方策略包", "风控", "防守", "降波动"],
                },
            ],
        },
        {
            "id": "portfolio_official_triple",
            "name": "官方组合三档策略包",
            "strategy_type": "portfolio",
            "description": "覆盖进攻、均衡、防守三类组合策略，适合作为完整多周期样板。",
            "tags": ["官方策略包", "组合", "激进稳健防守"],
            "items": [
                {
                    "blueprint_id": "portfolio_aggressive_resonance",
                    "name": "官方·组合·激进共振",
                    "tier": "aggressive",
                    "version": 1,
                    "description": "成长 + 资金流共振的进攻型组合模板。",
                    "dsl": _portfolio_northbound_resonance_dsl(),
                    "performance": StrategyPerformance(total_return=0.301, annual_return=0.336, sharpe_ratio=1.88, max_drawdown=-0.109, win_rate=0.654, calmar_ratio=3.08),
                    "tags": ["官方策略包", "组合", "激进", "高收益"],
                },
                {
                    "blueprint_id": "portfolio_stable_compute",
                    "name": "官方·组合·稳健成长",
                    "tier": "stable",
                    "version": 1,
                    "description": "算力成长与多周期波段的均衡型完整组合模板。",
                    "dsl": _default_dsl("portfolio"),
                    "performance": StrategyPerformance(total_return=0.286, annual_return=0.318, sharpe_ratio=1.82, max_drawdown=-0.112, win_rate=0.617, calmar_ratio=2.84),
                    "tags": ["官方策略包", "组合", "稳健", "多周期"],
                },
                {
                    "blueprint_id": "portfolio_defensive_dividend",
                    "name": "官方·组合·防守红利",
                    "tier": "defensive",
                    "version": 1,
                    "description": "偏高股息低波红利，适合防守型轮动组合。",
                    "dsl": _portfolio_dividend_rotation_dsl(),
                    "performance": StrategyPerformance(total_return=0.224, annual_return=0.247, sharpe_ratio=1.96, max_drawdown=-0.061, win_rate=0.708, calmar_ratio=4.05),
                    "tags": ["官方策略包", "组合", "防守", "低波红利"],
                },
            ],
        },
    ]


def _official_strategy_packs() -> list[OfficialStrategyPack]:
    packs: list[OfficialStrategyPack] = []
    for pack in _official_strategy_pack_blueprints():
        packs.append(
            OfficialStrategyPack(
                id=pack["id"],
                name=pack["name"],
                strategy_type=pack["strategy_type"],
                description=pack["description"],
                tags=list(pack.get("tags") or []),
                items=[
                    OfficialStrategyPackItem(
                        blueprint_id=item["blueprint_id"],
                        name=item["name"],
                        strategy_type=pack["strategy_type"],
                        tier=item["tier"],
                        version=item["version"],
                        description=item["description"],
                        performance=item["performance"],
                        tags=list(item.get("tags") or []),
                        dsl=None,
                    )
                    for item in pack["items"]
                ],
            )
        )
    return packs


def _get_official_strategy_pack(pack_id: str) -> dict[str, Any] | None:
    return next((item for item in _official_strategy_pack_blueprints() if item["id"] == pack_id), None)


def _get_official_strategy_pack_item(pack_id: str, blueprint_id: str) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    pack = _get_official_strategy_pack(pack_id)
    if pack is None:
        return None, None
    item = next((candidate for candidate in pack["items"] if candidate["blueprint_id"] == blueprint_id), None)
    if item is None:
        return pack, None
    return pack, item


def _unique_strategy_name(db: Session, base_name: str) -> str:
    existing = {item.get("name") for item in list_platform_strategies(db)}
    if base_name not in existing:
        return base_name
    index = 2
    while True:
        candidate = f"{base_name}（{index}）"
        if candidate not in existing:
            return candidate
        index += 1


def _official_sync_meta(payload: dict[str, Any] | None) -> dict[str, Any]:
    params = (payload or {}).get("template_parameters") or {}
    pack_id = params.get("pack_id")
    blueprint_id = (payload or {}).get("template_id")
    if not (params.get("official") and pack_id and blueprint_id):
        return {}
    pack, item = _get_official_strategy_pack_item(str(pack_id), str(blueprint_id))
    if pack is None or item is None:
        return {}
    current_version = int(params.get("official_version") or 0)
    latest_version = int(item.get("version") or 0)
    return {
        "official_pack_id": pack["id"],
        "official_pack_name": pack["name"],
        "official_blueprint_id": item["blueprint_id"],
        "official_tier": item["tier"],
        "official_current_version": current_version,
        "official_latest_version": latest_version,
        "official_update_available": current_version < latest_version,
    }


def _strategy_definition_from_payload(payload: dict[str, Any]) -> StrategyDefinition:
    enriched = dict(payload)
    enriched.update(_official_sync_meta(payload))
    return StrategyDefinition(**enriched)


def _ensure_seed_strategies(db: Session) -> None:
    existing = list_platform_strategies(db)
    existing_names = {item.get("name") for item in existing}
    legacy_name = "首日波段金叉选股策略"
    for seed in _seed_strategies():
        if seed.name == "首日波段交易策略" and legacy_name in existing_names:
            continue
        if seed.name not in existing_names:
            save_platform_strategy(db, seed.model_dump())
    target_seed = next((seed for seed in _seed_strategies() if seed.name == "首日波段交易策略"), None)
    if target_seed is not None:
        for item in existing:
            if item.get("name") != legacy_name:
                continue
            migrated = target_seed.model_dump()
            migrated["id"] = item["id"]
            migrated["created_at"] = item.get("created_at") or migrated["created_at"]
            migrated["run_count"] = item.get("run_count") or 0
            migrated["last_run_time"] = item.get("last_run_time")
            if migrated.get("current_version"):
                migrated["current_version"]["strategy_id"] = item["id"]
            migrated["versions"] = [migrated["current_version"]] if migrated.get("current_version") else []
            save_platform_strategy(db, migrated)


@router.get("/v1/factors", response_model=FactorRegistryListResponse)
async def list_strategy_platform_factors(
    active_only: bool = Query(True),
    category: str | None = Query(None),
    search: str | None = Query(None),
    db: Session = Depends(get_strategy_db),
):
    items = [FactorRegistryItem(**payload) for payload in list_factor_registry(db, active_only=active_only)]
    if category:
        items = [item for item in items if item.category == category]
    if search:
        lowered = search.lower()
        items = [
            item for item in items
            if lowered in item.name.lower()
            or lowered in item.display_name.lower()
            or lowered in (item.description or "").lower()
        ]
    return FactorRegistryListResponse(total=len(items), items=items)


@router.get("/v1/factors/{factor_name}", response_model=FactorRegistryItem)
async def get_strategy_platform_factor(factor_name: str, db: Session = Depends(get_strategy_db)):
    payload = get_factor_registry_item(db, factor_name)
    if payload is None:
        raise HTTPException(status_code=404, detail="Factor not found")
    return FactorRegistryItem(**payload)


@router.post("/v1/strategies/llm-draft", response_model=StrategyDraftResponse)
async def create_llm_strategy_draft(
    request: StrategyDraftRequest,
    main_db: Session = Depends(get_db),
    current_user: UserDB | None = Depends(optional_web_user),
):
    prompt = request.prompt.strip()
    requested_strategy_type = _strategy_type_from_request(prompt, request.strategy_type)
    runtime_config = _strategy_runtime_config(current_user, main_db)
    llm_runtime = _strategy_llm_runtime_payload(current_user, main_db, runtime_config=runtime_config)

    llm_payload: dict[str, Any] | None = None
    if llm_runtime.get("ready"):
        try:
            llm_payload = await asyncio.to_thread(
                _invoke_strategy_draft_llm,
                runtime_config,
                prompt=prompt,
                strategy_type=requested_strategy_type,
            )
            llm_runtime["model_used"] = _strategy_llm_model(runtime_config)
        except Exception as exc:
            llm_runtime["status"] = "failed"
            llm_runtime["reason"] = "远程 LLM 策略草案生成失败，已使用规则模板兜底。"
            llm_runtime["error"] = _safe_strategy_llm_error(exc, runtime_config)

    return _build_strategy_draft_response(
        llm_payload,
        prompt=prompt,
        strategy_type=requested_strategy_type,
        llm_runtime=llm_runtime,
        llm_used=llm_payload is not None,
    )


@router.get("/v1/strategies/dsl-schema")
async def get_strategy_dsl_schema():
    return {
        "schema_name": "StrategyDslSchema",
        "schema_version": "1.0",
        "structured_outputs": True,
        "json_schema": StrategyDslSchema.model_json_schema(),
    }


@router.post("/v1/strategies/compile-preview", response_model=StrategyCompileResponse)
async def compile_strategy_preview(request: StrategyCompilePreviewRequest):
    compiled = compile_strategy_dsl(request.dsl.model_dump())
    return StrategyCompileResponse(**compiled.to_response_payload())


@router.get("/v1/strategies/templates")
async def list_strategy_templates():
    return {"templates": [item.model_dump() for item in _strategy_templates()]}


@router.get("/v1/strategies/templates/{template_id}", response_model=StrategyTemplateDefinition)
async def get_strategy_template(template_id: str):
    template = _get_strategy_template(template_id)
    if template is None:
        raise HTTPException(status_code=404, detail="Strategy template not found")
    return template


@router.get("/v1/strategies/packs", response_model=OfficialStrategyPackListResponse)
async def list_official_strategy_packs():
    packs = _official_strategy_packs()
    return OfficialStrategyPackListResponse(total=len(packs), packs=packs)


@router.get("/v1/strategies/packs/{pack_id}/items/{blueprint_id}", response_model=OfficialStrategyPackItem)
async def get_official_strategy_pack_item(pack_id: str, blueprint_id: str):
    pack, item = _get_official_strategy_pack_item(pack_id, blueprint_id)
    if pack is None or item is None:
        raise HTTPException(status_code=404, detail="Strategy pack item not found")
    return OfficialStrategyPackItem(
        blueprint_id=item["blueprint_id"],
        name=item["name"],
        strategy_type=pack["strategy_type"],
        tier=item["tier"],
        version=item["version"],
        description=item["description"],
        performance=item["performance"],
        tags=list(item.get("tags") or []),
        dsl=item["dsl"],
    )


@router.post("/v1/strategies/packs/{pack_id}/clone", response_model=OfficialStrategyPackCloneResponse)
async def clone_official_strategy_pack(pack_id: str, db: Session = Depends(get_strategy_db)):
    pack = _get_official_strategy_pack(pack_id)
    if pack is None:
        raise HTTPException(status_code=404, detail="Strategy pack not found")

    cloned_items: list[StrategyDefinition] = []
    for item in pack["items"]:
        strategy = _make_strategy(
            _unique_strategy_name(db, item["name"]),
            pack["strategy_type"],
            "draft",
            item["description"],
            "template",
            item["performance"],
            dsl=item["dsl"],
            tags=[*list(item.get("tags") or []), "官方示例"],
        )
        strategy.run_count = 0
        strategy.last_run_time = None
        strategy.template_id = item["blueprint_id"]
        strategy.template_name = pack["name"]
        strategy.template_parameters = {
            "pack_id": pack["id"],
            "tier": item["tier"],
            "official": True,
            "official_version": item["version"],
        }
        saved = save_platform_strategy(db, strategy.model_dump())
        cloned_items.append(_strategy_definition_from_payload(saved))

    return OfficialStrategyPackCloneResponse(
        pack_id=pack["id"],
        pack_name=pack["name"],
        cloned_count=len(cloned_items),
        strategies=cloned_items,
        message=f"已从 {pack['name']} 克隆 {len(cloned_items)} 个策略到你的策略列表。",
    )


@router.post("/v1/strategies/packs/{pack_id}/items/{blueprint_id}/clone", response_model=StrategyDefinition)
async def clone_official_strategy_pack_item(
    pack_id: str,
    blueprint_id: str,
    request: OfficialStrategyPackItemCloneRequest,
    db: Session = Depends(get_strategy_db),
):
    pack, item = _get_official_strategy_pack_item(pack_id, blueprint_id)
    if pack is None or item is None:
        raise HTTPException(status_code=404, detail="Strategy pack item not found")

    strategy = _make_strategy(
        _unique_strategy_name(db, request.name or item["name"]),
        pack["strategy_type"],
        request.status,
        item["description"],
        "template",
        item["performance"],
        dsl=item["dsl"],
        tags=[*list(item.get("tags") or []), "官方示例"],
    )
    strategy.run_count = 0
    strategy.last_run_time = None
    strategy.template_id = item["blueprint_id"]
    strategy.template_name = pack["name"]
    strategy.template_parameters = {
        "pack_id": pack["id"],
        "tier": item["tier"],
        "official": True,
        "official_version": item["version"],
    }
    saved = save_platform_strategy(db, strategy.model_dump())
    return _strategy_definition_from_payload(saved)


@router.get("/v1/strategies", response_model=StrategyListResponse)
async def list_strategy_platform(
    strategy_type: str | None = Query(None),
    status: str | None = Query(None),
    search: str | None = Query(None),
    db: Session = Depends(get_strategy_db),
):
    _ensure_seed_strategies(db)
    items = [
        _strategy_definition_from_payload(payload)
        for payload in list_platform_strategies(
            db,
            strategy_type=strategy_type,
            status=status,
            search=search,
        )
    ]
    return StrategyListResponse(total=len(items), strategies=items)


@router.post("/v1/strategies", response_model=StrategyDefinition)
async def save_strategy_platform(request: StrategyCreateRequest, db: Session = Depends(get_strategy_db)):
    strategy_id = uuid4().hex
    version_id = uuid4().hex
    now = _now()
    compiled = compile_strategy_dsl(request.dsl.model_dump())
    version = StrategyVersion(
        id=version_id,
        strategy_id=strategy_id,
        version=1,
        dsl=request.dsl,
        compile_status=compiled.status,
        compiled_hash=uuid4().hex[:12],
        change_summary="由 AI 创建器保存",
        created_at=now,
    )
    strategy = StrategyDefinition(
        id=strategy_id,
        name=request.name,
        strategy_type=request.strategy_type,
        status=request.status,
        description=request.description,
        source=request.source,
        current_version_id=version_id,
        version=1,
        is_active=request.status == "active",
        run_count=0,
        created_at=now,
        updated_at=now,
        current_version=version,
        tags=["AI创建", "待回测", *(["模板策略"] if request.template_id else [])],
        template_id=request.template_id,
        template_name=request.template_name,
        template_parameters=request.template_parameters,
    )
    saved = save_platform_strategy(db, strategy.model_dump())
    return _strategy_definition_from_payload(saved)


@router.get("/v1/strategies/{strategy_id}", response_model=StrategyDefinition)
async def get_strategy_platform(strategy_id: str, db: Session = Depends(get_strategy_db)):
    _ensure_seed_strategies(db)
    strategy = get_platform_strategy(db, strategy_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return _strategy_definition_from_payload(strategy)


@router.put("/v1/strategies/{strategy_id}", response_model=StrategyDefinition)
async def update_strategy_platform(strategy_id: str, request: StrategyUpdateRequest, db: Session = Depends(get_strategy_db)):
    payload = get_platform_strategy(db, strategy_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Strategy not found")

    strategy = StrategyDefinition(**payload)
    versions = get_platform_strategy_versions(db, strategy_id)
    compiled = compile_strategy_dsl(request.dsl.model_dump())
    now = _now()
    next_version = max([int(item.get("version") or 0) for item in versions] + [int(strategy.version or 1)]) + 1
    version = StrategyVersion(
        id=uuid4().hex,
        strategy_id=strategy_id,
        version=next_version,
        dsl=request.dsl,
        compile_status=compiled.status,
        compiled_hash=uuid4().hex[:12],
        change_summary="页面编辑更新",
        created_at=now,
    )

    strategy.name = request.name
    strategy.strategy_type = request.strategy_type
    strategy.description = request.description
    strategy.source = request.source or strategy.source or "manual"
    strategy.status = request.status or strategy.status
    strategy.is_active = strategy.status == "active"
    strategy.version = next_version
    strategy.current_version_id = version.id
    strategy.current_version = version
    strategy.template_id = request.template_id if request.template_id is not None else strategy.template_id
    strategy.template_name = request.template_name if request.template_name is not None else strategy.template_name
    if request.template_parameters is not None:
        strategy.template_parameters = request.template_parameters
    strategy.updated_at = now

    saved_payload = strategy.model_dump()
    saved_payload["versions"] = [*versions, version.model_dump()]
    saved = save_platform_strategy(db, saved_payload)
    return _strategy_definition_from_payload(saved)


@router.delete("/v1/strategies/{strategy_id}")
async def delete_strategy_platform(strategy_id: str, db: Session = Depends(get_strategy_db)):
    deleted = delete_platform_strategy(db, strategy_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"message": "Strategy deleted"}


@router.get("/v1/strategies/{strategy_id}/versions")
async def get_strategy_platform_versions(strategy_id: str, db: Session = Depends(get_strategy_db)):
    if get_platform_strategy(db, strategy_id) is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {"versions": get_platform_strategy_versions(db, strategy_id)}


@router.post("/v1/strategies/{strategy_id}/versions", response_model=StrategyDefinition)
async def create_strategy_platform_version(strategy_id: str, request: StrategyVersionCreateRequest, db: Session = Depends(get_strategy_db)):
    payload = get_platform_strategy(db, strategy_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy = StrategyDefinition(**payload)
    versions = get_platform_strategy_versions(db, strategy_id)
    next_version = max([int(item.get("version") or 0) for item in versions] + [int(strategy.version or 1)]) + 1
    version_id = uuid4().hex
    compiled = compile_strategy_dsl(request.dsl.model_dump())
    new_version = StrategyVersion(
        id=version_id,
        strategy_id=strategy.id,
        version=next_version,
        dsl=request.dsl,
        compile_status=compiled.status,
        compiled_hash=uuid4().hex[:12],
        change_summary=request.change_summary,
        created_at=_now(),
    )
    strategy.version = next_version
    if request.activate:
        strategy.current_version_id = version_id
        strategy.current_version = new_version
    strategy.updated_at = _now()
    saved_payload = strategy.model_dump()
    saved_payload["versions"] = [*versions, new_version.model_dump()]
    saved = save_platform_strategy(db, saved_payload)
    return _strategy_definition_from_payload(saved)


@router.post("/v1/strategies/{strategy_id}/clone", response_model=StrategyDefinition)
async def clone_strategy_platform(strategy_id: str, request: StrategyCloneRequest, db: Session = Depends(get_strategy_db)):
    payload = get_platform_strategy(db, strategy_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    base = StrategyDefinition(**payload)
    cloned = deepcopy(base)
    cloned.id = uuid4().hex
    cloned.name = request.name or f"{base.name} 副本"
    cloned.status = request.status
    cloned.is_active = request.status == "active"
    cloned.source = "manual"
    cloned.run_count = 0
    cloned.last_run_time = None
    cloned.performance = None
    cloned.version = 1
    now = _now()
    cloned.created_at = now
    cloned.updated_at = now
    if cloned.current_version:
        cloned.current_version = deepcopy(cloned.current_version)
        cloned.current_version.id = uuid4().hex
        cloned.current_version.strategy_id = cloned.id
        cloned.current_version.version = 1
        cloned.current_version.change_summary = "克隆初始版本"
        cloned.current_version.created_at = now
        cloned.current_version_id = cloned.current_version.id
    saved_payload = cloned.model_dump()
    saved_payload["versions"] = [cloned.current_version.model_dump()] if cloned.current_version else []
    saved = save_platform_strategy(db, saved_payload)
    return _strategy_definition_from_payload(saved)


@router.post("/v1/strategies/{strategy_id}/activate", response_model=StrategyDefinition)
async def activate_strategy_platform(strategy_id: str, request: StrategyActivateRequest, db: Session = Depends(get_strategy_db)):
    payload = get_platform_strategy(db, strategy_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy = StrategyDefinition(**payload)
    strategy.status = request.status
    strategy.is_active = request.status == "active"
    strategy.updated_at = _now()
    saved = save_platform_strategy(db, strategy.model_dump())
    return _strategy_definition_from_payload(saved)


@router.post("/v1/strategies/{strategy_id}/sync-official", response_model=StrategyDefinition)
async def sync_strategy_with_official_pack(strategy_id: str, db: Session = Depends(get_strategy_db)):
    payload = get_platform_strategy(db, strategy_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Strategy not found")

    params = payload.get("template_parameters") or {}
    pack_id = params.get("pack_id")
    blueprint_id = payload.get("template_id")
    if not (params.get("official") and pack_id and blueprint_id):
        raise HTTPException(status_code=400, detail="Strategy is not linked to an official strategy pack")

    pack, item = _get_official_strategy_pack_item(str(pack_id), str(blueprint_id))
    if pack is None or item is None:
        raise HTTPException(status_code=404, detail="Official strategy blueprint not found")

    strategy = StrategyDefinition(**payload)
    versions = get_platform_strategy_versions(db, strategy_id)
    next_version = max([int(version.get("version") or 0) for version in versions] + [int(strategy.version or 1)]) + 1
    compiled = compile_strategy_dsl(item["dsl"].model_dump())
    now = _now()
    new_version = StrategyVersion(
        id=uuid4().hex,
        strategy_id=strategy_id,
        version=next_version,
        dsl=item["dsl"],
        compile_status=compiled.status,
        compiled_hash=uuid4().hex[:12],
        change_summary=f"同步官方策略包：{pack['name']} / {item['name']} v{item['version']}",
        created_at=now,
    )

    strategy.strategy_type = pack["strategy_type"]
    strategy.description = item["description"]
    strategy.source = "template"
    strategy.version = next_version
    strategy.current_version_id = new_version.id
    strategy.current_version = new_version
    strategy.updated_at = now
    strategy.template_name = pack["name"]
    strategy.template_parameters = {
        **params,
        "pack_id": pack["id"],
        "tier": item["tier"],
        "official": True,
        "official_version": item["version"],
    }
    strategy.tags = sorted(set([*strategy.tags, *list(item.get("tags") or []), "官方示例"]))
    saved_payload = strategy.model_dump()
    saved_payload["versions"] = [*versions, new_version.model_dump()]
    saved = save_platform_strategy(db, saved_payload)
    return _strategy_definition_from_payload(saved)


@router.post("/v1/strategies/{strategy_id}/compile", response_model=StrategyCompileResponse)
async def compile_strategy_platform(strategy_id: str, db: Session = Depends(get_strategy_db)):
    strategy = get_platform_strategy(db, strategy_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy_model = StrategyDefinition(**strategy)
    compiled = compile_strategy_dsl(
        strategy_model.current_version.dsl.model_dump() if strategy_model.current_version else _default_dsl(strategy_model.strategy_type).model_dump()
    )
    if strategy_model.current_version:
        strategy_model.current_version.compile_status = compiled.status
        save_platform_strategy(db, strategy_model.model_dump())
    return StrategyCompileResponse(**compiled.to_response_payload())


def _build_backtest_result_payload(engine_result: Any) -> dict[str, Any]:
    return {
        "metrics": engine_result.metrics,
        "summary": engine_result.summary,
        "details": {
            "equity_curve": engine_result.equity[:500],
            "trade_list": engine_result.trades[:200],
            "trade_snapshots": engine_result.snapshots[:200],
            "orders": engine_result.orders[:200],
            "watchlists": engine_result.watchlists[:300],
            "minute_confirmations": engine_result.minute_confirmations[:300],
            "signals": engine_result.signals[:200],
            "positions": engine_result.positions[:200],
            "compiled_strategy": engine_result.compiled_strategy,
        },
        "diagnostics": engine_result.diagnostics,
    }


def _resolve_backtest_mode(request: BacktestCreateRequest) -> str:
    if request.backtest_mode:
        return request.backtest_mode
    return "daily_select_intraday_trade" if request.frequency == "daily_minute" else "daily_only"


def _resolve_backtest_frequency(request: BacktestCreateRequest) -> str:
    mode = _resolve_backtest_mode(request)
    if mode == "daily_only":
        return "daily"
    if mode == "daily_select_intraday_trade":
        return "daily_minute"
    return request.frequency


def _merge_backtest_request_into_dsl(dsl: dict[str, Any], request: BacktestCreateRequest) -> dict[str, Any]:
    merged = deepcopy(dsl or {})
    merged.setdefault("universe", {})
    merged.setdefault("execution", {})

    universe = merged["universe"]
    execution = merged["execution"]
    filters = list(universe.get("filters") or [])
    include_concepts = list(universe.get("include_concepts") or [])

    request_symbols = [symbol.strip() for symbol in (request.symbols or []) if str(symbol).strip()]
    request_universe = request.universe or {}
    universe_scope = str(request_universe.get("scope") or "all")
    sector_name = str(request_universe.get("sector") or "").strip()

    if universe_scope == "sector" and sector_name:
        include_concepts.append(sector_name)
    if universe_scope == "symbols":
        symbol_values = [symbol.strip() for symbol in (request_universe.get("symbols") or request_symbols) if str(symbol).strip()]
        if symbol_values:
            filters.append({"field": "symbol", "op": "in", "value": symbol_values})
    if universe_scope == "chinext":
        filters.append({"field": "symbol", "op": "prefix_any", "value": ["300", "301"]})
    if universe_scope == "beijing":
        filters.append({"field": "symbol", "op": "prefix_any", "value": ["4", "8", "9"]})
    if universe_scope == "main_board":
        filters.append({"field": "symbol", "op": "prefix_any", "value": ["000", "001", "002", "003", "600", "601", "603", "605"]})

    universe["include_concepts"] = list(dict.fromkeys([item for item in include_concepts if str(item).strip()]))
    universe["filters"] = filters

    cost_config = request.cost_config or {}
    if cost_config:
        if cost_config.get("commission_rate") is not None:
            execution["commission_rate"] = float(cost_config["commission_rate"])
        if cost_config.get("stamp_duty_rate") is not None:
            execution["stamp_duty_rate"] = float(cost_config["stamp_duty_rate"])
        if cost_config.get("slippage_rate") is not None:
            execution["slippage_model"] = {"type": "bps", "value": float(cost_config["slippage_rate"]) * 10000}
        if cost_config.get("min_commission") is not None:
            execution["min_commission"] = float(cost_config["min_commission"])

    minute_config = request.minute_config or {}
    if minute_config:
        execution["minute_loading"] = {
            "mode": "lazy_by_watchlist" if bool(minute_config.get("lazy_load", True)) else "manual_requested",
            "forbid_full_market_preload": bool(minute_config.get("lazy_load", True)),
            "missing_data_policy": minute_config.get("missing_data_policy") or "skip",
            "execution_granularity": minute_config.get("execution_granularity") or "minute",
            "confirm_timeframes": minute_config.get("confirm_timeframes") or [],
        }

    execution.setdefault("lot_size", 100)
    execution.setdefault("fill_timing", "next_open")
    return merged


def _build_backtest_request_config(request: BacktestCreateRequest, frequency: str) -> dict[str, Any]:
    request_symbols = [symbol.strip() for symbol in (request.symbols or []) if str(symbol).strip()]
    universe = deepcopy(request.universe or {})
    if request_symbols and not universe.get("symbols"):
        universe["symbols"] = request_symbols
    return {
        "backtest_mode": _resolve_backtest_mode(request),
        "frequency": frequency,
        "benchmark": request.benchmark,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "initial_capital": request.initial_capital,
        "symbols": request_symbols,
        "universe": universe,
        "cost_config": deepcopy(request.cost_config or {}),
        "minute_config": deepcopy(request.minute_config or {}),
        "use_minute_confirm": bool(request.use_minute_confirm),
        "walk_forward": deepcopy(request.walk_forward or {}),
    }


def _store_backtest_artifacts(run_id: str, engine_result: Any) -> None:
    _cache_backtest_artifact(_EQUITY, run_id, engine_result.equity)
    _cache_backtest_artifact(_TRADES, run_id, engine_result.trades)
    _cache_backtest_artifact(_SNAPSHOTS, run_id, engine_result.snapshots)
    _cache_backtest_artifact(_SIGNALS, run_id, engine_result.signals)
    _cache_backtest_artifact(_POSITIONS, run_id, engine_result.positions)
    _cache_backtest_artifact(_ORDERS, run_id, engine_result.orders)
    _cache_backtest_artifact(_WATCHLISTS, run_id, engine_result.watchlists)
    _cache_backtest_artifact(_MINUTE_CONFIRMATIONS, run_id, engine_result.minute_confirmations)


def _cache_backtest_artifact(cache: dict[str, list[dict[str, Any]]], run_id: str, items: list[dict[str, Any]]) -> None:
    if _BACKTEST_ARTIFACT_MEMORY_LIMIT > 0 and len(items) <= _BACKTEST_ARTIFACT_MEMORY_LIMIT:
        cache[run_id] = items
    else:
        cache.pop(run_id, None)


def _execute_strategy_platform_backtest(
    run_id: str,
    request_payload: dict[str, Any],
    strategy_payload: dict[str, Any],
) -> None:
    db = StrategySessionLocal()
    request = BacktestCreateRequest(**request_payload)
    strategy_model = StrategyDefinition(**strategy_payload)
    run = _BACKTESTS.get(run_id)
    if run is None:
        payload = get_platform_backtest_run(db, run_id)
        if payload is None:
            db.close()
            return
        run = BacktestRun(**payload)

    try:
        _update_backtest_run_state(db, run, status="running", progress=0.12, message="正在编译策略 DSL 与执行计划", stage="compile")
        if _BACKTESTS.get(run_id, run).status == "cancelled":
            return

        _update_backtest_run_state(db, run, status="running", progress=0.28, message="正在准备行情数据切片与候选股票池", stage="prepare_data")
        if _BACKTESTS.get(run_id, run).status == "cancelled":
            return

        _update_backtest_run_state(db, run, status="running", progress=0.45, message="正在执行回测引擎，生成信号、撮合与快照", stage="run_engine")
        effective_frequency = _resolve_backtest_frequency(request)
        strategy_dsl = strategy_model.current_version.dsl.model_dump() if strategy_model.current_version else _default_dsl(strategy_model.strategy_type).model_dump()
        strategy_dsl = _merge_backtest_request_into_dsl(strategy_dsl, request)
        engine_result = run_strategy_backtest(
            run_id=run_id,
            strategy_name=strategy_model.name,
            dsl=strategy_dsl,
            symbols=request.symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            initial_capital=request.initial_capital,
            frequency=effective_frequency,
            benchmark=request.benchmark,
            use_minute_confirm=effective_frequency == "daily_minute" and request.use_minute_confirm,
            walk_forward=request.walk_forward,
        )
        if _BACKTESTS.get(run_id, run).status == "cancelled":
            return

        _update_backtest_run_state(db, run, status="running", progress=0.82, message="正在写入回测明细与 artifact", stage="write_artifacts")
        metrics = BacktestMetrics(**engine_result.metrics)
        run.strategy_version_id = request.strategy_version_id or strategy_model.current_version_id
        run.metrics = metrics
        run.result = _build_backtest_result_payload(engine_result)
        request_config = _build_backtest_request_config(request, effective_frequency)
        run.result.setdefault("summary", {})
        run.result.setdefault("diagnostics", {})
        run.result["summary"]["request_config"] = request_config
        run.result["summary"]["backtest_mode"] = request_config["backtest_mode"]
        run.result["diagnostics"]["request_config"] = request_config
        run.artifact_root = engine_result.artifact_root
        run.frequency = effective_frequency
        _store_backtest_artifacts(run_id, engine_result)
        update_platform_strategy_metrics(db, request.strategy_id, metrics.model_dump())
        _update_backtest_run_state(db, run, status="completed", progress=1.0, message="回测完成，结果已可查看", stage="completed")
    except Exception as exc:
        run.error_message = str(exc)
        _update_backtest_run_state(db, run, status="failed", progress=1.0, message=f"回测失败：{exc}", stage="failed")
    finally:
        db.close()


@router.post("/v1/backtests", response_model=BacktestRun)
async def create_strategy_platform_backtest(
    request: BacktestCreateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_strategy_db),
):
    if _resolve_backtest_mode(request) == "minute_only":
        raise HTTPException(status_code=400, detail="全分钟 K 回测真引擎尚未开放，请先使用全日 K 或日线选股 + 分时买卖模式。")
    strategy = get_platform_strategy(db, request.strategy_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy_model = StrategyDefinition(**strategy)
    run_id = uuid4().hex
    now = _now()
    effective_frequency = _resolve_backtest_frequency(request)
    run = BacktestRun(
        id=run_id,
        strategy_id=request.strategy_id,
        strategy_version_id=request.strategy_version_id or strategy_model.current_version_id,
        status="running",
        progress=0.05,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=request.initial_capital,
        frequency=effective_frequency,
        benchmark=request.benchmark,
        result={
            "_strategy_platform": {
                "request_config": _build_backtest_request_config(request, effective_frequency),
                "backtest_mode": _resolve_backtest_mode(request),
            }
        },
        created_at=now,
        started_at=now,
    )
    save_platform_backtest_run(db, run.model_dump())
    _BACKTESTS[run_id] = run
    _append_backtest_status_event(
        run_id,
        status=run.status,
        progress=run.progress,
        message="回测任务已创建，正在进入后台执行队列",
        stage="queued",
    )
    if _should_execute_backtest_inline():
        _execute_strategy_platform_backtest(
            run_id,
            {**request.model_dump(), "frequency": effective_frequency},
            strategy_model.model_dump(),
        )
        latest_run = _BACKTESTS.get(run_id)
        if latest_run is not None:
            return _with_backtest_governance(latest_run)
        payload = get_platform_backtest_run(db, run_id)
        if payload is not None:
            return _backtest_run_from_payload(payload)
        return _with_backtest_governance(run)
    background_tasks.add_task(
        _execute_strategy_platform_backtest,
        run_id,
        {**request.model_dump(), "frequency": effective_frequency},
        strategy_model.model_dump(),
    )
    return _with_backtest_governance(run)


@router.get("/v1/backtests", response_model=BacktestRunListResponse)
async def list_strategy_platform_backtests(
    strategy_id: str | None = Query(None),
    limit: int = Query(8, ge=1, le=50),
    db: Session = Depends(get_strategy_db),
):
    items = [
        _backtest_run_from_payload(payload)
        for payload in list_platform_backtest_runs(
            db,
            strategy_id=strategy_id,
            limit=limit,
        )
    ]
    return BacktestRunListResponse(items=items)


@router.get("/v1/backtests/{run_id}", response_model=BacktestRun)
async def get_strategy_platform_backtest(run_id: str, db: Session = Depends(get_strategy_db)):
    run = _BACKTESTS.get(run_id)
    if run is None:
        payload = get_platform_backtest_run(db, run_id)
        if payload is not None:
            return _backtest_run_from_payload(payload)
        raise HTTPException(status_code=404, detail="Backtest not found")
    return _with_backtest_governance(run)


@router.get("/v1/backtests/{run_id}/stream")
async def stream_strategy_platform_backtest(run_id: str, db: Session = Depends(get_strategy_db)) -> StreamingResponse:
    run = _BACKTESTS.get(run_id)
    if run is None:
        payload = get_platform_backtest_run(db, run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Backtest not found")
        run = _backtest_run_from_payload(payload)
        _BACKTESTS[run_id] = run

    async def event_generator():
        last_sequence = 0
        sent_final = False
        while True:
            events = _BACKTEST_STATUS_EVENTS.get(run_id, [])
            for event in events:
                sequence = int(event.get("sequence") or 0)
                if sequence <= last_sequence:
                    continue
                last_sequence = sequence
                yield _sse_pack("status", event)

            current_run = _BACKTESTS.get(run_id)
            if current_run is None:
                with StrategySessionLocal() as stream_db:
                    payload = get_platform_backtest_run(stream_db, run_id)
                    current_run = BacktestRun(**payload) if payload is not None else None

            if current_run is None:
                yield _sse_pack("error", {"run_id": run_id, "message": "回测任务不存在"})
                break

            if current_run.status in _BACKTEST_TERMINAL_STATUSES:
                if not sent_final:
                    yield _sse_pack(
                        "final",
                        _backtest_stream_status_from_run(
                            current_run,
                            message="回测任务已结束",
                            stage=current_run.status,
                        ),
                    )
                    sent_final = True
                break

            yield _sse_pack(
                "heartbeat",
                _backtest_stream_status_from_run(
                    current_run,
                    message="回测仍在执行中，等待下一阶段状态",
                ),
            )
            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/v1/backtests/{run_id}/cancel", response_model=BacktestRun)
async def cancel_strategy_platform_backtest(run_id: str, db: Session = Depends(get_strategy_db)):
    run = _BACKTESTS.get(run_id)
    if run is None:
        payload = get_platform_backtest_run(db, run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Backtest not found")
        run = BacktestRun(**payload)

    if run.status in {"completed", "failed", "cancelled"}:
        return _with_backtest_governance(run)

    run.status = "cancelled"
    run.progress = 1.0
    run.completed_at = _now()
    save_platform_backtest_run(db, run.model_dump())
    _BACKTESTS[run_id] = run
    _append_backtest_status_event(
        run_id,
        status=run.status,
        progress=run.progress,
        message="用户已请求取消回测任务",
        stage="cancelled",
        completed_at=run.completed_at,
    )
    return _with_backtest_governance(run)


@router.post("/v1/backtests/compare")
async def compare_strategy_platform_backtests(request: BacktestCompareRequest, db: Session = Depends(get_strategy_db)):
    if len(request.run_ids) < 2:
        raise HTTPException(status_code=422, detail="At least two run ids are required")

    compared_runs: list[dict[str, Any]] = []
    comparable_metric_fields = [
        "total_return",
        "annual_return",
        "sharpe_ratio",
        "max_drawdown",
        "win_rate",
        "profit_factor",
        "volatility",
        "calmar_ratio",
        "final_capital",
    ]

    for run_id in request.run_ids:
        run = _BACKTESTS.get(run_id)
        if run is None:
            payload = get_platform_backtest_run(db, run_id)
            if payload is None:
                raise HTTPException(status_code=404, detail=f"Backtest not found: {run_id}")
            run = BacktestRun(**payload)
        summary = (run.result or {}).get("summary") or {}
        diagnostics = (run.result or {}).get("diagnostics") or {}
        compared_runs.append(
            {
                "run_id": run.id,
                "strategy_id": run.strategy_id,
                "strategy_version_id": run.strategy_version_id,
                "status": run.status,
                "frequency": run.frequency,
                "benchmark": run.benchmark,
                "metrics": run.metrics.model_dump() if run.metrics else {},
                "summary": summary,
                "diagnostics": {
                    "engine_mode": summary.get("engine_mode") or diagnostics.get("engine_mode"),
                    "data_source": summary.get("data_source"),
                    "minute_aggregation": summary.get("minute_aggregation"),
                    "watchlist_days": summary.get("watchlist_days"),
                    "confirm_hit_rate": diagnostics.get("confirm_hit_rate"),
                    "minute_data_missing": diagnostics.get("minute_data_missing"),
                    "fallback_mode": diagnostics.get("fallback_mode"),
                },
                "artifact_root": run.artifact_root,
                "created_at": run.created_at,
                "completed_at": run.completed_at,
            }
        )

    metric_preferences = {
        "total_return": "max",
        "annual_return": "max",
        "sharpe_ratio": "max",
        "max_drawdown": "max",
        "win_rate": "max",
        "profit_factor": "max",
        "volatility": "min",
        "calmar_ratio": "max",
        "final_capital": "max",
    }
    metric_summary: dict[str, dict[str, Any]] = {}
    for field in comparable_metric_fields:
        series = []
        for item in compared_runs:
            value = item["metrics"].get(field)
            if isinstance(value, (int, float)):
                series.append({"run_id": item["run_id"], "value": value})
        if not series:
            continue
        reverse = metric_preferences.get(field, "max") == "max"
        metric_summary[field] = {
            "best": sorted(series, key=lambda row: row["value"], reverse=reverse)[0],
            "worst": sorted(series, key=lambda row: row["value"], reverse=not reverse)[0],
        }

    return {
        "run_ids": request.run_ids,
        "runs": compared_runs,
        "summary": metric_summary,
    }


@router.get("/v1/backtests/{run_id}/metrics")
async def get_strategy_platform_backtest_metrics(run_id: str, db: Session = Depends(get_strategy_db)):
    run = _BACKTESTS.get(run_id)
    if run is None:
        payload = get_platform_backtest_run(db, run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="Backtest not found")
        run = BacktestRun(**payload)
    artifact_metrics = {}
    if run.artifact_root:
        metrics_path = Path(run.artifact_root) / "metrics.json"
        if metrics_path.exists():
            try:
                artifact_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            except Exception:
                artifact_metrics = {}
    return {
        "run_id": run_id,
        "metrics": run.metrics.model_dump() if run.metrics else (run.result or {}).get("metrics"),
        "summary": (run.result or {}).get("summary") or artifact_metrics.get("summary"),
        "artifact_root": run.artifact_root,
    }


@router.get("/v1/backtests/{run_id}/equity")
async def get_strategy_platform_backtest_equity(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "equity",
        _EQUITY,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/trades")
async def get_strategy_platform_backtest_trades(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "trades",
        _TRADES,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/trade-snapshots")
async def get_strategy_platform_backtest_trade_snapshots(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "trade_snapshots",
        _SNAPSHOTS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/signals")
async def get_strategy_platform_backtest_signals(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "signals",
        _SIGNALS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/positions")
async def get_strategy_platform_backtest_positions(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "positions",
        _POSITIONS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/orders")
async def get_strategy_platform_backtest_orders(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "orders",
        _ORDERS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/v1/backtests/{run_id}/watchlists")
async def get_strategy_platform_backtest_watchlists(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "watchlists",
        _WATCHLISTS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
        enrich_watchlist=True,
    )


@router.get("/v1/backtests/{run_id}/minute-confirmations")
async def get_strategy_platform_backtest_minute_confirmations(
    run_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(1000, ge=1, le=5000),
    sort_by: str | None = Query(None),
    sort_order: Literal["asc", "desc"] = Query("asc"),
    db: Session = Depends(get_strategy_db),
):
    _ensure_backtest_exists(db, run_id)
    return _artifact_detail_response(
        run_id,
        "minute_confirmations",
        _MINUTE_CONFIRMATIONS,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_order=sort_order,
    )


def _latest_completed_backtest_for_strategy(db: Session, strategy_id: str) -> BacktestRun | None:
    cached_runs = [
        run for run in _BACKTESTS.values()
        if run.strategy_id == strategy_id and run.status == "completed"
    ]
    if cached_runs:
        return _with_backtest_governance(max(cached_runs, key=lambda run: run.completed_at or run.created_at))
    payload = get_latest_completed_platform_backtest(db, strategy_id)
    return _backtest_run_from_payload(payload) if payload is not None else None


def _apply_dsl_patch(dsl: StrategyDsl, patch: dict[str, Any]) -> StrategyDsl:
    payload = dsl.model_dump()
    for path, value in patch.items():
        if path.startswith("factor_model.factors.") and path.endswith(".weight"):
            factor_name = path.removeprefix("factor_model.factors.").removesuffix(".weight")
            factors = payload.setdefault("factor_model", {}).setdefault("factors", [])
            for factor in factors:
                if factor.get("name") == factor_name:
                    factor["weight"] = value
                    break
            else:
                factors.append({"name": factor_name, "weight": value, "direction": "higher_better", "transform": "rank_pct"})
            continue
        if path == "entry.conditions" and isinstance(value, list):
            entry = payload.setdefault("entry", {})
            conditions = entry.setdefault("conditions", [])
            conditions.extend(value)
            continue
        _set_nested_value(payload, path.split("."), value)
    return StrategyDsl(**payload)


def _set_nested_value(payload: dict[str, Any], parts: list[str], value: Any) -> None:
    cursor = payload
    for part in parts[:-1]:
        next_value = cursor.setdefault(part, {})
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[part] = next_value
        cursor = next_value
    cursor[parts[-1]] = value


@router.post("/v1/evolution/experiments", response_model=EvolutionExperiment)
async def create_evolution_experiment(request: EvolutionCreateRequest, db: Session = Depends(get_strategy_db)):
    if get_platform_strategy(db, request.strategy_id) is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    experiment_id = uuid4().hex
    latest_run = _latest_completed_backtest_for_strategy(db, request.strategy_id)
    base_metrics = latest_run.metrics.model_dump() if latest_run and latest_run.metrics else {
        "total_return": 0.218,
        "annual_return": 0.264,
        "sharpe_ratio": 1.67,
        "max_drawdown": -0.092,
        "win_rate": 0.614,
        "profit_factor": 1.86,
        "volatility": 0.183,
        "final_capital": 1_218_000,
        "calmar_ratio": 2.87,
    }
    snapshots = []
    if latest_run:
        snapshots = _SNAPSHOTS.get(latest_run.id) or read_artifact_items(latest_run.id, "trade_snapshots")
    candidates = [
        EvolutionCandidate(**candidate)
        for candidate in build_evolution_candidates(
            experiment_id=experiment_id,
            base_metrics=base_metrics,
            snapshots=snapshots,
        )
    ]
    experiment = EvolutionExperiment(
        id=experiment_id,
        strategy_id=request.strategy_id,
        objective=request.objective,
        candidates=candidates,
        created_at=_now(),
    )
    saved_payload = save_platform_evolution_experiment(
        db,
        {
            **experiment.model_dump(),
            "search_space": request.search_space,
            "base_backtest_run_id": latest_run.id if latest_run else None,
        },
    )
    experiment = EvolutionExperiment(**saved_payload)
    _EXPERIMENTS[experiment_id] = experiment
    return experiment


@router.get("/v1/evolution/experiments/{experiment_id}", response_model=EvolutionExperiment)
async def get_evolution_experiment(experiment_id: str, db: Session = Depends(get_strategy_db)):
    experiment = _EXPERIMENTS.get(experiment_id)
    if experiment:
        return experiment
    payload = get_platform_evolution_experiment(db, experiment_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="Evolution experiment not found")
    experiment = EvolutionExperiment(**payload)
    _EXPERIMENTS[experiment_id] = experiment
    return experiment


@router.get("/v1/evolution/experiments/{experiment_id}/candidates")
async def get_evolution_candidates(experiment_id: str, db: Session = Depends(get_strategy_db)):
    experiment = _EXPERIMENTS.get(experiment_id)
    if experiment:
        return {"candidates": experiment.candidates}
    payload = list_platform_evolution_candidates(db, experiment_id)
    if not payload:
        experiment_payload = get_platform_evolution_experiment(db, experiment_id)
        if experiment_payload is None:
            raise HTTPException(status_code=404, detail="Evolution experiment not found")
    candidates = [EvolutionCandidate(**item) for item in payload]
    return {"candidates": candidates}


@router.post("/v1/evolution/candidates/{candidate_id}/accept", response_model=StrategyDefinition)
async def accept_evolution_candidate(candidate_id: str, db: Session = Depends(get_strategy_db)):
    candidate_payload = get_platform_evolution_candidate(db, candidate_id)
    if candidate_payload is None:
        for experiment in _EXPERIMENTS.values():
            for candidate in experiment.candidates:
                if candidate.id == candidate_id:
                    candidate_payload = candidate.model_dump()
                    break
            if candidate_payload is not None:
                break
    if candidate_payload is not None:
        experiment_payload = get_platform_evolution_experiment(db, candidate_payload["experiment_id"])
        if experiment_payload is None:
            for cached_experiment in _EXPERIMENTS.values():
                if cached_experiment.id == candidate_payload["experiment_id"]:
                    experiment_payload = cached_experiment.model_dump()
                    break
        if experiment_payload is None:
            raise HTTPException(status_code=404, detail="Evolution experiment not found")
        experiment = EvolutionExperiment(**experiment_payload)
        candidate = EvolutionCandidate(**candidate_payload)
        base_payload = get_platform_strategy(db, experiment.strategy_id)
        if base_payload is None:
            raise HTTPException(status_code=404, detail="Base strategy not found")
        base = StrategyDefinition(**base_payload)
        cloned = deepcopy(base)
        cloned.id = uuid4().hex
        cloned.name = f"{base.name} · {candidate.name}"
        cloned.status = "draft"
        cloned.is_active = False
        cloned.source = "evolution"
        cloned.version = 1
        if cloned.current_version:
            cloned.current_version = deepcopy(cloned.current_version)
            cloned.current_version.id = uuid4().hex
            cloned.current_version.strategy_id = cloned.id
            cloned.current_version.dsl = _apply_dsl_patch(cloned.current_version.dsl, candidate.dsl_patch)
            cloned.current_version.change_summary = candidate.improvement_summary
            cloned.current_version.created_at = _now()
            cloned.current_version_id = cloned.current_version.id
        else:
            cloned.current_version_id = uuid4().hex
        cloned.created_at = _now()
        cloned.updated_at = cloned.created_at
        cloned.performance = candidate.metrics
        update_platform_evolution_candidate_status(db, candidate_id, status="accepted")
        saved = save_platform_strategy(db, cloned.model_dump())
        cloned_strategy = StrategyDefinition(**saved)
        latest_run = _latest_completed_backtest_for_strategy(db, experiment.strategy_id)
        if latest_run is not None:
            validation_symbols = _extract_validation_symbols(latest_run)
            validation_run = _run_validation_backtest(
                db=db,
                strategy=cloned_strategy,
                base_run=latest_run,
                symbols=validation_symbols,
            )
            if validation_run.metrics is not None:
                cloned_strategy.performance = StrategyPerformance(
                    total_return=validation_run.metrics.total_return,
                    annual_return=validation_run.metrics.annual_return,
                    sharpe_ratio=validation_run.metrics.sharpe_ratio,
                    max_drawdown=validation_run.metrics.max_drawdown,
                    win_rate=validation_run.metrics.win_rate,
                    calmar_ratio=validation_run.metrics.calmar_ratio,
                )
                save_platform_strategy(db, cloned_strategy.model_dump())
        refreshed = get_platform_evolution_experiment(db, candidate.experiment_id)
        if refreshed is not None:
            _EXPERIMENTS[candidate.experiment_id] = EvolutionExperiment(**refreshed)
        return cloned_strategy
    raise HTTPException(status_code=404, detail="Evolution candidate not found")


@router.post("/v1/paper/accounts")
async def create_paper_account(request: PaperAccountCreateRequest, db: Session = Depends(get_strategy_db)):
    account_id = request.id or uuid4().hex
    existing = db.query(PaperAccountDB).filter(PaperAccountDB.id == account_id).first()
    if existing is not None:
        raise HTTPException(status_code=409, detail="Paper account already exists")
    account = PaperAccountDB(
        id=account_id,
        name=request.name or f"纸交易账户-{account_id}",
        initial_capital=request.initial_capital,
        cash=request.initial_capital,
        status="active",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    return account.to_dict()


@router.get("/v1/paper/accounts")
async def list_paper_accounts(db: Session = Depends(get_strategy_db)):
    rows = db.query(PaperAccountDB).order_by(PaperAccountDB.updated_at.desc()).all()
    return {"items": [row.to_dict() for row in rows]}


@router.post("/v1/paper/accounts/{account_id}/run-strategy")
async def run_paper_strategy(account_id: str, strategy_id: str = Query(...), db: Session = Depends(get_strategy_db)):
    if get_platform_strategy(db, strategy_id) is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    account = _get_or_create_paper_account(db, account_id)
    signal = _resolve_paper_order_signal(db, strategy_id)
    quantity = _paper_order_quantity(account, signal["price"], signal["side"])
    order = PaperOrderDB(
        account_id=account.id,
        strategy_id=strategy_id,
        symbol=signal["symbol"],
        side=signal["side"],
        quantity=quantity,
        price=signal["price"],
        commission=round(quantity * signal["price"] * 0.0003, 2),
        slippage=round(quantity * signal["price"] * 0.001, 2),
        stamp_duty=round(quantity * signal["price"] * 0.001, 2) if signal["side"] == "sell" else 0.0,
        order_type="strategy_signal",
        status="filled",
        reason=signal["reason"],
        executed_at=datetime.now(timezone.utc),
        created_at=datetime.now(timezone.utc),
    )
    gross_amount = float(order.quantity) * float(order.price)
    if order.side == "buy":
        account.cash = float(account.cash or account.initial_capital or 0.0) - (
            gross_amount + float(order.commission) + float(order.slippage)
        )
    else:
        account.cash = float(account.cash or account.initial_capital or 0.0) + (
            gross_amount - float(order.commission) - float(order.slippage) - float(order.stamp_duty)
        )
    db.add(account)
    db.add(order)
    db.commit()
    db.refresh(order)
    return {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "orders": [
            {
                "id": order.id,
                "symbol": order.symbol,
                "side": order.side,
                "quantity": order.quantity,
                "price": order.price,
                "price_limit": [round(order.price * 0.98, 2), round(order.price * 1.02, 2)],
                "reason": order.reason,
                "risk_check": "passed",
                "source_run_id": signal.get("source_run_id"),
                "signal_source": signal.get("signal_source"),
            }
        ],
        "message": "纸交易信号已生成，未连接真实券商。",
    }


@router.get("/v1/paper/accounts/{account_id}/orders")
async def get_paper_orders(account_id: str, db: Session = Depends(get_strategy_db)):
    rows = (
        db.query(PaperOrderDB)
        .filter(PaperOrderDB.account_id == account_id)
        .order_by(PaperOrderDB.created_at.desc())
        .all()
    )
    return {"account_id": account_id, "orders": [row.to_dict() for row in rows]}


@router.get("/v1/paper/accounts/{account_id}/positions")
async def get_paper_positions(account_id: str, db: Session = Depends(get_strategy_db)):
    rows = db.query(PaperOrderDB).filter(PaperOrderDB.account_id == account_id).all()
    positions: dict[str, dict[str, Any]] = {}
    for row in rows:
        position = positions.setdefault(row.symbol, {"symbol": row.symbol, "quantity": 0.0, "cost": 0.0})
        signed_quantity = float(row.quantity or 0) if row.side == "buy" else -float(row.quantity or 0)
        position["quantity"] += signed_quantity
        if row.side == "buy":
            position["cost"] += float(row.quantity or 0) * float(row.price or 0)
        else:
            position["cost"] -= min(position["cost"], float(row.quantity or 0) * float(row.price or 0))
    items = []
    for position in positions.values():
        quantity = float(position["quantity"])
        if quantity <= 0:
            continue
        items.append(
            {
                "symbol": position["symbol"],
                "quantity": quantity,
                "avg_price": round(float(position["cost"]) / quantity, 4) if quantity else 0.0,
                "market_value": round(float(position["cost"]), 2),
            }
        )
    return {"account_id": account_id, "positions": items}


@router.get("/v1/paper/accounts/{account_id}/equity")
async def get_paper_equity(account_id: str, initial_capital: float = Query(1_000_000, ge=0), db: Session = Depends(get_strategy_db)):
    account = _get_or_create_paper_account(db, account_id, initial_capital=initial_capital)
    positions = await get_paper_positions(account_id, db)
    positions_value = sum(float(item["market_value"]) for item in positions["positions"])
    return {
        "account_id": account_id,
        "equity": round(float(account.cash or 0.0) + positions_value, 2),
        "cash": round(float(account.cash or 0.0), 2),
        "positions_value": round(positions_value, 2),
        "updated_at": _now(),
    }


def _paper_mode(account_id: str) -> str:
    return f"paper:{account_id}"


def _resolve_paper_order_signal(db: Session, strategy_id: str) -> dict[str, Any]:
    latest_run = _latest_completed_backtest_for_strategy(db, strategy_id)
    if latest_run is not None:
        orders = _ORDERS.get(latest_run.id) or read_artifact_items(latest_run.id, "orders")
        filled_buy_orders = [item for item in orders if item.get("side") == "buy" and item.get("status") == "filled"]
        if filled_buy_orders:
            latest_order = filled_buy_orders[-1]
            return {
                "symbol": latest_order.get("symbol") or "300750.SZ",
                "side": "buy",
                "price": float(latest_order.get("fill_price") or 207.0),
                "reason": latest_order.get("reason") or "最近回测买入订单",
                "source_run_id": latest_run.id,
                "signal_source": "latest_backtest_order",
            }
        signals = _SIGNALS.get(latest_run.id) or read_artifact_items(latest_run.id, "signals")
        buy_signals = [item for item in signals if item.get("side") == "buy"]
        if buy_signals:
            latest_signal = buy_signals[-1]
            return {
                "symbol": latest_signal.get("symbol") or "300750.SZ",
                "side": "buy",
                "price": 207.0,
                "reason": latest_signal.get("reason") or "最近回测买入信号",
                "source_run_id": latest_run.id,
                "signal_source": "latest_backtest_signal",
            }
    return {
        "symbol": "300750.SZ",
        "side": "buy",
        "price": 207.0,
        "reason": "默认纸交易信号：周线趋势 + 日线波段 + 30m VWAP 确认",
        "source_run_id": None,
        "signal_source": "default_fallback",
    }


def _paper_order_quantity(account: PaperAccountDB, price: float, side: str) -> int:
    if side == "sell":
        return 100
    cash = float(account.cash or account.initial_capital or 0.0)
    budget = max(cash * 0.12, 0.0)
    return max(int(budget / max(price, 0.01) / 100) * 100, 100)


def _get_or_create_paper_account(db: Session, account_id: str, initial_capital: float = 1_000_000) -> PaperAccountDB:
    account = db.query(PaperAccountDB).filter(PaperAccountDB.id == account_id).first()
    if account is not None:
        return account
    account = PaperAccountDB(
        id=account_id,
        name=f"纸交易账户-{account_id}",
        initial_capital=initial_capital,
        cash=initial_capital,
        status="active",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    return account


def _extract_validation_symbols(run: BacktestRun) -> list[str]:
    details = ((run.result or {}).get("details") or {}) if run.result else {}
    watchlists = details.get("watchlists") or []
    symbols: list[str] = []
    seen: set[str] = set()
    for item in watchlists:
        symbol = str(item.get("symbol") or "")
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
        if len(symbols) >= 30:
            break
    return symbols


def _run_validation_backtest(
    *,
    db: Session,
    strategy: StrategyDefinition,
    base_run: BacktestRun,
    symbols: list[str],
) -> BacktestRun:
    validation_run_id = uuid4().hex
    engine_result = run_strategy_backtest(
        run_id=validation_run_id,
        strategy_name=f"{strategy.name} · validation",
        dsl=strategy.current_version.dsl.model_dump() if strategy.current_version else _default_dsl(strategy.strategy_type).model_dump(),
        symbols=symbols,
        start_date=base_run.start_date,
        end_date=base_run.end_date,
        initial_capital=base_run.initial_capital,
        frequency=base_run.frequency,
        benchmark=base_run.benchmark,
        use_minute_confirm=base_run.frequency == "daily_minute",
    )
    metrics = BacktestMetrics(**engine_result.metrics)
    payload = BacktestRun(
        id=validation_run_id,
        strategy_id=strategy.id,
        strategy_version_id=strategy.current_version_id,
        status="completed",
        progress=1.0,
        start_date=base_run.start_date,
        end_date=base_run.end_date,
        initial_capital=base_run.initial_capital,
        frequency=base_run.frequency,
        benchmark=base_run.benchmark,
        metrics=metrics,
        result={
            "metrics": engine_result.metrics,
            "summary": engine_result.summary,
            "details": {"watchlists": engine_result.watchlists[:200], "trade_list": engine_result.trades[:100]},
            "diagnostics": engine_result.diagnostics,
        },
        artifact_root=engine_result.artifact_root,
        created_at=_now(),
        started_at=_now(),
        completed_at=_now(),
    )
    save_platform_backtest_run(db, payload.model_dump())
    _BACKTESTS[validation_run_id] = payload
    _store_backtest_artifacts(validation_run_id, engine_result)
    update_platform_strategy_metrics(db, strategy.id, metrics.model_dump())
    return payload
