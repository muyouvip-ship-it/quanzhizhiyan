from __future__ import annotations

import json
import logging
import math
import os
import re
import hashlib
import asyncio
import threading
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Iterable
from uuid import uuid4

import pandas as pd
import requests
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from api.core.stock_map import get_reverse_stock_map
from api.database import SessionLocal
from api.services import news_eye_service, news_theme_service
from api.services.daily_review_market_behavior import interpret_market_behavior
from api.services.market_data_pipeline_service import preferred_daily_kline_table, preferred_minute_kline_table
from api.services.qmt_market_data_service import capture_intraday_symbols
from tradingagents.dataflows.trade_calendar import is_cn_trading_day, now_cn, previous_cn_trading_day
from api.core.utils import run_async


SUPPORTED_WINDOWS = {"premarket", "24h", "72h", "7d"}
DEFAULT_SELECTION_LIMIT = 10
MAX_SELECTION_LIMIT = 30
CN_TZ_NAME = "Asia/Shanghai"
SCORE_VERSION = "ai-quant-closed-loop-v4"
MIN_THEME_SCORE = 8.0
MIN_MAINLINE_ALIGNMENT_SCORE = 24.0
FEEDBACK_MODEL_VERSION = "settlement-feedback-v1"
REALTIME_FEEDBACK_MODEL_VERSION = "settlement-realtime-feedback-v2"
RISK_EXECUTION_GATES = ("allow", "allow_probe", "confirm", "blocked", "reduce_only")
INTRADAY_PULSE_PROFILE_KEYS = ("confirming", "mixed", "weak", "risk_off")
PROTECTIVE_RISK_GATES = {"blocked", "reduce_only"}
PERMISSIVE_RISK_GATES = {"allow", "allow_probe", "confirm"}
POSITIVE_SETTLEMENT_OUTCOMES = {"hit", "strong_hit"}
NEGATIVE_SETTLEMENT_OUTCOMES = {"miss", "weak_miss"}
REALTIME_FEEDBACK_EVENT_TYPES = {
    "minute_features",
    "no_signal",
    "signal_generated",
    "signal_blocked",
    "order_submitted",
    "order_rejected",
    "order_error",
    "approval_created",
    "trade_confirmed",
    "position_changed",
}
REALTIME_SYMBOL_FEEDBACK_EVENT_TYPES = {
    "signal_generated",
    "order_submitted",
    "trade_confirmed",
    "position_changed",
}
CLOSED_LOOP_REQUIREMENTS: tuple[tuple[str, str], ...] = (
    ("opportunity_discovery", "主动发现机会"),
    ("event_understanding", "理解新闻/政策事件"),
    ("market_state", "判断市场状态"),
    ("dynamic_ranking", "动态排序标的"),
    ("risk_control", "控制风险"),
    ("feedback_learning", "结果反哺模型"),
)
MAX_EVENT_REACTION_CAPTURE_SYMBOLS = 30
DEFAULT_EVENT_MINUTE_CAPTURE_TIMEOUT_SECONDS = 4.0
DEFAULT_EVENT_MINUTE_HISTORY_BACKFILL_TIMEOUT_SECONDS = 2.5
DEFAULT_EVENT_MINUTE_AKSHARE_BACKFILL_SYMBOLS = 5
DEFAULT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS = 180.0
DEFAULT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS = 3
FEEDBACK_RECENCY_HALF_LIFE_DAYS = 30.0
FEEDBACK_RECENCY_MIN_WEIGHT = 0.15
PREMARKET_NEWS_CUTOFF = time(hour=9, minute=25)
REALTIME_SELECTION_CACHE_TTL_SECONDS = 60
CATALYST_AUTO_MONITOR_ENABLED = "AI_QUANT_CATALYST_AUTO_MONITOR"
CATALYST_AUTO_MONITOR_START = "AI_QUANT_CATALYST_AUTO_MONITOR_START"
CATALYST_AUTO_MONITOR_ACCOUNT_KEY = "AI_QUANT_CATALYST_AUTO_MONITOR_ACCOUNT_KEY"
LLM_RUNTIME_FINGERPRINT_KEYS = (
    "enabled",
    "ready",
    "status",
    "provider",
    "model",
    "base_url",
    "source",
    "runtime_package_source",
    "api_key_source",
    "provider_source",
    "base_url_source",
    "model_source",
    "requires_api_key",
    "has_api_key",
)
logger = logging.getLogger(__name__)

_EVENT_AKSHARE_BACKFILL_LOCK = threading.RLock()
_EVENT_AKSHARE_BACKFILL_JOBS: dict[str, dict[str, Any]] = {}
_EVENT_HISTORY_BRIDGE_LOCK = threading.RLock()
_EVENT_HISTORY_BRIDGE_FAILURES: dict[str, dict[str, Any]] = {}
_CATALYST_SCHEMA_LOCK = threading.RLock()
_CATALYST_SCHEMA_ENSURED_BINDS: set[str] = set()
_SELECTION_REFRESH_LOCK = threading.RLock()
_SELECTION_REFRESH_TASKS: set[str] = set()
_EVENT_DRIVEN_REFRESH_LOCK = threading.RLock()
_EVENT_DRIVEN_REFRESH_TASKS: set[str] = set()
_EVENT_DRIVEN_REFRESH_PENDING: dict[str, dict[str, Any]] = {}

THEME_INDUSTRY_ALIASES: dict[str, tuple[str, ...]] = {
    "半导体": ("半导体", "芯片", "封测", "集成电路", "电子", "先进封装", "存储", "HBM", "晶圆", "EDA"),
    "机器人": ("机器人", "自动化", "机械", "执行器", "减速器", "电机", "热管理", "汽车零部件"),
    "算力": ("算力", "服务器", "数据中心", "通信", "光模块", "CPO", "液冷", "IDC", "计算机", "AI"),
    "人工智能": ("人工智能", "AI", "软件", "算法", "算力", "智能", "计算机", "传媒"),
    "消费电子": ("消费电子", "苹果", "手机", "光学", "元件", "电子"),
    "汽车": ("汽车", "整车", "零部件", "智能驾驶", "电驱", "热管理", "电池"),
    "新能源": ("新能源", "锂电", "电池", "储能", "光伏", "风电", "电力设备"),
    "低空经济": ("低空", "无人机", "航空", "飞行汽车", "通航"),
    "有色金属": ("有色", "铜", "铝", "锂", "钴", "稀土", "黄金"),
    "军工": ("军工", "航空", "航天", "卫星", "国防"),
    "医药": ("医药", "医疗", "创新药", "器械", "CXO"),
    "金融": ("银行", "证券", "保险", "金融", "券商"),
}

HIGH_CERTAINTY_TOKENS = (
    "国务院",
    "工信部",
    "发改委",
    "人民日报",
    "新华社",
    "上市公司公告",
    "公告",
    "签约",
    "中标",
    "获批",
    "量产",
    "订单",
    "增长",
    "超预期",
)
CONSUMABLE_TOKENS = ("传闻", "网传", "小作文", "据传", "可能", "预期", "传出")
RISK_TOKENS = ("澄清", "减持", "监管", "处罚", "调查", "不及预期", "亏损", "退市", "风险")
BASE_SCORE_WEIGHTS: dict[str, float] = {
    "catalyst": 0.30,
    "theme": 0.20,
    "relation": 0.18,
    "market_confirm": 0.12,
    "event_intelligence": 0.08,
    "adaptive_feedback": 0.08,
    "momentum": 0.07,
    "fundamental": 0.05,
    "continuity": 0.03,
}


def ensure_catalyst_selection_tables(db: Session) -> None:
    bind_key = _schema_bind_key(db)
    if bind_key in _CATALYST_SCHEMA_ENSURED_BINDS:
        return
    with _CATALYST_SCHEMA_LOCK:
        if bind_key in _CATALYST_SCHEMA_ENSURED_BINDS:
            return
        _ensure_catalyst_selection_tables_uncached(db)
        _CATALYST_SCHEMA_ENSURED_BINDS.add(bind_key)


def _schema_bind_key(db: Session) -> str:
    try:
        bind = db.get_bind()
    except Exception:
        return f"session:{id(db)}"
    bind_url = getattr(bind, "url", None)
    if bind_url is not None:
        try:
            return bind_url.render_as_string(hide_password=False)
        except Exception:
            return str(bind_url)
    return f"bind:{id(bind)}"


def _ensure_catalyst_selection_tables_uncached(db: Session) -> None:
    news_eye_service.ensure_news_tables(db)
    news_theme_service.ensure_theme_tables(db)
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_runs (
                run_id VARCHAR(36) PRIMARY KEY,
                trade_date VARCHAR(10) NOT NULL,
                window_label VARCHAR(20) NOT NULL,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                score_version VARCHAR(40) NOT NULL,
                market_background TEXT,
                market_behavior_json TEXT DEFAULT '{}',
                data_governance_json TEXT DEFAULT '{}',
                item_count INTEGER NOT NULL DEFAULT 0,
                source VARCHAR(120) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                UNIQUE (trade_date, window_label)
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_selection_runs_date ON catalyst_selection_runs (trade_date DESC, window_label)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_items (
                run_id VARCHAR(36) NOT NULL,
                trade_date VARCHAR(10) NOT NULL,
                window_label VARCHAR(20) NOT NULL,
                rank INTEGER NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(80),
                industry VARCHAR(160),
                sector VARCHAR(160),
                concepts_json TEXT DEFAULT '[]',
                score FLOAT NOT NULL,
                catalyst_score FLOAT NOT NULL DEFAULT 0,
                theme_score FLOAT NOT NULL DEFAULT 0,
                relation_score FLOAT NOT NULL DEFAULT 0,
                market_confirm_score FLOAT NOT NULL DEFAULT 0,
                event_intelligence_score FLOAT NOT NULL DEFAULT 0,
                momentum_score FLOAT NOT NULL DEFAULT 0,
                fundamental_score FLOAT NOT NULL DEFAULT 0,
                continuity_score FLOAT NOT NULL DEFAULT 0,
                adaptive_feedback_score FLOAT NOT NULL DEFAULT 50,
                risk_penalty FLOAT NOT NULL DEFAULT 0,
                risk_flags_json TEXT DEFAULT '[]',
                reason_parts_json TEXT DEFAULT '[]',
                theme_matches_json TEXT DEFAULT '[]',
                signal_flags_json TEXT DEFAULT '[]',
                metric_snapshot_json TEXT DEFAULT '{}',
                risk_control_json TEXT DEFAULT '{}',
                closed_loop_trace_json TEXT DEFAULT '{}',
                market_background TEXT,
                market_behavior_json TEXT DEFAULT '{}',
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (run_id, symbol)
            )
            """
        )
    )
    db.execute(text("ALTER TABLE catalyst_selection_items ADD COLUMN IF NOT EXISTS event_intelligence_score FLOAT NOT NULL DEFAULT 0"))
    db.execute(text("ALTER TABLE catalyst_selection_items ADD COLUMN IF NOT EXISTS adaptive_feedback_score FLOAT NOT NULL DEFAULT 50"))
    db.execute(text("ALTER TABLE catalyst_selection_items ADD COLUMN IF NOT EXISTS risk_control_json TEXT DEFAULT '{}'"))
    db.execute(text("ALTER TABLE catalyst_selection_items ADD COLUMN IF NOT EXISTS closed_loop_trace_json TEXT DEFAULT '{}'"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_selection_items_date ON catalyst_selection_items (trade_date DESC, rank)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_selection_items_symbol ON catalyst_selection_items (symbol, trade_date DESC)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_opportunity_events (
                event_id VARCHAR(64) PRIMARY KEY,
                run_id VARCHAR(36) NOT NULL,
                trade_date VARCHAR(10) NOT NULL,
                window_label VARCHAR(20) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(80),
                rank INTEGER NOT NULL,
                score FLOAT NOT NULL,
                previous_rank INTEGER,
                previous_score FLOAT,
                rank_delta INTEGER,
                score_delta FLOAT,
                event_level VARCHAR(20) NOT NULL,
                event_types_json TEXT DEFAULT '[]',
                reasons_json TEXT DEFAULT '[]',
                risk_action VARCHAR(40),
                risk_level VARCHAR(40),
                trace_json TEXT DEFAULT '{}',
                created_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_opportunity_events_date ON catalyst_selection_opportunity_events (trade_date DESC, window_label, event_level)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_opportunity_events_symbol ON catalyst_selection_opportunity_events (symbol, trade_date DESC)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_settlements (
                trade_date VARCHAR(10) NOT NULL,
                settlement_date VARCHAR(10) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(80),
                rank INTEGER NOT NULL,
                entry_price FLOAT,
                close_price FLOAT,
                next_open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                change_pct FLOAT,
                max_up_pct FLOAT,
                max_down_pct FLOAT,
                hit_score FLOAT,
                outcome VARCHAR(40) NOT NULL,
                protected BOOLEAN NOT NULL DEFAULT FALSE,
                settlement_notes_json TEXT DEFAULT '[]',
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (trade_date, settlement_date, symbol)
            )
            """
        )
    )
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_closed_loop_audits (
                audit_id VARCHAR(36) PRIMARY KEY,
                trade_date VARCHAR(10),
                trigger_name VARCHAR(120) NOT NULL,
                status VARCHAR(24) NOT NULL,
                audit_json TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_closed_loop_audits_created_at ON catalyst_selection_closed_loop_audits (created_at DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_closed_loop_audits_trade_date ON catalyst_selection_closed_loop_audits (trade_date DESC, created_at DESC)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_event_refresh_runs (
                refresh_key VARCHAR(256) PRIMARY KEY,
                trigger_name VARCHAR(120) NOT NULL,
                user_id VARCHAR(64),
                trade_date VARCHAR(10),
                windows_json TEXT NOT NULL DEFAULT '[]',
                limit_value INTEGER,
                reason TEXT,
                context_json TEXT NOT NULL DEFAULT '{}',
                status VARCHAR(32) NOT NULL,
                deduped BOOLEAN NOT NULL DEFAULT FALSE,
                generated_json TEXT NOT NULL DEFAULT '[]',
                errors_json TEXT NOT NULL DEFAULT '[]',
                skipped BOOLEAN NOT NULL DEFAULT FALSE,
                skip_reason TEXT,
                audit_id VARCHAR(36),
                duration_ms INTEGER,
                scheduled_at TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_event_refresh_runs_user_updated ON catalyst_selection_event_refresh_runs (user_id, updated_at DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_event_refresh_runs_trigger_updated ON catalyst_selection_event_refresh_runs (trigger_name, updated_at DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_event_refresh_runs_status_updated ON catalyst_selection_event_refresh_runs (status, updated_at DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_selection_settlements_symbol ON catalyst_selection_settlements (symbol, trade_date DESC)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_realtime_feedback (
                feedback_id VARCHAR(64) PRIMARY KEY,
                source_event_id VARCHAR(64) UNIQUE NOT NULL,
                monitor_id VARCHAR(36) NOT NULL,
                strategy_id VARCHAR(36),
                user_id VARCHAR(64),
                account_key VARCHAR(64),
                trade_date VARCHAR(10) NOT NULL,
                event_time TIMESTAMP,
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(80),
                event_type VARCHAR(64) NOT NULL,
                signal_side VARCHAR(16),
                signal_source VARCHAR(80),
                feedback_kind VARCHAR(40) NOT NULL,
                outcome VARCHAR(40) NOT NULL,
                hit_score FLOAT,
                change_pct FLOAT,
                risk_gate VARCHAR(40),
                risk_favorable BOOLEAN,
                symbol_feedback BOOLEAN NOT NULL DEFAULT FALSE,
                risk_feedback BOOLEAN NOT NULL DEFAULT TRUE,
                themes_json TEXT DEFAULT '[]',
                event_types_json TEXT DEFAULT '[]',
                theme_matches_json TEXT DEFAULT '[]',
                candidate_snapshot_json TEXT DEFAULT '{}',
                raw_event_json TEXT DEFAULT '{}',
                source VARCHAR(80) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("ALTER TABLE catalyst_selection_realtime_feedback ADD COLUMN IF NOT EXISTS signal_side VARCHAR(16)"))
    db.execute(text("ALTER TABLE catalyst_selection_realtime_feedback ADD COLUMN IF NOT EXISTS signal_source VARCHAR(80)"))
    db.execute(text("ALTER TABLE catalyst_selection_realtime_feedback ADD COLUMN IF NOT EXISTS risk_favorable BOOLEAN"))
    db.execute(text("ALTER TABLE catalyst_selection_realtime_feedback ADD COLUMN IF NOT EXISTS symbol_feedback BOOLEAN NOT NULL DEFAULT FALSE"))
    db.execute(text("ALTER TABLE catalyst_selection_realtime_feedback ADD COLUMN IF NOT EXISTS risk_feedback BOOLEAN NOT NULL DEFAULT TRUE"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_realtime_feedback_symbol ON catalyst_selection_realtime_feedback (symbol, trade_date DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_realtime_feedback_monitor ON catalyst_selection_realtime_feedback (monitor_id, event_time DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_realtime_feedback_risk_gate ON catalyst_selection_realtime_feedback (risk_gate, trade_date DESC)"))
    _backfill_realtime_feedback_trade_dates(db)
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS catalyst_selection_feedback_profiles (
                profile_scope VARCHAR(20) NOT NULL,
                profile_key VARCHAR(160) NOT NULL,
                model_version VARCHAR(40) NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                hit_count INTEGER NOT NULL DEFAULT 0,
                miss_count INTEGER NOT NULL DEFAULT 0,
                average_change_pct FLOAT,
                average_hit_score FLOAT,
                hit_rate FLOAT,
                learned_score FLOAT NOT NULL DEFAULT 50,
                confidence FLOAT NOT NULL DEFAULT 0,
                last_trade_date VARCHAR(10),
                last_settlement_date VARCHAR(10),
                feature_snapshot_json TEXT DEFAULT '{}',
                updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (profile_scope, profile_key)
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_catalyst_selection_feedback_profiles_updated ON catalyst_selection_feedback_profiles (updated_at DESC)"))
    db.commit()


def list_or_generate_selections(
    db: Session,
    *,
    trade_date: str | None = None,
    window: str = "premarket",
    limit: int = DEFAULT_SELECTION_LIMIT,
    force: bool = False,
    user_id: str | None = None,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    normalized_window = _normalize_window(window)
    resolved_date = _resolve_trade_date(db, trade_date)
    bounded_limit = max(1, min(int(limit or DEFAULT_SELECTION_LIMIT), MAX_SELECTION_LIMIT))
    if not force:
        stored = _load_selection_run(db, trade_date=resolved_date, window=normalized_window, limit=bounded_limit)
        if stored and _can_reuse_selection_run(stored, normalized_window, db=db, user_id=user_id):
            return _merge_live_event_backfill_status(stored, db=db)
        if stored:
            cache_state = _selection_cache_reuse_state(stored, normalized_window, db=db, user_id=user_id)
            _schedule_selection_refresh(
                trade_date=resolved_date,
                window=normalized_window,
                limit=bounded_limit,
                user_id=user_id,
            )
            return _merge_live_event_backfill_status(_mark_stale_selection_run(stored, cache_state=cache_state), db=db)
    return _merge_live_event_backfill_status(
        generate_selections(
            db,
            trade_date=resolved_date,
            window=normalized_window,
            limit=bounded_limit,
            user_id=user_id,
        ),
        db=db,
    )


def _merge_live_event_backfill_status(payload: dict[str, Any], db: Session | None = None) -> dict[str, Any]:
    governance = payload.get("data_governance") if isinstance(payload.get("data_governance"), dict) else None
    closed_loop = governance.get("closed_loop") if isinstance(governance, dict) and isinstance(governance.get("closed_loop"), dict) else None
    _merge_live_realtime_feedback_status(payload, db=db, governance=governance, closed_loop=closed_loop)
    event_reaction = (
        closed_loop.get("event_market_reaction")
        if isinstance(closed_loop, dict) and isinstance(closed_loop.get("event_market_reaction"), dict)
        else None
    )
    capture = event_reaction.get("capture") if isinstance(event_reaction, dict) and isinstance(event_reaction.get("capture"), dict) else None
    akshare = capture.get("akshare_backfill") if isinstance(capture, dict) and isinstance(capture.get("akshare_backfill"), dict) else None
    job_key = str((akshare or {}).get("job_key") or "").strip()
    if not job_key:
        return payload

    with _EVENT_AKSHARE_BACKFILL_LOCK:
        live_job = dict(_EVENT_AKSHARE_BACKFILL_JOBS.get(job_key) or {})
    if not live_job:
        return payload

    live_public = _public_event_akshare_backfill_job(live_job)
    merged_akshare = {**dict(akshare or {}), **live_public}
    capture["akshare_backfill"] = merged_akshare
    if isinstance(closed_loop, dict):
        closed_loop["minute_backfill"] = _event_minute_backfill_summary(capture)
    return payload


def _merge_live_realtime_feedback_status(
    payload: dict[str, Any],
    *,
    db: Session | None,
    governance: dict[str, Any] | None,
    closed_loop: dict[str, Any] | None,
) -> None:
    if db is None or not isinstance(closed_loop, dict):
        return
    governance_payload = governance or {}
    trade_date = _realtime_feedback_lookup_trade_date(
        trade_date=str(governance_payload.get("trade_date") or payload.get("trade_date") or "").strip() or None,
        event_reaction_trade_date=str(governance_payload.get("event_reaction_trade_date") or "").strip() or None,
        window=str(governance_payload.get("window") or payload.get("window") or ""),
    )
    try:
        closed_loop["realtime_feedback"] = summarize_realtime_feedback(db, trade_date=trade_date)
    except Exception:
        logger.exception("[catalyst-selection] failed to merge live realtime feedback trade_date=%s", trade_date)


def _event_minute_backfill_summary(capture: dict[str, Any]) -> dict[str, Any]:
    history = capture.get("history_backfill") if isinstance(capture.get("history_backfill"), dict) else {}
    akshare = capture.get("akshare_backfill") if isinstance(capture.get("akshare_backfill"), dict) else {}
    selection_refresh = akshare.get("selection_refresh") if isinstance(akshare.get("selection_refresh"), dict) else {}
    qmt_rows = int(capture.get("rows") or 0)
    akshare_rows = int(akshare.get("rows") or 0)
    selection_status = str(selection_refresh.get("status") or "")
    akshare_status = str(akshare.get("status") or "")
    history_status = str(history.get("status") or "")
    if qmt_rows > 0:
        status = "qmt_completed"
        message = f"QMT分钟线补采已写入 {qmt_rows} 行"
    elif selection_status in {"completed", "partial_failed", "failed", "skipped", "running", "pending"}:
        status = f"selection_refresh_{selection_status}"
        message = str(selection_refresh.get("message") or "")
    elif akshare_status:
        status = f"akshare_{akshare_status}"
        message = str(akshare.get("message") or "")
    elif history_status:
        status = f"history_{history_status}"
        message = str(history.get("message") or "")
    else:
        status = "not_started"
        message = str(capture.get("message") or "")
    return {
        "status": status,
        "message": message,
        "qmt_rows": qmt_rows,
        "history_status": history_status or None,
        "akshare_status": akshare_status or None,
        "akshare_rows": akshare_rows,
        "selection_refresh_status": selection_status or None,
        "selection_refreshed_count": int(selection_refresh.get("refreshed_count") or 0),
        "selection_failed_count": int(selection_refresh.get("failed_count") or 0),
        "updated_at": akshare.get("updated_at") or selection_refresh.get("updated_at"),
    }


def _selection_refresh_key(trade_date: str, window: str, limit: int, user_id: str | None) -> str:
    return f"{trade_date}:{window}:{limit}:{user_id or ''}"


def _schedule_selection_refresh(*, trade_date: str, window: str, limit: int, user_id: str | None) -> bool:
    refresh_key = _selection_refresh_key(trade_date, window, limit, user_id)
    with _SELECTION_REFRESH_LOCK:
        if refresh_key in _SELECTION_REFRESH_TASKS:
            return False
        _SELECTION_REFRESH_TASKS.add(refresh_key)

    def _run() -> None:
        db = SessionLocal()
        try:
            generate_selections(
                db,
                trade_date=trade_date,
                window=window,
                limit=limit,
                user_id=user_id,
            )
        except Exception:
            logger.exception(
                "[catalyst-selection] cached refresh failed trade_date=%s window=%s user=%s",
                trade_date,
                window,
                user_id,
            )
        finally:
            try:
                db.close()
            except Exception:
                pass
            with _SELECTION_REFRESH_LOCK:
                _SELECTION_REFRESH_TASKS.discard(refresh_key)

    threading.Thread(target=_run, name=f"catalyst-selection-refresh-{window}-{trade_date}", daemon=True).start()
    return True


def _mark_stale_selection_run(stored: dict[str, Any], *, cache_state: dict[str, Any] | None = None) -> dict[str, Any]:
    result = dict(stored)
    result["message"] = f"{stored.get('message') or _selection_message(str(stored.get('window') or ''))}（缓存结果，后台刷新中）"
    governance = dict(stored.get("data_governance") or {})
    public_cache_state = {
        "status": "stale",
        "refresh_scheduled": True,
        "updated_at": stored.get("updated_at"),
    }
    if cache_state:
        reason = str(cache_state.get("reason") or "").strip()
        if reason:
            public_cache_state["reason"] = reason
        llm_state = cache_state.get("llm_runtime") if isinstance(cache_state.get("llm_runtime"), dict) else None
        if llm_state and llm_state.get("status") == "changed":
            public_cache_state["llm_runtime_changed"] = True
            governance = _replace_stale_llm_runtime_governance(governance, llm_state)
            closed_loop = governance.get("closed_loop") if isinstance(governance.get("closed_loop"), dict) else {}
            llm_payload = closed_loop.get("llm_event_understanding") if isinstance(closed_loop.get("llm_event_understanding"), dict) else {}
            _mark_items_stale_llm_runtime(result.get("items") or [], llm_payload)
    governance["cache_state"] = public_cache_state
    result["data_governance"] = governance
    result["source"] = f"{stored.get('source') or 'cache'}+stale"
    return result


def schedule_event_driven_selection_refresh(
    *,
    trigger: str,
    windows: Iterable[str] = ("premarket",),
    limit: int = DEFAULT_SELECTION_LIMIT,
    user_id: str | None = None,
    trade_date: str | None = None,
    reason: str | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_windows = tuple(_normalize_windows(windows))
    bounded_limit = max(1, min(int(limit or DEFAULT_SELECTION_LIMIT), MAX_SELECTION_LIMIT))
    refresh_key = _event_driven_refresh_key(
        trigger=trigger,
        windows=normalized_windows,
        limit=bounded_limit,
        user_id=user_id,
        trade_date=trade_date,
    )
    now_iso = _utcnow().isoformat()
    base_state = {
        "refresh_key": refresh_key,
        "trigger": trigger,
        "windows": list(normalized_windows),
        "limit": bounded_limit,
        "user_id": user_id,
        "trade_date": trade_date,
        "reason": reason,
        "context": context or {},
        "generated": [],
        "errors": [],
        "skipped": False,
        "status": "scheduled",
        "deduped": False,
        "scheduled_at": now_iso,
        "updated_at": now_iso,
    }
    with _EVENT_DRIVEN_REFRESH_LOCK:
        if refresh_key in _EVENT_DRIVEN_REFRESH_TASKS:
            pending = dict(_EVENT_DRIVEN_REFRESH_PENDING.get(refresh_key) or base_state)
            pending.update(
                {
                    "status": "running",
                    "deduped": True,
                    "reason": reason or pending.get("reason") or "event_driven_selection_refresh_already_running",
                    "updated_at": now_iso,
                }
            )
            _EVENT_DRIVEN_REFRESH_PENDING[refresh_key] = dict(pending)
            _persist_event_refresh_state_in_new_session(
                {
                    **pending,
                    "refresh_key": refresh_key,
                    "trigger": trigger,
                    "windows": list(normalized_windows),
                    "limit": bounded_limit,
                    "user_id": user_id,
                    "trade_date": trade_date,
                    "context": context or pending.get("context") or {},
                }
            )
            return pending
        _EVENT_DRIVEN_REFRESH_TASKS.add(refresh_key)
        _EVENT_DRIVEN_REFRESH_PENDING[refresh_key] = dict(base_state)
    _persist_event_refresh_state_in_new_session(base_state)

    threading.Thread(
        target=_run_scheduled_event_driven_selection_refresh,
        name=f"catalyst-event-refresh-{abs(hash(refresh_key))}",
        daemon=True,
        kwargs={
            "refresh_key": refresh_key,
            "trigger": trigger,
            "windows": normalized_windows,
            "limit": bounded_limit,
            "user_id": user_id,
            "trade_date": trade_date,
            "reason": reason,
            "context": context or {},
        },
    ).start()
    return base_state


def _event_driven_refresh_key(
    *,
    trigger: str,
    windows: Iterable[str],
    limit: int,
    user_id: str | None,
    trade_date: str | None,
) -> str:
    window_key = ",".join(str(window).strip() for window in windows if str(window).strip())
    return f"{trigger}:{trade_date or ''}:{window_key}:{int(limit or 0)}:{user_id or ''}"


def _event_refresh_audit_id(payload: dict[str, Any]) -> str | None:
    audit = payload.get("closed_loop_audit") if isinstance(payload.get("closed_loop_audit"), dict) else {}
    audit_id = str(audit.get("audit_id") or "").strip()
    return audit_id or None


def _event_refresh_public_state(row: Any) -> dict[str, Any]:
    try:
        keys = set(row.keys())
    except Exception:
        keys = set()
    get_value = row.get if hasattr(row, "get") else lambda key, default=None: getattr(row, key, default)
    return {
        "refresh_key": str(get_value("refresh_key") or ""),
        "trigger": str(get_value("trigger_name") or get_value("trigger") or ""),
        "user_id": get_value("user_id"),
        "trade_date": get_value("trade_date"),
        "windows": _loads(get_value("windows_json"), []) if "windows_json" in keys else list(get_value("windows") or []),
        "limit": int(get_value("limit_value") or get_value("limit") or 0),
        "reason": get_value("reason"),
        "context": _loads(get_value("context_json"), {}) if "context_json" in keys else dict(get_value("context") or {}),
        "status": str(get_value("status") or "unknown"),
        "deduped": bool(get_value("deduped")),
        "generated": _loads(get_value("generated_json"), []) if "generated_json" in keys else list(get_value("generated") or []),
        "errors": _loads(get_value("errors_json"), []) if "errors_json" in keys else list(get_value("errors") or []),
        "skipped": bool(get_value("skipped")),
        "skip_reason": get_value("skip_reason"),
        "audit_id": get_value("audit_id"),
        "duration_ms": get_value("duration_ms"),
        "scheduled_at": _iso(get_value("scheduled_at")),
        "started_at": _iso(get_value("started_at")),
        "finished_at": _iso(get_value("finished_at")),
        "updated_at": _iso(get_value("updated_at")),
    }


def _persist_event_refresh_state_in_new_session(state: dict[str, Any]) -> None:
    try:
        with SessionLocal() as db:
            _persist_event_refresh_state(db, state)
            db.commit()
    except Exception:
        logger.exception("[catalyst-selection] failed to persist async event refresh state")


def _persist_event_refresh_state(db: Session, state: dict[str, Any]) -> None:
    if not state:
        return
    ensure_catalyst_selection_tables(db)
    refresh_key = str(state.get("refresh_key") or "").strip()
    if not refresh_key:
        return
    now_value = _parse_datetime_or_none(state.get("updated_at")) or _utcnow()
    scheduled_at = _parse_datetime_or_none(state.get("scheduled_at"))
    started_at = _parse_datetime_or_none(state.get("started_at"))
    finished_at = _parse_datetime_or_none(state.get("finished_at"))
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_event_refresh_runs (
                refresh_key, trigger_name, user_id, trade_date, windows_json, limit_value,
                reason, context_json, status, deduped, generated_json, errors_json,
                skipped, skip_reason, audit_id, duration_ms, scheduled_at, started_at,
                finished_at, updated_at
            ) VALUES (
                :refresh_key, :trigger_name, :user_id, :trade_date, :windows_json, :limit_value,
                :reason, :context_json, :status, :deduped, :generated_json, :errors_json,
                :skipped, :skip_reason, :audit_id, :duration_ms, :scheduled_at, :started_at,
                :finished_at, :updated_at
            )
            ON CONFLICT (refresh_key) DO UPDATE SET
                trigger_name = EXCLUDED.trigger_name,
                user_id = EXCLUDED.user_id,
                trade_date = COALESCE(EXCLUDED.trade_date, catalyst_selection_event_refresh_runs.trade_date),
                windows_json = EXCLUDED.windows_json,
                limit_value = EXCLUDED.limit_value,
                reason = COALESCE(EXCLUDED.reason, catalyst_selection_event_refresh_runs.reason),
                context_json = EXCLUDED.context_json,
                status = EXCLUDED.status,
                deduped = catalyst_selection_event_refresh_runs.deduped OR EXCLUDED.deduped,
                generated_json = EXCLUDED.generated_json,
                errors_json = EXCLUDED.errors_json,
                skipped = EXCLUDED.skipped,
                skip_reason = EXCLUDED.skip_reason,
                audit_id = COALESCE(EXCLUDED.audit_id, catalyst_selection_event_refresh_runs.audit_id),
                duration_ms = CASE
                    WHEN EXCLUDED.status IN ('scheduled', 'running') THEN EXCLUDED.duration_ms
                    ELSE COALESCE(EXCLUDED.duration_ms, catalyst_selection_event_refresh_runs.duration_ms)
                END,
                scheduled_at = COALESCE(catalyst_selection_event_refresh_runs.scheduled_at, EXCLUDED.scheduled_at),
                started_at = COALESCE(EXCLUDED.started_at, catalyst_selection_event_refresh_runs.started_at),
                finished_at = CASE
                    WHEN EXCLUDED.status IN ('scheduled', 'running') THEN EXCLUDED.finished_at
                    ELSE COALESCE(EXCLUDED.finished_at, catalyst_selection_event_refresh_runs.finished_at)
                END,
                updated_at = EXCLUDED.updated_at
            """
        ),
        {
            "refresh_key": refresh_key,
            "trigger_name": str(state.get("trigger") or state.get("trigger_name") or "unknown").strip() or "unknown",
            "user_id": state.get("user_id"),
            "trade_date": state.get("trade_date"),
            "windows_json": json.dumps(list(state.get("windows") or []), ensure_ascii=False, default=str),
            "limit_value": int(state.get("limit") or state.get("limit_value") or DEFAULT_SELECTION_LIMIT),
            "reason": state.get("reason"),
            "context_json": json.dumps(state.get("context") or {}, ensure_ascii=False, default=str),
            "status": str(state.get("status") or "unknown").strip() or "unknown",
            "deduped": bool(state.get("deduped")),
            "generated_json": json.dumps(state.get("generated") or [], ensure_ascii=False, default=str),
            "errors_json": json.dumps(state.get("errors") or [], ensure_ascii=False, default=str),
            "skipped": bool(state.get("skipped")),
            "skip_reason": state.get("skip_reason"),
            "audit_id": state.get("audit_id") or _event_refresh_audit_id(state),
            "duration_ms": state.get("duration_ms"),
            "scheduled_at": scheduled_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "updated_at": now_value,
        },
    )


def list_event_refresh_runs(
    db: Session,
    *,
    user_id: str | None = None,
    limit: int = 20,
    status: str | None = None,
    trigger: str | None = None,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 20), 100))}
    if user_id:
        clauses.append("user_id = :user_id")
        params["user_id"] = user_id
    if status:
        clauses.append("status = :status")
        params["status"] = str(status).strip()
    if trigger:
        clauses.append("trigger_name = :trigger")
        params["trigger"] = str(trigger).strip()
    sql = """
        SELECT refresh_key, trigger_name, user_id, trade_date, windows_json, limit_value,
               reason, context_json, status, deduped, generated_json, errors_json,
               skipped, skip_reason, audit_id, duration_ms, scheduled_at, started_at,
               finished_at, updated_at
        FROM catalyst_selection_event_refresh_runs
    """
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY updated_at DESC LIMIT :limit"
    rows = db.execute(text(sql), params).mappings().all()
    items = [_event_refresh_public_state(row) for row in rows]
    return {
        "items": items,
        "filters": {
            "user_id": user_id,
            "status": status,
            "trigger": trigger,
            "limit": params["limit"],
        },
        "updated_at": items[0].get("updated_at") if items else _utcnow().isoformat(),
    }


def _run_scheduled_event_driven_selection_refresh(
    *,
    refresh_key: str,
    trigger: str,
    windows: tuple[str, ...],
    limit: int,
    user_id: str | None,
    trade_date: str | None,
    reason: str | None,
    context: dict[str, Any],
) -> None:
    started_at = _utcnow()
    _persist_event_refresh_state_in_new_session(
        {
            "refresh_key": refresh_key,
            "trigger": trigger,
            "windows": list(windows),
            "limit": limit,
            "user_id": user_id,
            "trade_date": trade_date,
            "reason": reason,
            "context": context,
            "status": "running",
            "deduped": False,
            "generated": [],
            "errors": [],
            "skipped": False,
            "started_at": started_at.isoformat(),
            "updated_at": started_at.isoformat(),
        }
    )
    try:
        with SessionLocal() as db:
            payload = refresh_event_driven_selection(
                db,
                trigger=trigger,
                windows=windows,
                limit=limit,
                user_id=user_id,
                trade_date=trade_date,
                refresh_key=refresh_key,
                trigger_context={**(context or {}), "reason": reason} if reason else context,
            )
            status = "skipped" if payload.get("skipped") else ("partial_failed" if payload.get("errors") else "completed")
            finished_at = _utcnow()
            duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
            _persist_event_refresh_state(
                db,
                {
                    "refresh_key": refresh_key,
                    "trigger": trigger,
                    "windows": list(windows),
                    "limit": limit,
                    "user_id": user_id,
                    "trade_date": payload.get("trade_date") or trade_date,
                    "reason": reason,
                    "context": context,
                    "status": status,
                    "deduped": False,
                    "generated": payload.get("generated") or [],
                    "errors": payload.get("errors") or [],
                    "skipped": bool(payload.get("skipped")),
                    "skip_reason": payload.get("skip_reason"),
                    "audit_id": _event_refresh_audit_id(payload),
                    "duration_ms": duration_ms,
                    "started_at": started_at.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "updated_at": finished_at.isoformat(),
                },
            )
            db.commit()
            logger.info(
                "[catalyst-selection] async event refresh %s trigger=%s windows=%s generated=%s errors=%s",
                status,
                trigger,
                ",".join(windows),
                len(payload.get("generated") or []),
                len(payload.get("errors") or []),
            )
    except Exception as exc:
        finished_at = _utcnow()
        duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
        _persist_event_refresh_state_in_new_session(
            {
                "refresh_key": refresh_key,
                "trigger": trigger,
                "windows": list(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
                "reason": reason,
                "context": context,
                "status": "failed",
                "deduped": False,
                "generated": [],
                "errors": [{"error": str(exc)[:500]}],
                "skipped": False,
                "duration_ms": duration_ms,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "updated_at": finished_at.isoformat(),
            }
        )
        logger.exception("[catalyst-selection] async event refresh failed trigger=%s windows=%s", trigger, ",".join(windows))
    finally:
        with _EVENT_DRIVEN_REFRESH_LOCK:
            _EVENT_DRIVEN_REFRESH_TASKS.discard(refresh_key)
            _EVENT_DRIVEN_REFRESH_PENDING.pop(refresh_key, None)


def refresh_event_driven_selection(
    db: Session,
    *,
    trigger: str,
    windows: Iterable[str] = ("premarket",),
    limit: int = DEFAULT_SELECTION_LIMIT,
    user_id: str | None = None,
    trade_date: str | None = None,
    refresh_key: str | None = None,
    trigger_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    current_trade_date = _effective_cn_trade_date()
    if trade_date:
        try:
            resolved_date = _resolve_trade_date(db, trade_date)
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                logger.exception("[catalyst-selection] rollback failed after trade date resolution error")
            logger.warning(
                "[catalyst-selection] event-driven refresh skipped, trade date unavailable trigger=%s trade_date=%s error=%s",
                trigger,
                trade_date,
                exc,
            )
            audit = _build_skipped_event_refresh_audit(
                trigger=trigger,
                trade_date=None,
                skip_reason="交易日不可用，事件驱动机会榜刷新已跳过。",
            )
            _persist_closed_loop_audit(db, audit)
            db.commit()
            return {
                "trigger": trigger,
                "trade_date": None,
                "generated": [],
                "errors": [],
                "skipped": True,
                "skip_reason": "交易日不可用，事件驱动机会榜刷新已跳过。",
                "closed_loop_audit": audit,
                "updated_at": _utcnow().isoformat(),
            }
    else:
        try:
            resolved_date = _resolve_trade_date(db, None)
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                logger.exception("[catalyst-selection] rollback failed after latest daily date lookup error")
            logger.warning(
                "[catalyst-selection] event-driven refresh skipped, daily table unavailable trigger=%s error=%s",
                trigger,
                exc,
            )
            audit = _build_skipped_event_refresh_audit(
                trigger=trigger,
                trade_date=None,
                skip_reason="缺少可用日线表，事件驱动机会榜刷新已跳过。",
            )
            _persist_closed_loop_audit(db, audit)
            db.commit()
            return {
                "trigger": trigger,
                "trade_date": None,
                "generated": [],
                "errors": [],
                "skipped": True,
                "skip_reason": "缺少可用日线表，事件驱动机会榜刷新已跳过。",
                "closed_loop_audit": audit,
                "updated_at": _utcnow().isoformat(),
            }
        if not resolved_date:
            audit = _build_skipped_event_refresh_audit(
                trigger=trigger,
                trade_date=None,
                skip_reason="缺少可用日线数据，事件驱动机会榜刷新已跳过。",
            )
            _persist_closed_loop_audit(db, audit)
            db.commit()
            return {
                "trigger": trigger,
                "trade_date": None,
                "generated": [],
                "errors": [],
                "skipped": True,
                "skip_reason": "缺少可用日线数据，事件驱动机会榜刷新已跳过。",
                "closed_loop_audit": audit,
                "updated_at": _utcnow().isoformat(),
            }

    normalized_windows = _normalize_windows(windows)
    try:
        feature_trade_date = _latest_available_daily_trade_date(db, resolved_date)
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            logger.exception("[catalyst-selection] rollback failed after daily date precheck error")
        logger.warning(
            "[catalyst-selection] event-driven refresh skipped, daily table unavailable trigger=%s trade_date=%s error=%s",
            trigger,
            resolved_date,
            exc,
        )
        audit = _build_skipped_event_refresh_audit(
            trigger=trigger,
            trade_date=resolved_date,
            skip_reason="缺少可用日线表，事件驱动机会榜刷新已跳过。",
        )
        _persist_closed_loop_audit(db, audit)
        db.commit()
        return {
            "trigger": trigger,
            "trade_date": resolved_date,
            "generated": [],
            "errors": [],
            "skipped": True,
            "skip_reason": "缺少可用日线表，事件驱动机会榜刷新已跳过。",
            "closed_loop_audit": audit,
            "updated_at": _utcnow().isoformat(),
        }
    if not feature_trade_date:
        audit = _build_skipped_event_refresh_audit(
            trigger=trigger,
            trade_date=resolved_date,
            skip_reason="缺少可用日线数据，事件驱动机会榜刷新已跳过。",
        )
        _persist_closed_loop_audit(db, audit)
        db.commit()
        return {
            "trigger": trigger,
            "trade_date": resolved_date,
            "generated": [],
            "errors": [],
            "skipped": True,
            "skip_reason": "缺少可用日线数据，事件驱动机会榜刷新已跳过。",
            "closed_loop_audit": audit,
            "updated_at": _utcnow().isoformat(),
        }
    settlement_refresh = settle_pending_selections(db, before_trade_date=resolved_date, limit=5)
    generated: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for window in normalized_windows:
        try:
            llm_trigger_context = {
                **(trigger_context or {}),
                "source": "catalyst_selection_event_refresh",
                "trigger": trigger,
                "refresh_key": refresh_key,
                "reason": (trigger_context or {}).get("reason"),
                "trade_date": resolved_date,
                "window": window,
                "limit": limit,
                "triggered_at": _utcnow().isoformat(),
            }
            payload = generate_selections(
                db,
                trade_date=resolved_date,
                window=window,
                limit=limit,
                user_id=user_id,
                trigger_context=llm_trigger_context,
            )
            items = payload.get("items") or []
            top_item = items[0] if items else {}
            governance = payload.get("data_governance") if isinstance(payload.get("data_governance"), dict) else {}
            closed_loop = governance.get("closed_loop") if isinstance(governance.get("closed_loop"), dict) else {}
            risk_summary = closed_loop.get("risk_control_summary") if isinstance(closed_loop.get("risk_control_summary"), dict) else _build_risk_control_summary(items)
            feedback_state = closed_loop.get("feedback_learning_state") if isinstance(closed_loop.get("feedback_learning_state"), dict) else {}
            risk_gate_feedback_summary = closed_loop.get("risk_gate_feedback_summary") if isinstance(closed_loop.get("risk_gate_feedback_summary"), dict) else {}
            learning_adjustment_summary = closed_loop.get("learning_adjustment_summary") if isinstance(closed_loop.get("learning_adjustment_summary"), dict) else {}
            learning_impact_summary = closed_loop.get("learning_impact_summary") if isinstance(closed_loop.get("learning_impact_summary"), dict) else {}
            try:
                monitor_activation = maintain_realtime_monitor_from_selection(
                    db,
                    selection=payload,
                    user_id=user_id,
                )
            except Exception as exc:
                logger.exception("[catalyst-selection] realtime monitor activation failed trigger=%s window=%s", trigger, window)
                monitor_activation = {
                    "status": "failed",
                    "reason": "activation_error",
                    "error": str(exc)[:240],
                }
            generated.append(
                {
                    "window": window,
                    "item_count": len(items),
                    "top_symbol": top_item.get("symbol"),
                    "top_name": top_item.get("name"),
                    "top_score": top_item.get("score"),
                    "opportunity_event_count": len((governance.get("opportunity_events") or [])),
                    "top_opportunity_events": (governance.get("opportunity_events") or [])[:3],
                    "risk_control_summary": risk_summary,
                    "feedback_learning_state": feedback_state,
                    "risk_gate_feedback_summary": risk_gate_feedback_summary,
                    "learning_adjustment_summary": learning_adjustment_summary,
                    "learning_impact_summary": learning_impact_summary,
                    "top_learning_impacts": _top_learning_impacts(items),
                    "feedback_risk_gate_count": closed_loop.get("feedback_risk_gate_count"),
                    "realtime_feedback": closed_loop.get("realtime_feedback") or {},
                    "market_state": closed_loop.get("market_state"),
                    "market_state_freshness": closed_loop.get("market_state_freshness") or governance.get("market_state_freshness") or {},
                    "intraday_event_pulse": closed_loop.get("intraday_event_pulse") or {},
                    "monitor_activation": monitor_activation,
                    "score_profile_counts": closed_loop.get("score_profile_counts") or {},
                    "llm_event_understanding": closed_loop.get("llm_event_understanding") or {},
                    "end_to_end_evidence": closed_loop.get("end_to_end_evidence") or {},
                    "updated_at": payload.get("updated_at"),
                }
            )
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                logger.exception("[catalyst-selection] rollback failed after event refresh error")
            logger.exception(
                "[catalyst-selection] event-driven refresh failed trigger=%s trade_date=%s window=%s",
                trigger,
                resolved_date,
                window,
            )
            errors.append({"window": window, "error": str(exc)})
    audit = _build_event_refresh_closed_loop_audit(
        trigger=trigger,
        windows=normalized_windows,
        generated=generated,
        settlement_refresh=settlement_refresh,
        errors=errors,
        trade_date=resolved_date,
    )
    _persist_closed_loop_audit(db, audit)
    db.commit()
    return {
        "trigger": trigger,
        "trade_date": resolved_date,
        "generated": generated,
        "errors": errors,
        "skipped": False,
        "settlement_refresh": settlement_refresh,
        "closed_loop_audit": audit,
        "updated_at": _utcnow().isoformat(),
    }


def _closed_loop_check(
    requirement_id: str,
    label: str,
    *,
    status: str,
    evidence: list[str] | None = None,
    gaps: list[str] | None = None,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": requirement_id,
        "label": label,
        "status": status,
        "evidence": [item for item in (evidence or []) if str(item or "").strip()],
        "gaps": [item for item in (gaps or []) if str(item or "").strip()],
        "metrics": metrics or {},
    }


def _summarize_settlement_feedback_refresh(settlement_refresh: dict[str, Any]) -> dict[str, Any]:
    settled = settlement_refresh.get("settled") if isinstance(settlement_refresh.get("settled"), list) else []
    totals = Counter()
    model_version = None
    updated_at = None
    trade_dates: list[str] = []
    profile_changes: list[dict[str, Any]] = []
    for item in settled:
        if not isinstance(item, dict):
            continue
        trade_date = str(item.get("trade_date") or "").strip()
        if trade_date:
            trade_dates.append(trade_date)
        refresh = item.get("feedback_refresh") if isinstance(item.get("feedback_refresh"), dict) else {}
        if not refresh:
            continue
        model_version = str(refresh.get("model_version") or model_version or "")
        refresh_updated_at = str(refresh.get("updated_at") or "")
        if refresh_updated_at and (updated_at is None or refresh_updated_at > updated_at):
            updated_at = refresh_updated_at
        for key in (
            "updated_profile_count",
            "new_profile_count",
            "changed_profile_count",
            "symbol_profile_count",
            "theme_profile_count",
            "event_type_profile_count",
            "risk_gate_profile_count",
            "intraday_pulse_profile_count",
        ):
            totals[key] += int(refresh.get(key) or 0)
        changes = refresh.get("top_profile_changes") if isinstance(refresh.get("top_profile_changes"), list) else []
        profile_changes.extend(change for change in changes if isinstance(change, dict))
    top_changes = _top_feedback_profile_changes(profile_changes, limit=8) if profile_changes else []
    return {
        "model_version": model_version or FEEDBACK_MODEL_VERSION,
        "updated_profile_count": int(totals.get("updated_profile_count", 0)),
        "new_profile_count": int(totals.get("new_profile_count", 0)),
        "changed_profile_count": int(totals.get("changed_profile_count", 0)),
        "top_profile_changes": top_changes,
        "symbol_profile_count": int(totals.get("symbol_profile_count", 0)),
        "theme_profile_count": int(totals.get("theme_profile_count", 0)),
        "event_type_profile_count": int(totals.get("event_type_profile_count", 0)),
        "risk_gate_profile_count": int(totals.get("risk_gate_profile_count", 0)),
        "intraday_pulse_profile_count": int(totals.get("intraday_pulse_profile_count", 0)),
        "settled_trade_dates": sorted(set(trade_dates), reverse=True)[:8],
        "updated_at": updated_at,
    }


def _top_learning_impacts(items: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    impacts: list[dict[str, Any]] = []
    for item in items:
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        scoring = trace.get("scoring") if isinstance(trace.get("scoring"), dict) else {}
        impact = scoring.get("learning_impact") if isinstance(scoring.get("learning_impact"), dict) else {}
        if not impact:
            continue
        feedback = trace.get("feedback") if isinstance(trace.get("feedback"), dict) else {}
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        gate_feedback = monitor.get("gate_feedback") if isinstance(monitor.get("gate_feedback"), dict) else {}
        event = trace.get("event") if isinstance(trace.get("event"), dict) else {}
        market = trace.get("market") if isinstance(trace.get("market"), dict) else {}
        pulse = market.get("intraday_event_pulse") if isinstance(market.get("intraday_event_pulse"), dict) else {}
        symbol_profile = feedback.get("symbol_profile") if isinstance(feedback.get("symbol_profile"), dict) else {}
        theme_profile = feedback.get("theme_profile") if isinstance(feedback.get("theme_profile"), dict) else {}
        event_type_profile = feedback.get("event_type_profile") if isinstance(feedback.get("event_type_profile"), dict) else {}
        pulse_profile = pulse.get("feedback_profile") if isinstance(pulse.get("feedback_profile"), dict) else {}
        profiles = {
            "symbol": str(symbol_profile.get("profile_key") or item.get("symbol") or "").strip(),
            "theme": str(theme_profile.get("profile_key") or event.get("theme") or "").strip(),
            "event_type": str(event_type_profile.get("profile_key") or "").strip(),
            "risk_gate": str(gate_feedback.get("profile_key") or gate_feedback.get("gate") or monitor.get("execution_gate") or "").strip(),
            "intraday_pulse": str(pulse_profile.get("profile_key") or pulse.get("status") or "").strip(),
        }
        impacts.append(
            {
                "symbol": item.get("symbol"),
                "name": item.get("name"),
                "rank": item.get("rank"),
                "score": item.get("score"),
                "adaptive_feedback_score": item.get("adaptive_feedback_score"),
                "primary_theme": event.get("theme"),
                "profiles": {key: value for key, value in profiles.items() if value},
                "status": impact.get("status"),
                "score_delta_from_learning_policy": impact.get("score_delta_from_learning_policy"),
                "rank_before_learning_policy": impact.get("rank_before_learning_policy") or item.get("rank_before_learning_policy"),
                "final_rank": impact.get("final_rank") or item.get("final_rank") or item.get("rank"),
                "rank_delta_from_learning_policy": impact.get("rank_delta_from_learning_policy") or item.get("rank_delta_from_learning_policy"),
                "risk_effect": impact.get("risk_effect") if isinstance(impact.get("risk_effect"), dict) else {},
                "risk_gate_effect": impact.get("risk_gate_effect") if isinstance(impact.get("risk_gate_effect"), dict) else {},
            }
        )

    def _sort_key(item: dict[str, Any]) -> tuple[float, float, int]:
        score_delta = abs(float(_num(item.get("score_delta_from_learning_policy")) or 0.0))
        rank_delta = abs(float(_num(item.get("rank_delta_from_learning_policy")) or 0.0))
        active = 1 if str(item.get("status") or "") == "active" else 0
        return (score_delta, rank_delta, active)

    return sorted(impacts, key=_sort_key, reverse=True)[: max(0, int(limit or 0))]


def _profile_change_matches_learning_impact(change: dict[str, Any], impact: dict[str, Any]) -> bool:
    scope = str(change.get("profile_scope") or "").strip()
    key = str(change.get("profile_key") or "").strip()
    if not scope or not key:
        return False
    profiles = impact.get("profiles") if isinstance(impact.get("profiles"), dict) else {}
    if str(profiles.get(scope) or "").strip() == key:
        return True
    if scope == "symbol" and str(impact.get("symbol") or "").strip() == key:
        return True
    if scope == "theme" and str(impact.get("primary_theme") or "").strip() == key:
        return True
    return False


def _build_settlement_feedback_replay(feedback_refresh: dict[str, Any], generated: list[dict[str, Any]]) -> dict[str, Any]:
    changes = feedback_refresh.get("top_profile_changes") if isinstance(feedback_refresh.get("top_profile_changes"), list) else []
    impacts = _learning_impacts_from_generated(generated)

    matched: list[dict[str, Any]] = []
    matched_symbols: set[str] = set()
    for change in changes:
        if not isinstance(change, dict):
            continue
        for impact in impacts:
            if not _profile_change_matches_learning_impact(change, impact):
                continue
            symbol = str(impact.get("symbol") or "")
            if symbol:
                matched_symbols.add(symbol)
            risk_effect = impact.get("risk_effect") if isinstance(impact.get("risk_effect"), dict) else {}
            gate_effect = impact.get("risk_gate_effect") if isinstance(impact.get("risk_gate_effect"), dict) else {}
            matched.append(
                {
                    "window": impact.get("window"),
                    "profile_scope": change.get("profile_scope"),
                    "profile_key": change.get("profile_key"),
                    "direction": change.get("direction"),
                    "learned_score_delta": change.get("learned_score_delta"),
                    "sample_count_delta": change.get("sample_count_delta"),
                    "symbol": impact.get("symbol"),
                    "name": impact.get("name"),
                    "rank_before_learning_policy": impact.get("rank_before_learning_policy"),
                    "final_rank": impact.get("final_rank"),
                    "rank_delta_from_learning_policy": impact.get("rank_delta_from_learning_policy"),
                    "score_delta_from_learning_policy": impact.get("score_delta_from_learning_policy"),
                    "action_changed": bool(risk_effect.get("action_changed")),
                    "max_position_delta_pct": risk_effect.get("max_position_delta_pct"),
                    "gate_applied": bool(gate_effect.get("applied")),
                }
            )
            break

    rank_changed_count = sum(1 for item in matched if abs(float(_num(item.get("rank_delta_from_learning_policy")) or 0.0)) >= 1.0)
    score_changed_count = sum(1 for item in matched if abs(float(_num(item.get("score_delta_from_learning_policy")) or 0.0)) >= 0.01)
    risk_changed_count = sum(
        1
        for item in matched
        if item.get("action_changed")
        or abs(float(_num(item.get("max_position_delta_pct")) or 0.0)) >= 0.01
        or item.get("gate_applied")
    )
    status = "no_profile_change"
    if changes and not matched:
        status = "unmatched"
    if matched:
        status = "active" if score_changed_count or rank_changed_count or risk_changed_count else "matched"
    return {
        "status": status,
        "profile_change_count": len(changes),
        "candidate_impact_count": len(impacts),
        "matched_selection_count": len(matched_symbols),
        "matched_profile_change_count": len(matched),
        "score_changed_count": score_changed_count,
        "rank_changed_count": rank_changed_count,
        "risk_changed_count": risk_changed_count,
        "items": matched[:8],
    }


def _learning_impacts_from_generated(generated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    impacts: list[dict[str, Any]] = []
    for item in generated:
        if not isinstance(item, dict):
            continue
        for impact in item.get("top_learning_impacts") or []:
            if isinstance(impact, dict):
                impacts.append({**impact, "window": item.get("window")})
    return impacts


def _best_realtime_feedback_summary_from_generated(generated: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for item in generated:
        realtime = item.get("realtime_feedback") if isinstance(item.get("realtime_feedback"), dict) else {}
        if int(realtime.get("sample_count") or 0) >= int(summary.get("sample_count") or 0):
            summary = dict(realtime)
    return summary


def _realtime_feedback_replay_signal_sets(realtime_feedback: dict[str, Any]) -> dict[str, set[str]]:
    top_symbols = realtime_feedback.get("top_symbols") if isinstance(realtime_feedback.get("top_symbols"), list) else []
    top_themes = realtime_feedback.get("top_themes") if isinstance(realtime_feedback.get("top_themes"), list) else []
    risk_gate_counts = realtime_feedback.get("risk_gate_counts") if isinstance(realtime_feedback.get("risk_gate_counts"), dict) else {}
    event_type_counts = realtime_feedback.get("semantic_event_type_counts") if isinstance(realtime_feedback.get("semantic_event_type_counts"), dict) else {}
    return {
        "symbol": {
            _normalize_symbol(item.get("symbol"))
            for item in top_symbols
            if isinstance(item, dict) and str(item.get("symbol") or "").strip()
        },
        "theme": {
            str(item.get("theme") or "").strip()
            for item in top_themes
            if isinstance(item, dict) and str(item.get("theme") or "").strip()
        },
        "risk_gate": {str(key or "").strip() for key in risk_gate_counts.keys() if str(key or "").strip()},
        "event_type": {_normalize_event_type(key) for key in event_type_counts.keys() if _normalize_event_type(key)},
    }


def _build_realtime_feedback_replay(realtime_feedback: dict[str, Any], generated: list[dict[str, Any]]) -> dict[str, Any]:
    impacts = _learning_impacts_from_generated(generated)
    sample_count = int((realtime_feedback or {}).get("sample_count") or 0)
    signal_sets = _realtime_feedback_replay_signal_sets(realtime_feedback or {})
    matched: list[dict[str, Any]] = []
    matched_symbols: set[str] = set()
    for impact in impacts:
        profiles = impact.get("profiles") if isinstance(impact.get("profiles"), dict) else {}
        symbol = _normalize_symbol(impact.get("symbol"))
        theme = str(profiles.get("theme") or impact.get("primary_theme") or "").strip()
        risk_gate = str(profiles.get("risk_gate") or "").strip()
        event_type = _normalize_event_type(profiles.get("event_type"))
        matched_signals: list[str] = []
        if symbol and symbol in signal_sets["symbol"]:
            matched_signals.append("symbol")
        if theme and theme in signal_sets["theme"]:
            matched_signals.append("theme")
        if risk_gate and risk_gate in signal_sets["risk_gate"]:
            matched_signals.append("risk_gate")
        if event_type and event_type in signal_sets["event_type"]:
            matched_signals.append("event_type")
        if not matched_signals:
            continue
        if symbol:
            matched_symbols.add(symbol)
        risk_effect = impact.get("risk_effect") if isinstance(impact.get("risk_effect"), dict) else {}
        gate_effect = impact.get("risk_gate_effect") if isinstance(impact.get("risk_gate_effect"), dict) else {}
        matched.append(
            {
                "window": impact.get("window"),
                "matched_signals": matched_signals,
                "symbol": impact.get("symbol"),
                "name": impact.get("name"),
                "primary_theme": impact.get("primary_theme"),
                "risk_gate": risk_gate or None,
                "event_type": event_type or None,
                "rank_before_learning_policy": impact.get("rank_before_learning_policy"),
                "final_rank": impact.get("final_rank"),
                "rank_delta_from_learning_policy": impact.get("rank_delta_from_learning_policy"),
                "score_delta_from_learning_policy": impact.get("score_delta_from_learning_policy"),
                "action_changed": bool(risk_effect.get("action_changed")),
                "max_position_delta_pct": risk_effect.get("max_position_delta_pct"),
                "gate_applied": bool(gate_effect.get("applied")),
            }
        )

    rank_changed_count = sum(1 for item in matched if abs(float(_num(item.get("rank_delta_from_learning_policy")) or 0.0)) >= 1.0)
    score_changed_count = sum(1 for item in matched if abs(float(_num(item.get("score_delta_from_learning_policy")) or 0.0)) >= 0.01)
    risk_changed_count = sum(
        1
        for item in matched
        if item.get("action_changed")
        or abs(float(_num(item.get("max_position_delta_pct")) or 0.0)) >= 0.01
        or item.get("gate_applied")
    )
    status = "no_realtime_feedback"
    if sample_count > 0 and not impacts:
        status = "no_learning_impact"
    elif sample_count > 0 and not matched:
        status = "unmatched"
    elif matched:
        status = "active" if score_changed_count or rank_changed_count or risk_changed_count else "matched"
    return {
        "status": status,
        "sample_count": sample_count,
        "candidate_impact_count": len(impacts),
        "matched_selection_count": len(matched_symbols),
        "matched_feedback_signal_count": len(matched),
        "score_changed_count": score_changed_count,
        "rank_changed_count": rank_changed_count,
        "risk_changed_count": risk_changed_count,
        "signal_counts": {
            "symbol": len(signal_sets["symbol"]),
            "theme": len(signal_sets["theme"]),
            "risk_gate": len(signal_sets["risk_gate"]),
            "event_type": len(signal_sets["event_type"]),
        },
        "items": matched[:8],
    }


def _build_closed_loop_requirement_checks(
    *,
    windows: list[str],
    generated: list[dict[str, Any]],
    settlement_refresh: dict[str, Any],
    errors: list[dict[str, str]],
    skipped: bool = False,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    labels = dict(CLOSED_LOOP_REQUIREMENTS)
    requested_window_count = len(windows)
    generated_window_count = len(generated)
    failed_window_count = len(errors)
    total_selected_count = 0
    opportunity_event_count = 0
    llm_ready_count = 0
    llm_semantic_count = 0
    llm_symbol_theme_count = 0
    score_profile_count = 0
    risk_action_counts: Counter[str] = Counter()
    risk_level_counts: Counter[str] = Counter()
    market_state_count = 0
    market_freshness_counts: Counter[str] = Counter()
    intraday_pulse_counts: Counter[str] = Counter()
    feedback_profile_count = 0
    feedback_sample_count = 0
    selected_with_feedback_count = 0
    realtime_sample_count = 0
    risk_gate_profile_count = 0
    risk_gate_used_count = 0
    learning_impact_candidate_count = 0
    learning_impact_active_count = 0
    learning_impact_score_changed_count = 0
    learning_impact_rank_changed_count = 0
    learning_impact_risk_changed_count = 0
    monitor_started_count = 0
    monitor_failed_count = 0

    for item in generated:
        feedback_state = item.get("feedback_learning_state") if isinstance(item.get("feedback_learning_state"), dict) else {}
        selected_count = int(feedback_state.get("selected_count") or item.get("item_count") or 0)
        total_selected_count += selected_count
        opportunity_event_count += int(item.get("opportunity_event_count") or 0)

        llm = item.get("llm_event_understanding") if isinstance(item.get("llm_event_understanding"), dict) else {}
        if bool(llm.get("ready")):
            llm_ready_count += 1
        llm_semantic_count += int(llm.get("used_semantic_theme_count") or 0)
        llm_symbol_theme_count += int(llm.get("used_symbol_theme_count") or 0)

        score_profiles = item.get("score_profile_counts") if isinstance(item.get("score_profile_counts"), dict) else {}
        score_profile_count += sum(int(value or 0) for value in score_profiles.values())

        risk_summary = item.get("risk_control_summary") if isinstance(item.get("risk_control_summary"), dict) else {}
        risk_action_counts.update({str(k): int(v or 0) for k, v in dict(risk_summary.get("action_counts") or {}).items()})
        risk_level_counts.update({str(k): int(v or 0) for k, v in dict(risk_summary.get("risk_level_counts") or {}).items()})

        if item.get("market_state"):
            market_state_count += 1
        freshness = item.get("market_state_freshness") if isinstance(item.get("market_state_freshness"), dict) else {}
        freshness_status = str(freshness.get("status") or "").strip()
        if freshness_status:
            market_freshness_counts[freshness_status] += 1
        pulse = item.get("intraday_event_pulse") if isinstance(item.get("intraday_event_pulse"), dict) else {}
        pulse_status = str(pulse.get("status") or "").strip()
        if pulse_status:
            intraday_pulse_counts[pulse_status] += 1

        feedback_profile_count = max(feedback_profile_count, int(feedback_state.get("profile_count") or 0))
        feedback_sample_count = max(feedback_sample_count, int(feedback_state.get("sample_count") or 0))
        selected_with_feedback_count += int(feedback_state.get("selected_with_feedback_count") or 0)
        risk_gate_profile_count = max(risk_gate_profile_count, int(feedback_state.get("risk_gate_profile_count") or item.get("feedback_risk_gate_count") or 0))
        risk_gate_summary = item.get("risk_gate_feedback_summary") if isinstance(item.get("risk_gate_feedback_summary"), dict) else {}
        risk_gate_used_count += int(risk_gate_summary.get("used_count") or 0)
        realtime = item.get("realtime_feedback") if isinstance(item.get("realtime_feedback"), dict) else {}
        realtime_sample_count = max(realtime_sample_count, int(realtime.get("sample_count") or 0))
        learning_summary = item.get("learning_impact_summary") if isinstance(item.get("learning_impact_summary"), dict) else {}
        top_learning_impacts = [impact for impact in item.get("top_learning_impacts") or [] if isinstance(impact, dict)]
        top_learning_summary = _summarize_learning_replay_impacts(top_learning_impacts) if top_learning_impacts else {}
        learning_impact_candidate_count += int(learning_summary.get("item_count") or top_learning_summary.get("candidate_impact_count") or 0)
        learning_impact_active_count += int(learning_summary.get("active_count") or top_learning_summary.get("active_impact_count") or 0)
        learning_impact_score_changed_count += int(
            learning_summary.get("score_changed_count")
            or top_learning_summary.get("score_changed_count")
            or (learning_summary.get("active_count") if learning_summary.get("average_score_delta") is not None else 0)
            or 0
        )
        learning_impact_rank_changed_count += int(
            learning_summary.get("rank_changed_count")
            or top_learning_summary.get("rank_changed_count")
            or int(learning_summary.get("improved_rank_count") or 0) + int(learning_summary.get("reduced_rank_count") or 0)
        )
        learning_impact_risk_changed_count += int(
            learning_summary.get("risk_changed_count")
            or top_learning_summary.get("risk_changed_count")
            or int(learning_summary.get("action_changed_count") or 0) + int(learning_summary.get("gate_applied_count") or 0)
        )

        monitor_activation = item.get("monitor_activation") if isinstance(item.get("monitor_activation"), dict) else {}
        if bool(monitor_activation.get("started")):
            monitor_started_count += 1
        if str(monitor_activation.get("status") or "") == "failed":
            monitor_failed_count += 1

    settled = settlement_refresh.get("settled") if isinstance(settlement_refresh.get("settled"), list) else []
    settlement_errors = settlement_refresh.get("errors") if isinstance(settlement_refresh.get("errors"), list) else []
    settlement_feedback_refresh = _summarize_settlement_feedback_refresh(settlement_refresh)
    settlement_feedback_replay = _build_settlement_feedback_replay(settlement_feedback_refresh, generated)
    settlement_feedback_updated_count = int(settlement_feedback_refresh.get("updated_profile_count") or 0)
    settlement_feedback_replay_matched_count = int(settlement_feedback_replay.get("matched_selection_count") or 0)
    realtime_feedback_replay = _build_realtime_feedback_replay(
        _best_realtime_feedback_summary_from_generated(generated),
        generated,
    )
    realtime_feedback_replay_matched_count = int(realtime_feedback_replay.get("matched_selection_count") or 0)
    risk_action_total = sum(risk_action_counts.values())
    market_aligned_count = int(market_freshness_counts.get("aligned", 0))
    market_lagged_count = sum(
        int(market_freshness_counts.get(status, 0))
        for status in ("minute_ready_daily_lagged", "ready_with_lagged_daily_features")
    )

    if skipped:
        checks = [
            _closed_loop_check(
                requirement_id,
                label,
                status="missing",
                gaps=[skip_reason or "事件驱动刷新被跳过，闭环证据未产生。"],
                metrics={"requested_window_count": requested_window_count, "generated_window_count": 0},
            )
            for requirement_id, label in CLOSED_LOOP_REQUIREMENTS
        ]
    else:
        checks = [
            _closed_loop_check(
                "opportunity_discovery",
                labels["opportunity_discovery"],
                status="active" if opportunity_event_count > 0 else ("degraded" if total_selected_count > 0 else "missing"),
                evidence=[
                    f"生成窗口 {generated_window_count}/{requested_window_count}",
                    f"主动机会事件 {opportunity_event_count}",
                    f"入选标的 {total_selected_count}",
                ],
                gaps=[] if opportunity_event_count > 0 else ["已生成榜单但未形成主动机会事件。"] if total_selected_count > 0 else ["未生成可用机会榜。"],
                metrics={
                    "requested_window_count": requested_window_count,
                    "generated_window_count": generated_window_count,
                    "failed_window_count": failed_window_count,
                    "opportunity_event_count": opportunity_event_count,
                    "total_selected_count": total_selected_count,
                },
            ),
            _closed_loop_check(
                "event_understanding",
                labels["event_understanding"],
                status="active" if llm_ready_count > 0 and llm_semantic_count > 0 else ("degraded" if llm_ready_count > 0 else "missing"),
                evidence=[
                    f"LLM就绪窗口 {llm_ready_count}",
                    f"语义增强主题 {llm_semantic_count}",
                    f"LLM推荐标的主题 {llm_symbol_theme_count}",
                ],
                gaps=[] if llm_semantic_count > 0 else ["远程 LLM 就绪但本次未产出语义增强。"] if llm_ready_count > 0 else ["远程 LLM 未就绪或未被使用。"],
                metrics={
                    "llm_ready_window_count": llm_ready_count,
                    "used_semantic_theme_count": llm_semantic_count,
                    "used_symbol_theme_count": llm_symbol_theme_count,
                },
            ),
            _closed_loop_check(
                "market_state",
                labels["market_state"],
                status="active" if market_aligned_count > 0 else ("degraded" if market_state_count > 0 or market_lagged_count > 0 else "missing"),
                evidence=[
                    f"市场状态窗口 {market_state_count}",
                    f"新鲜度 {dict(market_freshness_counts) or '--'}",
                    f"事件池脉冲 {dict(intraday_pulse_counts) or '--'}",
                ],
                gaps=[] if market_aligned_count > 0 else ["市场状态可用但存在日线/宽度滞后。"] if market_state_count > 0 or market_lagged_count > 0 else ["缺少市场状态判定。"],
                metrics={
                    "market_state_window_count": market_state_count,
                    "freshness_status_counts": dict(market_freshness_counts),
                    "intraday_pulse_status_counts": dict(intraday_pulse_counts),
                },
            ),
            _closed_loop_check(
                "dynamic_ranking",
                labels["dynamic_ranking"],
                status="active" if total_selected_count > 0 and score_profile_count > 0 else ("degraded" if total_selected_count > 0 else "missing"),
                evidence=[
                    f"入选标的 {total_selected_count}",
                    f"评分轮廓样本 {score_profile_count}",
                    f"风险等级 {dict(risk_level_counts) or '--'}",
                ],
                gaps=[] if score_profile_count > 0 else ["榜单已生成，但缺少评分轮廓分布证据。"] if total_selected_count > 0 else ["未产生动态排序结果。"],
                metrics={
                    "total_selected_count": total_selected_count,
                    "score_profile_count": score_profile_count,
                    "risk_level_counts": dict(risk_level_counts),
                },
            ),
            _closed_loop_check(
                "risk_control",
                labels["risk_control"],
                status="active" if risk_action_total > 0 else ("degraded" if total_selected_count > 0 else "missing"),
                evidence=[
                    f"风控动作 {dict(risk_action_counts) or '--'}",
                    f"风险门禁画像 {risk_gate_profile_count}",
                    f"监控启动 {monitor_started_count} / 失败 {monitor_failed_count}",
                ],
                gaps=[] if risk_action_total > 0 else ["标的已排序，但未产出风控动作。"] if total_selected_count > 0 else ["未进入风控环节。"],
                metrics={
                    "risk_action_counts": dict(risk_action_counts),
                    "risk_gate_profile_count": risk_gate_profile_count,
                    "risk_gate_used_count": risk_gate_used_count,
                    "monitor_started_count": monitor_started_count,
                    "monitor_failed_count": monitor_failed_count,
                },
            ),
            _closed_loop_check(
                "feedback_learning",
                labels["feedback_learning"],
                status=(
                    "active"
                    if (
                        feedback_profile_count > 0
                        and (selected_with_feedback_count > 0 or realtime_sample_count > 0 or settled or learning_impact_active_count > 0)
                    ) or settlement_feedback_updated_count > 0
                    else ("warming_up" if total_selected_count > 0 else "missing")
                ),
                evidence=[
                    f"反馈画像 {feedback_profile_count} / 样本 {feedback_sample_count}",
                    f"入选标的带反馈 {selected_with_feedback_count}",
                    f"实时反馈样本 {realtime_sample_count}",
                    f"学习影响 active {learning_impact_active_count} / 候选 {learning_impact_candidate_count}",
                    f"学习改分 {learning_impact_score_changed_count} / 改名次 {learning_impact_rank_changed_count} / 改风控 {learning_impact_risk_changed_count}",
                    f"实时反哺回放命中 {realtime_feedback_replay_matched_count}",
                    f"结算刷新画像 {settlement_feedback_updated_count}",
                    f"反哺回放命中 {settlement_feedback_replay_matched_count}",
                    f"结算 {len(settled)} / 错误 {len(settlement_errors)}",
                ],
                gaps=[] if feedback_profile_count > 0 or settlement_feedback_updated_count > 0 or learning_impact_active_count > 0 else ["还没有足够历史结算或实时反馈形成画像。"],
                metrics={
                    "feedback_profile_count": feedback_profile_count,
                    "feedback_sample_count": feedback_sample_count,
                    "selected_with_feedback_count": selected_with_feedback_count,
                    "realtime_sample_count": realtime_sample_count,
                    "learning_impact_candidate_count": learning_impact_candidate_count,
                    "learning_impact_active_count": learning_impact_active_count,
                    "learning_impact_score_changed_count": learning_impact_score_changed_count,
                    "learning_impact_rank_changed_count": learning_impact_rank_changed_count,
                    "learning_impact_risk_changed_count": learning_impact_risk_changed_count,
                    "realtime_feedback_replay": realtime_feedback_replay,
                    "realtime_feedback_replay_matched_count": realtime_feedback_replay_matched_count,
                    "settlement_feedback_updated_profile_count": settlement_feedback_updated_count,
                    "settlement_feedback_refresh": settlement_feedback_refresh,
                    "settlement_feedback_replay": settlement_feedback_replay,
                    "settlement_feedback_replay_matched_count": settlement_feedback_replay_matched_count,
                    "settled_count": len(settled),
                    "settlement_error_count": len(settlement_errors),
                },
            ),
        ]

    status_counts = Counter(str(check.get("status") or "unknown") for check in checks)
    active_like_count = int(status_counts.get("active", 0) + status_counts.get("warming_up", 0))
    overall_status = "active"
    if status_counts.get("missing", 0):
        overall_status = "incomplete"
    elif status_counts.get("degraded", 0) or status_counts.get("warming_up", 0) or failed_window_count:
        overall_status = "degraded"
    return {
        "summary": {
            "overall_status": overall_status,
            "active_count": int(status_counts.get("active", 0)),
            "warming_up_count": int(status_counts.get("warming_up", 0)),
            "degraded_count": int(status_counts.get("degraded", 0)),
            "missing_count": int(status_counts.get("missing", 0)),
            "total_count": len(checks),
            "active_like_count": active_like_count,
            "pass_rate": round(active_like_count / len(checks), 4) if checks else 0.0,
        },
        "checks": checks,
    }


def _build_event_refresh_closed_loop_audit(
    *,
    trigger: str,
    windows: list[str],
    generated: list[dict[str, Any]],
    settlement_refresh: dict[str, Any],
    errors: list[dict[str, str]],
    trade_date: str | None,
) -> dict[str, Any]:
    risk_action_counts: Counter[str] = Counter()
    risk_level_counts: Counter[str] = Counter()
    feedback_profile_count = 0
    feedback_sample_count = 0
    selected_with_feedback_count = 0
    risk_gate_profile_count = 0
    risk_gate_used_count = 0
    risk_gate_applied_count = 0
    risk_gate_tightened_count = 0
    risk_gate_supportive_count = 0
    risk_gate_overly_conservative_count = 0
    realtime_feedback_summary: dict[str, Any] = {}
    monitor_activation_counts: Counter[str] = Counter()
    monitor_activation_items: list[dict[str, Any]] = []
    total_selected_count = 0
    opportunity_event_count = 0
    llm_ready_count = 0
    for item in generated:
        risk_summary = item.get("risk_control_summary") if isinstance(item.get("risk_control_summary"), dict) else {}
        risk_action_counts.update({str(k): int(v or 0) for k, v in dict(risk_summary.get("action_counts") or {}).items()})
        risk_level_counts.update({str(k): int(v or 0) for k, v in dict(risk_summary.get("risk_level_counts") or {}).items()})
        feedback_state = item.get("feedback_learning_state") if isinstance(item.get("feedback_learning_state"), dict) else {}
        feedback_profile_count = max(feedback_profile_count, int(feedback_state.get("profile_count") or 0))
        feedback_sample_count = max(feedback_sample_count, int(feedback_state.get("sample_count") or 0))
        selected_with_feedback_count += int(feedback_state.get("selected_with_feedback_count") or 0)
        risk_gate_profile_count = max(
            risk_gate_profile_count,
            int(feedback_state.get("risk_gate_profile_count") or item.get("feedback_risk_gate_count") or 0),
        )
        risk_gate_summary = item.get("risk_gate_feedback_summary") if isinstance(item.get("risk_gate_feedback_summary"), dict) else {}
        risk_gate_used_count += int(risk_gate_summary.get("used_count") or 0)
        risk_gate_applied_count += int(risk_gate_summary.get("applied_count") or 0)
        risk_gate_tightened_count += int(risk_gate_summary.get("tightened_count") or 0)
        risk_gate_supportive_count += int(risk_gate_summary.get("supportive_count") or 0)
        risk_gate_overly_conservative_count += int(risk_gate_summary.get("overly_conservative_count") or 0)
        realtime_feedback = item.get("realtime_feedback") if isinstance(item.get("realtime_feedback"), dict) else {}
        if int(realtime_feedback.get("sample_count") or 0) >= int(realtime_feedback_summary.get("sample_count") or 0):
            realtime_feedback_summary = dict(realtime_feedback)
        monitor_activation = item.get("monitor_activation") if isinstance(item.get("monitor_activation"), dict) else {}
        if monitor_activation:
            activation_status = str(monitor_activation.get("status") or "unknown").strip() or "unknown"
            monitor_activation_counts[activation_status] += 1
            monitor_activation_items.append(
                {
                    "window": item.get("window"),
                    "status": activation_status,
                    "reason": monitor_activation.get("reason"),
                    "monitor_id": monitor_activation.get("monitor_id"),
                    "monitor_status": monitor_activation.get("monitor_status"),
                    "monitor_symbol_count": monitor_activation.get("monitor_symbol_count"),
                    "started": bool(monitor_activation.get("started")),
                }
            )
        total_selected_count += int(feedback_state.get("selected_count") or item.get("item_count") or 0)
        opportunity_event_count += int(item.get("opportunity_event_count") or 0)
        llm = item.get("llm_event_understanding") if isinstance(item.get("llm_event_understanding"), dict) else {}
        if bool(llm.get("ready")):
            llm_ready_count += 1
    settled = settlement_refresh.get("settled") if isinstance(settlement_refresh.get("settled"), list) else []
    settlement_errors = settlement_refresh.get("errors") if isinstance(settlement_refresh.get("errors"), list) else []
    settlement_feedback_refresh = _summarize_settlement_feedback_refresh(settlement_refresh)
    settlement_feedback_replay = _build_settlement_feedback_replay(settlement_feedback_refresh, generated)
    realtime_feedback_replay = _build_realtime_feedback_replay(realtime_feedback_summary, generated)
    end_to_end_evidence = _build_event_refresh_end_to_end_evidence(
        trigger=trigger,
        windows=windows,
        generated=generated,
        settlement_feedback_refresh=settlement_feedback_refresh,
        realtime_feedback_summary=realtime_feedback_summary,
        errors=errors,
    )
    status = "completed"
    if errors and generated:
        status = "partial_failed"
    elif errors:
        status = "failed"
    requirement_state = _build_closed_loop_requirement_checks(
        windows=windows,
        generated=generated,
        settlement_refresh=settlement_refresh,
        errors=errors,
    )
    return {
        "audit_id": uuid4().hex,
        "trigger": trigger,
        "trade_date": trade_date,
        "status": status,
        "requirement_summary": requirement_state["summary"],
        "requirement_checks": requirement_state["checks"],
        "end_to_end_evidence": end_to_end_evidence,
        "requested_window_count": len(windows),
        "generated_window_count": len(generated),
        "failed_window_count": len(errors),
        "total_selected_count": total_selected_count,
        "opportunity_event_count": opportunity_event_count,
        "risk_action_counts": dict(risk_action_counts),
        "risk_level_counts": dict(risk_level_counts),
        "feedback": {
            "profile_count": feedback_profile_count,
            "sample_count": feedback_sample_count,
            "selected_with_feedback_count": selected_with_feedback_count,
            "selected_count": total_selected_count,
            "risk_gate_profile_count": risk_gate_profile_count,
            "risk_gate_used_count": risk_gate_used_count,
            "risk_gate_applied_count": risk_gate_applied_count,
            "risk_gate_tightened_count": risk_gate_tightened_count,
            "risk_gate_supportive_count": risk_gate_supportive_count,
            "risk_gate_overly_conservative_count": risk_gate_overly_conservative_count,
            "realtime": realtime_feedback_summary,
            "realtime_sample_count": int(realtime_feedback_summary.get("sample_count") or 0),
            "realtime_symbol_feedback_count": int(realtime_feedback_summary.get("symbol_feedback_count") or 0),
            "realtime_risk_feedback_count": int(realtime_feedback_summary.get("risk_feedback_count") or 0),
            "realtime_replay": realtime_feedback_replay,
            "realtime_replay_matched_count": int(realtime_feedback_replay.get("matched_selection_count") or 0),
            "realtime_replay_score_changed_count": int(realtime_feedback_replay.get("score_changed_count") or 0),
            "realtime_replay_rank_changed_count": int(realtime_feedback_replay.get("rank_changed_count") or 0),
            "realtime_replay_risk_changed_count": int(realtime_feedback_replay.get("risk_changed_count") or 0),
        },
        "monitor_activation": {
            "status_counts": dict(monitor_activation_counts),
            "created_count": int(monitor_activation_counts.get("created", 0) + monitor_activation_counts.get("created_running", 0)),
            "updated_count": int(monitor_activation_counts.get("updated", 0) + monitor_activation_counts.get("updated_running", 0)),
            "running_count": int(monitor_activation_counts.get("created_running", 0) + monitor_activation_counts.get("updated_running", 0)),
            "skipped_count": int(monitor_activation_counts.get("skipped", 0) + monitor_activation_counts.get("disabled", 0)),
            "failed_count": int(monitor_activation_counts.get("failed", 0)),
            "items": monitor_activation_items[:8],
        },
        "llm_ready_window_count": llm_ready_count,
        "settlement": {
            "skipped": bool(settlement_refresh.get("skipped")),
            "settled_count": len(settled),
            "error_count": len(settlement_errors),
            "feedback_refresh": settlement_feedback_refresh,
            "feedback_replay": settlement_feedback_replay,
        },
        "generated": generated,
        "errors": errors,
        "created_at": _utcnow().isoformat(),
        "updated_at": _utcnow().isoformat(),
    }


def _build_skipped_event_refresh_audit(
    *,
    trigger: str,
    trade_date: str | None,
    skip_reason: str,
) -> dict[str, Any]:
    now_value = _utcnow().isoformat()
    requirement_state = _build_closed_loop_requirement_checks(
        windows=[],
        generated=[],
        settlement_refresh={"skipped": True, "settled": [], "errors": []},
        errors=[],
        skipped=True,
        skip_reason=skip_reason,
    )
    return {
        "audit_id": uuid4().hex,
        "trigger": trigger,
        "trade_date": trade_date,
        "status": "skipped",
        "skip_reason": skip_reason,
        "requirement_summary": requirement_state["summary"],
        "requirement_checks": requirement_state["checks"],
        "end_to_end_evidence": {
            "status": "skipped",
            "trigger": trigger,
            "requested_windows": [],
            "generated_window_count": 0,
            "failed_window_count": 0,
            "active_window_count": 0,
            "degraded_window_count": 0,
            "incomplete_window_count": 0,
            "stage_rollup": {},
            "feedback_profile_updated_count": 0,
            "realtime_feedback_sample_count": 0,
            "window_evidence": [],
        },
        "requested_window_count": 0,
        "generated_window_count": 0,
        "failed_window_count": 0,
        "total_selected_count": 0,
        "opportunity_event_count": 0,
        "risk_action_counts": {},
        "risk_level_counts": {},
        "feedback": {
            "profile_count": 0,
            "sample_count": 0,
            "selected_with_feedback_count": 0,
            "selected_count": 0,
            "risk_gate_profile_count": 0,
            "risk_gate_used_count": 0,
            "risk_gate_applied_count": 0,
            "risk_gate_tightened_count": 0,
            "risk_gate_supportive_count": 0,
            "risk_gate_overly_conservative_count": 0,
        },
        "monitor_activation": {
            "status_counts": {},
            "created_count": 0,
            "updated_count": 0,
            "running_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
            "items": [],
        },
        "llm_ready_window_count": 0,
        "settlement": {
            "skipped": True,
            "settled_count": 0,
            "error_count": 0,
            "feedback_refresh": _summarize_settlement_feedback_refresh({"settled": [], "errors": [], "skipped": True}),
            "feedback_replay": _build_settlement_feedback_replay(
                _summarize_settlement_feedback_refresh({"settled": [], "errors": [], "skipped": True}),
                [],
            ),
        },
        "generated": [],
        "errors": [],
        "created_at": now_value,
        "updated_at": now_value,
    }


def _persist_closed_loop_audit(db: Session, audit: dict[str, Any]) -> None:
    if not audit:
        return
    try:
        audit_id = str(audit.get("audit_id") or uuid4().hex)
        payload = dict(audit)
        payload["audit_id"] = audit_id
        trigger_name = str(payload.get("trigger") or "").strip() or "unknown"
        status = str(payload.get("status") or "unknown").strip() or "unknown"
        trade_date = payload.get("trade_date")
        created_at = _parse_datetime_or_none(payload.get("created_at")) or _utcnow()
        updated_at = _parse_datetime_or_none(payload.get("updated_at")) or created_at
        payload["created_at"] = _iso(created_at)
        payload["updated_at"] = _iso(updated_at)
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_closed_loop_audits (
                    audit_id, trade_date, trigger_name, status, audit_json, created_at, updated_at
                ) VALUES (
                    :audit_id, :trade_date, :trigger_name, :status, :audit_json, :created_at, :updated_at
                )
                """
            ),
            {
                "audit_id": audit_id,
                "trade_date": trade_date,
                "trigger_name": trigger_name,
                "status": status,
                "audit_json": json.dumps(payload, ensure_ascii=False, default=str),
                "created_at": created_at,
                "updated_at": updated_at,
            },
        )
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        logger.exception("[catalyst-selection] failed to persist closed-loop audit")


def _ensure_audit_requirement_checks(payload: dict[str, Any]) -> dict[str, Any]:
    checks = payload.get("requirement_checks") if isinstance(payload.get("requirement_checks"), list) else []
    feedback_check = next((item for item in checks if isinstance(item, dict) and item.get("id") == "feedback_learning"), {})
    feedback_metrics = feedback_check.get("metrics") if isinstance(feedback_check.get("metrics"), dict) else {}
    settlement = payload.get("settlement") if isinstance(payload.get("settlement"), dict) else {}
    feedback_payload = payload.get("feedback") if isinstance(payload.get("feedback"), dict) else {}
    if (
        payload.get("requirement_summary")
        and checks
        and "feedback_refresh" in settlement
        and "settlement_feedback_updated_profile_count" in feedback_metrics
        and "realtime_feedback_replay_matched_count" in feedback_metrics
        and "learning_impact_active_count" in feedback_metrics
        and "realtime_replay" in feedback_payload
    ):
        return payload
    generated = payload.get("generated") if isinstance(payload.get("generated"), list) else []
    requested_count = int(payload.get("requested_window_count") or len(generated) or 0)
    windows = [
        str(item.get("window") or "").strip()
        for item in generated
        if isinstance(item, dict) and str(item.get("window") or "").strip()
    ]
    while len(windows) < requested_count:
        windows.append(f"window_{len(windows) + 1}")
    settled_count = max(0, int(settlement.get("settled_count") or 0))
    settlement_error_count = max(0, int(settlement.get("error_count") or 0))
    settlement_feedback_refresh = settlement.get("feedback_refresh") if isinstance(settlement.get("feedback_refresh"), dict) else {}
    settled_payloads = []
    for index in range(settled_count):
        settled_payloads.append({"feedback_refresh": settlement_feedback_refresh} if index == 0 and settlement_feedback_refresh else {})
    errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
    state = _build_closed_loop_requirement_checks(
        windows=windows,
        generated=[item for item in generated if isinstance(item, dict)],
        settlement_refresh={
            "skipped": bool(settlement.get("skipped")),
            "settled": settled_payloads,
            "errors": [{} for _ in range(settlement_error_count)],
        },
        errors=[item for item in errors if isinstance(item, dict)],
        skipped=str(payload.get("status") or "") == "skipped",
        skip_reason=str(payload.get("skip_reason") or "") or None,
    )
    payload["requirement_summary"] = state["summary"]
    payload["requirement_checks"] = state["checks"]
    if settlement_feedback_refresh and "feedback_replay" not in settlement:
        settlement["feedback_replay"] = _build_settlement_feedback_replay(
            settlement_feedback_refresh,
            [item for item in generated if isinstance(item, dict)],
        )
        payload["settlement"] = settlement
    feedback = payload.get("feedback") if isinstance(payload.get("feedback"), dict) else {}
    if generated and "realtime_replay" not in feedback:
        realtime_feedback_replay = _build_realtime_feedback_replay(
            _best_realtime_feedback_summary_from_generated([item for item in generated if isinstance(item, dict)]),
            [item for item in generated if isinstance(item, dict)],
        )
        feedback["realtime_replay"] = realtime_feedback_replay
        feedback["realtime_replay_matched_count"] = int(realtime_feedback_replay.get("matched_selection_count") or 0)
        feedback["realtime_replay_score_changed_count"] = int(realtime_feedback_replay.get("score_changed_count") or 0)
        feedback["realtime_replay_rank_changed_count"] = int(realtime_feedback_replay.get("rank_changed_count") or 0)
        feedback["realtime_replay_risk_changed_count"] = int(realtime_feedback_replay.get("risk_changed_count") or 0)
        payload["feedback"] = feedback
    return payload


def list_closed_loop_audits(db: Session, *, limit: int = 10, trade_date: str | None = None) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    base_sql = """
        SELECT audit_id, trade_date, trigger_name, status, audit_json, created_at, updated_at
        FROM catalyst_selection_closed_loop_audits
    """
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 10), 50))}
    if trade_date:
        base_sql += " WHERE trade_date = :trade_date"
        params["trade_date"] = trade_date
    base_sql += " ORDER BY created_at DESC LIMIT :limit"
    rows = db.execute(text(base_sql), params).mappings().all()
    items: list[dict[str, Any]] = []
    for row in rows:
        payload = _loads(row.get("audit_json"), {})
        if not isinstance(payload, dict):
            payload = {}
        payload = _ensure_audit_requirement_checks(payload)
        payload.update(
            {
                "audit_id": row["audit_id"],
                "trade_date": row.get("trade_date"),
                "trigger": row.get("trigger_name"),
                "status": row.get("status"),
                "created_at": _iso(row.get("created_at")) if row.get("created_at") else str(payload.get("created_at") or ""),
                "updated_at": _iso(row.get("updated_at")) if row.get("updated_at") else str(payload.get("updated_at") or ""),
            }
        )
        items.append(payload)
    return {
        "items": items,
        "updated_at": items[0].get("updated_at") if items else _utcnow().isoformat(),
    }


def get_learning_replay(db: Session, *, trade_date: str | None = None, limit: int = 20) -> dict[str, Any]:
    """Return current evidence that feedback learning affected ranking or risk."""
    ensure_catalyst_selection_tables(db)
    bounded_limit = max(1, min(int(limit or 20), 80))
    resolved_trade_date = str(trade_date or "").strip()
    if not resolved_trade_date:
        resolved_trade_date = str(
            db.execute(
                text(
                    """
                    SELECT max(trade_date)
                    FROM catalyst_selection_runs
                    WHERE score_version = :score_version
                    """
                ),
                {"score_version": SCORE_VERSION},
            ).scalar()
            or ""
        )
    if not resolved_trade_date:
        return _empty_learning_replay(status="no_selection", trade_date=None, limit=bounded_limit)

    rows = db.execute(
        text(
            """
            SELECT r.run_id, r.trade_date, r.window_label, r.updated_at AS run_updated_at,
                   i.rank, i.symbol, i.name, i.score, i.adaptive_feedback_score,
                   i.risk_control_json, i.closed_loop_trace_json
            FROM catalyst_selection_runs r
            JOIN catalyst_selection_items i ON i.run_id = r.run_id
            WHERE r.trade_date = :trade_date
              AND r.score_version = :score_version
            ORDER BY r.updated_at DESC, r.window_label, i.rank
            """
        ),
        {"trade_date": resolved_trade_date, "score_version": SCORE_VERSION},
    ).mappings().all()
    if not rows:
        return _empty_learning_replay(status="no_selection", trade_date=resolved_trade_date, limit=bounded_limit)

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        window = str(row.get("window_label") or "").strip() or "unknown"
        group = grouped.setdefault(
            window,
            {
                "window": window,
                "run_id": row.get("run_id"),
                "trade_date": row.get("trade_date"),
                "updated_at": _iso(row.get("run_updated_at")),
                "items": [],
            },
        )
        group["items"].append(
            {
                "rank": int(row.get("rank") or 0),
                "symbol": row.get("symbol"),
                "name": row.get("name") or row.get("symbol"),
                "score": _round_or_none(row.get("score"), 2),
                "adaptive_feedback_score": _round_or_none(row.get("adaptive_feedback_score"), 2),
                "risk_control": _loads(row.get("risk_control_json"), {}),
                "closed_loop_trace": _loads(row.get("closed_loop_trace_json"), {}),
            }
        )

    window_summaries: list[dict[str, Any]] = []
    all_impacts: list[dict[str, Any]] = []
    for window, group in sorted(grouped.items(), key=lambda pair: (pair[0] != "premarket", pair[0])):
        impacts = [
            {**impact, "window": window, "run_id": group.get("run_id"), "run_updated_at": group.get("updated_at")}
            for impact in _top_learning_impacts(group["items"], limit=max(len(group["items"]), bounded_limit))
        ]
        all_impacts.extend(impacts)
        summary = _summarize_learning_replay_impacts(impacts)
        window_summaries.append(
            {
                "window": window,
                "run_id": group.get("run_id"),
                "updated_at": group.get("updated_at"),
                "item_count": len(group["items"]),
                **summary,
                "top_symbols": [
                    {
                        "symbol": impact.get("symbol"),
                        "name": impact.get("name"),
                        "score_delta_from_learning_policy": impact.get("score_delta_from_learning_policy"),
                        "rank_delta_from_learning_policy": impact.get("rank_delta_from_learning_policy"),
                    }
                    for impact in impacts[:5]
                ],
            }
        )

    summary = _summarize_learning_replay_impacts(all_impacts)
    status = _learning_replay_status(summary)
    audits = list_closed_loop_audits(db, trade_date=resolved_trade_date, limit=1).get("items") or []
    latest_audit = audits[0] if audits else {}
    settlement = latest_audit.get("settlement") if isinstance(latest_audit.get("settlement"), dict) else {}
    settlement_replay = settlement.get("feedback_replay") if isinstance(settlement.get("feedback_replay"), dict) else {}
    feedback = latest_audit.get("feedback") if isinstance(latest_audit.get("feedback"), dict) else {}
    realtime_replay = feedback.get("realtime_replay") if isinstance(feedback.get("realtime_replay"), dict) else {}
    evidence, gaps = _learning_replay_evidence_and_gaps(
        summary=summary,
        latest_audit=latest_audit,
        settlement_replay=settlement_replay,
        realtime_replay=realtime_replay,
    )
    return {
        "trade_date": resolved_trade_date,
        "status": status,
        "source": "postgresql:catalyst_selection_items+catalyst_selection_closed_loop_audits",
        "score_version": SCORE_VERSION,
        "feedback_model_version": FEEDBACK_MODEL_VERSION,
        "realtime_feedback_model_version": REALTIME_FEEDBACK_MODEL_VERSION,
        "audit_id": latest_audit.get("audit_id"),
        "audit_created_at": latest_audit.get("created_at"),
        **summary,
        "windows": window_summaries,
        "items": _sort_learning_replay_impacts(all_impacts)[:bounded_limit],
        "settlement_feedback_replay": settlement_replay,
        "realtime_feedback_replay": realtime_replay,
        "evidence": evidence,
        "gaps": gaps,
        "updated_at": _utcnow().isoformat(),
    }


def _empty_learning_replay(*, status: str, trade_date: str | None, limit: int) -> dict[str, Any]:
    del limit
    summary = _summarize_learning_replay_impacts([])
    return {
        "trade_date": trade_date,
        "status": status,
        "source": "postgresql:catalyst_selection_items+catalyst_selection_closed_loop_audits",
        "score_version": SCORE_VERSION,
        "feedback_model_version": FEEDBACK_MODEL_VERSION,
        "realtime_feedback_model_version": REALTIME_FEEDBACK_MODEL_VERSION,
        "audit_id": None,
        "audit_created_at": None,
        **summary,
        "windows": [],
        "items": [],
        "settlement_feedback_replay": {},
        "realtime_feedback_replay": {},
        "evidence": [],
        "gaps": ["缺少可回放的催化选股运行记录。"],
        "updated_at": _utcnow().isoformat(),
    }


def _sort_learning_replay_impacts(impacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _sort_key(item: dict[str, Any]) -> tuple[int, float, float, float]:
        active = 1 if str(item.get("status") or "") == "active" else 0
        score_delta = abs(float(_num(item.get("score_delta_from_learning_policy")) or 0.0))
        rank_delta = abs(float(_num(item.get("rank_delta_from_learning_policy")) or 0.0))
        position_delta = abs(float(_num((item.get("risk_effect") or {}).get("max_position_delta_pct")) or 0.0)) if isinstance(item.get("risk_effect"), dict) else 0.0
        return (active, score_delta, rank_delta, position_delta)

    return sorted(impacts, key=_sort_key, reverse=True)


def _summarize_learning_replay_impacts(impacts: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    profile_scope_counts: Counter[str] = Counter()
    score_deltas: list[float] = []
    rank_deltas: list[int] = []
    position_deltas: list[float] = []
    score_changed_count = 0
    rank_changed_count = 0
    risk_changed_count = 0
    gate_applied_count = 0
    action_changed_count = 0
    improved_rank_count = 0
    reduced_rank_count = 0
    symbols: set[str] = set()
    for impact in impacts:
        status = str(impact.get("status") or "unknown").strip() or "unknown"
        status_counts[status] += 1
        symbol = str(impact.get("symbol") or "").strip()
        if symbol:
            symbols.add(symbol)
        profiles = impact.get("profiles") if isinstance(impact.get("profiles"), dict) else {}
        for scope, value in profiles.items():
            if str(value or "").strip():
                profile_scope_counts[str(scope)] += 1
        score_delta = _num(impact.get("score_delta_from_learning_policy"))
        if score_delta is not None:
            score_deltas.append(score_delta)
            if abs(score_delta) >= 0.01:
                score_changed_count += 1
        rank_delta_raw = impact.get("rank_delta_from_learning_policy")
        if rank_delta_raw is not None:
            try:
                rank_delta = int(rank_delta_raw)
            except Exception:
                rank_delta = 0
            rank_deltas.append(rank_delta)
            if abs(rank_delta) >= 1:
                rank_changed_count += 1
            if rank_delta > 0:
                improved_rank_count += 1
            elif rank_delta < 0:
                reduced_rank_count += 1
        risk_effect = impact.get("risk_effect") if isinstance(impact.get("risk_effect"), dict) else {}
        gate_effect = impact.get("risk_gate_effect") if isinstance(impact.get("risk_gate_effect"), dict) else {}
        position_delta = _num(risk_effect.get("max_position_delta_pct"))
        if position_delta is not None:
            position_deltas.append(position_delta)
        action_changed = bool(risk_effect.get("action_changed"))
        gate_applied = bool(gate_effect.get("applied"))
        if action_changed:
            action_changed_count += 1
        if gate_applied:
            gate_applied_count += 1
        if action_changed or gate_applied or abs(float(position_delta or 0.0)) >= 0.01:
            risk_changed_count += 1
    return {
        "candidate_impact_count": len(impacts),
        "active_impact_count": int(status_counts.get("active", 0)),
        "unique_symbol_count": len(symbols),
        "status_counts": dict(status_counts),
        "profile_scope_counts": dict(profile_scope_counts),
        "score_changed_count": score_changed_count,
        "rank_changed_count": rank_changed_count,
        "risk_changed_count": risk_changed_count,
        "gate_applied_count": gate_applied_count,
        "action_changed_count": action_changed_count,
        "improved_rank_count": improved_rank_count,
        "reduced_rank_count": reduced_rank_count,
        "average_score_delta": round(sum(score_deltas) / len(score_deltas), 2) if score_deltas else None,
        "max_abs_score_delta": round(max((abs(value) for value in score_deltas), default=0.0), 2) if score_deltas else None,
        "average_rank_delta": round(sum(rank_deltas) / len(rank_deltas), 2) if rank_deltas else None,
        "average_max_position_delta_pct": round(sum(position_deltas) / len(position_deltas), 2) if position_deltas else None,
    }


def _learning_replay_status(summary: dict[str, Any]) -> str:
    if int(summary.get("candidate_impact_count") or 0) <= 0:
        return "no_learning_impact"
    if int(summary.get("active_impact_count") or 0) <= 0:
        return "warming_up"
    if (
        int(summary.get("score_changed_count") or 0) > 0
        or int(summary.get("rank_changed_count") or 0) > 0
        or int(summary.get("risk_changed_count") or 0) > 0
    ):
        return "active"
    return "observed"


def _learning_replay_evidence_and_gaps(
    *,
    summary: dict[str, Any],
    latest_audit: dict[str, Any],
    settlement_replay: dict[str, Any],
    realtime_replay: dict[str, Any],
) -> tuple[list[str], list[str]]:
    evidence: list[str] = []
    gaps: list[str] = []
    candidate_count = int(summary.get("candidate_impact_count") or 0)
    active_count = int(summary.get("active_impact_count") or 0)
    if candidate_count:
        evidence.append(f"候选学习影响 {candidate_count} 个，active {active_count} 个")
        evidence.append(
            "反馈改分 {score} 个，改名次 {rank} 个，改风控 {risk} 个".format(
                score=int(summary.get("score_changed_count") or 0),
                rank=int(summary.get("rank_changed_count") or 0),
                risk=int(summary.get("risk_changed_count") or 0),
            )
        )
    else:
        gaps.append("当前候选没有 learning_impact 轨迹，无法证明反馈进入排序。")
    if latest_audit.get("audit_id"):
        evidence.append(f"关联闭环审计 {latest_audit.get('audit_id')}")
    else:
        gaps.append("缺少同交易日闭环审计记录。")
    if settlement_replay:
        replay_status = str(settlement_replay.get("status") or "").strip()
        replay_candidate_count = int(settlement_replay.get("candidate_impact_count") or 0)
        evidence.append(f"结算反哺回放 {replay_status or 'unknown'}，候选影响 {replay_candidate_count} 个")
        realtime_status = str((realtime_replay or {}).get("status") or "").strip()
        if replay_status in {"no_profile_change", "unmatched"} and realtime_status not in {"active", "matched"}:
            gaps.append("最近结算未产生可匹配的新画像变化；当前回放展示已有画像对候选的影响。")
    if realtime_replay:
        realtime_status = str(realtime_replay.get("status") or "").strip()
        evidence.append(
            "实时反哺回放 {status}，命中 {matched} 个，改分 {score}，改名次 {rank}，改风控 {risk}".format(
                status=realtime_status or "unknown",
                matched=int(realtime_replay.get("matched_selection_count") or 0),
                score=int(realtime_replay.get("score_changed_count") or 0),
                rank=int(realtime_replay.get("rank_changed_count") or 0),
                risk=int(realtime_replay.get("risk_changed_count") or 0),
            )
        )
        if realtime_status in {"no_realtime_feedback", "no_learning_impact", "unmatched"}:
            gaps.append("实时反馈尚未匹配到候选学习影响。")
    return evidence, gaps


def generate_selections(
    db: Session,
    *,
    trade_date: str,
    window: str = "premarket",
    limit: int = DEFAULT_SELECTION_LIMIT,
    user_id: str | None = None,
    trigger_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    normalized_window = _normalize_window(window)
    bounded_limit = max(1, min(int(limit or DEFAULT_SELECTION_LIMIT), MAX_SELECTION_LIMIT))
    now_value = _utcnow()
    anchor_now = _selection_anchor_now(trade_date, normalized_window)
    news_window_start, news_window_end = news_theme_service.resolve_news_window_range(normalized_window, anchor_now)
    feature_trade_date = _feature_trade_date_for_selection(db, trade_date)
    event_reaction_trade_date = _event_reaction_trade_date(trade_date, normalized_window)
    ranking_payload = news_theme_service.list_theme_rankings(
        db,
        window=normalized_window,
        limit=20,
        include_evidence=True,
        user_id=user_id,
        now=anchor_now,
        force_sync_llm=True,
        trigger_context=trigger_context,
    )
    ranking_governance = ranking_payload.get("data_governance") if isinstance(ranking_payload.get("data_governance"), dict) else {}
    llm_core_stock_governance = _safe_llm_runtime_payload(
        ranking_governance.get("llm_core_stock") if isinstance(ranking_governance.get("llm_core_stock"), dict) else {}
    )
    trigger_news_context = _trigger_news_context_from_trigger_context(trigger_context)
    theme_items = [
        item for item in ranking_payload.get("items") or []
        if float(item.get("score") or 0.0) >= MIN_THEME_SCORE
    ]
    theme_items = _apply_trigger_news_context_to_theme_items(theme_items, trigger_news_context)
    market_snapshot = _load_market_snapshot(db, feature_trade_date)
    if not market_snapshot.get("market_stats"):
        raise ValueError(f"交易日 {feature_trade_date} 缺少市场日线数据，无法生成催化选股")
    market_behavior = interpret_market_behavior(market_snapshot)
    raw_theme_count = len(theme_items)
    theme_items = _mainline_aligned_theme_items(
        theme_items,
        market_snapshot=market_snapshot,
        market_behavior=market_behavior,
    )
    symbols = _candidate_symbols_from_themes(theme_items)
    if not symbols:
        symbols = _symbols_from_daily_data(db, trade_date=feature_trade_date, limit=200)
    daily_features = _load_daily_features(db, symbols=symbols, trade_date=feature_trade_date)
    if not daily_features:
        raise ValueError(f"交易日 {feature_trade_date} 缺少日线数据，无法生成催化选股")
    event_reaction_governance = _attach_event_reaction_features(
        db,
        features_by_symbol=daily_features,
        theme_items=theme_items,
        trade_date=event_reaction_trade_date,
        feature_trade_date=feature_trade_date,
        user_id=user_id,
        selection_trade_date=trade_date,
        selection_window=normalized_window,
        selection_limit=bounded_limit,
    )
    intraday_event_pulse = _build_intraday_event_pulse(daily_features)
    minute_market_proxy = _load_minute_market_proxy(db, event_reaction_trade_date)
    market_state_freshness = _build_market_state_freshness(
        feature_trade_date=feature_trade_date,
        event_reaction_trade_date=event_reaction_trade_date,
        event_reaction_governance=event_reaction_governance,
        minute_market_proxy=minute_market_proxy,
    )
    market_state_freshness["intraday_event_pulse"] = intraday_event_pulse
    market_behavior = _apply_intraday_market_state_to_behavior(
        market_behavior,
        market_state_freshness=market_state_freshness,
        minute_market_proxy=minute_market_proxy,
    )
    market_behavior["intraday_event_pulse"] = intraday_event_pulse
    market_behavior["minute_market_proxy"] = minute_market_proxy
    market_behavior["market_state_freshness"] = market_state_freshness
    market_background = _build_market_background(
        trade_date=trade_date,
        window=normalized_window,
        news_window_start=news_window_start,
        news_window_end=news_window_end,
        theme_items=theme_items,
        market_behavior=market_behavior,
        market_state_freshness=market_state_freshness,
    )
    previous_state = _load_previous_selection_state(db, symbols=list(daily_features.keys()), trade_date=trade_date)
    history_stats = _load_symbol_settlement_stats(db, symbols=list(daily_features.keys()), trade_date=trade_date)
    theme_feedback = _load_theme_settlement_stats(db, trade_date=trade_date)
    feedback_as_of_trade_date = _realtime_feedback_lookup_trade_date(
        trade_date=trade_date,
        event_reaction_trade_date=event_reaction_trade_date,
        window=normalized_window,
    )
    feedback_profiles = _load_feedback_profiles(
        db,
        symbols=list(daily_features.keys()),
        themes=[str(item.get("theme") or "").strip() for item in theme_items],
        event_types=_event_type_keys_from_theme_items(theme_items),
        risk_gates=list(RISK_EXECUTION_GATES),
        intraday_pulses=[str(intraday_event_pulse.get("status") or "").strip()],
        as_of_trade_date=feedback_as_of_trade_date,
    )
    pulse_feedback = (feedback_profiles.get("intraday_pulses") or {}).get(str(intraday_event_pulse.get("status") or "").strip())
    if pulse_feedback:
        intraday_event_pulse["feedback_profile"] = pulse_feedback
        market_behavior["intraday_event_pulse_feedback"] = pulse_feedback
    history_stats = _merge_symbol_feedback_profiles(history_stats, feedback_profiles.get("symbols") or {})
    theme_feedback = _merge_theme_feedback_profiles(theme_feedback, feedback_profiles.get("themes") or {})
    theme_items = _attach_event_feedback_profiles(theme_items, feedback_profiles.get("event_types") or {})
    scored = [
        _score_candidate(
            symbol=symbol,
            features=features,
            theme_items=theme_items,
            previous_state=previous_state.get(symbol, {}),
            history_stats=history_stats.get(symbol, {}),
            theme_feedback=theme_feedback,
            market_background=market_background,
            market_behavior=market_behavior,
            risk_gate_feedback=feedback_profiles.get("risk_gates") or {},
            trigger_news_context=trigger_news_context,
        )
        for symbol, features in daily_features.items()
    ]
    scored = [item for item in scored if item["score"] > 0]
    baseline_rank_by_symbol = _baseline_learning_rank_by_symbol(scored)
    scored.sort(key=lambda item: (-float(item["score"]), float(item["risk_penalty"]), item["symbol"]))
    selected = scored[:bounded_limit]
    for rank, item in enumerate(selected, start=1):
        item["rank"] = rank
        _attach_learning_rank_impact(item, final_rank=rank, baseline_rank=baseline_rank_by_symbol.get(str(item.get("symbol") or "")))
    selected_mainline_theme = _selected_mainline_theme_summary(selected, theme_items=theme_items)
    market_background = _build_market_background(
        trade_date=trade_date,
        window=normalized_window,
        news_window_start=news_window_start,
        news_window_end=news_window_end,
        theme_items=theme_items,
        market_behavior=market_behavior,
        market_state_freshness=market_state_freshness,
        selected_items=selected,
    )
    for item in selected:
        item["market_background"] = market_background
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        market_trace = trace.get("market") if isinstance(trace.get("market"), dict) else None
        if market_trace is not None:
            market_trace["background"] = market_background
    previous_snapshot = _load_existing_selection_snapshot(db, trade_date=trade_date, window=normalized_window)
    opportunity_events = _build_opportunity_events(
        items=selected,
        previous_snapshot=previous_snapshot,
    )
    opportunity_by_symbol = {
        str(event.get("symbol") or ""): event
        for event in opportunity_events
        if event.get("symbol")
    }
    for item in selected:
        event = opportunity_by_symbol.get(str(item.get("symbol") or ""))
        if not event:
            continue
        trace = item.setdefault("closed_loop_trace", {})
        if isinstance(trace, dict):
            trace["opportunity_event"] = event
    _attach_candidate_llm_event_runtime(selected, llm_core_stock_governance)
    score_profiles = [
        str(((item.get("closed_loop_trace") or {}).get("scoring") or {}).get("profile") or "")
        for item in selected
    ]
    feedback_learning_state = _build_feedback_learning_state(feedback_profiles, selected)
    risk_control_summary = _build_risk_control_summary(selected)
    risk_monitoring_summary = _build_risk_monitoring_summary(selected)
    learning_adjustment_summary = _build_learning_adjustment_summary(selected)
    learning_impact_summary = _build_learning_impact_summary(selected)
    risk_gate_feedback_summary = _build_risk_gate_feedback_summary(selected)
    realtime_feedback_summary = summarize_realtime_feedback(db, trade_date=feedback_as_of_trade_date)
    end_to_end_evidence = _build_ai_quant_end_to_end_evidence(
        trigger_context=trigger_context,
        selected=selected,
        opportunity_events=opportunity_events,
        llm_runtime=llm_core_stock_governance,
        market_state_freshness=market_state_freshness,
        risk_control_summary=risk_control_summary,
        feedback_learning_state=feedback_learning_state,
        learning_adjustment_summary=learning_adjustment_summary,
        learning_impact_summary=learning_impact_summary,
        realtime_feedback_summary=realtime_feedback_summary,
    )

    data_governance = {
        "score_version": SCORE_VERSION,
        "raw_theme_count": raw_theme_count,
        "theme_count": len(theme_items),
        "candidate_symbol_count": len(symbols),
        "selected_count": len(selected),
        "trade_date": trade_date,
        "feature_trade_date": feature_trade_date,
        "window": normalized_window,
        "event_reaction_trade_date": event_reaction_trade_date,
        "feedback_as_of_trade_date": feedback_as_of_trade_date,
        "market_state_freshness": market_state_freshness,
        "intraday_event_pulse": intraday_event_pulse,
        "minute_market_proxy": minute_market_proxy,
        "llm_core_stock": llm_core_stock_governance,
        "fresh_news_trigger": {
            "event_count": int(trigger_news_context.get("event_count") or 0),
            "included_count": int(trigger_news_context.get("included_count") or 0),
            "summary": trigger_news_context.get("summary") or {},
            "events": trigger_news_context.get("events") or [],
        },
        "news_time_window": {
            "policy": "premarket_cutoff_09:25" if normalized_window == "premarket" else "rolling_window",
            "anchor_now": _iso(anchor_now),
            "window_start": _iso(news_window_start),
            "window_end": _iso(news_window_end),
        },
        "mainline_themes": [
            {
                "theme": item.get("theme"),
                "score": round(float(item.get("score") or 0.0), 2),
                "mainline_alignment_score": round(float(item.get("mainline_alignment_score") or 0.0), 2),
                "reasons": item.get("mainline_alignment_reasons") or [],
            }
            for item in theme_items[:8]
        ],
        "selected_mainline_theme": selected_mainline_theme,
        "filtered_out_theme_count": max(raw_theme_count - len(theme_items), 0),
        "closed_loop": {
            "event_understanding": True,
            "llm_event_understanding": llm_core_stock_governance,
            "market_state": bool(market_behavior),
            "market_state_freshness": market_state_freshness,
            "intraday_event_pulse": intraday_event_pulse,
            "minute_market_proxy": minute_market_proxy,
            "event_market_reaction": event_reaction_governance,
            "news_trigger_context": {
                "event_count": int(trigger_news_context.get("event_count") or 0),
                "included_count": int(trigger_news_context.get("included_count") or 0),
                "summary": trigger_news_context.get("summary") or {},
            },
            "proactive_opportunity_detection": True,
            "opportunity_event_count": len(opportunity_events),
            "dynamic_ranking": True,
            "adaptive_scoring": True,
            "score_profile_counts": {
                profile: score_profiles.count(profile)
                for profile in sorted(set(score_profiles))
                if profile
            },
            "risk_control": True,
            "risk_control_summary": risk_control_summary,
            "risk_monitoring_summary": risk_monitoring_summary,
            "learning_adjustment_summary": learning_adjustment_summary,
            "learning_impact_summary": learning_impact_summary,
            "feedback_learning": True,
            "feedback_theme_count": len(theme_feedback),
            "feedback_model_version": FEEDBACK_MODEL_VERSION,
            "feedback_profile_count": int(feedback_profiles.get("profile_count") or 0),
            "feedback_sample_count": int(feedback_profiles.get("sample_count") or 0),
            "feedback_event_type_count": len(feedback_profiles.get("event_types") or {}),
            "feedback_risk_gate_count": len(feedback_profiles.get("risk_gates") or {}),
            "feedback_intraday_pulse_count": len(feedback_profiles.get("intraday_pulses") or {}),
            "feedback_profile_updated_at": feedback_profiles.get("latest_updated_at"),
            "feedback_learning_state": feedback_learning_state,
            "risk_gate_feedback_summary": risk_gate_feedback_summary,
            "realtime_feedback": realtime_feedback_summary,
            "end_to_end_evidence": end_to_end_evidence,
        },
        "opportunity_events": opportunity_events,
    }

    run_id = uuid4().hex
    _persist_selection_run(
        db,
        run_id=run_id,
        trade_date=trade_date,
        window=normalized_window,
        window_start=_latest_window_start(theme_items),
        window_end=_latest_window_end(theme_items),
        market_background=market_background,
        market_behavior=market_behavior,
        items=selected,
        data_governance=data_governance,
        opportunity_events=opportunity_events,
        now_value=now_value,
    )
    db.commit()
    return {
        "trade_date": trade_date,
        "window": normalized_window,
        "updated_at": now_value.isoformat(),
        "source": "postgresql:market_news_items+theme_rankings+stock_daily_kline+settlement_feedback_profiles",
        "message": _selection_message(normalized_window),
        "items": selected,
        "market_background": market_background,
        "market_behavior_labels": market_behavior,
        "data_governance": data_governance,
    }


def list_history(db: Session, *, limit: int = 30) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    current_trade_date = _effective_cn_trade_date()
    rows = db.execute(
        text(
            """
            SELECT
                r.trade_date,
                r.item_count,
                r.source,
                r.updated_at,
                i.symbol AS top_symbol,
                i.name AS top_name,
                COUNT(s.symbol) FILTER (WHERE s.protected) AS protected_count,
                AVG(s.change_pct) AS average_change_pct,
                AVG(CASE WHEN s.outcome IN ('hit', 'strong_hit') THEN 1 ELSE 0 END) AS hit_rate
            FROM catalyst_selection_runs r
            LEFT JOIN catalyst_selection_items i
              ON i.run_id = r.run_id AND i.rank = 1
            LEFT JOIN catalyst_selection_settlements s
              ON s.trade_date = r.trade_date
            WHERE r.trade_date <= :current_trade_date
              AND r.score_version = :score_version
              AND r.window_label = 'premarket'
            GROUP BY r.trade_date, r.item_count, r.source, r.updated_at, i.symbol, i.name
            ORDER BY r.trade_date DESC
            LIMIT :limit
            """
        ),
        {
            "current_trade_date": current_trade_date,
            "score_version": SCORE_VERSION,
            "limit": max(1, min(int(limit or 30), 120)),
        },
    ).mappings().all()
    return {
        "items": [
            {
                "trade_date": row["trade_date"],
                "item_count": int(row["item_count"] or 0),
                "top_symbol": row["top_symbol"],
                "top_name": row["top_name"],
                "average_change_pct": _round_or_none(row["average_change_pct"], 4),
                "hit_rate": _round_or_none((float(row["hit_rate"]) * 100) if row["hit_rate"] is not None else None, 2),
                "protected_count": int(row["protected_count"] or 0),
                "data_source": row["source"],
                "updated_at": _iso(row["updated_at"]),
            }
            for row in rows
        ],
        "updated_at": _utcnow().isoformat(),
    }


def list_opportunity_events(
    db: Session,
    *,
    trade_date: str | None = None,
    window: str | None = None,
    symbol: str | None = None,
    event_level: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    normalized_window = _normalize_window(window) if window else None
    normalized_symbol = _normalize_symbol(symbol) if symbol else None
    normalized_level = str(event_level or "").strip().upper() or None
    bounded_limit = max(1, min(int(limit or 50), 200))
    rows = db.execute(
        text(
            """
            SELECT *
            FROM catalyst_selection_opportunity_events
            WHERE (:trade_date IS NULL OR trade_date = :trade_date)
              AND (:window IS NULL OR window_label = :window)
              AND (:symbol IS NULL OR symbol = :symbol)
              AND (:event_level IS NULL OR event_level = :event_level)
            ORDER BY trade_date DESC, created_at DESC, rank ASC
            LIMIT :limit
            """
        ),
        {
            "trade_date": trade_date,
            "window": normalized_window,
            "symbol": normalized_symbol,
            "event_level": normalized_level,
            "limit": bounded_limit,
        },
    ).mappings().all()
    return {
        "items": [_row_to_opportunity_event(row) for row in rows],
        "filters": {
            "trade_date": trade_date,
            "window": normalized_window,
            "symbol": normalized_symbol,
            "event_level": normalized_level,
            "limit": bounded_limit,
        },
        "updated_at": _utcnow().isoformat(),
    }


def build_monitor_pool(
    db: Session,
    *,
    trade_date: str | None = None,
    window: str = "24h",
    limit: int = DEFAULT_SELECTION_LIMIT,
    force: bool = False,
    user_id: str | None = None,
) -> dict[str, Any]:
    selection = list_or_generate_selections(
        db,
        trade_date=trade_date,
        window=window,
        limit=limit,
        force=force,
        user_id=user_id,
    )
    return build_monitor_pool_from_selection(selection)


def build_monitor_pool_from_selection(selection: dict[str, Any]) -> dict[str, Any]:
    items = list(selection.get("items") or [])
    monitor_symbols: list[str] = []
    entry_symbols: list[str] = []
    confirm_symbols: list[str] = []
    blocked_symbols: list[str] = []
    reduce_only_symbols: list[str] = []
    candidates: list[dict[str, Any]] = []
    risk_by_symbol: dict[str, Any] = {}
    max_position_by_symbol: dict[str, float] = {}
    stop_loss_by_symbol: dict[str, float] = {}
    gate_counts: Counter[str] = Counter()

    for item in items:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        gate = str(monitor.get("execution_gate") or "unknown").strip() or "unknown"
        action = str(risk_control.get("action") or "").strip() or "observe"
        gate_counts[gate] += 1
        max_position = _num(risk_control.get("max_position_pct"))
        stop_loss = _num(risk_control.get("stop_loss_pct"))
        if max_position is not None:
            max_position_by_symbol[symbol] = round(float(max_position), 2)
        if stop_loss is not None:
            stop_loss_by_symbol[symbol] = round(float(stop_loss), 2)

        theme_matches = item.get("theme_matches") if isinstance(item.get("theme_matches"), list) else []
        monitor_theme_matches = _monitor_pool_theme_matches(theme_matches)
        themes = _dedupe_strings(match.get("theme") for match in monitor_theme_matches if isinstance(match, dict))
        event_types = _dedupe_strings(
            _event_type_from_match(match)
            for match in monitor_theme_matches
            if isinstance(match, dict)
        )
        monitor_symbols.append(symbol)
        if gate in {"allow", "allow_probe"}:
            entry_symbols.append(symbol)
        elif gate == "confirm":
            confirm_symbols.append(symbol)
        elif gate == "reduce_only":
            reduce_only_symbols.append(symbol)
        elif gate == "blocked":
            blocked_symbols.append(symbol)

        candidate = {
            "rank": int(item.get("rank") or 0),
            "symbol": symbol,
            "name": item.get("name") or symbol,
            "score": _round_or_none(item.get("score"), 2),
            "action": action,
            "execution_gate": gate,
            "risk_level": risk_control.get("risk_level"),
            "max_position_pct": _round_or_none(max_position, 2),
            "stop_loss_pct": _round_or_none(stop_loss, 2),
            "take_profit_pct": _round_or_none(risk_control.get("take_profit_pct"), 2),
            "next_action": monitor.get("next_action") or risk_control.get("next_action"),
            "adaptive_feedback_score": _round_or_none(item.get("adaptive_feedback_score"), 2),
            "risk_penalty": _round_or_none(item.get("risk_penalty"), 2),
            "themes": themes,
            "event_types": event_types,
            "primary_theme": themes[0] if themes else None,
            "primary_event_type": event_types[0] if event_types else None,
            "theme_matches": monitor_theme_matches,
            "reason_parts": list(item.get("reason_parts") or [])[:4],
            "risk_flags": list(item.get("risk_flags") or [])[:4],
            "signal_flags": list(item.get("signal_flags") or [])[:4],
        }
        candidates.append(candidate)
        risk_by_symbol[symbol] = {
            key: value
            for key, value in candidate.items()
            if key in {
                "action",
                "execution_gate",
                "risk_level",
                "max_position_pct",
                "stop_loss_pct",
                "take_profit_pct",
                "next_action",
                "risk_penalty",
            }
        }

    monitor_symbols = _dedupe_strings(monitor_symbols)
    entry_symbols = _dedupe_strings(entry_symbols)
    confirm_symbols = _dedupe_strings(confirm_symbols)
    blocked_symbols = _dedupe_strings(blocked_symbols)
    reduce_only_symbols = _dedupe_strings(reduce_only_symbols)
    watch_symbols = monitor_symbols
    tradable_symbols = _dedupe_strings([*entry_symbols, *confirm_symbols])
    gate_counts_dict = dict(gate_counts)
    monitor_pool = {
        "mode": "manual_only",
        "source": "catalyst-selection",
        "trade_date": selection.get("trade_date"),
        "window": selection.get("window"),
        "gate_counts": gate_counts_dict,
        "symbols": monitor_symbols,
        "watch_symbols": watch_symbols,
        "tradable_symbols": tradable_symbols,
        "manual_symbols": monitor_symbols,
        "entry_symbols": entry_symbols,
        "confirm_symbols": confirm_symbols,
        "blocked_symbols": blocked_symbols,
        "reduce_only_symbols": reduce_only_symbols,
        "candidates": candidates,
    }
    risk_config = {
        "source": "catalyst-selection",
        "suggested_execution_mode": "monitor_only",
        "gate_counts": gate_counts_dict,
        "execution_gates": {symbol: data.get("execution_gate") for symbol, data in risk_by_symbol.items()},
        "watch_symbols": watch_symbols,
        "tradable_symbols": tradable_symbols,
        "risk_by_symbol": risk_by_symbol,
        "max_position_pct_by_symbol": max_position_by_symbol,
        "stop_loss_pct_by_symbol": stop_loss_by_symbol,
        "blocked_symbols": blocked_symbols,
        "reduce_only_symbols": reduce_only_symbols,
    }
    return {
        "trade_date": str(selection.get("trade_date") or ""),
        "window": str(selection.get("window") or ""),
        "updated_at": _utcnow().isoformat(),
        "source": "catalyst-selection",
        "suggested_execution_mode": "monitor_only",
        "monitor_pool": monitor_pool,
        "risk_config": risk_config,
        "summary": {
            "item_count": len(items),
            "monitor_symbol_count": len(monitor_symbols),
            "watch_symbol_count": len(watch_symbols),
            "tradable_symbol_count": len(tradable_symbols),
            "entry_symbol_count": len(entry_symbols),
            "confirm_symbol_count": len(confirm_symbols),
            "blocked_symbol_count": len(blocked_symbols),
            "reduce_only_symbol_count": len(reduce_only_symbols),
            "gate_counts": gate_counts_dict,
        },
    }


def maintain_realtime_monitor_from_selection(
    main_db: Session,
    *,
    selection: dict[str, Any],
    user_id: str | None,
    strategy_id: str | None = None,
    account_key: str | None = None,
    start: bool | None = None,
) -> dict[str, Any]:
    """Create or refresh a monitor-only realtime monitor for a catalyst selection."""
    if not user_id:
        return {"status": "skipped", "reason": "missing_user_id", "enabled": False}
    if not _ai_quant_env_enabled(CATALYST_AUTO_MONITOR_ENABLED, default=False):
        return {"status": "disabled", "reason": "auto_monitor_disabled", "enabled": False}

    pool_payload = build_monitor_pool_from_selection(selection)
    monitor_pool = pool_payload.get("monitor_pool") if isinstance(pool_payload.get("monitor_pool"), dict) else {}
    risk_config = pool_payload.get("risk_config") if isinstance(pool_payload.get("risk_config"), dict) else {}
    monitor_symbols = [
        str(item).strip().upper()
        for item in (
            monitor_pool.get("watch_symbols")
            or monitor_pool.get("manual_symbols")
            or monitor_pool.get("symbols")
            or []
        )
        if str(item).strip()
    ]
    if not monitor_symbols:
        return {
            "status": "skipped",
            "reason": "empty_monitor_pool",
            "enabled": True,
            "monitor_symbol_count": 0,
            "trade_date": pool_payload.get("trade_date"),
            "window": pool_payload.get("window"),
        }

    try:
        from api.core.strategy_db import get_strategy_db_ctx
        from api.models.strategy_models import RealtimeMonitorDB
        from api.services import auth_service, realtime_monitor_service
        from api.services.strategy_platform_repository import list_platform_strategies
    except Exception as exc:
        return {"status": "skipped", "reason": "strategy_runtime_unavailable", "enabled": True, "error": str(exc)[:240]}

    start_monitor = _ai_quant_env_enabled(CATALYST_AUTO_MONITOR_START, default=True) if start is None else bool(start)
    resolved_account_key = _resolve_catalyst_monitor_account_key(main_db, user_id=user_id, explicit=account_key)
    if not resolved_account_key:
        return {
            "status": "skipped",
            "reason": "missing_paper_account",
            "enabled": True,
            "monitor_symbol_count": len(monitor_symbols),
            "trade_date": pool_payload.get("trade_date"),
            "window": pool_payload.get("window"),
        }

    with get_strategy_db_ctx() as strategy_db:
        resolved_strategy_id = _resolve_catalyst_monitor_strategy_id(
            strategy_db,
            explicit=strategy_id,
            list_platform_strategies_fn=list_platform_strategies,
        )
        if not resolved_strategy_id:
            return {
                "status": "skipped",
                "reason": "missing_active_realtime_strategy",
                "enabled": True,
                "account_key": resolved_account_key,
                "monitor_symbol_count": len(monitor_symbols),
                "trade_date": pool_payload.get("trade_date"),
                "window": pool_payload.get("window"),
            }

        existing = _find_existing_catalyst_monitor(
            strategy_db,
            RealtimeMonitorDB=RealtimeMonitorDB,
            user_id=user_id,
            window=str(pool_payload.get("window") or ""),
        )
        cleanup_ids = _stale_catalyst_monitor_ids(
            strategy_db,
            RealtimeMonitorDB=RealtimeMonitorDB,
            user_id=user_id,
            window=str(pool_payload.get("window") or ""),
            keep_id=getattr(existing, "id", None),
        )
        if cleanup_ids:
            realtime_monitor_service.delete_monitor_records(strategy_db, cleanup_ids, user_id=user_id)
        incoming_trade_date = str(pool_payload.get("trade_date") or "")
        if existing is not None:
            existing_pool = existing.monitor_pool_json if isinstance(existing.monitor_pool_json, dict) else {}
            existing_config = existing.config_json if isinstance(existing.config_json, dict) else {}
            existing_trade_date = _catalyst_monitor_trade_date(existing, pool=existing_pool, config=existing_config)
            if existing_trade_date and incoming_trade_date and existing_trade_date > incoming_trade_date:
                if cleanup_ids:
                    strategy_db.commit()
                monitor_payload = realtime_monitor_service.get_monitor(strategy_db, user_id, existing.id)
                return {
                    "status": "skipped_stale_selection",
                    "enabled": True,
                    "monitor_id": monitor_payload.get("id"),
                    "monitor_status": monitor_payload.get("status"),
                    "strategy_id": resolved_strategy_id,
                    "account_key": resolved_account_key,
                    "monitor_symbol_count": len(monitor_symbols),
                    "trade_date": pool_payload.get("trade_date"),
                    "window": pool_payload.get("window"),
                    "started": False,
                    "stale_monitor_cleanup_count": len(cleanup_ids),
                    "kept_trade_date": existing_trade_date,
                }
        config_patch = {
            "source": "catalyst-selection",
            "catalyst_trade_date": pool_payload.get("trade_date"),
            "catalyst_window": pool_payload.get("window"),
            "poll_interval_seconds": 20,
            "max_signals_per_cycle": min(max(len(monitor_symbols), 1), 3),
        }
        if existing is not None:
            old_pool = existing.monitor_pool_json if isinstance(existing.monitor_pool_json, dict) else {}
            pool_changed = _monitor_pool_runtime_signature(old_pool) != _monitor_pool_runtime_signature(monitor_pool)
            existing.name = f"AI监控池 {pool_payload.get('window')} {pool_payload.get('trade_date')}"
            existing.account_key = resolved_account_key
            existing.strategy_id = resolved_strategy_id
            existing.execution_mode = "monitor_only"
            existing.auto_trade_enabled = False
            existing.live_trading_enabled = False
            existing.monitor_pool_json = monitor_pool
            existing.risk_config_json = risk_config
            existing.config_json = {**dict(existing.config_json or {}), **config_patch}
            existing.fused_reason = None if existing.status in {"ready", "paused", "running", "fused"} else existing.fused_reason
            existing.status = "ready" if existing.status in {"error", "halted", "fused"} else existing.status
            if pool_changed:
                state = dict(existing.state_json or {})
                state.pop("signal_clock", None)
                state["monitor_pool_refreshed_at"] = _utcnow().isoformat()
                state["monitor_pool_refresh_reason"] = "catalyst-selection-updated"
                existing.state_json = state
            existing.updated_at = _utcnow()
            for field in ("monitor_pool_json", "risk_config_json", "config_json"):
                flag_modified(existing, field)
            if pool_changed:
                flag_modified(existing, "state_json")
            strategy_db.add(existing)
            strategy_db.commit()
            monitor_payload = (
                realtime_monitor_service.start_monitor(strategy_db, user_id, existing.id)
                if start_monitor and existing.status != "running"
                else realtime_monitor_service.get_monitor(strategy_db, user_id, existing.id)
            )
            return {
                "status": "updated_running" if monitor_payload.get("status") == "running" else "updated",
                "enabled": True,
                "monitor_id": monitor_payload.get("id"),
                "monitor_status": monitor_payload.get("status"),
                "strategy_id": resolved_strategy_id,
                "account_key": resolved_account_key,
                "monitor_symbol_count": len(monitor_symbols),
                "trade_date": pool_payload.get("trade_date"),
                "window": pool_payload.get("window"),
                "started": bool(start_monitor and monitor_payload.get("status") == "running"),
                "pool_changed": pool_changed,
                "signal_clock_reset": pool_changed,
                "stale_monitor_cleanup_count": len(cleanup_ids),
            }

        monitor_payload = realtime_monitor_service.create_monitor(
            strategy_db,
            main_db,
            user_id,
            {
                "name": f"AI监控池 {pool_payload.get('window')} {pool_payload.get('trade_date')}",
                "strategy_id": resolved_strategy_id,
                "account_key": resolved_account_key,
                "execution_mode": "monitor_only",
                "live_trading_enabled": False,
                "live_confirmed": False,
                "monitor_pool": monitor_pool,
                "risk_config": risk_config,
                "config": config_patch,
            },
        )
        if start_monitor:
            monitor_payload = realtime_monitor_service.start_monitor(strategy_db, user_id, str(monitor_payload["id"]))
        return {
            "status": "created_running" if monitor_payload.get("status") == "running" else "created",
            "enabled": True,
            "monitor_id": monitor_payload.get("id"),
            "monitor_status": monitor_payload.get("status"),
            "strategy_id": resolved_strategy_id,
            "account_key": resolved_account_key,
            "monitor_symbol_count": len(monitor_symbols),
            "trade_date": pool_payload.get("trade_date"),
            "window": pool_payload.get("window"),
            "started": bool(start_monitor and monitor_payload.get("status") == "running"),
            "stale_monitor_cleanup_count": len(cleanup_ids),
        }


def _resolve_catalyst_monitor_account_key(main_db: Session, *, user_id: str, explicit: str | None = None) -> str | None:
    explicit_value = str(explicit or os.getenv(CATALYST_AUTO_MONITOR_ACCOUNT_KEY) or "").strip()
    if explicit_value:
        return explicit_value
    from api.services import auth_service

    configs = auth_service.get_user_qmt_account_configs(main_db, user_id)
    paper = configs.get("paper") if isinstance(configs, dict) else None
    if isinstance(paper, dict) and bool(paper.get("enabled")):
        return str(paper.get("key") or "paper_sim").strip() or "paper_sim"
    return None


def _resolve_catalyst_monitor_strategy_id(
    strategy_db: Session,
    *,
    explicit: str | None,
    list_platform_strategies_fn: Any,
) -> str | None:
    explicit_value = str(explicit or "").strip()
    if explicit_value:
        return explicit_value
    strategies = list_platform_strategies_fn(strategy_db, strategy_type="trading", status="active", include_test=False)
    if not strategies:
        strategies = list_platform_strategies_fn(strategy_db, status="active", include_test=False)
    if not strategies:
        return None
    preferred = [
        item for item in strategies
        if str(item.get("strategy_type") or "") == "trading"
    ] or strategies
    return str(preferred[0].get("id") or "").strip() or None


def _find_existing_catalyst_monitor(
    strategy_db: Session,
    *,
    RealtimeMonitorDB: Any,
    user_id: str,
    window: str,
) -> Any | None:
    rows = (
        strategy_db.query(RealtimeMonitorDB)
        .filter(RealtimeMonitorDB.user_id == user_id)
        .order_by(RealtimeMonitorDB.updated_at.desc(), RealtimeMonitorDB.created_at.desc())
        .all()
    )
    for row in sorted(rows, key=_catalyst_monitor_recency_key, reverse=True):
        pool = row.monitor_pool_json if isinstance(row.monitor_pool_json, dict) else {}
        config = row.config_json if isinstance(row.config_json, dict) else {}
        if (
            _is_catalyst_realtime_monitor(row, pool=pool, config=config)
            and _catalyst_monitor_window(row, pool=pool, config=config) == str(window or "")
        ):
            return row
    return None


def _stale_catalyst_monitor_ids(
    strategy_db: Session,
    *,
    RealtimeMonitorDB: Any,
    user_id: str,
    window: str,
    keep_id: str | None = None,
) -> list[str]:
    keep = str(keep_id or "").strip()
    target_window = str(window or "").strip()
    rows = (
        strategy_db.query(RealtimeMonitorDB)
        .filter(RealtimeMonitorDB.user_id == user_id)
        .order_by(RealtimeMonitorDB.updated_at.desc(), RealtimeMonitorDB.created_at.desc())
        .all()
    )
    stale_ids: list[str] = []
    newest_seen = bool(keep)
    for row in sorted(rows, key=_catalyst_monitor_recency_key, reverse=True):
        pool = row.monitor_pool_json if isinstance(row.monitor_pool_json, dict) else {}
        config = row.config_json if isinstance(row.config_json, dict) else {}
        if not _is_catalyst_realtime_monitor(row, pool=pool, config=config):
            continue
        if _catalyst_monitor_window(row, pool=pool, config=config) != target_window:
            continue
        if keep and row.id == keep:
            continue
        if not newest_seen:
            newest_seen = True
            continue
        stale_ids.append(row.id)
    return stale_ids


def _catalyst_monitor_recency_key(row: Any) -> tuple[str, datetime, datetime]:
    pool = row.monitor_pool_json if isinstance(row.monitor_pool_json, dict) else {}
    config = row.config_json if isinstance(row.config_json, dict) else {}
    return (
        _catalyst_monitor_trade_date(row, pool=pool, config=config),
        getattr(row, "updated_at", None) or datetime.min,
        getattr(row, "created_at", None) or datetime.min,
    )


def _is_catalyst_realtime_monitor(row: Any, *, pool: dict[str, Any], config: dict[str, Any]) -> bool:
    return (
        str(pool.get("source") or "").strip() == "catalyst-selection"
        or str(config.get("source") or "").strip() == "catalyst-selection"
        or str(getattr(row, "name", "") or "").strip().startswith("AI监控池 ")
    )


def _catalyst_monitor_window(row: Any, *, pool: dict[str, Any], config: dict[str, Any]) -> str:
    if pool.get("window"):
        return str(pool.get("window") or "").strip()
    if config.get("catalyst_window"):
        return str(config.get("catalyst_window") or "").strip()
    name = str(getattr(row, "name", "") or "").strip()
    parts = name.split()
    if len(parts) >= 2 and parts[0] == "AI监控池":
        return parts[1]
    return ""


def _catalyst_monitor_trade_date(row: Any, *, pool: dict[str, Any], config: dict[str, Any]) -> str:
    if pool.get("trade_date"):
        return str(pool.get("trade_date") or "").strip()
    if config.get("catalyst_trade_date"):
        return str(config.get("catalyst_trade_date") or "").strip()
    name = str(getattr(row, "name", "") or "").strip()
    parts = name.split()
    if len(parts) >= 3 and parts[0] == "AI监控池":
        return parts[2]
    return ""


def _monitor_pool_runtime_signature(pool: dict[str, Any]) -> str:
    candidates = []
    for candidate in pool.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        symbol = _normalize_symbol(candidate.get("symbol"))
        if not symbol:
            continue
        candidates.append(
            {
                "symbol": symbol,
                "gate": str(candidate.get("execution_gate") or "").strip(),
                "action": str(candidate.get("action") or "").strip(),
            }
        )
    payload = {
        "source": str(pool.get("source") or "").strip(),
        "trade_date": str(pool.get("trade_date") or "").strip(),
        "window": str(pool.get("window") or "").strip(),
        "watch_symbols": [_normalize_symbol(symbol) for symbol in pool.get("watch_symbols") or [] if _normalize_symbol(symbol)],
        "tradable_symbols": [_normalize_symbol(symbol) for symbol in pool.get("tradable_symbols") or [] if _normalize_symbol(symbol)],
        "manual_symbols": [_normalize_symbol(symbol) for symbol in pool.get("manual_symbols") or [] if _normalize_symbol(symbol)],
        "entry_symbols": [_normalize_symbol(symbol) for symbol in pool.get("entry_symbols") or [] if _normalize_symbol(symbol)],
        "confirm_symbols": [_normalize_symbol(symbol) for symbol in pool.get("confirm_symbols") or [] if _normalize_symbol(symbol)],
        "blocked_symbols": [_normalize_symbol(symbol) for symbol in pool.get("blocked_symbols") or [] if _normalize_symbol(symbol)],
        "reduce_only_symbols": [_normalize_symbol(symbol) for symbol in pool.get("reduce_only_symbols") or [] if _normalize_symbol(symbol)],
        "candidates": sorted(candidates, key=lambda item: item["symbol"]),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _ai_quant_env_enabled(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "auto"}


def _monitor_pool_theme_matches(theme_matches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for match in theme_matches[:4]:
        if not isinstance(match, dict):
            continue
        semantic = match.get("event_semantic") if isinstance(match.get("event_semantic"), dict) else {}
        compact.append(
            {
                "theme": str(match.get("theme") or "").strip(),
                "score": _round_or_none(match.get("score"), 2),
                "relation_score": _round_or_none(match.get("relation_score"), 2),
                "mainline_alignment_score": _round_or_none(match.get("mainline_alignment_score"), 2),
                "source_tier": match.get("source_tier"),
                "semantic_source": match.get("semantic_source"),
                "event_semantic": {
                    "event_type": _normalize_event_type(semantic.get("event_type")),
                    "catalyst_strength": _round_or_none(semantic.get("catalyst_strength"), 2),
                    "confidence": _round_or_none(semantic.get("confidence"), 4),
                },
            }
        )
    return [item for item in compact if item.get("theme") or _event_type_from_match(item)]


def capture_realtime_monitor_feedback(
    strategy_db: Session,
    main_db: Session,
    *,
    monitor_id: str | None = None,
    limit: int = 500,
    refresh_profiles: bool = True,
    now_value: datetime | None = None,
) -> dict[str, Any]:
    """Capture catalyst-selection realtime monitor events as learning samples."""
    ensure_catalyst_selection_tables(main_db)
    try:
        from api.models.strategy_models import RealtimeEventDB, RealtimeMonitorDB
    except Exception as exc:
        logger.warning("[catalyst-selection] realtime feedback unavailable: %s", exc)
        return {
            "captured_count": 0,
            "skipped_count": 0,
            "reason": "strategy_models_unavailable",
            "feedback_refresh": None,
        }

    bounded_limit = max(1, min(int(limit or 500), 2000))
    query = (
        strategy_db.query(RealtimeEventDB, RealtimeMonitorDB)
        .join(RealtimeMonitorDB, RealtimeEventDB.monitor_id == RealtimeMonitorDB.id)
        .filter(RealtimeEventDB.event_type.in_(sorted(REALTIME_FEEDBACK_EVENT_TYPES)))
    )
    if monitor_id:
        query = query.filter(RealtimeEventDB.monitor_id == str(monitor_id))
    query = query.order_by(RealtimeEventDB.created_at.desc()).limit(bounded_limit)

    samples: list[dict[str, Any]] = []
    new_samples: list[dict[str, Any]] = []
    existing_sample_count = 0
    skipped = 0
    for event, monitor in query.all():
        event_samples = _realtime_feedback_samples_from_event(monitor, event)
        if not event_samples:
            skipped += 1
            continue
        existing_source_ids = _existing_realtime_feedback_source_ids(
            main_db,
            [
                str(sample.get("source_event_id") or "").strip()
                for sample in event_samples
                if str(sample.get("source_event_id") or "").strip()
            ],
        )
        for sample in event_samples:
            _upsert_realtime_feedback_sample(main_db, sample=sample, now_value=now_value or _utcnow())
            samples.append(sample)
            if str(sample.get("source_event_id") or "").strip() in existing_source_ids:
                existing_sample_count += 1
            else:
                new_samples.append(sample)

    feedback_refresh: dict[str, Any] | None = None
    if refresh_profiles and new_samples:
        feedback_refresh = _refresh_feedback_profiles_from_settlements(
            main_db,
            symbols=[
                str(sample.get("symbol") or "")
                for sample in new_samples
                if sample.get("symbol_feedback")
            ],
            themes=[
                theme
                for sample in new_samples
                if sample.get("symbol_feedback")
                for theme in (sample.get("themes") or [])
            ],
            event_types=[
                event_type
                for sample in new_samples
                if sample.get("symbol_feedback")
                for event_type in (sample.get("event_types") or [])
            ],
            risk_gates=[
                str(sample.get("risk_gate") or "")
                for sample in new_samples
                if sample.get("risk_feedback") and str(sample.get("risk_gate") or "").strip()
            ],
            now_value=now_value or _utcnow(),
        )
    main_db.commit()
    return {
        "captured_count": len(samples),
        "new_sample_count": len(new_samples),
        "existing_sample_count": existing_sample_count,
        "skipped_count": skipped,
        "monitor_id": monitor_id,
        "event_types": dict(Counter(str(sample.get("event_type") or "") for sample in samples)),
        "symbol_feedback_count": sum(1 for sample in samples if sample.get("symbol_feedback")),
        "risk_feedback_count": sum(1 for sample in samples if sample.get("risk_feedback")),
        "feedback_refresh": feedback_refresh,
        "updated_at": (now_value or _utcnow()).isoformat(),
    }


def summarize_realtime_feedback(
    db: Session,
    *,
    trade_date: str | None = None,
    lookback_days: int = 30,
    limit: int = 5000,
) -> dict[str, Any]:
    """Summarize realtime monitor feedback samples for closed-loop learning visibility."""
    ensure_catalyst_selection_tables(db)
    bounded_limit = max(1, min(int(limit or 5000), 20000))
    bounded_days = max(1, min(int(lookback_days or 30), 365))
    clauses: list[str] = []
    params: dict[str, Any] = {"limit": bounded_limit}
    if trade_date:
        try:
            end_date = date.fromisoformat(str(trade_date)[:10])
            start_date = end_date - timedelta(days=bounded_days)
            clauses.append("COALESCE(CAST(event_time AS date)::text, trade_date) >= :start_date")
            clauses.append("COALESCE(CAST(event_time AS date)::text, trade_date) <= :end_date")
            params["start_date"] = start_date.isoformat()
            params["end_date"] = end_date.isoformat()
        except Exception:
            clauses.append("COALESCE(CAST(event_time AS date)::text, trade_date) = :trade_date")
            params["trade_date"] = str(trade_date)[:10]
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db.execute(
        text(
            f"""
            SELECT source_event_id, monitor_id, trade_date, event_time, symbol, event_type,
                   hit_score, outcome, risk_gate, risk_favorable, symbol_feedback, risk_feedback,
                   themes_json, event_types_json, updated_at
            FROM catalyst_selection_realtime_feedback
            {where_sql}
            {"AND" if where_sql else "WHERE"} (symbol_feedback = TRUE OR risk_feedback = TRUE)
            ORDER BY event_time DESC NULLS LAST, updated_at DESC
            LIMIT :limit
            """
        ),
        params,
    ).mappings().all()

    event_type_counts: Counter[str] = Counter()
    risk_gate_counts: Counter[str] = Counter()
    symbol_counts: Counter[str] = Counter()
    theme_counts: Counter[str] = Counter()
    semantic_event_type_counts: Counter[str] = Counter()
    monitor_ids: set[str] = set()
    hit_scores: list[float] = []
    latest_event_time: datetime | None = None
    latest_trade_date: str | None = None
    symbol_feedback_count = 0
    risk_feedback_count = 0
    risk_favorable_count = 0
    risk_adverse_count = 0
    for row in rows:
        event_type = str(row.get("event_type") or "").strip()
        risk_gate = str(row.get("risk_gate") or "").strip()
        symbol = _normalize_symbol(row.get("symbol"))
        monitor_id = str(row.get("monitor_id") or "").strip()
        trade_date_value = str(row.get("trade_date") or "").strip()
        if event_type:
            event_type_counts[event_type] += 1
        if risk_gate:
            risk_gate_counts[risk_gate] += 1
        if symbol:
            symbol_counts[symbol] += 1
        if monitor_id:
            monitor_ids.add(monitor_id)
        if row.get("symbol_feedback"):
            symbol_feedback_count += 1
        if row.get("risk_feedback"):
            risk_feedback_count += 1
            if row.get("risk_favorable") is True:
                risk_favorable_count += 1
            elif row.get("risk_favorable") is False:
                risk_adverse_count += 1
        for theme in _loads(row.get("themes_json"), []):
            theme_value = str(theme or "").strip()
            if theme_value:
                theme_counts[theme_value] += 1
        for raw_event_type in _loads(row.get("event_types_json"), []):
            semantic_event_type = _normalize_event_type(raw_event_type)
            if semantic_event_type:
                semantic_event_type_counts[semantic_event_type] += 1
        hit_score = _num(row.get("hit_score"))
        if hit_score is not None:
            hit_scores.append(hit_score)
        event_time = _parse_datetime_or_none(row.get("event_time"))
        if event_time and (latest_event_time is None or event_time > latest_event_time):
            latest_event_time = event_time
        effective_trade_date = event_time.date().isoformat() if event_time else trade_date_value
        if effective_trade_date and (latest_trade_date is None or effective_trade_date > latest_trade_date):
            latest_trade_date = effective_trade_date

    sample_count = len(rows)
    return {
        "status": "active" if sample_count else "warming_up",
        "source": "catalyst_selection_realtime_feedback",
        "model_version": REALTIME_FEEDBACK_MODEL_VERSION,
        "trade_date": trade_date,
        "lookback_days": bounded_days,
        "sample_count": sample_count,
        "symbol_feedback_count": symbol_feedback_count,
        "risk_feedback_count": risk_feedback_count,
        "symbol_count": len(symbol_counts),
        "theme_count": len(theme_counts),
        "event_type_count": len(event_type_counts),
        "semantic_event_type_count": len(semantic_event_type_counts),
        "monitor_count": len(monitor_ids),
        "latest_event_time": _iso(latest_event_time) if latest_event_time else None,
        "latest_trade_date": latest_trade_date,
        "average_hit_score": round(sum(hit_scores) / len(hit_scores), 2) if hit_scores else None,
        "risk_favorable_count": risk_favorable_count,
        "risk_adverse_count": risk_adverse_count,
        "event_type_counts": dict(event_type_counts),
        "risk_gate_counts": dict(risk_gate_counts),
        "semantic_event_type_counts": dict(semantic_event_type_counts),
        "top_symbols": [
            {"symbol": symbol, "count": count}
            for symbol, count in symbol_counts.most_common(8)
        ],
        "top_themes": [
            {"theme": theme, "count": count}
            for theme, count in theme_counts.most_common(8)
        ],
    }


def _realtime_feedback_sample_from_event(monitor: Any, event: Any) -> dict[str, Any] | None:
    samples = _realtime_feedback_samples_from_event(monitor, event)
    return samples[0] if samples else None


def _realtime_feedback_samples_from_event(monitor: Any, event: Any) -> list[dict[str, Any]]:
    pool = monitor.monitor_pool_json if isinstance(getattr(monitor, "monitor_pool_json", None), dict) else {}
    if str(pool.get("source") or "").strip() != "catalyst-selection":
        return []
    event_type = str(getattr(event, "event_type", "") or "").strip()
    if event_type not in REALTIME_FEEDBACK_EVENT_TYPES:
        return []
    if event_type == "minute_features":
        return _minute_feature_feedback_samples_from_event(monitor, event, pool)
    if event_type == "no_signal":
        return _no_signal_feedback_samples_from_event(monitor, event, pool)
    signal_payload = getattr(event, "signal_payload", None) if isinstance(getattr(event, "signal_payload", None), dict) else {}
    order_payload = getattr(event, "order_payload", None) if isinstance(getattr(event, "order_payload", None), dict) else {}
    risk_payload = getattr(event, "risk_payload", None) if isinstance(getattr(event, "risk_payload", None), dict) else {}
    symbol = _normalize_symbol(
        getattr(event, "symbol", None)
        or signal_payload.get("symbol")
        or order_payload.get("symbol")
        or risk_payload.get("symbol")
    )
    if not symbol:
        return []

    risk_config = monitor.risk_config_json if isinstance(getattr(monitor, "risk_config_json", None), dict) else {}
    candidate = _monitor_pool_candidate_for_symbol(pool, symbol)
    if not candidate:
        return []
    theme_matches = candidate.get("theme_matches") if isinstance(candidate.get("theme_matches"), list) else []
    themes = _dedupe_strings(candidate.get("themes") or [match.get("theme") for match in theme_matches if isinstance(match, dict)])
    event_types = _dedupe_strings(
        candidate.get("event_types")
        or [
            _event_type_from_match(match)
            for match in theme_matches
            if isinstance(match, dict)
        ]
    )
    execution_gates = risk_config.get("execution_gates") if isinstance(risk_config.get("execution_gates"), dict) else {}
    risk_by_symbol = risk_config.get("risk_by_symbol") if isinstance(risk_config.get("risk_by_symbol"), dict) else {}
    risk_snapshot = risk_by_symbol.get(symbol) if isinstance(risk_by_symbol.get(symbol), dict) else {}
    risk_gate = str(
        candidate.get("execution_gate")
        or execution_gates.get(symbol)
        or risk_snapshot.get("execution_gate")
        or ""
    ).strip()
    side = str(signal_payload.get("side") or order_payload.get("side") or "").strip().lower()
    hit_score = _realtime_feedback_hit_score(event_type=event_type, risk_gate=risk_gate)
    event_time = _realtime_event_time(event)
    symbol_feedback = (
        event_type in REALTIME_SYMBOL_FEEDBACK_EVENT_TYPES
        and side != "sell"
        and bool(themes or event_types or candidate)
    )
    risk_feedback = bool(risk_gate)
    if not symbol_feedback and not risk_feedback:
        return []
    source_event_id = str(getattr(event, "id", "") or uuid4().hex)
    return [{
        "feedback_id": f"rtf-{source_event_id[:58]}",
        "source_event_id": source_event_id,
        "monitor_id": str(getattr(monitor, "id", "") or ""),
        "strategy_id": str(getattr(event, "strategy_id", "") or getattr(monitor, "strategy_id", "") or ""),
        "user_id": str(getattr(event, "user_id", "") or getattr(monitor, "user_id", "") or ""),
        "account_key": str(getattr(event, "account_key", "") or getattr(monitor, "account_key", "") or ""),
        "trade_date": _realtime_feedback_trade_date(pool, event_time),
        "event_time": event_time,
        "symbol": symbol,
        "name": candidate.get("name") or symbol,
        "event_type": event_type,
        "signal_side": side or None,
        "signal_source": str(signal_payload.get("source") or "").strip() or None,
        "feedback_kind": _realtime_feedback_kind(event_type),
        "outcome": _settlement_outcome(hit_score),
        "hit_score": hit_score,
        "change_pct": _num(signal_payload.get("change_pct") or signal_payload.get("return_pct")),
        "risk_gate": risk_gate or None,
        "risk_favorable": _realtime_risk_favorable(event_type=event_type, risk_gate=risk_gate),
        "symbol_feedback": symbol_feedback,
        "risk_feedback": risk_feedback,
        "themes": themes,
        "event_types": event_types,
        "theme_matches": theme_matches,
        "candidate_snapshot": {
            key: candidate.get(key)
            for key in (
                "rank",
                "score",
                "action",
                "execution_gate",
                "risk_level",
                "adaptive_feedback_score",
                "risk_penalty",
                "primary_theme",
                "primary_event_type",
            )
            if candidate.get(key) is not None
        },
        "raw_event": _realtime_event_payload(event),
        "source": "realtime_monitor_event",
    }]


def _no_signal_feedback_samples_from_event(monitor: Any, event: Any, pool: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = [
        dict(candidate)
        for candidate in (pool.get("candidates") or [])
        if isinstance(candidate, dict) and _normalize_symbol(candidate.get("symbol"))
    ]
    if not candidates:
        return []
    risk_config = monitor.risk_config_json if isinstance(getattr(monitor, "risk_config_json", None), dict) else {}
    execution_gates = risk_config.get("execution_gates") if isinstance(risk_config.get("execution_gates"), dict) else {}
    risk_by_symbol = risk_config.get("risk_by_symbol") if isinstance(risk_config.get("risk_by_symbol"), dict) else {}
    event_time = _realtime_event_time(event)
    event_id = str(getattr(event, "id", "") or uuid4().hex)
    samples: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = _normalize_symbol(candidate.get("symbol"))
        if not symbol:
            continue
        theme_matches = candidate.get("theme_matches") if isinstance(candidate.get("theme_matches"), list) else []
        themes = _dedupe_strings(candidate.get("themes") or [match.get("theme") for match in theme_matches if isinstance(match, dict)])
        event_types = _dedupe_strings(
            candidate.get("event_types")
            or [
                _event_type_from_match(match)
                for match in theme_matches
                if isinstance(match, dict)
            ]
        )
        risk_snapshot = risk_by_symbol.get(symbol) if isinstance(risk_by_symbol.get(symbol), dict) else {}
        risk_gate = str(
            candidate.get("execution_gate")
            or execution_gates.get(symbol)
            or risk_snapshot.get("execution_gate")
            or ""
        ).strip()
        hit_score = _realtime_feedback_hit_score(event_type="no_signal", risk_gate=risk_gate)
        symbol_feedback = risk_gate not in PROTECTIVE_RISK_GATES and bool(themes or event_types)
        risk_feedback = bool(risk_gate)
        if not symbol_feedback and not risk_feedback:
            continue
        source_event_id = _realtime_feedback_source_event_id(event_id, symbol)
        samples.append(
            {
                "feedback_id": f"rtf-{source_event_id[:58]}",
                "source_event_id": source_event_id,
                "monitor_id": str(getattr(monitor, "id", "") or ""),
                "strategy_id": str(getattr(event, "strategy_id", "") or getattr(monitor, "strategy_id", "") or ""),
                "user_id": str(getattr(event, "user_id", "") or getattr(monitor, "user_id", "") or ""),
                "account_key": str(getattr(event, "account_key", "") or getattr(monitor, "account_key", "") or ""),
                "trade_date": _realtime_feedback_trade_date(pool, event_time),
                "event_time": event_time,
                "symbol": symbol,
                "name": candidate.get("name") or symbol,
                "event_type": "no_signal",
                "signal_side": None,
                "signal_source": None,
                "feedback_kind": _realtime_feedback_kind("no_signal"),
                "outcome": _settlement_outcome(hit_score),
                "hit_score": hit_score,
                "change_pct": None,
                "risk_gate": risk_gate or None,
                "risk_favorable": _realtime_risk_favorable(event_type="no_signal", risk_gate=risk_gate),
                "symbol_feedback": symbol_feedback,
                "risk_feedback": risk_feedback,
                "themes": themes,
                "event_types": event_types,
                "theme_matches": theme_matches,
                "candidate_snapshot": {
                    key: candidate.get(key)
                    for key in (
                        "rank",
                        "score",
                        "action",
                        "execution_gate",
                        "risk_level",
                        "adaptive_feedback_score",
                        "risk_penalty",
                        "primary_theme",
                        "primary_event_type",
                    )
                    if candidate.get(key) is not None
                },
                "raw_event": _realtime_event_payload(event),
                "source": "realtime_monitor_event",
            }
        )
    return samples


def _minute_feature_feedback_samples_from_event(monitor: Any, event: Any, pool: dict[str, Any]) -> list[dict[str, Any]]:
    payload = getattr(event, "payload", None) if isinstance(getattr(event, "payload", None), dict) else {}
    items_by_symbol = {
        _normalize_symbol(item.get("symbol")): item
        for item in (payload.get("items") or [])
        if isinstance(item, dict) and _normalize_symbol(item.get("symbol"))
    }
    if not items_by_symbol:
        return []
    candidates = [
        dict(candidate)
        for candidate in (pool.get("candidates") or [])
        if isinstance(candidate, dict) and _normalize_symbol(candidate.get("symbol")) in items_by_symbol
    ]
    if not candidates:
        return []
    risk_config = monitor.risk_config_json if isinstance(getattr(monitor, "risk_config_json", None), dict) else {}
    execution_gates = risk_config.get("execution_gates") if isinstance(risk_config.get("execution_gates"), dict) else {}
    risk_by_symbol = risk_config.get("risk_by_symbol") if isinstance(risk_config.get("risk_by_symbol"), dict) else {}
    event_time = _realtime_event_time(event)
    event_id = str(getattr(event, "id", "") or uuid4().hex)
    samples: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = _normalize_symbol(candidate.get("symbol"))
        item = items_by_symbol.get(symbol)
        if not symbol or not isinstance(item, dict):
            continue
        theme_matches = candidate.get("theme_matches") if isinstance(candidate.get("theme_matches"), list) else []
        themes = _dedupe_strings(candidate.get("themes") or [match.get("theme") for match in theme_matches if isinstance(match, dict)])
        event_types = _dedupe_strings(
            candidate.get("event_types")
            or [
                _event_type_from_match(match)
                for match in theme_matches
                if isinstance(match, dict)
            ]
        )
        risk_snapshot = risk_by_symbol.get(symbol) if isinstance(risk_by_symbol.get(symbol), dict) else {}
        risk_gate = str(
            candidate.get("execution_gate")
            or execution_gates.get(symbol)
            or risk_snapshot.get("execution_gate")
            or ""
        ).strip()
        confirmed = bool(item.get("confirmed") is True)
        feedback_event_type = "minute_confirmed" if confirmed else "minute_unconfirmed"
        hit_score = _realtime_feedback_hit_score(event_type=feedback_event_type, risk_gate=risk_gate)
        symbol_feedback = bool(themes or event_types) and (confirmed or risk_gate not in PROTECTIVE_RISK_GATES)
        risk_feedback = bool(risk_gate)
        if not symbol_feedback and not risk_feedback:
            continue
        source_event_id = _minute_feature_feedback_source_event_id(
            monitor_id=str(getattr(monitor, "id", "") or ""),
            event_id=event_id,
            symbol=symbol,
            bar_end=str(item.get("bar_end") or payload.get("latest_closed_bar_end") or ""),
        )
        samples.append(
            {
                "feedback_id": f"rtf-{source_event_id[:58]}",
                "source_event_id": source_event_id,
                "monitor_id": str(getattr(monitor, "id", "") or ""),
                "strategy_id": str(getattr(event, "strategy_id", "") or getattr(monitor, "strategy_id", "") or ""),
                "user_id": str(getattr(event, "user_id", "") or getattr(monitor, "user_id", "") or ""),
                "account_key": str(getattr(event, "account_key", "") or getattr(monitor, "account_key", "") or ""),
                "trade_date": _realtime_feedback_trade_date(pool, event_time),
                "event_time": event_time,
                "symbol": symbol,
                "name": candidate.get("name") or symbol,
                "event_type": feedback_event_type,
                "signal_side": None,
                "signal_source": str(payload.get("source") or "").strip() or None,
                "feedback_kind": _realtime_feedback_kind(feedback_event_type),
                "outcome": _settlement_outcome(hit_score),
                "hit_score": hit_score,
                "change_pct": _minute_feature_change_pct(item),
                "risk_gate": risk_gate or None,
                "risk_favorable": _realtime_risk_favorable(event_type=feedback_event_type, risk_gate=risk_gate),
                "symbol_feedback": symbol_feedback,
                "risk_feedback": risk_feedback,
                "themes": themes,
                "event_types": event_types,
                "theme_matches": theme_matches,
                "candidate_snapshot": {
                    key: candidate.get(key)
                    for key in (
                        "rank",
                        "score",
                        "action",
                        "execution_gate",
                        "risk_level",
                        "adaptive_feedback_score",
                        "risk_penalty",
                        "primary_theme",
                        "primary_event_type",
                    )
                    if candidate.get(key) is not None
                },
                "raw_event": {
                    **_realtime_event_payload(event),
                    "minute_feature_item": item,
                },
                "source": "realtime_monitor_event",
            }
        )
    return samples


def _minute_feature_change_pct(item: dict[str, Any]) -> float | None:
    direct = _num(item.get("change_pct") or item.get("return_pct"))
    if direct is not None:
        return _round_or_none(direct, 4)
    open_price = _num(item.get("open"))
    close_price = _num(item.get("close"))
    if open_price is None or close_price is None or open_price == 0:
        return None
    return _round_or_none((close_price / open_price - 1.0) * 100.0, 4)


def _realtime_feedback_trade_date(pool: dict[str, Any], event_time: datetime | None) -> str:
    if event_time:
        return event_time.date().isoformat()
    pool_trade_date = str(pool.get("trade_date") or "").strip()
    if pool_trade_date:
        return pool_trade_date[:10]
    return ""


def _realtime_feedback_source_event_id(event_id: str, symbol: str) -> str:
    source = f"{str(event_id or uuid4().hex)}:{_normalize_symbol(symbol)}"
    return source[:64]


def _minute_feature_feedback_source_event_id(*, monitor_id: str, event_id: str, symbol: str, bar_end: str) -> str:
    seed = f"{monitor_id or event_id}:{_normalize_symbol(symbol)}:{bar_end or event_id}:minute_features"
    return f"mf-{hashlib.sha1(seed.encode('utf-8')).hexdigest()}"


def _existing_realtime_feedback_source_ids(db: Session, source_event_ids: list[str]) -> set[str]:
    normalized_ids = sorted({str(source_id or "").strip() for source_id in source_event_ids if str(source_id or "").strip()})
    if not normalized_ids:
        return set()
    rows = db.execute(
        text(
            """
            SELECT source_event_id
            FROM catalyst_selection_realtime_feedback
            WHERE source_event_id IN :source_event_ids
            """
        ).bindparams(bindparam("source_event_ids", expanding=True)),
        {"source_event_ids": normalized_ids},
    ).scalars().all()
    return {str(row) for row in rows if str(row or "").strip()}


def _monitor_pool_candidate_for_symbol(pool: dict[str, Any], symbol: str) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    for candidate in pool.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        if _normalize_symbol(candidate.get("symbol")) == normalized:
            return dict(candidate)
    return {}


def _realtime_feedback_hit_score(*, event_type: str, risk_gate: str | None) -> float:
    gate = str(risk_gate or "").strip()
    if event_type == "minute_confirmed":
        return 64.0
    if event_type == "minute_unconfirmed":
        return 56.0 if gate in PROTECTIVE_RISK_GATES else 45.0
    if event_type == "no_signal":
        return 56.0 if gate in PROTECTIVE_RISK_GATES else 45.0
    if event_type == "signal_generated":
        return 62.0
    if event_type == "order_submitted":
        return 70.0
    if event_type == "trade_confirmed":
        return 78.0
    if event_type == "position_changed":
        return 74.0
    if event_type == "approval_created":
        return 52.0
    if event_type in {"signal_blocked", "order_rejected", "order_error"}:
        return 32.0
    return 50.0


def _realtime_feedback_kind(event_type: str) -> str:
    if event_type == "minute_confirmed":
        return "intraday_minute_confirmed"
    if event_type == "minute_unconfirmed":
        return "intraday_minute_unconfirmed"
    if event_type == "no_signal":
        return "intraday_no_confirmation"
    if event_type in REALTIME_SYMBOL_FEEDBACK_EVENT_TYPES:
        return "intraday_signal_confirmed"
    if event_type == "approval_created":
        return "legacy_manual_gate"
    if event_type in {"signal_blocked", "order_rejected", "order_error"}:
        return "risk_gate_block"
    return "realtime_execution_feedback"


def _realtime_risk_favorable(*, event_type: str, risk_gate: str | None) -> bool | None:
    gate = str(risk_gate or "").strip()
    if not gate:
        return None
    if event_type == "minute_confirmed":
        return gate in PERMISSIVE_RISK_GATES
    if event_type == "minute_unconfirmed":
        return gate in PROTECTIVE_RISK_GATES
    if event_type == "no_signal":
        return gate in PROTECTIVE_RISK_GATES
    if event_type in REALTIME_SYMBOL_FEEDBACK_EVENT_TYPES:
        return gate in PERMISSIVE_RISK_GATES
    if event_type in {"signal_blocked", "order_rejected", "order_error"}:
        return gate in PROTECTIVE_RISK_GATES
    if event_type == "approval_created":
        return gate == "confirm"
    return None


def _realtime_event_time(event: Any) -> datetime | None:
    for value in (getattr(event, "trade_time", None), getattr(event, "created_at", None)):
        parsed = _parse_datetime_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _realtime_event_payload(event: Any) -> dict[str, Any]:
    if hasattr(event, "to_dict"):
        try:
            return event.to_dict()
        except Exception:
            pass
    return {
        "id": str(getattr(event, "id", "") or ""),
        "event_type": str(getattr(event, "event_type", "") or ""),
        "symbol": str(getattr(event, "symbol", "") or ""),
        "payload": getattr(event, "payload", None) or {},
        "signal_payload": getattr(event, "signal_payload", None) or {},
        "risk_payload": getattr(event, "risk_payload", None) or {},
        "order_payload": getattr(event, "order_payload", None) or {},
    }


def _upsert_realtime_feedback_sample(db: Session, *, sample: dict[str, Any], now_value: datetime) -> None:
    sample = _normalize_realtime_feedback_sample_dates(sample, now_value=now_value)
    if not sample.get("source_event_id"):
        sample["source_event_id"] = sample.get("feedback_id") or uuid4().hex
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_realtime_feedback (
                feedback_id, source_event_id, monitor_id, strategy_id, user_id, account_key,
                trade_date, event_time, symbol, name, event_type, signal_side, signal_source,
                feedback_kind, outcome, hit_score, change_pct, risk_gate, risk_favorable,
                symbol_feedback, risk_feedback, themes_json, event_types_json, theme_matches_json,
                candidate_snapshot_json, raw_event_json, source, created_at, updated_at
            )
            VALUES (
                :feedback_id, :source_event_id, :monitor_id, :strategy_id, :user_id, :account_key,
                :trade_date, :event_time, :symbol, :name, :event_type, :signal_side, :signal_source,
                :feedback_kind, :outcome, :hit_score, :change_pct, :risk_gate, :risk_favorable,
                :symbol_feedback, :risk_feedback, :themes_json, :event_types_json, :theme_matches_json,
                :candidate_snapshot_json, :raw_event_json, :source, :created_at, :updated_at
            )
            ON CONFLICT (source_event_id) DO UPDATE SET
                trade_date = EXCLUDED.trade_date,
                event_time = EXCLUDED.event_time,
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                event_type = EXCLUDED.event_type,
                signal_side = EXCLUDED.signal_side,
                signal_source = EXCLUDED.signal_source,
                feedback_kind = EXCLUDED.feedback_kind,
                outcome = EXCLUDED.outcome,
                hit_score = EXCLUDED.hit_score,
                change_pct = EXCLUDED.change_pct,
                risk_gate = EXCLUDED.risk_gate,
                risk_favorable = EXCLUDED.risk_favorable,
                symbol_feedback = EXCLUDED.symbol_feedback,
                risk_feedback = EXCLUDED.risk_feedback,
                themes_json = EXCLUDED.themes_json,
                event_types_json = EXCLUDED.event_types_json,
                theme_matches_json = EXCLUDED.theme_matches_json,
                candidate_snapshot_json = EXCLUDED.candidate_snapshot_json,
                raw_event_json = EXCLUDED.raw_event_json,
                source = EXCLUDED.source,
                updated_at = EXCLUDED.updated_at
            """
        ),
        {
            **sample,
            "themes_json": json.dumps(sample.get("themes") or [], ensure_ascii=False),
            "event_types_json": json.dumps(sample.get("event_types") or [], ensure_ascii=False),
            "theme_matches_json": json.dumps(sample.get("theme_matches") or [], ensure_ascii=False, default=str),
            "candidate_snapshot_json": json.dumps(sample.get("candidate_snapshot") or {}, ensure_ascii=False, default=str),
            "raw_event_json": json.dumps(sample.get("raw_event") or {}, ensure_ascii=False, default=str),
            "created_at": now_value,
            "updated_at": now_value,
        },
    )


def _normalize_realtime_feedback_sample_dates(sample: dict[str, Any], *, now_value: datetime) -> dict[str, Any]:
    normalized = dict(sample)
    event_time = _parse_datetime_or_none(normalized.get("event_time"))
    if event_time is not None:
        normalized["event_time"] = event_time
        normalized["trade_date"] = event_time.date().isoformat()
        return normalized
    trade_date_value = str(normalized.get("trade_date") or "").strip()
    if trade_date_value:
        normalized["trade_date"] = trade_date_value[:10]
    else:
        normalized["trade_date"] = now_value.date().isoformat()
    return normalized


def _backfill_realtime_feedback_trade_dates(db: Session) -> int:
    try:
        result = db.execute(
            text(
                """
                UPDATE catalyst_selection_realtime_feedback
                SET trade_date = CAST(event_time AS date)::text
                WHERE event_time IS NOT NULL
                  AND trade_date <> CAST(event_time AS date)::text
                """
            )
        )
        return int(result.rowcount or 0)
    except Exception:
        logger.exception("[catalyst-selection] failed to backfill realtime feedback trade dates")
        return 0


def settle_selection(
    db: Session,
    *,
    trade_date: str,
    force: bool = False,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    trade_date = _resolve_trade_date(db, trade_date)
    existing = db.execute(
        text("SELECT COUNT(*) FROM catalyst_selection_settlements WHERE trade_date = :trade_date"),
        {"trade_date": trade_date},
    ).scalar()
    if existing and not force:
        return _load_settlements(db, trade_date)
    if force:
        db.execute(text("DELETE FROM catalyst_selection_settlements WHERE trade_date = :trade_date"), {"trade_date": trade_date})

    run_payload = _load_selection_run(db, trade_date=trade_date, window="premarket", limit=MAX_SELECTION_LIMIT)
    if not run_payload:
        run_payload = generate_selections(db, trade_date=trade_date, window="premarket", limit=DEFAULT_SELECTION_LIMIT)
    items = run_payload.get("items") or []
    settlement_date = _next_trade_date(db, trade_date)
    if not settlement_date:
        return {
            "trade_date": trade_date,
            "settlement_date": None,
            "items": [],
            "updated_at": _utcnow().isoformat(),
            "message": "缺少下一交易日日线数据，暂不能结算。",
        }

    symbols = [item["symbol"] for item in items if item.get("symbol")]
    current_rows = _load_daily_price_rows(db, symbols=symbols, trade_date=trade_date)
    next_rows = _load_daily_price_rows(db, symbols=symbols, trade_date=settlement_date)
    now_value = _utcnow()
    settlements: list[dict[str, Any]] = []
    for item in items:
        symbol = item["symbol"]
        current = current_rows.get(symbol, {})
        next_day = next_rows.get(symbol, {})
        entry = _num(current.get("close"))
        next_open = _num(next_day.get("open"))
        close_price = _num(next_day.get("close"))
        high = _num(next_day.get("high"))
        low = _num(next_day.get("low"))
        change_pct = _pct(close_price, entry)
        max_up_pct = _pct(high, entry)
        max_down_pct = _pct(low, entry)
        hit_score = _settlement_hit_score(change_pct, max_up_pct, max_down_pct)
        outcome = _settlement_outcome(hit_score)
        protected = bool((hit_score or 0) >= 60 and float(item.get("score") or 0) >= 65)
        notes = _settlement_notes(item, change_pct, max_up_pct, max_down_pct, outcome)
        payload = {
            "trade_date": trade_date,
            "settlement_date": settlement_date,
            "symbol": symbol,
            "name": item.get("name") or symbol,
            "rank": int(item.get("rank") or 0),
            "entry_price": entry,
            "close_price": close_price,
            "next_open_price": next_open,
            "high_price": high,
            "low_price": low,
            "change_pct": change_pct,
            "max_up_pct": max_up_pct,
            "max_down_pct": max_down_pct,
            "hit_score": hit_score,
            "outcome": outcome,
            "protected": protected,
            "settlement_notes": notes,
        }
        settlements.append(payload)
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_settlements (
                    trade_date, settlement_date, symbol, name, rank,
                    entry_price, close_price, next_open_price, high_price, low_price,
                    change_pct, max_up_pct, max_down_pct, hit_score, outcome,
                    protected, settlement_notes_json, updated_at
                )
                VALUES (
                    :trade_date, :settlement_date, :symbol, :name, :rank,
                    :entry_price, :close_price, :next_open_price, :high_price, :low_price,
                    :change_pct, :max_up_pct, :max_down_pct, :hit_score, :outcome,
                    :protected, :settlement_notes_json, :updated_at
                )
                ON CONFLICT (trade_date, settlement_date, symbol)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    rank = EXCLUDED.rank,
                    entry_price = EXCLUDED.entry_price,
                    close_price = EXCLUDED.close_price,
                    next_open_price = EXCLUDED.next_open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    change_pct = EXCLUDED.change_pct,
                    max_up_pct = EXCLUDED.max_up_pct,
                    max_down_pct = EXCLUDED.max_down_pct,
                    hit_score = EXCLUDED.hit_score,
                    outcome = EXCLUDED.outcome,
                    protected = EXCLUDED.protected,
                    settlement_notes_json = EXCLUDED.settlement_notes_json,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                **payload,
                "settlement_notes_json": json.dumps(notes, ensure_ascii=False),
                "updated_at": now_value,
            },
        )
    feedback_refresh = _refresh_feedback_profiles_from_settlements(
        db,
        symbols=symbols,
        themes=_themes_from_selection_items(items),
        event_types=_event_types_from_selection_items(items),
        now_value=now_value,
    )
    db.commit()
    return {
        "trade_date": trade_date,
        "settlement_date": settlement_date,
        "items": settlements,
        "updated_at": now_value.isoformat(),
        "feedback_refresh": feedback_refresh,
        "message": "已按下一交易日日线完成结算。",
    }


def settle_pending_selections(
    db: Session,
    *,
    before_trade_date: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    ensure_catalyst_selection_tables(db)
    try:
        latest_trade_date = _parse_trade_date(before_trade_date) if before_trade_date else _latest_available_daily_trade_date(db, _effective_cn_trade_date())
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            logger.exception("[catalyst-selection] rollback failed after pending settlement date lookup")
        logger.warning("[catalyst-selection] pending settlement skipped: %s", exc)
        return {
            "settled": [],
            "errors": [],
            "skipped": True,
            "skip_reason": "缺少可用交易日，自动结算已跳过。",
            "updated_at": _utcnow().isoformat(),
        }
    if not latest_trade_date:
        return {
            "settled": [],
            "errors": [],
            "skipped": True,
            "skip_reason": "缺少可用日线数据，自动结算已跳过。",
            "updated_at": _utcnow().isoformat(),
        }

    rows = db.execute(
        text(
            """
            SELECT r.trade_date
            FROM catalyst_selection_runs r
            WHERE r.window_label = 'premarket'
              AND r.score_version = :score_version
              AND r.trade_date < :latest_trade_date
              AND NOT EXISTS (
                  SELECT 1
                  FROM catalyst_selection_settlements s
                  WHERE s.trade_date = r.trade_date
              )
            ORDER BY r.trade_date DESC
            LIMIT :limit
            """
        ),
        {
            "score_version": SCORE_VERSION,
            "latest_trade_date": latest_trade_date,
            "limit": max(1, min(int(limit or 5), 30)),
        },
    ).mappings().all()
    settled: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for row in rows:
        trade_date = str(row["trade_date"])
        try:
            payload = settle_selection(db, trade_date=trade_date, force=False)
            settled.append(
                {
                    "trade_date": payload.get("trade_date"),
                    "settlement_date": payload.get("settlement_date"),
                    "item_count": len(payload.get("items") or []),
                    "feedback_refresh": payload.get("feedback_refresh"),
                }
            )
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                logger.exception("[catalyst-selection] rollback failed after pending settlement error")
            logger.exception("[catalyst-selection] pending settlement failed trade_date=%s", trade_date)
            errors.append({"trade_date": trade_date, "error": str(exc)})
    return {
        "latest_trade_date": latest_trade_date,
        "settled": settled,
        "errors": errors,
        "skipped": False,
        "updated_at": _utcnow().isoformat(),
    }


def _load_selection_run(db: Session, *, trade_date: str, window: str, limit: int) -> dict[str, Any] | None:
    run = db.execute(
        text(
            """
            SELECT *
            FROM catalyst_selection_runs
            WHERE trade_date = :trade_date AND window_label = :window
            ORDER BY updated_at DESC
            LIMIT 1
            """
        ),
        {"trade_date": trade_date, "window": window},
    ).mappings().first()
    if not run:
        return None
    if str(run.get("score_version") or "") != SCORE_VERSION:
        return None
    items = db.execute(
        text(
            """
            SELECT i.*, s.settlement_date, s.entry_price, s.close_price, s.next_open_price,
                   s.high_price, s.low_price, s.change_pct, s.max_up_pct, s.max_down_pct,
                   s.hit_score, s.outcome, s.protected, s.settlement_notes_json
            FROM catalyst_selection_items i
            LEFT JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.run_id = :run_id
            ORDER BY i.rank
            LIMIT :limit
            """
        ),
        {"run_id": run["run_id"], "limit": limit},
    ).mappings().all()
    market_behavior = _loads(run["market_behavior_json"], {})
    return {
        "trade_date": run["trade_date"],
        "window": run["window_label"],
        "updated_at": _iso(run["updated_at"]),
        "source": run["source"],
        "message": _selection_message(str(run["window_label"] or "")),
        "items": [_row_to_item(row) for row in items],
        "market_background": run["market_background"] or "",
        "market_behavior_labels": market_behavior,
        "data_governance": _loads(run["data_governance_json"], {}),
    }


def _load_existing_selection_snapshot(db: Session, *, trade_date: str, window: str) -> dict[str, dict[str, Any]]:
    rows = db.execute(
        text(
            """
            WITH latest_run AS (
                SELECT run_id
                FROM catalyst_selection_runs
                WHERE trade_date = :trade_date
                  AND window_label = :window
                  AND score_version = :score_version
                ORDER BY updated_at DESC
                LIMIT 1
            )
            SELECT
                i.symbol,
                i.name,
                i.rank,
                i.score,
                i.risk_penalty,
                i.risk_control_json,
                i.closed_loop_trace_json
            FROM catalyst_selection_items i
            JOIN latest_run r ON r.run_id = i.run_id
            """
        ),
        {"trade_date": trade_date, "window": window, "score_version": SCORE_VERSION},
    ).mappings().all()
    return {
        str(row["symbol"]): {
            "symbol": row["symbol"],
            "name": row["name"],
            "rank": int(row["rank"] or 0),
            "score": float(row["score"] or 0.0),
            "risk_penalty": float(row["risk_penalty"] or 0.0),
            "risk_control": _loads(row["risk_control_json"], {}),
            "closed_loop_trace": _loads(row["closed_loop_trace_json"], {}),
        }
        for row in rows
    }


def _build_opportunity_events(
    *,
    items: list[dict[str, Any]],
    previous_snapshot: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for item in items:
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        previous = previous_snapshot.get(symbol)
        score = float(item.get("score") or 0.0)
        rank = int(item.get("rank") or 0)
        previous_score = float(previous.get("score") or 0.0) if previous else None
        previous_rank = int(previous.get("rank") or 0) if previous else None
        score_delta = round(score - previous_score, 2) if previous_score is not None else None
        rank_delta = previous_rank - rank if previous_rank is not None else None
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        action = str(risk_control.get("action") or "")
        risk_level = str(risk_control.get("risk_level") or "")
        primary_theme = (item.get("theme_matches") or [{}])[0] if isinstance(item.get("theme_matches"), list) else {}
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        market_trace = trace.get("market") if isinstance(trace.get("market"), dict) else {}
        event_reaction = market_trace.get("event_reaction") if isinstance(market_trace.get("event_reaction"), dict) else {}

        event_types: list[str] = []
        reasons: list[str] = []
        if previous is None:
            event_types.append("new_opportunity")
            reasons.append("首次进入当前事件驱动机会榜")
        if rank_delta is not None and rank_delta >= 2:
            event_types.append("rank_jump")
            reasons.append(f"排名较上一版提升{rank_delta}位")
        if score_delta is not None and score_delta >= 6:
            event_types.append("score_jump")
            reasons.append(f"评分较上一版提升{score_delta:.1f}")
        if float(item.get("event_intelligence_score") or 0.0) >= 70 and float(item.get("market_confirm_score") or 0.0) >= 45:
            event_types.append("event_market_confirmed")
            reasons.append("事件强度与市场确认同时达标")
        if event_reaction.get("status") == "confirmed":
            event_types.append("minute_reaction_confirmed")
            reasons.append("事件后分钟级价格/成交反应确认")
        elif event_reaction.get("status") == "daily_proxy_confirmed":
            event_types.append("daily_proxy_reaction_confirmed")
            reasons.append("分钟线缺失，日内代理反应确认")
        elif event_reaction.get("status") == "divergent":
            event_types.append("minute_reaction_divergent")
            reasons.append("事件后分钟级反应背离，需降低追涨优先级")
        elif event_reaction.get("status") == "daily_proxy_divergent":
            event_types.append("daily_proxy_reaction_divergent")
            reasons.append("分钟线缺失，日内代理反应背离")
        if action in {"observe", "wait"} or risk_level in {"high", "very_high"} or item.get("risk_flags"):
            event_types.append("risk_suppressed")
            reasons.append("存在风控压制，需要等待确认或降仓观察")

        event_types = _dedupe_strings(event_types)
        if not event_types:
            continue
        event_level = _opportunity_event_level(score=score, event_types=event_types, risk_action=action, risk_level=risk_level)
        if primary_theme.get("theme"):
            reasons.append(f"主线：{primary_theme.get('theme')}")
        semantic = primary_theme.get("event_semantic") if isinstance(primary_theme.get("event_semantic"), dict) else {}
        if semantic.get("event_type"):
            reasons.append(f"事件：{semantic.get('event_type')}")
        if market_trace.get("mainline_alignment_score") is not None:
            reasons.append(f"主线对齐 {float(market_trace.get('mainline_alignment_score') or 0.0):.1f}")
        if event_reaction.get("score") is not None:
            reasons.append(f"分钟反应 {float(event_reaction.get('score') or 0.0):.1f}")

        events.append(
            {
                "symbol": symbol,
                "name": item.get("name") or symbol,
                "rank": rank,
                "score": round(score, 2),
                "previous_rank": previous_rank,
                "previous_score": round(previous_score, 2) if previous_score is not None else None,
                "rank_delta": rank_delta,
                "score_delta": score_delta,
                "event_level": event_level,
                "event_types": event_types,
                "reasons": _dedupe_strings(reasons)[:8],
                "risk_action": action or None,
                "risk_level": risk_level or None,
                "trace": {
                    "theme": primary_theme.get("theme"),
                    "event_semantic": semantic,
                    "market_confirm_score": round(float(item.get("market_confirm_score") or 0.0), 2),
                    "event_intelligence_score": round(float(item.get("event_intelligence_score") or 0.0), 2),
                    "event_reaction": event_reaction,
                    "risk_flags": item.get("risk_flags") or [],
                },
            }
        )
    level_order = {"S": 0, "A": 1, "B": 2, "WATCH": 3}
    events.sort(key=lambda event: (level_order.get(str(event.get("event_level")), 9), -float(event.get("score") or 0.0), int(event.get("rank") or 999)))
    return events[:20]


def _opportunity_event_level(*, score: float, event_types: list[str], risk_action: str, risk_level: str) -> str:
    if "risk_suppressed" in event_types and (risk_action in {"observe", "wait"} or risk_level in {"high", "very_high"}):
        return "WATCH"
    if score >= 75 and any(event in event_types for event in ("new_opportunity", "rank_jump", "score_jump", "event_market_confirmed", "minute_reaction_confirmed", "daily_proxy_reaction_confirmed")):
        return "S"
    if score >= 62:
        return "A"
    return "B"


def _persist_selection_run(
    db: Session,
    *,
    run_id: str,
    trade_date: str,
    window: str,
    window_start: str | None,
    window_end: str | None,
    market_background: str,
    market_behavior: dict[str, Any],
    items: list[dict[str, Any]],
    data_governance: dict[str, Any],
    opportunity_events: list[dict[str, Any]],
    now_value: datetime,
) -> None:
    if _normalize_window(window) == "premarket":
        db.execute(text("DELETE FROM catalyst_selection_settlements WHERE trade_date = :trade_date"), {"trade_date": trade_date})
    db.execute(text("DELETE FROM catalyst_selection_opportunity_events WHERE trade_date = :trade_date AND window_label = :window"), {"trade_date": trade_date, "window": window})
    db.execute(text("DELETE FROM catalyst_selection_items WHERE trade_date = :trade_date AND window_label = :window"), {"trade_date": trade_date, "window": window})
    db.execute(text("DELETE FROM catalyst_selection_runs WHERE trade_date = :trade_date AND window_label = :window"), {"trade_date": trade_date, "window": window})
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_runs (
                run_id, trade_date, window_label, window_start, window_end,
                score_version, market_background, market_behavior_json,
                data_governance_json, item_count, source, created_at, updated_at
            )
            VALUES (
                :run_id, :trade_date, :window_label, :window_start, :window_end,
                :score_version, :market_background, :market_behavior_json,
                :data_governance_json, :item_count, :source, :created_at, :updated_at
            )
            """
        ),
        {
            "run_id": run_id,
            "trade_date": trade_date,
            "window_label": window,
            "window_start": _parse_datetime_or_none(window_start),
            "window_end": _parse_datetime_or_none(window_end),
            "score_version": SCORE_VERSION,
            "market_background": market_background,
            "market_behavior_json": json.dumps(market_behavior, ensure_ascii=False, default=str),
            "data_governance_json": json.dumps(data_governance, ensure_ascii=False, default=str),
            "item_count": len(items),
            "source": "postgresql:market_news_items+theme_rankings+stock_daily_kline+settlement_feedback_profiles",
            "created_at": now_value,
            "updated_at": now_value,
        },
    )
    for item in items:
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_items (
                    run_id, trade_date, window_label, rank, symbol, name, industry, sector,
                    concepts_json, score, catalyst_score, theme_score, relation_score,
                    market_confirm_score, event_intelligence_score, momentum_score, fundamental_score, continuity_score,
                    adaptive_feedback_score,
                    risk_penalty, risk_flags_json, reason_parts_json, theme_matches_json,
                    signal_flags_json, metric_snapshot_json, risk_control_json, closed_loop_trace_json, market_background,
                    market_behavior_json, created_at, updated_at
                )
                VALUES (
                    :run_id, :trade_date, :window_label, :rank, :symbol, :name, :industry, :sector,
                    :concepts_json, :score, :catalyst_score, :theme_score, :relation_score,
                    :market_confirm_score, :event_intelligence_score, :momentum_score, :fundamental_score, :continuity_score,
                    :adaptive_feedback_score,
                    :risk_penalty, :risk_flags_json, :reason_parts_json, :theme_matches_json,
                    :signal_flags_json, :metric_snapshot_json, :risk_control_json, :closed_loop_trace_json, :market_background,
                    :market_behavior_json, :created_at, :updated_at
                )
                """
            ),
            {
                **item,
                "run_id": run_id,
                "trade_date": trade_date,
                "window_label": window,
                "concepts_json": json.dumps(item.get("concepts") or [], ensure_ascii=False),
                "risk_flags_json": json.dumps(item.get("risk_flags") or [], ensure_ascii=False),
                "reason_parts_json": json.dumps(item.get("reason_parts") or [], ensure_ascii=False),
                "theme_matches_json": json.dumps(item.get("theme_matches") or [], ensure_ascii=False),
                "signal_flags_json": json.dumps(item.get("signal_flags") or [], ensure_ascii=False),
                "metric_snapshot_json": json.dumps(item.get("metric_snapshot") or {}, ensure_ascii=False, default=str),
                "event_intelligence_score": item.get("event_intelligence_score") or 0.0,
                "adaptive_feedback_score": item.get("adaptive_feedback_score") or 50.0,
                "risk_control_json": json.dumps(item.get("risk_control") or {}, ensure_ascii=False, default=str),
                "closed_loop_trace_json": json.dumps(item.get("closed_loop_trace") or {}, ensure_ascii=False, default=str),
                "market_behavior_json": json.dumps(market_behavior, ensure_ascii=False, default=str),
                "created_at": now_value,
                "updated_at": now_value,
            },
        )
    _persist_opportunity_events(
        db,
        run_id=run_id,
        trade_date=trade_date,
        window=window,
        opportunity_events=opportunity_events,
        now_value=now_value,
    )


def _persist_opportunity_events(
    db: Session,
    *,
    run_id: str,
    trade_date: str,
    window: str,
    opportunity_events: list[dict[str, Any]],
    now_value: datetime,
) -> None:
    for event in opportunity_events:
        symbol = str(event.get("symbol") or "")
        event_id = hashlib.sha256(f"{run_id}|{symbol}|{json.dumps(event.get('event_types') or [], ensure_ascii=False, sort_keys=True)}".encode("utf-8")).hexdigest()
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_opportunity_events (
                    event_id, run_id, trade_date, window_label, symbol, name, rank, score,
                    previous_rank, previous_score, rank_delta, score_delta, event_level,
                    event_types_json, reasons_json, risk_action, risk_level, trace_json, created_at
                )
                VALUES (
                    :event_id, :run_id, :trade_date, :window_label, :symbol, :name, :rank, :score,
                    :previous_rank, :previous_score, :rank_delta, :score_delta, :event_level,
                    :event_types_json, :reasons_json, :risk_action, :risk_level, :trace_json, :created_at
                )
                """
            ),
            {
                "event_id": event_id,
                "run_id": run_id,
                "trade_date": trade_date,
                "window_label": window,
                "symbol": symbol,
                "name": event.get("name"),
                "rank": int(event.get("rank") or 0),
                "score": float(event.get("score") or 0.0),
                "previous_rank": event.get("previous_rank"),
                "previous_score": event.get("previous_score"),
                "rank_delta": event.get("rank_delta"),
                "score_delta": event.get("score_delta"),
                "event_level": event.get("event_level") or "B",
                "event_types_json": json.dumps(event.get("event_types") or [], ensure_ascii=False),
                "reasons_json": json.dumps(event.get("reasons") or [], ensure_ascii=False),
                "risk_action": event.get("risk_action"),
                "risk_level": event.get("risk_level"),
                "trace_json": json.dumps(event.get("trace") or {}, ensure_ascii=False, default=str),
                "created_at": now_value,
            },
        )


def _weighted_candidate_score(
    *,
    component_scores: dict[str, float],
    weights: dict[str, Any],
    score_bias: float,
    risk_penalty: float,
) -> float:
    score = float(score_bias or 0.0) - float(risk_penalty or 0.0)
    for key, value in component_scores.items():
        score += float(weights.get(key) or 0.0) * float(value or 0.0)
    return max(0.0, min(100.0, score))


def _learning_baseline_score(item: dict[str, Any]) -> float:
    trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
    scoring = trace.get("scoring") if isinstance(trace.get("scoring"), dict) else {}
    impact = scoring.get("learning_impact") if isinstance(scoring.get("learning_impact"), dict) else {}
    value = _num(impact.get("baseline_score_before_learning_policy"))
    if value is None:
        value = _num(item.get("score"))
    return float(value or 0.0)


def _baseline_learning_rank_by_symbol(items: list[dict[str, Any]]) -> dict[str, int]:
    ranked = sorted(
        items,
        key=lambda item: (-_learning_baseline_score(item), float(item.get("risk_penalty") or 0.0), str(item.get("symbol") or "")),
    )
    return {
        str(item.get("symbol") or ""): rank
        for rank, item in enumerate(ranked, start=1)
        if str(item.get("symbol") or "").strip()
    }


def _attach_learning_rank_impact(item: dict[str, Any], *, final_rank: int, baseline_rank: int | None) -> None:
    trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else None
    if trace is None:
        return
    scoring = trace.get("scoring") if isinstance(trace.get("scoring"), dict) else None
    if scoring is None:
        return
    impact = scoring.get("learning_impact") if isinstance(scoring.get("learning_impact"), dict) else None
    if impact is None:
        return
    impact["final_rank"] = int(final_rank)
    impact["rank_before_learning_policy"] = int(baseline_rank) if baseline_rank is not None else None
    impact["rank_delta_from_learning_policy"] = (
        int(baseline_rank) - int(final_rank)
        if baseline_rank is not None
        else None
    )


def _score_candidate(
    *,
    symbol: str,
    features: dict[str, Any],
    theme_items: list[dict[str, Any]],
    previous_state: dict[str, Any],
    history_stats: dict[str, Any],
    theme_feedback: dict[str, dict[str, Any]],
    market_background: str,
    market_behavior: dict[str, Any],
    risk_gate_feedback: dict[str, dict[str, Any]] | None = None,
    trigger_news_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    theme_matches = _theme_matches_for_symbol(symbol, features, theme_items)
    if not theme_matches:
        return {"symbol": symbol, "score": 0}
    primary_theme = theme_matches[0]
    trigger_news_signal = _trigger_news_signal_for_candidate(
        symbol=symbol,
        features=features,
        primary_theme=primary_theme,
        trigger_news_context=trigger_news_context or {},
    )
    theme_score = min(float(primary_theme.get("score") or 0.0), 100.0)
    catalyst_score = _catalyst_score(primary_theme)
    relation_score = max(float(match.get("relation_score") or 0.0) for match in theme_matches)
    market_confirm_score = _market_confirm_score(features, primary_theme)
    event_intelligence_score, event_intelligence_reasons = _event_intelligence_score(primary_theme, market_behavior=market_behavior)
    momentum_score = _momentum_score(features)
    fundamental_score = _fundamental_score(features)
    continuity_score = _continuity_score(previous_state, history_stats)
    raw_risk_penalty, risk_flags = _risk_penalty(features, primary_theme, history_stats)
    adaptive_feedback_score, adaptive_feedback_reasons = _adaptive_feedback_score(
        symbol=symbol,
        primary_theme=primary_theme,
        history_stats=history_stats,
        theme_feedback=theme_feedback,
    )
    learning_policy = _learning_adjustment_policy(
        symbol=symbol,
        primary_theme=primary_theme,
        history_stats=history_stats,
        theme_feedback=theme_feedback,
        adaptive_feedback_score=adaptive_feedback_score,
    )
    base_score_profile = _adaptive_score_profile(market_behavior)
    score_profile = _apply_learning_adjustment_policy(base_score_profile, learning_policy)
    weights = score_profile["weights"]
    component_scores = {
        "catalyst": catalyst_score,
        "theme": theme_score,
        "relation": relation_score,
        "market_confirm": market_confirm_score,
        "event_intelligence": event_intelligence_score,
        "adaptive_feedback": adaptive_feedback_score,
        "momentum": momentum_score,
        "fundamental": fundamental_score,
        "continuity": continuity_score,
    }
    baseline_risk_penalty = min(raw_risk_penalty * float(base_score_profile.get("risk_penalty_multiplier") or 1.0), 36.0)
    risk_penalty = min(raw_risk_penalty * float(score_profile.get("risk_penalty_multiplier") or 1.0), 36.0)
    risk_control = _risk_control_plan(
        features=features,
        primary_theme=primary_theme,
        market_behavior=market_behavior,
        risk_penalty=risk_penalty,
        risk_flags=risk_flags,
        event_intelligence_score=event_intelligence_score,
        adaptive_feedback_score=adaptive_feedback_score,
        learning_policy=learning_policy,
        risk_gate_feedback=risk_gate_feedback or {},
    )
    baseline_score = _weighted_candidate_score(
        component_scores=component_scores,
        weights=base_score_profile.get("weights") or {},
        score_bias=0.0,
        risk_penalty=baseline_risk_penalty,
    )
    pre_execution_score = _weighted_candidate_score(
        component_scores=component_scores,
        weights=weights,
        score_bias=float(learning_policy.get("score_bias") or 0.0),
        risk_penalty=risk_penalty,
    )
    execution_adjustment = _execution_gate_score_adjustment(risk_control)
    trigger_news_adjustment = _trigger_news_score_adjustment(trigger_news_signal)
    total_score = max(
        0.0,
        min(
            100.0,
            pre_execution_score
            + float(execution_adjustment.get("score_delta") or 0.0)
            + float(trigger_news_adjustment.get("score_delta") or 0.0),
        ),
    )
    signal_flags = _signal_flags(features, previous_state, history_stats, risk_flags)
    reason_parts = _reason_parts(features, primary_theme, signal_flags, risk_flags)
    if execution_adjustment.get("reason"):
        reason_parts.append(str(execution_adjustment["reason"]))
    if trigger_news_adjustment.get("reason"):
        reason_parts.append(str(trigger_news_adjustment["reason"]))
    primary_theme_name = str(primary_theme.get("theme") or "").strip()
    learning_impact = _build_learning_impact_trace(
        baseline_score=baseline_score,
        final_score=pre_execution_score,
        base_score_profile=base_score_profile,
        adjusted_score_profile=score_profile,
        component_scores=component_scores,
        adaptive_feedback_score=adaptive_feedback_score,
        learning_policy=learning_policy,
        baseline_risk_penalty=baseline_risk_penalty,
        effective_risk_penalty=risk_penalty,
        risk_control=risk_control,
    )
    closed_loop_trace = _build_closed_loop_trace(
        symbol=symbol,
        features=features,
        primary_theme=primary_theme,
        event_intelligence_score=event_intelligence_score,
        event_intelligence_reasons=event_intelligence_reasons,
        adaptive_feedback_score=adaptive_feedback_score,
        adaptive_feedback_reasons=adaptive_feedback_reasons,
        risk_control=risk_control,
        market_background=market_background,
        market_behavior=market_behavior,
        market_confirm_score=market_confirm_score,
        score_profile=score_profile,
        learning_policy=learning_policy,
        symbol_feedback_profile=_public_scoring_feedback_profile("symbol", symbol, history_stats),
        theme_feedback_profile=_public_scoring_feedback_profile("theme", primary_theme_name, theme_feedback.get(primary_theme_name) or {}),
        component_scores={
            **component_scores,
            "learning_policy_bias": float(learning_policy.get("score_bias") or 0.0),
            "pre_execution_score": pre_execution_score,
            "execution_gate_adjustment": float(execution_adjustment.get("score_delta") or 0.0),
            "fresh_news_trigger_adjustment": float(trigger_news_adjustment.get("score_delta") or 0.0),
        },
        execution_adjustment=execution_adjustment,
        trigger_news_signal=trigger_news_signal,
        trigger_news_adjustment=trigger_news_adjustment,
        raw_risk_penalty=raw_risk_penalty,
        effective_risk_penalty=risk_penalty,
        learning_impact=learning_impact,
    )
    return {
        "rank": 0,
        "symbol": symbol,
        "name": features.get("name") or symbol,
        "industry": features.get("industry"),
        "sector": features.get("sector"),
        "concepts": features.get("concepts") or [],
        "score": round(total_score, 2),
        "pre_execution_score": round(pre_execution_score, 2),
        "execution_gate_adjustment": execution_adjustment,
        "catalyst_score": round(catalyst_score, 2),
        "theme_score": round(theme_score, 2),
        "relation_score": round(relation_score, 2),
        "market_confirm_score": round(market_confirm_score, 2),
        "event_intelligence_score": round(event_intelligence_score, 2),
        "momentum_score": round(momentum_score, 2),
        "fundamental_score": round(fundamental_score, 2),
        "continuity_score": round(continuity_score, 2),
        "adaptive_feedback_score": round(adaptive_feedback_score, 2),
        "risk_penalty": round(risk_penalty, 2),
        "risk_flags": risk_flags,
        "reason_parts": reason_parts,
        "theme_matches": [
            {
                "theme": match["theme"],
                "score": round(float(match.get("score") or 0), 2),
                "catalyst": match.get("catalyst"),
                "summary": match.get("summary"),
                "source_tier": match.get("source_tier"),
                "evidence_count": int(match.get("evidence_count") or 0),
                "relation_score": round(float(match.get("relation_score") or 0), 2),
                "mainline_alignment_score": round(float(match.get("mainline_alignment_score") or 0), 2),
                "mainline_alignment_reasons": match.get("mainline_alignment_reasons") or [],
                "event_semantic": match.get("event_semantic") or {},
                "semantic_source": match.get("semantic_source"),
                "symbol_suggestion_source": match.get("symbol_suggestion_source"),
                "trigger_news_match": match.get("trigger_news_match") or {},
            }
            for match in theme_matches[:4]
        ],
        "signal_flags": signal_flags,
        "risk_control": risk_control,
        "closed_loop_trace": closed_loop_trace,
        "market_background": market_background,
        "market_behavior_labels": market_behavior,
        "metric_snapshot": {
            "change_pct": _round_or_none(features.get("change_pct"), 4),
            "turnover_rate": _round_or_none(features.get("turnover_rate"), 4),
            "amount_ratio_20d": _round_or_none(features.get("amount_ratio_20d"), 4),
            "momentum_20d": _round_or_none(features.get("momentum_20d"), 4),
            "momentum_60d": _round_or_none(features.get("momentum_60d"), 4),
            "r60": _round_or_none(features.get("r60"), 2),
            "net_profit_growth_proxy": _round_or_none(features.get("net_profit_growth_proxy"), 4),
            "close": _round_or_none(features.get("close"), 4),
            "event_reaction_status": (features.get("event_reaction") or {}).get("status") if isinstance(features.get("event_reaction"), dict) else None,
            "event_reaction_score": _round_or_none((features.get("event_reaction") or {}).get("score") if isinstance(features.get("event_reaction"), dict) else None, 2),
            "event_reaction_change_pct": _round_or_none((features.get("event_reaction") or {}).get("change_pct") if isinstance(features.get("event_reaction"), dict) else None, 4),
            "event_reaction_amount_share": _round_or_none((features.get("event_reaction") or {}).get("amount_share") if isinstance(features.get("event_reaction"), dict) else None, 4),
        },
    }


def _theme_matches_for_symbol(symbol: str, features: dict[str, Any], theme_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    haystack = " ".join(
        str(value or "")
        for value in [
            features.get("name"),
            features.get("industry"),
            features.get("sector"),
            *(features.get("concepts") or []),
        ]
    )
    matches: list[dict[str, Any]] = []
    for item in theme_items:
        theme = str(item.get("theme") or "").strip()
        if not theme:
            continue
        relation = 0.0
        reasons: list[str] = []
        related_symbols = item.get("related_symbols") or []
        if any(str(row.get("symbol") or "").upper() == symbol for row in related_symbols if isinstance(row, dict)):
            relation += 95.0
            reasons.append("新闻直接点名")
        aliases = [theme, *(THEME_INDUSTRY_ALIASES.get(theme) or ())]
        alias_hits = [alias for alias in aliases if alias and alias in haystack]
        if alias_hits:
            relation = max(relation, min(85.0, 55.0 + len(alias_hits) * 8.0))
            reasons.append("行业/概念匹配：" + "、".join(alias_hits[:4]))
        evidence_text = " ".join(str(row.get("content") or "") for row in (item.get("evidence_items") or []) if isinstance(row, dict))
        if features.get("name") and str(features["name"]) in evidence_text:
            relation = max(relation, 90.0)
            reasons.append("证据文本提及公司")
        if relation <= 0:
            continue
        matches.append(
            {
                "theme": theme,
                "score": item.get("score") or 0,
                "catalyst": item.get("catalyst"),
                "summary": item.get("summary"),
                "source_tier": item.get("source_tier"),
                "top_source_tier": item.get("top_source_tier"),
                "policy_boost": item.get("policy_boost"),
                "evidence_items": item.get("evidence_items") or [],
                "evidence_count": len(item.get("evidence_items") or []),
                "relation_score": min(relation, 100.0),
                "relation_reasons": reasons,
                "risk_note": item.get("risk_note"),
                "mainline_alignment_score": item.get("mainline_alignment_score"),
                "mainline_alignment_reasons": item.get("mainline_alignment_reasons") or [],
                "event_semantic": item.get("event_semantic") or {},
                "semantic_source": item.get("semantic_source"),
                "symbol_suggestion_source": item.get("symbol_suggestion_source"),
                "event_feedback_profile": item.get("event_feedback_profile") or {},
                "trigger_news_match": item.get("fresh_news_trigger") or {},
            }
        )
    matches.sort(key=lambda row: (-float(row.get("relation_score") or 0), -float(row.get("score") or 0)))
    return matches


def _attach_candidate_llm_event_runtime(items: list[dict[str, Any]], llm_runtime: dict[str, Any]) -> None:
    safe_runtime = _safe_llm_runtime_payload(llm_runtime)
    if not safe_runtime:
        return
    for item in items:
        trace = item.get("closed_loop_trace")
        if not isinstance(trace, dict):
            continue
        event = trace.get("event")
        if not isinstance(event, dict):
            continue
        primary_match = (item.get("theme_matches") or [{}])[0] if isinstance(item.get("theme_matches"), list) else {}
        if not isinstance(primary_match, dict):
            primary_match = {}
        event["llm_event_understanding"] = safe_runtime
        event["runtime_source"] = {
            "runtime_package_source": safe_runtime.get("runtime_package_source"),
            "provider": safe_runtime.get("provider"),
            "model": safe_runtime.get("model"),
            "base_url": safe_runtime.get("base_url"),
            "api_key_source": safe_runtime.get("api_key_source"),
            "provider_source": safe_runtime.get("provider_source"),
            "base_url_source": safe_runtime.get("base_url_source"),
            "model_source": safe_runtime.get("model_source"),
            "mixed_account_runtime": safe_runtime.get("mixed_account_runtime"),
            "ready": safe_runtime.get("ready"),
            "status": safe_runtime.get("status"),
        }
        event["semantic_source"] = event.get("semantic_source") or primary_match.get("semantic_source")
        event["symbol_suggestion_source"] = primary_match.get("symbol_suggestion_source") or event.get("symbol_suggestion_source")


def _mark_items_stale_llm_runtime(items: list[dict[str, Any]], llm_runtime: dict[str, Any]) -> None:
    safe_runtime = _safe_llm_runtime_payload(llm_runtime)
    if not safe_runtime:
        return
    for item in items:
        if not isinstance(item, dict):
            continue
        trace = item.setdefault("closed_loop_trace", {})
        if not isinstance(trace, dict):
            continue
        event = trace.setdefault("event", {})
        if not isinstance(event, dict):
            continue
        primary_match = (item.get("theme_matches") or [{}])[0] if isinstance(item.get("theme_matches"), list) else {}
        if not isinstance(primary_match, dict):
            primary_match = {}
        event["llm_event_understanding"] = safe_runtime
        event["runtime_source"] = {
            "cache_status": safe_runtime.get("cache_status"),
            "stale_reason": safe_runtime.get("stale_reason"),
            "runtime_package_source": safe_runtime.get("runtime_package_source"),
            "provider": safe_runtime.get("provider"),
            "model": safe_runtime.get("model"),
            "base_url": safe_runtime.get("base_url"),
            "api_key_source": safe_runtime.get("api_key_source"),
            "provider_source": safe_runtime.get("provider_source"),
            "base_url_source": safe_runtime.get("base_url_source"),
            "model_source": safe_runtime.get("model_source"),
            "mixed_account_runtime": safe_runtime.get("mixed_account_runtime"),
            "ready": safe_runtime.get("ready"),
            "status": safe_runtime.get("status"),
        }
        event["semantic_source"] = event.get("semantic_source") or primary_match.get("semantic_source")
        event["symbol_suggestion_source"] = primary_match.get("symbol_suggestion_source") or event.get("symbol_suggestion_source")


def _trigger_news_context_from_trigger_context(trigger_context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(trigger_context, dict):
        return {"events": [], "summary": {}, "event_count": 0, "included_count": 0}
    raw_events = trigger_context.get("fresh_news_events") or trigger_context.get("events") or []
    if not isinstance(raw_events, list):
        raw_events = []
    events = [_normalize_trigger_news_event(event) for event in raw_events if isinstance(event, dict)]
    events = [event for event in events if event.get("content") or event.get("symbols") or event.get("positive_sectors") or event.get("negative_sectors")]
    raw_summary = trigger_context.get("fresh_news_summary") or trigger_context.get("summary") or {}
    summary = dict(raw_summary) if isinstance(raw_summary, dict) else {}
    event_count = int(trigger_context.get("fresh_event_count") or summary.get("event_count") or len(events) or 0)
    included_count = int(summary.get("included_count") or len(events) or 0)
    return {
        "events": events[:12],
        "summary": summary,
        "event_count": event_count,
        "included_count": included_count,
        "trigger": trigger_context.get("trigger"),
        "source": trigger_context.get("source"),
        "reason": trigger_context.get("reason"),
        "news_ingest": trigger_context.get("news_ingest") if isinstance(trigger_context.get("news_ingest"), dict) else {},
    }


def _normalize_trigger_news_event(event: dict[str, Any]) -> dict[str, Any]:
    symbols = _trigger_event_symbols(event)
    explicit_positive_symbols = _trigger_event_symbols({"symbols": event.get("positive_symbols") or event.get("positive_symbol_labels") or []})
    explicit_negative_symbols = _trigger_event_symbols({"symbols": event.get("negative_symbols") or event.get("negative_symbol_labels") or []})
    sentiment = str(event.get("sentiment") or "neutral").strip().lower()
    positive_symbols = explicit_positive_symbols
    negative_symbols = explicit_negative_symbols
    if not positive_symbols and not negative_symbols:
        if sentiment == "positive":
            positive_symbols = symbols
        elif sentiment == "negative":
            negative_symbols = symbols
    return {
        "digest": str(event.get("digest") or "")[:64],
        "dedupe_key": str(event.get("dedupe_key") or "")[:96],
        "change_type": str(event.get("change_type") or "new"),
        "source": str(event.get("source") or "")[:80],
        "published_at": str(event.get("published_at") or ""),
        "sentiment": sentiment if sentiment in {"positive", "negative", "neutral", "mixed"} else "neutral",
        "content": str(event.get("content") or "")[:360],
        "positive_sectors": _dedupe_strings(event.get("positive_sectors") or [])[:8],
        "negative_sectors": _dedupe_strings(event.get("negative_sectors") or [])[:8],
        "symbols": symbols,
        "positive_symbols": positive_symbols,
        "negative_symbols": negative_symbols,
    }


def _trigger_event_symbols(event: dict[str, Any]) -> list[dict[str, str]]:
    raw_values: list[Any] = []
    for key in ("symbols", "related_symbols", "positive_symbols", "negative_symbols"):
        value = event.get(key)
        if isinstance(value, list):
            raw_values.extend(value)
    symbols: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in raw_values:
        label = ""
        name = ""
        symbol = ""
        if isinstance(value, dict):
            label = str(value.get("label") or value.get("name") or value.get("symbol") or "").strip()
            name = str(value.get("name") or "").strip()
            symbol = _normalize_symbol(value.get("symbol"))
        else:
            label = str(value or "").strip()
        if not symbol:
            match = re.search(r"((?:SH|SZ|BJ)?\d{6}(?:\.(?:SH|SZ|BJ))?)", label, flags=re.IGNORECASE)
            if match:
                symbol = _normalize_symbol(match.group(1))
        if not name and label:
            name = re.sub(r"\s*[\(（]?\s*(?:SH|SZ|BJ)?\d{6}(?:\.(?:SH|SZ|BJ))?\s*[\)）]?\s*", "", label, flags=re.IGNORECASE).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append({"symbol": symbol, "name": name, "label": label or name or symbol})
    return symbols[:12]


def _apply_trigger_news_context_to_theme_items(theme_items: list[dict[str, Any]], trigger_news_context: dict[str, Any]) -> list[dict[str, Any]]:
    events = trigger_news_context.get("events") if isinstance(trigger_news_context, dict) else []
    if not theme_items or not isinstance(events, list) or not events:
        return theme_items
    adjusted: list[dict[str, Any]] = []
    for item in theme_items:
        if not isinstance(item, dict):
            continue
        theme = str(item.get("theme") or "").strip()
        aliases = _theme_aliases(theme) if theme else []
        related_symbols = {
            _normalize_symbol(row.get("symbol"))
            for row in item.get("related_symbols") or []
            if isinstance(row, dict) and row.get("symbol")
        }
        positive_matches: list[dict[str, Any]] = []
        negative_matches: list[dict[str, Any]] = []
        direct_symbols: list[str] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            match = _trigger_news_event_theme_match(event, aliases=aliases, related_symbols=related_symbols)
            if not match.get("matched"):
                continue
            direct_symbols.extend(match.get("direct_symbols") or [])
            if match.get("direction") == "negative":
                negative_matches.append(match)
            else:
                positive_matches.append(match)
        if not positive_matches and not negative_matches:
            adjusted.append(item)
            continue
        score_delta = min(len(positive_matches) * 3.0 + len(_dedupe_strings(direct_symbols)) * 1.5, 8.0)
        score_delta -= min(len(negative_matches) * 4.0, 6.0)
        score_delta = max(-6.0, min(score_delta, 8.0))
        payload = dict(item)
        payload["score"] = round(max(0.0, min(100.0, float(item.get("score") or 0.0) + score_delta)), 2)
        reasons = list(item.get("mainline_alignment_reasons") or [])
        if score_delta > 0:
            reasons.append(f"新鲜资讯触发 +{score_delta:.1f}")
        elif score_delta < 0:
            reasons.append(f"新鲜资讯风险 {score_delta:.1f}")
        payload["mainline_alignment_reasons"] = _dedupe_strings(reasons)
        payload["fresh_news_trigger"] = {
            "matched": True,
            "score_delta": round(score_delta, 2),
            "positive_count": len(positive_matches),
            "negative_count": len(negative_matches),
            "direct_symbols": _dedupe_strings(direct_symbols)[:8],
            "events": _compact_trigger_news_matches([*positive_matches, *negative_matches]),
        }
        adjusted.append(payload)
    adjusted.sort(
        key=lambda row: (
            -float((row.get("fresh_news_trigger") or {}).get("score_delta") or 0.0),
            -float(row.get("score") or 0.0),
            str(row.get("theme") or ""),
        )
    )
    return adjusted


def _trigger_news_event_theme_match(event: dict[str, Any], *, aliases: list[str], related_symbols: set[str]) -> dict[str, Any]:
    content = str(event.get("content") or "")
    positive_sectors = _dedupe_strings(event.get("positive_sectors") or [])
    negative_sectors = _dedupe_strings(event.get("negative_sectors") or [])
    event_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    positive_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("positive_symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    negative_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("negative_symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    event_symbol_list = sorted(event_symbols | positive_symbols | negative_symbols)
    positive_alias_hits = [alias for alias in aliases if alias and (alias in content or alias in positive_sectors)]
    negative_alias_hits = [alias for alias in aliases if alias and (alias in content or alias in negative_sectors)]
    direct_symbols = sorted((event_symbols | positive_symbols | negative_symbols) & related_symbols)
    if not direct_symbols and (positive_alias_hits or negative_alias_hits):
        direct_symbols = event_symbol_list
    matched = bool(direct_symbols or positive_alias_hits or negative_alias_hits)
    direction = _trigger_event_direction(event, positive_symbols=positive_symbols, negative_symbols=negative_symbols, direct_symbols=direct_symbols)
    if negative_alias_hits and not positive_alias_hits and not positive_symbols:
        direction = "negative"
    return {
        "matched": matched,
        "direction": direction,
        "digest": event.get("digest"),
        "source": event.get("source"),
        "published_at": event.get("published_at"),
        "sentiment": event.get("sentiment"),
        "change_type": event.get("change_type"),
        "content": content[:160],
        "alias_hits": _dedupe_strings([*positive_alias_hits, *negative_alias_hits])[:6],
        "direct_symbols": direct_symbols[:8],
    }


def _trigger_event_direction(
    event: dict[str, Any],
    *,
    positive_symbols: set[str] | None = None,
    negative_symbols: set[str] | None = None,
    direct_symbols: list[str] | None = None,
) -> str:
    sentiment = str(event.get("sentiment") or "neutral").strip().lower()
    direct_set = set(direct_symbols or [])
    if direct_set and negative_symbols and direct_set & negative_symbols:
        return "negative"
    if direct_set and positive_symbols and direct_set & positive_symbols:
        return "positive"
    if sentiment in {"positive", "negative"}:
        return sentiment
    if event.get("positive_sectors") and not event.get("negative_sectors"):
        return "positive"
    if event.get("negative_sectors") and not event.get("positive_sectors"):
        return "negative"
    return "neutral"


def _compact_trigger_news_matches(matches: list[dict[str, Any]], *, limit: int = 4) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for match in matches[: max(0, int(limit or 0))]:
        compact.append(
            {
                "direction": match.get("direction"),
                "source": match.get("source"),
                "published_at": match.get("published_at"),
                "sentiment": match.get("sentiment"),
                "alias_hits": match.get("alias_hits") or [],
                "direct_symbols": match.get("direct_symbols") or [],
                "content": match.get("content"),
            }
        )
    return compact


def _trigger_news_signal_for_candidate(
    *,
    symbol: str,
    features: dict[str, Any],
    primary_theme: dict[str, Any],
    trigger_news_context: dict[str, Any],
) -> dict[str, Any]:
    events = trigger_news_context.get("events") if isinstance(trigger_news_context, dict) else []
    if not isinstance(events, list) or not events:
        return {"matched": False, "status": "none", "events": []}
    normalized_symbol = _normalize_symbol(symbol)
    name = str(features.get("name") or "").strip()
    aliases = _theme_aliases(str(primary_theme.get("theme") or "").strip())
    feature_terms = _dedupe_strings(
        [
            name,
            str(features.get("industry") or ""),
            str(features.get("sector") or ""),
            *(features.get("concepts") or []),
            *aliases,
        ]
    )
    positive_matches: list[dict[str, Any]] = []
    negative_matches: list[dict[str, Any]] = []
    theme_trigger = primary_theme.get("fresh_news_trigger") if isinstance(primary_theme.get("fresh_news_trigger"), dict) else {}
    for event in events:
        if not isinstance(event, dict):
            continue
        match = _trigger_news_event_candidate_match(
            event,
            symbol=normalized_symbol,
            name=name,
            feature_terms=feature_terms,
        )
        if not match.get("matched"):
            continue
        if match.get("direction") == "negative":
            negative_matches.append(match)
        else:
            positive_matches.append(match)
    if not positive_matches and not negative_matches and theme_trigger.get("matched"):
        return {
            "matched": True,
            "status": "theme_only",
            "positive_count": int(theme_trigger.get("positive_count") or 0),
            "negative_count": int(theme_trigger.get("negative_count") or 0),
            "direct": False,
            "theme_trigger": theme_trigger,
            "events": theme_trigger.get("events") or [],
        }
    status = "none"
    if positive_matches and negative_matches:
        status = "mixed"
    elif positive_matches:
        status = "positive"
    elif negative_matches:
        status = "negative"
    return {
        "matched": bool(positive_matches or negative_matches),
        "status": status,
        "positive_count": len(positive_matches),
        "negative_count": len(negative_matches),
        "direct": any(match.get("direct") for match in [*positive_matches, *negative_matches]),
        "events": _compact_trigger_news_matches([*positive_matches, *negative_matches]),
        "theme_trigger": theme_trigger,
    }


def _trigger_news_event_candidate_match(
    event: dict[str, Any],
    *,
    symbol: str,
    name: str,
    feature_terms: list[str],
) -> dict[str, Any]:
    event_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    positive_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("positive_symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    negative_symbols = {
        str(row.get("symbol") or "").strip().upper()
        for row in event.get("negative_symbols") or []
        if isinstance(row, dict) and row.get("symbol")
    }
    content = str(event.get("content") or "")
    direct = symbol in event_symbols or symbol in positive_symbols or symbol in negative_symbols
    name_hit = bool(name and name in content)
    term_hits = [
        term
        for term in feature_terms
        if term and len(term) >= 2 and (term in content or term in (event.get("positive_sectors") or []) or term in (event.get("negative_sectors") or []))
    ]
    matched = direct or name_hit or bool(term_hits)
    direction = _trigger_event_direction(event, positive_symbols=positive_symbols, negative_symbols=negative_symbols, direct_symbols=[symbol] if direct else [])
    if symbol in negative_symbols:
        direction = "negative"
    elif symbol in positive_symbols:
        direction = "positive"
    return {
        "matched": matched,
        "direction": direction,
        "direct": direct or name_hit,
        "digest": event.get("digest"),
        "source": event.get("source"),
        "published_at": event.get("published_at"),
        "sentiment": event.get("sentiment"),
        "change_type": event.get("change_type"),
        "content": content[:160],
        "alias_hits": _dedupe_strings(term_hits)[:6],
        "direct_symbols": [symbol] if direct else [],
    }


def _trigger_news_score_adjustment(trigger_news_signal: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(trigger_news_signal, dict) or not trigger_news_signal.get("matched"):
        return {"score_delta": 0.0, "reason": None, "status": "none"}
    status = str(trigger_news_signal.get("status") or "none")
    positive_count = int(trigger_news_signal.get("positive_count") or 0)
    negative_count = int(trigger_news_signal.get("negative_count") or 0)
    direct = bool(trigger_news_signal.get("direct"))
    score_delta = 0.0
    reasons: list[str] = []
    if status == "positive":
        score_delta += 4.0 if direct else 2.5
        if positive_count > 1:
            score_delta += min((positive_count - 1) * 0.8, 2.0)
        reasons.append("新鲜利好资讯直接触发" if direct else "新鲜利好资讯主题触发")
    elif status == "negative":
        score_delta -= 6.0 if direct else 3.5
        if negative_count > 1:
            score_delta -= min((negative_count - 1) * 0.8, 2.0)
        reasons.append("新鲜风险资讯直接触发" if direct else "新鲜风险资讯主题触发")
    elif status == "mixed":
        score_delta += (2.0 if direct else 1.0) * max(positive_count, 1)
        score_delta -= (4.0 if direct else 2.0) * max(negative_count, 1)
        reasons.append("新鲜资讯多空分歧")
    elif status == "theme_only":
        theme_trigger = trigger_news_signal.get("theme_trigger") if isinstance(trigger_news_signal.get("theme_trigger"), dict) else {}
        score_delta += min(float(theme_trigger.get("score_delta") or 0.0), 3.0)
        reasons.append("新鲜资讯推升主题")
    score_delta = max(-6.0, min(score_delta, 4.5))
    return {
        "status": status,
        "score_delta": round(score_delta, 2),
        "reason": "；".join(_dedupe_strings(reasons)) if reasons and score_delta else None,
    }


def _mainline_aligned_theme_items(
    theme_items: list[dict[str, Any]],
    *,
    market_snapshot: dict[str, Any],
    market_behavior: dict[str, Any],
) -> list[dict[str, Any]]:
    if not theme_items:
        return []

    market_stats = market_snapshot.get("market_stats") or {}
    if not market_stats.get("index_turnover_amount") and not market_stats.get("up_count"):
        return [
            {
                **item,
                "mainline_alignment_score": 0.0,
                "mainline_alignment_reasons": ["市场成交额/涨跌家数缺失，先按资讯热度回退"],
            }
            for item in theme_items
        ]

    market_theme_scores = _market_theme_scores(market_snapshot, market_behavior)
    behavior_leaders = set(_behavior_leaders(market_behavior))
    aligned: list[dict[str, Any]] = []
    for item in theme_items:
        theme = str(item.get("theme") or "").strip()
        if not theme:
            continue
        aliases = _theme_aliases(theme)
        market_score = max((market_theme_scores.get(alias, 0.0) for alias in aliases), default=0.0)
        confirmation = item.get("market_confirmation") or {}
        confirmation_score = _num(confirmation.get("score")) or 0.0
        message_score = min(float(item.get("score") or 0.0), 100.0) * 0.18
        policy_score = 12.0 if item.get("policy_boost") or item.get("top_source_tier") == "S" else 0.0
        leader_score = 18.0 if any(alias in behavior_leaders for alias in aliases) else 0.0
        alignment_score = market_score + min(confirmation_score, 18.0) + message_score + policy_score + leader_score
        reasons = _mainline_alignment_reasons(
            theme=theme,
            aliases=aliases,
            market_theme_scores=market_theme_scores,
            behavior_leaders=behavior_leaders,
            confirmation_score=confirmation_score,
            item=item,
        )
        if alignment_score < MIN_MAINLINE_ALIGNMENT_SCORE and not policy_score:
            continue
        payload = dict(item)
        payload["mainline_alignment_score"] = round(alignment_score, 2)
        payload["mainline_alignment_reasons"] = reasons
        payload["score"] = round(float(item.get("score") or 0.0) + alignment_score * 0.35, 2)
        aligned.append(payload)

    if aligned:
        aligned.sort(
            key=lambda row: (
                -float(row.get("mainline_alignment_score") or 0.0),
                -float(row.get("score") or 0.0),
                str(row.get("theme") or ""),
            )
        )
        return aligned

    return [
        {
            **item,
            "mainline_alignment_score": 0.0,
            "mainline_alignment_reasons": ["市场主线数据不足，暂按资讯热度回退"],
        }
        for item in theme_items
    ]


def _market_theme_scores(market_snapshot: dict[str, Any], market_behavior: dict[str, Any]) -> dict[str, float]:
    scores: dict[str, float] = defaultdict(float)
    for item in market_snapshot.get("sector_gainers") or []:
        name = str(item.get("sector_name") or "").strip()
        if not name:
            continue
        change_pct = _num(item.get("change_pct")) or 0.0
        amount = _num(item.get("amount")) or 0.0
        scores[name] += min(max(change_pct, 0.0), 10.0) * 4.0
        scores[name] += min(max(amount / 100_000_000, 0.0), 20.0) * 0.18
    for item in market_snapshot.get("sector_losers") or []:
        name = str(item.get("sector_name") or "").strip()
        if not name:
            continue
        change_pct = _num(item.get("change_pct")) or 0.0
        scores[name] += max(change_pct, -10.0) * 2.0
    for item in market_snapshot.get("sector_inflows") or []:
        name = str(item.get("sector_name") or "").strip()
        if not name:
            continue
        net_inflow = _num(item.get("net_inflow")) or 0.0
        scores[name] += min(max(net_inflow / 100_000_000, 0.0), 20.0) * 1.4
    for item in market_snapshot.get("sector_outflows") or []:
        name = str(item.get("sector_name") or "").strip()
        if not name:
            continue
        net_inflow = _num(item.get("net_inflow")) or 0.0
        scores[name] += max(net_inflow / 100_000_000, -20.0) * 0.8

    for leader in _behavior_leaders(market_behavior):
        scores[leader] += 16.0
    return dict(scores)


def _behavior_leaders(market_behavior: dict[str, Any]) -> list[str]:
    leaders: list[str] = []
    for key in ("sector_battlefield", "style_rotation"):
        score = market_behavior.get(key, {}).get("score")
        if not isinstance(score, dict):
            continue
        leaders.extend(str(item).strip() for item in score.get("leaders") or [] if str(item).strip())
        leaders.extend(str(item).strip() for item in score.get("inflows") or [] if str(item).strip())
    return _dedupe_strings(leaders)


def _theme_aliases(theme: str) -> list[str]:
    return _dedupe_strings([theme, *(THEME_INDUSTRY_ALIASES.get(theme) or ())])


def _mainline_alignment_reasons(
    *,
    theme: str,
    aliases: list[str],
    market_theme_scores: dict[str, float],
    behavior_leaders: set[str],
    confirmation_score: float,
    item: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    market_hits = [
        alias
        for alias in aliases
        if market_theme_scores.get(alias, 0.0) > 0
    ]
    if market_hits:
        reasons.append("盘面强度确认：" + "、".join(market_hits[:3]))
    leader_hits = [alias for alias in aliases if alias in behavior_leaders]
    if leader_hits:
        reasons.append("市场行为标签指向：" + "、".join(leader_hits[:3]))
    if confirmation_score > 0:
        reasons.append(f"板块确认分 {confirmation_score:.1f}")
    if item.get("policy_boost") or item.get("top_source_tier") == "S":
        reasons.append("S级/政策催化保留")
    if not reasons:
        reasons.append(f"{theme}仅有资讯热度，盘面主线确认不足")
    return reasons[:5]


def _candidate_symbols_from_themes(theme_items: list[dict[str, Any]]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    def append_symbol(value: Any) -> None:
        symbol = _normalize_symbol(value)
        if not symbol or symbol in seen:
            return
        seen.add(symbol)
        symbols.append(symbol)

    for item in theme_items:
        for row in item.get("related_symbols") or []:
            if not isinstance(row, dict):
                continue
            append_symbol(row.get("symbol"))
        fresh_trigger = item.get("fresh_news_trigger") if isinstance(item.get("fresh_news_trigger"), dict) else {}
        for symbol in fresh_trigger.get("direct_symbols") or []:
            append_symbol(symbol)
    return symbols


def _load_daily_features(db: Session, *, symbols: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
    start_date = (pd.to_datetime(trade_date) - pd.Timedelta(days=120)).date().isoformat()
    table = preferred_daily_kline_table()
    code_to_name = get_reverse_stock_map()
    params: dict[str, Any] = {"start_date": start_date, "end_date": trade_date}
    symbol_clause = ""
    symbol_variants = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
    if symbol_variants:
        symbol_clause = "AND symbol IN :symbols"
        params["symbols"] = symbol_variants
    statement = text(
        f"""
        SELECT symbol, trade_date, open, high, low, close, volume, amount,
               turnover_rate, pre_close, float_market_cap, total_market_cap,
               net_profit_ttm, sw_industry_l1, sw_industry_l2, sw_industry_l3
        FROM {table}
        WHERE trade_date >= :start_date
          AND trade_date <= :end_date
          {symbol_clause}
        ORDER BY symbol, trade_date
        """
    )
    if symbol_variants:
        statement = statement.bindparams(bindparam("symbols", expanding=True))
    frame = pd.read_sql_query(statement, db.bind, params=params)
    if frame.empty:
        return {}
    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for column in ["open", "high", "low", "close", "volume", "amount", "turnover_rate", "pre_close", "net_profit_ttm"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(["symbol", "trade_date"])
    grouped = frame.groupby("symbol", group_keys=False)
    frame["pre_close"] = frame["pre_close"].fillna(grouped["close"].shift(1))
    frame["change_pct"] = ((frame["close"] / frame["pre_close"].replace(0, pd.NA)) - 1) * 100
    frame["amount_ma20"] = grouped["amount"].transform(lambda series: series.rolling(20, min_periods=1).mean())
    frame["amount_ratio_20d"] = (frame["amount"] / frame["amount_ma20"].replace(0, pd.NA)).fillna(1.0)
    frame["momentum_20d"] = grouped["close"].transform(lambda series: series.pct_change(20).fillna(0.0))
    frame["momentum_60d"] = grouped["close"].transform(lambda series: series.pct_change(60).fillna(0.0))
    frame["net_profit_growth_proxy"] = grouped["net_profit_ttm"].transform(lambda series: series.pct_change(60).fillna(0.0))
    latest = frame[frame["trade_date"].dt.date.astype(str) == trade_date].copy()
    if latest.empty:
        return {}
    latest["r60"] = latest["momentum_60d"].rank(pct=True).fillna(0.5) * 100
    result: dict[str, dict[str, Any]] = {}
    for _, row in latest.iterrows():
        symbol = _normalize_symbol(row["symbol"])
        concepts = _concepts_from_row(row)
        industry = _first_non_empty(row.get("sw_industry_l3"), row.get("sw_industry_l2"), row.get("sw_industry_l1"))
        result[symbol] = {
            "symbol": symbol,
            "name": code_to_name.get(symbol, symbol),
            "industry": industry,
            "sector": row.get("sw_industry_l1"),
            "concepts": concepts,
            "open": _num(row.get("open")),
            "high": _num(row.get("high")),
            "low": _num(row.get("low")),
            "close": _num(row.get("close")),
            "amount": _num(row.get("amount")),
            "turnover_rate": _num(row.get("turnover_rate")),
            "change_pct": _num(row.get("change_pct")),
            "amount_ratio_20d": _num(row.get("amount_ratio_20d")),
            "momentum_20d": _num(row.get("momentum_20d")),
            "momentum_60d": _num(row.get("momentum_60d")),
            "r60": _num(row.get("r60")),
            "net_profit_growth_proxy": _num(row.get("net_profit_growth_proxy")),
        }
    return result


def _attach_event_reaction_features(
    db: Session,
    *,
    features_by_symbol: dict[str, dict[str, Any]],
    theme_items: list[dict[str, Any]],
    trade_date: str,
    feature_trade_date: str | None = None,
    user_id: str | None = None,
    selection_trade_date: str | None = None,
    selection_window: str | None = None,
    selection_limit: int | None = None,
) -> dict[str, Any]:
    minute_table = preferred_minute_kline_table()
    feature_trade_date = feature_trade_date or trade_date
    daily_proxy_allowed = feature_trade_date == trade_date
    governance = {
        "enabled": True,
        "source": f"postgresql:{minute_table}",
        "trade_date": trade_date,
        "feature_trade_date": feature_trade_date,
        "daily_proxy_allowed": daily_proxy_allowed,
        "data_freshness": _load_event_reaction_data_freshness(
            db,
            trade_date=trade_date,
            feature_trade_date=feature_trade_date,
            minute_table=minute_table,
        ),
        "symbol_count": len(features_by_symbol),
        "covered_symbol_count": 0,
        "minute_covered_symbol_count": 0,
        "proxy_count": 0,
        "confirmed_count": 0,
        "divergent_count": 0,
        "missing_count": 0,
        "capture": {
            "requested": False,
            "requested_symbol_count": 0,
            "captured_symbol_count": 0,
            "rows": 0,
            "success": None,
            "message": None,
        },
    }
    if not features_by_symbol or not theme_items:
        return governance

    reactions = _load_event_minute_reactions(
        db,
        features_by_symbol=features_by_symbol,
        theme_items=theme_items,
        trade_date=trade_date,
        minute_table=minute_table,
    )
    missing_symbols = [symbol for symbol in features_by_symbol if symbol not in reactions]
    capture_governance = _capture_missing_event_reaction_minutes(
        db,
        symbols=missing_symbols,
        trade_date=trade_date,
        user_id=user_id,
        selection_trade_date=selection_trade_date,
        selection_window=selection_window,
        selection_limit=selection_limit,
    )
    governance["capture"] = capture_governance
    if _event_minute_capture_written_rows(capture_governance) > 0:
        reactions = _load_event_minute_reactions(
            db,
            features_by_symbol=features_by_symbol,
            theme_items=theme_items,
            trade_date=trade_date,
            minute_table=minute_table,
        )
    for symbol, features in features_by_symbol.items():
        reaction = reactions.get(symbol)
        if not reaction:
            matches = _theme_matches_for_symbol(symbol, features, theme_items)
            reaction = _daily_proxy_event_reaction(
                symbol=symbol,
                features=features,
                primary_theme=matches[0] if matches else {},
                trade_date=trade_date,
            ) if matches and daily_proxy_allowed else None
        if not reaction:
            reaction = _missing_event_reaction(trade_date=trade_date, source=f"postgresql:{minute_table}")
        features["event_reaction"] = reaction
        status = str(reaction.get("status") or "")
        if status != "missing":
            governance["covered_symbol_count"] += 1
        if reaction.get("proxy"):
            governance["proxy_count"] += 1
        elif status != "missing":
            governance["minute_covered_symbol_count"] += 1
        if status in {"confirmed", "daily_proxy_confirmed"}:
            governance["confirmed_count"] += 1
        elif status in {"divergent", "daily_proxy_divergent"}:
            governance["divergent_count"] += 1
        elif status == "missing":
            governance["missing_count"] += 1
    return governance


def _capture_missing_event_reaction_minutes(
    db: Session,
    *,
    symbols: list[str],
    trade_date: str,
    user_id: str | None,
    selection_trade_date: str | None = None,
    selection_window: str | None = None,
    selection_limit: int | None = None,
) -> dict[str, Any]:
    normalized = _dedupe_strings(_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol))
    if not normalized:
        return {
            "requested": False,
            "requested_symbol_count": 0,
            "captured_symbol_count": 0,
            "rows": 0,
            "success": None,
            "message": None,
        }
    if not user_id:
        return {
            "requested": False,
            "requested_symbol_count": len(normalized),
            "captured_symbol_count": 0,
            "rows": 0,
            "success": None,
            "message": "缺少用户上下文，跳过QMT分钟线主动补采",
            "missing_symbols": normalized,
        }
    if not _event_minute_capture_allowed(trade_date):
        return {
            "requested": False,
            "requested_symbol_count": len(normalized),
            "captured_symbol_count": 0,
            "rows": 0,
            "success": None,
            "message": _event_minute_capture_skip_message(trade_date),
            "missing_symbols": normalized,
        }
    requested_symbols = normalized[:MAX_EVENT_REACTION_CAPTURE_SYMBOLS]
    payload: dict[str, Any] = {
        "requested": True,
        "requested_symbol_count": len(requested_symbols),
        "skipped_symbol_count": max(len(normalized) - len(requested_symbols), 0),
        "captured_symbol_count": 0,
        "rows": 0,
        "success": False,
        "message": None,
        "source": "qmt_intraday",
        "trade_date": trade_date,
        "symbols": requested_symbols,
        "missing_symbols": requested_symbols,
        "timeout_seconds": _event_minute_capture_timeout_seconds(),
        "retry_missing": False,
        "history_backfill": {
            "requested": False,
            "status": "skipped",
            "message": None,
        },
        "akshare_backfill": {
            "requested": False,
            "status": "skipped",
            "message": None,
        },
    }
    try:
        result = capture_intraday_symbols(
            requested_symbols,
            trade_date=trade_date,
            period="1m",
            account_key=None,
            db=db,
            user_id=user_id,
            timeout_seconds=float(payload["timeout_seconds"]),
            retry_missing=False,
        )
    except Exception as exc:
        payload["message"] = f"QMT分钟线补采异常：{exc}"
        logger.warning(
            "[catalyst-selection] event minute capture failed symbols=%s trade_date=%s error=%s",
            len(requested_symbols),
            trade_date,
            exc,
        )
        payload["history_backfill"] = _request_event_minute_history_backfill(
            requested_symbols,
            trade_date=trade_date,
            db=db,
            user_id=user_id,
        )
        if str((payload.get("history_backfill") or {}).get("status") or "") != "scheduled":
            payload["akshare_backfill"] = _schedule_event_minute_akshare_backfill(
                requested_symbols,
                trade_date=trade_date,
                selection_trade_date=selection_trade_date,
                selection_window=selection_window,
                selection_limit=selection_limit,
                user_id=user_id,
            )
        return payload

    captured_symbols = [
        _normalize_symbol(symbol)
        for symbol in (result.get("captured_symbols") or [])
        if _normalize_symbol(symbol)
    ]
    payload.update(
        {
            "captured_symbol_count": len(set(captured_symbols)),
            "rows": int(result.get("rows") or 0),
            "success": bool(result.get("success")),
            "message": result.get("message"),
            "source": result.get("source") or "qmt_intraday",
            "missing_symbols": result.get("missing_symbols") or [],
            "symbol_rows": result.get("symbol_rows") or {},
            "symbol_latest_trade_times": result.get("symbol_latest_trade_times") or {},
            "partial": bool(result.get("partial")),
        }
    )
    if int(payload.get("rows") or 0) <= 0:
        payload["history_backfill"] = _request_event_minute_history_backfill(
            requested_symbols,
            trade_date=trade_date,
            db=db,
            user_id=user_id,
        )
        if str((payload.get("history_backfill") or {}).get("status") or "") != "scheduled":
            payload["akshare_backfill"] = _schedule_event_minute_akshare_backfill(
                requested_symbols,
                trade_date=trade_date,
                selection_trade_date=selection_trade_date,
                selection_window=selection_window,
                selection_limit=selection_limit,
                user_id=user_id,
            )
    return payload


def _event_minute_capture_written_rows(capture_governance: dict[str, Any]) -> int:
    rows = int(capture_governance.get("rows") or 0)
    akshare = capture_governance.get("akshare_backfill") if isinstance(capture_governance.get("akshare_backfill"), dict) else {}
    return rows + int((akshare or {}).get("rows") or 0)


def _request_event_minute_history_backfill(
    symbols: list[str],
    *,
    trade_date: str,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized = _dedupe_strings(_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol))
    base_payload: dict[str, Any] = {
        "requested": False,
        "status": "skipped",
        "source": "qmt_history_bridge",
        "trade_date": trade_date,
        "start_date": trade_date,
        "end_date": trade_date,
        "requested_symbol_count": len(normalized),
        "symbols": normalized,
        "job_id": None,
        "bridge": None,
        "account_key": None,
        "role": None,
        "timeout_seconds": _event_minute_history_backfill_timeout_seconds(),
        "message": None,
    }
    if not normalized:
        base_payload["message"] = "无缺失标的，跳过QMT历史分钟线回填"
        return base_payload
    if not _event_minute_history_backfill_enabled():
        base_payload["message"] = "AI_QUANT_EVENT_MINUTE_HISTORY_BACKFILL 已关闭，跳过历史分钟线回填"
        return base_payload

    bridge_config = _resolve_event_minute_history_bridge(db=db, user_id=user_id)
    if not bridge_config:
        base_payload["message"] = "未找到可用的QMT历史分钟线bridge，无法发起定向回填"
        return base_payload

    base_url = str(bridge_config.get("bridge_base_url") or "").rstrip("/")
    token = str(bridge_config.get("bridge_token") or "")
    account_key = str(bridge_config.get("account_key") or "paper_sim").strip() or "paper_sim"
    role = str(bridge_config.get("role") or "paper").strip() or "paper"
    base_payload.update({"bridge": base_url or None, "account_key": account_key, "role": role})
    if not base_url:
        base_payload["message"] = "QMT历史分钟线bridge_base_url为空"
        return base_payload
    bridge_key = _event_history_bridge_key(base_url, account_key, role)
    cooldown_state = _event_history_bridge_cooldown_state(bridge_key)
    if cooldown_state:
        base_payload.update(
            {
                "status": "cooldown",
                "degraded": True,
                "cooldown_until": cooldown_state.get("cooldown_until"),
                "last_error": cooldown_state.get("last_error"),
                "message": (
                    f"QMT历史分钟线bridge处于不可达冷却期，跳过本次请求：{base_url}；"
                    "已转向外部分钟线补缺。"
                ),
            }
        )
        return base_payload

    database_url = str(os.getenv("QMT_MINUTE_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        base_payload.update({"status": "failed", "message": "缺少 QMT_MINUTE_DATABASE_URL / DATABASE_URL，无法让Windows bridge导入分钟线"})
        return base_payload
    try:
        from api.data_downloader import DataDownloader
    except Exception as exc:
        base_payload.update({"status": "failed", "message": f"初始化QMT历史分钟线回填失败：{exc}"[:240]})
        return base_payload
    if DataDownloader._database_url_is_localhost_for_remote_bridge(database_url, base_url):
        base_payload.update(
            {
                "status": "failed",
                "message": "当前分钟线数据库地址对远端QMT bridge不可达，请配置 QMT_MINUTE_DATABASE_URL 为Windows可访问的PostgreSQL地址。",
            }
        )
        return base_payload

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    request_payload = {
        "period": "1m",
        "start_date": trade_date,
        "end_date": trade_date,
        "sector": "all_a",
        "symbols": normalized,
        "file_format": "parquet",
        "import_db": True,
        "skip_export": str(os.getenv("QMT_MINUTE_SKIP_EXPORT", "1") or "1").strip().lower() in {"1", "true", "yes", "on"},
        "database_url": database_url,
        "force": False,
        "window_days": int(os.getenv("QMT_MINUTE_WINDOW_DAYS", "365") or 365),
        "retry_times": 2,
        "retry_sleep": 1,
    }
    base_payload["requested"] = True
    try:
        response = requests.post(
            f"{base_url}/history/minute/sync",
            json=request_payload,
            headers=headers,
            timeout=float(base_payload["timeout_seconds"]),
        )
        response.raise_for_status()
        job = response.json()
    except Exception as exc:
        logger.warning(
            "[catalyst-selection] event minute history backfill request failed symbols=%s trade_date=%s bridge=%s error=%s",
            len(normalized),
            trade_date,
            base_url,
            exc,
        )
        compact_error = _compact_qmt_history_backfill_error(exc, base_url)
        failure_state = _remember_event_history_bridge_failure(
            bridge_key,
            bridge=base_url,
            account_key=account_key,
            role=role,
            error=compact_error,
        )
        base_payload.update(
            {
                "status": "failed",
                "degraded": True,
                "cooldown_until": failure_state.get("cooldown_until"),
                "message": compact_error,
            }
        )
        return base_payload

    job_id = str(job.get("job_id") or "")
    if not job_id:
        compact_error = f"QMT历史分钟线bridge未返回job_id：{str(job)[:180]}"
        failure_state = _remember_event_history_bridge_failure(
            bridge_key,
            bridge=base_url,
            account_key=account_key,
            role=role,
            error=compact_error,
        )
        base_payload.update(
            {
                "status": "failed",
                "degraded": True,
                "cooldown_until": failure_state.get("cooldown_until"),
                "message": compact_error,
            }
        )
        return base_payload
    _clear_event_history_bridge_failure(bridge_key)
    base_payload.update(
        {
            "status": "scheduled",
            "job_id": job_id,
            "message": str(job.get("message") or "已发起QMT历史分钟线定向回填，刷新后会重新读取真实分钟反应。")[:240],
            "bridge_progress": job.get("progress"),
        }
    )
    return base_payload


def _event_history_bridge_key(base_url: str, account_key: str, role: str) -> str:
    return "|".join(
        (
            str(base_url or "").strip().rstrip("/"),
            str(account_key or "").strip(),
            str(role or "").strip(),
        )
    )


def _event_history_bridge_cooldown_state(bridge_key: str) -> dict[str, Any] | None:
    with _EVENT_HISTORY_BRIDGE_LOCK:
        state = _EVENT_HISTORY_BRIDGE_FAILURES.get(bridge_key)
        if not state:
            return None
        failed_at = _parse_datetime_or_none(state.get("failed_at"))
        if failed_at is None:
            _EVENT_HISTORY_BRIDGE_FAILURES.pop(bridge_key, None)
            return None
        elapsed = (_utcnow() - failed_at).total_seconds()
        cooldown_seconds = _event_minute_history_bridge_cooldown_seconds()
        if elapsed >= cooldown_seconds:
            _EVENT_HISTORY_BRIDGE_FAILURES.pop(bridge_key, None)
            return None
        cooldown_until = failed_at + timedelta(seconds=cooldown_seconds)
        return {
            **state,
            "cooldown_seconds": cooldown_seconds,
            "remaining_seconds": max(round(cooldown_seconds - elapsed, 1), 0.0),
            "cooldown_until": cooldown_until.isoformat(),
        }


def _remember_event_history_bridge_failure(
    bridge_key: str,
    *,
    bridge: str,
    account_key: str,
    role: str,
    error: str,
) -> dict[str, Any]:
    failed_at = _utcnow()
    cooldown_until = failed_at + timedelta(seconds=_event_minute_history_bridge_cooldown_seconds())
    state = {
        "bridge": bridge,
        "account_key": account_key,
        "role": role,
        "last_error": str(error or "")[:240],
        "failed_at": failed_at.isoformat(),
        "cooldown_until": cooldown_until.isoformat(),
    }
    with _EVENT_HISTORY_BRIDGE_LOCK:
        _EVENT_HISTORY_BRIDGE_FAILURES[bridge_key] = state
    return state


def _clear_event_history_bridge_failure(bridge_key: str) -> None:
    with _EVENT_HISTORY_BRIDGE_LOCK:
        _EVENT_HISTORY_BRIDGE_FAILURES.pop(bridge_key, None)


def _compact_qmt_history_backfill_error(exc: Exception, base_url: str) -> str:
    message = str(exc) or exc.__class__.__name__
    lowered = message.lower()
    if "connecttimeout" in lowered or "timed out" in lowered or "timeout" in lowered:
        return f"QMT历史分钟线bridge连接超时：{base_url}"
    if "connection" in lowered or "max retries" in lowered or "failed to establish" in lowered:
        return f"QMT历史分钟线bridge不可达：{base_url}"
    return f"QMT历史分钟线回填任务创建失败：{message}"[:240]


def _compact_external_minute_error(error: Any) -> str:
    message = str(error or "").strip()
    lowered = message.lower()
    if not message:
        return "unknown"
    if "proxyerror" in lowered or "unable to connect to proxy" in lowered or "remote end closed connection" in lowered:
        return "外部行情源代理连接失败"
    if "connecttimeout" in lowered or "read timed out" in lowered or "timeout" in lowered:
        return "外部行情源连接超时"
    if "no_data" in lowered or "无数据" in message or "日期范围内无数据" in message:
        return "外部行情源无分钟线"
    return message[:120]


def _resolve_event_minute_history_bridge(*, db: Session | None, user_id: str | None) -> dict[str, str] | None:
    preferred_key = (
        str(os.getenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY") or os.getenv("QMT_HISTORY_ACCOUNT_KEY") or "").strip()
        or None
    )
    if db is not None and user_id:
        try:
            from api.services import qmt_virtual_account_service

            configs = qmt_virtual_account_service._load_runtime_configs(db=db, user_id=user_id)
            usable = [
                config for config in configs
                if bool(config.enabled and str(config.bridge_base_url or "").strip())
            ]
            if preferred_key:
                for config in usable:
                    if config.key == preferred_key:
                        return {
                            "bridge_base_url": str(config.bridge_base_url or ""),
                            "bridge_token": str(config.bridge_token or ""),
                            "account_key": str(config.key or preferred_key),
                            "account_id": str(config.account_id or ""),
                            "role": str(config.role or "paper"),
                        }
            for role in ("paper", "live"):
                for config in usable:
                    if str(config.role or "").strip().lower() == role:
                        return {
                            "bridge_base_url": str(config.bridge_base_url or ""),
                            "bridge_token": str(config.bridge_token or ""),
                            "account_key": str(config.key or ""),
                            "account_id": str(config.account_id or ""),
                            "role": str(config.role or role),
                        }
            if usable:
                config = usable[0]
                return {
                    "bridge_base_url": str(config.bridge_base_url or ""),
                    "bridge_token": str(config.bridge_token or ""),
                    "account_key": str(config.key or ""),
                    "account_id": str(config.account_id or ""),
                    "role": str(config.role or "paper"),
                }
        except Exception as exc:
            logger.warning("[catalyst-selection] failed to resolve user QMT history bridge user_id=%s error=%s", user_id, exc)
    try:
        from api.data_downloader import DataDownloader

        return DataDownloader._resolve_qmt_history_bridge()
    except Exception as exc:
        logger.warning("[catalyst-selection] failed to resolve env QMT history bridge error=%s", exc)
        return None


def _schedule_event_minute_akshare_backfill(
    symbols: list[str],
    *,
    trade_date: str,
    selection_trade_date: str | None = None,
    selection_window: str | None = None,
    selection_limit: int | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized = _dedupe_strings(_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol))
    sync_mode = _event_minute_akshare_sync_enabled(trade_date)
    max_symbols = _event_minute_akshare_backfill_symbol_limit()
    if sync_mode:
        max_symbols = min(max_symbols, _event_minute_akshare_sync_symbol_limit())
    selected = normalized[:max_symbols]
    base_payload: dict[str, Any] = {
        "requested": False,
        "status": "skipped",
        "source": "akshare",
        "mode": "sync" if sync_mode else "async",
        "trade_date": trade_date,
        "requested_symbol_count": len(selected),
        "skipped_symbol_count": max(len(normalized) - len(selected), 0),
        "symbols": selected,
        "job_key": None,
        "rows": 0,
        "message": None,
    }
    refresh_context = _build_event_akshare_selection_refresh_context(
        trade_date=selection_trade_date,
        window=selection_window,
        limit=selection_limit,
        user_id=user_id,
    )
    if not selected:
        base_payload["message"] = "无缺失标的，跳过AKShare分钟线补缺"
        return base_payload
    if not _event_minute_akshare_backfill_enabled():
        base_payload["message"] = "AI_QUANT_EVENT_MINUTE_AKSHARE_BACKFILL 已关闭，跳过AKShare分钟线补缺"
        return base_payload

    job_key = hashlib.sha1(f"{trade_date}:{','.join(selected)}".encode("utf-8")).hexdigest()[:16]
    base_payload["job_key"] = job_key
    with _EVENT_AKSHARE_BACKFILL_LOCK:
        existing = _EVENT_AKSHARE_BACKFILL_JOBS.get(job_key)
        if existing and _event_akshare_backfill_job_recent(existing):
            _merge_event_akshare_selection_refresh_context_locked(
                existing,
                refresh_context,
                enabled=not sync_mode,
            )
            return {**base_payload, **_public_event_akshare_backfill_job(existing)}
        job = {
            **base_payload,
            "requested": True,
            "status": "scheduled",
            "message": (
                "AKShare候选标的分钟线同步补缺中，完成后本次刷新会尝试读取真实分钟反应。"
                if sync_mode
                else "已安排AKShare候选标的分钟线补缺，完成后刷新将读取真实分钟反应。"
            ),
            "created_at": _utcnow().isoformat(),
            "updated_at": _utcnow().isoformat(),
            "selection_refresh_enabled": bool(refresh_context and not sync_mode),
            "selection_refresh_contexts": {},
            "selection_refresh": _initial_event_akshare_selection_refresh(
                refresh_context,
                enabled=not sync_mode,
            ),
        }
        _merge_event_akshare_selection_refresh_context_locked(
            job,
            refresh_context,
            enabled=not sync_mode,
        )
        _EVENT_AKSHARE_BACKFILL_JOBS[job_key] = job
        if not sync_mode:
            thread = threading.Thread(
                target=_run_event_minute_akshare_backfill_job,
                args=(job_key, selected, trade_date),
                daemon=True,
                name=f"event-akshare-minute-{job_key}",
            )
            thread.start()
            return {**base_payload, **_public_event_akshare_backfill_job(job)}
    if sync_mode:
        _run_event_minute_akshare_backfill_job(job_key, selected, trade_date)
        with _EVENT_AKSHARE_BACKFILL_LOCK:
            finished = dict(_EVENT_AKSHARE_BACKFILL_JOBS.get(job_key) or job)
        return {**base_payload, **_public_event_akshare_backfill_job(finished)}
    return {**base_payload, **_public_event_akshare_backfill_job(job)}


def _build_event_akshare_selection_refresh_context(
    *,
    trade_date: str | None,
    window: str | None,
    limit: int | None,
    user_id: str | None,
) -> dict[str, Any] | None:
    if not trade_date or not window or not limit:
        return None
    try:
        normalized_window = _normalize_window(window)
    except Exception:
        normalized_window = str(window or "").strip() or "premarket"
    return {
        "trade_date": str(trade_date)[:10],
        "window": normalized_window,
        "limit": max(1, min(int(limit or DEFAULT_SELECTION_LIMIT), MAX_SELECTION_LIMIT)),
        "user_id": user_id,
    }


def _event_akshare_selection_refresh_context_key(context: dict[str, Any]) -> str:
    return _selection_refresh_key(
        str(context.get("trade_date") or ""),
        str(context.get("window") or ""),
        int(context.get("limit") or DEFAULT_SELECTION_LIMIT),
        str(context.get("user_id") or "") or None,
    )


def _initial_event_akshare_selection_refresh(context: dict[str, Any] | None, *, enabled: bool) -> dict[str, Any]:
    if not context:
        return {
            "requested": False,
            "status": "skipped",
            "message": "未携带机会榜刷新上下文，AKShare补缺完成后不自动重算",
        }
    if not enabled:
        return {
            "requested": False,
            "status": "not_needed",
            "message": "同步补缺会在当前生成流程内直接读取分钟线，不额外触发后台重算",
        }
    return {
        "requested": True,
        "status": "pending",
        "message": "AKShare分钟线补缺完成后将自动重算对应机会榜",
        "refreshed_count": 0,
        "failed_count": 0,
    }


def _merge_event_akshare_selection_refresh_context_locked(
    job: dict[str, Any],
    context: dict[str, Any] | None,
    *,
    enabled: bool,
) -> None:
    if not context:
        return
    contexts = job.setdefault("selection_refresh_contexts", {})
    if not isinstance(contexts, dict):
        contexts = {}
        job["selection_refresh_contexts"] = contexts
    contexts[_event_akshare_selection_refresh_context_key(context)] = context
    job["selection_refresh_enabled"] = bool(job.get("selection_refresh_enabled") or enabled)
    refresh = dict(job.get("selection_refresh") or {})
    if enabled and str(refresh.get("status") or "") in {"", "skipped", "not_needed"}:
        refresh.update(_initial_event_akshare_selection_refresh(context, enabled=True))
    job["selection_refresh"] = refresh or _initial_event_akshare_selection_refresh(context, enabled=enabled)


def _run_event_minute_akshare_backfill_job(job_key: str, symbols: list[str], trade_date: str) -> None:
    try:
        run_async(_run_event_minute_akshare_backfill_job_async(job_key, symbols, trade_date))
    except Exception as exc:
        logger.warning("[catalyst-selection] AKShare event minute backfill job failed key=%s error=%s", job_key, exc)
        _update_event_akshare_backfill_job(
            job_key,
            status="failed",
            message=f"AKShare分钟线补缺异常：{exc}"[:240],
        )


async def _run_event_minute_akshare_backfill_job_async(job_key: str, symbols: list[str], trade_date: str) -> None:
    from api.data_downloader import DataDownloader

    _update_event_akshare_backfill_job(job_key, status="running", message="AKShare候选标的分钟线补缺运行中")
    target_date = date.fromisoformat(trade_date)
    downloader = DataDownloader(None)
    rows_total = 0
    failures: list[str] = []
    for symbol in symbols:
        result = await downloader.download_minute_kline(
            symbol,
            target_date,
            target_date,
            force=False,
            source="akshare",
        )
        rows = int(result.get("records") or 0)
        rows_total += rows
        if not result.get("success"):
            failures.append(f"{symbol}:{_compact_external_minute_error(result.get('error') or 'no_data')}")
        _update_event_akshare_backfill_job(
            job_key,
            status="running",
            rows=rows_total,
            message=f"AKShare分钟线补缺运行中，已处理 {symbol}，累计 {rows_total} 行",
        )
    status = "completed" if rows_total > 0 else "empty"
    message = f"AKShare分钟线补缺完成，写入 {rows_total} 行"
    if failures:
        message = f"{message}；失败 {len(failures)} 只：{'；'.join(failures[:3])}"
    _update_event_akshare_backfill_job(
        job_key,
        status=status,
        rows=rows_total,
        failures=failures[:8],
        message=message[:240],
        finished_at=_utcnow().isoformat(),
    )
    _refresh_selection_runs_after_event_akshare_backfill(job_key, rows_total=rows_total)


def _event_akshare_selection_refresh_contexts(job: dict[str, Any]) -> list[dict[str, Any]]:
    contexts = job.get("selection_refresh_contexts") or {}
    if isinstance(contexts, dict):
        return [dict(value) for value in contexts.values() if isinstance(value, dict)]
    if isinstance(contexts, list):
        return [dict(value) for value in contexts if isinstance(value, dict)]
    return []


def _public_event_akshare_selection_context(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_date": context.get("trade_date"),
        "window": context.get("window"),
        "limit": context.get("limit"),
        "user_bound": bool(context.get("user_id")),
    }


def _update_event_akshare_selection_refresh(job_key: str, **updates: Any) -> None:
    with _EVENT_AKSHARE_BACKFILL_LOCK:
        job = _EVENT_AKSHARE_BACKFILL_JOBS.setdefault(job_key, {"job_key": job_key})
        refresh = dict(job.get("selection_refresh") or {})
        refresh.update(updates)
        refresh["updated_at"] = _utcnow().isoformat()
        job["selection_refresh"] = refresh
        job["updated_at"] = _utcnow().isoformat()


def _refresh_selection_runs_after_event_akshare_backfill(job_key: str, *, rows_total: int) -> None:
    with _EVENT_AKSHARE_BACKFILL_LOCK:
        job = dict(_EVENT_AKSHARE_BACKFILL_JOBS.get(job_key) or {})
        contexts = _event_akshare_selection_refresh_contexts(job)
        enabled = bool(job.get("selection_refresh_enabled"))

    if not enabled or not contexts:
        _update_event_akshare_selection_refresh(
            job_key,
            requested=False,
            status="skipped",
            message="未携带机会榜刷新上下文，AKShare补缺完成后不自动重算",
        )
        return
    if rows_total <= 0:
        _update_event_akshare_selection_refresh(
            job_key,
            requested=True,
            status="skipped",
            message="AKShare分钟线补缺未写入新行，跳过机会榜自动重算",
            refreshed_count=0,
            failed_count=0,
            results=[],
            errors=[],
        )
        return

    _update_event_akshare_selection_refresh(
        job_key,
        requested=True,
        status="running",
        message="AKShare分钟线补缺已写入，正在自动重算对应机会榜",
    )
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for context in contexts:
        db = SessionLocal()
        try:
            payload = generate_selections(
                db,
                trade_date=str(context.get("trade_date") or ""),
                window=str(context.get("window") or "premarket"),
                limit=int(context.get("limit") or DEFAULT_SELECTION_LIMIT),
                user_id=context.get("user_id"),
            )
            items = payload.get("items") or []
            top_item = items[0] if items else {}
            results.append(
                {
                    **_public_event_akshare_selection_context(context),
                    "item_count": len(items),
                    "top_symbol": top_item.get("symbol"),
                    "top_score": top_item.get("score"),
                    "updated_at": payload.get("updated_at"),
                }
            )
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                logger.exception("[catalyst-selection] rollback failed after AKShare-triggered refresh error")
            logger.exception(
                "[catalyst-selection] AKShare-triggered selection refresh failed job=%s trade_date=%s window=%s",
                job_key,
                context.get("trade_date"),
                context.get("window"),
            )
            errors.append(
                {
                    **_public_event_akshare_selection_context(context),
                    "error": str(exc)[:240],
                }
            )
        finally:
            try:
                db.close()
            except Exception:
                pass

    status = "completed" if not errors else ("partial_failed" if results else "failed")
    _update_event_akshare_selection_refresh(
        job_key,
        requested=True,
        status=status,
        message=(
            f"AKShare补缺后已自动重算机会榜 {len(results)} 个窗口"
            if not errors
            else f"AKShare补缺后机会榜重算完成 {len(results)} 个窗口，失败 {len(errors)} 个"
        ),
        refreshed_count=len(results),
        failed_count=len(errors),
        results=results,
        errors=errors,
        finished_at=_utcnow().isoformat(),
    )


def _update_event_akshare_backfill_job(job_key: str, **updates: Any) -> None:
    with _EVENT_AKSHARE_BACKFILL_LOCK:
        job = _EVENT_AKSHARE_BACKFILL_JOBS.setdefault(job_key, {"job_key": job_key})
        job.update(updates)
        job["updated_at"] = _utcnow().isoformat()


def _public_event_akshare_backfill_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "requested": bool(job.get("requested")),
        "status": str(job.get("status") or "unknown"),
        "source": "akshare",
        "mode": job.get("mode") or "async",
        "trade_date": job.get("trade_date"),
        "requested_symbol_count": int(job.get("requested_symbol_count") or 0),
        "skipped_symbol_count": int(job.get("skipped_symbol_count") or 0),
        "symbols": job.get("symbols") or [],
        "job_key": job.get("job_key"),
        "rows": int(job.get("rows") or 0),
        "message": job.get("message"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "failures": job.get("failures") or [],
        "selection_refresh": {
            **dict(job.get("selection_refresh") or {}),
            "contexts": [
                _public_event_akshare_selection_context(context)
                for context in _event_akshare_selection_refresh_contexts(job)
            ],
            "context_count": len(_event_akshare_selection_refresh_contexts(job)),
        },
    }


def _event_akshare_backfill_job_recent(job: dict[str, Any]) -> bool:
    status = str(job.get("status") or "")
    if status in {"scheduled", "running"}:
        return True
    updated_at = str(job.get("updated_at") or "").strip()
    if not updated_at:
        return False
    try:
        updated = datetime.fromisoformat(updated_at)
    except ValueError:
        return False
    return (_utcnow() - updated).total_seconds() <= _event_minute_akshare_backfill_cooldown_seconds()


def _load_event_reaction_data_freshness(
    db: Session,
    *,
    trade_date: str,
    feature_trade_date: str,
    minute_table: str,
) -> dict[str, Any]:
    daily_table = preferred_daily_kline_table()
    payload: dict[str, Any] = {
        "daily_table": daily_table,
        "minute_table": minute_table,
        "event_reaction_trade_date": trade_date,
        "feature_trade_date": feature_trade_date,
        "status": "unknown",
        "message": "分钟线新鲜度待确认",
    }
    try:
        current_trade_date = _effective_cn_trade_date()
        preopen_session = trade_date == current_trade_date and _current_cn_before_open_session()
        latest_daily = db.execute(text(f"SELECT trade_date FROM {daily_table} ORDER BY trade_date DESC LIMIT 1")).scalar()
        latest_minute = db.execute(text(f"SELECT trade_time FROM {minute_table} ORDER BY trade_time DESC LIMIT 1")).scalar()
        start = datetime.combine(date.fromisoformat(trade_date), time.min)
        end = start + timedelta(days=1)
        target = db.execute(
            text(
                f"""
                SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols, MAX(trade_time) AS latest_trade_time
                FROM {minute_table}
                WHERE trade_time >= :start AND trade_time < :end
                """
            ),
            {"start": start, "end": end},
        ).mappings().first()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        payload.update({"status": "error", "message": f"读取分钟线新鲜度失败：{exc}"[:240]})
        return payload

    latest_daily_text = str(latest_daily or "")[:10] or None
    latest_minute_text = str(latest_minute or "") or None
    latest_minute_date = latest_minute_text[:10] if latest_minute_text else None
    target_rows = int((target or {}).get("rows") or 0)
    target_symbols = int((target or {}).get("symbols") or 0)
    target_latest = str((target or {}).get("latest_trade_time") or "") or None
    payload.update(
        {
            "latest_daily_trade_date": latest_daily_text,
            "latest_minute_trade_time": latest_minute_text,
            "latest_minute_trade_date": latest_minute_date,
            "target_minute_rows": target_rows,
            "target_minute_symbol_count": target_symbols,
            "target_latest_trade_time": target_latest,
        }
    )
    if target_rows > 0:
        daily_lagged = bool(feature_trade_date and feature_trade_date < trade_date)
        payload.update(
            {
                "status": "ready_with_lagged_daily_features" if daily_lagged else "ready",
                "message": (
                    f"事件反应日 {trade_date} 已有分钟线 {target_rows} 行，覆盖 {target_symbols} 只标的；"
                    f"日线特征截至 {feature_trade_date}，市场状态按滞后日K降级使用。"
                    if daily_lagged
                    else f"事件反应日 {trade_date} 已有分钟线 {target_rows} 行，覆盖 {target_symbols} 只标的。"
                ),
            }
        )
    elif latest_minute_date:
        stale = latest_minute_date < trade_date
        payload.update(
            {
                "status": "pending_preopen" if preopen_session else ("stale" if stale else "target_missing"),
                "message": (
                    f"事件反应日 {trade_date} 仍在盘前，尚无当日分钟线；待开盘后自动补采。"
                    if preopen_session
                    else f"事件反应日 {trade_date} 缺分钟线；分钟线最新到 {latest_minute_text}。"
                ),
            }
        )
    else:
        payload.update(
            {
                "status": "pending_preopen" if preopen_session else "empty",
                "message": (
                    f"事件反应日 {trade_date} 仍在盘前，分钟线尚未产生；待开盘后自动补采。"
                    if preopen_session
                    else f"事件反应日 {trade_date} 缺分钟线；{minute_table} 当前无可用分钟线。"
                ),
            }
        )
    return payload


def _build_market_state_freshness(
    *,
    feature_trade_date: str,
    event_reaction_trade_date: str,
    event_reaction_governance: dict[str, Any],
    minute_market_proxy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    freshness = event_reaction_governance.get("data_freshness")
    if not isinstance(freshness, dict):
        freshness = {}
    minute_status = str(freshness.get("status") or "unknown")
    target_rows = int(freshness.get("target_minute_rows") or 0)
    target_symbols = int(freshness.get("target_minute_symbol_count") or 0)
    latest_daily = str(freshness.get("latest_daily_trade_date") or feature_trade_date or "")[:10] or None
    latest_minute_date = str(freshness.get("latest_minute_trade_date") or "")[:10] or None
    aligned = feature_trade_date == event_reaction_trade_date
    if aligned:
        status = "aligned"
        message = f"市场状态与事件反应同为 {feature_trade_date}。"
    elif target_rows > 0:
        status = "minute_ready_daily_lagged"
        message = (
            f"事件反应已使用 {event_reaction_trade_date} 分钟线（{target_rows} 行/{target_symbols} 只），"
            f"但市场宽度和日线特征仍截至 {feature_trade_date}。"
        )
    else:
        status = "daily_lagged_minute_unready"
        message = (
            f"事件反应日 {event_reaction_trade_date} 分钟线未就绪，"
            f"市场宽度和日线特征仍截至 {feature_trade_date}。"
        )
    payload = {
        "status": status,
        "message": message,
        "feature_trade_date": feature_trade_date,
        "event_reaction_trade_date": event_reaction_trade_date,
        "latest_daily_trade_date": latest_daily,
        "latest_minute_trade_date": latest_minute_date,
        "minute_status": minute_status,
        "target_minute_rows": target_rows,
        "target_minute_symbol_count": target_symbols,
        "is_aligned": aligned,
    }
    if isinstance(minute_market_proxy, dict) and minute_market_proxy:
        payload["minute_market_proxy"] = minute_market_proxy
    return payload


def _apply_intraday_market_state_to_behavior(
    market_behavior: dict[str, Any],
    *,
    market_state_freshness: dict[str, Any] | None = None,
    minute_market_proxy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    behavior = dict(market_behavior or {})
    proxy = minute_market_proxy if isinstance(minute_market_proxy, dict) else {}
    status = str(proxy.get("status") or "").strip()
    if status in {"", "unavailable"}:
        return behavior

    symbol_count = int(proxy.get("symbol_count") or 0)
    row_count = int(proxy.get("row_count") or 0)
    positive_ratio = _num(proxy.get("positive_ratio"))
    average_change = _num(proxy.get("average_change_pct"))
    trade_date = str(proxy.get("trade_date") or "")[:10] or None
    full_market = bool(proxy.get("is_full_market_breadth"))
    coverage_label = "全市场分钟宽度" if full_market else "部分分钟样本"
    ratio_text = f"{positive_ratio:.0%}" if positive_ratio is not None else "--"
    average_text = f"{average_change:+.2f}%" if average_change is not None else "--"

    labels = {
        "constructive": ("盘中样本偏强", 72.0, "盘中样本偏强，可提高事件确认权重，但仍需看主线承接。"),
        "mixed": ("盘中样本分化", 55.0, "盘中样本分化，优先等待核心标的承接，不追后排扩散。"),
        "weak": ("盘中样本偏弱", 38.0, "盘中样本偏弱，降低开仓优先级并提高确认要求。"),
        "risk_off": ("盘中样本转弱", 25.0, "盘中样本转弱，优先保护本金和观察风险释放。"),
        "thin_sample": ("盘中样本不足", 42.0, "盘中样本不足，只能作为弱确认信号。"),
    }
    label, score, action_hint = labels.get(status, ("盘中样本待确认", 50.0, "盘中样本待确认，不能替代全市场宽度。"))
    detail = (
        f"{label}：{coverage_label} {symbol_count} 只/{row_count} 行，"
        f"上涨占比 {ratio_text}，均值 {average_text}。{action_hint}"
    )
    if not full_market:
        detail += "该口径不是全市场宽度。"

    intraday_state = {
        "label": label,
        "detail": detail,
        "score": score,
        "status": status,
        "trade_date": trade_date,
        "coverage_scope": proxy.get("coverage_scope"),
        "symbol_count": symbol_count,
        "row_count": row_count,
        "positive_ratio": proxy.get("positive_ratio"),
        "average_change_pct": proxy.get("average_change_pct"),
        "is_full_market_breadth": full_market,
        "source": proxy.get("source"),
    }
    behavior["intraday_market_state"] = intraday_state
    behavior["market_state_source"] = (
        "intraday_minute_proxy_with_lagged_daily_features"
        if not (market_state_freshness or {}).get("is_aligned")
        else "intraday_minute_proxy"
    )

    if status in {"weak", "risk_off", "thin_sample", "mixed"}:
        behavior["market_regime"] = {
            "label": label,
            "detail": detail,
            "score": score,
        }
        behavior["risk_pressure"] = {
            "label": "盘中分化风险" if status == "mixed" else label,
            "detail": detail,
            "score": score,
        }
    elif status == "constructive":
        behavior["market_regime"] = {
            "label": "盘中样本偏强",
            "detail": detail,
            "score": score,
        }

    data_quality = dict(behavior.get("data_quality") or {})
    sources = dict(data_quality.get("source") or {})
    sources["intraday_market_state"] = proxy.get("source")
    data_quality["source"] = sources
    limitations = list(data_quality.get("limitations") or [])
    if not full_market and "minute_proxy_not_full_market_breadth" not in limitations:
        limitations.append("minute_proxy_not_full_market_breadth")
    if not (market_state_freshness or {}).get("is_aligned") and "daily_features_lagged" not in limitations:
        limitations.append("daily_features_lagged")
    data_quality["limitations"] = limitations
    behavior["data_quality"] = data_quality

    locked_values = dict(behavior.get("locked_values") or {})
    locked_values.update(
        {
            "intraday_symbol_count": symbol_count,
            "intraday_row_count": row_count,
            "intraday_positive_ratio": proxy.get("positive_ratio"),
            "intraday_average_change_pct": proxy.get("average_change_pct"),
            "intraday_trade_date": trade_date,
        }
    )
    behavior["locked_values"] = locked_values

    old_anchors = [str(item) for item in behavior.get("narrative_anchors") or [] if str(item).strip()]
    lagged_prefix = ""
    feature_trade_date = str((market_state_freshness or {}).get("feature_trade_date") or "")[:10]
    if feature_trade_date and feature_trade_date != str((market_state_freshness or {}).get("event_reaction_trade_date") or "")[:10]:
        lagged_prefix = f"滞后日线参考（{feature_trade_date}）："
    lagged_anchors = [
        f"{lagged_prefix}{item}" if lagged_prefix else item
        for item in old_anchors[:2]
    ]
    freshness_message = str((market_state_freshness or {}).get("message") or "").strip()
    anchors = [detail]
    if freshness_message:
        anchors.append(f"数据新鲜度：{freshness_message}")
    anchors.extend(lagged_anchors)
    behavior["narrative_anchors"] = _dedupe_strings(anchors + old_anchors[2:])[:8]
    return behavior


def _load_minute_market_proxy(db: Session, trade_date: str) -> dict[str, Any]:
    return _load_minute_market_proxy_for_date(db, trade_date, requested_trade_date=trade_date, allow_fallback=True)


def _load_minute_market_proxy_for_date(
    db: Session,
    trade_date: str,
    *,
    requested_trade_date: str | None = None,
    allow_fallback: bool = False,
) -> dict[str, Any]:
    minute_table = preferred_minute_kline_table()
    requested = str(requested_trade_date or trade_date or "")[:10]
    try:
        start = datetime.fromisoformat(f"{str(trade_date)[:10]} 00:00:00")
        end = start + timedelta(days=1)
    except Exception:
        return {
            "scope": "minute_market_proxy",
            "coverage_scope": "partial_minute_sample",
            "status": "unavailable",
            "trade_date": str(trade_date or "")[:10],
            "requested_trade_date": requested,
            "fallback": False,
            "row_count": 0,
            "symbol_count": 0,
            "is_full_market_breadth": False,
            "message": f"事件反应日 {trade_date} 无法解析，分钟市场代理不可用。",
        }

    try:
        rows = db.execute(
            text(
                f"""
                WITH symbol_bars AS (
                    SELECT
                        symbol,
                        (ARRAY_AGG(open ORDER BY trade_time ASC))[1] AS first_open,
                        (ARRAY_AGG(close ORDER BY trade_time DESC))[1] AS last_close,
                        MAX(high) AS high_price,
                        MIN(low) AS low_price,
                        SUM(COALESCE(amount, 0)) AS total_amount,
                        COUNT(*) AS bar_count,
                        MIN(trade_time) AS first_trade_time,
                        MAX(trade_time) AS last_trade_time
                    FROM {minute_table}
                    WHERE trade_time >= :start_time
                      AND trade_time < :end_time
                    GROUP BY symbol
                )
                SELECT *
                FROM symbol_bars
                WHERE first_open IS NOT NULL
                  AND last_close IS NOT NULL
                """
            ),
            {"start_time": start, "end_time": end},
        ).mappings().all()
    except Exception as exc:
        logger.warning("[catalyst-selection] minute market proxy query failed trade_date=%s error=%s", trade_date, exc)
        return {
            "scope": "minute_market_proxy",
            "coverage_scope": "partial_minute_sample",
            "status": "unavailable",
            "trade_date": str(trade_date or "")[:10],
            "requested_trade_date": requested,
            "fallback": False,
            "row_count": 0,
            "symbol_count": 0,
            "is_full_market_breadth": False,
            "source": f"postgresql:{minute_table}",
            "message": f"事件反应日 {trade_date} 分钟市场代理查询失败，不能替代全市场宽度。",
        }

    symbol_count = len(rows)
    if not rows:
        fallback_date = _latest_available_minute_trade_date(db, before_or_equal_trade_date=str(trade_date)[:10]) if allow_fallback else None
        if fallback_date and fallback_date != str(trade_date)[:10]:
            fallback = _load_minute_market_proxy_for_date(
                db,
                fallback_date,
                requested_trade_date=requested,
                allow_fallback=False,
            )
            if fallback.get("status") != "unavailable":
                fallback["fallback"] = True
                fallback["requested_trade_date"] = requested
                fallback["message"] = (
                    f"事件反应日 {requested} 暂无分钟线，使用上一可用分钟交易日 {fallback_date} 的代理："
                    f"{fallback.get('message')}"
                )
                return fallback
        return {
            "scope": "minute_market_proxy",
            "coverage_scope": "partial_minute_sample",
            "status": "unavailable",
            "trade_date": str(trade_date or "")[:10],
            "requested_trade_date": requested,
            "fallback": False,
            "row_count": 0,
            "symbol_count": 0,
            "is_full_market_breadth": False,
            "source": f"postgresql:{minute_table}",
            "message": f"事件反应日 {trade_date} 暂无分钟市场代理样本，仍以日线市场状态为准。",
        }

    changes: list[float] = []
    total_amount = 0.0
    row_count = 0
    first_time: datetime | None = None
    last_time: datetime | None = None
    leaders: list[dict[str, Any]] = []
    for row in rows:
        first_open = _num(row.get("first_open"))
        last_close = _num(row.get("last_close"))
        change_pct = _pct(last_close, first_open)
        if change_pct is not None:
            changes.append(change_pct)
        amount = _num(row.get("total_amount")) or 0.0
        total_amount += amount
        row_count += int(row.get("bar_count") or 0)
        row_first = row.get("first_trade_time")
        row_last = row.get("last_trade_time")
        if isinstance(row_first, datetime) and (first_time is None or row_first < first_time):
            first_time = row_first
        if isinstance(row_last, datetime) and (last_time is None or row_last > last_time):
            last_time = row_last
        leaders.append(
            {
                "symbol": row.get("symbol"),
                "change_pct": _round_or_none(change_pct, 4),
                "amount": _round_or_none(amount, 2),
            }
        )

    up_count = sum(1 for value in changes if value > 0)
    down_count = sum(1 for value in changes if value < 0)
    flat_count = max(len(changes) - up_count - down_count, 0)
    positive_ratio = up_count / len(changes) if changes else 0.0
    average_change = sum(changes) / len(changes) if changes else None
    leaders.sort(key=lambda item: float(item.get("change_pct") if item.get("change_pct") is not None else -999.0), reverse=True)
    laggards = sorted(leaders, key=lambda item: float(item.get("change_pct") if item.get("change_pct") is not None else 999.0))

    if symbol_count < 20:
        status = "thin_sample"
    elif positive_ratio <= 0.35 or (average_change is not None and average_change <= -0.4):
        status = "risk_off"
    elif positive_ratio < 0.45 or (average_change is not None and average_change < -0.1):
        status = "weak"
    elif positive_ratio >= 0.58 and (average_change is not None and average_change >= 0.1):
        status = "constructive"
    else:
        status = "mixed"

    status_label = {
        "constructive": "部分分钟样本偏强",
        "mixed": "部分分钟样本分化",
        "weak": "部分分钟样本偏弱",
        "risk_off": "部分分钟样本转弱",
        "thin_sample": "分钟样本过少",
    }.get(status, "分钟样本待确认")
    message = (
        f"{status_label}：{symbol_count} 只/{row_count} 行，"
        f"上涨 {up_count} / 下跌 {down_count} / 平 {flat_count}，"
        f"上涨占比 {positive_ratio:.0%}，均值 {average_change if average_change is not None else 0.0:.2f}%。"
        "该口径只是盘中部分分钟样本代理，不等同全市场宽度。"
    )

    return {
        "scope": "minute_market_proxy",
        "coverage_scope": "partial_minute_sample",
        "status": status,
        "trade_date": str(trade_date or "")[:10],
        "requested_trade_date": requested,
        "fallback": requested != str(trade_date or "")[:10],
        "row_count": row_count,
        "symbol_count": symbol_count,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "positive_ratio": round(float(positive_ratio), 4),
        "average_change_pct": _round_or_none(average_change, 4),
        "total_amount": _round_or_none(total_amount, 2),
        "first_trade_time": _iso(first_time),
        "last_trade_time": _iso(last_time),
        "is_full_market_breadth": False,
        "source": f"postgresql:{minute_table}",
        "message": message,
        "leaders": leaders[:3],
        "laggards": laggards[:3],
    }


def _latest_available_minute_trade_date(db: Session, *, before_or_equal_trade_date: str) -> str | None:
    minute_table = preferred_minute_kline_table()
    try:
        cutoff = datetime.fromisoformat(f"{str(before_or_equal_trade_date)[:10]} 23:59:59")
        latest = db.execute(
            text(
                f"""
                SELECT MAX(trade_time) AS latest_trade_time
                FROM {minute_table}
                WHERE trade_time <= :cutoff
                """
            ),
            {"cutoff": cutoff},
        ).scalar()
    except Exception as exc:
        logger.warning("[catalyst-selection] latest minute trade date query failed before=%s error=%s", before_or_equal_trade_date, exc)
        return None
    if isinstance(latest, datetime):
        return latest.date().isoformat()
    text_value = str(latest or "").strip()
    return text_value[:10] if text_value else None


def _build_intraday_event_pulse(features_by_symbol: dict[str, dict[str, Any]]) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    for symbol, features in features_by_symbol.items():
        reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
        status = str(reaction.get("status") or "missing")
        status_counts[status] = status_counts.get(status, 0) + 1
        if status == "missing" or reaction.get("proxy"):
            continue
        samples.append(
            {
                "symbol": symbol,
                "name": features.get("name") or symbol,
                "status": status,
                "score": _num(reaction.get("score")),
                "change_pct": _num(reaction.get("change_pct")),
                "high_pct": _num(reaction.get("high_pct")),
                "low_pct": _num(reaction.get("low_pct")),
                "amount_share": _num(reaction.get("amount_share")),
            }
        )

    sample_count = len(samples)
    if sample_count == 0:
        return {
            "scope": "event_candidate_universe",
            "status": "unavailable",
            "message": "事件候选池暂无真实分钟反应样本。",
            "sample_count": 0,
            "symbol_count": len(features_by_symbol),
            "status_counts": status_counts,
        }

    changes = [float(item["change_pct"]) for item in samples if item.get("change_pct") is not None]
    scores = [float(item["score"]) for item in samples if item.get("score") is not None]
    amount_shares = [float(item["amount_share"]) for item in samples if item.get("amount_share") is not None]
    positive_count = sum(1 for value in changes if value > 0)
    negative_count = sum(1 for value in changes if value < 0)
    confirmed_count = sum(1 for item in samples if item.get("status") == "confirmed")
    divergent_count = sum(1 for item in samples if item.get("status") == "divergent")
    weak_count = sum(1 for item in samples if item.get("status") == "weak")
    positive_ratio = positive_count / len(changes) if changes else 0.0
    avg_change = sum(changes) / len(changes) if changes else None
    avg_score = sum(scores) / len(scores) if scores else None
    total_amount_share = sum(amount_shares) if amount_shares else None

    if divergent_count > 0 and (positive_ratio < 0.4 or (avg_score is not None and avg_score < 48)):
        pulse_status = "risk_off"
    elif confirmed_count >= max(1, int(sample_count * 0.35)) and positive_ratio >= 0.55 and (avg_score or 0.0) >= 56:
        pulse_status = "confirming"
    elif (avg_score is not None and avg_score < 50) or positive_ratio < 0.4:
        pulse_status = "weak"
    else:
        pulse_status = "mixed"

    status_label = {
        "confirming": "事件池分钟反应扩散确认",
        "mixed": "事件池分钟反应分化",
        "weak": "事件池分钟反应偏弱",
        "risk_off": "事件池分钟反应背离",
    }.get(pulse_status, "事件池分钟反应待确认")
    message = (
        f"{status_label}：真实分钟样本 {sample_count}/{len(features_by_symbol)}，"
        f"上涨占比 {positive_ratio:.0%}，均值 {avg_change if avg_change is not None else 0.0:.2f}%，"
        f"确认 {confirmed_count} / 背离 {divergent_count} / 弱 {weak_count}。"
    )

    ranked = sorted(
        samples,
        key=lambda item: (
            float(item.get("change_pct") if item.get("change_pct") is not None else -999.0),
            float(item.get("score") if item.get("score") is not None else -999.0),
        ),
        reverse=True,
    )
    laggards = sorted(
        samples,
        key=lambda item: (
            float(item.get("change_pct") if item.get("change_pct") is not None else 999.0),
            float(item.get("score") if item.get("score") is not None else 999.0),
        ),
    )

    def _compact_sample(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "status": item.get("status"),
            "score": _round_or_none(item.get("score"), 2),
            "change_pct": _round_or_none(item.get("change_pct"), 4),
        }

    return {
        "scope": "event_candidate_universe",
        "status": pulse_status,
        "message": message,
        "sample_count": sample_count,
        "symbol_count": len(features_by_symbol),
        "status_counts": status_counts,
        "confirmed_count": confirmed_count,
        "divergent_count": divergent_count,
        "weak_count": weak_count,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "positive_ratio": round(float(positive_ratio), 4),
        "average_change_pct": _round_or_none(avg_change, 4),
        "average_score": _round_or_none(avg_score, 2),
        "total_amount_share": _round_or_none(total_amount_share, 4),
        "leaders": [_compact_sample(item) for item in ranked[:3]],
        "laggards": [_compact_sample(item) for item in laggards[:3]],
    }


def _event_minute_capture_allowed(trade_date: str) -> bool:
    enabled = str(os.getenv("AI_QUANT_EVENT_MINUTE_CAPTURE", "1") or "1").strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return False
    if trade_date == _effective_cn_trade_date():
        if _current_cn_trading_session():
            return True
        after_hours = str(os.getenv("AI_QUANT_EVENT_MINUTE_CAPTURE_AFTER_HOURS", "auto") or "auto").strip().lower()
        if after_hours in {"1", "true", "yes", "on"}:
            return True
        if after_hours in {"0", "false", "no", "off"}:
            return False
        return _current_cn_after_close_session()
    historical = str(os.getenv("AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL", "0") or "0").strip().lower()
    return historical in {"1", "true", "yes", "on"}


def _event_minute_capture_skip_message(trade_date: str) -> str:
    if trade_date == _effective_cn_trade_date() and not _current_cn_trading_session():
        current = now_cn().time()
        if current < time(hour=9, minute=30):
            return "盘前尚无当日分钟线，跳过QMT主动补采；盘中或盘后会自动尝试补采"
        return "非盘中时段，跳过QMT主动分钟线补采；如需盘后主动补采请设置 AI_QUANT_EVENT_MINUTE_CAPTURE_AFTER_HOURS=auto 或 1"
    return "非当前交易日，跳过QMT实时分钟线补采；如需历史补采请开启 AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL"


def _current_cn_trading_session() -> bool:
    current = now_cn().time()
    return time(hour=9, minute=30) <= current <= time(hour=11, minute=30) or time(hour=13, minute=0) <= current <= time(hour=15, minute=5)


def _current_cn_before_open_session() -> bool:
    return now_cn().time() < time(hour=9, minute=30)


def _current_cn_after_close_session() -> bool:
    return now_cn().time() >= time(hour=15, minute=5)


def _event_minute_capture_timeout_seconds() -> float:
    raw = os.getenv("AI_QUANT_EVENT_MINUTE_CAPTURE_TIMEOUT_SECONDS", str(DEFAULT_EVENT_MINUTE_CAPTURE_TIMEOUT_SECONDS))
    try:
        value = float(raw or DEFAULT_EVENT_MINUTE_CAPTURE_TIMEOUT_SECONDS)
    except Exception:
        value = DEFAULT_EVENT_MINUTE_CAPTURE_TIMEOUT_SECONDS
    return max(1.0, min(value, 12.0))


def _event_minute_history_backfill_enabled() -> bool:
    raw = str(os.getenv("AI_QUANT_EVENT_MINUTE_HISTORY_BACKFILL", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _event_minute_history_backfill_timeout_seconds() -> float:
    raw = os.getenv(
        "AI_QUANT_EVENT_MINUTE_HISTORY_BACKFILL_TIMEOUT_SECONDS",
        str(DEFAULT_EVENT_MINUTE_HISTORY_BACKFILL_TIMEOUT_SECONDS),
    )
    try:
        value = float(raw or DEFAULT_EVENT_MINUTE_HISTORY_BACKFILL_TIMEOUT_SECONDS)
    except Exception:
        value = DEFAULT_EVENT_MINUTE_HISTORY_BACKFILL_TIMEOUT_SECONDS
    return max(0.5, min(value, 8.0))


def _event_minute_history_bridge_cooldown_seconds() -> float:
    raw = os.getenv(
        "AI_QUANT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS",
        str(DEFAULT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS),
    )
    try:
        value = float(raw or DEFAULT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS)
    except Exception:
        value = DEFAULT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS
    return max(5.0, min(value, 1800.0))


def _event_minute_akshare_backfill_enabled() -> bool:
    raw = str(os.getenv("AI_QUANT_EVENT_MINUTE_AKSHARE_BACKFILL", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _event_minute_akshare_backfill_symbol_limit() -> int:
    raw = os.getenv("AI_QUANT_EVENT_MINUTE_AKSHARE_BACKFILL_SYMBOLS", str(DEFAULT_EVENT_MINUTE_AKSHARE_BACKFILL_SYMBOLS))
    try:
        value = int(raw or DEFAULT_EVENT_MINUTE_AKSHARE_BACKFILL_SYMBOLS)
    except Exception:
        value = DEFAULT_EVENT_MINUTE_AKSHARE_BACKFILL_SYMBOLS
    return max(1, min(value, 20))


def _event_minute_akshare_sync_enabled(trade_date: str) -> bool:
    raw = str(os.getenv("AI_QUANT_EVENT_MINUTE_AKSHARE_SYNC", "auto") or "auto").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return trade_date == _effective_cn_trade_date() and (_current_cn_trading_session() or _current_cn_after_close_session())


def _event_minute_akshare_sync_symbol_limit() -> int:
    raw = os.getenv("AI_QUANT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS", str(DEFAULT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS))
    try:
        value = int(raw or DEFAULT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS)
    except Exception:
        value = DEFAULT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS
    return max(1, min(value, 10))


def _event_minute_akshare_backfill_cooldown_seconds() -> float:
    raw = os.getenv("AI_QUANT_EVENT_MINUTE_AKSHARE_BACKFILL_COOLDOWN_SECONDS", "600")
    try:
        value = float(raw or 600)
    except Exception:
        value = 600.0
    return max(30.0, min(value, 3600.0))


def _event_reaction_trade_date(market_trade_date: str, window: str) -> str:
    if _normalize_window(window) == "premarket":
        return market_trade_date
    current_trade_date = _effective_cn_trade_date()
    if date.fromisoformat(current_trade_date) >= date.fromisoformat(market_trade_date):
        return current_trade_date
    return market_trade_date


def _realtime_feedback_lookup_trade_date(
    *,
    trade_date: str | None,
    event_reaction_trade_date: str | None,
    window: str,
) -> str | None:
    if _normalize_window(window) != "premarket" and event_reaction_trade_date:
        return str(event_reaction_trade_date)[:10]
    return str(trade_date)[:10] if trade_date else None


def _missing_event_reaction(*, trade_date: str, source: str) -> dict[str, Any]:
    return {
        "status": "missing",
        "score": 50.0,
        "confidence": 0.0,
        "reason": "缺少事件后分钟K线确认",
        "trade_date": trade_date,
        "source": source,
    }


def _load_event_minute_reactions(
    db: Session,
    *,
    features_by_symbol: dict[str, dict[str, Any]],
    theme_items: list[dict[str, Any]],
    trade_date: str,
    minute_table: str,
) -> dict[str, dict[str, Any]]:
    symbols = sorted(features_by_symbol)
    if not symbols:
        return {}
    trade_day = pd.to_datetime(trade_date).date()
    start_time = datetime.combine(trade_day, time(hour=9, minute=25))
    end_time = datetime.combine(trade_day, time(hour=15, minute=1))
    symbol_variants = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
    statement = text(
        f"""
        SELECT symbol, trade_time, open, high, low, close, volume, amount
        FROM {minute_table}
        WHERE symbol IN :symbols
          AND trade_time >= :start_time
          AND trade_time <= :end_time
        ORDER BY symbol, trade_time
        """
    ).bindparams(bindparam("symbols", expanding=True))
    try:
        frame = pd.read_sql_query(
            statement,
            db.bind,
            params={"symbols": symbol_variants, "start_time": start_time, "end_time": end_time},
        )
    except Exception as exc:
        logger.warning("[catalyst-selection] load minute reaction failed table=%s err=%s", minute_table, exc)
        return {}
    if frame.empty:
        return {}

    frame["symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["trade_time"] = pd.to_datetime(frame["trade_time"])
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    reactions: dict[str, dict[str, Any]] = {}
    grouped = frame.groupby("symbol", group_keys=False)
    for symbol, features in features_by_symbol.items():
        symbol_rows = grouped.get_group(symbol).copy() if symbol in grouped.groups else pd.DataFrame()
        if symbol_rows.empty:
            continue
        matches = _theme_matches_for_symbol(symbol, features, theme_items)
        if not matches:
            continue
        primary_theme = matches[0]
        reaction_start = _event_reaction_start(primary_theme, trade_date)
        reaction_end = _event_reaction_end(reaction_start)
        before_rows = symbol_rows[symbol_rows["trade_time"] < reaction_start]
        window_rows = symbol_rows[
            (symbol_rows["trade_time"] >= reaction_start)
            & (symbol_rows["trade_time"] <= reaction_end)
        ].copy()
        if window_rows.empty:
            continue
        baseline_price = _minute_reaction_baseline(
            before_rows=before_rows,
            window_rows=window_rows,
            features=features,
        )
        if baseline_price is None or baseline_price <= 0:
            continue
        reactions[symbol] = _score_event_minute_reaction(
            symbol=symbol,
            features=features,
            primary_theme=primary_theme,
            window_rows=window_rows,
            baseline_price=baseline_price,
            reaction_start=reaction_start,
            reaction_end=reaction_end,
            minute_table=minute_table,
        )
    return reactions


def _event_reaction_start(theme: dict[str, Any], trade_date: str) -> datetime:
    trade_day = pd.to_datetime(trade_date).date()
    published_at = _latest_evidence_published_at(theme)
    open_time = datetime.combine(trade_day, time(hour=9, minute=30))
    midday_open = datetime.combine(trade_day, time(hour=13, minute=0))
    midday_close = datetime.combine(trade_day, time(hour=11, minute=30))
    close_time = datetime.combine(trade_day, time(hour=15, minute=0))
    if published_at is None or published_at.date() < trade_day:
        return open_time
    if published_at.date() > trade_day:
        return close_time
    if published_at < open_time:
        return open_time
    if midday_close <= published_at < midday_open:
        return midday_open
    if published_at >= close_time:
        return close_time
    return published_at.replace(second=0, microsecond=0)


def _event_reaction_end(reaction_start: datetime) -> datetime:
    trade_day = reaction_start.date()
    midday_close = datetime.combine(trade_day, time(hour=11, minute=30))
    midday_open = datetime.combine(trade_day, time(hour=13, minute=0))
    close_time = datetime.combine(trade_day, time(hour=15, minute=0))
    reaction_end = reaction_start + timedelta(minutes=30)
    if reaction_start < midday_close and reaction_end > midday_close:
        reaction_end += midday_open - midday_close
    return min(reaction_end, close_time)


def _latest_evidence_published_at(theme: dict[str, Any]) -> datetime | None:
    latest: datetime | None = None
    for item in theme.get("evidence_items") or []:
        if not isinstance(item, dict):
            continue
        published_at = _parse_datetime_or_none(item.get("published_at"))
        if published_at is None:
            continue
        if latest is None or published_at > latest:
            latest = published_at
    return latest


def _minute_reaction_baseline(
    *,
    before_rows: pd.DataFrame,
    window_rows: pd.DataFrame,
    features: dict[str, Any],
) -> float | None:
    if not before_rows.empty:
        return _num(before_rows.iloc[-1].get("close"))
    first_open = _num(window_rows.iloc[0].get("open"))
    if first_open is not None and first_open > 0:
        return first_open
    for key in ("open", "pre_close", "close"):
        value = _num(features.get(key))
        if value is not None and value > 0:
            return value
    return None


def _score_event_minute_reaction(
    *,
    symbol: str,
    features: dict[str, Any],
    primary_theme: dict[str, Any],
    window_rows: pd.DataFrame,
    baseline_price: float,
    reaction_start: datetime,
    reaction_end: datetime,
    minute_table: str,
) -> dict[str, Any]:
    close_after = _num(window_rows.iloc[-1].get("close"))
    high_after = _num(window_rows["high"].max())
    low_after = _num(window_rows["low"].min())
    amount_after = _num(window_rows["amount"].sum()) or 0.0
    volume_after = _num(window_rows["volume"].sum()) or 0.0
    daily_amount = _num(features.get("amount"))
    change_pct = _pct(close_after, baseline_price) if close_after is not None else None
    high_pct = _pct(high_after, baseline_price) if high_after is not None else None
    low_pct = _pct(low_after, baseline_price) if low_after is not None else None
    amount_share = amount_after / daily_amount if daily_amount and daily_amount > 0 else None
    bar_count = int(len(window_rows))
    score = 50.0
    reasons: list[str] = []

    if change_pct is not None:
        if change_pct >= 3.0:
            score += 18.0
            reasons.append(f"事件后30分钟涨幅{change_pct:.2f}%")
        elif change_pct >= 1.5:
            score += 12.0
            reasons.append(f"事件后30分钟涨幅{change_pct:.2f}%")
        elif change_pct >= 0.5:
            score += 6.0
            reasons.append(f"事件后30分钟温和上行{change_pct:.2f}%")
        elif change_pct <= -2.0:
            score -= 18.0
            reasons.append(f"事件后30分钟下跌{change_pct:.2f}%")
        elif change_pct <= -0.8:
            score -= 8.0
            reasons.append(f"事件后30分钟偏弱{change_pct:.2f}%")

    if high_pct is not None:
        if high_pct >= 5.0:
            score += 10.0
            reasons.append(f"盘中最高响应{high_pct:.2f}%")
        elif high_pct >= 3.0:
            score += 6.0
            reasons.append(f"盘中最高响应{high_pct:.2f}%")

    if amount_share is not None:
        if amount_share >= 0.2:
            score += 12.0
            reasons.append(f"事件窗口成交占全天{amount_share:.1%}")
        elif amount_share >= 0.1:
            score += 8.0
            reasons.append(f"事件窗口成交占全天{amount_share:.1%}")
        elif amount_share >= 0.05:
            score += 4.0
            reasons.append(f"事件窗口成交占全天{amount_share:.1%}")

    if low_pct is not None and low_pct <= -4.0:
        score -= 8.0
        reasons.append(f"事件窗口最大回撤{low_pct:.2f}%")
    if bar_count < 5:
        score -= 4.0
        reasons.append("事件窗口分钟K线不足5根")

    score = max(0.0, min(score, 100.0))
    status = "weak"
    if score >= 62.0 and ((change_pct is not None and change_pct >= 0.5) or (amount_share is not None and amount_share >= 0.1)):
        status = "confirmed"
    elif score <= 42.0 or (change_pct is not None and change_pct <= -0.8):
        status = "divergent"
    confidence = min(1.0, max(0.0, bar_count / 30.0) * 0.65 + (0.25 if amount_share is not None else 0.0) + 0.10)
    published_at = _latest_evidence_published_at(primary_theme)
    if not reasons:
        reasons.append("事件后30分钟反应中性")

    return {
        "status": status,
        "score": round(score, 2),
        "confidence": round(confidence, 4),
        "symbol": symbol,
        "name": features.get("name") or symbol,
        "theme": primary_theme.get("theme"),
        "published_at": _iso(published_at) if published_at else None,
        "reaction_start": reaction_start.isoformat(),
        "reaction_end": reaction_end.isoformat(),
        "baseline_price": _round_or_none(baseline_price, 4),
        "close_after": _round_or_none(close_after, 4),
        "high_after": _round_or_none(high_after, 4),
        "low_after": _round_or_none(low_after, 4),
        "change_pct": _round_or_none(change_pct, 4),
        "high_pct": _round_or_none(high_pct, 4),
        "low_pct": _round_or_none(low_pct, 4),
        "amount": _round_or_none(amount_after, 4),
        "volume": _round_or_none(volume_after, 4),
        "amount_share": _round_or_none(amount_share, 4),
        "bar_count": bar_count,
        "reasons": _dedupe_strings(reasons)[:6],
        "source": f"postgresql:{minute_table}",
    }


def _daily_proxy_event_reaction(
    *,
    symbol: str,
    features: dict[str, Any],
    primary_theme: dict[str, Any],
    trade_date: str,
) -> dict[str, Any] | None:
    if not primary_theme:
        return None
    open_price = _num(features.get("open"))
    close_price = _num(features.get("close"))
    high_price = _num(features.get("high"))
    low_price = _num(features.get("low"))
    if open_price is None or open_price <= 0 or close_price is None:
        return None
    change_pct = _pct(close_price, open_price)
    high_pct = _pct(high_price, open_price) if high_price is not None else None
    low_pct = _pct(low_price, open_price) if low_price is not None else None
    amount_ratio = _num(features.get("amount_ratio_20d"))
    score = 50.0
    reasons = ["分钟K线缺失，用当日开收盘做代理反应"]
    if change_pct is not None:
        if change_pct >= 3.0:
            score += 12.0
            reasons.append(f"当日开收涨幅{change_pct:.2f}%")
        elif change_pct >= 1.0:
            score += 7.0
            reasons.append(f"当日开收上行{change_pct:.2f}%")
        elif change_pct <= -2.0:
            score -= 12.0
            reasons.append(f"当日开收下跌{change_pct:.2f}%")
        elif change_pct <= -0.8:
            score -= 7.0
            reasons.append(f"当日开收偏弱{change_pct:.2f}%")
    if high_pct is not None and high_pct >= 4.0:
        score += 5.0
        reasons.append(f"盘中最高响应{high_pct:.2f}%")
    if low_pct is not None and low_pct <= -4.0:
        score -= 5.0
        reasons.append(f"盘中低点回撤{low_pct:.2f}%")
    if amount_ratio is not None:
        if amount_ratio >= 2.0:
            score += 7.0
            reasons.append(f"全天量能为20日均量{amount_ratio:.2f}倍")
        elif amount_ratio >= 1.3:
            score += 4.0
            reasons.append(f"全天量能为20日均量{amount_ratio:.2f}倍")
    score = max(0.0, min(score, 100.0))
    status = "daily_proxy_neutral"
    if score >= 60.0 and change_pct is not None and change_pct >= 0.8:
        status = "daily_proxy_confirmed"
    elif score <= 44.0 or (change_pct is not None and change_pct <= -0.8):
        status = "daily_proxy_divergent"
    published_at = _latest_evidence_published_at(primary_theme)
    return {
        "status": status,
        "score": round(score, 2),
        "confidence": 0.35,
        "proxy": True,
        "symbol": symbol,
        "name": features.get("name") or symbol,
        "theme": primary_theme.get("theme"),
        "published_at": _iso(published_at) if published_at else None,
        "trade_date": trade_date,
        "baseline_price": _round_or_none(open_price, 4),
        "close_after": _round_or_none(close_price, 4),
        "high_after": _round_or_none(high_price, 4),
        "low_after": _round_or_none(low_price, 4),
        "change_pct": _round_or_none(change_pct, 4),
        "high_pct": _round_or_none(high_pct, 4),
        "low_pct": _round_or_none(low_pct, 4),
        "amount_ratio_20d": _round_or_none(amount_ratio, 4),
        "reasons": _dedupe_strings(reasons)[:6],
        "source": f"postgresql:{preferred_daily_kline_table()}:open_close_proxy",
    }


def _load_market_snapshot(db: Session, trade_date: str) -> dict[str, Any]:
    table = preferred_daily_kline_table()
    rows = db.execute(
        text(
            f"""
            SELECT symbol, close, pre_close, amount, sw_industry_l1
            FROM {table}
            WHERE trade_date = :trade_date
            """
        ),
        {"trade_date": trade_date},
    ).mappings().all()
    if not rows:
        return {"market_stats": {}, "indices": [], "sector_gainers": [], "sector_losers": [], "sector_inflows": [], "sector_outflows": []}
    up_count = 0
    down_count = 0
    limit_up_count = 0
    limit_down_count = 0
    total_amount = 0.0
    sectors: dict[str, list[float]] = defaultdict(list)
    sector_amounts: dict[str, float] = defaultdict(float)
    for row in rows:
        close = _num(row.get("close"))
        pre_close = _num(row.get("pre_close"))
        amount = _num(row.get("amount")) or 0.0
        total_amount += amount
        pct = _pct(close, pre_close)
        if pct is not None:
            if pct > 0:
                up_count += 1
            elif pct < 0:
                down_count += 1
            if pct >= 9.5:
                limit_up_count += 1
            if pct <= -9.5:
                limit_down_count += 1
        sector = str(row.get("sw_industry_l1") or "").strip()
        if sector and pct is not None:
            sectors[sector].append(pct)
            sector_amounts[sector] += amount
    sector_items = [
        {
            "sector_name": sector,
            "change_pct": round(sum(values) / len(values), 4),
            "amount": round(sector_amounts.get(sector, 0.0), 2),
        }
        for sector, values in sectors.items()
        if values
    ]
    sector_items.sort(key=lambda item: float(item["change_pct"]), reverse=True)
    return {
        "indices": [],
        "sector_gainers": sector_items[:8],
        "sector_losers": list(reversed(sector_items[-8:])),
        "sector_inflows": sorted(sector_items, key=lambda item: float(item.get("amount") or 0), reverse=True)[:8],
        "sector_outflows": [],
        "market_stats": {
            "total_amount": round(total_amount, 2),
            "up_count": up_count,
            "down_count": down_count,
            "limit_up_count": limit_up_count,
            "limit_down_count": limit_down_count,
            "source": f"postgresql:{table}",
        },
    }


def _build_market_background(
    *,
    trade_date: str,
    window: str,
    news_window_start: datetime | None = None,
    news_window_end: datetime | None = None,
    theme_items: list[dict[str, Any]],
    market_behavior: dict[str, Any],
    market_state_freshness: dict[str, Any] | None = None,
    selected_items: list[dict[str, Any]] | None = None,
) -> str:
    anchors = [str(item) for item in market_behavior.get("narrative_anchors") or [] if str(item).strip()]
    selected_theme = _selected_mainline_theme_summary(selected_items or [], theme_items=theme_items)
    top_theme = selected_theme or (theme_items[0] if theme_items else {})
    theme_text = ""
    if top_theme:
        catalyst_text = top_theme.get("catalyst") or top_theme.get("summary") or "等待更多证据"
        selected_count = int(top_theme.get("selected_count") or 0)
        symbol_text = ""
        selected_symbols = [str(item) for item in top_theme.get("selected_symbols") or [] if str(item).strip()]
        if selected_count:
            symbol_text = f"（入选{selected_count}只"
            if selected_symbols:
                symbol_text += "：" + "、".join(selected_symbols[:4])
            symbol_text += "）"
        label = "入选主线" if selected_count else "核心催化"
        theme_text = f"{label}：{top_theme.get('theme')}{symbol_text}，{catalyst_text}。"
    labels = " | ".join(anchors[:3])
    freshness_text = ""
    if isinstance(market_state_freshness, dict) and not market_state_freshness.get("is_aligned"):
        freshness_text = f" | 市场状态新鲜度：{market_state_freshness.get('message')}"
    pulse = market_state_freshness.get("intraday_event_pulse") if isinstance(market_state_freshness, dict) else None
    pulse_text = ""
    if isinstance(pulse, dict) and pulse.get("status") not in {None, "", "unavailable"}:
        pulse_text = f" | 事件池分钟脉冲：{pulse.get('message')}"
    minute_proxy = market_state_freshness.get("minute_market_proxy") if isinstance(market_state_freshness, dict) else None
    minute_proxy_text = ""
    if isinstance(minute_proxy, dict) and minute_proxy.get("status") not in {None, "", "unavailable"}:
        minute_proxy_text = f" | 分钟市场代理：{minute_proxy.get('message')}"
    if _normalize_window(window) == "premarket":
        window_label = f"盘前资讯：{trade_date} 09:25前"
    else:
        window_label = f"资讯窗口：{_compact_datetime(news_window_start)}至{_compact_datetime(news_window_end)}；市场基准：{trade_date}"
    return f"{window_label}{freshness_text}{pulse_text}{minute_proxy_text} | {labels or '市场态势数据待补全'} | {theme_text}".strip()


def _selected_mainline_theme_summary(
    selected_items: list[dict[str, Any]],
    *,
    theme_items: list[dict[str, Any]],
) -> dict[str, Any]:
    if not selected_items:
        return {}
    theme_catalog = {
        str(item.get("theme") or "").strip(): item
        for item in theme_items
        if isinstance(item, dict) and str(item.get("theme") or "").strip()
    }
    stats: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(selected_items):
        if not isinstance(item, dict):
            continue
        matches = item.get("theme_matches") if isinstance(item.get("theme_matches"), list) else []
        primary = matches[0] if matches and isinstance(matches[0], dict) else {}
        theme = str(primary.get("theme") or "").strip()
        if not theme:
            continue
        rank = int(item.get("rank") or index + 1)
        score = _num(item.get("score")) or 0.0
        weight = max(score, 1.0) * max(1, len(selected_items) - index)
        entry = stats.setdefault(
            theme,
            {
                "theme": theme,
                "selected_count": 0,
                "selected_symbols": [],
                "weight": 0.0,
                "best_rank": rank,
                "candidate_theme": primary,
            },
        )
        entry["selected_count"] = int(entry.get("selected_count") or 0) + 1
        entry["weight"] = float(entry.get("weight") or 0.0) + weight
        entry["best_rank"] = min(int(entry.get("best_rank") or rank), rank)
        symbol_label = str(item.get("name") or item.get("symbol") or "").strip()
        if symbol_label and symbol_label not in entry["selected_symbols"]:
            entry["selected_symbols"].append(symbol_label)
    if not stats:
        return {}
    winner = sorted(
        stats.values(),
        key=lambda entry: (
            -float(entry.get("weight") or 0.0),
            int(entry.get("best_rank") or 9999),
            str(entry.get("theme") or ""),
        ),
    )[0]
    theme = str(winner.get("theme") or "").strip()
    source = theme_catalog.get(theme) if isinstance(theme_catalog.get(theme), dict) else {}
    primary = winner.get("candidate_theme") if isinstance(winner.get("candidate_theme"), dict) else {}
    return {
        **dict(source or {}),
        **dict(primary or {}),
        "theme": theme,
        "selected_count": int(winner.get("selected_count") or 0),
        "selected_symbols": list(winner.get("selected_symbols") or [])[:8],
        "selected_weight": round(float(winner.get("weight") or 0.0), 4),
        "selected_best_rank": int(winner.get("best_rank") or 0),
        "source": "selected_candidates",
    }


def _compact_datetime(value: datetime | None) -> str:
    if value is None:
        return "--"
    return value.strftime("%m-%d %H:%M")


def _catalyst_score(theme: dict[str, Any]) -> float:
    base = min(float(theme.get("score") or 0.0), 100.0)
    evidence_text = " ".join(str(row.get("content") or "") for row in (theme.get("evidence_items") or []) if isinstance(row, dict))
    catalyst = str(theme.get("catalyst") or "")
    text_value = f"{evidence_text} {catalyst}"
    boost = 0.0
    if theme.get("policy_boost") or theme.get("top_source_tier") == "S":
        boost += 18
    if any(token in text_value for token in HIGH_CERTAINTY_TOKENS):
        boost += 12
    if any(token in text_value for token in CONSUMABLE_TOKENS):
        boost -= 8
    semantic = theme.get("event_semantic") if isinstance(theme.get("event_semantic"), dict) else {}
    if semantic:
        strength = _num(semantic.get("catalyst_strength"))
        confidence = _num(semantic.get("confidence")) or 0.5
        if strength is not None:
            boost += (strength - 50.0) * min(max(confidence, 0.0), 1.0) * 0.28
    return max(0.0, min(100.0, base * 0.72 + boost))


def _market_confirm_score(features: dict[str, Any], theme: dict[str, Any]) -> float:
    score = 45.0
    change_pct = _num(features.get("change_pct")) or 0.0
    amount_ratio = _num(features.get("amount_ratio_20d")) or 1.0
    if change_pct >= 9.5:
        score += 28
    elif change_pct >= 5:
        score += 20
    elif change_pct >= 2:
        score += 12
    if amount_ratio >= 2:
        score += 16
    elif amount_ratio >= 1.3:
        score += 8
    confirmation = theme.get("market_confirmation") or {}
    score += min(float(confirmation.get("score") or 0.0), 16.0)
    event_reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
    reaction_score = _num(event_reaction.get("score"))
    reaction_status = str(event_reaction.get("status") or "")
    if reaction_score is not None:
        if reaction_status == "confirmed":
            score += min(max((reaction_score - 50.0) * 0.45, 0.0), 18.0)
        elif reaction_status == "daily_proxy_confirmed":
            score += min(max((reaction_score - 50.0) * 0.25, 0.0), 8.0)
        elif reaction_status == "divergent":
            score -= min(max((50.0 - reaction_score) * 0.5, 6.0), 18.0)
        elif reaction_status == "daily_proxy_divergent":
            score -= min(max((50.0 - reaction_score) * 0.3, 4.0), 10.0)
        elif reaction_status == "weak":
            score += max(min((reaction_score - 50.0) * 0.2, 5.0), -5.0)
    return max(0.0, min(score, 100.0))


def _momentum_score(features: dict[str, Any]) -> float:
    r60 = _num(features.get("r60"))
    momentum20 = _num(features.get("momentum_20d")) or 0.0
    base = r60 if r60 is not None else 50.0
    if momentum20 > 0.12:
        base += 10
    elif momentum20 < -0.08:
        base -= 10
    return max(0.0, min(base, 100.0))


def _fundamental_score(features: dict[str, Any]) -> float:
    growth = _num(features.get("net_profit_growth_proxy")) or 0.0
    if growth >= 1:
        return 90.0
    if growth >= 0.3:
        return 75.0
    if growth >= 0.05:
        return 60.0
    if growth < -0.2:
        return 30.0
    return 50.0


def _continuity_score(previous_state: dict[str, Any], history_stats: dict[str, Any]) -> float:
    streak = int(previous_state.get("streak") or 0)
    hit_rate = _num(history_stats.get("hit_rate"))
    count = int(history_stats.get("count") or 0)
    score = min(45.0, streak * 12.0)
    if count >= 3 and hit_rate is not None:
        score += min(40.0, hit_rate * 40.0)
    return max(0.0, min(score, 100.0))


def _risk_penalty(features: dict[str, Any], theme: dict[str, Any], history_stats: dict[str, Any]) -> tuple[float, list[str]]:
    penalty = 0.0
    flags: list[str] = []
    change_pct = _num(features.get("change_pct")) or 0.0
    amount_ratio = _num(features.get("amount_ratio_20d")) or 1.0
    risk_text = f"{theme.get('risk_note') or ''} {theme.get('catalyst') or ''}"
    if change_pct >= 9.5:
        penalty += 8
        flags.append("T-1涨停，注意高开兑现")
    elif change_pct >= 7:
        penalty += 6
        flags.append(f"T-1涨幅{change_pct:.2f}%，追高风险")
    if amount_ratio >= 3:
        penalty += 4
        flags.append("量能极端放大，次日分歧概率上升")
    if any(token in risk_text for token in RISK_TOKENS):
        penalty += 10
        flags.append("催化证据含风险或澄清线索")
    semantic = theme.get("event_semantic") if isinstance(theme.get("event_semantic"), dict) else {}
    if semantic:
        risk_signals = [str(item) for item in semantic.get("risk_signals") or [] if str(item).strip()]
        invalidations = [str(item) for item in semantic.get("invalidation_conditions") or [] if str(item).strip()]
        if risk_signals:
            penalty += min(len(risk_signals) * 2.5, 7.5)
            flags.append("事件语义风险：" + "；".join(risk_signals[:2]))
        if any(any(token in text for token in RISK_TOKENS) for text in invalidations):
            penalty += 4
            flags.append("事件失效条件偏硬")
    if (history_stats.get("loss_count") or 0) >= 2:
        penalty += 4
        flags.append("历史结算波动偏弱")
    event_reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
    if event_reaction.get("status") == "divergent":
        penalty += 8
        flags.append("事件后分钟级市场反应背离")
    elif event_reaction.get("status") == "daily_proxy_divergent":
        penalty += 4
        flags.append("事件后日内代理反应背离")
    return min(penalty, 30.0), flags


def _event_intelligence_score(theme: dict[str, Any], *, market_behavior: dict[str, Any]) -> tuple[float, list[str]]:
    score = 30.0
    reasons: list[str] = []
    source_tier = str(theme.get("top_source_tier") or theme.get("source_tier") or "B").strip().upper() or "B"
    tier_boost = {"S": 28.0, "A": 16.0, "B": 8.0, "C": 0.0}.get(source_tier, 6.0)
    score += tier_boost
    reasons.append(f"来源层级 {source_tier}")

    if theme.get("policy_boost"):
        score += 14.0
        reasons.append("政策/权威消息")

    evidence_items = [item for item in theme.get("evidence_items") or [] if isinstance(item, dict)]
    evidence_count = len(evidence_items)
    score += min(evidence_count * 3.5, 12.0)
    if evidence_count:
        reasons.append(f"证据条数 {evidence_count}")

    consensus_rate = _num(theme.get("consensus_rate"))
    if consensus_rate is not None:
        if consensus_rate >= 0.8:
            score += 8.0
            reasons.append(f"共识率 {consensus_rate:.0%}")
        elif consensus_rate < 0.55:
            score -= 8.0
            reasons.append(f"共识率偏低 {consensus_rate:.0%}")

    negative_count = int(theme.get("negative_count") or 0)
    if negative_count:
        score -= min(negative_count * 3.0, 15.0)
        reasons.append(f"{negative_count} 条分歧或风险线索")

    latest_published_at = None
    for item in sorted(evidence_items, key=lambda row: str(row.get("published_at") or ""), reverse=True):
        latest_published_at = _parse_datetime_or_none(item.get("published_at"))
        if latest_published_at is not None:
            break
    if latest_published_at is not None:
        age_hours = max((_utcnow() - latest_published_at).total_seconds() / 3600, 0.0)
        if age_hours <= 6:
            score += 10.0
            reasons.append("6小时内最新事件")
        elif age_hours <= 24:
            score += 6.0
            reasons.append("24小时内事件")
        elif age_hours <= 72:
            score += 2.0

    market_confirmation = theme.get("market_confirmation") or {}
    market_score = _num(market_confirmation.get("score"))
    if market_score is not None:
        score += min(market_score, 12.0)
        reasons.append(f"市场确认 {market_score:.1f}")

    behavior_risk = str((market_behavior.get("risk_pressure") or {}).get("label") or "")
    if behavior_risk and any(token in behavior_risk for token in ("退潮", "分歧", "压力")):
        score -= 5.0
        reasons.append(behavior_risk)

    if theme.get("crowding_risk"):
        score -= 4.0
        reasons.append(str(theme["crowding_risk"])[:24])

    semantic = theme.get("event_semantic") if isinstance(theme.get("event_semantic"), dict) else {}
    if semantic:
        strength = _num(semantic.get("catalyst_strength"))
        confidence = min(max(_num(semantic.get("confidence")) or 0.5, 0.0), 1.0)
        event_type = str(semantic.get("event_type") or "").strip()
        if strength is not None:
            score += (strength - 50.0) * 0.35 * confidence
            reasons.append(f"事件语义强度 {strength:.1f}/置信度 {confidence:.0%}")
        if event_type in {"政策支持", "订单兑现", "资质获批", "产业进展"}:
            score += 6.0
            reasons.append(f"事件类型 {event_type}")
        risk_signals = [str(item) for item in semantic.get("risk_signals") or [] if str(item).strip()]
        if risk_signals:
            score -= min(len(risk_signals) * 2.5, 7.5)
            reasons.append("语义风险：" + "；".join(risk_signals[:2]))

    return max(0.0, min(score, 100.0)), _dedupe_strings(reasons)


def _adaptive_feedback_score(
    *,
    symbol: str,
    primary_theme: dict[str, Any],
    history_stats: dict[str, Any],
    theme_feedback: dict[str, dict[str, Any]],
) -> tuple[float, list[str]]:
    score = 50.0
    reasons: list[str] = []

    symbol_count = int(history_stats.get("count") or 0)
    symbol_hit_rate = _num(history_stats.get("hit_rate"))
    if symbol_count >= 3 and symbol_hit_rate is not None:
        symbol_adjustment = (symbol_hit_rate - 0.5) * 42.0
        score += symbol_adjustment
        reasons.append(f"{symbol} 历史命中率 {symbol_hit_rate:.0%}")
    elif symbol_count > 0:
        score += 2.0
        reasons.append(f"{symbol} 反馈样本 {symbol_count} 次")
    else:
        reasons.append(f"{symbol} 反馈样本不足")

    symbol_model_score = _num(history_stats.get("learned_score"))
    symbol_confidence = min(max(_num(history_stats.get("confidence")) or 0.0, 0.0), 1.0)
    if symbol_model_score is not None and symbol_confidence > 0:
        score += (symbol_model_score - 50.0) * symbol_confidence * 0.9
        reasons.append(f"{symbol} 学习画像 {symbol_model_score:.1f}/置信度 {symbol_confidence:.0%}")

    theme_name = str(primary_theme.get("theme") or "").strip()
    theme_stats = theme_feedback.get(theme_name) or {}
    theme_count = int(theme_stats.get("count") or 0)
    theme_hit_rate = _num(theme_stats.get("hit_rate"))
    theme_avg_change_pct = _num(theme_stats.get("average_change_pct"))
    if theme_count >= 3 and theme_hit_rate is not None:
        theme_adjustment = (theme_hit_rate - 0.5) * 36.0
        score += theme_adjustment
        reasons.append(f"{theme_name} 历史命中率 {theme_hit_rate:.0%}")
    elif theme_count > 0:
        reasons.append(f"{theme_name} 反馈样本 {theme_count} 次")
    else:
        reasons.append(f"{theme_name} 主题反馈样本不足")

    if theme_avg_change_pct is not None:
        score += max(min(theme_avg_change_pct, 8.0), -8.0) * 1.8

    theme_model_score = _num(theme_stats.get("learned_score"))
    theme_confidence = min(max(_num(theme_stats.get("confidence")) or 0.0, 0.0), 1.0)
    if theme_model_score is not None and theme_confidence > 0:
        score += (theme_model_score - 50.0) * theme_confidence * 0.7
        reasons.append(f"{theme_name} 学习画像 {theme_model_score:.1f}/置信度 {theme_confidence:.0%}")

    if symbol_count >= 3 and symbol_hit_rate is not None and symbol_hit_rate < 0.45:
        score -= 8.0
        reasons.append("该标的历史表现偏弱")
    if theme_count >= 3 and theme_hit_rate is not None and theme_hit_rate < 0.45:
        score -= 6.0
        reasons.append("主题历史反馈偏弱")

    event_profile = primary_theme.get("event_feedback_profile") if isinstance(primary_theme.get("event_feedback_profile"), dict) else {}
    event_type = _event_type_from_match(primary_theme)
    event_count = int(event_profile.get("sample_count") or 0)
    event_learned_score = _num(event_profile.get("learned_score"))
    event_confidence = min(max(_num(event_profile.get("confidence")) or 0.0, 0.0), 1.0)
    event_hit_rate = _num(event_profile.get("hit_rate"))
    event_avg_change_pct = _num(event_profile.get("average_change_pct"))
    if event_type and event_count > 0:
        reasons.append(f"事件类型{event_type}反馈样本 {event_count} 次")
    if event_learned_score is not None and event_confidence > 0:
        score += (event_learned_score - 50.0) * event_confidence * 0.8
        reasons.append(f"事件类型{event_type}学习画像 {event_learned_score:.1f}/置信度 {event_confidence:.0%}")
    if event_avg_change_pct is not None:
        score += max(min(event_avg_change_pct, 6.0), -6.0) * 1.2
    if event_count >= 3 and event_hit_rate is not None and event_hit_rate < 0.45:
        score -= 5.0
        reasons.append("同类事件历史反馈偏弱")

    return max(0.0, min(score, 100.0)), _dedupe_strings(reasons)


def _learning_adjustment_policy(
    *,
    symbol: str,
    primary_theme: dict[str, Any],
    history_stats: dict[str, Any],
    theme_feedback: dict[str, dict[str, Any]],
    adaptive_feedback_score: float,
) -> dict[str, Any]:
    theme_name = str(primary_theme.get("theme") or "").strip()
    event_type = _event_type_from_match(primary_theme)
    theme_stats = theme_feedback.get(theme_name) or {}
    event_profile = primary_theme.get("event_feedback_profile") if isinstance(primary_theme.get("event_feedback_profile"), dict) else {}
    profile_inputs = [
        ("symbol", symbol, history_stats, 1.0),
        ("theme", theme_name, theme_stats, 0.75),
        ("event_type", event_type, event_profile, 0.85),
    ]
    signals: list[dict[str, Any]] = []
    weighted_edge = 0.0
    confidence_weight = 0.0
    for scope, key, stats, scope_weight in profile_inputs:
        if not isinstance(stats, dict) or not str(key or "").strip():
            continue
        learned_score = _num(stats.get("learned_score"))
        confidence = min(max(_num(stats.get("confidence")) or 0.0, 0.0), 1.0)
        sample_count = int(stats.get("sample_count") or stats.get("count") or stats.get("profile_sample_count") or 0)
        if learned_score is None or confidence <= 0:
            continue
        sample_weight = min(max(sample_count, 1) / 6.0, 1.0)
        effective_weight = confidence * sample_weight * scope_weight
        edge = learned_score - 50.0
        weighted_edge += edge * effective_weight
        confidence_weight += effective_weight
        signals.append(
            {
                "scope": scope,
                "key": str(key),
                "learned_score": round(float(learned_score), 2),
                "confidence": round(float(confidence), 4),
                "sample_count": sample_count,
                "edge": round(float(edge), 2),
            }
        )
    if confidence_weight <= 0:
        return {
            "status": "warming_up",
            "stance": "neutral",
            "learning_edge": 0.0,
            "confidence_weight": 0.0,
            "weight_adjustments": {},
            "risk_penalty_multiplier_delta": 0.0,
            "max_position_multiplier": 1.0,
            "max_position_cap_pct": None,
            "score_bias": 0.0,
            "signals": [],
            "reasons": ["反馈画像样本不足，暂不自动调参"],
        }

    learning_edge = weighted_edge / confidence_weight
    stance = "neutral"
    weight_adjustments: dict[str, float] = {}
    risk_delta = 0.0
    max_position_multiplier = 1.0
    max_position_cap_pct: float | None = None
    score_bias = max(min(learning_edge * 0.08, 3.0), -4.0)
    reasons: list[str] = []

    if learning_edge >= 8.0 and adaptive_feedback_score >= 58:
        stance = "expand"
        weight_adjustments = {
            "adaptive_feedback": 0.025,
            "continuity": 0.01,
            "momentum": -0.01,
        }
        risk_delta = -0.06
        max_position_multiplier = 1.12
        reasons.append("历史反馈画像偏强，适度提高反馈/连续性权重并放宽仓位")
    elif learning_edge <= -8.0 or adaptive_feedback_score <= 42:
        stance = "tighten"
        weight_adjustments = {
            "adaptive_feedback": 0.04,
            "fundamental": 0.015,
            "momentum": -0.025,
            "catalyst": -0.015,
        }
        risk_delta = 0.16
        max_position_multiplier = 0.72
        max_position_cap_pct = 5.0
        reasons.append("历史反馈画像偏弱，自动提高反馈约束、降低动量和题材放大")
    else:
        reasons.append("历史反馈画像中性，仅保留基础自适应评分")

    if signals:
        top_signal = sorted(signals, key=lambda item: abs(float(item.get("edge") or 0.0)), reverse=True)[0]
        reasons.append(
            f"{top_signal['scope']}:{top_signal['key']} 学习边际 {float(top_signal.get('edge') or 0.0):.1f}"
        )

    return {
        "status": "active",
        "stance": stance,
        "learning_edge": round(float(learning_edge), 2),
        "confidence_weight": round(float(confidence_weight), 4),
        "weight_adjustments": {key: round(float(value), 4) for key, value in weight_adjustments.items()},
        "risk_penalty_multiplier_delta": round(float(risk_delta), 4),
        "max_position_multiplier": round(float(max_position_multiplier), 4),
        "max_position_cap_pct": round(float(max_position_cap_pct), 2) if max_position_cap_pct is not None else None,
        "score_bias": round(float(score_bias), 2),
        "signals": signals,
        "reasons": _dedupe_strings(reasons),
    }


def _apply_learning_adjustment_policy(score_profile: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    result = dict(score_profile)
    weights = dict(result.get("weights") or {})
    adjustments = policy.get("weight_adjustments") if isinstance(policy.get("weight_adjustments"), dict) else {}
    for key, delta in adjustments.items():
        if key not in weights:
            continue
        weights[key] = max(0.0, round(float(weights.get(key) or 0.0) + float(delta or 0.0), 4))
    risk_multiplier = float(result.get("risk_penalty_multiplier") or 1.0)
    risk_multiplier += float(policy.get("risk_penalty_multiplier_delta") or 0.0)
    result["weights"] = {key: round(float(value), 4) for key, value in weights.items()}
    result["risk_penalty_multiplier"] = round(max(0.75, min(risk_multiplier, 1.55)), 4)
    reasons = [str(item) for item in result.get("reasons") or [] if str(item).strip()]
    if policy.get("status") == "active":
        reasons.extend(str(item) for item in policy.get("reasons") or [] if str(item).strip())
    result["reasons"] = _dedupe_strings(reasons)
    return result


def _build_learning_impact_trace(
    *,
    baseline_score: float,
    final_score: float,
    base_score_profile: dict[str, Any],
    adjusted_score_profile: dict[str, Any],
    component_scores: dict[str, float],
    adaptive_feedback_score: float,
    learning_policy: dict[str, Any],
    baseline_risk_penalty: float,
    effective_risk_penalty: float,
    risk_control: dict[str, Any],
) -> dict[str, Any]:
    base_weights = base_score_profile.get("weights") if isinstance(base_score_profile.get("weights"), dict) else {}
    adjusted_weights = adjusted_score_profile.get("weights") if isinstance(adjusted_score_profile.get("weights"), dict) else {}
    weight_deltas = {
        key: round(float(adjusted_weights.get(key) or 0.0) - float(base_weights.get(key) or 0.0), 4)
        for key in sorted(set(base_weights) | set(adjusted_weights))
        if round(float(adjusted_weights.get(key) or 0.0) - float(base_weights.get(key) or 0.0), 4) != 0
    }
    adaptive_weight = float(adjusted_weights.get("adaptive_feedback") or 0.0)
    feedback_delta = float(adaptive_feedback_score or 0.0) - 50.0
    learning_effect = risk_control.get("learning_effect") if isinstance(risk_control.get("learning_effect"), dict) else {}
    risk_gate_effect = risk_control.get("risk_gate_effect") if isinstance(risk_control.get("risk_gate_effect"), dict) else {}
    profile_signals = learning_policy.get("signals") if isinstance(learning_policy.get("signals"), list) else []
    dominant_signals = sorted(
        [signal for signal in profile_signals if isinstance(signal, dict)],
        key=lambda signal: abs(float(signal.get("edge") or 0.0)),
        reverse=True,
    )[:3]
    score_delta = float(final_score or 0.0) - float(baseline_score or 0.0)
    return {
        "version": "learning-impact-v1",
        "status": "active" if (
            abs(score_delta) >= 0.01
            or bool(weight_deltas)
            or bool(learning_effect.get("action_changed"))
            or bool(risk_gate_effect.get("applied"))
            or abs(feedback_delta) >= 0.01
        ) else "neutral",
        "baseline_score_before_learning_policy": round(float(baseline_score or 0.0), 2),
        "final_score": round(float(final_score or 0.0), 2),
        "score_delta_from_learning_policy": round(score_delta, 2),
        "adaptive_feedback_score": round(float(adaptive_feedback_score or 0.0), 2),
        "adaptive_feedback_delta_vs_neutral": round(feedback_delta, 2),
        "adaptive_feedback_weight": round(adaptive_weight, 4),
        "adaptive_feedback_contribution_delta_vs_neutral": round(adaptive_weight * feedback_delta, 2),
        "score_bias": _round_or_none(learning_policy.get("score_bias"), 2),
        "learning_edge": _round_or_none(learning_policy.get("learning_edge"), 2),
        "policy_stance": learning_policy.get("stance") or "neutral",
        "weight_deltas": weight_deltas,
        "risk_penalty_before_learning_policy": round(float(baseline_risk_penalty or 0.0), 2),
        "risk_penalty_after_learning_policy": round(float(effective_risk_penalty or 0.0), 2),
        "risk_penalty_delta_from_learning_policy": round(float(effective_risk_penalty or 0.0) - float(baseline_risk_penalty or 0.0), 2),
        "profile_signals": profile_signals,
        "dominant_profile_signals": dominant_signals,
        "component_scores": {
            key: round(float(value or 0.0), 2)
            for key, value in component_scores.items()
        },
        "risk_effect": {
            "action_before_learning": learning_effect.get("action_before_learning"),
            "action_after_learning": learning_effect.get("action_after_learning"),
            "action_changed": bool(learning_effect.get("action_changed")),
            "risk_level_before_learning": learning_effect.get("risk_level_before_learning"),
            "risk_level_after_learning": learning_effect.get("risk_level_after_learning"),
            "max_position_before_learning_pct": learning_effect.get("max_position_before_learning_pct"),
            "max_position_after_learning_pct": learning_effect.get("max_position_after_learning_pct"),
            "max_position_delta_pct": learning_effect.get("max_position_delta_pct"),
            "max_position_multiplier": learning_policy.get("max_position_multiplier"),
            "max_position_cap_pct": learning_policy.get("max_position_cap_pct"),
        },
        "risk_gate_effect": risk_gate_effect,
    }


def _adaptive_score_profile(market_behavior: dict[str, Any]) -> dict[str, Any]:
    weights = dict(BASE_SCORE_WEIGHTS)
    risk_penalty_multiplier = 1.0
    profile = "balanced"
    reasons: list[str] = []

    liquidity_label = str((market_behavior.get("liquidity_state") or {}).get("label") or "")
    breadth_label = str((market_behavior.get("breadth_state") or {}).get("label") or "")
    regime_label = str((market_behavior.get("market_regime") or {}).get("label") or "")
    sentiment_label = str((market_behavior.get("sentiment_state") or {}).get("label") or "")
    risk_label = str((market_behavior.get("risk_pressure") or {}).get("label") or "")
    label_text = " ".join([liquidity_label, breadth_label, regime_label, sentiment_label, risk_label])
    intraday_pulse = market_behavior.get("intraday_event_pulse") if isinstance(market_behavior.get("intraday_event_pulse"), dict) else {}
    pulse_status = str(intraday_pulse.get("status") or "")
    minute_proxy = market_behavior.get("minute_market_proxy") if isinstance(market_behavior.get("minute_market_proxy"), dict) else {}
    minute_proxy_status = str(minute_proxy.get("status") or "")
    pulse_feedback = market_behavior.get("intraday_event_pulse_feedback") if isinstance(market_behavior.get("intraday_event_pulse_feedback"), dict) else {}
    pulse_learned_score = _num(pulse_feedback.get("learned_score"))
    pulse_confidence = _num(pulse_feedback.get("confidence")) or 0.0

    high_risk = any(token in label_text for token in ("退潮", "强分歧", "封板质量风险", "接力降温", "个股失血", "指数托举"))
    broad_risk = any(token in label_text for token in ("普涨后分化", "分化风险"))
    offensive = (
        ("流动性" in liquidity_label and any(token in breadth_label for token in ("普涨", "温和扩散")))
        or any(token in regime_label for token in ("情绪主升", "流动性外溢"))
    )

    if high_risk:
        profile = "defensive"
        weights.update(
            {
                "catalyst": 0.26,
                "theme": 0.17,
                "relation": 0.16,
                "market_confirm": 0.15,
                "event_intelligence": 0.12,
                "adaptive_feedback": 0.12,
                "momentum": 0.04,
                "fundamental": 0.06,
                "continuity": 0.03,
            }
        )
        risk_penalty_multiplier = 1.25
        reasons.append("市场风险压力升高，降低纯题材和动量权重，提高确认度、反馈和基本面权重")
    elif offensive:
        profile = "offensive"
        weights.update(
            {
                "catalyst": 0.33,
                "theme": 0.21,
                "relation": 0.17,
                "market_confirm": 0.13,
                "event_intelligence": 0.10,
                "adaptive_feedback": 0.06,
                "momentum": 0.07,
                "fundamental": 0.03,
                "continuity": 0.01,
            }
        )
        risk_penalty_multiplier = 0.9 if not broad_risk else 1.05
        reasons.append("流动性和赚钱效应扩散，优先放大事件质量、主线强度和市场确认")

    if broad_risk and not high_risk:
        profile = "offensive_guarded" if profile == "offensive" else "guarded"
        risk_penalty_multiplier = max(risk_penalty_multiplier, 1.05)
        weights["adaptive_feedback"] = max(weights["adaptive_feedback"], 0.08)
        weights["continuity"] = max(weights["continuity"], 0.02)
        reasons.append("普涨后存在分化风险，保留反馈和连续性约束")

    if pulse_status in {"weak", "risk_off"}:
        profile = f"{profile}_intraday_guarded" if profile else "intraday_guarded"
        risk_penalty_multiplier = max(risk_penalty_multiplier, 1.34 if pulse_status == "risk_off" else 1.22)
        weights["theme"] = min(weights["theme"], 0.17)
        weights["momentum"] = min(weights["momentum"], 0.04)
        weights["market_confirm"] = max(weights["market_confirm"], 0.15)
        weights["adaptive_feedback"] = max(weights["adaptive_feedback"], 0.12)
        reasons.append(str(intraday_pulse.get("message") or "事件池分钟反应偏弱，收紧实时执行权重"))
    elif pulse_status == "confirming" and not high_risk:
        weights["market_confirm"] = max(weights["market_confirm"], 0.14)
        reasons.append(str(intraday_pulse.get("message") or "事件池分钟反应扩散确认，提高盘面确认权重"))

    if minute_proxy_status in {"weak", "risk_off", "thin_sample"}:
        profile = f"{profile}_minute_proxy_guarded" if profile else "minute_proxy_guarded"
        if minute_proxy_status == "risk_off":
            risk_penalty_multiplier = max(risk_penalty_multiplier, 1.32)
        else:
            risk_penalty_multiplier = max(risk_penalty_multiplier, 1.18)
        weights["market_confirm"] = min(weights["market_confirm"], 0.12)
        weights["momentum"] = min(weights["momentum"], 0.04)
        weights["adaptive_feedback"] = max(weights["adaptive_feedback"], 0.12)
        reasons.append(str(minute_proxy.get("message") or "分钟市场代理样本偏弱，收紧实时执行权重"))
    elif minute_proxy_status == "constructive" and not high_risk:
        reasons.append(str(minute_proxy.get("message") or "分钟市场代理样本偏强，仅作为部分样本确认"))

    if pulse_learned_score is not None and pulse_confidence >= 0.25:
        if pulse_learned_score <= 45.0:
            risk_penalty_multiplier = max(risk_penalty_multiplier, 1.30)
            weights["adaptive_feedback"] = max(weights["adaptive_feedback"], 0.13)
            reasons.append(f"事件池脉冲历史反馈偏弱：{pulse_status} 学习分 {pulse_learned_score:.1f}")
        elif pulse_learned_score >= 65.0:
            risk_penalty_multiplier = max(0.9, min(risk_penalty_multiplier, 1.05))
            reasons.append(f"事件池脉冲历史反馈较强：{pulse_status} 学习分 {pulse_learned_score:.1f}")

    missing_fields = [
        str(item)
        for item in ((market_behavior.get("data_quality") or {}).get("missing_fields") or [])
        if str(item).strip()
    ]
    if missing_fields:
        profile = "data_guarded" if profile == "balanced" else f"{profile}_data_guarded"
        risk_penalty_multiplier = max(risk_penalty_multiplier, 1.18)
        weights["market_confirm"] = min(weights["market_confirm"], 0.09)
        weights["event_intelligence"] = max(weights["event_intelligence"], 0.11)
        weights["adaptive_feedback"] = max(weights["adaptive_feedback"], 0.10)
        weights["momentum"] = min(weights["momentum"], 0.05)
        reasons.append("市场状态字段缺失，降低盘面确认和动量依赖，提高事件质量与历史反馈约束")

    if not reasons:
        reasons.append("市场状态中性，沿用均衡评分权重")

    return {
        "profile": profile,
        "weights": {key: round(float(value), 4) for key, value in weights.items()},
        "risk_penalty_multiplier": round(float(risk_penalty_multiplier), 4),
        "reasons": _dedupe_strings(reasons),
        "market_labels": {
            "liquidity_state": liquidity_label,
            "breadth_state": breadth_label,
            "market_regime": regime_label,
            "sentiment_state": sentiment_label,
            "risk_pressure": risk_label,
            "missing_fields": missing_fields,
            "minute_market_proxy": {
                "status": minute_proxy_status,
                "coverage_scope": minute_proxy.get("coverage_scope"),
                "symbol_count": minute_proxy.get("symbol_count"),
                "positive_ratio": minute_proxy.get("positive_ratio"),
                "average_change_pct": minute_proxy.get("average_change_pct"),
                "is_full_market_breadth": bool(minute_proxy.get("is_full_market_breadth")),
            } if minute_proxy else {},
        },
    }


def _risk_control_plan(
    *,
    features: dict[str, Any],
    primary_theme: dict[str, Any],
    market_behavior: dict[str, Any],
    risk_penalty: float,
    risk_flags: list[str],
    event_intelligence_score: float,
    adaptive_feedback_score: float,
    learning_policy: dict[str, Any] | None = None,
    risk_gate_feedback: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    change_pct = _num(features.get("change_pct")) or 0.0
    amount_ratio = _num(features.get("amount_ratio_20d")) or 1.0
    breadth_label = str((market_behavior.get("breadth_state") or {}).get("label") or "")
    liquidity_label = str((market_behavior.get("liquidity_state") or {}).get("label") or "")
    risk_pressure = str((market_behavior.get("risk_pressure") or {}).get("label") or "")
    intraday_pulse = market_behavior.get("intraday_event_pulse") if isinstance(market_behavior.get("intraday_event_pulse"), dict) else {}
    pulse_status = str(intraday_pulse.get("status") or "")
    minute_proxy = market_behavior.get("minute_market_proxy") if isinstance(market_behavior.get("minute_market_proxy"), dict) else {}
    minute_proxy_status = str(minute_proxy.get("status") or "")

    action = "follow"
    risk_level = "medium"
    max_position_pct = 8.0
    stop_loss_pct = 4.8
    invalidations: list[str] = []
    notes: list[str] = []

    if "缺失" in breadth_label or "缺失" in liquidity_label:
        action = "observe"
        risk_level = "high"
        max_position_pct = 3.0
        stop_loss_pct = 3.6
        notes.append("市场状态数据不完整，先以观察为主")
    elif any(token in risk_pressure for token in ("退潮", "强分歧", "压力")):
        action = "wait"
        risk_level = "high"
        max_position_pct = 4.0
        stop_loss_pct = 3.2
        notes.append(f"市场风险压力：{risk_pressure}")
    elif event_intelligence_score >= 72 and adaptive_feedback_score >= 60 and risk_penalty <= 6:
        action = "deploy"
        risk_level = "low"
        max_position_pct = 10.0
        stop_loss_pct = 5.6
        notes.append("事件质量和历史反馈同时支持")
    elif adaptive_feedback_score < 42 or event_intelligence_score < 42:
        action = "observe"
        risk_level = "medium"
        max_position_pct = 5.0
        stop_loss_pct = 4.0
        notes.append("事件或反馈信号不足")

    if change_pct >= 9.5:
        invalidations.append("T-1 接近涨停，次日更看承接不看追高")
    elif change_pct >= 7:
        invalidations.append(f"T-1 涨幅 {change_pct:.2f}%，存在追高风险")
    if amount_ratio >= 3:
        invalidations.append("量能极端放大，分歧和兑现概率上升")
    semantic = primary_theme.get("event_semantic") if isinstance(primary_theme.get("event_semantic"), dict) else {}
    if semantic:
        invalidations.extend(str(item) for item in semantic.get("invalidation_conditions") or [] if str(item).strip())
        semantic_risks = [str(item) for item in semantic.get("risk_signals") or [] if str(item).strip()]
        if semantic_risks:
            notes.append("事件语义风险：" + "；".join(semantic_risks[:2]))
    if risk_flags:
        invalidations.extend(risk_flags[:2])
    if "证券" in str(primary_theme.get("theme") or "") and action == "deploy":
        notes.append("金融主题仅在确认度高时放大仓位")

    if pulse_status in {"weak", "risk_off"}:
        if action == "deploy":
            action = "follow" if pulse_status == "weak" else "observe"
        if pulse_status == "risk_off":
            risk_level = "high"
            max_position_pct = min(max_position_pct, 3.0)
        else:
            risk_level = "high" if risk_level == "high" else "medium"
            max_position_pct = min(max_position_pct, 5.0)
        invalidations.append("事件池分钟脉冲未确认或转弱")
        notes.append(str(intraday_pulse.get("message") or "事件池分钟脉冲偏弱，降低实时执行强度"))

    if minute_proxy_status in {"weak", "risk_off", "thin_sample"}:
        if action == "deploy":
            action = "follow" if minute_proxy_status != "risk_off" else "observe"
        if minute_proxy_status == "risk_off":
            risk_level = "high"
            max_position_pct = min(max_position_pct, 3.5)
        else:
            risk_level = "high" if risk_level == "high" else "medium"
            max_position_pct = min(max_position_pct, 5.0)
        invalidations.append("分钟市场代理未确认或转弱")
        notes.append(str(minute_proxy.get("message") or "分钟市场代理偏弱，降低实时执行强度"))

    action_before_learning = action
    risk_level_before_learning = risk_level
    max_position_before_learning_pct = max_position_pct
    policy = learning_policy if isinstance(learning_policy, dict) else {}
    if policy.get("status") == "active":
        stance = str(policy.get("stance") or "neutral")
        position_multiplier = float(policy.get("max_position_multiplier") or 1.0)
        position_cap = _num(policy.get("max_position_cap_pct"))
        if stance == "tighten":
            if action == "deploy":
                action = "follow"
                risk_level = "medium"
            max_position_pct *= position_multiplier
            if position_cap is not None:
                max_position_pct = min(max_position_pct, position_cap)
            notes.append("学习调参：历史反馈偏弱，自动收紧仓位")
        elif stance == "expand":
            max_position_pct *= position_multiplier
            notes.append("学习调参：历史反馈偏强，允许小幅放宽仓位")
        for reason in policy.get("reasons") or []:
            if str(reason).strip():
                notes.append("学习调参：" + str(reason))
        max_position_pct = max(1.0, min(max_position_pct, 12.0))
    action_after_learning = action
    risk_level_after_learning = risk_level
    max_position_after_learning_pct = max_position_pct

    monitor = _risk_monitoring_state(
        features=features,
        action=action,
        risk_level=risk_level,
        max_position_pct=max_position_pct,
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=min(stop_loss_pct * 2.5, 18.0),
        invalidations=_dedupe_strings(invalidations),
        risk_flags=risk_flags,
        notes=_dedupe_strings(notes),
    )
    gate_before_feedback = str(monitor.get("execution_gate") or "")
    status_before_feedback = str(monitor.get("status") or "")
    max_position_before_gate_pct = max_position_pct
    risk_level_before_gate = risk_level
    max_position_pct, risk_level = _apply_risk_gate_feedback(
        monitor=monitor,
        max_position_pct=max_position_pct,
        risk_level=risk_level,
        notes=notes,
        risk_gate_feedback=risk_gate_feedback or {},
    )
    gate_after_feedback = str(monitor.get("execution_gate") or "")
    gate_feedback = monitor.get("gate_feedback") if isinstance(monitor.get("gate_feedback"), dict) else {}

    return {
        "action": action,
        "risk_level": risk_level,
        "max_position_pct": round(max_position_pct, 2),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "take_profit_pct": round(min(stop_loss_pct * 2.5, 18.0), 2),
        "invalidations": _dedupe_strings(invalidations),
        "notes": _dedupe_strings(notes),
        "learning_adjustment": {
            "status": policy.get("status") or "warming_up",
            "stance": policy.get("stance") or "neutral",
            "learning_edge": policy.get("learning_edge"),
            "max_position_multiplier": policy.get("max_position_multiplier"),
            "max_position_cap_pct": policy.get("max_position_cap_pct"),
        },
        "learning_effect": {
            "action_before_learning": action_before_learning,
            "action_after_learning": action_after_learning,
            "action_changed": action_before_learning != action_after_learning,
            "risk_level_before_learning": risk_level_before_learning,
            "risk_level_after_learning": risk_level_after_learning,
            "risk_level_changed": risk_level_before_learning != risk_level_after_learning,
            "max_position_before_learning_pct": round(max_position_before_learning_pct, 2),
            "max_position_after_learning_pct": round(max_position_after_learning_pct, 2),
            "max_position_delta_pct": round(max_position_after_learning_pct - max_position_before_learning_pct, 2),
            "policy_stance": policy.get("stance") or "neutral",
        },
        "risk_gate_effect": {
            "gate_before_feedback": gate_before_feedback,
            "gate_after_feedback": gate_after_feedback,
            "gate_changed": gate_before_feedback != gate_after_feedback,
            "status_before_feedback": status_before_feedback,
            "status_after_feedback": monitor.get("status"),
            "risk_level_before_gate": risk_level_before_gate,
            "risk_level_after_gate": risk_level,
            "max_position_before_gate_pct": round(max_position_before_gate_pct, 2),
            "max_position_after_gate_pct": round(max_position_pct, 2),
            "max_position_delta_pct": round(max_position_pct - max_position_before_gate_pct, 2),
            "applied": bool(gate_feedback.get("applied")),
            "influence": gate_feedback.get("influence"),
            "adjustment": gate_feedback.get("adjustment"),
        },
        "risk_monitoring": monitor,
    }


def _apply_risk_gate_feedback(
    *,
    monitor: dict[str, Any],
    max_position_pct: float,
    risk_level: str,
    notes: list[str],
    risk_gate_feedback: dict[str, dict[str, Any]],
) -> tuple[float, str]:
    gate = str(monitor.get("execution_gate") or "").strip()
    profile = risk_gate_feedback.get(gate) if gate else None
    public_profile = _public_risk_gate_feedback_profile(profile or {})
    if not public_profile:
        return max_position_pct, risk_level

    learned_score = _num(profile.get("learned_score")) if isinstance(profile, dict) else None
    confidence = _num(profile.get("confidence")) if isinstance(profile, dict) else None
    sample_count = int(profile.get("sample_count") or 0) if isinstance(profile, dict) else 0
    score = learned_score if learned_score is not None else 50.0
    effective_confidence = confidence if confidence is not None else 0.0
    public_profile.update(
        {
            "applied": False,
            "influence": "insufficient_history",
            "current_gate": gate,
        }
    )
    monitor["gate_feedback"] = public_profile
    if sample_count < 3 or effective_confidence < 0.25:
        return max_position_pct, risk_level

    adjustment_note = ""
    if gate in {"allow", "allow_probe"}:
        if score <= 45.0:
            max_position_pct *= 0.55
            risk_level = "medium" if risk_level == "low" else risk_level
            monitor["execution_gate"] = "confirm"
            monitor["status"] = "armed"
            monitor["severity"] = _max_severity(str(monitor.get("severity") or "medium"), "medium")
            monitor["position_limit_pct"] = round(max_position_pct, 2)
            monitor["next_action"] = "历史放行效果偏弱，降为确认后再执行；只允许等待量价和事件反应二次确认。"
            adjustment_note = "风险gate学习：历史放行后命中不足，自动降为确认并收紧仓位"
            public_profile.update(
                {
                    "applied": True,
                    "influence": "tighten",
                    "adjustment": "downgrade_to_confirm",
                    "recommended_gate": "confirm",
                    "position_multiplier": 0.55,
                }
            )
        elif score >= 65.0:
            adjustment_note = "风险gate学习：历史放行结果支持当前 gate"
            public_profile.update({"influence": "supportive", "adjustment": "keep_current_gate"})
        else:
            public_profile.update({"influence": "neutral", "adjustment": "keep_current_gate"})
    elif gate == "confirm":
        if score <= 45.0:
            max_position_pct *= 0.70
            monitor["position_limit_pct"] = round(max_position_pct, 2)
            monitor["next_action"] = "历史确认 gate 的胜率偏弱，继续确认且降低试错仓位。"
            adjustment_note = "风险gate学习：历史确认 gate 偏弱，降低试错仓位"
            public_profile.update(
                {
                    "applied": True,
                    "influence": "tighten",
                    "adjustment": "confirm_tightened",
                    "recommended_gate": "confirm",
                    "position_multiplier": 0.70,
                }
            )
        elif score >= 65.0:
            adjustment_note = "风险gate学习：历史确认 gate 有效，维持二次确认流程"
            public_profile.update({"influence": "supportive", "adjustment": "keep_current_gate"})
        else:
            public_profile.update({"influence": "neutral", "adjustment": "keep_current_gate"})
    elif gate in PROTECTIVE_RISK_GATES:
        if score <= 45.0:
            adjustment_note = "风险gate学习：历史拦截存在机会成本，标记为可能过度保守"
            monitor["next_action"] = "历史显示该类拦截可能错过机会；仍不直接放行，满足盘口承接后仅允许观察性试仓并再次确认。"
            public_profile.update(
                {
                    "applied": True,
                    "influence": "review_conservatism",
                    "adjustment": "overly_conservative",
                    "overly_conservative": True,
                    "recommended_gate": "confirm_after_recheck",
                }
            )
        elif score >= 65.0:
            adjustment_note = "风险gate学习：历史拦截有效，支持当前保护性 gate"
            public_profile.update({"influence": "supportive", "adjustment": "keep_current_gate"})
        else:
            public_profile.update({"influence": "neutral", "adjustment": "keep_current_gate"})

    if adjustment_note:
        notes.append(adjustment_note)
        monitor_notes = monitor.get("notes") if isinstance(monitor.get("notes"), list) else []
        monitor["notes"] = _dedupe_strings([*monitor_notes, adjustment_note])[:8]
    monitor["gate_feedback"] = public_profile
    return max(1.0, min(max_position_pct, 12.0)), risk_level


def _max_severity(left: str, right: str) -> str:
    order = {"low": 0, "medium": 1, "high": 2, "very_high": 3}
    return left if order.get(left, 1) >= order.get(right, 1) else right


def _risk_monitoring_state(
    *,
    features: dict[str, Any],
    action: str,
    risk_level: str,
    max_position_pct: float,
    stop_loss_pct: float,
    take_profit_pct: float,
    invalidations: list[str],
    risk_flags: list[str],
    notes: list[str],
) -> dict[str, Any]:
    event_reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
    reaction_status = str(event_reaction.get("status") or "")
    reaction_score = _num(event_reaction.get("score"))
    reaction_change = _num(event_reaction.get("change_pct"))
    triggers: list[str] = []
    if risk_flags:
        triggers.extend(risk_flags[:3])
    if invalidations:
        triggers.extend(invalidations[:4])

    status = "armed"
    gate = "confirm"
    severity = "medium"
    next_action = "等待事件后盘面确认，满足承接后再按仓位上限执行。"
    review_clock = "intraday_30m"

    if action in {"observe", "wait"}:
        status = "blocked"
        gate = "blocked"
        severity = "high" if risk_level in {"high", "very_high"} else "medium"
        next_action = "不建仓，仅保留观察；待风险触发项解除后再重评。"
    elif action == "follow":
        status = "armed"
        gate = "confirm"
        severity = "medium"
        next_action = "只允许轻仓跟踪；必须等待量价或事件反应继续确认。"
    elif action == "deploy":
        status = "active"
        gate = "allow"
        severity = "low"
        next_action = "允许执行，但不得超过模型给出的最大仓位。"

    if reaction_status in {"divergent", "daily_proxy_divergent"}:
        status = "invalidated"
        gate = "blocked" if reaction_status == "divergent" else "reduce_only"
        severity = "high"
        next_action = "事件后市场反应背离，剔除或降为只减不加。"
        triggers.insert(0, "事件后市场反应背离")
    elif reaction_status == "missing":
        if gate == "allow":
            gate = "confirm"
        if status == "active":
            status = "pending_confirmation"
        severity = max(severity, "medium", key={"low": 0, "medium": 1, "high": 2}.get)
        next_action = "分钟确认缺失，等待补齐或用下一轮刷新确认后再放大仓位。"
        triggers.append("事件后分钟确认缺失")
    elif reaction_status in {"confirmed", "daily_proxy_confirmed"} and gate == "confirm":
        status = "active_confirmed"
        gate = "allow_probe" if action == "follow" else "allow"
        next_action = "事件反应已确认，可按风控上限执行或继续跟踪。"

    if risk_level in {"high", "very_high"} and gate in {"allow", "allow_probe"}:
        gate = "confirm"
        status = "armed"
        severity = "high"
        next_action = "风险级别偏高，执行前必须等待额外确认。"

    hard_exits = _dedupe_strings([
        f"跌破入选后成本 {stop_loss_pct:.1f}% 止损",
        *invalidations,
        "事件后市场反应转为背离",
    ])
    return {
        "status": status,
        "execution_gate": gate,
        "severity": severity,
        "next_action": next_action,
        "review_clock": review_clock,
        "position_limit_pct": round(max_position_pct, 2),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "take_profit_pct": round(take_profit_pct, 2),
        "event_reaction_status": reaction_status or None,
        "event_reaction_score": _round_or_none(reaction_score, 2),
        "event_reaction_change_pct": _round_or_none(reaction_change, 4),
        "trigger_count": len(_dedupe_strings(triggers)),
        "triggers": _dedupe_strings(triggers)[:8],
        "hard_exits": hard_exits[:8],
        "notes": notes[:6],
    }


def _execution_gate_score_adjustment(risk_control: dict[str, Any]) -> dict[str, Any]:
    monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
    gate = str(monitor.get("execution_gate") or "").strip()
    status = str(monitor.get("status") or "").strip()
    action = str(risk_control.get("action") or "").strip()
    position_limit = _num(monitor.get("position_limit_pct"))
    if position_limit is None:
        position_limit = _num(risk_control.get("max_position_pct"))

    score_delta = 0.0
    reasons: list[str] = []
    if gate == "blocked":
        score_delta -= 18.0
        reasons.append("执行门控 blocked，禁止建仓")
    elif gate == "reduce_only":
        score_delta -= 14.0
        reasons.append("执行门控 reduce_only，只减不加")
    elif gate == "confirm":
        score_delta -= 6.0
        reasons.append("执行门控 confirm，等待二次确认")
    elif gate == "allow_probe":
        score_delta -= 2.5
        reasons.append("执行门控 allow_probe，仅观察性试仓")

    if status == "invalidated":
        score_delta -= 6.0
        reasons.append("风控状态 invalidated")
    elif status == "blocked" and gate != "blocked":
        score_delta -= 6.0
        reasons.append("风控状态 blocked")

    if action in {"observe", "wait", "avoid"} and gate not in {"blocked", "reduce_only"}:
        score_delta -= 5.0
        reasons.append(f"执行动作 {action}")

    if position_limit is not None and position_limit <= 3.0 and gate not in {"allow", "allow_probe"}:
        score_delta -= 2.0
        reasons.append(f"仓位上限 {position_limit:.1f}%")

    score_delta = max(score_delta, -28.0)
    return {
        "gate": gate or None,
        "status": status or None,
        "action": action or None,
        "score_delta": round(score_delta, 2),
        "reason": "；".join(_dedupe_strings(reasons)) if reasons else None,
    }


def _build_closed_loop_trace(
    *,
    symbol: str,
    features: dict[str, Any],
    primary_theme: dict[str, Any],
    event_intelligence_score: float,
    event_intelligence_reasons: list[str],
    adaptive_feedback_score: float,
    adaptive_feedback_reasons: list[str],
    risk_control: dict[str, Any],
    market_background: str,
    market_behavior: dict[str, Any],
    market_confirm_score: float,
    score_profile: dict[str, Any],
    learning_policy: dict[str, Any],
    symbol_feedback_profile: dict[str, Any],
    theme_feedback_profile: dict[str, Any],
    component_scores: dict[str, float],
    execution_adjustment: dict[str, Any],
    trigger_news_signal: dict[str, Any],
    trigger_news_adjustment: dict[str, Any],
    raw_risk_penalty: float,
    effective_risk_penalty: float,
    learning_impact: dict[str, Any],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "event": {
            "theme": primary_theme.get("theme"),
            "semantic": primary_theme.get("event_semantic") or {},
            "semantic_source": primary_theme.get("semantic_source"),
            "source_tier": primary_theme.get("source_tier"),
            "top_source_tier": primary_theme.get("top_source_tier"),
            "policy_boost": bool(primary_theme.get("policy_boost")),
            "evidence_count": int(primary_theme.get("evidence_count") or 0),
            "summary": primary_theme.get("summary"),
            "catalyst": primary_theme.get("catalyst"),
            "score": round(event_intelligence_score, 2),
            "reasons": event_intelligence_reasons,
            "fresh_news_trigger": trigger_news_signal,
        },
        "market": {
            "background": market_background,
            "behavior_labels": {
                "liquidity_state": (market_behavior.get("liquidity_state") or {}).get("label"),
                "breadth_state": (market_behavior.get("breadth_state") or {}).get("label"),
                "market_regime": (market_behavior.get("market_regime") or {}).get("label"),
                "risk_pressure": (market_behavior.get("risk_pressure") or {}).get("label"),
            },
            "mainline_alignment_score": _num(primary_theme.get("mainline_alignment_score")) or 0.0,
            "mainline_alignment_reasons": primary_theme.get("mainline_alignment_reasons") or [],
            "market_confirm_score": round(float(market_confirm_score or 0.0), 2),
            "event_reaction": features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {},
            "intraday_event_pulse": market_behavior.get("intraday_event_pulse") if isinstance(market_behavior.get("intraday_event_pulse"), dict) else {},
            "minute_market_proxy": market_behavior.get("minute_market_proxy") if isinstance(market_behavior.get("minute_market_proxy"), dict) else {},
        },
        "feedback": {
            "model_version": FEEDBACK_MODEL_VERSION,
            "score": round(adaptive_feedback_score, 2),
            "reasons": adaptive_feedback_reasons,
            "symbol_profile": symbol_feedback_profile,
            "theme_profile": theme_feedback_profile,
            "event_type_profile": _public_feedback_profile(primary_theme.get("event_feedback_profile") or {}),
        },
        "scoring": {
            "score_version": SCORE_VERSION,
            "profile": score_profile.get("profile"),
            "weights": score_profile.get("weights") or {},
            "risk_penalty_multiplier": score_profile.get("risk_penalty_multiplier"),
            "raw_risk_penalty": round(float(raw_risk_penalty or 0.0), 2),
            "effective_risk_penalty": round(float(effective_risk_penalty or 0.0), 2),
            "component_scores": {
                key: round(float(value or 0.0), 2)
                for key, value in component_scores.items()
            },
            "execution_gate_adjustment": execution_adjustment,
            "fresh_news_trigger_adjustment": trigger_news_adjustment,
            "reasons": score_profile.get("reasons") or [],
            "market_labels": score_profile.get("market_labels") or {},
            "learning_adjustment_policy": learning_policy,
            "learning_impact": learning_impact,
        },
        "risk_control": risk_control,
    }


def _signal_flags(features: dict[str, Any], previous_state: dict[str, Any], history_stats: dict[str, Any], risk_flags: list[str]) -> list[str]:
    flags: list[str] = []
    if (features.get("change_pct") or 0) >= 9.5:
        flags.append("T-1涨停延续")
    if (features.get("r60") or 0) >= 75:
        flags.append(f"R60={float(features.get('r60')):.2f}强势")
    streak = int(previous_state.get("streak") or 0)
    if streak >= 1:
        flags.append(f"连续入选{streak + 1}次")
    else:
        flags.append("首次入选")
    count = int(history_stats.get("count") or 0)
    hit_rate = _num(history_stats.get("hit_rate"))
    if count >= 3 and hit_rate is not None and hit_rate >= 0.6:
        flags.append(f"PROTECTED({count}次/{hit_rate:.0%}WR)")
    event_reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
    reaction_status = str(event_reaction.get("status") or "")
    reaction_change = _num(event_reaction.get("change_pct"))
    if reaction_status == "confirmed":
        suffix = f"{reaction_change:+.2f}%" if reaction_change is not None else "确认"
        flags.append(f"分钟反应确认({suffix})")
    elif reaction_status == "daily_proxy_confirmed":
        suffix = f"{reaction_change:+.2f}%" if reaction_change is not None else "确认"
        flags.append(f"日内代理确认({suffix})")
    elif reaction_status == "divergent":
        suffix = f"{reaction_change:+.2f}%" if reaction_change is not None else "背离"
        flags.append(f"分钟反应背离({suffix})")
    elif reaction_status == "daily_proxy_divergent":
        suffix = f"{reaction_change:+.2f}%" if reaction_change is not None else "背离"
        flags.append(f"日内代理背离({suffix})")
    if risk_flags:
        flags.append("风险待确认")
    return flags


def _reason_parts(features: dict[str, Any], primary_theme: dict[str, Any], signal_flags: list[str], risk_flags: list[str]) -> list[str]:
    parts: list[str] = []
    if primary_theme.get("catalyst"):
        parts.append(str(primary_theme["catalyst"])[:80])
    elif primary_theme.get("summary"):
        parts.append(str(primary_theme["summary"])[:80])
    relation_reasons = primary_theme.get("relation_reasons") or []
    parts.extend(str(item) for item in relation_reasons[:2])
    event_reaction = features.get("event_reaction") if isinstance(features.get("event_reaction"), dict) else {}
    reaction_reasons = [str(item) for item in event_reaction.get("reasons") or [] if str(item).strip()]
    if reaction_reasons:
        parts.append("分钟反馈：" + "；".join(reaction_reasons[:2]))
    parts.extend(signal_flags[:3])
    if risk_flags:
        parts.append("风险：" + "；".join(risk_flags[:2]))
    if not parts:
        parts.append(f"{features.get('name') or features.get('symbol')}进入主线候选池。")
    return parts[:8]


def _load_previous_selection_state(db: Session, *, symbols: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    rows = db.execute(
        text(
            """
            SELECT symbol, trade_date, rank
            FROM catalyst_selection_items
            WHERE symbol IN :symbols
              AND trade_date < :trade_date
            ORDER BY symbol, trade_date DESC
            """
        ).bindparams(bindparam("symbols", expanding=True)),
        {"symbols": symbols, "trade_date": trade_date},
    ).mappings().all()
    state: dict[str, dict[str, Any]] = defaultdict(lambda: {"streak": 0, "last_rank": None})
    last_date_by_symbol: dict[str, date] = {}
    for row in rows:
        symbol = row["symbol"]
        row_date = pd.to_datetime(row["trade_date"]).date()
        if symbol not in last_date_by_symbol:
            last_date_by_symbol[symbol] = row_date
            state[symbol]["last_rank"] = row["rank"]
            state[symbol]["streak"] = 1
            continue
        if (last_date_by_symbol[symbol] - row_date).days <= 4:
            state[symbol]["streak"] += 1
            last_date_by_symbol[symbol] = row_date
    return dict(state)


def _load_symbol_settlement_stats(db: Session, *, symbols: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    rows = db.execute(
        text(
            """
            SELECT symbol,
                   COUNT(*) AS count,
                   SUM(CASE WHEN outcome IN ('hit', 'strong_hit') THEN 1 ELSE 0 END) AS hit_count,
                   SUM(CASE WHEN outcome IN ('miss', 'weak_miss') THEN 1 ELSE 0 END) AS loss_count
            FROM catalyst_selection_settlements
            WHERE symbol IN :symbols
              AND trade_date < :trade_date
            GROUP BY symbol
            """
        ).bindparams(bindparam("symbols", expanding=True)),
        {"symbols": symbols, "trade_date": trade_date},
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        count = int(row["count"] or 0)
        hit_count = int(row["hit_count"] or 0)
        stats[row["symbol"]] = {
            "count": count,
            "hit_count": hit_count,
            "loss_count": int(row["loss_count"] or 0),
            "hit_rate": hit_count / count if count else None,
        }
    return stats


def _refresh_feedback_profiles_from_settlements(
    db: Session,
    *,
    symbols: list[str],
    themes: list[str],
    now_value: datetime,
    event_types: list[str] | None = None,
    risk_gates: list[str] | None = None,
) -> dict[str, Any]:
    symbol_profiles = _compute_symbol_feedback_profiles(db, symbols=symbols)
    theme_profiles = _compute_theme_feedback_profiles(db, themes=themes)
    event_type_profiles = _compute_event_type_feedback_profiles(db, event_types=event_types or [])
    risk_gate_profiles = _compute_risk_gate_feedback_profiles(db, risk_gates=risk_gates or list(RISK_EXECUTION_GATES))
    intraday_pulse_profiles = _compute_intraday_pulse_feedback_profiles(db, pulse_statuses=list(INTRADAY_PULSE_PROFILE_KEYS))
    all_profiles = [*symbol_profiles, *theme_profiles, *event_type_profiles, *risk_gate_profiles, *intraday_pulse_profiles]
    previous_profiles = _load_feedback_profile_snapshots(db, all_profiles)
    profile_changes: list[dict[str, Any]] = []
    for profile in all_profiles:
        key = (str(profile.get("profile_scope") or ""), str(profile.get("profile_key") or ""))
        profile_changes.append(_feedback_profile_change(profile, previous_profiles.get(key)))
        _upsert_feedback_profile(db, profile=profile, now_value=now_value)
    material_changes = [change for change in profile_changes if bool(change.get("is_changed"))]
    return {
        "model_version": FEEDBACK_MODEL_VERSION,
        "updated_profile_count": len(all_profiles),
        "new_profile_count": sum(1 for change in profile_changes if bool(change.get("is_new"))),
        "changed_profile_count": len(material_changes),
        "top_profile_changes": _top_feedback_profile_changes(material_changes, limit=8),
        "symbol_profile_count": len(symbol_profiles),
        "theme_profile_count": len(theme_profiles),
        "event_type_profile_count": len(event_type_profiles),
        "risk_gate_profile_count": len(risk_gate_profiles),
        "intraday_pulse_profile_count": len(intraday_pulse_profiles),
        "updated_at": now_value.isoformat(),
    }


def _load_feedback_profile_snapshots(
    db: Session,
    profiles: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    scope_keys: dict[str, set[str]] = defaultdict(set)
    for profile in profiles:
        scope = str(profile.get("profile_scope") or "").strip()
        key = str(profile.get("profile_key") or "").strip()
        if scope and key:
            scope_keys[scope].add(key)
    snapshots: dict[tuple[str, str], dict[str, Any]] = {}
    for scope, keys in scope_keys.items():
        rows = db.execute(
            text(
                """
                SELECT *
                FROM catalyst_selection_feedback_profiles
                WHERE profile_scope = :scope
                  AND profile_key IN :keys
                  AND model_version = :model_version
                """
            ).bindparams(bindparam("keys", expanding=True)),
            {"scope": scope, "keys": sorted(keys), "model_version": FEEDBACK_MODEL_VERSION},
        ).mappings().all()
        for row in rows:
            profile = _row_to_feedback_profile(row)
            snapshots[(str(row["profile_scope"]), str(row["profile_key"]))] = profile
    return snapshots


def _feedback_profile_change(current: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    scope = str(current.get("profile_scope") or "").strip()
    key = str(current.get("profile_key") or "").strip()
    before_score = _num((previous or {}).get("learned_score"))
    after_score = _num(current.get("learned_score"))
    before_confidence = _num((previous or {}).get("confidence"))
    after_confidence = _num(current.get("confidence"))
    before_hit_rate = _num((previous or {}).get("hit_rate"))
    after_hit_rate = _num(current.get("hit_rate"))
    before_avg_change = _num((previous or {}).get("average_change_pct"))
    after_avg_change = _num(current.get("average_change_pct"))
    before_samples = int((previous or {}).get("sample_count") or 0)
    after_samples = int(current.get("sample_count") or 0)
    score_delta = (after_score - before_score) if after_score is not None and before_score is not None else None
    confidence_delta = (after_confidence - before_confidence) if after_confidence is not None and before_confidence is not None else None
    hit_rate_delta = (after_hit_rate - before_hit_rate) if after_hit_rate is not None and before_hit_rate is not None else None
    avg_change_delta = (after_avg_change - before_avg_change) if after_avg_change is not None and before_avg_change is not None else None
    sample_delta = after_samples - before_samples
    is_new = previous is None
    is_changed = bool(
        is_new
        or sample_delta != 0
        or (score_delta is not None and abs(score_delta) >= 0.01)
        or (confidence_delta is not None and abs(confidence_delta) >= 0.0001)
        or (hit_rate_delta is not None and abs(hit_rate_delta) >= 0.0001)
        or (avg_change_delta is not None and abs(avg_change_delta) >= 0.0001)
    )
    if is_new:
        direction = "new"
    elif score_delta is not None and score_delta > 0.01:
        direction = "improved"
    elif score_delta is not None and score_delta < -0.01:
        direction = "weakened"
    elif sample_delta != 0:
        direction = "sample_changed"
    else:
        direction = "unchanged"
    return {
        "profile_scope": scope,
        "profile_key": key,
        "is_new": is_new,
        "is_changed": is_changed,
        "direction": direction,
        "sample_count_before": before_samples if previous is not None else None,
        "sample_count_after": after_samples,
        "sample_count_delta": sample_delta if previous is not None else after_samples,
        "learned_score_before": _round_or_none(before_score, 2),
        "learned_score_after": _round_or_none(after_score, 2),
        "learned_score_delta": _round_or_none(score_delta, 2),
        "confidence_before": _round_or_none(before_confidence, 4),
        "confidence_after": _round_or_none(after_confidence, 4),
        "confidence_delta": _round_or_none(confidence_delta, 4),
        "hit_rate_before": _round_or_none(before_hit_rate, 4),
        "hit_rate_after": _round_or_none(after_hit_rate, 4),
        "hit_rate_delta": _round_or_none(hit_rate_delta, 4),
        "average_change_pct_before": _round_or_none(before_avg_change, 4),
        "average_change_pct_after": _round_or_none(after_avg_change, 4),
        "average_change_pct_delta": _round_or_none(avg_change_delta, 4),
    }


def _top_feedback_profile_changes(changes: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    def _sort_key(change: dict[str, Any]) -> tuple[float, int, int]:
        score_delta = abs(float(change.get("learned_score_delta") or 0.0))
        sample_delta = abs(int(change.get("sample_count_delta") or 0))
        is_new = 1 if change.get("is_new") else 0
        return (score_delta, sample_delta, is_new)

    ranked = sorted(changes, key=_sort_key, reverse=True)
    return ranked[: max(0, int(limit or 0))]


def _new_feedback_stat() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "hit_count": 0,
        "miss_count": 0,
        "change_sum": 0.0,
        "change_count": 0,
        "hit_score_sum": 0.0,
        "hit_score_count": 0,
        "last_trade_date": None,
        "last_settlement_date": None,
    }


def _add_feedback_stat(
    stats: dict[str, dict[str, Any]],
    key: str,
    *,
    outcome: Any,
    change_pct: Any,
    hit_score: Any,
    trade_date: Any,
    settlement_date: Any = None,
) -> None:
    normalized_key = str(key or "").strip()
    if not normalized_key:
        return
    outcome_value = str(outcome or "").strip()
    if outcome_value not in POSITIVE_SETTLEMENT_OUTCOMES and outcome_value not in NEGATIVE_SETTLEMENT_OUTCOMES:
        return
    stat = stats[normalized_key]
    stat["sample_count"] += 1
    if outcome_value in POSITIVE_SETTLEMENT_OUTCOMES:
        stat["hit_count"] += 1
    elif outcome_value in NEGATIVE_SETTLEMENT_OUTCOMES:
        stat["miss_count"] += 1
    numeric_change = _num(change_pct)
    if numeric_change is not None:
        stat["change_sum"] += numeric_change
        stat["change_count"] += 1
    numeric_hit_score = _num(hit_score)
    if numeric_hit_score is not None:
        stat["hit_score_sum"] += numeric_hit_score
        stat["hit_score_count"] += 1
    trade_date_value = str(trade_date or "")
    settlement_date_value = str(settlement_date or "")
    if trade_date_value and (not stat["last_trade_date"] or trade_date_value > stat["last_trade_date"]):
        stat["last_trade_date"] = trade_date_value
    if settlement_date_value and (not stat["last_settlement_date"] or settlement_date_value > stat["last_settlement_date"]):
        stat["last_settlement_date"] = settlement_date_value


def _feedback_profile_from_stat(*, scope: str, key: str, stat: dict[str, Any]) -> dict[str, Any]:
    change_count = int(stat.get("change_count") or 0)
    hit_score_count = int(stat.get("hit_score_count") or 0)
    return _build_feedback_profile(
        scope=scope,
        key=key,
        sample_count=int(stat.get("sample_count") or 0),
        hit_count=int(stat.get("hit_count") or 0),
        miss_count=int(stat.get("miss_count") or 0),
        average_change_pct=(float(stat.get("change_sum") or 0.0) / change_count) if change_count else None,
        average_hit_score=(float(stat.get("hit_score_sum") or 0.0) / hit_score_count) if hit_score_count else None,
        last_trade_date=stat.get("last_trade_date"),
        last_settlement_date=stat.get("last_settlement_date"),
    )


def _compute_symbol_feedback_profiles(db: Session, *, symbols: list[str]) -> list[dict[str, Any]]:
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if str(symbol or "").strip()})
    if not normalized_symbols:
        return []
    stats: dict[str, dict[str, Any]] = defaultdict(_new_feedback_stat)
    settlement_rows = db.execute(
        text(
            """
            SELECT symbol,
                   outcome,
                   change_pct,
                   hit_score,
                   trade_date,
                   settlement_date
            FROM catalyst_selection_settlements
            WHERE symbol IN :symbols
            """
        ).bindparams(bindparam("symbols", expanding=True)),
        {"symbols": normalized_symbols},
    ).mappings().all()
    for row in settlement_rows:
        _add_feedback_stat(
            stats,
            _normalize_symbol(row["symbol"]),
            outcome=row.get("outcome"),
            change_pct=row.get("change_pct"),
            hit_score=row.get("hit_score"),
            trade_date=row.get("trade_date"),
            settlement_date=row.get("settlement_date"),
        )
    realtime_rows = db.execute(
        text(
            """
            SELECT symbol, outcome, change_pct, hit_score, trade_date
            FROM catalyst_selection_realtime_feedback
            WHERE symbol IN :symbols
              AND symbol_feedback = TRUE
            """
        ).bindparams(bindparam("symbols", expanding=True)),
        {"symbols": normalized_symbols},
    ).mappings().all()
    for row in realtime_rows:
        _add_feedback_stat(
            stats,
            _normalize_symbol(row["symbol"]),
            outcome=row.get("outcome"),
            change_pct=row.get("change_pct"),
            hit_score=row.get("hit_score"),
            trade_date=row.get("trade_date"),
        )
    return [
        _feedback_profile_from_stat(
            scope="symbol",
            key=symbol,
            stat=stat,
        )
        for symbol, stat in stats.items()
    ]


def _compute_theme_feedback_profiles(db: Session, *, themes: list[str]) -> list[dict[str, Any]]:
    target_themes = {str(theme or "").strip() for theme in themes if str(theme or "").strip()}
    if not target_themes:
        return []
    rows = db.execute(
        text(
            """
            SELECT i.theme_matches_json, s.outcome, s.change_pct, s.hit_score, s.trade_date, s.settlement_date
            FROM catalyst_selection_items i
            JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.theme_matches_json IS NOT NULL
            """
        )
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = defaultdict(_new_feedback_stat)
    for row in rows:
        theme_matches = _loads(row.get("theme_matches_json"), [])
        seen_themes: set[str] = set()
        for match in theme_matches or []:
            if not isinstance(match, dict):
                continue
            theme = str(match.get("theme") or "").strip()
            if not theme or theme not in target_themes or theme in seen_themes:
                continue
            seen_themes.add(theme)
            _add_feedback_stat(
                stats,
                theme,
                outcome=row.get("outcome"),
                change_pct=row.get("change_pct"),
                hit_score=row.get("hit_score"),
                trade_date=row.get("trade_date"),
                settlement_date=row.get("settlement_date"),
            )

    realtime_rows = db.execute(
        text(
            """
            SELECT themes_json, outcome, change_pct, hit_score, trade_date
            FROM catalyst_selection_realtime_feedback
            WHERE symbol_feedback = TRUE
              AND themes_json IS NOT NULL
            """
        )
    ).mappings().all()
    for row in realtime_rows:
        seen_themes: set[str] = set()
        for theme in _loads(row.get("themes_json"), []):
            normalized_theme = str(theme or "").strip()
            if not normalized_theme or normalized_theme not in target_themes or normalized_theme in seen_themes:
                continue
            seen_themes.add(normalized_theme)
            _add_feedback_stat(
                stats,
                normalized_theme,
                outcome=row.get("outcome"),
                change_pct=row.get("change_pct"),
                hit_score=row.get("hit_score"),
                trade_date=row.get("trade_date"),
            )

    profiles: list[dict[str, Any]] = []
    for theme, stat in stats.items():
        profiles.append(
            _feedback_profile_from_stat(
                scope="theme",
                key=theme,
                stat=stat,
            )
        )
    return profiles


def _compute_event_type_feedback_profiles(db: Session, *, event_types: list[str]) -> list[dict[str, Any]]:
    target_event_types = {
        _normalize_event_type(event_type)
        for event_type in event_types
        if _normalize_event_type(event_type)
    }
    if not target_event_types:
        return []
    rows = db.execute(
        text(
            """
            SELECT i.theme_matches_json, s.outcome, s.change_pct, s.hit_score, s.trade_date, s.settlement_date
            FROM catalyst_selection_items i
            JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.theme_matches_json IS NOT NULL
            """
        )
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = defaultdict(_new_feedback_stat)
    for row in rows:
        theme_matches = _loads(row.get("theme_matches_json"), [])
        seen_event_types: set[str] = set()
        for match in theme_matches or []:
            if not isinstance(match, dict):
                continue
            event_type = _event_type_from_match(match)
            if not event_type or event_type not in target_event_types or event_type in seen_event_types:
                continue
            seen_event_types.add(event_type)
            _add_feedback_stat(
                stats,
                event_type,
                outcome=row.get("outcome"),
                change_pct=row.get("change_pct"),
                hit_score=row.get("hit_score"),
                trade_date=row.get("trade_date"),
                settlement_date=row.get("settlement_date"),
            )

    realtime_rows = db.execute(
        text(
            """
            SELECT event_types_json, outcome, change_pct, hit_score, trade_date
            FROM catalyst_selection_realtime_feedback
            WHERE symbol_feedback = TRUE
              AND event_types_json IS NOT NULL
            """
        )
    ).mappings().all()
    for row in realtime_rows:
        seen_event_types: set[str] = set()
        for raw_event_type in _loads(row.get("event_types_json"), []):
            event_type = _normalize_event_type(raw_event_type)
            if not event_type or event_type not in target_event_types or event_type in seen_event_types:
                continue
            seen_event_types.add(event_type)
            _add_feedback_stat(
                stats,
                event_type,
                outcome=row.get("outcome"),
                change_pct=row.get("change_pct"),
                hit_score=row.get("hit_score"),
                trade_date=row.get("trade_date"),
            )

    profiles: list[dict[str, Any]] = []
    for event_type, stat in stats.items():
        profiles.append(
            _feedback_profile_from_stat(
                scope="event_type",
                key=event_type,
                stat=stat,
            )
        )
    return profiles


def _compute_risk_gate_feedback_profiles(db: Session, *, risk_gates: list[str]) -> list[dict[str, Any]]:
    target_gates = {
        str(gate or "").strip()
        for gate in risk_gates
        if str(gate or "").strip()
    }
    if not target_gates:
        return []
    rows = db.execute(
        text(
            """
            SELECT i.risk_control_json,
                   s.outcome,
                   s.change_pct,
                   s.hit_score,
                   s.trade_date,
                   s.settlement_date
            FROM catalyst_selection_items i
            JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.risk_control_json IS NOT NULL
            """
        )
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "sample_count": 0,
            "favorable_count": 0,
            "adverse_count": 0,
            "protection_count": 0,
            "opportunity_cost_count": 0,
            "raw_change_sum": 0.0,
            "raw_change_count": 0,
            "effective_change_sum": 0.0,
            "effective_change_count": 0,
            "raw_hit_score_sum": 0.0,
            "raw_hit_score_count": 0,
            "effective_hit_score_sum": 0.0,
            "effective_hit_score_count": 0,
            "last_trade_date": None,
            "last_settlement_date": None,
        }
    )
    for row in rows:
        risk_control = _loads(row.get("risk_control_json"), {})
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control, dict) else {}
        if not isinstance(monitor, dict):
            continue
        gate = str(monitor.get("execution_gate") or "").strip()
        if not gate or gate not in target_gates:
            continue
        outcome = str(row.get("outcome") or "").strip()
        if outcome not in POSITIVE_SETTLEMENT_OUTCOMES and outcome not in NEGATIVE_SETTLEMENT_OUTCOMES:
            continue
        protective_gate = gate in PROTECTIVE_RISK_GATES
        favorable = outcome in NEGATIVE_SETTLEMENT_OUTCOMES if protective_gate else outcome in POSITIVE_SETTLEMENT_OUTCOMES
        stat = stats[gate]
        stat["sample_count"] += 1
        if favorable:
            stat["favorable_count"] += 1
        else:
            stat["adverse_count"] += 1
        if protective_gate and outcome in NEGATIVE_SETTLEMENT_OUTCOMES:
            stat["protection_count"] += 1
        if protective_gate and outcome in POSITIVE_SETTLEMENT_OUTCOMES:
            stat["opportunity_cost_count"] += 1
        raw_change = _num(row.get("change_pct"))
        if raw_change is not None:
            stat["raw_change_sum"] += raw_change
            stat["raw_change_count"] += 1
            effective_change = -raw_change if protective_gate else raw_change
            stat["effective_change_sum"] += effective_change
            stat["effective_change_count"] += 1
        raw_hit_score = _num(row.get("hit_score"))
        if raw_hit_score is not None:
            stat["raw_hit_score_sum"] += raw_hit_score
            stat["raw_hit_score_count"] += 1
            effective_hit_score = 100.0 - raw_hit_score if protective_gate else raw_hit_score
            stat["effective_hit_score_sum"] += effective_hit_score
            stat["effective_hit_score_count"] += 1
        trade_date = str(row.get("trade_date") or "")
        settlement_date = str(row.get("settlement_date") or "")
        if trade_date and (not stat["last_trade_date"] or trade_date > stat["last_trade_date"]):
            stat["last_trade_date"] = trade_date
        if settlement_date and (not stat["last_settlement_date"] or settlement_date > stat["last_settlement_date"]):
            stat["last_settlement_date"] = settlement_date

    realtime_rows = db.execute(
        text(
            """
            SELECT risk_gate, event_type, outcome, change_pct, hit_score, risk_favorable, trade_date
            FROM catalyst_selection_realtime_feedback
            WHERE risk_feedback = TRUE
              AND risk_gate IN :risk_gates
            """
        ).bindparams(bindparam("risk_gates", expanding=True)),
        {"risk_gates": sorted(target_gates)},
    ).mappings().all()
    for row in realtime_rows:
        gate = str(row.get("risk_gate") or "").strip()
        if not gate or gate not in target_gates:
            continue
        favorable = row.get("risk_favorable")
        if favorable is None:
            outcome = str(row.get("outcome") or "").strip()
            if outcome not in POSITIVE_SETTLEMENT_OUTCOMES and outcome not in NEGATIVE_SETTLEMENT_OUTCOMES:
                continue
            protective_gate = gate in PROTECTIVE_RISK_GATES
            favorable = outcome in NEGATIVE_SETTLEMENT_OUTCOMES if protective_gate else outcome in POSITIVE_SETTLEMENT_OUTCOMES
        favorable = bool(favorable)
        event_type = str(row.get("event_type") or "").strip()
        protective_gate = gate in PROTECTIVE_RISK_GATES
        stat = stats[gate]
        stat["sample_count"] += 1
        if favorable:
            stat["favorable_count"] += 1
        else:
            stat["adverse_count"] += 1
        if protective_gate and favorable:
            stat["protection_count"] += 1
        if protective_gate and not favorable:
            stat["opportunity_cost_count"] += 1
        raw_change = _num(row.get("change_pct"))
        if raw_change is not None:
            stat["raw_change_sum"] += raw_change
            stat["raw_change_count"] += 1
            stat["effective_change_sum"] += abs(raw_change) if favorable else -abs(raw_change)
            stat["effective_change_count"] += 1
        raw_hit_score = _num(row.get("hit_score"))
        if raw_hit_score is not None:
            stat["raw_hit_score_sum"] += raw_hit_score
            stat["raw_hit_score_count"] += 1
            effective_hit_score = raw_hit_score
            if event_type in {"signal_blocked", "order_rejected", "order_error"} and favorable:
                effective_hit_score = 100.0 - raw_hit_score
            elif not favorable:
                effective_hit_score = min(raw_hit_score, 100.0 - raw_hit_score)
            stat["effective_hit_score_sum"] += effective_hit_score
            stat["effective_hit_score_count"] += 1
        trade_date = str(row.get("trade_date") or "")
        if trade_date and (not stat["last_trade_date"] or trade_date > stat["last_trade_date"]):
            stat["last_trade_date"] = trade_date

    profiles: list[dict[str, Any]] = []
    for gate, stat in stats.items():
        sample_count = int(stat.get("sample_count") or 0)
        raw_change_count = int(stat.get("raw_change_count") or 0)
        raw_hit_score_count = int(stat.get("raw_hit_score_count") or 0)
        effective_change_count = int(stat.get("effective_change_count") or 0)
        effective_hit_score_count = int(stat.get("effective_hit_score_count") or 0)
        protective_gate = gate in PROTECTIVE_RISK_GATES
        profile = _build_feedback_profile(
            scope="risk_gate",
            key=gate,
            sample_count=sample_count,
            hit_count=int(stat.get("favorable_count") or 0),
            miss_count=int(stat.get("adverse_count") or 0),
            average_change_pct=(float(stat.get("effective_change_sum") or 0.0) / effective_change_count) if effective_change_count else None,
            average_hit_score=(float(stat.get("effective_hit_score_sum") or 0.0) / effective_hit_score_count) if effective_hit_score_count else None,
            last_trade_date=stat.get("last_trade_date"),
            last_settlement_date=stat.get("last_settlement_date"),
        )
        profile["feature_snapshot"] = {
            **(profile.get("feature_snapshot") or {}),
            "profile_kind": "risk_gate",
            "gate_policy": "protective" if protective_gate else "permissive",
            "favorable_count": int(stat.get("favorable_count") or 0),
            "adverse_count": int(stat.get("adverse_count") or 0),
            "favorable_rate": (float(stat.get("favorable_count") or 0) / sample_count) if sample_count else None,
            "protection_count": int(stat.get("protection_count") or 0),
            "opportunity_cost_count": int(stat.get("opportunity_cost_count") or 0),
            "raw_average_change_pct": (float(stat.get("raw_change_sum") or 0.0) / raw_change_count) if raw_change_count else None,
            "raw_average_hit_score": (float(stat.get("raw_hit_score_sum") or 0.0) / raw_hit_score_count) if raw_hit_score_count else None,
            "effective_average_change_pct": profile.get("average_change_pct"),
            "effective_average_hit_score": profile.get("average_hit_score"),
        }
        profiles.append(profile)
    return profiles


def _compute_intraday_pulse_feedback_profiles(db: Session, *, pulse_statuses: list[str]) -> list[dict[str, Any]]:
    target_statuses = {
        str(status or "").strip()
        for status in pulse_statuses
        if str(status or "").strip()
    }
    if not target_statuses:
        return []
    rows = db.execute(
        text(
            """
            SELECT i.closed_loop_trace_json,
                   s.outcome,
                   s.change_pct,
                   s.hit_score,
                   s.trade_date,
                   s.settlement_date
            FROM catalyst_selection_items i
            JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.closed_loop_trace_json IS NOT NULL
            """
        )
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = defaultdict(_new_feedback_stat)
    for row in rows:
        trace = _loads(row.get("closed_loop_trace_json"), {})
        market = trace.get("market") if isinstance(trace, dict) else {}
        pulse = market.get("intraday_event_pulse") if isinstance(market, dict) else {}
        status = str((pulse or {}).get("status") or "").strip()
        if not status or status not in target_statuses:
            continue
        _add_feedback_stat(
            stats,
            status,
            outcome=row.get("outcome"),
            change_pct=row.get("change_pct"),
            hit_score=row.get("hit_score"),
            trade_date=row.get("trade_date"),
            settlement_date=row.get("settlement_date"),
        )
    profiles: list[dict[str, Any]] = []
    for status, stat in stats.items():
        profile = _feedback_profile_from_stat(scope="intraday_pulse", key=status, stat=stat)
        profile["feature_snapshot"] = {
            **(profile.get("feature_snapshot") or {}),
            "profile_kind": "intraday_pulse",
            "pulse_status": status,
        }
        profiles.append(profile)
    return profiles


def _build_feedback_profile(
    *,
    scope: str,
    key: str,
    sample_count: int,
    hit_count: int,
    miss_count: int,
    average_change_pct: float | None,
    average_hit_score: float | None,
    last_trade_date: str | None,
    last_settlement_date: str | None,
) -> dict[str, Any]:
    hit_rate = hit_count / sample_count if sample_count else None
    confidence = min(max(sample_count / 8.0, 0.0), 1.0)
    change_component = max(min(average_change_pct if average_change_pct is not None else 0.0, 8.0), -8.0) * 2.2
    hit_rate_component = ((hit_rate if hit_rate is not None else 0.5) - 0.5) * 48.0
    hit_score_component = ((average_hit_score if average_hit_score is not None else 50.0) - 50.0) * 0.22
    raw_score = 50.0 + change_component + hit_rate_component + hit_score_component
    learned_score = 50.0 + (raw_score - 50.0) * confidence
    feature_snapshot = {
        "model_version": FEEDBACK_MODEL_VERSION,
        "hit_rate": hit_rate,
        "average_change_pct": average_change_pct,
        "average_hit_score": average_hit_score,
        "sample_count": sample_count,
        "hit_count": hit_count,
        "miss_count": miss_count,
    }
    return {
        "profile_scope": scope,
        "profile_key": key,
        "model_version": FEEDBACK_MODEL_VERSION,
        "sample_count": sample_count,
        "hit_count": hit_count,
        "miss_count": miss_count,
        "average_change_pct": average_change_pct,
        "average_hit_score": average_hit_score,
        "hit_rate": hit_rate,
        "learned_score": max(0.0, min(learned_score, 100.0)),
        "confidence": confidence,
        "last_trade_date": last_trade_date,
        "last_settlement_date": last_settlement_date,
        "feature_snapshot": feature_snapshot,
    }


def _upsert_feedback_profile(db: Session, *, profile: dict[str, Any], now_value: datetime) -> None:
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_feedback_profiles (
                profile_scope, profile_key, model_version, sample_count, hit_count, miss_count,
                average_change_pct, average_hit_score, hit_rate, learned_score, confidence,
                last_trade_date, last_settlement_date, feature_snapshot_json, updated_at
            )
            VALUES (
                :profile_scope, :profile_key, :model_version, :sample_count, :hit_count, :miss_count,
                :average_change_pct, :average_hit_score, :hit_rate, :learned_score, :confidence,
                :last_trade_date, :last_settlement_date, :feature_snapshot_json, :updated_at
            )
            ON CONFLICT (profile_scope, profile_key) DO UPDATE SET
                model_version = EXCLUDED.model_version,
                sample_count = EXCLUDED.sample_count,
                hit_count = EXCLUDED.hit_count,
                miss_count = EXCLUDED.miss_count,
                average_change_pct = EXCLUDED.average_change_pct,
                average_hit_score = EXCLUDED.average_hit_score,
                hit_rate = EXCLUDED.hit_rate,
                learned_score = EXCLUDED.learned_score,
                confidence = EXCLUDED.confidence,
                last_trade_date = EXCLUDED.last_trade_date,
                last_settlement_date = EXCLUDED.last_settlement_date,
                feature_snapshot_json = EXCLUDED.feature_snapshot_json,
                updated_at = EXCLUDED.updated_at
            """
        ),
        {
            **profile,
            "feature_snapshot_json": json.dumps(profile.get("feature_snapshot") or {}, ensure_ascii=False, default=str),
            "updated_at": now_value,
        },
    )


def _load_feedback_profiles(
    db: Session,
    *,
    symbols: list[str],
    themes: list[str],
    event_types: list[str] | None = None,
    risk_gates: list[str] | None = None,
    intraday_pulses: list[str] | None = None,
    as_of_trade_date: str | None = None,
) -> dict[str, Any]:
    symbol_keys = sorted({_normalize_symbol(symbol) for symbol in symbols if str(symbol or "").strip()})
    theme_keys = sorted({str(theme or "").strip() for theme in themes if str(theme or "").strip()})
    event_type_keys = sorted({_normalize_event_type(event_type) for event_type in (event_types or []) if _normalize_event_type(event_type)})
    risk_gate_keys = sorted({str(gate or "").strip() for gate in (risk_gates or []) if str(gate or "").strip()})
    intraday_pulse_keys = sorted({str(status or "").strip() for status in (intraday_pulses or []) if str(status or "").strip()})
    result = {"symbols": {}, "themes": {}, "event_types": {}, "risk_gates": {}, "intraday_pulses": {}, "profile_count": 0, "sample_count": 0, "latest_updated_at": None}

    def _load(scope: str, keys: list[str]) -> dict[str, dict[str, Any]]:
        if not keys:
            return {}
        rows = db.execute(
            text(
                """
                SELECT *
                FROM catalyst_selection_feedback_profiles
                WHERE profile_scope = :scope
                  AND profile_key IN :keys
                  AND model_version = :model_version
                """
            ).bindparams(bindparam("keys", expanding=True)),
            {"scope": scope, "keys": keys, "model_version": FEEDBACK_MODEL_VERSION},
        ).mappings().all()
        return {str(row["profile_key"]): _row_to_feedback_profile(row, as_of_trade_date=as_of_trade_date) for row in rows}

    symbol_profiles = _load("symbol", symbol_keys)
    theme_profiles = _load("theme", theme_keys)
    event_type_profiles = _load("event_type", event_type_keys)
    risk_gate_profiles = _load("risk_gate", risk_gate_keys)
    intraday_pulse_profiles = _load("intraday_pulse", intraday_pulse_keys)
    all_profiles = [*symbol_profiles.values(), *theme_profiles.values(), *event_type_profiles.values(), *risk_gate_profiles.values(), *intraday_pulse_profiles.values()]
    latest_updated_at = None
    for profile in all_profiles:
        result["sample_count"] = int(result["sample_count"]) + int(profile.get("sample_count") or 0)
        updated_at = profile.get("updated_at")
        if updated_at and (latest_updated_at is None or str(updated_at) > str(latest_updated_at)):
            latest_updated_at = updated_at
    result["symbols"] = symbol_profiles
    result["themes"] = theme_profiles
    result["event_types"] = event_type_profiles
    result["risk_gates"] = risk_gate_profiles
    result["intraday_pulses"] = intraday_pulse_profiles
    result["profile_count"] = len(all_profiles)
    result["latest_updated_at"] = latest_updated_at
    return result


def _row_to_feedback_profile(row: Any, *, as_of_trade_date: str | None = None) -> dict[str, Any]:
    base_confidence = _num(row["confidence"]) or 0.0
    recency = _feedback_recency_adjustment(
        last_trade_date=str(row["last_trade_date"] or "") or None,
        last_settlement_date=str(row["last_settlement_date"] or "") or None,
        as_of_trade_date=as_of_trade_date,
    )
    effective_confidence = base_confidence * float(recency.get("recency_weight") or 1.0)
    return {
        "profile_scope": row["profile_scope"],
        "profile_key": row["profile_key"],
        "model_version": row["model_version"],
        "sample_count": int(row["sample_count"] or 0),
        "hit_count": int(row["hit_count"] or 0),
        "miss_count": int(row["miss_count"] or 0),
        "average_change_pct": _num(row["average_change_pct"]),
        "average_hit_score": _num(row["average_hit_score"]),
        "hit_rate": _num(row["hit_rate"]),
        "learned_score": _num(row["learned_score"]) or 50.0,
        "base_confidence": round(base_confidence, 4),
        "confidence": round(effective_confidence, 4),
        "recency_weight": recency.get("recency_weight"),
        "recency_days": recency.get("recency_days"),
        "recency_source_date": recency.get("recency_source_date"),
        "is_recency_decayed": bool(recency.get("is_recency_decayed")),
        "last_trade_date": row["last_trade_date"],
        "last_settlement_date": row["last_settlement_date"],
        "feature_snapshot": _loads(row["feature_snapshot_json"], {}),
        "updated_at": _iso(row["updated_at"]),
    }


def _feedback_recency_adjustment(
    *,
    last_trade_date: str | None,
    last_settlement_date: str | None,
    as_of_trade_date: str | None,
) -> dict[str, Any]:
    if not as_of_trade_date:
        return {
            "recency_weight": 1.0,
            "recency_days": None,
            "recency_source_date": last_settlement_date or last_trade_date,
            "is_recency_decayed": False,
        }
    try:
        as_of = date.fromisoformat(str(as_of_trade_date)[:10])
    except Exception:
        return {
            "recency_weight": 1.0,
            "recency_days": None,
            "recency_source_date": last_settlement_date or last_trade_date,
            "is_recency_decayed": False,
        }
    source_date = last_settlement_date or last_trade_date
    try:
        source = date.fromisoformat(str(source_date)[:10]) if source_date else None
    except Exception:
        source = None
    if source is None:
        return {
            "recency_weight": 1.0,
            "recency_days": None,
            "recency_source_date": None,
            "is_recency_decayed": False,
        }
    recency_days = max((as_of - source).days, 0)
    if recency_days <= 0:
        weight = 1.0
    else:
        weight = max(FEEDBACK_RECENCY_MIN_WEIGHT, 0.5 ** (recency_days / FEEDBACK_RECENCY_HALF_LIFE_DAYS))
    return {
        "recency_weight": round(float(weight), 4),
        "recency_days": recency_days,
        "recency_source_date": source.isoformat(),
        "is_recency_decayed": recency_days > 0 and weight < 0.999,
    }


def _merge_symbol_feedback_profiles(
    history_stats: dict[str, dict[str, Any]],
    symbol_profiles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {symbol: dict(stats) for symbol, stats in history_stats.items()}
    for symbol, profile in symbol_profiles.items():
        key = _normalize_symbol(symbol)
        stats = merged.setdefault(key, {})
        stats.update(
            {
                "learned_score": profile.get("learned_score"),
                "confidence": profile.get("confidence"),
                "base_confidence": profile.get("base_confidence"),
                "recency_weight": profile.get("recency_weight"),
                "recency_days": profile.get("recency_days"),
                "recency_source_date": profile.get("recency_source_date"),
                "is_recency_decayed": profile.get("is_recency_decayed"),
                "profile_sample_count": profile.get("sample_count"),
                "profile_updated_at": profile.get("updated_at"),
            }
        )
        if not stats.get("count"):
            stats["count"] = profile.get("sample_count") or 0
            stats["hit_count"] = profile.get("hit_count") or 0
            stats["loss_count"] = profile.get("miss_count") or 0
            stats["hit_rate"] = profile.get("hit_rate")
    return merged


def _merge_theme_feedback_profiles(
    theme_feedback: dict[str, dict[str, Any]],
    theme_profiles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    merged = {theme: dict(stats) for theme, stats in theme_feedback.items()}
    for theme, profile in theme_profiles.items():
        stats = merged.setdefault(theme, {})
        stats.update(
            {
                "learned_score": profile.get("learned_score"),
                "confidence": profile.get("confidence"),
                "base_confidence": profile.get("base_confidence"),
                "recency_weight": profile.get("recency_weight"),
                "recency_days": profile.get("recency_days"),
                "recency_source_date": profile.get("recency_source_date"),
                "is_recency_decayed": profile.get("is_recency_decayed"),
                "profile_sample_count": profile.get("sample_count"),
                "profile_updated_at": profile.get("updated_at"),
            }
        )
        if not stats.get("count"):
            stats["count"] = profile.get("sample_count") or 0
            stats["hit_count"] = profile.get("hit_count") or 0
            stats["loss_count"] = profile.get("miss_count") or 0
            stats["hit_rate"] = profile.get("hit_rate")
            stats["average_change_pct"] = profile.get("average_change_pct")
    return merged


def _attach_event_feedback_profiles(
    theme_items: list[dict[str, Any]],
    event_type_profiles: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not event_type_profiles:
        return theme_items
    attached: list[dict[str, Any]] = []
    for item in theme_items:
        event_type = _event_type_from_match(item)
        profile = event_type_profiles.get(event_type) if event_type else None
        if profile:
            attached.append({**item, "event_feedback_profile": profile})
        else:
            attached.append(item)
    return attached


def _themes_from_selection_items(items: list[dict[str, Any]]) -> list[str]:
    themes: list[str] = []
    seen: set[str] = set()
    for item in items:
        for match in item.get("theme_matches") or []:
            if not isinstance(match, dict):
                continue
            theme = str(match.get("theme") or "").strip()
            if theme and theme not in seen:
                seen.add(theme)
                themes.append(theme)
    return themes


def _event_type_keys_from_theme_items(theme_items: list[dict[str, Any]]) -> list[str]:
    event_types: list[str] = []
    seen: set[str] = set()
    for item in theme_items:
        event_type = _event_type_from_match(item)
        if event_type and event_type not in seen:
            seen.add(event_type)
            event_types.append(event_type)
    return event_types


def _event_types_from_selection_items(items: list[dict[str, Any]]) -> list[str]:
    event_types: list[str] = []
    seen: set[str] = set()
    for item in items:
        for match in item.get("theme_matches") or []:
            if not isinstance(match, dict):
                continue
            event_type = _event_type_from_match(match)
            if event_type and event_type not in seen:
                seen.add(event_type)
                event_types.append(event_type)
    return event_types


def _event_type_from_match(match: dict[str, Any]) -> str:
    semantic = match.get("event_semantic") if isinstance(match.get("event_semantic"), dict) else {}
    return _normalize_event_type(semantic.get("event_type"))


def _normalize_event_type(value: Any) -> str:
    return str(value or "").strip()[:40]


def _public_feedback_profile(profile: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(profile, dict) or not profile:
        return {}
    return {
        "profile_scope": profile.get("profile_scope"),
        "profile_key": profile.get("profile_key"),
        "sample_count": int(profile.get("sample_count") or 0),
        "hit_rate": _round_or_none(profile.get("hit_rate"), 4),
        "average_change_pct": _round_or_none(profile.get("average_change_pct"), 4),
        "learned_score": _round_or_none(profile.get("learned_score"), 2),
        "confidence": _round_or_none(profile.get("confidence"), 4),
        "base_confidence": _round_or_none(profile.get("base_confidence"), 4),
        "recency_weight": _round_or_none(profile.get("recency_weight"), 4),
        "recency_days": int(profile.get("recency_days")) if profile.get("recency_days") is not None else None,
        "recency_source_date": profile.get("recency_source_date"),
        "is_recency_decayed": bool(profile.get("is_recency_decayed")),
        "updated_at": profile.get("updated_at"),
    }


def _public_risk_gate_feedback_profile(profile: dict[str, Any]) -> dict[str, Any]:
    base = _public_feedback_profile(profile)
    if not base:
        return {}
    snapshot = profile.get("feature_snapshot") if isinstance(profile.get("feature_snapshot"), dict) else {}
    base.update(
        {
            "gate_policy": snapshot.get("gate_policy"),
            "favorable_count": int(snapshot.get("favorable_count") or profile.get("hit_count") or 0),
            "adverse_count": int(snapshot.get("adverse_count") or profile.get("miss_count") or 0),
            "favorable_rate": _round_or_none(snapshot.get("favorable_rate") if snapshot.get("favorable_rate") is not None else profile.get("hit_rate"), 4),
            "protection_count": int(snapshot.get("protection_count") or 0),
            "opportunity_cost_count": int(snapshot.get("opportunity_cost_count") or 0),
            "raw_average_change_pct": _round_or_none(snapshot.get("raw_average_change_pct"), 4),
            "raw_average_hit_score": _round_or_none(snapshot.get("raw_average_hit_score"), 4),
            "effective_average_change_pct": _round_or_none(snapshot.get("effective_average_change_pct") if snapshot.get("effective_average_change_pct") is not None else profile.get("average_change_pct"), 4),
            "effective_average_hit_score": _round_or_none(snapshot.get("effective_average_hit_score") if snapshot.get("effective_average_hit_score") is not None else profile.get("average_hit_score"), 4),
        }
    )
    return base


def _public_scoring_feedback_profile(scope: str, key: str, stats: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(stats, dict) or not str(key or "").strip():
        return {}
    sample_count = int(stats.get("sample_count") or stats.get("count") or stats.get("profile_sample_count") or 0)
    hit_count = int(stats.get("hit_count") or 0)
    miss_count = int(stats.get("miss_count") or stats.get("loss_count") or 0)
    hit_rate = _num(stats.get("hit_rate"))
    if hit_rate is None and sample_count > 0:
        hit_rate = hit_count / sample_count
    learned_score = _num(stats.get("learned_score"))
    confidence = _num(stats.get("confidence"))
    base_confidence = _num(stats.get("base_confidence"))
    average_change_pct = _num(stats.get("average_change_pct"))
    average_hit_score = _num(stats.get("average_hit_score"))
    if (
        sample_count <= 0
        and hit_rate is None
        and learned_score is None
        and confidence is None
        and average_change_pct is None
        and average_hit_score is None
    ):
        return {}
    return {
        "profile_scope": scope,
        "profile_key": str(key),
        "model_version": FEEDBACK_MODEL_VERSION,
        "sample_count": sample_count,
        "hit_count": hit_count,
        "miss_count": miss_count,
        "hit_rate": _round_or_none(hit_rate, 4),
        "average_change_pct": _round_or_none(average_change_pct, 4),
        "average_hit_score": _round_or_none(average_hit_score, 4),
        "learned_score": _round_or_none(learned_score, 2),
        "confidence": _round_or_none(confidence, 4),
        "base_confidence": _round_or_none(base_confidence, 4),
        "recency_weight": _round_or_none(stats.get("recency_weight"), 4),
        "recency_days": int(stats.get("recency_days")) if stats.get("recency_days") is not None else None,
        "recency_source_date": stats.get("recency_source_date"),
        "is_recency_decayed": bool(stats.get("is_recency_decayed")),
        "updated_at": stats.get("updated_at") or stats.get("profile_updated_at"),
    }


def _build_feedback_learning_state(feedback_profiles: dict[str, Any], selected: list[dict[str, Any]]) -> dict[str, Any]:
    symbol_profiles = feedback_profiles.get("symbols") if isinstance(feedback_profiles.get("symbols"), dict) else {}
    theme_profiles = feedback_profiles.get("themes") if isinstance(feedback_profiles.get("themes"), dict) else {}
    event_type_profiles = feedback_profiles.get("event_types") if isinstance(feedback_profiles.get("event_types"), dict) else {}
    risk_gate_profiles = feedback_profiles.get("risk_gates") if isinstance(feedback_profiles.get("risk_gates"), dict) else {}
    intraday_pulse_profiles = feedback_profiles.get("intraday_pulses") if isinstance(feedback_profiles.get("intraday_pulses"), dict) else {}
    all_profiles: list[dict[str, Any]] = []
    for bucket in (symbol_profiles, theme_profiles, event_type_profiles, intraday_pulse_profiles):
        for profile in bucket.values():
            public_profile = _public_feedback_profile(profile)
            if public_profile:
                all_profiles.append(public_profile)
    for profile in risk_gate_profiles.values():
        public_profile = _public_risk_gate_feedback_profile(profile)
        if public_profile:
            all_profiles.append(public_profile)

    def _profile_sort_key(profile: dict[str, Any], *, positive: bool) -> tuple[float, float, int]:
        learned_score = _num(profile.get("learned_score"))
        confidence = _num(profile.get("confidence")) or 0.0
        sample_count = int(profile.get("sample_count") or 0)
        score = learned_score if learned_score is not None else 50.0
        edge = score - 50.0
        effective_edge = max(edge, 0.0) * confidence if positive else max(-edge, 0.0) * confidence
        return (effective_edge, confidence, sample_count)

    positive_profiles = [
        profile for profile in all_profiles
        if (_num(profile.get("learned_score")) or 50.0) > 50.0 and (_num(profile.get("confidence")) or 0.0) > 0
    ]
    negative_profiles = [
        profile for profile in all_profiles
        if (_num(profile.get("learned_score")) or 50.0) < 50.0 and (_num(profile.get("confidence")) or 0.0) > 0
    ]
    positive_profiles.sort(key=lambda profile: _profile_sort_key(profile, positive=True), reverse=True)
    negative_profiles.sort(key=lambda profile: _profile_sort_key(profile, positive=False), reverse=True)

    selected_scores = [
        float(item.get("adaptive_feedback_score"))
        for item in selected
        if _num(item.get("adaptive_feedback_score")) is not None
    ]
    selected_with_feedback = 0
    for item in selected:
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        feedback = trace.get("feedback") if isinstance(trace.get("feedback"), dict) else {}
        profiles = [
            feedback.get("symbol_profile"),
            feedback.get("theme_profile"),
            feedback.get("event_type_profile"),
        ]
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        gate_feedback = monitor.get("gate_feedback") if isinstance(monitor.get("gate_feedback"), dict) else {}
        profiles.append(gate_feedback)
        if any(isinstance(profile, dict) and int(profile.get("sample_count") or 0) > 0 for profile in profiles):
            selected_with_feedback += 1

    recency_weights = [
        float(profile.get("recency_weight"))
        for profile in all_profiles
        if _num(profile.get("recency_weight")) is not None
    ]
    recency_days = [
        int(profile.get("recency_days"))
        for profile in all_profiles
        if profile.get("recency_days") is not None
    ]
    decayed_profiles = [
        profile for profile in all_profiles
        if bool(profile.get("is_recency_decayed"))
    ]

    return {
        "status": "active" if all_profiles else "warming_up",
        "model_version": FEEDBACK_MODEL_VERSION,
        "profile_count": int(feedback_profiles.get("profile_count") or len(all_profiles)),
        "sample_count": int(feedback_profiles.get("sample_count") or sum(int(profile.get("sample_count") or 0) for profile in all_profiles)),
        "latest_updated_at": feedback_profiles.get("latest_updated_at"),
        "symbol_profile_count": len(symbol_profiles),
        "theme_profile_count": len(theme_profiles),
        "event_type_profile_count": len(event_type_profiles),
        "risk_gate_profile_count": len(risk_gate_profiles),
        "intraday_pulse_profile_count": len(intraday_pulse_profiles),
        "selected_count": len(selected),
        "selected_with_feedback_count": selected_with_feedback,
        "selected_adaptive_feedback_avg": round(sum(selected_scores) / len(selected_scores), 2) if selected_scores else None,
        "top_positive_profiles": positive_profiles[:5],
        "top_negative_profiles": negative_profiles[:5],
        "recency": {
            "half_life_days": FEEDBACK_RECENCY_HALF_LIFE_DAYS,
            "min_weight": FEEDBACK_RECENCY_MIN_WEIGHT,
            "profile_count": len(all_profiles),
            "decayed_profile_count": len(decayed_profiles),
            "stale_profile_count": sum(1 for weight in recency_weights if weight < 0.5),
            "average_recency_weight": round(sum(recency_weights) / len(recency_weights), 4) if recency_weights else None,
            "max_recency_days": max(recency_days) if recency_days else None,
        },
    }


def _build_risk_control_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    action_counts: Counter[str] = Counter()
    risk_level_counts: Counter[str] = Counter()
    invalidation_count = 0
    max_position_values: list[float] = []
    stop_loss_values: list[float] = []
    for item in items:
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        action = str(risk_control.get("action") or "unknown").strip() or "unknown"
        risk_level = str(risk_control.get("risk_level") or "unknown").strip() or "unknown"
        action_counts[action] += 1
        risk_level_counts[risk_level] += 1
        invalidations = risk_control.get("invalidations") if isinstance(risk_control.get("invalidations"), list) else []
        invalidation_count += sum(1 for value in invalidations if str(value).strip())
        max_position = _num(risk_control.get("max_position_pct"))
        if max_position is not None:
            max_position_values.append(max_position)
        stop_loss = _num(risk_control.get("stop_loss_pct"))
        if stop_loss is not None:
            stop_loss_values.append(stop_loss)
    return {
        "item_count": len(items),
        "action_counts": dict(action_counts),
        "risk_level_counts": dict(risk_level_counts),
        "deploy_count": int(action_counts.get("deploy", 0)),
        "follow_count": int(action_counts.get("follow", 0)),
        "wait_count": int(action_counts.get("wait", 0)),
        "observe_count": int(action_counts.get("observe", 0)),
        "restricted_count": int(action_counts.get("wait", 0) + action_counts.get("observe", 0)),
        "invalidation_count": int(invalidation_count),
        "average_max_position_pct": round(sum(max_position_values) / len(max_position_values), 2) if max_position_values else None,
        "average_stop_loss_pct": round(sum(stop_loss_values) / len(stop_loss_values), 2) if stop_loss_values else None,
    }


def _build_risk_monitoring_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    gate_counts: Counter[str] = Counter()
    severity_counts: Counter[str] = Counter()
    trigger_count = 0
    blocked_symbols: list[dict[str, Any]] = []
    for item in items:
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        status = str(monitor.get("status") or "unknown").strip() or "unknown"
        gate = str(monitor.get("execution_gate") or "unknown").strip() or "unknown"
        severity = str(monitor.get("severity") or "unknown").strip() or "unknown"
        status_counts[status] += 1
        gate_counts[gate] += 1
        severity_counts[severity] += 1
        trigger_count += int(monitor.get("trigger_count") or 0)
        if gate in {"blocked", "reduce_only"} or status in {"blocked", "invalidated"}:
            blocked_symbols.append(
                {
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "status": status,
                    "execution_gate": gate,
                    "next_action": monitor.get("next_action"),
                    "severity": severity,
                }
            )
    return {
        "item_count": len(items),
        "status_counts": dict(status_counts),
        "gate_counts": dict(gate_counts),
        "severity_counts": dict(severity_counts),
        "allow_count": int(gate_counts.get("allow", 0) + gate_counts.get("allow_probe", 0)),
        "confirm_count": int(gate_counts.get("confirm", 0)),
        "blocked_count": int(gate_counts.get("blocked", 0)),
        "reduce_only_count": int(gate_counts.get("reduce_only", 0)),
        "invalidated_count": int(status_counts.get("invalidated", 0)),
        "pending_confirmation_count": int(status_counts.get("pending_confirmation", 0)),
        "trigger_count": trigger_count,
        "blocked_symbols": blocked_symbols[:8],
    }


def _build_risk_gate_feedback_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    influence_counts: Counter[str] = Counter()
    adjustment_counts: Counter[str] = Counter()
    profile_keys: set[str] = set()
    applied_count = 0
    overly_conservative_count = 0
    tightened_count = 0
    supportive_count = 0
    for item in items:
        risk_control = item.get("risk_control") if isinstance(item.get("risk_control"), dict) else {}
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        feedback = monitor.get("gate_feedback") if isinstance(monitor.get("gate_feedback"), dict) else {}
        if not feedback:
            continue
        profile_key = str(feedback.get("profile_key") or "").strip()
        if profile_key:
            profile_keys.add(profile_key)
        influence = str(feedback.get("influence") or "unknown").strip() or "unknown"
        adjustment = str(feedback.get("adjustment") or "none").strip() or "none"
        influence_counts[influence] += 1
        adjustment_counts[adjustment] += 1
        if feedback.get("applied"):
            applied_count += 1
        if feedback.get("overly_conservative"):
            overly_conservative_count += 1
        if influence == "tighten":
            tightened_count += 1
        if influence == "supportive":
            supportive_count += 1
    return {
        "item_count": len(items),
        "profile_count": len(profile_keys),
        "used_count": sum(influence_counts.values()),
        "applied_count": applied_count,
        "tightened_count": tightened_count,
        "supportive_count": supportive_count,
        "overly_conservative_count": overly_conservative_count,
        "influence_counts": dict(influence_counts),
        "adjustment_counts": dict(adjustment_counts),
    }


def _build_learning_adjustment_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    stance_counts: Counter[str] = Counter()
    active_count = 0
    learning_edges: list[float] = []
    for item in items:
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        scoring = trace.get("scoring") if isinstance(trace.get("scoring"), dict) else {}
        policy = scoring.get("learning_adjustment_policy") if isinstance(scoring.get("learning_adjustment_policy"), dict) else {}
        stance = str(policy.get("stance") or "neutral").strip() or "neutral"
        stance_counts[stance] += 1
        if policy.get("status") == "active":
            active_count += 1
        edge = _num(policy.get("learning_edge"))
        if edge is not None:
            learning_edges.append(edge)
    return {
        "item_count": len(items),
        "active_count": active_count,
        "stance_counts": dict(stance_counts),
        "tighten_count": int(stance_counts.get("tighten", 0)),
        "expand_count": int(stance_counts.get("expand", 0)),
        "neutral_count": int(stance_counts.get("neutral", 0)),
        "average_learning_edge": round(sum(learning_edges) / len(learning_edges), 2) if learning_edges else None,
    }


def _build_ai_quant_end_to_end_evidence(
    *,
    trigger_context: dict[str, Any] | None,
    selected: list[dict[str, Any]],
    opportunity_events: list[dict[str, Any]],
    llm_runtime: dict[str, Any],
    market_state_freshness: dict[str, Any],
    risk_control_summary: dict[str, Any],
    feedback_learning_state: dict[str, Any],
    learning_adjustment_summary: dict[str, Any],
    learning_impact_summary: dict[str, Any],
    realtime_feedback_summary: dict[str, Any],
) -> dict[str, Any]:
    context = trigger_context if isinstance(trigger_context, dict) else {}
    selected_count = len(selected)
    news_ingest = context.get("news_ingest") if isinstance(context.get("news_ingest"), dict) else {}
    fresh_event_count = _safe_int(context.get("fresh_event_count") or context.get("event_count"))
    included_event_count = _safe_int(context.get("included_count"))
    trigger_name = str(context.get("trigger") or "").strip()
    trigger_source = str(context.get("source") or "").strip()
    refresh_key = str(context.get("refresh_key") or "").strip()
    event_triggered = bool(
        trigger_name
        or trigger_source
        or refresh_key
        or fresh_event_count > 0
        or int(news_ingest.get("new") or 0) > 0
    )
    if event_triggered:
        discovery_mode = "event_driven"
    elif opportunity_events:
        discovery_mode = "selection_diff"
    elif selected_count:
        discovery_mode = "ranked_candidates"
    else:
        discovery_mode = "empty"
    llm_used_symbol_count = int(llm_runtime.get("used_symbol_theme_count") or 0) if isinstance(llm_runtime, dict) else 0
    llm_used_semantic_count = int(llm_runtime.get("used_semantic_theme_count") or 0) if isinstance(llm_runtime, dict) else 0
    llm_ready = bool(llm_runtime.get("ready")) if isinstance(llm_runtime, dict) else False
    llm_invoked = llm_ready and (llm_used_symbol_count > 0 or llm_used_semantic_count > 0)
    risk_action_counts = risk_control_summary.get("action_counts") if isinstance(risk_control_summary.get("action_counts"), dict) else {}
    risk_active_count = sum(int(risk_action_counts.get(key) or 0) for key in ("deploy", "follow", "wait", "observe"))
    feedback_profile_count = int(feedback_learning_state.get("profile_count") or 0)
    feedback_sample_count = int(feedback_learning_state.get("sample_count") or 0)
    realtime_sample_count = int(realtime_feedback_summary.get("sample_count") or 0) if isinstance(realtime_feedback_summary, dict) else 0
    learning_active_count = int(learning_adjustment_summary.get("active_count") or 0)
    learning_impact_active_count = int(learning_impact_summary.get("active_count") or 0)
    top_items = [
        {
            "rank": item.get("rank"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "score": item.get("score"),
            "risk_action": (item.get("risk_control") or {}).get("action") if isinstance(item.get("risk_control"), dict) else None,
            "adaptive_feedback_score": item.get("adaptive_feedback_score"),
        }
        for item in selected[:5]
    ]
    stages = [
        {
            "id": "proactive_opportunity_discovery",
            "label": "主动发现机会",
            "status": "active" if selected_count and (event_triggered or opportunity_events) else ("warming_up" if selected_count else "missing"),
            "metrics": {
                "opportunity_event_count": len(opportunity_events),
                "selected_count": selected_count,
                "discovery_mode": discovery_mode,
                "event_triggered": event_triggered,
                "trigger": trigger_name or None,
                "trigger_source": trigger_source or None,
                "refresh_key": refresh_key or None,
                "fresh_event_count": fresh_event_count,
                "included_event_count": included_event_count,
                "news_ingest_saved": _safe_int(news_ingest.get("saved")),
                "news_ingest_new": _safe_int(news_ingest.get("new")),
                "news_ingest_updated": _safe_int(news_ingest.get("updated")),
            },
        },
        {
            "id": "event_understanding",
            "label": "理解新闻/政策事件",
            "status": "active" if llm_invoked else ("degraded" if llm_ready else "missing"),
            "metrics": {
                "llm_ready": llm_ready,
                "used_symbol_theme_count": llm_used_symbol_count,
                "used_semantic_theme_count": llm_used_semantic_count,
                "provider": llm_runtime.get("provider") if isinstance(llm_runtime, dict) else None,
                "model": llm_runtime.get("model") if isinstance(llm_runtime, dict) else None,
                "runtime_package_source": llm_runtime.get("runtime_package_source") if isinstance(llm_runtime, dict) else None,
            },
        },
        {
            "id": "market_state_judgement",
            "label": "判断市场状态",
            "status": "active" if market_state_freshness else "missing",
            "metrics": {
                "freshness_status": market_state_freshness.get("status") if isinstance(market_state_freshness, dict) else None,
                "event_reaction_status": market_state_freshness.get("event_reaction_status") if isinstance(market_state_freshness, dict) else None,
            },
        },
        {
            "id": "dynamic_ranking",
            "label": "动态排序标的",
            "status": "active" if selected_count > 0 else "missing",
            "metrics": {
                "selected_count": selected_count,
                "top_items": top_items,
            },
        },
        {
            "id": "risk_control",
            "label": "控制风险",
            "status": "active" if risk_active_count > 0 else ("warming_up" if selected_count else "missing"),
            "metrics": {
                "action_counts": dict(risk_action_counts),
                "risk_level_counts": risk_control_summary.get("risk_level_counts") if isinstance(risk_control_summary, dict) else {},
                "average_max_position_pct": risk_control_summary.get("average_max_position_pct") if isinstance(risk_control_summary, dict) else None,
            },
        },
        {
            "id": "feedback_learning",
            "label": "结果反哺模型",
            "status": "active" if feedback_profile_count > 0 or realtime_sample_count > 0 else "warming_up",
            "metrics": {
                "profile_count": feedback_profile_count,
                "sample_count": feedback_sample_count,
                "realtime_sample_count": realtime_sample_count,
                "learning_adjustment_active_count": learning_active_count,
                "learning_impact_active_count": learning_impact_active_count,
            },
        },
    ]
    status_counts = Counter(str(stage.get("status") or "unknown") for stage in stages)
    active_like_count = int(status_counts.get("active", 0) + status_counts.get("warming_up", 0))
    overall_status = "active"
    if status_counts.get("missing", 0):
        overall_status = "incomplete"
    elif status_counts.get("degraded", 0) or status_counts.get("warming_up", 0):
        overall_status = "degraded"
    return {
        "status": overall_status,
        "active_count": int(status_counts.get("active", 0)),
        "warming_up_count": int(status_counts.get("warming_up", 0)),
        "degraded_count": int(status_counts.get("degraded", 0)),
        "missing_count": int(status_counts.get("missing", 0)),
        "active_like_count": active_like_count,
        "stage_count": len(stages),
        "pass_rate": round(active_like_count / len(stages), 4) if stages else 0.0,
        "trigger": context.get("trigger"),
        "refresh_key": context.get("refresh_key"),
        "trigger_source": context.get("source"),
        "triggered_at": context.get("triggered_at"),
        "stages": stages,
    }


def _build_event_refresh_end_to_end_evidence(
    *,
    trigger: str,
    windows: list[str],
    generated: list[dict[str, Any]],
    settlement_feedback_refresh: dict[str, Any],
    realtime_feedback_summary: dict[str, Any],
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    window_evidence = [
        item.get("end_to_end_evidence")
        for item in generated
        if isinstance(item, dict) and isinstance(item.get("end_to_end_evidence"), dict)
    ]
    stage_statuses: dict[str, Counter[str]] = {}
    for evidence in window_evidence:
        for stage in evidence.get("stages") or []:
            if not isinstance(stage, dict):
                continue
            stage_id = str(stage.get("id") or "").strip()
            if not stage_id:
                continue
            stage_statuses.setdefault(stage_id, Counter())[str(stage.get("status") or "unknown")] += 1
    stage_rollup = {
        stage_id: {
            "active": int(counts.get("active", 0)),
            "warming_up": int(counts.get("warming_up", 0)),
            "degraded": int(counts.get("degraded", 0)),
            "missing": int(counts.get("missing", 0)),
            "window_count": sum(counts.values()),
        }
        for stage_id, counts in sorted(stage_statuses.items())
    }
    active_window_count = sum(1 for evidence in window_evidence if evidence.get("status") == "active")
    degraded_window_count = sum(1 for evidence in window_evidence if evidence.get("status") == "degraded")
    incomplete_window_count = sum(1 for evidence in window_evidence if evidence.get("status") == "incomplete")
    feedback_updated_count = int(settlement_feedback_refresh.get("updated_profile_count") or 0)
    realtime_sample_count = int(realtime_feedback_summary.get("sample_count") or 0) if isinstance(realtime_feedback_summary, dict) else 0
    status = "active"
    if errors and not generated:
        status = "failed"
    elif incomplete_window_count:
        status = "incomplete"
    elif errors or degraded_window_count or not window_evidence:
        status = "degraded"
    return {
        "status": status,
        "trigger": trigger,
        "requested_windows": list(windows),
        "generated_window_count": len(generated),
        "failed_window_count": len(errors),
        "active_window_count": active_window_count,
        "degraded_window_count": degraded_window_count,
        "incomplete_window_count": incomplete_window_count,
        "stage_rollup": stage_rollup,
        "feedback_profile_updated_count": feedback_updated_count,
        "realtime_feedback_sample_count": realtime_sample_count,
        "window_evidence": window_evidence[:8],
    }


def _build_learning_impact_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: Counter[str] = Counter()
    score_deltas: list[float] = []
    rank_deltas: list[int] = []
    position_deltas: list[float] = []
    action_changed_count = 0
    gate_applied_count = 0
    score_changed_count = 0
    rank_changed_count = 0
    risk_changed_count = 0
    improved_rank_count = 0
    reduced_rank_count = 0
    for item in items:
        trace = item.get("closed_loop_trace") if isinstance(item.get("closed_loop_trace"), dict) else {}
        scoring = trace.get("scoring") if isinstance(trace.get("scoring"), dict) else {}
        impact = scoring.get("learning_impact") if isinstance(scoring.get("learning_impact"), dict) else {}
        if not impact:
            continue
        status_counts[str(impact.get("status") or "unknown")] += 1
        score_delta = _num(impact.get("score_delta_from_learning_policy"))
        if score_delta is not None:
            score_deltas.append(score_delta)
            if abs(score_delta) >= 0.01:
                score_changed_count += 1
        rank_delta = impact.get("rank_delta_from_learning_policy")
        if rank_delta is not None:
            try:
                rank_delta_value = int(rank_delta)
            except Exception:
                rank_delta_value = 0
            rank_deltas.append(rank_delta_value)
            if abs(rank_delta_value) >= 1:
                rank_changed_count += 1
            if rank_delta_value > 0:
                improved_rank_count += 1
            elif rank_delta_value < 0:
                reduced_rank_count += 1
        risk_effect = impact.get("risk_effect") if isinstance(impact.get("risk_effect"), dict) else {}
        if risk_effect.get("action_changed"):
            action_changed_count += 1
        position_delta = _num(risk_effect.get("max_position_delta_pct"))
        if position_delta is not None:
            position_deltas.append(position_delta)
        gate_effect = impact.get("risk_gate_effect") if isinstance(impact.get("risk_gate_effect"), dict) else {}
        if gate_effect.get("applied"):
            gate_applied_count += 1
        if risk_effect.get("action_changed") or abs(float(position_delta or 0.0)) >= 0.01 or gate_effect.get("applied"):
            risk_changed_count += 1
    return {
        "item_count": len(items),
        "status_counts": dict(status_counts),
        "active_count": int(status_counts.get("active", 0)),
        "score_changed_count": score_changed_count,
        "rank_changed_count": rank_changed_count,
        "risk_changed_count": risk_changed_count,
        "average_score_delta": round(sum(score_deltas) / len(score_deltas), 2) if score_deltas else None,
        "max_abs_score_delta": round(max((abs(value) for value in score_deltas), default=0.0), 2) if score_deltas else None,
        "average_rank_delta": round(sum(rank_deltas) / len(rank_deltas), 2) if rank_deltas else None,
        "improved_rank_count": improved_rank_count,
        "reduced_rank_count": reduced_rank_count,
        "action_changed_count": action_changed_count,
        "gate_applied_count": gate_applied_count,
        "average_max_position_delta_pct": round(sum(position_deltas) / len(position_deltas), 2) if position_deltas else None,
    }


def _load_theme_settlement_stats(db: Session, *, trade_date: str) -> dict[str, dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT i.theme_matches_json, s.outcome, s.change_pct
            FROM catalyst_selection_items i
            JOIN catalyst_selection_settlements s
              ON s.trade_date = i.trade_date AND s.symbol = i.symbol
            WHERE i.trade_date < :trade_date
              AND i.theme_matches_json IS NOT NULL
            """
        ),
        {"trade_date": trade_date},
    ).mappings().all()
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "hit_count": 0, "loss_count": 0, "change_sum": 0.0, "change_count": 0})
    for row in rows:
        outcome = str(row.get("outcome") or "")
        change_pct = _num(row.get("change_pct"))
        theme_matches = _loads(row.get("theme_matches_json"), [])
        seen_themes: set[str] = set()
        for match in theme_matches or []:
            if not isinstance(match, dict):
                continue
            theme = str(match.get("theme") or "").strip()
            if not theme or theme in seen_themes:
                continue
            seen_themes.add(theme)
            stat = stats[theme]
            stat["count"] += 1
            if outcome in {"hit", "strong_hit"}:
                stat["hit_count"] += 1
            elif outcome in {"miss", "weak_miss"}:
                stat["loss_count"] += 1
            if change_pct is not None:
                stat["change_sum"] += change_pct
                stat["change_count"] += 1
    result: dict[str, dict[str, Any]] = {}
    for theme, stat in stats.items():
        count = int(stat.get("count") or 0)
        hit_count = int(stat.get("hit_count") or 0)
        result[theme] = {
            "count": count,
            "hit_count": hit_count,
            "loss_count": int(stat.get("loss_count") or 0),
            "hit_rate": hit_count / count if count else None,
            "average_change_pct": (stat.get("change_sum") / stat.get("change_count")) if stat.get("change_count") else None,
        }
    return result


def _symbols_from_daily_data(db: Session, *, trade_date: str, limit: int) -> list[str]:
    table = preferred_daily_kline_table()
    rows = db.execute(
        text(
            f"""
            SELECT symbol
            FROM {table}
            WHERE trade_date = :trade_date
              AND sw_industry_l1 IS NOT NULL
            ORDER BY amount DESC NULLS LAST
            LIMIT :limit
            """
        ),
        {"trade_date": trade_date, "limit": limit},
    ).mappings().all()
    return [_normalize_symbol(row["symbol"]) for row in rows]


def _next_trade_date(db: Session, trade_date: str) -> str | None:
    table = preferred_daily_kline_table()
    value = db.execute(
        text(f"SELECT min(trade_date) FROM {table} WHERE trade_date > :trade_date"),
        {"trade_date": trade_date},
    ).scalar()
    return str(value) if value is not None else None


def _load_daily_price_rows(db: Session, *, symbols: list[str], trade_date: str) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    table = preferred_daily_kline_table()
    variants = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
    rows = db.execute(
        text(
            f"""
            SELECT symbol, open, high, low, close
            FROM {table}
            WHERE trade_date = :trade_date
              AND symbol IN :symbols
            """
        ).bindparams(bindparam("symbols", expanding=True)),
        {"trade_date": trade_date, "symbols": variants},
    ).mappings().all()
    return {_normalize_symbol(row["symbol"]): dict(row) for row in rows}


def _load_settlements(db: Session, trade_date: str) -> dict[str, Any]:
    rows = db.execute(
        text(
            """
            SELECT *
            FROM catalyst_selection_settlements
            WHERE trade_date = :trade_date
            ORDER BY rank
            """
        ),
        {"trade_date": trade_date},
    ).mappings().all()
    settlement_date = rows[0]["settlement_date"] if rows else None
    return {
        "trade_date": trade_date,
        "settlement_date": settlement_date,
        "items": [
            {
                "trade_date": row["trade_date"],
                "settlement_date": row["settlement_date"],
                "symbol": row["symbol"],
                "name": row["name"] or row["symbol"],
                "rank": int(row["rank"] or 0),
                "entry_price": _round_or_none(row["entry_price"], 4),
                "close_price": _round_or_none(row["close_price"], 4),
                "next_open_price": _round_or_none(row["next_open_price"], 4),
                "high_price": _round_or_none(row["high_price"], 4),
                "low_price": _round_or_none(row["low_price"], 4),
                "change_pct": _round_or_none(row["change_pct"], 4),
                "max_up_pct": _round_or_none(row["max_up_pct"], 4),
                "max_down_pct": _round_or_none(row["max_down_pct"], 4),
                "hit_score": _round_or_none(row["hit_score"], 2),
                "outcome": row["outcome"],
                "protected": bool(row["protected"]),
                "settlement_notes": _loads(row["settlement_notes_json"], []),
            }
            for row in rows
        ],
        "updated_at": _utcnow().isoformat(),
    }


def _row_to_item(row: Any) -> dict[str, Any]:
    settlement = None
    if row.get("settlement_date"):
        settlement = {
            "trade_date": row["trade_date"],
            "settlement_date": row["settlement_date"],
            "symbol": row["symbol"],
            "name": row["name"] or row["symbol"],
            "rank": int(row["rank"] or 0),
            "entry_price": _round_or_none(row["entry_price"], 4),
            "close_price": _round_or_none(row["close_price"], 4),
            "next_open_price": _round_or_none(row["next_open_price"], 4),
            "high_price": _round_or_none(row["high_price"], 4),
            "low_price": _round_or_none(row["low_price"], 4),
            "change_pct": _round_or_none(row["change_pct"], 4),
            "max_up_pct": _round_or_none(row["max_up_pct"], 4),
            "max_down_pct": _round_or_none(row["max_down_pct"], 4),
            "hit_score": _round_or_none(row["hit_score"], 2),
            "outcome": row["outcome"],
            "protected": bool(row["protected"]),
            "settlement_notes": _loads(row["settlement_notes_json"], []),
        }
    closed_loop_trace = _loads(row["closed_loop_trace_json"], {})
    scoring_trace = closed_loop_trace.get("scoring") if isinstance(closed_loop_trace.get("scoring"), dict) else {}
    component_scores = scoring_trace.get("component_scores") if isinstance(scoring_trace.get("component_scores"), dict) else {}
    execution_adjustment = scoring_trace.get("execution_gate_adjustment") if isinstance(scoring_trace.get("execution_gate_adjustment"), dict) else {}
    if not execution_adjustment and "execution_gate_adjustment" in component_scores:
        risk_control = _loads(row["risk_control_json"], {})
        monitor = risk_control.get("risk_monitoring") if isinstance(risk_control.get("risk_monitoring"), dict) else {}
        execution_adjustment = {
            "gate": monitor.get("execution_gate"),
            "status": monitor.get("status"),
            "action": risk_control.get("action"),
            "score_delta": _round_or_none(component_scores.get("execution_gate_adjustment"), 2),
        }
    return {
        "rank": int(row["rank"] or 0),
        "symbol": row["symbol"],
        "name": row["name"] or row["symbol"],
        "industry": row["industry"],
        "sector": row["sector"],
        "concepts": _loads(row["concepts_json"], []),
        "score": round(float(row["score"] or 0.0), 2),
        "pre_execution_score": _round_or_none(component_scores.get("pre_execution_score"), 2),
        "execution_gate_adjustment": execution_adjustment,
        "catalyst_score": round(float(row["catalyst_score"] or 0.0), 2),
        "theme_score": round(float(row["theme_score"] or 0.0), 2),
        "relation_score": round(float(row["relation_score"] or 0.0), 2),
        "market_confirm_score": round(float(row["market_confirm_score"] or 0.0), 2),
        "event_intelligence_score": round(float(row["event_intelligence_score"] or 0.0), 2),
        "momentum_score": round(float(row["momentum_score"] or 0.0), 2),
        "fundamental_score": round(float(row["fundamental_score"] or 0.0), 2),
        "continuity_score": round(float(row["continuity_score"] or 0.0), 2),
        "adaptive_feedback_score": round(float(row["adaptive_feedback_score"] or 50.0), 2),
        "risk_penalty": round(float(row["risk_penalty"] or 0.0), 2),
        "risk_flags": _loads(row["risk_flags_json"], []),
        "reason_parts": _loads(row["reason_parts_json"], []),
        "theme_matches": _loads(row["theme_matches_json"], []),
        "signal_flags": _loads(row["signal_flags_json"], []),
        "risk_control": _loads(row["risk_control_json"], {}),
        "closed_loop_trace": closed_loop_trace,
        "market_background": row["market_background"] or "",
        "market_behavior_labels": _loads(row["market_behavior_json"], {}),
        "metric_snapshot": _loads(row["metric_snapshot_json"], {}),
        "settlement": settlement,
    }


def _row_to_opportunity_event(row: Any) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "run_id": row["run_id"],
        "trade_date": row["trade_date"],
        "window": row["window_label"],
        "symbol": row["symbol"],
        "name": row["name"] or row["symbol"],
        "rank": int(row["rank"] or 0),
        "score": round(float(row["score"] or 0.0), 2),
        "previous_rank": int(row["previous_rank"]) if row["previous_rank"] is not None else None,
        "previous_score": _round_or_none(row["previous_score"], 2),
        "rank_delta": int(row["rank_delta"]) if row["rank_delta"] is not None else None,
        "score_delta": _round_or_none(row["score_delta"], 2),
        "event_level": row["event_level"],
        "event_types": _loads(row["event_types_json"], []),
        "reasons": _loads(row["reasons_json"], []),
        "risk_action": row["risk_action"],
        "risk_level": row["risk_level"],
        "trace": _loads(row["trace_json"], {}),
        "created_at": _iso(row["created_at"]),
    }


def _settlement_hit_score(change_pct: float | None, max_up_pct: float | None, max_down_pct: float | None) -> float | None:
    if change_pct is None and max_up_pct is None:
        return None
    score = 50.0
    if change_pct is not None:
        score += change_pct * 5
    if max_up_pct is not None:
        score += max(0.0, max_up_pct) * 2.2
    if max_down_pct is not None and max_down_pct < -5:
        score += max_down_pct * 2
    return round(max(0.0, min(score, 100.0)), 2)


def _settlement_outcome(hit_score: float | None) -> str:
    if hit_score is None:
        return "pending_data"
    if hit_score >= 75:
        return "strong_hit"
    if hit_score >= 60:
        return "hit"
    if hit_score <= 35:
        return "weak_miss"
    return "miss"


def _settlement_notes(item: dict[str, Any], change_pct: float | None, max_up_pct: float | None, max_down_pct: float | None, outcome: str) -> list[str]:
    notes = [f"结算结果：{outcome}"]
    if change_pct is not None:
        notes.append(f"次日收盘相对入选日收盘 {change_pct:+.2f}%")
    if max_up_pct is not None:
        notes.append(f"次日最高浮盈 {max_up_pct:+.2f}%")
    if max_down_pct is not None:
        notes.append(f"次日最大回撤 {max_down_pct:+.2f}%")
    if item.get("risk_flags"):
        notes.append("入选时风险：" + "；".join(item["risk_flags"][:2]))
    return notes


def _normalize_window(window: str) -> str:
    value = str(window or "premarket").strip()
    return value if value in SUPPORTED_WINDOWS else "premarket"


def _normalize_windows(windows: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw_window in windows:
        window = _normalize_window(str(raw_window or "premarket"))
        if window not in seen:
            seen.add(window)
            result.append(window)
    return result or ["premarket"]


def _selection_message(window: str) -> str:
    if _normalize_window(window) == "premarket":
        return "盘前催化选股仅用于研究和复盘，不构成直接买卖建议。"
    return "实时事件机会榜仅用于研究和复盘，不构成直接买卖建议。"


def _resolve_trade_date(db: Session, trade_date: str | None) -> str:
    current_trade_date = _effective_cn_trade_date()
    if trade_date:
        requested = _parse_trade_date(trade_date)
        if requested > current_trade_date:
            raise ValueError(f"trade_date {requested} 不能晚于当前交易日 {current_trade_date}")
        if is_cn_trading_day(requested):
            return requested
        return previous_cn_trading_day(requested)
    latest_available = _latest_available_daily_trade_date(db, current_trade_date)
    latest_minute = _latest_available_minute_trade_date(db, before_or_equal_trade_date=current_trade_date)
    if latest_minute and (not latest_available or date.fromisoformat(latest_minute) > date.fromisoformat(latest_available)):
        return latest_minute
    if latest_available:
        return latest_available
    return current_trade_date


def _effective_cn_trade_date() -> str:
    today = now_cn().date().isoformat()
    if is_cn_trading_day(today):
        return today
    return previous_cn_trading_day(today)


def _parse_trade_date(value: str) -> str:
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except Exception as exc:
        raise ValueError(f"无效的 trade_date: {value}") from exc


def _selection_anchor_now(trade_date: str, window: str = "premarket") -> datetime:
    current_now = now_cn()
    parsed = date.fromisoformat(trade_date)
    if _normalize_window(window) != "premarket":
        return current_now
    cutoff = datetime.combine(parsed, PREMARKET_NEWS_CUTOFF).replace(tzinfo=current_now.tzinfo)
    if trade_date == current_now.date().isoformat() and current_now < cutoff:
        return current_now
    return cutoff


def _can_reuse_selection_run(
    stored: dict[str, Any],
    window: str,
    *,
    db: Session | None = None,
    user_id: str | None = None,
) -> bool:
    return bool(_selection_cache_reuse_state(stored, window, db=db, user_id=user_id).get("reusable"))


def _selection_cache_reuse_state(
    stored: dict[str, Any],
    window: str,
    *,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    llm_runtime = _selection_llm_runtime_cache_state(stored, db=db, user_id=user_id)
    if llm_runtime.get("status") == "changed":
        return {
            "reusable": False,
            "reason": "llm_runtime_changed",
            "llm_runtime": llm_runtime,
        }
    if _normalize_window(window) == "premarket":
        return {"reusable": True, "reason": "premarket_cache", "llm_runtime": llm_runtime}
    updated_at = _parse_datetime_or_none(stored.get("updated_at"))
    if updated_at is None:
        return {"reusable": False, "reason": "missing_updated_at", "llm_runtime": llm_runtime}
    age_seconds = (_utcnow() - updated_at).total_seconds()
    if 0 <= age_seconds <= REALTIME_SELECTION_CACHE_TTL_SECONDS:
        return {
            "reusable": True,
            "reason": "ttl_valid",
            "age_seconds": age_seconds,
            "llm_runtime": llm_runtime,
        }
    return {
        "reusable": False,
        "reason": "ttl_expired",
        "age_seconds": age_seconds,
        "llm_runtime": llm_runtime,
    }


def _selection_llm_runtime_cache_state(
    stored: dict[str, Any],
    *,
    db: Session | None,
    user_id: str | None,
) -> dict[str, Any]:
    if db is None:
        return {"status": "unchecked", "matches": True}
    current_runtime = _current_selection_llm_runtime(db, user_id=user_id)
    if not current_runtime:
        return {"status": "unknown", "matches": True}
    cached_runtime = _cached_selection_llm_runtime(stored)
    current_fingerprint = _llm_runtime_fingerprint(current_runtime)
    cached_fingerprint = _llm_runtime_fingerprint(cached_runtime)
    if current_fingerprint == cached_fingerprint:
        return {
            "status": "matched",
            "matches": True,
            "current_runtime": _safe_llm_runtime_payload(current_runtime),
            "cached_runtime": _safe_llm_runtime_payload(cached_runtime),
            "current_fingerprint": current_fingerprint,
            "cached_fingerprint": cached_fingerprint,
        }
    return {
        "status": "changed",
        "matches": False,
        "current_runtime": _safe_llm_runtime_payload(current_runtime),
        "cached_runtime": _safe_llm_runtime_payload(cached_runtime),
        "current_fingerprint": current_fingerprint,
        "cached_fingerprint": cached_fingerprint,
    }


def _current_selection_llm_runtime(db: Session, *, user_id: str | None) -> dict[str, Any]:
    try:
        runtime = news_theme_service.core_stock_llm_readiness(db, user_id=user_id)
    except Exception:
        logger.exception("[catalyst-selection] failed to inspect current LLM runtime for cache validation")
        return {}
    return runtime if isinstance(runtime, dict) else {}


def _cached_selection_llm_runtime(stored: dict[str, Any]) -> dict[str, Any]:
    governance = stored.get("data_governance") if isinstance(stored.get("data_governance"), dict) else {}
    top_level_runtime = governance.get("llm_core_stock") if isinstance(governance.get("llm_core_stock"), dict) else {}
    if top_level_runtime:
        return top_level_runtime
    closed_loop = governance.get("closed_loop") if isinstance(governance.get("closed_loop"), dict) else {}
    runtime = closed_loop.get("llm_event_understanding") if isinstance(closed_loop.get("llm_event_understanding"), dict) else {}
    return runtime if isinstance(runtime, dict) else {}


def _llm_runtime_fingerprint(runtime: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime, dict):
        return {}
    return {
        key: _normalize_llm_runtime_value(key, runtime.get(key))
        for key in LLM_RUNTIME_FINGERPRINT_KEYS
        if key in runtime
    }


def _normalize_llm_runtime_value(key: str, value: Any) -> Any:
    if key in {"enabled", "ready", "requires_api_key", "has_api_key"}:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
    if value is None:
        return None
    text_value = str(value).strip()
    if key == "provider":
        return text_value.lower()
    if key == "base_url":
        return text_value.rstrip("/")
    return text_value


def _safe_llm_runtime_payload(runtime: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(runtime, dict):
        return {}
    blocked_keys = {"api_key", "news_api_key", "authorization", "headers"}
    return {key: value for key, value in runtime.items() if key not in blocked_keys}


def _replace_stale_llm_runtime_governance(governance: dict[str, Any], llm_state: dict[str, Any]) -> dict[str, Any]:
    result = dict(governance)
    closed_loop = dict(result.get("closed_loop") or {})
    current_runtime = _safe_llm_runtime_payload(llm_state.get("current_runtime") or {})
    cached_runtime = _safe_llm_runtime_payload(llm_state.get("cached_runtime") or {})
    cached_usage = {
        key: value
        for key, value in cached_runtime.items()
        if key.startswith("used_") or key.endswith("_count")
    }
    llm_payload = dict(current_runtime)
    llm_payload.update(
        {
            "cache_status": "stale",
            "stale_reason": "llm_runtime_changed",
            "current_runtime": current_runtime,
            "cached_runtime": cached_runtime,
            "cached_usage": cached_usage,
        }
    )
    result["llm_core_stock"] = llm_payload
    closed_loop["llm_event_understanding"] = llm_payload
    result["closed_loop"] = closed_loop
    return result


def _latest_available_daily_trade_date(db: Session, current_trade_date: str) -> str | None:
    table = preferred_daily_kline_table()
    value = db.execute(
        text(f"SELECT max(trade_date) FROM {table} WHERE trade_date <= :trade_date"),
        {"trade_date": current_trade_date},
    ).scalar()
    return str(value) if value is not None else None


def _feature_trade_date_for_selection(db: Session, trade_date: str) -> str:
    feature_trade_date = _latest_available_daily_trade_date(db, trade_date)
    if feature_trade_date:
        return feature_trade_date
    return trade_date


def _latest_window_start(theme_items: list[dict[str, Any]]) -> str | None:
    for item in theme_items:
        if item.get("window_start"):
            return str(item["window_start"])
    return None


def _latest_window_end(theme_items: list[dict[str, Any]]) -> str | None:
    for item in theme_items:
        if item.get("window_end"):
            return str(item["window_end"])
    return None


def _symbol_variants(symbol: str) -> list[str]:
    normalized = _normalize_symbol(symbol)
    if "." not in normalized:
        return [normalized]
    code, suffix = normalized.split(".", 1)
    return [normalized, f"{suffix}{code}", code]


def _normalize_symbol(symbol: Any) -> str:
    value = str(symbol or "").strip().upper()
    if "." in value and len(value.split(".", 1)[0]) == 6:
        return value
    if len(value) >= 8 and value[:2] in {"SH", "SZ", "BJ"}:
        return f"{value[2:8]}.{value[:2]}"
    if len(value) == 6 and value.isdigit():
        if value.startswith("6"):
            return f"{value}.SH"
        if value.startswith(("0", "3")):
            return f"{value}.SZ"
        if value.startswith(("4", "8")):
            return f"{value}.BJ"
    return value


def _concepts_from_row(row: Any) -> list[str]:
    concepts: list[str] = []
    for key in ("sw_industry_l1", "sw_industry_l2", "sw_industry_l3"):
        value = str(row.get(key) or "").strip()
        if value and value not in concepts:
            concepts.append(value)
    return concepts


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        text_value = str(value or "").strip()
        if text_value:
            return text_value
    return None


def _dedupe_strings(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text_value = str(value or "").strip()
        if not text_value or text_value in seen:
            continue
        seen.add(text_value)
        result.append(text_value)
    return result


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_int(value: Any) -> int:
    number = _num(value)
    return int(number) if number is not None else 0


def _pct(value: Any, base: Any) -> float | None:
    value_num = _num(value)
    base_num = _num(base)
    if value_num is None or base_num is None or base_num == 0:
        return None
    return round((value_num / base_num - 1) * 100, 4)


def _round_or_none(value: Any, digits: int) -> float | None:
    number = _num(value)
    return round(number, digits) if number is not None else None


def _loads(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return fallback


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None:
        return ""
    return str(value)


def _parse_datetime_or_none(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _utcnow() -> datetime:
    return datetime.utcnow()
