from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
from fastapi import HTTPException
from langchain_core.messages import HumanMessage, SystemMessage
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.core.runtime_config import (
    account_llm_runtime_sources,
    build_news_runtime_config,
    has_mixed_account_llm_runtime,
    llm_runtime_package_source,
)
from api.core.stock_map import get_reverse_stock_map, get_stock_map_version
from api.database import SessionLocal, ScheduledAnalysisDB, WatchlistItemDB
from api.services.data_source_governance import build_news_eye_governance
from tradingagents.llm_clients.factory import create_llm_client


logger = logging.getLogger(__name__)
CN_TZ = ZoneInfo("Asia/Shanghai")
NEWS_LLM_TIMEOUT_SECONDS = max(float(os.getenv("NEWS_EYE_LLM_TIMEOUT_SECONDS", "120")), 5.0)

POSITIVE_KEYWORDS = ("利好", "增长", "突破", "中标", "回购", "增持", "涨价", "扩产", "创新高", "超预期", "获批", "签约")
NEGATIVE_KEYWORDS = ("利空", "下滑", "亏损", "减持", "处罚", "调查", "暴跌", "下调", "违约", "风险", "退市", "低于预期")
POSITIVE_PHRASES: tuple[tuple[str, int], ...] = (
    ("业绩增长", 3),
    ("大幅增长", 3),
    ("同比增长", 2),
    ("订单增长", 2),
    ("超预期", 3),
    ("签约", 2),
    ("中标", 3),
    ("获批", 3),
    ("回购", 2),
    ("增持", 2),
    ("扩产", 2),
    ("涨价", 2),
    ("走强", 2),
    ("景气回升", 2),
    ("创新高", 3),
    ("加快", 1),
    ("改善", 1),
)
NEGATIVE_PHRASES: tuple[tuple[str, int], ...] = (
    ("不及预期", 3),
    ("低于预期", 3),
    ("业绩下滑", 3),
    ("同比下滑", 2),
    ("订单下滑", 2),
    ("亏损", 3),
    ("减持", 2),
    ("处罚", 3),
    ("调查", 2),
    ("违约", 3),
    ("退市", 4),
    ("暴跌", 3),
    ("承压", 2),
    ("走弱", 2),
    ("下调", 2),
    ("风险", 1),
    ("停产", 3),
)
SECTOR_KEYWORDS = (
    "算力",
    "人工智能",
    "半导体",
    "芯片",
    "新能源",
    "锂电池",
    "光伏",
    "机器人",
    "低空经济",
    "医药",
    "银行",
    "证券",
    "地产",
    "煤炭",
    "有色",
    "军工",
    "汽车",
    "消费电子",
)
A_SHARE_MARKET_KEYWORDS = (
    "A股",
    "沪深",
    "上证",
    "深证",
    "创业板",
    "科创板",
    "北交所",
    "沪指",
    "深成指",
    "中证",
    "证监会",
    "国务院",
    "央行",
    "财政部",
    "发改委",
    "工信部",
    "商务部",
    "国资委",
    "上市公司",
    "交易所",
    "涨停",
    "跌停",
    "龙虎榜",
    "北向资金",
    "南向资金",
    "沪股通",
    "深股通",
    "港股通",
    "融资融券",
    "两融",
    "人民币",
    "MLF",
    "LPR",
)
GLOBAL_IMPACT_KEYWORDS = (
    "美联储",
    "关税",
    "制裁",
    "原油",
    "黄金",
    "铜价",
    "油价",
    "运价",
    "航运",
    "美元指数",
    "离岸人民币",
    "美债",
    "非农",
)
OVERSEAS_NOISE_KEYWORDS = (
    "道琼斯",
    "标普500",
    "纳斯达克",
    "纳指",
    "欧洲股市",
    "美股",
    "欧股",
    "日经",
    "韩国综合指数",
    "微软",
    "亚马逊",
    "谷歌",
    "META",
)

_TASK: asyncio.Task | None = None
_STOP_EVENT: asyncio.Event | None = None
_NEWS_SCHEMA_LOCK = threading.RLock()
_NEWS_SCHEMA_ENSURED_BINDS: set[str] = set()
_SEARCH_INDEX_BACKFILLED_BINDS: set[str] = set()
_DEDUPE_KEYS_BACKFILLED_BINDS: set[str] = set()
_POLL_SECONDS = max(int(os.getenv("NEWS_EYE_POLL_SECONDS", "45")), 15)
_BACKGROUND_LIMIT = max(int(os.getenv("NEWS_EYE_BACKGROUND_LIMIT", "120")), 20)
_MANUAL_LIMIT = max(int(os.getenv("NEWS_EYE_MANUAL_LIMIT", "160")), 20)
_WATCHLIST_SYMBOL_LIMIT = max(int(os.getenv("NEWS_EYE_WATCHLIST_SYMBOL_LIMIT", "12")), 0)
_SYMBOL_SOURCE_LIMIT = max(int(os.getenv("NEWS_EYE_SYMBOL_SOURCE_LIMIT", "6")), 1)
_SOURCE_TIMEOUT_SECONDS = max(float(os.getenv("NEWS_EYE_SOURCE_TIMEOUT_SECONDS", "8")), 2.0)
_SOURCE_FETCH_WORKERS = max(int(os.getenv("NEWS_EYE_SOURCE_FETCH_WORKERS", "6")), 2)
_SOURCE_FETCH_EXECUTOR = ThreadPoolExecutor(max_workers=_SOURCE_FETCH_WORKERS, thread_name_prefix="news-eye-source")
_SYNC_STATE_KEY = "news_eye"
_CLAUSE_SPLIT_PATTERN = re.compile(r"[。！？!?\n；;]+|(?<=\S)，")
_A_SHARE_CODE_PATTERN = re.compile(r"(?<!\d)(\d{6})(?!\d)")
_SYMBOL_NAME_FRAGMENT_PATTERN = re.compile(r"[0-9A-Za-z\u4e00-\u9fff]{2,}")
_NEWS_CONTENT_DUPE_STRIP_PATTERN = re.compile(r"[\s\u3000【】\[\]（）()《》<>“”\"'‘’、，,。；;：:！？!?·.\-_/|]+")
_SYMBOL_PREFIX_LENGTH = 2
_EVENT_SELECTION_REFRESH_LOCK = threading.RLock()
_EVENT_SELECTION_REFRESH_TASKS: set[str] = set()
_EVENT_SELECTION_REFRESH_PENDING: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class SymbolLookupIndex:
    source_token: tuple[int, int]
    code_to_symbol: dict[str, str]
    name_prefix_candidates: dict[str, tuple[tuple[str, str], ...]]


_SYMBOL_LOOKUP_INDEX: SymbolLookupIndex | None = None


@dataclass(frozen=True)
class NewsSourceSpec:
    label: str
    func_name: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    symbol_param: str | None = None
    symbol_transform: Any | None = None


GENERAL_SOURCE_SPECS: tuple[NewsSourceSpec, ...] = (
    NewsSourceSpec("财联社电报", "stock_info_global_cls", {"symbol": "全部"}),
    NewsSourceSpec("东方财富全球快讯", "stock_info_global_em"),
    NewsSourceSpec("东方财富财经早餐", "stock_info_cjzc_em"),
    NewsSourceSpec("新浪7x24", "stock_info_global_sina"),
    NewsSourceSpec("富途快讯", "stock_info_global_futu"),
    NewsSourceSpec("同花顺全球直播", "stock_info_global_ths"),
)
SYMBOL_SOURCE_SPECS: tuple[NewsSourceSpec, ...] = (
    NewsSourceSpec(
        "东方财富个股新闻",
        "stock_news_em",
        symbol_param="symbol",
        symbol_transform=lambda symbol: str(symbol).split(".", 1)[0],
    ),
)


async def start_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _TASK and not _TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _TASK = asyncio.create_task(_run_loop(), name="news-eye-sync")


async def stop_background_worker() -> None:
    global _TASK, _STOP_EVENT
    if _STOP_EVENT is not None:
        _STOP_EVENT.set()
    if _TASK is not None:
        try:
            await _TASK
        except Exception:
            logger.exception("[news-eye] stop worker failed")
    _TASK = None
    _STOP_EVENT = None


async def _run_loop() -> None:
    logger.info("[news-eye] background worker started")
    while _STOP_EVENT is not None and not _STOP_EVENT.is_set():
        try:
            await asyncio.to_thread(_scan_and_refresh_once)
        except Exception:
            logger.exception("[news-eye] background refresh failed")
        try:
            await asyncio.wait_for(_STOP_EVENT.wait(), timeout=_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    logger.info("[news-eye] background worker stopped")


def _scan_and_refresh_once() -> None:
    with SessionLocal() as db:
        ensure_news_tables(db)
        background_user_id = _load_background_refresh_user_id(db)
        user_symbols = load_user_focus_symbols(db, background_user_id, limit=_WATCHLIST_SYMBOL_LIMIT) if background_user_id else []
        symbols = _dedupe_symbols([*user_symbols, *_load_global_focus_symbols(db, limit=_WATCHLIST_SYMBOL_LIMIT)])[:_WATCHLIST_SYMBOL_LIMIT]
        db.rollback()
        result = refresh_news_cache(
            db,
            limit=_BACKGROUND_LIMIT,
            symbols=symbols,
            trigger="background",
            user_id=background_user_id,
            async_event_driven_selection=True,
        )
        logger.info(
            "[news-eye] background refresh saved=%s user=%s sources=%s symbols=%s warnings=%s",
            result.get("saved", 0),
            background_user_id or "system",
            ",".join(result.get("active_sources", [])[:8]) or "none",
            ",".join(result.get("tracked_symbols", [])[:8]) or "none",
            len(result.get("warnings", [])),
        )


def ensure_news_tables(db: Session) -> None:
    bind_key = _schema_bind_key(db)
    if bind_key in _NEWS_SCHEMA_ENSURED_BINDS:
        return
    with _NEWS_SCHEMA_LOCK:
        if bind_key in _NEWS_SCHEMA_ENSURED_BINDS:
            return
        _ensure_news_tables_uncached(db, bind_key=bind_key)
        _NEWS_SCHEMA_ENSURED_BINDS.add(bind_key)


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


def _ensure_news_tables_uncached(db: Session, *, bind_key: int) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS market_news_items (
                digest VARCHAR(64) PRIMARY KEY,
                dedupe_key VARCHAR(80),
                content TEXT NOT NULL,
                published_at TIMESTAMP NOT NULL,
                source VARCHAR(80) NOT NULL,
                url TEXT,
                sentiment VARCHAR(20) DEFAULT 'neutral',
                positive_sectors_json TEXT DEFAULT '[]',
                negative_sectors_json TEXT DEFAULT '[]',
                positive_symbols_json TEXT DEFAULT '[]',
                negative_symbols_json TEXT DEFAULT '[]',
                related_symbols_json TEXT DEFAULT '[]',
                fetched_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("ALTER TABLE market_news_items ADD COLUMN IF NOT EXISTS dedupe_key VARCHAR(80)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_items_published_at ON market_news_items (published_at DESC)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_items_source ON market_news_items (source)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_items_sentiment ON market_news_items (sentiment)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS market_news_item_symbols (
                digest VARCHAR(64) NOT NULL,
                symbol VARCHAR(16) NOT NULL,
                name VARCHAR(80),
                tag_group VARCHAR(20) NOT NULL,
                PRIMARY KEY (digest, symbol, tag_group),
                FOREIGN KEY (digest) REFERENCES market_news_items(digest) ON DELETE CASCADE
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_item_symbols_symbol ON market_news_item_symbols (symbol)"))
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_item_symbols_name ON market_news_item_symbols (name)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS market_news_item_sectors (
                digest VARCHAR(64) NOT NULL,
                sector VARCHAR(40) NOT NULL,
                tag_group VARCHAR(20) NOT NULL,
                PRIMARY KEY (digest, sector, tag_group),
                FOREIGN KEY (digest) REFERENCES market_news_items(digest) ON DELETE CASCADE
            )
            """
        )
    )
    db.execute(text("CREATE INDEX IF NOT EXISTS ix_market_news_item_sectors_sector ON market_news_item_sectors (sector)"))
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS market_news_sync_state (
                worker_name VARCHAR(32) PRIMARY KEY,
                status VARCHAR(20) NOT NULL,
                last_run_at TIMESTAMP,
                last_success_at TIMESTAMP,
                last_error TEXT,
                active_sources_json TEXT DEFAULT '[]',
                tracked_symbols_json TEXT DEFAULT '[]',
                saved_count INTEGER DEFAULT 0,
                new_count INTEGER DEFAULT 0,
                updated_count INTEGER DEFAULT 0,
                unchanged_count INTEGER DEFAULT 0,
                fresh_event_count INTEGER DEFAULT 0,
                event_selection_json TEXT DEFAULT '{}',
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(text("ALTER TABLE market_news_sync_state ADD COLUMN IF NOT EXISTS new_count INTEGER DEFAULT 0"))
    db.execute(text("ALTER TABLE market_news_sync_state ADD COLUMN IF NOT EXISTS updated_count INTEGER DEFAULT 0"))
    db.execute(text("ALTER TABLE market_news_sync_state ADD COLUMN IF NOT EXISTS unchanged_count INTEGER DEFAULT 0"))
    db.execute(text("ALTER TABLE market_news_sync_state ADD COLUMN IF NOT EXISTS fresh_event_count INTEGER DEFAULT 0"))
    db.execute(text("ALTER TABLE market_news_sync_state ADD COLUMN IF NOT EXISTS event_selection_json TEXT DEFAULT '{}'"))
    db.commit()
    if bind_key not in _DEDUPE_KEYS_BACKFILLED_BINDS:
        _backfill_news_dedupe_keys_if_needed(db)
        _DEDUPE_KEYS_BACKFILLED_BINDS.add(bind_key)
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_market_news_items_dedupe_key ON market_news_items (dedupe_key)"))
    db.commit()
    if bind_key not in _SEARCH_INDEX_BACKFILLED_BINDS:
        _backfill_news_search_index_if_needed(db)
        _SEARCH_INDEX_BACKFILLED_BINDS.add(bind_key)


def list_news_items(
    db: Session,
    *,
    limit: int,
    offset: int = 0,
    source: str | None = None,
    sentiment: str | None = None,
    symbol: str | None = None,
    sector: str | None = None,
) -> dict[str, Any]:
    ensure_news_tables(db)
    clauses = ["1=1"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if source:
        clauses.append("source = :source")
        params["source"] = source
    if sentiment and sentiment != "all":
        clauses.append("sentiment = :sentiment")
        params["sentiment"] = sentiment
    if symbol:
        symbol_text = symbol.strip()
        if symbol_text:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM market_news_item_symbols idx
                    WHERE idx.digest = market_news_items.digest
                      AND (idx.symbol = :symbol_exact OR idx.name LIKE :symbol_name)
                )
                """
            )
            params["symbol_exact"] = symbol_text.upper()
            params["symbol_name"] = f"%{symbol_text}%"
    if sector:
        sector_text = sector.strip()
        if sector_text:
            clauses.append(
                """
                EXISTS (
                    SELECT 1
                    FROM market_news_item_sectors idx
                    WHERE idx.digest = market_news_items.digest
                      AND idx.sector LIKE :sector
                )
                """
            )
            params["sector"] = f"%{sector_text}%"

    total_row = db.execute(
        text(
            f"""
            SELECT COUNT(*) AS total_count,
                   MIN(published_at) AS earliest_published_at,
                   MAX(published_at) AS latest_published_at
            FROM market_news_items
            WHERE {' AND '.join(clauses)}
            """
        ),
        params,
    ).mappings().first() or {}
    rows = db.execute(
        text(
            f"""
            SELECT digest, content, published_at, source, url, sentiment,
                   positive_sectors_json, negative_sectors_json, positive_symbols_json, negative_symbols_json,
                   related_symbols_json, fetched_at
            FROM market_news_items
            WHERE {' AND '.join(clauses)}
            ORDER BY published_at DESC, fetched_at DESC
            LIMIT :limit
            OFFSET :offset
            """
        ),
        params,
    ).mappings().all()
    latest_row = db.execute(text("SELECT MAX(fetched_at) AS latest_fetched_at FROM market_news_items")).mappings().first()
    state = _load_sync_state(db)
    updated_at = _iso_or_none(
        (latest_row or {}).get("latest_fetched_at")
        or state.get("last_success_at")
        or _utcnow_naive()
    )
    returned = len(rows)
    total_available = int(total_row.get("total_count") or 0)
    payload = {
        "items": [_row_to_news_item(row) for row in rows],
        "total": total_available,
        "updated_at": updated_at,
        "source": "cache:market_news_items",
        "fallback": False,
        "background": {
            "enabled": True,
            "interval_seconds": _POLL_SECONDS,
            "status": state.get("status") or ("running" if _TASK and not _TASK.done() else "idle"),
            "last_run_at": _iso_or_none(state.get("last_run_at")),
            "last_success_at": _iso_or_none(state.get("last_success_at")),
            "last_error": state.get("last_error"),
            "active_sources": _loads(state.get("active_sources_json")),
            "tracked_symbols": _loads(state.get("tracked_symbols_json")),
            "saved_count": int(state.get("saved_count") or 0),
            "new_count": int(state.get("new_count") or 0),
            "updated_count": int(state.get("updated_count") or 0),
            "unchanged_count": int(state.get("unchanged_count") or 0),
            "fresh_event_count": int(state.get("fresh_event_count") or 0),
            "event_driven_selection": _loads_dict(state.get("event_selection_json")),
        },
        "history": {
            "offset": int(offset),
            "limit": int(limit),
            "returned": returned,
            "has_more": offset + returned < total_available,
            "earliest_published_at": _iso_or_none(total_row.get("earliest_published_at")),
            "latest_published_at": _iso_or_none(total_row.get("latest_published_at")),
            "total_available": total_available,
        },
    }
    payload["data_governance"] = build_news_eye_governance(payload)
    return payload


def analyze_news_item(db: Session, *, user_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    config = _resolve_news_llm_config(db, user_id=user_id)
    provider = str(config.get("llm_provider") or "").strip().lower()
    model = str(config.get("quick_think_llm") or config.get("deep_think_llm") or "").strip()
    if not provider or not model:
        raise HTTPException(status_code=400, detail="请先在设置页配置可用的模型后再执行资讯分析。")
    diagnostic = _news_llm_runtime_diagnostic(config)
    if diagnostic.get("mixed_account_runtime"):
        raise HTTPException(
            status_code=400,
            detail="资讯分析 LLM 配置不是同源运行包；请在设置页同时保存 provider、Base URL、模型和 Key。",
        )

    client_kwargs: dict[str, Any] = {}
    api_key = str(config.get("api_key") or "").strip()
    if api_key:
        client_kwargs["api_key"] = api_key

    try:
        client = create_llm_client(
            provider=provider,
            model=model,
            base_url=str(config.get("backend_url") or "").strip() or None,
            timeout=NEWS_LLM_TIMEOUT_SECONDS,
            **client_kwargs,
        )
        llm = client.get_llm()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"初始化模型失败：{exc}") from exc

    content = str(payload.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="资讯内容不能为空。")

    heuristic = {
        "sentiment": str(payload.get("sentiment") or _classify_sentiment(content)),
        "positive_sectors": _string_list(payload.get("positive_sectors")),
        "negative_sectors": _string_list(payload.get("negative_sectors")),
        "positive_symbols": _symbol_labels(payload.get("positive_symbols")),
        "negative_symbols": _symbol_labels(payload.get("negative_symbols")),
        "related_symbols": _symbol_labels(payload.get("related_symbols")),
    }
    prompt = _build_news_analysis_prompt(payload, heuristic)

    try:
        result = llm.invoke(
            [
                SystemMessage(content=_NEWS_EYE_ANALYSIS_SYSTEM_PROMPT),
                HumanMessage(content=prompt),
            ]
        )
        raw = str(getattr(result, "content", "") or "").strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"资讯分析失败：{exc}") from exc

    parsed = _parse_news_analysis_payload(raw)
    if not parsed:
        parsed = {
            "summary": raw[:180] or content[:120],
            "sentiment": heuristic["sentiment"],
            "sentiment_reason": "模型已返回文本，但未能稳定解析为结构化 JSON，已保留原始解读。",
            "positive_sectors": heuristic["positive_sectors"],
            "negative_sectors": heuristic["negative_sectors"],
            "positive_symbols": heuristic["positive_symbols"],
            "negative_symbols": heuristic["negative_symbols"],
            "trading_takeaway": raw[:180] or "建议结合盘口、成交量和公告原文继续确认。",
            "raw": raw or None,
        }

    return {
        "provider": provider,
        "model": model,
        "summary": str(parsed.get("summary") or "").strip() or content[:120],
        "sentiment": _normalize_sentiment_label(parsed.get("sentiment"), fallback=heuristic["sentiment"]),
        "sentiment_reason": str(parsed.get("sentiment_reason") or "").strip() or "模型未给出明确原因。",
        "positive_sectors": _string_list(parsed.get("positive_sectors")) or heuristic["positive_sectors"],
        "negative_sectors": _string_list(parsed.get("negative_sectors")) or heuristic["negative_sectors"],
        "positive_symbols": _string_list(parsed.get("positive_symbols")) or heuristic["positive_symbols"],
        "negative_symbols": _string_list(parsed.get("negative_symbols")) or heuristic["negative_symbols"],
        "trading_takeaway": str(parsed.get("trading_takeaway") or "").strip() or "建议结合后续公告与资金流确认持续性。",
        "generated_at": _iso_or_none(_utcnow_naive()),
        "raw": str(parsed.get("raw") or raw or "").strip() or None,
    }


def _resolve_news_llm_config(db: Session, *, user_id: str) -> dict[str, Any]:
    return build_news_runtime_config(user_id=user_id, db=db)


def _news_llm_runtime_diagnostic(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "runtime_package_source": llm_runtime_package_source(config),
        "api_key_source": str(config.get("_api_key_source") or "").strip() or None,
        "provider_source": str(config.get("_llm_provider_source") or "").strip() or None,
        "base_url_source": str(config.get("_backend_url_source") or "").strip() or None,
        "model_source": str(config.get("_quick_think_llm_source") or config.get("_deep_think_llm_source") or "").strip() or None,
        "account_runtime_sources": sorted(account_llm_runtime_sources(config)),
        "mixed_account_runtime": has_mixed_account_llm_runtime(config),
    }


def refresh_news_cache(
    db: Session,
    *,
    limit: int,
    symbols: list[str] | None = None,
    trigger: str = "manual",
    user_id: str | None = None,
    async_event_driven_selection: bool = False,
) -> dict[str, Any]:
    ensure_news_tables(db)
    run_started_at = _utcnow_naive()
    symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
    try:
        items, active_sources, warnings = _fetch_external_news(limit, symbols=symbols)
        items = _dedupe_items(items)
        sync_last_error = _sync_state_error_from_warnings(warnings, active_sources)
        sync_status = "success" if active_sources and not sync_last_error else ("degraded" if active_sources else "degraded")
        saved = 0
        new_count = 0
        updated_count = 0
        unchanged_count = 0
        fresh_news_events: list[dict[str, Any]] = []
        for item in items:
            enriched = _enrich_news_item(item)
            existing = _load_existing_news_item_for_dedupe(db, str(enriched.get("dedupe_key") or ""))
            is_new = not existing
            material_changed = bool(existing and _news_material_changed(existing, enriched))
            if is_new:
                new_count += 1
            elif material_changed:
                updated_count += 1
            else:
                unchanged_count += 1
            saved_row = db.execute(
                text(
                    """
                    INSERT INTO market_news_items (
                        digest, dedupe_key, content, published_at, source, url, sentiment,
                        positive_sectors_json, negative_sectors_json, positive_symbols_json, negative_symbols_json,
                        related_symbols_json, fetched_at
                    )
                    VALUES (
                        :digest, :dedupe_key, :content, :published_at, :source, :url, :sentiment,
                        :positive_sectors_json, :negative_sectors_json, :positive_symbols_json, :negative_symbols_json,
                        :related_symbols_json, :fetched_at
                    )
                    ON CONFLICT (dedupe_key) DO UPDATE SET
                        content = EXCLUDED.content,
                        published_at = EXCLUDED.published_at,
                        source = EXCLUDED.source,
                        url = EXCLUDED.url,
                        sentiment = EXCLUDED.sentiment,
                        positive_sectors_json = EXCLUDED.positive_sectors_json,
                        negative_sectors_json = EXCLUDED.negative_sectors_json,
                        positive_symbols_json = EXCLUDED.positive_symbols_json,
                        negative_symbols_json = EXCLUDED.negative_symbols_json,
                        related_symbols_json = EXCLUDED.related_symbols_json,
                        fetched_at = EXCLUDED.fetched_at
                    RETURNING digest
                    """
                ),
                enriched,
            ).mappings().first()
            if saved_row and saved_row.get("digest"):
                enriched["digest"] = saved_row["digest"]
            _replace_news_search_index(db, enriched)
            if is_new or material_changed:
                fresh_news_events.append(
                    _public_fresh_news_event(
                        enriched,
                        change_type="new" if is_new else "updated",
                    )
                )
            saved += 1
        fresh_event_count = new_count + updated_count
        fresh_event_context = _build_fresh_event_context(fresh_news_events)
        news_ingest = {
            "saved": saved,
            "new": new_count,
            "updated": updated_count,
            "unchanged": unchanged_count,
        }
        initial_event_driven_selection = (
            {
                "trigger": f"news-eye:{trigger}",
                "generated": [],
                "errors": [],
                "skipped": False,
                "triggered": True,
                "status": "running",
                "fresh_event_count": fresh_event_count,
                "news_ingest": news_ingest,
                "fresh_news_events": fresh_event_context.get("fresh_news_events") or [],
                "fresh_news_summary": fresh_event_context.get("fresh_news_summary") or {},
                "updated_at": _iso_or_none(_utcnow_naive()),
            }
            if fresh_event_count > 0
            else {}
        )
        _record_sync_state(
            db,
            status=sync_status,
            last_run_at=run_started_at,
            last_success_at=run_started_at if active_sources else None,
            last_error=sync_last_error,
            active_sources=active_sources,
            tracked_symbols=symbols,
            saved_count=saved,
            new_count=new_count,
            updated_count=updated_count,
            unchanged_count=unchanged_count,
            event_driven_selection=initial_event_driven_selection,
        )
        try:
            from api.services import news_theme_service

            news_theme_service.refresh_theme_rankings(
                db,
                windows=("premarket", "24h", "72h", "7d"),
                limit=20,
                persist=True,
                user_id=user_id,
                allow_async_llm=True,
                trigger_context={
                    "source": "news_eye",
                    "trigger": trigger,
                    "reason": "news_eye_theme_refresh",
                    "fresh_event_count": fresh_event_count,
                    "news_ingest": news_ingest,
                    **fresh_event_context,
                    "triggered_at": _iso_or_none(_utcnow_naive()),
                },
            )
        except Exception:
            logger.exception("[news-eye] theme ranking refresh failed trigger=%s", trigger)
        db.commit()
        event_driven_selection: dict[str, Any] | None = None
        if fresh_event_count > 0:
            try:
                if async_event_driven_selection:
                    event_driven_selection = _schedule_event_driven_selection_refresh(
                        trigger=f"news-eye:{trigger}",
                        windows=("premarket", "24h"),
                        limit=10,
                        user_id=user_id,
                        fresh_event_count=fresh_event_count,
                        news_ingest=news_ingest,
                        fresh_event_context=fresh_event_context,
                    )
                else:
                    from api.services import catalyst_selection_service

                    refreshed = catalyst_selection_service.refresh_event_driven_selection(
                        db,
                        trigger=f"news-eye:{trigger}",
                        windows=("premarket", "24h"),
                        limit=10,
                        user_id=user_id,
                        trigger_context={
                            "source": "news_eye",
                            "trigger": trigger,
                            "reason": "news_eye_fresh_events",
                            "fresh_event_count": fresh_event_count,
                            "news_ingest": news_ingest,
                            **fresh_event_context,
                        },
                    )
                    event_driven_selection = _public_event_driven_selection_state(
                        refreshed,
                        trigger=f"news-eye:{trigger}",
                        windows=("premarket", "24h"),
                        fresh_event_count=fresh_event_count,
                        news_ingest=news_ingest,
                        fresh_event_context=fresh_event_context,
                    )
            except Exception as exc:
                try:
                    db.rollback()
                except Exception:
                    logger.exception("[news-eye] rollback failed after event-driven selection error")
                logger.exception("[news-eye] event-driven selection refresh failed trigger=%s", trigger)
                event_driven_selection = {
                    "trigger": f"news-eye:{trigger}",
                    "generated": [],
                    "errors": [{"window": "premarket", "error": str(exc)}],
                    "skipped": False,
                    "triggered": True,
                    "status": "failed",
                    "fresh_event_count": fresh_event_count,
                    "news_ingest": news_ingest,
                    "fresh_news_events": fresh_event_context.get("fresh_news_events") or [],
                    "fresh_news_summary": fresh_event_context.get("fresh_news_summary") or {},
                    "updated_at": _iso_or_none(_utcnow_naive()),
                }
        else:
            event_driven_selection = {
                "trigger": f"news-eye:{trigger}",
                "generated": [],
                "errors": [],
                "skipped": True,
                "triggered": False,
                "status": "skipped",
                "reason": "no_new_or_changed_news",
                "fresh_event_count": 0,
                "news_ingest": news_ingest,
                "updated_at": _iso_or_none(_utcnow_naive()),
            }
        skip_final_event_state_write = (
            async_event_driven_selection
            and fresh_event_count > 0
            and str((event_driven_selection or {}).get("status") or "") in {"scheduled", "running"}
        )
        if not skip_final_event_state_write:
            _record_sync_state(
                db,
                status=sync_status,
                last_run_at=run_started_at,
                last_success_at=run_started_at if active_sources else None,
                last_error=sync_last_error,
                active_sources=active_sources,
                tracked_symbols=symbols,
                saved_count=saved,
                new_count=new_count,
                updated_count=updated_count,
                unchanged_count=unchanged_count,
                event_driven_selection=event_driven_selection,
            )
            db.commit()
        return {
            "saved": saved,
            "new": new_count,
            "updated": updated_count,
            "unchanged": unchanged_count,
            "fresh_event_count": fresh_event_count,
            "source": ", ".join(active_sources) if active_sources else "external",
            "fallback": not bool(active_sources),
            "message": sync_last_error or f"资讯刷新完成（{trigger}）",
            "updated_at": _iso_or_none(run_started_at),
            "active_sources": active_sources,
            "tracked_symbols": symbols,
            "warnings": warnings,
            "fresh_news_events": fresh_event_context.get("fresh_news_events") or [],
            "fresh_news_summary": fresh_event_context.get("fresh_news_summary") or {},
            "event_driven_selection": event_driven_selection,
        }
    except Exception as exc:
        db.rollback()
        _record_sync_state(
            db,
            status="error",
            last_run_at=run_started_at,
            last_success_at=None,
            last_error=str(exc),
            active_sources=[],
            tracked_symbols=symbols,
            saved_count=0,
            new_count=0,
            updated_count=0,
            unchanged_count=0,
            event_driven_selection={},
        )
        db.commit()
        raise


def _public_event_driven_selection_state(
    payload: dict[str, Any] | None,
    *,
    trigger: str,
    windows: Iterable[str],
    fresh_event_count: int,
    news_ingest: dict[str, Any],
    fresh_event_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = dict(payload or {})
    errors = list(result.get("errors") or [])
    status = "skipped" if result.get("skipped") else ("partial_failed" if errors else "completed")
    fresh_context = fresh_event_context if isinstance(fresh_event_context, dict) else {}
    result.update(
        {
            "trigger": result.get("trigger") or trigger,
            "triggered": True,
            "status": status,
            "windows": [str(window) for window in windows],
            "fresh_event_count": int(fresh_event_count or 0),
            "news_ingest": news_ingest,
            "fresh_news_events": fresh_context.get("fresh_news_events") or result.get("fresh_news_events") or [],
            "fresh_news_summary": fresh_context.get("fresh_news_summary") or result.get("fresh_news_summary") or {},
            "updated_at": result.get("updated_at") or _iso_or_none(_utcnow_naive()),
        }
    )
    return result


def _event_selection_refresh_key(*, trigger: str, windows: Iterable[str], limit: int, user_id: str | None) -> str:
    window_key = ",".join(str(window).strip() for window in windows if str(window).strip())
    return f"{trigger}:{window_key}:{int(limit or 0)}:{user_id or ''}"


def _schedule_event_driven_selection_refresh(
    *,
    trigger: str,
    windows: Iterable[str],
    limit: int,
    user_id: str | None,
    fresh_event_count: int,
    news_ingest: dict[str, Any],
    fresh_event_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_windows = tuple(str(window).strip() for window in windows if str(window).strip())
    from api.services import catalyst_selection_service
    fresh_context = fresh_event_context if isinstance(fresh_event_context, dict) else {}

    scheduled = catalyst_selection_service.schedule_event_driven_selection_refresh(
        trigger=trigger,
        windows=normalized_windows,
        limit=limit,
        user_id=user_id,
        reason="news_eye_fresh_events",
        context={
            "source": "news_eye",
            "fresh_event_count": int(fresh_event_count or 0),
            "news_ingest": news_ingest,
            **fresh_context,
        },
    )
    status = str(scheduled.get("status") or "scheduled")
    return {
        "refresh_key": scheduled.get("refresh_key"),
        "trigger": trigger,
        "generated": list(scheduled.get("generated") or []),
        "errors": list(scheduled.get("errors") or []),
        "skipped": bool(scheduled.get("skipped")),
        "triggered": True,
        "status": status,
        "deduped": bool(scheduled.get("deduped")),
        "reason": scheduled.get("reason"),
        "windows": list(normalized_windows),
        "fresh_event_count": int(fresh_event_count or 0),
        "news_ingest": news_ingest,
        "fresh_news_events": fresh_context.get("fresh_news_events") or [],
        "fresh_news_summary": fresh_context.get("fresh_news_summary") or {},
        "updated_at": scheduled.get("updated_at") or _iso_or_none(_utcnow_naive()),
    }


def _run_event_driven_selection_refresh_task(
    *,
    refresh_key: str,
    trigger: str,
    windows: tuple[str, ...],
    limit: int,
    user_id: str | None,
    fresh_event_count: int,
    news_ingest: dict[str, Any],
    fresh_event_context: dict[str, Any] | None = None,
) -> None:
    fresh_context = fresh_event_context if isinstance(fresh_event_context, dict) else {}
    try:
        with SessionLocal() as db:
            from api.services import catalyst_selection_service

            refreshed = catalyst_selection_service.refresh_event_driven_selection(
                db,
                trigger=trigger,
                windows=windows,
                limit=limit,
                user_id=user_id,
                trigger_context={
                    "source": "news_eye",
                    "trigger": trigger,
                    "reason": "news_eye_fresh_events",
                    "fresh_event_count": int(fresh_event_count or 0),
                    "news_ingest": news_ingest,
                    **fresh_context,
                },
            )
            event_driven_selection = _public_event_driven_selection_state(
                refreshed,
                trigger=trigger,
                windows=windows,
                fresh_event_count=fresh_event_count,
                news_ingest=news_ingest,
                fresh_event_context=fresh_context,
            )
    except Exception as exc:
        logger.exception("[news-eye] async event-driven selection refresh failed trigger=%s", trigger)
        event_driven_selection = {
            "trigger": trigger,
            "generated": [],
            "errors": [{"window": ",".join(windows), "error": str(exc)}],
            "skipped": False,
            "triggered": True,
            "status": "failed",
            "windows": list(windows),
            "fresh_event_count": int(fresh_event_count or 0),
            "news_ingest": news_ingest,
            "fresh_news_events": fresh_context.get("fresh_news_events") or [],
            "fresh_news_summary": fresh_context.get("fresh_news_summary") or {},
            "updated_at": _iso_or_none(_utcnow_naive()),
        }
    finally:
        with _EVENT_SELECTION_REFRESH_LOCK:
            _EVENT_SELECTION_REFRESH_TASKS.discard(refresh_key)
            _EVENT_SELECTION_REFRESH_PENDING.pop(refresh_key, None)
    _record_event_driven_selection_state(event_driven_selection)


def _record_event_driven_selection_state(event_driven_selection: dict[str, Any]) -> None:
    try:
        with SessionLocal() as db:
            ensure_news_tables(db)
            db.execute(
                text(
                    """
                    INSERT INTO market_news_sync_state (
                        worker_name, status, active_sources_json, tracked_symbols_json,
                        saved_count, new_count, updated_count, unchanged_count, fresh_event_count,
                        event_selection_json, updated_at
                    )
                    VALUES (
                        :worker_name, 'success', '[]', '[]',
                        0, 0, 0, 0, :fresh_event_count,
                        :event_selection_json, :updated_at
                    )
                    ON CONFLICT (worker_name) DO UPDATE SET
                        event_selection_json = EXCLUDED.event_selection_json,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "worker_name": _SYNC_STATE_KEY,
                    "fresh_event_count": int(event_driven_selection.get("fresh_event_count") or 0),
                    "event_selection_json": json.dumps(event_driven_selection or {}, ensure_ascii=False, default=str),
                    "updated_at": _utcnow_naive(),
                },
            )
            db.commit()
    except Exception:
        logger.exception("[news-eye] failed to record async event-driven selection state")


def load_user_focus_symbols(db: Session, user_id: str, *, limit: int = _WATCHLIST_SYMBOL_LIMIT) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()

    watchlist_rows = (
        db.query(WatchlistItemDB.symbol)
        .filter(WatchlistItemDB.user_id == user_id)
        .order_by(WatchlistItemDB.created_at.desc())
        .all()
    )
    scheduled_rows = (
        db.query(ScheduledAnalysisDB.symbol)
        .filter(ScheduledAnalysisDB.user_id == user_id)
        .order_by(ScheduledAnalysisDB.created_at.desc())
        .all()
    )
    for row in list(watchlist_rows) + list(scheduled_rows):
        symbol = str(row[0] or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def _load_background_refresh_user_id(db: Session) -> str | None:
    row = db.execute(
        text(
            """
            SELECT u.id
            FROM users u
            WHERE u.is_active = true
              AND (
                EXISTS (SELECT 1 FROM watchlist_items w WHERE w.user_id = u.id)
                OR EXISTS (
                    SELECT 1
                    FROM scheduled_analyses s
                    WHERE s.user_id = u.id
                      AND COALESCE(s.is_active, true) = true
                )
                OR EXISTS (SELECT 1 FROM user_llm_configs c WHERE c.user_id = u.id)
              )
            ORDER BY u.last_login_at DESC NULLS LAST, u.created_at DESC NULLS LAST
            LIMIT 1
            """
        )
    ).mappings().first()
    return str(row["id"]) if row and row.get("id") else None


def _load_global_focus_symbols(db: Session, *, limit: int) -> list[str]:
    if limit <= 0:
        return []
    symbols: list[str] = []
    seen: set[str] = set()
    rows = (
        db.query(WatchlistItemDB.symbol)
        .order_by(WatchlistItemDB.created_at.desc())
        .limit(limit * 3)
        .all()
    )
    scheduled_rows = (
        db.query(ScheduledAnalysisDB.symbol)
        .order_by(ScheduledAnalysisDB.created_at.desc())
        .limit(limit * 3)
        .all()
    )
    for row in list(rows) + list(scheduled_rows):
        symbol = str(row[0] or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def _dedupe_symbols(symbols: Iterable[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result


def _fetch_external_news(limit: int, *, symbols: list[str]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    import akshare as ak

    items: list[dict[str, Any]] = []
    active_sources: list[str] = []
    warnings: list[str] = []
    per_source_limit = max(10, min(limit, 60))

    for spec in GENERAL_SOURCE_SPECS:
        func = getattr(ak, spec.func_name, None)
        if func is None:
            warnings.append(f"{spec.label} 接口不存在")
            continue
        try:
            frame = _call_news_source(func, spec.kwargs, label=spec.label)
            normalized = _normalize_news_frame(frame, spec.label, limit=per_source_limit)
            normalized = [item for item in normalized if _is_a_share_relevant_news(item)]
            if normalized:
                items.extend(normalized)
                active_sources.append(spec.label)
            else:
                warnings.append(f"{spec.label} 暂无高相关资讯")
        except Exception as exc:
            warnings.append(f"{spec.label} 拉取失败: {exc}")

    for symbol in symbols[:_WATCHLIST_SYMBOL_LIMIT]:
        for spec in SYMBOL_SOURCE_SPECS:
            func = getattr(ak, spec.func_name, None)
            if func is None or spec.symbol_param is None:
                continue
            call_kwargs = dict(spec.kwargs)
            transformed_symbol = spec.symbol_transform(symbol) if callable(spec.symbol_transform) else symbol
            call_kwargs[spec.symbol_param] = transformed_symbol
            try:
                active_label = f"{spec.label}:{symbol}"
                frame = _call_news_source(func, call_kwargs, label=active_label)
                normalized = _normalize_news_frame(
                    frame,
                    spec.label,
                    limit=_SYMBOL_SOURCE_LIMIT,
                    seed_symbols=[symbol],
                )
                if normalized:
                    items.extend(normalized)
                    if active_label not in active_sources:
                        active_sources.append(active_label)
            except Exception as exc:
                warnings.append(f"{spec.label}({symbol}) 拉取失败: {exc}")

    return _dedupe_items(items)[: max(limit, 20)], active_sources, warnings


def _call_news_source(func: Any, kwargs: dict[str, Any], *, label: str) -> Any:
    future = _SOURCE_FETCH_EXECUTOR.submit(func, **dict(kwargs or {}))
    try:
        return future.result(timeout=_SOURCE_TIMEOUT_SECONDS)
    except FutureTimeoutError as exc:
        future.cancel()
        raise TimeoutError(f"{label} 拉取超过 {_SOURCE_TIMEOUT_SECONDS:g}s") from exc


def _sync_state_error_from_warnings(warnings: list[str], active_sources: list[str]) -> str | None:
    if not warnings:
        return None
    if not active_sources:
        return "；".join(warnings[:5])
    important_warnings = [warning for warning in warnings if not _is_noncritical_news_warning(warning)]
    return "；".join(important_warnings[:5]) if important_warnings else None


def _is_noncritical_news_warning(warning: str) -> bool:
    text_value = str(warning or "")
    return "个股新闻(" in text_value or "暂无高相关资讯" in text_value or "拉取超过" in text_value


def _normalize_news_frame(
    frame: Any,
    source_name: str,
    *,
    limit: int,
    seed_symbols: list[str] | None = None,
) -> list[dict[str, Any]]:
    if frame is None:
        return []
    data = pd.DataFrame(frame)
    if data.empty:
        return []
    items: list[dict[str, Any]] = []
    for _, row in data.head(limit).iterrows():
        title = _first_text(row, ("标题", "title", "新闻标题"))
        body = _first_text(row, ("内容", "摘要", "summary", "digest", "rich_text", "content", "正文"))
        content = body or title
        if title and body and title not in body:
            content = f"{title}；{body}"
        if not content:
            continue
        published_at = _parse_time_from_row(row)
        source = _first_text(row, ("来源", "媒体", "source")) or source_name
        url = _first_text(row, ("链接", "url", "URL", "uniqueUrl", "detailUrl"))
        items.append(
            {
                "content": content,
                "published_at": published_at,
                "source": source,
                "url": url or None,
                "seed_symbols": list(seed_symbols or []),
            }
        )
    return items


def _is_a_share_relevant_news(item: dict[str, Any]) -> bool:
    content = str(item.get("content") or "").strip()
    if not content:
        return False
    if item.get("seed_symbols"):
        return True
    if _extract_symbols(content) or _extract_sectors(content):
        return True
    if any(keyword in content for keyword in A_SHARE_MARKET_KEYWORDS):
        return True
    if any(keyword in content for keyword in GLOBAL_IMPACT_KEYWORDS):
        return True
    if any(keyword in content.upper() for keyword in ("SH", "SZ", "BJ")) and any(char.isdigit() for char in content):
        return True
    if any(keyword in content for keyword in OVERSEAS_NOISE_KEYWORDS):
        return False
    return False


def _first_text(row: Any, keys: tuple[str, ...]) -> str:
    for key in keys:
        if key in row and pd.notna(row[key]):
            value = str(row[key]).strip()
            if value and value.lower() != "nan":
                return value
    return ""


def _parse_time_from_row(row: Any) -> str:
    if "发布日期" in row and "发布时间" in row and pd.notna(row["发布日期"]) and pd.notna(row["发布时间"]):
        combined = f"{row['发布日期']} {row['发布时间']}"
        return _parse_time(str(combined))
    return _parse_time(
        _first_text(row, ("发布时间", "时间", "发布日期", "datetime", "time", "date", "showTime", "create_time"))
    )


def _parse_time(value: str) -> str:
    if not value:
        return _iso_or_none(_utcnow_naive())
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return _iso_or_none(_utcnow_naive())
    return parsed.to_pydatetime().replace(tzinfo=None).isoformat()


def _dedupe_items(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _news_identity_key(item)
        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = item
            continue
        best_by_key[key] = _merge_duplicate_news_item(existing, item)
    return sorted(
        best_by_key.values(),
        key=lambda item: (str(item.get("published_at") or ""), str(item.get("source") or "")),
        reverse=True,
    )


def _news_identity_key(item: dict[str, Any]) -> str:
    return f"dedupe:{_make_news_dedupe_key(item)}"


def _merge_duplicate_news_item(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    preferred = dict(incoming if _news_preference_key(incoming) > _news_preference_key(existing) else existing)
    preferred["seed_symbols"] = _merge_symbols(
        list(existing.get("seed_symbols") or []),
        list(incoming.get("seed_symbols") or []),
    )
    if not preferred.get("url"):
        preferred["url"] = existing.get("url") or incoming.get("url")
    return preferred


def _news_preference_key(item: dict[str, Any]) -> tuple[int, int, str]:
    content = str(item.get("content") or "")
    return (
        1 if str(item.get("url") or "").strip() else 0,
        len(content),
        str(item.get("published_at") or ""),
    )


def _make_news_dedupe_key(item: dict[str, Any]) -> str:
    canonical_content = _canonicalize_news_content(str(item.get("content") or ""))
    published_date = str(item.get("published_at") or "")[:10]
    if canonical_content:
        return hashlib.sha256(f"text:{published_date}:{canonical_content}".encode("utf-8")).hexdigest()
    url = str(item.get("url") or "").strip()
    if url:
        return hashlib.sha256(f"url:{url}".encode("utf-8")).hexdigest()
    return hashlib.sha256(json.dumps(item, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _canonicalize_news_content(content: str) -> str:
    text_value = str(content or "").strip().lower()
    if not text_value:
        return ""
    return _NEWS_CONTENT_DUPE_STRIP_PATTERN.sub("", text_value)[:600]


def _enrich_news_item(item: dict[str, Any]) -> dict[str, Any]:
    content = str(item.get("content") or "").strip()
    lookup_index = _get_symbol_lookup_index()
    related_symbols = _extract_symbols(content, lookup_index=lookup_index)
    symbols = _merge_symbols(related_symbols, item.get("seed_symbols") or [])
    positive_sectors, negative_sectors, positive_symbols, negative_symbols, sentiment = _extract_impact_payload(
        content,
        seed_symbols=item.get("seed_symbols") or [],
        related_symbols=related_symbols,
        lookup_index=lookup_index,
    )
    digest = _make_news_digest(item)
    return {
        "digest": digest,
        "dedupe_key": _make_news_dedupe_key(item),
        "content": content,
        "published_at": item.get("published_at") or _iso_or_none(_utcnow_naive()),
        "source": item.get("source") or "未知来源",
        "url": item.get("url"),
        "sentiment": sentiment,
        "positive_sectors_json": json.dumps(positive_sectors, ensure_ascii=False),
        "negative_sectors_json": json.dumps(negative_sectors, ensure_ascii=False),
        "positive_symbols_json": json.dumps(symbols_to_payload(positive_symbols), ensure_ascii=False),
        "negative_symbols_json": json.dumps(symbols_to_payload(negative_symbols), ensure_ascii=False),
        "related_symbols_json": json.dumps(symbols_to_payload(symbols), ensure_ascii=False),
        "fetched_at": _iso_or_none(_utcnow_naive()),
    }


def _public_fresh_news_event(enriched: dict[str, Any], *, change_type: str) -> dict[str, Any]:
    content = str(enriched.get("content") or "").strip()
    positive_symbols = _loads(enriched.get("positive_symbols_json"))
    related_symbols = _loads(enriched.get("related_symbols_json"))
    positive_sectors = _loads(enriched.get("positive_sectors_json"))
    negative_sectors = _loads(enriched.get("negative_sectors_json"))
    symbols = positive_symbols if positive_symbols else related_symbols
    return {
        "digest": str(enriched.get("digest") or "")[:64],
        "dedupe_key": str(enriched.get("dedupe_key") or "")[:80],
        "change_type": str(change_type or "new"),
        "source": str(enriched.get("source") or "")[:80],
        "published_at": _iso_or_none(enriched.get("published_at")),
        "sentiment": str(enriched.get("sentiment") or "neutral"),
        "content": content[:180],
        "positive_sectors": _string_list(positive_sectors),
        "negative_sectors": _string_list(negative_sectors),
        "symbols": _symbol_labels(symbols),
    }


def _build_fresh_event_context(events: list[dict[str, Any]], *, limit: int = 8) -> dict[str, Any]:
    compact_events = [event for event in events if isinstance(event, dict)][: max(0, int(limit or 0))]
    if not compact_events:
        return {"fresh_news_events": [], "fresh_news_summary": {"event_count": 0}}
    source_counts: dict[str, int] = {}
    sector_counts: dict[str, int] = {}
    symbol_counts: dict[str, int] = {}
    sentiment_counts: dict[str, int] = {}
    for event in compact_events:
        source = str(event.get("source") or "unknown")
        sentiment = str(event.get("sentiment") or "neutral")
        source_counts[source] = source_counts.get(source, 0) + 1
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        for sector in [*event.get("positive_sectors", []), *event.get("negative_sectors", [])]:
            key = str(sector or "").strip()
            if key:
                sector_counts[key] = sector_counts.get(key, 0) + 1
        for symbol in event.get("symbols", []):
            key = str(symbol.get("symbol") if isinstance(symbol, dict) else symbol or "").strip()
            if key:
                symbol_counts[key] = symbol_counts.get(key, 0) + 1
    return {
        "fresh_news_events": compact_events,
        "fresh_news_summary": {
            "event_count": len(events),
            "included_count": len(compact_events),
            "source_counts": source_counts,
            "sentiment_counts": sentiment_counts,
            "top_sectors": sorted(sector_counts, key=sector_counts.get, reverse=True)[:6],
            "top_symbols": sorted(symbol_counts, key=symbol_counts.get, reverse=True)[:8],
        },
    }


def _load_existing_news_item_for_dedupe(db: Session, dedupe_key: str) -> dict[str, Any] | None:
    if not dedupe_key:
        return None
    row = db.execute(
        text(
            """
            SELECT digest, content, published_at, source, url, sentiment,
                   positive_sectors_json, negative_sectors_json,
                   positive_symbols_json, negative_symbols_json,
                   related_symbols_json
            FROM market_news_items
            WHERE dedupe_key = :dedupe_key
            LIMIT 1
            """
        ),
        {"dedupe_key": dedupe_key},
    ).mappings().first()
    return dict(row) if row else None


def _news_material_changed(existing: dict[str, Any], incoming: dict[str, Any]) -> bool:
    scalar_keys = ("content", "source", "url", "sentiment")
    for key in scalar_keys:
        if str(existing.get(key) or "") != str(incoming.get(key) or ""):
            return True
    if _iso_or_none(existing.get("published_at")) != _iso_or_none(incoming.get("published_at")):
        return True
    json_keys = (
        "positive_sectors_json",
        "negative_sectors_json",
        "positive_symbols_json",
        "negative_symbols_json",
        "related_symbols_json",
    )
    return any(
        _canonical_json_text(existing.get(key)) != _canonical_json_text(incoming.get(key))
        for key in json_keys
    )


def _canonical_json_text(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return value.strip()
    else:
        parsed = value
    try:
        return json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(parsed)


def _classify_sentiment(content: str) -> str:
    score = _score_sentiment_text(content)
    if score > 0:
        return "positive"
    if score < 0:
        return "negative"
    return "neutral"


def _symbol_lookup_source_token() -> tuple[int, int]:
    return get_stock_map_version(), id(get_reverse_stock_map)


def _get_symbol_lookup_index() -> SymbolLookupIndex:
    global _SYMBOL_LOOKUP_INDEX

    source_token = _symbol_lookup_source_token()
    cached = _SYMBOL_LOOKUP_INDEX
    if cached is not None and cached.source_token == source_token:
        return cached

    reverse_map = get_reverse_stock_map()
    code_to_symbol: dict[str, str] = {}
    name_prefix_candidates: dict[str, list[tuple[str, str]]] = {}

    for symbol, raw_name in reverse_map.items():
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            continue
        code = normalized_symbol.split(".", 1)[0]
        if len(code) == 6 and code.isdigit():
            code_to_symbol[code] = normalized_symbol

        name = str(raw_name or "").strip()
        if len(name) < _SYMBOL_PREFIX_LENGTH:
            continue
        prefix = name[:_SYMBOL_PREFIX_LENGTH]
        name_prefix_candidates.setdefault(prefix, []).append((name, normalized_symbol))

    frozen_name_prefix_candidates = {
        prefix: tuple(sorted(candidates, key=lambda item: (-len(item[0]), item[0], item[1])))
        for prefix, candidates in name_prefix_candidates.items()
    }
    _SYMBOL_LOOKUP_INDEX = SymbolLookupIndex(
        source_token=source_token,
        code_to_symbol=code_to_symbol,
        name_prefix_candidates=frozen_name_prefix_candidates,
    )
    return _SYMBOL_LOOKUP_INDEX


def _append_symbol_hit(hits: list[str], seen: set[str], symbol: str) -> bool:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol or normalized_symbol in seen:
        return False
    seen.add(normalized_symbol)
    hits.append(normalized_symbol)
    return len(hits) >= 8


def _extract_symbols(content: str, *, lookup_index: SymbolLookupIndex | None = None) -> list[str]:
    text = str(content or "").strip()
    if not text:
        return []

    lookup = lookup_index or _get_symbol_lookup_index()
    hits: list[str] = []
    seen: set[str] = set()

    for match in _A_SHARE_CODE_PATTERN.finditer(text):
        symbol = lookup.code_to_symbol.get(match.group(1))
        if symbol and _append_symbol_hit(hits, seen, symbol):
            return hits

    seen_prefixes: set[str] = set()
    for fragment in _SYMBOL_NAME_FRAGMENT_PATTERN.findall(text):
        if len(fragment) < _SYMBOL_PREFIX_LENGTH:
            continue
        for index in range(len(fragment) - _SYMBOL_PREFIX_LENGTH + 1):
            prefix = fragment[index:index + _SYMBOL_PREFIX_LENGTH]
            if prefix in seen_prefixes:
                continue
            seen_prefixes.add(prefix)
            candidates = lookup.name_prefix_candidates.get(prefix)
            if not candidates:
                continue
            for name, symbol in candidates:
                if name in text and _append_symbol_hit(hits, seen, symbol):
                    return hits

    return hits


def _extract_sectors(content: str) -> list[str]:
    hits: list[str] = []
    for sector in SECTOR_KEYWORDS:
        if sector in content:
            hits.append(sector)
    return hits


def _score_sentiment_text(content: str) -> int:
    text = str(content or "").strip()
    if not text:
        return 0
    score = 0
    for phrase, weight in POSITIVE_PHRASES:
        if phrase in text:
            score += weight
    for phrase, weight in NEGATIVE_PHRASES:
        if phrase in text:
            score -= weight
    for keyword in POSITIVE_KEYWORDS:
        if keyword in text:
            score += 1
    for keyword in NEGATIVE_KEYWORDS:
        if keyword in text:
            score -= 1
    return score


def _split_content_clauses(content: str) -> list[str]:
    text = str(content or "").strip()
    if not text:
        return []
    clauses = [part.strip(" ，,") for part in _CLAUSE_SPLIT_PATTERN.split(text) if part.strip(" ，,")]
    return clauses or [text]


def _extract_impact_payload(
    content: str,
    *,
    seed_symbols: Iterable[str] | None = None,
    related_symbols: Iterable[str] | None = None,
    lookup_index: SymbolLookupIndex | None = None,
) -> tuple[list[str], list[str], list[str], list[str], str]:
    clauses = _split_content_clauses(content)
    resolved_lookup_index = lookup_index or _get_symbol_lookup_index()
    clause_symbol_cache: dict[str, list[str]] = {}
    positive_score = 0
    negative_score = 0
    positive_sectors: list[str] = []
    negative_sectors: list[str] = []
    positive_symbols: list[str] = []
    negative_symbols: list[str] = []

    for clause in clauses:
        clause_score = _score_sentiment_text(clause)
        if clause_score == 0:
            continue
        clause_sectors = _extract_sectors(clause)
        clause_symbols = clause_symbol_cache.get(clause)
        if clause_symbols is None:
            clause_symbols = _extract_symbols(clause, lookup_index=resolved_lookup_index)
            clause_symbol_cache[clause] = clause_symbols
        if clause_score > 0:
            positive_score += clause_score
            positive_sectors = _merge_symbols(positive_sectors, clause_sectors)
            positive_symbols = _merge_symbols(positive_symbols, clause_symbols)
        else:
            negative_score += abs(clause_score)
            negative_sectors = _merge_symbols(negative_sectors, clause_sectors)
            negative_symbols = _merge_symbols(negative_symbols, clause_symbols)

    merged_related_symbols = _merge_symbols(
        list(related_symbols) if related_symbols is not None else _extract_symbols(content, lookup_index=resolved_lookup_index),
        seed_symbols or [],
    )
    net_score = positive_score - negative_score
    if net_score > 0 and not positive_symbols:
        positive_symbols = _merge_symbols(positive_symbols, merged_related_symbols)
    if net_score < 0 and not negative_symbols:
        negative_symbols = _merge_symbols(negative_symbols, merged_related_symbols)

    if positive_score > 0 and negative_score > 0 and abs(net_score) <= 2:
        sentiment = "neutral"
    elif net_score > 0:
        sentiment = "positive"
    elif net_score < 0:
        sentiment = "negative"
    else:
        sentiment = "neutral"

    return positive_sectors, negative_sectors, positive_symbols, negative_symbols, sentiment


_NEWS_EYE_ANALYSIS_SYSTEM_PROMPT = """你是 A 股资讯研判助手。你需要把一条市场资讯压缩成适合交易员快速浏览的结构化结论。

输出要求：
1. 只返回 JSON，不要输出解释、前后缀或 Markdown。
2. JSON 字段必须包含：
   summary, sentiment, sentiment_reason, positive_sectors, negative_sectors, positive_symbols, negative_symbols, trading_takeaway
3. sentiment 只能是 positive、negative、neutral 之一。
4. summary 和 trading_takeaway 都要简洁，适合页面卡片直接展示。
5. 如果无法确定，不要编造，返回空数组或 neutral。"""


def _build_news_analysis_prompt(payload: dict[str, Any], heuristic: dict[str, Any]) -> str:
    compact_payload = {
        "source": str(payload.get("source") or "").strip(),
        "published_at": payload.get("published_at"),
        "content": str(payload.get("content") or "").strip(),
        "heuristic_tags": heuristic,
    }
    return (
        "请分析下面这条 A 股市场资讯，判断它更偏利好、利空还是中性，并提炼受影响的板块与个股。\n"
        "已有标签只是规则提取结果，可参考但不要盲从。\n"
        f"{json.dumps(compact_payload, ensure_ascii=False)}"
    )


def _parse_news_analysis_payload(raw: str) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"\s*```$", "", text, flags=re.MULTILINE)
    candidates = [text]
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        cleaned = re.sub(r",\s*([\]}])", r"\1", candidate.strip())
        try:
            parsed = json.loads(cleaned)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        text_value = str(item or "").strip()
        if not text_value or text_value in seen:
            continue
        seen.add(text_value)
        result.append(text_value)
    return result[:8]


def _symbol_labels(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    labels: list[str] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, dict):
            symbol = str(item.get("symbol") or "").strip().upper()
            name = str(item.get("name") or "").strip()
            label = f"{name}({symbol})" if symbol and name and name != symbol else (name or symbol)
        else:
            label = str(item or "").strip()
        if not label or label in seen:
            continue
        seen.add(label)
        labels.append(label)
    return labels[:8]


def _normalize_sentiment_label(value: Any, *, fallback: str = "neutral") -> str:
    text_value = str(value or "").strip().lower()
    if text_value in {"positive", "negative", "neutral"}:
        return text_value
    if text_value in {"bullish", "利好"}:
        return "positive"
    if text_value in {"bearish", "利空"}:
        return "negative"
    if text_value in {"中性"}:
        return "neutral"
    return fallback if fallback in {"positive", "negative", "neutral"} else "neutral"


def _merge_symbols(primary: Iterable[str], extra: Iterable[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in list(primary) + list(extra):
        symbol = str(raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        merged.append(symbol)
    return merged[:8]


def symbols_to_payload(symbols: list[str]) -> list[dict[str, str]]:
    code_to_name = get_reverse_stock_map()
    return [{"symbol": symbol, "name": code_to_name.get(symbol, symbol)} for symbol in symbols]


def _replace_news_search_index(db: Session, enriched: dict[str, Any]) -> None:
    digest = str(enriched.get("digest") or "").strip()
    if not digest:
        return

    positive_symbols = _loads(enriched.get("positive_symbols_json"))
    negative_symbols = _loads(enriched.get("negative_symbols_json"))
    related_symbols = _loads(enriched.get("related_symbols_json"))
    positive_sectors = _loads(enriched.get("positive_sectors_json"))
    negative_sectors = _loads(enriched.get("negative_sectors_json"))

    db.execute(text("DELETE FROM market_news_item_symbols WHERE digest = :digest"), {"digest": digest})
    db.execute(text("DELETE FROM market_news_item_sectors WHERE digest = :digest"), {"digest": digest})

    seen_symbol_rows: set[tuple[str, str]] = set()
    for tag_group, rows in (
        ("positive", positive_symbols),
        ("negative", negative_symbols),
        ("related", related_symbols),
    ):
        for row in rows if isinstance(rows, list) else []:
            symbol = str((row or {}).get("symbol") or "").strip().upper()
            if not symbol:
                continue
            row_key = (symbol, tag_group)
            if row_key in seen_symbol_rows:
                continue
            seen_symbol_rows.add(row_key)
            db.execute(
                text(
                    """
                    INSERT INTO market_news_item_symbols (digest, symbol, name, tag_group)
                    VALUES (:digest, :symbol, :name, :tag_group)
                    """
                ),
                {
                    "digest": digest,
                    "symbol": symbol,
                    "name": str((row or {}).get("name") or "").strip() or None,
                    "tag_group": tag_group,
                },
            )

    seen_sector_rows: set[tuple[str, str]] = set()
    for tag_group, sectors in (
        ("positive", positive_sectors),
        ("negative", negative_sectors),
    ):
        for sector in sectors if isinstance(sectors, list) else []:
            sector_text = str(sector or "").strip()
            if not sector_text:
                continue
            row_key = (sector_text, tag_group)
            if row_key in seen_sector_rows:
                continue
            seen_sector_rows.add(row_key)
            db.execute(
                text(
                    """
                    INSERT INTO market_news_item_sectors (digest, sector, tag_group)
                    VALUES (:digest, :sector, :tag_group)
                    """
                ),
                {
                    "digest": digest,
                    "sector": sector_text,
                    "tag_group": tag_group,
                },
            )


def _backfill_news_dedupe_keys_if_needed(db: Session) -> None:
    rows = db.execute(
        text(
            """
            SELECT digest, content, published_at, source, url
            FROM market_news_items
            WHERE dedupe_key IS NULL
            """
        )
    ).mappings().all()
    if not rows:
        return

    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        row_dict = dict(row)
        groups.setdefault(_make_news_dedupe_key(row_dict), []).append(row_dict)

    existing_rows = db.execute(
        text(
            """
            SELECT dedupe_key, digest
            FROM market_news_items
            WHERE dedupe_key IS NOT NULL
            """
        )
    ).mappings().all()
    existing_by_key = {
        str(row["dedupe_key"]): str(row["digest"])
        for row in existing_rows
        if row.get("dedupe_key") and row.get("digest")
    }
    update_rows: list[dict[str, str]] = []
    delete_rows: list[dict[str, str]] = []
    for dedupe_key, group in groups.items():
        existing_digest = existing_by_key.get(dedupe_key)
        if existing_digest:
            keep_digest = existing_digest
        else:
            keep_digest = str(max(group, key=_news_preference_key)["digest"])
            update_rows.append({"dedupe_key": dedupe_key, "digest": keep_digest})
            existing_by_key[dedupe_key] = keep_digest

        for row in group:
            digest = str(row.get("digest") or "")
            if digest and digest != keep_digest:
                delete_rows.append({"digest": digest})

    if update_rows:
        db.execute(
            text("UPDATE market_news_items SET dedupe_key = :dedupe_key WHERE digest = :digest"),
            update_rows,
        )
    if delete_rows:
        db.execute(text("DELETE FROM market_news_items WHERE digest = :digest"), delete_rows)

    db.commit()


def _backfill_news_search_index_if_needed(db: Session) -> None:
    total_row = db.execute(text("SELECT COUNT(*) AS total_count FROM market_news_items")).mappings().first() or {}
    total_items = int(total_row.get("total_count") or 0)
    if total_items <= 0:
        return

    indexed_row = db.execute(text("SELECT COUNT(DISTINCT digest) AS indexed_count FROM market_news_item_symbols")).mappings().first() or {}
    indexed_items = int(indexed_row.get("indexed_count") or 0)
    sector_indexed_row = db.execute(text("SELECT COUNT(DISTINCT digest) AS indexed_count FROM market_news_item_sectors")).mappings().first() or {}
    sector_indexed_items = int(sector_indexed_row.get("indexed_count") or 0)
    if indexed_items > 0 or sector_indexed_items > 0:
        return

    rows = db.execute(
        text(
            """
            SELECT digest, positive_sectors_json, negative_sectors_json,
                   positive_symbols_json, negative_symbols_json, related_symbols_json
            FROM market_news_items
            """
        )
    ).mappings().all()
    for row in rows:
        _replace_news_search_index(db, row)
    db.commit()


def _row_to_news_item(row: Any) -> dict[str, Any]:
    return {
        "id": row["digest"],
        "content": row["content"],
        "published_at": _iso_or_none(row["published_at"]),
        "source": row["source"],
        "url": row["url"],
        "sentiment": row["sentiment"],
        "positive_sectors": _loads(row["positive_sectors_json"]),
        "negative_sectors": _loads(row["negative_sectors_json"]),
        "positive_symbols": _loads(row["positive_symbols_json"]),
        "negative_symbols": _loads(row["negative_symbols_json"]),
        "related_symbols": _loads(row["related_symbols_json"]),
        "fetched_at": _iso_or_none(row["fetched_at"]),
    }


def _make_news_digest(item: dict[str, Any]) -> str:
    return _make_news_dedupe_key(item)


def _record_sync_state(
    db: Session,
    *,
    status: str,
    last_run_at: datetime | None,
    last_success_at: datetime | None,
    last_error: str | None,
    active_sources: list[str],
    tracked_symbols: list[str],
    saved_count: int,
    new_count: int,
    updated_count: int,
    unchanged_count: int,
    event_driven_selection: dict[str, Any] | None = None,
) -> None:
    fresh_event_count = int(new_count or 0) + int(updated_count or 0)
    db.execute(
        text(
            """
            INSERT INTO market_news_sync_state (
                worker_name, status, last_run_at, last_success_at, last_error,
                active_sources_json, tracked_symbols_json, saved_count,
                new_count, updated_count, unchanged_count, fresh_event_count,
                event_selection_json, updated_at
            )
            VALUES (
                :worker_name, :status, :last_run_at, :last_success_at, :last_error,
                :active_sources_json, :tracked_symbols_json, :saved_count,
                :new_count, :updated_count, :unchanged_count, :fresh_event_count,
                :event_selection_json, :updated_at
            )
            ON CONFLICT (worker_name) DO UPDATE SET
                status = EXCLUDED.status,
                last_run_at = EXCLUDED.last_run_at,
                last_success_at = COALESCE(EXCLUDED.last_success_at, market_news_sync_state.last_success_at),
                last_error = EXCLUDED.last_error,
                active_sources_json = EXCLUDED.active_sources_json,
                tracked_symbols_json = EXCLUDED.tracked_symbols_json,
                saved_count = EXCLUDED.saved_count,
                new_count = EXCLUDED.new_count,
                updated_count = EXCLUDED.updated_count,
                unchanged_count = EXCLUDED.unchanged_count,
                fresh_event_count = EXCLUDED.fresh_event_count,
                event_selection_json = EXCLUDED.event_selection_json,
                updated_at = EXCLUDED.updated_at
            """
        ),
        {
            "worker_name": _SYNC_STATE_KEY,
            "status": status,
            "last_run_at": last_run_at,
            "last_success_at": last_success_at,
            "last_error": last_error,
            "active_sources_json": json.dumps(active_sources, ensure_ascii=False),
            "tracked_symbols_json": json.dumps(tracked_symbols, ensure_ascii=False),
            "saved_count": int(saved_count or 0),
            "new_count": int(new_count or 0),
            "updated_count": int(updated_count or 0),
            "unchanged_count": int(unchanged_count or 0),
            "fresh_event_count": fresh_event_count,
            "event_selection_json": json.dumps(event_driven_selection or {}, ensure_ascii=False, default=str),
            "updated_at": _utcnow_naive(),
        },
    )


def _load_sync_state(db: Session) -> dict[str, Any]:
    row = db.execute(
        text(
            """
            SELECT worker_name, status, last_run_at, last_success_at, last_error,
                   active_sources_json, tracked_symbols_json, saved_count,
                   new_count, updated_count, unchanged_count, fresh_event_count,
                   event_selection_json, updated_at
            FROM market_news_sync_state
            WHERE worker_name = :worker_name
            """
        ),
        {"worker_name": _SYNC_STATE_KEY},
    ).mappings().first()
    return dict(row) if row else {}


def _loads(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _loads_dict(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _utcnow_naive() -> datetime:
    return datetime.now(CN_TZ).replace(tzinfo=None)


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
