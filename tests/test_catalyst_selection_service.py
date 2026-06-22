from __future__ import annotations

import json
import asyncio
from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

import api.database as database_module
import api.core.strategy_db as strategy_db_module
from api.services import (
    auth_service,
    catalyst_selection_service,
    news_eye_service,
    news_theme_service,
    realtime_monitor_service,
)
from api.services import market_data_pipeline_service
from api.services.strategy_platform_repository import save_platform_strategy
from api.routes.strategy_platform import _default_dsl
from api.models.strategy_models import RealtimeMonitorDB
from tests.postgres_test_utils import isolated_postgres_engine


CN_TZ = ZoneInfo("Asia/Shanghai")


@pytest.fixture
def db(monkeypatch):
    with isolated_postgres_engine(schema_prefix="ta_catalyst_selection") as (test_engine, _schema_url, _schema):
        test_session_local = database_module.sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
        monkeypatch.setattr(database_module, "engine", test_engine)
        monkeypatch.setattr(database_module, "SessionLocal", test_session_local)
        monkeypatch.setattr(market_data_pipeline_service, "engine", test_engine)
        monkeypatch.setattr(market_data_pipeline_service, "SessionLocal", test_session_local, raising=False)
        monkeypatch.setattr(strategy_db_module, "strategy_engine", test_engine)
        monkeypatch.setattr(strategy_db_module, "StrategySessionLocal", test_session_local)
        database_module._init_db_completed_for = None
        strategy_db_module._strategy_schema_ready = False
        database_module.init_db(force=True)
        strategy_db_module.ensure_strategy_schema_ready()
        with test_session_local() as session:
            yield session
    database_module._init_db_completed_for = None
    strategy_db_module._strategy_schema_ready = False


def test_ai_quant_schema_ensure_is_cached_per_database_bind(monkeypatch):
    class FakeDb:
        def __init__(self, bind):
            self._bind = bind

        def get_bind(self):
            return self._bind

    news_seen = set(news_eye_service._NEWS_SCHEMA_ENSURED_BINDS)
    theme_seen = set(news_theme_service._THEME_SCHEMA_ENSURED_BINDS)
    catalyst_seen = set(catalyst_selection_service._CATALYST_SCHEMA_ENSURED_BINDS)
    news_calls: list[int] = []
    theme_calls: list[int] = []
    catalyst_calls: list[int] = []
    try:
        news_eye_service._NEWS_SCHEMA_ENSURED_BINDS.clear()
        news_theme_service._THEME_SCHEMA_ENSURED_BINDS.clear()
        catalyst_selection_service._CATALYST_SCHEMA_ENSURED_BINDS.clear()
        monkeypatch.setattr(
            news_eye_service,
            "_ensure_news_tables_uncached",
            lambda session, *, bind_key: news_calls.append(bind_key),
        )
        monkeypatch.setattr(
            news_theme_service,
            "_ensure_theme_tables_uncached",
            lambda session: theme_calls.append(id(session.get_bind())),
        )
        monkeypatch.setattr(
            catalyst_selection_service,
            "_ensure_catalyst_selection_tables_uncached",
            lambda session: catalyst_calls.append(id(session.get_bind())),
        )

        bind_a = object()
        bind_b = object()
        ensure_calls = (
            news_eye_service.ensure_news_tables,
            news_theme_service.ensure_theme_tables,
            catalyst_selection_service.ensure_catalyst_selection_tables,
        )
        for ensure in ensure_calls:
            ensure(FakeDb(bind_a))
            ensure(FakeDb(bind_a))
            ensure(FakeDb(bind_b))

        assert len(news_calls) == 2
        assert len(theme_calls) == 2
        assert len(catalyst_calls) == 2
    finally:
        news_eye_service._NEWS_SCHEMA_ENSURED_BINDS.clear()
        news_eye_service._NEWS_SCHEMA_ENSURED_BINDS.update(news_seen)
        news_theme_service._THEME_SCHEMA_ENSURED_BINDS.clear()
        news_theme_service._THEME_SCHEMA_ENSURED_BINDS.update(theme_seen)
        catalyst_selection_service._CATALYST_SCHEMA_ENSURED_BINDS.clear()
        catalyst_selection_service._CATALYST_SCHEMA_ENSURED_BINDS.update(catalyst_seen)


def _seed_market_data(db) -> None:
    news_eye_service.ensure_news_tables(db)
    news_theme_service.ensure_theme_tables(db)
    catalyst_selection_service.ensure_catalyst_selection_tables(db)

    db.execute(
        text(
            """
            INSERT INTO stock_daily_kline (
                symbol, trade_date, open, high, low, close, volume, amount, turnover_rate,
                pre_close, float_market_cap, total_market_cap, net_profit_ttm,
                sw_industry_l1, sw_industry_l2, sw_industry_l3, created_at, updated_at
            )
            VALUES
            (:symbol1, :trade_date1, 10.0, 10.8, 9.8, 10.5, 1000000, 15000000, 1.8, 10.0, 100.0, 120.0, 1.2, '电子', '半导体', '芯片', NOW(), NOW()),
            (:symbol2, :trade_date2, 20.0, 20.6, 19.6, 20.4, 800000, 20000000, 1.4, 20.0, 200.0, 180.0, 0.8, '电子', '消费电子', '手机', NOW(), NOW()),
            (:symbol3, :trade_date3, 5.0, 5.2, 4.9, 5.1, 500000, 3000000, 1.0, 5.0, 50.0, 55.0, 0.2, '金融', '银行', '银行', NOW(), NOW())
            """
        ),
        {
            "symbol1": "600584.SH",
            "trade_date1": "2026-05-26",
            "symbol2": "002156.SZ",
            "trade_date2": "2026-05-26",
            "symbol3": "601689.SH",
            "trade_date3": "2026-05-26",
        },
    )
    db.execute(
        text(
            """
            INSERT INTO stock_daily_kline (
                symbol, trade_date, open, high, low, close, volume, amount, turnover_rate,
                pre_close, float_market_cap, total_market_cap, net_profit_ttm,
                sw_industry_l1, sw_industry_l2, sw_industry_l3, created_at, updated_at
            )
            VALUES
            ('600584.SH', '2026-05-25', 9.9, 10.4, 9.8, 10.0, 900000, 12000000, 1.5, 9.8, 98.0, 110.0, 1.0, '电子', '半导体', '芯片', NOW(), NOW()),
            ('002156.SZ', '2026-05-25', 19.8, 20.2, 19.5, 19.9, 700000, 14000000, 1.3, 19.7, 190.0, 175.0, 0.7, '电子', '消费电子', '手机', NOW(), NOW()),
            ('601689.SH', '2026-05-25', 5.1, 5.2, 4.8, 5.0, 450000, 2500000, 0.9, 5.1, 48.0, 54.0, 0.1, '金融', '银行', '银行', NOW(), NOW())
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO stock_daily_kline (
                symbol, trade_date, open, high, low, close, volume, amount, turnover_rate,
                pre_close, float_market_cap, total_market_cap, net_profit_ttm,
                sw_industry_l1, sw_industry_l2, sw_industry_l3, created_at, updated_at
            )
            VALUES
            ('600584.SH', '2026-05-27', 10.6, 11.1, 10.5, 10.95, 1100000, 18000000, 1.9, 10.5, 105.0, 128.0, 1.4, '电子', '半导体', '芯片', NOW(), NOW()),
            ('002156.SZ', '2026-05-27', 20.5, 21.3, 20.2, 21.1, 900000, 24000000, 1.6, 20.4, 205.0, 190.0, 0.85, '电子', '消费电子', '手机', NOW(), NOW()),
            ('601689.SH', '2026-05-27', 5.0, 5.1, 4.7, 4.8, 480000, 2600000, 0.95, 5.1, 47.0, 53.0, 0.05, '金融', '银行', '银行', NOW(), NOW())
            """
        )
    )
    db.commit()


def _seed_news_items(db) -> None:
    news_eye_service.ensure_news_tables(db)
    rows = [
        {
            "digest": "d1" * 32,
            "dedupe_key": "d1",
            "content": "华为昇腾推出新一代芯片封装方案，半导体产业链获得新增订单。",
            "published_at": datetime(2026, 5, 26, 8, 30, 0),
            "source": "人民日报",
            "url": "https://example.com/1",
            "sentiment": "positive",
            "positive_sectors_json": "[]",
            "negative_sectors_json": "[]",
            "positive_symbols_json": '[{"symbol":"600584.SH","name":"长电科技"}]',
            "negative_symbols_json": "[]",
            "related_symbols_json": '[{"symbol":"600584.SH","name":"长电科技"}]',
            "fetched_at": datetime(2026, 5, 26, 8, 30, 0),
        },
        {
            "digest": "d2" * 32,
            "dedupe_key": "d2",
            "content": "消费电子新品发布带动供链订单增加，苹果链关注度提升。",
            "published_at": datetime(2026, 5, 26, 8, 40, 0),
            "source": "财联社电报",
            "url": "https://example.com/2",
            "sentiment": "positive",
            "positive_sectors_json": "[]",
            "negative_sectors_json": "[]",
            "positive_symbols_json": '[{"symbol":"002156.SZ","name":"通富微电"}]',
            "negative_symbols_json": "[]",
            "related_symbols_json": '[{"symbol":"002156.SZ","name":"通富微电"}]',
            "fetched_at": datetime(2026, 5, 26, 8, 40, 0),
        },
        {
            "digest": "d3" * 32,
            "dedupe_key": "d3",
            "content": "银行板块出现风险提示，行业传闻未获证实。",
            "published_at": datetime(2026, 5, 26, 8, 45, 0),
            "source": "新浪7x24",
            "url": "https://example.com/3",
            "sentiment": "negative",
            "positive_sectors_json": "[]",
            "negative_sectors_json": "[]",
            "positive_symbols_json": "[]",
            "negative_symbols_json": '[{"symbol":"601689.SH","name":"拓普集团"}]',
            "related_symbols_json": '[{"symbol":"601689.SH","name":"拓普集团"}]',
            "fetched_at": datetime(2026, 5, 26, 8, 45, 0),
        },
    ]
    for row in rows:
        db.execute(
            text(
                """
                INSERT INTO market_news_items (
                    digest, dedupe_key, content, published_at, source, url, sentiment,
                    positive_sectors_json, negative_sectors_json, positive_symbols_json,
                    negative_symbols_json, related_symbols_json, fetched_at
                )
                VALUES (
                    :digest, :dedupe_key, :content, :published_at, :source, :url, :sentiment,
                    :positive_sectors_json, :negative_sectors_json, :positive_symbols_json,
                    :negative_symbols_json, :related_symbols_json, :fetched_at
                )
                """
            ),
            row,
        )
    db.commit()


def test_generate_selection_persists_rankings_and_history(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)

    monkeypatch.setattr(
        news_theme_service,
        "list_theme_rankings",
        lambda *args, **kwargs: {
            "window": "premarket",
            "updated_at": "2026-05-26T09:00:00+08:00",
            "source": "cache:mock",
            "message": "mock",
            "items": [
                {
                    "theme": "半导体",
                    "parent_theme": "科技",
                    "rank": 1,
                    "score": 92.0,
                    "message_count": 8,
                    "positive_count": 8,
                    "negative_count": 0,
                    "source_tier": "S",
                    "top_source_tier": "S",
                    "policy_boost": True,
                    "disagreement_level": "none",
                    "crowding_risk": None,
                    "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
                    "raw_tags": ["半导体", "芯片"],
                    "summary": "半导体产能与封装持续受益。",
                    "catalyst": "华为昇腾发布封装方案",
                    "risk_note": None,
                    "market_confirmation": {"score": 10},
                    "evidence_items": [{"content": "华为昇腾推出新一代芯片封装方案", "source_tier": "S"}],
                    "event_semantic": {
                        "event_type": "产业进展",
                        "catalyst_strength": 88.0,
                        "confidence": 0.86,
                    },
                    "semantic_source": "llm:mock/mock-model",
                    "symbol_suggestion_source": "llm:mock/mock-model",
                },
                {
                    "theme": "消费电子",
                    "parent_theme": "消费科技",
                    "rank": 2,
                    "score": 88.0,
                    "message_count": 6,
                    "positive_count": 6,
                    "negative_count": 0,
                    "source_tier": "A",
                    "top_source_tier": "A",
                    "policy_boost": False,
                    "disagreement_level": "none",
                    "crowding_risk": None,
                    "related_symbols": [{"symbol": "002156.SZ", "name": "通富微电"}],
                    "raw_tags": ["消费电子", "苹果链"],
                    "summary": "消费电子新品周期启动。",
                    "catalyst": "新品发布",
                    "risk_note": None,
                    "market_confirmation": {"score": 8},
                    "evidence_items": [{"content": "消费电子新品发布带动供链订单增加", "source_tier": "A"}],
                },
            ],
            "data_governance": {
                "llm_core_stock": {
                    "enabled": True,
                    "ready": True,
                    "status": "ready",
                    "provider": "volcengine-ark",
                    "model": "mock-model",
                    "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
                    "runtime_package_source": "user_news_config",
                    "api_key_source": "user_news_config",
                    "provider_source": "user_news_config",
                    "base_url_source": "user_news_config",
                    "model_source": "user_news_config",
                    "mixed_account_runtime": False,
                    "api_key": "must-not-leak",
                }
            },
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "interpret_market_behavior",
        lambda market: {
            "narrative_anchors": ["流动性高位外溢", "结构性分化轮动", "电子主线延续"],
            "locked_values": {"total_amount_label": "2.40 万亿元"},
            "data_quality": {"missing_fields": []},
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "get_reverse_stock_map",
        lambda: {
            "600584.SH": "长电科技",
            "002156.SZ": "通富微电",
            "601689.SH": "拓普集团",
        },
    )
    monkeypatch.setattr(catalyst_selection_service, "_load_previous_selection_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_daily_features",
        lambda *args, **kwargs: {
            "600584.SH": {
                "symbol": "600584.SH",
                "name": "长电科技",
                "industry": "半导体",
                "sector": "电子",
                "concepts": ["电子", "半导体", "芯片"],
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "amount": 15_000_000,
                "turnover_rate": 1.8,
                "change_pct": 5.0,
                "amount_ratio_20d": 2.0,
                "momentum_20d": 0.12,
                "momentum_60d": 0.18,
                "r60": 79.66,
                "net_profit_growth_proxy": 0.35,
            },
            "002156.SZ": {
                "symbol": "002156.SZ",
                "name": "通富微电",
                "industry": "消费电子",
                "sector": "电子",
                "concepts": ["电子", "消费电子", "苹果链"],
                "open": 20.0,
                "high": 20.6,
                "low": 19.6,
                "close": 20.4,
                "amount": 20_000_000,
                "turnover_rate": 1.4,
                "change_pct": 4.2,
                "amount_ratio_20d": 1.6,
                "momentum_20d": 0.08,
                "momentum_60d": 0.14,
                "r60": 68.15,
                "net_profit_growth_proxy": 0.22,
            },
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_symbol_settlement_stats",
        lambda *args, **kwargs: {
            "600584.SH": {"count": 4, "hit_rate": 0.75, "loss_count": 0},
            "002156.SZ": {"count": 2, "hit_rate": 0.5, "loss_count": 0},
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "capture_intraday_symbols",
        lambda symbols, **kwargs: {
            "success": False,
            "rows": 0,
            "captured_symbols": [],
            "missing_symbols": symbols,
            "message": "mock no minute bars",
            "source": "qmt_intraday",
        },
    )

    payload = catalyst_selection_service.generate_selections(
        db,
        trade_date="2026-05-26",
        window="premarket",
        limit=5,
        user_id="test-user",
    )

    assert payload["trade_date"] == "2026-05-26"
    assert payload["data_governance"]["selected_count"] == 2
    assert payload["items"][0]["symbol"] == "600584.SH"
    assert payload["items"][0]["score"] > payload["items"][1]["score"]
    assert payload["items"][0]["theme_matches"][0]["relation_score"] >= 90
    assert payload["items"][0]["event_intelligence_score"] > 70
    assert payload["items"][0]["adaptive_feedback_score"] > 50
    assert payload["items"][0]["risk_control"]["action"] in {"deploy", "follow", "wait", "observe"}
    assert payload["items"][0]["closed_loop_trace"]["event"]["theme"] == "半导体"
    assert payload["items"][0]["closed_loop_trace"]["opportunity_event"]["event_types"][0] == "new_opportunity"
    event_trace = payload["items"][0]["closed_loop_trace"]["event"]
    assert event_trace["semantic_source"] == "llm:mock/mock-model"
    assert event_trace["symbol_suggestion_source"] == "llm:mock/mock-model"
    assert event_trace["runtime_source"]["runtime_package_source"] == "user_news_config"
    assert event_trace["runtime_source"]["provider"] == "volcengine-ark"
    assert event_trace["llm_event_understanding"]["model"] == "mock-model"
    assert event_trace["llm_event_understanding"]["api_key_source"] == "user_news_config"
    assert "api_key" not in event_trace["llm_event_understanding"]
    assert any(flag.startswith("R60=") for flag in payload["items"][0]["signal_flags"])
    assert any(flag.startswith("PROTECTED") for flag in payload["items"][0]["signal_flags"])
    assert payload["data_governance"]["closed_loop"]["feedback_learning"] is True
    assert payload["data_governance"]["closed_loop"]["risk_control_summary"]["item_count"] == 2
    assert payload["data_governance"]["closed_loop"]["risk_control_summary"]["action_counts"]
    assert payload["data_governance"]["closed_loop"]["risk_monitoring_summary"]["item_count"] == 2
    assert payload["data_governance"]["closed_loop"]["risk_monitoring_summary"]["gate_counts"]
    assert payload["data_governance"]["closed_loop"]["feedback_learning_state"]["selected_count"] == 2
    assert payload["data_governance"]["closed_loop"]["proactive_opportunity_detection"] is True
    assert payload["data_governance"]["closed_loop"]["opportunity_event_count"] == 2
    assert payload["data_governance"]["opportunity_events"][0]["symbol"] == "600584.SH"
    assert "new_opportunity" in payload["data_governance"]["opportunity_events"][0]["event_types"]

    history = catalyst_selection_service.list_history(db, limit=10)
    assert history["items"][0]["trade_date"] == "2026-05-26"
    assert history["items"][0]["item_count"] == 2
    assert history["items"][0]["top_symbol"] == "600584.SH"

    rows = db.execute(
        text(
            """
            SELECT run_id, trade_date, window_label, data_governance_json
            FROM catalyst_selection_runs
            WHERE trade_date = '2026-05-26'
            """
        )
    ).mappings().all()
    assert len(rows) == 1
    assert "selected_count" in rows[0]["data_governance_json"]
    event_count = db.execute(
        text(
            """
            SELECT COUNT(*)
            FROM catalyst_selection_opportunity_events
            WHERE trade_date = '2026-05-26' AND window_label = 'premarket'
            """
        )
    ).scalar()
    assert event_count == 2
    event_payload = catalyst_selection_service.list_opportunity_events(
        db,
        trade_date="2026-05-26",
        window="premarket",
        limit=10,
    )
    assert event_payload["items"][0]["symbol"] == "600584.SH"
    assert event_payload["items"][0]["event_level"] in {"S", "A", "B", "WATCH"}
    assert event_payload["filters"]["trade_date"] == "2026-05-26"
    assert event_payload["filters"]["window"] == "premarket"


def test_fresh_news_trigger_boosts_theme_candidate_and_candidate_pool():
    trigger_context = {
        "source": "news_eye",
        "trigger": "manual",
        "fresh_event_count": 1,
        "fresh_news_events": [
            {
                "digest": "fresh-ai",
                "source": "工信部",
                "sentiment": "positive",
                "content": "工信部推动人工智能算力基础设施建设，浪潮信息服务器产业链受益。",
                "published_at": "2026-05-26T09:10:00+08:00",
                "positive_sectors": ["人工智能", "算力"],
                "negative_sectors": [],
                "symbols": ["浪潮信息(000977.SZ)"],
            }
        ],
        "fresh_news_summary": {"event_count": 1, "included_count": 1},
    }
    trigger_news_context = catalyst_selection_service._trigger_news_context_from_trigger_context(trigger_context)
    theme_items = [
        {
            "theme": "人工智能",
            "score": 76.0,
            "summary": "AI基础设施政策升温",
            "catalyst": "政策支持算力基础设施",
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "603019.SH", "name": "中科曙光"}],
            "evidence_items": [{"content": "人工智能算力基础设施建设", "source_tier": "S"}],
            "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
            "mainline_alignment_score": 70.0,
        }
    ]

    adjusted = catalyst_selection_service._apply_trigger_news_context_to_theme_items(theme_items, trigger_news_context)

    assert adjusted[0]["score"] > theme_items[0]["score"]
    assert adjusted[0]["fresh_news_trigger"]["score_delta"] > 0
    assert "000977.SZ" in adjusted[0]["fresh_news_trigger"]["direct_symbols"]
    assert "000977.SZ" in catalyst_selection_service._candidate_symbols_from_themes(adjusted)

    features = {
        "symbol": "000977.SZ",
        "name": "浪潮信息",
        "industry": "人工智能",
        "sector": "计算机",
        "concepts": ["人工智能", "算力", "服务器"],
        "change_pct": 2.6,
        "amount_ratio_20d": 1.5,
        "momentum_20d": 0.08,
        "momentum_60d": 0.11,
        "r60": 66.0,
        "net_profit_growth_proxy": 0.18,
    }
    scored = catalyst_selection_service._score_candidate(
        symbol="000977.SZ",
        features=features,
        theme_items=adjusted,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "风险可控"}},
        trigger_news_context=trigger_news_context,
    )

    assert scored["theme_matches"][0]["trigger_news_match"]["matched"] is True
    assert scored["closed_loop_trace"]["event"]["fresh_news_trigger"]["status"] == "positive"
    assert scored["closed_loop_trace"]["scoring"]["fresh_news_trigger_adjustment"]["score_delta"] > 0
    assert any("新鲜利好资讯" in reason for reason in scored["reason_parts"])


def test_negative_fresh_news_symbol_is_not_treated_as_positive():
    trigger_context = {
        "fresh_event_count": 1,
        "fresh_news_events": [
            {
                "digest": "fresh-risk",
                "source": "交易所",
                "sentiment": "negative",
                "content": "浪潮信息相关事项收到监管关注函，短线风险升温。",
                "published_at": "2026-05-26T10:05:00+08:00",
                "positive_sectors": [],
                "negative_sectors": ["人工智能"],
                "symbols": ["浪潮信息(000977.SZ)"],
            }
        ],
    }
    trigger_news_context = catalyst_selection_service._trigger_news_context_from_trigger_context(trigger_context)

    signal = catalyst_selection_service._trigger_news_signal_for_candidate(
        symbol="000977.SZ",
        features={
            "symbol": "000977.SZ",
            "name": "浪潮信息",
            "industry": "人工智能",
            "sector": "计算机",
            "concepts": ["人工智能", "算力"],
        },
        primary_theme={"theme": "人工智能"},
        trigger_news_context=trigger_news_context,
    )
    adjustment = catalyst_selection_service._trigger_news_score_adjustment(signal)

    assert trigger_news_context["events"][0]["negative_symbols"][0]["symbol"] == "000977.SZ"
    assert trigger_news_context["events"][0]["positive_symbols"] == []
    assert signal["status"] == "negative"
    assert signal["direct"] is True
    assert adjustment["score_delta"] < 0
    assert "新鲜风险资讯" in adjustment["reason"]


def test_realtime_window_does_not_pollute_premarket_history_or_settlement(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    now_value = datetime(2026, 5, 26, 10, 0, 0)
    catalyst_selection_service._persist_selection_run(
        db,
        run_id="premarket-run",
        trade_date="2026-05-26",
        window="premarket",
        window_start="2026-05-25T15:00:00",
        window_end="2026-05-26T09:25:00",
        market_background="mock",
        market_behavior={},
        items=[],
        data_governance={"score_version": catalyst_selection_service.SCORE_VERSION},
        opportunity_events=[],
        now_value=now_value,
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_settlements (
                trade_date, settlement_date, symbol, name, rank,
                outcome, protected, settlement_notes_json, updated_at
            )
            VALUES (
                '2026-05-26', '2026-05-27', '600584.SH', '长电科技', 1,
                'hit', true, '[]', :updated_at
            )
            """
        ),
        {"updated_at": now_value},
    )
    catalyst_selection_service._persist_selection_run(
        db,
        run_id="realtime-run",
        trade_date="2026-05-26",
        window="24h",
        window_start="2026-05-25T10:00:00",
        window_end="2026-05-26T10:00:00",
        market_background="mock",
        market_behavior={},
        items=[],
        data_governance={"score_version": catalyst_selection_service.SCORE_VERSION},
        opportunity_events=[],
        now_value=now_value,
    )
    db.commit()

    settlement_count = db.execute(
        text("SELECT COUNT(*) FROM catalyst_selection_settlements WHERE trade_date = '2026-05-26'")
    ).scalar()
    history = catalyst_selection_service.list_history(db, limit=10)
    realtime_loaded = catalyst_selection_service._load_selection_run(
        db,
        trade_date="2026-05-26",
        window="24h",
        limit=10,
    )

    assert settlement_count == 1
    assert len(history["items"]) == 1
    assert history["items"][0]["trade_date"] == "2026-05-26"
    assert realtime_loaded is not None
    assert realtime_loaded["message"].startswith("实时事件机会榜")


def test_opportunity_events_detect_rank_score_jump_and_risk_suppression():
    events = catalyst_selection_service._build_opportunity_events(
        items=[
            {
                "rank": 1,
                "symbol": "600584.SH",
                "name": "长电科技",
                "score": 82.0,
                "event_intelligence_score": 78.0,
                "market_confirm_score": 58.0,
                "risk_flags": [],
                "risk_control": {"action": "deploy", "risk_level": "low"},
                "theme_matches": [
                    {
                        "theme": "半导体",
                        "event_semantic": {"event_type": "产业进展"},
                    }
                ],
                "closed_loop_trace": {"market": {"mainline_alignment_score": 72.5}},
            },
            {
                "rank": 2,
                "symbol": "002156.SZ",
                "name": "通富微电",
                "score": 61.0,
                "event_intelligence_score": 65.0,
                "market_confirm_score": 35.0,
                "risk_flags": ["高位兑现"],
                "risk_control": {"action": "observe", "risk_level": "high"},
                "theme_matches": [{"theme": "消费电子", "event_semantic": {"event_type": "订单兑现"}}],
                "closed_loop_trace": {},
            },
        ],
        previous_snapshot={
            "600584.SH": {"rank": 5, "score": 70.0},
            "002156.SZ": {"rank": 2, "score": 60.0},
        },
    )

    strong = next(event for event in events if event["symbol"] == "600584.SH")
    risk = next(event for event in events if event["symbol"] == "002156.SZ")
    assert strong["event_level"] == "S"
    assert "rank_jump" in strong["event_types"]
    assert "score_jump" in strong["event_types"]
    assert "event_market_confirmed" in strong["event_types"]
    assert strong["rank_delta"] == 4
    assert strong["score_delta"] == 12.0
    assert risk["event_level"] == "WATCH"
    assert "risk_suppressed" in risk["event_types"]


def test_theme_feedback_learning_loads_from_prior_settlements(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_items (
                run_id, trade_date, window_label, rank, symbol, name, score,
                theme_matches_json, market_background, created_at, updated_at
            )
            VALUES
            ('r1', '2026-05-20', 'premarket', 1, '600584.SH', '长电科技', 80,
             :matches1, 'mock', NOW(), NOW()),
            ('r2', '2026-05-21', 'premarket', 1, '600584.SH', '长电科技', 75,
             :matches2, 'mock', NOW(), NOW())
            """
        ),
        {
            "matches1": json.dumps([{"theme": "半导体", "score": 90, "relation_score": 95}], ensure_ascii=False),
            "matches2": json.dumps([{"theme": "半导体", "score": 88, "relation_score": 90}], ensure_ascii=False),
        },
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_settlements (
                trade_date, settlement_date, symbol, name, rank,
                change_pct, hit_score, outcome, protected, settlement_notes_json, updated_at
            )
            VALUES
            ('2026-05-20', '2026-05-21', '600584.SH', '长电科技', 1, 6.0, 82, 'strong_hit', TRUE, '[]', NOW()),
            ('2026-05-21', '2026-05-22', '600584.SH', '长电科技', 1, -2.0, 42, 'miss', FALSE, '[]', NOW())
            """
        )
    )
    db.commit()

    stats = catalyst_selection_service._load_theme_settlement_stats(db, trade_date="2026-05-26")

    assert stats["半导体"]["count"] == 2
    assert stats["半导体"]["hit_count"] == 1
    assert stats["半导体"]["hit_rate"] == pytest.approx(0.5)
    assert stats["半导体"]["average_change_pct"] == pytest.approx(2.0)


def test_persistent_feedback_profiles_feed_adaptive_score(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_feedback_profiles (
                profile_scope, profile_key, model_version, sample_count, hit_count, miss_count,
                average_change_pct, average_hit_score, hit_rate, learned_score, confidence,
                last_trade_date, last_settlement_date, feature_snapshot_json, updated_at
            )
            VALUES
            ('symbol', '600584.SH', :model_version, 6, 5, 1, 4.8, 76.0, 0.8333, 82.0, 0.75, '2026-05-24', '2026-05-25', '{}', NOW()),
            ('theme', '半导体', :model_version, 8, 6, 2, 3.2, 72.0, 0.7500, 76.0, 1.00, '2026-05-24', '2026-05-25', '{}', NOW())
            """
        ),
        {"model_version": catalyst_selection_service.FEEDBACK_MODEL_VERSION},
    )
    db.commit()

    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=["600584.SH"],
        themes=["半导体"],
    )
    history_stats = catalyst_selection_service._merge_symbol_feedback_profiles({}, profiles["symbols"])
    theme_feedback = catalyst_selection_service._merge_theme_feedback_profiles({}, profiles["themes"])
    score, reasons = catalyst_selection_service._adaptive_feedback_score(
        symbol="600584.SH",
        primary_theme={"theme": "半导体"},
        history_stats=history_stats["600584.SH"],
        theme_feedback=theme_feedback,
    )

    assert profiles["profile_count"] == 2
    assert profiles["sample_count"] == 14
    assert score > 70
    assert any("600584.SH 学习画像" in reason for reason in reasons)
    assert any("半导体 学习画像" in reason for reason in reasons)


def test_feedback_profile_recency_decay_reduces_effective_confidence(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_feedback_profiles (
                profile_scope, profile_key, model_version, sample_count, hit_count, miss_count,
                average_change_pct, average_hit_score, hit_rate, learned_score, confidence,
                last_trade_date, last_settlement_date, feature_snapshot_json, updated_at
            )
            VALUES
            ('symbol', '600584.SH', :model_version, 8, 8, 0, 6.0, 82.0, 1.0, 90.0, 1.0,
             '2026-01-01', '2026-01-02', '{}', NOW())
            """
        ),
        {"model_version": catalyst_selection_service.FEEDBACK_MODEL_VERSION},
    )
    db.commit()

    fresh = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=["600584.SH"],
        themes=[],
    )
    stale = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=["600584.SH"],
        themes=[],
        as_of_trade_date="2026-06-01",
    )

    fresh_profile = fresh["symbols"]["600584.SH"]
    stale_profile = stale["symbols"]["600584.SH"]
    assert fresh_profile["confidence"] == pytest.approx(1.0)
    assert stale_profile["base_confidence"] == pytest.approx(1.0)
    assert stale_profile["confidence"] == pytest.approx(catalyst_selection_service.FEEDBACK_RECENCY_MIN_WEIGHT)
    assert stale_profile["recency_weight"] == pytest.approx(catalyst_selection_service.FEEDBACK_RECENCY_MIN_WEIGHT)
    assert stale_profile["recency_days"] > 100
    assert stale_profile["is_recency_decayed"] is True


def test_feedback_learning_state_summarizes_profiles_and_selected_usage():
    feedback_profiles = {
        "symbols": {
            "600584.SH": {
                "profile_scope": "symbol",
                "profile_key": "600584.SH",
                "sample_count": 6,
                "hit_count": 5,
                "miss_count": 1,
                "hit_rate": 0.8333,
                "learned_score": 82.0,
                "confidence": 0.75,
                "updated_at": "2026-05-25T15:00:00",
            }
        },
        "themes": {
            "金融": {
                "profile_scope": "theme",
                "profile_key": "金融",
                "sample_count": 5,
                "hit_count": 1,
                "miss_count": 4,
                "hit_rate": 0.2,
                "learned_score": 34.0,
                "confidence": 0.625,
                "updated_at": "2026-05-25T15:00:00",
            }
        },
        "event_types": {},
        "profile_count": 2,
        "sample_count": 11,
        "latest_updated_at": "2026-05-25T15:00:00",
    }
    selected = [
        {
            "symbol": "600584.SH",
            "adaptive_feedback_score": 68.5,
            "closed_loop_trace": {
                "feedback": {
                    "symbol_profile": {"sample_count": 6, "learned_score": 82.0},
                    "theme_profile": {},
                    "event_type_profile": {},
                }
            },
        }
    ]

    state = catalyst_selection_service._build_feedback_learning_state(feedback_profiles, selected)

    assert state["status"] == "active"
    assert state["profile_count"] == 2
    assert state["sample_count"] == 11
    assert state["selected_with_feedback_count"] == 1
    assert state["selected_adaptive_feedback_avg"] == 68.5
    assert state["top_positive_profiles"][0]["profile_key"] == "600584.SH"
    assert state["top_negative_profiles"][0]["profile_key"] == "金融"


def test_ai_quant_end_to_end_evidence_includes_event_driven_discovery_metrics():
    evidence = catalyst_selection_service._build_ai_quant_end_to_end_evidence(
        trigger_context={
            "source": "catalyst_selection_event_refresh",
            "trigger": "news-eye:background",
            "refresh_key": "refresh-key-1",
            "fresh_event_count": 4,
            "included_count": 3,
            "news_ingest": {"saved": 120, "new": 4, "updated": 2},
        },
        selected=[
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 61.2,
                "adaptive_feedback_score": 42.0,
                "risk_control": {"action": "observe"},
            }
        ],
        opportunity_events=[{"symbol": "603019.SH", "event_types": ["new_opportunity"]}],
        llm_runtime={
            "ready": True,
            "provider": "volcengine-ark",
            "model": "deepseek-v4-flash",
            "runtime_package_source": "user_news_config",
            "used_symbol_theme_count": 8,
            "used_semantic_theme_count": 10,
        },
        market_state_freshness={"status": "aligned"},
        risk_control_summary={"action_counts": {"observe": 1}, "risk_level_counts": {"medium": 1}},
        feedback_learning_state={"profile_count": 2, "sample_count": 15},
        learning_adjustment_summary={"active_count": 1},
        learning_impact_summary={"active_count": 1},
        realtime_feedback_summary={"sample_count": 6},
    )

    proactive_stage = next(stage for stage in evidence["stages"] if stage["id"] == "proactive_opportunity_discovery")
    assert evidence["status"] == "active"
    assert proactive_stage["status"] == "active"
    assert proactive_stage["metrics"]["discovery_mode"] == "event_driven"
    assert proactive_stage["metrics"]["event_triggered"] is True
    assert proactive_stage["metrics"]["trigger"] == "news-eye:background"
    assert proactive_stage["metrics"]["trigger_source"] == "catalyst_selection_event_refresh"
    assert proactive_stage["metrics"]["refresh_key"] == "refresh-key-1"
    assert proactive_stage["metrics"]["fresh_event_count"] == 4
    assert proactive_stage["metrics"]["included_event_count"] == 3
    assert proactive_stage["metrics"]["news_ingest_new"] == 4
    assert proactive_stage["metrics"]["news_ingest_saved"] == 120


def test_monitor_pool_from_selection_respects_execution_gates():
    payload = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "601138.SH",
                "name": "工业富联",
                "score": 82.5,
                "adaptive_feedback_score": 61.0,
                "risk_penalty": 3.0,
                "reason_parts": ["算力主线"],
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 91.0,
                        "relation_score": 96.0,
                        "mainline_alignment_score": 72.0,
                        "source_tier": "S",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
                    }
                ],
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 4.5,
                    "risk_monitoring": {"execution_gate": "allow", "next_action": "等待分时确认"},
                },
            },
            {
                "rank": 2,
                "symbol": "300308.SZ",
                "name": "中际旭创",
                "score": 79.0,
                "adaptive_feedback_score": 55.0,
                "risk_penalty": 6.0,
                "risk_control": {
                    "action": "wait",
                    "risk_level": "medium",
                    "max_position_pct": 5.0,
                    "stop_loss_pct": 5.0,
                    "risk_monitoring": {"execution_gate": "confirm", "next_action": "等待量能确认"},
                },
            },
            {
                "rank": 3,
                "symbol": "600030.SH",
                "name": "中信证券",
                "score": 62.0,
                "adaptive_feedback_score": 38.0,
                "risk_penalty": 16.0,
                "risk_control": {
                    "action": "observe",
                    "risk_level": "high",
                    "risk_monitoring": {"execution_gate": "blocked", "next_action": "禁止开仓"},
                },
            },
            {
                "rank": 4,
                "symbol": "000001.SZ",
                "name": "平安银行",
                "score": 58.0,
                "risk_control": {
                    "action": "observe",
                    "risk_level": "high",
                    "risk_monitoring": {"execution_gate": "reduce_only", "next_action": "只减不加"},
                },
            },
        ],
    }

    pool = catalyst_selection_service.build_monitor_pool_from_selection(payload)

    assert pool["suggested_execution_mode"] == "monitor_only"
    assert pool["monitor_pool"]["mode"] == "manual_only"
    assert pool["monitor_pool"]["watch_symbols"] == ["601138.SH", "300308.SZ", "600030.SH", "000001.SZ"]
    assert pool["monitor_pool"]["tradable_symbols"] == ["601138.SH", "300308.SZ"]
    assert pool["monitor_pool"]["manual_symbols"] == ["601138.SH", "300308.SZ", "600030.SH", "000001.SZ"]
    assert pool["monitor_pool"]["entry_symbols"] == ["601138.SH"]
    assert pool["monitor_pool"]["confirm_symbols"] == ["300308.SZ"]
    assert pool["monitor_pool"]["blocked_symbols"] == ["600030.SH"]
    assert pool["monitor_pool"]["reduce_only_symbols"] == ["000001.SZ"]
    assert pool["monitor_pool"]["gate_counts"] == {"allow": 1, "confirm": 1, "blocked": 1, "reduce_only": 1}
    assert pool["risk_config"]["gate_counts"] == {"allow": 1, "confirm": 1, "blocked": 1, "reduce_only": 1}
    assert pool["risk_config"]["execution_gates"]["600030.SH"] == "blocked"
    assert pool["risk_config"]["watch_symbols"] == ["601138.SH", "300308.SZ", "600030.SH", "000001.SZ"]
    assert pool["risk_config"]["tradable_symbols"] == ["601138.SH", "300308.SZ"]
    assert pool["risk_config"]["max_position_pct_by_symbol"]["601138.SH"] == 8.0
    assert pool["summary"]["monitor_symbol_count"] == 4
    assert pool["summary"]["watch_symbol_count"] == 4
    assert pool["summary"]["tradable_symbol_count"] == 2
    assert pool["summary"]["blocked_symbol_count"] == 1
    first_candidate = pool["monitor_pool"]["candidates"][0]
    assert first_candidate["themes"] == ["人工智能"]
    assert first_candidate["event_types"] == ["政策支持"]
    assert first_candidate["theme_matches"][0]["event_semantic"]["event_type"] == "政策支持"


def test_catalyst_selection_auto_maintains_monitor_only_realtime_monitor(db, monkeypatch):
    monkeypatch.setenv("AI_QUANT_CATALYST_AUTO_MONITOR", "1")
    monkeypatch.setenv("AI_QUANT_CATALYST_AUTO_MONITOR_START", "1")
    user_id = "auto-monitor-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        qmt_paper_account_config={
            "key": "paper_sim",
            "role": "paper",
            "enabled": True,
            "host": "192.168.10.1",
            "port": 58610,
            "account_id": "paper-001",
            "account_type": "STOCK",
            "account_name": "测试模拟账户",
        },
    )
    strategy_id = uuid4().hex
    version_id = uuid4().hex
    save_platform_strategy(
        db,
        {
            "id": strategy_id,
            "name": "AI量化监控测试策略",
            "strategy_type": "trading",
            "description": "测试自动承接催化监控池",
            "status": "active",
            "is_active": True,
            "version": 1,
            "source": "manual",
            "current_version_id": version_id,
            "current_version": {
                "id": version_id,
                "strategy_id": strategy_id,
                "version": 1,
                "dsl": _default_dsl("trading").model_dump(),
                "created_at": "2026-05-29T09:00:00",
                "change_summary": "初始版本",
            },
            "versions": [],
            "performance": {},
        },
    )
    db.commit()
    selection = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 88.0,
                "adaptive_feedback_score": 66.0,
                "risk_penalty": 4.0,
                "reason_parts": ["AI算力政策催化"],
                "signal_flags": ["强势"],
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 90.0,
                        "relation_score": 95.0,
                        "mainline_alignment_score": 80.0,
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88, "confidence": 0.82},
                    }
                ],
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 5.0,
                    "risk_monitoring": {
                        "execution_gate": "allow",
                        "next_action": "monitor_entry",
                    },
                },
            }
        ],
    }

    created = catalyst_selection_service.maintain_realtime_monitor_from_selection(
        db,
        selection=selection,
        user_id=user_id,
    )
    monitor = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.user_id == user_id).one()
    monitor.state_json = {"signal_clock": {"last_evaluated_bar_key": "old-bar"}}
    db.add(monitor)
    db.commit()
    updated = catalyst_selection_service.maintain_realtime_monitor_from_selection(
        db,
        selection={
            **selection,
            "items": [
                {
                    **selection["items"][0],
                    "symbol": "688041.SH",
                    "name": "海光信息",
                    "score": 91.0,
                }
            ],
        },
        user_id=user_id,
    )
    next_day = catalyst_selection_service.maintain_realtime_monitor_from_selection(
        db,
        selection={
            **selection,
            "trade_date": "2026-06-01",
        },
        user_id=user_id,
    )
    stale_update = catalyst_selection_service.maintain_realtime_monitor_from_selection(
        db,
        selection={
            **selection,
            "trade_date": "2026-05-28",
            "items": [
                {
                    **selection["items"][0],
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                }
            ],
        },
        user_id=user_id,
    )
    monitors = db.query(RealtimeMonitorDB).filter(RealtimeMonitorDB.user_id == user_id).all()

    assert created["status"] == "created_running"
    assert created["monitor_symbol_count"] == 1
    assert updated["status"] == "updated_running"
    assert updated["monitor_id"] == created["monitor_id"]
    assert next_day["status"] == "updated_running"
    assert next_day["monitor_id"] == created["monitor_id"]
    assert stale_update["status"] == "skipped_stale_selection"
    assert stale_update["monitor_id"] == created["monitor_id"]
    assert stale_update["kept_trade_date"] == "2026-06-01"
    assert updated["pool_changed"] is True
    assert updated["signal_clock_reset"] is True
    assert len(monitors) == 1
    assert monitors[0].status == "running"
    assert monitors[0].execution_mode == "monitor_only"
    assert monitors[0].auto_trade_enabled is False
    assert monitors[0].monitor_pool_json["source"] == "catalyst-selection"
    assert monitors[0].monitor_pool_json["trade_date"] == "2026-06-01"
    assert monitors[0].monitor_pool_json["watch_symbols"] == ["603019.SH"]
    assert monitors[0].monitor_pool_json["tradable_symbols"] == ["603019.SH"]
    assert monitors[0].monitor_pool_json["manual_symbols"] == ["603019.SH"]
    assert monitors[0].monitor_pool_json["gate_counts"] == {"allow": 1}
    assert monitors[0].risk_config_json["gate_counts"] == {"allow": 1}
    assert "signal_clock" not in (monitors[0].state_json or {})
    assert realtime_monitor_service.list_monitors(db, user_id) == []


def test_realtime_monitor_feedback_is_captured_and_merged_into_profiles(db):
    from api.models.strategy_models import (
        Base as StrategyBase,
        RealtimeEventDB,
        RealtimeMonitorDB,
        StrategyDB,
        StrategyStatus,
        StrategyType,
    )

    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    StrategyBase.metadata.create_all(bind=db.get_bind())
    strategy = StrategyDB(
        id="strategy-realtime-feedback",
        name="实时反馈策略",
        strategy_type=StrategyType.SELECTION,
        status=StrategyStatus.ACTIVE,
        is_active=True,
        created_at=datetime(2026, 5, 29, 9, 20, 0),
        updated_at=datetime(2026, 5, 29, 9, 20, 0),
    )
    db.add(strategy)
    selection = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 86.0,
                "adaptive_feedback_score": 58.0,
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 4.5,
                    "risk_monitoring": {"execution_gate": "allow", "next_action": "等待分时确认"},
                },
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 90.0,
                        "relation_score": 95.0,
                        "source_tier": "S",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
                    }
                ],
            }
        ],
    }
    pool_payload = catalyst_selection_service.build_monitor_pool_from_selection(selection)
    monitor = RealtimeMonitorDB(
        id="monitor-realtime-feedback",
        user_id="user-1",
        name="催化监控",
        account_key="paper_sim",
        account_role="paper",
        strategy_id=strategy.id,
        status="running",
        execution_mode="monitor_only",
        auto_trade_enabled=False,
        live_trading_enabled=False,
        monitor_pool_json=pool_payload["monitor_pool"],
        config_json={},
        risk_config_json=pool_payload["risk_config"],
        state_json={},
        created_at=datetime(2026, 5, 29, 9, 25, 0),
        updated_at=datetime(2026, 5, 29, 9, 25, 0),
    )
    db.add(monitor)
    db.add(
        RealtimeEventDB(
            id="event-realtime-feedback-1",
            monitor_id=monitor.id,
            user_id="user-1",
            event_type="signal_generated",
            account_key="paper_sim",
            strategy_id=strategy.id,
            symbol="603019.SH",
            trade_time=datetime(2026, 5, 29, 10, 2, 0),
            signal_payload={"symbol": "603019.SH", "side": "buy", "source": "dsl_realtime_ir"},
            request_id="signal-1",
            correlation_id="cycle-1",
            created_at=datetime(2026, 5, 29, 10, 2, 0),
        )
    )
    db.commit()

    captured = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 5, 29, 10, 3, 0),
    )
    captured_again = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 5, 29, 10, 4, 0),
    )

    row = db.execute(
        text(
            """
            SELECT source_event_id, symbol, outcome, symbol_feedback, risk_feedback,
                   themes_json, event_types_json, risk_gate
            FROM catalyst_selection_realtime_feedback
            WHERE source_event_id = 'event-realtime-feedback-1'
            """
        )
    ).mappings().first()
    total_rows = db.execute(text("SELECT COUNT(*) FROM catalyst_selection_realtime_feedback")).scalar()
    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=["603019.SH"],
        themes=["人工智能"],
        event_types=["政策支持"],
        risk_gates=["allow"],
        as_of_trade_date="2026-05-29",
    )
    realtime_summary = catalyst_selection_service.summarize_realtime_feedback(db, trade_date="2026-05-29")

    assert captured["captured_count"] == 1
    assert captured["feedback_refresh"]["symbol_profile_count"] == 1
    assert captured_again["captured_count"] == 1
    assert total_rows == 1
    assert row["symbol"] == "603019.SH"
    assert row["outcome"] == "hit"
    assert row["symbol_feedback"] is True
    assert row["risk_feedback"] is True
    assert json.loads(row["themes_json"]) == ["人工智能"]
    assert json.loads(row["event_types_json"]) == ["政策支持"]
    assert row["risk_gate"] == "allow"
    assert profiles["symbols"]["603019.SH"]["sample_count"] == 1
    assert profiles["symbols"]["603019.SH"]["learned_score"] > 50
    assert profiles["themes"]["人工智能"]["sample_count"] == 1
    assert profiles["event_types"]["政策支持"]["sample_count"] == 1
    assert profiles["risk_gates"]["allow"]["sample_count"] == 1
    assert realtime_summary["sample_count"] == 1
    assert realtime_summary["symbol_feedback_count"] == 1
    assert realtime_summary["risk_feedback_count"] == 1
    assert realtime_summary["monitor_count"] == 1
    assert realtime_summary["event_type_counts"]["signal_generated"] == 1
    assert realtime_summary["risk_gate_counts"]["allow"] == 1
    assert realtime_summary["top_themes"][0]["theme"] == "人工智能"


def test_capture_realtime_monitor_feedback_expands_no_signal_to_candidate_samples(db):
    from api.models.strategy_models import (
        Base as StrategyBase,
        RealtimeEventDB,
        RealtimeMonitorDB,
        StrategyDB,
        StrategyStatus,
        StrategyType,
    )

    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    StrategyBase.metadata.create_all(bind=db.get_bind())
    strategy = StrategyDB(
        id="strategy-realtime-no-signal",
        name="实时无信号反馈策略",
        strategy_type=StrategyType.SELECTION,
        status=StrategyStatus.ACTIVE,
        is_active=True,
        created_at=datetime(2026, 5, 29, 9, 20, 0),
        updated_at=datetime(2026, 5, 29, 9, 20, 0),
    )
    db.add(strategy)
    selection = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 86.0,
                "adaptive_feedback_score": 58.0,
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 4.5,
                    "risk_monitoring": {"execution_gate": "allow", "next_action": "等待分时确认"},
                },
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 90.0,
                        "relation_score": 95.0,
                        "source_tier": "S",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
                    }
                ],
            },
            {
                "rank": 2,
                "symbol": "000001.SZ",
                "name": "平安银行",
                "score": 68.0,
                "adaptive_feedback_score": 41.0,
                "risk_control": {
                    "action": "avoid",
                    "risk_level": "high",
                    "max_position_pct": 0.0,
                    "stop_loss_pct": 3.0,
                    "risk_monitoring": {"execution_gate": "blocked", "next_action": "风险过高，禁止追入"},
                },
                "theme_matches": [
                    {
                        "theme": "金融",
                        "score": 75.0,
                        "relation_score": 80.0,
                        "source_tier": "A",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 60.0, "confidence": 0.7},
                    }
                ],
            },
        ],
    }
    pool_payload = catalyst_selection_service.build_monitor_pool_from_selection(selection)
    monitor = RealtimeMonitorDB(
        id="monitor-realtime-no-signal",
        user_id="user-1",
        name="催化无信号监控",
        account_key="paper_sim",
        account_role="paper",
        strategy_id=strategy.id,
        status="running",
        execution_mode="monitor_only",
        auto_trade_enabled=False,
        live_trading_enabled=False,
        monitor_pool_json=pool_payload["monitor_pool"],
        config_json={},
        risk_config_json=pool_payload["risk_config"],
        state_json={},
        created_at=datetime(2026, 5, 29, 9, 25, 0),
        updated_at=datetime(2026, 5, 29, 9, 25, 0),
    )
    db.add(monitor)
    db.add(
        RealtimeEventDB(
            id="event-no-signal-1",
            monitor_id=monitor.id,
            user_id="user-1",
            event_type="no_signal",
            account_key="paper_sim",
            strategy_id=strategy.id,
            symbol=None,
            trade_time=datetime(2026, 6, 2, 10, 2, 0),
            payload={"cycle_id": "cycle-1", "trigger_source": "worker"},
            request_id="no-signal-1",
            correlation_id="cycle-1",
            created_at=datetime(2026, 6, 2, 10, 2, 0),
        )
    )
    db.commit()

    captured = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 6, 2, 10, 3, 0),
    )
    rows = db.execute(
        text(
            """
            SELECT source_event_id, trade_date, event_time, symbol, outcome, hit_score,
                   symbol_feedback, risk_feedback, risk_favorable, themes_json, risk_gate
            FROM catalyst_selection_realtime_feedback
            ORDER BY symbol
            """
        )
    ).mappings().all()
    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=["603019.SH", "000001.SZ"],
        themes=["人工智能", "金融"],
        event_types=["政策支持"],
        risk_gates=["allow", "blocked"],
        as_of_trade_date="2026-06-02",
    )
    realtime_summary = catalyst_selection_service.summarize_realtime_feedback(db, trade_date="2026-06-02")

    assert captured["captured_count"] == 2
    assert captured["feedback_refresh"]["symbol_profile_count"] == 1
    assert captured["feedback_refresh"]["risk_gate_profile_count"] == 2
    assert {row["symbol"] for row in rows} == {"000001.SZ", "603019.SH"}
    allow_row = next(row for row in rows if row["symbol"] == "603019.SH")
    blocked_row = next(row for row in rows if row["symbol"] == "000001.SZ")
    assert allow_row["source_event_id"] == "event-no-signal-1:603019.SH"
    assert allow_row["trade_date"] == "2026-06-02"
    assert str(allow_row["event_time"]).startswith("2026-06-02")
    assert allow_row["outcome"] == "miss"
    assert allow_row["hit_score"] == 45.0
    assert allow_row["symbol_feedback"] is True
    assert allow_row["risk_feedback"] is True
    assert allow_row["risk_favorable"] is False
    assert json.loads(allow_row["themes_json"]) == ["人工智能"]
    assert allow_row["risk_gate"] == "allow"
    assert blocked_row["symbol_feedback"] is False
    assert blocked_row["risk_feedback"] is True
    assert blocked_row["risk_favorable"] is True
    assert blocked_row["risk_gate"] == "blocked"
    assert profiles["symbols"]["603019.SH"]["sample_count"] == 1
    assert "000001.SZ" not in profiles["symbols"]
    assert profiles["risk_gates"]["allow"]["sample_count"] == 1
    assert profiles["risk_gates"]["blocked"]["sample_count"] == 1
    assert realtime_summary["status"] == "active"
    assert realtime_summary["sample_count"] == 2
    assert realtime_summary["symbol_feedback_count"] == 1
    assert realtime_summary["risk_feedback_count"] == 2


def test_capture_realtime_monitor_feedback_uses_minute_features_as_candidate_feedback(db):
    from api.models.strategy_models import (
        Base as StrategyBase,
        RealtimeEventDB,
        RealtimeMonitorDB,
        StrategyDB,
        StrategyStatus,
        StrategyType,
    )

    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    StrategyBase.metadata.create_all(bind=db.get_bind())
    strategy = StrategyDB(
        id="strategy-realtime-minute-feedback",
        name="实时分钟反馈策略",
        strategy_type=StrategyType.SELECTION,
        status=StrategyStatus.ACTIVE,
        is_active=True,
        created_at=datetime(2026, 5, 29, 9, 20, 0),
        updated_at=datetime(2026, 5, 29, 9, 20, 0),
    )
    db.add(strategy)
    selection = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 86.0,
                "adaptive_feedback_score": 58.0,
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 4.5,
                    "risk_monitoring": {"execution_gate": "allow", "next_action": "等待分时确认"},
                },
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 90.0,
                        "relation_score": 95.0,
                        "source_tier": "S",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
                    }
                ],
            },
            {
                "rank": 2,
                "symbol": "000001.SZ",
                "name": "平安银行",
                "score": 68.0,
                "adaptive_feedback_score": 41.0,
                "risk_control": {
                    "action": "avoid",
                    "risk_level": "high",
                    "max_position_pct": 0.0,
                    "stop_loss_pct": 3.0,
                    "risk_monitoring": {"execution_gate": "blocked", "next_action": "风险过高，禁止追入"},
                },
                "theme_matches": [
                    {
                        "theme": "金融",
                        "score": 75.0,
                        "relation_score": 80.0,
                        "source_tier": "A",
                        "event_semantic": {"event_type": "政策支持", "catalyst_strength": 60.0, "confidence": 0.7},
                    }
                ],
            },
        ],
    }
    pool_payload = catalyst_selection_service.build_monitor_pool_from_selection(selection)
    monitor = RealtimeMonitorDB(
        id="monitor-realtime-minute-feedback",
        user_id="user-1",
        name="催化分钟反馈监控",
        account_key="paper_sim",
        account_role="paper",
        strategy_id=strategy.id,
        status="running",
        execution_mode="monitor_only",
        auto_trade_enabled=False,
        live_trading_enabled=False,
        monitor_pool_json=pool_payload["monitor_pool"],
        config_json={},
        risk_config_json=pool_payload["risk_config"],
        state_json={},
        created_at=datetime(2026, 5, 29, 9, 25, 0),
        updated_at=datetime(2026, 5, 29, 9, 25, 0),
    )
    db.add(monitor)
    db.add(
        RealtimeEventDB(
            id="event-minute-feedback-1",
            monitor_id=monitor.id,
            user_id="user-1",
            event_type="minute_features",
            account_key="paper_sim",
            strategy_id=strategy.id,
            symbol=None,
            trade_time=datetime(2026, 6, 2, 10, 2, 0),
            payload={
                "source": "qmt_intraday+postgresql:stock_minute_kline",
                "timeframe": "30m",
                "latest_closed_bar_end": "2026-06-02T10:00:00",
                "items": [
                    {
                        "symbol": "603019.SH",
                        "bar_start": "2026-06-02T09:30:00",
                        "bar_end": "2026-06-02T10:00:00",
                        "open": 100.0,
                        "close": 102.0,
                        "confirmed": True,
                    },
                    {
                        "symbol": "000001.SZ",
                        "bar_start": "2026-06-02T09:30:00",
                        "bar_end": "2026-06-02T10:00:00",
                        "open": 10.0,
                        "close": 9.9,
                        "confirmed": False,
                    },
                    {
                        "symbol": "300520.SZ",
                        "bar_start": "2026-06-02T09:30:00",
                        "bar_end": "2026-06-02T10:00:00",
                        "open": 20.0,
                        "close": 21.0,
                        "confirmed": True,
                    },
                ],
            },
            request_id="minute-feedback-1",
            correlation_id="cycle-1",
            created_at=datetime(2026, 6, 2, 10, 2, 0),
        )
    )
    db.commit()

    captured = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 6, 2, 10, 3, 0),
    )
    captured_again = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 6, 2, 10, 4, 0),
    )
    rows = db.execute(
        text(
            """
            SELECT trade_date, event_time, symbol, event_type, outcome, hit_score, change_pct,
                   symbol_feedback, risk_feedback, risk_favorable, risk_gate, signal_source
            FROM catalyst_selection_realtime_feedback
            ORDER BY symbol
            """
        )
    ).mappings().all()
    realtime_summary = catalyst_selection_service.summarize_realtime_feedback(db, trade_date="2026-06-02")

    assert captured["captured_count"] == 2
    assert captured["new_sample_count"] == 2
    assert captured["existing_sample_count"] == 0
    assert captured["event_types"] == {"minute_confirmed": 1, "minute_unconfirmed": 1}
    assert captured["symbol_feedback_count"] == 1
    assert captured["risk_feedback_count"] == 2
    assert captured["feedback_refresh"]["symbol_profile_count"] == 1
    assert captured["feedback_refresh"]["risk_gate_profile_count"] == 2
    assert captured_again["captured_count"] == 2
    assert captured_again["new_sample_count"] == 0
    assert captured_again["existing_sample_count"] == 2
    assert captured_again["feedback_refresh"] is None
    assert db.execute(text("SELECT COUNT(*) FROM catalyst_selection_realtime_feedback")).scalar() == 2
    assert {row["symbol"] for row in rows} == {"000001.SZ", "603019.SH"}
    confirmed_row = next(row for row in rows if row["symbol"] == "603019.SH")
    unconfirmed_row = next(row for row in rows if row["symbol"] == "000001.SZ")
    assert confirmed_row["trade_date"] == "2026-06-02"
    assert str(confirmed_row["event_time"]).startswith("2026-06-02")
    assert confirmed_row["event_type"] == "minute_confirmed"
    assert confirmed_row["outcome"] == "hit"
    assert confirmed_row["hit_score"] == 64.0
    assert confirmed_row["change_pct"] == 2.0
    assert confirmed_row["symbol_feedback"] is True
    assert confirmed_row["risk_feedback"] is True
    assert confirmed_row["risk_favorable"] is True
    assert confirmed_row["risk_gate"] == "allow"
    assert confirmed_row["signal_source"] == "qmt_intraday+postgresql:stock_minute_kline"
    assert unconfirmed_row["event_type"] == "minute_unconfirmed"
    assert unconfirmed_row["symbol_feedback"] is False
    assert unconfirmed_row["risk_feedback"] is True
    assert unconfirmed_row["risk_favorable"] is True
    assert unconfirmed_row["risk_gate"] == "blocked"
    assert realtime_summary["sample_count"] == 2
    assert realtime_summary["event_type_counts"] == {"minute_confirmed": 1, "minute_unconfirmed": 1}
    assert realtime_summary["symbol_feedback_count"] == 1
    assert realtime_summary["risk_feedback_count"] == 2


def test_capture_realtime_monitor_feedback_skips_position_events_outside_candidate_pool(db):
    from api.models.strategy_models import (
        Base as StrategyBase,
        RealtimeEventDB,
        RealtimeMonitorDB,
        StrategyDB,
        StrategyStatus,
        StrategyType,
    )

    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    StrategyBase.metadata.create_all(bind=db.get_bind())
    strategy = StrategyDB(
        id="strategy-realtime-skip-position",
        name="实时持仓过滤策略",
        strategy_type=StrategyType.SELECTION,
        status=StrategyStatus.ACTIVE,
        is_active=True,
        created_at=datetime(2026, 5, 29, 9, 20, 0),
        updated_at=datetime(2026, 5, 29, 9, 20, 0),
    )
    db.add(strategy)
    selection = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "items": [
            {
                "rank": 1,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 86.0,
                "risk_control": {
                    "action": "deploy",
                    "risk_level": "low",
                    "max_position_pct": 8.0,
                    "stop_loss_pct": 4.5,
                    "risk_monitoring": {"execution_gate": "allow"},
                },
                "theme_matches": [
                    {
                        "theme": "人工智能",
                        "score": 90.0,
                        "relation_score": 95.0,
                        "event_semantic": {"event_type": "政策支持"},
                    }
                ],
            }
        ],
    }
    pool_payload = catalyst_selection_service.build_monitor_pool_from_selection(selection)
    monitor = RealtimeMonitorDB(
        id="monitor-realtime-skip-position",
        user_id="user-1",
        name="催化持仓过滤监控",
        account_key="paper_sim",
        account_role="paper",
        strategy_id=strategy.id,
        status="running",
        execution_mode="monitor_only",
        auto_trade_enabled=False,
        live_trading_enabled=False,
        monitor_pool_json=pool_payload["monitor_pool"],
        config_json={},
        risk_config_json=pool_payload["risk_config"],
        state_json={},
        created_at=datetime(2026, 5, 29, 9, 25, 0),
        updated_at=datetime(2026, 5, 29, 9, 25, 0),
    )
    db.add(monitor)
    db.add(
        RealtimeEventDB(
            id="event-position-outside-pool",
            monitor_id=monitor.id,
            user_id="user-1",
            event_type="position_changed",
            account_key="paper_sim",
            strategy_id=strategy.id,
            symbol="300520.SZ",
            trade_time=datetime(2026, 6, 2, 10, 2, 0),
            request_id="position-outside-pool",
            correlation_id="cycle-1",
            created_at=datetime(2026, 6, 2, 10, 2, 0),
        )
    )
    db.commit()

    captured = catalyst_selection_service.capture_realtime_monitor_feedback(
        db,
        db,
        monitor_id=monitor.id,
        now_value=datetime(2026, 6, 2, 10, 3, 0),
    )
    total_rows = db.execute(text("SELECT COUNT(*) FROM catalyst_selection_realtime_feedback")).scalar()

    assert captured["captured_count"] == 0
    assert captured["skipped_count"] == 1
    assert captured["feedback_refresh"] is None
    assert total_rows == 0


def test_event_type_feedback_profiles_are_learned_and_used(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    event_match = json.dumps(
        [
            {
                "theme": "人工智能",
                "score": 90,
                "relation_score": 95,
                "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88, "confidence": 0.85},
            }
        ],
        ensure_ascii=False,
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_items (
                run_id, trade_date, window_label, rank, symbol, name, score,
                theme_matches_json, market_background, created_at, updated_at
            )
            VALUES
            ('event-r1', '2026-05-20', 'premarket', 1, '603019.SH', '中科曙光', 82, :matches, 'mock', NOW(), NOW()),
            ('event-r2', '2026-05-21', 'premarket', 1, '603019.SH', '中科曙光', 78, :matches, 'mock', NOW(), NOW())
            """
        ),
        {"matches": event_match},
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_settlements (
                trade_date, settlement_date, symbol, name, rank,
                change_pct, hit_score, outcome, protected, settlement_notes_json, updated_at
            )
            VALUES
            ('2026-05-20', '2026-05-21', '603019.SH', '中科曙光', 1, 6.0, 82, 'strong_hit', TRUE, '[]', NOW()),
            ('2026-05-21', '2026-05-22', '603019.SH', '中科曙光', 1, 3.0, 68, 'hit', TRUE, '[]', NOW())
            """
        )
    )
    db.commit()

    refresh = catalyst_selection_service._refresh_feedback_profiles_from_settlements(
        db,
        symbols=[],
        themes=[],
        event_types=["政策支持"],
        now_value=datetime(2026, 5, 23, 9, 0, 0),
    )
    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=[],
        themes=[],
        event_types=["政策支持"],
    )
    theme_items = catalyst_selection_service._attach_event_feedback_profiles(
        [
            {
                "theme": "人工智能",
                "event_semantic": {"event_type": "政策支持"},
            }
        ],
        profiles["event_types"],
    )
    score, reasons = catalyst_selection_service._adaptive_feedback_score(
        symbol="603019.SH",
        primary_theme=theme_items[0],
        history_stats={},
        theme_feedback={},
    )

    assert refresh["event_type_profile_count"] == 1
    assert profiles["profile_count"] == 1
    assert profiles["sample_count"] == 2
    assert profiles["event_types"]["政策支持"]["hit_rate"] == pytest.approx(1.0)
    assert theme_items[0]["event_feedback_profile"]["profile_key"] == "政策支持"
    assert score > 55
    assert any("事件类型政策支持学习画像" in reason for reason in reasons)


def test_risk_gate_feedback_profiles_are_learned_from_settlements(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    rows = [
        ("allow", "miss", -3.0, 28.0),
        ("allow", "weak_miss", -2.4, 32.0),
        ("allow", "miss", -4.0, 24.0),
        ("allow", "weak_miss", -1.8, 36.0),
        ("blocked", "miss", -3.2, 30.0),
        ("blocked", "weak_miss", -2.8, 34.0),
        ("blocked", "miss", -4.5, 24.0),
        ("blocked", "weak_miss", -1.5, 38.0),
    ]
    for index, (gate, outcome, change_pct, hit_score) in enumerate(rows):
        trade_day = f"2026-05-{10 + index:02d}"
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_items (
                    run_id, trade_date, window_label, rank, symbol, name, score,
                    risk_control_json, market_background, created_at, updated_at
                )
                VALUES (
                    :run_id, :trade_date, 'premarket', 1, :symbol, :name, 80,
                    :risk_control_json, 'mock', NOW(), NOW()
                )
                """
            ),
            {
                "run_id": f"risk-gate-{index}",
                "trade_date": trade_day,
                "symbol": f"600{index:03d}.SH",
                "name": f"gate-{index}",
                "risk_control_json": json.dumps({"risk_monitoring": {"execution_gate": gate}}, ensure_ascii=False),
            },
        )
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_settlements (
                    trade_date, settlement_date, symbol, name, rank,
                    change_pct, hit_score, outcome, protected, settlement_notes_json, updated_at
                )
                VALUES (
                    :trade_date, :settlement_date, :symbol, :name, 1,
                    :change_pct, :hit_score, :outcome, TRUE, '[]', NOW()
                )
                """
            ),
            {
                "trade_date": trade_day,
                "settlement_date": f"2026-05-{11 + index:02d}",
                "symbol": f"600{index:03d}.SH",
                "name": f"gate-{index}",
                "change_pct": change_pct,
                "hit_score": hit_score,
                "outcome": outcome,
            },
        )
    db.commit()

    refresh = catalyst_selection_service._refresh_feedback_profiles_from_settlements(
        db,
        symbols=[],
        themes=[],
        event_types=[],
        risk_gates=["allow", "blocked"],
        now_value=datetime(2026, 5, 20, 9, 0, 0),
    )
    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=[],
        themes=[],
        risk_gates=["allow", "blocked"],
    )

    assert refresh["risk_gate_profile_count"] == 2
    assert profiles["profile_count"] == 2
    allow_profile = profiles["risk_gates"]["allow"]
    blocked_profile = profiles["risk_gates"]["blocked"]
    assert allow_profile["learned_score"] < 45
    assert allow_profile["feature_snapshot"]["adverse_count"] == 4
    assert blocked_profile["learned_score"] > 60
    assert blocked_profile["feature_snapshot"]["gate_policy"] == "protective"
    assert blocked_profile["feature_snapshot"]["protection_count"] == 4
    assert blocked_profile["feature_snapshot"]["opportunity_cost_count"] == 0


def test_intraday_pulse_feedback_profiles_are_learned_from_settlements(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    trace = json.dumps(
        {
            "market": {
                "intraday_event_pulse": {
                    "status": "weak",
                    "message": "事件池分钟反应偏弱",
                    "sample_count": 4,
                }
            }
        },
        ensure_ascii=False,
    )
    rows = [
        ("2026-05-10", "miss", -2.4, 32.0),
        ("2026-05-11", "weak_miss", -1.6, 38.0),
        ("2026-05-12", "miss", -3.2, 28.0),
    ]
    for index, (trade_day, outcome, change_pct, hit_score) in enumerate(rows):
        symbol = f"6880{index:02d}.SH"
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_items (
                    run_id, trade_date, window_label, rank, symbol, name, score,
                    closed_loop_trace_json, market_background, created_at, updated_at
                )
                VALUES (
                    :run_id, :trade_date, '24h', 1, :symbol, :name, 70,
                    :trace, 'mock', NOW(), NOW()
                )
                """
            ),
            {
                "run_id": f"pulse-{index}",
                "trade_date": trade_day,
                "symbol": symbol,
                "name": f"pulse-{index}",
                "trace": trace,
            },
        )
        db.execute(
            text(
                """
                INSERT INTO catalyst_selection_settlements (
                    trade_date, settlement_date, symbol, name, rank,
                    change_pct, hit_score, outcome, protected, settlement_notes_json, updated_at
                )
                VALUES (
                    :trade_date, :settlement_date, :symbol, :name, 1,
                    :change_pct, :hit_score, :outcome, TRUE, '[]', NOW()
                )
                """
            ),
            {
                "trade_date": trade_day,
                "settlement_date": f"2026-05-{11 + index:02d}",
                "symbol": symbol,
                "name": f"pulse-{index}",
                "change_pct": change_pct,
                "hit_score": hit_score,
                "outcome": outcome,
            },
        )
    db.commit()

    refresh = catalyst_selection_service._refresh_feedback_profiles_from_settlements(
        db,
        symbols=[],
        themes=[],
        event_types=[],
        risk_gates=[],
        now_value=datetime(2026, 5, 20, 9, 0, 0),
    )
    profiles = catalyst_selection_service._load_feedback_profiles(
        db,
        symbols=[],
        themes=[],
        intraday_pulses=["weak"],
    )
    weak_profile = profiles["intraday_pulses"]["weak"]
    profile = catalyst_selection_service._adaptive_score_profile(
        {
            "intraday_event_pulse": {"status": "weak", "message": "事件池分钟反应偏弱"},
            "intraday_event_pulse_feedback": weak_profile,
            "liquidity_state": {"label": "流动性温和"},
            "breadth_state": {"label": "温和扩散"},
            "market_regime": {"label": "结构性轮动"},
            "risk_pressure": {"label": "结构性执行风险"},
        }
    )

    assert refresh["intraday_pulse_profile_count"] == 1
    assert weak_profile["sample_count"] == 3
    assert weak_profile["learned_score"] < 45
    assert weak_profile["feature_snapshot"]["profile_kind"] == "intraday_pulse"
    assert weak_profile["feature_snapshot"]["pulse_status"] == "weak"
    assert profiles["profile_count"] == 1
    assert any("事件池脉冲历史反馈偏弱" in reason for reason in profile["reasons"])


def test_risk_gate_feedback_downgrades_poor_allow_gate():
    theme_items = [
        {
            "theme": "人工智能",
            "score": 90.0,
            "summary": "AI基础设施政策支持",
            "catalyst": "国务院政策支持AI基础设施",
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "evidence_items": [{"content": "国务院政策支持人工智能基础设施"}],
            "related_symbols": [{"symbol": "603019.SH", "name": "中科曙光"}],
            "market_confirmation": {"score": 12.0},
            "event_semantic": {"event_type": "政策支持", "catalyst_strength": 88.0, "confidence": 0.9},
            "mainline_alignment_score": 72.0,
            "mainline_alignment_reasons": ["盘面强度确认：人工智能"],
        }
    ]
    features = {
        "symbol": "603019.SH",
        "name": "中科曙光",
        "industry": "人工智能",
        "sector": "计算机",
        "concepts": ["人工智能", "算力"],
        "change_pct": 2.0,
        "amount_ratio_20d": 1.4,
        "momentum_20d": 0.08,
        "momentum_60d": 0.12,
        "r60": 70.0,
        "net_profit_growth_proxy": 0.2,
    }

    scored = catalyst_selection_service._score_candidate(
        symbol="603019.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={"learned_score": 78.0, "confidence": 1.0},
        theme_feedback={"人工智能": {"learned_score": 76.0, "confidence": 1.0}},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "风险可控"}},
        risk_gate_feedback={
            "allow": {
                "profile_scope": "risk_gate",
                "profile_key": "allow",
                "sample_count": 8,
                "hit_count": 1,
                "miss_count": 7,
                "hit_rate": 0.125,
                "learned_score": 28.0,
                "confidence": 1.0,
                "feature_snapshot": {"gate_policy": "permissive", "favorable_count": 1, "adverse_count": 7},
            }
        },
    )

    monitor = scored["risk_control"]["risk_monitoring"]
    assert scored["risk_control"]["action"] == "deploy"
    assert monitor["execution_gate"] == "confirm"
    assert monitor["gate_feedback"]["adjustment"] == "downgrade_to_confirm"
    assert monitor["gate_feedback"]["influence"] == "tighten"
    assert monitor["position_limit_pct"] < 10.0
    assert any("历史放行后命中不足" in note for note in monitor["notes"])
    assert scored["risk_control"]["risk_gate_effect"]["gate_before_feedback"] == "allow"
    assert scored["risk_control"]["risk_gate_effect"]["gate_after_feedback"] == "confirm"
    assert scored["risk_control"]["risk_gate_effect"]["applied"] is True
    impact = scored["closed_loop_trace"]["scoring"]["learning_impact"]
    assert impact["status"] == "active"
    assert impact["risk_gate_effect"]["adjustment"] == "downgrade_to_confirm"


def test_risk_gate_feedback_flags_overly_conservative_blocked_gate():
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "summary": "产业进展",
            "catalyst": "产业进展",
            "source_tier": "A",
            "top_source_tier": "A",
            "evidence_items": [{"content": "产业进展"}],
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "market_confirmation": {"score": 8.0},
            "event_semantic": {"event_type": "产业进展", "catalyst_strength": 78.0, "confidence": 0.8},
            "mainline_alignment_score": 66.0,
        }
    ]
    features = {
        "symbol": "600584.SH",
        "name": "长电科技",
        "industry": "半导体",
        "sector": "电子",
        "concepts": ["半导体", "芯片"],
        "change_pct": 1.5,
        "amount_ratio_20d": 1.2,
        "momentum_20d": 0.04,
        "momentum_60d": 0.08,
        "r60": 68.0,
        "net_profit_growth_proxy": 0.1,
    }

    scored = catalyst_selection_service._score_candidate(
        symbol="600584.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={"learned_score": 72.0, "confidence": 1.0},
        theme_feedback={"半导体": {"learned_score": 70.0, "confidence": 1.0}},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "退潮压力"}},
        risk_gate_feedback={
            "blocked": {
                "profile_scope": "risk_gate",
                "profile_key": "blocked",
                "sample_count": 8,
                "hit_count": 1,
                "miss_count": 7,
                "hit_rate": 0.125,
                "learned_score": 30.0,
                "confidence": 1.0,
                "feature_snapshot": {
                    "gate_policy": "protective",
                    "protection_count": 1,
                    "opportunity_cost_count": 7,
                    "raw_average_change_pct": 4.5,
                },
            }
        },
    )

    monitor = scored["risk_control"]["risk_monitoring"]
    assert monitor["execution_gate"] == "blocked"
    assert monitor["gate_feedback"]["adjustment"] == "overly_conservative"
    assert monitor["gate_feedback"]["overly_conservative"] is True
    assert monitor["gate_feedback"]["recommended_gate"] == "confirm_after_recheck"
    assert "观察性试仓" in monitor["next_action"]


def test_blocked_execution_gate_discounts_final_candidate_score():
    theme_items = [
        {
            "theme": "人工智能",
            "score": 96.0,
            "summary": "AI政策催化",
            "catalyst": "AI政策催化",
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "evidence_items": [{"content": "AI政策催化"}],
            "related_symbols": [{"symbol": "002230.SZ", "name": "科大讯飞"}],
            "market_confirmation": {"score": 12.0},
            "event_semantic": {"event_type": "政策支持", "catalyst_strength": 86.0, "confidence": 0.9},
            "mainline_alignment_score": 82.0,
        }
    ]
    features = {
        "symbol": "002230.SZ",
        "name": "科大讯飞",
        "industry": "软件开发",
        "sector": "计算机",
        "concepts": ["人工智能", "大模型"],
        "change_pct": 2.0,
        "amount_ratio_20d": 1.6,
        "momentum_20d": 0.08,
        "momentum_60d": 0.12,
        "r60": 72.0,
        "net_profit_growth_proxy": 0.2,
        "event_reaction": {"status": "weak", "score": 46.0},
    }

    scored = catalyst_selection_service._score_candidate(
        symbol="002230.SZ",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={"learned_score": 78.0, "confidence": 1.0},
        theme_feedback={"人工智能": {"learned_score": 76.0, "confidence": 1.0}},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "退潮压力"}},
        risk_gate_feedback={},
    )

    adjustment = scored["execution_gate_adjustment"]
    assert scored["risk_control"]["risk_monitoring"]["execution_gate"] == "blocked"
    assert adjustment["score_delta"] <= -18.0
    assert scored["pre_execution_score"] > scored["score"]
    assert scored["score"] == pytest.approx(scored["pre_execution_score"] + adjustment["score_delta"], abs=0.01)
    assert any("执行门控 blocked" in part for part in scored["reason_parts"])
    trace_scores = scored["closed_loop_trace"]["scoring"]["component_scores"]
    assert trace_scores["execution_gate_adjustment"] == adjustment["score_delta"]


def test_row_to_item_restores_execution_gate_adjustment_from_trace():
    row = {
        "settlement_date": None,
        "rank": 1,
        "symbol": "002230.SZ",
        "name": "科大讯飞",
        "industry": "软件开发",
        "sector": "计算机",
        "concepts_json": json.dumps(["人工智能"], ensure_ascii=False),
        "score": 64.0,
        "catalyst_score": 90.0,
        "theme_score": 88.0,
        "relation_score": 92.0,
        "market_confirm_score": 46.0,
        "event_intelligence_score": 84.0,
        "momentum_score": 70.0,
        "fundamental_score": 55.0,
        "continuity_score": 30.0,
        "adaptive_feedback_score": 68.0,
        "risk_penalty": 7.0,
        "risk_flags_json": "[]",
        "reason_parts_json": "[]",
        "theme_matches_json": "[]",
        "signal_flags_json": "[]",
        "risk_control_json": json.dumps(
            {"action": "observe", "risk_monitoring": {"status": "blocked", "execution_gate": "blocked"}},
            ensure_ascii=False,
        ),
        "closed_loop_trace_json": json.dumps(
            {
                "scoring": {
                    "component_scores": {
                        "pre_execution_score": 82.0,
                        "execution_gate_adjustment": -18.0,
                    },
                    "execution_gate_adjustment": {
                        "gate": "blocked",
                        "status": "blocked",
                        "action": "observe",
                        "score_delta": -18.0,
                        "reason": "执行门控 blocked，禁止建仓",
                    },
                }
            },
            ensure_ascii=False,
        ),
        "market_background": "mock",
        "market_behavior_json": "{}",
        "metric_snapshot_json": "{}",
    }

    item = catalyst_selection_service._row_to_item(row)

    assert item["pre_execution_score"] == 82.0
    assert item["execution_gate_adjustment"]["score_delta"] == -18.0
    assert item["execution_gate_adjustment"]["reason"] == "执行门控 blocked，禁止建仓"


def test_learning_profile_changes_candidate_ranking_and_risk_action():
    theme_items = [
        {
            "theme": "半导体",
            "score": 86.0,
            "summary": "先进封装订单增长",
            "catalyst": "先进封装订单增长",
            "source_tier": "A",
            "top_source_tier": "A",
            "policy_boost": False,
            "evidence_items": [{"content": "先进封装订单增长", "published_at": "2026-05-26T09:00:00"}],
            "consensus_rate": 1.0,
            "mainline_alignment_score": 60.0,
            "mainline_alignment_reasons": ["盘面强度确认：半导体"],
        }
    ]
    features = {
        "symbol": "600584.SH",
        "name": "长电科技",
        "industry": "半导体",
        "sector": "电子",
        "concepts": ["电子", "半导体", "芯片"],
        "change_pct": 2.5,
        "amount_ratio_20d": 1.5,
        "momentum_20d": 0.08,
        "momentum_60d": 0.12,
        "r60": 72.0,
        "net_profit_growth_proxy": 0.2,
    }
    market_behavior = {"risk_pressure": {"label": "风险可控"}}

    strong = catalyst_selection_service._score_candidate(
        symbol="600584.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={"learned_score": 86.0, "confidence": 1.0},
        theme_feedback={"半导体": {"learned_score": 78.0, "confidence": 1.0}},
        market_background="mock",
        market_behavior=market_behavior,
    )
    weak = catalyst_selection_service._score_candidate(
        symbol="600584.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={"learned_score": 24.0, "confidence": 1.0, "loss_count": 3},
        theme_feedback={"半导体": {"learned_score": 35.0, "confidence": 1.0}},
        market_background="mock",
        market_behavior=market_behavior,
    )

    assert strong["adaptive_feedback_score"] > weak["adaptive_feedback_score"]
    assert strong["score"] > weak["score"]
    assert weak["risk_control"]["action"] == "observe"
    assert strong["closed_loop_trace"]["feedback"]["model_version"] == catalyst_selection_service.FEEDBACK_MODEL_VERSION
    assert strong["closed_loop_trace"]["feedback"]["symbol_profile"]["profile_key"] == "600584.SH"
    assert strong["closed_loop_trace"]["feedback"]["symbol_profile"]["learned_score"] == 86.0
    assert strong["closed_loop_trace"]["feedback"]["theme_profile"]["profile_key"] == "半导体"
    assert strong["closed_loop_trace"]["feedback"]["theme_profile"]["learned_score"] == 78.0
    strong_impact = strong["closed_loop_trace"]["scoring"]["learning_impact"]
    weak_impact = weak["closed_loop_trace"]["scoring"]["learning_impact"]
    assert strong_impact["score_delta_from_learning_policy"] > 0
    assert weak_impact["score_delta_from_learning_policy"] < 0
    assert weak_impact["risk_effect"]["max_position_delta_pct"] < 0
    assert weak_impact["risk_effect"]["action_changed"] is False


def test_learning_adjustment_policy_changes_risk_plan_position_and_action():
    features = {
        "symbol": "603019.SH",
        "name": "中科曙光",
        "change_pct": 2.0,
        "amount_ratio_20d": 1.2,
    }
    primary_theme = {
        "theme": "人工智能",
        "event_semantic": {
            "event_type": "政策支持",
            "invalidation_conditions": [],
            "risk_signals": [],
        },
    }
    market_behavior = {"risk_pressure": {"label": "风险可控"}}
    base_kwargs = {
        "features": features,
        "primary_theme": primary_theme,
        "market_behavior": market_behavior,
        "risk_penalty": 2.0,
        "risk_flags": [],
        "event_intelligence_score": 82.0,
        "adaptive_feedback_score": 70.0,
        "risk_gate_feedback": {},
    }

    neutral = catalyst_selection_service._risk_control_plan(
        **base_kwargs,
        learning_policy={"status": "warming_up", "stance": "neutral"},
    )
    expand = catalyst_selection_service._risk_control_plan(
        **base_kwargs,
        learning_policy={
            "status": "active",
            "stance": "expand",
            "learning_edge": 14.0,
            "max_position_multiplier": 1.12,
            "reasons": ["历史反馈画像偏强"],
        },
    )
    tighten = catalyst_selection_service._risk_control_plan(
        **base_kwargs,
        learning_policy={
            "status": "active",
            "stance": "tighten",
            "learning_edge": -14.0,
            "max_position_multiplier": 0.72,
            "max_position_cap_pct": 5.0,
            "reasons": ["历史反馈画像偏弱"],
        },
    )

    assert neutral["action"] == "deploy"
    assert neutral["max_position_pct"] == 10.0
    assert expand["action"] == "deploy"
    assert expand["max_position_pct"] > neutral["max_position_pct"]
    assert expand["learning_adjustment"]["stance"] == "expand"
    assert any("历史反馈偏强" in note for note in expand["notes"])
    assert tighten["action"] == "follow"
    assert tighten["risk_level"] == "medium"
    assert tighten["max_position_pct"] <= 5.0
    assert tighten["learning_adjustment"]["stance"] == "tighten"
    assert tighten["learning_effect"]["action_before_learning"] == "deploy"
    assert tighten["learning_effect"]["action_after_learning"] == "follow"
    assert tighten["learning_effect"]["action_changed"] is True
    assert tighten["learning_effect"]["max_position_delta_pct"] < 0
    assert any("历史反馈偏弱" in note for note in tighten["notes"])


def test_feedback_profile_model_version_remains_settlement_learning_version():
    assert catalyst_selection_service.FEEDBACK_MODEL_VERSION == "settlement-feedback-v1"
    assert catalyst_selection_service.REALTIME_FEEDBACK_MODEL_VERSION == "settlement-realtime-feedback-v2"


def test_event_semantics_drive_candidate_score_risk_and_trace():
    features = {
        "symbol": "603019.SH",
        "name": "中科曙光",
        "industry": "计算机",
        "sector": "信息技术",
        "concepts": ["人工智能", "算力", "AI服务器"],
        "change_pct": 2.2,
        "amount_ratio_20d": 1.4,
        "momentum_20d": 0.08,
        "momentum_60d": 0.14,
        "r60": 74.0,
        "net_profit_growth_proxy": 0.2,
    }
    common_theme = {
        "theme": "人工智能",
        "score": 82.0,
        "summary": "人工智能获得政策支持",
        "catalyst": "国务院行动方案支持人工智能基础设施",
        "source_tier": "S",
        "top_source_tier": "S",
        "policy_boost": True,
        "related_symbols": [{"symbol": "603019.SH", "name": "中科曙光"}],
        "evidence_items": [{"content": "国务院行动方案支持人工智能基础设施", "source_tier": "S", "published_at": "2026-05-26T09:00:00"}],
        "consensus_rate": 0.9,
        "mainline_alignment_score": 70.0,
        "mainline_alignment_reasons": ["盘面强度确认：人工智能"],
    }
    strong_theme = {
        **common_theme,
        "event_semantic": {
            "event_type": "政策支持",
            "catalyst_strength": 90.0,
            "beneficiary_chain": ["算力基础设施", "AI服务器"],
            "invalidation_conditions": ["政策落地低于预期"],
            "risk_signals": [],
            "confidence": 0.9,
            "reasoning": "政策直接支持AI基础设施",
        },
        "semantic_source": "llm:mock/mock-model",
    }
    weak_theme = {
        **common_theme,
        "event_semantic": {
            "event_type": "消息催化",
            "catalyst_strength": 35.0,
            "beneficiary_chain": ["人工智能"],
            "invalidation_conditions": ["消息被澄清"],
            "risk_signals": ["高位拥挤"],
            "confidence": 0.8,
            "reasoning": "证据强度不足且存在风险",
        },
        "semantic_source": "llm:mock/mock-model",
    }
    market_behavior = {"risk_pressure": {"label": "风险可控"}}

    strong = catalyst_selection_service._score_candidate(
        symbol="603019.SH",
        features=features,
        theme_items=[strong_theme],
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior=market_behavior,
    )
    weak = catalyst_selection_service._score_candidate(
        symbol="603019.SH",
        features=features,
        theme_items=[weak_theme],
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior=market_behavior,
    )

    assert strong["event_intelligence_score"] > weak["event_intelligence_score"]
    assert strong["catalyst_score"] > weak["catalyst_score"]
    assert strong["score"] > weak["score"]
    assert "政策落地低于预期" in strong["risk_control"]["invalidations"]
    assert any("事件语义风险" in note for note in weak["risk_control"]["notes"])
    assert weak["risk_penalty"] > strong["risk_penalty"]
    assert strong["theme_matches"][0]["event_semantic"]["event_type"] == "政策支持"
    assert strong["closed_loop_trace"]["event"]["semantic"]["event_type"] == "政策支持"
    assert strong["closed_loop_trace"]["event"]["semantic_source"] == "llm:mock/mock-model"


def test_event_minute_reaction_updates_market_confirmation_risk_and_trace(db):
    _seed_market_data(db)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            ('600584.SH', '2026-05-26 09:30:00', 10.50, 10.60, 10.45, 10.55, 100000, 1000000, NOW(), NOW()),
            ('600584.SH', '2026-05-26 09:45:00', 10.55, 10.92, 10.52, 10.90, 180000, 2200000, NOW(), NOW()),
            ('600584.SH', '2026-05-26 10:00:00', 10.90, 11.12, 10.88, 11.00, 200000, 2400000, NOW(), NOW()),
            ('002156.SZ', '2026-05-26 09:30:00', 20.40, 20.45, 20.20, 20.30, 70000, 900000, NOW(), NOW()),
            ('002156.SZ', '2026-05-26 09:45:00', 20.30, 20.35, 20.02, 20.10, 60000, 700000, NOW(), NOW()),
            ('002156.SZ', '2026-05-26 10:00:00', 20.10, 20.15, 19.95, 20.00, 50000, 500000, NOW(), NOW())
            """
        )
    )
    db.commit()
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：半导体"],
        },
        {
            "theme": "消费电子",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "002156.SZ", "name": "通富微电"}],
            "evidence_items": [{"content": "消费电子订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：消费电子"],
        },
    ]
    features = catalyst_selection_service._load_daily_features(
        db,
        symbols=["600584.SH", "002156.SZ"],
        trade_date="2026-05-26",
    )

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
    )

    assert governance["covered_symbol_count"] == 2
    assert governance["confirmed_count"] == 1
    assert governance["divergent_count"] == 1
    assert governance["data_freshness"]["status"] == "ready"
    assert governance["data_freshness"]["target_minute_rows"] == 6
    assert features["600584.SH"]["event_reaction"]["status"] == "confirmed"
    assert features["002156.SZ"]["event_reaction"]["status"] == "divergent"

    strong = catalyst_selection_service._score_candidate(
        symbol="600584.SH",
        features=features["600584.SH"],
        theme_items=theme_items,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "风险可控"}},
    )
    weak = catalyst_selection_service._score_candidate(
        symbol="002156.SZ",
        features=features["002156.SZ"],
        theme_items=theme_items,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior={"risk_pressure": {"label": "风险可控"}},
    )

    assert strong["market_confirm_score"] > weak["market_confirm_score"]
    assert strong["closed_loop_trace"]["market"]["event_reaction"]["status"] == "confirmed"
    assert weak["closed_loop_trace"]["market"]["event_reaction"]["status"] == "divergent"
    assert weak["risk_control"]["risk_monitoring"]["status"] == "invalidated"
    assert weak["risk_control"]["risk_monitoring"]["execution_gate"] == "blocked"
    assert "事件后市场反应背离" in weak["risk_control"]["risk_monitoring"]["triggers"]
    assert any("分钟反应确认" in flag for flag in strong["signal_flags"])
    assert any("分钟级市场反应背离" in flag for flag in weak["risk_flags"])


def test_event_reaction_uses_daily_proxy_when_minute_kline_missing(db):
    _seed_market_data(db)
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：半导体"],
        },
    ]
    features = catalyst_selection_service._load_daily_features(
        db,
        symbols=["600584.SH"],
        trade_date="2026-05-26",
    )

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
    )

    reaction = features["600584.SH"]["event_reaction"]
    assert governance["covered_symbol_count"] == 1
    assert governance["proxy_count"] == 1
    assert governance["missing_count"] == 0
    assert governance["data_freshness"]["status"] == "empty"
    assert reaction["status"] == "daily_proxy_confirmed"
    assert reaction["proxy"] is True
    assert "open_close_proxy" in reaction["source"]


def test_realtime_event_reaction_uses_current_trade_date_without_stale_daily_proxy(db):
    _seed_market_data(db)
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-27T10:15:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：半导体"],
        },
    ]
    features = catalyst_selection_service._load_daily_features(
        db,
        symbols=["600584.SH"],
        trade_date="2026-05-26",
    )

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-27",
        feature_trade_date="2026-05-26",
    )

    reaction = features["600584.SH"]["event_reaction"]
    assert governance["trade_date"] == "2026-05-27"
    assert governance["feature_trade_date"] == "2026-05-26"
    assert governance["daily_proxy_allowed"] is False
    assert governance["proxy_count"] == 0
    assert governance["missing_count"] == 1
    assert governance["data_freshness"]["status"] == "empty"
    assert reaction["status"] == "missing"
    assert reaction["trade_date"] == "2026-05-27"


def test_realtime_event_reaction_trade_date_follows_current_effective_trade_date(monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_effective_cn_trade_date", lambda: "2026-06-01")

    assert catalyst_selection_service._event_reaction_trade_date("2026-05-29", "24h") == "2026-06-01"
    assert catalyst_selection_service._event_reaction_trade_date("2026-05-29", "72h") == "2026-06-01"
    assert catalyst_selection_service._event_reaction_trade_date("2026-05-29", "premarket") == "2026-05-29"


def test_resolve_trade_date_prefers_later_minute_date_when_daily_lagged(db, monkeypatch):
    db.execute(
        text(
            """
            INSERT INTO stock_daily_kline (
                symbol, trade_date, open, high, low, close, volume, amount, turnover_rate,
                pre_close, sw_industry_l1, sw_industry_l2, sw_industry_l3, created_at, updated_at
            )
            VALUES (
                '600584.SH', '2026-06-01', 10.0, 10.8, 9.8, 10.5, 1000000, 15000000, 1.8,
                10.0, '电子', '半导体', '芯片', NOW(), NOW()
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (
                symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at
            )
            VALUES (
                '600584.SH', '2026-06-03 14:39:00', 10.0, 10.3, 9.9, 10.2, 10000, 120000, NOW(), NOW()
            )
            """
        )
    )
    db.commit()
    monkeypatch.setattr(catalyst_selection_service, "_effective_cn_trade_date", lambda: "2026-06-03")

    assert catalyst_selection_service._resolve_trade_date(db, None) == "2026-06-03"
    assert catalyst_selection_service._feature_trade_date_for_selection(db, "2026-06-03") == "2026-06-01"


def test_realtime_feedback_lookup_trade_date_uses_event_reaction_date_for_realtime_windows():
    assert (
        catalyst_selection_service._realtime_feedback_lookup_trade_date(
            trade_date="2026-06-01",
            event_reaction_trade_date="2026-06-02",
            window="24h",
        )
        == "2026-06-02"
    )
    assert (
        catalyst_selection_service._realtime_feedback_lookup_trade_date(
            trade_date="2026-06-01",
            event_reaction_trade_date="2026-06-02",
            window="premarket",
        )
        == "2026-06-01"
    )


def test_current_event_minute_capture_auto_allows_after_close_but_skips_preopen(monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 6, 1, 19, 0, tzinfo=CN_TZ))
    monkeypatch.setattr(catalyst_selection_service, "is_cn_trading_day", lambda value: True)
    monkeypatch.delenv("AI_QUANT_EVENT_MINUTE_CAPTURE_AFTER_HOURS", raising=False)

    assert catalyst_selection_service._event_minute_capture_allowed("2026-06-01") is True

    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 6, 1, 8, 50, tzinfo=CN_TZ))
    assert catalyst_selection_service._event_minute_capture_allowed("2026-06-01") is False
    assert "盘前尚无当日分钟线" in catalyst_selection_service._event_minute_capture_skip_message("2026-06-01")

    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 6, 1, 19, 0, tzinfo=CN_TZ))
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_AFTER_HOURS", "0")
    assert catalyst_selection_service._event_minute_capture_allowed("2026-06-01") is False
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_AFTER_HOURS", "1")
    assert catalyst_selection_service._event_minute_capture_allowed("2026-06-01") is True


def test_event_reaction_requests_qmt_capture_for_missing_candidate_minutes(db, monkeypatch):
    _seed_market_data(db)
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL", "1")
    captured: dict[str, object] = {}

    def fake_capture(symbols, *, trade_date, period, account_key, db, user_id, timeout_seconds=None, retry_missing=True):
        captured.update(
            {
                "symbols": list(symbols),
                "trade_date": trade_date,
                "period": period,
                "account_key": account_key,
                "user_id": user_id,
                "timeout_seconds": timeout_seconds,
                "retry_missing": retry_missing,
            }
        )
        db.execute(
            text(
                """
                INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
                VALUES
                ('600584.SH', '2026-05-26 09:30:00', 10.50, 10.60, 10.45, 10.55, 100000, 1000000, NOW(), NOW()),
                ('600584.SH', '2026-05-26 09:45:00', 10.55, 10.92, 10.52, 10.90, 180000, 2200000, NOW(), NOW()),
                ('600584.SH', '2026-05-26 10:00:00', 10.90, 11.12, 10.88, 11.00, 200000, 2400000, NOW(), NOW())
                """
            )
        )
        db.commit()
        return {
            "success": True,
            "rows": 3,
            "captured_symbols": ["600584.SH"],
            "missing_symbols": [],
            "symbol_rows": {"600584.SH": 3},
            "symbol_latest_trade_times": {"600584.SH": "2026-05-26 10:00:00"},
            "message": "mock captured",
            "source": "qmt_intraday",
        }

    monkeypatch.setattr(catalyst_selection_service, "capture_intraday_symbols", fake_capture)
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：半导体"],
        },
    ]
    features = catalyst_selection_service._load_daily_features(
        db,
        symbols=["600584.SH"],
        trade_date="2026-05-26",
    )

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
        user_id="user-1",
    )

    assert captured["symbols"] == ["600584.SH"]
    assert captured["period"] == "1m"
    assert captured["user_id"] == "user-1"
    assert captured["timeout_seconds"] == 4.0
    assert captured["retry_missing"] is False
    assert governance["capture"]["requested"] is True
    assert governance["capture"]["rows"] == 3
    assert governance["capture"]["timeout_seconds"] == 4.0
    assert governance["capture"]["retry_missing"] is False
    assert governance["minute_covered_symbol_count"] == 1
    assert governance["proxy_count"] == 0
    assert features["600584.SH"]["event_reaction"]["status"] == "confirmed"
    assert features["600584.SH"]["event_reaction"]["source"] == "postgresql:stock_minute_kline"


def test_event_reaction_schedules_history_backfill_when_fast_capture_empty(db, monkeypatch):
    _seed_market_data(db)
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL", "1")
    monkeypatch.setenv("QMT_MINUTE_DATABASE_URL", "postgresql://qmt_sync:secret@10.0.0.2:5432/trading_agents")
    created: dict[str, object] = {}

    def fake_capture(symbols, **kwargs):
        created["fast_symbols"] = list(symbols)
        return {
            "success": False,
            "rows": 0,
            "captured_symbols": [],
            "missing_symbols": list(symbols),
            "message": "no intraday bars",
            "source": "qmt_intraday",
        }

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"job_id": "job-123", "progress": 10, "message": "history job created"}

    def fake_post(url, json, headers, timeout):
        created.update({"url": url, "payload": json, "headers": headers, "timeout": timeout})
        return FakeResponse()

    from api.data_downloader import DataDownloader

    monkeypatch.setattr(catalyst_selection_service, "capture_intraday_symbols", fake_capture)
    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: {"bridge_base_url": "http://192.168.10.1:8710", "bridge_token": "bridge-token", "account_key": "paper_sim", "role": "paper"}),
    )
    monkeypatch.setattr(catalyst_selection_service.requests, "post", fake_post)
    features = catalyst_selection_service._load_daily_features(
        db,
        symbols=["600584.SH"],
        trade_date="2026-05-26",
    )
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
        },
    ]

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
        user_id="user-1",
    )

    history = governance["capture"]["history_backfill"]
    assert governance["capture"]["requested"] is True
    assert governance["capture"]["rows"] == 0
    assert history["requested"] is True
    assert history["status"] == "scheduled"
    assert history["job_id"] == "job-123"
    assert history["bridge"] == "http://192.168.10.1:8710"
    assert created["url"] == "http://192.168.10.1:8710/history/minute/sync"
    assert created["payload"]["symbols"] == ["600584.SH"]
    assert created["payload"]["start_date"] == "2026-05-26"
    assert created["headers"]["Authorization"] == "Bearer bridge-token"
    assert features["600584.SH"]["event_reaction"]["status"] == "daily_proxy_confirmed"


def test_event_reaction_history_backfill_reports_bridge_timeout(monkeypatch):
    monkeypatch.setenv("QMT_MINUTE_DATABASE_URL", "postgresql://qmt_sync:secret@10.0.0.2:5432/trading_agents")
    catalyst_selection_service._EVENT_HISTORY_BRIDGE_FAILURES.clear()
    from api.data_downloader import DataDownloader

    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: {"bridge_base_url": "http://192.168.10.1:8710", "bridge_token": "", "account_key": "paper_sim", "role": "paper"}),
    )
    monkeypatch.setattr(
        catalyst_selection_service.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("bridge timeout")),
    )

    result = catalyst_selection_service._request_event_minute_history_backfill(
        ["600584.SH"],
        trade_date="2026-05-26",
    )

    assert result["requested"] is True
    assert result["status"] == "failed"
    assert result["degraded"] is True
    assert result["cooldown_until"]
    assert "连接超时" in result["message"]
    assert "192.168.10.1:8710" in result["message"]


def test_event_reaction_history_backfill_skips_unreachable_bridge_during_cooldown(monkeypatch):
    monkeypatch.setenv("QMT_MINUTE_DATABASE_URL", "postgresql://qmt_sync:secret@10.0.0.2:5432/trading_agents")
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_HISTORY_BRIDGE_COOLDOWN_SECONDS", "300")
    catalyst_selection_service._EVENT_HISTORY_BRIDGE_FAILURES.clear()
    from api.data_downloader import DataDownloader

    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: {"bridge_base_url": "http://192.168.10.1:8710", "bridge_token": "", "account_key": "paper_sim", "role": "paper"}),
    )
    calls = {"count": 0}

    def fail_post(*args, **kwargs):
        calls["count"] += 1
        raise TimeoutError("bridge timeout")

    monkeypatch.setattr(catalyst_selection_service.requests, "post", fail_post)

    first = catalyst_selection_service._request_event_minute_history_backfill(
        ["600584.SH"],
        trade_date="2026-05-26",
    )
    second = catalyst_selection_service._request_event_minute_history_backfill(
        ["600584.SH"],
        trade_date="2026-05-26",
    )

    assert first["status"] == "failed"
    assert second["requested"] is False
    assert second["status"] == "cooldown"
    assert second["degraded"] is True
    assert "不可达冷却期" in second["message"]
    assert second["cooldown_until"] == first["cooldown_until"]
    assert calls["count"] == 1
    catalyst_selection_service._EVENT_HISTORY_BRIDGE_FAILURES.clear()


def test_event_minute_history_bridge_prefers_user_qmt_config(db, monkeypatch):
    from api.services import auth_service
    from api.data_downloader import DataDownloader

    user_id = "qmt-history-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        qmt_paper_account_config={
            "key": "paper_sim",
            "role": "paper",
            "enabled": True,
            "bridge_base_url": "http://192.168.10.1:8710",
            "bridge_token": "paper-token",
            "account_id": "68042452",
        },
        qmt_live_account_config={
            "key": "live_real",
            "role": "live",
            "enabled": True,
            "bridge_base_url": "http://192.168.10.1:8711",
            "bridge_token": "live-token",
            "account_id": "8886186680",
        },
    )
    monkeypatch.setenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", "live_real")
    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: (_ for _ in ()).throw(AssertionError("env fallback should not be used when user config exists"))),
    )

    bridge = catalyst_selection_service._resolve_event_minute_history_bridge(db=db, user_id=user_id)

    assert bridge == {
        "bridge_base_url": "http://192.168.10.1:8711",
        "bridge_token": "your-bridge-token",
        "account_key": "live_real",
        "account_id": "8886186680",
        "role": "live",
    }


def test_event_reaction_schedules_akshare_backfill_when_qmt_bridge_fails(db, monkeypatch):
    _seed_market_data(db)
    catalyst_selection_service._EVENT_HISTORY_BRIDGE_FAILURES.clear()
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL", "1")
    monkeypatch.setenv("QMT_MINUTE_DATABASE_URL", "postgresql://qmt_sync:secret@10.0.0.2:5432/trading_agents")
    from api.data_downloader import DataDownloader

    started: dict[str, int] = {"count": 0}

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started["count"] += 1

    monkeypatch.setattr(catalyst_selection_service.threading, "Thread", FakeThread)
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    monkeypatch.setattr(
        catalyst_selection_service,
        "capture_intraday_symbols",
        lambda symbols, **kwargs: {
            "success": False,
            "rows": 0,
            "captured_symbols": [],
            "missing_symbols": list(symbols),
            "message": "no intraday bars",
            "source": "qmt_intraday",
        },
    )
    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: {"bridge_base_url": "http://192.168.10.1:8710", "bridge_token": "", "account_key": "paper_sim", "role": "paper"}),
    )
    monkeypatch.setattr(
        catalyst_selection_service.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("bridge timeout")),
    )
    features = catalyst_selection_service._load_daily_features(db, symbols=["600584.SH"], trade_date="2026-05-26")
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
        },
    ]

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
        user_id="user-1",
    )

    assert governance["capture"]["history_backfill"]["status"] == "failed"
    akshare = governance["capture"]["akshare_backfill"]
    assert akshare["requested"] is True
    assert akshare["status"] == "scheduled"
    assert akshare["symbols"] == ["600584.SH"]
    assert started["count"] == 1


def test_event_reaction_schedules_akshare_backfill_when_qmt_capture_raises(db, monkeypatch):
    _seed_market_data(db)
    catalyst_selection_service._EVENT_HISTORY_BRIDGE_FAILURES.clear()
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_CAPTURE_HISTORICAL", "1")
    monkeypatch.setenv("QMT_MINUTE_DATABASE_URL", "postgresql://qmt_sync:secret@10.0.0.2:5432/trading_agents")
    from api.data_downloader import DataDownloader

    started: dict[str, int] = {"count": 0}

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started["count"] += 1

    monkeypatch.setattr(catalyst_selection_service.threading, "Thread", FakeThread)
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    monkeypatch.setattr(
        catalyst_selection_service,
        "capture_intraday_symbols",
        lambda symbols, **kwargs: (_ for _ in ()).throw(RuntimeError("bridge unavailable")),
    )
    monkeypatch.setattr(
        DataDownloader,
        "_resolve_qmt_history_bridge",
        staticmethod(lambda: {"bridge_base_url": "http://192.168.10.1:8710", "bridge_token": "", "account_key": "paper_sim", "role": "paper"}),
    )
    monkeypatch.setattr(
        catalyst_selection_service.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("bridge timeout")),
    )
    features = catalyst_selection_service._load_daily_features(db, symbols=["600584.SH"], trade_date="2026-05-26")
    theme_items = [
        {
            "theme": "半导体",
            "score": 88.0,
            "source_tier": "S",
            "top_source_tier": "S",
            "related_symbols": [{"symbol": "600584.SH", "name": "长电科技"}],
            "evidence_items": [{"content": "先进封装订单增长", "source_tier": "S", "published_at": "2026-05-26T08:45:00"}],
            "market_confirmation": {"score": 8.0},
            "mainline_alignment_score": 70.0,
        },
    ]

    governance = catalyst_selection_service._attach_event_reaction_features(
        db,
        features_by_symbol=features,
        theme_items=theme_items,
        trade_date="2026-05-26",
        user_id="user-1",
    )

    capture = governance["capture"]
    assert capture["requested"] is True
    assert capture["rows"] == 0
    assert "QMT分钟线补采异常" in capture["message"]
    assert capture["history_backfill"]["status"] == "failed"
    akshare = capture["akshare_backfill"]
    assert akshare["requested"] is True
    assert akshare["status"] == "scheduled"
    assert akshare["symbols"] == ["600584.SH"]
    assert started["count"] == 1


def test_event_akshare_backfill_worker_updates_job(monkeypatch):
    from api.data_downloader import DataDownloader

    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    job_key = "unit-akshare-job"
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key] = {
        "job_key": job_key,
        "requested": True,
        "status": "scheduled",
        "symbols": ["600584.SH"],
        "requested_symbol_count": 1,
        "trade_date": "2026-05-26",
    }

    async def fake_download(self, symbol, start_date, end_date, force=False, source="akshare"):
        assert symbol == "600584.SH"
        assert start_date.isoformat() == "2026-05-26"
        assert end_date.isoformat() == "2026-05-26"
        assert source == "akshare"
        return {"success": True, "records": 3, "source": "akshare"}

    monkeypatch.setattr(DataDownloader, "download_minute_kline", fake_download)

    asyncio.run(
        catalyst_selection_service._run_event_minute_akshare_backfill_job_async(
            job_key,
            ["600584.SH"],
            "2026-05-26",
        )
    )

    job = catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key]
    assert job["status"] == "completed"
    assert job["rows"] == 3
    assert "写入 3 行" in job["message"]


def test_event_akshare_backfill_auto_refreshes_selection_after_rows(db, monkeypatch):
    from api.data_downloader import DataDownloader

    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    job_key = "unit-akshare-refresh-job"
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key] = {
        "job_key": job_key,
        "requested": True,
        "status": "scheduled",
        "mode": "async",
        "symbols": ["600584.SH"],
        "requested_symbol_count": 1,
        "trade_date": "2026-05-26",
        "selection_refresh_enabled": True,
        "selection_refresh_contexts": {
            "2026-05-26:24h:5:user-1": {
                "trade_date": "2026-05-26",
                "window": "24h",
                "limit": 5,
                "user_id": "user-1",
            }
        },
        "selection_refresh": {
            "requested": True,
            "status": "pending",
            "message": "AKShare分钟线补缺完成后将自动重算对应机会榜",
        },
    }

    calls: list[tuple[str, str, int, str | None]] = []
    trigger_contexts: list[dict[str, object]] = []

    def fake_generate_selections(session, *, trade_date, window, limit, user_id=None, **kwargs):
        calls.append((trade_date, window, limit, user_id))
        local_trigger_context = dict(kwargs.get("trigger_context") or {})
        trigger_contexts.append(local_trigger_context)
        return {
            "items": [
                {
                    "symbol": "600584.SH",
                    "name": "长电科技",
                    "score": 88.0,
                    "risk_control": {"action": "deploy", "risk_level": "low", "max_position_pct": 10.0, "stop_loss_pct": 5.6, "invalidations": []},
                }
            ],
            "data_governance": {
                "opportunity_events": [{"symbol": "600584.SH", "event_level": "S"}],
                "closed_loop": {
                    "risk_control_summary": {
                        "item_count": 1,
                        "action_counts": {"deploy": 1},
                        "risk_level_counts": {"low": 1},
                        "deploy_count": 1,
                        "follow_count": 0,
                        "wait_count": 0,
                        "observe_count": 0,
                        "restricted_count": 0,
                        "invalidation_count": 0,
                        "average_max_position_pct": 10.0,
                        "average_stop_loss_pct": 5.6,
                    },
                    "feedback_learning_state": {
                        "profile_count": 2,
                        "sample_count": 11,
                        "selected_with_feedback_count": 1,
                        "selected_count": 1,
                        "selected_adaptive_feedback_avg": 68.5,
                        "risk_gate_profile_count": 1,
                    },
                    "risk_gate_feedback_summary": {
                        "profile_count": 1,
                        "used_count": 1,
                        "applied_count": 1,
                        "tightened_count": 1,
                        "supportive_count": 0,
                        "overly_conservative_count": 0,
                    },
                    "feedback_risk_gate_count": 1,
                    "realtime_feedback": {
                        "status": "active",
                        "sample_count": 3,
                        "symbol_feedback_count": 2,
                        "risk_feedback_count": 3,
                        "monitor_count": 1,
                        "latest_event_time": "2026-05-26T10:05:00",
                        "event_type_counts": {"signal_generated": 2, "signal_blocked": 1},
                        "risk_gate_counts": {"allow": 2, "blocked": 1},
                        "semantic_event_type_counts": {"产业进展": 2},
                        "top_symbols": [{"symbol": "600584.SH", "count": 2}],
                        "top_themes": [{"theme": "半导体", "count": 2}],
                    },
                    "score_profile_counts": {"offensive": 1},
                    "market_state": True,
                    "market_state_freshness": {"status": "aligned"},
                    "intraday_event_pulse": {"status": "confirming"},
                    "llm_event_understanding": {
                        "ready": True,
                        "model": "mock-model",
                        "used_semantic_theme_count": 2,
                        "used_symbol_theme_count": 1,
                    },
                    "end_to_end_evidence": {
                        "status": "active",
                        "active_count": 6,
                        "warming_up_count": 0,
                        "degraded_count": 0,
                        "missing_count": 0,
                        "stage_count": 6,
                        "pass_rate": 1.0,
                        "trigger": local_trigger_context.get("trigger"),
                        "refresh_key": local_trigger_context.get("refresh_key"),
                        "trigger_source": local_trigger_context.get("source"),
                        "stages": [
                            {"id": "proactive_opportunity_discovery", "status": "active"},
                            {"id": "event_understanding", "status": "active"},
                            {"id": "market_state_judgement", "status": "active"},
                            {"id": "dynamic_ranking", "status": "active"},
                            {"id": "risk_control", "status": "active"},
                            {"id": "feedback_learning", "status": "active"},
                        ],
                    },
                },
            },
            "updated_at": "2026-05-26T09:00:00+08:00",
        }

    async def fake_download(self, symbol, start_date, end_date, force=False, source="akshare"):
        assert symbol == "600584.SH"
        return {"success": True, "records": 3, "source": "akshare"}

    monkeypatch.setattr(catalyst_selection_service, "generate_selections", fake_generate_selections)
    monkeypatch.setattr(DataDownloader, "download_minute_kline", fake_download)

    asyncio.run(
        catalyst_selection_service._run_event_minute_akshare_backfill_job_async(
            job_key,
            ["600584.SH"],
            "2026-05-26",
        )
    )

    job = catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key]
    public_job = catalyst_selection_service._public_event_akshare_backfill_job(job)
    assert job["status"] == "completed"
    assert job["rows"] == 3
    assert job["selection_refresh"]["status"] == "completed"
    assert job["selection_refresh"]["requested"] is True
    assert job["selection_refresh"]["refreshed_count"] == 1
    assert job["selection_refresh"]["failed_count"] == 0
    assert public_job["selection_refresh"]["contexts"][0]["window"] == "24h"
    assert public_job["selection_refresh"]["contexts"][0]["user_bound"] is True
    assert calls == [("2026-05-26", "24h", 5, "user-1")]


def test_cached_selection_merges_latest_event_akshare_backfill_status(monkeypatch):
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    job_key = "unit-live-job"
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key] = {
        "job_key": job_key,
        "requested": True,
        "status": "completed",
        "mode": "async",
        "trade_date": "2026-05-26",
        "symbols": ["600584.SH"],
        "requested_symbol_count": 1,
        "rows": 3,
        "message": "AKShare分钟线补缺完成，写入 3 行",
        "updated_at": "2026-05-26T10:00:00+08:00",
        "finished_at": "2026-05-26T10:00:00+08:00",
        "selection_refresh_enabled": True,
        "selection_refresh_contexts": {
            "2026-05-26:24h:5:user-1": {
                "trade_date": "2026-05-26",
                "window": "24h",
                "limit": 5,
                "user_id": "user-1",
            }
        },
        "selection_refresh": {
            "requested": True,
            "status": "completed",
            "message": "AKShare补缺后已自动重算机会榜 1 个窗口",
            "refreshed_count": 1,
            "failed_count": 0,
            "updated_at": "2026-05-26T10:01:00+08:00",
        },
    }
    payload = {
        "data_governance": {
            "closed_loop": {
                "event_market_reaction": {
                    "capture": {
                        "rows": 0,
                        "history_backfill": {"status": "failed", "message": "QMT历史分钟线bridge连接超时"},
                        "akshare_backfill": {
                            "requested": True,
                            "status": "scheduled",
                            "job_key": job_key,
                            "rows": 0,
                            "message": "已安排AKShare候选标的分钟线补缺",
                        },
                    }
                }
            }
        }
    }

    merged = catalyst_selection_service._merge_live_event_backfill_status(payload)
    capture = merged["data_governance"]["closed_loop"]["event_market_reaction"]["capture"]
    akshare = capture["akshare_backfill"]

    assert akshare["status"] == "completed"
    assert akshare["rows"] == 3
    assert akshare["selection_refresh"]["status"] == "completed"
    assert akshare["selection_refresh"]["refreshed_count"] == 1
    assert akshare["selection_refresh"]["contexts"][0]["window"] == "24h"
    summary = merged["data_governance"]["closed_loop"]["minute_backfill"]
    assert summary["status"] == "selection_refresh_completed"
    assert summary["akshare_rows"] == 3
    assert summary["selection_refreshed_count"] == 1


def test_event_akshare_backfill_skips_refresh_when_no_rows(db, monkeypatch):
    from api.data_downloader import DataDownloader

    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    job_key = "unit-akshare-empty-job"
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key] = {
        "job_key": job_key,
        "requested": True,
        "status": "scheduled",
        "mode": "async",
        "symbols": ["600584.SH"],
        "requested_symbol_count": 1,
        "trade_date": "2026-05-26",
        "selection_refresh_enabled": True,
        "selection_refresh_contexts": {
            "2026-05-26:24h:5:user-1": {
                "trade_date": "2026-05-26",
                "window": "24h",
                "limit": 5,
                "user_id": "user-1",
            }
        },
        "selection_refresh": {
            "requested": True,
            "status": "pending",
            "message": "AKShare分钟线补缺完成后将自动重算对应机会榜",
        },
    }

    async def fake_download(self, symbol, start_date, end_date, force=False, source="akshare"):
        return {"success": False, "records": 0, "source": "akshare", "error": "no data"}

    monkeypatch.setattr(DataDownloader, "download_minute_kline", fake_download)
    monkeypatch.setattr(catalyst_selection_service, "generate_selections", lambda *args, **kwargs: pytest.fail("selection refresh should not run when no rows were written"))

    asyncio.run(
        catalyst_selection_service._run_event_minute_akshare_backfill_job_async(
            job_key,
            ["600584.SH"],
            "2026-05-26",
        )
    )

    job = catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key]
    assert job["status"] == "empty"
    assert job["rows"] == 0
    assert job["selection_refresh"]["status"] == "skipped"
    assert job["selection_refresh"]["requested"] is True
    assert job["selection_refresh"]["refreshed_count"] == 0
    assert job["selection_refresh"]["failed_count"] == 0


def test_event_akshare_backfill_records_refresh_failure_without_blocking_completion(db, monkeypatch):
    from api.data_downloader import DataDownloader

    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    job_key = "unit-akshare-refresh-failed-job"
    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key] = {
        "job_key": job_key,
        "requested": True,
        "status": "scheduled",
        "mode": "async",
        "symbols": ["600584.SH"],
        "requested_symbol_count": 1,
        "trade_date": "2026-05-26",
        "selection_refresh_enabled": True,
        "selection_refresh_contexts": {
            "2026-05-26:24h:5:user-1": {
                "trade_date": "2026-05-26",
                "window": "24h",
                "limit": 5,
                "user_id": "user-1",
            }
        },
        "selection_refresh": {
            "requested": True,
            "status": "pending",
            "message": "AKShare分钟线补缺完成后将自动重算对应机会榜",
        },
    }

    async def fake_download(self, symbol, start_date, end_date, force=False, source="akshare"):
        return {"success": True, "records": 3, "source": "akshare"}

    def fake_generate_selections(session, *, trade_date, window, limit, user_id=None, **kwargs):
        raise RuntimeError("selection refresh exploded")

    monkeypatch.setattr(DataDownloader, "download_minute_kline", fake_download)
    monkeypatch.setattr(catalyst_selection_service, "generate_selections", fake_generate_selections)

    asyncio.run(
        catalyst_selection_service._run_event_minute_akshare_backfill_job_async(
            job_key,
            ["600584.SH"],
            "2026-05-26",
        )
    )

    job = catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS[job_key]
    assert job["status"] == "completed"
    assert job["rows"] == 3
    assert job["selection_refresh"]["status"] == "failed"
    assert job["selection_refresh"]["requested"] is True
    assert job["selection_refresh"]["refreshed_count"] == 0
    assert job["selection_refresh"]["failed_count"] == 1
    assert "selection refresh exploded" in job["selection_refresh"]["errors"][0]["error"]


def test_event_akshare_backfill_can_run_synchronously(monkeypatch):
    from api.data_downloader import DataDownloader

    catalyst_selection_service._EVENT_AKSHARE_BACKFILL_JOBS.clear()
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_AKSHARE_SYNC", "1")
    monkeypatch.setenv("AI_QUANT_EVENT_MINUTE_AKSHARE_SYNC_SYMBOLS", "1")

    class UnexpectedThread:
        def __init__(self, *args, **kwargs):
            raise AssertionError("sync backfill should not start a background thread")

    async def fake_download(self, symbol, start_date, end_date, force=False, source="akshare"):
        assert symbol == "600584.SH"
        return {"success": True, "records": 3, "source": "akshare"}

    monkeypatch.setattr(catalyst_selection_service.threading, "Thread", UnexpectedThread)
    monkeypatch.setattr(DataDownloader, "download_minute_kline", fake_download)

    payload = catalyst_selection_service._schedule_event_minute_akshare_backfill(
        ["600584.SH", "300750.SZ"],
        trade_date="2026-06-02",
    )

    assert payload["mode"] == "sync"
    assert payload["status"] == "completed"
    assert payload["rows"] == 3
    assert payload["symbols"] == ["600584.SH"]
    assert payload["skipped_symbol_count"] == 1


def test_external_minute_error_is_compacted():
    message = (
        "HTTPSConnectionPool(host='push2his.eastmoney.com', port=443): "
        "Max retries exceeded (Caused by ProxyError('Unable to connect to proxy', "
        "RemoteDisconnected('Remote end closed connection without response')))"
    )

    assert catalyst_selection_service._compact_external_minute_error(message) == "外部行情源代理连接失败"


def test_event_reaction_data_freshness_reports_stale_minute_date(db):
    _seed_market_data(db)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES ('600584.SH', '2026-05-26 09:30:00', 10.50, 10.60, 10.45, 10.55, 100000, 1000000, NOW(), NOW())
            """
        )
    )
    db.commit()

    freshness = catalyst_selection_service._load_event_reaction_data_freshness(
        db,
        trade_date="2026-05-27",
        feature_trade_date="2026-05-26",
        minute_table="stock_minute_kline",
    )

    assert freshness["status"] == "stale"
    assert freshness["latest_minute_trade_date"] == "2026-05-26"
    assert "2026-05-27" in freshness["message"]


def test_event_reaction_data_freshness_reports_pending_preopen(db, monkeypatch):
    _seed_market_data(db)
    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 5, 27, 8, 50, tzinfo=CN_TZ))
    monkeypatch.setattr(catalyst_selection_service, "is_cn_trading_day", lambda value: True)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES ('600584.SH', '2026-05-26 09:30:00', 10.50, 10.60, 10.45, 10.55, 100000, 1000000, NOW(), NOW())
            """
        )
    )
    db.commit()

    freshness = catalyst_selection_service._load_event_reaction_data_freshness(
        db,
        trade_date="2026-05-27",
        feature_trade_date="2026-05-26",
        minute_table="stock_minute_kline",
    )

    assert freshness["status"] == "pending_preopen"
    assert "盘前" in freshness["message"]


def test_event_reaction_data_freshness_reports_lagged_daily_when_minutes_ready(db):
    _seed_market_data(db)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            ('600584.SH', '2026-05-27 09:30:00', 10.60, 10.70, 10.55, 10.65, 100000, 1000000, NOW(), NOW()),
            ('002156.SZ', '2026-05-27 09:30:00', 20.40, 20.45, 20.20, 20.30, 70000, 900000, NOW(), NOW())
            """
        )
    )
    db.commit()

    freshness = catalyst_selection_service._load_event_reaction_data_freshness(
        db,
        trade_date="2026-05-27",
        feature_trade_date="2026-05-26",
        minute_table="stock_minute_kline",
    )
    market_freshness = catalyst_selection_service._build_market_state_freshness(
        feature_trade_date="2026-05-26",
        event_reaction_trade_date="2026-05-27",
        event_reaction_governance={"data_freshness": freshness},
    )
    background = catalyst_selection_service._build_market_background(
        trade_date="2026-05-26",
        window="24h",
        news_window_start=datetime(2026, 5, 26, 10, 0),
        news_window_end=datetime(2026, 5, 27, 10, 0),
        theme_items=[{"theme": "半导体", "catalyst": "先进封装订单增长"}],
        market_behavior={"narrative_anchors": ["市场态势测试"]},
        market_state_freshness=market_freshness,
    )

    assert freshness["status"] == "ready_with_lagged_daily_features"
    assert market_freshness["status"] == "minute_ready_daily_lagged"
    assert market_freshness["is_aligned"] is False
    assert market_freshness["target_minute_rows"] == 2
    assert "日线特征截至 2026-05-26" in freshness["message"]
    assert "市场状态新鲜度" in background
    assert "市场宽度和日线特征仍截至 2026-05-26" in background


def test_minute_market_proxy_is_partial_sample_not_full_market_breadth(db):
    _seed_market_data(db)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            ('600584.SH', '2026-05-27 09:30:00', 10.00, 10.10, 9.90, 9.95, 10000, 100000, NOW(), NOW()),
            ('600584.SH', '2026-05-27 15:00:00', 9.95, 10.00, 9.80, 9.90, 12000, 120000, NOW(), NOW()),
            ('002156.SZ', '2026-05-27 09:30:00', 20.00, 20.10, 19.90, 20.05, 8000, 160000, NOW(), NOW()),
            ('002156.SZ', '2026-05-27 15:00:00', 20.05, 20.20, 20.00, 20.18, 9000, 180000, NOW(), NOW()),
            ('601689.SH', '2026-05-27 09:30:00', 5.00, 5.05, 4.95, 4.98, 6000, 30000, NOW(), NOW()),
            ('601689.SH', '2026-05-27 15:00:00', 4.98, 5.00, 4.90, 4.92, 7000, 35000, NOW(), NOW())
            """
        )
    )
    db.commit()

    proxy = catalyst_selection_service._load_minute_market_proxy(db, "2026-05-27")
    freshness = catalyst_selection_service._build_market_state_freshness(
        feature_trade_date="2026-05-26",
        event_reaction_trade_date="2026-05-27",
        event_reaction_governance={"data_freshness": {"status": "ready_with_lagged_daily_features", "target_minute_rows": 6, "target_minute_symbol_count": 3}},
        minute_market_proxy=proxy,
    )
    background = catalyst_selection_service._build_market_background(
        trade_date="2026-05-26",
        window="24h",
        news_window_start=datetime(2026, 5, 26, 10, 0),
        news_window_end=datetime(2026, 5, 27, 10, 0),
        theme_items=[{"theme": "半导体", "catalyst": "先进封装订单增长"}],
        market_behavior={"narrative_anchors": ["市场态势测试"], "minute_market_proxy": proxy},
        market_state_freshness=freshness,
    )

    assert proxy["scope"] == "minute_market_proxy"
    assert proxy["coverage_scope"] == "partial_minute_sample"
    assert proxy["status"] == "thin_sample"
    assert proxy["symbol_count"] == 3
    assert proxy["row_count"] == 6
    assert proxy["is_full_market_breadth"] is False
    assert "不等同全市场宽度" in proxy["message"]
    assert freshness["minute_market_proxy"]["status"] == "thin_sample"
    assert "分钟市场代理" in background


def test_intraday_market_state_precedes_lagged_daily_narrative():
    minute_proxy = {
        "scope": "minute_market_proxy",
        "coverage_scope": "partial_minute_sample",
        "status": "weak",
        "trade_date": "2026-06-03",
        "row_count": 24000,
        "symbol_count": 143,
        "positive_ratio": 0.38,
        "average_change_pct": -0.25,
        "is_full_market_breadth": False,
        "source": "postgresql:stock_minute_kline",
        "message": "部分分钟样本偏弱",
    }
    freshness = {
        "is_aligned": False,
        "feature_trade_date": "2026-06-01",
        "event_reaction_trade_date": "2026-06-03",
        "message": "事件反应已使用 2026-06-03 分钟线，但市场宽度和日线特征仍截至 2026-06-01。",
    }
    market_behavior = {
        "market_regime": {"label": "流动性外溢普涨修复", "detail": "流动性外溢普涨修复：旧日线口径。"},
        "risk_pressure": {"label": "普涨后分化风险", "detail": "普涨后分化风险：旧日线口径。"},
        "narrative_anchors": [
            "流动性极度充沛：两市成交约 2.90 万亿元。",
            "全市场右侧多头普涨修复：上涨 3776 家，下跌 1682 家。",
        ],
    }

    adjusted = catalyst_selection_service._apply_intraday_market_state_to_behavior(
        market_behavior,
        market_state_freshness=freshness,
        minute_market_proxy=minute_proxy,
    )
    background = catalyst_selection_service._build_market_background(
        trade_date="2026-06-03",
        window="24h",
        news_window_start=datetime(2026, 6, 2, 15, 0),
        news_window_end=datetime(2026, 6, 3, 15, 0),
        theme_items=[{"theme": "人工智能", "catalyst": "算力订单增长"}],
        market_behavior=adjusted,
        market_state_freshness={**freshness, "minute_market_proxy": minute_proxy},
    )

    assert adjusted["intraday_market_state"]["label"] == "盘中样本偏弱"
    assert adjusted["market_regime"]["label"] == "盘中样本偏弱"
    assert adjusted["risk_pressure"]["label"] == "盘中样本偏弱"
    assert adjusted["narrative_anchors"][0].startswith("盘中样本偏弱")
    assert "滞后日线参考（2026-06-01）" in adjusted["narrative_anchors"][2]
    assert background.index("盘中样本偏弱") < background.index("滞后日线参考")
    assert "minute_proxy_not_full_market_breadth" in adjusted["data_quality"]["limitations"]


def test_market_background_prefers_selected_mainline_over_first_theme_item():
    background = catalyst_selection_service._build_market_background(
        trade_date="2026-06-03",
        window="24h",
        news_window_start=datetime(2026, 6, 2, 15, 0),
        news_window_end=datetime(2026, 6, 3, 15, 0),
        theme_items=[
            {"theme": "金融", "catalyst": "ETF上市消息，对金融板块直接催化有限"},
            {"theme": "算力", "catalyst": "数据中心和算力基础设施政策催化"},
        ],
        selected_items=[
            {
                "rank": 1,
                "symbol": "000977.SZ",
                "name": "浪潮信息",
                "score": 63.4,
                "theme_matches": [{"theme": "算力", "catalyst": "数据中心和算力基础设施政策催化"}],
            },
            {
                "rank": 2,
                "symbol": "603019.SH",
                "name": "中科曙光",
                "score": 62.8,
                "theme_matches": [{"theme": "算力", "catalyst": "数据中心和算力基础设施政策催化"}],
            },
        ],
        market_behavior={"narrative_anchors": ["市场态势测试"]},
        market_state_freshness={"is_aligned": True},
    )

    assert "入选主线：算力" in background
    assert "入选2只：浪潮信息、中科曙光" in background
    assert "核心催化：金融" not in background


def test_minute_market_proxy_falls_back_to_latest_available_minute_day(db):
    _seed_market_data(db)
    db.execute(
        text(
            """
            INSERT INTO stock_minute_kline (symbol, trade_time, open, high, low, close, volume, amount, created_at, updated_at)
            VALUES
            ('600584.SH', '2026-05-27 09:30:00', 10.00, 10.20, 9.90, 10.10, 10000, 100000, NOW(), NOW()),
            ('600584.SH', '2026-05-27 15:00:00', 10.10, 10.40, 10.00, 10.35, 12000, 120000, NOW(), NOW()),
            ('002156.SZ', '2026-05-27 09:30:00', 20.00, 20.10, 19.90, 19.95, 8000, 160000, NOW(), NOW()),
            ('002156.SZ', '2026-05-27 15:00:00', 19.95, 20.05, 19.70, 19.80, 9000, 180000, NOW(), NOW())
            """
        )
    )
    db.commit()

    latest = catalyst_selection_service._latest_available_minute_trade_date(
        db,
        before_or_equal_trade_date="2026-05-28",
    )
    proxy = catalyst_selection_service._load_minute_market_proxy(db, "2026-05-28")

    assert latest == "2026-05-27"
    assert proxy["fallback"] is True
    assert proxy["requested_trade_date"] == "2026-05-28"
    assert proxy["trade_date"] == "2026-05-27"
    assert proxy["symbol_count"] == 2
    assert proxy["coverage_scope"] == "partial_minute_sample"
    assert proxy["is_full_market_breadth"] is False
    assert "上一可用分钟交易日 2026-05-27" in proxy["message"]
    assert "不等同全市场宽度" in proxy["message"]


def test_intraday_event_pulse_summarizes_real_minute_reactions():
    features = {
        "600584.SH": {
            "name": "长电科技",
            "event_reaction": {"status": "confirmed", "score": 68.0, "change_pct": 1.2, "amount_share": 0.02},
        },
        "002156.SZ": {
            "name": "通富微电",
            "event_reaction": {"status": "confirmed", "score": 62.0, "change_pct": 0.6, "amount_share": 0.01},
        },
        "601689.SH": {
            "name": "拓普集团",
            "event_reaction": {"status": "weak", "score": 52.0, "change_pct": -0.1, "amount_share": 0.005},
        },
        "000001.SZ": {
            "name": "平安银行",
            "event_reaction": {"status": "daily_proxy_confirmed", "proxy": True, "score": 60.0, "change_pct": 0.8},
        },
    }

    pulse = catalyst_selection_service._build_intraday_event_pulse(features)

    assert pulse["scope"] == "event_candidate_universe"
    assert pulse["status"] == "confirming"
    assert pulse["sample_count"] == 3
    assert pulse["symbol_count"] == 4
    assert pulse["confirmed_count"] == 2
    assert pulse["positive_ratio"] > 0.6
    assert pulse["leaders"][0]["symbol"] == "600584.SH"
    assert "真实分钟样本 3/4" in pulse["message"]


def test_intraday_event_pulse_tightens_score_profile_and_risk_control():
    pulse = {
        "status": "weak",
        "message": "事件池分钟反应偏弱：真实分钟样本 3/4，上涨占比 33%。",
    }
    market_behavior = {
        "intraday_event_pulse": pulse,
        "liquidity_state": {"label": "流动性温和"},
        "breadth_state": {"label": "温和扩散"},
        "market_regime": {"label": "结构性轮动"},
        "sentiment_state": {"label": "中性"},
        "risk_pressure": {"label": "结构性执行风险"},
    }

    profile = catalyst_selection_service._adaptive_score_profile(market_behavior)
    risk_plan = catalyst_selection_service._risk_control_plan(
        features={"change_pct": 0.0, "amount_ratio_20d": 1.0, "event_reaction": {"status": "weak", "score": 46.0}},
        primary_theme={"theme": "算力"},
        market_behavior=market_behavior,
        risk_penalty=2.0,
        risk_flags=[],
        event_intelligence_score=80.0,
        adaptive_feedback_score=70.0,
    )

    assert "intraday_guarded" in profile["profile"]
    assert profile["risk_penalty_multiplier"] >= 1.22
    assert any("事件池分钟反应偏弱" in reason for reason in profile["reasons"])
    assert risk_plan["action"] == "follow"
    assert risk_plan["max_position_pct"] <= 5.0
    assert "事件池分钟脉冲未确认或转弱" in risk_plan["invalidations"]
    assert any("事件池分钟反应偏弱" in note for note in risk_plan["notes"])


def test_minute_market_proxy_tightens_score_profile_and_risk_control():
    minute_proxy = {
        "status": "risk_off",
        "coverage_scope": "partial_minute_sample",
        "symbol_count": 116,
        "row_count": 26391,
        "positive_ratio": 0.28,
        "average_change_pct": -0.45,
        "is_full_market_breadth": False,
        "message": "部分分钟样本转弱：116 只/26391 行，上涨占比 28%，均值 -0.45%。该口径只是盘中部分分钟样本代理，不等同全市场宽度。",
    }
    market_behavior = {
        "minute_market_proxy": minute_proxy,
        "liquidity_state": {"label": "流动性温和"},
        "breadth_state": {"label": "温和扩散"},
        "market_regime": {"label": "结构性轮动"},
        "sentiment_state": {"label": "中性"},
        "risk_pressure": {"label": "结构性执行风险"},
    }

    profile = catalyst_selection_service._adaptive_score_profile(market_behavior)
    risk_plan = catalyst_selection_service._risk_control_plan(
        features={"change_pct": 0.0, "amount_ratio_20d": 1.0, "event_reaction": {"status": "confirmed", "score": 60.0}},
        primary_theme={"theme": "算力"},
        market_behavior=market_behavior,
        risk_penalty=2.0,
        risk_flags=[],
        event_intelligence_score=82.0,
        adaptive_feedback_score=72.0,
    )

    assert "minute_proxy_guarded" in profile["profile"]
    assert profile["risk_penalty_multiplier"] >= 1.32
    assert profile["market_labels"]["minute_market_proxy"]["is_full_market_breadth"] is False
    assert any("不等同全市场宽度" in reason for reason in profile["reasons"])
    assert risk_plan["action"] == "observe"
    assert risk_plan["max_position_pct"] <= 3.5
    assert "分钟市场代理未确认或转弱" in risk_plan["invalidations"]
    assert any("不等同全市场宽度" in note for note in risk_plan["notes"])


def test_intraday_event_pulse_is_persisted_in_candidate_trace():
    pulse = {
        "scope": "event_candidate_universe",
        "status": "weak",
        "message": "事件池分钟反应偏弱：真实分钟样本 3/4，上涨占比 33%。",
        "sample_count": 3,
        "symbol_count": 4,
    }
    features = {
        "symbol": "000977.SZ",
        "name": "浪潮信息",
        "industry": "服务器",
        "sector": "计算机",
        "concepts": ["算力", "AI服务器"],
        "change_pct": 1.0,
        "amount_ratio_20d": 1.2,
        "momentum_20d": 0.05,
        "momentum_60d": 0.10,
        "r60": 66.0,
        "net_profit_growth_proxy": 0.2,
        "event_reaction": {"status": "weak", "score": 46.0, "change_pct": -0.1},
    }
    theme_items = [
        {
            "theme": "算力",
            "score": 88.0,
            "summary": "算力政策催化",
            "catalyst": "算力政策催化",
            "source_tier": "S",
            "top_source_tier": "S",
            "policy_boost": True,
            "related_symbols": [{"symbol": "000977.SZ", "name": "浪潮信息"}],
            "evidence_items": [{"content": "加强算力网建设", "source_tier": "S"}],
            "mainline_alignment_score": 70.0,
            "mainline_alignment_reasons": ["盘面强度确认：通信"],
            "event_semantic": {"event_type": "政策支持", "catalyst_strength": 78.0, "confidence": 0.8},
        }
    ]

    scored = catalyst_selection_service._score_candidate(
        symbol="000977.SZ",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior={
            "intraday_event_pulse": pulse,
            "liquidity_state": {"label": "流动性温和"},
            "breadth_state": {"label": "温和扩散"},
            "market_regime": {"label": "结构性轮动"},
            "risk_pressure": {"label": "结构性执行风险"},
        },
    )

    trace_pulse = scored["closed_loop_trace"]["market"]["intraday_event_pulse"]
    assert trace_pulse["status"] == "weak"
    assert trace_pulse["sample_count"] == 3
    assert "intraday_guarded" in scored["closed_loop_trace"]["scoring"]["profile"]
    assert any("事件池分钟反应偏弱" in reason for reason in scored["closed_loop_trace"]["scoring"]["reasons"])


def test_market_state_adapts_candidate_score_weights_and_risk_multiplier():
    features = {
        "symbol": "603019.SH",
        "name": "中科曙光",
        "industry": "计算机",
        "sector": "信息技术",
        "concepts": ["人工智能", "算力", "AI服务器"],
        "change_pct": 8.0,
        "amount_ratio_20d": 3.2,
        "momentum_20d": 0.16,
        "momentum_60d": 0.18,
        "r60": 82.0,
        "net_profit_growth_proxy": 0.2,
    }
    theme_items = [
        {
            "theme": "人工智能",
            "score": 84.0,
            "summary": "AI基础设施订单兑现",
            "catalyst": "AI基础设施订单兑现",
            "source_tier": "A",
            "top_source_tier": "A",
            "policy_boost": False,
            "related_symbols": [{"symbol": "603019.SH", "name": "中科曙光"}],
            "evidence_items": [{"content": "AI基础设施订单兑现", "source_tier": "A", "published_at": "2026-05-26T09:00:00"}],
            "consensus_rate": 0.9,
            "mainline_alignment_score": 72.0,
            "mainline_alignment_reasons": ["盘面强度确认：人工智能"],
            "event_semantic": {
                "event_type": "订单兑现",
                "catalyst_strength": 82.0,
                "confidence": 0.85,
                "risk_signals": [],
                "invalidation_conditions": ["订单兑现低于预期"],
            },
        }
    ]
    offensive_behavior = {
        "liquidity_state": {"label": "流动性高位外溢"},
        "breadth_state": {"label": "全市场右侧多头普涨修复"},
        "market_regime": {"label": "流动性外溢普涨修复"},
        "sentiment_state": {"label": "接力情绪修复但后排需承接"},
        "risk_pressure": {"label": "结构性执行风险"},
        "data_quality": {"missing_fields": []},
    }
    defensive_behavior = {
        "liquidity_state": {"label": "存量博弈/缩量约束"},
        "breadth_state": {"label": "个股失血/指数失真压力"},
        "market_regime": {"label": "高位接力强分歧"},
        "sentiment_state": {"label": "高位接力强分歧/退潮压力"},
        "risk_pressure": {"label": "封板质量风险"},
        "data_quality": {"missing_fields": []},
    }

    offensive = catalyst_selection_service._score_candidate(
        symbol="603019.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior=offensive_behavior,
    )
    defensive = catalyst_selection_service._score_candidate(
        symbol="603019.SH",
        features=features,
        theme_items=theme_items,
        previous_state={},
        history_stats={},
        theme_feedback={},
        market_background="mock",
        market_behavior=defensive_behavior,
    )

    offensive_scoring = offensive["closed_loop_trace"]["scoring"]
    defensive_scoring = defensive["closed_loop_trace"]["scoring"]
    assert offensive_scoring["profile"] == "offensive"
    assert defensive_scoring["profile"] == "defensive"
    assert defensive_scoring["weights"]["momentum"] < offensive_scoring["weights"]["momentum"]
    assert defensive_scoring["weights"]["adaptive_feedback"] > offensive_scoring["weights"]["adaptive_feedback"]
    assert defensive_scoring["risk_penalty_multiplier"] > offensive_scoring["risk_penalty_multiplier"]
    assert defensive_scoring["effective_risk_penalty"] > offensive_scoring["effective_risk_penalty"]
    assert defensive["score"] < offensive["score"]
    assert any("市场风险压力升高" in reason for reason in defensive_scoring["reasons"])


def test_generate_selection_rejects_future_trade_date(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)

    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 5, 26, 18, 0, tzinfo=CN_TZ))
    monkeypatch.setattr(catalyst_selection_service, "is_cn_trading_day", lambda value: True)
    monkeypatch.setattr(catalyst_selection_service, "previous_cn_trading_day", lambda value: "2026-05-26")

    with pytest.raises(ValueError, match="不能晚于当前交易日"):
        catalyst_selection_service.list_or_generate_selections(db, trade_date="2026-05-27", window="premarket", limit=5)


def test_generate_selection_anchors_theme_window_by_trade_date(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)

    captured: dict[str, datetime] = {}

    def fake_list_theme_rankings(*args, **kwargs):
        captured["now"] = kwargs.get("now")
        captured["force_sync_llm"] = kwargs.get("force_sync_llm")
        return {"window": "premarket", "updated_at": "2026-05-26T09:00:00+08:00", "source": "cache:mock", "message": "mock", "items": []}

    monkeypatch.setattr(news_theme_service, "list_theme_rankings", fake_list_theme_rankings)
    monkeypatch.setattr(catalyst_selection_service, "interpret_market_behavior", lambda market: {"narrative_anchors": ["结构性分化"], "locked_values": {}, "data_quality": {"missing_fields": []}})
    monkeypatch.setattr(catalyst_selection_service, "get_reverse_stock_map", lambda: {"600584.SH": "长电科技", "002156.SZ": "通富微电", "601689.SH": "拓普集团"})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_daily_features",
        lambda *args, **kwargs: {
            "600584.SH": {
                "symbol": "600584.SH",
                "name": "长电科技",
                "industry": "半导体",
                "sector": "电子",
                "concepts": ["电子", "半导体", "芯片"],
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "amount": 15_000_000,
                "turnover_rate": 1.8,
                "change_pct": 5.0,
                "amount_ratio_20d": 2.0,
                "momentum_20d": 0.12,
                "momentum_60d": 0.18,
                "r60": 79.66,
                "net_profit_growth_proxy": 0.35,
            }
        },
    )
    monkeypatch.setattr(catalyst_selection_service, "_load_previous_selection_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(catalyst_selection_service, "_load_symbol_settlement_stats", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_score_candidate",
        lambda *args, **kwargs: {
            "rank": 0,
            "symbol": "600584.SH",
            "name": "长电科技",
            "industry": "半导体",
            "sector": "电子",
            "concepts": ["电子", "半导体", "芯片"],
            "score": 80.0,
            "catalyst_score": 70.0,
            "theme_score": 80.0,
            "relation_score": 90.0,
            "market_confirm_score": 60.0,
            "momentum_score": 75.0,
            "fundamental_score": 55.0,
            "continuity_score": 40.0,
            "risk_penalty": 5.0,
            "risk_flags": [],
            "reason_parts": ["mock catalyst"],
            "theme_matches": [{"theme": "半导体", "score": 92.0, "relation_score": 95.0, "catalyst": "mock", "summary": "mock", "source_tier": "S", "evidence_count": 1}],
            "signal_flags": ["R60=80.00强势"],
            "market_background": "mock",
            "market_behavior_labels": {},
            "metric_snapshot": {"r60": 80.0, "change_pct": 5.0, "amount_ratio_20d": 2.0},
        },
    )

    payload = catalyst_selection_service.generate_selections(
        db,
        trade_date="2026-05-25",
        window="premarket",
        limit=5,
    )

    assert payload["trade_date"] == "2026-05-25"
    assert captured["now"].date().isoformat() == "2026-05-25"
    assert captured["now"].hour == 9
    assert captured["now"].minute == 25
    assert captured["force_sync_llm"] is True
    assert payload["data_governance"]["news_time_window"]["policy"] == "premarket_cutoff_09:25"
    assert payload["data_governance"]["news_time_window"]["window_end"].startswith("2026-05-25T09:25")
    assert payload["data_governance"]["closed_loop"]["realtime_feedback"]["status"] == "warming_up"
    assert payload["data_governance"]["closed_loop"]["realtime_feedback"]["sample_count"] == 0


def test_realtime_news_window_uses_current_time_when_daily_trade_date_is_older(monkeypatch):
    current_now = datetime(2026, 6, 1, 18, 36, 0, tzinfo=CN_TZ)
    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: current_now)

    anchor = catalyst_selection_service._selection_anchor_now("2026-05-29", "24h")
    window_start, window_end = news_theme_service.resolve_news_window_range("24h", anchor)

    assert anchor == current_now
    assert window_start.isoformat() == "2026-05-31T18:36:00"
    assert window_end.isoformat() == "2026-06-01T18:36:00"


def test_realtime_selection_cache_expires_after_one_minute(monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_utcnow", lambda: datetime(2026, 6, 1, 10, 0, 0))

    assert catalyst_selection_service._can_reuse_selection_run(
        {"updated_at": "2026-06-01T09:59:30"},
        "24h",
    )
    assert not catalyst_selection_service._can_reuse_selection_run(
        {"updated_at": "2026-06-01T09:58:59"},
        "24h",
    )
    assert catalyst_selection_service._can_reuse_selection_run(
        {"updated_at": "2026-05-26T09:25:00"},
        "premarket",
    )


def test_realtime_selection_cache_invalidates_when_llm_runtime_package_changes(monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_utcnow", lambda: datetime(2026, 6, 1, 10, 0, 0))
    current_runtime = {
        "enabled": True,
        "ready": True,
        "status": "ready",
        "provider": "volcengine-ark",
        "model": "deepseek-v4-flash",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
        "source": "user_runtime",
        "runtime_package_source": "user_news_config",
        "api_key_source": "user_news_config",
        "provider_source": "user_news_config",
        "base_url_source": "user_news_config",
        "model_source": "user_news_config",
        "requires_api_key": True,
        "has_api_key": True,
    }
    stored = {
        "updated_at": "2026-06-01T09:59:45",
        "source": "postgresql:test",
        "items": [
            {
                "symbol": "600584.SH",
                "theme_matches": [
                    {
                        "theme": "半导体",
                        "semantic_source": "heuristic:event_rules",
                        "symbol_suggestion_source": "fallback:positive_news",
                    }
                ],
                "closed_loop_trace": {
                    "event": {
                        "theme": "半导体",
                        "semantic_source": "heuristic:event_rules",
                    }
                },
            }
        ],
        "data_governance": {
            "closed_loop": {
                "dynamic_ranking": True,
                "llm_event_understanding": {
                    "enabled": True,
                    "ready": True,
                    "status": "ready",
                    "provider": "openai",
                    "model": "main-quick",
                    "base_url": "https://main.example/v1",
                    "source": "user_runtime",
                    "runtime_package_source": "user_config",
                    "api_key_source": "user_config",
                    "provider_source": "user_config",
                    "base_url_source": "user_config",
                    "model_source": "user_config",
                    "requires_api_key": True,
                    "has_api_key": True,
                    "used_symbol_theme_count": 2,
                },
            },
        },
    }
    monkeypatch.setattr(news_theme_service, "core_stock_llm_readiness", lambda *args, **kwargs: current_runtime)

    state = catalyst_selection_service._selection_cache_reuse_state(
        stored,
        "24h",
        db=object(),
        user_id="user-1",
    )

    assert state["reusable"] is False
    assert state["reason"] == "llm_runtime_changed"
    assert state["llm_runtime"]["current_fingerprint"]["provider"] == "volcengine-ark"
    assert state["llm_runtime"]["cached_fingerprint"]["provider"] == "openai"

    stale_payload = catalyst_selection_service._mark_stale_selection_run(stored, cache_state=state)
    cache_state = stale_payload["data_governance"]["cache_state"]
    llm = stale_payload["data_governance"]["closed_loop"]["llm_event_understanding"]
    assert cache_state["reason"] == "llm_runtime_changed"
    assert cache_state["llm_runtime_changed"] is True
    assert stale_payload["data_governance"]["llm_core_stock"] == llm
    assert llm["provider"] == "volcengine-ark"
    assert llm["runtime_package_source"] == "user_news_config"
    assert llm["cached_runtime"]["provider"] == "openai"
    assert llm["cached_usage"]["used_symbol_theme_count"] == 2
    assert "api_key" not in llm
    assert "api_key" not in llm["current_runtime"]
    assert "api_key" not in llm["cached_runtime"]
    stale_event = stale_payload["items"][0]["closed_loop_trace"]["event"]
    assert stale_event["runtime_source"]["cache_status"] == "stale"
    assert stale_event["runtime_source"]["stale_reason"] == "llm_runtime_changed"
    assert stale_event["runtime_source"]["runtime_package_source"] == "user_news_config"
    assert stale_event["runtime_source"]["provider"] == "volcengine-ark"
    assert stale_event["semantic_source"] == "heuristic:event_rules"
    assert stale_event["symbol_suggestion_source"] == "fallback:positive_news"
    assert stale_event["llm_event_understanding"]["cached_runtime"]["provider"] == "openai"
    assert "api_key" not in stale_event["llm_event_understanding"]


def test_realtime_selection_cache_reuses_when_llm_runtime_package_matches(monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_utcnow", lambda: datetime(2026, 6, 1, 10, 0, 0))
    runtime = {
        "enabled": True,
        "ready": True,
        "status": "ready",
        "provider": "volcengine-ark",
        "model": "deepseek-v4-flash",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3/",
        "source": "user_runtime",
        "runtime_package_source": "user_news_config",
        "api_key_source": "user_news_config",
        "provider_source": "user_news_config",
        "base_url_source": "user_news_config",
        "model_source": "user_news_config",
        "requires_api_key": True,
        "has_api_key": True,
    }
    stored = {
        "updated_at": "2026-06-01T09:59:45",
        "data_governance": {
            "closed_loop": {
                "llm_event_understanding": {
                    **runtime,
                    "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
                    "used_symbol_theme_count": 2,
                },
            },
        },
    }
    monkeypatch.setattr(news_theme_service, "core_stock_llm_readiness", lambda *args, **kwargs: runtime)

    state = catalyst_selection_service._selection_cache_reuse_state(
        stored,
        "24h",
        db=object(),
        user_id="user-1",
    )

    assert state["reusable"] is True
    assert state["reason"] == "ttl_valid"


def test_cached_selection_llm_runtime_reads_top_level_governance_first():
    stored = {
        "data_governance": {
            "llm_core_stock": {
                "provider": "volcengine-ark",
                "model": "deepseek-v4-flash",
                "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
                "runtime_package_source": "user_news_config",
            },
            "closed_loop": {
                "llm_event_understanding": {
                    "provider": "openai",
                    "model": "legacy",
                    "runtime_package_source": "user_config",
                },
            },
        },
    }

    runtime = catalyst_selection_service._cached_selection_llm_runtime(stored)

    assert runtime["provider"] == "volcengine-ark"
    assert runtime["model"] == "deepseek-v4-flash"
    assert runtime["runtime_package_source"] == "user_news_config"


def test_stale_realtime_selection_returns_cache_and_schedules_background_refresh(monkeypatch):
    stored = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "updated_at": "2026-06-01T09:58:00",
        "source": "postgresql:test",
        "message": "实时事件机会榜仅用于研究和复盘，不构成直接买卖建议。",
        "items": [{"symbol": "600584.SH", "name": "长电科技", "rank": 1, "score": 80.0}],
        "market_background": "mock",
        "market_behavior_labels": {},
        "data_governance": {"closed_loop": {"dynamic_ranking": True}},
    }
    scheduled: list[dict[str, object]] = []
    monkeypatch.setattr(catalyst_selection_service, "ensure_catalyst_selection_tables", lambda db: None)
    monkeypatch.setattr(catalyst_selection_service, "_resolve_trade_date", lambda db, trade_date: "2026-05-29")
    monkeypatch.setattr(catalyst_selection_service, "_load_selection_run", lambda *args, **kwargs: stored)
    monkeypatch.setattr(catalyst_selection_service, "_can_reuse_selection_run", lambda *args, **kwargs: False)
    monkeypatch.setattr(catalyst_selection_service, "_schedule_selection_refresh", lambda **kwargs: scheduled.append(kwargs) or True)
    monkeypatch.setattr(catalyst_selection_service, "generate_selections", lambda *args, **kwargs: pytest.fail("stale cache should return before synchronous generation"))

    payload = catalyst_selection_service.list_or_generate_selections(
        object(),
        window="24h",
        limit=5,
        user_id="user-1",
    )

    assert payload["items"][0]["symbol"] == "600584.SH"
    assert payload["data_governance"]["cache_state"]["status"] == "stale"
    assert payload["data_governance"]["cache_state"]["refresh_scheduled"] is True
    assert payload["source"].endswith("+stale")
    assert scheduled == [{"trade_date": "2026-05-29", "window": "24h", "limit": 5, "user_id": "user-1"}]


def test_cached_realtime_selection_merges_live_realtime_feedback(db, monkeypatch):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    catalyst_selection_service._upsert_realtime_feedback_sample(
        db,
        sample={
            "feedback_id": "rtf-cache-live-1",
            "source_event_id": "cache-live-event-1",
            "monitor_id": "monitor-cache-live",
            "strategy_id": "strategy-cache-live",
            "user_id": "user-1",
            "account_key": "paper_sim",
            "trade_date": "2026-05-29",
            "event_time": datetime(2026, 5, 29, 10, 0, 0),
            "symbol": "603019.SH",
            "name": "中科曙光",
            "event_type": "minute_unconfirmed",
            "signal_side": None,
            "signal_source": "unit-test",
            "feedback_kind": "intraday_minute_unconfirmed",
            "outcome": "miss",
            "hit_score": 45.0,
            "change_pct": -0.5,
            "risk_gate": "allow",
            "risk_favorable": False,
            "symbol_feedback": True,
            "risk_feedback": True,
            "themes": ["人工智能"],
            "event_types": ["政策支持"],
            "theme_matches": [],
            "candidate_snapshot": {},
            "raw_event": {},
            "source": "realtime_monitor_event",
        },
        now_value=datetime(2026, 6, 2, 10, 1, 0),
    )
    db.commit()
    stored = {
        "trade_date": "2026-05-29",
        "window": "24h",
        "updated_at": "2026-06-01T09:59:30",
        "source": "postgresql:test",
        "message": "实时事件机会榜仅用于研究和复盘，不构成直接买卖建议。",
        "items": [{"symbol": "603019.SH", "name": "中科曙光", "rank": 1, "score": 80.0}],
        "data_governance": {
            "trade_date": "2026-05-29",
            "closed_loop": {
                "dynamic_ranking": True,
                "realtime_feedback": {"status": "warming_up", "sample_count": 0},
            },
        },
    }
    monkeypatch.setattr(catalyst_selection_service, "_resolve_trade_date", lambda session, trade_date: "2026-05-29")
    monkeypatch.setattr(catalyst_selection_service, "_load_selection_run", lambda *args, **kwargs: stored)
    monkeypatch.setattr(catalyst_selection_service, "_can_reuse_selection_run", lambda *args, **kwargs: True)
    monkeypatch.setattr(catalyst_selection_service, "generate_selections", lambda *args, **kwargs: pytest.fail("fresh cache should not regenerate"))

    payload = catalyst_selection_service.list_or_generate_selections(
        db,
        trade_date="2026-05-29",
        window="24h",
        limit=5,
        force=False,
        user_id="user-1",
    )

    realtime = payload["data_governance"]["closed_loop"]["realtime_feedback"]
    assert realtime["status"] == "active"
    assert realtime["sample_count"] == 1
    assert realtime["risk_feedback_count"] == 1
    assert realtime["event_type_counts"] == {"minute_unconfirmed": 1}


def test_cached_realtime_feedback_merge_uses_event_reaction_trade_date_when_daily_lagged(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    catalyst_selection_service._upsert_realtime_feedback_sample(
        db,
        sample={
            "feedback_id": "rtf-lagged-daily-live-1",
            "source_event_id": "lagged-daily-event-1",
            "monitor_id": "monitor-lagged-daily",
            "strategy_id": "strategy-lagged-daily",
            "user_id": "user-1",
            "account_key": "paper_sim",
            "trade_date": "2026-06-01",
            "event_time": datetime(2026, 6, 2, 10, 0, 0),
            "symbol": "603019.SH",
            "name": "中科曙光",
            "event_type": "signal_generated",
            "signal_side": "buy",
            "signal_source": "unit-test",
            "feedback_kind": "realtime_signal",
            "outcome": "hit",
            "hit_score": 72.0,
            "change_pct": 1.2,
            "risk_gate": "allow",
            "risk_favorable": True,
            "symbol_feedback": True,
            "risk_feedback": True,
            "themes": ["人工智能"],
            "event_types": ["政策支持"],
            "theme_matches": [],
            "candidate_snapshot": {},
            "raw_event": {},
            "source": "realtime_monitor_event",
        },
        now_value=datetime(2026, 6, 2, 10, 1, 0),
    )
    db.commit()
    payload = {
        "trade_date": "2026-06-01",
        "window": "24h",
        "data_governance": {
            "trade_date": "2026-06-01",
            "window": "24h",
            "event_reaction_trade_date": "2026-06-02",
            "closed_loop": {
                "realtime_feedback": {"status": "warming_up", "sample_count": 0},
            },
        },
    }

    merged = catalyst_selection_service._merge_live_event_backfill_status(payload, db=db)

    realtime = merged["data_governance"]["closed_loop"]["realtime_feedback"]
    assert realtime["trade_date"] == "2026-06-02"
    assert realtime["sample_count"] == 1
    assert realtime["symbol_feedback_count"] == 1
    assert realtime["risk_feedback_count"] == 1
    assert realtime["latest_trade_date"] == "2026-06-02"
    assert realtime["event_type_counts"] == {"signal_generated": 1}


def test_realtime_feedback_trade_date_follows_event_time_and_backfills_legacy_rows(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    catalyst_selection_service._upsert_realtime_feedback_sample(
        db,
        sample={
            "feedback_id": "rtf-time-normalized",
            "source_event_id": "time-normalized-event",
            "monitor_id": "monitor-time-normalized",
            "strategy_id": "strategy-time-normalized",
            "user_id": "user-1",
            "account_key": "paper_sim",
            "trade_date": "2026-05-29",
            "event_time": datetime(2026, 6, 2, 10, 0, 0),
            "symbol": "603019.SH",
            "name": "中科曙光",
            "event_type": "minute_unconfirmed",
            "signal_side": None,
            "signal_source": "unit-test",
            "feedback_kind": "intraday_minute_unconfirmed",
            "outcome": "miss",
            "hit_score": 45.0,
            "change_pct": -0.5,
            "risk_gate": "allow",
            "risk_favorable": False,
            "symbol_feedback": True,
            "risk_feedback": True,
            "themes": ["人工智能"],
            "event_types": ["政策支持"],
            "theme_matches": [],
            "candidate_snapshot": {},
            "raw_event": {},
            "source": "realtime_monitor_event",
        },
        now_value=datetime(2026, 6, 2, 10, 1, 0),
    )
    stored_trade_date = db.execute(
        text(
            """
            SELECT trade_date
            FROM catalyst_selection_realtime_feedback
            WHERE source_event_id = 'time-normalized-event'
            """
        )
    ).scalar()

    assert stored_trade_date == "2026-06-02"

    db.execute(
        text(
            """
            UPDATE catalyst_selection_realtime_feedback
            SET trade_date = '2026-05-29'
            WHERE source_event_id = 'time-normalized-event'
            """
        )
    )
    fixed = catalyst_selection_service._backfill_realtime_feedback_trade_dates(db)
    old_window = catalyst_selection_service.summarize_realtime_feedback(db, trade_date="2026-05-29")
    current_window = catalyst_selection_service.summarize_realtime_feedback(db, trade_date="2026-06-02")

    assert fixed == 1
    assert old_window["sample_count"] == 0
    assert current_window["sample_count"] == 1
    assert current_window["latest_trade_date"] == "2026-06-02"


def test_premarket_news_window_excludes_same_day_after_cutoff_news(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)
    news_eye_service.ensure_news_tables(db)
    db.execute(
        text(
            """
            INSERT INTO market_news_items (
                digest, dedupe_key, content, published_at, source, url, sentiment,
                positive_sectors_json, negative_sectors_json, positive_symbols_json,
                negative_symbols_json, related_symbols_json, fetched_at
            )
            VALUES (
                :digest, :dedupe_key, :content, :published_at, :source, :url, :sentiment,
                :positive_sectors_json, :negative_sectors_json, :positive_symbols_json,
                :negative_symbols_json, :related_symbols_json, :fetched_at
            )
            """
        ),
        {
            "digest": "future" * 10 + "f1",
            "dedupe_key": "future-after-close",
            "content": "收盘后证券行业突发重大利好，券商板块获得政策支持。",
            "published_at": datetime(2026, 5, 26, 17, 35, 0),
            "source": "财联社电报",
            "url": "https://example.com/future",
            "sentiment": "positive",
            "positive_sectors_json": '["证券"]',
            "negative_sectors_json": "[]",
            "positive_symbols_json": '[{"symbol":"601688.SH","name":"华泰证券"}]',
            "negative_symbols_json": "[]",
            "related_symbols_json": '[{"symbol":"601688.SH","name":"华泰证券"}]',
            "fetched_at": datetime(2026, 5, 26, 17, 35, 0),
        },
    )
    db.commit()
    monkeypatch.setattr(catalyst_selection_service, "now_cn", lambda: datetime(2026, 5, 26, 18, 0, tzinfo=CN_TZ))

    anchor = catalyst_selection_service._selection_anchor_now("2026-05-26", "premarket")
    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=False,
        now=anchor,
    )["premarket"]

    assert anchor.hour == 9
    assert anchor.minute == 25
    evidence_items = [evidence for item in ranking for evidence in (item.get("evidence_items") or [])]
    assert evidence_items
    assert all(str(item.get("published_at") or "") <= "2026-05-26T09:25:00" for item in evidence_items)
    assert not any("收盘后证券行业突发重大利好" in str(item.get("content") or "") for item in evidence_items)


def test_load_daily_features_requires_exact_trade_date(db):
    _seed_market_data(db)
    features = catalyst_selection_service._load_daily_features(db, symbols=["600584.SH"], trade_date="2026-05-24")
    assert features == {}


def test_load_selection_run_ignores_stale_score_version(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)

    monkeypatch.setattr(
        news_theme_service,
        "list_theme_rankings",
        lambda *args, **kwargs: {"window": "premarket", "updated_at": "2026-05-26T09:00:00+08:00", "source": "cache:mock", "message": "mock", "items": []},
    )
    monkeypatch.setattr(catalyst_selection_service, "interpret_market_behavior", lambda market: {"narrative_anchors": ["结构性分化"], "locked_values": {}, "data_quality": {"missing_fields": []}})
    monkeypatch.setattr(catalyst_selection_service, "get_reverse_stock_map", lambda: {"600584.SH": "长电科技", "002156.SZ": "通富微电", "601689.SH": "拓普集团"})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_daily_features",
        lambda *args, **kwargs: {
            "600584.SH": {
                "symbol": "600584.SH",
                "name": "长电科技",
                "industry": "半导体",
                "sector": "电子",
                "concepts": ["电子", "半导体", "芯片"],
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "amount": 15_000_000,
                "turnover_rate": 1.8,
                "change_pct": 5.0,
                "amount_ratio_20d": 2.0,
                "momentum_20d": 0.12,
                "momentum_60d": 0.18,
                "r60": 79.66,
                "net_profit_growth_proxy": 0.35,
            }
        },
    )
    monkeypatch.setattr(catalyst_selection_service, "_load_previous_selection_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(catalyst_selection_service, "_load_symbol_settlement_stats", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_score_candidate",
        lambda *args, **kwargs: {
            "rank": 0,
            "symbol": "600584.SH",
            "name": "长电科技",
            "industry": "半导体",
            "sector": "电子",
            "concepts": ["电子", "半导体", "芯片"],
            "score": 80.0,
            "catalyst_score": 70.0,
            "theme_score": 80.0,
            "relation_score": 90.0,
            "market_confirm_score": 60.0,
            "momentum_score": 75.0,
            "fundamental_score": 55.0,
            "continuity_score": 40.0,
            "risk_penalty": 5.0,
            "risk_flags": [],
            "reason_parts": ["mock catalyst"],
            "theme_matches": [{"theme": "半导体", "score": 92.0, "relation_score": 95.0, "catalyst": "mock", "summary": "mock", "source_tier": "S", "evidence_count": 1}],
            "signal_flags": ["R60=80.00强势"],
            "market_background": "mock",
            "market_behavior_labels": {},
            "metric_snapshot": {"r60": 80.0, "change_pct": 5.0, "amount_ratio_20d": 2.0},
        },
    )

    payload = catalyst_selection_service.generate_selections(db, trade_date="2026-05-26", window="premarket", limit=5)
    assert payload["items"]

    db.execute(
        text(
            """
            UPDATE catalyst_selection_runs
            SET score_version = 'catalyst-selection-v1'
            WHERE trade_date = '2026-05-26'
            """
        )
    )
    db.commit()

    loaded = catalyst_selection_service.list_or_generate_selections(db, trade_date="2026-05-26", window="premarket", limit=5, force=False)
    assert loaded["data_governance"]["score_version"] == catalyst_selection_service.SCORE_VERSION


def test_refresh_event_driven_selection_dedupes_windows_and_reports_generated(db, monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_effective_cn_trade_date", lambda: "2026-05-26")
    monkeypatch.setattr(catalyst_selection_service, "_latest_available_daily_trade_date", lambda *args, **kwargs: "2026-05-26")
    monkeypatch.setattr(
        catalyst_selection_service,
        "settle_pending_selections",
        lambda *args, **kwargs: {
            "settled": [
                {
                    "trade_date": "2026-05-25",
                    "feedback_refresh": {
                        "model_version": catalyst_selection_service.FEEDBACK_MODEL_VERSION,
                        "updated_profile_count": 5,
                        "new_profile_count": 1,
                        "changed_profile_count": 2,
                        "top_profile_changes": [
                            {
                                "profile_scope": "symbol",
                                "profile_key": "600584.SH",
                                "learned_score_before": 50.0,
                                "learned_score_after": 68.0,
                                "learned_score_delta": 18.0,
                                "sample_count_after": 4,
                            }
                        ],
                        "symbol_profile_count": 2,
                        "theme_profile_count": 1,
                        "event_type_profile_count": 1,
                        "risk_gate_profile_count": 1,
                        "intraday_pulse_profile_count": 0,
                        "updated_at": "2026-05-26T08:30:00",
                    },
                }
            ],
            "errors": [],
            "skipped": False,
        },
    )

    calls: list[tuple[str, str, int, str | None]] = []
    trigger_contexts: list[dict[str, object]] = []

    def fake_generate_selections(session, *, trade_date, window, limit, user_id=None, **kwargs):
        calls.append((trade_date, window, limit, user_id))
        local_trigger_context = dict(kwargs.get("trigger_context") or {})
        trigger_contexts.append(local_trigger_context)
        return {
            "items": [
                {
                    "rank": 1,
                    "symbol": "600584.SH",
                    "name": "长电科技",
                    "score": 88.0,
                    "adaptive_feedback_score": 68.5,
                    "risk_control": {
                        "action": "deploy",
                        "risk_level": "low",
                        "max_position_pct": 10.0,
                        "stop_loss_pct": 5.6,
                        "invalidations": [],
                        "risk_monitoring": {
                            "execution_gate": "allow",
                            "gate_feedback": {"profile_key": "allow", "applied": True},
                        },
                    },
                    "closed_loop_trace": {
                        "event": {"theme": "半导体"},
                        "feedback": {
                            "symbol_profile": {"profile_scope": "symbol", "profile_key": "600584.SH"},
                            "theme_profile": {"profile_scope": "theme", "profile_key": "半导体"},
                            "event_type_profile": {"profile_scope": "event_type", "profile_key": "产业进展"},
                        },
                        "scoring": {
                            "learning_impact": {
                                "status": "active",
                                "score_delta_from_learning_policy": 4.2,
                                "rank_before_learning_policy": 2,
                                "final_rank": 1,
                                "rank_delta_from_learning_policy": 1,
                                "risk_effect": {
                                    "action_changed": False,
                                    "max_position_delta_pct": 1.5,
                                },
                                "risk_gate_effect": {
                                    "applied": True,
                                    "adjustment": "support_allow",
                                },
                            }
                        },
                    },
                }
            ],
            "data_governance": {
                "opportunity_events": [{"symbol": "600584.SH", "event_level": "S"}],
                "closed_loop": {
                    "risk_control_summary": {
                        "item_count": 1,
                        "action_counts": {"deploy": 1},
                        "risk_level_counts": {"low": 1},
                        "deploy_count": 1,
                        "follow_count": 0,
                        "wait_count": 0,
                        "observe_count": 0,
                        "restricted_count": 0,
                        "invalidation_count": 0,
                        "average_max_position_pct": 10.0,
                        "average_stop_loss_pct": 5.6,
                    },
                    "feedback_learning_state": {
                        "profile_count": 2,
                        "sample_count": 11,
                        "selected_with_feedback_count": 1,
                        "selected_count": 1,
                        "selected_adaptive_feedback_avg": 68.5,
                        "risk_gate_profile_count": 1,
                    },
                    "risk_gate_feedback_summary": {
                        "profile_count": 1,
                        "used_count": 1,
                        "applied_count": 1,
                        "tightened_count": 1,
                        "supportive_count": 0,
                        "overly_conservative_count": 0,
                    },
                    "learning_adjustment_summary": {
                        "item_count": 1,
                        "active_count": 1,
                        "stance_counts": {"expand": 1},
                        "expand_count": 1,
                    },
                    "learning_impact_summary": {
                        "item_count": 1,
                        "active_count": 1,
                        "average_score_delta": 4.2,
                        "improved_rank_count": 1,
                        "gate_applied_count": 1,
                    },
                    "feedback_risk_gate_count": 1,
                    "realtime_feedback": {
                        "status": "active",
                        "sample_count": 3,
                        "symbol_feedback_count": 2,
                        "risk_feedback_count": 3,
                        "monitor_count": 1,
                        "latest_event_time": "2026-05-26T10:05:00",
                        "event_type_counts": {"signal_generated": 2, "signal_blocked": 1},
                        "risk_gate_counts": {"allow": 2, "blocked": 1},
                    },
                    "score_profile_counts": {"offensive": 1},
                    "market_state": True,
                    "market_state_freshness": {"status": "aligned"},
                    "intraday_event_pulse": {"status": "confirming"},
                    "llm_event_understanding": {
                        "ready": True,
                        "model": "mock-model",
                        "used_semantic_theme_count": 2,
                        "used_symbol_theme_count": 1,
                    },
                    "end_to_end_evidence": {
                        "status": "active",
                        "active_count": 6,
                        "warming_up_count": 0,
                        "degraded_count": 0,
                        "missing_count": 0,
                        "stage_count": 6,
                        "pass_rate": 1.0,
                        "trigger": local_trigger_context.get("trigger"),
                        "refresh_key": local_trigger_context.get("refresh_key"),
                        "trigger_source": local_trigger_context.get("source"),
                        "stages": [
                            {"id": "proactive_opportunity_discovery", "status": "active"},
                            {"id": "event_understanding", "status": "active"},
                            {"id": "market_state_judgement", "status": "active"},
                            {"id": "dynamic_ranking", "status": "active"},
                            {"id": "risk_control", "status": "active"},
                            {"id": "feedback_learning", "status": "active"},
                        ],
                    },
                },
            },
            "updated_at": "2026-05-26T09:00:00+08:00",
        }

    monkeypatch.setattr(catalyst_selection_service, "generate_selections", fake_generate_selections)

    payload = catalyst_selection_service.refresh_event_driven_selection(
        db,
        trigger="news-eye:manual",
        windows=("premarket", "premarket", "24h"),
        limit=5,
        user_id="user-1",
    )

    assert payload["skipped"] is False
    assert payload["settlement_refresh"]["settled"][0]["trade_date"] == "2026-05-25"
    assert payload["trade_date"] == "2026-05-26"
    assert [item["window"] for item in payload["generated"]] == ["premarket", "24h"]
    assert [item["trigger"] for item in trigger_contexts] == ["news-eye:manual", "news-eye:manual"]
    assert [item["window"] for item in trigger_contexts] == ["premarket", "24h"]
    assert [item["source"] for item in trigger_contexts] == ["catalyst_selection_event_refresh", "catalyst_selection_event_refresh"]
    assert payload["generated"][0]["top_symbol"] == "600584.SH"
    assert payload["generated"][0]["risk_control_summary"]["deploy_count"] == 1
    assert payload["generated"][0]["feedback_learning_state"]["selected_count"] == 1
    assert payload["generated"][0]["learning_impact_summary"]["improved_rank_count"] == 1
    assert payload["generated"][0]["top_learning_impacts"][0]["symbol"] == "600584.SH"
    assert payload["generated"][0]["top_learning_impacts"][0]["profiles"]["symbol"] == "600584.SH"
    assert payload["generated"][0]["end_to_end_evidence"]["status"] == "active"
    assert payload["generated"][0]["end_to_end_evidence"]["trigger_source"] == "catalyst_selection_event_refresh"
    assert payload["generated"][0]["monitor_activation"]["status"] == "skipped"
    assert payload["closed_loop_audit"]["status"] == "completed"
    assert payload["closed_loop_audit"]["end_to_end_evidence"]["status"] == "active"
    assert payload["closed_loop_audit"]["end_to_end_evidence"]["active_window_count"] == 2
    assert payload["closed_loop_audit"]["end_to_end_evidence"]["stage_rollup"]["event_understanding"]["active"] == 2
    assert payload["closed_loop_audit"]["risk_action_counts"]["deploy"] == 2
    assert payload["closed_loop_audit"]["monitor_activation"]["skipped_count"] == 2
    assert payload["closed_loop_audit"]["feedback"]["selected_with_feedback_count"] == 2
    assert payload["generated"][0]["risk_gate_feedback_summary"]["tightened_count"] == 1
    assert payload["closed_loop_audit"]["feedback"]["risk_gate_profile_count"] == 1
    assert payload["closed_loop_audit"]["feedback"]["risk_gate_used_count"] == 2
    assert payload["closed_loop_audit"]["feedback"]["risk_gate_applied_count"] == 2
    assert payload["closed_loop_audit"]["feedback"]["risk_gate_tightened_count"] == 2
    assert payload["closed_loop_audit"]["feedback"]["realtime_sample_count"] == 3
    assert payload["closed_loop_audit"]["feedback"]["realtime_symbol_feedback_count"] == 2
    assert payload["closed_loop_audit"]["feedback"]["realtime"]["event_type_counts"]["signal_blocked"] == 1
    realtime_replay = payload["closed_loop_audit"]["feedback"]["realtime_replay"]
    assert realtime_replay["status"] == "active"
    assert realtime_replay["matched_selection_count"] == 1
    assert realtime_replay["score_changed_count"] == 2
    assert realtime_replay["rank_changed_count"] == 2
    assert realtime_replay["risk_changed_count"] == 2
    assert payload["closed_loop_audit"]["settlement"]["feedback_refresh"]["updated_profile_count"] == 5
    assert payload["closed_loop_audit"]["settlement"]["feedback_refresh"]["new_profile_count"] == 1
    assert payload["closed_loop_audit"]["settlement"]["feedback_refresh"]["changed_profile_count"] == 2
    assert payload["closed_loop_audit"]["settlement"]["feedback_refresh"]["top_profile_changes"][0]["profile_key"] == "600584.SH"
    assert payload["closed_loop_audit"]["settlement"]["feedback_refresh"]["symbol_profile_count"] == 2
    replay = payload["closed_loop_audit"]["settlement"]["feedback_replay"]
    assert replay["status"] == "active"
    assert replay["matched_selection_count"] == 1
    assert replay["score_changed_count"] == 1
    assert replay["rank_changed_count"] == 1
    assert replay["risk_changed_count"] == 1
    assert replay["items"][0]["symbol"] == "600584.SH"
    assert replay["items"][0]["score_delta_from_learning_policy"] == 4.2
    assert payload["closed_loop_audit"]["requirement_summary"]["overall_status"] == "active"
    assert payload["closed_loop_audit"]["requirement_summary"]["active_count"] == 6
    checks = {item["id"]: item for item in payload["closed_loop_audit"]["requirement_checks"]}
    assert checks["opportunity_discovery"]["status"] == "active"
    assert checks["event_understanding"]["metrics"]["used_semantic_theme_count"] == 4
    assert checks["market_state"]["metrics"]["freshness_status_counts"]["aligned"] == 2
    assert checks["feedback_learning"]["metrics"]["realtime_sample_count"] == 3
    assert checks["feedback_learning"]["metrics"]["learning_impact_active_count"] == 2
    assert checks["feedback_learning"]["metrics"]["learning_impact_score_changed_count"] == 2
    assert checks["feedback_learning"]["metrics"]["learning_impact_rank_changed_count"] == 2
    assert checks["feedback_learning"]["metrics"]["learning_impact_risk_changed_count"] == 2
    assert checks["feedback_learning"]["metrics"]["realtime_feedback_replay_matched_count"] == 1
    assert checks["feedback_learning"]["metrics"]["settlement_feedback_updated_profile_count"] == 5
    assert checks["feedback_learning"]["metrics"]["settlement_feedback_replay_matched_count"] == 1
    audit_history = catalyst_selection_service.list_closed_loop_audits(db, trade_date="2026-05-26", limit=5)
    assert audit_history["items"][0]["audit_id"] == payload["closed_loop_audit"]["audit_id"]
    assert audit_history["items"][0]["status"] == "completed"
    assert audit_history["items"][0]["risk_action_counts"]["deploy"] == 2
    assert audit_history["items"][0]["monitor_activation"]["skipped_count"] == 2
    assert audit_history["items"][0]["feedback"]["risk_gate_used_count"] == 2
    assert audit_history["items"][0]["feedback"]["realtime_sample_count"] == 3
    assert audit_history["items"][0]["settlement"]["feedback_replay"]["status"] == "active"
    assert audit_history["items"][0]["requirement_summary"]["active_like_count"] == 6
    assert calls == [
        ("2026-05-26", "premarket", 5, "user-1"),
        ("2026-05-26", "24h", 5, "user-1"),
    ]


def test_learning_replay_reads_persisted_learning_impacts(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    run_id = uuid4().hex
    now = datetime(2026, 5, 26, 9, 30)
    items = [
        {
            "rank": 1,
            "symbol": "600584.SH",
            "name": "长电科技",
            "industry": "电子",
            "sector": "半导体",
            "concepts": ["半导体"],
            "score": 86.0,
            "catalyst_score": 80.0,
            "theme_score": 78.0,
            "relation_score": 76.0,
            "market_confirm_score": 60.0,
            "event_intelligence_score": 74.0,
            "momentum_score": 55.0,
            "fundamental_score": 50.0,
            "continuity_score": 52.0,
            "adaptive_feedback_score": 66.0,
            "risk_penalty": 4.0,
            "risk_flags": [],
            "reason_parts": ["反馈画像压制过热但仍保留核心排序"],
            "theme_matches": [{"theme": "半导体"}],
            "signal_flags": [],
            "metric_snapshot": {},
            "risk_control": {
                "action": "deploy",
                "risk_level": "medium",
                "max_position_pct": 8.0,
                "risk_monitoring": {
                    "execution_gate": "confirm",
                    "gate_feedback": {"profile_key": "confirm", "applied": True},
                },
            },
            "closed_loop_trace": {
                "event": {"theme": "半导体"},
                "feedback": {
                    "symbol_profile": {"profile_scope": "symbol", "profile_key": "600584.SH"},
                    "theme_profile": {"profile_scope": "theme", "profile_key": "半导体"},
                    "event_type_profile": {"profile_scope": "event_type", "profile_key": "产业进展"},
                },
                "scoring": {
                    "learning_impact": {
                        "status": "active",
                        "score_delta_from_learning_policy": -5.2,
                        "rank_before_learning_policy": 2,
                        "final_rank": 1,
                        "rank_delta_from_learning_policy": 1,
                        "risk_effect": {
                            "action_changed": False,
                            "max_position_delta_pct": -1.5,
                        },
                        "risk_gate_effect": {
                            "applied": True,
                            "adjustment": "confirm_required",
                        },
                    }
                },
            },
            "market_background": "",
        },
        {
            "rank": 2,
            "symbol": "300750.SZ",
            "name": "宁德时代",
            "industry": "电力设备",
            "sector": "电池",
            "concepts": ["固态电池"],
            "score": 82.0,
            "catalyst_score": 76.0,
            "theme_score": 74.0,
            "relation_score": 72.0,
            "market_confirm_score": 58.0,
            "event_intelligence_score": 70.0,
            "momentum_score": 54.0,
            "fundamental_score": 56.0,
            "continuity_score": 50.0,
            "adaptive_feedback_score": 62.0,
            "risk_penalty": 3.0,
            "risk_flags": [],
            "reason_parts": ["历史反馈支持产业链核心标的"],
            "theme_matches": [{"theme": "固态电池"}],
            "signal_flags": [],
            "metric_snapshot": {},
            "risk_control": {"action": "deploy", "risk_level": "low", "max_position_pct": 10.0},
            "closed_loop_trace": {
                "event": {"theme": "固态电池"},
                "feedback": {
                    "symbol_profile": {"profile_scope": "symbol", "profile_key": "300750.SZ"},
                    "theme_profile": {"profile_scope": "theme", "profile_key": "固态电池"},
                },
                "scoring": {
                    "learning_impact": {
                        "status": "active",
                        "score_delta_from_learning_policy": 3.6,
                        "rank_before_learning_policy": 4,
                        "final_rank": 2,
                        "rank_delta_from_learning_policy": 2,
                        "risk_effect": {"action_changed": False, "max_position_delta_pct": 0.0},
                        "risk_gate_effect": {"applied": False},
                    }
                },
            },
            "market_background": "",
        },
    ]
    catalyst_selection_service._persist_selection_run(
        db,
        run_id=run_id,
        trade_date="2026-05-26",
        window="24h",
        window_start=None,
        window_end=None,
        market_background="",
        market_behavior={},
        items=items,
        data_governance={"closed_loop": {"feedback_learning": True}},
        opportunity_events=[],
        now_value=now,
    )
    catalyst_selection_service._persist_closed_loop_audit(
        db,
        {
            "audit_id": "learning-replay-audit",
            "trigger": "test",
            "trade_date": "2026-05-26",
            "status": "completed",
            "settlement": {
                "feedback_replay": {
                    "status": "no_profile_change",
                    "candidate_impact_count": 2,
                    "matched_selection_count": 0,
                }
            },
            "feedback": {
                "realtime_replay": {
                    "status": "active",
                    "sample_count": 6,
                    "candidate_impact_count": 2,
                    "matched_selection_count": 2,
                    "score_changed_count": 2,
                    "rank_changed_count": 2,
                    "risk_changed_count": 1,
                }
            },
            "generated": [],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        },
    )
    db.commit()

    replay = catalyst_selection_service.get_learning_replay(db, trade_date="2026-05-26", limit=10)

    assert replay["status"] == "active"
    assert replay["trade_date"] == "2026-05-26"
    assert replay["audit_id"] == "learning-replay-audit"
    assert replay["candidate_impact_count"] == 2
    assert replay["active_impact_count"] == 2
    assert replay["score_changed_count"] == 2
    assert replay["rank_changed_count"] == 2
    assert replay["risk_changed_count"] == 1
    assert replay["gate_applied_count"] == 1
    assert replay["windows"][0]["window"] == "24h"
    assert replay["windows"][0]["candidate_impact_count"] == 2
    assert replay["items"][0]["symbol"] == "600584.SH"
    assert replay["items"][0]["window"] == "24h"
    assert replay["items"][0]["profiles"]["theme"] == "半导体"
    assert replay["realtime_feedback_replay"]["status"] == "active"
    assert replay["realtime_feedback_replay"]["matched_selection_count"] == 2
    assert any("实时反哺回放 active" in item for item in replay["evidence"])
    assert not any("最近结算未产生可匹配的新画像变化" in item for item in replay["gaps"])


def test_schedule_event_driven_selection_refresh_dedupes_running_task(monkeypatch):
    started: list[dict[str, object]] = []
    persisted: list[dict[str, object]] = []

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            started.append(self.kwargs)

    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()
    monkeypatch.setattr(catalyst_selection_service.threading, "Thread", FakeThread)
    monkeypatch.setattr(catalyst_selection_service, "_persist_event_refresh_state_in_new_session", lambda state: persisted.append(dict(state)))

    try:
        first = catalyst_selection_service.schedule_event_driven_selection_refresh(
            trigger="qmt-minute-subscription:intraday",
            windows=("24h",),
            limit=10,
            user_id="user-1",
            reason="minute_capture",
            context={"capture_rows": 128},
        )
        second = catalyst_selection_service.schedule_event_driven_selection_refresh(
            trigger="qmt-minute-subscription:intraday",
            windows=("24h",),
            limit=10,
            user_id="user-1",
            reason="minute_capture",
            context={"capture_rows": 88},
        )

        assert first["status"] == "scheduled"
        assert first["windows"] == ["24h"]
        assert first["context"] == {"capture_rows": 128}
        assert second["status"] == "running"
        assert second["deduped"] is True
        assert len(started) == 1
        assert started[0]["kwargs"]["trigger"] == "qmt-minute-subscription:intraday"
        assert started[0]["kwargs"]["windows"] == ("24h",)
        assert started[0]["kwargs"]["user_id"] == "user-1"
        assert [item["status"] for item in persisted] == ["scheduled", "running"]
        assert persisted[-1]["deduped"] is True
    finally:
        catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
        catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()


def test_schedule_event_driven_selection_refresh_persists_run_state(db, monkeypatch):
    test_session_local = database_module.sessionmaker(autocommit=False, autoflush=False, bind=db.get_bind())
    monkeypatch.setattr(catalyst_selection_service, "SessionLocal", test_session_local)
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()

    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        def start(self):
            return None

    monkeypatch.setattr(catalyst_selection_service.threading, "Thread", FakeThread)

    try:
        first = catalyst_selection_service.schedule_event_driven_selection_refresh(
            trigger="qmt-market-sync:intraday",
            windows=("24h",),
            limit=7,
            user_id="user-persist",
            trade_date="2026-05-26",
            reason="minute_capture",
            context={"capture_success": True, "capture_rows": 128},
        )
        second = catalyst_selection_service.schedule_event_driven_selection_refresh(
            trigger="qmt-market-sync:intraday",
            windows=("24h",),
            limit=7,
            user_id="user-persist",
            trade_date="2026-05-26",
            reason="minute_capture",
            context={"capture_success": True, "capture_rows": 88},
        )

        assert first["status"] == "scheduled"
        assert second["status"] == "running"

        runs = catalyst_selection_service.list_event_refresh_runs(db, user_id="user-persist", limit=5)
        assert len(runs["items"]) == 1
        item = runs["items"][0]
        assert item["refresh_key"] == first["refresh_key"]
        assert item["trigger"] == "qmt-market-sync:intraday"
        assert item["status"] == "running"
        assert item["deduped"] is True
        assert item["windows"] == ["24h"]
        assert item["limit"] == 7
        assert item["context"]["capture_rows"] == 88
    finally:
        catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
        catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()


def test_run_scheduled_event_driven_selection_refresh_persists_completed_state(db, monkeypatch):
    test_session_local = database_module.sessionmaker(autocommit=False, autoflush=False, bind=db.get_bind())
    monkeypatch.setattr(catalyst_selection_service, "SessionLocal", test_session_local)
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.add("refresh-complete")
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING["refresh-complete"] = {"status": "running"}

    captured: dict[str, object] = {}

    def fake_refresh(session, *, trigger, windows, limit, user_id=None, trade_date=None, refresh_key=None, trigger_context=None):
        captured["refresh_key"] = refresh_key
        captured["trigger_context"] = trigger_context or {}
        catalyst_selection_service.ensure_catalyst_selection_tables(session)
        return {
            "trigger": trigger,
            "trade_date": trade_date or "2026-05-26",
            "generated": [{"window": windows[0], "item_count": 2, "top_symbol": "600584.SH"}],
            "errors": [],
            "skipped": False,
            "closed_loop_audit": {"audit_id": "audit-complete"},
            "updated_at": "2026-05-26T09:31:00",
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh)

    catalyst_selection_service._run_scheduled_event_driven_selection_refresh(
        refresh_key="refresh-complete",
        trigger="qmt-minute-subscription:intraday",
        windows=("24h",),
        limit=10,
        user_id="user-run",
        trade_date="2026-05-26",
        reason="minute_capture",
        context={"capture_rows": 12},
    )

    runs = catalyst_selection_service.list_event_refresh_runs(db, user_id="user-run", limit=5)
    assert len(runs["items"]) == 1
    item = runs["items"][0]
    assert item["refresh_key"] == "refresh-complete"
    assert item["status"] == "completed"
    assert item["generated"][0]["top_symbol"] == "600584.SH"
    assert item["errors"] == []
    assert item["audit_id"] == "audit-complete"
    assert isinstance(item["duration_ms"], int)
    assert captured["refresh_key"] == "refresh-complete"
    assert captured["trigger_context"]["capture_rows"] == 12
    assert captured["trigger_context"]["reason"] == "minute_capture"
    assert "refresh-complete" not in catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS
    assert "refresh-complete" not in catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING


def test_event_refresh_running_upsert_clears_previous_finished_state(db):
    completed_state = {
        "refresh_key": "refresh-reused",
        "trigger": "news-eye:background",
        "windows": ["premarket", "24h"],
        "limit": 10,
        "user_id": "user-run",
        "trade_date": "2026-05-26",
        "status": "completed",
        "deduped": False,
        "generated": [{"window": "24h", "item_count": 2}],
        "errors": [],
        "skipped": False,
        "duration_ms": 1200,
        "started_at": "2026-05-26T09:30:00",
        "finished_at": "2026-05-26T09:30:01",
        "updated_at": "2026-05-26T09:30:01",
    }
    catalyst_selection_service._persist_event_refresh_state(db, completed_state)
    db.commit()

    running_state = {
        **completed_state,
        "status": "running",
        "deduped": True,
        "generated": [],
        "duration_ms": None,
        "started_at": "2026-05-26T09:35:00",
        "finished_at": None,
        "updated_at": "2026-05-26T09:35:00",
    }
    catalyst_selection_service._persist_event_refresh_state(db, running_state)
    db.commit()

    runs = catalyst_selection_service.list_event_refresh_runs(db, user_id="user-run", limit=5)
    item = runs["items"][0]
    assert item["refresh_key"] == "refresh-reused"
    assert item["status"] == "running"
    assert item["deduped"] is True
    assert item["duration_ms"] is None
    assert not item["finished_at"]
    assert item["generated"] == []


def test_run_scheduled_event_driven_selection_refresh_persists_failed_state(db, monkeypatch):
    test_session_local = database_module.sessionmaker(autocommit=False, autoflush=False, bind=db.get_bind())
    monkeypatch.setattr(catalyst_selection_service, "SessionLocal", test_session_local)
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_PENDING.clear()
    catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS.add("refresh-failed")

    def fail_refresh(*args, **kwargs):
        raise RuntimeError("refresh exploded")

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fail_refresh)

    catalyst_selection_service._run_scheduled_event_driven_selection_refresh(
        refresh_key="refresh-failed",
        trigger="qmt-minute-subscription:intraday",
        windows=("24h",),
        limit=10,
        user_id="user-run",
        trade_date="2026-05-26",
        reason="minute_capture",
        context={},
    )

    runs = catalyst_selection_service.list_event_refresh_runs(db, user_id="user-run", status="failed", limit=5)
    assert len(runs["items"]) == 1
    item = runs["items"][0]
    assert item["status"] == "failed"
    assert item["errors"][0]["error"] == "refresh exploded"
    assert item["finished_at"]
    assert "refresh-failed" not in catalyst_selection_service._EVENT_DRIVEN_REFRESH_TASKS


def test_refresh_event_driven_selection_skips_without_daily_data(db, monkeypatch):
    monkeypatch.setattr(catalyst_selection_service, "_effective_cn_trade_date", lambda: "2026-05-26")
    monkeypatch.setattr(catalyst_selection_service, "_latest_available_daily_trade_date", lambda *args, **kwargs: None)

    payload = catalyst_selection_service.refresh_event_driven_selection(
        db,
        trigger="news-eye:manual",
        windows=("premarket",),
        limit=5,
    )

    assert payload["skipped"] is True
    assert payload["generated"] == []
    assert payload["errors"] == []
    assert payload["closed_loop_audit"]["status"] == "skipped"
    assert payload["closed_loop_audit"]["requirement_summary"]["overall_status"] == "incomplete"
    assert payload["closed_loop_audit"]["requirement_summary"]["missing_count"] == 6
    assert {item["status"] for item in payload["closed_loop_audit"]["requirement_checks"]} == {"missing"}
    audit_history = catalyst_selection_service.list_closed_loop_audits(db, limit=5)
    assert audit_history["items"][0]["audit_id"] == payload["closed_loop_audit"]["audit_id"]
    assert audit_history["items"][0]["skip_reason"] == payload["skip_reason"]


def test_list_closed_loop_audits_backfills_requirement_checks_for_legacy_rows(db):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    legacy_audit = {
        "audit_id": "legacy-audit-1",
        "trigger": "legacy:scheduler",
        "trade_date": "2026-05-26",
        "status": "completed",
        "requested_window_count": 1,
        "generated_window_count": 1,
        "failed_window_count": 0,
        "total_selected_count": 1,
        "opportunity_event_count": 1,
        "risk_action_counts": {"observe": 1},
        "risk_level_counts": {"medium": 1},
        "feedback": {"profile_count": 1, "sample_count": 8, "selected_with_feedback_count": 1, "selected_count": 1},
        "monitor_activation": {"created_count": 1, "items": [{"status": "created", "started": True}]},
        "llm_ready_window_count": 1,
        "settlement": {
            "skipped": False,
            "settled_count": 1,
            "error_count": 0,
            "feedback_refresh": {
                "updated_profile_count": 3,
                "symbol_profile_count": 1,
                "theme_profile_count": 1,
                "event_type_profile_count": 1,
                "risk_gate_profile_count": 0,
                "intraday_pulse_profile_count": 0,
            },
        },
        "generated": [
            {
                "window": "24h",
                "item_count": 1,
                "opportunity_event_count": 1,
                "risk_control_summary": {
                    "action_counts": {"observe": 1},
                    "risk_level_counts": {"medium": 1},
                },
                "feedback_learning_state": {
                    "profile_count": 1,
                    "sample_count": 8,
                    "selected_with_feedback_count": 1,
                    "selected_count": 1,
                },
                "score_profile_counts": {"defensive": 1},
                "llm_event_understanding": {
                    "ready": True,
                    "used_semantic_theme_count": 1,
                    "used_symbol_theme_count": 1,
                },
                "monitor_activation": {"status": "created", "started": True},
            }
        ],
        "errors": [],
        "created_at": "2026-05-26T09:00:00",
        "updated_at": "2026-05-26T09:00:00",
    }
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_closed_loop_audits (
                audit_id, trade_date, trigger_name, status, audit_json, created_at, updated_at
            ) VALUES (
                :audit_id, :trade_date, :trigger_name, :status, :audit_json, NOW(), NOW()
            )
            """
        ),
        {
            "audit_id": legacy_audit["audit_id"],
            "trade_date": legacy_audit["trade_date"],
            "trigger_name": legacy_audit["trigger"],
            "status": legacy_audit["status"],
            "audit_json": json.dumps(legacy_audit, ensure_ascii=False),
        },
    )
    db.commit()

    audit_history = catalyst_selection_service.list_closed_loop_audits(db, trade_date="2026-05-26", limit=5)
    item = audit_history["items"][0]
    checks = {check["id"]: check for check in item["requirement_checks"]}

    assert item["requirement_summary"]["total_count"] == 6
    assert checks["opportunity_discovery"]["status"] == "active"
    assert checks["event_understanding"]["status"] == "active"
    assert checks["feedback_learning"]["metrics"]["settled_count"] == 1
    assert checks["feedback_learning"]["metrics"]["settlement_feedback_updated_profile_count"] == 3


def test_settle_pending_selections_only_settles_current_version_without_existing_settlement(db, monkeypatch):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_runs (
                run_id, trade_date, window_label, score_version, item_count, source, created_at, updated_at
            )
            VALUES
            ('pending-v2', '2026-05-27', 'premarket', :score_version, 1, 'mock', NOW(), NOW()),
            ('settled-v2', '2026-05-26', 'premarket', :score_version, 1, 'mock', NOW(), NOW()),
            ('old-v1', '2026-05-25', 'premarket', 'catalyst-selection-v1', 1, 'mock', NOW(), NOW())
            """
        ),
        {"score_version": catalyst_selection_service.SCORE_VERSION},
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_settlements (
                trade_date, settlement_date, symbol, name, rank, outcome, protected, settlement_notes_json, updated_at
            )
            VALUES ('2026-05-26', '2026-05-27', '600584.SH', '长电科技', 1, 'hit', TRUE, '[]', NOW())
            """
        )
    )
    db.commit()

    called: list[str] = []

    def fake_settle_selection(session, *, trade_date, force=False):
        called.append(trade_date)
        return {
            "trade_date": trade_date,
            "settlement_date": "2026-05-28",
            "items": [{"symbol": "600584.SH"}],
            "feedback_refresh": {"updated_profile_count": 2},
        }

    monkeypatch.setattr(catalyst_selection_service, "settle_selection", fake_settle_selection)

    payload = catalyst_selection_service.settle_pending_selections(db, before_trade_date="2026-05-28", limit=10)

    assert called == ["2026-05-27"]
    assert payload["settled"][0]["trade_date"] == "2026-05-27"
    assert payload["settled"][0]["feedback_refresh"]["updated_profile_count"] == 2
    assert payload["errors"] == []


def test_mainline_alignment_filters_news_only_theme_candidates():
    theme_items = [
        {
            "theme": "半导体",
            "score": 70.0,
            "market_confirmation": {"score": 6.0},
            "policy_boost": False,
            "top_source_tier": "A",
        },
        {
            "theme": "金融",
            "score": 90.0,
            "market_confirmation": {"score": 0.0},
            "policy_boost": False,
            "top_source_tier": "B",
        },
    ]
    market_snapshot = {
        "sector_gainers": [
            {"sector_name": "电子", "change_pct": 5.5, "amount": 300_000_000_000},
            {"sector_name": "半导体", "change_pct": 4.8, "amount": 180_000_000_000},
        ],
        "sector_losers": [{"sector_name": "银行", "change_pct": -1.2}],
        "sector_inflows": [{"sector_name": "电子", "net_inflow": 6_000_000_000}],
        "sector_outflows": [{"sector_name": "银行", "net_inflow": -2_000_000_000}],
        "market_stats": {
            "index_turnover_amount": 2_200_000_000_000,
            "up_count": 3100,
            "down_count": 1800,
        },
    }
    market_behavior = {
        "sector_battlefield": {"score": {"leaders": ["电子", "半导体"], "laggards": ["银行"]}},
        "style_rotation": {"score": {"leaders": ["电子"], "inflows": ["电子"]}},
    }

    aligned = catalyst_selection_service._mainline_aligned_theme_items(
        theme_items,
        market_snapshot=market_snapshot,
        market_behavior=market_behavior,
    )

    assert [item["theme"] for item in aligned] == ["半导体"]
    assert aligned[0]["mainline_alignment_score"] > 0
    assert any("盘面强度确认" in reason for reason in aligned[0]["mainline_alignment_reasons"])


def test_mainline_alignment_falls_back_when_market_width_missing():
    theme_items = [{"theme": "金融", "score": 90.0}]

    aligned = catalyst_selection_service._mainline_aligned_theme_items(
        theme_items,
        market_snapshot={"market_stats": {}},
        market_behavior={},
    )

    assert aligned[0]["theme"] == "金融"
    assert aligned[0]["mainline_alignment_score"] == 0.0
    assert "市场成交额/涨跌家数缺失" in aligned[0]["mainline_alignment_reasons"][0]


def test_settlement_reuses_existing_run_and_marks_protected(db, monkeypatch):
    _seed_market_data(db)
    _seed_news_items(db)

    monkeypatch.setattr(news_theme_service, "list_theme_rankings", lambda *args, **kwargs: {"window": "premarket", "updated_at": "2026-05-26T09:00:00+08:00", "source": "cache:mock", "message": "mock", "items": []})
    monkeypatch.setattr(catalyst_selection_service, "interpret_market_behavior", lambda market: {"narrative_anchors": ["结构性分化"], "locked_values": {}, "data_quality": {"missing_fields": []}})
    monkeypatch.setattr(catalyst_selection_service, "get_reverse_stock_map", lambda: {"600584.SH": "长电科技", "002156.SZ": "通富微电", "601689.SH": "拓普集团"})

    def fake_candidate(*, symbol, features, theme_items, previous_state, history_stats, theme_feedback=None, market_background, market_behavior, risk_gate_feedback=None, trigger_news_context=None):
        del risk_gate_feedback, trigger_news_context
        score = 80.0 if symbol == "600584.SH" else 72.0 if symbol == "002156.SZ" else 0.0
        if score <= 0:
            return {"symbol": symbol, "score": 0}
        return {
            "rank": 0,
            "symbol": symbol,
            "name": features.get("name") or symbol,
            "industry": features.get("industry"),
            "sector": features.get("sector"),
            "concepts": features.get("concepts") or [],
            "score": score,
            "catalyst_score": 70.0,
            "theme_score": 80.0,
            "relation_score": 90.0,
            "market_confirm_score": 60.0,
            "momentum_score": 75.0,
            "fundamental_score": 55.0,
            "continuity_score": 40.0,
            "risk_penalty": 5.0,
            "risk_flags": [],
            "reason_parts": ["mock catalyst"],
            "theme_matches": [{"theme": "半导体", "score": 92.0, "relation_score": 95.0, "catalyst": "mock", "summary": "mock", "source_tier": "S", "evidence_count": 1}],
            "signal_flags": ["R60=80.00强势"],
            "market_background": market_background,
            "market_behavior_labels": market_behavior,
            "metric_snapshot": {"r60": 80.0, "change_pct": 5.0, "amount_ratio_20d": 2.0},
        }

    monkeypatch.setattr(catalyst_selection_service, "_score_candidate", fake_candidate)
    monkeypatch.setattr(catalyst_selection_service, "_load_previous_selection_state", lambda *args, **kwargs: {"600584.SH": {"streak": 1, "last_rank": 1}})
    monkeypatch.setattr(catalyst_selection_service, "_load_symbol_settlement_stats", lambda *args, **kwargs: {"600584.SH": {"count": 3, "hit_rate": 0.7, "loss_count": 0}})

    generated = catalyst_selection_service.generate_selections(
        db,
        trade_date="2026-05-26",
        window="premarket",
        limit=5,
    )
    assert generated["items"][0]["symbol"] == "600584.SH"

    payload = catalyst_selection_service.settle_selection(db, trade_date="2026-05-26", force=True)

    assert payload["settlement_date"] == "2026-05-27"
    assert len(payload["items"]) == 2
    assert payload["items"][0]["symbol"] == "600584.SH"
    assert payload["items"][0]["protected"] is True
    assert payload["items"][0]["outcome"] in {"hit", "strong_hit", "miss", "weak_miss"}
    assert any("结算结果" in note for note in payload["items"][0]["settlement_notes"])
    assert payload["feedback_refresh"]["model_version"] == catalyst_selection_service.FEEDBACK_MODEL_VERSION
    assert payload["feedback_refresh"]["updated_profile_count"] >= 1

    stored = db.execute(
        text(
            """
            SELECT protected, outcome, settlement_notes_json
            FROM catalyst_selection_settlements
            WHERE trade_date = '2026-05-26' AND symbol = '600584.SH'
            """
        )
    ).mappings().one()
    assert stored["protected"] is True
    assert "结算结果" in stored["settlement_notes_json"]

    profile_rows = db.execute(
        text(
            """
            SELECT profile_scope, profile_key, sample_count, learned_score, confidence, feature_snapshot_json
            FROM catalyst_selection_feedback_profiles
            WHERE (profile_scope = 'symbol' AND profile_key = '600584.SH')
               OR (profile_scope = 'theme' AND profile_key = '半导体')
            ORDER BY profile_scope, profile_key
            """
        )
    ).mappings().all()
    assert len(profile_rows) == 2
    assert profile_rows[0]["sample_count"] >= 1
    assert profile_rows[1]["learned_score"] > 50
    assert "hit_rate" in profile_rows[1]["feature_snapshot_json"]


def test_settlement_feedback_profiles_reorder_next_generated_selection(db, monkeypatch):
    catalyst_selection_service.ensure_catalyst_selection_tables(db)
    theme_match = {
        "theme": "人工智能",
        "score": 88.0,
        "relation_score": 95.0,
        "catalyst": "AI基础设施政策支持",
        "summary": "政策支持人工智能基础设施。",
        "source_tier": "S",
        "evidence_count": 1,
        "event_semantic": {
            "event_type": "政策支持",
            "catalyst_strength": 86.0,
            "confidence": 0.9,
        },
    }
    symbols = [
        ("600584.SH", "强反馈标的", "strong_hit", 4.0, 82.0),
        ("002156.SZ", "弱反馈标的", "weak_miss", -4.0, 28.0),
    ]
    for index in range(6):
        trade_date = f"2026-05-{10 + index:02d}"
        settlement_date = f"2026-05-{11 + index:02d}"
        for symbol, name, outcome, change_pct, hit_score in symbols:
            db.execute(
                text(
                    """
                    INSERT INTO catalyst_selection_items (
                        run_id, trade_date, window_label, rank, symbol, name, score,
                        theme_matches_json, market_background, created_at, updated_at
                    )
                    VALUES (
                        :run_id, :trade_date, 'premarket', 1, :symbol, :name, 80,
                        :theme_matches_json, 'mock', NOW(), NOW()
                    )
                    """
                ),
                {
                    "run_id": f"feedback-{symbol}-{index}",
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "name": name,
                    "theme_matches_json": json.dumps([theme_match], ensure_ascii=False),
                },
            )
            db.execute(
                text(
                    """
                    INSERT INTO catalyst_selection_settlements (
                        trade_date, settlement_date, symbol, name, rank,
                        change_pct, hit_score, outcome, protected, settlement_notes_json, updated_at
                    )
                    VALUES (
                        :trade_date, :settlement_date, :symbol, :name, 1,
                        :change_pct, :hit_score, :outcome, TRUE, '[]', NOW()
                    )
                    """
                ),
                {
                    "trade_date": trade_date,
                    "settlement_date": settlement_date,
                    "symbol": symbol,
                    "name": name,
                    "change_pct": change_pct,
                    "hit_score": hit_score,
                    "outcome": outcome,
                },
            )
    db.commit()

    refresh = catalyst_selection_service._refresh_feedback_profiles_from_settlements(
        db,
        symbols=["600584.SH", "002156.SZ"],
        themes=["人工智能"],
        event_types=["政策支持"],
        now_value=datetime(2026, 5, 20, 9, 0, 0),
    )
    db.commit()

    assert refresh["symbol_profile_count"] == 2
    assert refresh["theme_profile_count"] == 1
    assert refresh["event_type_profile_count"] == 1
    assert refresh["new_profile_count"] == refresh["updated_profile_count"]
    assert refresh["changed_profile_count"] == refresh["updated_profile_count"]
    assert refresh["top_profile_changes"]
    assert any(change["profile_scope"] == "symbol" for change in refresh["top_profile_changes"])

    monkeypatch.setattr(
        news_theme_service,
        "list_theme_rankings",
        lambda *args, **kwargs: {
            "window": "premarket",
            "updated_at": "2026-05-27T09:00:00+08:00",
            "source": "cache:mock",
            "message": "mock",
            "items": [
                {
                    "theme": "人工智能",
                    "parent_theme": "科技",
                    "rank": 1,
                    "score": 88.0,
                    "message_count": 8,
                    "positive_count": 8,
                    "negative_count": 0,
                    "source_tier": "S",
                    "top_source_tier": "S",
                    "policy_boost": True,
                    "related_symbols": [
                        {"symbol": "600584.SH", "name": "强反馈标的"},
                        {"symbol": "002156.SZ", "name": "弱反馈标的"},
                    ],
                    "summary": "政策支持人工智能基础设施。",
                    "catalyst": "AI基础设施政策支持",
                    "market_confirmation": {"score": 8.0},
                    "evidence_items": [
                        {
                            "content": "国务院行动方案支持人工智能基础设施",
                            "source_tier": "S",
                            "published_at": "2026-05-27T08:55:00",
                        }
                    ],
                    "event_semantic": {
                        "event_type": "政策支持",
                        "catalyst_strength": 86.0,
                        "confidence": 0.9,
                    },
                    "semantic_source": "llm:mock/mock-model",
                }
            ],
            "data_governance": {"llm_core_stock": {"ready": True, "model": "mock-model"}},
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_market_snapshot",
        lambda *args, **kwargs: {
            "market_stats": {"total_amount": 2_000_000_000_000, "up_count": 3000, "down_count": 1800},
            "sector_gainers": [{"sector_name": "人工智能", "change_pct": 2.0, "amount": 180_000_000_000}],
            "sector_losers": [],
            "sector_inflows": [{"sector_name": "人工智能", "amount": 180_000_000_000}],
            "sector_outflows": [],
            "indices": [],
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "interpret_market_behavior",
        lambda market: {
            "narrative_anchors": ["流动性温和扩散", "人工智能主线确认"],
            "risk_pressure": {"label": "风险可控"},
            "data_quality": {"missing_fields": []},
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "_mainline_aligned_theme_items",
        lambda items, **kwargs: [
            {
                **item,
                "mainline_alignment_score": 72.0,
                "mainline_alignment_reasons": ["盘面强度确认：人工智能"],
            }
            for item in items
        ],
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "_load_daily_features",
        lambda *args, **kwargs: {
            "600584.SH": {
                "symbol": "600584.SH",
                "name": "强反馈标的",
                "industry": "人工智能",
                "sector": "计算机",
                "concepts": ["人工智能", "算力"],
                "open": 10.0,
                "high": 10.5,
                "low": 9.9,
                "close": 10.3,
                "amount": 100_000_000,
                "turnover_rate": 2.0,
                "change_pct": 2.0,
                "amount_ratio_20d": 1.4,
                "momentum_20d": 0.08,
                "momentum_60d": 0.12,
                "r60": 70.0,
                "net_profit_growth_proxy": 0.2,
            },
            "002156.SZ": {
                "symbol": "002156.SZ",
                "name": "弱反馈标的",
                "industry": "人工智能",
                "sector": "计算机",
                "concepts": ["人工智能", "算力"],
                "open": 10.0,
                "high": 10.5,
                "low": 9.9,
                "close": 10.3,
                "amount": 100_000_000,
                "turnover_rate": 2.0,
                "change_pct": 2.0,
                "amount_ratio_20d": 1.4,
                "momentum_20d": 0.08,
                "momentum_60d": 0.12,
                "r60": 70.0,
                "net_profit_growth_proxy": 0.2,
            },
        },
    )
    monkeypatch.setattr(catalyst_selection_service, "_load_previous_selection_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(catalyst_selection_service, "_load_symbol_settlement_stats", lambda *args, **kwargs: {})
    monkeypatch.setattr(catalyst_selection_service, "_load_theme_settlement_stats", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        catalyst_selection_service,
        "_attach_event_reaction_features",
        lambda session, *, features_by_symbol, **kwargs: {
            "enabled": True,
            "symbol_count": len(features_by_symbol),
            "covered_symbol_count": 0,
            "missing_count": len(features_by_symbol),
            "capture": {"requested": False},
        },
    )

    payload = catalyst_selection_service.generate_selections(
        db,
        trade_date="2026-05-27",
        window="premarket",
        limit=2,
    )

    assert [item["symbol"] for item in payload["items"]] == ["600584.SH", "002156.SZ"]
    strong, weak = payload["items"]
    assert strong["adaptive_feedback_score"] > weak["adaptive_feedback_score"]
    assert strong["score"] > weak["score"]
    assert strong["closed_loop_trace"]["feedback"]["symbol_profile"]["learned_score"] > 70
    assert weak["closed_loop_trace"]["feedback"]["symbol_profile"]["learned_score"] < 35
    assert strong["closed_loop_trace"]["scoring"]["learning_adjustment_policy"]["stance"] == "expand"
    assert weak["closed_loop_trace"]["scoring"]["learning_adjustment_policy"]["stance"] == "tighten"
    assert strong["closed_loop_trace"]["scoring"]["learning_impact"]["rank_before_learning_policy"] is not None
    assert weak["closed_loop_trace"]["scoring"]["learning_impact"]["rank_before_learning_policy"] is not None
    assert strong["closed_loop_trace"]["scoring"]["learning_impact"]["score_delta_from_learning_policy"] > 0
    assert weak["closed_loop_trace"]["scoring"]["learning_impact"]["score_delta_from_learning_policy"] < 0
    assert weak["risk_control"]["learning_adjustment"]["stance"] == "tighten"
    closed_loop = payload["data_governance"]["closed_loop"]
    assert closed_loop["feedback_profile_count"] == 4
    assert closed_loop["feedback_learning_state"]["selected_with_feedback_count"] == 2
    assert closed_loop["learning_adjustment_summary"]["expand_count"] == 1
    assert closed_loop["learning_adjustment_summary"]["tighten_count"] == 1
    assert closed_loop["learning_impact_summary"]["active_count"] == 2
    assert closed_loop["learning_impact_summary"]["average_score_delta"] is not None


def test_catalyst_selection_routes_smoke(monkeypatch):
    from api.main import app

    monkeypatch.setattr(
        catalyst_selection_service,
        "list_or_generate_selections",
        lambda *args, **kwargs: {
            "trade_date": "2026-05-26",
            "window": "premarket",
            "updated_at": "2026-05-26T09:00:00+08:00",
            "source": "mock",
            "message": "mock",
            "items": [],
            "market_background": "mock",
            "market_behavior_labels": {},
            "data_governance": {},
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "list_history",
        lambda *args, **kwargs: {"items": [], "updated_at": "2026-05-26T09:00:00+08:00"},
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "list_opportunity_events",
        lambda *args, **kwargs: {
            "items": [
                {
                    "event_id": "event-1",
                    "run_id": "run-1",
                    "trade_date": "2026-05-26",
                    "window": "premarket",
                    "symbol": "600584.SH",
                    "name": "长电科技",
                    "rank": 1,
                    "score": 80.0,
                    "event_level": "S",
                    "event_types": ["new_opportunity"],
                    "reasons": ["首次进入当前事件驱动机会榜"],
                    "trace": {},
                    "created_at": "2026-05-26T09:00:00+08:00",
                }
            ],
            "filters": {},
            "updated_at": "2026-05-26T09:00:00+08:00",
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "list_closed_loop_audits",
        lambda *args, **kwargs: {
            "items": [
                {
                    "audit_id": "audit-1",
                    "trade_date": "2026-05-26",
                    "trigger": "unit-test",
                    "status": "completed",
                    "requirement_summary": {"overall_status": "active", "active_count": 6},
                    "requirement_checks": [],
                    "end_to_end_evidence": {
                        "status": "active",
                        "active_window_count": 1,
                        "generated_window_count": 1,
                        "failed_window_count": 0,
                        "stage_rollup": {
                            "event_understanding": {"active": 1, "window_count": 1},
                        },
                    },
                    "requested_window_count": 1,
                    "generated_window_count": 1,
                    "failed_window_count": 0,
                    "total_selected_count": 1,
                    "opportunity_event_count": 1,
                    "risk_action_counts": {"observe": 1},
                    "risk_level_counts": {"medium": 1},
                    "feedback": {},
                    "monitor_activation": {},
                    "llm_ready_window_count": 1,
                    "settlement": {},
                    "generated": [],
                    "errors": [],
                    "created_at": "2026-05-26T09:00:00+08:00",
                    "updated_at": "2026-05-26T09:00:00+08:00",
                }
            ],
            "updated_at": "2026-05-26T09:00:00+08:00",
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "list_event_refresh_runs",
        lambda *args, **kwargs: {
            "items": [
                {
                    "refresh_key": "refresh-route-1",
                    "trigger": "qmt-minute-subscription:intraday",
                    "user_id": "route-user",
                    "trade_date": "2026-05-26",
                    "windows": ["24h"],
                    "limit": 10,
                    "reason": "minute_capture",
                    "context": {"capture_rows": 128},
                    "status": "completed",
                    "deduped": False,
                    "generated": [{"window": "24h", "item_count": 2}],
                    "errors": [],
                    "skipped": False,
                    "skip_reason": None,
                    "audit_id": "audit-refresh-route",
                    "duration_ms": 42,
                    "scheduled_at": "2026-05-26T09:00:00",
                    "started_at": "2026-05-26T09:00:01",
                    "finished_at": "2026-05-26T09:00:02",
                    "updated_at": "2026-05-26T09:00:02",
                }
            ],
            "filters": {"status": kwargs.get("status"), "trigger": kwargs.get("trigger"), "limit": kwargs.get("limit")},
            "updated_at": "2026-05-26T09:00:02",
        },
    )
    monkeypatch.setattr(
        catalyst_selection_service,
        "settle_selection",
        lambda *args, **kwargs: {
            "trade_date": "2026-05-26",
            "settlement_date": "2026-05-27",
            "items": [],
            "updated_at": "2026-05-26T09:00:00+08:00",
            "message": "ok",
        },
    )

    client = TestClient(app, raise_server_exceptions=False)
    email = f"catalyst-{uuid4().hex[:8]}@test.com"
    response = client.post("/v1/auth/request-code", json={"email": email})
    code = response.json()["dev_code"]
    verified = client.post("/v1/auth/verify-code", json={"email": email, "code": code})
    token = verified.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    list_response = client.get("/v1/catalyst-selection?trade_date=2026-05-26&window=premarket&limit=5", headers=headers)
    history_response = client.get("/v1/catalyst-selection/history?limit=5", headers=headers)
    opportunity_response = client.get("/v1/catalyst-selection/opportunity-events?trade_date=2026-05-26&window=premarket&limit=5", headers=headers)
    opportunity_alias_response = client.get("/v1/catalyst-selection/events?trade_date=2026-05-26&window=premarket&limit=5", headers=headers)
    audit_response = client.get("/v1/catalyst-selection/closed-loop-audits?trade_date=2026-05-26&limit=5", headers=headers)
    audit_alias_response = client.get("/v1/catalyst-selection/closed-loop/audits?trade_date=2026-05-26&limit=5", headers=headers)
    refresh_run_response = client.get(
        "/v1/catalyst-selection/event-refresh-runs?status=completed&limit=5",
        headers=headers,
    )
    settle_response = client.post("/v1/catalyst-selection/settle", headers=headers, json={"trade_date": "2026-05-26", "force": True})

    assert list_response.status_code == 200
    assert history_response.status_code == 200
    assert opportunity_response.status_code == 200
    assert opportunity_response.json()["items"][0]["event_types"] == ["new_opportunity"]
    assert opportunity_alias_response.status_code == 200
    assert opportunity_alias_response.json()["items"][0]["symbol"] == "600584.SH"
    assert audit_response.status_code == 200
    assert audit_response.json()["items"][0]["requirement_summary"]["overall_status"] == "active"
    assert audit_response.json()["items"][0]["end_to_end_evidence"]["status"] == "active"
    assert audit_response.json()["items"][0]["end_to_end_evidence"]["stage_rollup"]["event_understanding"]["active"] == 1
    assert audit_alias_response.status_code == 200
    assert audit_alias_response.json()["items"][0]["audit_id"] == "audit-1"
    assert refresh_run_response.status_code == 200
    assert refresh_run_response.json()["items"][0]["status"] == "completed"
    assert refresh_run_response.json()["items"][0]["audit_id"] == "audit-refresh-route"
    assert refresh_run_response.json()["items"][0]["context"]["capture_rows"] == 128
    assert settle_response.status_code == 200
