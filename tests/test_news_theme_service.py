from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import text

from tests.postgres_test_utils import isolated_postgres_session
from api.database import Base
from api.services import auth_service, news_eye_service, news_theme_service


@pytest.fixture
def db():
    with isolated_postgres_session(Base, schema_prefix="ta_news_theme") as session:
        yield session


def _seed_news(db, monkeypatch, items: list[dict]):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (items, ["测试资讯源"], []),
    )
    news_eye_service.refresh_news_cache(db, limit=max(len(items), 20), symbols=[], trigger="test")


def _ranking(db, *, now: datetime = datetime(2026, 5, 10, 10, 0, 0)):
    return news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=now,
    )["premarket"]


def test_policy_tier_can_beat_many_low_quality_reposts(db, monkeypatch):
    items = [
        {
            "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
            "published_at": "2026-05-09T09:00:00",
            "source": "国务院",
            "url": "https://example.com/policy",
        }
    ]
    items.extend(
        {
            "content": f"行业传闻称算力订单增长，算力板块走强，第{i}条转载",
            "published_at": f"2026-05-09T10:{i:02d}:00",
            "source": "行业小报",
            "url": f"https://example.com/repost-{i}",
        }
        for i in range(20)
    )
    _seed_news(db, monkeypatch, items)

    ranking = _ranking(db)

    assert ranking[0]["theme"] == "人工智能"
    assert ranking[0]["source_tier"] == "S"
    assert ranking[0]["policy_boost"] is True
    ai = next(item for item in ranking if item["theme"] == "人工智能")
    compute = next(item for item in ranking if item["theme"] == "算力")
    assert ai["score"] > compute["score"]


def test_theme_card_uses_dominant_tier_and_keeps_top_policy_tier(db, monkeypatch):
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ]
        + [
            {
                "content": f"财联社电报，人工智能应用订单增长，上市公司公告显示业务进展，第{i}条",
                "published_at": f"2026-05-10T09:{i + 1:02d}:00",
                "source": "财联社电报",
                "url": f"https://example.com/ai-a-{i}",
            }
            for i in range(3)
        ],
    )

    ai = next(item for item in _ranking(db) if item["theme"] == "人工智能")

    assert ai["source_tier"] == "A"
    assert ai["top_source_tier"] == "S"
    assert ai["policy_boost"] is True
    assert "主导来源层级A" in ai["summary"]
    assert "含S级政策催化" in ai["summary"]


def test_official_notice_tier_beats_fast_news_reposts(db, monkeypatch):
    items = [
        {
            "content": "公告｜中科曙光(603019.SH)｜日常经营｜关于签订算力中心建设合同的公告",
            "published_at": "2026-05-10T09:00:00",
            "source": "巨潮资讯公告",
            "url": "https://static.cninfo.com.cn/finalpage/notice.pdf",
            "seed_symbols": ["603019.SH"],
        }
    ]
    items.extend(
        {
            "content": f"市场快讯称人工智能方向走强，第{i}条",
            "published_at": f"2026-05-10T09:{i + 1:02d}:00",
            "source": "新浪7x24",
            "url": f"https://example.com/flash-{i}",
        }
        for i in range(6)
    )
    _seed_news(db, monkeypatch, items)

    ranking = _ranking(db)

    compute = next(item for item in ranking if item["theme"] == "算力")
    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert compute["source_tier"] == "S"
    assert compute["policy_boost"] is True
    assert compute["score"] > ai["score"]
    assert compute["evidence_items"][0]["source"] == "巨潮资讯公告"


def test_theme_aliases_are_normalized_to_standard_catalog(db, monkeypatch):
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "大模型应用订单增长，LLM 和人工智能模型方向走强",
                "published_at": "2026-05-09T09:00:00",
                "source": "财联社电报",
                "url": "https://example.com/ai",
            }
        ],
    )

    ranking = _ranking(db)

    assert [item["theme"] for item in ranking] == ["人工智能"]
    assert {"大模型", "LLM", "人工智能模型"} & set(ranking[0]["raw_tags"])


def test_compute_theme_stays_independent_from_parent_ai_theme(db, monkeypatch):
    items = [
        {
            "content": f"算力订单增长，数据中心和服务器需求走强，第{i}条",
            "published_at": f"2026-05-09T09:{i:02d}:00",
            "source": "财联社电报",
            "url": f"https://example.com/compute-{i}",
        }
        for i in range(10)
    ]
    items.extend(
        {
            "content": f"人工智能应用增长，大模型方向活跃，第{i}条",
            "published_at": f"2026-05-09T10:{i:02d}:00",
            "source": "财联社电报",
            "url": f"https://example.com/ai-{i}",
        }
        for i in range(5)
    )
    _seed_news(db, monkeypatch, items)

    ranking = _ranking(db)

    assert ranking[0]["theme"] == "算力"
    assert ranking[0]["parent_theme"] == "AI"
    assert next(item for item in ranking if item["theme"] == "人工智能")["parent_theme"] == "AI"


def test_research_provider_symbols_are_not_theme_recommendations(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "get_reverse_stock_map",
        lambda: {
            "601688.SH": "华泰证券",
            "300857.SZ": "协创数据",
            "300059.SZ": "东方财富",
        },
    )
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "【华泰证券维持英伟达买入评级】华泰证券表示，Agentic AI推动低时延推理芯片需求，人工智能基础设施景气提升。",
                "published_at": "2026-05-10T09:00:00",
                "source": "财联社电报",
                "url": "https://example.com/huatai-research",
            },
            {
                "content": "协创数据AI芯片订单增长，人工智能终端需求走强",
                "published_at": "2026-05-10T09:10:00",
                "source": "财联社电报",
                "url": "https://example.com/ai-stock",
            },
            {
                "content": "【东方财富财经早餐】人工智能与先进制造业深度融合，政策支持持续加码。",
                "published_at": "2026-05-10T09:20:00",
                "source": "东方财富财经早餐",
                "url": "https://example.com/eastmoney-breakfast",
            },
        ],
    )

    ai = next(item for item in _ranking(db) if item["theme"] == "人工智能")

    assert {"symbol": "300857.SZ", "name": "协创数据"} in ai["related_symbols"]
    assert all(item["name"] != "华泰证券" for item in ai["related_symbols"])
    assert all(item["name"] != "东方财富" for item in ai["related_symbols"])


def test_generic_theme_keyword_symbol_names_are_not_recommendations(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "get_reverse_stock_map",
        lambda: {"300024.SZ": "机器人"},
    )
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "人工智能政策推动智能机器人应用增长，相关行业销售收入提升",
                "published_at": "2026-05-10T09:00:00",
                "source": "财联社电报",
                "url": "https://example.com/generic-robot",
            }
        ],
    )

    ai = next(item for item in _ranking(db) if item["theme"] == "人工智能")

    assert all(item["name"] != "机器人" for item in ai["related_symbols"])


def test_theme_recommendations_keep_symbols_in_same_theme_positive_context(db):
    news_eye_service.ensure_news_tables(db)
    rows = [
        {
            "digest": "finance-noise".ljust(64, "0"),
            "dedupe_key": "finance-noise",
            "content": "财政部发布通知，推进金融支持实体经济和医保基金监管。永创智能公告称包装设备订单增长。",
            "published_at": "2026-05-10T09:00:00",
            "source": "财政部",
            "url": "https://example.com/finance-noise",
            "sentiment": "positive",
            "positive_sectors_json": json.dumps(["金融"], ensure_ascii=False),
            "negative_sectors_json": "[]",
            "positive_symbols_json": json.dumps([{"symbol": "603901.SH", "name": "永创智能"}], ensure_ascii=False),
            "negative_symbols_json": "[]",
            "related_symbols_json": json.dumps([{"symbol": "603901.SH", "name": "永创智能"}], ensure_ascii=False),
            "fetched_at": "2026-05-10T09:01:00",
        },
        {
            "digest": "finance-bank".ljust(64, "0"),
            "dedupe_key": "finance-bank",
            "content": "平安银行业绩增长，银行和金融科技服务改善。",
            "published_at": "2026-05-10T09:10:00",
            "source": "财联社电报",
            "url": "https://example.com/finance-bank",
            "sentiment": "positive",
            "positive_sectors_json": json.dumps(["银行"], ensure_ascii=False),
            "negative_sectors_json": "[]",
            "positive_symbols_json": json.dumps([{"symbol": "000001.SZ", "name": "平安银行"}], ensure_ascii=False),
            "negative_symbols_json": "[]",
            "related_symbols_json": json.dumps([{"symbol": "000001.SZ", "name": "平安银行"}], ensure_ascii=False),
            "fetched_at": "2026-05-10T09:11:00",
        },
        {
            "digest": "finance-street".ljust(64, "0"),
            "dedupe_key": "finance-street",
            "content": "金融街4月和5月至今销售签约金额较一季度提升。",
            "published_at": "2026-05-10T09:20:00",
            "source": "新浪7x24",
            "url": "https://example.com/finance-street",
            "sentiment": "positive",
            "positive_sectors_json": json.dumps(["金融"], ensure_ascii=False),
            "negative_sectors_json": "[]",
            "positive_symbols_json": json.dumps([{"symbol": "000402.SZ", "name": "金 融 街"}], ensure_ascii=False),
            "negative_symbols_json": "[]",
            "related_symbols_json": json.dumps([{"symbol": "000402.SZ", "name": "金 融 街"}], ensure_ascii=False),
            "fetched_at": "2026-05-10T09:21:00",
        },
        {
            "digest": "finance-research".ljust(64, "0"),
            "dedupe_key": "finance-research",
            "content": "中邮证券：维持永创智能增持评级。中邮证券研报指出，永创智能业绩增长。",
            "published_at": "2026-05-10T09:30:00",
            "source": "新浪7x24",
            "url": "https://example.com/finance-research",
            "sentiment": "positive",
            "positive_sectors_json": json.dumps(["证券"], ensure_ascii=False),
            "negative_sectors_json": "[]",
            "positive_symbols_json": json.dumps([{"symbol": "603901.SH", "name": "永创智能"}], ensure_ascii=False),
            "negative_symbols_json": "[]",
            "related_symbols_json": json.dumps([{"symbol": "603901.SH", "name": "永创智能"}], ensure_ascii=False),
            "fetched_at": "2026-05-10T09:31:00",
        },
    ]
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
        rows,
    )
    db.commit()

    finance = next(item for item in _ranking(db) if item["theme"] == "金融")

    assert {"symbol": "000001.SZ", "name": "平安银行"} in finance["related_symbols"]
    assert all(item["name"] != "永创智能" for item in finance["related_symbols"])
    assert all(item["name"] != "金 融 街" for item in finance["related_symbols"])


def test_llm_core_stock_suggestions_replace_text_extraction(db, monkeypatch):
    stock_map = {
        "603019.SH": "中科曙光",
        "601688.SH": "华泰证券",
        "600522.SH": "中天科技",
    }
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            },
            {
                "content": "人工智能公司减持，人工智能板块承压",
                "published_at": "2026-05-10T09:10:00",
                "source": "财联社电报",
                "url": "https://example.com/ai-risk",
            },
        ],
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)

    def fake_invoke(config, context):
        captured["context"] = context
        return {
            "items": [
                {
                    "theme": "人工智能",
                    "event_type": "政策支持",
                    "catalyst_strength": 86,
                    "beneficiary_chain": ["算力基础设施", "AI服务器"],
                    "invalidation_conditions": ["政策落地低于预期"],
                    "risk_signals": ["高位拥挤"],
                    "confidence": 0.82,
                    "reasoning": "国务院行动方案直接支持人工智能基础设施",
                    "symbols": [
                        {"symbol": "603019.SH", "name": "中科曙光", "reason": "算力基础设施核心标的"},
                        {"symbol": "601688.SH", "name": "华泰证券", "reason": "研报来源，应该被过滤"},
                        {"symbol": "600522.SH", "name": "中天科技", "reason": "光缆通信公司"},
                    ],
                }
            ]
        }

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fake_invoke)

    ai = next(item for item in news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        trigger_context={
            "source": "catalyst_selection_event_refresh",
            "trigger": "news-eye:manual",
            "refresh_key": "refresh-ai-1",
            "reason": "news_eye_fresh_events",
        },
    )["premarket"] if item["theme"] == "人工智能")

    assert ai["related_symbols"] == [{"symbol": "603019.SH", "name": "中科曙光"}]
    assert all(item["name"] != "中天科技" for item in ai["related_symbols"])
    assert {"symbol": "600522.SH", "name": "中天科技", "reason": "光缆通信公司"} in ai["llm_symbol_rejections"]
    assert ai["symbol_suggestion_source"] == "llm:mock/mock-model"
    assert ai["llm_symbol_trace"]["status"] == "invoked"
    assert ai["llm_symbol_trace"]["provider"] == "mock"
    assert ai["llm_symbol_trace"]["model"] == "mock-model"
    assert ai["llm_symbol_trace"]["suggested_theme_count"] == 1
    assert ai["llm_symbol_trace"]["semantic_theme_count"] == 1
    assert ai["llm_symbol_trace"]["cache_key"]
    assert ai["llm_symbol_trace"]["evidence_hash"]
    assert ai["llm_symbol_trace"]["trigger_context"]["refresh_key"] == "refresh-ai-1"
    assert ai["llm_symbol_trace"]["trigger_context"]["trigger"] == "news-eye:manual"
    assert ai["event_semantic"]["event_type"] == "政策支持"
    assert ai["event_semantic"]["catalyst_strength"] == 86.0
    assert ai["event_semantic"]["beneficiary_chain"] == ["算力基础设施", "AI服务器"]
    assert ai["event_semantic"]["invalidation_conditions"] == ["政策落地低于预期"]
    assert ai["event_semantic"]["risk_signals"] == ["高位拥挤"]
    assert ai["event_semantic"]["confidence"] == pytest.approx(0.82)
    assert ai["semantic_source"] == "llm:mock/mock-model"
    evidence_text = json.dumps(captured["context"], ensure_ascii=False)
    assert "专项支持" in evidence_text
    prompt_evidence_text = json.dumps(captured["context"]["themes"][0]["evidence"], ensure_ascii=False)
    assert "减持" not in prompt_evidence_text

    cache_row = db.execute(
        text(
            """
            SELECT event_semantics_json, trigger_context_json
            FROM market_news_theme_symbol_suggestions
            WHERE error IS NULL
            """
        )
    ).mappings().one()
    cached_semantics = json.loads(cache_row["event_semantics_json"])
    assert cached_semantics["人工智能"]["event_type"] == "政策支持"
    cached_trigger = json.loads(cache_row["trigger_context_json"])
    assert cached_trigger["refresh_key"] == "refresh-ai-1"
    assert cached_trigger["reason"] == "news_eye_fresh_events"

    snapshot = news_theme_service.list_theme_snapshots(
        db,
        snapshot_date="2026-05-10",
        window="premarket",
        limit=20,
    )
    snapshot_ai = next(item for item in snapshot["items"] if item["theme"] == "人工智能")
    assert snapshot_ai["event_semantic"]["event_type"] == "政策支持"
    assert snapshot_ai["semantic_source"] == "llm:mock/mock-model"


def test_llm_empty_core_stock_suggestion_clears_text_fallback(db, monkeypatch):
    stock_map = {
        "603019.SH": "中科曙光",
        "600522.SH": "中天科技",
    }
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，中天科技被市场归入人工智能相关概念",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            },
        ],
    )
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    monkeypatch.setattr(
        news_theme_service,
        "_invoke_core_stock_llm",
        lambda config, context: {
            "items": [
                {
                    "theme": "人工智能",
                    "event_type": "政策支持",
                    "beneficiary_chain": ["AI基础设施"],
                    "confidence": 0.71,
                    "symbols": [],
                }
            ]
        },
    )

    ai = next(item for item in news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
    )["premarket"] if item["theme"] == "人工智能")

    assert ai["related_symbols"] == []
    assert ai["symbol_suggestion_source"] == "llm:mock/mock-model:no_symbols"
    assert ai["semantic_source"] == "llm:mock/mock-model"


def test_llm_core_stock_prompt_includes_settlement_feedback(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    db.execute(
        text(
            """
            CREATE TABLE catalyst_selection_feedback_profiles (
                profile_scope VARCHAR(20) NOT NULL,
                profile_key VARCHAR(80) NOT NULL,
                model_version VARCHAR(40) NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                hit_count INTEGER NOT NULL DEFAULT 0,
                miss_count INTEGER NOT NULL DEFAULT 0,
                average_change_pct FLOAT,
                average_hit_score FLOAT,
                hit_rate FLOAT,
                learned_score FLOAT,
                confidence FLOAT,
                last_trade_date VARCHAR(10),
                last_settlement_date VARCHAR(10),
                feature_snapshot_json TEXT DEFAULT '{}',
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_feedback_profiles (
                profile_scope, profile_key, model_version, sample_count, hit_count, miss_count,
                average_change_pct, average_hit_score, hit_rate, learned_score, confidence,
                last_trade_date, last_settlement_date, feature_snapshot_json, updated_at
            )
            VALUES
                ('theme', '人工智能', 'settlement-feedback-v1', 6, 5, 1, 4.2, 78.0, 83.3, 74.0, 0.72, '2026-05-27', '2026-05-28', '{"source":"theme"}', '2026-05-29T10:00:00'),
                ('event_type', '政策支持', 'settlement-feedback-v1', 4, 3, 1, 3.1, 71.0, 75.0, 68.0, 0.61, '2026-05-27', '2026-05-28', '{"source":"event"}', '2026-05-29T10:00:00')
            """
        )
    )
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy-feedback",
            }
        ],
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)

    def fake_invoke(config, context):
        captured["context"] = context
        return {"items": [{"theme": "人工智能", "symbols": [{"symbol": "603019.SH", "name": "中科曙光"}]}]}

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fake_invoke)

    news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="feedback-user",
    )

    theme_context = captured["context"]["themes"][0]
    feedback = theme_context["historical_feedback"]
    assert theme_context["event_semantic"]["event_type"] == "政策支持"
    assert feedback["theme_profile"]["learned_score"] == 74.0
    assert feedback["theme_profile"]["instruction"] == "prioritize_if_current_evidence_confirms"
    assert feedback["theme_profile"]["feature_snapshot"]["source"] == "theme"
    assert feedback["event_type_profile"]["profile_key"] == "政策支持"
    assert feedback["event_type_profile"]["hit_rate"] == 75.0


def test_llm_core_stock_feedback_profile_changes_cache_key(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_enabled", lambda: False)
    db.execute(
        text(
            """
            CREATE TABLE catalyst_selection_feedback_profiles (
                profile_scope VARCHAR(20) NOT NULL,
                profile_key VARCHAR(80) NOT NULL,
                model_version VARCHAR(40) NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                hit_count INTEGER NOT NULL DEFAULT 0,
                miss_count INTEGER NOT NULL DEFAULT 0,
                average_change_pct FLOAT,
                average_hit_score FLOAT,
                hit_rate FLOAT,
                learned_score FLOAT,
                confidence FLOAT,
                last_trade_date VARCHAR(10),
                last_settlement_date VARCHAR(10),
                feature_snapshot_json TEXT DEFAULT '{}',
                updated_at TIMESTAMP NOT NULL
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO catalyst_selection_feedback_profiles (
                profile_scope, profile_key, model_version, sample_count, hit_count, miss_count,
                average_change_pct, average_hit_score, hit_rate, learned_score, confidence,
                last_trade_date, last_settlement_date, feature_snapshot_json, updated_at
            )
            VALUES (
                'theme', '人工智能', 'settlement-feedback-v1', 4, 1, 3,
                -2.4, 38.0, 25.0, 32.0, 0.44,
                '2026-05-27', '2026-05-28', '{"regime":"weak"}', '2026-05-29T10:00:00'
            )
            """
        )
    )
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy-feedback-cache",
            }
        ],
    )
    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=False,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id=None,
    )["premarket"]
    prompt_before = news_theme_service._build_core_stock_prompt_items(db, ranking)
    hash_before = news_theme_service._make_core_stock_evidence_hash(prompt_before)
    cache_before = news_theme_service._make_core_stock_cache_key("premarket", hash_before, "same-config")

    db.execute(
        text(
            """
            UPDATE catalyst_selection_feedback_profiles
            SET sample_count = 9,
                hit_count = 8,
                miss_count = 1,
                average_change_pct = 5.6,
                average_hit_score = 84.0,
                hit_rate = 88.9,
                learned_score = 81.0,
                confidence = 0.83,
                feature_snapshot_json = '{"regime":"strong"}',
                updated_at = '2026-05-30T10:00:00'
            WHERE profile_scope = 'theme'
              AND profile_key = '人工智能'
              AND model_version = 'settlement-feedback-v1'
            """
        )
    )
    prompt_after = news_theme_service._build_core_stock_prompt_items(db, ranking)
    hash_after = news_theme_service._make_core_stock_evidence_hash(prompt_after)
    cache_after = news_theme_service._make_core_stock_cache_key("premarket", hash_after, "same-config")

    feedback_before = prompt_before[0]["historical_feedback"]["theme_profile"]
    feedback_after = prompt_after[0]["historical_feedback"]["theme_profile"]
    assert feedback_before["instruction"] == "deprioritize_or_require_stronger_confirmation"
    assert feedback_after["instruction"] == "prioritize_if_current_evidence_confirms"
    assert feedback_before["feature_snapshot"]["regime"] == "weak"
    assert feedback_after["feature_snapshot"]["regime"] == "strong"
    assert hash_before != hash_after
    assert cache_before != cache_after


def test_theme_ranking_has_heuristic_event_semantic_without_llm(db, monkeypatch):
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_enabled", lambda: False)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    ai = next(item for item in _ranking(db) if item["theme"] == "人工智能")

    assert ai["event_semantic"]["event_type"] == "政策支持"
    assert ai["event_semantic"]["catalyst_strength"] > 50
    assert ai["event_semantic"]["invalidation_conditions"] == ["政策落地节奏低于预期"]
    assert ai["semantic_source"] == "heuristic:event_rules"


def test_llm_core_stock_suggestions_can_refresh_async_without_blocking_page(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: False)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 0)

    started: dict[str, object] = {}

    def fake_queue(**kwargs):
        started["window"] = kwargs["window"]
        started["evidence_hash"] = kwargs["evidence_hash"]
        started["prompt_items"] = kwargs["prompt_items"]
        started["config"] = kwargs["config"]
        started["trigger_context"] = kwargs.get("trigger_context") or {}
        return True

    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", fake_queue)

    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        trigger_context={"source": "news_eye", "trigger": "manual-refresh", "fresh_event_count": 1},
    )["premarket"]

    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert ai["symbol_suggestion_source"] == "fallback:positive_news"
    assert isinstance(ai["related_symbols"], list)
    assert started["window"] == "premarket"
    assert started["prompt_items"]
    assert started["trigger_context"]["source"] == "news_eye"
    assert started["trigger_context"]["trigger"] == "manual-refresh"


def test_allow_async_llm_queues_even_when_sync_mode_enabled(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 0)
    invoked: list[str] = []
    queued: dict[str, object] = {}

    def fail_if_invoked(config, context):
        invoked.append(str(context["window"]))
        raise AssertionError("page async request must not invoke LLM synchronously")

    def fake_queue(**kwargs):
        queued.update(kwargs)
        return True

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fail_if_invoked)
    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", fake_queue)

    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        allow_async_llm=True,
    )["premarket"]

    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert invoked == []
    assert queued["window"] == "premarket"
    assert ai["llm_symbol_trace"]["status"] == "queued"
    assert ai["symbol_suggestion_source"] == "fallback:positive_news"


def test_llm_core_stock_async_wait_uses_cache_when_ready(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: False)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 1)

    def fake_queue(**kwargs):
        news_theme_service._store_core_stock_suggestion_cache(
            db,
            cache_key=kwargs["cache_key"],
            window=kwargs["window"],
            evidence_hash=kwargs["evidence_hash"],
            provider="mock",
            model="mock-model",
            suggestions={"人工智能": [{"symbol": "603019.SH", "name": "中科曙光", "reason": "AI算力核心受益"}]},
            event_semantics={"人工智能": {"event_type": "政策支持", "catalyst_strength": 80, "confidence": 0.8}},
            trigger_context=kwargs.get("trigger_context") or {},
            error=None,
        )
        return True

    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", fake_queue)

    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
    )["premarket"]

    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert ai["symbol_suggestion_source"] == "llm:cache"
    assert ai["related_symbols"] == [{"symbol": "603019.SH", "name": "中科曙光"}]
    assert ai["semantic_source"] == "llm:cache"
    assert ai["llm_symbol_trace"]["status"] == "cache_hit_after_async_wait"


def test_llm_core_stock_async_poll_reuses_recent_success_for_same_runtime(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    config = {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": "same-key"}
    monkeypatch.setattr(news_theme_service, "_resolve_core_stock_llm_config", lambda db, user_id: config)
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: False)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 0)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_RECENT_SUCCESS_TTL_SECONDS", 1800)
    config_hash = news_theme_service._make_core_stock_config_hash(config)
    recent_cache_key = news_theme_service._make_core_stock_cache_key("premarket", "previous-evidence", config_hash)
    news_theme_service.ensure_theme_tables(db)
    news_theme_service._store_core_stock_suggestion_cache(
        db,
        cache_key=recent_cache_key,
        window="premarket",
        evidence_hash="previous-evidence",
        config_hash=config_hash,
        provider="mock",
        model="mock-model",
        suggestions={"人工智能": [{"symbol": "603019.SH", "name": "中科曙光", "reason": "AI算力核心受益"}]},
        event_semantics={"人工智能": {"event_type": "政策支持", "catalyst_strength": 82, "confidence": 0.8}},
        trigger_context={"refresh_key": "previous-refresh"},
        error=None,
    )
    queued: dict[str, object] = {}

    def fake_queue(**kwargs):
        queued.update(kwargs)
        return True

    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", fake_queue)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持，新增算力基础设施细则。",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy-recent-cache",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        trigger_context={"refresh_key": "current-refresh", "trigger": "poll"},
    )["premarket"]

    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert queued["cache_key"] != recent_cache_key
    assert ai["related_symbols"] == [{"symbol": "603019.SH", "name": "中科曙光"}]
    assert ai["symbol_suggestion_source"] == "llm:recent_cache"
    assert ai["semantic_source"] == "llm:recent_cache"
    assert ai["llm_symbol_trace"]["status"] == "recent_cache_hit_refresh_queued"
    assert ai["llm_symbol_trace"]["recent_cache_key"] == recent_cache_key
    assert ai["llm_symbol_trace"]["recent_evidence_hash"] == "previous-evidence"
    assert ai["llm_symbol_trace"]["queued_refresh"] is True
    assert ai["llm_symbol_trace"]["trigger_context"]["refresh_key"] == "previous-refresh"


def test_llm_recent_success_cache_is_scoped_by_runtime_config(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    old_config = {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": "same-key"}
    new_config = {"provider": "mock", "model": "other-model", "base_url": None, "api_key": "same-key"}
    old_config_hash = news_theme_service._make_core_stock_config_hash(old_config)
    news_theme_service.ensure_theme_tables(db)
    news_theme_service._store_core_stock_suggestion_cache(
        db,
        cache_key=news_theme_service._make_core_stock_cache_key("premarket", "previous-evidence", old_config_hash),
        window="premarket",
        evidence_hash="previous-evidence",
        config_hash=old_config_hash,
        provider="mock",
        model="mock-model",
        suggestions={"人工智能": [{"symbol": "603019.SH", "name": "中科曙光", "reason": "AI算力核心受益"}]},
        event_semantics={"人工智能": {"event_type": "政策支持", "catalyst_strength": 82, "confidence": 0.8}},
        trigger_context={"refresh_key": "previous-refresh"},
        error=None,
    )
    monkeypatch.setattr(news_theme_service, "_resolve_core_stock_llm_config", lambda db, user_id: new_config)
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: False)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 0)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_RECENT_SUCCESS_TTL_SECONDS", 1800)
    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", lambda **kwargs: False)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持，新增算力基础设施细则。",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy-recent-cache-scope",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
    )["premarket"]

    ai = next(item for item in ranking if item["theme"] == "人工智能")
    assert ai["symbol_suggestion_source"] == "fallback:positive_news"
    assert ai["llm_symbol_trace"]["status"] == "queue_skipped"
    assert "recent_cache_key" not in ai["llm_symbol_trace"]


def test_background_theme_refresh_without_user_queues_system_llm(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    resolved: dict[str, object] = {}

    def fake_resolve(db, user_id):
        resolved["user_id"] = user_id
        return {
            "provider": "openai",
            "model": "astron-code-latest",
            "base_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "api_key": "system-key",
        }

    monkeypatch.setattr(news_theme_service, "_resolve_core_stock_llm_config", fake_resolve)
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: False)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ASYNC_WAIT_SECONDS", 0)
    started: dict[str, object] = {}

    def fake_queue(**kwargs):
        started.update(kwargs)
        return True

    monkeypatch.setattr(news_theme_service, "_queue_core_stock_suggestion_refresh", fake_queue)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    ranking = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id=None,
        allow_async_llm=True,
    )["premarket"]

    assert resolved["user_id"] is None
    assert started["config"]["base_url"] == "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2"
    assert started["window"] == "premarket"
    assert started["trigger_context"] == {}
    assert next(item for item in ranking if item["theme"] == "人工智能")["symbol_suggestion_source"] == "fallback:positive_news"


def test_error_cache_temporarily_hides_repeated_llm_failures(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", lambda config, context: (_ for _ in ()).throw(TimeoutError("boom")))
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ERROR_CACHE_TTL_SECONDS", 600)

    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    first = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
    )["premarket"]
    ai_first = next(item for item in first if item["theme"] == "人工智能")
    assert ai_first["symbol_suggestion_source"] == "fallback:positive_news"
    assert ai_first["llm_symbol_trace"]["status"] == "failed"
    assert ai_first["llm_symbol_trace"]["error"] == "boom"

    calls: list[str] = []

    def fail_if_called(config, context):
        calls.append("called")
        raise AssertionError("LLM should not be called again within error cache ttl")

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fail_if_called)
    second = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 30),
        user_id="user-1",
    )["premarket"]

    ai_second = next(item for item in second if item["theme"] == "人工智能")
    assert ai_second["symbol_suggestion_source"] == "fallback:positive_news"
    assert ai_second["llm_symbol_trace"]["status"] == "cache_hit"
    assert ai_second["llm_symbol_trace"]["cache_has_error"] is True
    assert ai_second["llm_symbol_trace"]["cache_updated_at"]
    assert not calls


def test_force_sync_llm_bypasses_recent_error_cache(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", lambda config, context: (_ for _ in ()).throw(TimeoutError("boom")))
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ERROR_CACHE_TTL_SECONDS", 600)

    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )
    news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
    )

    calls: list[str] = []

    def successful_retry(config, context):
        calls.append("called")
        return {
            "items": [
                {
                    "theme": "人工智能",
                    "symbols": [{"symbol": "603019.SH", "name": "中科曙光"}],
                }
            ]
        }

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", successful_retry)
    recovered = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 30),
        user_id="user-1",
        force_sync_llm=True,
    )["premarket"]

    ai = next(item for item in recovered if item["theme"] == "人工智能")
    assert calls == ["called"]
    assert ai["related_symbols"] == [{"symbol": "603019.SH", "name": "中科曙光"}]
    assert ai["symbol_suggestion_source"] == "llm:mock/mock-model"


def test_force_sync_llm_bypasses_success_cache_and_updates_trigger_context(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(
        news_theme_service,
        "_resolve_core_stock_llm_config",
        lambda db, user_id: {"provider": "mock", "model": "mock-model", "base_url": None, "api_key": None},
    )
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    calls: list[str] = []

    def fake_invoke(config, context):
        calls.append(str(context["window"]))
        return {
            "items": [
                {
                    "theme": "人工智能",
                    "symbols": [{"symbol": "603019.SH", "name": "中科曙光", "reason": "算力核心标的"}],
                }
            ]
        }

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fake_invoke)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        force_sync_llm=True,
        trigger_context={"refresh_key": "refresh-old", "trigger": "old"},
    )
    second = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="user-1",
        force_sync_llm=True,
        trigger_context={"refresh_key": "refresh-new", "trigger": "new"},
    )["premarket"]

    ai = next(item for item in second if item["theme"] == "人工智能")
    assert calls == ["premarket", "premarket"]
    assert ai["llm_symbol_trace"]["status"] == "invoked"
    assert ai["llm_symbol_trace"]["trigger_context"]["refresh_key"] == "refresh-new"
    cache_row = db.execute(
        text("SELECT trigger_context_json FROM market_news_theme_symbol_suggestions WHERE error IS NULL")
    ).mappings().one()
    assert json.loads(cache_row["trigger_context_json"])["refresh_key"] == "refresh-new"


def test_llm_error_cache_is_scoped_by_model_config(db, monkeypatch):
    stock_map = {"603019.SH": "中科曙光"}
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: stock_map)
    monkeypatch.setattr(news_theme_service, "get_reverse_stock_map", lambda: stock_map)
    configs = {
        "bad-user": {"provider": "openai", "model": "gpt-4o-mini", "base_url": "https://api.openai.com/v1", "api_key": None},
        "good-user": {"provider": "openai", "model": "astron-code-latest", "base_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2", "api_key": "key"},
    }
    monkeypatch.setattr(news_theme_service, "_resolve_core_stock_llm_config", lambda db, user_id: configs[user_id])
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_sync_enabled", lambda: True)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_ERROR_CACHE_TTL_SECONDS", 600)
    calls: list[str] = []

    def fake_invoke(config, context):
        calls.append(str(config["model"]))
        if config["model"] == "gpt-4o-mini":
            raise TimeoutError("bad config")
        return {"items": [{"theme": "人工智能", "symbols": [{"symbol": "603019.SH", "name": "中科曙光"}]}]}

    monkeypatch.setattr(news_theme_service, "_invoke_core_stock_llm", fake_invoke)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    failed = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 0),
        user_id="bad-user",
    )["premarket"]
    assert next(item for item in failed if item["theme"] == "人工智能")["symbol_suggestion_source"] == "fallback:positive_news"

    recovered = news_theme_service.refresh_theme_rankings(
        db,
        windows=("premarket",),
        limit=20,
        persist=True,
        now=datetime(2026, 5, 10, 10, 0, 30),
        user_id="good-user",
    )["premarket"]
    ai = next(item for item in recovered if item["theme"] == "人工智能")
    assert ai["related_symbols"] == [{"symbol": "603019.SH", "name": "中科曙光"}]
    assert ai["symbol_suggestion_source"] == "llm:openai/astron-code-latest"
    assert calls == ["gpt-4o-mini", "astron-code-latest"]


def test_news_llm_uses_complete_news_runtime_config_when_available(db, monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    user_id = "complete-news-config-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        llm_provider="openai",
        backend_url="https://main.example/v1",
        quick_think_llm="main-quick",
        deep_think_llm="main-deep",
        api_key="main-key",
        news_llm_provider="openai",
        news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        news_analysis_llm="deepseek-v4-flash",
        news_api_key="volcengine-news-key",
    )

    theme_config = news_theme_service._resolve_core_stock_llm_config(db, user_id=user_id)
    assert theme_config is not None
    assert {
        "provider": theme_config["provider"],
        "model": theme_config["model"],
        "base_url": theme_config["base_url"],
        "api_key": theme_config["api_key"],
        "runtime_package_source": theme_config["runtime_package_source"],
        "api_key_source": theme_config["api_key_source"],
        "provider_source": theme_config["provider_source"],
        "base_url_source": theme_config["base_url_source"],
        "model_source": theme_config["model_source"],
    } == {
        "provider": "openai",
        "model": "deepseek-v4-flash",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
        "api_key": "volcengine-news-key",
        "runtime_package_source": "user_news_config",
        "api_key_source": "user_news_config",
        "provider_source": "user_news_config",
        "base_url_source": "user_news_config",
        "model_source": "user_news_config",
    }

    news_config = news_eye_service._resolve_news_llm_config(db, user_id=user_id)
    assert news_config["llm_provider"] == "openai"
    assert news_config["backend_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert news_config["quick_think_llm"] == "deepseek-v4-flash"
    assert news_config["deep_think_llm"] == "deepseek-v4-flash"
    assert news_config["api_key"] == "volcengine-news-key"
    assert news_config["_api_key_source"] == "user_news_config"


def test_core_stock_llm_readiness_reports_complete_volcengine_runtime_set(db, monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    user_id = "complete-volcengine-config-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        llm_provider="volcengine-ark",
        backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        quick_think_llm="deepseek-v4-flash",
        deep_think_llm="deepseek-v4-pro",
        api_key="volcengine-main-key",
        news_llm_provider="volcengine-ark",
        news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        news_analysis_llm="deepseek-v4-flash",
        news_api_key="volcengine-news-key",
    )

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id=user_id)
    theme_config = news_theme_service._resolve_core_stock_llm_config(db, user_id=user_id)

    assert readiness["ready"] is True
    assert readiness["provider"] == "volcengine-ark"
    assert readiness["model"] == "deepseek-v4-flash"
    assert readiness["base_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert readiness["runtime_package_source"] == "user_news_config"
    assert readiness["provider_source"] == "user_news_config"
    assert readiness["api_key_source"] == "user_news_config"
    assert readiness["base_url_source"] == "user_news_config"
    assert readiness["model_source"] == "user_news_config"
    assert readiness["mixed_account_runtime"] is False
    assert theme_config is not None
    assert {
        "provider": theme_config["provider"],
        "model": theme_config["model"],
        "base_url": theme_config["base_url"],
        "api_key": theme_config["api_key"],
        "runtime_package_source": theme_config["runtime_package_source"],
        "api_key_source": theme_config["api_key_source"],
        "provider_source": theme_config["provider_source"],
        "base_url_source": theme_config["base_url_source"],
        "model_source": theme_config["model_source"],
    } == {
        "provider": "volcengine-ark",
        "model": "deepseek-v4-flash",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
        "api_key": "volcengine-news-key",
        "runtime_package_source": "user_news_config",
        "api_key_source": "user_news_config",
        "provider_source": "user_news_config",
        "base_url_source": "user_news_config",
        "model_source": "user_news_config",
    }


def test_core_stock_llm_cache_hash_includes_runtime_package_source():
    base_config = {
        "provider": "volcengine-ark",
        "model": "deepseek-v4-flash",
        "base_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
        "api_key": "same-volcengine-key",
    }
    main_runtime_hash = news_theme_service._make_core_stock_config_hash(
        {
            **base_config,
            "runtime_package_source": "user_config",
            "api_key_source": "user_config",
            "provider_source": "user_config",
            "base_url_source": "user_config",
            "model_source": "user_config",
        }
    )
    news_runtime_hash = news_theme_service._make_core_stock_config_hash(
        {
            **base_config,
            "runtime_package_source": "user_news_config",
            "api_key_source": "user_news_config",
            "provider_source": "user_news_config",
            "base_url_source": "user_news_config",
            "model_source": "user_news_config",
        }
    )

    assert main_runtime_hash != news_runtime_hash


def test_news_llm_skips_incomplete_news_runtime_config(db, monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    user_id = "incomplete-news-config-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        llm_provider="openai",
        backend_url="https://main.example/v1",
        quick_think_llm="main-quick",
        deep_think_llm="main-deep",
        api_key="main-key",
        news_llm_provider="openai",
        news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
        news_analysis_llm="deepseek-v4-flash",
    )

    theme_config = news_theme_service._resolve_core_stock_llm_config(db, user_id=user_id)
    assert theme_config is not None
    assert {
        "provider": theme_config["provider"],
        "model": theme_config["model"],
        "base_url": theme_config["base_url"],
        "api_key": theme_config["api_key"],
        "runtime_package_source": theme_config["runtime_package_source"],
    } == {
        "provider": "openai",
        "model": "main-quick",
        "base_url": "https://main.example/v1",
        "api_key": "main-key",
        "runtime_package_source": "user_config",
    }

    news_config = news_eye_service._resolve_news_llm_config(db, user_id=user_id)
    assert news_config["backend_url"] == "https://main.example/v1"
    assert news_config["quick_think_llm"] == "main-quick"
    assert news_config["api_key"] == "main-key"
    assert news_config["_news_llm_runtime_skipped"] == "incomplete_user_news_config"


def test_core_stock_llm_rejects_account_key_mixed_with_default_endpoint(db, monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    for key in (
        "TA_API_KEY",
        "MAAS_API_KEY",
        "XFYUN_API_KEY",
        "XF_YUN_API_KEY",
        "XF_API_KEY",
        "IFLYTEK_API_KEY",
        "OPENAI_API_KEY",
        "TA_BASE_URL",
        "MAAS_BASE_URL",
        "XFYUN_BASE_URL",
        "XF_YUN_BASE_URL",
        "OPENAI_BASE_URL",
        "OPENAI_API_BASE",
        "TA_LLM_PROVIDER",
        "TA_LLM_QUICK",
        "TA_LLM_DEEP",
        "TA_LLM_MODEL",
    ):
        monkeypatch.delenv(key, raising=False)
    user_id = "mixed-account-key-user"
    auth_service.upsert_user_llm_config(db, user_id, api_key="volcengine-key-only")

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id=user_id)

    assert readiness["ready"] is False
    assert readiness["status"] == "mixed_runtime_rejected"
    assert readiness["runtime_package_source"] == "mixed_runtime"
    assert readiness["api_key_source"] == "user_config"
    assert readiness["mixed_account_runtime"] is True
    assert readiness["account_runtime_sources"] == ["user_config"]
    assert news_theme_service._resolve_core_stock_llm_config(db, user_id=user_id) is None


def test_core_stock_llm_config_rejects_local_model(db, monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    user_id = "local-model-user"
    auth_service.upsert_user_llm_config(
        db,
        user_id,
        llm_provider="ollama",
        backend_url="http://127.0.0.1:11434/v1",
        quick_think_llm="local-qwen",
        api_key="local-key",
    )

    assert news_theme_service._resolve_core_stock_llm_config(db, user_id=user_id) is None
    assert news_theme_service._is_local_llm_config(provider="openai", base_url="http://localhost:11434/v1") is True


def test_core_stock_llm_readiness_reports_missing_api_key(db, monkeypatch):
    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "astron-code-latest",
            "deep_think_llm": "astron-code-latest",
            "api_key": "",
        }

    monkeypatch.setattr(news_theme_service, "build_news_runtime_config", lambda user_id=None, db=None: fake_build_runtime_config({}, user_id=user_id, db=db))

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id="missing-key-user")

    assert readiness["ready"] is False
    assert readiness["status"] == "missing_api_key"
    assert readiness["provider"] == "openai"
    assert readiness["model"] == "astron-code-latest"
    assert readiness["requires_api_key"] is True
    assert readiness["has_api_key"] is False


def test_core_stock_llm_readiness_accepts_maas_api_key_alias(db, monkeypatch):
    for key in (
        "TA_API_KEY",
        "MAAS_API_KEY",
        "XFYUN_API_KEY",
        "XF_YUN_API_KEY",
        "XF_API_KEY",
        "IFLYTEK_API_KEY",
        "OPENAI_API_KEY",
        "TA_BASE_URL",
        "MAAS_BASE_URL",
        "XFYUN_BASE_URL",
        "XF_YUN_BASE_URL",
        "OPENAI_BASE_URL",
        "OPENAI_API_BASE",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MAAS_API_KEY", "maas-key")
    monkeypatch.setenv("MAAS_BASE_URL", "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2")

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id=None)

    assert readiness["ready"] is True
    assert readiness["status"] == "ready"
    assert readiness["has_api_key"] is True
    assert readiness["api_key_source"] == "MAAS_API_KEY"
    assert readiness["base_url_source"] == "MAAS_BASE_URL"
    assert readiness["base_url"] == "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2"


def test_core_stock_llm_readiness_reports_recent_auth_failed(db, monkeypatch):
    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "astron-code-latest",
            "deep_think_llm": "astron-code-latest",
            "api_key": "real-user-key",
        }

    monkeypatch.setattr(news_theme_service, "build_news_runtime_config", lambda user_id=None, db=None: fake_build_runtime_config({}, user_id=user_id, db=db))
    news_theme_service.ensure_theme_tables(db)
    now_value = news_theme_service._now_cn_naive()
    db.execute(
        text(
            """
            INSERT INTO market_news_theme_symbol_suggestions (
                cache_key, window_label, evidence_hash, provider, model, suggestions_json,
                event_semantics_json, error, created_at, updated_at
            ) VALUES (
                'cache-auth-failed', '24h', 'hash-auth-failed', 'openai', 'astron-code-latest', '{}',
                '{}', 'Error code: 401 - {\"message\": \"HMAC signature cannot be verified: apikey not found\"}',
                :now_value, :now_value
            )
            """
        ),
        {"now_value": now_value},
    )
    db.commit()

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id="auth-failed-user")

    assert readiness["ready"] is False
    assert readiness["status"] == "auth_failed"
    assert "Key" in readiness["reason"]
    assert "apikey not found" in readiness["last_error"]


def test_core_stock_llm_readiness_reports_local_rejected(db, monkeypatch):
    def fake_build_runtime_config(overrides, user_id=None, db=None):
        return {
            "llm_provider": "openai",
            "backend_url": "http://127.0.0.1:1234/v1",
            "quick_think_llm": "local-model",
            "deep_think_llm": "local-model",
            "api_key": "local-key",
        }

    monkeypatch.setattr(news_theme_service, "build_news_runtime_config", lambda user_id=None, db=None: fake_build_runtime_config({}, user_id=user_id, db=db))

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id="local-user")

    assert readiness["ready"] is False
    assert readiness["status"] == "local_rejected"
    assert readiness["base_url"] == "http://127.0.0.1:1234/v1"


def test_list_theme_rankings_exposes_llm_governance(db, monkeypatch):
    monkeypatch.setattr(news_theme_service, "_llm_symbol_suggestions_enabled", lambda: False)
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "国务院印发人工智能行动方案，人工智能产业获得专项支持",
                "published_at": "2026-05-10T09:00:00",
                "source": "国务院",
                "url": "https://example.com/ai-policy",
            }
        ],
    )

    payload = news_theme_service.list_theme_rankings(
        db,
        window="premarket",
        limit=20,
        include_evidence=True,
        user_id="governance-user",
        now=datetime(2026, 5, 10, 10, 0, 0),
    )

    llm_governance = payload["data_governance"]["llm_core_stock"]
    assert llm_governance["enabled"] is False
    assert llm_governance["status"] == "disabled"
    assert llm_governance["used_symbol_theme_count"] == 0
    assert llm_governance["used_semantic_theme_count"] == 0
    assert llm_governance["symbol_source_counts"]["fallback:positive_news"] >= 1
    assert llm_governance["semantic_source_counts"]["heuristic:event_rules"] >= 1
    ai = next(item for item in payload["items"] if item["theme"] == "人工智能")
    assert ai["llm_symbol_trace"]["status"] == "disabled"
    assert ai["llm_symbol_trace"]["reason"] == "NEWS_THEME_LLM_SYMBOLS disabled"


def test_llm_core_stock_background_queue_is_single_flight(monkeypatch):
    class FakeThread:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def start(self):
            return None

    monkeypatch.setattr(news_theme_service.threading, "Thread", FakeThread)
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_GLOBAL_ERROR_COOLDOWN_SECONDS", 300)
    news_theme_service._CORE_STOCK_SUGGESTION_TASKS.clear()
    news_theme_service._CORE_STOCK_LAST_FAILURE_AT.clear()
    payload = {
        "config": {"provider": "mock", "model": "mock-model"},
        "window": "premarket",
        "evidence_hash": "hash",
        "window_start": datetime(2026, 5, 10, 9, 0, 0),
        "window_end": datetime(2026, 5, 10, 10, 0, 0),
        "prompt_items": [{"theme": "人工智能", "evidence": [{"content": "政策支持"}]}],
    }
    try:
        assert news_theme_service._queue_core_stock_suggestion_refresh(cache_key="cache-1", **payload) is True
        assert news_theme_service._queue_core_stock_suggestion_refresh(cache_key="cache-2", **payload) is False
    finally:
        news_theme_service._CORE_STOCK_SUGGESTION_TASKS.clear()
        news_theme_service._CORE_STOCK_LAST_FAILURE_AT.clear()


def test_llm_core_stock_background_queue_respects_failure_cooldown(monkeypatch):
    news_theme_service._CORE_STOCK_SUGGESTION_TASKS.clear()
    news_theme_service._CORE_STOCK_LAST_FAILURE_AT.clear()
    monkeypatch.setattr(news_theme_service, "LLM_SYMBOL_GLOBAL_ERROR_COOLDOWN_SECONDS", 300)
    config_hash = news_theme_service._make_core_stock_config_hash({"provider": "mock", "model": "mock-model"})
    news_theme_service._CORE_STOCK_LAST_FAILURE_AT[config_hash] = datetime(2026, 5, 10, 10, 0, 0)
    try:
        assert news_theme_service._core_stock_global_failure_cooldown_active(datetime(2026, 5, 10, 10, 4, 0), config_hash=config_hash) is True
        assert news_theme_service._core_stock_global_failure_cooldown_active(datetime(2026, 5, 10, 10, 6, 0), config_hash=config_hash) is False
        other_hash = news_theme_service._make_core_stock_config_hash({"provider": "mock", "model": "other-model"})
        assert news_theme_service._core_stock_global_failure_cooldown_active(datetime(2026, 5, 10, 10, 4, 0), config_hash=other_hash) is False
    finally:
        news_theme_service._CORE_STOCK_SUGGESTION_TASKS.clear()
        news_theme_service._CORE_STOCK_LAST_FAILURE_AT.clear()


def test_consensus_rate_and_disagreement_level_are_exposed(db, monkeypatch):
    items = [
        {
            "content": f"算力订单增长，算力板块走强，第{i}条",
            "published_at": f"2026-05-09T09:{i:02d}:00",
            "source": "财联社电报",
            "url": f"https://example.com/pos-{i}",
        }
        for i in range(10)
    ]
    items.extend(
        {
            "content": f"算力公司减持，算力板块承压，第{i}条",
            "published_at": f"2026-05-09T10:{i:02d}:00",
            "source": "财联社电报",
            "url": f"https://example.com/neg-{i}",
        }
        for i in range(3)
    )
    _seed_news(db, monkeypatch, items)

    compute = next(item for item in _ranking(db) if item["theme"] == "算力")

    assert compute["positive_count"] == 10
    assert compute["negative_count"] == 3
    assert compute["consensus_rate"] == pytest.approx(10 / 13, rel=1e-3)
    assert compute["disagreement_level"] == "healthy"


def test_crowding_risk_is_generated_for_over_consensus_theme(db, monkeypatch):
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": f"算力订单增长，算力板块走强，第{i}条",
                "published_at": f"2026-05-09T09:{i:02d}:00",
                "source": "财联社电报",
                "url": f"https://example.com/hot-{i}",
            }
            for i in range(10)
        ],
    )

    compute = next(item for item in _ranking(db) if item["theme"] == "算力")

    assert compute["consensus_rate"] == 1
    assert "兑现" in compute["crowding_risk"]


def test_snapshot_history_and_performance_are_available(db, monkeypatch):
    _seed_news(
        db,
        monkeypatch,
        [
            {
                "content": "算力订单增长，算力板块走强",
                "published_at": "2026-05-09T09:00:00",
                "source": "财联社电报",
                "url": "https://example.com/snapshot",
            }
        ],
    )
    _ranking(db)
    db.execute(
        text(
            """
            CREATE TABLE stock_daily_kline (
                symbol VARCHAR(16),
                trade_date VARCHAR(10),
                sw_industry_l1 VARCHAR(80),
                pre_close FLOAT,
                close FLOAT
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO stock_daily_kline (symbol, trade_date, sw_industry_l1, pre_close, close)
            VALUES
                ('000001.SZ', '2026-05-11', '算力', 10, 11),
                ('000001.SZ', '2026-05-12', '算力', 11, 12),
                ('000001.SZ', '2026-05-13', '算力', 12, 13)
            """
        )
    )
    db.commit()

    snapshots = news_theme_service.list_theme_snapshots(db, snapshot_date="2026-05-10")
    performance = news_theme_service.get_theme_performance(db, snapshot_date="2026-05-10", horizon="3d")

    assert snapshots["items"][0]["theme"] == "算力"
    assert performance["items"][0]["theme"] == "算力"
    assert performance["items"][0]["change_pct"] == pytest.approx(30.0)
