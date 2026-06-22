from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from sqlalchemy import text

from tests.postgres_test_utils import isolated_postgres_session
from api.database import Base
from api.services import auth_service, catalyst_selection_service, news_eye_service, news_theme_service


@pytest.fixture
def db():
    with isolated_postgres_session(Base, schema_prefix="ta_news_eye") as session:
        yield session


def test_refresh_news_cache_persists_items_and_sync_state(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "宁德时代签约扩产，锂电池板块走强",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "财联社电报",
                    "url": "https://example.com/a",
                    "seed_symbols": ["300750.SZ"],
                },
                {
                    "content": "平安银行一季报增长超预期",
                    "published_at": "2026-04-30T20:31:00",
                    "source": "东方财富全球快讯",
                    "url": "https://example.com/b",
                    "seed_symbols": ["000001.SZ"],
                },
            ],
            ["财联社电报", "东方财富全球快讯"],
            [],
        ),
    )

    result = news_eye_service.refresh_news_cache(
        db,
        limit=20,
        symbols=["300750.SZ", "000001.SZ"],
        trigger="manual",
    )

    assert result["saved"] == 2
    listing = news_eye_service.list_news_items(db, limit=20)
    assert listing["total"] == 2
    assert listing["history"]["total_available"] == 2
    assert listing["background"]["active_sources"] == ["财联社电报", "东方财富全球快讯"]
    assert "300750.SZ" in (listing["background"]["tracked_symbols"] or [])
    assert any(item["source"] == "财联社电报" for item in listing["items"])
    assert any(symbol["symbol"] == "300750.SZ" for symbol in listing["items"][0]["related_symbols"] + listing["items"][1]["related_symbols"])
    assert result["event_driven_selection"]["skipped"] is True


def test_prune_old_news_items_deletes_week_old_news_and_indexes(db):
    news_eye_service.ensure_news_tables(db)
    rows = [
        {
            "digest": "old-news",
            "dedupe_key": "old-news",
            "content": "一周前旧资讯",
            "published_at": datetime(2026, 5, 2, 8, 0, 0),
            "source": "测试源",
            "fetched_at": datetime(2026, 5, 2, 8, 1, 0),
        },
        {
            "digest": "fresh-news",
            "dedupe_key": "fresh-news",
            "content": "近一周资讯",
            "published_at": datetime(2026, 5, 4, 8, 0, 0),
            "source": "测试源",
            "fetched_at": datetime(2026, 5, 4, 8, 1, 0),
        },
    ]
    db.execute(
        text(
            """
            INSERT INTO market_news_items (digest, dedupe_key, content, published_at, source, fetched_at)
            VALUES (:digest, :dedupe_key, :content, :published_at, :source, :fetched_at)
            """
        ),
        rows,
    )
    db.execute(
        text(
            """
            INSERT INTO market_news_item_symbols (digest, symbol, name, tag_group)
            VALUES
                ('old-news', '300750.SZ', '宁德时代', 'related'),
                ('fresh-news', '000001.SZ', '平安银行', 'related')
            """
        )
    )

    result = news_eye_service.prune_old_news_items(
        db,
        reference_time=datetime(2026, 5, 10, 9, 0, 0),
        retention_days=7,
    )

    assert result["deleted"] == 1
    remaining_items = db.execute(text("SELECT digest FROM market_news_items ORDER BY digest")).scalars().all()
    remaining_symbols = db.execute(text("SELECT digest FROM market_news_item_symbols ORDER BY digest")).scalars().all()
    assert remaining_items == ["fresh-news"]
    assert remaining_symbols == ["fresh-news"]


def test_refresh_news_cache_triggers_event_driven_selection(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "华为昇腾推出新一代芯片封装方案，半导体产业链获得新增订单。",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "人民日报",
                    "url": "https://example.com/a",
                    "seed_symbols": ["300750.SZ"],
                }
            ],
            ["人民日报"],
            [],
        ),
    )
    captured: dict[str, object] = {}

    def fake_refresh_theme_rankings(*args, **kwargs):
        captured["theme_allow_async_llm"] = kwargs.get("allow_async_llm")
        return {"premarket": []}

    monkeypatch.setattr(news_theme_service, "refresh_theme_rankings", fake_refresh_theme_rankings)

    def fake_refresh_event_driven_selection(session, *, trigger, windows, limit, user_id=None, trade_date=None, trigger_context=None):
        captured.update(
            {
                "trigger": trigger,
                "windows": tuple(windows),
                "limit": limit,
                "user_id": user_id,
                "trade_date": trade_date,
                "trigger_context": trigger_context or {},
            }
        )
        return {
            "trigger": trigger,
            "trade_date": "2026-04-30",
            "generated": [{"window": "premarket", "item_count": 1, "top_symbol": "600584.SH", "top_name": "长电科技", "top_score": 88.0}],
            "errors": [],
            "skipped": False,
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh_event_driven_selection)

    result = news_eye_service.refresh_news_cache(db, limit=20, symbols=["300750.SZ"], trigger="manual", user_id="user-1")

    assert captured["trigger"] == "news-eye:manual"
    assert captured["windows"] == ("premarket", "24h")
    assert captured["user_id"] == "user-1"
    assert captured["theme_allow_async_llm"] is True
    assert captured["trigger_context"]["fresh_event_count"] == 1
    assert captured["trigger_context"]["fresh_news_events"][0]["source"] == "人民日报"
    assert captured["trigger_context"]["fresh_news_summary"]["event_count"] == 1
    assert result["event_driven_selection"]["generated"][0]["top_symbol"] == "600584.SH"
    assert result["event_driven_selection"]["status"] == "completed"
    assert result["event_driven_selection"]["windows"] == ["premarket", "24h"]
    assert result["event_driven_selection"]["updated_at"]

    listing = news_eye_service.list_news_items(db, limit=20)
    background_selection = listing["background"]["event_driven_selection"]
    assert background_selection["triggered"] is True
    assert background_selection["status"] == "completed"
    assert background_selection["fresh_event_count"] == 1
    assert background_selection["news_ingest"]["new"] == 1
    assert background_selection["generated"][0]["top_symbol"] == "600584.SH"
    assert background_selection["updated_at"]


def test_refresh_news_cache_can_schedule_event_driven_selection_async(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "工信部推动人工智能算力基础设施建设，AI服务器产业链受益。",
                    "published_at": "2026-04-30T20:32:00",
                    "source": "财联社电报",
                    "url": "https://example.com/async-ai",
                    "seed_symbols": ["603019.SH"],
                }
            ],
            ["财联社电报"],
            [],
        ),
    )
    monkeypatch.setattr(news_theme_service, "refresh_theme_rankings", lambda *args, **kwargs: {"premarket": []})
    scheduled_calls: list[dict[str, object]] = []

    def should_not_run_sync(*args, **kwargs):
        raise AssertionError("selection refresh should be scheduled, not run inline")

    def fake_schedule_event_driven_selection_refresh(**kwargs):
        scheduled_calls.append(kwargs)
        return {
            "refresh_key": "news-eye-refresh-key",
            "trigger": kwargs["trigger"],
            "status": "scheduled",
            "windows": list(kwargs["windows"]),
            "limit": kwargs["limit"],
            "user_id": kwargs["user_id"],
            "context": kwargs["context"],
            "generated": [],
            "errors": [],
            "skipped": False,
            "updated_at": "2026-04-30T12:00:00",
        }

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", should_not_run_sync)
    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule_event_driven_selection_refresh)

    result = news_eye_service.refresh_news_cache(
        db,
        limit=20,
        symbols=["603019.SH"],
        trigger="manual",
        user_id="user-async",
        async_event_driven_selection=True,
    )
    listing = news_eye_service.list_news_items(db, limit=20)

    assert result["fresh_event_count"] == 1
    assert result["event_driven_selection"]["status"] == "scheduled"
    assert result["event_driven_selection"]["triggered"] is True
    assert result["event_driven_selection"]["windows"] == ["premarket", "24h"]
    assert scheduled_calls
    assert scheduled_calls[0]["trigger"] == "news-eye:manual"
    assert scheduled_calls[0]["context"]["fresh_event_count"] == 1
    assert scheduled_calls[0]["context"]["news_ingest"]["new"] == 1
    assert listing["background"]["event_driven_selection"]["status"] == "running"
    assert listing["background"]["event_driven_selection"]["fresh_event_count"] == 1


def test_event_driven_selection_async_scheduler_dedupes_running_refresh(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_schedule_event_driven_selection_refresh(**kwargs):
        calls.append(kwargs)
        return {
            "refresh_key": "dedupe-key",
            "trigger": kwargs["trigger"],
            "status": "scheduled" if len(calls) == 1 else "running",
            "deduped": len(calls) > 1,
            "reason": "event_driven_selection_refresh_already_running" if len(calls) > 1 else None,
            "windows": list(kwargs["windows"]),
            "generated": [],
            "errors": [],
            "skipped": False,
            "updated_at": "2026-04-30T12:00:00",
        }

    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule_event_driven_selection_refresh)

    first = news_eye_service._schedule_event_driven_selection_refresh(
        trigger="news-eye:manual",
        windows=("premarket", "24h"),
        limit=10,
        user_id="user-1",
        fresh_event_count=2,
        news_ingest={"saved": 2, "new": 2, "updated": 0, "unchanged": 0},
    )
    second = news_eye_service._schedule_event_driven_selection_refresh(
        trigger="news-eye:manual",
        windows=("premarket", "24h"),
        limit=10,
        user_id="user-1",
        fresh_event_count=1,
        news_ingest={"saved": 1, "new": 1, "updated": 0, "unchanged": 0},
    )

    assert first["status"] == "scheduled"
    assert second["status"] == "running"
    assert second["deduped"] is True
    assert len(calls) == 2
    assert calls[0]["context"]["fresh_event_count"] == 2
    assert calls[1]["context"]["fresh_event_count"] == 1


def test_refresh_news_cache_skips_selection_when_news_is_unchanged(db, monkeypatch):
    item = {
        "content": "工信部推动人工智能产业创新，算力基础设施受关注。",
        "published_at": "2026-04-30T20:30:00",
        "source": "财联社电报",
        "url": "https://example.com/ai-policy",
        "seed_symbols": ["603019.SH"],
    }
    monkeypatch.setattr(news_eye_service, "_fetch_external_news", lambda limit, symbols: ([dict(item)], ["财联社电报"], []))
    monkeypatch.setattr(news_theme_service, "refresh_theme_rankings", lambda *args, **kwargs: {"premarket": []})
    calls: list[str] = []

    def fake_refresh_event_driven_selection(session, *, trigger, windows, limit, user_id=None, trade_date=None, trigger_context=None):
        calls.append(trigger)
        return {"trigger": trigger, "generated": [], "errors": [], "skipped": False}

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", fake_refresh_event_driven_selection)

    first = news_eye_service.refresh_news_cache(db, limit=20, symbols=["603019.SH"], trigger="manual")
    second = news_eye_service.refresh_news_cache(db, limit=20, symbols=["603019.SH"], trigger="manual")
    listing = news_eye_service.list_news_items(db, limit=20)

    assert first["new"] == 1
    assert first["fresh_event_count"] == 1
    assert second["new"] == 0
    assert second["updated"] == 0
    assert second["unchanged"] == 1
    assert second["fresh_event_count"] == 0
    assert second["event_driven_selection"]["triggered"] is False
    assert second["event_driven_selection"]["reason"] == "no_new_or_changed_news"
    assert calls == ["news-eye:manual"]
    assert listing["background"]["fresh_event_count"] == 0
    assert listing["background"]["event_driven_selection"]["triggered"] is False


def test_background_refresh_uses_active_user_focus_and_llm_context(db, monkeypatch):
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            """
            INSERT INTO users (id, email, is_active, created_at, updated_at, last_login_at)
            VALUES ('user-active', 'active@example.com', true, :now, :now, :now)
            """
        ),
        {"now": now},
    )
    db.execute(
        text(
            """
            INSERT INTO watchlist_items (id, user_id, symbol, sort_order, created_at)
            VALUES ('watch-user-active-1', 'user-active', '300750.SZ', 0, :now)
            """
        ),
        {"now": now},
    )
    db.commit()

    class FakeSessionLocal:
        def __enter__(self):
            return db

        def __exit__(self, exc_type, exc, tb):
            return False

    captured: dict[str, object] = {}

    monkeypatch.setattr(news_eye_service, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            captured.update({"fetch_symbols": tuple(symbols)}) or [
                {
                    "content": "宁德时代签约储能订单，锂电池产业链获得新增订单。",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "财联社电报",
                    "url": "https://example.com/background-user",
                    "seed_symbols": ["300750.SZ"],
                }
            ],
            ["财联社电报"],
            [],
        ),
    )

    def fake_refresh_theme_rankings(*args, **kwargs):
        captured["theme_user_id"] = kwargs.get("user_id")
        captured["theme_allow_async_llm"] = kwargs.get("allow_async_llm")
        return {"premarket": [], "24h": []}

    def fake_schedule_event_driven_selection_refresh(*, trigger, windows, limit, user_id=None, trade_date=None, reason=None, context=None):
        captured.update(
            {
                "selection_trigger": trigger,
                "selection_windows": tuple(windows),
                "selection_user_id": user_id,
                "selection_reason": reason,
                "selection_context": context,
            }
        )
        return {"trigger": trigger, "status": "scheduled", "generated": [], "errors": [], "skipped": False}

    monkeypatch.setattr(news_theme_service, "refresh_theme_rankings", fake_refresh_theme_rankings)
    monkeypatch.setattr(catalyst_selection_service, "schedule_event_driven_selection_refresh", fake_schedule_event_driven_selection_refresh)

    news_eye_service._scan_and_refresh_once()

    assert captured["fetch_symbols"] == ("300750.SZ",)
    assert captured["theme_user_id"] == "user-active"
    assert captured["theme_allow_async_llm"] is True
    assert captured["selection_trigger"] == "news-eye:background"
    assert captured["selection_windows"] == ("premarket", "24h")
    assert captured["selection_user_id"] == "user-active"
    assert captured["selection_reason"] == "news_eye_fresh_events"
    assert captured["selection_context"]["fresh_event_count"] == 1


def test_refresh_news_cache_keeps_success_when_event_driven_selection_fails(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "证监会完善上市公司分红制度，A股红利板块受关注",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "财联社电报",
                    "url": "https://example.com/source-ok",
                }
            ],
            ["财联社电报"],
            [],
        ),
    )
    monkeypatch.setattr(news_theme_service, "refresh_theme_rankings", lambda *args, **kwargs: {"premarket": []})

    def boom(*args, **kwargs):
        raise RuntimeError("selection-refresh failed")

    monkeypatch.setattr(catalyst_selection_service, "refresh_event_driven_selection", boom)

    result = news_eye_service.refresh_news_cache(db, limit=20, symbols=["600047.SH"], trigger="background")

    assert result["saved"] == 1
    assert result["event_driven_selection"]["errors"][0]["error"] == "selection-refresh failed"


def test_news_eye_analysis_rejects_account_key_mixed_with_default_endpoint(db, monkeypatch):
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
    user_id = "mixed-news-eye-user"
    auth_service.upsert_user_llm_config(db, user_id, api_key="volcengine-key-only")
    config = news_eye_service._resolve_news_llm_config(db, user_id=user_id)

    diagnostic = news_eye_service._news_llm_runtime_diagnostic(config)
    assert diagnostic["runtime_package_source"] == "mixed_runtime"
    assert diagnostic["api_key_source"] == "user_config"
    assert diagnostic["mixed_account_runtime"] is True

    with pytest.raises(Exception) as exc_info:
        news_eye_service.analyze_news_item(
            db,
            user_id=user_id,
            payload={"content": "人工智能政策利好，算力基础设施受关注。"},
        )

    assert getattr(exc_info.value, "status_code", None) == 400
    assert "同源运行包" in str(getattr(exc_info.value, "detail", ""))


def test_fetch_external_news_collects_general_and_symbol_sources(monkeypatch):
    import pandas as pd
    import akshare as ak

    monkeypatch.setattr(
        ak,
        "stock_info_global_cls",
        lambda symbol="全部": pd.DataFrame(
            [{"标题": "财联社快讯", "内容": "算力方向再获催化", "发布日期": "2026-04-30", "发布时间": "20:10:00"}]
        ),
    )
    monkeypatch.setattr(
        ak,
        "stock_info_global_em",
        lambda: pd.DataFrame(
            [{"标题": "东财快讯", "摘要": "人工智能板块活跃", "发布时间": "2026-04-30 20:11:00", "链接": "https://example.com/em"}]
        ),
    )
    monkeypatch.setattr(ak, "stock_info_global_ths", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_sina", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_futu", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_cjzc_em", lambda: pd.DataFrame())
    monkeypatch.setattr(news_eye_service, "NOTICE_SOURCE_SPECS", ())
    monkeypatch.setattr(
        news_eye_service,
        "SYMBOL_SOURCE_SPECS",
        (
            news_eye_service.NewsSourceSpec(
                "东方财富个股新闻",
                "stock_news_em",
                symbol_param="symbol",
                symbol_transform=lambda symbol: str(symbol).split(".", 1)[0],
            ),
        ),
    )
    monkeypatch.setattr(
        ak,
        "stock_news_em",
        lambda symbol="300750": pd.DataFrame(
            [{"标题": "宁德时代新闻", "内容": "宁德时代订单增长", "发布时间": "2026-04-30 20:12:00", "链接": "https://example.com/symbol"}]
        ),
    )

    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=["300750.SZ"])

    assert len(items) >= 3
    assert "财联社电报" in active_sources
    assert "东方财富全球快讯" in active_sources
    assert any(source.startswith("东方财富个股新闻:300750.SZ") for source in active_sources)
    assert not any("拉取失败" in warning for warning in warnings)


def test_fetch_external_news_collects_notice_sources(monkeypatch):
    import pandas as pd
    import akshare as ak

    monkeypatch.setattr(ak, "stock_info_global_cls", lambda symbol="全部": pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_cjzc_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_sina", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_futu", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_ths", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_news_em", lambda symbol="300750": pd.DataFrame())
    monkeypatch.setattr(
        ak,
        "stock_notice_report",
        lambda symbol="重大事项", date="20260430": pd.DataFrame(
            [
                {
                    "代码": "300750",
                    "名称": "宁德时代",
                    "公告标题": "宁德时代:关于签订储能合作协议的公告",
                    "公告类型": "重大事项",
                    "公告日期": "2026-04-30",
                    "网址": "https://example.com/notice",
                }
            ]
        ),
    )

    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=[])

    assert "东方财富公告:重大事项" in active_sources
    assert any("宁德时代:关于签订储能合作协议的公告" in item["content"] for item in items)
    assert any(item.get("seed_symbols") == ["300750.SZ"] for item in items)
    assert not any("东方财富公告" in warning and "拉取失败" in warning for warning in warnings)


def test_fetch_external_news_collects_official_notice_sources(monkeypatch):
    import pandas as pd
    import akshare as ak

    monkeypatch.setattr(news_eye_service, "GENERAL_SOURCE_SPECS", ())
    monkeypatch.setattr(news_eye_service, "NOTICE_SOURCE_SPECS", ())
    monkeypatch.setattr(news_eye_service, "SYMBOL_SOURCE_SPECS", ())
    def fake_cninfo(symbol="", market="沪深京", keyword="", category="", start_date="20260429", end_date="20260430"):
        rows_by_symbol = {
            "300750": [
                {
                    "证券代码": "300750",
                    "证券简称": "宁德时代",
                    "公告标题": "关于签订储能合作协议的公告",
                    "公告类型": "日常经营",
                    "公告时间": "2026-04-30",
                    "公告链接": "finalpage/2026-04-30/notice-cninfo.PDF",
                }
            ],
            "600519": [
                {
                    "证券代码": "600519",
                    "证券简称": "贵州茅台",
                    "公告标题": "2026年第一季度报告",
                    "公告日期": "2026-04-30",
                    "链接": "https://www.sse.com.cn/disclosure/listedinfo/announcement/c/new.pdf",
                }
            ],
            "000001": [
                {
                    "股票代码": "000001",
                    "股票简称": "平安银行",
                    "标题": "关于召开股东大会的公告",
                    "发布日期": "2026-04-30",
                    "URL": "https://www.szse.cn/disclosure/listed/bulletinDetail/index.html",
                }
            ],
        }
        return pd.DataFrame(rows_by_symbol.get(str(symbol), []))

    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", fake_cninfo)

    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=["300750.SZ", "600519.SH", "000001.SZ"])

    assert "巨潮资讯公告" in active_sources
    assert "上交所公告" in active_sources
    assert "深交所公告" in active_sources
    assert any(item["source"] == "巨潮资讯公告" and item["seed_symbols"] == ["300750.SZ"] for item in items)
    assert any("贵州茅台" in item["content"] for item in items)
    assert any("平安银行" in item["content"] for item in items)
    assert not any("官方公告" in warning and "拉取失败" in warning for warning in warnings)


def test_load_user_focus_symbols_includes_positions_and_recent_selection_results(db):
    now = datetime.now(timezone.utc)
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS selection_center_tasks (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(64) NOT NULL,
                name VARCHAR(200) NOT NULL,
                mode VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                universe VARCHAR(200),
                config_json JSON,
                candidates_json JSON,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
    )
    db.execute(
        text(
            """
            INSERT INTO imported_portfolio_positions (
                id, user_id, source, symbol, security_name, current_position, trade_points_count,
                last_imported_at, created_at, updated_at
            )
            VALUES (
                'position-focus-1', 'focus-user', 'manual', '600519.SH', '贵州茅台', 100, 0,
                :now, :now, :now
            )
            """
        ),
        {"now": now},
    )
    db.execute(
        text(
            """
            INSERT INTO selection_center_tasks (
                id, user_id, name, mode, status, universe, config_json, candidates_json, created_at, updated_at
            )
            VALUES (
                'selection-focus-1', 'focus-user', '首日波段', 'strategy', 'completed', '主板', '{}',
                '[{"symbol":"300750.SZ","name":"宁德时代"},{"code":"000001.SZ","name":"平安银行"}]',
                :now, :now
            )
            """
        ),
        {"now": now},
    )
    db.commit()

    symbols = news_eye_service.load_user_focus_symbols(db, "focus-user", limit=10)

    assert "600519.SH" in symbols
    assert "300750.SZ" in symbols
    assert "000001.SZ" in symbols


def test_normalize_a_share_symbol_treats_920_prefix_as_beijing_exchange():
    assert news_eye_service._normalize_a_share_symbol("920161") == "920161.BJ"


def test_fetch_external_news_collects_research_reports_for_focus_symbols(monkeypatch):
    import pandas as pd
    import akshare as ak

    monkeypatch.setattr(ak, "stock_info_global_cls", lambda symbol="全部": pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_cjzc_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_sina", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_futu", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_ths", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_news_em", lambda symbol="300750": pd.DataFrame())
    monkeypatch.setattr(ak, "stock_notice_report", lambda symbol="重大事项", date="20260430": pd.DataFrame())
    monkeypatch.setattr(
        ak,
        "stock_research_report_em",
        lambda symbol="300750": pd.DataFrame(
            [
                {
                    "股票代码": "300750",
                    "股票简称": "宁德时代",
                    "报告名称": "技术创新夯实龙头领先优势",
                    "东财评级": "买入",
                    "机构": "国信证券",
                    "行业": "电池",
                    "日期": "2026-04-30",
                    "报告PDF链接": "https://example.com/research.pdf",
                }
            ]
        ),
    )

    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=["300750.SZ"])

    assert any(source.startswith("东方财富研报:300750.SZ") for source in active_sources)
    assert any("国信证券" in item["content"] and "买入" in item["content"] for item in items)
    assert any(item.get("url") == "https://example.com/research.pdf" for item in items)
    assert not any("东方财富研报" in warning and "拉取失败" in warning for warning in warnings)


def test_fetch_external_news_times_out_slow_source_without_blocking_other_sources(monkeypatch):
    import pandas as pd
    import akshare as ak

    def slow_source(symbol="全部"):
        time.sleep(0.2)
        return pd.DataFrame([{"标题": "慢源", "内容": "人工智能政策利好", "发布日期": "2026-04-30", "发布时间": "20:10:00"}])

    def fast_source(symbol="全部"):
        return pd.DataFrame([{"标题": "快源", "内容": "算力板块新增订单", "发布日期": "2026-04-30", "发布时间": "20:11:00"}])

    monkeypatch.setattr(ak, "stock_info_global_cls", slow_source)
    monkeypatch.setattr(ak, "stock_info_global_em", fast_source)
    monkeypatch.setattr(ak, "stock_info_cjzc_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_sina", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_futu", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_ths", lambda: pd.DataFrame())
    monkeypatch.setattr(
        news_eye_service,
        "GENERAL_SOURCE_SPECS",
        (
            news_eye_service.NewsSourceSpec("慢源", "stock_info_global_cls"),
            news_eye_service.NewsSourceSpec("快源", "stock_info_global_em"),
        ),
    )
    monkeypatch.setattr(news_eye_service, "SYMBOL_SOURCE_SPECS", ())
    monkeypatch.setattr(news_eye_service, "NOTICE_SOURCE_SPECS", ())
    monkeypatch.setattr(news_eye_service, "_SOURCE_TIMEOUT_SECONDS", 0.05)

    started = time.monotonic()
    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=[])
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
    assert active_sources == ["快源"]
    assert any("慢源 拉取超过" in warning for warning in warnings)
    assert any(item["source"] == "快源" for item in items)
    assert news_eye_service._sync_state_error_from_warnings(warnings, active_sources) is None


def test_fetch_external_news_uses_extra_general_sources_and_dedupes(monkeypatch):
    import pandas as pd
    import akshare as ak

    duplicate_content = "上市公司签约算力订单，人工智能板块走强"
    monkeypatch.setattr(ak, "stock_info_global_cls", lambda symbol="全部": pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_global_em", lambda: pd.DataFrame())
    monkeypatch.setattr(ak, "stock_info_cjzc_em", lambda: pd.DataFrame())
    monkeypatch.setattr(
        ak,
        "stock_info_global_sina",
        lambda: pd.DataFrame([{"时间": "2026-04-30 20:10:01", "内容": duplicate_content}]),
    )
    monkeypatch.setattr(
        ak,
        "stock_info_global_futu",
        lambda: pd.DataFrame(
            [
                {
                    "标题": "",
                    "内容": duplicate_content,
                    "发布时间": "2026-04-30 20:10:08",
                    "链接": "https://example.com/futu-1",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        ak,
        "stock_info_global_ths",
        lambda: pd.DataFrame(
            [{"标题": "龙虎榜活跃", "内容": "龙虎榜显示机器人板块资金活跃", "发布时间": "2026-04-30 20:12:00"}]
        ),
    )
    monkeypatch.setattr(news_eye_service, "NOTICE_SOURCE_SPECS", ())
    monkeypatch.setattr(news_eye_service, "SYMBOL_SOURCE_SPECS", ())

    items, active_sources, warnings = news_eye_service._fetch_external_news(20, symbols=[])

    assert "新浪7x24" in active_sources
    assert "富途快讯" in active_sources
    assert "同花顺全球直播" in active_sources
    assert len(items) == 2
    assert sum(1 for item in items if duplicate_content in item["content"]) == 1
    assert not news_eye_service._sync_state_error_from_warnings(warnings, active_sources)


def test_refresh_news_cache_dedupes_shared_pool_by_content(db, monkeypatch):
    duplicate_content = "宁德时代签约扩产，锂电池板块走强"
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": duplicate_content,
                    "published_at": "2026-04-30T20:30:01",
                    "source": "新浪7x24",
                    "url": None,
                },
                {
                    "content": duplicate_content,
                    "published_at": "2026-04-30T20:30:08",
                    "source": "富途快讯",
                    "url": "https://example.com/futu-duplicate",
                },
            ],
            ["新浪7x24", "富途快讯"],
            [],
        ),
    )

    result = news_eye_service.refresh_news_cache(db, limit=20, symbols=[], trigger="manual")
    listing = news_eye_service.list_news_items(db, limit=20)

    assert result["saved"] == 1
    assert listing["total"] == 1
    assert listing["items"][0]["content"] == duplicate_content


def test_refresh_news_cache_indexes_actual_digest_when_dedupe_hits_legacy_row(db, monkeypatch):
    content = "宁德时代签约扩产，锂电池板块走强"
    legacy_digest = "a" * 64
    dedupe_key = news_eye_service._make_news_dedupe_key({"content": content, "published_at": "2026-04-30T20:30:01"})
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
                :digest, :dedupe_key, :content, :published_at, '旧源', NULL, 'neutral',
                '[]', '[]', '[]', '[]', '[]', :published_at
            )
            """
        ),
        {
            "digest": legacy_digest,
            "dedupe_key": dedupe_key,
            "content": content,
            "published_at": "2026-04-30T20:30:01",
        },
    )
    db.commit()
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: {})
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": content,
                    "published_at": "2026-04-30T20:30:01",
                    "source": "新浪7x24",
                    "url": "https://example.com/sina-duplicate",
                    "seed_symbols": ["300750.SZ"],
                }
            ],
            ["新浪7x24"],
            [],
        ),
    )

    news_eye_service.refresh_news_cache(db, limit=20, symbols=["300750.SZ"], trigger="manual")
    indexed = db.execute(
        text(
            """
            SELECT COUNT(*) AS count
            FROM market_news_item_symbols
            WHERE digest = :digest AND symbol = '300750.SZ'
            """
        ),
        {"digest": legacy_digest},
    ).scalar()
    wrong_digest_indexed = db.execute(
        text(
            """
            SELECT COUNT(*) AS count
            FROM market_news_item_symbols
            WHERE digest = :digest
            """
        ),
        {"digest": dedupe_key},
    ).scalar()

    assert indexed >= 1
    assert wrong_digest_indexed == 0


def test_refresh_news_cache_ignores_symbol_only_failures_for_sync_state(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "证监会完善上市公司分红制度，A股红利板块受关注",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "财联社电报",
                    "url": "https://example.com/source-ok",
                }
            ],
            ["财联社电报"],
            ["东方财富个股新闻(600047.SH) 拉取失败: 'code'"],
        ),
    )

    news_eye_service.refresh_news_cache(db, limit=20, symbols=["600047.SH"], trigger="background")
    listing = news_eye_service.list_news_items(db, limit=20)

    assert listing["background"]["status"] == "success"
    assert listing["background"]["last_error"] is None


def test_extract_impact_payload_separates_positive_and_negative_entities(monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "get_reverse_stock_map",
        lambda: {
            "300750.SZ": "宁德时代",
            "000001.SZ": "平安银行",
        },
    )

    positive_sectors, negative_sectors, positive_symbols, negative_symbols, sentiment = news_eye_service._extract_impact_payload(
        "宁德时代签约扩产，锂电池板块走强；平安银行遭处罚，银行板块承压"
    )

    assert sentiment == "neutral"
    assert "锂电池" in positive_sectors
    assert "银行" in negative_sectors
    assert "300750.SZ" in positive_symbols
    assert "000001.SZ" in negative_symbols


def test_extract_symbols_matches_stock_codes_and_names_without_duplicate_hits(monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "get_reverse_stock_map",
        lambda: {
            "300750.SZ": "宁德时代",
            "000001.SZ": "平安银行",
        },
    )

    symbols = news_eye_service._extract_symbols("宁德时代公告扩产，300750 再获关注，平安银行维持稳健增长")

    assert symbols == ["300750.SZ", "000001.SZ"]


def test_enrich_news_item_falls_back_to_seed_symbol_for_positive_story(monkeypatch):
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: {})

    enriched = news_eye_service._enrich_news_item(
        {
            "content": "公司签约扩产，订单增长超预期",
            "published_at": "2026-04-30T20:31:00",
            "source": "东方财富个股新闻",
            "seed_symbols": ["300750.SZ"],
        }
    )

    positive_symbols = news_eye_service._loads(enriched["positive_symbols_json"])
    related_symbols = news_eye_service._loads(enriched["related_symbols_json"])

    assert enriched["sentiment"] == "positive"
    assert any(item["symbol"] == "300750.SZ" for item in positive_symbols)
    assert any(item["symbol"] == "300750.SZ" for item in related_symbols)


def test_list_news_items_supports_offset_history(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": f"第{i}条快讯，宁德时代订单增长",
                    "published_at": f"2026-04-30T20:{i:02d}:00",
                    "source": "财联社电报",
                    "url": f"https://example.com/{i}",
                    "seed_symbols": ["300750.SZ"],
                }
                for i in range(5)
            ],
            ["财联社电报"],
            [],
        ),
    )
    news_eye_service.refresh_news_cache(db, limit=10, symbols=["300750.SZ"], trigger="manual")

    first_page = news_eye_service.list_news_items(db, limit=2, offset=0)
    second_page = news_eye_service.list_news_items(db, limit=2, offset=2)

    assert first_page["total"] == 5
    assert first_page["history"]["has_more"] is True
    assert first_page["history"]["returned"] == 2
    assert second_page["history"]["offset"] == 2
    assert len(second_page["items"]) == 2


def test_list_news_items_filters_by_symbol_and_sector_via_index_tables(db, monkeypatch):
    monkeypatch.setattr(
        news_eye_service,
        "_fetch_external_news",
        lambda limit, symbols: (
            [
                {
                    "content": "宁德时代签约扩产，锂电池板块走强",
                    "published_at": "2026-04-30T20:30:00",
                    "source": "财联社电报",
                    "url": "https://example.com/a1",
                    "seed_symbols": ["300750.SZ"],
                },
                {
                    "content": "平安银行遭处罚，银行板块承压",
                    "published_at": "2026-04-30T20:31:00",
                    "source": "东方财富全球快讯",
                    "url": "https://example.com/a2",
                    "seed_symbols": ["000001.SZ"],
                },
            ],
            ["财联社电报", "东方财富全球快讯"],
            [],
        ),
    )
    monkeypatch.setattr(
        news_eye_service,
        "get_reverse_stock_map",
        lambda: {
            "300750.SZ": "宁德时代",
            "000001.SZ": "平安银行",
        },
    )

    news_eye_service.refresh_news_cache(db, limit=10, symbols=["300750.SZ", "000001.SZ"], trigger="manual")

    symbol_filtered = news_eye_service.list_news_items(db, limit=10, symbol="300750.SZ")
    sector_filtered = news_eye_service.list_news_items(db, limit=10, sector="银行")

    assert len(symbol_filtered["items"]) == 1
    assert symbol_filtered["items"][0]["source"] == "财联社电报"
    assert len(sector_filtered["items"]) == 1
    assert sector_filtered["items"][0]["source"] == "东方财富全球快讯"


def test_parse_news_analysis_payload_strips_code_fences():
    parsed = news_eye_service._parse_news_analysis_payload(
        """```json
        {"summary":"测试","sentiment":"positive","sentiment_reason":"订单增长","positive_sectors":["锂电池"],"negative_sectors":[],"positive_symbols":["宁德时代(300750.SZ)"],"negative_symbols":[],"trading_takeaway":"关注持续性"}
        ```"""
    )

    assert parsed is not None
    assert parsed["sentiment"] == "positive"


def test_is_a_share_relevant_news_filters_overseas_noise(monkeypatch):
    monkeypatch.setattr(news_eye_service, "get_reverse_stock_map", lambda: {})

    assert news_eye_service._is_a_share_relevant_news({
        "content": "证监会表示将持续完善上市公司分红制度，A股红利板块受关注",
        "source": "财联社电报",
    }) is True
    assert news_eye_service._is_a_share_relevant_news({
        "content": "道琼斯指数收涨，微软和亚马逊领涨美股科技股",
        "source": "东方财富全球快讯",
    }) is False
    assert news_eye_service._is_a_share_relevant_news({
        "content": "美联储释放降息预期，原油与铜价走高，有望带动A股有色板块情绪",
        "source": "东方财富全球快讯",
    }) is True
