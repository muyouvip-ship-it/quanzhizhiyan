from api.services.data_source_governance import (
    build_backtest_governance,
    build_market_overview_governance,
    describe_sources,
    list_news_source_links,
    list_surface_registry,
    list_registered_sources,
    split_source_values,
)


def test_split_source_values_supports_composite_tokens():
    assert split_source_values("qmt_realtime+postgresql_fallback / akshare") == [
        "qmt_realtime",
        "postgresql_fallback",
        "akshare",
    ]


def test_describe_sources_normalizes_registered_prefixes():
    items = describe_sources(["cache:market_news_items", "qmt_bridge:min1_fallback", "synthetic:fallback"])
    keys = [item["key"] for item in items]
    assert keys == ["news_cache", "qmt_bridge", "synthetic"]


def test_build_backtest_governance_marks_synthetic_results_as_low_trust():
    payload = build_backtest_governance(
        {
            "created_at": "2026-05-06T10:00:00Z",
            "completed_at": "2026-05-06T10:05:00Z",
            "artifact_root": "/tmp/backtests/demo",
            "result": {
                "summary": {
                    "data_source": "synthetic:fallback",
                    "engine_mode": "fallback_engine",
                    "minute_aggregation": "5m",
                },
                "diagnostics": {
                    "fallback_mode": True,
                    "minute_data_missing": 12,
                },
            },
        }
    )
    assert payload["domain"] == "backtest_result"
    assert payload["items"][0]["value"] == "Synthetic / 合成"
    assert any("Synthetic 数据" in warning for warning in payload["warnings"])


def test_registered_sources_contains_core_qmt_registry_item():
    sources = list_registered_sources()
    qmt = next(item for item in sources if item["key"] == "qmt")
    assert qmt["label"] == "QMT 实时行情链路"


def test_surface_registry_exposes_page_to_source_mapping():
    surfaces = list_surface_registry()
    realtime = next(item for item in surfaces if item["id"] == "realtime-monitor")
    assert realtime["route"] == "/realtime"
    assert "qmt" in realtime["source_keys"]
    assert any(source["key"] == "realtime_event_stream" for source in realtime["sources"])


def test_news_source_links_expose_fetch_names_and_urls():
    sources = list_news_source_links()
    names = {item["name"] for item in sources}
    assert "财联社电报" in names
    assert "巨潮资讯公告" in names
    assert "上交所公告" in names
    assert "深交所公告" in names
    assert "东方财富全球快讯" in names
    official = [item for item in sources if item.get("tier") == "一级"]
    assert [item["name"] for item in official[:3]] == ["巨潮资讯公告", "上交所公告", "深交所公告"]
    assert all(item["url"].startswith("https://") for item in sources)


def test_market_overview_governance_includes_market_stats_source():
    payload = build_market_overview_governance(
        {
            "source": "qmt_realtime+postgresql_fallback",
            "updated_at": "2026-05-26T10:00:00Z",
            "indices": [{"source": "qmt_realtime"}],
            "top_gainers": [{"source": "postgresql:stock_daily_kline"}],
            "top_losers": [],
            "sector_gainers": [],
            "sector_losers": [],
            "sector_fund_inflows": [],
            "sector_fund_outflows": [],
            "market_stats": {
                "source": "postgresql:stock_daily_kline",
                "up_count": 3100,
                "down_count": 1800,
                "total_amount": 2_200_000_000_000,
            },
        }
    )

    labels = [item["label"] for item in payload["items"]]
    assert "市场宽度与成交额" in labels
    stats_item = next(item for item in payload["items"] if item["label"] == "市场宽度与成交额")
    assert stats_item["value"] == "postgresql:stock_daily_kline"
