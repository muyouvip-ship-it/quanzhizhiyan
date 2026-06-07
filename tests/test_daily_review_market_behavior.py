from __future__ import annotations

from api.services.daily_review_market_behavior import interpret_market_behavior


def test_interpret_market_behavior_maps_liquidity_breadth_and_sentiment() -> None:
    labels = interpret_market_behavior(
        {
            "indices": [
                {"symbol": "000001.SH", "name": "上证指数", "change_pct": 0.96},
                {"symbol": "399006.SZ", "name": "创业板指", "change_pct": 2.1},
                {"symbol": "000688.SH", "name": "科创50", "change_pct": 5.88},
            ],
            "sector_gainers": [{"sector_name": "半导体", "change_pct": 7.2}],
            "sector_losers": [{"sector_name": "银行", "change_pct": -1.1}],
            "sector_inflows": [{"sector_name": "电子", "net_inflow": 100}],
            "sector_outflows": [{"sector_name": "非银金融", "net_inflow": -20}],
            "market_stats": {
                "index_turnover_amount": 3_205_800_000_000,
                "amount_change": 302_000_000_000,
                "up_count": 2181,
                "down_count": 3218,
                "limit_up_count": 103,
                "limit_down_count": 40,
                "limit_up_promotion_rate": 18.18,
                "limit_up_promotion_count": 8,
                "limit_up_promotion_base": 44,
                "failed_limit_up_rate": 35.0,
                "failed_limit_up_count": 20,
                "limit_up_touch_count": 57,
                "sentiment_source": "test",
            },
        }
    )

    assert labels["liquidity_state"]["label"] == "流动性极度充沛"
    assert labels["breadth_state"]["label"] == "个股失血/指数失真压力"
    assert labels["market_regime"]["label"] in {"指数托举/个股失血", "硬科技权重抱团/主线虹吸"}
    assert labels["locked_values"]["total_amount_label"] == "3.21 万亿元"
    assert labels["locked_values"]["limit_up_promotion_rate_label"] == "18.18%"
    assert labels["locked_values"]["up_count"] == 2181
    assert labels["locked_values"]["down_count"] == 3218


def test_interpret_market_behavior_reports_missing_fields() -> None:
    labels = interpret_market_behavior({"market_stats": {}})

    assert labels["liquidity_state"]["label"] == "成交额数据缺失"
    assert "total_amount" in labels["data_quality"]["missing_fields"]
    assert "breadth" in labels["data_quality"]["missing_fields"]
    assert "indices" in labels["data_quality"]["missing_fields"]


def test_interpret_market_behavior_treats_zero_breadth_sample_as_missing() -> None:
    labels = interpret_market_behavior(
        {
            "market_stats": {
                "total_amount": 2_000_000_000_000,
                "up_count": 0,
                "down_count": 0,
            }
        }
    )

    assert labels["breadth_state"]["label"] == "涨跌家数缺失"
    assert labels["locked_values"]["breadth_ratio"] is None
    assert labels["locked_values"]["breadth_gap_pct"] is None
    assert "breadth" in labels["data_quality"]["missing_fields"]
