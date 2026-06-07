from api.services.backtest_data_auto_update_service import _effective_subscription_data_source


def test_daily_kline_subscription_source_is_quantclass_even_when_config_is_stale() -> None:
    assert _effective_subscription_data_source("daily_kline", "tencent") == "quantclass"
    assert _effective_subscription_data_source("daily_kline", "qmt") == "quantclass"
    assert _effective_subscription_data_source("daily_kline", None) == "quantclass"


def test_minute_kline_subscription_keeps_qmt_source() -> None:
    assert _effective_subscription_data_source("minute_kline", "qmt") == "qmt"
