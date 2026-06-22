import sys
import types
from datetime import date

import pandas as pd

from api.services import qmt_market_data_service
from api.services.qmt_virtual_account_service import QmtRuntimeConfig


def _qmt_config(key: str, *, enabled: bool, role: str, bridge: str = "") -> QmtRuntimeConfig:
    return QmtRuntimeConfig(
        key=key,
        enabled=enabled,
        host="127.0.0.1",
        port=58610,
        account_id="",
        account_type="STOCK",
        account_name=key,
        userdata_path="",
        role=role,
        bridge_base_url=bridge,
        bridge_token="",
        refresh_interval_seconds=10,
    )


def test_resolve_market_account_key_uses_db_config_not_disabled_default(monkeypatch):
    monkeypatch.setattr(
        qmt_market_data_service.qmt_virtual_account_service,
        "_load_runtime_configs",
        lambda db=None, user_id=None: [
            _qmt_config("paper_sim", enabled=True, role="paper", bridge="http://192.168.10.1:8710"),
            _qmt_config("live_real", enabled=True, role="live", bridge="http://192.168.10.1:8711"),
        ],
    )

    assert (
        qmt_market_data_service.resolve_market_account_key(
            db=object(),
            user_id="user-1",
            preferred_account_key="paper_sim",
        )
        == "paper_sim"
    )


def test_resolve_market_account_key_falls_back_to_enabled_paper(monkeypatch):
    monkeypatch.setattr(
        qmt_market_data_service.qmt_virtual_account_service,
        "_load_runtime_configs",
        lambda db=None, user_id=None: [
            _qmt_config("paper_sim", enabled=False, role="paper", bridge=""),
            _qmt_config("paper_db", enabled=True, role="paper", bridge="http://192.168.10.1:8710"),
            _qmt_config("live_real", enabled=True, role="live", bridge="http://192.168.10.1:8711"),
        ],
    )

    assert (
        qmt_market_data_service.resolve_market_account_key(
            db=object(),
            user_id="user-1",
            preferred_account_key="paper_sim",
        )
        == "paper_db"
    )


def test_sync_index_minute_history_persists_rows_by_trade_day(monkeypatch):
    inserted_tables = []
    progress = []

    monkeypatch.setattr(
        qmt_market_data_service,
        "_load_cn_trade_dates",
        lambda: ([date(2026, 4, 27), date(2026, 4, 28)], None),
    )
    monkeypatch.setattr(
        qmt_market_data_service,
        "_fetch_intraday_payload_safe",
        lambda symbols, trade_date, period, account_key: {
            "items": [
                {
                    "symbol": symbols[0],
                    "trade_time": f"{trade_date} 09:31:00",
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.0,
                    "volume": 100,
                    "amount": 1000.0,
                },
                {
                    "symbol": symbols[1],
                    "trade_time": f"{trade_date} 09:31:00",
                    "open": 2.0,
                    "high": 2.1,
                    "low": 1.9,
                    "close": 2.0,
                    "volume": 200,
                    "amount": 2000.0,
                },
            ],
            "symbol_errors": {},
        },
    )
    monkeypatch.setattr(
        qmt_market_data_service,
        "_upsert_intraday_rows",
        lambda table_name, rows: inserted_tables.append((table_name, len(rows))) or len(rows),
    )

    result = qmt_market_data_service.sync_index_minute_history(
        start_date="2026-04-27",
        end_date="2026-04-28",
        symbols=["000300.SH", "399001.SZ"],
        progress_callback=lambda value, message: progress.append((value, message)),
    )

    assert result["success"] is True
    assert result["rows"] == 4
    assert result["day_rows"] == {"2026-04-27": 2, "2026-04-28": 2}
    assert result["missing_symbols"] == []
    assert inserted_tables == [("index_minute_kline", 2), ("index_minute_kline", 2)]
    assert progress[-1][0] == 100


def test_sync_index_minute_history_reports_missing_index_symbols(monkeypatch):
    monkeypatch.setattr(
        qmt_market_data_service,
        "_load_cn_trade_dates",
        lambda: ([date(2026, 4, 28)], None),
    )
    monkeypatch.setattr(
        qmt_market_data_service,
        "_fetch_intraday_payload_safe",
        lambda symbols, trade_date, period, account_key: {
            "items": [
                {
                    "symbol": "000300.SH",
                    "trade_time": f"{trade_date} 09:31:00",
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.0,
                    "volume": 100,
                    "amount": 1000.0,
                }
            ],
            "symbol_errors": {},
        },
    )
    monkeypatch.setattr(qmt_market_data_service, "_upsert_intraday_rows", lambda table_name, rows: len(rows))

    result = qmt_market_data_service.sync_index_minute_history(
        start_date="2026-04-28",
        end_date="2026-04-28",
        symbols=["000300.SH", "399001.SZ", "000001.SZ"],
    )

    assert result["success"] is True
    assert result["rows"] == 1
    assert result["symbols"] == ["000300.SH", "399001.SZ"]
    assert result["missing_symbols"] == ["399001.SZ"]


def test_sync_index_daily_history_uses_market_page_presets_with_akshare(monkeypatch):
    captured_rows = []

    fake_ak = types.SimpleNamespace(
        index_zh_a_hist=lambda symbol, period, start_date, end_date: pd.DataFrame(
            [
                {
                    "日期": "2026-04-28",
                    "开盘": 1.0,
                    "最高": 1.1,
                    "最低": 0.9,
                    "收盘": 1.0,
                    "成交量": 100,
                    "成交额": 1000.0,
                }
            ]
        ),
        stock_zh_index_daily=lambda symbol: pd.DataFrame(),
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    def fake_upsert(rows):
        captured_rows.extend(rows)
        return len(rows)

    monkeypatch.setattr(qmt_market_data_service, "_upsert_index_daily_rows", fake_upsert)

    payload = qmt_market_data_service.sync_index_daily_history(
        start_date="2026-04-28",
        end_date="2026-04-28",
        data_source="akshare",
    )

    assert payload["success"] is True
    assert payload["rows"] == 8
    assert payload["symbols"] == [item["symbol"] for item in qmt_market_data_service.get_index_presets()]
    assert {row["symbol"] for row in captured_rows} == set(payload["symbols"])
    assert "000300.SH" in payload["symbols"]
    assert "000300.SZ" not in payload["symbols"]


def test_sync_index_daily_history_normalizes_plain_index_codes(monkeypatch):
    monkeypatch.setattr(
        qmt_market_data_service,
        "_fetch_daily_rows_safe",
        lambda symbols, start_date, end_date, account_key, db=None, user_id=None: [
            {
                "symbol": symbols[0],
                "trade_date": "2026-04-28",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.0,
                "volume": 100,
                "amount": 1000.0,
            }
        ],
    )
    monkeypatch.setattr(qmt_market_data_service, "_upsert_index_daily_rows", lambda rows: len(rows))

    payload = qmt_market_data_service.sync_index_daily_history(
        start_date="2026-04-28",
        end_date="2026-04-28",
        symbols=["000300", "399006"],
    )

    assert payload["symbols"] == ["000300.SH", "399006.SZ"]
    assert payload["missing_symbols"] == ["399006.SZ"]
