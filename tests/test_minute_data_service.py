from __future__ import annotations

import pandas as pd

from api.services import minute_data_service
from api.services.minute_data_service import _aggregate_minute_frame


def _minute_frame(rows: list[tuple[str, float, float, float, float, int, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "300520.SZ",
                "trade_time": trade_time,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
            }
            for trade_time, open_, high, low, close, volume, amount in rows
        ]
    )


def test_aggregate_minute_frame_keeps_closed_five_minute_bar():
    frame = _minute_frame(
        [
            ("2026-04-27 14:16:00", 34.91, 34.94, 34.90, 34.94, 100, 349400.0),
            ("2026-04-27 14:17:00", 34.95, 34.95, 34.93, 34.93, 100, 349300.0),
            ("2026-04-27 14:18:00", 34.94, 34.94, 34.91, 34.91, 100, 349100.0),
            ("2026-04-27 14:19:00", 34.92, 34.92, 34.88, 34.88, 100, 348800.0),
            ("2026-04-27 14:20:00", 34.87, 34.87, 34.83, 34.83, 100, 348300.0),
        ]
    )

    aggregated = _aggregate_minute_frame(frame, "5m")

    assert len(aggregated) == 1
    row = aggregated.iloc[0]
    assert str(row["bar_end"]) == "2026-04-27 14:20:00"
    assert row["open"] == 34.91
    assert row["high"] == 34.95
    assert row["low"] == 34.83
    assert row["close"] == 34.83


def test_aggregate_minute_frame_drops_incomplete_current_five_minute_bar():
    frame = _minute_frame(
        [
            ("2026-04-27 14:16:00", 34.91, 34.94, 34.90, 34.94, 100, 349400.0),
            ("2026-04-27 14:17:00", 34.95, 34.95, 34.93, 34.93, 100, 349300.0),
            ("2026-04-27 14:18:00", 34.94, 34.94, 34.91, 34.91, 100, 349100.0),
            ("2026-04-27 14:19:00", 34.92, 34.92, 34.88, 34.88, 100, 348800.0),
            ("2026-04-27 14:20:00", 34.87, 34.87, 34.83, 34.83, 100, 348300.0),
            ("2026-04-27 14:21:00", 34.84, 34.86, 34.84, 34.85, 87, 303145.0),
        ]
    )

    aggregated = _aggregate_minute_frame(frame, "5m")

    assert len(aggregated) == 1
    assert str(aggregated.iloc[0]["bar_end"]) == "2026-04-27 14:20:00"


def test_aggregate_minute_frame_supports_sixty_minute_bar():
    frame = _minute_frame(
        [
            ("2026-04-27 09:31:00", 10.0, 10.2, 9.9, 10.1, 100, 1010.0),
            ("2026-04-27 09:59:00", 10.1, 10.3, 10.0, 10.2, 120, 1224.0),
            ("2026-04-27 10:00:00", 10.2, 10.4, 10.1, 10.3, 130, 1339.0),
        ]
    )

    aggregated = _aggregate_minute_frame(frame, "60m")

    assert len(aggregated) == 1
    row = aggregated.iloc[0]
    assert str(row["bar_end"]) == "2026-04-27 10:00:00"
    assert row["open"] == 10.0
    assert row["close"] == 10.3


def test_load_aggregated_minute_bars_uses_synthetic_fallback_when_source_missing(monkeypatch):
    monkeypatch.setattr(minute_data_service, "_try_load_minute_frame", lambda symbols, trade_date: None)
    monkeypatch.setattr(minute_data_service, "_write_minute_cache", lambda *args, **kwargs: {})

    result = minute_data_service.load_aggregated_minute_bars(
        symbols=["300520.SZ"],
        trade_date="2026-04-27",
        timeframe="5m",
    )

    assert result.source == "synthetic:fallback"
    assert result.items
    assert result.missing_symbols == []
