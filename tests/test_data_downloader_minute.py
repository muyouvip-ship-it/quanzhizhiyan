from __future__ import annotations

import asyncio
from datetime import date

import pandas as pd

from api import data_downloader
from api.data_downloader import DataDownloader


def test_eastmoney_minute_frame_parses_trends(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "trends": [
                        "2026-06-02 09:30,10.00,10.10,10.20,9.90,1000,100000.0,10.05",
                        "2026-06-02 09:31,10.10,10.12,10.18,10.05,1200,122000.0,10.09",
                    ]
                }
            }

    class FakeSession:
        trust_env = True

        def get(self, url, params, headers, timeout):
            assert params["secid"] == "1.600584"
            assert self.trust_env is False
            return FakeResponse()

    monkeypatch.setattr(data_downloader.requests, "Session", lambda: FakeSession())

    frame = data_downloader._fetch_eastmoney_minute_frame("600584.SH", ndays=1)

    assert list(frame.columns) == ["时间", "开盘", "收盘", "最高", "最低", "成交量", "成交额"]
    assert len(frame) == 2
    assert frame.iloc[0]["时间"] == "2026-06-02 09:30"


def test_download_minute_kline_falls_back_to_eastmoney_and_publishes(monkeypatch):
    published: list[tuple[date, list[str]]] = []
    ingested: list[dict] = []

    def fake_akshare(*args, **kwargs):
        raise RuntimeError("akshare proxy failed")

    def fake_eastmoney(symbol, *, ndays):
        assert symbol == "600584.SH"
        return pd.DataFrame(
            [
                {
                    "时间": "2026-06-02 09:30",
                    "开盘": "10.00",
                    "收盘": "10.10",
                    "最高": "10.20",
                    "最低": "9.90",
                    "成交量": "1000",
                    "成交额": "100000.0",
                }
            ]
        )

    def fake_ingest(source, rows):
        ingested.extend(rows)
        assert source == "akshare"
        return {"success": True, "rows": len(rows), "trade_dates": [date(2026, 6, 2)]}

    monkeypatch.setattr(data_downloader.ak, "stock_zh_a_hist_min_em", fake_akshare)
    monkeypatch.setattr(data_downloader, "_fetch_eastmoney_minute_frame", fake_eastmoney)
    monkeypatch.setattr(data_downloader, "ingest_raw_minute_rows", fake_ingest)
    monkeypatch.setattr(
        data_downloader,
        "publish_minute_trade_date",
        lambda trade_date, symbols, minimum_coverage_ratio=0.0: published.append((trade_date, symbols)),
    )
    async def no_sleep(seconds):
        return None

    monkeypatch.setattr(data_downloader.asyncio, "sleep", no_sleep)

    result = asyncio.run(
        DataDownloader(None).download_minute_kline(
            "600584.SH",
            date(2026, 6, 2),
            date(2026, 6, 2),
            source="akshare",
        )
    )

    assert result == {"success": True, "records": 1, "source": "akshare"}
    assert ingested[0]["symbol"] == "600584.SH"
    assert published == [(date(2026, 6, 2), ["600584.SH"])]
