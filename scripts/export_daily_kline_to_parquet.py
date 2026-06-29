from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.core.env import load_project_env
from api.services.daily_kline_parquet_store import get_daily_kline_parquet_root, write_daily_kline_parquet_cache


def export_daily_kline_to_parquet(
    *,
    start_date: str | None,
    end_date: str | None,
    batch_days: int,
    root: Path | None = None,
) -> dict[str, Any]:
    load_project_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL 未配置，无法从 PostgreSQL 导出日 K。")

    root = root or get_daily_kline_parquet_root()
    engine = create_engine(database_url)
    try:
        bounds = _resolve_bounds(engine, start_date=start_date, end_date=end_date)
        if bounds["start_date"] is None or bounds["end_date"] is None:
            return {"row_count": 0, "file_count": 0, "root": str(root), "message": "stock_daily_kline 无可导出数据"}

        started = time.perf_counter()
        total_rows = 0
        written_paths: set[str] = set()
        batch_days = max(int(batch_days), 1)
        window_start = bounds["start_date"]
        while window_start <= bounds["end_date"]:
            window_end = min(
                (pd.Timestamp(window_start) + pd.Timedelta(days=batch_days - 1)).date(),
                bounds["end_date"],
            )
            with engine.connect() as conn:
                frame = pd.read_sql_query(
                    text(
                        """
                        SELECT symbol, trade_date AS date, open, high, low, close, volume, amount,
                               turnover_rate, pre_close, float_market_cap, total_market_cap, net_profit_ttm
                        FROM stock_daily_kline
                        WHERE trade_date >= :start_date
                          AND trade_date <= :end_date
                        ORDER BY trade_date, symbol
                        """
                    ),
                    conn,
                    params={"start_date": window_start, "end_date": window_end},
                )
            if frame.empty:
                window_start = (pd.Timestamp(window_end) + pd.Timedelta(days=1)).date()
                continue
            total_rows += len(frame)
            written = write_daily_kline_parquet_cache(frame, root=root)
            if written:
                written_paths.update(path for path in written.split(",") if path)
            window_start = (pd.Timestamp(window_end) + pd.Timedelta(days=1)).date()

        return {
            "row_count": total_rows,
            "file_count": len(written_paths),
            "root": str(root),
            "start_date": str(bounds["start_date"]),
            "end_date": str(bounds["end_date"]),
            "elapsed_seconds": round(time.perf_counter() - started, 4),
            "files": sorted(written_paths),
        }
    finally:
        engine.dispose()


def _resolve_bounds(engine, *, start_date: str | None, end_date: str | None) -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
                FROM stock_daily_kline
                """
            )
        ).mappings().first()
    if not row or row["min_date"] is None:
        return {"start_date": None, "end_date": None}
    return {
        "start_date": pd.to_datetime(start_date or row["min_date"]).date(),
        "end_date": pd.to_datetime(end_date or row["max_date"]).date(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="导出 stock_daily_kline 到按年 Parquet 缓存。")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-days", type=int, default=180)
    parser.add_argument("--root", default=None)
    args = parser.parse_args()

    result = export_daily_kline_to_parquet(
        start_date=args.start_date,
        end_date=args.end_date,
        batch_days=args.batch_days,
        root=Path(args.root) if args.root else None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
