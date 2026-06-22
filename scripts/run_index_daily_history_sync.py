from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.core.env import load_project_env
from api.database import init_db
from api.services.qmt_market_data_service import sync_index_daily_history
from tradingagents.dataflows.trade_calendar import CN_TZ


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步市场页主要指数历史日K到 PostgreSQL index_daily_kline")
    parser.add_argument("--start-date", default="1990-12-19", help="开始日期 YYYY-MM-DD，默认取上证指数起始日")
    parser.add_argument("--end-date", default=None, help="结束日期 YYYY-MM-DD，默认今天")
    parser.add_argument("--symbols", nargs="*", default=None, help="可选：指定指数代码列表，默认市场页全部指数")
    parser.add_argument("--data-source", default="akshare", choices=("qmt", "akshare"), help="日K数据源")
    parser.add_argument("--account-key", default=None, help="可选：QMT 账号配置 key，仅 data-source=qmt 时使用")
    return parser.parse_args()


def main() -> int:
    load_project_env()
    init_db()
    args = parse_args()
    end_date = args.end_date or datetime.now(CN_TZ).date().isoformat()
    started_at = datetime.now(CN_TZ).isoformat()
    print(
        f"[index-daily-sync] started_at={started_at} source={args.data_source} "
        f"start={args.start_date} end={end_date}",
        flush=True,
    )

    def progress(progress_value: int, message: str) -> None:
        print(f"[index-daily-sync] progress={progress_value} message={message}", flush=True)

    payload = sync_index_daily_history(
        start_date=args.start_date,
        end_date=end_date,
        symbols=args.symbols,
        account_key=args.account_key,
        data_source=args.data_source,
        progress_callback=progress,
    )
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return 0 if payload.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
