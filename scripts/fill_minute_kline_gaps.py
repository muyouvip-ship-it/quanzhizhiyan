#!/usr/bin/env python3
"""
增量补齐股票 1 分钟 K 线近期数据。

扫描 stock_daily_kline 与 stock_minute_kline 的 (symbol, date) 缺口，
通过通达信 pytdx 批量拉取补齐。

用法:
  python scripts/fill_minute_kline_gaps.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--workers 8] [--limit N]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from multiprocessing import Pool
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from api.core.env import load_project_env
except Exception:
    def load_project_env() -> None:
        return None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fill_minute_kline_gaps.log", encoding="utf-8")],
)
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://wolf@/trading_agents?host=/tmp")
TDX_SERVERS = [("180.153.18.170", 7709), ("180.153.18.171", 7709),
               ("202.108.253.130", 7709), ("202.108.253.131", 7709)]
CATEGORY_1MIN = 8


def normalize_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if "." in s:
        return s
    if len(s) == 6 and s.isdigit():
        if s.startswith(("4", "8", "9", "92")):
            return f"{s}.BJ"
        if s.startswith("6"):
            return f"{s}.SH"
        return f"{s}.SZ"
    return s


def find_gap_pairs(start_date: date, end_date: date, *, min_bars: int = 240) -> list[tuple[str, date, int]]:
    """返回需要补齐的 (normalized_symbol, trade_date, market_code)。market: 0=SZ, 1=SH, 2=BJ"""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT d.symbol, d.trade_date
        FROM stock_daily_kline d
        LEFT JOIN (
            SELECT symbol, trade_time::date AS td, COUNT(*) AS bar_count FROM stock_minute_kline
            WHERE trade_time >= %s AND trade_time < %s
            GROUP BY symbol, trade_time::date
        ) m ON m.symbol = d.symbol AND m.td = d.trade_date
        WHERE d.trade_date >= %s AND d.trade_date <= %s
          AND COALESCE(m.bar_count, 0) < %s
        ORDER BY d.symbol, d.trade_date
    """, (start_date, end_date + timedelta(days=1), start_date, end_date, min_bars))
    rows_raw = cur.fetchall()
    cur.close()
    conn.close()

    result: list[tuple[str, date, int]] = []
    seen = set()
    for sym_raw, td in rows_raw:
        sym = normalize_symbol(sym_raw)
        if not sym or (sym, td) in seen:
            continue
        seen.add((sym, td))
        if sym.endswith(".BJ"):
            result.append((sym, td, 2))
        elif sym.endswith(".SH"):
            result.append((sym, td, 1))
        else:
            result.append((sym, td, 0))
    return result


def fetch_tdx_bars(market: int, code: str, num_pages: int = 5) -> list[tuple]:
    from pytdx.hq import TdxHq_API
    api = TdxHq_API()
    ok = api.connect(TDX_SERVERS[0][0], TDX_SERVERS[0][1])
    if not ok:
        raise RuntimeError("TDX connect failed")
    try:
        rows: list[tuple] = []
        for page in range(num_pages):
            data = api.get_security_bars(CATEGORY_1MIN, market, code, page * 800, 800)
            if not data:
                break
            for bar in data:
                tt = datetime(bar["year"], bar["month"], bar["day"], bar["hour"], bar["minute"])
                if market == 1:
                    suffix = "SH"
                elif market == 2:
                    suffix = "BJ"
                else:
                    suffix = "SZ"
                rows.append((f"{code}.{suffix}", tt, float(bar["open"]), float(bar["high"]),
                             float(bar["low"]), float(bar["close"]), int(bar["vol"]), float(bar["amount"])))
            if len(data) < 800:
                break
        return rows
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def process_gap_stock(args: tuple) -> tuple[str, int, int, str]:
    """args: (symbol, market, start_dt_str, end_dt_str, pages)"""
    symbol, market, start_dt_str, end_dt_str, pages = args
    conn = None
    try:
        code = symbol.split(".")[0]

        bars = fetch_tdx_bars(market, code, num_pages=pages)
        if not bars:
            return symbol, 0, 0, "no_data"

        start_dt = datetime.fromisoformat(start_dt_str)
        end_dt = datetime.fromisoformat(end_dt_str) + timedelta(days=1)
        filtered = [(sym, tt, o, h, l, c, v, a) for (sym, tt, o, h, l, c, v, a) in bars if start_dt <= tt < end_dt]
        if not filtered:
            return symbol, 0, 0, "no_missing_in_window"

        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        execute_values(cur, """INSERT INTO stock_minute_kline (symbol,trade_time,open,high,low,close,volume,amount)
            VALUES %s ON CONFLICT DO NOTHING""", filtered, page_size=5000)
        conn.commit()
        cur.close()
        conn.close()
        return symbol, len(filtered), 0, ""

    except Exception as e:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        return symbol, 0, 1, str(e)[:120]


def main() -> int:
    load_project_env()
    parser = argparse.ArgumentParser(description="增量补齐股票 1 分钟 K 线")
    parser.add_argument("--start-date", default="2026-05-20")
    parser.add_argument("--end-date", default="2026-06-05")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0, help="测试限制股票数")
    parser.add_argument("--min-bars", type=int, default=240, help="低于该分钟数视为缺口，默认 240")
    parser.add_argument("--pages", type=int, default=int(os.getenv("MINUTE_GAP_FILLER_TDX_PAGES", "5") or 5), help="TDX 每只股票拉取页数，每页 800 根")
    args = parser.parse_args()

    start_d = date.fromisoformat(args.start_date)
    end_d = date.fromisoformat(args.end_date)

    gap_pairs = find_gap_pairs(start_d, end_d, min_bars=max(int(args.min_bars or 240), 1))
    log.info("缺口 (symbol, trade_date) 对总数: %s", len(gap_pairs))

    tasks_map: OrderedDict[str, dict] = OrderedDict()
    for sym, td, mkt in gap_pairs:
        if sym not in tasks_map:
            tasks_map[sym] = {"symbol": sym, "market": mkt, "dates": []}
        tasks_map[sym]["dates"].append(td)

    symbols_needed = list(tasks_map.keys())
    if args.limit > 0:
        symbols_needed = symbols_needed[:args.limit]
    log.info("需要补齐的股票数: %s (limit=%s)", len(symbols_needed), args.limit or "无限制")

    tasks = [(sym, info["market"], start_d.isoformat(), end_d.isoformat(), max(int(args.pages or 5), 1))
             for sym, info in ((s, tasks_map[s]) for s in symbols_needed)]

    total_inserted = 0
    total_errors = 0
    started = time.time()

    with Pool(processes=args.workers) as pool:
        for i, (sym, inserted, errs, msg) in enumerate(pool.imap_unordered(process_gap_stock, tasks)):
            total_inserted += inserted
            total_errors += errs
            if (i + 1) % 20 == 0 or i == len(tasks) - 1:
                elapsed = time.time() - started
                pct = (i + 1) / len(tasks)
                eta = elapsed / pct * (1 - pct) if pct > 0 else 0
                log.info("进度: %d/%d (%.1f%%) 已插入 %d 行 耗时 %.1fmin 预计剩余 %.1fmin",
                         i + 1, len(tasks), pct * 100, total_inserted, elapsed / 60, eta / 60)

    log.info("完成! 总计插入 %d 行, 错误 %d, 耗时 %.1fmin", total_inserted, total_errors, (time.time() - started) / 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
