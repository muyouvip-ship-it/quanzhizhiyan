from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import bindparam, text

from api.core.env import load_project_env
from api.database import SessionLocal
from api.services.market_data_pipeline_service import ingest_raw_daily_rows, reconcile_daily_trade_dates
from tradingagents.dataflows.trade_calendar import is_cn_trading_day


EASTMONEY_DAILY_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
TENCENT_DAILY_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
EASTMONEY_FIELDS1 = "f1,f2,f3,f4,f5,f6"
EASTMONEY_FIELDS2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116"
EASTMONEY_UT = "7eea3edcaed734bea9cbfc24409ed989"


def _normalize_symbol(symbol: str) -> str:
    text_value = str(symbol or "").strip().upper()
    if "." in text_value:
        return text_value
    if len(text_value) == 6 and text_value.isdigit():
        if text_value.startswith(("4", "8")):
            return f"{text_value}.BJ"
        if text_value.startswith(("5", "6", "9")):
            return f"{text_value}.SH"
        return f"{text_value}.SZ"
    return text_value


def _stock_code(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    return normalized.split(".", 1)[0] if "." in normalized else normalized


def _eastmoney_secid(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    code = _stock_code(normalized)
    if normalized.endswith(".SH") or code.startswith(("5", "6", "9")):
        return f"1.{code}"
    return f"0.{code}"


def _tencent_symbol(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    code = _stock_code(normalized)
    if normalized.endswith(".SH") or code.startswith(("5", "6", "9")):
        return f"sh{code}"
    if normalized.endswith(".BJ") or code.startswith(("4", "8")):
        return f"bj{code}"
    return f"sz{code}"


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value or text_value in {"-", "None", "nan"}:
        return None
    try:
        return float(text_value)
    except (TypeError, ValueError):
        return None


def _parse_eastmoney_kline_payload(symbol: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") or {}
    klines = data.get("klines") or []
    rows: list[dict[str, Any]] = []
    for raw_line in klines:
        parts = str(raw_line or "").split(",")
        if len(parts) < 11:
            continue
        trade_date = datetime.strptime(parts[0], "%Y-%m-%d").date()
        open_price = _safe_float(parts[1])
        close_price = _safe_float(parts[2])
        high_price = _safe_float(parts[3])
        low_price = _safe_float(parts[4])
        volume_hands = _safe_float(parts[5])
        amount = _safe_float(parts[6])
        change_value = _safe_float(parts[9])
        pre_close = close_price - change_value if close_price is not None and change_value is not None else None
        if open_price is None or close_price is None or high_price is None or low_price is None:
            continue
        rows.append(
            {
                "symbol": _normalize_symbol(symbol),
                "trade_date": trade_date,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                # Eastmoney daily kline volume is in hands; the existing final table stores shares.
                "volume": volume_hands * 100 if volume_hands is not None else None,
                "amount": amount,
                "turnover_rate": _safe_float(parts[10]),
                "pre_close": pre_close,
            }
        )
    return rows


def _parse_tencent_kline_payload(
    symbol: str,
    raw_text: str,
    *,
    start_date: date,
    end_date: date,
    adjust: str,
) -> list[dict[str, Any]]:
    json_start = raw_text.find("={")
    json_text = raw_text[json_start + 1:] if json_start >= 0 else raw_text
    payload = json.loads(json_text)
    data = payload.get("data") or {}
    tencent_symbol = _tencent_symbol(symbol)
    symbol_payload = data.get(tencent_symbol) or {}
    kline_key = f"{adjust}day" if adjust else "day"
    klines = symbol_payload.get(kline_key) or symbol_payload.get("qfqday") or symbol_payload.get("day") or []

    rows: list[dict[str, Any]] = []
    previous_close: float | None = None
    for parts in klines:
        if not isinstance(parts, list) or len(parts) < 6:
            continue
        trade_date = datetime.strptime(str(parts[0]), "%Y-%m-%d").date()
        open_price = _safe_float(parts[1])
        close_price = _safe_float(parts[2])
        high_price = _safe_float(parts[3])
        low_price = _safe_float(parts[4])
        volume_hands = _safe_float(parts[5])
        turnover_rate = _safe_float(parts[7]) if len(parts) > 7 else None
        amount_wan = _safe_float(parts[8]) if len(parts) > 8 else None
        if open_price is None or close_price is None or high_price is None or low_price is None:
            continue
        pre_close = previous_close
        previous_close = close_price
        if trade_date < start_date or trade_date > end_date:
            continue
        rows.append(
            {
                "symbol": _normalize_symbol(symbol),
                "trade_date": trade_date,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                # Tencent daily kline volume is in hands and amount is in 10k CNY.
                "volume": volume_hands * 100 if volume_hands is not None else None,
                "amount": amount_wan * 10000 if amount_wan is not None else None,
                "turnover_rate": turnover_rate,
                "pre_close": pre_close,
            }
        )
    return rows


def _curl_json(url: str, params: dict[str, str], *, timeout_seconds: int) -> dict[str, Any]:
    command = ["curl", "-sS", "--noproxy", "*", "--get", url]
    for key, value in params.items():
        command.extend(["--data-urlencode", f"{key}={value}"])
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"curl exited {result.returncode}").strip())
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid json: {result.stdout[:200]}") from exc


def _curl_text(url: str, params: dict[str, str], *, timeout_seconds: int) -> str:
    command = ["curl", "-sS", "--noproxy", "*", "--get", url]
    for key, value in params.items():
        command.extend(["--data-urlencode", f"{key}={value}"])
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"curl exited {result.returncode}").strip())
    return result.stdout


def _curl_post_json(
    url: str,
    payload: dict[str, Any],
    *,
    timeout_seconds: int,
    token: str | None = None,
) -> dict[str, Any]:
    command = [
        "curl",
        "-sS",
        "-H",
        "Content-Type: application/json",
        "-X",
        "POST",
        url,
        "-d",
        json.dumps(payload, ensure_ascii=False),
    ]
    if token:
        command[2:2] = ["-H", f"Authorization: Bearer {token}"]
    result = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"curl exited {result.returncode}").strip())
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid json: {result.stdout[:200]}") from exc


def _fetch_symbol_daily_rows(
    symbol: str,
    *,
    start_date: date,
    end_date: date,
    timeout_seconds: int,
    retries: int,
) -> tuple[str, list[dict[str, Any]], str | None]:
    params = {
        "fields1": EASTMONEY_FIELDS1,
        "fields2": EASTMONEY_FIELDS2,
        "ut": EASTMONEY_UT,
        "klt": "101",
        "fqt": "1",
        "secid": _eastmoney_secid(symbol),
        "beg": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
    }
    last_error: str | None = None
    for attempt in range(max(int(retries), 1)):
        try:
            payload = _curl_json(EASTMONEY_DAILY_URL, params, timeout_seconds=timeout_seconds)
            if payload.get("rc") not in (0, "0"):
                return symbol, [], f"eastmoney rc={payload.get('rc')}"
            return symbol, _parse_eastmoney_kline_payload(symbol, payload), None
        except Exception as exc:
            last_error = str(exc)
            if attempt + 1 < retries:
                time.sleep(min(0.2 * (attempt + 1), 1.0))
    return symbol, [], last_error


def _fetch_symbol_daily_rows_tencent(
    symbol: str,
    *,
    start_date: date,
    end_date: date,
    timeout_seconds: int,
    retries: int,
    adjust: str = "qfq",
) -> tuple[str, list[dict[str, Any]], str | None]:
    tencent_symbol = _tencent_symbol(symbol)
    start_year = min(start_date.year, end_date.year)
    end_year = max(start_date.year, end_date.year)
    all_rows: list[dict[str, Any]] = []
    last_error: str | None = None
    for year in range(start_year, end_year + 1):
        params = {
            "_var": f"kline_day{adjust}{year}",
            "param": f"{tencent_symbol},day,{year}-01-01,{year + 1}-12-31,640,{adjust}",
            "r": "0.8205512681390605",
        }
        for attempt in range(max(int(retries), 1)):
            try:
                raw_text = _curl_text(TENCENT_DAILY_URL, params, timeout_seconds=timeout_seconds)
                all_rows.extend(
                    _parse_tencent_kline_payload(
                        symbol,
                        raw_text,
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                    )
                )
                last_error = None
                break
            except Exception as exc:
                last_error = str(exc)
                if attempt + 1 < retries:
                    time.sleep(min(0.2 * (attempt + 1), 1.0))
        if last_error:
            return symbol, [], last_error
    return symbol, all_rows, None


def _date_range(start_date: date, end_date: date, *, include_non_trading: bool) -> list[date]:
    values: list[date] = []
    current = start_date
    while current <= end_date:
        if include_non_trading or is_cn_trading_day(current.isoformat()):
            values.append(current)
        current += timedelta(days=1)
    return values


def _load_symbol_universe(limit: int | None = None) -> list[str]:
    with SessionLocal() as db:
        universe_row = db.execute(
            text(
                """
                SELECT trade_date, COUNT(DISTINCT symbol) AS symbol_count
                FROM stock_daily_kline
                GROUP BY trade_date
                ORDER BY symbol_count DESC, trade_date DESC
                LIMIT 1
                """
            )
        ).fetchone()
        if universe_row is None or universe_row.trade_date is None:
            return []
        statement = text(
            """
            SELECT DISTINCT symbol
            FROM stock_daily_kline
            WHERE trade_date = :trade_date
            ORDER BY symbol
            """
        )
        rows = db.execute(statement, {"trade_date": universe_row.trade_date}).scalars().all()
        print(
            f"[daily-gap] symbol_universe_date={universe_row.trade_date} symbol_count={int(universe_row.symbol_count or 0)}",
            flush=True,
        )
    symbols = [_normalize_symbol(item) for item in rows if str(item or "").strip()]
    return symbols[:limit] if limit else symbols


def _load_missing_symbols(symbols: list[str], trade_day: date, *, force: bool) -> list[str]:
    if force:
        return symbols
    normalized = [_normalize_symbol(item) for item in symbols]
    with SessionLocal() as db:
        existing = set(
            db.execute(
                text(
                    """
                    SELECT symbol
                    FROM stock_daily_kline
                    WHERE trade_date = :trade_date
                      AND symbol IN :symbols
                    """
                ).bindparams(bindparam("symbols", expanding=True)),
                {"trade_date": trade_day, "symbols": normalized},
            ).scalars().all()
        )
    return [item for item in normalized if item not in existing]


def _sync_one_window(
    *,
    start_date: date,
    end_date: date,
    symbols: list[str],
    workers: int,
    timeout_seconds: int,
    retries: int,
    batch_size: int,
    source: str,
) -> dict[str, Any]:
    fetched_rows: list[dict[str, Any]] = []
    failed: list[tuple[str, str]] = []
    processed = 0
    started = time.time()

    with ThreadPoolExecutor(max_workers=max(int(workers or 1), 1)) as executor:
        fetcher = _fetch_symbol_daily_rows_tencent if source == "tencent" else _fetch_symbol_daily_rows
        futures = [
            executor.submit(
                fetcher,
                symbol,
                start_date=start_date,
                end_date=end_date,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
            for symbol in symbols
        ]
        for future in as_completed(futures):
            symbol, rows, error = future.result()
            processed += 1
            if rows:
                fetched_rows.extend(rows)
            if error:
                failed.append((symbol, error))
            if processed % max(int(batch_size or 500), 1) == 0 or processed == len(symbols):
                elapsed = max(time.time() - started, 0.001)
                print(
                    f"[daily-gap] fetched {processed}/{len(symbols)} symbols, rows={len(fetched_rows)}, "
                    f"failed={len(failed)}, speed={processed / elapsed:.1f}/s",
                    flush=True,
                )

    if not fetched_rows:
        return {
            "success": False,
            "rows": 0,
            "failed": failed[:20],
            "error": "no rows fetched",
        }

    ingest_result = ingest_raw_daily_rows(source="akshare", rows=fetched_rows)
    if not ingest_result.get("success"):
        return {
            "success": False,
            "rows": len(fetched_rows),
            "failed": failed[:20],
            "error": ingest_result.get("error") or "ingest failed",
        }
    reconcile_result = reconcile_daily_trade_dates(trade_dates=ingest_result.get("trade_dates") or [])
    return {
        "success": True,
        "rows": len(fetched_rows),
        "trade_dates": [item.isoformat() if hasattr(item, "isoformat") else str(item) for item in ingest_result.get("trade_dates") or []],
        "failed_count": len(failed),
        "failed_sample": failed[:20],
        "ingest": ingest_result,
        "reconcile": reconcile_result,
    }


def _normalize_qmt_bridge_daily_items(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        symbol = _normalize_symbol(str(item.get("symbol") or ""))
        trade_date_raw = item.get("trade_date")
        if not symbol or not trade_date_raw:
            continue
        trade_date = date.fromisoformat(str(trade_date_raw)[:10])
        open_price = _safe_float(item.get("open"))
        high_price = _safe_float(item.get("high"))
        low_price = _safe_float(item.get("low"))
        close_price = _safe_float(item.get("close"))
        if open_price is None or high_price is None or low_price is None or close_price is None:
            continue
        volume_hands = _safe_float(item.get("volume"))
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                # QMT xtdata daily volume is in hands; the final table uses shares.
                "volume": volume_hands * 100 if volume_hands is not None else None,
                "amount": _safe_float(item.get("amount")),
            }
        )
    return rows


def _sync_one_window_from_qmt_bridge(
    *,
    trade_day: date,
    symbols: list[str],
    bridge_url: str,
    bridge_token: str | None,
    timeout_seconds: int,
    batch_size: int,
) -> dict[str, Any]:
    bridge_url = str(bridge_url or "").rstrip("/")
    if not bridge_url:
        return {"success": False, "rows": 0, "error": "qmt bridge url is empty"}

    fetched_rows: list[dict[str, Any]] = []
    failed_batches: list[dict[str, Any]] = []
    started = time.time()
    batch_size = max(int(batch_size or 50), 1)
    batches = [symbols[index:index + batch_size] for index in range(0, len(symbols), batch_size)]
    for index, batch in enumerate(batches, start=1):
        payload = {
            "symbols": batch,
            "start_date": trade_day.isoformat(),
            "end_date": trade_day.isoformat(),
        }
        try:
            response = _curl_post_json(
                f"{bridge_url}/market/daily-bars",
                payload,
                timeout_seconds=timeout_seconds,
                token=bridge_token,
            )
            rows = _normalize_qmt_bridge_daily_items(response.get("items") or [])
            fetched_rows.extend(rows)
            if not response.get("success"):
                failed_batches.append({"batch": index, "error": str(response)[:300]})
        except Exception as exc:
            failed_batches.append({"batch": index, "error": str(exc)[:300]})
        processed = min(index * batch_size, len(symbols))
        if index % 5 == 0 or index == len(batches):
            elapsed = max(time.time() - started, 0.001)
            print(
                f"[daily-gap:qmt] {trade_day} batches={index}/{len(batches)} symbols={processed}/{len(symbols)} "
                f"rows={len(fetched_rows)} failed_batches={len(failed_batches)} speed={processed / elapsed:.1f}/s",
                flush=True,
            )

    if not fetched_rows:
        return {
            "success": False,
            "rows": 0,
            "failed_batches": failed_batches[:20],
            "error": "no rows fetched from qmt bridge",
        }

    ingest_result = ingest_raw_daily_rows(source="postgresql", rows=fetched_rows)
    if not ingest_result.get("success"):
        return {
            "success": False,
            "rows": len(fetched_rows),
            "failed_batches": failed_batches[:20],
            "error": ingest_result.get("error") or "ingest failed",
        }
    reconcile_result = reconcile_daily_trade_dates(trade_dates=ingest_result.get("trade_dates") or [])
    return {
        "success": True,
        "rows": len(fetched_rows),
        "trade_dates": [item.isoformat() if hasattr(item, "isoformat") else str(item) for item in ingest_result.get("trade_dates") or []],
        "failed_batch_count": len(failed_batches),
        "failed_batches": failed_batches[:20],
        "ingest": ingest_result,
        "reconcile": reconcile_result,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing A-share daily kline rows via Eastmoney historical kline.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--timeout-seconds", type=int, default=12)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--source", choices=["eastmoney", "tencent", "qmt-bridge"], default="eastmoney")
    parser.add_argument("--bridge-url", default=os.getenv("QMT_DAILY_BRIDGE_BASE_URL") or os.getenv("QMT_HISTORY_BRIDGE_BASE_URL") or "")
    parser.add_argument("--bridge-token", default=os.getenv("QMT_DAILY_BRIDGE_TOKEN") or os.getenv("QMT_HISTORY_BRIDGE_TOKEN") or os.getenv("QMT_BRIDGE_TOKEN") or "")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--include-non-trading", action="store_true")
    return parser.parse_args()


def main() -> int:
    load_project_env()
    args = parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    if start_date > end_date:
        raise SystemExit("--start-date must be before or equal to --end-date")

    symbols = [_normalize_symbol(item) for item in (args.symbols or []) if str(item or "").strip()]
    if not symbols:
        symbols = _load_symbol_universe(limit=args.limit or None)
    if not symbols:
        raise SystemExit("no symbols resolved from arguments or stock_daily_kline latest universe")

    results: dict[str, Any] = {"windows": []}
    for trade_day in _date_range(start_date, end_date, include_non_trading=args.include_non_trading):
        missing_symbols = _load_missing_symbols(symbols, trade_day, force=bool(args.force))
        print(f"[daily-gap] {trade_day} target_symbols={len(symbols)} missing_symbols={len(missing_symbols)}", flush=True)
        if not missing_symbols:
            results["windows"].append({"trade_date": trade_day.isoformat(), "success": True, "rows": 0, "skipped": True})
            continue
        if args.source == "qmt-bridge":
            result = _sync_one_window_from_qmt_bridge(
                trade_day=trade_day,
                symbols=missing_symbols,
                bridge_url=args.bridge_url,
                bridge_token=args.bridge_token,
                timeout_seconds=args.timeout_seconds,
                batch_size=args.batch_size,
            )
        else:
            result = _sync_one_window(
                start_date=trade_day,
                end_date=trade_day,
                symbols=missing_symbols,
                workers=args.workers,
                timeout_seconds=args.timeout_seconds,
                retries=args.retries,
                batch_size=args.batch_size,
                source=args.source,
            )
        result["trade_date"] = trade_day.isoformat()
        results["windows"].append(result)
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)

    print(json.dumps(results, ensure_ascii=False, default=str), flush=True)
    return 0 if all(item.get("success") for item in results["windows"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
