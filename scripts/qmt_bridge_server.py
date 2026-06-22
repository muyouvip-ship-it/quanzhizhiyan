from __future__ import annotations

import json
import math
import os
import queue
import re
import subprocess
import sys
import threading
import time
import atexit
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field


app = FastAPI(title="QMT Bridge Server", version="1.0.0")
_SECURITY_NAME_CACHE: dict[str, str] = {}
_HISTORY_JOBS: dict[str, dict[str, Any]] = {}
_HISTORY_JOBS_LOCK = threading.Lock()
_TRADER_CACHE: dict[str, dict[str, Any]] = {}
_TRADER_CACHE_LOCK = threading.RLock()
_QUOTE_SUBSCRIPTIONS: set[tuple[str, str]] = set()
_QUOTE_SUBSCRIPTIONS_LOCK = threading.RLock()


def _log(message: str) -> None:
    print(f"[qmt-bridge] {message}", flush=True)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_safe(item())
        except Exception:
            pass
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            pass
    return str(value)


class OrderSubmitRequest(BaseModel):
    account_id: str = Field(..., min_length=1)
    account_type: str = Field(default="STOCK", min_length=1)
    account_key: str | None = None
    symbol: str = Field(..., min_length=1)
    side: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    price: float | None = Field(default=None, gt=0)
    price_type: str = Field(default="limit", min_length=1)
    strategy_name: str | None = None
    order_remark: str | None = None


class HistoryMinuteSyncRequest(BaseModel):
    period: str = Field(default="1m", min_length=1)
    start_date: str = Field(..., min_length=10)
    end_date: str = Field(..., min_length=10)
    sector: str = Field(default="all_a", min_length=1)
    symbols: list[str] = Field(default_factory=list)
    output_root: str | None = None
    file_format: str = Field(default="parquet", pattern="^(parquet|csv)$")
    import_db: bool = True
    skip_export: bool = False
    database_url: str | None = None
    force: bool = False
    window_days: int = Field(default=365, ge=1)
    retry_times: int = Field(default=2, ge=0)
    retry_sleep: float = Field(default=1.0, ge=0)


class MinuteBarsRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    trade_date: str = Field(..., min_length=10)
    period: str = Field(default="1m", min_length=2)


class QuoteRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)


class DailyBarsRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    start_date: str = Field(..., min_length=10)
    end_date: str = Field(..., min_length=10)


def _bridge_token() -> str:
    return str(os.getenv("QMT_BRIDGE_TOKEN") or "").strip()


def _require_token(authorization: str | None) -> None:
    expected = _bridge_token()
    if not expected:
        return
    token = str(authorization or "").removeprefix("Bearer ").strip()
    if token != expected:
        raise HTTPException(status_code=401, detail="bridge token invalid")


def _bridge_role() -> str:
    role = str(os.getenv("QMT_BRIDGE_ROLE") or "").strip().lower()
    if role:
        return role
    return "live" if str(os.getenv("QMT_BRIDGE_PORT") or "").strip() == "8711" else "paper"


def _bridge_trading_allowed() -> bool:
    raw = os.getenv("QMT_BRIDGE_ALLOW_TRADING")
    if raw is not None:
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}
    return _bridge_role() == "paper"


def _bridge_account_key() -> str:
    return str(os.getenv("QMT_BRIDGE_ACCOUNT_KEY") or "").strip().lower()


def _account_key_role(account_key: str | None) -> str:
    key = str(account_key or "").strip().lower()
    if not key:
        return ""
    if key.startswith("live") or key.endswith("_real"):
        return "live"
    if key.startswith("paper") or "sim" in key or "demo" in key:
        return "paper"
    return ""


def _require_trading_allowed(action: str, account_key: str | None) -> None:
    role = _bridge_role()
    normalized_key = str(account_key or "").strip().lower()
    if not _bridge_trading_allowed():
        _log(f"reject trading action={action} role={role} account_key={account_key} allow={_bridge_trading_allowed()}")
        raise HTTPException(status_code=403, detail="QMT bridge is readonly for this account; trading is disabled")
    expected_key = _bridge_account_key()
    if expected_key and normalized_key and normalized_key != expected_key:
        _log(f"reject trading action={action} role={role} account_key={account_key} expected_account_key={expected_key}")
        raise HTTPException(status_code=403, detail="QMT bridge account_key mismatch; trading is disabled")
    account_role = _account_key_role(normalized_key)
    if account_role and role in {"paper", "live"} and account_role != role:
        _log(f"reject trading action={action} role={role} account_key={account_key} account_role={account_role}")
        raise HTTPException(status_code=403, detail="QMT bridge role/account_key mismatch; trading is disabled")
    if role in {"paper", "live"}:
        return
    _log(f"reject trading action={action} role={role} account_key={account_key} allow={_bridge_trading_allowed()}")
    raise HTTPException(status_code=403, detail="QMT bridge role is invalid; trading is disabled")


def _symbol_for_xt(value: str) -> str:
    symbol = _normalize_symbol(value)
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    return symbol


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    if "." in symbol:
        return symbol
    if len(symbol) == 6:
        if symbol.startswith("6"):
            return f"{symbol}.SH"
        if symbol.startswith(("0", "3")):
            return f"{symbol}.SZ"
        if symbol.startswith(("4", "8")):
            return f"{symbol}.BJ"
    return symbol


def _looks_like_symbol(value: Any) -> bool:
    text = str(value or "").strip().upper()
    return (len(text) == 6 and text.isdigit()) or (
        len(text) == 9 and text[:6].isdigit() and text[6:] in (".SH", ".SZ", ".BJ")
    )


def _query_security_name(symbol: str) -> str:
    normalized = _normalize_symbol(symbol)
    if not normalized:
        return ""
    if normalized in _SECURITY_NAME_CACHE:
        return _SECURITY_NAME_CACHE[normalized]
    name = ""
    try:
        from xtquant import xtdata

        detail = xtdata.get_instrument_detail(normalized) or {}
        if isinstance(detail, dict):
            for key in ("InstrumentName", "instrument_name", "StockName", "stock_name", "name"):
                value = str(detail.get(key) or "").strip()
                if value and not _looks_like_symbol(value):
                    name = value
                    break
    except Exception as exc:
        _log(f"query security name failed symbol={normalized}: {exc}")
    if name:
        _SECURITY_NAME_CACHE[normalized] = name
    return name


def _resolve_payload_symbol(payload: dict[str, Any]) -> str:
    return _normalize_symbol(
        payload.get("stockCode")
        or payload.get("stock_code")
        or payload.get("symbol")
        or payload.get("m_strStockCode")
    )


def _resolve_payload_name(payload: dict[str, Any], symbol: str) -> str:
    for key in (
        "stockName",
        "stock_name",
        "security_name",
        "name",
        "instrument_name",
        "InstrumentName",
        "m_strStockName",
        "m_strInstrumentName",
    ):
        value = str(payload.get(key) or "").strip()
        if value and not _looks_like_symbol(value):
            return value
    return _query_security_name(symbol) or symbol


def _enrich_security_names(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for item in items:
        payload = dict(item)
        symbol = _resolve_payload_symbol(payload)
        if symbol:
            payload.setdefault("symbol", symbol)
            payload.setdefault("stock_code", symbol)
            payload["security_name"] = _resolve_payload_name(payload, symbol)
            payload.setdefault("stockName", payload["security_name"])
        enriched.append(payload)
    return enriched


def _resolve_order_params(side: str, price_type: str, symbol: str) -> tuple[int, int]:
    try:
        from xtquant import xtconstant
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"xtconstant unavailable: {exc}") from exc

    side_key = str(side or "").strip().lower()
    if side_key in {"buy", "long_buy", "b"}:
        order_type = getattr(xtconstant, "STOCK_BUY", 23)
    elif side_key in {"sell", "long_sell", "s"}:
        order_type = getattr(xtconstant, "STOCK_SELL", 24)
    else:
        raise HTTPException(status_code=400, detail=f"unsupported side: {side}")

    price_key = str(price_type or "limit").strip().lower()
    exchange = symbol.split(".")[-1] if "." in symbol else ""
    price_type_map = {
        "limit": getattr(xtconstant, "FIX_PRICE", 11),
        "latest": getattr(xtconstant, "FIX_PRICE", 11),
        "opponent": getattr(xtconstant, "MARKET_PEER_PRICE_FIRST", getattr(xtconstant, "FIX_PRICE", 11)),
        "self_best": getattr(xtconstant, "MARKET_MINE_PRICE_FIRST", getattr(xtconstant, "FIX_PRICE", 11)),
        "best5_cancel": getattr(
            xtconstant,
            "MARKET_SH_CONVERT_5_CANCEL" if exchange == "SH" else "MARKET_SZ_CONVERT_5_CANCEL",
            getattr(xtconstant, "FIX_PRICE", 11),
        ),
    }
    if price_key not in price_type_map:
        raise HTTPException(status_code=400, detail=f"unsupported price_type: {price_type}")
    return order_type, price_type_map[price_key]


def _query_latest_price(symbol: str) -> float:
    normalized = _normalize_symbol(symbol)

    def _extract_price(payload: Any) -> float | None:
        if payload is None:
            return None
        if isinstance(payload, (int, float)):
            price = float(payload)
            return price if price > 0 else None
        if isinstance(payload, dict):
            for key in (
                "lastPrice",
                "last_price",
                "latest",
                "price",
                "last",
                "close",
                "m_dLastPrice",
                "m_dClose",
            ):
                try:
                    price = float(payload.get(key))
                except Exception:
                    continue
                if price > 0:
                    return price
            for value in payload.values():
                found = _extract_price(value)
                if found:
                    return found
        if isinstance(payload, (list, tuple)):
            for value in payload:
                found = _extract_price(value)
                if found:
                    return found
        for attr in ("iloc",):
            if hasattr(payload, attr):
                try:
                    found = _extract_price(payload.iloc[-1])
                    if found:
                        return found
                except Exception:
                    pass
        return None

    try:
        from xtquant import xtdata

        for stock in (normalized, symbol):
            ticks = xtdata.get_full_tick([stock]) or {}
            price = _extract_price(ticks.get(stock) if isinstance(ticks, dict) else ticks)
            if price:
                return price
        get_market_data = getattr(xtdata, "get_market_data", None)
        if callable(get_market_data):
            data = get_market_data(field_list=["close"], stock_list=[normalized], period="1d", count=1)
            price = _extract_price(data)
            if price:
                return price
    except Exception as exc:
        _log(f"query latest price failed symbol={normalized}: {exc}")
    raise HTTPException(status_code=400, detail=f"latest price unavailable: {normalized}")


def _build_trader(account_id: str, account_type: str):
    try:
        from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
        from xtquant.xttype import StockAccount
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"xtquant unavailable: {exc}") from exc

    userdata_path = str(os.getenv("QMT_USERDATA_PATH") or "").strip()
    if not userdata_path:
        raise HTTPException(status_code=400, detail="QMT_USERDATA_PATH is required")

    session_id = int(time.time() * 1000) % 100000000
    _log(f"create trader session={session_id} account_id={account_id} account_type={account_type}")
    trader = XtQuantTrader(userdata_path, session_id)
    account = StockAccount(account_id, account_type)

    class _Callback(XtQuantTraderCallback):
        def on_disconnected(self):
            _log("xttrader disconnected")

        def on_account_status(self, status):
            status_value = getattr(status, "status", None)
            _log(f"account status update: {status_value}")

    register_callback = getattr(trader, "register_callback", None)
    if callable(register_callback):
        _log("register callback")
        register_callback(_Callback())

    start = getattr(trader, "start", None)
    if callable(start):
        _log("start trader thread")
        start()
    _log("connect trader")
    connect_result = getattr(trader, "connect")()
    _log(f"connect result={connect_result}")
    if connect_result not in (0, None):
        raise HTTPException(status_code=502, detail=f"connect failed: {connect_result}")
    subscribe = getattr(trader, "subscribe", None)
    if callable(subscribe):
        _log("subscribe account")
        subscribe_result = subscribe(account)
        _log(f"subscribe result={subscribe_result}")
    return trader, account


def _trader_cache_key(account_id: str, account_type: str) -> str:
    return f"{str(account_id).strip()}::{str(account_type).strip().upper()}"


def _create_trader(account_id: str, account_type: str):
    key = _trader_cache_key(account_id, account_type)
    with _TRADER_CACHE_LOCK:
        cached = _TRADER_CACHE.get(key)
        if cached:
            cached["last_used_at"] = time.time()
            return cached["trader"], cached["account"], cached["lock"], key
        trader, account = _build_trader(account_id, account_type)
        entry = {
            "trader": trader,
            "account": account,
            "lock": threading.RLock(),
            "created_at": time.time(),
            "last_used_at": time.time(),
        }
        _TRADER_CACHE[key] = entry
        return trader, account, entry["lock"], key


def _stop_trader(trader: Any) -> None:
    stop = getattr(trader, "stop", None)
    if callable(stop):
        try:
            _log("stop trader")
            stop()
        except Exception:
            pass


def _dispose_trader(cache_key: str | None, trader: Any | None) -> None:
    if cache_key:
        with _TRADER_CACHE_LOCK:
            cached = _TRADER_CACHE.get(cache_key)
            if cached and cached.get("trader") is trader:
                _TRADER_CACHE.pop(cache_key, None)
    if trader is not None:
        _stop_trader(trader)


def _cleanup_all_traders() -> None:
    with _TRADER_CACHE_LOCK:
        entries = list(_TRADER_CACHE.values())
        _TRADER_CACHE.clear()
    for payload in entries:
        _stop_trader(payload.get("trader"))


atexit.register(_cleanup_all_traders)


def _query_snapshot(account_id: str, account_type: str) -> dict[str, Any]:
    trader, account, trader_lock, cache_key = _create_trader(account_id, account_type)

    positions = None
    asset = None
    orders = None
    trades = None
    try:
        with trader_lock:
            _log("query stock asset")
            asset = trader.query_stock_asset(account)
            _log("query stock positions")
            if positions in (None, []):
                positions = trader.query_stock_positions(account)
            _log("query stock orders")
            query_stock_orders = getattr(trader, "query_stock_orders", None)
            if callable(query_stock_orders):
                orders = query_stock_orders(account)
            _log("query stock trades")
            query_stock_trades = getattr(trader, "query_stock_trades", None)
            if callable(query_stock_trades):
                trades = query_stock_trades(account)
    except Exception:
        _dispose_trader(cache_key, trader)
        raise

    def normalize(item: Any) -> dict[str, Any]:
        if isinstance(item, dict):
            safe_item = _json_safe(item)
            return safe_item if isinstance(safe_item, dict) else {}
        data: dict[str, Any] = {}
        for key in dir(item):
            if key.startswith("_"):
                continue
            value = getattr(item, key, None)
            if callable(value):
                continue
            if isinstance(value, (str, int, float, bool, list, dict)) or value is None:
                data[key] = _json_safe(value)
        return data

    normalized_positions = [normalize(item) for item in (positions or [])]
    normalized_orders = [normalize(item) for item in (orders or [])]
    normalized_trades = [normalize(item) for item in (trades or [])]

    return _json_safe({
        "asset": normalize(asset),
        "positions": _enrich_security_names(normalized_positions),
        "orders": _enrich_security_names(normalized_orders),
        "trades": _enrich_security_names(normalized_trades),
    })


def _submit_order(request: OrderSubmitRequest) -> dict[str, Any]:
    symbol = _symbol_for_xt(request.symbol)
    order_type, price_mode = _resolve_order_params(request.side, request.price_type, symbol)
    price_key = str(request.price_type or "limit").strip().lower()
    order_price = request.price
    if price_key == "latest" and order_price is None:
        try:
            order_price = _query_latest_price(symbol)
            _log(f"resolved latest price symbol={symbol} price={order_price}")
        except HTTPException as exc:
            order_type, price_mode = _resolve_order_params(request.side, "opponent", symbol)
            order_price = 0.0
            _log(f"latest price unavailable, fallback to opponent price symbol={symbol} detail={exc.detail}")
    if price_key == "limit" and order_price is None:
        raise HTTPException(status_code=400, detail="limit order requires price")

    trader, account, trader_lock, cache_key = _create_trader(request.account_id, request.account_type)
    try:
        order_stock = getattr(trader, "order_stock", None)
        if not callable(order_stock):
            raise HTTPException(status_code=500, detail="xttrader.order_stock unavailable")
        _log(
            f"submit order symbol={symbol} side={request.side} qty={request.quantity} price={order_price} price_type={request.price_type} price_mode={price_mode}"
        )
        holder: dict[str, Any] = {}

        def _call_order_stock() -> None:
            try:
                with trader_lock:
                    holder["result"] = order_stock(
                        account,
                        symbol,
                        order_type,
                        int(request.quantity),
                        price_mode,
                        float(order_price or 0.0),
                        str(request.strategy_name or "CodexQmtBridge"),
                        str(request.order_remark or ""),
                    )
            except Exception as exc:
                holder["error"] = exc

        worker = threading.Thread(target=_call_order_stock, daemon=True)
        worker.start()
        worker.join(float(os.getenv("QMT_ORDER_TIMEOUT_SECONDS") or "12"))
        if worker.is_alive():
            _log(f"submit order timeout symbol={symbol} qty={request.quantity} price_type={request.price_type}")
            raise HTTPException(status_code=504, detail="xttrader.order_stock timeout; please query orders/trades to confirm")
        if holder.get("error") is not None:
            raise holder["error"]
        result = holder.get("result")
        _log(f"submit order result={result}")
        return {
            "success": True,
            "order_id": str(result),
            "result": result,
            "request": {**request.model_dump(), "resolved_price": order_price, "resolved_price_mode": price_mode},
        }
    except Exception:
        _dispose_trader(cache_key, trader)
        raise


def _cancel_order(account_id: str, account_type: str, order_id: str) -> dict[str, Any]:
    trader, account, trader_lock, cache_key = _create_trader(account_id, account_type)
    try:
        cancel_order_stock = getattr(trader, "cancel_order_stock", None)
        if not callable(cancel_order_stock):
            raise HTTPException(status_code=500, detail="xttrader.cancel_order_stock unavailable")
        cancel_arg: Any = int(order_id) if str(order_id).isdigit() else order_id
        _log(f"cancel order order_id={order_id}")
        with trader_lock:
            result = cancel_order_stock(account, cancel_arg)
        _log(f"cancel order result={result}")
        return {
            "success": True,
            "order_id": str(order_id),
            "result": result,
        }
    except Exception:
        _dispose_trader(cache_key, trader)
        raise


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _history_script_path() -> Path:
    return Path(__file__).resolve().parent / "qmt_minute_history_sync.py"


def _history_output_root(request: HistoryMinuteSyncRequest) -> str:
    return str(request.output_root or os.getenv("QMT_MINUTE_OUTPUT_ROOT") or r"D:\QMT\data\minute_history")


def _history_database_url(request: HistoryMinuteSyncRequest) -> str:
    return str(request.database_url or os.getenv("QMT_MINUTE_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()


def _update_history_job(job_id: str, **updates: Any) -> None:
    with _HISTORY_JOBS_LOCK:
        job = _HISTORY_JOBS.setdefault(job_id, {})
        job.update(updates)
        job["updated_at"] = _now_iso()


def _append_history_log(job_id: str, line: str) -> None:
    with _HISTORY_JOBS_LOCK:
        job = _HISTORY_JOBS.setdefault(job_id, {})
        logs = list(job.get("logs") or [])
        logs.append(line)
        job["logs"] = logs[-200:]
        job["updated_at"] = _now_iso()


def _handle_history_progress_line(job_id: str, line: str) -> None:
    universe_match = re.search(r"universe=(\d+)", line)
    windows_match = re.search(r"windows=(\d+)", line)
    symbol_match = re.search(r"\((\d+)/(\d+)\)\s+symbol=([A-Z0-9.]+)", line)
    if universe_match:
        total = int(universe_match.group(1))
        _update_history_job(job_id, progress=12, message=f"已解析股票池，共 {total} 只股票", universe=total)
        return
    if windows_match:
        total = int(windows_match.group(1))
        _update_history_job(job_id, progress=16, message=f"已拆分下载窗口，共 {total} 个时间窗口", windows=total)
        return
    if symbol_match:
        current = int(symbol_match.group(1))
        total = int(symbol_match.group(2))
        symbol = symbol_match.group(3)
        progress = min(88, 20 + int((current / max(total, 1)) * 68))
        _update_history_job(
            job_id,
            progress=progress,
            message=f"QMT 正在处理第 {current}/{total} 只股票：{symbol}",
            current_symbol=symbol,
            current_symbol_index=current,
            total_symbols=total,
        )
        return
    if "retry symbol=" in line:
        _update_history_job(job_id, progress=30, message=line.replace("[qmt-minute-sync] ", ""))
        return
    if "error symbol=" in line or "symbol worker timeout" in line or "exceeded failed window limit" in line:
        _update_history_job(job_id, progress=30, message=line.replace("[qmt-minute-sync] ", ""))


def _kill_process_tree(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            process.terminate()
    except Exception:
        try:
            process.kill()
        except Exception:
            return


def _pipe_reader(stream_name: str, stream, output_queue: "queue.Queue[tuple[str, str | None]]") -> None:
    try:
        for raw_line in iter(stream.readline, ""):
            if raw_line == "":
                break
            output_queue.put((stream_name, raw_line.rstrip("\r\n")))
    finally:
        try:
            stream.close()
        except Exception:
            pass
        output_queue.put((stream_name, None))


def _run_history_minute_job(job_id: str, request: HistoryMinuteSyncRequest) -> None:
    script_path = _history_script_path()
    if not script_path.exists():
        _update_history_job(job_id, status="failed", progress=0, message=f"脚本不存在：{script_path}", error=f"script not found: {script_path}", finished_at=_now_iso())
        return

    database_url = _history_database_url(request)
    if request.import_db and not database_url:
        _update_history_job(job_id, status="failed", progress=0, message="缺少 QMT_MINUTE_DATABASE_URL / DATABASE_URL，无法导入数据库", error="database_url is required", finished_at=_now_iso())
        return

    command = [
        sys.executable,
        "-u",
        str(script_path),
        "--period",
        request.period,
        "--start-date",
        request.start_date,
        "--end-date",
        request.end_date,
        "--output-root",
        _history_output_root(request),
        "--format",
        request.file_format,
        "--window-days",
        str(request.window_days),
        "--retry-times",
        str(request.retry_times),
        "--retry-sleep",
        str(request.retry_sleep),
    ]
    if request.symbols:
        command.extend(["--symbols", *request.symbols])
    else:
        command.extend(["--sector", request.sector])
    if request.import_db:
        command.extend(["--import-db", "--database-url", database_url])
    if request.skip_export:
        command.append("--skip-export")
    if request.force:
        command.append("--force")

    _update_history_job(job_id, status="running", progress=10, message="已启动 QMT 历史分钟线脚本，正在解析股票池", command=command, started_at=_now_iso())
    try:
        silence_timeout = max(float(os.getenv("QMT_HISTORY_SCRIPT_SILENCE_TIMEOUT_SECONDS", "180") or 180), 30.0)
        process = subprocess.Popen(
            command,
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        assert process.stdout is not None
        assert process.stderr is not None
        output_queue: queue.Queue[tuple[str, str | None]] = queue.Queue()
        reader_threads = [
            threading.Thread(target=_pipe_reader, args=("stdout", process.stdout, output_queue), daemon=True),
            threading.Thread(target=_pipe_reader, args=("stderr", process.stderr, output_queue), daemon=True),
        ]
        for thread in reader_threads:
            thread.start()

        open_streams = {"stdout", "stderr"}
        stderr_lines: list[str] = []
        last_output_at = time.monotonic()
        while open_streams:
            try:
                stream_name, line = output_queue.get(timeout=1.0)
            except queue.Empty:
                if process.poll() is None and (time.monotonic() - last_output_at) >= silence_timeout:
                    message = f"QMT 历史分钟线脚本静默超时 {int(silence_timeout)} 秒，已终止"
                    _append_history_log(job_id, message)
                    _kill_process_tree(process)
                    _update_history_job(job_id, status="failed", progress=0, message=message, error=message, finished_at=_now_iso())
                    return
                if process.poll() is not None and not any(thread.is_alive() for thread in reader_threads):
                    break
                continue

            if line is None:
                open_streams.discard(stream_name)
                continue

            cleaned = line.strip()
            if not cleaned:
                continue
            last_output_at = time.monotonic()
            _append_history_log(job_id, cleaned)
            _handle_history_progress_line(job_id, cleaned)
            if stream_name == "stderr":
                stderr_lines.append(cleaned)

        stderr_text = "\n".join(stderr_lines).strip()
        returncode = process.wait()
        if returncode != 0:
            message = stderr_text or f"QMT 历史分钟线脚本失败，exit_code={returncode}"
            _update_history_job(job_id, status="failed", progress=0, message=message[:500], error=message, finished_at=_now_iso())
            return
        rows_total = 0
        logs = _HISTORY_JOBS.get(job_id, {}).get("logs") or []
        for item in reversed(logs):
            try:
                payload = json.loads(item)
            except Exception:
                continue
            if isinstance(payload, dict) and payload.get("type") == "run_finished":
                rows_total = int(payload.get("imported_rows_total") or payload.get("rows_total") or 0)
                break
        _update_history_job(job_id, status="completed", progress=100, message=f"QMT 分钟线同步完成，导入/生成记录约 {rows_total} 条", rows_total=rows_total, finished_at=_now_iso())
    except Exception as exc:
        _update_history_job(job_id, status="failed", progress=0, message=f"QMT 历史分钟线任务异常：{exc}", error=str(exc), finished_at=_now_iso())


def _download_history_window_light(xtdata: Any, symbol: str, period: str, start_time: str, end_time: str) -> None:
    downloader = getattr(xtdata, "download_history_data2", None) or getattr(xtdata, "download_history_data", None)
    if downloader is None:
        raise RuntimeError("xtdata 未提供 download_history_data / download_history_data2")
    try:
        downloader(symbol, period, start_time=start_time, end_time=end_time)
    except TypeError:
        downloader(symbol, period, start_time, end_time)


def _read_history_window_light(xtdata: Any, symbol: str, period: str, start_time: str, end_time: str):
    reader = getattr(xtdata, "get_market_data_ex", None) or getattr(xtdata, "get_market_data", None)
    if reader is None:
        raise RuntimeError("xtdata 未提供 get_market_data_ex / get_market_data")
    fields = ["time", "open", "high", "low", "close", "volume", "amount"]
    try:
        return reader(
            field_list=fields,
            stock_list=[symbol],
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type="none",
            fill_data=False,
        )
    except TypeError:
        return reader(fields, [symbol], period, start_time, end_time, -1, "none", False)


def _read_full_kline_light(xtdata: Any, symbol: str, period: str, start_time: str, end_time: str):
    reader = getattr(xtdata, "get_full_kline", None)
    if reader is None:
        return None
    try:
        return reader(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[symbol],
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=-1,
            dividend_type="none",
            fill_data=False,
        )
    except TypeError:
        try:
            return reader(
                ["time", "open", "high", "low", "close", "volume", "amount"],
                [symbol],
                period,
                start_time,
                end_time,
                -1,
                "none",
                False,
            )
        except TypeError:
            return None


def _ensure_quote_subscription_light(xtdata: Any, symbol: str, period: str) -> None:
    subscriber = getattr(xtdata, "subscribe_quote", None)
    if subscriber is None:
        return
    key = (symbol, period)
    with _QUOTE_SUBSCRIPTIONS_LOCK:
        if key in _QUOTE_SUBSCRIPTIONS:
            return
    attempts = (
        lambda: subscriber(symbol, period=period, start_time="", end_time="", count=0, callback=None),
        lambda: subscriber(stock_code=symbol, period=period, start_time="", end_time="", count=0, callback=None),
        lambda: subscriber(symbol, period=period, count=0),
        lambda: subscriber(symbol, period),
        lambda: subscriber(symbol),
    )
    last_error: Exception | None = None
    for attempt in attempts:
        try:
            attempt()
            with _QUOTE_SUBSCRIPTIONS_LOCK:
                _QUOTE_SUBSCRIPTIONS.add(key)
            _log(f"subscribed quote symbol={symbol} period={period}")
            return
        except TypeError as exc:
            last_error = exc
            continue
        except Exception as exc:
            last_error = exc
            break
    if last_error is not None:
        _log(f"subscribe quote skipped symbol={symbol} period={period}: {last_error}")


def _extract_field_dict_frame_light(payload: Any, symbol: str):
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError(f"缺少 pandas: {exc}") from exc

    if not isinstance(payload, dict):
        return None
    required_fields = ["time", "open", "high", "low", "close"]
    if not any(field in payload for field in required_fields):
        return None

    def _pick_series(frame_like: Any, candidates: list[str]):
        if isinstance(frame_like, pd.Series):
            return frame_like
        if not isinstance(frame_like, pd.DataFrame):
            if isinstance(frame_like, dict):
                frame_like = pd.DataFrame(frame_like)
            else:
                return None
        if frame_like.empty:
            return None
        for candidate in candidates:
            if candidate in frame_like.index:
                return frame_like.loc[candidate]
            candidate_lower = candidate.lower()
            for index_value in frame_like.index:
                if str(index_value).strip().lower() == candidate_lower:
                    return frame_like.loc[index_value]
        if len(frame_like.index) == 1:
            return frame_like.iloc[0]
        return None

    normalized = _normalize_symbol(symbol)
    candidates = [
        normalized,
        symbol,
        normalized.split(".")[0],
        normalized.lower(),
        symbol.lower(),
        normalized.replace(".", ""),
        normalized.replace(".", "").lower(),
    ]
    time_source = payload.get("time")
    if time_source is None:
        time_source = payload.get("trade_time")
    open_source = payload.get("open")
    base_source = open_source if open_source is not None else time_source
    base_series = _pick_series(base_source, candidates)
    if base_series is None:
        return None

    columns = list(base_series.index)
    time_series = _pick_series(time_source, candidates)
    if time_series is not None and len(time_series.index) == len(columns):
        time_values = list(time_series.values)
    else:
        time_values = columns

    rows: list[dict[str, Any]] = []
    for idx, time_value in enumerate(time_values):
        row: dict[str, Any] = {"time": time_value}
        has_payload = False
        for field in ("open", "high", "low", "close", "volume", "amount"):
            series = _pick_series(payload.get(field), candidates)
            value = None
            if series is not None and idx < len(series):
                value = series.iloc[idx]
                has_payload = True
            row[field] = value
        if has_payload:
            rows.append(row)
    if not rows:
        return None
    return pd.DataFrame(rows)


def _extract_symbol_frame_light(payload: Any, symbol: str):
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError(f"缺少 pandas: {exc}") from exc

    if payload is None:
        return None
    if isinstance(payload, pd.DataFrame):
        return payload
    field_dict_frame = _extract_field_dict_frame_light(payload, symbol)
    if field_dict_frame is not None:
        return field_dict_frame
    if isinstance(payload, dict):
        candidates = [symbol, _normalize_symbol(symbol), symbol.split(".")[0], symbol.lower(), _normalize_symbol(symbol).lower()]
        for key in candidates:
            value = payload.get(key)
            if isinstance(value, pd.DataFrame):
                return value
            if isinstance(value, dict):
                return pd.DataFrame(value)
            if isinstance(value, list):
                return pd.DataFrame(value)
        if len(payload) == 1:
            only = next(iter(payload.values()))
            if isinstance(only, pd.DataFrame):
                return only
            if isinstance(only, dict):
                return pd.DataFrame(only)
            if isinstance(only, list):
                return pd.DataFrame(only)
        if any(name in payload for name in ("time", "open", "close")):
            return pd.DataFrame(payload)
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    return None


def _normalize_time_value_light(value: Any) -> datetime | None:
    try:
        import pandas as pd
    except Exception:
        pd = None

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if pd is not None and isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, (int, float)):
        number = int(value)
        digits = len(str(abs(number)))
        if digits >= 18:
            return datetime.fromtimestamp(number / 1_000_000_000.0)
        if digits >= 16:
            return datetime.fromtimestamp(number / 1_000_000.0)
        if digits >= 13:
            return datetime.fromtimestamp(number / 1000.0)
        if digits >= 10:
            return datetime.fromtimestamp(number)
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _safe_float_light(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 4)
    except Exception:
        return None


def _normalize_history_frame_light(payload: Any, symbol: str):
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError(f"缺少 pandas: {exc}") from exc

    frame = _extract_symbol_frame_light(payload, symbol)
    if frame is None:
        return pd.DataFrame(columns=["symbol", "trade_time", "open", "high", "low", "close", "volume", "amount"])
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    data = frame.copy()
    if "time" not in data.columns:
        if isinstance(data.index, pd.DatetimeIndex):
            data = data.reset_index().rename(columns={data.columns[0]: "time"})
        elif "trade_time" in data.columns:
            data["time"] = data["trade_time"]
        elif "datetime" in data.columns:
            data["time"] = data["datetime"]
        elif data.index.name:
            data = data.reset_index().rename(columns={data.index.name: "time"})
    data = data.rename(columns={
        "Time": "time",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Amount": "amount",
    })
    required = ["time", "open", "high", "low", "close", "volume", "amount"]
    for column in required:
        if column not in data.columns:
            data[column] = None
    data["trade_time"] = data["time"].map(_normalize_time_value_light)
    data["symbol"] = _normalize_symbol(symbol)
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["trade_time", "open", "high", "low", "close"], how="any")
    data["trade_time"] = pd.to_datetime(data["trade_time"]).dt.tz_localize(None)
    data["volume"] = data["volume"].fillna(0).astype("int64")
    data["amount"] = data["amount"].fillna(0.0).astype(float)
    data = data.sort_values("trade_time").drop_duplicates(["symbol", "trade_time"], keep="last")
    return data[["symbol", "trade_time", "open", "high", "low", "close", "volume", "amount"]].reset_index(drop=True)


def _normalize_quote_time_light(value: Any) -> str | None:
    parsed = _normalize_time_value_light(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_quote_item(symbol: str, payload: Any) -> dict[str, Any]:
    item = payload if isinstance(payload, dict) else {}
    price = _safe_float_light(
        item.get("lastPrice")
        or item.get("last_price")
        or item.get("price")
        or item.get("last")
        or item.get("close")
    )
    previous_close = _safe_float_light(
        item.get("lastClose")
        or item.get("last_close")
        or item.get("preClose")
        or item.get("prevClose")
        or item.get("previous_close")
    )
    change = _safe_float_light(item.get("change"))
    if change is None and price is not None and previous_close not in (None, 0):
        change = round(price - float(previous_close), 4)
    change_pct = _safe_float_light(item.get("change_pct") or item.get("pct_chg"))
    if change_pct is None and change is not None and previous_close not in (None, 0):
        change_pct = round(change / float(previous_close) * 100, 4)
    return {
        "symbol": symbol,
        "price": price,
        "open": _safe_float_light(item.get("open")),
        "high": _safe_float_light(item.get("high")),
        "low": _safe_float_light(item.get("low")),
        "previous_close": previous_close,
        "change": change,
        "change_pct": change_pct,
        "volume": _safe_float_light(item.get("volume")),
        "amount": _safe_float_light(item.get("amount") or item.get("turnover")),
        "quote_time": _normalize_quote_time_light(item.get("time") or item.get("timetag")),
        "source": "qmt_bridge",
    }


def _query_quotes(symbols: list[str]) -> dict[str, Any]:
    try:
        from xtquant import xtdata
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"实时行情能力不可用: {exc}") from exc

    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        normalized = _normalize_symbol(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            normalized_symbols.append(normalized)
    if not normalized_symbols:
        raise HTTPException(status_code=400, detail="symbols is required")

    for symbol in normalized_symbols:
        _ensure_quote_subscription_light(xtdata, symbol, "1m")

    raw = xtdata.get_full_tick(normalized_symbols) or {}
    items: list[dict[str, Any]] = []
    for symbol in normalized_symbols:
        item = raw.get(symbol) if isinstance(raw, dict) else None
        normalized_item = _normalize_quote_item(symbol, item)
        if normalized_item.get("price") is None:
            continue
        items.append(normalized_item)
    return {
        "success": True,
        "symbols": normalized_symbols,
        "items": items,
        "rows": len(items),
    }


def _query_daily_bars(symbols: list[str], start_date: str, end_date: str) -> dict[str, Any]:
    try:
        from xtquant import xtdata
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"日线能力不可用: {exc}") from exc

    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        normalized = _normalize_symbol(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            normalized_symbols.append(normalized)
    if not normalized_symbols:
        raise HTTPException(status_code=400, detail="symbols is required")

    start_day = str(start_date or "").replace("-", "").strip()
    end_day = str(end_date or "").replace("-", "").strip()
    if len(start_day) != 8 or len(end_day) != 8 or (not start_day.isdigit()) or (not end_day.isdigit()):
        raise HTTPException(status_code=400, detail="start_date / end_date 格式应为 YYYY-MM-DD")

    items: list[dict[str, Any]] = []
    symbol_rows: dict[str, int] = {}
    for symbol in normalized_symbols:
        try:
            _download_history_window_light(xtdata, symbol, "1d", f"{start_day}000000", f"{end_day}235959")
            raw = _read_history_window_light(xtdata, symbol, "1d", f"{start_day}000000", f"{end_day}235959")
            frame = _normalize_history_frame_light(raw, symbol)
            if frame.empty:
                symbol_rows[symbol] = 0
                continue
            dumped = frame.copy()
            dumped["trade_date"] = dumped["trade_time"].dt.strftime("%Y-%m-%d")
            dumped["symbol"] = symbol
            records = dumped[["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]].to_dict("records")
            symbol_rows[symbol] = len(records)
            items.extend(records)
        except Exception as exc:
            _log(f"query daily bars failed symbol={symbol}: {exc}")
            symbol_rows[symbol] = 0
    return {
        "success": True,
        "symbols": normalized_symbols,
        "start_date": start_date,
        "end_date": end_date,
        "items": items,
        "symbol_rows": symbol_rows,
        "rows": len(items),
    }


def _query_minute_bars(symbols: list[str], trade_date: str, period: str = "1m") -> dict[str, Any]:
    try:
        from xtquant import xtdata
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"分钟线能力不可用: {exc}") from exc

    normalized_symbols: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        normalized = _normalize_symbol(item)
        if normalized and normalized not in seen:
            seen.add(normalized)
            normalized_symbols.append(normalized)
    if not normalized_symbols:
        raise HTTPException(status_code=400, detail="symbols is required")

    trade_day = str(trade_date or "").replace("-", "").strip()
    if len(trade_day) != 8 or not trade_day.isdigit():
        raise HTTPException(status_code=400, detail="trade_date 格式应为 YYYY-MM-DD")
    start_time = f"{trade_day}000000"
    end_time = f"{trade_day}235959"

    items: list[dict[str, Any]] = []
    symbol_rows: dict[str, int] = {}
    symbol_errors: dict[str, dict[str, Any]] = {}
    for symbol in normalized_symbols:
        try:
            if trade_day == datetime.now().strftime("%Y%m%d"):
                _ensure_quote_subscription_light(xtdata, symbol, period)
            _download_history_window_light(xtdata, symbol, period, start_time, end_time)
            raw = _read_history_window_light(xtdata, symbol, period, start_time, end_time)
            frame = _normalize_history_frame_light(raw, symbol)
            if frame.empty:
                full_kline_raw = _read_full_kline_light(xtdata, symbol, period, start_time, end_time)
                if full_kline_raw is not None:
                    frame = _normalize_history_frame_light(full_kline_raw, symbol)
                    if not frame.empty:
                        _log(f"query minute bars fallback get_full_kline hit symbol={symbol} rows={len(frame)}")
            if frame.empty:
                symbol_rows[symbol] = 0
                continue
            frame_to_dump = frame.copy()
            frame_to_dump["trade_time"] = frame_to_dump["trade_time"].astype(str)
            records = frame_to_dump.to_dict("records")
            symbol_rows[symbol] = len(records)
            items.extend(records)
        except Exception as exc:
            _log(f"query minute bars failed symbol={symbol}: {exc}")
            symbol_rows[symbol] = 0
            message = str(exc)
            lower_message = message.lower()
            symbol_errors[symbol] = {
                "message": message,
                "unsupported": "function not realize" in lower_message or "未支持此功能" in message or "errorid" in lower_message and "300000" in lower_message,
            }
    return {
        "success": True,
        "trade_date": trade_date,
        "period": period,
        "symbols": normalized_symbols,
        "items": items,
        "symbol_rows": symbol_rows,
        "symbol_errors": symbol_errors,
        "rows": len(items),
    }


@app.get("/health")
def health(authorization: str | None = Header(default=None)):
    _require_token(authorization)
    return {
        "status": "ok",
        "bridge": "qmt",
        "role": _bridge_role(),
        "account_key": _bridge_account_key() or None,
        "trading_allowed": _bridge_trading_allowed(),
        "userdata_path": str(os.getenv("QMT_USERDATA_PATH") or ""),
    }


@app.get("/snapshot")
def snapshot(
    account_id: str = Query(...),
    account_type: str = Query("STOCK"),
    account_key: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
):
    _require_token(authorization)
    payload = _query_snapshot(account_id, account_type)
    payload["bridge"] = {
        "mode": "http_bridge",
        "account_key": account_key,
        "account_id": account_id,
        "role": _bridge_role(),
        "trading_allowed": _bridge_trading_allowed(),
    }
    return _json_safe(payload)


@app.post("/orders")
def submit_order(body: OrderSubmitRequest, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    _require_trading_allowed("submit_order", body.account_key)
    payload = _submit_order(body)
    payload["bridge"] = {
        "mode": "http_bridge",
        "account_key": body.account_key,
        "account_id": body.account_id,
        "role": _bridge_role(),
        "trading_allowed": _bridge_trading_allowed(),
    }
    return payload


@app.post("/orders/{order_id}/cancel")
def cancel_order(
    order_id: str,
    account_id: str = Query(...),
    account_type: str = Query("STOCK"),
    account_key: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
):
    _require_token(authorization)
    _require_trading_allowed("cancel_order", account_key)
    payload = _cancel_order(account_id, account_type, order_id)
    payload["bridge"] = {
        "mode": "http_bridge",
        "account_key": account_key,
        "account_id": account_id,
        "role": _bridge_role(),
        "trading_allowed": _bridge_trading_allowed(),
    }
    return payload


@app.post("/history/minute/sync")
def start_history_minute_sync(body: HistoryMinuteSyncRequest, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    job_id = uuid4().hex
    now = _now_iso()
    with _HISTORY_JOBS_LOCK:
        _HISTORY_JOBS[job_id] = {
            "job_id": job_id,
            "status": "pending",
            "progress": 0,
            "message": "任务已创建",
            "request": body.model_dump(),
            "logs": [],
            "created_at": now,
            "updated_at": now,
        }
    thread = threading.Thread(target=_run_history_minute_job, args=(job_id, body), daemon=True)
    thread.start()
    return _HISTORY_JOBS[job_id]


@app.post("/market/quotes")
def get_market_quotes(body: QuoteRequest, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    return _query_quotes(body.symbols)


@app.post("/market/daily-bars")
def get_market_daily_bars(body: DailyBarsRequest, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    return _query_daily_bars(body.symbols, body.start_date, body.end_date)


@app.post("/market/minute-bars")
def get_market_minute_bars(body: MinuteBarsRequest, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    return _query_minute_bars(body.symbols, body.trade_date, body.period)


@app.get("/history/minute/jobs/{job_id}")
def get_history_minute_job(job_id: str, authorization: str | None = Header(default=None)):
    _require_token(authorization)
    with _HISTORY_JOBS_LOCK:
        job = _HISTORY_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="history minute job not found")
        return dict(job)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("QMT_BRIDGE_HOST", "0.0.0.0")
    port = int(os.getenv("QMT_BRIDGE_PORT", "8710"))
    uvicorn.run(app, host=host, port=port, reload=False)
