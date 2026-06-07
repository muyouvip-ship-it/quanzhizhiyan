from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from api.services.qmt_market_data_service import capture_intraday_symbols
from api.core.utils import safe_float as _safe_float


logger = logging.getLogger(__name__)


def capture_today_minute_bars(
    *,
    account_key: str,
    symbols: list[str],
    trade_date: str | None = None,
    db: Session | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return {"success": False, "message": "empty symbols", "rows": 0, "symbols": []}

    effective_trade_date = trade_date or datetime.now().date().isoformat()
    try:
        result = capture_intraday_symbols(
            normalized_symbols,
            trade_date=effective_trade_date,
            period="1m",
            account_key=account_key,
            db=db,
            user_id=user_id,
        )
        return result
    except Exception as exc:
        logger.warning("[qmt-minute-capture] capture failed account=%s symbols=%s error=%s", account_key, len(normalized_symbols), exc)
        return {
            "success": False,
            "message": str(exc),
            "rows": 0,
            "symbols": normalized_symbols,
            "trade_date": effective_trade_date,
            "source": "qmt_intraday",
        }


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        return ""
    if "." in symbol:
        return symbol
    if len(symbol) == 6 and symbol.isdigit():
        if symbol.startswith("6"):
            return f"{symbol}.SH"
        if symbol.startswith(("0", "3")):
            return f"{symbol}.SZ"
        if symbol.startswith(("4", "8")):
            return f"{symbol}.BJ"
    return symbol


def _normalize_trade_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except Exception:
        return None


