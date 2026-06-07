from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from api.core.stock_map import get_reverse_stock_map
from api.services.daily_kline_parquet_store import load_daily_kline_slice_from_parquet
from api.services.market_data_pipeline_service import preferred_daily_kline_table, preferred_minute_kline_table
from api.core.utils import safe_float as _safe_float


def build_portfolio_technical_diagnostics(
    db: Session,
    *,
    trade_date: str,
    holdings: list[dict[str, Any]],
    watchlist: list[dict[str, Any]],
    quotes: dict[str, Any] | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    targets = _select_targets(holdings, watchlist, limit=limit)
    if not targets:
        return []

    symbols = [item["symbol"] for item in targets]
    daily_frame, daily_source = _load_recent_daily_frame(db, symbols=symbols, trade_date=trade_date)
    minute_frame, minute_source = _load_minute_frame(db, symbols=symbols, trade_date=trade_date)
    quote_map = quotes or {}

    diagnostics: list[dict[str, Any]] = []
    for item in targets:
        symbol = item["symbol"]
        normalized_symbol = _normalize_symbol(symbol)
        daily_rows = _rows_for_symbol(daily_frame, normalized_symbol, date_column="trade_date", tail=90)
        minute_rows = _rows_for_symbol(minute_frame, normalized_symbol, date_column="trade_time", tail=None)
        diagnostics.append(
            compute_stock_technical_diagnostic(
                symbol=normalized_symbol,
                name=item.get("name") or normalized_symbol,
                daily_rows=daily_rows,
                minute_rows=minute_rows,
                quote=_quote_for_symbol(quote_map, normalized_symbol),
                daily_source=daily_source,
                minute_source=minute_source,
            )
        )
    return diagnostics


def compute_stock_technical_diagnostic(
    *,
    symbol: str,
    name: str,
    daily_rows: list[dict[str, Any]],
    minute_rows: list[dict[str, Any]] | None = None,
    quote: dict[str, Any] | None = None,
    daily_source: str = "",
    minute_source: str = "",
) -> dict[str, Any]:
    normalized_symbol = _normalize_symbol(symbol)
    daily = _prepare_daily_frame(daily_rows)
    minute = _prepare_minute_frame(minute_rows or [])
    missing_fields: list[str] = []

    latest_price = _safe_float((quote or {}).get("price"))
    change_pct = _safe_float((quote or {}).get("change_pct") or (quote or {}).get("pct_chg"))
    latest_trade_date: str | None = None
    if not daily.empty:
        latest = daily.iloc[-1]
        latest_price = _safe_float(latest.get("close")) if _safe_float(latest.get("close")) is not None else latest_price
        change_pct = _daily_change_pct(daily)
        latest_trade_date = _safe_date(latest.get("trade_date"))
    else:
        missing_fields.append("daily_kline")

    minute_macd = None
    if not minute.empty:
        minute_60m = _aggregate_minute_60m(minute)
        if not minute_60m.empty:
            minute_macd = _compute_macd_signal(minute_60m["close"], as_of=_safe_date(minute_60m.iloc[-1].get("bar_end")))
        else:
            missing_fields.append("minute_60m_aggregation")
    else:
        missing_fields.append("minute_kline")

    daily_macd = None
    bollinger = None
    volume_price = _empty_volume_price(change_pct)
    t0_plan = _unknown_t0_plan()
    if not daily.empty:
        daily_macd = _compute_macd_signal(daily["close"], as_of=latest_trade_date)
        if len(daily) < 26:
            missing_fields.append("daily_macd_sample_lt_26")
        bollinger = _compute_bollinger(daily)
        if bollinger is None:
            missing_fields.append("bollinger_20d")
        volume_price = _compute_volume_price(daily)
        t0_plan = _compute_t0_plan(daily, bollinger=bollinger, latest_price=latest_price)

    return _json_sanitize(
        {
            "symbol": normalized_symbol,
            "name": name or normalized_symbol,
            "latest_price": _round_float(latest_price),
            "change_pct": _round_float(change_pct),
            "daily_macd": daily_macd,
            "minute_macd_60m": minute_macd,
            "bollinger": bollinger,
            "volume_price": volume_price,
            "t0_plan": t0_plan,
            "data_quality": {
                "daily_rows": int(len(daily)),
                "minute_rows": int(len(minute)),
                "missing_fields": missing_fields,
                "source": {
                    "daily": daily_source,
                    "minute": minute_source,
                },
            },
        }
    )


def _select_targets(
    holdings: list[dict[str, Any]],
    watchlist: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, str]]:
    code_to_name = get_reverse_stock_map()
    source = holdings if holdings else watchlist[:limit]
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in source:
        symbol = _normalize_symbol(item.get("symbol"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(
            {
                "symbol": symbol,
                "name": str(item.get("name") or code_to_name.get(symbol, symbol)).strip() or symbol,
            }
        )
        if not holdings and len(result) >= limit:
            break
    return result


def _load_recent_daily_frame(db: Session, *, symbols: list[str], trade_date: str) -> tuple[pd.DataFrame, str]:
    table_name = preferred_daily_kline_table()
    postgres_source = f"postgresql:{table_name}"
    if not symbols:
        return pd.DataFrame(), postgres_source
    start_date = _daily_history_start_date(trade_date)
    query_symbols = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})

    parquet_frame = _load_daily_frame_from_parquet(query_symbols, start_date=start_date, trade_date=trade_date)
    if parquet_frame is not None and not parquet_frame.empty:
        frames = [parquet_frame]
        sources = ["parquet:daily_kline"]
        requested_symbols = {_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)}
        covered_symbols = set(parquet_frame["normalized_symbol"].dropna().astype(str).unique())
        missing_symbols = sorted(requested_symbols - covered_symbols)
        if missing_symbols:
            missing_query_symbols = sorted({variant for symbol in missing_symbols for variant in _symbol_variants(symbol)})
            missing_frame, missing_source = _query_daily_frame_from_db(
                db,
                table_name=table_name,
                query_symbols=missing_query_symbols,
                trade_date=trade_date,
                start_date=start_date,
                start_exclusive=False,
                source_prefix="postgresql_missing",
            )
            sources.append(missing_source)
            if not missing_frame.empty:
                frames.append(missing_frame)
        max_parquet_date = parquet_frame["trade_date"].max()
        if pd.notna(max_parquet_date) and str(max_parquet_date)[:10] < str(trade_date)[:10]:
            tail_frame, tail_source = _query_daily_frame_from_db(
                db,
                table_name=table_name,
                query_symbols=query_symbols,
                trade_date=trade_date,
                start_date=str(max_parquet_date)[:10],
                start_exclusive=True,
                source_prefix="postgresql_tail",
            )
            sources.append(tail_source)
            if not tail_frame.empty:
                frames.append(tail_frame)
        merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if merged.empty:
            return pd.DataFrame(), "+".join(sources)
        merged = (
            merged.sort_values(["normalized_symbol", "trade_date"])
            .drop_duplicates(subset=["normalized_symbol", "trade_date"], keep="last")
            .reset_index(drop=True)
        )
        return merged, "+".join(sources)

    return _query_daily_frame_from_db(
        db,
        table_name=table_name,
        query_symbols=query_symbols,
        trade_date=trade_date,
        start_date=start_date,
        start_exclusive=False,
        source_prefix="postgresql",
    )


def _daily_history_start_date(trade_date: str) -> str:
    try:
        return (pd.to_datetime(trade_date) - pd.Timedelta(days=180)).strftime("%Y-%m-%d")
    except Exception:
        return (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")


def _load_daily_frame_from_parquet(query_symbols: list[str], *, start_date: str, trade_date: str) -> pd.DataFrame | None:
    try:
        frame = load_daily_kline_slice_from_parquet(
            symbols=query_symbols,
            start_date=start_date,
            end_date=trade_date,
            columns=["open", "high", "low", "close", "volume", "amount", "pre_close"],
        )
    except Exception:
        return None
    if frame is None or frame.empty:
        return None
    normalized = _normalize_daily_source_frame(frame, date_column="date")
    return normalized if not normalized.empty else None


def _query_daily_frame_from_db(
    db: Session,
    *,
    table_name: str,
    query_symbols: list[str],
    trade_date: str,
    start_date: str | None,
    start_exclusive: bool,
    source_prefix: str,
) -> tuple[pd.DataFrame, str]:
    source = f"{source_prefix}:{table_name}"
    if not query_symbols:
        return pd.DataFrame(), source
    try:
        start_condition = ""
        params: dict[str, Any] = {"symbols": query_symbols, "trade_date": trade_date}
        if start_date:
            comparator = ">" if start_exclusive else ">="
            start_condition = f"AND trade_date {comparator} :start_date"
            params["start_date"] = start_date
        statement = text(
            f"""
            SELECT symbol, trade_date, open, high, low, close, volume, amount, pre_close
            FROM {table_name}
            WHERE symbol IN :symbols
              {start_condition}
              AND trade_date <= :trade_date
            ORDER BY symbol, trade_date
            """
        ).bindparams(bindparam("symbols", expanding=True))
        rows = db.execute(statement, params).mappings().all()
    except Exception as exc:
        return pd.DataFrame(), f"{source}:error:{exc.__class__.__name__}"
    if not rows:
        return pd.DataFrame(), source
    frame = pd.DataFrame([dict(row) for row in rows])
    return _normalize_daily_source_frame(frame, date_column="trade_date"), source


def _normalize_daily_source_frame(frame: pd.DataFrame, *, date_column: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    if date_column != "trade_date" and date_column in frame.columns:
        frame = frame.rename(columns={date_column: "trade_date"})
    if "symbol" not in frame.columns:
        return pd.DataFrame()
    if "trade_date" not in frame.columns:
        frame["trade_date"] = pd.NaT
    frame["normalized_symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount", "pre_close"):
        if column not in frame.columns:
            frame[column] = pd.NA
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=["normalized_symbol", "trade_date"])
        .loc[lambda item: item["normalized_symbol"].astype(str).str.len() > 0]
        .sort_values(["normalized_symbol", "trade_date"])
        .reset_index(drop=True)
    )


def _load_minute_frame(db: Session, *, symbols: list[str], trade_date: str) -> tuple[pd.DataFrame, str]:
    table_name = preferred_minute_kline_table()
    source = f"postgresql:{table_name}"
    if not symbols:
        return pd.DataFrame(), source
    try:
        query_symbols = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
        start_time = datetime.fromisoformat(str(trade_date))
        end_time = start_time + timedelta(days=1)
        statement = text(
            f"""
            SELECT symbol, trade_time, open, high, low, close, volume, amount
            FROM {table_name}
            WHERE symbol IN :symbols
              AND trade_time >= :start_time
              AND trade_time < :end_time
            ORDER BY symbol, trade_time
            """
        ).bindparams(bindparam("symbols", expanding=True))
        rows = db.execute(
            statement,
            {"symbols": query_symbols, "start_time": start_time, "end_time": end_time},
        ).mappings().all()
    except Exception as exc:
        return pd.DataFrame(), f"{source}:error:{exc.__class__.__name__}"
    if not rows:
        return pd.DataFrame(), source
    frame = pd.DataFrame([dict(row) for row in rows])
    frame["normalized_symbol"] = frame["symbol"].map(_normalize_symbol)
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values(["normalized_symbol", "trade_time"]).reset_index(drop=True), source


def _rows_for_symbol(frame: pd.DataFrame, symbol: str, *, date_column: str, tail: int | None) -> list[dict[str, Any]]:
    if frame.empty or "normalized_symbol" not in frame.columns:
        return []
    subset = frame[frame["normalized_symbol"] == symbol].copy()
    if subset.empty:
        return []
    subset = subset.sort_values(date_column).drop_duplicates(subset=[date_column], keep="last")
    if tail is not None:
        subset = subset.tail(tail)
    return subset.to_dict("records")


def _prepare_daily_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "volume", "amount", "pre_close"])
    frame = pd.DataFrame(rows)
    if "trade_date" not in frame.columns:
        frame["trade_date"] = pd.NaT
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount", "pre_close"):
        if column not in frame.columns:
            frame[column] = pd.NA
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=["trade_date", "close"])
        .sort_values("trade_date")
        .drop_duplicates(subset=["trade_date"], keep="last")
        .tail(90)
        .reset_index(drop=True)
    )


def _prepare_minute_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["trade_time", "open", "high", "low", "close", "volume", "amount"])
    frame = pd.DataFrame(rows)
    if "trade_time" not in frame.columns:
        frame["trade_time"] = pd.NaT
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column not in frame.columns:
            frame[column] = pd.NA
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return (
        frame.dropna(subset=["trade_time", "open", "high", "low", "close"])
        .sort_values("trade_time")
        .drop_duplicates(subset=["trade_time"], keep="last")
        .reset_index(drop=True)
    )


def _aggregate_minute_60m(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount"])
    data = frame.set_index("trade_time").sort_index()
    aggregated = data.resample("60min", label="right", closed="right").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        amount=("amount", "sum"),
    )
    aggregated = aggregated.dropna(subset=["open", "high", "low", "close"], how="any")
    if aggregated.empty:
        return pd.DataFrame(columns=["bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount"])
    aggregated["bar_end"] = aggregated.index
    aggregated["bar_start"] = aggregated["bar_end"] - pd.to_timedelta("60min")
    return aggregated.reset_index(drop=True)[["bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount"]]


def _compute_macd_signal(close: pd.Series, *, as_of: str | None = None) -> dict[str, Any] | None:
    values = pd.to_numeric(close, errors="coerce").dropna().reset_index(drop=True)
    if values.empty:
        return None
    ema12 = values.ewm(span=12, adjust=False).mean()
    ema26 = values.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    histogram = dif - dea
    latest_index = len(values) - 1
    latest_hist = _safe_float(histogram.iloc[latest_index]) or 0.0
    previous_hist = _safe_float(histogram.iloc[latest_index - 1]) if latest_index > 0 else None
    latest_dif = _safe_float(dif.iloc[latest_index]) or 0.0
    latest_dea = _safe_float(dea.iloc[latest_index]) or 0.0
    return {
        "dif": _round_float(latest_dif),
        "dea": _round_float(latest_dea),
        "histogram": _round_float(latest_hist),
        "histogram_formula": "DIF-DEA",
        "zero_axis_state": _macd_zero_axis_state(latest_dif, latest_dea),
        "histogram_change": _macd_histogram_change(latest_hist, previous_hist),
        "divergence_hint": _macd_divergence_hint(values, histogram),
        "sample_rows": int(len(values)),
        "as_of": as_of,
    }


def _compute_bollinger(frame: pd.DataFrame) -> dict[str, Any] | None:
    if len(frame) < 20:
        return None
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if len(close) < 20:
        return None
    window = close.tail(20)
    middle = float(window.mean())
    std = float(window.std(ddof=0))
    upper = middle + 2 * std
    lower = middle - 2 * std
    latest_close = float(close.iloc[-1])
    bandwidth = (upper - lower) / middle if middle else None
    position_ratio = (latest_close - lower) / (upper - lower) if upper != lower else None
    return {
        "middle": _round_float(middle),
        "upper": _round_float(upper),
        "lower": _round_float(lower),
        "bandwidth": _round_float(bandwidth, 4),
        "position_ratio": _round_float(position_ratio, 4),
        "track_position": _bollinger_track_position(latest_close, middle, upper, lower),
        "opening_state": _bollinger_opening_state(frame),
        "sample_rows": int(len(close)),
    }


def _compute_volume_price(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return _empty_volume_price(None)
    latest = frame.iloc[-1]
    previous = frame.iloc[:-1].tail(20)
    volume_ratio = _ratio_to_baseline(latest.get("volume"), previous["volume"] if "volume" in previous else pd.Series(dtype=float))
    amount_ratio = _ratio_to_baseline(latest.get("amount"), previous["amount"] if "amount" in previous else pd.Series(dtype=float))
    change_pct = _daily_change_pct(frame)
    tags = _volume_price_tags(change_pct, volume_ratio, amount_ratio)
    return {
        "volume_ratio": _round_float(volume_ratio, 3),
        "amount_ratio": _round_float(amount_ratio, 3),
        "change_pct": _round_float(change_pct),
        "tags": tags,
        "volume": _round_float(_safe_float(latest.get("volume")), 2),
        "amount": _round_float(_safe_float(latest.get("amount")), 2),
    }


def _compute_t0_plan(
    frame: pd.DataFrame,
    *,
    bollinger: dict[str, Any] | None,
    latest_price: float | None,
) -> dict[str, Any]:
    current_price = _safe_float(latest_price)
    if current_price is None or frame.empty:
        return _unknown_t0_plan()

    candidates_pressure: list[tuple[float, str]] = []
    candidates_support: list[tuple[float, str]] = []
    if bollinger:
        _add_candidate(candidates_pressure, bollinger.get("upper"), "布林上轨")
        _add_candidate(candidates_support, bollinger.get("middle"), "布林中轨/MA20")
        _add_candidate(candidates_support, bollinger.get("lower"), "布林下轨")
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    high = pd.to_numeric(frame["high"], errors="coerce").dropna()
    low = pd.to_numeric(frame["low"], errors="coerce").dropna()
    if len(high) >= 2:
        _add_candidate(candidates_pressure, high.tail(20).max(), "20日高点/前高筹码区")
    if len(high) >= 60:
        _add_candidate(candidates_pressure, high.tail(60).max(), "60日高点/前高筹码区")
    if len(low) >= 2:
        _add_candidate(candidates_support, low.tail(20).min(), "20日低点")
    if len(close) >= 60:
        _add_candidate(candidates_support, close.tail(60).mean(), "MA60")

    pressure = _nearest_zone(candidates_pressure, current_price=current_price, side="pressure")
    support = _nearest_zone(candidates_support, current_price=current_price, side="support")
    opening_watchpoint = _build_opening_watchpoint(pressure, support)
    return {
        "pressure_zone": pressure,
        "support_zone": support,
        "opening_watchpoint": opening_watchpoint,
    }


def _daily_change_pct(frame: pd.DataFrame) -> float | None:
    if frame.empty:
        return None
    latest = frame.iloc[-1]
    close = _safe_float(latest.get("close"))
    pre_close = _safe_float(latest.get("pre_close"))
    if (pre_close is None or pre_close <= 0) and len(frame) >= 2:
        pre_close = _safe_float(frame.iloc[-2].get("close"))
    if close is None or pre_close is None or pre_close <= 0:
        return None
    return (close - pre_close) / pre_close * 100


def _ratio_to_baseline(value: Any, baseline: pd.Series) -> float | None:
    latest = _safe_float(value)
    values = pd.to_numeric(baseline, errors="coerce").dropna()
    values = values[values > 0]
    if latest is None or values.empty:
        return None
    baseline_mean = float(values.mean())
    if baseline_mean <= 0:
        return None
    return latest / baseline_mean


def _volume_price_tags(change_pct: float | None, volume_ratio: float | None, amount_ratio: float | None) -> list[str]:
    cp = change_pct if change_pct is not None else 0.0
    vr = volume_ratio if volume_ratio is not None else 0.0
    ar = amount_ratio if amount_ratio is not None else 0.0
    tags: list[str] = []
    if vr >= 1.8 and -1.0 <= cp <= 1.0:
        tags.append("爆量滞涨")
    if cp >= 2.0 and vr < 0.8:
        tags.append("缩量空拉")
    if cp <= -2.0 and vr >= 1.3:
        tags.append("放量下跌")
    if cp >= 2.0 and (vr >= 1.2 or ar >= 1.2):
        tags.append("放量上涨")
    if cp < 0 and vr < 0.8:
        tags.append("缩量回踩")
    return tags or ["量价中性"]


def _empty_volume_price(change_pct: float | None) -> dict[str, Any]:
    return {
        "volume_ratio": None,
        "amount_ratio": None,
        "change_pct": _round_float(change_pct),
        "tags": ["日线量价数据不足"],
        "volume": None,
        "amount": None,
    }


def _unknown_t0_plan() -> dict[str, Any]:
    return {
        "pressure_zone": {"lower": None, "upper": None, "label": "需盘中确认", "basis": "缺少可用日线压力参考"},
        "support_zone": {"lower": None, "upper": None, "label": "需盘中确认", "basis": "缺少可用日线支撑参考"},
        "opening_watchpoint": "缺少足够日线/分钟数据，次日开盘先确认量能与分时承接，不给出虚假精确价位。",
    }


def _add_candidate(candidates: list[tuple[float, str]], value: Any, basis: str) -> None:
    price = _safe_float(value)
    if price is None or price <= 0:
        return
    candidates.append((price, basis))


def _nearest_zone(candidates: list[tuple[float, str]], *, current_price: float, side: str) -> dict[str, Any]:
    if side == "pressure":
        valid = [(price, basis) for price, basis in candidates if price >= current_price * 0.998]
        if not valid:
            return {"lower": None, "upper": None, "label": "需盘中确认", "basis": "现价上方暂无明确历史/布林压力"}
        price, basis = min(valid, key=lambda item: abs(item[0] - current_price))
        return {
            "lower": _round_float(price * 0.995),
            "upper": _round_float(price * 1.01),
            "label": f"{_format_price(price * 0.995)}-{_format_price(price * 1.01)}",
            "basis": basis,
        }
    valid = [(price, basis) for price, basis in candidates if price <= current_price * 1.002]
    if not valid:
        return {"lower": None, "upper": None, "label": "需盘中确认", "basis": "现价下方暂无明确均线/布林支撑"}
    price, basis = min(valid, key=lambda item: abs(item[0] - current_price))
    return {
        "lower": _round_float(price * 0.99),
        "upper": _round_float(price * 1.005),
        "label": f"{_format_price(price * 0.99)}-{_format_price(price * 1.005)}",
        "basis": basis,
    }


def _build_opening_watchpoint(pressure: dict[str, Any], support: dict[str, Any]) -> str:
    pressure_label = pressure.get("label") or "需盘中确认"
    support_label = support.get("label") or "需盘中确认"
    return (
        f"开盘前30分钟先看量能与VWAP承接：放量站稳 {pressure_label} 上沿再考虑高抛后的回补确认；"
        f"若跌破 {support_label} 下沿且反抽无量，T+0以减仓防守为先。"
    )


def _macd_zero_axis_state(dif: float, dea: float) -> str:
    if dif > 0 and dea > 0:
        return "零轴上方强势"
    if dif < 0 and dea < 0:
        return "零轴下方弱势"
    return "零轴附近/穿越"


def _macd_histogram_change(latest: float, previous: float | None) -> str:
    if previous is None:
        return "样本起点，需继续观察"
    if latest >= 0 and previous < 0:
        return "绿翻红"
    if latest < 0 and previous >= 0:
        return "红翻绿"
    if latest >= 0:
        return "红柱放大" if latest > previous else "红柱收缩"
    return "绿柱放大" if abs(latest) > abs(previous) else "绿柱收缩"


def _macd_divergence_hint(close: pd.Series, histogram: pd.Series) -> str:
    if len(close) < 20 or len(histogram) < 20:
        return "样本不足，背离需盘中确认"
    current_close = float(close.iloc[-1])
    current_hist = float(histogram.iloc[-1])
    previous_close = close.iloc[:-1].tail(20)
    previous_hist = histogram.iloc[:-1].tail(20)
    if current_close > float(previous_close.max()) and current_hist < float(previous_hist.max()) * 0.85:
        return "价格新高但MACD柱未同步创新高，警惕顶背离"
    if current_close < float(previous_close.min()) and current_hist > float(previous_hist.min()) * 0.85:
        return "价格新低但MACD柱未同步走弱，留意底背离"
    return "未见明确背离"


def _bollinger_track_position(close: float, middle: float, upper: float, lower: float) -> str:
    if close > upper:
        return "上轨上方超买/逼空"
    if close >= middle:
        return "中上轨强势区"
    if close >= lower:
        return "中下轨震荡/弱势区"
    return "下轨下方超卖"


def _bollinger_opening_state(frame: pd.DataFrame) -> str:
    close = pd.to_numeric(frame["close"], errors="coerce").dropna()
    if len(close) < 21:
        return "样本不足，开口状态需确认"
    latest = _rolling_boll_bandwidth(close, end_offset=0)
    previous = _rolling_boll_bandwidth(close, end_offset=1)
    if latest is None:
        return "样本不足，开口状态需确认"
    if latest >= 0.18 or (previous is not None and latest > previous * 1.12):
        return "开口扩张/趋势释放"
    if latest <= 0.06:
        return "收口窄幅震荡"
    return "正常延伸"


def _rolling_boll_bandwidth(close: pd.Series, *, end_offset: int) -> float | None:
    end = len(close) - end_offset
    if end < 20:
        return None
    window = close.iloc[end - 20 : end]
    middle = float(window.mean())
    if middle == 0:
        return None
    std = float(window.std(ddof=0))
    return (4 * std) / middle


def _quote_for_symbol(quotes: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    code = symbol.split(".", 1)[0]
    return quotes.get(symbol) or quotes.get(code) or quotes.get(symbol.upper()) or quotes.get(code.upper())


def _normalize_symbol(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    code = value.split(".", 1)[0]
    if "." in value:
        return value
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("4", "8")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _symbol_variants(symbol: str) -> set[str]:
    normalized = _normalize_symbol(symbol)
    code = normalized.split(".", 1)[0]
    raw = str(symbol or "").strip().upper()
    return {raw, normalized, code}




def _round_float(value: Any, digits: int = 2) -> float | None:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number, digits)


def _safe_date(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        text = isoformat()
        return text[:19] if "T" in text else text[:10]
    return str(value)


def _format_price(value: float) -> str:
    return f"{value:.2f}"


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [_json_sanitize(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return _safe_date(value)
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    return value
