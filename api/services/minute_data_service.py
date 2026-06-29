from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine

from api.core.env import load_project_env
from api.services.market_data_pipeline_service import preferred_minute_kline_table
from api.core.utils import safe_float as _safe_float


MINUTE_CACHE_ROOT = Path("data/artifacts/minute_cache")
_minute_engine: Engine | None = None
_minute_engine_url: str | None = None


@dataclass
class MinuteAggregationResult:
    timeframe: str
    trade_date: str
    items: list[dict[str, Any]]
    source: str
    missing_symbols: list[str]
    cache_path: str | None = None
    parquet_cache_path: str | None = None


@dataclass
class MinuteSignalResult:
    timeframe: str
    trade_date: str
    items: list[dict[str, Any]]
    source: str
    missing_symbols: list[str]


def get_minute_cache_root() -> Path:
    return Path(os.getenv("MINUTE_CACHE_ROOT") or MINUTE_CACHE_ROOT)


def load_aggregated_minute_bars(
    *,
    symbols: list[str],
    trade_date: str,
    timeframe: str,
    allow_cache: bool = True,
    allow_synthetic: bool = True,
) -> MinuteAggregationResult:
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return MinuteAggregationResult(
            timeframe=timeframe,
            trade_date=trade_date,
            items=[],
            source="empty",
            missing_symbols=[],
            cache_path=None,
            parquet_cache_path=None,
        )
    frame = _try_load_minute_frame(normalized_symbols, trade_date)
    source = f"postgresql:{preferred_minute_kline_table()}"
    cache_paths: dict[str, str | None] = {"json": None, "parquet": None}
    if frame is None or frame.empty:
        cached = _try_load_cached_aggregated_frame(normalized_symbols, trade_date=trade_date, timeframe=timeframe)
        if allow_cache and cached is not None:
            aggregated, cache_source, cache_paths = cached
            source = f"cache:{cache_source}"
        elif allow_synthetic:
            frame = _generate_synthetic_minute_frame(normalized_symbols, trade_date)
            source = "synthetic:fallback" if not frame.empty else "empty"
            aggregated = _aggregate_minute_frame(frame, timeframe)
        else:
            aggregated = pd.DataFrame(columns=["symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"])
            source = "empty"
    else:
        aggregated = _aggregate_minute_frame(frame, timeframe)
    missing_symbols = sorted(set(normalized_symbols) - set(aggregated["symbol"].unique())) if not aggregated.empty else normalized_symbols
    if not cache_paths.get("json") and not cache_paths.get("parquet"):
        cache_paths = _write_minute_cache(aggregated, trade_date=trade_date, timeframe=timeframe, source=source)
    return MinuteAggregationResult(
        timeframe=timeframe,
        trade_date=trade_date,
        items=aggregated.to_dict("records"),
        source=source,
        missing_symbols=missing_symbols,
        cache_path=cache_paths.get("json"),
        parquet_cache_path=cache_paths.get("parquet"),
    )


def evaluate_intraday_confirmation(
    *,
    symbols: list[str],
    trade_date: str,
    timeframe: str = "30m",
    allow_cache: bool = True,
    allow_synthetic: bool = True,
) -> MinuteAggregationResult:
    result = load_aggregated_minute_bars(
        symbols=symbols,
        trade_date=trade_date,
        timeframe=timeframe,
        allow_cache=allow_cache,
        allow_synthetic=allow_synthetic,
    )
    frame = pd.DataFrame(result.items)
    if frame.empty:
        result.items = []
        return result
    frame = frame.sort_values(["symbol", "bar_end"]).reset_index(drop=True)
    grouped = frame.groupby("symbol", group_keys=False)
    prev_close = grouped["close"].shift(1)
    prev_vwap = grouped["vwap"].shift(1)
    cross_above = (frame["close"] >= frame["vwap"]) & ((prev_close < prev_vwap) | prev_close.isna() | prev_vwap.isna())
    confirmation_frames = [
        _select_confirmation_row(group, cross_above.loc[group.index], str(symbol))
        for symbol, group in frame.groupby("symbol", sort=False)
    ]
    confirmation_rows = pd.concat(confirmation_frames, ignore_index=True) if confirmation_frames else pd.DataFrame()
    result.items = confirmation_rows.to_dict("records") if not confirmation_rows.empty else []
    return result


def evaluate_first_day_band_signals(
    *,
    symbols: list[str],
    trade_date: str,
    timeframe: str = "5m",
    lookback_days: int = 7,
    supplement_frame: pd.DataFrame | None = None,
    supplement_source: str | None = None,
) -> MinuteSignalResult:
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return MinuteSignalResult(
            timeframe=timeframe,
            trade_date=trade_date,
            items=[],
            source=f"postgresql:{preferred_minute_kline_table()}",
            missing_symbols=[],
        )

    end_dt = datetime.fromisoformat(str(trade_date))
    start_dt = (end_dt - timedelta(days=max(lookback_days, 1) + 3)).date().isoformat()
    frame = _try_load_minute_frame_range(normalized_symbols, start_date=start_dt, end_date=trade_date)
    source = f"postgresql:{preferred_minute_kline_table()}"
    if supplement_frame is not None and not supplement_frame.empty:
        extra = supplement_frame.copy()
        if "symbol" in extra.columns:
            extra["symbol"] = extra["symbol"].map(_normalize_symbol)
        if "trade_time" in extra.columns:
            extra["trade_time"] = pd.to_datetime(extra["trade_time"])
        if frame is None or frame.empty:
            frame = extra
            source = supplement_source or source
        else:
            frame = (
                pd.concat([frame, extra], ignore_index=True)
                .sort_values(["symbol", "trade_time"])
                .drop_duplicates(subset=["symbol", "trade_time"], keep="last")
                .reset_index(drop=True)
            )
            if supplement_source:
                source = f"{source}+{supplement_source}"
    if frame is None or frame.empty:
        frame = pd.DataFrame(columns=["symbol", "trade_time", "open", "high", "low", "close", "volume", "amount"])
        source = "empty"

    aggregated = _aggregate_minute_frame(frame, timeframe)
    if aggregated.empty:
        return MinuteSignalResult(
            timeframe=timeframe,
            trade_date=trade_date,
            items=[],
            source=source,
            missing_symbols=normalized_symbols,
        )

    aggregated = aggregated.sort_values(["symbol", "bar_end"]).reset_index(drop=True)
    items: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()

    for symbol, group in aggregated.groupby("symbol", sort=False):
        computed = _compute_first_day_band(group.copy())
        if computed.empty:
            continue
        latest = computed.iloc[-1]
        previous = computed.iloc[-2] if len(computed) > 1 else None
        band = _safe_float(latest.get("first_day_band"))
        b1 = _safe_float(latest.get("first_day_band_b1"))
        prev_band = _safe_float(previous.get("first_day_band")) if previous is not None else None
        prev_b1 = _safe_float(previous.get("first_day_band_b1")) if previous is not None else None
        cross_above = bool(
            band is not None
            and b1 is not None
            and (prev_band is None or prev_b1 is None or prev_band <= prev_b1)
            and band > b1
        )
        cross_below = bool(
            band is not None
            and b1 is not None
            and prev_band is not None
            and prev_b1 is not None
            and prev_band >= prev_b1
            and band < b1
        )
        signal = "buy" if cross_above else "sell" if cross_below else "hold"
        items.append(
            {
                "symbol": symbol,
                "bar_start": _safe_iso(latest.get("bar_start")),
                "bar_end": _safe_iso(latest.get("bar_end")),
                "open": _safe_float(latest.get("open")),
                "high": _safe_float(latest.get("high")),
                "low": _safe_float(latest.get("low")),
                "close": _safe_float(latest.get("close")),
                "volume": _safe_float(latest.get("volume")),
                "amount": _safe_float(latest.get("amount")),
                "first_day_band": band,
                "first_day_band_b1": b1,
                "previous_band": prev_band,
                "previous_b1": prev_b1,
                "cross_above": cross_above,
                "cross_below": cross_below,
                "signal": signal,
                "confirmed": signal in {"buy", "sell"},
            }
        )
        seen_symbols.add(symbol)

    missing_symbols = sorted(set(normalized_symbols) - seen_symbols)
    return MinuteSignalResult(
        timeframe=timeframe,
        trade_date=trade_date,
        items=items,
        source=source,
        missing_symbols=missing_symbols,
    )


def _try_load_minute_frame(symbols: list[str], trade_date: str) -> pd.DataFrame | None:
    engine = _get_minute_engine()
    if engine is None or not symbols:
        return None
    try:
        table_name = preferred_minute_kline_table()
        query_symbols = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
        start_time, end_time = _date_range_bounds(trade_date, trade_date)
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
        frame = pd.read_sql_query(
            statement,
            engine,
            params={"symbols": query_symbols, "start_time": start_time, "end_time": end_time},
        )
        if frame.empty:
            return None
        frame["symbol"] = frame["symbol"].map(_normalize_symbol)
        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        return frame
    except Exception:
        return None


def _try_load_minute_frame_range(symbols: list[str], *, start_date: str, end_date: str) -> pd.DataFrame | None:
    engine = _get_minute_engine()
    if engine is None or not symbols:
        return None
    try:
        table_name = preferred_minute_kline_table()
        query_symbols = sorted({variant for symbol in symbols for variant in _symbol_variants(symbol)})
        start_time, end_time = _date_range_bounds(start_date, end_date)
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
        frame = pd.read_sql_query(
            statement,
            engine,
            params={"symbols": query_symbols, "start_time": start_time, "end_time": end_time},
        )
        if frame.empty:
            return None
        frame["symbol"] = frame["symbol"].map(_normalize_symbol)
        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        return frame
    except Exception:
        return None


def _get_minute_engine() -> Engine | None:
    global _minute_engine, _minute_engine_url
    load_project_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None
    if _minute_engine is None or _minute_engine_url != database_url:
        if _minute_engine is not None:
            _minute_engine.dispose()
        _minute_engine = create_engine(database_url)
        _minute_engine_url = database_url
    return _minute_engine


def _date_range_bounds(start_date: str, end_date: str) -> tuple[datetime, datetime]:
    start_day = pd.to_datetime(start_date).date()
    end_day = pd.to_datetime(end_date).date()
    start_time = datetime.combine(start_day, datetime.min.time())
    end_time = datetime.combine(end_day + timedelta(days=1), datetime.min.time())
    return start_time, end_time


def _generate_synthetic_minute_frame(symbols: list[str], trade_date: str) -> pd.DataFrame:
    trading_day = pd.Timestamp(trade_date).date()
    session_one = pd.date_range(f"{trading_day} 09:30:00", f"{trading_day} 11:29:00", freq="1min")
    session_two = pd.date_range(f"{trading_day} 13:00:00", f"{trading_day} 14:59:00", freq="1min")
    timeline = session_one.append(session_two)
    rows: list[dict[str, Any]] = []
    for symbol_index, symbol in enumerate(symbols):
        seed = sum(ord(ch) for ch in symbol)
        base_price = 12 + (seed % 80)
        for idx, ts in enumerate(timeline):
            drift = 1 + idx * (0.00018 + symbol_index * 0.00002)
            wave = 1 + math.sin(idx / 18 + symbol_index) * 0.0035
            close = round(base_price * drift * wave, 2)
            open_price = round(close * (0.999 + ((idx + symbol_index) % 4) * 0.0007), 2)
            high = round(max(open_price, close) * 1.0015, 2)
            low = round(min(open_price, close) * 0.9985, 2)
            volume = float(2_000 + (idx % 25) * 120 + symbol_index * 200)
            amount = round(close * volume, 2)
            rows.append(
                {
                    "symbol": symbol,
                    "trade_time": ts,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": amount,
                }
            )
    return pd.DataFrame(rows)


def _try_load_cached_aggregated_frame(
    symbols: list[str],
    *,
    trade_date: str,
    timeframe: str,
) -> tuple[pd.DataFrame, str, dict[str, str | None]] | None:
    cache_dir = get_minute_cache_root() / trade_date / timeframe
    if not cache_dir.exists():
        return None

    normalized_symbols = set(_normalize_symbols(symbols))
    for parquet_path in sorted(cache_dir.glob("*.parquet")):
        try:
            frame = pd.read_parquet(parquet_path)
        except Exception:
            continue
        prepared = _prepare_cached_aggregated_frame(frame, normalized_symbols)
        if prepared is None:
            continue
        json_path = parquet_path.with_suffix(".json")
        return prepared, parquet_path.stem, {
            "json": str(json_path) if json_path.exists() else None,
            "parquet": str(parquet_path),
        }

    for json_path in sorted(cache_dir.glob("*.json")):
        try:
            frame = pd.read_json(json_path)
        except Exception:
            continue
        prepared = _prepare_cached_aggregated_frame(frame, normalized_symbols)
        if prepared is None:
            continue
        parquet_path = json_path.with_suffix(".parquet")
        return prepared, json_path.stem, {
            "json": str(json_path),
            "parquet": str(parquet_path) if parquet_path.exists() else None,
        }

    return None


def _prepare_cached_aggregated_frame(frame: pd.DataFrame, normalized_symbols: set[str]) -> pd.DataFrame | None:
    if frame.empty or "symbol" not in frame.columns:
        return None
    prepared = frame.copy()
    prepared["symbol"] = prepared["symbol"].map(_normalize_symbol)
    if normalized_symbols:
        prepared = prepared[prepared["symbol"].isin(normalized_symbols)]
    if prepared.empty:
        return None
    for column in ("bar_start", "bar_end"):
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column])
    for column in ("open", "high", "low", "close", "volume", "amount", "vwap"):
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    columns = ["symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"]
    available = [column for column in columns if column in prepared.columns]
    if len(available) != len(columns):
        return None
    return prepared[columns].sort_values(["symbol", "bar_end"]).reset_index(drop=True)


def _aggregate_minute_frame(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"])
    rule = _to_pandas_rule(timeframe)
    data = frame.copy()
    data["trade_time"] = pd.to_datetime(data["trade_time"])
    data = data.sort_values(["symbol", "trade_time"])
    aggregated_frames: list[pd.DataFrame] = []
    for symbol, group in data.groupby("symbol"):
        group = group.set_index("trade_time")
        latest_trade_time = group.index.max()
        latest_closed_end = latest_trade_time.floor(rule)
        resampled = group.resample(rule, label="right", closed="right").agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            amount=("amount", "sum"),
        ).dropna(subset=["open", "high", "low", "close"], how="any")
        resampled = resampled[resampled.index <= latest_closed_end]
        if resampled.empty:
            continue
        resampled["symbol"] = symbol
        resampled["bar_end"] = resampled.index
        resampled["bar_start"] = resampled["bar_end"] - pd.to_timedelta(rule)
        resampled["vwap"] = (resampled["amount"] / resampled["volume"].replace(0, pd.NA)).fillna(resampled["close"])
        aggregated_frames.append(resampled.reset_index(drop=True))
    if not aggregated_frames:
        return pd.DataFrame(columns=["symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"])
    result = pd.concat(aggregated_frames, ignore_index=True)
    return result[["symbol", "bar_start", "bar_end", "open", "high", "low", "close", "volume", "amount", "vwap"]]


def _compute_first_day_band(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    frame = frame.sort_values("bar_end").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    typical = (2 * frame["close"] + frame["high"] + frame["low"]) / 4
    low_min = frame["low"].rolling(window=9, min_periods=9).min()
    high_max = frame["high"].rolling(window=9, min_periods=9).max()
    denominator = (high_max - low_min).where((high_max - low_min) != 0)
    normalized = ((typical - low_min) / denominator) * 100
    band = normalized.ewm(span=8, adjust=False, min_periods=8).mean()
    b1_seed = 0.667 * band.shift(1) + 0.333 * band
    b1 = b1_seed.ewm(span=2, adjust=False, min_periods=2).mean()
    frame["first_day_band"] = band
    frame["first_day_band_b1"] = b1
    return frame.dropna(subset=["first_day_band", "first_day_band_b1"], how="any")


def _select_confirmation_row(group: pd.DataFrame, cross_mask: pd.Series, symbol: str) -> pd.DataFrame:
    hits = group[cross_mask.fillna(False)]
    if not hits.empty:
        row = hits.iloc[[0]].copy()
        row["confirmed"] = True
    else:
        row = group.iloc[[-1]].copy()
        row["confirmed"] = False
    row["symbol"] = symbol
    return row


def _write_minute_cache(frame: pd.DataFrame, *, trade_date: str, timeframe: str, source: str) -> dict[str, str | None]:
    if frame.empty:
        return {"json": None, "parquet": None}
    cache_dir = get_minute_cache_root() / trade_date / timeframe
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{source.replace(':', '_')}.json"
    frame_to_save = frame.copy()
    frame_to_save["bar_start"] = frame_to_save["bar_start"].astype(str)
    frame_to_save["bar_end"] = frame_to_save["bar_end"].astype(str)
    frame_to_save.to_json(path, orient="records", force_ascii=False, indent=2)
    parquet_path = None
    if _has_module("pyarrow"):
        try:
            parquet_path = cache_dir / f"{source.replace(':', '_')}.parquet"
            frame.to_parquet(parquet_path, index=False)
        except Exception:
            parquet_path = None
    return {"json": str(path), "parquet": str(parquet_path) if parquet_path else None}


def _to_pandas_rule(timeframe: str) -> str:
    mapping = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min"}
    return mapping.get(timeframe, "30min")


def _normalize_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _normalize_symbol(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    code = value.split(".")[0]
    if "." in value:
        return value
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def _symbol_variants(symbol: str) -> set[str]:
    normalized = _normalize_symbol(symbol)
    code = normalized.split(".")[0]
    return {str(symbol or "").strip().upper(), normalized, code}


def _has_module(name: str) -> bool:
    try:
        __import__(name)
        return True
    except Exception:
        return False




def _safe_iso(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            return str(value)
    return str(value)
