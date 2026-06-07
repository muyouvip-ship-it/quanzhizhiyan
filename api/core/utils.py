"""Shared utility functions used across the API layer.

Import from this module instead of redefining common helpers like
``_env_flag``, ``_safe_float``, or ``_normalize_symbol`` locally.
"""

from __future__ import annotations

import math
import os
from typing import Any


def env_flag(name: str, default: str = "0") -> bool:
    """Return True when *name* is set to a truthy value in the environment."""
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def safe_float(value: Any, *, ndigits: int | None = None) -> float | None:
    """Convert *value* to float, returning None on failure.

    Handles None, empty strings, NaN, and ±Inf gracefully.
    If *ndigits* is given the result is rounded to that many decimal places.
    """
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    if ndigits is not None:
        return round(result, ndigits)
    return result


def normalize_symbol(raw: Any) -> str:
    """Normalize a stock symbol to the standard ``code.EXCHANGE`` format.

    Accepts strings like ``sh600000``, ``600000.SH``, ``000001`` (with
    optional prefix/suffix) and returns ``600000.SH``-style strings.
    """
    if not isinstance(raw, str) or not raw.strip():
        return ""
    s = raw.strip()
    # Already canonical: 600000.SH / 000001.SZ / 920118.BJ
    if "." in s and len(s.split(".")) == 2:
        parts = s.split(".")
        code, market = parts[0].strip(), parts[1].strip().upper()
        if code.isdigit() and market in ("SH", "SZ", "BJ"):
            return f"{code}.{market}"
    # sh600000 / sz000001 / bj920118
    if len(s) >= 8 and s[:2].lower() in ("sh", "sz", "bj"):
        market = s[:2].upper()
        code = s[2:]
        if code.isdigit():
            return f"{code}.{market}"
    # Plain 6-digit code – guess exchange by prefix
    if s.isdigit() and len(s) == 6:
        if s.startswith(("6", "9")):
            return f"{s}.SH"
        if s.startswith(("0", "3")):
            return f"{s}.SZ"
        if s.startswith(("4", "8")):
            return f"{s}.BJ"
    return s


def normalize_symbols(symbols: list[str]) -> list[str]:
    """Apply :func:`normalize_symbol` to every element, dropping empties."""
    return [ns for raw in symbols if (ns := normalize_symbol(raw))]

import asyncio as _asyncio
import threading as _threading

def run_async(coro):
    """Safely run an async coroutine from a synchronous context.

    Uses ``asyncio.run()`` when no event loop is running, and falls back
    to creating a new event loop in a thread when one is already active.
    """
    try:
        loop = _asyncio.get_running_loop()
    except RuntimeError:
        return _asyncio.run(coro)

    # An event loop is already running — run in a new thread
    result_container: list = []
    error_container: list = []

    def _runner():
        try:
            new_loop = _asyncio.new_event_loop()
            _asyncio.set_event_loop(new_loop)
            result_container.append(new_loop.run_until_complete(coro))
        except Exception as exc:
            error_container.append(exc)
        finally:
            new_loop.close()

    thread = _threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if error_container:
        raise error_container[0]
    return result_container[0] if result_container else None
