"""Lightweight TTL cache with optional Redis backend.

Provides a simple key-value cache that can be backed by Redis when
``REDIS_URL`` is configured, falling back to an in-process dict with
TTL eviction.  Designed for hot-path data such as stock maps, trade
calendars, and market snapshots.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

_redis_client: Any = None
_redis_available: bool = False


def _init_redis() -> None:
    global _redis_client, _redis_available
    if _redis_client is not None:
        return
    redis_url = os.getenv("REDIS_URL") or os.getenv("JOB_STORE_REDIS_URL") or ""
    if not redis_url:
        _redis_available = False
        return
    try:
        import redis  # type: ignore

        _redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        _redis_client.ping()
        _redis_available = True
        logger.info("Cache: Redis backend connected at %s", redis_url)
    except Exception:
        _redis_available = False
        logger.debug("Cache: Redis unavailable, using in-memory fallback")


# ── In-memory TTL store ──────────────────────────────────────────────────────
_MEM_STORE: dict[str, tuple[float, Any]] = {}
_MEM_LOCK = threading.Lock()
_MEM_DEFAULT_TTL = 300  # seconds


def _mem_get(key: str) -> Any | None:
    with _MEM_LOCK:
        entry = _MEM_STORE.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() > expires_at:
            del _MEM_STORE[key]
            return None
        return value


def _mem_set(key: str, value: Any, ttl: int) -> None:
    with _MEM_LOCK:
        _MEM_STORE[key] = (time.monotonic() + ttl, value)


def _mem_delete(key: str) -> None:
    with _MEM_LOCK:
        _MEM_STORE.pop(key, None)


# ── Public API ────────────────────────────────────────────────────────────────
def cache_get(key: str) -> Any | None:
    """Return cached value or None if missing/expired."""
    _init_redis()
    if _redis_available:
        try:
            raw = _redis_client.get(key)
            if raw is None:
                return None
            import json

            return json.loads(raw)
        except Exception:
            pass
    return _mem_get(key)


def cache_set(key: str, value: Any, ttl: int = _MEM_DEFAULT_TTL) -> None:
    """Store *value* under *key* with a TTL in seconds."""
    _init_redis()
    if _redis_available:
        try:
            import json

            _redis_client.setex(key, ttl, json.dumps(value, default=str))
            return
        except Exception:
            pass
    _mem_set(key, value, ttl)


def cache_delete(key: str) -> None:
    """Remove *key* from cache."""
    _init_redis()
    if _redis_available:
        try:
            _redis_client.delete(key)
            return
        except Exception:
            pass
    _mem_delete(key)


def cache_get_or_set(key: str, factory: Callable[[], Any], ttl: int = _MEM_DEFAULT_TTL) -> Any:
    """Return cached value or compute via *factory*, store, and return."""
    cached = cache_get(key)
    if cached is not None:
        return cached
    value = factory()
    if value is not None:
        cache_set(key, value, ttl)
    return value


def cache_clear() -> None:
    """Clear all in-memory cache entries.  Does NOT flush Redis."""
    with _MEM_LOCK:
        _MEM_STORE.clear()
