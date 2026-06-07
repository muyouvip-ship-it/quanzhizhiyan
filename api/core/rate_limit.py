"""Simple in-memory rate limiter for FastAPI endpoints.

Uses a sliding-window counter per (route, client-IP) key.
Configured via environment variables; falls back to no-op when disabled.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import defaultdict
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

_DEFAULT_WINDOW = 60  # seconds
_DEFAULT_MAX_REQUESTS = 120  # per window

_RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}

# Per-route overrides: {path_prefix: (window_seconds, max_requests)}
_ROUTE_LIMITS: dict[str, tuple[int, int]] = {
    "/v1/auth/": (60, 10),          # login: 10 req/min
    "/v1/chat/": (60, 30),          # LLM chat: 30 req/min
    "/v1/data-download/": (60, 20), # data download: 20 req/min
}

_WINDOWS: dict[str, list[float]] = defaultdict(list)
_LOCK = threading.Lock()


def _cleanup_expired(now: float, window_sec: int) -> None:
    """Periodically purge expired entries to avoid memory leaks."""
    with _LOCK:
        stale = [k for k, timestamps in _WINDOWS.items() if not timestamps or timestamps[-1] < now - window_sec * 2]
        for k in stale:
            del _WINDOWS[k]


_last_cleanup = time.monotonic()
_CLEANUP_INTERVAL = 300  # seconds


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if not _RATE_LIMIT_ENABLED:
            return await call_next(request)

        global _last_cleanup
        now = time.monotonic()
        if now - _last_cleanup > _CLEANUP_INTERVAL:
            _cleanup_expired(now, _DEFAULT_WINDOW)
            _last_cleanup = now

        path = request.url.path
        client_ip = request.client.host if request.client else "unknown"

        # Determine limit for this route
        window_sec = _DEFAULT_WINDOW
        max_req = _DEFAULT_MAX_REQUESTS
        for prefix, (w, m) in _ROUTE_LIMITS.items():
            if path.startswith(prefix):
                window_sec = w
                max_req = m
                break

        key = f"{path}:{client_ip}"
        with _LOCK:
            timestamps = _WINDOWS[key]
            # Remove expired entries
            cutoff = now - window_sec
            while timestamps and timestamps[0] < cutoff:
                timestamps.pop(0)

            if len(timestamps) >= max_req:
                logger.warning("Rate limit hit: %s from %s (%d/%d per %ds)", path, client_ip, len(timestamps), max_req, window_sec)
                return Response(
                    content='{"detail":"Too many requests. Please try again later."}',
                    status_code=429,
                    media_type="application/json",
                )

            timestamps.append(now)

        return await call_next(request)
