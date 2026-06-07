"""Middleware that adds API version headers to every response.

This allows clients to discover the API version without relying on
URL-prefix conventions (which are currently inconsistent across routes).
"""

from __future__ import annotations

from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from api.core.versioning import get_version


class VersionHeaderMiddleware(BaseHTTPMiddleware):
    """Inject ``X-API-Version`` and ``X-API-Version-Date`` response headers."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        response.headers["X-API-Version"] = get_version()
        # Don't cache version header
        response.headers["Vary"] = (
            response.headers.get("Vary", "") + ", X-API-Version"
        ).strip(", ")
        return response
