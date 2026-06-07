from __future__ import annotations

import os


def get_version() -> str:
    """Get app version: APP_VERSION env > package metadata > 'dev'."""
    v = os.getenv("APP_VERSION")
    if v:
        return v
    try:
        from importlib.metadata import version as pkg_version
        return pkg_version("tradingagents")
    except Exception:
        import logging
        logging.getLogger(__name__).debug("Could not determine package version, using 'dev'")
        return "dev"
