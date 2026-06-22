from __future__ import annotations

import os
import subprocess
import time
from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from api.database import VersionStatsDB, get_db
from api.core.http_utils import get_real_ip
from api.core.versioning import get_version
from api.deps import optional_web_user
from api.services.data_source_governance import list_news_source_links, list_registered_sources, list_surface_registry
from api.services.market_data_pipeline_service import get_market_data_publish_status
from api.services.system_data_source_status_service import build_system_data_update_overview

router = APIRouter(tags=["System"])

_vs_rate_limit: Dict[str, float] = {}
_VS_RATE_INTERVAL = 3600


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return os.getenv("GIT_COMMIT", "unknown")


def _build_date() -> str:
    try:
        return subprocess.check_output(
            ["git", "show", "-s", "--format=%cd", "--date=format:%Y-%m-%d", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return os.getenv("BUILD_DATE", "unknown")


@router.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({
        "status": "ok",
        "version": get_version(),
        "commit": _git_commit(),
        "build_date": _build_date(),
    })


@router.get("/v1/version")
async def get_app_version() -> JSONResponse:
    return JSONResponse({
        "version": get_version(),
        "commit": _git_commit(),
        "build_date": _build_date(),
    })


@router.get("/v1/system/data-sources")
async def list_system_data_sources(
    db: Session = Depends(get_db),
    current_user=Depends(optional_web_user),
) -> dict[str, Any]:
    update_overview = build_system_data_update_overview(
        db,
        user_id=str(current_user.id) if current_user else None,
    )
    return {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources": list_registered_sources(),
        "surfaces": list_surface_registry(),
        "news_sources": list_news_source_links(),
        "update_cards": update_overview["cards"],
        "workers": update_overview["workers"],
    }


@router.get("/v1/system/market-data-status")
async def get_system_market_data_status(
    trade_date: str | None = None,
    symbols: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    symbol_list = [item.strip() for item in str(symbols or "").split(",") if item.strip()]
    payload = get_market_data_publish_status(
        trade_date=trade_date,
        symbols=symbol_list,
        limit=limit,
    )
    payload["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return payload


@router.post("/api/version-stats")
def version_stats(payload: Dict[str, Any] = Body(...), request: Request = None, db: Session = Depends(get_db)):
    remote_ip = get_real_ip(request)
    now = time.time()
    if remote_ip:
        last = _vs_rate_limit.get(remote_ip, 0)
        if now - last < _VS_RATE_INTERVAL:
            return {"status": "ok"}
        _vs_rate_limit[remote_ip] = now

    record = VersionStatsDB(
        version=str(payload.get("v", ""))[:50],
        nonce=str(payload.get("nonce", ""))[:64],
        remote_ip=remote_ip,
    )
    db.add(record)
    db.commit()
    return {"status": "ok"}
