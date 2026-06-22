from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from api.core.strategy_db import get_strategy_db
from api.database import get_db
from api.deps import require_api_user
from api.services import realtime_monitor_service


router = APIRouter(tags=["Realtime Monitor"])


class RealtimeMonitorCreateRequest(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    account_key: str = Field(default="paper_sim", min_length=1)
    strategy_id: str = Field(..., min_length=1)
    strategy_version_id: str | None = None
    execution_mode: Literal["auto", "monitor_only"] = "auto"
    live_trading_enabled: bool = False
    live_confirmed: bool = False
    monitor_pool: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    risk_config: dict[str, Any] = Field(default_factory=dict)


class RealtimeMonitorUpdateRequest(BaseModel):
    name: str | None = Field(default=None, max_length=200)
    account_key: str | None = Field(default=None, min_length=1)
    strategy_id: str | None = Field(default=None, min_length=1)
    strategy_version_id: str | None = None
    execution_mode: Literal["auto", "monitor_only"] | None = None
    live_trading_enabled: bool | None = None
    live_confirmed: bool | None = None
    monitor_pool: dict[str, Any] | None = None
    config: dict[str, Any] | None = None
    risk_config: dict[str, Any] | None = None


def _as_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=500, detail=str(exc))


@router.post("/v1/realtime/monitors")
def create_realtime_monitor(
    body: RealtimeMonitorCreateRequest,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
    db: Session = Depends(get_db),
):
    try:
        return realtime_monitor_service.create_monitor(strategy_db, db, current_user.id, body.model_dump(exclude_none=True))
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors")
def list_realtime_monitors(
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return {"items": realtime_monitor_service.list_monitors(strategy_db, current_user.id)}
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}")
def get_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.get_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.put("/v1/realtime/monitors/{monitor_id}")
def update_realtime_monitor(
    monitor_id: str,
    body: RealtimeMonitorUpdateRequest,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
    db: Session = Depends(get_db),
):
    try:
        return realtime_monitor_service.update_monitor(
            strategy_db,
            db,
            current_user.id,
            monitor_id,
            body.model_dump(exclude_unset=True),
        )
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.delete("/v1/realtime/monitors/{monitor_id}")
def delete_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.delete_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/start")
def start_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.start_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/pause")
def pause_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.pause_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/stop")
def stop_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.stop_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/resume")
def resume_realtime_monitor(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.resume_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/run-once")
def run_realtime_monitor_once(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
    db: Session = Depends(get_db),
):
    try:
        return realtime_monitor_service.run_monitor_once(strategy_db, db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.post("/v1/realtime/monitors/{monitor_id}/fuse-reset")
def reset_realtime_monitor_fuse(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return realtime_monitor_service.fuse_reset_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}/events")
def get_realtime_monitor_events(
    monitor_id: str,
    limit: int = Query(default=200, ge=1, le=50000),
    after_id: str | None = Query(default=None),
    since_started: bool = Query(default=False),
    activity_only: bool = Query(default=False),
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return {
            "items": realtime_monitor_service.list_events(
                strategy_db,
                current_user.id,
                monitor_id,
                limit=limit,
                after_id=after_id,
                since_started=since_started,
                activity_only=activity_only,
            )
        }
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}/stream")
async def stream_realtime_monitor(
    monitor_id: str,
    initial_limit: int = Query(default=200, ge=0, le=5000),
    initial_since_started: bool = Query(default=False),
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        monitor = realtime_monitor_service.get_monitor(strategy_db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc

    def _pack(event: str, payload: dict[str, Any]) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    async def event_generator():
        last_event_id: str | None = None
        seen_event_ids: set[str] = set()
        event_queue = realtime_monitor_service.subscribe_event_queue(current_user.id, monitor_id)
        yield _pack(
            "ready",
            {
                "monitor_id": monitor_id,
                "status": monitor.get("status"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        if initial_limit > 0:
            initial_items = realtime_monitor_service.list_events(
                strategy_db,
                current_user.id,
                monitor_id,
                limit=initial_limit,
                since_started=initial_since_started,
            )
            for item in initial_items:
                last_event_id = item.get("id") or last_event_id
                if item.get("id"):
                    seen_event_ids.add(str(item["id"]))
                yield _pack("event", {"initial": True, "item": item})

        try:
            while True:
                try:
                    try:
                        first_item = await asyncio.wait_for(event_queue.get(), timeout=15)
                        fresh_items = [first_item]
                        while len(fresh_items) < 200 and not event_queue.empty():
                            fresh_items.append(event_queue.get_nowait())
                    except asyncio.TimeoutError:
                        fresh_items = realtime_monitor_service.list_events(
                            strategy_db,
                            current_user.id,
                            monitor_id,
                            limit=200,
                            after_id=last_event_id,
                        )

                    emitted = False
                    for item in fresh_items:
                        item_id = str(item.get("id") or "")
                        if item_id and item_id in seen_event_ids:
                            continue
                        if item_id:
                            seen_event_ids.add(item_id)
                            if len(seen_event_ids) > 5000:
                                seen_event_ids = set(list(seen_event_ids)[-2500:])
                        last_event_id = item.get("id") or last_event_id
                        emitted = True
                        yield _pack("event", {"initial": False, "item": item})
                    if emitted:
                        monitor_payload = realtime_monitor_service.get_monitor(strategy_db, current_user.id, monitor_id)
                        yield _pack("state", {"monitor": monitor_payload})
                    else:
                        yield ": ping\n\n"
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    yield _pack(
                        "error",
                        {
                            "message": str(exc),
                            "monitor_id": monitor_id,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                    await asyncio.sleep(1)
        finally:
            realtime_monitor_service.unsubscribe_event_queue(current_user.id, monitor_id, event_queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/v1/realtime/monitors/{monitor_id}/orders")
def get_realtime_monitor_orders(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return {"items": realtime_monitor_service.list_orders(strategy_db, current_user.id, monitor_id)}
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}/trades")
def get_realtime_monitor_trades(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
):
    try:
        return {"items": realtime_monitor_service.list_trades(strategy_db, current_user.id, monitor_id)}
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}/positions")
def get_realtime_monitor_positions(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
    db: Session = Depends(get_db),
):
    try:
        return realtime_monitor_service.get_positions(strategy_db, db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc


@router.get("/v1/realtime/monitors/{monitor_id}/performance")
def get_realtime_monitor_performance(
    monitor_id: str,
    current_user=Depends(require_api_user),
    strategy_db: Session = Depends(get_strategy_db),
    db: Session = Depends(get_db),
):
    try:
        return realtime_monitor_service.get_performance(strategy_db, db, current_user.id, monitor_id)
    except Exception as exc:
        raise _as_http_error(exc) from exc

