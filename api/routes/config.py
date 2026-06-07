from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from api.database import SessionLocal, UserDB, get_db
from api.deps import require_web_user
from api.schemas.config import (
    UserRuntimeConfigResponse,
    UserRuntimeConfigUpdateRequest,
    UserRuntimeWarmupRequest,
    UserRuntimeWarmupResponse,
    WecomWebhookWarmupRequest,
    WecomWebhookWarmupResponse,
)
from api.services import auth_service

router = APIRouter(prefix="/v1", tags=["Config"])
logger = logging.getLogger(__name__)

_LLM_EVENT_REFRESH_FIELDS = {
    "llm_provider",
    "backend_url",
    "quick_think_llm",
    "deep_think_llm",
    "news_llm_provider",
    "news_backend_url",
    "news_analysis_llm",
    "api_key",
    "news_api_key",
    "clear_api_key",
    "clear_news_api_key",
}


def _llm_config_update_requested(updates: UserRuntimeConfigUpdateRequest) -> bool:
    payload = updates.model_dump()
    for key in _LLM_EVENT_REFRESH_FIELDS:
        value = payload.get(key)
        if key in {"clear_api_key", "clear_news_api_key"}:
            if value:
                return True
            continue
        if value is not None:
            return True
    return False


def _build_event_selection_refresh_state(
    db: Session,
    *,
    user_id: str,
    updates: UserRuntimeConfigUpdateRequest,
) -> dict:
    requested = _llm_config_update_requested(updates)
    base = {
        "requested": requested,
        "triggered": False,
        "status": "skipped",
        "reason": "non_llm_config_change",
        "windows": ["premarket", "24h"],
    }
    if not requested:
        return base

    from api.services import news_theme_service

    readiness = news_theme_service.core_stock_llm_readiness(db, user_id=user_id)
    if not readiness.get("ready"):
        return {
            **base,
            "reason": readiness.get("status") or "llm_not_ready",
            "llm_core_stock": readiness,
        }
    return {
        **base,
        "triggered": True,
        "status": "scheduled",
        "reason": "llm_ready",
        "llm_core_stock": readiness,
    }


def _run_event_driven_selection_refresh(user_id: str) -> None:
    from api.services import catalyst_selection_service

    db = SessionLocal()
    try:
        catalyst_selection_service.refresh_event_driven_selection(
            db,
            trigger="config:llm-updated",
            windows=("premarket", "24h"),
            limit=10,
            user_id=user_id,
        )
    except Exception:
        logger.exception("[config] event-driven catalyst refresh failed after LLM config update")
    finally:
        db.close()


@router.get("/config", response_model=UserRuntimeConfigResponse)
def get_runtime_config(
    db: Session = Depends(get_db),
    current_user: UserDB = Depends(require_web_user),
):
    from api import main as compat

    return compat._config_response_for_user(current_user, db)


@router.patch("/config")
def update_runtime_config(
    updates: UserRuntimeConfigUpdateRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: UserDB = Depends(require_web_user),
):
    normalized_wecom_webhook = None
    if updates.wecom_webhook_url:
        from api.services.wecom_notification_service import normalize_webhook_url

        try:
            normalized_wecom_webhook = normalize_webhook_url(updates.wecom_webhook_url)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    persistent_user = db.query(UserDB).filter(UserDB.id == current_user.id).first() or current_user
    from api import main as compat

    before_cfg = compat._config_response_for_user(persistent_user, db)
    pending_cfg = compat._build_pending_runtime_config(updates, persistent_user.id, db)
    if compat._should_probe_runtime_config(before_cfg, pending_cfg, updates):
        compat._probe_runtime_config(pending_cfg)

    row = auth_service.upsert_user_llm_config(
        db,
        persistent_user.id,
        llm_provider=updates.llm_provider,
        deep_think_llm=updates.deep_think_llm,
        quick_think_llm=updates.quick_think_llm,
        backend_url=updates.backend_url,
        news_llm_provider=updates.news_llm_provider,
        news_backend_url=updates.news_backend_url,
        news_analysis_llm=updates.news_analysis_llm,
        max_debate_rounds=updates.max_debate_rounds,
        max_risk_discuss_rounds=updates.max_risk_discuss_rounds,
        api_key=updates.api_key,
        news_api_key=updates.news_api_key,
        wecom_webhook_url=normalized_wecom_webhook,
        clear_api_key=updates.clear_api_key,
        clear_news_api_key=updates.clear_news_api_key,
        clear_wecom_webhook=updates.clear_wecom_webhook,
        default_analysts=updates.default_analysts,
        qmt_paper_account_config=updates.qmt_paper_account.model_dump() if updates.qmt_paper_account else None,
        qmt_live_account_config=updates.qmt_live_account.model_dump() if updates.qmt_live_account else None,
    )
    if updates.email_report_enabled is not None:
        persistent_user.email_report_enabled = updates.email_report_enabled
    if updates.wecom_report_enabled is not None:
        persistent_user.wecom_report_enabled = updates.wecom_report_enabled
    db.commit()

    if _llm_config_update_requested(updates):
        from api.services import news_theme_service

        news_theme_service.clear_core_stock_llm_error_cache(db)
        db.commit()

    current_cfg = compat._config_response_for_user(persistent_user, db)
    warmup_models = compat._warmup_model_names(current_cfg.model_dump())
    should_warmup = compat._should_trigger_config_warmup(before_cfg, current_cfg, updates)
    if should_warmup:
        background_tasks.add_task(compat._run_config_warmup, pending_cfg, persistent_user.id)
    event_selection_refresh = _build_event_selection_refresh_state(
        db,
        user_id=persistent_user.id,
        updates=updates,
    )
    if event_selection_refresh.get("triggered"):
        background_tasks.add_task(_run_event_driven_selection_refresh, persistent_user.id)

    applied = {
        key: value
        for key, value in updates.model_dump().items()
        if value is not None
        and key not in {"api_key", "news_api_key", "wecom_webhook_url", "warmup", "force_warmup"}
        and not (key in {"clear_api_key", "clear_news_api_key", "clear_wecom_webhook"} and not value)
    }
    return {
        "message": "用户配置已更新",
        "applied": applied,
        "current": current_cfg,
        "has_api_key": bool(row.api_key_encrypted),
        "warmup": {
            "requested": bool(updates.warmup or updates.force_warmup),
            "triggered": bool(should_warmup),
            "status": "scheduled" if should_warmup else "skipped",
            "models": warmup_models,
        },
        "event_driven_selection": event_selection_refresh,
    }


@router.post("/config/warmup", response_model=UserRuntimeWarmupResponse)
def warmup_runtime_config(
    request: UserRuntimeWarmupRequest,
    db: Session = Depends(get_db),
    current_user: UserDB = Depends(require_web_user),
):
    from api import main as compat

    pending_cfg = compat._build_pending_runtime_config(request, current_user.id, db)
    prompt = (request.prompt or "").strip() or "你好"
    results = compat._invoke_runtime_warmup(pending_cfg, prompt, current_user.id)
    return {"prompt": prompt, "results": results}


@router.post("/config/wecom/warmup", response_model=WecomWebhookWarmupResponse)
async def warmup_wecom_webhook(
    request: WecomWebhookWarmupRequest,
    db: Session = Depends(get_db),
    current_user: UserDB = Depends(require_web_user),
):
    from api import main as compat
    from api.services.wecom_notification_service import build_test_message, normalize_webhook_url, send_message

    webhook_url = (request.wecom_webhook_url or "").strip()
    if not webhook_url:
        user_cfg = auth_service.get_user_llm_config(db, current_user.id)
        webhook_url = auth_service.decrypt_secret(getattr(user_cfg, "wecom_webhook_encrypted", None)) or ""
    if not webhook_url:
        raise HTTPException(status_code=400, detail="请先填写或保存企业微信 Webhook")
    webhook_url = normalize_webhook_url(webhook_url)
    sent = await asyncio.to_thread(send_message, build_test_message(request.content), webhook_url)
    if not sent:
        raise HTTPException(status_code=400, detail="Webhook 测试发送失败，请检查地址或机器人状态")
    return {
        "sent": True,
        "message": "Webhook 测试发送成功",
        "webhook_display": compat._mask_wecom_webhook(webhook_url),
    }
