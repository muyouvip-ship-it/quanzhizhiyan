from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from langchain_core.messages import HumanMessage
from sqlalchemy.orm import Session

from api.core.runtime_config import apply_forced_environment_llm_runtime, build_runtime_config
from api.database import UserDB
from api.schemas.config import QmtAccountConfigPayload, UserRuntimeConfigResponse, UserRuntimeConfigUpdateRequest
from api.services import auth_service
from tradingagents.llm_clients.factory import create_llm_client

_CONFIG_ALLOWED_KEYS = {
    "llm_provider",
    "deep_think_llm",
    "quick_think_llm",
    "backend_url",
    "max_debate_rounds",
    "max_risk_discuss_rounds",
}
_CONFIG_MODEL_KEYS = (
    "llm_provider",
    "backend_url",
    "quick_think_llm",
    "deep_think_llm",
    "news_llm_provider",
    "news_backend_url",
    "news_analysis_llm",
)
_DEFAULT_ANALYSTS = ["market", "social", "news", "fundamentals", "macro", "smart_money", "volume_price"]
_NORMAL_LLM_UPDATE_KEYS = {
    "llm_provider",
    "backend_url",
    "quick_think_llm",
    "deep_think_llm",
    "api_key",
    "clear_api_key",
}
_NEWS_LLM_UPDATE_KEYS = {
    "news_llm_provider",
    "news_backend_url",
    "news_analysis_llm",
    "news_api_key",
    "clear_news_api_key",
}


def warmup_model_names(config: Dict[str, Any]) -> List[str]:
    seen: set[str] = set()
    models: List[str] = []
    for key in ("quick_think_llm", "deep_think_llm"):
        value = str(config.get(key) or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        models.append(value)
    return models


def should_trigger_config_warmup(
    before_cfg: UserRuntimeConfigResponse,
    after_cfg: UserRuntimeConfigResponse,
    updates: UserRuntimeConfigUpdateRequest,
) -> bool:
    if bool(updates.force_warmup):
        return True
    if not bool(updates.warmup):
        return False
    for key in _CONFIG_MODEL_KEYS:
        if getattr(before_cfg, key) != getattr(after_cfg, key):
            return True
    return bool(updates.api_key or updates.news_api_key)


def _update_requested(updates: UserRuntimeConfigUpdateRequest, keys: set[str]) -> bool:
    payload = updates.model_dump()
    for key in keys:
        value = payload.get(key)
        if key.startswith("clear_"):
            if value:
                return True
            continue
        if value is not None:
            return True
    return False


def _build_pending_news_runtime_config(updates: UserRuntimeConfigUpdateRequest, user_id: str, db: Session) -> Dict[str, Any]:
    user_cfg = auth_service.get_user_llm_config(db, user_id)

    provider = updates.news_llm_provider
    if provider is None and user_cfg:
        provider = user_cfg.news_llm_provider

    backend_url = updates.news_backend_url
    if backend_url is None and user_cfg:
        backend_url = user_cfg.news_backend_url

    model = updates.news_analysis_llm
    if model is None and user_cfg:
        model = user_cfg.news_analysis_llm

    if updates.clear_news_api_key:
        api_key = ""
    elif updates.news_api_key:
        api_key = updates.news_api_key
    else:
        api_key = auth_service.decrypt_secret(getattr(user_cfg, "news_api_key_encrypted", None))

    return {
        "llm_provider": str(provider or "").strip(),
        "backend_url": str(backend_url or "").strip(),
        "quick_think_llm": str(model or "").strip(),
        "deep_think_llm": str(model or "").strip(),
        "api_key": str(api_key or "").strip(),
        "_pending_llm_package": "user_news_config",
        "_llm_provider_source": "user_news_config" if str(provider or "").strip() else None,
        "_backend_url_source": "user_news_config" if str(backend_url or "").strip() else None,
        "_quick_think_llm_source": "user_news_config" if str(model or "").strip() else None,
        "_deep_think_llm_source": "user_news_config" if str(model or "").strip() else None,
        "_api_key_source": "user_news_config" if str(api_key or "").strip() else None,
    }


def build_pending_runtime_config(updates: UserRuntimeConfigUpdateRequest, user_id: str, db: Session) -> Dict[str, Any]:
    news_update_requested = _update_requested(updates, _NEWS_LLM_UPDATE_KEYS)
    normal_update_requested = _update_requested(updates, _NORMAL_LLM_UPDATE_KEYS)
    if news_update_requested:
        pending_news = _build_pending_news_runtime_config(updates, user_id, db)
        news_package_complete = all(
            str(pending_news.get(key) or "").strip()
            for key in ("llm_provider", "backend_url", "quick_think_llm", "api_key")
        )
        if not normal_update_requested or news_package_complete:
            return pending_news

    config = build_runtime_config({}, user_id=user_id, db=db)
    for key in _CONFIG_ALLOWED_KEYS:
        value = getattr(updates, key, None)
        if value is not None:
            config[key] = value
    if updates.clear_api_key:
        config["api_key"] = ""
    elif updates.api_key:
        config["api_key"] = updates.api_key
    quick = config.get("quick_think_llm")
    deep = config.get("deep_think_llm")
    if not deep and quick:
        config["deep_think_llm"] = quick
    if not quick and deep:
        config["quick_think_llm"] = deep
    apply_forced_environment_llm_runtime(config)
    return config


def should_probe_runtime_config(
    before_cfg: UserRuntimeConfigResponse,
    pending_cfg: Dict[str, Any],
    updates: UserRuntimeConfigUpdateRequest,
) -> bool:
    del before_cfg
    if updates.clear_api_key or updates.clear_news_api_key:
        return False
    payload = updates.model_dump()
    llm_changed = any(
        payload.get(key) is not None
        for key in (
            "llm_provider",
            "backend_url",
            "quick_think_llm",
            "deep_think_llm",
            "news_llm_provider",
            "news_backend_url",
            "news_analysis_llm",
        )
    )
    if pending_cfg.get("_pending_llm_package") == "user_news_config":
        package_complete = all(
            str(pending_cfg.get(key) or "").strip()
            for key in ("llm_provider", "backend_url", "quick_think_llm", "api_key")
        )
        return package_complete and (
            bool(updates.news_api_key)
            or any(payload.get(key) is not None for key in ("news_llm_provider", "news_backend_url", "news_analysis_llm"))
            or bool(updates.force_warmup)
        )
    return bool(str(pending_cfg.get("api_key") or "").strip()) and (
        bool(updates.api_key)
        or llm_changed
        or bool(updates.force_warmup)
    )


def probe_runtime_config(config: Dict[str, Any]) -> Dict[str, str]:
    targets = warmup_model_names(config)
    if not targets:
        raise HTTPException(status_code=400, detail="请先配置至少一个可用模型。")
    try:
        result = _invoke_llm_once(config, targets[0], "请只回复 OK", timeout=12.0)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"模型 Key 验证失败：{_safe_error_message(exc, config)}") from exc
    return {"status": "ok", "model": targets[0], "content": result[:200]}


def invoke_runtime_warmup(
    config: Dict[str, Any],
    prompt: str,
    user_id: str,
    timeout: float = 20.0,
) -> List[Dict[str, Any]]:
    del user_id
    targets = warmup_model_names(config)
    if not targets:
        raise HTTPException(status_code=400, detail="请先配置至少一个可用模型。")
    results: List[Dict[str, Any]] = []
    for model in targets:
        try:
            started = time.monotonic()
            content = _invoke_llm_once(config, model, prompt, timeout=timeout)
            results.append(
                {
                    "model": model,
                    "targets": [model],
                    "content": content,
                    "error": None,
                    "latency_ms": int((time.monotonic() - started) * 1000),
                }
            )
        except Exception as exc:
            results.append(
                {
                    "model": model,
                    "targets": [model],
                    "content": None,
                    "error": _safe_error_message(exc, config),
                }
            )
    return results


def run_config_warmup(config: Dict[str, Any], user_id: str) -> None:
    try:
        invoke_runtime_warmup(config, "请只回复 OK", user_id, timeout=12.0)
    except Exception:
        pass


def _invoke_llm_once(config: Dict[str, Any], model: str, prompt: str, *, timeout: float) -> str:
    provider = str(config.get("llm_provider") or "").strip()
    if not provider:
        raise ValueError("LLM provider 未配置")
    if not model:
        raise ValueError("模型名未配置")
    kwargs: dict[str, Any] = {"timeout": timeout}
    api_key = str(config.get("api_key") or "").strip()
    if api_key:
        kwargs["api_key"] = api_key
    client = create_llm_client(
        provider=provider,
        model=model,
        base_url=str(config.get("backend_url") or "").strip() or None,
        **kwargs,
    )
    result = client.get_llm().invoke([HumanMessage(content=prompt)])
    return str(getattr(result, "content", "") or "").strip()


def _safe_error_message(exc: Exception, config: Dict[str, Any]) -> str:
    message = str(exc) or exc.__class__.__name__
    api_key = str(config.get("api_key") or "").strip()
    if api_key:
        message = message.replace(api_key, "***")
    return message[:500]


def mask_wecom_webhook(webhook_url: Optional[str]) -> Optional[str]:
    if not webhook_url:
        return None
    if "key=" not in webhook_url:
        return webhook_url
    prefix, key = webhook_url.split("key=", 1)
    if len(key) <= 4:
        return f"{prefix}key={key}"
    return f"{prefix}key=***{key[-4:]}"


def config_response_for_user(user: Optional[UserDB], db: Session) -> UserRuntimeConfigResponse:
    cfg = build_runtime_config({}, user_id=user.id if user else None, db=db)
    user_cfg = auth_service.get_user_llm_config(db, user.id) if user else None
    webhook_url = auth_service.decrypt_secret(getattr(user_cfg, "wecom_webhook_encrypted", None))
    default_analysts = _DEFAULT_ANALYSTS
    qmt_configs = auth_service.get_user_qmt_account_configs(db, user.id) if user else auth_service.default_qmt_account_configs()
    llm_core_stock: Dict[str, Any] = {}
    if user:
        from api.services import news_theme_service

        llm_core_stock = news_theme_service.core_stock_llm_readiness(db, user_id=user.id)
    if user_cfg and user_cfg.default_analysts:
        try:
            parsed = json.loads(user_cfg.default_analysts)
            if isinstance(parsed, list) and parsed:
                default_analysts = parsed
        except Exception:
            pass
    return UserRuntimeConfigResponse(
        llm_provider=str(cfg.get("llm_provider") or ""),
        deep_think_llm=str(cfg.get("deep_think_llm") or ""),
        quick_think_llm=str(cfg.get("quick_think_llm") or ""),
        backend_url=str(cfg.get("backend_url") or ""),
        news_llm_provider=str(getattr(user_cfg, "news_llm_provider", None) or "") if user_cfg else "",
        news_backend_url=str(getattr(user_cfg, "news_backend_url", None) or "") if user_cfg else "",
        news_analysis_llm=str(getattr(user_cfg, "news_analysis_llm", None) or "") if user_cfg else "",
        max_debate_rounds=int(cfg.get("max_debate_rounds") or 0),
        max_risk_discuss_rounds=int(cfg.get("max_risk_discuss_rounds") or 0),
        has_api_key=bool(user_cfg and user_cfg.api_key_encrypted),
        has_news_api_key=bool(user_cfg and getattr(user_cfg, "news_api_key_encrypted", None)),
        has_wecom_webhook=bool(webhook_url),
        wecom_webhook_display=mask_wecom_webhook(webhook_url),
        server_fallback_enabled=bool(cfg.get("server_fallback_enabled", True)),
        email_report_enabled=user.email_report_enabled if user else True,
        wecom_report_enabled=user.wecom_report_enabled if user else True,
        default_analysts=default_analysts,
        llm_core_stock=llm_core_stock,
        qmt_paper_account=QmtAccountConfigPayload(**qmt_configs["paper"]),
        qmt_live_account=QmtAccountConfigPayload(**qmt_configs["live"]),
    )
