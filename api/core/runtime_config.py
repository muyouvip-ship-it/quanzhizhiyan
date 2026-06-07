from __future__ import annotations

import os
from copy import deepcopy
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from api.database import UserDB, get_db_ctx
from api.services import auth_service

_CONFIG_OVERRIDES_ALLOWLIST = {
    "llm_provider", "backend_url", "deep_think_llm", "quick_think_llm",
    "max_debate_rounds", "max_risk_discuss_rounds",
    "prompt_language",
}
_API_KEY_ENV_ALIASES = (
    "TA_API_KEY",
    "VOLCENGINE_API_KEY",
    "ARK_API_KEY",
    "MAAS_API_KEY",
    "XFYUN_API_KEY",
    "XF_YUN_API_KEY",
    "XF_API_KEY",
    "IFLYTEK_API_KEY",
    "OPENAI_API_KEY",
)
_BASE_URL_ENV_ALIASES = (
    "TA_BASE_URL",
    "VOLCENGINE_BASE_URL",
    "ARK_BASE_URL",
    "MAAS_BASE_URL",
    "XFYUN_BASE_URL",
    "XF_YUN_BASE_URL",
    "OPENAI_BASE_URL",
    "OPENAI_API_BASE",
)
_QUICK_MODEL_ENV_ALIASES = ("TA_LLM_QUICK", "TA_LLM_MODEL", "VOLCENGINE_MODEL", "ARK_MODEL", "MAAS_MODEL", "OPENAI_MODEL")
_DEEP_MODEL_ENV_ALIASES = ("TA_LLM_DEEP", "TA_LLM_MODEL", "VOLCENGINE_MODEL", "ARK_MODEL", "MAAS_MODEL", "OPENAI_MODEL")
_FORCE_LLM_RUNTIME_ENV_ALIASES = ("TA_FORCE_LLM_RUNTIME", "TA_FORCE_LLM_ENDPOINT", "TA_ENFORCE_LLM_RUNTIME")
_USER_CONFIG_SOURCE = "user_config"
_USER_NEWS_CONFIG_SOURCE = "user_news_config"
_ACCOUNT_LLM_CONFIG_SOURCES = {_USER_CONFIG_SOURCE, _USER_NEWS_CONFIG_SOURCE}
_LLM_RUNTIME_SOURCE_KEYS = (
    "_api_key_source",
    "_llm_provider_source",
    "_backend_url_source",
    "_quick_think_llm_source",
    "_deep_think_llm_source",
)


def deep_merge(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _first_env(names: tuple[str, ...]) -> tuple[Optional[str], Optional[str]]:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip():
            return str(value).strip(), name
    return None, None


def _env_flag(names: tuple[str, ...], default: str = "0") -> bool:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return str(value).strip().lower() in {"1", "true", "yes", "on"}
    return default.strip().lower() in {"1", "true", "yes", "on"}


def force_environment_llm_runtime_enabled() -> bool:
    return _env_flag(_FORCE_LLM_RUNTIME_ENV_ALIASES)


def has_complete_user_llm_runtime(config: Dict[str, Any]) -> bool:
    """Return True when the user's stored LLM config is complete enough to use as a set."""
    if str(config.get("_api_key_source") or "").strip() != _USER_CONFIG_SOURCE:
        return False
    if not str(config.get("api_key") or "").strip():
        return False
    required_sources = {
        "llm_provider": "_llm_provider_source",
        "backend_url": "_backend_url_source",
    }
    for value_key, source_key in required_sources.items():
        if not str(config.get(value_key) or "").strip():
            return False
        if str(config.get(source_key) or "").strip() != _USER_CONFIG_SOURCE:
            return False
    model_sources = (
        ("quick_think_llm", "_quick_think_llm_source"),
        ("deep_think_llm", "_deep_think_llm_source"),
    )
    return any(
        str(config.get(value_key) or "").strip()
        and str(config.get(source_key) or "").strip() == _USER_CONFIG_SOURCE
        for value_key, source_key in model_sources
    )


def has_complete_user_news_llm_runtime(config: Dict[str, Any]) -> bool:
    """Return True when the user's stored news LLM config is complete enough to use as a set."""
    if str(config.get("_api_key_source") or "").strip() != _USER_NEWS_CONFIG_SOURCE:
        return False
    if not str(config.get("api_key") or "").strip():
        return False
    required_sources = {
        "llm_provider": "_llm_provider_source",
        "backend_url": "_backend_url_source",
        "quick_think_llm": "_quick_think_llm_source",
    }
    for value_key, source_key in required_sources.items():
        if not str(config.get(value_key) or "").strip():
            return False
        if str(config.get(source_key) or "").strip() != _USER_NEWS_CONFIG_SOURCE:
            return False
    return True


def account_llm_runtime_sources(config: Dict[str, Any]) -> set[str]:
    """Return account-backed sources currently participating in the LLM runtime."""
    return {
        str(config.get(key) or "").strip()
        for key in _LLM_RUNTIME_SOURCE_KEYS
        if str(config.get(key) or "").strip() in _ACCOUNT_LLM_CONFIG_SOURCES
    }


def llm_runtime_package_source(config: Dict[str, Any]) -> Optional[str]:
    """Identify whether provider, endpoint, model and key come from a complete package."""
    explicit_news_source = str(config.get("_news_llm_runtime_source") or "").strip()
    if explicit_news_source:
        return explicit_news_source
    if has_complete_user_news_llm_runtime(config):
        return _USER_NEWS_CONFIG_SOURCE
    if has_complete_user_llm_runtime(config):
        return _USER_CONFIG_SOURCE
    if account_llm_runtime_sources(config):
        return "mixed_runtime"
    if any(str(config.get(key) or "").strip() for key in _LLM_RUNTIME_SOURCE_KEYS):
        return "environment_runtime"
    return None


def has_mixed_account_llm_runtime(config: Dict[str, Any]) -> bool:
    """Return True when an account LLM field is mixed with another runtime package."""
    if has_complete_user_news_llm_runtime(config) or has_complete_user_llm_runtime(config):
        return False
    return bool(account_llm_runtime_sources(config))


def llm_runtime_source_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    """Return public runtime provenance fields without exposing secrets."""
    return {
        "runtime_package_source": llm_runtime_package_source(config),
        "api_key_source": str(config.get("_api_key_source") or "").strip() or None,
        "provider_source": str(config.get("_llm_provider_source") or "").strip() or None,
        "base_url_source": str(config.get("_backend_url_source") or "").strip() or None,
        "model_source": str(config.get("_deep_think_llm_source") or config.get("_quick_think_llm_source") or "").strip() or None,
        "account_runtime_sources": sorted(account_llm_runtime_sources(config)),
        "mixed_account_runtime": has_mixed_account_llm_runtime(config),
    }


def apply_environment_llm_endpoint_aliases(config: Dict[str, Any]) -> None:
    provider, provider_source = _first_env(("TA_LLM_PROVIDER",))
    if provider:
        config["llm_provider"] = provider
        config["_llm_provider_source"] = provider_source

    backend_url, backend_source = _first_env(_BASE_URL_ENV_ALIASES)
    if backend_url:
        config["backend_url"] = backend_url
        config["_backend_url_source"] = backend_source

    quick_model, quick_source = _first_env(_QUICK_MODEL_ENV_ALIASES)
    if quick_model:
        config["quick_think_llm"] = quick_model
        config["_quick_think_llm_source"] = quick_source

    deep_model, deep_source = _first_env(_DEEP_MODEL_ENV_ALIASES)
    if deep_model:
        config["deep_think_llm"] = deep_model
        config["_deep_think_llm_source"] = deep_source


def apply_forced_environment_llm_runtime(config: Dict[str, Any]) -> None:
    if not force_environment_llm_runtime_enabled():
        return
    if has_complete_user_llm_runtime(config):
        config["_llm_runtime_force_skipped"] = "complete_user_config"
        return
    apply_environment_llm_endpoint_aliases(config)
    config["_llm_runtime_forced"] = True


def _apply_environment_llm_aliases(config: Dict[str, Any]) -> None:
    apply_environment_llm_endpoint_aliases(config)
    api_key, api_key_source = _first_env(_API_KEY_ENV_ALIASES)
    if api_key:
        config["api_key"] = api_key
        config["_api_key_source"] = api_key_source
    elif str(config.get("api_key") or "").strip():
        config["_api_key_source"] = "default_config"
    else:
        config["_api_key_source"] = None


def user_config_overrides(user_id: Optional[str], db: Optional[Session] = None) -> Dict[str, Any]:
    if not user_id:
        return {}

    def _query(sess: Session) -> Dict[str, Any]:
        user_cfg = auth_service.get_user_llm_config(sess, user_id)
        if not user_cfg:
            return {}
        result: Dict[str, Any] = {}
        for key in (
            "llm_provider",
            "backend_url",
            "quick_think_llm",
            "deep_think_llm",
            "max_debate_rounds",
            "max_risk_discuss_rounds",
        ):
            value = getattr(user_cfg, key, None)
            if value is not None:
                result[key] = value
                if key in {"llm_provider", "backend_url", "quick_think_llm", "deep_think_llm"} and str(value).strip():
                    result[f"_{key}_source"] = "user_config"
        api_key = auth_service.decrypt_secret(user_cfg.api_key_encrypted)
        if api_key:
            result["api_key"] = api_key
            result["_api_key_source"] = "user_config"
        return result

    if db is not None:
        return _query(db)
    with get_db_ctx() as own_db:
        return _query(own_db)


def user_news_config_overrides(user_id: Optional[str], db: Optional[Session] = None) -> Dict[str, Any]:
    if not user_id:
        return {}

    def _query(sess: Session) -> Dict[str, Any]:
        user_cfg = auth_service.get_user_llm_config(sess, user_id)
        if not user_cfg:
            return {}
        result: Dict[str, Any] = {}
        mappings = (
            ("news_llm_provider", "llm_provider"),
            ("news_backend_url", "backend_url"),
            ("news_analysis_llm", "quick_think_llm"),
            ("news_analysis_llm", "deep_think_llm"),
        )
        for source_key, target_key in mappings:
            value = getattr(user_cfg, source_key, None)
            if value is not None:
                result[target_key] = value
                if str(value).strip():
                    result[f"_{target_key}_source"] = _USER_NEWS_CONFIG_SOURCE
        api_key = auth_service.decrypt_secret(getattr(user_cfg, "news_api_key_encrypted", None))
        if api_key:
            result["api_key"] = api_key
            result["_api_key_source"] = _USER_NEWS_CONFIG_SOURCE
        return result

    if db is not None:
        return _query(db)
    with get_db_ctx() as own_db:
        return _query(own_db)


def build_runtime_config(overrides: Dict[str, Any], user_id: Optional[str] = None, db: Optional[Session] = None) -> Dict[str, Any]:
    from tradingagents.default_config import DEFAULT_CONFIG

    config = deepcopy(DEFAULT_CONFIG)
    _apply_environment_llm_aliases(config)
    server_fallback_enabled = os.getenv("ALLOW_SERVER_LLM_FALLBACK", "1").strip().lower() in ("1", "true", "yes", "on")
    config["server_fallback_enabled"] = server_fallback_enabled

    overrides = {k: v for k, v in overrides.items() if k in _CONFIG_OVERRIDES_ALLOWLIST}
    user_overrides = user_config_overrides(user_id, db=db)

    filtered_user_overrides = {k: v for k, v in user_overrides.items() if v not in (None, "", [])}
    filtered_request_overrides = {k: v for k, v in overrides.items() if v not in (None, "", [])}

    if filtered_user_overrides:
        config = deep_merge(config, filtered_user_overrides)
    if filtered_request_overrides:
        config = deep_merge(config, filtered_request_overrides)

    apply_forced_environment_llm_runtime(config)

    quick = config.get("quick_think_llm")
    deep = config.get("deep_think_llm")

    if not deep and quick:
        config["deep_think_llm"] = quick
    if not quick and deep:
        config["quick_think_llm"] = deep

    package_source = llm_runtime_package_source(config)
    if package_source:
        config["_llm_runtime_package_source"] = package_source
    if has_mixed_account_llm_runtime(config):
        config["_llm_runtime_mixed_account"] = True

    return config


def build_news_runtime_config(user_id: Optional[str] = None, db: Optional[Session] = None) -> Dict[str, Any]:
    """Build the complete runtime config for news/theme LLM tasks.

    Hidden legacy ``news_*`` fields are treated as an all-or-nothing package:
    provider, base URL, model and API key must all come from the same account
    record. If that package is incomplete, callers keep using the normal
    account runtime config instead of mixing only a news key into another
    endpoint.
    """
    config = build_runtime_config({}, user_id=user_id, db=db)
    news_overrides = {k: v for k, v in user_news_config_overrides(user_id, db=db).items() if v not in (None, "", [])}
    if not news_overrides:
        return config
    candidate = deep_merge(deepcopy(config), news_overrides)
    if has_complete_user_news_llm_runtime(candidate):
        candidate["_news_llm_runtime_source"] = _USER_NEWS_CONFIG_SOURCE
        candidate["_llm_runtime_package_source"] = _USER_NEWS_CONFIG_SOURCE
        return candidate
    config["_news_llm_runtime_skipped"] = "incomplete_user_news_config"
    package_source = llm_runtime_package_source(config)
    if package_source:
        config["_llm_runtime_package_source"] = package_source
    if has_mixed_account_llm_runtime(config):
        config["_llm_runtime_mixed_account"] = True
    return config
