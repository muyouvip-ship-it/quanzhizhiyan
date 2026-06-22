from __future__ import annotations

import json
import os
from dataclasses import dataclass

from api.core.env import load_project_env

load_project_env()


@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("ENV", "dev")
    app_version: str = os.getenv("APP_VERSION", "dev")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    cors_allow_origins: str = os.getenv("CORS_ALLOW_ORIGINS", "")
    cors_allow_origin_regex: str = os.getenv("CORS_ALLOW_ORIGIN_REGEX", "")
    allow_server_llm_fallback: bool = os.getenv("ALLOW_SERVER_LLM_FALLBACK", "1").lower() in {"1", "true", "yes", "on"}
    ta_job_timeout: int = int(os.getenv("TA_JOB_TIMEOUT", "600"))
    ta_app_secret_key: str = os.getenv("TA_APP_SECRET_KEY", "")

    def __post_init__(self):
        if self.env == "prod" and not self.ta_app_secret_key:
            raise RuntimeError("TA_APP_SECRET_KEY must be set in production")
    database_url: str = os.getenv("DATABASE_URL", "")
    strategy_database_url: str = os.getenv("STRATEGY_DATABASE_URL", os.getenv("DATABASE_URL", ""))
    qmt_enabled: bool = os.getenv("QMT_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
    qmt_host: str = os.getenv("QMT_HOST", "192.168.10.1")
    qmt_port: int = int(os.getenv("QMT_PORT", "58610"))
    qmt_account_id: str = os.getenv("QMT_ACCOUNT_ID", "")
    qmt_account_type: str = os.getenv("QMT_ACCOUNT_TYPE", "STOCK")
    qmt_account_name: str = os.getenv("QMT_ACCOUNT_NAME", "QMT 模拟账户")
    qmt_userdata_path: str = os.getenv("QMT_USERDATA_PATH", "")
    qmt_refresh_interval_seconds: int = int(os.getenv("QMT_REFRESH_INTERVAL_SECONDS", "10"))
    qmt_default_account_key: str = os.getenv("QMT_DEFAULT_ACCOUNT_KEY", "")
    qmt_history_account_key: str = os.getenv("QMT_HISTORY_ACCOUNT_KEY", "paper_sim")
    qmt_minute_history_account_key: str = os.getenv("QMT_MINUTE_HISTORY_ACCOUNT_KEY", "live_real")
    qmt_accounts_json: str = os.getenv("QMT_ACCOUNTS_JSON", "")
    qmt_bridge_base_url: str = os.getenv("QMT_BRIDGE_BASE_URL", "")
    qmt_bridge_token: str = os.getenv("QMT_BRIDGE_TOKEN", "")

    def qmt_accounts(self) -> list[dict[str, object]]:
        raw = (self.qmt_accounts_json or "").strip()
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    normalized: list[dict[str, object]] = []
                    for index, item in enumerate(parsed):
                        if not isinstance(item, dict):
                            continue
                        normalized.append({
                            "key": str(item.get("key") or f"qmt_{index + 1}"),
                            "enabled": bool(item.get("enabled", True)),
                            "host": str(item.get("host") or self.qmt_host),
                            "port": int(item.get("port") or self.qmt_port),
                            "account_id": str(item.get("account_id") or ""),
                            "account_type": str(item.get("account_type") or self.qmt_account_type or "STOCK"),
                            "account_name": str(item.get("account_name") or item.get("name") or "QMT 账户"),
                            "userdata_path": str(item.get("userdata_path") or ""),
                            "role": str(item.get("role") or "paper"),
                            "bridge_base_url": str(item.get("bridge_base_url") or self.qmt_bridge_base_url or ""),
                            "bridge_token": str(item.get("bridge_token") or self.qmt_bridge_token or ""),
                            "refresh_interval_seconds": int(item.get("refresh_interval_seconds") or self.qmt_refresh_interval_seconds or 10),
                        })
                    if normalized:
                        return normalized
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning(
                    "Failed to parse QMT_ACCOUNTS_JSON, falling back to single-account config: %s", exc
                )
        return [{
            "key": self.qmt_default_account_key or "qmt_default",
            "enabled": self.qmt_enabled,
            "host": self.qmt_host,
            "port": self.qmt_port,
            "account_id": self.qmt_account_id,
            "account_type": self.qmt_account_type,
            "account_name": self.qmt_account_name,
            "userdata_path": self.qmt_userdata_path,
            "role": "paper",
            "bridge_base_url": self.qmt_bridge_base_url,
            "bridge_token": self.qmt_bridge_token,
            "refresh_interval_seconds": self.qmt_refresh_interval_seconds,
        }]


settings = Settings()
