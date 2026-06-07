from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import logging

logger = logging.getLogger(__name__)
from typing import Any, Optional
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken
import jwt
from jwt.exceptions import PyJWTError as JWTError
from sqlalchemy.orm import Session

from api.core.settings import settings
from api.database import EmailVerificationCodeDB, UserDB, UserLLMConfigDB


ALGORITHM = "HS256"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


_DEFAULT_SECRET = "tradingagents-ashare-dev-secret-local-2026"


def _secret_key() -> str:
    return os.getenv("TA_APP_SECRET_KEY") or _DEFAULT_SECRET


def _fernet_from_key(key: str) -> Fernet:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _fernet() -> Fernet:
    return _fernet_from_key(_secret_key())


def is_custom_secret_configured() -> bool:
    return bool(os.getenv("TA_APP_SECRET_KEY"))


def encrypt_secret(value: str) -> str:
    return _fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        return _fernet().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        return None


def decrypt_secret_with_fallback(value: Optional[str]) -> Optional[str]:
    """Decrypt trying current key first, then default key as fallback."""
    if not value:
        return None
    # Try current key
    try:
        return _fernet().decrypt(value.encode("utf-8")).decode("utf-8")
    except InvalidToken:
        pass
    # Try default key (first-time migration: no key → custom key)
    if is_custom_secret_configured():
        try:
            return _fernet_from_key(_DEFAULT_SECRET).decrypt(value.encode("utf-8")).decode("utf-8")
        except InvalidToken:
            pass
    return None


def normalize_email(email: str) -> str:
    return email.strip().lower()


def generate_login_code() -> str:
    return f"{secrets.randbelow(1000000):06d}"


def hash_code(email: str, code: str) -> str:
    return hashlib.sha256(f"{normalize_email(email)}:{code}:{_secret_key()}".encode("utf-8")).hexdigest()


def create_access_token(user: UserDB, expires_days: int = 30) -> str:
    now = _utcnow()
    payload = {
        "sub": user.id,
        "email": user.email,
        "exp": now + timedelta(days=expires_days),
        "iat": now,
    }
    return jwt.encode(payload, _secret_key(), algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, _secret_key(), algorithms=[ALGORITHM])


def get_user_by_email(db: Session, email: str) -> Optional[UserDB]:
    return db.query(UserDB).filter(UserDB.email == normalize_email(email)).first()


def get_user_by_id(db: Session, user_id: str) -> Optional[UserDB]:
    return db.query(UserDB).filter(UserDB.id == user_id).first()


def upsert_login_code(db: Session, email: str, purpose: str = "login") -> str:
    email = normalize_email(email)
    code = generate_login_code()
    now = _utcnow()

    db.query(EmailVerificationCodeDB).filter(
        EmailVerificationCodeDB.email == email,
        EmailVerificationCodeDB.purpose == purpose,
        EmailVerificationCodeDB.consumed_at.is_(None),
    ).update({"consumed_at": now})

    row = EmailVerificationCodeDB(
        id=str(uuid4()),
        email=email,
        code_hash=hash_code(email, code),
        purpose=purpose,
        expires_at=now + timedelta(minutes=10),
        created_at=now,
    )
    db.add(row)
    db.commit()
    return code


def verify_login_code(db: Session, email: str, code: str, purpose: str = "login", client_ip: Optional[str] = None) -> Optional[UserDB]:
    email = normalize_email(email)
    now = _utcnow()
    code_row = (
        db.query(EmailVerificationCodeDB)
        .filter(
            EmailVerificationCodeDB.email == email,
            EmailVerificationCodeDB.purpose == purpose,
            EmailVerificationCodeDB.consumed_at.is_(None),
        )
        .order_by(EmailVerificationCodeDB.created_at.desc())
        .first()
    )
    expires_at = _as_utc(code_row.expires_at) if code_row else None
    if not code_row or not expires_at or expires_at < now:
        return None
    if code_row.code_hash != hash_code(email, code):
        return None

    code_row.consumed_at = now
    user = get_user_by_email(db, email)
    if not user:
        user = UserDB(
            id=str(uuid4()),
            email=email,
            is_active=True,
            created_at=now,
            updated_at=now,
            last_login_at=now,
            last_login_ip=client_ip,
        )
        db.add(user)
    else:
        user.last_login_at = now
        user.last_login_ip = client_ip
        user.updated_at = now
    db.commit()
    db.refresh(user)
    return user


def get_env_alias(keys: list[str], default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            return v
    return default


def send_login_code(email: str, code: str) -> Optional[str]:
    smtp_host = get_env_alias(["MAIL_HOST", "MAIL_SERVER", "SMTP_HOST"]).strip()
    if not smtp_host:
        logger.info("[auth] login code for %s: %s", email, code)
        if os.getenv("APP_ENV", "development") != "production":
            return code
        return None

    smtp_port = int(get_env_alias(["MAIL_PORT", "SMTP_PORT"]) or "587")
    smtp_user = get_env_alias(["MAIL_USER", "MAIL_USERNAME", "SMTP_USER"]).strip()
    smtp_password = get_env_alias(["MAIL_PASS", "MAIL_PASSWORD", "SMTP_PASSWORD"]).strip()
    smtp_from = get_env_alias(["MAIL_FROM", "SMTP_FROM"], smtp_user or "noreply@example.com").strip()
    
    # 兼容旧版的逻辑
    smtp_starttls_str = get_env_alias(["MAIL_STARTTLS", "SMTP_TLS"], "1").strip().lower()
    smtp_starttls = smtp_starttls_str not in ("0", "false", "off", "no")
    
    smtp_ssl_tls_str = get_env_alias(["MAIL_SSL", "MAIL_SSL_TLS"], "0").strip().lower()
    smtp_ssl_tls = smtp_ssl_tls_str in ("1", "true", "on", "yes")

    msg = EmailMessage()
    msg["Subject"] = "量化之神登录验证码"
    msg["From"] = smtp_from
    msg["To"] = email
    msg.set_content(f"你的量化之神登录验证码是：{code}\n\n10 分钟内有效。")

    try:
        logger.info("[auth] connecting to %s:%s (SSL: %s, STARTTLS: %s)", smtp_host, smtp_port, smtp_ssl_tls, smtp_starttls)
        smtp_cls = smtplib.SMTP_SSL if smtp_ssl_tls else smtplib.SMTP
        with smtp_cls(smtp_host, smtp_port, timeout=20) as server:
            if smtp_starttls and not smtp_ssl_tls:
                server.starttls()
            if smtp_user:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
        return None
    except Exception as e:
        logger.warning("[auth] failed to send email via %s: %s", smtp_host, e)
        logger.info("[auth] falling back to console log. code for %s: %s", email, code)
        if os.getenv("APP_ENV", "development") != "production":
            return code
        return None


def get_user_llm_config(db: Session, user_id: str) -> Optional[UserLLMConfigDB]:
    return db.query(UserLLMConfigDB).filter(UserLLMConfigDB.user_id == user_id).first()


def _parse_json_text(value: Optional[str]) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def normalize_qmt_account_config(payload: Optional[dict[str, Any]], *, role: str, defaults: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    base = dict(defaults or {})
    source = dict(payload or {})
    target_role = "live" if str(role or "").strip().lower() == "live" else "paper"
    key_default = "live_real" if target_role == "live" else "paper_sim"

    port_value = source.get("port", base.get("port", 58610))
    try:
        port = int(port_value or 58610)
    except Exception:
        port = 58610

    return {
        "key": str(source.get("key") or base.get("key") or key_default).strip() or key_default,
        "role": target_role,
        "enabled": bool(source.get("enabled", base.get("enabled", False))),
        "host": str(source.get("host") or base.get("host") or settings.qmt_host).strip() or settings.qmt_host,
        "port": port,
        "account_id": str(source.get("account_id") or base.get("account_id") or "").strip(),
        "account_type": str(source.get("account_type") or base.get("account_type") or "STOCK").strip() or "STOCK",
        "account_name": str(source.get("account_name") or base.get("account_name") or ("QMT 实盘账户" if target_role == "live" else "QMT 虚拟账户")).strip() or ("QMT 实盘账户" if target_role == "live" else "QMT 虚拟账户"),
        "userdata_path": str(source.get("userdata_path") or base.get("userdata_path") or "").strip(),
        "bridge_base_url": str(source.get("bridge_base_url") or base.get("bridge_base_url") or "").strip(),
    }


def default_qmt_account_configs() -> dict[str, dict[str, Any]]:
    # QMT account identity must come from the user runtime settings stored in DB.
    # Environment variables may still provide generic connectivity defaults, but
    # they should not silently enable or inject a trading account.
    defaults: dict[str, dict[str, Any]] = {
        "paper": normalize_qmt_account_config(
            {
                "key": "paper_sim",
                "role": "paper",
                "enabled": False,
                "host": settings.qmt_host,
                "port": settings.qmt_port,
                "account_type": settings.qmt_account_type,
                "account_name": "QMT 模拟账户",
            },
            role="paper",
        ),
        "live": normalize_qmt_account_config(
            {
                "key": "live_real",
                "role": "live",
                "enabled": False,
                "host": settings.qmt_host,
                "port": settings.qmt_port,
                "account_id": "",
                "account_type": settings.qmt_account_type,
                "account_name": "QMT 实盘账户",
                "userdata_path": "",
                "bridge_base_url": "",
            },
            role="live",
        ),
    }
    return defaults


def get_user_qmt_account_configs(db: Session, user_id: str) -> dict[str, dict[str, Any]]:
    defaults = default_qmt_account_configs()
    row = get_user_llm_config(db, user_id)
    if not row:
        return defaults
    paper_raw = _parse_json_text(getattr(row, "qmt_paper_account_config", None))
    live_raw = _parse_json_text(getattr(row, "qmt_live_account_config", None))
    return {
        "paper": normalize_qmt_account_config(paper_raw, role="paper", defaults=defaults["paper"]),
        "live": normalize_qmt_account_config(live_raw, role="live", defaults=defaults["live"]),
    }


def upsert_user_llm_config(
    db: Session,
    user_id: str,
    *,
    llm_provider: Optional[str] = None,
    backend_url: Optional[str] = None,
    quick_think_llm: Optional[str] = None,
    deep_think_llm: Optional[str] = None,
    news_llm_provider: Optional[str] = None,
    news_backend_url: Optional[str] = None,
    news_analysis_llm: Optional[str] = None,
    max_debate_rounds: Optional[int] = None,
    max_risk_discuss_rounds: Optional[int] = None,
    api_key: Optional[str] = None,
    news_api_key: Optional[str] = None,
    wecom_webhook_url: Optional[str] = None,
    clear_api_key: bool = False,
    clear_news_api_key: bool = False,
    clear_wecom_webhook: bool = False,
    default_analysts: Optional[list] = None,
    qmt_paper_account_config: Optional[dict[str, Any]] = None,
    qmt_live_account_config: Optional[dict[str, Any]] = None,
) -> UserLLMConfigDB:
    row = get_user_llm_config(db, user_id)
    now = _utcnow()
    if not row:
        row = UserLLMConfigDB(user_id=user_id, created_at=now, updated_at=now)
        db.add(row)

    if llm_provider is not None:
        row.llm_provider = llm_provider
    if backend_url is not None:
        row.backend_url = backend_url
    if quick_think_llm is not None:
        row.quick_think_llm = quick_think_llm
    if deep_think_llm is not None:
        row.deep_think_llm = deep_think_llm
    if news_llm_provider is not None:
        row.news_llm_provider = news_llm_provider
    if news_backend_url is not None:
        row.news_backend_url = news_backend_url
    if news_analysis_llm is not None:
        row.news_analysis_llm = news_analysis_llm
    if max_debate_rounds is not None:
        row.max_debate_rounds = max_debate_rounds
    if max_risk_discuss_rounds is not None:
        row.max_risk_discuss_rounds = max_risk_discuss_rounds

    if clear_api_key:
        row.api_key_encrypted = None
    elif api_key:
        row.api_key_encrypted = encrypt_secret(api_key)

    if clear_news_api_key:
        row.news_api_key_encrypted = None
    elif news_api_key:
        row.news_api_key_encrypted = encrypt_secret(news_api_key)

    if clear_wecom_webhook:
        row.wecom_webhook_encrypted = None
    elif wecom_webhook_url:
        row.wecom_webhook_encrypted = encrypt_secret(wecom_webhook_url)

    if default_analysts is not None:
        row.default_analysts = json.dumps(default_analysts)
    defaults = default_qmt_account_configs()
    if qmt_paper_account_config is not None:
        row.qmt_paper_account_config = json.dumps(
            normalize_qmt_account_config(qmt_paper_account_config, role="paper", defaults=defaults["paper"]),
            ensure_ascii=False,
        )
    if qmt_live_account_config is not None:
        row.qmt_live_account_config = json.dumps(
            normalize_qmt_account_config(qmt_live_account_config, role="live", defaults=defaults["live"]),
            ensure_ascii=False,
        )

    row.updated_at = now
    db.commit()
    db.refresh(row)
    return row
