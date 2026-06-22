from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.database import UserDB, get_db_ctx
from api.services import auth_service, token_service

_auth_scheme = HTTPBearer(auto_error=False)
# Dev-only token – only active when APP_ENV != "production" and TA_DEV_ACCESS_TOKEN is explicitly set.
_DEFAULT_DEV_ACCESS_TOKEN = os.getenv("TA_DEV_ACCESS_TOKEN", "")
_DEFAULT_DEV_USER_ID = os.getenv("TA_DEV_USER_ID", "test-user-001")
_DEFAULT_DEV_USER_EMAIL = os.getenv("TA_DEV_USER_EMAIL", "test@example.com")


def _is_dev_mode() -> bool:
    return os.getenv("APP_ENV", "development").lower() != "production"


def _dev_access_token() -> str | None:
    return os.getenv("TA_DEV_ACCESS_TOKEN", "").strip() or None


def _dev_user_email() -> str:
    return os.getenv("TA_DEV_USER_EMAIL", _DEFAULT_DEV_USER_EMAIL).strip().lower() or _DEFAULT_DEV_USER_EMAIL


def _explicit_dev_user_id() -> Optional[str]:
    value = os.getenv("TA_DEV_USER_ID")
    return value.strip() if value and value.strip() else None


def _resolve_dev_user(db) -> UserDB:
    now = datetime.now(timezone.utc)
    dev_email = _dev_user_email()
    dev_user_id = _explicit_dev_user_id()
    if dev_user_id:
        user = auth_service.get_user_by_id(db, dev_user_id) or auth_service.get_user_by_email(db, dev_email)
        create_user_id = dev_user_id
    else:
        user = auth_service.get_user_by_email(db, dev_email)
        create_user_id = _DEFAULT_DEV_USER_ID if dev_email == _DEFAULT_DEV_USER_EMAIL else str(uuid4())
    if user is None:
        user = UserDB(
            id=create_user_id,
            email=dev_email,
            is_active=True,
            created_at=now,
            updated_at=now,
            last_login_at=now,
            last_login_ip="127.0.0.1",
        )
        db.add(user)
    else:
        user.id = user.id or create_user_id
        user.email = user.email or dev_email
        user.is_active = True
        user.updated_at = now
        user.last_login_at = now
        user.last_login_ip = "127.0.0.1"
    db.commit()
    db.refresh(user)
    return user


class RequireUser:
    def __init__(self, allow_api_token: bool = True):
        self.allow_api_token = allow_api_token

    def __call__(self, credentials: Optional[HTTPAuthorizationCredentials] = Depends(_auth_scheme)) -> UserDB:
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="请先登录")
        token = credentials.credentials
        with get_db_ctx() as db:
            dev_token = _dev_access_token()
            if _is_dev_mode() and dev_token and token == dev_token:
                return _resolve_dev_user(db)
            try:
                payload = auth_service.decode_access_token(token)
                user_id = str(payload.get("sub") or "")
                user = auth_service.get_user_by_id(db, user_id)
                if user and user.is_active:
                    return user
            except Exception:
                pass
            if self.allow_api_token and token.startswith(token_service.TOKEN_PREFIX):
                user = token_service.verify_token(db, token)
                if user and user.is_active:
                    return user
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="身份验证失败")


require_api_user = RequireUser(allow_api_token=True)
require_web_user = RequireUser(allow_api_token=False)


def optional_web_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(_auth_scheme)) -> Optional[UserDB]:
    if not credentials:
        return None
    try:
        return RequireUser(allow_api_token=False)(credentials)
    except HTTPException:
        return None
