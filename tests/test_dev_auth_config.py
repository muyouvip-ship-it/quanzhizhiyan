from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient

from api.database import UserDB, get_db_ctx, init_db
from api.services import auth_service


def test_dev_access_token_uses_configured_user_email(monkeypatch):
    from api.main import app

    init_db()
    email = f"dev-user-{uuid4().hex[:8]}@example.com"
    now = datetime.now(timezone.utc)
    with get_db_ctx() as db:
        user = UserDB(
            id=str(uuid4()),
            email=email,
            is_active=True,
            created_at=now,
            updated_at=now,
            last_login_at=now,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        expected_user_id = user.id

    monkeypatch.setenv("TA_DEV_USER_EMAIL", email)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/v1/auth/me", headers={"Authorization": "Bearer dev-test-token-001"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == expected_user_id
    assert payload["email"] == email


def test_dev_access_token_reads_complete_configured_user_llm_runtime(monkeypatch):
    from api.main import app

    init_db()
    email = f"dev-llm-{uuid4().hex[:8]}@example.com"
    now = datetime.now(timezone.utc)
    with get_db_ctx() as db:
        user = UserDB(
            id=str(uuid4()),
            email=email,
            is_active=True,
            created_at=now,
            updated_at=now,
            last_login_at=now,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        auth_service.upsert_user_llm_config(
            db,
            user.id,
            llm_provider="openai",
            backend_url="https://example-llm.test/v1",
            quick_think_llm="remote-test-model",
            deep_think_llm="remote-test-model",
            api_key="test-remote-key",
        )

    monkeypatch.setenv("TA_DEV_USER_EMAIL", email)
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "1")
    monkeypatch.setenv("TA_BASE_URL", "https://forced-llm.example/v1")
    monkeypatch.setenv("TA_LLM_QUICK", "forced-quick-model")
    monkeypatch.setenv("TA_LLM_DEEP", "forced-deep-model")
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/v1/config", headers={"Authorization": "Bearer dev-test-token-001"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["has_api_key"] is True
    assert payload["backend_url"] == "https://example-llm.test/v1"
    assert payload["quick_think_llm"] == "remote-test-model"
    assert payload["deep_think_llm"] == "remote-test-model"
    assert payload["llm_core_stock"]["ready"] is True
    assert payload["llm_core_stock"]["api_key_source"] == "user_config"
    assert payload["llm_core_stock"]["base_url_source"] == "user_config"
    assert payload["llm_core_stock"]["model_source"] == "user_config"
