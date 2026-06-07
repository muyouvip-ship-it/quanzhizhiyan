from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from api.database import UserDB, get_db_ctx, init_db
from api.main import _build_runtime_config
from api.schemas.config import UserRuntimeConfigUpdateRequest
from api.services import config_service
from api.services import auth_service
from api.core.runtime_config import build_runtime_config, llm_runtime_source_payload


def test_forced_runtime_llm_keeps_complete_user_runtime_config(monkeypatch):
    init_db()
    email = f"force-llm-{uuid4().hex[:8]}@example.com"
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
            backend_url="https://legacy.example/v1",
            quick_think_llm="legacy-quick",
            deep_think_llm="legacy-deep",
            api_key="legacy-key",
        )

        monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "1")
        monkeypatch.setenv("TA_LLM_PROVIDER", "openai")
        monkeypatch.setenv("TA_BASE_URL", "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2")
        monkeypatch.setenv("TA_LLM_QUICK", "astron-code-latest")
        monkeypatch.setenv("TA_LLM_DEEP", "astron-code-latest")

        runtime = build_runtime_config({}, user_id=user.id, db=db)
        compat_runtime = _build_runtime_config({}, user_id=user.id, db=db)

    for cfg in (runtime, compat_runtime):
        assert cfg["backend_url"] == "https://legacy.example/v1"
        assert cfg["quick_think_llm"] == "legacy-quick"
        assert cfg["deep_think_llm"] == "legacy-deep"
        assert cfg["api_key"] == "legacy-key"
        assert cfg["_api_key_source"] == "user_config"
        assert cfg["_backend_url_source"] == "user_config"
        assert cfg["_quick_think_llm_source"] == "user_config"
        assert cfg["_llm_runtime_force_skipped"] == "complete_user_config"


def test_forced_runtime_can_fill_incomplete_user_key_config(monkeypatch):
    init_db()
    email = f"force-fill-{uuid4().hex[:8]}@example.com"
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
        auth_service.upsert_user_llm_config(db, user.id, api_key="legacy-key")

        monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "1")
        monkeypatch.setenv("TA_LLM_PROVIDER", "openai")
        monkeypatch.setenv("TA_BASE_URL", "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2")
        monkeypatch.setenv("TA_LLM_QUICK", "astron-code-latest")
        monkeypatch.setenv("TA_LLM_DEEP", "astron-code-latest")

        runtime = build_runtime_config({}, user_id=user.id, db=db)
        compat_runtime = _build_runtime_config({}, user_id=user.id, db=db)

    for cfg in (runtime, compat_runtime):
        assert cfg["backend_url"] == "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2"
        assert cfg["quick_think_llm"] == "astron-code-latest"
        assert cfg["deep_think_llm"] == "astron-code-latest"
        assert cfg["api_key"] == "legacy-key"
        assert cfg["_api_key_source"] == "user_config"
        assert cfg["_backend_url_source"] == "TA_BASE_URL"
        assert cfg["_quick_think_llm_source"] == "TA_LLM_QUICK"
        assert cfg["_llm_runtime_forced"] is True
        assert cfg["_llm_runtime_package_source"] == "mixed_runtime"
        assert cfg["_llm_runtime_mixed_account"] is True


def test_runtime_marks_account_key_only_as_mixed_package(monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    monkeypatch.delenv("TA_API_KEY", raising=False)
    init_db()
    email = f"mixed-key-only-{uuid4().hex[:8]}@example.com"
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
        auth_service.upsert_user_llm_config(db, user.id, api_key="volcengine-key-only")

        runtime = build_runtime_config({}, user_id=user.id, db=db)

    assert runtime["api_key"] == "volcengine-key-only"
    assert runtime["_api_key_source"] == "user_config"
    assert runtime["_llm_runtime_package_source"] == "mixed_runtime"
    assert runtime["_llm_runtime_mixed_account"] is True
    sources = llm_runtime_source_payload(runtime)
    assert sources["runtime_package_source"] == "mixed_runtime"
    assert sources["api_key_source"] == "user_config"
    assert sources["mixed_account_runtime"] is True
    assert sources["account_runtime_sources"] == ["user_config"]


def test_runtime_source_payload_reports_complete_volcengine_package(monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    init_db()
    email = f"volcengine-complete-{uuid4().hex[:8]}@example.com"
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
            llm_provider="volcengine-ark",
            backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
            quick_think_llm="deepseek-v4-flash",
            deep_think_llm="deepseek-v4-pro",
            api_key="volcengine-key",
        )

        runtime = build_runtime_config({}, user_id=user.id, db=db)

    sources = llm_runtime_source_payload(runtime)
    assert sources["runtime_package_source"] == "user_config"
    assert sources["api_key_source"] == "user_config"
    assert sources["provider_source"] == "user_config"
    assert sources["base_url_source"] == "user_config"
    assert sources["model_source"] == "user_config"
    assert sources["mixed_account_runtime"] is False


def test_probe_runtime_config_calls_remote_llm(monkeypatch):
    captured: dict[str, object] = {}

    class FakeLLM:
        def invoke(self, messages):
            captured["messages"] = messages
            return type("Result", (), {"content": "OK"})()

    class FakeClient:
        def __init__(self, provider, model, base_url, **kwargs):
            captured.update({"provider": provider, "model": model, "base_url": base_url, "kwargs": kwargs})

        def get_llm(self):
            return FakeLLM()

    monkeypatch.setattr(config_service, "create_llm_client", lambda provider, model, base_url=None, **kwargs: FakeClient(provider, model, base_url, **kwargs))

    result = config_service.probe_runtime_config(
        {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "astron-code-latest",
            "api_key": "maas-key",
        }
    )

    assert result["status"] == "ok"
    assert captured["provider"] == "openai"
    assert captured["model"] == "astron-code-latest"
    assert captured["base_url"] == "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2"
    assert captured["kwargs"]["api_key"] == "maas-key"


def test_pending_news_runtime_uses_complete_news_package_for_key_update(monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    init_db()
    email = f"news-package-{uuid4().hex[:8]}@example.com"
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
            backend_url="https://main.example/v1",
            quick_think_llm="main-quick",
            deep_think_llm="main-deep",
            api_key="main-key",
            news_llm_provider="volcengine-ark",
            news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
            news_analysis_llm="deepseek-v4-flash",
            news_api_key="old-news-key",
        )

        updates = UserRuntimeConfigUpdateRequest(news_api_key="new-news-key")
        pending = config_service.build_pending_runtime_config(updates, user.id, db)

    assert pending["_pending_llm_package"] == "user_news_config"
    assert pending["llm_provider"] == "volcengine-ark"
    assert pending["backend_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert pending["quick_think_llm"] == "deepseek-v4-flash"
    assert pending["deep_think_llm"] == "deepseek-v4-flash"
    assert pending["api_key"] == "new-news-key"
    assert pending["_api_key_source"] == "user_news_config"
    assert config_service.should_probe_runtime_config(None, pending, updates) is True


def test_pending_runtime_prefers_complete_news_package_when_settings_submits_all_llm_fields(monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    init_db()
    email = f"settings-news-package-{uuid4().hex[:8]}@example.com"
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
            backend_url="https://main.example/v1",
            quick_think_llm="main-quick",
            deep_think_llm="main-deep",
            api_key="main-key",
            news_llm_provider="volcengine-ark",
            news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
            news_analysis_llm="deepseek-v4-flash",
            news_api_key="old-news-key",
        )

        updates = UserRuntimeConfigUpdateRequest(
            llm_provider="openai",
            backend_url="https://main.example/v1",
            quick_think_llm="main-quick",
            deep_think_llm="main-deep",
            api_key="main-key",
            news_llm_provider="volcengine-ark",
            news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
            news_analysis_llm="deepseek-v4-flash",
            news_api_key="new-news-key",
        )
        pending = config_service.build_pending_runtime_config(updates, user.id, db)

    assert pending["_pending_llm_package"] == "user_news_config"
    assert pending["llm_provider"] == "volcengine-ark"
    assert pending["backend_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert pending["quick_think_llm"] == "deepseek-v4-flash"
    assert pending["deep_think_llm"] == "deepseek-v4-flash"
    assert pending["api_key"] == "new-news-key"
    assert pending["_api_key_source"] == "user_news_config"
    assert config_service.should_probe_runtime_config(None, pending, updates) is True


def test_pending_news_runtime_does_not_borrow_normal_runtime_when_incomplete(monkeypatch):
    monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
    init_db()
    email = f"news-incomplete-{uuid4().hex[:8]}@example.com"
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
            backend_url="https://main.example/v1",
            quick_think_llm="main-quick",
            deep_think_llm="main-deep",
            api_key="main-key",
            news_llm_provider="volcengine-ark",
            news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
            news_analysis_llm="deepseek-v4-flash",
        )

        updates = UserRuntimeConfigUpdateRequest(news_analysis_llm="deepseek-v4-pro")
        pending = config_service.build_pending_runtime_config(updates, user.id, db)

    assert pending["_pending_llm_package"] == "user_news_config"
    assert pending["llm_provider"] == "volcengine-ark"
    assert pending["backend_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
    assert pending["quick_think_llm"] == "deepseek-v4-pro"
    assert pending["api_key"] == ""
    assert pending["_api_key_source"] is None
    assert config_service.should_probe_runtime_config(None, pending, updates) is False


def test_probe_runtime_config_rejects_upstream_auth_error(monkeypatch):
    class FakeLLM:
        def invoke(self, messages):
            raise RuntimeError("401 HMAC signature cannot be verified: maas-key")

    class FakeClient:
        def get_llm(self):
            return FakeLLM()

    monkeypatch.setattr(config_service, "create_llm_client", lambda *args, **kwargs: FakeClient())

    try:
        config_service.probe_runtime_config(
            {
                "llm_provider": "openai",
                "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
                "quick_think_llm": "astron-code-latest",
                "api_key": "maas-key",
            }
        )
    except Exception as exc:
        assert "模型 Key 验证失败" in str(exc)
        assert "maas-key" not in str(exc)
    else:
        raise AssertionError("probe should reject invalid upstream key")


def test_manual_warmup_returns_real_success_and_error(monkeypatch):
    class FakeLLM:
        def __init__(self, model: str):
            self.model = model

        def invoke(self, messages):
            if self.model == "bad-model":
                raise RuntimeError("upstream timeout")
            return type("Result", (), {"content": f"{self.model}:OK"})()

    class FakeClient:
        def __init__(self, model: str):
            self.model = model

        def get_llm(self):
            return FakeLLM(self.model)

    monkeypatch.setattr(config_service, "create_llm_client", lambda provider, model, base_url=None, **kwargs: FakeClient(model))

    results = config_service.invoke_runtime_warmup(
        {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "good-model",
            "deep_think_llm": "bad-model",
            "api_key": "maas-key",
        },
        "你好",
        "user-1",
        timeout=1.0,
    )

    assert results[0]["content"] == "good-model:OK"
    assert results[0]["error"] is None
    assert results[1]["content"] is None
    assert "upstream timeout" in results[1]["error"]
