"""API smoke tests using FastAPI TestClient (no external server needed).

Covers:
1. AnalyzeRequest schema — query field exists, symbol optional
2. /v1/analyze dry_run — legacy single-horizon path works
3. /v1/analyze with query field — schema accepts it, dry_run still short-circuits
4. /v1/chat/completions — unrecognizable stock returns 400
5. /v1/chat/completions — valid stock dry_run completes job
6. /v1/jobs/{id}/result — completed job returns result
"""
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.database import ImportedPortfolioPositionDB, get_db_ctx


# ---------------------------------------------------------------------------
# Schema-only test (no server needed)
# ---------------------------------------------------------------------------

class TestAnalyzeRequestSchema:
    def test_query_field_exists_and_optional(self):
        from api.main import AnalyzeRequest
        # query defaults to None
        req = AnalyzeRequest(symbol="600519.SH")
        assert req.query is None

    def test_query_field_accepts_string(self):
        from api.main import AnalyzeRequest
        req = AnalyzeRequest(symbol="600519.SH", query="分析贵州茅台短线机会")
        assert req.query == "分析贵州茅台短线机会"

    def test_symbol_is_optional(self):
        from api.main import AnalyzeRequest
        # should not raise
        req = AnalyzeRequest()
        assert req.symbol == ""

    def test_dry_run_defaults_false(self):
        from api.main import AnalyzeRequest
        req = AnalyzeRequest(symbol="600519.SH")
        assert req.dry_run is False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client():
    """Create a TestClient for the FastAPI app."""
    from api.main import app
    return TestClient(app, raise_server_exceptions=False)


def _auth(client: TestClient) -> str:
    """Register a test user and return a valid JWT token."""
    r = client.post("/v1/auth/request-code", json={"email": "apitest@test.com"})
    code = r.json()["dev_code"]
    r2 = client.post("/v1/auth/verify-code", json={"email": "apitest@test.com", "code": code})
    return r2.json()["access_token"]


def _get_user_id_from_token(token: str) -> str:
    from api.services import auth_service

    payload = auth_service.decode_access_token(token)
    return str(payload["sub"])


def _auth_unique(client: TestClient) -> str:
    from api.database import UserDB, get_db_ctx, init_db
    from api.services import auth_service

    init_db()
    email = auth_service.normalize_email(f"apitest-{uuid4().hex[:8]}@test.com")
    now = datetime.now(timezone.utc)
    with get_db_ctx() as db:
        user = auth_service.get_user_by_email(db, email)
        if not user:
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
    return auth_service.create_access_token(user)


def _wait_job(client: TestClient, token: str, job_id: str, timeout: float = 5.0) -> dict:
    """Poll until job is no longer running, return result dict."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/v1/jobs/{job_id}", headers={"Authorization": f"Bearer {token}"})
        status = r.json().get("status")
        if status in ("completed", "failed"):
            break
        time.sleep(0.2)
    r2 = client.get(f"/v1/jobs/{job_id}/result", headers={"Authorization": f"Bearer {token}"})
    return r2.json()


# ---------------------------------------------------------------------------
# API integration tests
# ---------------------------------------------------------------------------

class TestAnalyzeEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_dry_run_completes(self):
        """Legacy path: symbol + dry_run → completed immediately."""
        r = self.client.post("/v1/analyze", headers=self.headers, json={
            "symbol": "600519.SH",
            "trade_date": "2024-01-15",
            "dry_run": True,
        })
        assert r.status_code == 200
        job_id = r.json()["job_id"]
        result = _wait_job(self.client, self.token, job_id)
        assert result["status"] == "completed"
        assert result["decision"] == "DRY_RUN"
        assert result["result"]["symbol"] == "600519.SH"

    def test_query_field_accepted_with_dry_run(self):
        """query field is accepted by schema; dry_run still short-circuits before LLM."""
        r = self.client.post("/v1/analyze", headers=self.headers, json={
            "symbol": "600519.SH",
            "trade_date": "2024-01-15",
            "query": "分析贵州茅台短线机会，关注量价关系",
            "dry_run": True,
        })
        assert r.status_code == 200
        job_id = r.json()["job_id"]
        result = _wait_job(self.client, self.token, job_id)
        assert result["status"] == "completed"
        assert result["decision"] == "DRY_RUN"

    def test_missing_symbol_accepted_by_schema(self):
        """symbol is optional in schema; job is created (may fail later without LLM, but 200 on submit)."""
        r = self.client.post("/v1/analyze", headers=self.headers, json={
            "trade_date": "2024-01-15",
            "dry_run": True,
        })
        assert r.status_code == 200
        assert "job_id" in r.json()

    def test_requires_auth(self):
        """Unauthenticated request returns 401/403."""
        r = self.client.post("/v1/analyze", json={
            "symbol": "600519.SH", "dry_run": True,
        })
        assert r.status_code in (401, 403)

    def test_selected_analysts_field(self):
        """selected_analysts are echoed back in dry_run result."""
        r = self.client.post("/v1/analyze", headers=self.headers, json={
            "symbol": "600519.SH",
            "selected_analysts": ["market", "news"],
            "dry_run": True,
        })
        job_id = r.json()["job_id"]
        result = _wait_job(self.client, self.token, job_id)
        assert result["result"]["selected_analysts"] == ["market", "news"]

    def test_dry_run_merges_imported_position_context_for_manual_analysis(self):
        current_user = self.client.get("/v1/auth/me", headers=self.headers).json()
        now = datetime.now(timezone.utc)

        with get_db_ctx() as db:
            db.add(
                ImportedPortfolioPositionDB(
                    id=uuid4().hex,
                    user_id=current_user["id"],
                    source="manual",
                    symbol="600519.SH",
                    security_name="贵州茅台",
                    current_position=300.0,
                    average_cost=1680.5,
                    market_value=504150.0,
                    current_position_pct=42.5,
                    trade_points_json=[],
                    trade_points_count=0,
                    last_imported_at=now,
                )
            )
            db.commit()

        r = self.client.post("/v1/analyze", headers=self.headers, json={
            "symbol": "600519.SH",
            "trade_date": "2024-01-15",
            "dry_run": True,
        })
        assert r.status_code == 200
        job_id = r.json()["job_id"]
        result = _wait_job(self.client, self.token, job_id)

        user_context = result["result"]["user_context"]
        assert user_context["current_position"] == pytest.approx(300.0)
        assert user_context["average_cost"] == pytest.approx(1680.5)
        assert user_context["current_position_pct"] == pytest.approx(42.5)
        assert "持仓导入" in (user_context.get("user_notes") or "")


class TestSystemMarketDataStatusEndpoint:
    def test_market_data_status_endpoint_returns_shape(self):
        client = _get_client()
        response = client.get("/v1/system/market-data-status")
        assert response.status_code == 200
        payload = response.json()
        assert "daily" in payload
        assert "minute" in payload
        assert "tables" in payload
        assert "updated_at" in payload


class TestChatCompletionsEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_unrecognizable_stock_returns_error(self):
        """Non-stock text returns 400 with Chinese error message."""
        # Mock the LLM used for stock extraction to return no stock
        with patch("api.main._ai_extract_symbol_and_date", return_value=(None, None, ["short"], [], [], {})):
            r = self.client.post("/v1/chat/completions", headers=self.headers, json={
                "messages": [{"role": "user", "content": "今天天气真好"}],
                "stream": False,
                "dry_run": True,
            })
        assert r.status_code == 400

    def test_valid_stock_dry_run_creates_job(self):
        """Valid stock message with dry_run creates and completes a job."""
        with patch("api.main._ai_extract_symbol_and_date", return_value=("600519.SH", "2024-01-15", ["short"], [], [], {})):
            r = self.client.post("/v1/chat/completions", headers=self.headers, json={
                "messages": [{"role": "user", "content": "分析600519短线机会"}],
                "stream": False,
                "dry_run": True,
            })
        assert r.status_code == 200
        body = r.json()
        # Non-stream returns OpenAI-compatible format with job_id embedded in content
        assert "choices" in body
        content = body["choices"][0]["message"]["content"]
        # Extract job_id from content (format: "已启动分析任务：<job_id>")
        job_id = body["id"].replace("chatcmpl-", "")
        result = _wait_job(self.client, self.token, job_id)
        assert result["status"] == "completed"
        assert result["decision"] == "DRY_RUN"

    def test_chat_completion_rejects_mixed_account_runtime_before_llm_parse(self):
        mixed_runtime = {
            "llm_provider": "openai",
            "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "quick_think_llm": "env-quick",
            "deep_think_llm": "env-deep",
            "api_key": "volcengine-key-only",
            "_api_key_source": "user_config",
            "_llm_provider_source": "TA_LLM_PROVIDER",
            "_backend_url_source": "TA_BASE_URL",
            "_quick_think_llm_source": "TA_LLM_QUICK",
            "_deep_think_llm_source": "TA_LLM_DEEP",
        }
        with (
            patch("api.main._build_runtime_config", return_value=mixed_runtime),
            patch("api.main._ai_extract_symbol_and_date") as extract_symbol,
        ):
            r = self.client.post("/v1/chat/completions", headers=self.headers, json={
                "messages": [{"role": "user", "content": "分析600519短线机会"}],
                "stream": False,
                "dry_run": True,
            })

        assert r.status_code == 400
        assert "同源运行包" in r.json()["detail"]
        extract_symbol.assert_not_called()

    def test_chat_completion_uses_fallback_symbol_from_request(self):
        """When prompt omits stock text, current page symbol can be used as fallback."""
        with patch("api.main._ai_extract_symbol_and_date", return_value=(None, None, ["short"], [], [], {})):
            r = self.client.post("/v1/chat/completions", headers=self.headers, json={
                "messages": [{"role": "user", "content": "帮我看一下今天还能不能继续拿"}],
                "symbol": "600519.SH",
                "stream": False,
                "dry_run": True,
            })
        assert r.status_code == 200
        body = r.json()
        assert body["id"].startswith("chatcmpl-")
        job_id = body["id"].replace("chatcmpl-", "")
        result = _wait_job(self.client, self.token, job_id)
        assert result["status"] == "completed"
        assert result["result"]["symbol"] == "600519.SH"

    def test_non_stream_chat_completion_runs_background_job(self):
        """Non-streaming non-dry-run chat starts the real background analysis job."""
        async def fake_background_job(job_id, symbol, trade_date, query, user_id, selected_analysts):
            from api import main as compat

            result = {
                "status": "completed",
                "decision": "BUY",
                "job_id": job_id,
                "symbol": symbol,
                "trade_date": trade_date,
                "query": query,
                "selected_analysts": selected_analysts,
                "analysis_mode": "deep",
            }
            compat._set_job(
                job_id,
                user_id=user_id,
                status="completed",
                decision="BUY",
                symbol=symbol,
                trade_date=trade_date,
                result=result,
            )

        with (
            patch("api.main._ai_extract_symbol_and_date", return_value=("600519.SH", "2024-01-15", ["short"], [], [], {})),
            patch("api.routes.chat._run_background_analysis_job", side_effect=fake_background_job),
        ):
            r = self.client.post("/v1/chat/completions", headers=self.headers, json={
                "messages": [{"role": "user", "content": "分析600519短线机会"}],
                "stream": False,
                "dry_run": False,
                "selected_analysts": ["market", "news"],
            })
        assert r.status_code == 200
        body = r.json()
        job_id = body["id"].replace("chatcmpl-", "")
        result = _wait_job(self.client, self.token, job_id)
        assert result["status"] == "completed"
        assert result["decision"] == "BUY"
        assert result["result"]["analysis_mode"] == "deep"
        assert result["result"]["selected_analysts"] == ["market", "news"]

    def test_job_status_includes_progress_fields(self):
        from api import main as compat

        compat._set_job(
            "job-progress-1",
            user_id=_get_user_id_from_token(self.token),
            status="running",
            current_agent="Market Analyst",
            current_stage="streaming_report",
            analysis_stage="market_analysis",
            progress={
                "current_agent": "Market Analyst",
                "current_stage": "streaming_report",
                "analysis_stage": "market_analysis",
                "status": {"Market Analyst": "in_progress"},
                "debate": {"name": None, "agent": None, "round": None, "is_verdict": False},
                "completed_agents": [],
                "has_streamed_content": True,
            },
        )
        r = self.client.get("/v1/jobs/job-progress-1", headers=self.headers)
        assert r.status_code == 200
        body = r.json()
        assert body["current_agent"] == "Market Analyst"
        assert body["current_stage"] == "streaming_report"
        assert body["analysis_stage"] == "market_analysis"
        assert body["progress"]["status"]["Market Analyst"] == "in_progress"

    def test_streaming_chat_completion_emits_job_events(self):
        with patch("api.main._ai_extract_symbol_and_date", return_value=("600519.SH", "2024-01-15", ["short"], [], [], {})):
            with self.client.stream(
                "POST",
                "/v1/chat/completions",
                headers=self.headers,
                json={
                    "messages": [{"role": "user", "content": "分析 600519.SH 今天走势"}],
                    "stream": True,
                },
            ) as response:
                assert response.status_code == 200
                assert response.headers["content-type"].startswith("text/event-stream")
                body = "".join(response.iter_text())
        assert "event: job.created" in body
        assert "event: job.completed" in body
        assert "event: job.failed" not in body
        assert "event: done" in body

    def test_requires_auth(self):
        r = self.client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "分析600519"}],
            "stream": False,
        })
        assert r.status_code in (401, 403)


class TestOpenAPISchema:
    def test_analyze_request_has_query_field(self):
        client = _get_client()
        r = client.get("/openapi.json")
        assert r.status_code == 200
        schema = r.json()["components"]["schemas"]["AnalyzeRequest"]
        assert "query" in schema["properties"]

    def test_analyze_request_symbol_not_required(self):
        client = _get_client()
        r = client.get("/openapi.json")
        schema = r.json()["components"]["schemas"]["AnalyzeRequest"]
        assert "symbol" not in schema.get("required", [])

    def test_healthz(self):
        client = _get_client()
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestRuntimeConfigWarmup:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_get_config_exposes_core_stock_llm_readiness(self):
        readiness = {
            "enabled": True,
            "ready": False,
            "status": "missing_api_key",
            "reason": "远程 LLM 已配置但缺少 API Key",
            "provider": "openai",
            "model": "astron-code-latest",
            "base_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "api_key_source": None,
            "base_url_source": "TA_BASE_URL",
            "model_source": "TA_LLM_MODEL",
            "requires_api_key": True,
            "has_api_key": False,
        }
        with patch("api.services.news_theme_service.core_stock_llm_readiness", return_value=readiness) as llm_readiness:
            r = self.client.get("/v1/config", headers=self.headers)

        assert r.status_code == 200
        body = r.json()
        assert body["llm_core_stock"]["status"] == "missing_api_key"
        assert body["llm_core_stock"]["provider"] == "openai"
        assert body["llm_core_stock"]["base_url_source"] == "TA_BASE_URL"
        assert body["llm_core_stock"]["has_api_key"] is False
        assert "api_key" not in body["llm_core_stock"]
        llm_readiness.assert_called_once()
        assert llm_readiness.call_args.kwargs["user_id"] == _get_user_id_from_token(self.token)

    def test_model_change_schedules_warmup(self, monkeypatch):
        monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
        model_name = f"gpt-test-quick-{uuid4().hex[:8]}"
        with patch("api.main._probe_runtime_config", return_value={"status": "ok", "model": model_name}) as probe, \
             patch("api.main._run_config_warmup") as warmup, \
             patch("api.services.news_theme_service.core_stock_llm_readiness", return_value={"ready": False, "status": "missing_api_key"}), \
             patch("api.routes.config._run_event_driven_selection_refresh") as refresh:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "quick_think_llm": model_name,
            })
        assert r.status_code == 200
        body = r.json()
        assert body["warmup"]["status"] == "scheduled"
        assert body["warmup"]["triggered"] is True
        assert model_name in body["warmup"]["models"]
        warmup.assert_called_once()
        refresh.assert_not_called()

    def test_llm_config_change_schedules_event_driven_selection_refresh(self):
        model_name = f"astron-code-latest-{uuid4().hex[:8]}"
        readiness = {
            "enabled": True,
            "ready": True,
            "status": "ready",
            "reason": "远程 LLM 配置可用",
            "provider": "openai",
            "model": model_name,
            "base_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "requires_api_key": True,
            "has_api_key": True,
        }
        with patch("api.main._probe_runtime_config", return_value={"status": "ok", "model": model_name}), \
             patch("api.main._run_config_warmup"), \
             patch("api.services.news_theme_service.core_stock_llm_readiness", return_value=readiness), \
             patch("api.routes.config._run_event_driven_selection_refresh") as refresh:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "llm_provider": "openai",
                "backend_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
                "quick_think_llm": model_name,
                "api_key": "sk-test-valid",
            })

        assert r.status_code == 200
        body = r.json()
        assert body["event_driven_selection"]["requested"] is True
        assert body["event_driven_selection"]["triggered"] is True
        assert body["event_driven_selection"]["status"] == "scheduled"
        assert body["event_driven_selection"]["llm_core_stock"]["status"] == "ready"
        refresh.assert_called_once()

    def test_llm_config_change_skips_event_refresh_when_llm_not_ready(self):
        readiness = {
            "enabled": True,
            "ready": False,
            "status": "missing_api_key",
            "reason": "远程 LLM 已配置但缺少 API Key",
            "provider": "openai",
            "model": "astron-code-latest",
            "base_url": "https://maas-coding-api.cn-huabei-1.xf-yun.com/v2",
            "requires_api_key": True,
            "has_api_key": False,
        }
        with patch("api.main._run_config_warmup"), \
             patch("api.services.news_theme_service.core_stock_llm_readiness", return_value=readiness), \
             patch("api.routes.config._run_event_driven_selection_refresh") as refresh:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "quick_think_llm": "astron-code-latest",
                "warmup": False,
            })

        assert r.status_code == 200
        body = r.json()
        assert body["event_driven_selection"]["requested"] is True
        assert body["event_driven_selection"]["triggered"] is False
        assert body["event_driven_selection"]["reason"] == "missing_api_key"
        refresh.assert_not_called()

    def test_non_model_change_skips_warmup(self):
        with patch("api.main._run_config_warmup") as warmup:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "max_debate_rounds": 3,
            })
        assert r.status_code == 200
        body = r.json()
        assert body["warmup"]["status"] == "skipped"
        assert body["warmup"]["triggered"] is False
        assert body["event_driven_selection"]["requested"] is False
        warmup.assert_not_called()

    def test_api_key_is_probed_before_save(self):
        readiness = {
            "enabled": True,
            "ready": True,
            "status": "ready",
            "reason": "远程 LLM 配置可用",
            "provider": "openai",
            "model": "moonshot-v1-8k",
            "base_url": "https://api.moonshot.cn/v1",
            "requires_api_key": True,
            "has_api_key": True,
        }
        with patch("api.main._probe_runtime_config", return_value={"status": "ok", "model": "moonshot-v1-8k"}) as probe, \
             patch("api.main._run_config_warmup") as warmup, \
             patch("api.services.news_theme_service.core_stock_llm_readiness", return_value=readiness), \
             patch("api.routes.config._run_event_driven_selection_refresh") as refresh:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "llm_provider": "openai",
                "backend_url": "https://api.moonshot.cn/v1",
                "quick_think_llm": "moonshot-v1-8k",
                "api_key": "sk-test-valid",
            })
        assert r.status_code == 200
        probe.assert_called_once()
        warmup.assert_called_once()
        refresh.assert_called_once()

    def test_config_update_persists_complete_news_llm_runtime_package(self):
        user_id = _get_user_id_from_token(self.token)
        with patch("api.main._probe_runtime_config", return_value={"status": "ok", "model": "deepseek-v4-flash"}), \
             patch("api.main._run_config_warmup"), \
             patch("api.routes.config._run_event_driven_selection_refresh"):
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "llm_provider": "volcengine-ark",
                "backend_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
                "quick_think_llm": "deepseek-v4-flash",
                "deep_think_llm": "deepseek-v4-pro",
                "api_key": "volcengine-main-key",
                "news_llm_provider": "volcengine-ark",
                "news_backend_url": "https://ark.cn-beijing.volces.com/api/coding/v3",
                "news_analysis_llm": "deepseek-v4-flash",
                "news_api_key": "volcengine-news-key",
            })

        assert r.status_code == 200
        body = r.json()
        assert body["current"]["news_llm_provider"] == "volcengine-ark"
        assert body["current"]["news_backend_url"] == "https://ark.cn-beijing.volces.com/api/coding/v3"
        assert body["current"]["news_analysis_llm"] == "deepseek-v4-flash"
        assert body["current"]["has_news_api_key"] is True
        assert body["current"]["llm_core_stock"]["ready"] is True
        assert body["current"]["llm_core_stock"]["runtime_package_source"] == "user_news_config"
        assert body["current"]["llm_core_stock"]["provider_source"] == "user_news_config"
        assert body["current"]["llm_core_stock"]["base_url_source"] == "user_news_config"
        assert body["current"]["llm_core_stock"]["model_source"] == "user_news_config"
        assert body["current"]["llm_core_stock"]["api_key_source"] == "user_news_config"

        from api.services import auth_service

        with get_db_ctx() as db:
            row = auth_service.get_user_llm_config(db, user_id)
            assert row is not None
            assert row.news_llm_provider == "volcengine-ark"
            assert row.news_backend_url == "https://ark.cn-beijing.volces.com/api/coding/v3"
            assert row.news_analysis_llm == "deepseek-v4-flash"
            assert auth_service.decrypt_secret(row.news_api_key_encrypted) == "volcengine-news-key"

    def test_clear_api_key_clears_news_llm_key_together(self):
        user_id = _get_user_id_from_token(self.token)
        from api.services import auth_service

        with get_db_ctx() as db:
            auth_service.upsert_user_llm_config(
                db,
                user_id,
                llm_provider="volcengine-ark",
                backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
                quick_think_llm="deepseek-v4-flash",
                api_key="volcengine-main-key",
                news_llm_provider="volcengine-ark",
                news_backend_url="https://ark.cn-beijing.volces.com/api/coding/v3",
                news_analysis_llm="deepseek-v4-flash",
                news_api_key="volcengine-news-key",
            )

        r = self.client.patch("/v1/config", headers=self.headers, json={
            "clear_api_key": True,
            "clear_news_api_key": True,
            "warmup": False,
        })

        assert r.status_code == 200
        body = r.json()
        assert body["current"]["has_api_key"] is False
        assert body["current"]["has_news_api_key"] is False
        with get_db_ctx() as db:
            row = auth_service.get_user_llm_config(db, user_id)
            assert row is not None
            assert row.api_key_encrypted is None
            assert row.news_api_key_encrypted is None

    def test_invalid_api_key_is_rejected_before_save(self):
        with patch("api.main._probe_runtime_config", side_effect=HTTPException(status_code=400, detail="模型 Key 验证失败")) as probe, \
             patch("api.main._run_config_warmup") as warmup:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "llm_provider": "openai",
                "backend_url": "https://api.moonshot.cn/v1",
                "quick_think_llm": "moonshot-v1-8k",
                "api_key": "sk-test-invalid",
            })
        assert r.status_code == 400
        assert "模型 Key 验证失败" in r.json()["detail"]
        probe.assert_called_once()
        warmup.assert_not_called()

    def test_force_warmup_schedules_even_without_model_change(self):
        with patch("api.main._run_config_warmup") as warmup:
            r = self.client.patch("/v1/config", headers=self.headers, json={
                "max_debate_rounds": 3,
                "force_warmup": True,
            })
        assert r.status_code == 200
        body = r.json()
        assert body["warmup"]["status"] == "scheduled"
        assert body["warmup"]["triggered"] is True
        warmup.assert_called_once()

    def test_manual_warmup_returns_model_reply(self, monkeypatch):
        monkeypatch.setenv("TA_FORCE_LLM_RUNTIME", "0")
        with patch("api.main._invoke_runtime_warmup", return_value=[{
            "model": "gpt-test-quick",
            "targets": ["常规模型"],
            "content": "你好，我已准备就绪。",
            "error": None,
        }]) as invoke:
            r = self.client.post("/v1/config/warmup", headers=self.headers, json={
                "quick_think_llm": "gpt-test-quick",
                "prompt": "你好",
            })

        assert r.status_code == 200
        body = r.json()
        assert body["prompt"] == "你好"
        assert body["results"][0]["content"] == "你好，我已准备就绪。"
        invoke.assert_called_once()
        assert invoke.call_args.args[0]["quick_think_llm"] == "gpt-test-quick"
        assert invoke.call_args.args[1] == "你好"

    def test_manual_warmup_surfaces_upstream_error(self):
        with patch(
            "api.main._invoke_runtime_warmup",
            side_effect=HTTPException(status_code=400, detail="模型 warmup 失败：upstream timeout"),
        ):
            r = self.client.post("/v1/config/warmup", headers=self.headers, json={
                "quick_think_llm": "gpt-test-quick",
                "prompt": "你好",
            })

        assert r.status_code == 400
        assert "模型 warmup 失败" in r.json()["detail"]


class TestWecomRuntimeConfig:
    WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e1d21302-1925-4247-ad5a-6bc023c7fd2a"

    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth_unique(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_config_returns_masked_webhook_and_toggle_state(self):
        r = self.client.patch("/v1/config", headers=self.headers, json={
            "wecom_webhook_url": self.WEBHOOK_URL,
            "wecom_report_enabled": False,
            "warmup": False,
        })

        assert r.status_code == 200
        body = r.json()
        current = body["current"]
        assert current["has_wecom_webhook"] is True
        assert current["wecom_report_enabled"] is False
        assert current["wecom_webhook_display"].startswith("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=")
        assert current["wecom_webhook_display"] != self.WEBHOOK_URL
        assert "fd2a" in current["wecom_webhook_display"]
        assert body["applied"]["wecom_report_enabled"] is False

        config_resp = self.client.get("/v1/config", headers=self.headers)
        assert config_resp.status_code == 200
        assert config_resp.json()["wecom_report_enabled"] is False

    def test_wecom_warmup_uses_stored_webhook_when_input_missing(self):
        save_resp = self.client.patch("/v1/config", headers=self.headers, json={
            "wecom_webhook_url": self.WEBHOOK_URL,
            "warmup": False,
        })
        assert save_resp.status_code == 200

        with patch("api.services.wecom_notification_service.send_message", return_value=True) as mock_send:
            r = self.client.post("/v1/config/wecom/warmup", headers=self.headers, json={})

        assert r.status_code == 200
        body = r.json()
        assert body["sent"] is True
        assert "成功" in body["message"]
        assert mock_send.call_count == 1
        assert "量化之神 Webhook 预热" in mock_send.call_args.args[0]
        assert mock_send.call_args.args[1] == self.WEBHOOK_URL

    def test_inline_wecom_warmup_does_not_persist_unsaved_webhook(self):
        with patch("api.services.wecom_notification_service.send_message", return_value=True) as mock_send:
            r = self.client.post("/v1/config/wecom/warmup", headers=self.headers, json={
                "wecom_webhook_url": "inline-key-1234",
            })

        assert r.status_code == 200
        assert mock_send.call_count == 1
        assert mock_send.call_args.args[1] == (
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=inline-key-1234"
        )

        config_resp = self.client.get("/v1/config", headers=self.headers)
        assert config_resp.status_code == 200
        assert config_resp.json()["has_wecom_webhook"] is False

    def test_invalid_wecom_url_is_rejected(self):
        r = self.client.patch("/v1/config", headers=self.headers, json={
            "wecom_webhook_url": "http://169.254.169.254/latest/meta-data/",
            "warmup": False,
        })

        assert r.status_code == 400
        assert "企业微信 Webhook" in r.json()["detail"]


class TestWatchlistAddEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth_unique(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_batch_add_supports_codes_and_full_names(self):
        name_to_code = {
            "贵州茅台": "600519.SH",
            "宁德时代": "300750.SZ",
        }
        code_to_name = {value: key for key, value in name_to_code.items()}
        with patch("api.main._load_cn_stock_map", return_value=name_to_code), \
             patch("api.main._get_reverse_stock_map", return_value=code_to_name):
            r = self.client.post("/v1/watchlist", headers=self.headers, json={
                "text": "600519 宁德时代, 未知标的",
            })
        assert r.status_code == 200
        body = r.json()
        assert body["summary"] == {"total": 3, "added": 2, "duplicate": 0, "failed": 1}
        assert [item["status"] for item in body["results"]] == ["added", "added", "invalid"]
        assert body["results"][0]["symbol"] == "600519.SH"
        assert body["results"][1]["symbol"] == "300750.SZ"

    def test_batch_add_marks_duplicates(self):
        name_to_code = {
            "贵州茅台": "600519.SH",
        }
        code_to_name = {value: key for key, value in name_to_code.items()}
        with patch("api.main._load_cn_stock_map", return_value=name_to_code), \
             patch("api.main._get_reverse_stock_map", return_value=code_to_name):
            r = self.client.post("/v1/watchlist", headers=self.headers, json={
                "text": "600519.SH 贵州茅台",
            })
        assert r.status_code == 200
        body = r.json()
        assert body["summary"] == {"total": 2, "added": 1, "duplicate": 1, "failed": 0}
        assert [item["status"] for item in body["results"]] == ["added", "duplicate"]


class TestReportsEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth_unique(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def _create_report(self, symbol: str, trade_date: str, decision: str):
        response = self.client.post("/v1/reports", headers=self.headers, json={
            "symbol": symbol,
            "trade_date": trade_date,
            "decision": decision,
        })
        assert response.status_code == 200
        return response.json()

    def test_latest_by_symbols_returns_only_each_symbol_latest_report(self):
        self._create_report("600519.SH", "2026-03-28", "HOLD")
        self._create_report("600519.SH", "2026-03-30", "BUY")
        self._create_report("300750.SZ", "2026-03-29", "SELL")

        response = self.client.post(
            "/v1/reports/latest-by-symbols",
            headers=self.headers,
            json={"symbols": ["300750.SZ", "600519.SH", "000001.SZ"]},
        )

        assert response.status_code == 200
        body = response.json()
        assert [item["symbol"] for item in body["reports"]] == ["300750.SZ", "600519.SH"]
        assert body["reports"][0]["decision"] == "SELL"
        assert body["reports"][1]["decision"] == "BUY"

    def test_batch_delete_endpoint_removes_multiple_reports(self):
        first = self._create_report("600519.SH", "2026-03-28", "HOLD")
        second = self._create_report("300750.SZ", "2026-03-29", "SELL")
        third = self._create_report("000001.SZ", "2026-03-30", "BUY")

        response = self.client.post(
            "/v1/reports/batch/delete",
            headers=self.headers,
            json={"report_ids": [first["id"], second["id"], "missing-report-id"]},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["deleted_ids"] == [first["id"], second["id"]]
        assert body["missing_ids"] == ["missing-report-id"]

        remaining = self.client.get("/v1/reports", headers=self.headers)
        assert remaining.status_code == 200
        remaining_ids = [item["id"] for item in remaining.json()["reports"]]
        assert remaining_ids == [third["id"]]


class TestPortfolioOverviewEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth_unique(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.name_to_code = {
            "贵州茅台": "600519.SH",
            "宁德时代": "300750.SZ",
        }
        self.code_to_name = {value: key for key, value in self.name_to_code.items()}

    def _add_watchlist(self, text: str):
        with patch("api.main._load_cn_stock_map", return_value=self.name_to_code), \
             patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.post("/v1/watchlist", headers=self.headers, json={"text": text})
        assert response.status_code == 200

    def _create_scheduled(self, symbol: str):
        with patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.post(
                "/v1/scheduled",
                headers=self.headers,
                json={"symbol": symbol, "horizon": "short", "trigger_time": "20:00"},
            )
        assert response.status_code == 201

    def test_overview_returns_watchlist_scheduled_portfolio_and_latest_reports(self):
        from api.database import ImportedPortfolioPositionDB, get_db_ctx

        self._add_watchlist("600519.SH 300750.SZ")
        self._create_scheduled("600519.SH")

        self.client.post("/v1/reports", headers=self.headers, json={
            "symbol": "600519.SH",
            "trade_date": "2026-03-30",
            "decision": "BUY",
        })
        self.client.post("/v1/reports", headers=self.headers, json={
            "symbol": "300750.SZ",
            "trade_date": "2026-03-29",
            "decision": "SELL",
        })

        current_user = self.client.get("/v1/auth/me", headers=self.headers).json()
        with get_db_ctx() as db:
            db.add(
                ImportedPortfolioPositionDB(
                    id=uuid4().hex,
                    user_id=current_user["id"],
                    source="manual",
                    symbol="600519.SH",
                    security_name="贵州茅台",
                    current_position=300.0,
                    average_cost=1680.5,
                    market_value=504150.0,
                )
            )
            db.commit()

        with patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.get("/v1/portfolio/overview", headers=self.headers)

        assert response.status_code == 200
        body = response.json()
        assert [item["symbol"] for item in body["watchlist"]] == ["600519.SH", "300750.SZ"]
        assert body["watchlist"][0]["name"] == "贵州茅台"
        assert len(body["scheduled"]) == 1
        assert body["scheduled"][0]["symbol"] == "600519.SH"
        assert body["scheduled"][0]["has_imported_context"] is True
        assert [item["symbol"] for item in body["latest_reports"]] == ["600519.SH", "300750.SZ"]
        assert body["portfolio_import"]["summary"]["positions"] == 1


class TestScheduledBatchEndpoints:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.client = _get_client()
        self.token = _auth_unique(self.client)
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.code_to_name = {
            "300750.SZ": "宁德时代",
            "600519.SH": "贵州茅台",
        }

    def _create_scheduled(self, symbol: str):
        with patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.post(
                "/v1/scheduled",
                headers=self.headers,
                json={"symbol": symbol, "horizon": "short", "trigger_time": "20:00"},
            )
        assert response.status_code == 201
        return response.json()

    def test_batch_update_endpoint_updates_multiple_items(self):
        first = self._create_scheduled("300750.SZ")
        second = self._create_scheduled("600519.SH")

        with patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.patch(
                "/v1/scheduled/batch",
                headers=self.headers,
                json={
                    "item_ids": [first["id"], second["id"]],
                    "horizon": "medium",
                    "trigger_time": "21:30",
                    "is_active": True,
                },
            )

        assert response.status_code == 200
        body = response.json()
        assert [item["horizon"] for item in body["items"]] == ["medium", "medium"]
        assert [item["trigger_time"] for item in body["items"]] == ["21:30", "21:30"]
        assert [item["name"] for item in body["items"]] == ["宁德时代", "贵州茅台"]

    def test_batch_delete_endpoint_removes_multiple_items(self):
        first = self._create_scheduled("300750.SZ")
        second = self._create_scheduled("600519.SH")

        response = self.client.post(
            "/v1/scheduled/batch/delete",
            headers=self.headers,
            json={"item_ids": [first["id"], second["id"]]},
        )

        assert response.status_code == 200
        body = response.json()
        assert body["deleted_ids"] == [first["id"], second["id"]]
        assert body["missing_ids"] == []

        remaining = self.client.get("/v1/scheduled", headers=self.headers)
        assert remaining.status_code == 200
        assert remaining.json()["items"] == []

    def test_manual_trigger_endpoint_queues_single_scheduled_task(self):
        item = self._create_scheduled("300750.SZ")
        run_once = AsyncMock()

        def _close_coro(coro):
            coro.close()
            return MagicMock()

        with patch("api.main._run_scheduled_analysis_once", run_once), \
             patch("api.main._create_tracked_task", side_effect=_close_coro), \
             patch("api.main.cn_today_str", return_value="2026-03-31"), \
             patch("api.main._resolve_scheduled_trade_date", return_value="2026-03-31"):
            response = self.client.post(
                f"/v1/scheduled/{item['id']}/trigger",
                headers=self.headers,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "pending"
        assert run_once.call_count == 1

        reports = self.client.get("/v1/reports", headers=self.headers)
        report_rows = [row for row in reports.json()["reports"] if row["id"] == body["job_id"]]
        assert len(report_rows) == 1
        assert report_rows[0]["status"] == "pending"
        assert report_rows[0]["symbol"] == "300750.SZ"
        assert report_rows[0]["trade_date"] == "2026-03-31"

        args, kwargs = run_once.call_args
        assert args[0]["id"] == item["id"]
        assert args[0]["symbol"] == "300750.SZ"
        assert args[0]["user_id"]
        assert args[1] == "2026-03-31"
        assert args[2] == body["job_id"]
        assert kwargs == {"mark_schedule_run": False}

    def test_batch_trigger_endpoint_queues_selected_tasks_with_position_context(self):
        from api.database import ImportedPortfolioPositionDB, get_db_ctx

        first = self._create_scheduled("300750.SZ")
        second = self._create_scheduled("600519.SH")
        current_user = self.client.get("/v1/auth/me", headers=self.headers).json()

        with get_db_ctx() as db:
            db.add(
                ImportedPortfolioPositionDB(
                    id=uuid4().hex,
                    user_id=current_user["id"],
                    source="manual",
                    symbol="600519.SH",
                    security_name="贵州茅台",
                    current_position=300.0,
                    average_cost=1680.5,
                    market_value=504150.0,
                )
            )
            db.commit()

        run_once = AsyncMock()

        def _close_coro(coro):
            coro.close()
            return MagicMock()

        with patch("api.main._run_scheduled_analysis_once", run_once), \
             patch("api.main._create_tracked_task", side_effect=_close_coro), \
             patch("api.main.cn_today_str", return_value="2026-03-31"), \
             patch("api.main._resolve_scheduled_trade_date", return_value="2026-03-31"), \
             patch("api.main._get_reverse_stock_map", return_value=self.code_to_name):
            response = self.client.post(
                "/v1/scheduled/batch/trigger",
                headers=self.headers,
                json={"item_ids": [first["id"], second["id"]]},
            )

        assert response.status_code == 200
        body = response.json()
        assert body["summary"] == {
            "total": 2,
            "with_position_context": 1,
        }
        assert [job["symbol"] for job in body["jobs"]] == ["300750.SZ", "600519.SH"]
        assert body["jobs"][0]["current_position"] is None
        assert body["jobs"][0]["average_cost"] is None
        assert body["jobs"][1]["current_position"] == pytest.approx(300.0)
        assert body["jobs"][1]["average_cost"] == pytest.approx(1680.5)
        assert run_once.call_count == 2

        reports = self.client.get("/v1/reports", headers=self.headers)
        report_rows_by_id = {row["id"]: row for row in reports.json()["reports"]}
        for job in body["jobs"]:
            report_row = report_rows_by_id[job["job_id"]]
            assert report_row["status"] == "pending"
            assert report_row["symbol"] == job["symbol"]
            assert report_row["trade_date"] == "2026-03-31"

        first_args, first_kwargs = run_once.call_args_list[0]
        second_args, second_kwargs = run_once.call_args_list[1]
        assert first_args[0]["id"] == first["id"]
        assert first_args[0]["symbol"] == "300750.SZ"
        assert second_args[0]["id"] == second["id"]
        assert second_args[0]["symbol"] == "600519.SH"
        assert first_args[1] == "2026-03-31"
        assert second_args[1] == "2026-03-31"
        assert first_kwargs == {"mark_schedule_run": False}
        assert second_kwargs == {"mark_schedule_run": False}
