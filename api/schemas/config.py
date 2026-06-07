from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class QmtAccountConfigPayload(BaseModel):
    key: str
    role: str
    enabled: bool = False
    host: str = ""
    port: int = 58610
    account_id: str = ""
    account_type: str = "STOCK"
    account_name: str = ""
    userdata_path: str = ""
    bridge_base_url: str = ""


class UserRuntimeConfigResponse(BaseModel):
    llm_provider: str
    deep_think_llm: str
    quick_think_llm: str
    backend_url: str
    news_llm_provider: Optional[str] = None
    news_backend_url: Optional[str] = None
    news_analysis_llm: Optional[str] = None
    max_debate_rounds: int
    max_risk_discuss_rounds: int
    has_api_key: bool = False
    has_news_api_key: bool = False
    has_wecom_webhook: bool = False
    wecom_webhook_display: Optional[str] = None
    server_fallback_enabled: bool = True
    email_report_enabled: bool = True
    wecom_report_enabled: bool = True
    default_analysts: List[str] = Field(default_factory=list)
    llm_core_stock: Dict[str, Any] = Field(default_factory=dict)
    qmt_paper_account: QmtAccountConfigPayload
    qmt_live_account: QmtAccountConfigPayload


class UserRuntimeConfigUpdateRequest(BaseModel):
    llm_provider: Optional[str] = None
    deep_think_llm: Optional[str] = None
    quick_think_llm: Optional[str] = None
    backend_url: Optional[str] = None
    news_llm_provider: Optional[str] = None
    news_backend_url: Optional[str] = None
    news_analysis_llm: Optional[str] = None
    max_debate_rounds: Optional[int] = None
    max_risk_discuss_rounds: Optional[int] = None
    email_report_enabled: Optional[bool] = None
    wecom_report_enabled: Optional[bool] = None
    api_key: Optional[str] = None
    news_api_key: Optional[str] = None
    wecom_webhook_url: Optional[str] = None
    clear_api_key: bool = False
    clear_news_api_key: bool = False
    clear_wecom_webhook: bool = False
    warmup: bool = True
    force_warmup: bool = False
    default_analysts: Optional[List[str]] = None
    qmt_paper_account: Optional[QmtAccountConfigPayload] = None
    qmt_live_account: Optional[QmtAccountConfigPayload] = None


class UserRuntimeWarmupRequest(UserRuntimeConfigUpdateRequest):
    prompt: str = "你好"


class RuntimeWarmupResult(BaseModel):
    model: str
    targets: List[str] = Field(default_factory=list)
    content: Optional[str] = None
    error: Optional[str] = None


class UserRuntimeWarmupResponse(BaseModel):
    prompt: str
    results: List[RuntimeWarmupResult]


class WecomWebhookWarmupRequest(BaseModel):
    wecom_webhook_url: Optional[str] = None
    content: Optional[str] = None


class WecomWebhookWarmupResponse(BaseModel):
    sent: bool = True
    message: str
    webhook_display: Optional[str] = None
