import logging
import os
import time
from json import JSONDecodeError
from typing import Any, Optional
from urllib.parse import urlparse

from langchain_openai import ChatOpenAI

_logger = logging.getLogger(__name__)

from .base_client import BaseLLMClient
from .validators import validate_model

_OPENAI_COMPATIBLE_PROVIDER_BASE_URLS = {
    "openai": "https://api.openai.com/v1",
    "xai": "https://api.x.ai/v1",
    "openrouter": "https://openrouter.ai/api/v1",
    "ollama": "http://localhost:11434/v1",
    "volcengine": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "volcengine-ark": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "ark": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "dashscope": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "moonshot": "https://api.moonshot.cn/v1",
    "zhipu": "https://open.bigmodel.cn/api/paas/v4",
    "siliconflow": "https://api.siliconflow.cn/v1",
}

_PROVIDER_API_KEY_ENV = {
    "xai": "XAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "volcengine": "VOLCENGINE_API_KEY",
    "volcengine-ark": "VOLCENGINE_API_KEY",
    "ark": "ARK_API_KEY",
}


class UnifiedChatOpenAI(ChatOpenAI):
    """ChatOpenAI subclass that strips incompatible params for certain models."""

    def __init__(self, **kwargs):
        # 彻底移除重试参数，由构造函数统一控制
        kwargs.pop("response_parse_retries", None)
        kwargs.pop("response_parse_retry_delay", None)

        model = kwargs.get("model") or kwargs.get("model_name", "")
        base_url = kwargs.get("base_url")

        # LOG_LEVEL=DEBUG 时开启 LangChain verbose，打印完整的 LLM 请求和响应
        if os.environ.get("LOG_LEVEL", "").upper() == "DEBUG":
            kwargs["verbose"] = True

        # 1. Reasoning models (O1 etc) typically don't support temperature
        if self._is_reasoning_model(model):
            kwargs.pop("temperature", None)
            kwargs.pop("top_p", None)

        # 2. Moonshot (Kimi) models often strictly require temperature=1
        if self._is_moonshot_model(model, base_url):
            kwargs["temperature"] = 1

        super().__init__(**kwargs)

    def invoke(self, input: Any, config: Any = None, **kwargs: Any) -> Any:
        result = super().invoke(input=input, config=config, **kwargs)
        if _logger.isEnabledFor(logging.DEBUG):
            content = result.content if hasattr(result, "content") else str(result)
            _logger.debug(f"[LLM Response] model={self.model_name} length={len(content)}\n{content}")
        return result

    @staticmethod
    def _is_reasoning_model(model: str) -> bool:
        """Check if model is a reasoning model."""
        model_lower = str(model).lower()
        return (
            model_lower.startswith("o1")
            or model_lower.startswith("o3")
            or "gpt-5" in model_lower
            or "-r1" in model_lower
            or "thinking" in model_lower
            or "reasoning" in model_lower
        )

    @staticmethod
    def _is_moonshot_model(model: str, base_url: Optional[str] = None) -> bool:
        """Check if model or base_url is from Moonshot (Kimi)."""
        m = str(model).lower()
        b = (base_url or "").lower()
        return "moonshot" in m or "kimi" in m or "moonshot" in b or "kimi" in b


class OpenAIClient(BaseLLMClient):
    """Client for OpenAI, Ollama, OpenRouter, and xAI providers."""

    def __init__(
        self,
        model: str,
        base_url: Optional[str] = None,
        provider: str = "openai",
        **kwargs,
    ):
        super().__init__(model, base_url, **kwargs)
        self.provider = provider.lower()

    def get_llm(self) -> Any:
        """Return configured ChatOpenAI instance with long timeout and no retries."""
        llm_kwargs = {"model": self.model}

        if not UnifiedChatOpenAI._is_reasoning_model(self.model):
            llm_kwargs["temperature"] = self.kwargs.get("temperature", 0)

        # ── 极致稳定性配置 ──
        # 1. 禁用一切重试：避免 Thinking 模型重复扣费或因重连导致的状态丢失
        llm_kwargs["max_retries"] = 0
        
        # 2. 超长超时：默认 300 秒，给足推理模型思考时间
        llm_kwargs["timeout"] = self.kwargs.get("timeout", 300.0)
        
        target_url = self.base_url or _OPENAI_COMPATIBLE_PROVIDER_BASE_URLS.get(self.provider) or _OPENAI_COMPATIBLE_PROVIDER_BASE_URLS["openai"]
        
        print(f"[LLM Client] Init {self.provider} ({self.model}) at {target_url} (Retries=0, Timeout={llm_kwargs['timeout']}s)")

        if self.provider == "ollama":
            llm_kwargs["base_url"] = target_url
            llm_kwargs["api_key"] = "ollama"
        elif self.base_url or self.provider != "openai":
            env_key_name = _PROVIDER_API_KEY_ENV.get(self.provider)
            env_key = os.environ.get(env_key_name) if env_key_name else None
            llm_kwargs["base_url"] = target_url
            if env_key and "api_key" not in llm_kwargs:
                llm_kwargs["api_key"] = env_key

        if "base_url" not in llm_kwargs and self.base_url:
            llm_kwargs["base_url"] = self.base_url

        # Pass remaining keys
        for key in ("api_key", "callbacks", "reasoning_effort"):
            if key in self.kwargs:
                llm_kwargs[key] = self.kwargs[key]

        if (
            self.provider == "openai"
            and "api_key" not in llm_kwargs
            and _is_local_base_url(llm_kwargs.get("base_url"))
        ):
            llm_kwargs["api_key"] = "local"

        return UnifiedChatOpenAI(**llm_kwargs)

    def validate_model(self) -> bool:
        return validate_model(self.provider, self.model)


def _is_local_base_url(base_url: Optional[str]) -> bool:
    if not base_url:
        return False
    parsed = urlparse(base_url)
    hostname = (parsed.hostname or "").lower()
    return hostname in {"localhost", "127.0.0.1", "0.0.0.0"}
