from typing import Optional

from .base_client import BaseLLMClient
from .openai_client import OpenAIClient
from .anthropic_client import AnthropicClient
from .google_client import GoogleClient

OPENAI_COMPATIBLE_PROVIDERS = {
    "openai",
    "ollama",
    "openrouter",
    "volcengine",
    "volcengine-ark",
    "ark",
    "dashscope",
    "deepseek",
    "moonshot",
    "zhipu",
    "siliconflow",
}

OPENAI_COMPATIBLE_DEFAULT_BASE_URLS = {
    "volcengine": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "volcengine-ark": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "ark": "https://ark.cn-beijing.volces.com/api/coding/v3",
    "dashscope": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "moonshot": "https://api.moonshot.cn/v1",
    "zhipu": "https://open.bigmodel.cn/api/paas/v4",
    "siliconflow": "https://api.siliconflow.cn/v1",
}


def create_llm_client(
    provider: str,
    model: str,
    base_url: Optional[str] = None,
    **kwargs,
) -> BaseLLMClient:
    """Create an LLM client for the specified provider.

    Args:
        provider: LLM provider (openai, anthropic, google, xai, ollama, openrouter)
        model: Model name/identifier
        base_url: Optional base URL for API endpoint
        **kwargs: Additional provider-specific arguments

    Returns:
        Configured BaseLLMClient instance

    Raises:
        ValueError: If provider is not supported
    """
    provider_lower = provider.lower()

    if provider_lower in OPENAI_COMPATIBLE_PROVIDERS:
        resolved_base_url = base_url or OPENAI_COMPATIBLE_DEFAULT_BASE_URLS.get(provider_lower)
        return OpenAIClient(model, resolved_base_url, provider=provider_lower, **kwargs)

    if provider_lower == "xai":
        return OpenAIClient(model, base_url, provider="xai", **kwargs)

    if provider_lower == "anthropic":
        return AnthropicClient(model, base_url, **kwargs)

    if provider_lower == "google":
        return GoogleClient(model, base_url, **kwargs)

    raise ValueError(f"Unsupported LLM provider: {provider}")
