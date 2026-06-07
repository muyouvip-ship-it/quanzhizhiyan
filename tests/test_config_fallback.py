import unittest
from unittest.mock import MagicMock, patch
import os

# 模拟环境变量，因为 api.main 会在导入时读取它们
os.environ["QUICK_THINK_LLM"] = "env-default-quick"
os.environ["DEEP_THINK_LLM"] = "env-default-deep"

from api.main import _build_runtime_config
from tradingagents.llm_clients.factory import create_llm_client
from tradingagents.llm_clients.openai_client import OpenAIClient

_LLM_ENV_KEYS = [
    "TA_FORCE_LLM_RUNTIME",
    "TA_ENFORCE_LLM_RUNTIME",
    "TA_FORCE_LLM_ENDPOINT",
    "TA_LLM_PROVIDER",
    "TA_BASE_URL",
    "TA_LLM_QUICK",
    "TA_LLM_DEEP",
    "TA_LLM_MODEL",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
    "OPENAI_API_BASE",
]


class TestConfigFallback(unittest.TestCase):
    def test_priority_and_empty_filter(self):
        """验证: 用户配置(非空) > 环境变量，且空配置不覆盖环境变量"""
        # 强制 Mock DEFAULT_CONFIG 保证测试环境纯净
        clean_env = {key: "" for key in _LLM_ENV_KEYS}
        clean_env["TA_FORCE_LLM_RUNTIME"] = "0"
        with patch.dict(os.environ, clean_env, clear=False), patch('tradingagents.default_config.DEFAULT_CONFIG', {"quick_think_llm": "env-default-quick", "deep_think_llm": "env-default-deep"}):
            # 场景: 数据库里 quick 被填成了空字符串，deep 填了新值
            overrides = {
                "quick_think_llm": "",
                "deep_think_llm": "user-custom-deep"
            }
            config = _build_runtime_config(overrides)
            
            # 结果: quick 应该保留环境变量的默认值，而不是变成空
            self.assertEqual(config["quick_think_llm"], "env-default-quick", "空字符串不应覆盖环境变量默认值")
            self.assertEqual(config["deep_think_llm"], "user-custom-deep", "有效的用户配置应生效")

    def test_intelligent_cross_borrowing(self):
        """验证: 如果环境变量也没设(None)，则进行互相借用"""
        # 我们需要临时清除 config 里的默认值来模拟这种极端情况
        clean_env = {key: "" for key in _LLM_ENV_KEYS}
        clean_env["TA_FORCE_LLM_RUNTIME"] = "0"
        with patch.dict(os.environ, clean_env, clear=False), patch('tradingagents.default_config.DEFAULT_CONFIG', {"quick_think_llm": None, "deep_think_llm": None}):
            overrides = {
                "quick_think_llm": "only-one-model",
                "deep_think_llm": ""
            }
            config = _build_runtime_config(overrides)
            self.assertEqual(config["deep_think_llm"], "only-one-model", "Deep 应该借用唯一的有效配置")

    def test_no_hardcoded_fallback_in_client(self):
        """验证: OpenAIClient 不再有硬编码的 gpt-4o-mini 降级"""
        client = OpenAIClient(model="actual-model", provider="openai")
        self.assertEqual(client.model, "actual-model")
        
        # 如果真的传入空，它就应该是空（或者触发基类的初始化，但不应该自造 gpt-4o-mini）
        client_empty = OpenAIClient(model="", provider="openai")
        self.assertEqual(client_empty.model, "", "构造函数不应自造模型名")

    def test_volcengine_provider_alias_uses_ark_base_url(self):
        """验证: 火山 Ark provider 别名按 OpenAI-compatible 端点接入"""
        client = create_llm_client("volcengine-ark", "deepseek-v4-flash")
        self.assertIsInstance(client, OpenAIClient)
        self.assertEqual(client.provider, "volcengine-ark")
        self.assertEqual(client.base_url, "https://ark.cn-beijing.volces.com/api/coding/v3")

    def test_trading_graph_passes_account_key_to_volcengine_provider(self):
        """验证: 深度分析图使用账号配置里的火山 Ark Key，而不是只依赖环境变量。"""
        from tradingagents.graph.trading_graph import TradingAgentsGraph

        graph = TradingAgentsGraph.__new__(TradingAgentsGraph)
        graph.config = {
            "llm_provider": "volcengine-ark",
            "api_key": "volcengine-account-key",
        }

        kwargs = graph._get_provider_kwargs()

        self.assertEqual(kwargs["api_key"], "volcengine-account-key")

if __name__ == "__main__":
    unittest.main()
