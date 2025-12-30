import sys
import types
import os
import json
from types import SimpleNamespace

import pytest
from src.core.llms.deepseek import client as ds_client

fake_openai = types.ModuleType("openai")


class DummyOpenAI:
    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key
        self.base_url = base_url
        # provide a minimal chat.completions.create shape if accidentally used
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **kwargs: SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="DUMMY"))]
                )
            )
        )


fake_openai.OpenAI = DummyOpenAI
sys.modules.setdefault("openai", fake_openai)


@pytest.fixture(autouse=True)
def reset_module_state(monkeypatch):
    """Before each test: reset _client and restore environment variables."""
    orig_env = dict(os.environ)
    try:
        ds_client._client = None
        yield
    finally:
        os.environ.clear()
        os.environ.update(orig_env)
        ds_client._client = None


def test_get_client_raises_if_no_api_key(monkeypatch):
    # Ensure the variable is not set
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    ds_client._client = None
    with pytest.raises(ValueError):
        ds_client.get_client()


def test_get_client_creates_and_caches_openai_instance(monkeypatch):
    created = {}

    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None):
            created["api_key"] = api_key
            created["base_url"] = base_url

    # Patch the bound OpenAI in the module to our Fake class
    monkeypatch.setattr(ds_client, "OpenAI", FakeOpenAI)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    ds_client._client = None
    c1 = ds_client.get_client()

    assert created["api_key"] == "test-key"
    assert created["base_url"] == "https://api.deepseek.com"
    assert isinstance(c1, FakeOpenAI)

    # Cached instance is returned
    c2 = ds_client.get_client()
    assert c1 is c2


def test_analyze_text_uses_client_and_returns_message_content(monkeypatch):
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="OK_RESULT"))]
    )

    class FakeCompletions:
        def create(self, **kwargs):
            assert kwargs["model"] == "deepseek-chat"
            assert "messages" in kwargs
            return fake_response

    class FakeChat:
        def __init__(self):
            self.completions = FakeCompletions()

    class FakeClient:
        def __init__(self):
            self.chat = FakeChat()

    monkeypatch.setattr(ds_client, "get_client", lambda: FakeClient())

    result = ds_client.analyze_text("sys prompt", "some text", model="deepseek-chat")
    assert result == "OK_RESULT"


def test_analyze_antique_shops_batch_empty_returns_empty():
    res = ds_client.analyze_antique_shops_batch([], model="deepseek-chat")
    assert res == []


def test_analyze_antique_shops_batch_truncates_and_parses(monkeypatch):
    long_content = "A" * 5000
    pages = [
        {"url": "http://a.example", "content": long_content},
        {"url": "http://b.example", "content": "short"},
    ]

    recorded = {}

    def fake_analyze_text(prompt, combined_text, model="deepseek-chat"):
        recorded["prompt"] = prompt
        recorded["combined_text"] = combined_text
        return json.dumps(
            [
                {
                    "url": "http://a.example",
                    "is_antique_shop": False,
                    "confidence": 0.12,
                },
                {
                    "url": "http://b.example",
                    "is_antique_shop": True,
                    "confidence": 0.95,
                },
            ]
        )

    monkeypatch.setattr(ds_client, "analyze_text", fake_analyze_text)

    result = ds_client.analyze_antique_shops_batch(pages, model="deepseek-chat")

    assert isinstance(result, list)
    assert result[0]["url"] == "http://a.example"
    assert result[1]["is_antique_shop"] is True

    # Check that the very long content was truncated (<= 4003 incl. '...')
    assert "A" * 4000 in recorded["combined_text"]
    assert "A" * 5000 not in recorded["combined_text"]
    assert "..." in recorded["combined_text"]
