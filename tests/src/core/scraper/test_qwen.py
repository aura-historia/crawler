import json
import pytest
from unittest.mock import AsyncMock, Mock, patch

from src.core.scraper.qwen import (
    chat_completion,
    _find_balanced_brace_object,
    _parse_llm_response,
    extract,
    get_markdown,
)


class TestChatCompletion:
    @pytest.mark.asyncio
    async def test_chat_completion_success(self):
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = '{"test": "value"}'

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await chat_completion("test prompts")

        assert result == '{"test": "value"}'

    @pytest.mark.asyncio
    async def test_chat_completion_empty_response(self):
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = None

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await chat_completion("test prompts")

        assert result == ""

    @pytest.mark.asyncio
    async def test_chat_completion_error(self):
        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            side_effect=Exception("API Error"),
        ):
            result = await chat_completion("test prompts")

        assert result == "{}"

    @pytest.mark.asyncio
    async def test_chat_completion_parameters(self):
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "test"

        mock_create = AsyncMock(return_value=mock_response)

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new=mock_create,
        ):
            await chat_completion("test prompts")

        call_args = mock_create.call_args
        call_kwargs = call_args[1] if call_args else {}
        assert call_kwargs.get("temperature") == 0
        assert call_kwargs.get("max_tokens") == 2048
        assert call_kwargs.get("messages")[0]["content"] == "test prompts"


class TestFindBalancedBraceObject:
    def test_find_simple_object(self):
        text = 'Some text {"key": "value"} more text'
        assert _find_balanced_brace_object(text) == '{"key": "value"}'

    def test_find_nested_object(self):
        text = 'Text {"outer": {"inner": "value"}} text'
        assert _find_balanced_brace_object(text) == '{"outer": {"inner": "value"}}'

    def test_find_first_object_only(self):
        text = '{"first": 1} {"second": 2}'
        assert _find_balanced_brace_object(text) == '{"first": 1}'

    def test_no_object_found(self):
        assert _find_balanced_brace_object("No JSON") is None

    def test_unclosed_braces(self):
        assert _find_balanced_brace_object('{"a": 1') is None

    def test_empty_string(self):
        assert _find_balanced_brace_object("") is None


class TestParseLlmResponse:
    def test_parse_valid_json(self):
        response = '{"title": "Product", "price": 1999}'
        parsed = json.loads(_parse_llm_response(response))
        assert parsed["title"] == "Product"

    def test_parse_empty_response(self):
        assert _parse_llm_response("") == "{}"

    def test_parse_json_with_prefix(self):
        response = 'Here is JSON {"key": "value"}'
        parsed = json.loads(_parse_llm_response(response))
        assert parsed["key"] == "value"

    def test_parse_unicode_content(self):
        response = '{"title": "Möbel", "price": 1999}'
        parsed = json.loads(_parse_llm_response(response))
        assert parsed["title"] == "Möbel"

    def test_parse_invalid(self):
        assert _parse_llm_response("not json") == "{}"


class TestExtract:
    @pytest.mark.asyncio
    async def test_extract_invalid_returns_none(self):
        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value="not json",
        ):
            result = await extract("test")
        assert result is None

    @pytest.mark.asyncio
    async def test_extract_exception_propagates(self):
        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            side_effect=Exception("LLM Error"),
        ):
            with pytest.raises(Exception):
                await extract("test")


class TestGetMarkdown:
    @pytest.mark.asyncio
    async def test_get_markdown_truncation(self):
        long_markdown = "x" * 50000
        mock_result = Mock(success=True, markdown=long_markdown)

        mock_crawler = AsyncMock()
        mock_crawler.arun = AsyncMock(return_value=mock_result)
        mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
        mock_crawler.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "src.core.scraper.qwen.AsyncWebCrawler",
            return_value=mock_crawler,
        ):
            result = await get_markdown("https://example.com")

        assert len(result) == 30000

    @pytest.mark.asyncio
    async def test_get_markdown_failure(self):
        mock_result = Mock(success=False, exception=RuntimeError("Crawl failed"))

        mock_crawler = AsyncMock()
        mock_crawler.arun = AsyncMock(return_value=mock_result)
        mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
        mock_crawler.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "src.core.scraper.qwen.AsyncWebCrawler",
            return_value=mock_crawler,
        ):
            with pytest.raises(RuntimeError):
                await get_markdown("https://example.com")
