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
    """Tests for the chat_completion function."""

    @pytest.mark.asyncio
    async def test_chat_completion_success(self):
        """Test successful chat completion with valid response."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = '{"test": "value"}'

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await chat_completion("test prompt")

        assert result == '{"test": "value"}'

    @pytest.mark.asyncio
    async def test_chat_completion_empty_response(self):
        """Test chat completion with empty content."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = None

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await chat_completion("test prompt")

        assert result == ""

    @pytest.mark.asyncio
    async def test_chat_completion_error(self):
        """Test chat completion when an exception occurs."""
        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new_callable=AsyncMock,
            side_effect=Exception("API Error"),
        ):
            result = await chat_completion("test prompt")

        assert result == "{}"

    @pytest.mark.asyncio
    async def test_chat_completion_parameters(self):
        """Test that chat completion is called with correct parameters."""
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "test"

        mock_create = AsyncMock(return_value=mock_response)

        with patch(
            "src.core.scraper.qwen.client.chat.completions.create",
            new=mock_create,
        ):
            await chat_completion("test prompt")

        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["temperature"] == 0
        assert call_kwargs["max_tokens"] == 2048
        assert call_kwargs["messages"][0]["content"] == "test prompt"


class TestFindBalancedBraceObject:
    """Tests for the _find_balanced_brace_object function."""

    def test_find_simple_object(self):
        """Test finding a simple JSON object."""
        text = 'Some text {"key": "value"} more text'
        result = _find_balanced_brace_object(text)
        assert result == '{"key": "value"}'

    def test_find_nested_object(self):
        """Test finding a nested JSON object."""
        text = 'Text {"outer": {"inner": "value"}} text'
        result = _find_balanced_brace_object(text)
        assert result == '{"outer": {"inner": "value"}}'

    def test_find_first_object_only(self):
        """Test that only the first complete object is returned."""
        text = '{"first": 1} {"second": 2}'
        result = _find_balanced_brace_object(text)
        assert result == '{"first": 1}'

    def test_no_object_found(self):
        """Test when no JSON object is present."""
        text = "No JSON here"
        result = _find_balanced_brace_object(text)
        assert result is None

    def test_unclosed_braces(self):
        """Test when braces are not balanced."""
        text = '{"unclosed": "object"'
        result = _find_balanced_brace_object(text)
        assert result is None

    def test_empty_string(self):
        """Test with empty string."""
        result = _find_balanced_brace_object("")
        assert result is None

    def test_complex_nested_structure(self):
        """Test with deeply nested structure."""
        text = 'prefix {"a": {"b": {"c": "value"}}, "d": [1, 2, 3]} suffix'
        result = _find_balanced_brace_object(text)
        assert result == '{"a": {"b": {"c": "value"}}, "d": [1, 2, 3]}'


class TestParseLlmResponse:
    """Tests for the _parse_llm_response function."""

    def test_parse_valid_json(self):
        """Test parsing valid JSON directly."""
        response = '{"title": "Product", "price": 1999}'
        result = _parse_llm_response(response)
        parsed = json.loads(result)
        assert parsed["title"] == "Product"
        assert parsed["price"] == 1999

    def test_parse_empty_response(self):
        """Test parsing empty response."""
        result = _parse_llm_response("")
        assert result == "{}"

    def test_parse_none_response(self):
        """Test parsing None response."""
        result = _parse_llm_response(None)
        assert result == "{}"

    def test_parse_json_with_prefix(self):
        """Test parsing JSON with text prefix."""
        response = 'Here is the result: {"key": "value"}'
        result = _parse_llm_response(response)
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_parse_json_with_newlines(self):
        """Test parsing JSON with newlines."""
        response = '{\n  "key": "value"\n}'
        result = _parse_llm_response(response)
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_parse_malformed_json_with_newlines(self):
        """Test parsing malformed JSON that needs newline sanitization."""
        # This should be extracted and sanitized
        response = 'text {"key": "value\nwith newline"} text'
        result = _parse_llm_response(response)
        # Should return either valid JSON or {}
        assert result == "{}" or json.loads(result)

    def test_parse_completely_invalid(self):
        """Test parsing completely invalid content."""
        response = "This is not JSON at all, no braces here"
        result = _parse_llm_response(response)
        assert result == "{}"

    def test_parse_unicode_content(self):
        """Test parsing JSON with Unicode characters."""
        response = '{"title": "Möbel für Küche", "price": 1999}'
        result = _parse_llm_response(response)
        parsed = json.loads(result)
        assert parsed["title"] == "Möbel für Küche"

    def test_parse_empty_json_object(self):
        """Test parsing empty JSON object."""
        response = "{}"
        result = _parse_llm_response(response)
        assert result == "{}"


class TestExtract:
    """Tests for the extract function."""

    @pytest.mark.asyncio
    async def test_extract_success(self):
        """Test successful extraction with valid response."""
        markdown = "# Product Title\nPrice: €19.99"

        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value='{"title": "Product", "price": 1999}',
        ):
            result = await extract(markdown)

        parsed = json.loads(result)
        assert parsed["title"] == "Product"
        assert parsed["price"] == 1999

    @pytest.mark.asyncio
    async def test_extract_handles_chat_completion_error(self):
        """Test extraction when chat_completion raises an exception."""
        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            side_effect=Exception("LLM Error"),
        ):
            result = await extract("test markdown")

        assert result == "{}"

    @pytest.mark.asyncio
    async def test_extract_with_unicode_markdown(self):
        """Test extraction with Unicode characters in markdown."""
        markdown = "# Möbel\nPreis: €19,99\nBeschreibung: Schöne Antike"

        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value='{"title": "Möbel", "price": 1999}',
        ):
            result = await extract(markdown)

        parsed = json.loads(result)
        assert parsed["title"] == "Möbel"


class TestGetMarkdown:
    """Tests for the get_markdown function."""

    @pytest.mark.asyncio
    async def test_get_markdown_truncation(self):
        """Test that markdown is truncated to 40k characters."""
        long_markdown = "x" * 50000
        mock_result = Mock()
        mock_result.success = True
        mock_result.markdown = long_markdown

        mock_crawler = AsyncMock()
        mock_crawler.arun = AsyncMock(return_value=mock_result)
        mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
        mock_crawler.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "src.core.scraper.qwen.AsyncWebCrawler",
            return_value=mock_crawler,
        ):
            result = await get_markdown("https://example.com")

        assert len(result) == 40000

    @pytest.mark.asyncio
    async def test_get_markdown_failure(self):
        """Test markdown retrieval when crawl fails."""
        mock_result = Mock()
        mock_result.success = False
        mock_result.exception = RuntimeError("Crawl failed")

        mock_crawler = AsyncMock()
        mock_crawler.arun = AsyncMock(return_value=mock_result)
        mock_crawler.__aenter__ = AsyncMock(return_value=mock_crawler)
        mock_crawler.__aexit__ = AsyncMock(return_value=None)

        with patch(
            "src.core.scraper.qwen.AsyncWebCrawler",
            return_value=mock_crawler,
        ):
            with pytest.raises(RuntimeError, match="Crawl failed"):
                await get_markdown("https://example.com")


class TestIntegration:
    """Integration tests combining multiple functions."""

    @pytest.mark.asyncio
    async def test_end_to_end_extraction(self):
        """Test complete extraction flow from chat completion to parsed result."""
        llm_response = """
        Here's the extracted data:
        {
            "shop_item_id": "12345",
            "title": "Antique Table",
            "current_price": 15000,
            "currency": "EUR",
            "state": "AVAILABLE"
        }
        """

        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value=llm_response,
        ):
            result = await extract("# Product Page Content")

        parsed = json.loads(result)
        assert parsed["shop_item_id"] == "12345"
        assert parsed["title"] == "Antique Table"
        assert parsed["current_price"] == 15000
        assert parsed["currency"] == "EUR"
        assert parsed["state"] == "AVAILABLE"

    @pytest.mark.asyncio
    async def test_extraction_with_malformed_llm_output(self):
        """Test extraction handles various malformed LLM outputs gracefully."""
        test_cases = [
            "Just plain text, no JSON",
            '{"incomplete": "json"',
            "{}",
            "",
            '{"key": "value\nwith\nnewlines"}',
        ]

        for llm_response in test_cases:
            with patch(
                "src.core.scraper.qwen.chat_completion",
                new_callable=AsyncMock,
                return_value=llm_response,
            ):
                result = await extract("test")

            # Should always return valid JSON string
            assert isinstance(result, str)
            parsed = json.loads(result)  # Should not raise
            # Result should be a dict (even if empty)
            assert isinstance(parsed, dict) or parsed is None
