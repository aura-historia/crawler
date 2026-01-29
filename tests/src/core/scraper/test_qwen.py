import json
import pytest
from unittest.mock import AsyncMock, Mock, patch

from src.core.scraper.qwen import (
    chat_completion,
    _find_balanced_brace_object,
    _parse_llm_response,
    extract,
    get_markdown,
    _apply_boilerplate_removal,
)
from src.core.scraper.schemas.extracted_product import ExtractedProduct


class TestChatCompletion:
    @pytest.mark.asyncio
    async def test_chat_completion_success(self):
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = '{"test": "value"}'

        with patch(
            "src.core.scraper.base.client.chat.completions.create",
            new_callable=AsyncMock,
            return_value=mock_response,
        ):
            result = await chat_completion("system", "user prompts")

        assert result == '{"test": "value"}'

    @pytest.mark.asyncio
    async def test_chat_completion_error_returns_empty_json(self):
        with patch(
            "src.core.scraper.base.client.chat.completions.create",
            new_callable=AsyncMock,
            side_effect=Exception("API Error"),
        ):
            result = await chat_completion("system", "user prompts")

        assert result == "{}"


class TestLlmResponseParsing:
    @pytest.mark.parametrize(
        "input_text,expected_json",
        [
            ('{"key": "value"}', '{"key": "value"}'),
            ('Pre-text {"key": "value"} Post-text', '{"key": "value"}'),
            ('```json\n{"nested": {"id": 1}}\n```', '{"nested": {"id": 1}}'),
            ('{"title": "Möbel"}', '{"title": "Möbel"}'),  # Unicode
            ("Invalid text", "{}"),
            ("", "{}"),
        ],
    )
    def test_parse_llm_response_parametrized(self, input_text, expected_json):
        result = _parse_llm_response(input_text)
        assert result == expected_json

    @pytest.mark.parametrize(
        "text,expected",
        [
            ('{"a": 1}', '{"a": 1}'),
            ('text {"a": {"b": 2}} text', '{"a": {"b": 2}}'),
            ('{"a": 1} {"b": 2}', '{"a": 1}'),
            ('{"a": 1', None),  # Unbalanced
            ("", None),
        ],
    )
    def test_find_balanced_brace_object(self, text, expected):
        assert _find_balanced_brace_object(text) == expected


class TestExtractFlow:
    @pytest.mark.asyncio
    async def test_extract_full_model_state_validation(self):
        """Solid test verifying full model hydration and domain-based cleaning."""
        mock_llm_response = {
            "is_product": True,
            "shopsProductId": "LOT123",
            "title": {"text": "Antique Chair", "language": "en"},
            "description": {"text": "Fancy chair", "language": "en"},
            "price": {"amount": 50000, "currency": "EUR"},
            "state": "AVAILABLE",
            "images": ["http://img.jpg"],
        }

        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value=json.dumps(mock_llm_response),
        ):
            with patch(
                "src.core.scraper.qwen._apply_boilerplate_removal",
                new_callable=AsyncMock,
                side_effect=lambda m, d: f"CLEANED {m}",
            ) as mock_clean:
                result = await extract("original markdown", domain="shop.com")

                assert isinstance(result, ExtractedProduct)
                assert result.is_product is True
                assert result.shopsProductId == "LOT123"
                assert result.title.text == "Antique Chair"
                assert result.price.amount == 50000
                assert result.state == "AVAILABLE"

                # Verify cleaning was triggered correctly
                mock_clean.assert_called_once_with("original markdown", "shop.com")

    @pytest.mark.asyncio
    async def test_extract_returns_none_on_invalid_json(self):
        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value="Not JSON",
        ):
            result = await extract("markdown")
            assert result is None

    @pytest.mark.asyncio
    async def test_extract_validation_failure_returns_none(self):
        # Missing required field 'is_product'
        invalid_data = {"title": {"text": "test", "language": "en"}}
        with patch(
            "src.core.scraper.qwen.chat_completion",
            new_callable=AsyncMock,
            return_value=json.dumps(invalid_data),
        ):
            result = await extract("markdown")
            assert result is None


class TestBoilerplateIntegration:
    @pytest.mark.asyncio
    async def test_apply_boilerplate_removal_integrated(self):
        """Verify that _apply_boilerplate_removal coordinates noise removal and block removal correctly."""
        original = "## Related Products\nBad info\n## Footer\nThis is a footer\n## Header\nProduct Info"

        with patch(
            "src.core.scraper.qwen.boilerplate_remover.load_for_shop",
            new_callable=AsyncMock,
            return_value=[["This is a footer"]],
        ):
            result = await _apply_boilerplate_removal(original, "test.com")

            assert "Related Products" not in result
            assert "Bad info" not in result
            assert "This is a footer" not in result
            assert "Product Info" in result
            assert "Header" in result

    @pytest.mark.asyncio
    async def test_apply_boilerplate_removal_falls_back_on_error(self):
        with patch(
            "src.core.scraper.qwen.boilerplate_remover.load_for_shop",
            side_effect=Exception("S3 Failed"),
        ):
            result = await _apply_boilerplate_removal("some markdown", "fail.com")
            assert result == "some markdown"


class TestGetMarkdown:
    @pytest.mark.asyncio
    async def test_get_markdown_integration(self):
        """Verify get_markdown successfully returns content from crawler mock."""
        long_content = "content " * 1000
        mock_res = Mock(success=True, markdown=long_content)

        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = [mock_res]
        mock_crawler.__aenter__.return_value = mock_crawler

        with patch("src.core.scraper.base.AsyncWebCrawler", return_value=mock_crawler):
            result = await get_markdown("http://test.com")
            assert result == long_content

    @pytest.mark.asyncio
    async def test_get_markdown_handles_failure(self):
        mock_res = Mock(success=False, error_message="Failed")
        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = [mock_res]
        mock_crawler.__aenter__.return_value = mock_crawler

        with patch("src.core.scraper.base.AsyncWebCrawler", return_value=mock_crawler):
            result = await get_markdown("http://test.com")
            assert result == ""
