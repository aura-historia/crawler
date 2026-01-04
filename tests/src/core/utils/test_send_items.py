import os
import pytest
from unittest.mock import patch, AsyncMock
from src.core.utils import send_items


class TestSendItems:
    @pytest.mark.asyncio
    async def test_send_items_no_api_url(self, caplog):
        """Should log error and return if AWS_API_URL is not set."""
        with patch.dict(os.environ, {}, clear=True):
            await send_items.send_items([{"foo": "bar"}])
            assert "AWS_API_URL environment variable is not set." in caplog.text

    @pytest.mark.asyncio
    async def test_send_items_success(self):
        """Should call resilient_http_request with correct params if API URL is set."""
        items = [{"foo": "bar"}]
        api_url = "https://example.com/api"
        with patch.dict(os.environ, {"AWS_API_URL": api_url}):
            with patch(
                "src.core.utils.send_items.resilient_http_request",
                new_callable=AsyncMock,
            ) as mock_req:
                with patch("aiohttp.ClientSession") as mock_session:
                    mock_session.return_value.__aenter__.return_value = AsyncMock()
                    await send_items.send_items(items)
                    mock_req.assert_awaited_once()
                    args, kwargs = mock_req.call_args
                    assert args[0] == api_url
                    assert kwargs["method"] == "PUT"
                    assert kwargs["json_data"] == {"items": items}

    @pytest.mark.asyncio
    async def test_send_items_resilient_http_request_exception(self, caplog):
        """Should not raise if resilient_http_request throws exception (handled upstream)."""
        items = [{"foo": "bar"}]
        api_url = "https://example.com/api"
        with patch.dict(os.environ, {"AWS_API_URL": api_url}):
            with patch(
                "src.core.utils.send_items.resilient_http_request",
                new_callable=AsyncMock,
            ) as mock_req:
                mock_req.side_effect = Exception("network error")
                with patch("aiohttp.ClientSession") as mock_session:
                    mock_session.return_value.__aenter__.return_value = AsyncMock()
                    with pytest.raises(Exception):
                        await send_items.send_items(items)
