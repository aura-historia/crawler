import os
from unittest.mock import patch
import importlib


def test_api_client_initialization():
    with patch.dict(
        os.environ, {"API_KEY": "test_api_key", "API_BASE_URL": "https://api.test.com"}
    ):
        from src.core.utils import api_client

        importlib.reload(api_client)
        assert api_client.api_client._base_url == "https://api.test.com"
        assert api_client.headers == {"X-API-Key": "test_api_key"}


def test_api_client_missing_base_url():
    with patch.dict(os.environ, {"API_BASE_URL": ""}):
        from src.core.utils import api_client

        importlib.reload(api_client)
        assert api_client.api_client is None
