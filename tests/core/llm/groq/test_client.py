import os
from unittest.mock import patch, MagicMock
from src.core.llms.groq.client import get_client, _ClientProxy


class TestGroqClient:
    """Simple tests for Groq client."""

    def test_get_client_creates_instance(self):
        """Test that get_client returns a Groq instance."""
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                mock_instance = MagicMock()
                mock_groq.return_value = mock_instance

                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                client = get_client()

                # Verify Groq was instantiated with the API key
                mock_groq.assert_called_once_with(api_key="test-key")
                assert client == mock_instance

    def test_get_client_returns_same_instance(self):
        """Test that get_client returns the same instance on multiple calls."""
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                mock_instance = MagicMock()
                mock_groq.return_value = mock_instance

                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                client1 = get_client()
                client2 = get_client()

                # Should only be instantiated once
                assert mock_groq.call_count == 1
                assert client1 is client2

    def test_get_client_uses_env_variable(self):
        """Test that get_client uses GROQ_API_KEY from environment."""
        test_api_key = "my-secret-api-key-12345"

        with patch.dict(os.environ, {"GROQ_API_KEY": test_api_key}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                get_client()

                # Verify the correct API key was used
                mock_groq.assert_called_once_with(api_key=test_api_key)

    def test_get_client_with_no_api_key(self):
        """Test that get_client works even without API key (None is passed)."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                get_client()

                # Should be called with None
                mock_groq.assert_called_once_with(api_key=None)

    def test_client_proxy_delegates_to_real_client(self):
        """Test that _ClientProxy delegates attribute access to the real client."""
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                mock_instance = MagicMock()
                mock_instance.some_method = MagicMock(return_value="result")
                mock_groq.return_value = mock_instance

                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                proxy = _ClientProxy()
                result = proxy.some_method()

                # Verify the method was called on the real client
                mock_instance.some_method.assert_called_once()
                assert result == "result"

    def test_client_proxy_lazy_initialization(self):
        """Test that _ClientProxy doesn't create client until accessed."""
        with patch("src.core.llms.groq.client.Groq") as mock_groq:
            # Reset the global client
            import src.core.llms.groq.client as client_module

            client_module._client = None

            # Create proxy but don't access anything
            proxy = _ClientProxy()

            # Groq should not be instantiated yet
            mock_groq.assert_not_called()

            # Now access an attribute
            with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
                mock_instance = MagicMock()
                mock_groq.return_value = mock_instance

                _ = proxy.chat

                # Now it should be instantiated
                mock_groq.assert_called_once()

    def test_multiple_proxies_share_same_client(self):
        """Test that multiple _ClientProxy instances share the same underlying client."""
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                mock_instance = MagicMock()
                mock_groq.return_value = mock_instance

                # Reset the global client
                import src.core.llms.groq.client as client_module

                client_module._client = None

                proxy1 = _ClientProxy()
                proxy2 = _ClientProxy()

                # Access both proxies
                _ = proxy1.chat
                _ = proxy2.completions

                # Client should only be created once
                assert mock_groq.call_count == 1

    def test_get_client_resets_after_setting_none(self):
        """Test that setting _client to None allows recreation."""
        with patch.dict(os.environ, {"GROQ_API_KEY": "test-key"}):
            with patch("src.core.llms.groq.client.Groq") as mock_groq:
                mock_instance1 = MagicMock()
                mock_instance2 = MagicMock()
                mock_groq.side_effect = [mock_instance1, mock_instance2]

                import src.core.llms.groq.client as client_module

                # First call
                client_module._client = None
                client1 = get_client()

                # Reset
                client_module._client = None
                client2 = get_client()

                # Should have been created twice
                assert mock_groq.call_count == 2
                assert client1 is mock_instance1
                assert client2 is mock_instance2
