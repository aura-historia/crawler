import pytest
import responses
from requests.exceptions import RetryError
from src.core.utils.network import http_session


class TestResilientSession:
    @responses.activate
    def test_retry_on_502_error(self):
        """Test that the session retries on a 502 status code."""
        url = "https://api.example.com/test"

        responses.add(responses.GET, url, status=502)
        responses.add(responses.GET, url, status=502)
        responses.add(responses.GET, url, status=200, json={"message": "success"})

        response = http_session.get(url)

        assert response.status_code == 200
        assert len(responses.calls) == 3

    @responses.activate
    def test_max_retries_exceeded(self):
        """Test that RetryError is raised after 3 failed attempts (total=3)."""
        url = "https://api.example.com/fail"

        for _ in range(4):
            responses.add(responses.GET, url, status=504)

        with pytest.raises(RetryError):
            http_session.get(url)

        assert len(responses.calls) == 4

    @responses.activate
    def test_no_retry_on_404(self):
        """Test that it does NOT retry on a 404 (not in forcelist)."""
        url = "https://api.example.com/404"
        responses.add(responses.GET, url, status=404)

        response = http_session.get(url)

        assert response.status_code == 404
        assert len(responses.calls) == 1

    @responses.activate
    def test_patch_method_retries(self):
        """Verify that PATCH also retries."""
        url = "https://api.example.com/update"
        responses.add(responses.PATCH, url, status=500)
        responses.add(responses.PATCH, url, status=200)

        response = http_session.patch(url, json={"key": "val"})

        assert response.status_code == 200
        assert len(responses.calls) == 2
