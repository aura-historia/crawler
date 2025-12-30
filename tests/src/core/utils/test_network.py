import pytest
from aiohttp import ClientSession
from aioresponses import aioresponses
from src.core.utils.network import resilient_http_request

pytestmark = pytest.mark.asyncio


class TestResilientHttpRequest:
    @pytest.mark.asyncio
    async def test_retry_on_502_error(self):
        url = "https://api.example.com/test"
        with aioresponses() as m:
            m.get(url, status=502)
            m.get(url, status=502)
            m.get(url, status=200, payload={"message": "success"})
            async with ClientSession() as session:
                result = await resilient_http_request(
                    url, session, retry_attempts=3, timeout_seconds=1, return_json=True
                )
        assert result == {"message": "success"}
        total_calls = sum(len(v) for v in m.requests.values())
        assert total_calls == 3

    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self):
        url = "https://api.example.com/fail"
        with aioresponses() as m:
            for _ in range(4):
                m.get(url, status=504)
            async with ClientSession() as session:
                with pytest.raises(Exception):
                    await resilient_http_request(
                        url, session, retry_attempts=3, timeout_seconds=1
                    )
        total_calls = sum(len(v) for v in m.requests.values())
        assert total_calls == 3

    @pytest.mark.asyncio
    async def test_no_retry_on_404(self):
        url = "https://api.example.com/404"
        with aioresponses() as m:
            m.get(url, status=404)
            async with ClientSession() as session:
                with pytest.raises(Exception):
                    await resilient_http_request(url, session)
        total_calls = sum(len(v) for v in m.requests.values())
        assert total_calls == 1

    @pytest.mark.asyncio
    async def test_patch_method_retries(self):
        url = "https://api.example.com/update"
        with aioresponses() as m:
            m.patch(url, status=500)
            m.patch(url, status=200, payload={"ok": True})
            async with ClientSession() as session:
                result = await resilient_http_request(
                    url,
                    session,
                    method="PATCH",
                    json_data={"key": "val"},
                    return_json=True,
                    timeout_seconds=1,
                )
        assert result == {"ok": True}
        total_calls = sum(len(v) for v in m.requests.values())
        assert total_calls == 2
