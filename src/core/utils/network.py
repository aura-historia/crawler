import asyncio
import aiohttp
from aiohttp import ClientSession
from aiohttp_retry import RetryClient, ExponentialRetry
from typing import Optional, Union, Any

from src.core.utils.logger import logger

RETRY_STATUSES = {429, 500, 502, 503, 504}
RETRY_EXCEPTIONS = {asyncio.TimeoutError, aiohttp.ClientError}


async def resilient_http_request(
    url: str,
    session: Union[ClientSession, RetryClient],
    method: str = "GET",
    retry_attempts: int = 3,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    json_data: Optional[dict] = None,
    data: Any = None,
    timeout_seconds: float = 30.0,
    return_json: bool = False,
    return_bytes: bool = False,
    return_response: bool = False,
    **kwargs,
) -> Union[str, dict, bytes, aiohttp.ClientResponse]:
    """
    Perform an asynchronous HTTP request with automatic retries and error handling.

    This function uses aiohttp and aiohttp_retry's ExponentialRetry to robustly handle transient errors
    (e.g., rate limits, server errors, timeouts) for GET, POST, PUT, PATCH requests. It supports flexible
    session management (ClientSession or RetryClient), custom headers, query parameters, JSON/form data,
    and configurable timeouts. Responses can be returned as text, JSON, or bytes.

    Args:
        url (str): The request URL.
        session (ClientSession | RetryClient): An aiohttp ClientSession or RetryClient instance.
        method (str): HTTP method (default: "GET").
        retry_attempts (int): Number of retry attempts on failure.
        headers (dict, optional): HTTP headers.
        params (dict, optional): URL query parameters.
        json_data (dict, optional): JSON data for POST/PUT/PATCH.
        data (Any, optional): Form or binary data.
        timeout_seconds (float): Request timeout in seconds (default: 30.0).
        return_json (bool): If True, returns response as JSON (dict).
        return_bytes (bool): If True, returns response as bytes.
        return_response (bool): If True, returns the entire aiohttp response object.
        **kwargs: Additional arguments for aiohttp request.

    Returns:
        str | dict | bytes | ClientResponse: Response content as text, JSON, bytes, or the entire response object.

    Raises:
        aiohttp.ClientError: For network or protocol errors.
        ValueError: If JSON parsing fails when return_json is True.
        Exception: For other unexpected errors.
    """
    retry_options = ExponentialRetry(
        attempts=retry_attempts,
        start_timeout=1.0,
        max_timeout=20.0,
        factor=2.0,
        statuses=RETRY_STATUSES,
        retry_all_server_errors=True,
        exceptions=RETRY_EXCEPTIONS,
        methods={"GET", "POST", "PUT", "PATCH"},
    )

    if isinstance(session, RetryClient):
        client = session
    else:
        client = RetryClient(client_session=session, retry_options=retry_options)

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)

        async with client.request(
            method=method.upper(),
            url=url,
            headers=headers or {},
            params=params or {},
            json=json_data,
            data=data,
            timeout=timeout,
            **kwargs,
        ) as response:
            if return_response:
                return response
            try:
                response.raise_for_status()
            except Exception:
                if return_response:
                    return response
                raise
            if return_json:
                try:
                    return await response.json()
                except (aiohttp.ContentTypeError, ValueError) as e:
                    logger.error(f"Invalid JSON from {url}")
                    raise ValueError(f"URL {url} returned invalid JSON") from e
            if return_bytes:
                return await response.read()
            return await response.text()

    except Exception as e:
        logger.error(f"HTTP {method} to {url} failed: {str(e)}")
        raise
