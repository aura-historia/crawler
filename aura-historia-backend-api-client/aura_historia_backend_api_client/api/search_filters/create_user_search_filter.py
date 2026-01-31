from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.post_user_search_filter_data import PostUserSearchFilterData
from ...models.user_search_filter_data import UserSearchFilterData
from ...types import Response


def _get_kwargs(
    *,
    body: PostUserSearchFilterData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/me/search-filters",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | UserSearchFilterData | None:
    if response.status_code == 201:
        response_201 = UserSearchFilterData.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError.from_dict(response.json())

        return response_401

    if response.status_code == 500:
        response_500 = ApiError.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError | UserSearchFilterData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: PostUserSearchFilterData,
) -> Response[ApiError | UserSearchFilterData]:
    """Create a new search filter

     Creates a new search filter for the authenticated user.
    The search filter configuration is provided in the request body.
    Returns the created search filter with generated ID and metadata.
    Requires valid Cognito JWT authentication.

    Args:
        body (PostUserSearchFilterData): Request body for creating a new search filter

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterData]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: PostUserSearchFilterData,
) -> ApiError | UserSearchFilterData | None:
    """Create a new search filter

     Creates a new search filter for the authenticated user.
    The search filter configuration is provided in the request body.
    Returns the created search filter with generated ID and metadata.
    Requires valid Cognito JWT authentication.

    Args:
        body (PostUserSearchFilterData): Request body for creating a new search filter

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterData
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: PostUserSearchFilterData,
) -> Response[ApiError | UserSearchFilterData]:
    """Create a new search filter

     Creates a new search filter for the authenticated user.
    The search filter configuration is provided in the request body.
    Returns the created search filter with generated ID and metadata.
    Requires valid Cognito JWT authentication.

    Args:
        body (PostUserSearchFilterData): Request body for creating a new search filter

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterData]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: PostUserSearchFilterData,
) -> ApiError | UserSearchFilterData | None:
    """Create a new search filter

     Creates a new search filter for the authenticated user.
    The search filter configuration is provided in the request body.
    Returns the created search filter with generated ID and metadata.
    Requires valid Cognito JWT authentication.

    Args:
        body (PostUserSearchFilterData): Request body for creating a new search filter

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterData
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
