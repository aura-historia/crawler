from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.get_shop_data import GetShopData
from ...models.post_shop_data import PostShopData
from ...types import Response


def _get_kwargs(
    *,
    body: PostShopData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/shops",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> ApiError | GetShopData | None:
    if response.status_code == 201:
        response_201 = GetShopData.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 409:
        response_409 = ApiError.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = ApiError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = ApiError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError | GetShopData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: PostShopData,
) -> Response[ApiError | GetShopData]:
    """Create a new shop

     Creates a new shop in the system with the provided details.
    The shop must include at least one domain and can have up to 100 domains.
    Returns the created shop with generated ID and timestamps.

    **Uniqueness Checks**:
    - The shop name must be unique (via slug normalization). A slug is automatically generated from the
    shop name.
    - All shop domains must be unique - no domain can be associated with multiple shops.

    If either uniqueness constraint is violated, a 409 Conflict error is returned.

    Args:
        body (PostShopData): Data required to create a new shop

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
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
    client: AuthenticatedClient | Client,
    body: PostShopData,
) -> ApiError | GetShopData | None:
    """Create a new shop

     Creates a new shop in the system with the provided details.
    The shop must include at least one domain and can have up to 100 domains.
    Returns the created shop with generated ID and timestamps.

    **Uniqueness Checks**:
    - The shop name must be unique (via slug normalization). A slug is automatically generated from the
    shop name.
    - All shop domains must be unique - no domain can be associated with multiple shops.

    If either uniqueness constraint is violated, a 409 Conflict error is returned.

    Args:
        body (PostShopData): Data required to create a new shop

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetShopData
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: PostShopData,
) -> Response[ApiError | GetShopData]:
    """Create a new shop

     Creates a new shop in the system with the provided details.
    The shop must include at least one domain and can have up to 100 domains.
    Returns the created shop with generated ID and timestamps.

    **Uniqueness Checks**:
    - The shop name must be unique (via slug normalization). A slug is automatically generated from the
    shop name.
    - All shop domains must be unique - no domain can be associated with multiple shops.

    If either uniqueness constraint is violated, a 409 Conflict error is returned.

    Args:
        body (PostShopData): Data required to create a new shop

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: PostShopData,
) -> ApiError | GetShopData | None:
    """Create a new shop

     Creates a new shop in the system with the provided details.
    The shop must include at least one domain and can have up to 100 domains.
    Returns the created shop with generated ID and timestamps.

    **Uniqueness Checks**:
    - The shop name must be unique (via slug normalization). A slug is automatically generated from the
    shop name.
    - All shop domains must be unique - no domain can be associated with multiple shops.

    If either uniqueness constraint is violated, a 409 Conflict error is returned.

    Args:
        body (PostShopData): Data required to create a new shop

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetShopData
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
