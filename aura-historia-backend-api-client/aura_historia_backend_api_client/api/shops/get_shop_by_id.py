from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.get_shop_data import GetShopData
from ...types import Response


def _get_kwargs(
    shop_id: UUID,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/shops/{shop_id}".format(
            shop_id=quote(str(shop_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> ApiError | GetShopData | None:
    if response.status_code == 200:
        response_200 = GetShopData.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = ApiError.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = ApiError.from_dict(response.json())

        return response_500

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
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ApiError | GetShopData]:
    """Get shop details by ID

     Retrieves detailed information about a specific shop by its shop ID (UUID).
    Returns complete shop metadata including name, domains, image, and timestamps.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> ApiError | GetShopData | None:
    """Get shop details by ID

     Retrieves detailed information about a specific shop by its shop ID (UUID).
    Returns complete shop metadata including name, domains, image, and timestamps.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetShopData
    """

    return sync_detailed(
        shop_id=shop_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> Response[ApiError | GetShopData]:
    """Get shop details by ID

     Retrieves detailed information about a specific shop by its shop ID (UUID).
    Returns complete shop metadata including name, domains, image, and timestamps.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
) -> ApiError | GetShopData | None:
    """Get shop details by ID

     Retrieves detailed information about a specific shop by its shop ID (UUID).
    Returns complete shop metadata including name, domains, image, and timestamps.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetShopData
    """

    return (
        await asyncio_detailed(
            shop_id=shop_id,
            client=client,
        )
    ).parsed
