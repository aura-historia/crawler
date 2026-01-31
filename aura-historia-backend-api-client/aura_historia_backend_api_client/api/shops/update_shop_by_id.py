from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.get_shop_data import GetShopData
from ...models.patch_shop_data import PatchShopData
from ...types import Response


def _get_kwargs(
    shop_id: UUID,
    *,
    body: PatchShopData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/shops/{shop_id}".format(
            shop_id=quote(str(shop_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
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
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: PatchShopData,
) -> Response[ApiError | GetShopData]:
    """Update shop details by ID

     Updates an existing shop's information by its shop ID (UUID).
    All fields in the request body are optional - only provided fields will be updated.
    If the request body is empty or only contains null values, the shop is returned unchanged.
    When updating domains, the complete new set of domains must be provided.

    **Note**: The shop name cannot be updated as it determines the shop's slug identifier.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        body (PatchShopData): Partial update data for a shop.
            All fields are optional - only provided fields will be updated.
            If the request body is empty or all fields are null, the shop is returned unchanged.

            **Note**: The shop name cannot be updated after creation as it determines the shop slug
            identifier.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: PatchShopData,
) -> ApiError | GetShopData | None:
    """Update shop details by ID

     Updates an existing shop's information by its shop ID (UUID).
    All fields in the request body are optional - only provided fields will be updated.
    If the request body is empty or only contains null values, the shop is returned unchanged.
    When updating domains, the complete new set of domains must be provided.

    **Note**: The shop name cannot be updated as it determines the shop's slug identifier.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        body (PatchShopData): Partial update data for a shop.
            All fields are optional - only provided fields will be updated.
            If the request body is empty or all fields are null, the shop is returned unchanged.

            **Note**: The shop name cannot be updated after creation as it determines the shop slug
            identifier.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetShopData
    """

    return sync_detailed(
        shop_id=shop_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: PatchShopData,
) -> Response[ApiError | GetShopData]:
    """Update shop details by ID

     Updates an existing shop's information by its shop ID (UUID).
    All fields in the request body are optional - only provided fields will be updated.
    If the request body is empty or only contains null values, the shop is returned unchanged.
    When updating domains, the complete new set of domains must be provided.

    **Note**: The shop name cannot be updated as it determines the shop's slug identifier.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        body (PatchShopData): Partial update data for a shop.
            All fields are optional - only provided fields will be updated.
            If the request body is empty or all fields are null, the shop is returned unchanged.

            **Note**: The shop name cannot be updated after creation as it determines the shop slug
            identifier.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetShopData]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shop_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: PatchShopData,
) -> ApiError | GetShopData | None:
    """Update shop details by ID

     Updates an existing shop's information by its shop ID (UUID).
    All fields in the request body are optional - only provided fields will be updated.
    If the request body is empty or only contains null values, the shop is returned unchanged.
    When updating domains, the complete new set of domains must be provided.

    **Note**: The shop name cannot be updated as it determines the shop's slug identifier.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        body (PatchShopData): Partial update data for a shop.
            All fields are optional - only provided fields will be updated.
            If the request body is empty or all fields are null, the shop is returned unchanged.

            **Note**: The shop name cannot be updated after creation as it determines the shop slug
            identifier.

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
            body=body,
        )
    ).parsed
