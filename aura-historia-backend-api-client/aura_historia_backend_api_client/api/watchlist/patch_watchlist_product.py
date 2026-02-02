from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.watchlist_product_patch import WatchlistProductPatch
from ...models.watchlist_product_patch_response import WatchlistProductPatchResponse
from ...types import Response


def _get_kwargs(
    shop_id: UUID,
    shops_product_id: str,
    *,
    body: WatchlistProductPatch,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/me/watchlist/{shop_id}/{shops_product_id}".format(
            shop_id=quote(str(shop_id), safe=""),
            shops_product_id=quote(str(shops_product_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | WatchlistProductPatchResponse | None:
    if response.status_code == 200:
        response_200 = WatchlistProductPatchResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError.from_dict(response.json())

        return response_401

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
) -> Response[ApiError | WatchlistProductPatchResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient,
    body: WatchlistProductPatch,
) -> Response[ApiError | WatchlistProductPatchResponse]:
    """Update watchlist product settings

     Updates settings for a specific watchlist product (e.g., toggle notifications).
    Returns the updated watchlist product data with core identifiers and settings.
    Requires valid Cognito JWT authentication.

    Args:
        shop_id (UUID):
        shops_product_id (str):
        body (WatchlistProductPatch): Patch object for updating watchlist product settings

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistProductPatchResponse]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient,
    body: WatchlistProductPatch,
) -> ApiError | WatchlistProductPatchResponse | None:
    """Update watchlist product settings

     Updates settings for a specific watchlist product (e.g., toggle notifications).
    Returns the updated watchlist product data with core identifiers and settings.
    Requires valid Cognito JWT authentication.

    Args:
        shop_id (UUID):
        shops_product_id (str):
        body (WatchlistProductPatch): Patch object for updating watchlist product settings

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistProductPatchResponse
    """

    return sync_detailed(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient,
    body: WatchlistProductPatch,
) -> Response[ApiError | WatchlistProductPatchResponse]:
    """Update watchlist product settings

     Updates settings for a specific watchlist product (e.g., toggle notifications).
    Returns the updated watchlist product data with core identifiers and settings.
    Requires valid Cognito JWT authentication.

    Args:
        shop_id (UUID):
        shops_product_id (str):
        body (WatchlistProductPatch): Patch object for updating watchlist product settings

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistProductPatchResponse]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient,
    body: WatchlistProductPatch,
) -> ApiError | WatchlistProductPatchResponse | None:
    """Update watchlist product settings

     Updates settings for a specific watchlist product (e.g., toggle notifications).
    Returns the updated watchlist product data with core identifiers and settings.
    Requires valid Cognito JWT authentication.

    Args:
        shop_id (UUID):
        shops_product_id (str):
        body (WatchlistProductPatch): Patch object for updating watchlist product settings

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistProductPatchResponse
    """

    return (
        await asyncio_detailed(
            shop_id=shop_id,
            shops_product_id=shops_product_id,
            client=client,
            body=body,
        )
    ).parsed
