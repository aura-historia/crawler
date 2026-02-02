from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.product_key_data import ProductKeyData
from ...models.watchlist_product_patch_response import WatchlistProductPatchResponse
from ...types import Response


def _get_kwargs(
    *,
    body: ProductKeyData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/me/watchlist",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | WatchlistProductPatchResponse | None:
    if response.status_code == 201:
        response_201 = WatchlistProductPatchResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ApiError.from_dict(response.json())

        return response_401

    if response.status_code == 422:
        response_422 = ApiError.from_dict(response.json())

        return response_422

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
    *,
    client: AuthenticatedClient,
    body: ProductKeyData,
) -> Response[ApiError | WatchlistProductPatchResponse]:
    """Add product to watchlist

     Adds a product to the authenticated user's watchlist.
    The request body must contain the shop ID and shop's product ID.
    Each user is limited to a maximum of 5 watchlist products. If the user already has 5 products
    in their watchlist, adding another will result in a 422 Unprocessable Entity error.
    Returns a 201 Created response with a Location header pointing to the created resource.
    Requires valid Cognito JWT authentication.

    Args:
        body (ProductKeyData): Identifier for a product using shop ID and shop's product ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistProductPatchResponse]
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
    body: ProductKeyData,
) -> ApiError | WatchlistProductPatchResponse | None:
    """Add product to watchlist

     Adds a product to the authenticated user's watchlist.
    The request body must contain the shop ID and shop's product ID.
    Each user is limited to a maximum of 5 watchlist products. If the user already has 5 products
    in their watchlist, adding another will result in a 422 Unprocessable Entity error.
    Returns a 201 Created response with a Location header pointing to the created resource.
    Requires valid Cognito JWT authentication.

    Args:
        body (ProductKeyData): Identifier for a product using shop ID and shop's product ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistProductPatchResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ProductKeyData,
) -> Response[ApiError | WatchlistProductPatchResponse]:
    """Add product to watchlist

     Adds a product to the authenticated user's watchlist.
    The request body must contain the shop ID and shop's product ID.
    Each user is limited to a maximum of 5 watchlist products. If the user already has 5 products
    in their watchlist, adding another will result in a 422 Unprocessable Entity error.
    Returns a 201 Created response with a Location header pointing to the created resource.
    Requires valid Cognito JWT authentication.

    Args:
        body (ProductKeyData): Identifier for a product using shop ID and shop's product ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistProductPatchResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ProductKeyData,
) -> ApiError | WatchlistProductPatchResponse | None:
    """Add product to watchlist

     Adds a product to the authenticated user's watchlist.
    The request body must contain the shop ID and shop's product ID.
    Each user is limited to a maximum of 5 watchlist products. If the user already has 5 products
    in their watchlist, adding another will result in a 422 Unprocessable Entity error.
    Returns a 201 Created response with a Location header pointing to the created resource.
    Requires valid Cognito JWT authentication.

    Args:
        body (ProductKeyData): Identifier for a product using shop ID and shop's product ID

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistProductPatchResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
