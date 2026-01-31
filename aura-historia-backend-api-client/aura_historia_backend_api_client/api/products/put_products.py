from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.put_products_collection_data import PutProductsCollectionData
from ...models.put_products_response import PutProductsResponse
from ...types import Response


def _get_kwargs(
    *,
    body: PutProductsCollectionData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/api/v1/products",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | PutProductsResponse | None:
    if response.status_code == 200:
        response_200 = PutProductsResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ApiError.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = ApiError.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError | PutProductsResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: PutProductsCollectionData,
) -> Response[ApiError | PutProductsResponse]:
    """Bulk create or update products

     Creates or updates multiple products in a single batch request.
    This endpoint accepts a collection of product data and processes them asynchronously.

    **Shop Enrichment**: The shop information (shopId and shopName) is automatically
    enriched based on the product's URL. The domain is extracted from the product URL
    and must match a shop that is already registered in the system. If the shop is not
    found, the product will fail with a SHOP_NOT_FOUND error. If the URL does not contain
    a valid extractable domain, the product will fail with a NO_DOMAIN error.

    **Response Structure**:
    - `skipped`: Number of products that had no changes and were skipped
    - `unprocessed`: URLs of products that could not be processed due to temporary issues (can be
    retried)
    - `failed`: Map of product URLs to error codes for products that permanently failed processing

    Returns information about any products that could not be processed or failed during enrichment.

    Args:
        body (PutProductsCollectionData): Collection of products to create or update

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | PutProductsResponse]
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
    body: PutProductsCollectionData,
) -> ApiError | PutProductsResponse | None:
    """Bulk create or update products

     Creates or updates multiple products in a single batch request.
    This endpoint accepts a collection of product data and processes them asynchronously.

    **Shop Enrichment**: The shop information (shopId and shopName) is automatically
    enriched based on the product's URL. The domain is extracted from the product URL
    and must match a shop that is already registered in the system. If the shop is not
    found, the product will fail with a SHOP_NOT_FOUND error. If the URL does not contain
    a valid extractable domain, the product will fail with a NO_DOMAIN error.

    **Response Structure**:
    - `skipped`: Number of products that had no changes and were skipped
    - `unprocessed`: URLs of products that could not be processed due to temporary issues (can be
    retried)
    - `failed`: Map of product URLs to error codes for products that permanently failed processing

    Returns information about any products that could not be processed or failed during enrichment.

    Args:
        body (PutProductsCollectionData): Collection of products to create or update

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | PutProductsResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: PutProductsCollectionData,
) -> Response[ApiError | PutProductsResponse]:
    """Bulk create or update products

     Creates or updates multiple products in a single batch request.
    This endpoint accepts a collection of product data and processes them asynchronously.

    **Shop Enrichment**: The shop information (shopId and shopName) is automatically
    enriched based on the product's URL. The domain is extracted from the product URL
    and must match a shop that is already registered in the system. If the shop is not
    found, the product will fail with a SHOP_NOT_FOUND error. If the URL does not contain
    a valid extractable domain, the product will fail with a NO_DOMAIN error.

    **Response Structure**:
    - `skipped`: Number of products that had no changes and were skipped
    - `unprocessed`: URLs of products that could not be processed due to temporary issues (can be
    retried)
    - `failed`: Map of product URLs to error codes for products that permanently failed processing

    Returns information about any products that could not be processed or failed during enrichment.

    Args:
        body (PutProductsCollectionData): Collection of products to create or update

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | PutProductsResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: PutProductsCollectionData,
) -> ApiError | PutProductsResponse | None:
    """Bulk create or update products

     Creates or updates multiple products in a single batch request.
    This endpoint accepts a collection of product data and processes them asynchronously.

    **Shop Enrichment**: The shop information (shopId and shopName) is automatically
    enriched based on the product's URL. The domain is extracted from the product URL
    and must match a shop that is already registered in the system. If the shop is not
    found, the product will fail with a SHOP_NOT_FOUND error. If the URL does not contain
    a valid extractable domain, the product will fail with a NO_DOMAIN error.

    **Response Structure**:
    - `skipped`: Number of products that had no changes and were skipped
    - `unprocessed`: URLs of products that could not be processed due to temporary issues (can be
    retried)
    - `failed`: Map of product URLs to error codes for products that permanently failed processing

    Returns information about any products that could not be processed or failed during enrichment.

    Args:
        body (PutProductsCollectionData): Collection of products to create or update

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | PutProductsResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
