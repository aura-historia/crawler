import datetime
from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.currency_data import CurrencyData
from ...models.get_watchlist_products_order import GetWatchlistProductsOrder
from ...models.sort_watchlist_product_field_data import SortWatchlistProductFieldData
from ...models.watchlist_collection_data import WatchlistCollectionData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    currency: CurrencyData | Unset = UNSET,
    sort: SortWatchlistProductFieldData | Unset = UNSET,
    order: GetWatchlistProductsOrder | Unset = UNSET,
    search_after: datetime.datetime | Unset = UNSET,
    size: int | Unset = 21,
    accept_language: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(accept_language, Unset):
        headers["Accept-Language"] = accept_language

    params: dict[str, Any] = {}

    json_currency: str | Unset = UNSET
    if not isinstance(currency, Unset):
        json_currency = currency.value

    params["currency"] = json_currency

    json_sort: str | Unset = UNSET
    if not isinstance(sort, Unset):
        json_sort = sort.value

    params["sort"] = json_sort

    json_order: str | Unset = UNSET
    if not isinstance(order, Unset):
        json_order = order.value

    params["order"] = json_order

    json_search_after: str | Unset = UNSET
    if not isinstance(search_after, Unset):
        json_search_after = search_after.isoformat()
    params["searchAfter"] = json_search_after

    params["size"] = size

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/me/watchlist",
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | WatchlistCollectionData | None:
    if response.status_code == 200:
        response_200 = WatchlistCollectionData.from_dict(response.json())

        return response_200

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
) -> Response[ApiError | WatchlistCollectionData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    sort: SortWatchlistProductFieldData | Unset = UNSET,
    order: GetWatchlistProductsOrder | Unset = UNSET,
    search_after: datetime.datetime | Unset = UNSET,
    size: int | Unset = 21,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | WatchlistCollectionData]:
    """List user's watchlist products

     Retrieves all products in the authenticated user's watchlist.
    Results are paginated using search-after cursor-based pagination with timestamp.
    Requires valid Cognito JWT authentication.

    Args:
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        sort (SortWatchlistProductFieldData | Unset): Fields available for sorting watchlist
            products:
            - created: Sort by when product was added to watchlist
             Example: created.
        order (GetWatchlistProductsOrder | Unset):
        search_after (datetime.datetime | Unset):
        size (int | Unset):  Default: 21.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistCollectionData]
    """

    kwargs = _get_kwargs(
        currency=currency,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
        accept_language=accept_language,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    sort: SortWatchlistProductFieldData | Unset = UNSET,
    order: GetWatchlistProductsOrder | Unset = UNSET,
    search_after: datetime.datetime | Unset = UNSET,
    size: int | Unset = 21,
    accept_language: str | Unset = UNSET,
) -> ApiError | WatchlistCollectionData | None:
    """List user's watchlist products

     Retrieves all products in the authenticated user's watchlist.
    Results are paginated using search-after cursor-based pagination with timestamp.
    Requires valid Cognito JWT authentication.

    Args:
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        sort (SortWatchlistProductFieldData | Unset): Fields available for sorting watchlist
            products:
            - created: Sort by when product was added to watchlist
             Example: created.
        order (GetWatchlistProductsOrder | Unset):
        search_after (datetime.datetime | Unset):
        size (int | Unset):  Default: 21.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistCollectionData
    """

    return sync_detailed(
        client=client,
        currency=currency,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
        accept_language=accept_language,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    sort: SortWatchlistProductFieldData | Unset = UNSET,
    order: GetWatchlistProductsOrder | Unset = UNSET,
    search_after: datetime.datetime | Unset = UNSET,
    size: int | Unset = 21,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | WatchlistCollectionData]:
    """List user's watchlist products

     Retrieves all products in the authenticated user's watchlist.
    Results are paginated using search-after cursor-based pagination with timestamp.
    Requires valid Cognito JWT authentication.

    Args:
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        sort (SortWatchlistProductFieldData | Unset): Fields available for sorting watchlist
            products:
            - created: Sort by when product was added to watchlist
             Example: created.
        order (GetWatchlistProductsOrder | Unset):
        search_after (datetime.datetime | Unset):
        size (int | Unset):  Default: 21.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | WatchlistCollectionData]
    """

    kwargs = _get_kwargs(
        currency=currency,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
        accept_language=accept_language,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    sort: SortWatchlistProductFieldData | Unset = UNSET,
    order: GetWatchlistProductsOrder | Unset = UNSET,
    search_after: datetime.datetime | Unset = UNSET,
    size: int | Unset = 21,
    accept_language: str | Unset = UNSET,
) -> ApiError | WatchlistCollectionData | None:
    """List user's watchlist products

     Retrieves all products in the authenticated user's watchlist.
    Results are paginated using search-after cursor-based pagination with timestamp.
    Requires valid Cognito JWT authentication.

    Args:
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        sort (SortWatchlistProductFieldData | Unset): Fields available for sorting watchlist
            products:
            - created: Sort by when product was added to watchlist
             Example: created.
        order (GetWatchlistProductsOrder | Unset):
        search_after (datetime.datetime | Unset):
        size (int | Unset):  Default: 21.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | WatchlistCollectionData
    """

    return (
        await asyncio_detailed(
            client=client,
            currency=currency,
            sort=sort,
            order=order,
            search_after=search_after,
            size=size,
            accept_language=accept_language,
        )
    ).parsed
