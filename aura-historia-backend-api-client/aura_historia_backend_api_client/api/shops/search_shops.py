from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.search_shops_order import SearchShopsOrder
from ...models.shop_search_data import ShopSearchData
from ...models.shop_search_result_data import ShopSearchResultData
from ...models.sort_shop_field_data import SortShopFieldData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: ShopSearchData,
    sort: SortShopFieldData | Unset = UNSET,
    order: SearchShopsOrder | Unset = SearchShopsOrder.ASC,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    json_sort: str | Unset = UNSET
    if not isinstance(sort, Unset):
        json_sort = sort.value

    params["sort"] = json_sort

    json_order: str | Unset = UNSET
    if not isinstance(order, Unset):
        json_order = order.value

    params["order"] = json_order

    json_search_after: list[Any] | Unset = UNSET
    if not isinstance(search_after, Unset):
        json_search_after = search_after

    params["searchAfter"] = json_search_after

    params["size"] = size

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/shops/search",
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | ShopSearchResultData | None:
    if response.status_code == 200:
        response_200 = ShopSearchResultData.from_dict(response.json())

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
) -> Response[ApiError | ShopSearchResultData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ShopSearchData,
    sort: SortShopFieldData | Unset = UNSET,
    order: SearchShopsOrder | Unset = SearchShopsOrder.ASC,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> Response[ApiError | ShopSearchResultData]:
    """Search shops

     Performs an advanced search for shops using comprehensive filtering criteria.
    This endpoint accepts a ShopSearchData object in the request body,
    allowing for complex filtering by shop name and date ranges.
    Returns a paginated collection of shops matching the search criteria.

    Args:
        sort (SortShopFieldData | Unset): Fields available for sorting shops:
            - score: Sort by relevance score (default, only available when searching with text query)
            - name: Sort by shop name
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: name.
        order (SearchShopsOrder | Unset):  Default: SearchShopsOrder.ASC.
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ShopSearchData): Search filter configuration for shops with query parameters and
            filtering options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | ShopSearchResultData]
    """

    kwargs = _get_kwargs(
        body=body,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: ShopSearchData,
    sort: SortShopFieldData | Unset = UNSET,
    order: SearchShopsOrder | Unset = SearchShopsOrder.ASC,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> ApiError | ShopSearchResultData | None:
    """Search shops

     Performs an advanced search for shops using comprehensive filtering criteria.
    This endpoint accepts a ShopSearchData object in the request body,
    allowing for complex filtering by shop name and date ranges.
    Returns a paginated collection of shops matching the search criteria.

    Args:
        sort (SortShopFieldData | Unset): Fields available for sorting shops:
            - score: Sort by relevance score (default, only available when searching with text query)
            - name: Sort by shop name
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: name.
        order (SearchShopsOrder | Unset):  Default: SearchShopsOrder.ASC.
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ShopSearchData): Search filter configuration for shops with query parameters and
            filtering options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | ShopSearchResultData
    """

    return sync_detailed(
        client=client,
        body=body,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: ShopSearchData,
    sort: SortShopFieldData | Unset = UNSET,
    order: SearchShopsOrder | Unset = SearchShopsOrder.ASC,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> Response[ApiError | ShopSearchResultData]:
    """Search shops

     Performs an advanced search for shops using comprehensive filtering criteria.
    This endpoint accepts a ShopSearchData object in the request body,
    allowing for complex filtering by shop name and date ranges.
    Returns a paginated collection of shops matching the search criteria.

    Args:
        sort (SortShopFieldData | Unset): Fields available for sorting shops:
            - score: Sort by relevance score (default, only available when searching with text query)
            - name: Sort by shop name
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: name.
        order (SearchShopsOrder | Unset):  Default: SearchShopsOrder.ASC.
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ShopSearchData): Search filter configuration for shops with query parameters and
            filtering options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | ShopSearchResultData]
    """

    kwargs = _get_kwargs(
        body=body,
        sort=sort,
        order=order,
        search_after=search_after,
        size=size,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: ShopSearchData,
    sort: SortShopFieldData | Unset = UNSET,
    order: SearchShopsOrder | Unset = SearchShopsOrder.ASC,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> ApiError | ShopSearchResultData | None:
    """Search shops

     Performs an advanced search for shops using comprehensive filtering criteria.
    This endpoint accepts a ShopSearchData object in the request body,
    allowing for complex filtering by shop name and date ranges.
    Returns a paginated collection of shops matching the search criteria.

    Args:
        sort (SortShopFieldData | Unset): Fields available for sorting shops:
            - score: Sort by relevance score (default, only available when searching with text query)
            - name: Sort by shop name
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: name.
        order (SearchShopsOrder | Unset):  Default: SearchShopsOrder.ASC.
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ShopSearchData): Search filter configuration for shops with query parameters and
            filtering options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | ShopSearchResultData
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            sort=sort,
            order=order,
            search_after=search_after,
            size=size,
        )
    ).parsed
