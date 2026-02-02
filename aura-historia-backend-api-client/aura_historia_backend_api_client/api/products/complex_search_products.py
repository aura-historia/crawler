from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.complex_search_products_order import ComplexSearchProductsOrder
from ...models.personalized_product_search_result_data import PersonalizedProductSearchResultData
from ...models.product_search_data import ProductSearchData
from ...models.sort_product_field_data import SortProductFieldData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: ProductSearchData,
    sort: SortProductFieldData | Unset = UNSET,
    order: ComplexSearchProductsOrder | Unset = UNSET,
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
        "url": "/api/v1/products/search",
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | PersonalizedProductSearchResultData | None:
    if response.status_code == 200:
        response_200 = PersonalizedProductSearchResultData.from_dict(response.json())

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
) -> Response[ApiError | PersonalizedProductSearchResultData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: ProductSearchData,
    sort: SortProductFieldData | Unset = UNSET,
    order: ComplexSearchProductsOrder | Unset = UNSET,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> Response[ApiError | PersonalizedProductSearchResultData]:
    """Complex product search

     Performs an advanced search for products using a comprehensive search filter.
    This endpoint accepts a ProductSearchData object in the request body,
    allowing for complex filtering by multiple criteria simultaneously.
    Returns a paginated collection of products matching the search criteria.

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state for each product such as whether it's on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        sort (SortProductFieldData | Unset): Fields available for sorting:
            - score: Sort by relevance score (default, only available when searching with text query)
            - price: Sort by product price
            - originYear: Sort by product origin year
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: price.
        order (ComplexSearchProductsOrder | Unset):
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ProductSearchData): Product search configuration with query parameters and filtering
            options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | PersonalizedProductSearchResultData]
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
    client: AuthenticatedClient,
    body: ProductSearchData,
    sort: SortProductFieldData | Unset = UNSET,
    order: ComplexSearchProductsOrder | Unset = UNSET,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> ApiError | PersonalizedProductSearchResultData | None:
    """Complex product search

     Performs an advanced search for products using a comprehensive search filter.
    This endpoint accepts a ProductSearchData object in the request body,
    allowing for complex filtering by multiple criteria simultaneously.
    Returns a paginated collection of products matching the search criteria.

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state for each product such as whether it's on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        sort (SortProductFieldData | Unset): Fields available for sorting:
            - score: Sort by relevance score (default, only available when searching with text query)
            - price: Sort by product price
            - originYear: Sort by product origin year
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: price.
        order (ComplexSearchProductsOrder | Unset):
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ProductSearchData): Product search configuration with query parameters and filtering
            options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | PersonalizedProductSearchResultData
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
    client: AuthenticatedClient,
    body: ProductSearchData,
    sort: SortProductFieldData | Unset = UNSET,
    order: ComplexSearchProductsOrder | Unset = UNSET,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> Response[ApiError | PersonalizedProductSearchResultData]:
    """Complex product search

     Performs an advanced search for products using a comprehensive search filter.
    This endpoint accepts a ProductSearchData object in the request body,
    allowing for complex filtering by multiple criteria simultaneously.
    Returns a paginated collection of products matching the search criteria.

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state for each product such as whether it's on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        sort (SortProductFieldData | Unset): Fields available for sorting:
            - score: Sort by relevance score (default, only available when searching with text query)
            - price: Sort by product price
            - originYear: Sort by product origin year
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: price.
        order (ComplexSearchProductsOrder | Unset):
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ProductSearchData): Product search configuration with query parameters and filtering
            options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | PersonalizedProductSearchResultData]
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
    client: AuthenticatedClient,
    body: ProductSearchData,
    sort: SortProductFieldData | Unset = UNSET,
    order: ComplexSearchProductsOrder | Unset = UNSET,
    search_after: list[Any] | Unset = UNSET,
    size: int | Unset = 21,
) -> ApiError | PersonalizedProductSearchResultData | None:
    """Complex product search

     Performs an advanced search for products using a comprehensive search filter.
    This endpoint accepts a ProductSearchData object in the request body,
    allowing for complex filtering by multiple criteria simultaneously.
    Returns a paginated collection of products matching the search criteria.

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state for each product such as whether it's on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        sort (SortProductFieldData | Unset): Fields available for sorting:
            - score: Sort by relevance score (default, only available when searching with text query)
            - price: Sort by product price
            - originYear: Sort by product origin year
            - updated: Sort by last updated timestamp
            - created: Sort by creation timestamp
             Example: price.
        order (ComplexSearchProductsOrder | Unset):
        search_after (list[Any] | Unset):
        size (int | Unset):  Default: 21.
        body (ProductSearchData): Product search configuration with query parameters and filtering
            options

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | PersonalizedProductSearchResultData
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
