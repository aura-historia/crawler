from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.get_user_search_filters_order import GetUserSearchFiltersOrder
from ...models.sort_user_search_filter_field_data import SortUserSearchFilterFieldData
from ...models.user_search_filter_collection_data import UserSearchFilterCollectionData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    sort: SortUserSearchFilterFieldData | Unset = UNSET,
    order: GetUserSearchFiltersOrder | Unset = GetUserSearchFiltersOrder.ASC,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_sort: str | Unset = UNSET
    if not isinstance(sort, Unset):
        json_sort = sort.value

    params["sort"] = json_sort

    json_order: str | Unset = UNSET
    if not isinstance(order, Unset):
        json_order = order.value

    params["order"] = json_order

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/me/search-filters",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | UserSearchFilterCollectionData | None:
    if response.status_code == 200:
        response_200 = UserSearchFilterCollectionData.from_dict(response.json())

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
) -> Response[ApiError | UserSearchFilterCollectionData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    sort: SortUserSearchFilterFieldData | Unset = UNSET,
    order: GetUserSearchFiltersOrder | Unset = GetUserSearchFiltersOrder.ASC,
) -> Response[ApiError | UserSearchFilterCollectionData]:
    """List user search filters

     Retrieves all search filters for the authenticated user.
    Results can be optionally sorted by creation date.
    Requires valid Cognito JWT authentication.

    Args:
        sort (SortUserSearchFilterFieldData | Unset): Fields available for sorting search filters:
            - created: Sort by creation timestamp
             Example: created.
        order (GetUserSearchFiltersOrder | Unset):  Default: GetUserSearchFiltersOrder.ASC.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterCollectionData]
    """

    kwargs = _get_kwargs(
        sort=sort,
        order=order,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    sort: SortUserSearchFilterFieldData | Unset = UNSET,
    order: GetUserSearchFiltersOrder | Unset = GetUserSearchFiltersOrder.ASC,
) -> ApiError | UserSearchFilterCollectionData | None:
    """List user search filters

     Retrieves all search filters for the authenticated user.
    Results can be optionally sorted by creation date.
    Requires valid Cognito JWT authentication.

    Args:
        sort (SortUserSearchFilterFieldData | Unset): Fields available for sorting search filters:
            - created: Sort by creation timestamp
             Example: created.
        order (GetUserSearchFiltersOrder | Unset):  Default: GetUserSearchFiltersOrder.ASC.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterCollectionData
    """

    return sync_detailed(
        client=client,
        sort=sort,
        order=order,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    sort: SortUserSearchFilterFieldData | Unset = UNSET,
    order: GetUserSearchFiltersOrder | Unset = GetUserSearchFiltersOrder.ASC,
) -> Response[ApiError | UserSearchFilterCollectionData]:
    """List user search filters

     Retrieves all search filters for the authenticated user.
    Results can be optionally sorted by creation date.
    Requires valid Cognito JWT authentication.

    Args:
        sort (SortUserSearchFilterFieldData | Unset): Fields available for sorting search filters:
            - created: Sort by creation timestamp
             Example: created.
        order (GetUserSearchFiltersOrder | Unset):  Default: GetUserSearchFiltersOrder.ASC.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterCollectionData]
    """

    kwargs = _get_kwargs(
        sort=sort,
        order=order,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    sort: SortUserSearchFilterFieldData | Unset = UNSET,
    order: GetUserSearchFiltersOrder | Unset = GetUserSearchFiltersOrder.ASC,
) -> ApiError | UserSearchFilterCollectionData | None:
    """List user search filters

     Retrieves all search filters for the authenticated user.
    Results can be optionally sorted by creation date.
    Requires valid Cognito JWT authentication.

    Args:
        sort (SortUserSearchFilterFieldData | Unset): Fields available for sorting search filters:
            - created: Sort by creation timestamp
             Example: created.
        order (GetUserSearchFiltersOrder | Unset):  Default: GetUserSearchFiltersOrder.ASC.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterCollectionData
    """

    return (
        await asyncio_detailed(
            client=client,
            sort=sort,
            order=order,
        )
    ).parsed
