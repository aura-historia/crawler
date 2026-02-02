from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.patch_user_search_filter_data import PatchUserSearchFilterData
from ...models.user_search_filter_data import UserSearchFilterData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    user_search_filter_id: UUID,
    *,
    body: PatchUserSearchFilterData | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/me/search-filters/{user_search_filter_id}".format(
            user_search_filter_id=quote(str(user_search_filter_id), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | UserSearchFilterData | None:
    if response.status_code == 200:
        response_200 = UserSearchFilterData.from_dict(response.json())

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
) -> Response[ApiError | UserSearchFilterData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    user_search_filter_id: UUID,
    *,
    client: AuthenticatedClient,
    body: PatchUserSearchFilterData | Unset = UNSET,
) -> Response[ApiError | UserSearchFilterData]:
    """Update a search filter

     Updates a specific search filter by its ID for the authenticated user.
    Allows partial updates - only provided fields will be modified.
    If no fields are provided in the request body, returns the existing search filter unchanged.
    Requires valid Cognito JWT authentication.

    Args:
        user_search_filter_id (UUID):
        body (PatchUserSearchFilterData | Unset): Partial search filter update data.
            All fields are optional and only provided fields will be updated.
            Can update the search filter name and/or the search filter criteria.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterData]
    """

    kwargs = _get_kwargs(
        user_search_filter_id=user_search_filter_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    user_search_filter_id: UUID,
    *,
    client: AuthenticatedClient,
    body: PatchUserSearchFilterData | Unset = UNSET,
) -> ApiError | UserSearchFilterData | None:
    """Update a search filter

     Updates a specific search filter by its ID for the authenticated user.
    Allows partial updates - only provided fields will be modified.
    If no fields are provided in the request body, returns the existing search filter unchanged.
    Requires valid Cognito JWT authentication.

    Args:
        user_search_filter_id (UUID):
        body (PatchUserSearchFilterData | Unset): Partial search filter update data.
            All fields are optional and only provided fields will be updated.
            Can update the search filter name and/or the search filter criteria.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterData
    """

    return sync_detailed(
        user_search_filter_id=user_search_filter_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    user_search_filter_id: UUID,
    *,
    client: AuthenticatedClient,
    body: PatchUserSearchFilterData | Unset = UNSET,
) -> Response[ApiError | UserSearchFilterData]:
    """Update a search filter

     Updates a specific search filter by its ID for the authenticated user.
    Allows partial updates - only provided fields will be modified.
    If no fields are provided in the request body, returns the existing search filter unchanged.
    Requires valid Cognito JWT authentication.

    Args:
        user_search_filter_id (UUID):
        body (PatchUserSearchFilterData | Unset): Partial search filter update data.
            All fields are optional and only provided fields will be updated.
            Can update the search filter name and/or the search filter criteria.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | UserSearchFilterData]
    """

    kwargs = _get_kwargs(
        user_search_filter_id=user_search_filter_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    user_search_filter_id: UUID,
    *,
    client: AuthenticatedClient,
    body: PatchUserSearchFilterData | Unset = UNSET,
) -> ApiError | UserSearchFilterData | None:
    """Update a search filter

     Updates a specific search filter by its ID for the authenticated user.
    Allows partial updates - only provided fields will be modified.
    If no fields are provided in the request body, returns the existing search filter unchanged.
    Requires valid Cognito JWT authentication.

    Args:
        user_search_filter_id (UUID):
        body (PatchUserSearchFilterData | Unset): Partial search filter update data.
            All fields are optional and only provided fields will be updated.
            Can update the search filter name and/or the search filter criteria.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | UserSearchFilterData
    """

    return (
        await asyncio_detailed(
            user_search_filter_id=user_search_filter_id,
            client=client,
            body=body,
        )
    ).parsed
