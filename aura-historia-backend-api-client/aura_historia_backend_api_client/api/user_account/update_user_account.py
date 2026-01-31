from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.get_user_account_data import GetUserAccountData
from ...models.patch_user_account_data import PatchUserAccountData
from ...types import Response


def _get_kwargs(
    *,
    body: PatchUserAccountData,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "patch",
        "url": "/api/v1/me/account",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | GetUserAccountData | None:
    if response.status_code == 200:
        response_200 = GetUserAccountData.from_dict(response.json())

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
) -> Response[ApiError | GetUserAccountData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: PatchUserAccountData,
) -> Response[ApiError | GetUserAccountData]:
    """Update user account data

     Updates the authenticated user's account information.
    All fields in the request body are optional - only provided fields will be updated.
    Returns the updated user account data.
    Requires valid Cognito JWT authentication.

    Args:
        body (PatchUserAccountData): Partial user account update data.
            All fields are optional - only provided fields will be updated.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetUserAccountData]
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
    body: PatchUserAccountData,
) -> ApiError | GetUserAccountData | None:
    """Update user account data

     Updates the authenticated user's account information.
    All fields in the request body are optional - only provided fields will be updated.
    Returns the updated user account data.
    Requires valid Cognito JWT authentication.

    Args:
        body (PatchUserAccountData): Partial user account update data.
            All fields are optional - only provided fields will be updated.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetUserAccountData
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: PatchUserAccountData,
) -> Response[ApiError | GetUserAccountData]:
    """Update user account data

     Updates the authenticated user's account information.
    All fields in the request body are optional - only provided fields will be updated.
    Returns the updated user account data.
    Requires valid Cognito JWT authentication.

    Args:
        body (PatchUserAccountData): Partial user account update data.
            All fields are optional - only provided fields will be updated.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | GetUserAccountData]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: PatchUserAccountData,
) -> ApiError | GetUserAccountData | None:
    """Update user account data

     Updates the authenticated user's account information.
    All fields in the request body are optional - only provided fields will be updated.
    Returns the updated user account data.
    Requires valid Cognito JWT authentication.

    Args:
        body (PatchUserAccountData): Partial user account update data.
            All fields are optional - only provided fields will be updated.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | GetUserAccountData
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
