from http import HTTPStatus
from typing import Any
from urllib.parse import quote
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.currency_data import CurrencyData
from ...models.get_product_event_data import GetProductEventData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    shop_id: UUID,
    shops_product_id: str,
    *,
    currency: CurrencyData | Unset = UNSET,
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

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/shops/{shop_id}/products/{shops_product_id}/history".format(
            shop_id=quote(str(shop_id), safe=""),
            shops_product_id=quote(str(shops_product_id), safe=""),
        ),
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | list[GetProductEventData] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = GetProductEventData.from_dict(response_200_item_data)

            response_200.append(response_200_item)

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

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ApiError | list[GetProductEventData]]:
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
    client: AuthenticatedClient | Client,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | list[GetProductEventData]]:
    """Get product event history

     Retrieves the event history for a specific product by its shop ID and shop's product ID.
    Returns an array of events representing state changes, price changes, and other significant
    product lifecycle events, ordered chronologically.

    Returns localized content based on Accept-Language header and currency preferences for
    price information in the event payloads.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str):  Example: 6ba7b810.
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | list[GetProductEventData]]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        currency=currency,
        accept_language=accept_language,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient | Client,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> ApiError | list[GetProductEventData] | None:
    """Get product event history

     Retrieves the event history for a specific product by its shop ID and shop's product ID.
    Returns an array of events representing state changes, price changes, and other significant
    product lifecycle events, ordered chronologically.

    Returns localized content based on Accept-Language header and currency preferences for
    price information in the event payloads.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str):  Example: 6ba7b810.
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | list[GetProductEventData]
    """

    return sync_detailed(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        client=client,
        currency=currency,
        accept_language=accept_language,
    ).parsed


async def asyncio_detailed(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient | Client,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | list[GetProductEventData]]:
    """Get product event history

     Retrieves the event history for a specific product by its shop ID and shop's product ID.
    Returns an array of events representing state changes, price changes, and other significant
    product lifecycle events, ordered chronologically.

    Returns localized content based on Accept-Language header and currency preferences for
    price information in the event payloads.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str):  Example: 6ba7b810.
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ApiError | list[GetProductEventData]]
    """

    kwargs = _get_kwargs(
        shop_id=shop_id,
        shops_product_id=shops_product_id,
        currency=currency,
        accept_language=accept_language,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shop_id: UUID,
    shops_product_id: str,
    *,
    client: AuthenticatedClient | Client,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> ApiError | list[GetProductEventData] | None:
    """Get product event history

     Retrieves the event history for a specific product by its shop ID and shop's product ID.
    Returns an array of events representing state changes, price changes, and other significant
    product lifecycle events, ordered chronologically.

    Returns localized content based on Accept-Language header and currency preferences for
    price information in the event payloads.

    Args:
        shop_id (UUID):  Example: 550e8400-e29b-41d4-a716-446655440000.
        shops_product_id (str):  Example: 6ba7b810.
        currency (CurrencyData | Unset): Supported currencies (ISO 4217 codes):
            - EUR: Euro
            - GBP: British Pound
            - USD: US Dollar
            - AUD: Australian Dollar
            - CAD: Canadian Dollar
            - NZD: New Zealand Dollar
             Example: EUR.
        accept_language (str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ApiError | list[GetProductEventData]
    """

    return (
        await asyncio_detailed(
            shop_id=shop_id,
            shops_product_id=shops_product_id,
            client=client,
            currency=currency,
            accept_language=accept_language,
        )
    ).parsed
