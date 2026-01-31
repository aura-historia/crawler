from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_error import ApiError
from ...models.currency_data import CurrencyData
from ...models.personalized_get_product_data import PersonalizedGetProductData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    shop_slug_id: str,
    product_slug_id: str,
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
        "url": "/api/v1/by-slug/shops/{shop_slug_id}/products/{product_slug_id}".format(
            shop_slug_id=quote(str(shop_slug_id), safe=""),
            product_slug_id=quote(str(product_slug_id), safe=""),
        ),
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ApiError | PersonalizedGetProductData | None:
    if response.status_code == 200:
        response_200 = PersonalizedGetProductData.from_dict(response.json())

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
) -> Response[ApiError | PersonalizedGetProductData]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    shop_slug_id: str,
    product_slug_id: str,
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | PersonalizedGetProductData]:
    r"""Get a single product by slug

     Retrieves a single product by its shop slug ID and product slug ID.
    Returns localized content based on Accept-Language header and currency preferences.

    **Human-Readable Identifiers**: This endpoint uses slug-based identifiers which are human-readable
    kebab-case strings. Shop slugs are derived from the shop name (e.g., \"tech-store-premium\"),
    while product slugs combine the product title with a unique 6-character hexadecimal suffix
    (e.g., \"amazing-product-fa87c4\").

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state such as whether the product is on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        shop_slug_id (str):  Example: tech-store-premium.
        product_slug_id (str):  Example: amazing-product-fa87c4.
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
        Response[ApiError | PersonalizedGetProductData]
    """

    kwargs = _get_kwargs(
        shop_slug_id=shop_slug_id,
        product_slug_id=product_slug_id,
        currency=currency,
        accept_language=accept_language,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    shop_slug_id: str,
    product_slug_id: str,
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> ApiError | PersonalizedGetProductData | None:
    r"""Get a single product by slug

     Retrieves a single product by its shop slug ID and product slug ID.
    Returns localized content based on Accept-Language header and currency preferences.

    **Human-Readable Identifiers**: This endpoint uses slug-based identifiers which are human-readable
    kebab-case strings. Shop slugs are derived from the shop name (e.g., \"tech-store-premium\"),
    while product slugs combine the product title with a unique 6-character hexadecimal suffix
    (e.g., \"amazing-product-fa87c4\").

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state such as whether the product is on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        shop_slug_id (str):  Example: tech-store-premium.
        product_slug_id (str):  Example: amazing-product-fa87c4.
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
        ApiError | PersonalizedGetProductData
    """

    return sync_detailed(
        shop_slug_id=shop_slug_id,
        product_slug_id=product_slug_id,
        client=client,
        currency=currency,
        accept_language=accept_language,
    ).parsed


async def asyncio_detailed(
    shop_slug_id: str,
    product_slug_id: str,
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> Response[ApiError | PersonalizedGetProductData]:
    r"""Get a single product by slug

     Retrieves a single product by its shop slug ID and product slug ID.
    Returns localized content based on Accept-Language header and currency preferences.

    **Human-Readable Identifiers**: This endpoint uses slug-based identifiers which are human-readable
    kebab-case strings. Shop slugs are derived from the shop name (e.g., \"tech-store-premium\"),
    while product slugs combine the product title with a unique 6-character hexadecimal suffix
    (e.g., \"amazing-product-fa87c4\").

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state such as whether the product is on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        shop_slug_id (str):  Example: tech-store-premium.
        product_slug_id (str):  Example: amazing-product-fa87c4.
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
        Response[ApiError | PersonalizedGetProductData]
    """

    kwargs = _get_kwargs(
        shop_slug_id=shop_slug_id,
        product_slug_id=product_slug_id,
        currency=currency,
        accept_language=accept_language,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    shop_slug_id: str,
    product_slug_id: str,
    *,
    client: AuthenticatedClient,
    currency: CurrencyData | Unset = UNSET,
    accept_language: str | Unset = UNSET,
) -> ApiError | PersonalizedGetProductData | None:
    r"""Get a single product by slug

     Retrieves a single product by its shop slug ID and product slug ID.
    Returns localized content based on Accept-Language header and currency preferences.

    **Human-Readable Identifiers**: This endpoint uses slug-based identifiers which are human-readable
    kebab-case strings. Shop slugs are derived from the shop name (e.g., \"tech-store-premium\"),
    while product slugs combine the product title with a unique 6-character hexadecimal suffix
    (e.g., \"amazing-product-fa87c4\").

    **Personalization**: When authenticated (via optional Authorization header), the response includes
    user-specific state such as whether the product is on the user's watchlist and notification
    preferences.
    Anonymous requests receive product data without user state.

    Args:
        shop_slug_id (str):  Example: tech-store-premium.
        product_slug_id (str):  Example: amazing-product-fa87c4.
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
        ApiError | PersonalizedGetProductData
    """

    return (
        await asyncio_detailed(
            shop_slug_id=shop_slug_id,
            product_slug_id=product_slug_id,
            client=client,
            currency=currency,
            accept_language=accept_language,
        )
    ).parsed
