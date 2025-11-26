import pytest
from src.strategies.microdata import MicrodataExtractor


def wrap_product(properties):
    return {
        "microdata": [
            {
                "type": "http://schema.org/Product",
                "properties": properties,
            }
        ]
    }


def offer_with_properties(price=None, currency=None, availability=None, url=None):
    props = {}
    if price is not None:
        props["price"] = price
    if currency:
        props["priceCurrency"] = currency
    if availability is not None:
        props["availability"] = availability
    if url:
        props["url"] = url
    return {
        "type": "http://schema.org/Offer",
        "properties": props,
    }


@pytest.mark.asyncio
async def test_returns_none_when_no_product_items():
    extractor = MicrodataExtractor()
    data = {"microdata": [{"type": "http://schema.org/Thing"}]}
    assert await extractor.extract(data, "http://fallback") is None


@pytest.mark.asyncio
async def test_multiple_products_returns_first():
    extractor = MicrodataExtractor()
    data = {
        "microdata": [
            {"type": "http://schema.org/Product", "properties": {"sku": "FIRST"}},
            {"type": "http://schema.org/Product", "properties": {"sku": "SECOND"}},
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert res["shopsItemId"] == "FIRST"


@pytest.mark.asyncio
async def test_basic_price_with_currency_from_offers_properties_dict():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "A1",
            "name": "Test",
            "offers": offer_with_properties(price="10.00", currency="EUR"),
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1000
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_price_rounding_half_up_boundary_should_be_rounded():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "B2",
            "name": "Test",
            "offers": offer_with_properties(price="19.99", currency="EUR"),
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1999


@pytest.mark.asyncio
async def test_price_invalid_format_results_in_zero_and_default_currency():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "C3",
            "name": "Test",
            "offers": offer_with_properties(price="19,99", currency="EUR"),
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_price_missing_currency_sets_amount_and_keeps_default_currency():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "D4",
            "name": "Test",
            "offers": offer_with_properties(price="10.00"),
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1000
    assert res["price"]["currency"] == "UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "availability,expected",
    [
        ("", "UNKNOWN"),
        (None, "UNKNOWN"),
        ("http://schema.org/InStock", "AVAILABLE"),
        ("SomethingInStockElsewhere", "UNKNOWN"),
        ("http://schema.org/SoldOut", "SOLD"),
        ("http://schema.org/PreOrder", "LISTED"),
        ("http://schema.org/BackOrder", "LISTED"),
        ("http://schema.org/InStoreOnly", "AVAILABLE"),
        ("http://schema.org/OutOfStock", "SOLD"),
        ("http://schema.org/Discontinued", "REMOVED"),
        ("UNKNOWN_STATUS", "UNKNOWN"),
    ],
)
async def test_availability_mapping(availability, expected):
    extractor = MicrodataExtractor()
    offers = (
        offer_with_properties(availability=availability)
        if availability is not None
        else {"type": "http://schema.org/Offer", "properties": {}}
    )
    data = wrap_product(
        {
            "sku": "F6",
            "name": "Test",
            "offers": offers,
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == expected


@pytest.mark.asyncio
async def test_images_as_list_and_string_and_missing():
    extractor = MicrodataExtractor()
    data1 = wrap_product(
        {
            "sku": "G7",
            "name": "Test",
            "image": ["a.jpg", "b.jpg"],
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["images"] == ["a.jpg", "b.jpg"]

    data2 = wrap_product(
        {
            "sku": "G8",
            "name": "Test",
            "image": "c.jpg",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["images"] == ["c.jpg"]

    data3 = wrap_product(
        {
            "sku": "G9",
            "name": "Test",
        }
    )
    res3 = await extractor.extract(data3, "http://fallback")
    assert res3["images"] == []


@pytest.mark.asyncio
async def test_shops_item_id_precedence_and_defaults():
    extractor = MicrodataExtractor()
    data1 = wrap_product(
        {
            "sku": "H1",
            "productID": "PID1",
            "name": "Test",
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["shopsItemId"] == "H1"

    data2 = wrap_product(
        {
            "productID": "PID2",
            "name": "Test",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["shopsItemId"] == "PID2"

    data3 = wrap_product(
        {
            "name": "Test",
        }
    )
    res3 = await extractor.extract(data3, "http://fallback")
    assert res3["shopsItemId"] == "http://fallback"


@pytest.mark.asyncio
async def test_title_and_description_language_and_strip():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "I1",
            "name": "Title",
            "description": "  Description  ",
            "inLanguage": "en",
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["text"] == "Title"
    assert res["title"]["language"] == "en"
    assert res["description"]["text"] == "Description"
    assert res["description"]["language"] == "en"

    data2 = wrap_product(
        {
            "sku": "I2",
            "name": "Title2",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["description"]["text"] == "UNKNOWN"
    assert res2["title"]["language"] == "UNKNOWN"
    assert res2["description"]["language"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_url_priority_offers_over_product_over_fallback():
    extractor = MicrodataExtractor()

    data1 = wrap_product(
        {
            "sku": "J1",
            "name": "Test",
            "url": "http://product-url",
            "offers": offer_with_properties(url="http://offer-url"),
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["url"] == "http://offer-url"

    data2 = wrap_product(
        {
            "sku": "J2",
            "name": "Test",
            "url": "http://product-only-url",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["url"] == "http://product-only-url"

    data3 = wrap_product(
        {
            "sku": "J3",
            "name": "Test",
        }
    )
    res3 = await extractor.extract(data3, "http://fallback")
    assert res3["url"] == "http://fallback"


@pytest.mark.asyncio
async def test_offers_as_list_and_plain_dict():
    extractor = MicrodataExtractor()

    data1 = wrap_product(
        {
            "sku": "K1",
            "name": "Test",
            "offers": [
                offer_with_properties(
                    price="5.00",
                    currency="EUR",
                    availability="http://schema.org/SoldOut",
                ),
                offer_with_properties(price="10.00", currency="EUR"),
            ],
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["price"]["amount"] == 500
    assert res1["state"] == "SOLD"

    data2 = wrap_product(
        {
            "sku": "K2",
            "name": "Test",
            "offers": {"price": "7.00", "priceCurrency": "EUR"},
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["price"]["amount"] == 700
    assert res2["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_offers_invalid_type_is_handled_gracefully():
    extractor = MicrodataExtractor()
    data = wrap_product(
        {
            "sku": "K3",
            "name": "Test",
            "offers": "invalid",
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "UNKNOWN"
    assert res["state"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_nested_product_is_found():
    extractor = MicrodataExtractor()
    data = {
        "microdata": [
            {
                "type": "http://schema.org/Thing",
                "properties": {
                    "name": "Container",
                    "child": {
                        "type": "http://schema.org/Product",
                        "properties": {"sku": "NESTED", "name": "Nested"},
                    },
                },
            }
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert res["shopsItemId"] == "NESTED"
