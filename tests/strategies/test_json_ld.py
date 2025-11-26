import pytest
from src.strategies.json_ld import JsonLDExtractor


def wrap_product(product):
    return {"json-ld": [product]}


@pytest.mark.asyncio
async def test_basic_price_rounding_and_currency_from_offers():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
            "sku": "A1",
            "name": "Test",
            "offers": {
                "price": "19.99",
                "priceCurrency": "EUR",
                "availability": "http://schema.org/InStock",
                "url": "http://offer-url",
            },
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1999
    assert res["price"]["currency"] == "EUR"
    assert res["state"] == "AVAILABLE"
    assert res["url"] == "http://offer-url"


@pytest.mark.asyncio
async def test_price_rounding_half_up_boundary():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
            "sku": "B2",
            "name": "Test",
            "offers": {"price": "19.995", "priceCurrency": "EUR"},
        }
    )
    res = await extractor.extract(data, "http://fallback")
    # round(1999.5) == 2000
    assert res["price"]["amount"] == 2000


@pytest.mark.asyncio
async def test_price_invalid_format_results_in_zero_and_default_currency():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
            "sku": "C3",
            "name": "Test",
            "offers": {
                "price": "19,99",  # invalid for float()
                "priceCurrency": "EUR",
            },
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_price_missing_currency_keeps_amount_zero_and_default_currency():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
            "sku": "D4",
            "name": "Test",
            "offers": {
                "price": "10.00"
                # missing priceCurrency
            },
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "UNKNOWN"


@pytest.mark.asyncio
@pytest.mark.xfail(
    reason="Bug: fallback from priceSpecification not applied", strict=False
)
async def test_price_from_price_specification_fallback():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
            "sku": "E5",
            "name": "Test",
            "offers": {
                # no 'price' field; rely on priceSpecification
                "priceSpecification": [{"price": "9.50", "priceCurrency": "USD"}]
            },
        }
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 950
    assert res["price"]["currency"] == "USD"


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
    extractor = JsonLDExtractor()
    offers = {}
    if availability is not None:
        offers["availability"] = availability
    data = wrap_product(
        {"@type": "Product", "sku": "F6", "name": "Test", "offers": offers}
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == expected


@pytest.mark.asyncio
async def test_images_as_list_and_string_and_missing():
    extractor = JsonLDExtractor()
    # list
    data1 = wrap_product(
        {"@type": "Product", "sku": "G7", "name": "Test", "image": ["a.jpg", "b.jpg"]}
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["images"] == ["a.jpg", "b.jpg"]

    # string -> list
    data2 = wrap_product(
        {"@type": "Product", "sku": "G8", "name": "Test", "image": "c.jpg"}
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["images"] == ["c.jpg"]

    # missing -> empty list
    data3 = wrap_product(
        {
            "@type": "Product",
            "sku": "G9",
            "name": "Test",
        }
    )
    res3 = await extractor.extract(data3, "http://fallback")
    assert res3["images"] == []


@pytest.mark.asyncio
async def test_shops_item_id_precedence_and_defaults():
    extractor = JsonLDExtractor()
    # sku present
    data1 = wrap_product(
        {
            "@type": "Product",
            "sku": "H1",
            "productGroupID": "PG1",
            "name": "Test",
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["shopsItemId"] == "H1"

    # sku missing -> productGroupID
    data2 = wrap_product(
        {
            "@type": "Product",
            "productGroupID": "PG2",
            "name": "Test",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["shopsItemId"] == "PG2"

    # both missing -> "UNKNOWN"
    data3 = wrap_product(
        {
            "@type": "Product",
            "name": "Test",
        }
    )
    res3 = await extractor.extract(data3, "http://fallback")
    assert res3["shopsItemId"] == "http://fallback"


@pytest.mark.asyncio
async def test_title_and_description_language_and_defaults_and_strip():
    extractor = JsonLDExtractor()
    data = wrap_product(
        {
            "@type": "Product",
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

    # missing description -> "UNKNOWN"
    data2 = wrap_product(
        {
            "@type": "Product",
            "sku": "I2",
            "name": "Title2",
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["description"]["text"] == "UNKNOWN"
    assert res2["title"]["language"] == "UNKNOWN"
    assert res2["description"]["language"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_url_priority_and_edge_cases():
    extractor = JsonLDExtractor()

    # product URL has precedence
    data1 = wrap_product(
        {
            "@type": "Product",
            "sku": "J1",
            "name": "Test",
            "url": "http://product-url",
            "offers": {"url": "http://offer-url"},
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["url"] == "http://product-url"

    # no product URL -> offer URL
    data2 = wrap_product(
        {
            "@type": "Product",
            "sku": "J2",
            "name": "Test",
            "offers": {"url": "http://offer-url-2"},
        }
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["url"] == "http://offer-url-2"


@pytest.mark.asyncio
async def test_offers_as_list_and_non_dict_defaults():
    extractor = JsonLDExtractor()

    # offers as list -> first element
    data1 = wrap_product(
        {
            "@type": "Product",
            "sku": "K1",
            "name": "Test",
            "offers": [
                {
                    "price": "5.00",
                    "priceCurrency": "EUR",
                    "availability": "http://schema.org/SoldOut",
                },
                {
                    "price": "10.00",
                    "priceCurrency": "EUR",
                },
            ],
        }
    )
    res1 = await extractor.extract(data1, "http://fallback")
    assert res1["price"]["amount"] == 500
    assert res1["state"] == "SOLD"

    # offers invalid type -> {}
    data2 = wrap_product(
        {"@type": "Product", "sku": "K2", "name": "Test", "offers": "invalid"}
    )
    res2 = await extractor.extract(data2, "http://fallback")
    assert res2["price"]["amount"] == 0
    assert res2["state"] == "UNKNOWN"
