import pytest
from src.strategies.opengraph import OpenGraphExtractor


def wrap_opengraph(properties):
    """Helper to wrap OpenGraph properties in the expected extruct format."""
    return {"opengraph": [{"properties": properties}]}


@pytest.mark.asyncio
async def test_returns_none_when_no_opengraph_data():
    """Test that None is returned when opengraph data is missing."""
    extractor = OpenGraphExtractor()
    data = {"opengraph": {}}
    assert await extractor.extract(data, "http://fallback") is None


@pytest.mark.asyncio
async def test_returns_none_when_type_is_not_product():
    """Test that None is returned when og:type is not 'product'."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "website"),
            ("og:title", "Not a Product"),
        ]
    )
    assert await extractor.extract(data, "http://fallback") is None


@pytest.mark.asyncio
async def test_basic_product_with_all_fields():
    """Test basic product extraction with all standard fields."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test Product"),
            ("og:description", "A great product"),
            ("product:price:amount", "99.99"),
            ("product:price:currency", "EUR"),
            ("product:availability", "instock"),
            ("og:url", "http://example.com/product"),
            ("og:image", "http://example.com/image.jpg"),
            ("og:locale", "de_DE"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")

    assert res["shopsItemId"] == "http://fallback"
    assert res["title"]["text"] == "Test Product"
    assert res["title"]["language"] == "de"
    assert res["description"]["text"] == "A great product"
    assert res["description"]["language"] == "de"
    assert res["price"]["amount"] == 9999
    assert res["price"]["currency"] == "EUR"
    assert res["state"] == "AVAILABLE"
    assert res["url"] == "http://example.com/product"
    assert res["images"] == ["http://example.com/image.jpg"]


@pytest.mark.asyncio
async def test_product_type_case_insensitive():
    """Test that og:type matching is case-insensitive."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "PRODUCT"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res is not None
    assert res["title"]["text"] == "Test"


@pytest.mark.asyncio
async def test_price_with_og_prefix_fallback():
    """Test that og:price:amount is used as fallback when product:price:amount is missing."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("og:price:amount", "50.00"),
            ("product:price:currency", "USD"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 5000
    assert res["price"]["currency"] == "USD"


@pytest.mark.asyncio
async def test_price_product_prefix_has_priority():
    """Test that product:price:amount takes priority over og:price:amount."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "100.00"),
            ("og:price:amount", "50.00"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 10000


@pytest.mark.asyncio
async def test_price_with_comma_decimal_separator():
    """Test that European decimal comma format is correctly converted."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "19,99"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1999


@pytest.mark.asyncio
async def test_price_with_spaces():
    """Test that prices with spaces are correctly processed."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "1 500,50"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 150050


@pytest.mark.asyncio
async def test_price_rounding_half_up():
    """Test that prices are correctly rounded using banker's rounding."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "19.995"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 2000


@pytest.mark.asyncio
async def test_price_invalid_format_returns_unknown():
    """Test that invalid price format results in 'UNKNOWN'."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "invalid"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0


@pytest.mark.asyncio
async def test_price_missing_returns_unknown():
    """Test that missing price results in 'UNKNOWN'."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0


@pytest.mark.asyncio
async def test_currency_missing_returns_unknown():
    """Test that missing currency results in 'UNKNOWN'."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:price:amount", "100.00"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["currency"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_availability_product_prefix():
    """Test availability extraction with product: prefix."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:availability", "instock"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == "AVAILABLE"


@pytest.mark.asyncio
async def test_availability_og_prefix_fallback():
    """Test availability extraction with og: prefix as fallback."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("og:availability", "outofstock"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == "SOLD"


@pytest.mark.asyncio
async def test_availability_missing_defaults_to_unknown():
    """Test that missing availability results in 'UNKNOWN' state."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_availability_various_states():
    """Test various availability states mapping."""
    extractor = OpenGraphExtractor()

    test_cases = [
        ("instock", "AVAILABLE"),
        ("InStock", "AVAILABLE"),
        ("preorder", "LISTED"),
        ("backorder", "LISTED"),
        ("soldout", "SOLD"),
        ("outofstock", "SOLD"),
        ("discontinued", "REMOVED"),
        ("reserved", "RESERVED"),
    ]

    for availability, expected_state in test_cases:
        data = wrap_opengraph(
            [
                ("og:type", "product"),
                ("og:title", "Test"),
                ("product:availability", availability),
            ]
        )
        res = await extractor.extract(data, "http://fallback")
        assert res["state"] == expected_state, (
            f"Failed for availability: {availability}"
        )


@pytest.mark.asyncio
async def test_availability_with_schema_url():
    """Test availability with full schema.org URL."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("product:availability", "https://schema.org/InStock"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == "AVAILABLE"


@pytest.mark.asyncio
async def test_locale_extraction_and_language():
    """Test locale extraction and language derivation."""
    extractor = OpenGraphExtractor()

    test_cases = [
        ("en_US", "en"),
        ("de_DE", "de"),
        ("fr_FR", "fr"),
        ("es_ES", "es"),
    ]

    for locale, expected_lang in test_cases:
        data = wrap_opengraph(
            [
                ("og:type", "product"),
                ("og:title", "Test"),
                ("og:locale", locale),
            ]
        )
        res = await extractor.extract(data, "http://fallback")
        assert res["title"]["language"] == expected_lang
        assert res["description"]["language"] == expected_lang


@pytest.mark.asyncio
async def test_locale_missing_defaults_to_unknown():
    """Test that missing locale results in 'UNKNOWN' language."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["language"] == "UNKNOWN"
    assert res["description"]["language"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_url_from_og_url():
    """Test that og:url is used for the url field."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("og:url", "http://example.com/canonical"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["url"] == "http://example.com/canonical"


@pytest.mark.asyncio
async def test_url_fallback_to_provided_url():
    """Test that provided URL is used as fallback when og:url is missing."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback-url")
    assert res["url"] == "http://fallback-url"


@pytest.mark.asyncio
async def test_image_single():
    """Test single image extraction."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            ("og:image", "http://example.com/image.jpg"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["images"] == ["http://example.com/image.jpg"]


@pytest.mark.asyncio
async def test_image_missing_returns_unknown():
    """Test that missing image results in 'UNKNOWN'."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["images"] == []


@pytest.mark.asyncio
async def test_image_as_list():
    """Test that image as list returns only first image."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
            (
                "og:image",
                ["http://example.com/image1.jpg", "http://example.com/image2.jpg"],
            ),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["images"] == ["http://example.com/image1.jpg"]


@pytest.mark.asyncio
async def test_title_and_description_empty_strings():
    """Test that missing title and description default to empty strings."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["text"] == "UNKNOWN"
    assert res["description"]["text"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_properties_dict_format():
    """Test OpenGraph data with properties as dict inside dict (alternative format)."""
    extractor = OpenGraphExtractor()
    data = {
        "opengraph": {
            "properties": [
                ("og:type", "product"),
                ("og:title", "Dict Format Test"),
                ("product:price:amount", "25.00"),
                ("product:price:currency", "USD"),
            ]
        }
    }
    res = await extractor.extract(data, "http://fallback")
    assert res is not None
    assert res["title"]["text"] == "Dict Format Test"
    assert res["price"]["amount"] == 2500


@pytest.mark.asyncio
async def test_list_with_first_item_properties():
    """Test OpenGraph data as list with first item containing properties."""
    extractor = OpenGraphExtractor()
    data = {
        "opengraph": [
            {
                "properties": [
                    ("og:type", "product"),
                    ("og:title", "List Format Test"),
                ]
            }
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert res is not None
    assert res["title"]["text"] == "List Format Test"


@pytest.mark.asyncio
async def test_list_with_dict_without_properties_key():
    """Test OpenGraph data as list with dict that doesn't have 'properties' key."""
    extractor = OpenGraphExtractor()
    data = {
        "opengraph": [
            {
                "og:type": "product",
                "og:title": "Direct Dict Test",
            }
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert res is not None
    assert res["title"]["text"] == "Direct Dict Test"


@pytest.mark.asyncio
async def test_empty_opengraph_list():
    """Test that empty opengraph list returns None."""
    extractor = OpenGraphExtractor()
    data = {"opengraph": []}
    res = await extractor.extract(data, "http://fallback")
    assert res is None


@pytest.mark.asyncio
async def test_price_zero():
    """Test that price of 0 is correctly handled."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Free Item"),
            ("product:price:amount", "0"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0


@pytest.mark.asyncio
async def test_price_very_large():
    """Test that very large prices are correctly handled."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Expensive Item"),
            ("product:price:amount", "999999.99"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 99999999


@pytest.mark.asyncio
async def test_shops_item_id_uses_provided_url():
    """Test that shopsItemId is set to the provided URL parameter."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", "product"),
            ("og:title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://unique-url-12345")
    assert res["shopsItemId"] == "http://unique-url-12345"


@pytest.mark.asyncio
async def test_all_fields_as_lists():
    """Test that all fields handle list values correctly (taking first item)."""
    extractor = OpenGraphExtractor()
    data = wrap_opengraph(
        [
            ("og:type", ["product", "other"]),
            ("og:title", ["First Title", "Second Title"]),
            ("og:description", ["First Desc", "Second Desc"]),
            ("product:price:amount", ["100.00", "200.00"]),
            ("product:price:currency", ["EUR", "USD"]),
            ("og:url", ["http://first.com", "http://second.com"]),
            ("og:image", ["http://image1.jpg", "http://image2.jpg"]),
            ("og:locale", ["de_DE", "en_US"]),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["text"] == "First Title"
    assert res["description"]["text"] == "First Desc"
    assert res["price"]["amount"] == 10000
    assert res["price"]["currency"] == "EUR"
    assert res["url"] == "http://first.com"
    assert res["images"] == ["http://image1.jpg"]
    assert res["title"]["language"] == "de"
