import pytest
from src.strategies.rdfa import RdfaExtractor


def wrap_rdfa(properties):
    """Helper to wrap RDFa properties in the expected extruct format."""
    rdfa_item = {}
    for key, value in properties:
        if key not in rdfa_item:
            rdfa_item[key] = []
        rdfa_item[key].append({"@value": value})
    return {"rdfa": [rdfa_item]}


@pytest.mark.asyncio
async def test_returns_none_when_type_is_not_product():
    """Test that None is returned when type is not 'product'."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "website"),
            ("http://ogp.me/ns#title", "Not a Product"),
        ]
    )
    assert await extractor.extract(data, "http://fallback") is None


@pytest.mark.asyncio
async def test_basic_product_with_all_fields():
    """Test basic product extraction with all standard fields."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test Product"),
            ("http://ogp.me/ns#description", "A great product"),
            ("product:price:amount", "99.99"),
            ("product:price:currency", "EUR"),
            ("product:availability", "instock"),
            ("http://ogp.me/ns#url", "http://example.com/product"),
            ("http://ogp.me/ns#image", "http://example.com/image.jpg"),
            ("http://ogp.me/ns#locale", "de_DE"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")

    assert res["shopsProductId"] == "http://example.com/product"
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
    """Test that type matching is case-insensitive."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "PRODUCT"),
            ("http://ogp.me/ns#title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res is not None
    assert res["title"]["text"] == "Test"


@pytest.mark.asyncio
async def test_price_with_product_prefix():
    """Test that product:price:amount is correctly extracted."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "50.00"),
            ("product:price:currency", "USD"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 5000
    assert res["price"]["currency"] == "USD"


@pytest.mark.asyncio
async def test_price_fallback_to_product_price():
    """Test that product:price is used as fallback when product:price:amount is missing."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price", "75.50"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 7550
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_price_amount_has_priority_over_price():
    """Test that product:price:amount takes priority over product:price."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "100.00"),
            ("product:price", "50.00"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 10000


@pytest.mark.asyncio
async def test_price_with_comma_decimal_separator():
    """Test that European decimal comma format is correctly converted."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "19,99"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1999


@pytest.mark.asyncio
async def test_price_with_spaces():
    """Test that prices with spaces are correctly processed."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "1 500,50"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 150050


@pytest.mark.asyncio
async def test_price_rounding_down():
    """Test that prices are correctly rounded down."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "19.994"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 1999


@pytest.mark.asyncio
async def test_price_invalid_format_returns_zero():
    """Test that invalid price format results in 0."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "invalid"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_currency_missing_returns_unknown():
    """Test that missing currency results in 'UNKNOWN'."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("product:price:amount", "100.00"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["currency"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_availability_missing_defaults_to_unknown():
    """Test that missing availability results in 'UNKNOWN' state."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["state"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_availability_various_states():
    """Test various availability states mapping."""
    extractor = RdfaExtractor()

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
        data = wrap_rdfa(
            [
                ("http://ogp.me/ns#type", "product"),
                ("http://ogp.me/ns#title", "Test"),
                ("product:availability", availability),
            ]
        )
        res = await extractor.extract(data, "http://fallback")
        assert res["state"] == expected_state, (
            f"Failed for availability: {availability}"
        )


@pytest.mark.asyncio
async def test_multiple_images():
    """Test extraction of multiple images."""
    extractor = RdfaExtractor()
    data = {
        "rdfa": [
            {
                "http://ogp.me/ns#type": [{"@value": "product"}],
                "http://ogp.me/ns#title": [{"@value": "Test"}],
                "http://ogp.me/ns#image": [
                    {"@value": "http://example.com/image1.jpg"},
                    {"@value": "http://example.com/image2.jpg"},
                    {"@value": "http://example.com/image3.jpg"},
                ],
            }
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert len(res["images"]) == 3
    assert res["images"] == [
        "http://example.com/image1.jpg",
        "http://example.com/image2.jpg",
        "http://example.com/image3.jpg",
    ]


@pytest.mark.asyncio
async def test_no_images_returns_empty_list():
    """Test that missing images results in an empty list."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["images"] == []


@pytest.mark.asyncio
async def test_url_from_data():
    """Test that URL is correctly extracted from data."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("http://ogp.me/ns#url", "http://example.com/product/123"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["url"] == "http://example.com/product/123"
    assert res["shopsProductId"] == "http://example.com/product/123"


@pytest.mark.asyncio
async def test_url_fallback_when_missing():
    """Test that fallback URL is used when URL is missing from data."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["url"] == "http://fallback"
    assert res["shopsProductId"] == "http://fallback"


@pytest.mark.asyncio
async def test_language_extraction_from_locale():
    """Test that language is correctly extracted from locale."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("http://ogp.me/ns#locale", "en_US"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["language"] == "en"
    assert res["description"]["language"] == "en"


@pytest.mark.asyncio
async def test_language_various_locales():
    """Test language extraction from various locale formats."""
    extractor = RdfaExtractor()

    test_cases = [
        ("de_DE", "de"),
        ("fr_FR", "fr"),
        ("es_ES", "es"),
        ("en_GB", "en"),
        ("it_IT", "it"),
    ]

    for locale, expected_lang in test_cases:
        data = wrap_rdfa(
            [
                ("http://ogp.me/ns#type", "product"),
                ("http://ogp.me/ns#title", "Test"),
                ("http://ogp.me/ns#locale", locale),
            ]
        )
        res = await extractor.extract(data, "http://fallback")
        assert res["title"]["language"] == expected_lang, f"Failed for locale: {locale}"


@pytest.mark.asyncio
async def test_language_missing_returns_unknown():
    """Test that missing locale results in 'UNKNOWN' language."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["language"] == "UNKNOWN"
    assert res["description"]["language"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_empty_title_and_description():
    """Test extraction with empty title and description."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["text"] == "UNKNOWN"
    assert res["description"]["text"] == "UNKNOWN"


@pytest.mark.asyncio
async def test_multiple_rdfa_items_picks_first_product():
    """Test that the first product type item is extracted when multiple items exist."""
    extractor = RdfaExtractor()
    data = {
        "rdfa": [
            {
                "http://ogp.me/ns#type": [{"@value": "website"}],
                "http://ogp.me/ns#title": [{"@value": "Not Product"}],
            },
            {
                "http://ogp.me/ns#type": [{"@value": "product"}],
                "http://ogp.me/ns#title": [{"@value": "First Product"}],
            },
            {
                "http://ogp.me/ns#type": [{"@value": "product"}],
                "http://ogp.me/ns#title": [{"@value": "Second Product"}],
            },
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["text"] == "First Product"


@pytest.mark.asyncio
async def test_price_with_only_zeros():
    """Test that price of 0.00 is correctly handled."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Free Item"),
            ("product:price:amount", "0.00"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 0
    assert res["price"]["currency"] == "EUR"


@pytest.mark.asyncio
async def test_large_price_value():
    """Test that large price values are correctly handled."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Expensive Item"),
            ("product:price:amount", "99999.99"),
            ("product:price:currency", "EUR"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["price"]["amount"] == 9999999


@pytest.mark.asyncio
async def test_rdfa_with_missing_at_value():
    """Test handling of RDFa items without @value key."""
    extractor = RdfaExtractor()
    data = {
        "rdfa": [
            {
                "http://ogp.me/ns#type": [{"@value": "product"}],
                "http://ogp.me/ns#title": [{"@value": "Test"}],
                "http://ogp.me/ns#image": [
                    {"@value": "http://example.com/image1.jpg"},
                    {"invalid_key": "should_be_ignored"},
                    {"@value": "http://example.com/image2.jpg"},
                ],
            }
        ]
    }
    res = await extractor.extract(data, "http://fallback")
    assert len(res["images"]) == 2
    assert res["images"] == [
        "http://example.com/image1.jpg",
        "http://example.com/image2.jpg",
    ]


@pytest.mark.asyncio
async def test_locale_with_short_format():
    """Test locale extraction with short format (only language code)."""
    extractor = RdfaExtractor()
    data = wrap_rdfa(
        [
            ("http://ogp.me/ns#type", "product"),
            ("http://ogp.me/ns#title", "Test"),
            ("http://ogp.me/ns#locale", "de"),
        ]
    )
    res = await extractor.extract(data, "http://fallback")
    assert res["title"]["language"] == "de"
