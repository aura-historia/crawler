import pytest
from src.core.utils.standards_extractor import (
    extract_standard,
    is_valid_product,
    merge_products,
)


@pytest.mark.asyncio
async def test_extract_standard_with_generic_data(monkeypatch):
    """Test the extract_standard function with a realistic mix of structured data syntaxes."""

    data = {
        "microdata": [
            {
                "type": "https://schema.org/Product",
                "properties": {
                    "name": "Collectible Medal Set 1940s",
                    "image": [
                        "https://example-store.test/shop/_3780602.JPG?ts=1759398792",
                        "https://example-store.test/shop/_3780601.JPG?ts=1759398792",
                    ],
                    "releaseDate": "2025-10-07",
                    "offers": {
                        "type": "https://schema.org/Offer",
                        "properties": {
                            "url": "https://example-store.test/shop/collectible-medal-set-1940s/",
                            "priceCurrency": "EUR",
                            "price": "130",
                        },
                    },
                    "sku": "M78123",
                },
            }
        ],
        "json-ld": [
            {
                "@context": "https://schema.org",
                "@graph": [
                    {
                        "@type": "WebSite",
                        "@id": "https://example-store.test/#/schema/WebSite",
                        "url": "https://example-store.test/",
                        "name": "Example Collectibles",
                        "description": "Shop for vintage collectibles.",
                        "inLanguage": "de",
                    },
                    {
                        "@type": "WebPage",
                        "@id": "https://example-store.test/shop/collectible-medal-set-1940s/",
                        "url": "https://example-store.test/shop/collectible-medal-set-1940s/",
                        "name": "Collectible Medal Set 1940s - Example Collectibles",
                        "description": "Well-preserved set, lightly used.",
                        "inLanguage": "de",
                        "datePublished": "2025-10-01T11:26:56+00:00",
                    },
                    {
                        "@type": "Product",
                        "@id": "https://example-store.test/shop/collectible-medal-set-1940s/#product",
                        "name": "Collectible Medal Set 1940s",
                        "url": "https://example-store.test/shop/collectible-medal-set-1940s/",
                        "description": "Well-preserved set, original case included.",
                        "image": "https://example-store.test/media/medal-set.jpg",
                        "sku": "A1023",
                        "offers": [
                            {
                                "@type": "Offer",
                                "priceSpecification": [
                                    {
                                        "@type": "UnitPriceSpecification",
                                        "price": "275.00",
                                        "priceCurrency": "EUR",
                                        "valueAddedTaxIncluded": True,
                                        "validThrough": "2026-12-31",
                                    }
                                ],
                                "priceValidUntil": "2026-12-31",
                                "availability": "http://schema.org/InStock",
                            }
                        ],
                    },
                ],
            }
        ],
        "opengraph": [
            {
                "namespace": {
                    "og": "http://ogp.me/ns#",
                    "article": "http://ogp.me/ns/article#",
                },
                "properties": [
                    ("og:type", "product"),
                    ("og:site_name", "Example Collectibles"),
                    ("og:title", "Collectible Medal Set 1940s"),
                    ("og:description", "Well-preserved set, original case included."),
                    (
                        "og:url",
                        "https://example-store.test/shop/collectible-medal-set-1940s/",
                    ),
                    ("og:image", "https://example-store.test/media/medal-set.jpg"),
                    ("article:published_time", "2025-10-01T11:26:56+00:00"),
                ],
            }
        ],
        "rdfa": [
            {
                "@id": "https://example-store.test/shop/collectible-medal-set-1940s/",
                "http://ogp.me/ns#type": [{"@value": "product"}],
                "http://www.w3.org/1999/xhtml/vocab#role": [
                    {"@id": "http://www.w3.org/1999/xhtml/vocab#none"}
                ],
                "http://ogp.me/ns#description": [
                    {"@value": "Well-preserved set, original case included."}
                ],
                "http://ogp.me/ns#locale": [{"@value": "de_DE"}],
                "http://ogp.me/ns#image": [
                    {"@value": "https://example-store.test/media/medal-set.jpg"}
                ],
                "product:price": [{"@value": "275.00"}],
                "product:price:currency": [{"@value": "EUR"}],
            }
        ],
    }

    result = await extract_standard(
        data,
        "https://example-store.test/shop/collectible-medal-set-1940s/",
        preferred=["json-ld", "microdata", "opengraph", "rdfa"],
    )

    # --- Assertions ---
    assert isinstance(result, dict)
    assert is_valid_product(result)

    # Shops item ID assertion
    assert result["shopsItemId"] == "A1023"

    # Title assertions
    assert result["title"]["text"] == "Collectible Medal Set 1940s"
    assert "language" in result["title"]
    assert result["title"]["language"] == "de"

    # Description assertions
    assert "description" in result
    assert "well-preserved" in result["description"]["text"].lower()
    assert result["description"]["language"] == "de"

    # Price assertions
    assert result["price"]["amount"] == 27500
    assert result["price"]["currency"] == "EUR"

    # Image assertions
    assert result["images"] == [
        "https://example-store.test/media/medal-set.jpg",
        "https://example-store.test/shop/_3780602.JPG?ts=1759398792",
        "https://example-store.test/shop/_3780601.JPG?ts=1759398792",
    ]

    # Product state and URL assertions
    assert result["state"] == "AVAILABLE"
    assert (
        result["url"] == "https://example-store.test/shop/collectible-medal-set-1940s/"
    )


@pytest.mark.asyncio
async def test_extract_standard_returns_none_when_no_data():
    """Test that None is returned when no valid product data exists."""
    data = {
        "microdata": [],
        "json-ld": [],
        "opengraph": [],
        "rdfa": [],
    }
    result = await extract_standard(data, "http://example.com")
    assert result is None


@pytest.mark.asyncio
async def test_extract_standard_with_only_json_ld():
    """Test extraction with only JSON-LD data."""
    data = {
        "json-ld": [
            {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Test Product",
                "description": "Test Description",
                "sku": "TEST123",
                "offers": {
                    "@type": "Offer",
                    "price": "99.99",
                    "priceCurrency": "EUR",
                    "availability": "https://schema.org/InStock",
                },
            }
        ],
    }
    result = await extract_standard(data, "http://example.com")
    assert result is not None
    assert result["title"]["text"] == "Test Product"
    assert result["price"]["amount"] == 9999
    assert result["price"]["currency"] == "EUR"
    assert result["state"] == "AVAILABLE"


@pytest.mark.asyncio
async def test_extract_standard_no_language_detection_for_unknown_text():
    """Test that language detection is skipped when text is UNKNOWN."""
    data = {
        "opengraph": [
            {
                "properties": [
                    ("og:type", "product"),
                    ("product:price:amount", "50"),
                    ("product:price:currency", "EUR"),
                ]
            }
        ],
    }
    result = await extract_standard(data, "http://example.com")
    assert result is not None
    # Language should remain UNKNOWN, not be detected as random language like "sw"
    assert result["title"]["language"] == "UNKNOWN"
    assert result["description"]["language"] == "UNKNOWN"


def test_is_valid_product_with_title():
    """Test that a product with a title is considered valid."""
    product = {"title": {"text": "Test Product"}, "price": {"amount": 0}}
    assert is_valid_product(product) is True


def test_is_valid_product_with_price():
    """Test that a product with a price > 0 is considered valid."""
    product = {"title": {"text": ""}, "price": {"amount": 100}}
    assert is_valid_product(product) is False


def test_is_valid_product_invalid():
    """Test that a product without title and price is invalid."""
    product = {"title": {"text": ""}, "price": {"amount": 0}}
    assert is_valid_product(product) is False


def test_is_valid_product_none():
    """Test that None is invalid."""
    assert is_valid_product(None) is False


def test_merge_products_basic():
    """Test basic product merging."""
    base = {
        "title": {"text": "Product A", "language": "en"},
        "price": {"amount": 100, "currency": "EUR"},
    }
    new = {
        "description": {"text": "New description", "language": "en"},
        "state": "AVAILABLE",
    }
    merged = merge_products(base, new)
    assert merged["title"]["text"] == "Product A"
    assert merged["description"]["text"] == "New description"
    assert merged["price"]["amount"] == 100
    assert merged["state"] == "AVAILABLE"


def test_merge_products_price_override():
    """Test that valid price from new product overrides zero price."""
    base = {"price": {"amount": 0, "currency": "EUR"}}
    new = {"price": {"amount": 500, "currency": "USD"}}
    merged = merge_products(base, new)
    assert merged["price"]["amount"] == 500
    assert merged["price"]["currency"] == "USD"


def test_merge_products_shopsitemid_prefers_non_url():
    """Test that non-URL shopsItemId is preferred over URL."""
    base = {"shopsItemId": "http://example.com/product"}
    new = {"shopsItemId": "SKU123"}
    merged = merge_products(base, new)
    assert merged["shopsItemId"] == "SKU123"


def test_merge_products_images_deduplication():
    """Test that images are deduplicated during merge."""
    base = {"images": ["http://example.com/image1.jpg"]}
    new = {
        "images": [
            "http://example.com/image1.jpg",  # Duplicate
            "http://example.com/image2.jpg",  # New
        ]
    }
    merged = merge_products(base, new)
    assert len(merged["images"]) == 2
    assert "http://example.com/image1.jpg" in merged["images"]
    assert "http://example.com/image2.jpg" in merged["images"]


def test_merge_products_replaces_unknown_values():
    """Test that UNKNOWN values are replaced with valid values."""
    base = {"currency": "UNKNOWN", "state": "UNKNOWN"}
    new = {"currency": "EUR", "state": "AVAILABLE"}
    merged = merge_products(base, new)
    assert merged["currency"] == "EUR"
    assert merged["state"] == "AVAILABLE"


def test_merge_products_handles_string_images():
    """Test that merge handles images that are strings like 'UNKNOWN'."""
    base = {"images": "UNKNOWN"}
    new = {"images": ["http://example.com/image.jpg"]}
    merged = merge_products(base, new)
    assert isinstance(merged["images"], list)
    assert "http://example.com/image.jpg" in merged["images"]
