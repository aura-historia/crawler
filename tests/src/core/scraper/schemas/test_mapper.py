import pytest

from src.core.scraper.schemas.extracted_product import (
    ExtractedProduct,
    LocalizedText,
    MonetaryValue,
)
from src.core.scraper.schemas.mapper import map_extracted_product_to_api


def make_extracted_product(**overrides: object) -> ExtractedProduct:
    data: dict[str, object] = {
        "is_product": True,
        "shopsProductId": "SKU-123",
        "title": LocalizedText(text="Vintage Clock", language="en"),
        "description": None,
        "price": None,
        "priceEstimateMin": None,
        "priceEstimateMax": None,
        "state": "AVAILABLE",
        "images": ["https://shop.test/images/main.jpg"],
        "auctionStart": None,
        "auctionEnd": None,
        "url": "https://shop.test/product/sku-123",
    }
    data.update(overrides)
    return ExtractedProduct(**data)


def test_raises_for_non_product_page() -> None:
    product = make_extracted_product(is_product=False)

    with pytest.raises(ValueError, match="Cannot send non-product pages to backend"):
        map_extracted_product_to_api(product)


def test_raises_when_url_missing() -> None:
    product = make_extracted_product(url=None)

    with pytest.raises(
        ValueError, match="Product must have a URL to be sent to backend"
    ):
        map_extracted_product_to_api(product)


def test_maps_minimal_product_payload() -> None:
    product = make_extracted_product(images=[])

    result = map_extracted_product_to_api(product)

    assert result.shops_product_id == "SKU-123"
    assert result.url == "https://shop.test/product/sku-123"
    assert result.state.value == "AVAILABLE"
    assert result.title.text == "Vintage Clock"
    assert result.title.language.value == "en"
    assert result.description is None
    assert result.price is None
    assert result.images == []
    assert result.auction_start is None
    assert result.auction_end is None


def test_maps_full_product_payload() -> None:
    product = make_extracted_product(
        description=LocalizedText(text="Originalbeschreibung", language="de"),
        price=MonetaryValue(amount=19900, currency="EUR"),
        images=[
            "https://shop.test/images/1.jpg",
            "https://shop.test/images/2.jpg",
        ],
        auctionStart="2025-01-10T09:00:00Z",
        auctionEnd="2025-01-20T18:00:00Z",
        state="SOLD",
    )

    result = map_extracted_product_to_api(product)

    assert result.description and result.description.text == "Originalbeschreibung"
    assert result.description.language.value == "de"
    assert result.price and result.price.amount == 19900
    assert result.price.currency.value == "EUR"
    assert result.images == [
        "https://shop.test/images/1.jpg",
        "https://shop.test/images/2.jpg",
    ]
    assert result.state.value == "SOLD"
    assert result.auction_start == "2025-01-10T09:00:00Z"
    assert result.auction_end == "2025-01-20T18:00:00Z"
