from typing import Any, Dict

from src.core.scraper.schemas.extracted_product import ExtractedProduct


def map_extracted_product_to_schema(
    product: ExtractedProduct, url: str
) -> Dict[str, Any]:
    """
    Maps an ExtractedProduct instance to the PutProductsCollectionData dict format.
    """
    if product is None:
        # Optional: raise ValueError("product is None")
        return {}

    result = {
        "shopsProductId": product.shop_item_id or url,
        "title": {
            "text": product.title or "",
            "language": product.language or "UNKNOWN",
        },
        "description": {
            "text": product.description or "",
            "language": product.language or "UNKNOWN",
        },
        "state": product.state if product.state else "UNKNOWN",
        "url": url,
        "images": product.images or [],
        "auctionStart": product.auctionStart,
        "auctionEnd": product.auctionEnd,
    }

    # Optional: priceEstimateMin
    if (
        product.priceEstimateMinAmount is not None
        and product.priceEstimateMinCurrency is not None
    ):
        result["priceEstimateMin"] = {
            "amount": product.priceEstimateMinAmount,
            "currency": product.priceEstimateMinCurrency,
        }
    # Optional: priceEstimateMax
    if (
        product.priceEstimateMaxAmount is not None
        and product.priceEstimateMaxCurrency is not None
    ):
        result["priceEstimateMax"] = {
            "amount": product.priceEstimateMaxAmount,
            "currency": product.priceEstimateMaxCurrency,
        }

    if product.priceAmount is not None and product.priceCurrency is not None:
        result["price"] = {
            "amount": product.priceAmount,
            "currency": product.priceCurrency,
        }
    return result
