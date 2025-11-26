from typing import Optional, Dict, Any, List
from .base import BaseExtractor


def _extract_images(images: Any) -> List[str]:
    """
    Extract and normalize image URLs from various JSON-LD formats.

    Handles both string URLs and dictionary objects with multiple URL fields.

    Args:
        images: Image data (can be string, dict, or list of either)

    Returns:
        Deduplicated list of image URLs

    Examples:
        Input: "https://example.com/img.jpg"
        Output: ["https://example.com/img.jpg"]

        Input: [{"contentUrl": "img1.jpg"}, {"url": "img2.jpg"}]
        Output: ["img1.jpg", "img2.jpg"]
    """
    if not isinstance(images, list):
        images = [images] if images else []

    normalized_images = []
    for img in images:
        if isinstance(img, dict):
            # Prefer contentUrl, then url, @id, thumbnailUrl
            for key in ("contentUrl", "url", "@id", "thumbnailUrl"):
                img_url = img.get(key)
                if img_url and isinstance(img_url, str):
                    normalized_images.append(img_url)
                    break
        elif isinstance(img, str):
            normalized_images.append(img)

    # Remove duplicates while preserving order
    return list(dict.fromkeys(normalized_images))


class JsonLDExtractor(BaseExtractor):
    """
    Extractor for JSON-LD structured data format.

    Handles JSON-LD product data including nested structures in @graph arrays.
    """

    name = "json-ld"

    async def extract(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from JSON-LD structure.

        Args:
            data: Dictionary containing structured data with 'json-ld' key
            url: The URL of the page being processed

        Returns:
            Standardized product dictionary or None if no product found
        """
        products = self._find_product_items(
            data.get("json-ld", []), type_keys=["@type", "type"]
        )
        if not products:
            return None

        product_json = products[0]
        offers = self._normalize_offers(product_json.get("offers"))

        # Extract price with fallback to priceSpecification
        price = self._safe_get(offers, "price")
        currency = self._safe_get(offers, "priceCurrency")

        # Only use price if currency is also present, otherwise try priceSpecification
        if price is not None and currency is None:
            price = None  # Reset price if no currency

        if price is None:
            price_spec = self._safe_get(offers, "priceSpecification")
            if isinstance(price_spec, list) and price_spec:
                price = self._safe_get(price_spec[0], "price")
                currency = currency or self._safe_get(price_spec[0], "priceCurrency")

        images = _extract_images(product_json.get("image", []))

        # URL priority: product.url > offers.url > fallback url
        product_url = product_json.get("url")
        if not product_url:
            product_url = offers.get("url", url)

        return self._build_product_dict(
            item_id=self._safe_get(product_json, "sku")
            or self._safe_get(product_json, "productGroupID"),
            title=self._safe_get(product_json, "name"),
            description=self._safe_get(product_json, "description"),
            price=price,
            currency=currency,
            availability=self._safe_get(offers, "availability"),
            url=product_url,
            images=images,
            language=self._safe_get(product_json, "inLanguage", default="UNKNOWN")
            or "UNKNOWN",
        )
