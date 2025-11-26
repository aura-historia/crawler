from typing import Optional, Dict, Any
from .base import BaseExtractor


class MicrodataExtractor(BaseExtractor):
    """
    Extractor for Microdata structured data format.

    Handles Schema.org Microdata and data-vocabulary.org product markup.
    """

    name = "microdata"

    async def extract(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from microdata structure.

        Args:
            data: Dictionary containing structured data with 'microdata' key
            url: The URL of the page being processed

        Returns:
            Standardized product dictionary or None if no product found
        """
        products = self._find_product_items(
            data.get("microdata", []), type_keys=["type", "@type"]
        )
        if not products:
            return None

        product = products[0]
        props = product.get("properties", {})
        offers = self._normalize_offers(props.get("offers", {}))

        # URL priority: offers.url > props.url > fallback url
        product_url = offers.get("url")
        if not product_url:
            product_url = props.get("url", url)

        return self._build_product_dict(
            item_id=self._safe_get(props, "sku") or self._safe_get(props, "productID"),
            title=self._safe_get(props, "name"),
            description=self._safe_get(props, "description"),
            price=self._safe_get(offers, "price"),
            currency=self._safe_get(offers, "priceCurrency"),
            availability=self._safe_get(offers, "availability"),
            url=product_url,
            images=self._safe_get(props, "image"),
            language=self._safe_get(props, "inLanguage", default="UNKNOWN")
            or "UNKNOWN",
        )
