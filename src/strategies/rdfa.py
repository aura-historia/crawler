from typing import Optional, Dict, Any
from .base import BaseExtractor


class RdfaExtractor(BaseExtractor):
    """
    Extractor for RDFa (Resource Description Framework in Attributes) structured data.

    Handles RDFa product markup using ogp.me namespace.
    """

    name = "rdfa"

    async def extract(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product information from RDFa structured data.

        Args:
            data: Dictionary containing structured data with 'rdfa' key
            url: Fallback URL if product data doesn't include one

        Returns:
            Standardized product dictionary or None if no product found
        """
        products = self._find_product_items(
            data.get("rdfa", []), type_keys=["http://ogp.me/ns#type"]
        )
        if not products:
            return None
        product_item = products[0]

        # Extract basic product information
        title = self._get_first_value(product_item, "http://ogp.me/ns#title", "UNKNOWN")
        description = self._get_first_value(
            product_item, "http://ogp.me/ns#description", "UNKNOWN"
        )
        product_url = self._get_first_value(product_item, "http://ogp.me/ns#url", url)
        images = self._get_all_values(product_item, "http://ogp.me/ns#image")

        # Extract language
        language_value = self._get_first_value(
            product_item, "http://ogp.me/ns#locale", ""
        )
        language = language_value[:2] if language_value else "UNKNOWN"

        # Extract price information
        raw_price = self._get_first_value(
            product_item, "product:price:amount"
        ) or self._get_first_value(product_item, "product:price", "0")
        price = self._parse_price_string(raw_price)
        currency = self._get_first_value(
            product_item, "product:price:currency", "UNKNOWN"
        )

        # Extract availability
        availability = self._get_first_value(product_item, "product:availability", "")

        return self._build_product_dict(
            item_id=None,  # RDFa typically uses URL as identifier
            title=title,
            description=description,
            price=price,
            currency=currency,
            availability=availability,
            url=product_url,
            images=images,
            language=language,
        )
