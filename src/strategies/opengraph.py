from typing import Optional, Dict, Any
from .base import BaseExtractor


class OpenGraphExtractor(BaseExtractor):
    """
    Extractor for OpenGraph meta tags.

    Handles og: and product: prefixed meta properties for product data.
    """

    name = "opengraph"

    async def extract(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from OpenGraph meta tags.

        Args:
            data: Dictionary containing structured data with 'opengraph' key
            url: The URL of the page being processed

        Returns:
            Standardized product dictionary or None if no product found
        """
        og_data = data.get("opengraph", {})
        og = self._normalize_opengraph_data(og_data)

        if not og:
            return None

        og_type = self._get_first_value(og, "og:type", "").lower()
        if og_type not in ["product"]:
            return None

        # Extract and normalize price
        price_str = self._get_first_value(
            og, "product:price:amount"
        ) or self._get_first_value(og, "og:price:amount")
        price = self._parse_price_string(price_str)

        currency = self._get_first_value(
            og, "product:price:currency"
        ) or self._get_first_value(og, "og:price:currency", "UNKNOWN")

        availability = self._get_first_value(
            og, "product:availability"
        ) or self._get_first_value(og, "og:availability", "")

        # Extract language from locale
        locale = self._get_first_value(og, "og:locale", "")
        language = self._extract_language_from_locale(locale)

        product_url = self._get_first_value(og, "og:url", url)
        image = self._get_first_value(og, "og:image")

        return self._build_product_dict(
            item_id=url,  # OpenGraph typically doesn't have SKU, use fallback URL
            title=self._get_first_value(og, "og:title", "UNKNOWN"),
            description=self._get_first_value(og, "og:description", "UNKNOWN"),
            price=price,
            currency=currency,
            availability=availability,
            url=product_url,
            images=image if image else None,
            language=language,
        )

    def _normalize_opengraph_data(self, og_data: Any) -> Optional[Dict[str, Any]]:
        """
        Normalize OpenGraph data structure from extruct output.

        Handles various OpenGraph data formats from extruct, extracting
        the properties dictionary or using the data directly.

        Args:
            og_data: OpenGraph data from extruct (can be dict, list, or other)

        Returns:
            Normalized dictionary or None if invalid
        """
        if isinstance(og_data, dict) and "properties" in og_data:
            return dict(og_data["properties"])
        elif isinstance(og_data, list) and og_data:
            first = og_data[0]
            return dict(first.get("properties", [])) if "properties" in first else first
        return None
