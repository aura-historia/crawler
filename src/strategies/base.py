from typing import Optional, List, Dict, Any, Union
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from abc import ABC, abstractmethod
from ..core.utils.availability_normalizer import map_availability_to_state

# Constant for RDFa-style value key to avoid repeating the literal
AT_VALUE = "@value"


class BaseExtractor(ABC):
    """
    Abstract base class for all structured data extractors.

    Provides common functionality for extracting product information from
    different structured data formats (JSON-LD, Microdata, RDFa, OpenGraph).
    """

    name: str = "base"

    @abstractmethod
    async def extract(self, data: dict, url: str) -> Optional[Dict[str, Any]]:
        """
        Extract product data from the given data structure.

        Args:
            data: Dictionary containing structured data extracted by extruct
            url: The URL of the page being processed (used as fallback)

        Returns:
            A single product dictionary with standardized fields, or None if no valid product found

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError

    def _build_product_dict(
        self,
        item_id: Optional[str],
        title: Optional[str],
        description: Optional[str],
        price: Union[float, int, str, None],
        currency: Optional[str],
        availability: Optional[str],
        url: str,
        images: Union[List[str], str, None],
        language: str = "UNKNOWN",
    ) -> Dict[str, Any]:
        """
        Build a standardized product dictionary with consistent structure.

        Args:
            item_id: Product identifier (SKU, product ID, etc.)
            title: Product title/name
            description: Product description
            price: Product price (can be float, int, or string)
            currency: Currency code (e.g., "EUR", "USD")
            availability: Availability status string
            url: Product URL
            images: Single image URL, list of image URLs, or None
            language: Language code (ISO 639-1, e.g., "de", "en")

        Returns:
            Dictionary with standardized product fields:
                - shopsItemId: str
                - title: dict with 'text' and 'language'
                - description: dict with 'text' and 'language'
                - price: dict with 'currency' and 'amount' (in cents)
                - state: str (availability state)
                - url: str
                - images: list of str
        """
        return {
            "shopsItemId": str(item_id or url),
            "title": {"text": title or "UNKNOWN", "language": language},
            "description": {
                "text": (description or "UNKNOWN").strip(),
                "language": language,
            },
            "price": self._normalize_price(price, currency),
            "state": map_availability_to_state(availability),
            "url": url,
            "images": self._normalize_images(images),
        }

    def _normalize_price(
        self, price: Union[float, int, str, None], currency: Optional[str]
    ) -> Dict[str, Any]:
        """
        Convert price to standardized format with amount in cents.

        Args:
            price: Price value (can be floated, int, string, or None)
            currency: Currency code (e.g., "EUR", "USD")

        Returns:
            Dictionary with 'currency' and 'amount' keys.
            Amount is an integer representing cents (price * 100).
            Returns amount="UNKNOWN" if price is "UNKNOWN" string.
            Returns amount=0 if price cannot be parsed.
        """
        price_spec = {"currency": currency or "UNKNOWN", "amount": 0}

        if price is None:
            return price_spec

        # Special handling for "UNKNOWN" string
        if price == "UNKNOWN":
            price_spec["amount"] = "UNKNOWN"
            return price_spec

        try:
            cents = int(
                (Decimal(str(price)) * 100).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )
            price_spec["amount"] = cents
        except (InvalidOperation, ValueError, TypeError):
            pass

        return price_spec

    def _normalize_images(self, images: Union[List[str], str, None]) -> List[str]:
        """
        Normalize and deduplicate image URLs.

        Args:
            images: Single image URL, list of URLs, or None

        Returns:
            List of unique image URLs, preserving order. Empty list if input is None or invalid.
        """
        if images is None:
            return []
        if isinstance(images, str):
            images = [images]
        if not isinstance(images, list):
            return []
        return list(dict.fromkeys(img for img in images if img))

    def _safe_get(self, data: Any, *keys: str, default: Any = None) -> Any:
        """
        Safely get nested dictionary values without raising KeyError.

        Args:
            data: Dictionary or nested structure to search
            *keys: One or more keys to traverse
            default: Value to return if any key is not found

        Returns:
            The value at the specified path, or default if not found
        """
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
                if current is None:
                    return default
            else:
                return default
        return current if current is not None else default

    def _get_first_value(self, data: Any, key: str, default: str = "") -> str:
        """
        Get first value from a property that may be a list or dict.

        Handles various data formats:
        - List of dicts with @value key (RDFa format)
        - List of strings or values
        - Direct value

        Args:
            data: Dictionary containing the property
            key: Property key to retrieve
            default: Default value if key not found or empty

        Returns:
            First value found, or default if none
            "value"
        """
        if not isinstance(data, dict):
            return default

        value = data.get(key, default)

        # Handle list values
        if isinstance(value, list):
            if not value:
                return default
            first = value[0]
            # Handle RDFa-style @value objects
            has_at_value = isinstance(first, dict) and AT_VALUE in first
            if has_at_value:
                return first[AT_VALUE]
            return first if first else default

        return value if value else default

    def _get_all_values(self, data: Dict[str, Any], key: str) -> List[str]:
        """
        Get all values from a property as a list.

        Handles RDFa-style lists with @value objects and regular lists.

        Args:
            data: Dictionary containing the property
            key: Property key to retrieve

        Returns:
            List of all values found, empty list if none
        """
        items = data.get(key, [])
        if not isinstance(items, list):
            return [items] if items else []

        result = []
        for item in items:
            has_at_value = isinstance(item, dict) and AT_VALUE in item
            if has_at_value:
                result.append(item[AT_VALUE])
            elif isinstance(item, dict):
                # Skip dicts without AT_VALUE
                continue
            elif item:
                # Include non-dict items directly
                result.append(str(item))

        return result

    def _parse_price_string(self, price_str: Optional[str]) -> Optional[str]:
        """
        Parse and normalize price string to decimal format.

        Handles European decimal format (comma as decimal separator),
        removes spaces, and validates the result.

        Args:
            price_str: Price string to parse (e.g., "19,99", "29.95", "1 234,56")

        Returns:
            Normalized price string with dot as decimal separator,
            "0" if empty string or invalid format,
            None if input is None
        """
        if price_str is None:
            return None

        if price_str == "":
            return "0"

        if price_str in ("UNKNOWN", "0"):
            return price_str

        # Normalize: remove spaces, convert comma to dot
        normalized = str(price_str).replace(" ", "").replace(",", ".")

        # Verify it's a valid number
        try:
            float(normalized)
            return normalized
        except (ValueError, TypeError):
            return "0"  # Return 0 for invalid formats instead of UNKNOWN

    def _extract_language_from_locale(self, locale: str) -> str:
        """
        Extract two-letter language code from locale string.

        Args:
            locale: Locale string (e.g., "en_US", "de-DE", "fr")

        Returns:
            Two-letter ISO 639-1 language code or "UNKNOWN" if invalid
        """
        if not locale or not isinstance(locale, str):
            return "UNKNOWN"

        # Split on common separators and take first part
        for sep in ("_", "-"):
            if sep in locale:
                lang = locale.split(sep)[0]
                return lang.lower() if lang else "UNKNOWN"

        # If no separator, assume it's already a language code
        # Locale is a non-empty string here. Normalize and return two-letter code.
        locale_lower = locale.lower()
        if len(locale_lower) == 2:
            return locale_lower
        return locale_lower[:2]

    def _normalize_offers(self, offers: Any) -> Dict[str, Any]:
        """
        Normalize offers data to consistent dictionary format.

        Handles various offer structures from different formats:
        - Single dict
        - List of dicts (returns first)
        - Microdata-style with nested 'properties'

        Args:
            offers: Offers data (can be dict, list, or other)

        Returns:
            Dictionary containing offer information, empty dict if invalid
        """
        if isinstance(offers, dict):
            # Microdata-style with properties wrapper
            if "properties" in offers:
                return offers["properties"]
            return offers

        if isinstance(offers, list):
            if not offers:
                return {}
            first_offer = offers[0]
            if isinstance(first_offer, dict):
                # Check for nested properties in list items
                return first_offer.get("properties", first_offer)
            return {}

        return {}

    def _extract_type_from_dict(self, d: Dict[str, Any]) -> Optional[str]:
        """Extract a single string type from a dict using common keys.

        Returns the first non-empty string found for keys AT_VALUE, 'value', '@id', 'id'.
        """
        for k in (AT_VALUE, "value", "@id", "id"):
            v = d.get(k)
            if isinstance(v, str) and v:
                return v
        return None

    def _single_type_to_list(self, candidate: Any) -> List[str]:
        """Handle single (non-list) candidate: string or dict -> list of strings or empty list."""
        if isinstance(candidate, str):
            return [candidate]
        if isinstance(candidate, dict):
            v = self._extract_type_from_dict(candidate)
            return [v] if v else []
        return []

    def _collect_type_strings(self, candidate: Any) -> List[str]:
        """Normalize a candidate value into a list of type strings.

        Uses recursion for lists and a small helper for single values to keep complexity low.
        """
        if isinstance(candidate, list):
            collected: List[str] = []
            for el in candidate:
                collected.extend(self._collect_type_strings(el))
            # filter and normalize here
            return [t for t in collected if isinstance(t, str) and t and t.strip()]

        return self._single_type_to_list(candidate)

    def _extract_types(self, item: Any, keys: Optional[List[str]] = None) -> List[str]:
        """
        Extract type strings from a data item using multiple possible keys.

        Handles formats like:
        - string ("Product")
        - list of strings
        - dicts (e.g. RDFa entries like [{"@value": "Product"}])
        - URIs ("http://schema.org/Product")

        Args:
            item: The dict to inspect for type information
            keys: Optional list of keys to look for (defaults to common keys)

        Returns:
            A list of type strings
        """
        if not isinstance(item, dict):
            return []

        if keys is None:
            keys = ["@type", "type", "http://ogp.me/ns#type"]

        # Collect candidate values for the configured keys
        candidates: List[Any] = [item[k] for k in keys if k in item]

        # Flatten and normalize using helpers in one comprehension to reduce branching
        collected = [
            t
            for c in candidates
            for t in self._collect_type_strings(c)
            if isinstance(t, str) and t and t.strip()
        ]

        return [t.strip() for t in collected]

    def _is_product_item(
        self, item: Any, type_keys: Optional[List[str]] = None
    ) -> bool:
        """
        Return True if the given item represents a Product type.

        Performs normalization to accept plain 'Product', various casing,
        and URIs whose last segment is 'Product'.
        """
        types = self._extract_types(item, keys=type_keys)
        if not types:
            return False

        for t in types:
            lowered = t.strip().lower()
            if lowered == "product":
                return True
            # Check last path/fragment segment (handles URIs like http://schema.org/Product)
            last = lowered.split("/")[-1].split("#")[-1]
            if last == "product":
                return True
        return False

    def _find_product_items(
        self, data: Any, type_keys: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Recursively find product dicts in an arbitrary structured data blob.

        Traverses dicts and lists, returning any dict that matches _is_product_item.
        Useful for JSON-LD (@graph), Microdata trees and RDFa lists.
        """
        found: List[Dict[str, Any]] = []

        if isinstance(data, dict):
            if self._is_product_item(data, type_keys=type_keys):
                found.append(data)
            # Still traverse values (covers nested @graph and microdata trees)
            for v in data.values():
                found.extend(self._find_product_items(v, type_keys=type_keys))
        elif isinstance(data, list):
            for el in data:
                found.extend(self._find_product_items(el, type_keys=type_keys))

        return found
