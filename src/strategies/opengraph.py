from typing import Optional
from .base import BaseExtractor
from ..core.utils.availability_normalizer import map_availability_to_state


class OpenGraphExtractor(BaseExtractor):
    name = "opengraph"

    async def extract(self, data: dict, url: str) -> Optional[dict]:
        og_data = data.get("opengraph", {})

        # Convert Extruct "properties" list to dict if needed
        if isinstance(og_data, dict) and "properties" in og_data:
            og = dict(og_data["properties"])
        elif isinstance(og_data, list) and og_data:
            first = og_data[0]
            og = dict(first.get("properties", [])) if "properties" in first else first
        else:
            return None

        def get_val(key, default=""):
            val = og.get(key, default)
            if isinstance(val, list) and val:
                return val[0]
            return val

        og_type = get_val("og:type", "").lower()
        if og_type not in ["product", "article"]:
            return None

        # Extract price safely, normalize European decimal commas
        price_str = get_val("product:price:amount") or get_val("og:price:amount")

        if not price_str:
            price_str = "0"

        price_str = price_str.replace(",", ".").replace(" ", "")

        try:
            price_amount = int(round(float(price_str) * 100))
        except (ValueError, TypeError):
            price_amount = "UNKNOWN"

        currency = get_val("product:price:currency") or get_val(
            "og:price:currency", "UNKNOWN"
        )

        availability = (
            get_val("product:availability") or get_val("og:availability") or ""
        )

        locale = get_val("og:locale", "")
        language = locale.split("_")[0] if locale else "UNKNOWN"

        state = map_availability_to_state(availability)

        # Build result
        return {
            "shopsItemId": url,
            "title": {"text": get_val("og:title", "UNKNOWN"), "language": language},
            "description": {
                "text": get_val("og:description", "UNKNOWN"),
                "language": language,
            },
            "price": {"currency": currency, "amount": price_amount},
            "state": state,
            "url": get_val("og:url", url),
            "images": [get_val("og:image")] if get_val("og:image") else [],
        }
